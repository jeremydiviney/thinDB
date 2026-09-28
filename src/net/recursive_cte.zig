//! `WITH RECURSIVE` iteration driver: the operator a recursive CTE's stage
//! drains. It runs the anchor, then compiles and runs the recursive arms once
//! per iteration over the rows the previous iteration added (semi-naive
//! evaluation), until an iteration adds none. See DESIGN.md §6.6.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ir = @import("../ir/ir.zig");
const exec = @import("../exec/exec.zig");
const engine = @import("../engine/engine.zig");
const engine_v2 = @import("../exec/engine_v2.zig");
const mat_stage = @import("../exec/mat_stage.zig");
const cast = @import("../exec/cast.zig");
const cell_io = @import("../exec/cell_io.zig");
const scalar_fn = @import("../exec/scalar_fn.zig");
const storage = @import("../storage/storage.zig");
const types = @import("../types.zig");
const cte_stages = @import("cte_stages.zig");

const Column = types.Column;
const ColumnView = storage.ColumnView;
const Stage = mat_stage.Stage;

/// MySQL's default `cte_max_recursion_depth`: the recursive arms may run
/// this many times; one more run over a non-empty working set aborts.
const MAX_ITERATIONS: u64 = 1000;

/// Hash-table bookkeeping charged per distinct row on top of its key bytes.
const SEEN_ENTRY_OVERHEAD: usize = 32;

/// Rows one iteration added, in the CTE's column types.
const Delta = struct {
    stores: []engine.ColumnStore,
    rows: usize,

    const empty: Delta = .{ .stores = &.{}, .rows = 0 };

    fn deinit(self: Delta, allocator: Allocator) void {
        for (self.stores) |*st| st.deinit(allocator);
        if (self.stores.len > 0) allocator.free(self.stores);
    }
};

/// The recursive CTE `info` over the plan's stages so far (`map`), as the
/// query its own stage drains. The anchor compiles here, so the CTE's schema
/// is known before anything runs: the anchor's types, the column list's
/// names (else the anchor's), every column nullable. When the body has a
/// LIMIT, a Limit sits on top and the driver stops once enough rows exist.
pub fn create(input: engine_v2.CompileInput, info: *const ir.Op.Recursive, map: *cte_stages.StageMap) anyerror!exec.Query {
    var driver = try createDriver(input, info, map);
    const n = info.limit orelse return driver;
    errdefer driver.deinit();
    return exec.Limit.createOffset(input.allocator, driver, @intCast(n), @intCast(info.offset));
}

fn createDriver(input_in: engine_v2.CompileInput, info: *const ir.Op.Recursive, map: *cte_stages.StageMap) anyerror!exec.Query {
    const allocator = input_in.allocator;
    const arena = input_in.node_arena;
    var anchor = try cte_stages.compileBlock(input_in, info.anchor, map);
    errdefer anchor.deinit();

    const src = anchor.outputSchema();
    if (info.columns) |cols| if (cols.len != src.len) return exec.Error.TypeMismatch;
    const schema = try arena.alloc(Column, src.len);
    for (src, schema, 0..) |c, *out, i| {
        const name = if (info.columns) |cols| cols[i] else types.unqualifiedName(c.name);
        out.* = .{ .name = try arena.dupe(u8, name), .type = columnType(c.type), .nullable = true };
    }

    var pins: std.ArrayListUnmanaged(*Stage) = .empty;
    try collectPins(arena, info.step, map, &pins);
    var own_map = try map.clone(allocator);
    errdefer own_map.deinit(allocator);
    const views = try allocator.alloc(ColumnView, schema.len);
    errdefer allocator.free(views);
    const assign_from = try allocator.alloc(?types.Type, schema.len);
    errdefer allocator.free(assign_from);
    const assigned = try allocator.alloc(ColumnView, schema.len);
    errdefer allocator.free(assigned);

    var input = input_in;
    input.win_registry = null;
    input.region_ref_counts = null;

    const self = try allocator.create(RecursiveCte);
    errdefer allocator.destroy(self);
    self.* = .{
        .allocator = allocator,
        .input = input,
        .info = info,
        .map = own_map,
        .pins = pins.items,
        .anchor = anchor,
        .schema = schema,
        .views = views,
        .assign_from = assign_from,
        .assigned = assigned,
        .seen_arena = std.heap.ArenaAllocator.init(allocator),
        .step_arena = std.heap.ArenaAllocator.init(allocator),
        .stop_after = if (info.limit) |n| n +| info.offset else null,
    };
    for (pins.items) |st| st.registerUse();
    return exec.makeQuery(allocator, self);
}

pub const RecursiveCte = struct {
    allocator: Allocator,
    /// The statement's compile input, for compiling the recursive arms at
    /// run time. Its compile-scoped registries are cleared.
    input: engine_v2.CompileInput,
    info: *const ir.Op.Recursive,
    /// The plan's stages, plus this CTE's self-references bound to the
    /// current iteration's working set.
    map: cte_stages.StageMap,
    /// One use held on every plan stage the recursive arms read, so each
    /// iteration's compile finds them alive; released at deinit.
    pins: []const *Stage,
    anchor: ?exec.Query,
    schema: []const Column,
    views: []ColumnView,
    /// Per column, the type the running query's rows convert from by the
    /// assignment rule as they land (`cast.assignColumn`), or null.
    assign_from: []?types.Type,
    /// A batch's columns after that conversion.
    assigned: []ColumnView,
    /// Plan-node memory for the current iteration's compile. The statement's
    /// node arena is reachable only while the statement compiles, and each
    /// iteration's plan dies with its query, so this resets per iteration.
    step_arena: std.heap.ArenaAllocator,
    /// The rows the latest iteration added: emitted, then handed to the next
    /// iteration as its working set.
    delta: Delta = Delta.empty,
    emitted: usize = 0,
    /// Times the recursive arms have run.
    iterations: u64 = 0,
    produced: u64 = 0,
    /// `offset + limit` of a LIMIT over the body: no more rows are needed.
    stop_after: ?u64,
    done: bool = false,
    /// UNION DISTINCT: every row produced so far, as key bytes.
    seen: std.StringHashMapUnmanaged(void) = .empty,
    seen_arena: std.heap.ArenaAllocator,
    key_buf: std.ArrayList(u8) = .empty,
    delta_reserved: usize = 0,
    seen_reserved: usize = 0,

    pub fn next(self: *RecursiveCte) !?exec.Batch {
        while (true) {
            if (self.emitted < self.delta.rows) {
                const lo = self.emitted;
                const n = @min(mat_stage.chunk_rows, self.delta.rows - lo);
                for (self.delta.stores, self.schema, self.views) |*st, c, *v| {
                    v.* = mat_stage.MaterializedResult.presentAsSchemaType(engine.transform.subViewAligned(st.view(), lo, n), c.type);
                }
                self.emitted += n;
                return .{ .schema = self.schema, .values = self.views, .row_count = n };
            }
            if (self.done) return null;
            try self.iterate();
        }
    }

    fn iterate(self: *RecursiveCte) !void {
        var q = if (self.anchor) |a| blk: {
            self.anchor = null;
            break :blk a;
        } else blk: {
            if (self.iterations >= MAX_ITERATIONS) return exec.Error.RecursiveCteDepthExceeded;
            self.iterations += 1;
            break :blk try self.compileStep();
        };
        defer q.deinit();
        q = try self.conform(q);
        self.delta = try newDelta(self.allocator, self.schema);
        self.emitted = 0;
        while (try q.next()) |batch| {
            try self.absorb(batch);
            if (self.limitReached()) break;
        }
        if (self.delta.rows == 0 or self.limitReached()) self.done = true;
    }

    fn limitReached(self: *const RecursiveCte) bool {
        const stop = self.stop_after orelse return false;
        return self.produced >= stop;
    }

    /// The recursive arms over the current delta. The delta moves into a
    /// one-stage set every self-reference reads; the set lives as long as
    /// the returned query.
    fn compileStep(self: *RecursiveCte) !exec.Query {
        const prev = self.delta;
        self.delta = Delta.empty;
        self.releaseDelta();
        _ = self.step_arena.reset(.retain_capacity);
        const set = mat_stage.StageSet.create(self.allocator) catch |err| {
            prev.deinit(self.allocator);
            return err;
        };
        errdefer set.deinit();
        const working = try set.addStage(try WorkingSet.create(self.allocator, self.schema, prev), self.input.accountant);
        working.name = "recursive working set";
        try bindSelfRefs(self.allocator, self.info.step, self.info, working, &self.map);

        var registry: std.AutoHashMapUnmanaged(*const anyopaque, *anyopaque) = .empty;
        defer registry.deinit(self.allocator);
        var input = self.input;
        input.win_registry = &registry;
        input.node_arena = self.step_arena.allocator();
        var q = try cte_stages.compileBlock(input, self.info.step, &self.map);
        errdefer q.deinit();
        set.releaseCompilePins();
        return mat_stage.StagedRoot.create(self.allocator, q, set);
    }

    /// Rows land in the CTE's column types as a write into a table's columns
    /// does: by the assignment rule where it applies (`absorb`), else by a
    /// CAST over `q`. A type with neither is a TypeMismatch. On error `q`
    /// is left to the caller.
    fn conform(self: *RecursiveCte, q: exec.Query) !exec.Query {
        const got = q.outputSchema();
        if (got.len != self.schema.len) return exec.Error.TypeMismatch;
        const arena = self.step_arena.allocator();
        var casts: std.ArrayListUnmanaged(ir.Derived) = .empty;
        for (got, self.schema, self.assign_from) |g, want, *from| {
            from.* = null;
            if (cast.sameRepresentation(g.type, want.type)) continue;
            if (cast.assignsByRule(g.type, want.type)) {
                from.* = g.type;
                continue;
            }
            const fn_name = try scalar_fn.castFnName(arena, want.type) orelse return exec.Error.TypeMismatch;
            try casts.append(arena, try cte_stages.castDerived(arena, g.name, fn_name));
        }
        if (casts.items.len == 0) return q;
        return engine_v2.computeDerivedFused(self.allocator, q, casts.items, self.input.udf_registry);
    }

    fn absorb(self: *RecursiveCte, batch_in: exec.Batch) !void {
        const rows = batch_in.row_count;
        if (rows == 0) return;
        if (batch_in.values.len != self.schema.len) return exec.Error.TypeMismatch;
        var converted: usize = 0;
        defer for (self.assign_from[0..converted], self.assigned[0..converted]) |from, v| {
            if (from != null) cast.freeAssignedColumn(self.allocator, v);
        };
        for (batch_in.values, self.assign_from, self.schema, self.assigned) |v, from, want, *out| {
            out.* = if (from) |f| try cast.assignColumn(self.allocator, v, f, want.type, rows) else v;
            converted += 1;
        }
        const batch: exec.Batch = .{ .schema = self.schema, .values = self.assigned, .row_count = rows };
        const row_bytes = exec.memory.estimateRowBytes(self.schema);
        if (!self.info.distinct) {
            try self.reserve(&self.delta_reserved, row_bytes * rows);
            for (self.delta.stores, batch.values) |*st, v| try appendColumn(self.allocator, st, v, rows);
            self.delta.rows += rows;
            self.produced += rows;
            return;
        }
        for (self.delta.stores, batch.values) |*st, v| {
            if (!tagsMatch(st.*, v) and !v.allNull(rows)) return exec.Error.TypeMismatch;
        }
        var added: usize = 0;
        var key_bytes: usize = 0;
        for (0..rows) |r| {
            self.key_buf.clearRetainingCapacity();
            for (batch.values) |v| {
                if (!v.isValid(r)) {
                    try self.key_buf.append(self.allocator, 0);
                    continue;
                }
                try self.key_buf.append(self.allocator, 1);
                try v.appendValueBytes(self.allocator, &self.key_buf, @intCast(r));
            }
            const gop = try self.seen.getOrPut(self.allocator, self.key_buf.items);
            if (gop.found_existing) continue;
            gop.key_ptr.* = self.seen_arena.allocator().dupe(u8, self.key_buf.items) catch |err| {
                self.seen.removeByPtr(gop.key_ptr);
                return err;
            };
            for (self.delta.stores, batch.values) |*st, v| try appendRow(self.allocator, st, v, r);
            added += 1;
            key_bytes += self.key_buf.items.len + SEEN_ENTRY_OVERHEAD;
        }
        self.delta.rows += added;
        self.produced += added;
        try self.reserve(&self.delta_reserved, row_bytes * added);
        try self.reserve(&self.seen_reserved, key_bytes);
    }

    fn reserve(self: *RecursiveCte, held: *usize, bytes: usize) !void {
        const acct = self.input.accountant orelse return;
        if (bytes == 0) return;
        try acct.reserve(.materialize, bytes);
        held.* += bytes;
    }

    fn releaseDelta(self: *RecursiveCte) void {
        if (self.input.accountant) |acct| {
            if (self.delta_reserved > 0) acct.release(.materialize, self.delta_reserved);
        }
        self.delta_reserved = 0;
    }

    pub fn deinit(self: *RecursiveCte) void {
        if (self.anchor) |*a| a.deinit();
        self.delta.deinit(self.allocator);
        self.releaseDelta();
        if (self.input.accountant) |acct| {
            if (self.seen_reserved > 0) acct.release(.materialize, self.seen_reserved);
        }
        self.seen.deinit(self.allocator);
        self.seen_arena.deinit();
        self.step_arena.deinit();
        self.key_buf.deinit(self.allocator);
        self.map.deinit(self.allocator);
        for (self.pins) |st| st.releaseUse();
        self.allocator.free(self.views);
        self.allocator.free(self.assign_from);
        self.allocator.free(self.assigned);
        self.allocator.destroy(self);
    }

    pub fn outputSchema(self: *RecursiveCte) []const Column {
        return self.schema;
    }

    pub fn addPrune(_: *RecursiveCte, _: exec.Predicate) !void {}

    pub fn stats(self: *RecursiveCte) exec.PipelineStats {
        return .{ .upper_rows = self.stop_after orelse std.math.maxInt(u64) };
    }

    pub fn accountant(self: *RecursiveCte) ?*exec.memory.MemoryAccountant {
        return self.input.accountant;
    }

    pub fn explain(self: *RecursiveCte, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        var buf: [64]u8 = undefined;
        const line = std.fmt.bufPrint(&buf, "RecursiveCte cols={d} distinct={}", .{ self.schema.len, self.info.distinct }) catch "RecursiveCte";
        try exec.explainLine(out, allocator, depth, line);
        if (self.anchor) |a| try a.explain(out, allocator, depth + 1);
    }
};

/// One iteration's working set as a stage source: the previous delta,
/// handed to the stage whole (`takeOwnedChunks`) so it is never copied.
const WorkingSet = struct {
    allocator: Allocator,
    schema: []const Column,
    delta: ?Delta,
    rows: usize,
    views: []ColumnView,
    emitted: bool = false,

    /// Takes ownership of `delta`, even on error.
    fn create(allocator: Allocator, schema: []const Column, delta: Delta) !exec.Query {
        errdefer delta.deinit(allocator);
        const views = try allocator.alloc(ColumnView, schema.len);
        errdefer allocator.free(views);
        const self = try allocator.create(WorkingSet);
        self.* = .{ .allocator = allocator, .schema = schema, .delta = delta, .rows = delta.rows, .views = views };
        return exec.makeQuery(allocator, self);
    }

    pub fn next(self: *WorkingSet) !?exec.Batch {
        const d = self.delta orelse return null;
        if (self.emitted) return null;
        self.emitted = true;
        for (d.stores, self.views) |*st, *v| v.* = st.view();
        return .{ .schema = self.schema, .values = self.views, .row_count = d.rows };
    }

    pub fn takeOwnedChunks(self: *WorkingSet) !?exec.OwnedChunks {
        const d = self.delta orelse return null;
        const chunks = try self.allocator.alloc(exec.OwnedChunk, 1);
        chunks[0] = .{ .stores = d.stores, .rows = d.rows };
        self.delta = null;
        return .{ .chunks = chunks, .alloc = self.allocator };
    }

    pub fn deinit(self: *WorkingSet) void {
        if (self.delta) |d| d.deinit(self.allocator);
        self.allocator.free(self.views);
        self.allocator.destroy(self);
    }

    pub fn outputSchema(self: *WorkingSet) []const Column {
        return self.schema;
    }

    pub fn addPrune(_: *WorkingSet, _: exec.Predicate) !void {}

    pub fn stats(self: *WorkingSet) exec.PipelineStats {
        return .{ .upper_rows = self.rows };
    }

    pub fn accountant(_: *WorkingSet) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(self: *WorkingSet, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        var buf: [64]u8 = undefined;
        const line = std.fmt.bufPrint(&buf, "RecursiveWorkingSet rows={d}", .{self.rows}) catch "RecursiveWorkingSet";
        try exec.explainLine(out, allocator, depth, line);
    }
};

/// A CTE column's type: the anchor's, but an integer narrower than BIGINT
/// widens to BIGINT. MySQL types an integer literal BIGINT where thinDB
/// types it by its value, and a counter or a running product seeded by a
/// literal must not stop at the literal's type.
fn columnType(anchor: types.Type) types.Type {
    return switch (anchor) {
        .tinyint, .smallint, .int => .bigint,
        else => anchor,
    };
}

fn newDelta(allocator: Allocator, schema: []const Column) !Delta {
    const stores = try allocator.alloc(engine.ColumnStore, schema.len);
    var inited: usize = 0;
    errdefer {
        for (stores[0..inited]) |*st| st.deinit(allocator);
        allocator.free(stores);
    }
    for (schema, stores) |c, *st| {
        st.* = try engine.ColumnStore.init(allocator, c.type, true);
        inited += 1;
    }
    return .{ .stores = stores, .rows = 0 };
}

fn isStringTag(t: types.TypeTag) bool {
    return t == .varchar or t == .string or t == .char;
}

fn tagsMatch(st: engine.ColumnStore, v: ColumnView) bool {
    const a = std.meta.activeTag(st.data);
    const b = std.meta.activeTag(v.data);
    return a == b or (isStringTag(a) and isStringTag(b));
}

/// A column of a batch appended to the delta. A NULL-only column may carry
/// a placeholder type of its own; any other type mismatch is refused rather
/// than stored under the wrong type.
fn appendColumn(allocator: Allocator, st: *engine.ColumnStore, v: ColumnView, rows: usize) !void {
    if (tagsMatch(st.*, v)) return engine.transform.appendColumnRange(allocator, v, 0, rows, st);
    if (!v.allNull(rows)) return exec.Error.TypeMismatch;
    for (0..rows) |_| try cell_io.appendNullTo(allocator, st);
}

fn appendRow(allocator: Allocator, st: *engine.ColumnStore, v: ColumnView, row: usize) !void {
    if (tagsMatch(st.*, v)) return engine.transform.appendOneRow(allocator, v, row, st);
    try cell_io.appendNullTo(allocator, st);
}

/// Every plan stage the recursive arms can reach without passing through
/// another stage: what an iteration's compile binds.
fn collectPins(arena: Allocator, op: *const ir.Op, map: *const cte_stages.StageMap, pins: *std.ArrayListUnmanaged(*Stage)) !void {
    switch (op.*) {
        .materialize => |m| {
            if (map.get(op)) |st| {
                for (pins.items) |p| if (p == st) return;
                try pins.append(arena, st);
                return;
            }
            if (m.recursion == null) try collectPins(arena, m.upstream, map, pins);
        },
        .select, .exclude => |p| try collectPins(arena, p.upstream, map, pins),
        .filter => |f| try collectPins(arena, f.upstream, map, pins),
        .order_by => |o| try collectPins(arena, o.upstream, map, pins),
        .group_by => |g| try collectPins(arena, g.upstream, map, pins),
        .compute => |c| try collectPins(arena, c.upstream, map, pins),
        .alias => |a| try collectPins(arena, a.upstream, map, pins),
        .limit => |l| try collectPins(arena, l.upstream, map, pins),
        .window => |w| try collectPins(arena, w.upstream, map, pins),
        .table_fn => |t| for (t.inputs) |inp| try collectPins(arena, inp, map, pins),
        .join => |j| {
            try collectPins(arena, j.left, map, pins);
            try collectPins(arena, j.right, map, pins);
        },
        .set_union => |u| {
            try collectPins(arena, u.left, map, pins);
            try collectPins(arena, u.right, map, pins);
        },
        else => {},
    }
}

/// Point every self-reference of `info` in the recursive arms at `working`.
/// Found by marker rather than by pointer: rewrites before compile may have
/// copied a reference node into its parent's slot.
fn bindSelfRefs(allocator: Allocator, op: *const ir.Op, info: *const ir.Op.Recursive, working: *Stage, map: *cte_stages.StageMap) !void {
    switch (op.*) {
        .materialize => |m| {
            const rec = m.recursion orelse return;
            switch (rec) {
                .self_ref => |owner| if (owner == info) try map.put(allocator, op, working),
                .cte => {},
            }
        },
        .select, .exclude => |p| try bindSelfRefs(allocator, p.upstream, info, working, map),
        .filter => |f| try bindSelfRefs(allocator, f.upstream, info, working, map),
        .order_by => |o| try bindSelfRefs(allocator, o.upstream, info, working, map),
        .group_by => |g| try bindSelfRefs(allocator, g.upstream, info, working, map),
        .compute => |c| try bindSelfRefs(allocator, c.upstream, info, working, map),
        .alias => |a| try bindSelfRefs(allocator, a.upstream, info, working, map),
        .limit => |l| try bindSelfRefs(allocator, l.upstream, info, working, map),
        .window => |w| try bindSelfRefs(allocator, w.upstream, info, working, map),
        .join => |j| {
            try bindSelfRefs(allocator, j.left, info, working, map);
            try bindSelfRefs(allocator, j.right, info, working, map);
        },
        .set_union => |u| {
            try bindSelfRefs(allocator, u.left, info, working, map);
            try bindSelfRefs(allocator, u.right, info, working, map);
        },
        else => {},
    }
}
