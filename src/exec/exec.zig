//! Operator pipeline for v0.1.
//!
//! Type-erased `Query` value backed by a small vtable. Each operator
//! (Scan, Filter, Project, Limit) exposes `next()`, `deinit()`, and
//! `outputSchema()` methods; `makeQuery` lifts a `*Op` into a `Query`.
//!
//! `Query` combinators (`.filter`, `.project`, `.limit`, `.pipe`) build a
//! chain by allocating each downstream operator and wrapping the prior
//! query.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;
const Value = types.Value;
const ValueTag = types.ValueTag;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const api = @import("../api/api.zig");
const Table = api.Table;

pub const Error = error{
    ColumnNotFound,
    TypeMismatch,
    PredicateTypeMismatch,
    UnsupportedOperatorForType,
    SortNoKeys,
    AggregateNoSpecs,
    AggregateColumnRequired,
    AggregateUnsupportedType,
    ArithmeticOverflow,
};

// ---------------------------------------------------------------------------
// Batch
// ---------------------------------------------------------------------------

pub const Batch = struct {
    /// Schema metadata for each output column (name + type), in column order.
    schema: []const Column,
    /// Borrowed column views — pointing into operator-owned buffers. Valid
    /// only until the next `Query.next()` call.
    values: []const ColumnView,
    row_count: usize,

    pub fn columnIndex(self: Batch, name: []const u8) ?usize {
        for (self.schema, 0..) |c, i| {
            if (std.mem.eql(u8, c.name, name)) return i;
        }
        return null;
    }

    pub fn columnView(self: Batch, name: []const u8) ?ColumnView {
        const idx = self.columnIndex(name) orelse return null;
        return self.values[idx];
    }
};

// ---------------------------------------------------------------------------
// Query — type-erased operator handle
// ---------------------------------------------------------------------------

pub const VTable = struct {
    next: *const fn (ptr: *anyopaque) anyerror!?Batch,
    deinit: *const fn (ptr: *anyopaque) void,
    outputSchema: *const fn (ptr: *anyopaque) []const Column,
    /// Operators that can act on hints (e.g. Scan) use them to skip row
    /// groups; others (Filter, Project, Limit) simply forward to upstream.
    addPrune: *const fn (ptr: *anyopaque, pred: Predicate) anyerror!void,
};

pub const Query = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    allocator: Allocator,

    pub fn next(self: *Query) !?Batch {
        return self.vtable.next(self.ptr);
    }

    pub fn deinit(self: *Query) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }

    pub fn outputSchema(self: Query) []const Column {
        return self.vtable.outputSchema(self.ptr);
    }

    pub fn addPrune(self: *Query, pred: Predicate) !void {
        return self.vtable.addPrune(self.ptr, pred);
    }

    // ----- Combinators -----

    pub fn filter(self: Query, expr: PredicateExpr) !Query {
        return Filter.create(self.allocator, self, expr);
    }

    pub fn project(self: Query, columns: []const []const u8) !Query {
        return Project.create(self.allocator, self, columns);
    }

    pub fn limit(self: Query, n: usize) !Query {
        return Limit.create(self.allocator, self, n);
    }

    /// Aggregate over the entire upstream (no grouping).
    pub fn aggregate(self: Query, aggs: []const AggSpec) !Query {
        return Aggregate.create(self.allocator, self, &.{}, aggs);
    }

    /// Hash-grouped aggregation. `group_cols` lists the upstream columns to
    /// group by; one output row is emitted per distinct group.
    pub fn groupBy(self: Query, group_cols: []const []const u8, aggs: []const AggSpec) !Query {
        return Aggregate.create(self.allocator, self, group_cols, aggs);
    }

    /// Sort upstream rows by `sort_specs` (multi-column, ASC/DESC per key).
    /// Blocking — materializes all upstream rows before emitting any output.
    pub fn orderBy(self: Query, sort_specs: []const SortSpec) !Query {
        return Sort.create(self.allocator, self, sort_specs);
    }

    /// `f` is either a function taking `Query` and returning `!Query`, or a
    /// function returning `Query` (we accept both by being generic).
    pub fn pipe(self: Query, f: anytype) !Query {
        return f(self);
    }
};

/// Lift an operator pointer into a Query. The operator type must define
/// `next()`, `deinit()`, and `outputSchema()` methods.
pub fn makeQuery(allocator: Allocator, op: anytype) Query {
    const OpPtr = @TypeOf(op);
    const Op = comptime blk: {
        const info = @typeInfo(OpPtr);
        if (info != .pointer) @compileError("makeQuery: expected pointer to operator");
        break :blk info.pointer.child;
    };

    const Wrapper = struct {
        fn nextWrap(ptr: *anyopaque) anyerror!?Batch {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.next();
        }
        fn deinitWrap(ptr: *anyopaque) void {
            const o: *Op = @ptrCast(@alignCast(ptr));
            o.deinit();
        }
        fn outputSchemaWrap(ptr: *anyopaque) []const Column {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.outputSchema();
        }
        fn addPruneWrap(ptr: *anyopaque, pred: Predicate) anyerror!void {
            const o: *Op = @ptrCast(@alignCast(ptr));
            return o.addPrune(pred);
        }

        const vt: VTable = .{
            .next = nextWrap,
            .deinit = deinitWrap,
            .outputSchema = outputSchemaWrap,
            .addPrune = addPruneWrap,
        };
    };

    return .{ .ptr = op, .vtable = &Wrapper.vt, .allocator = allocator };
}

// ---------------------------------------------------------------------------
// Scan — reads segments (in manifest order) then memtable
// ---------------------------------------------------------------------------

pub const Scan = struct {
    allocator: Allocator,
    io: Io,
    table: *Table,

    segment_count: usize,

    phase: Phase = .segments,
    cur_seg_idx: usize = 0,
    cur_rg_idx: usize = 0,
    cur_segment: ?storage.ReadSegment = null,
    /// Sorted, deduped tombstone offsets for the current segment (or null).
    cur_segment_tomb: ?[]u32 = null,
    /// Prefix sum: `cur_rg_first_row[k]` is the first row offset of row group k
    /// within the current segment.
    cur_rg_first_row: []u32 = &.{},

    decoded: []storage.OwnedColumn,
    decoded_valid: bool = false,
    views: []ColumnView,

    /// Lazily allocated when a row group has rows tombstoned and we need to
    /// materialize a filtered batch. Reused across batches.
    filtered: ?[]ColumnStore = null,

    /// Pushed-down predicates used to skip row groups via min/max stats.
    prunes: std.ArrayList(PruneHint),

    const Phase = enum { segments, memtable, done };

    pub const PruneHint = struct {
        col_idx: usize,
        op: PredicateOp,
        val: Value,
    };

    pub fn create(allocator: Allocator, table: *Table) !Query {
        const n = table.schema.columns.len;

        const decoded = try allocator.alloc(storage.OwnedColumn, n);
        errdefer allocator.free(decoded);
        const views = try allocator.alloc(ColumnView, n);
        errdefer allocator.free(views);

        const self = try allocator.create(Scan);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .io = table.io,
            .table = table,
            .segment_count = table.manifest.segments.items.len,
            .decoded = decoded,
            .views = views,
            .prunes = .empty,
        };

        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Scan) void {
        self.releaseBatch();
        self.closeCurSegment();
        self.prunes.deinit(self.allocator);
        if (self.filtered) |arr| {
            for (arr) |*c| c.deinit(self.allocator);
            self.allocator.free(arr);
        }
        self.allocator.free(self.decoded);
        self.allocator.free(self.views);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    fn closeCurSegment(self: *Scan) void {
        if (self.cur_segment) |*seg| {
            seg.deinit();
            self.cur_segment = null;
        }
        if (self.cur_segment_tomb) |t| {
            self.allocator.free(t);
            self.cur_segment_tomb = null;
        }
        if (self.cur_rg_first_row.len > 0) {
            self.allocator.free(self.cur_rg_first_row);
            self.cur_rg_first_row = &.{};
        }
    }

    fn ensureFilteredBuffers(self: *Scan) ![]ColumnStore {
        if (self.filtered) |arr| return arr;
        const arr = try self.allocator.alloc(ColumnStore, self.table.schema.columns.len);
        errdefer self.allocator.free(arr);
        var inited: usize = 0;
        errdefer for (arr[0..inited]) |*c| c.deinit(self.allocator);
        for (self.table.schema.columns, 0..) |col, i| {
            arr[i] = try ColumnStore.init(self.allocator, col.type, col.nullable);
            inited += 1;
        }
        self.filtered = arr;
        return arr;
    }

    pub fn addPrune(self: *Scan, pred: Predicate) !void {
        const col_idx = blk: {
            for (self.table.schema.columns, 0..) |c, i| {
                if (std.mem.eql(u8, c.name, pred.col)) break :blk i;
            }
            return Error.ColumnNotFound;
        };
        // Strings have no stats in v0.2 — skip silently.
        if (self.table.schema.columns[col_idx].type.isString()) return;

        try self.prunes.append(self.allocator, .{
            .col_idx = col_idx,
            .op = pred.op,
            .val = pred.val,
        });
    }

    fn rowGroupCanMatch(self: Scan, rg: storage.RowGroupMeta) bool {
        for (self.prunes.items) |hint| {
            const stats = rg.stats[hint.col_idx];
            if (!statsOverlapPredicate(stats, hint.op, hint.val)) return false;
        }
        return true;
    }

    pub fn outputSchema(self: *Scan) []const Column {
        return self.table.schema.columns;
    }

    pub fn next(self: *Scan) !?Batch {
        self.releaseBatch();

        // Segments phase
        while (self.phase == .segments) {
            if (self.cur_segment == null) {
                if (self.cur_seg_idx >= self.segment_count) {
                    self.phase = .memtable;
                    break;
                }
                const entry = self.table.manifest.segments.items[self.cur_seg_idx];
                var name_buf: [32]u8 = undefined;
                const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
                self.cur_segment = try storage.readSegment(
                    self.allocator,
                    self.io,
                    self.table.segments_dir,
                    file_name,
                    self.table.schema,
                );

                self.cur_segment_tomb = try storage.tombstone.read(
                    self.allocator,
                    self.io,
                    self.table.segments_dir,
                    entry.segment_id,
                );

                const rgs = self.cur_segment.?.info.row_groups;
                self.cur_rg_first_row = try self.allocator.alloc(u32, rgs.len);
                var running: u32 = 0;
                for (rgs, 0..) |rg, i| {
                    self.cur_rg_first_row[i] = running;
                    running += rg.row_count;
                }
                self.cur_rg_idx = 0;
            }

            const seg = &self.cur_segment.?;
            if (self.cur_rg_idx >= seg.info.row_groups.len) {
                self.closeCurSegment();
                self.cur_seg_idx += 1;
                continue;
            }

            const rg = seg.info.row_groups[self.cur_rg_idx];
            if (!self.rowGroupCanMatch(rg)) {
                self.cur_rg_idx += 1;
                continue;
            }

            for (self.table.schema.columns, 0..) |_, i| {
                self.decoded[i] = try seg.decodeColumn(
                    self.allocator,
                    self.table.schema,
                    self.cur_rg_idx,
                    i,
                );
            }
            self.decoded_valid = true;

            const rg_first = self.cur_rg_first_row[self.cur_rg_idx];
            const rg_count = rg.row_count;
            self.cur_rg_idx += 1;

            // Apply tombstones if any fall within this row group.
            const masked = try self.applyTombsIfAny(rg_first, rg_count);
            if (masked) |out| return out;

            for (self.decoded, 0..) |c, i| self.views[i] = c.view();
            return Batch{
                .schema = self.table.schema.columns,
                .values = self.views,
                .row_count = rg_count,
            };
        }

        // Memtable phase
        if (self.phase == .memtable) {
            self.phase = .done;
            if (self.table.memtable.row_count == 0) return null;

            for (self.table.memtable.columns, 0..) |c, i| {
                self.views[i] = c.view();
            }
            return Batch{
                .schema = self.table.schema.columns,
                .values = self.views,
                .row_count = @intCast(self.table.memtable.row_count),
            };
        }

        return null;
    }

    fn releaseBatch(self: *Scan) void {
        if (self.decoded_valid) {
            for (self.decoded) |*c| c.deinit(self.allocator);
            self.decoded_valid = false;
        }
    }

    /// If any tombstone offsets fall within `[rg_first, rg_first + rg_count)`,
    /// materialize a filtered batch into `filtered` and return it. Otherwise
    /// returns null so the caller emits the unfiltered batch.
    fn applyTombsIfAny(self: *Scan, rg_first: u32, rg_count: u32) !?Batch {
        const tombs = self.cur_segment_tomb orelse return null;
        if (tombs.len == 0) return null;

        const rg_end = rg_first + rg_count;
        const lo = std.sort.lowerBound(u32, tombs, rg_first, struct {
            fn cmp(target: u32, item: u32) std.math.Order {
                return std.math.order(target, item);
            }
        }.cmp);
        const hi = std.sort.lowerBound(u32, tombs, rg_end, struct {
            fn cmp(target: u32, item: u32) std.math.Order {
                return std.math.order(target, item);
            }
        }.cmp);
        if (lo == hi) return null; // no tombs in this row group

        const tomb_slice = tombs[lo..hi];
        const filtered_cols = try self.ensureFilteredBuffers();
        for (filtered_cols) |*c| c.clear();

        // Build a keep mask for this row group: false where tombstoned.
        const mask = try self.allocator.alloc(bool, rg_count);
        defer self.allocator.free(mask);
        @memset(mask, true);
        for (tomb_slice) |off| {
            mask[off - rg_first] = false;
        }

        var kept: usize = 0;
        for (mask) |m| if (m) {
            kept += 1;
        };

        // Materialize each column through the mask.
        for (self.decoded, filtered_cols) |src, *dst| {
            try engine.memtable.appendMaskedColumn(self.allocator, src.view(), mask, dst);
        }
        for (filtered_cols, 0..) |c, i| self.views[i] = c.view();

        return Batch{
            .schema = self.table.schema.columns,
            .values = self.views,
            .row_count = kept,
        };
    }
};


// ---------------------------------------------------------------------------
// Filter
// ---------------------------------------------------------------------------

pub const PredicateOp = enum { eq, neq, lt, lte, gt, gte };

pub const Predicate = struct {
    col: []const u8,
    op: PredicateOp,
    val: Value,
};

/// Boolean expression over Predicates.
///
///   - `.leaf`       — a single column-op-value comparison
///   - `.is_null`    — column value is NULL
///   - `.is_not_null`— column value is non-NULL
///   - `.@"and"`     — all children must match
///   - `.@"or"`      — at least one child must match
///   - `.not`        — child must NOT match
pub const PredicateExpr = union(enum) {
    leaf: Predicate,
    is_null: []const u8,
    is_not_null: []const u8,
    @"and": []const PredicateExpr,
    @"or": []const PredicateExpr,
    not: *const PredicateExpr,
};

/// Build a leaf predicate expression. Shorthand for `.{ .leaf = ... }`.
pub fn leafExpr(col: []const u8, op: PredicateOp, val: Value) PredicateExpr {
    return .{ .leaf = .{ .col = col, .op = op, .val = val } };
}

pub fn isNullExpr(col: []const u8) PredicateExpr {
    return .{ .is_null = col };
}

pub fn isNotNullExpr(col: []const u8) PredicateExpr {
    return .{ .is_not_null = col };
}

pub const Filter = struct {
    allocator: Allocator,
    upstream: Query,
    expr: PredicateExpr,
    schema: []const Column,

    /// Per-column accumulator. Mirrors the memtable's storage shape so the
    /// same view() helper applies.
    filtered: []ColumnStore,
    views: []ColumnView,

    pub fn create(allocator: Allocator, upstream: Query, expr: PredicateExpr) !Query {
        const schema = upstream.outputSchema();
        try validateExpr(expr, schema);

        const views = try allocator.alloc(ColumnView, schema.len);
        errdefer allocator.free(views);

        const filtered = try allocator.alloc(ColumnStore, schema.len);
        errdefer allocator.free(filtered);
        var inited: usize = 0;
        errdefer for (filtered[0..inited]) |*c| c.deinit(allocator);
        for (schema, 0..) |col, i| {
            filtered[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const self = try allocator.create(Filter);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .expr = expr,
            .schema = schema,
            .filtered = filtered,
            .views = views,
        };

        // Push leaves through top-level ANDs down to Scan for row-group prune.
        var up = self.upstream;
        pushExprDown(&up, expr) catch |err| switch (err) {
            error.ColumnNotFound => {},
            else => return err,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Filter) void {
        var up = self.upstream;
        up.deinit();
        for (self.filtered) |*c| c.deinit(self.allocator);
        self.allocator.free(self.filtered);
        self.allocator.free(self.views);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Filter) []const Column {
        return self.schema;
    }

    pub fn addPrune(self: *Filter, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    pub fn next(self: *Filter) !?Batch {
        while (true) {
            const upstream_batch = (try self.upstream.next()) orelse return null;

            const n = upstream_batch.row_count;
            const mask = try self.allocator.alloc(bool, n);
            defer self.allocator.free(mask);
            try self.evaluateExpr(self.expr, upstream_batch, mask);

            var matched: usize = 0;
            for (mask) |m| if (m) {
                matched += 1;
            };
            if (matched == 0) continue;

            for (self.filtered) |*c| c.clear();
            for (upstream_batch.values, 0..) |view, ci| {
                try engine.memtable.appendMaskedColumn(self.allocator, view, mask, &self.filtered[ci]);
            }
            for (self.filtered, 0..) |c, ci| self.views[ci] = c.view();

            return Batch{ .schema = self.schema, .values = self.views, .row_count = matched };
        }
    }

    fn evaluateExpr(self: *Filter, expr: PredicateExpr, batch: Batch, out: []bool) anyerror!void {
        switch (expr) {
            .leaf => |p| {
                const col_idx = blk: {
                    for (self.schema, 0..) |c, i| {
                        if (std.mem.eql(u8, c.name, p.col)) break :blk i;
                    }
                    return Error.ColumnNotFound;
                };
                try evaluateMaskWithPred(batch.values[col_idx], p, batch.row_count, out);
            },
            .is_null => |col_name| {
                const col_idx = blk: {
                    for (self.schema, 0..) |c, i| {
                        if (std.mem.eql(u8, c.name, col_name)) break :blk i;
                    }
                    return Error.ColumnNotFound;
                };
                const view = batch.values[col_idx];
                for (0..batch.row_count) |i| out[i] = !view.isValid(i);
            },
            .is_not_null => |col_name| {
                const col_idx = blk: {
                    for (self.schema, 0..) |c, i| {
                        if (std.mem.eql(u8, c.name, col_name)) break :blk i;
                    }
                    return Error.ColumnNotFound;
                };
                const view = batch.values[col_idx];
                for (0..batch.row_count) |i| out[i] = view.isValid(i);
            },
            .@"and" => |children| {
                if (children.len == 0) {
                    @memset(out, true);
                    return;
                }
                try self.evaluateExpr(children[0], batch, out);
                if (children.len == 1) return;
                const scratch = try self.allocator.alloc(bool, out.len);
                defer self.allocator.free(scratch);
                for (children[1..]) |child| {
                    try self.evaluateExpr(child, batch, scratch);
                    for (out, scratch) |*o, s| o.* = o.* and s;
                }
            },
            .@"or" => |children| {
                if (children.len == 0) {
                    @memset(out, false);
                    return;
                }
                try self.evaluateExpr(children[0], batch, out);
                if (children.len == 1) return;
                const scratch = try self.allocator.alloc(bool, out.len);
                defer self.allocator.free(scratch);
                for (children[1..]) |child| {
                    try self.evaluateExpr(child, batch, scratch);
                    for (out, scratch) |*o, s| o.* = o.* or s;
                }
            },
            .not => |child| {
                try self.evaluateExpr(child.*, batch, out);
                for (out) |*o| o.* = !o.*;
            },
        }
    }
};

/// Type-check a PredicateExpr against a schema. Every leaf must reference an
/// existing column with a value-tag matching that column's type. String
/// columns only accept `.eq` and `.neq`.
fn validateExpr(expr: PredicateExpr, schema: []const Column) !void {
    switch (expr) {
        .leaf => |p| {
            const col_idx = blk: {
                for (schema, 0..) |c, i| {
                    if (std.mem.eql(u8, c.name, p.col)) break :blk i;
                }
                return Error.ColumnNotFound;
            };
            const col_type = schema[col_idx].type;
            if (ValueTag.fromType(col_type) != std.meta.activeTag(p.val)) {
                return Error.PredicateTypeMismatch;
            }
            if (col_type.isString() and p.op != .eq and p.op != .neq) {
                return Error.UnsupportedOperatorForType;
            }
        },
        .is_null, .is_not_null => |col_name| {
            for (schema) |c| {
                if (std.mem.eql(u8, c.name, col_name)) return;
            }
            return Error.ColumnNotFound;
        },
        .@"and" => |children| {
            for (children) |c| try validateExpr(c, schema);
        },
        .@"or" => |children| {
            for (children) |c| try validateExpr(c, schema);
        },
        .not => |child| try validateExpr(child.*, schema),
    }
}

/// Push every leaf reachable through top-level ANDs down to the upstream so
/// Scan can use them for row-group min/max pruning. OR/NOT branches are
/// skipped — they don't have monotonic stats overlap semantics.
fn pushExprDown(upstream: *Query, expr: PredicateExpr) !void {
    switch (expr) {
        .leaf => |p| {
            upstream.addPrune(p) catch |err| switch (err) {
                error.ColumnNotFound => {},
                else => return err,
            };
        },
        .@"and" => |children| {
            for (children) |c| try pushExprDown(upstream, c);
        },
        else => {},
    }
}

fn evaluateMaskWithPred(view: ColumnView, p: Predicate, n: usize, mask: []bool) !void {
    const op = p.op;
    switch (view.data) {
        .int => |s| {
            const want = p.val.int;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i32, v, want, op);
        },
        .bigint => |s| {
            const want = p.val.bigint;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i64, v, want, op);
        },
        .boolean => |s| {
            const want = @intFromBool(p.val.boolean);
            for (s[0..n], 0..) |v, i| mask[i] = cmp(u8, v, want, op);
        },
        .varchar => |sv| {
            if (op != .eq and op != .neq) return Error.UnsupportedOperatorForType;
            for (0..n) |i| {
                const eq = std.mem.eql(u8, sv.rowBytes(i), p.val.text);
                mask[i] = if (op == .eq) eq else !eq;
            }
        },
        .string => |sv| {
            if (op != .eq and op != .neq) return Error.UnsupportedOperatorForType;
            for (0..n) |i| {
                const eq = std.mem.eql(u8, sv.rowBytes(i), p.val.text);
                mask[i] = if (op == .eq) eq else !eq;
            }
        },
        .float => |s| {
            const want = p.val.float;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(f32, v, want, op);
        },
        .double => |s| {
            const want = p.val.double;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(f64, v, want, op);
        },
    }
    // Two-valued logic: a NULL value never matches a comparison.
    if (view.nulls != null) {
        for (0..n) |i| {
            if (!view.isValid(i)) mask[i] = false;
        }
    }
}

fn cmp(comptime T: type, a: T, b: T, op: PredicateOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .lte => a <= b,
        .gt => a > b,
        .gte => a >= b,
    };
}

// ---------------------------------------------------------------------------
// Project — pick a subset of columns by name
// ---------------------------------------------------------------------------

pub const Project = struct {
    allocator: Allocator,
    upstream: Query,
    output_schema: []Column,
    column_map: []usize, // output_idx → upstream_idx
    views: []ColumnView,

    pub fn create(allocator: Allocator, upstream: Query, names: []const []const u8) !Query {
        const up_schema = upstream.outputSchema();
        const out_schema = try allocator.alloc(Column, names.len);
        errdefer allocator.free(out_schema);
        const column_map = try allocator.alloc(usize, names.len);
        errdefer allocator.free(column_map);
        const views = try allocator.alloc(ColumnView, names.len);
        errdefer allocator.free(views);

        for (names, 0..) |name, i| {
            const idx = blk: {
                for (up_schema, 0..) |c, j| if (std.mem.eql(u8, c.name, name)) break :blk j;
                return Error.ColumnNotFound;
            };
            column_map[i] = idx;
            out_schema[i] = up_schema[idx];
        }

        const self = try allocator.create(Project);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .output_schema = out_schema,
            .column_map = column_map,
            .views = views,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Project) void {
        var up = self.upstream;
        up.deinit();
        self.allocator.free(self.output_schema);
        self.allocator.free(self.column_map);
        self.allocator.free(self.views);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Project) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *Project, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    pub fn next(self: *Project) !?Batch {
        const batch = (try self.upstream.next()) orelse return null;
        for (self.column_map, 0..) |src_idx, dst_idx| {
            self.views[dst_idx] = batch.values[src_idx];
        }
        return Batch{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = batch.row_count,
        };
    }
};

// ---------------------------------------------------------------------------
// Limit — stop after N total rows
// ---------------------------------------------------------------------------

pub const Limit = struct {
    allocator: Allocator,
    upstream: Query,
    remaining: usize,
    /// We materialize truncated batches into local buffers when the upstream
    /// batch overshoots. Simpler approach: just narrow the row_count and
    /// trust callers not to read past it. We slice the view's slices.
    views: []ColumnView,

    pub fn create(allocator: Allocator, upstream: Query, n: usize) !Query {
        const schema = upstream.outputSchema();
        const views = try allocator.alloc(ColumnView, schema.len);
        errdefer allocator.free(views);

        const self = try allocator.create(Limit);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .remaining = n,
            .views = views,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Limit) void {
        var up = self.upstream;
        up.deinit();
        self.allocator.free(self.views);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Limit) []const Column {
        return self.upstream.outputSchema();
    }

    pub fn addPrune(self: *Limit, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    pub fn next(self: *Limit) !?Batch {
        if (self.remaining == 0) return null;
        const batch = (try self.upstream.next()) orelse return null;

        if (batch.row_count <= self.remaining) {
            self.remaining -= batch.row_count;
            return batch;
        }

        // Truncate
        const take = self.remaining;
        for (batch.values, 0..) |v, i| {
            self.views[i] = truncateView(v, take);
        }
        self.remaining = 0;
        return Batch{
            .schema = batch.schema,
            .values = self.views,
            .row_count = take,
        };
    }

    fn truncateView(view: ColumnView, n: usize) ColumnView {
        const new_data: storage.column.ValueView = switch (view.data) {
            .int => |s| .{ .int = s[0..n] },
            .bigint => |s| .{ .bigint = s[0..n] },
            .boolean => |s| .{ .boolean = s[0..n] },
            .varchar => |sv| .{ .varchar = .{
                .offsets = sv.offsets[0 .. n + 1],
                .bytes = sv.bytes[0..sv.offsets[n]],
            } },
            .string => |sv| .{ .string = .{
                .offsets = sv.offsets[0 .. n + 1],
                .bytes = sv.bytes[0..sv.offsets[n]],
            } },
            .float => |s| .{ .float = s[0..n] },
            .double => |s| .{ .double = s[0..n] },
        };
        return .{ .data = new_data, .nulls = if (view.nulls) |b| b[0..storage.column.bitmapBytes(n)] else null };
    }
};

// ---------------------------------------------------------------------------
// Sort (ORDER BY)
// ---------------------------------------------------------------------------

pub const SortSpec = struct {
    col: []const u8,
    desc: bool = false,
};

pub const Sort = struct {
    allocator: Allocator,
    upstream: Query,
    schema: []const Column,

    sort_col_indices: []usize,
    sort_desc: []bool,

    /// All upstream rows, accumulated by column. Built lazily on first
    /// `next()` call (Sort is a blocking operator).
    accumulated: []ColumnStore,
    accumulated_rows: u64 = 0,

    drained: bool = false,
    perm: []u32 = &.{},
    emit_offset: usize = 0,

    output_columns: []ColumnStore,
    views: []ColumnView,

    const batch_size: usize = 1024;

    pub fn create(allocator: Allocator, upstream: Query, sort_specs: []const SortSpec) !Query {
        if (sort_specs.len == 0) return Error.SortNoKeys;
        const schema = upstream.outputSchema();

        const sort_col_indices = try allocator.alloc(usize, sort_specs.len);
        errdefer allocator.free(sort_col_indices);
        const sort_desc = try allocator.alloc(bool, sort_specs.len);
        errdefer allocator.free(sort_desc);

        for (sort_specs, 0..) |spec, i| {
            sort_col_indices[i] = blk: {
                for (schema, 0..) |c, j| if (std.mem.eql(u8, c.name, spec.col)) break :blk j;
                return Error.ColumnNotFound;
            };
            sort_desc[i] = spec.desc;
        }

        const accumulated = try allocator.alloc(ColumnStore, schema.len);
        errdefer allocator.free(accumulated);
        var inited: usize = 0;
        errdefer for (accumulated[0..inited]) |*c| c.deinit(allocator);
        for (schema, 0..) |col, i| {
            accumulated[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const output_columns = try allocator.alloc(ColumnStore, schema.len);
        errdefer allocator.free(output_columns);
        var oinited: usize = 0;
        errdefer for (output_columns[0..oinited]) |*c| c.deinit(allocator);
        for (schema, 0..) |col, i| {
            output_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            oinited += 1;
        }

        const views = try allocator.alloc(ColumnView, schema.len);
        errdefer allocator.free(views);

        const self = try allocator.create(Sort);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .schema = schema,
            .sort_col_indices = sort_col_indices,
            .sort_desc = sort_desc,
            .accumulated = accumulated,
            .output_columns = output_columns,
            .views = views,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Sort) void {
        var up = self.upstream;
        up.deinit();
        for (self.accumulated) |*c| c.deinit(self.allocator);
        self.allocator.free(self.accumulated);
        if (self.perm.len > 0) self.allocator.free(self.perm);
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.sort_col_indices);
        self.allocator.free(self.sort_desc);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Sort) []const Column {
        return self.schema;
    }

    pub fn addPrune(self: *Sort, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    pub fn next(self: *Sort) !?Batch {
        if (!self.drained) try self.drainAndSort();

        const remaining = self.accumulated_rows - self.emit_offset;
        if (remaining == 0) return null;
        const n: usize = @intCast(@min(@as(u64, batch_size), remaining));

        for (self.output_columns) |*c| c.clear();
        for (self.output_columns, 0..) |*out, ci| {
            try engine.memtable.appendByIndices(
                self.allocator,
                self.accumulated[ci].view(),
                self.perm[self.emit_offset .. self.emit_offset + n],
                out,
            );
        }

        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        self.emit_offset += n;
        return Batch{ .schema = self.schema, .values = self.views, .row_count = n };
    }

    fn drainAndSort(self: *Sort) !void {
        while (try self.upstream.next()) |batch| {
            for (batch.values, 0..) |view, ci| {
                try engine.memtable.appendAllColumn(self.allocator, view, &self.accumulated[ci]);
            }
            self.accumulated_rows += batch.row_count;
        }

        const n: usize = @intCast(self.accumulated_rows);
        if (n == 0) {
            self.drained = true;
            return;
        }
        self.perm = try self.allocator.alloc(u32, n);
        for (self.perm, 0..) |*p, i| p.* = @intCast(i);

        const Ctx = struct {
            accumulated: []const ColumnStore,
            indices: []const usize,
            desc: []const bool,

            pub fn lessThan(ctx: @This(), a: u32, b: u32) bool {
                for (ctx.indices, 0..) |ci, i| {
                    const ord = engine.memtable.compareInColumnPub(ctx.accumulated[ci], a, b);
                    if (ord == .lt) return !ctx.desc[i];
                    if (ord == .gt) return ctx.desc[i];
                }
                return false;
            }
        };

        std.sort.heap(u32, self.perm, Ctx{
            .accumulated = self.accumulated,
            .indices = self.sort_col_indices,
            .desc = self.sort_desc,
        }, Ctx.lessThan);

        self.drained = true;
    }
};

// ---------------------------------------------------------------------------
// Aggregate / GROUP BY
// ---------------------------------------------------------------------------

pub const AggFunc = enum { count, sum, min, max };

pub const AggSpec = struct {
    func: AggFunc,
    /// Column to aggregate. `null` is only valid for `COUNT(*)`.
    col: ?[]const u8 = null,
    /// Output column name.
    as: []const u8,
};

/// Per-aggregate accumulator state. Integer types accumulate into i64
/// (MIN/MAX) or i128 (SUM); float/double types accumulate into f64. The
/// final value is cast back to the declared output column type.
const AccState = union(enum) {
    count: u64,
    sum_int: i128,
    sum_float: f64,
    min_int: ?i64,
    max_int: ?i64,
    min_float: ?f64,
    max_float: ?f64,
};

pub const Aggregate = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    upstream: Query,

    /// Index in the *upstream* schema for each group-by column.
    group_col_indices: []usize,
    /// For each agg, index in upstream schema (or null for COUNT(*)).
    agg_col_indices: []?usize,
    /// Each agg's spec (borrowed from caller).
    aggs: []const AggSpec,

    output_schema: []Column,
    output_columns: []ColumnStore,
    views: []ColumnView,

    /// Used only when there are no group-by columns (single global group).
    single_state: []AccState,
    /// Used only when grouping. Maps compound-key bytes → owned state array.
    groups: std.StringHashMapUnmanaged([]AccState),

    emitted: bool = false,

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        group_cols: []const []const u8,
        aggs: []const AggSpec,
    ) !Query {
        if (aggs.len == 0) return Error.AggregateNoSpecs;
        const up_schema = upstream.outputSchema();

        // Resolve group-by column indices.
        const group_col_indices = try allocator.alloc(usize, group_cols.len);
        errdefer allocator.free(group_col_indices);
        for (group_cols, 0..) |name, i| {
            group_col_indices[i] = blk: {
                for (up_schema, 0..) |c, j| {
                    if (std.mem.eql(u8, c.name, name)) break :blk j;
                }
                return Error.ColumnNotFound;
            };
        }

        // Resolve agg column indices and build output schema.
        const agg_col_indices = try allocator.alloc(?usize, aggs.len);
        errdefer allocator.free(agg_col_indices);

        const output_schema = try allocator.alloc(Column, group_cols.len + aggs.len);
        errdefer allocator.free(output_schema);

        // Group columns come first in the output, in order.
        for (group_col_indices, 0..) |src_idx, i| {
            output_schema[i] = up_schema[src_idx];
        }

        // Then the aggregate columns.
        for (aggs, 0..) |a, i| {
            agg_col_indices[i] = if (a.col) |name| blk: {
                for (up_schema, 0..) |c, j| {
                    if (std.mem.eql(u8, c.name, name)) break :blk j;
                }
                return Error.ColumnNotFound;
            } else null;

            output_schema[group_cols.len + i] = .{
                .name = a.as,
                .type = try aggOutputType(a.func, if (agg_col_indices[i]) |idx| up_schema[idx].type else null),
            };
        }

        // Validate per-agg compatibility (e.g. SUM on a string is an error).
        for (aggs, agg_col_indices) |a, maybe_idx| {
            const t = if (maybe_idx) |idx| up_schema[idx].type else null;
            try validateAggFn(a.func, t);
        }

        const output_columns = try allocator.alloc(ColumnStore, output_schema.len);
        errdefer allocator.free(output_columns);
        var inited: usize = 0;
        errdefer for (output_columns[0..inited]) |*c| c.deinit(allocator);
        for (output_schema, 0..) |col, i| {
            output_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        errdefer allocator.free(views);

        // Single-state buffer (used iff no group-by columns).
        const single_state = try allocator.alloc(AccState, aggs.len);
        errdefer allocator.free(single_state);
        for (aggs, agg_col_indices, single_state) |a, idx, *s| {
            const in_t: ?Type = if (idx) |i| up_schema[i].type else null;
            s.* = initialState(a.func, in_t);
        }

        const self = try allocator.create(Aggregate);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .upstream = upstream,
            .group_col_indices = group_col_indices,
            .agg_col_indices = agg_col_indices,
            .aggs = aggs,
            .output_schema = output_schema,
            .output_columns = output_columns,
            .views = views,
            .single_state = single_state,
            .groups = .empty,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Aggregate) void {
        var up = self.upstream;
        up.deinit();
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.group_col_indices);
        self.allocator.free(self.agg_col_indices);
        self.allocator.free(self.single_state);
        self.arena.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Aggregate) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *Aggregate, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    pub fn next(self: *Aggregate) !?Batch {
        if (self.emitted) return null;
        self.emitted = true;

        // Drain upstream and accumulate.
        while (try self.upstream.next()) |batch| {
            try self.accumulateBatch(batch);
        }

        // Emit results into our output columns.
        if (self.group_col_indices.len == 0) {
            try self.appendSingleResult();
        } else {
            try self.appendGroupedResults();
        }

        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        return Batch{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = self.output_columns[0].rowCount(),
        };
    }

    fn accumulateBatch(self: *Aggregate, batch: Batch) !void {
        const n = batch.row_count;
        if (self.group_col_indices.len == 0) {
            for (self.aggs, 0..) |a, ai| {
                try updateState(&self.single_state[ai], a.func, batch, self.agg_col_indices[ai], 0, @intCast(n));
            }
            return;
        }

        const aa = self.arena.allocator();
        var row: u32 = 0;
        while (row < n) : (row += 1) {
            const key_bytes = try compoundGroupKey(aa, batch, self.group_col_indices, row);
            const gop = try self.groups.getOrPut(aa, key_bytes);
            if (!gop.found_existing) {
                const state = try aa.alloc(AccState, self.aggs.len);
                const up_schema = self.upstream.outputSchema();
                for (self.aggs, self.agg_col_indices, state) |a, maybe_idx, *s| {
                    const in_t: ?Type = if (maybe_idx) |i| up_schema[i].type else null;
                    s.* = initialState(a.func, in_t);
                }
                gop.value_ptr.* = state;
            }
            const state = gop.value_ptr.*;
            for (self.aggs, 0..) |a, ai| {
                try updateState(&state[ai], a.func, batch, self.agg_col_indices[ai], row, row + 1);
            }
        }
    }

    fn appendSingleResult(self: *Aggregate) !void {
        // No group columns; emit single row of aggregate results.
        for (self.aggs, 0..) |a, ai| {
            try appendAccToColumn(self.allocator, a.func, self.single_state[ai], &self.output_columns[ai], self.output_schema[ai].type);
        }
    }

    fn appendGroupedResults(self: *Aggregate) !void {
        var it = self.groups.iterator();
        while (it.next()) |entry| {
            const key_bytes = entry.key_ptr.*;
            const state = entry.value_ptr.*;

            // Append group-by column values for this group from the key bytes.
            try appendGroupKey(self.allocator, key_bytes, self.group_col_indices, self.upstream.outputSchema(), self.output_columns[0..self.group_col_indices.len]);

            for (self.aggs, 0..) |a, ai| {
                const out_idx = self.group_col_indices.len + ai;
                try appendAccToColumn(self.allocator, a.func, state[ai], &self.output_columns[out_idx], self.output_schema[out_idx].type);
            }
        }
    }
};

fn initialState(func: AggFunc, in: ?Type) AccState {
    return switch (func) {
        .count => .{ .count = 0 },
        .sum => if (in != null and in.?.isFloat())
            .{ .sum_float = 0.0 }
        else
            .{ .sum_int = 0 },
        .min => if (in != null and in.?.isFloat())
            .{ .min_float = null }
        else
            .{ .min_int = null },
        .max => if (in != null and in.?.isFloat())
            .{ .max_float = null }
        else
            .{ .max_int = null },
    };
}

fn aggOutputType(func: AggFunc, in: ?Type) !Type {
    return switch (func) {
        .count => .bigint,
        .sum => blk: {
            const t = in orelse return Error.AggregateColumnRequired;
            break :blk if (t.isFloat()) .double else .bigint;
        },
        .min, .max => in orelse return Error.AggregateNoSpecs,
    };
}

fn validateAggFn(func: AggFunc, in: ?Type) !void {
    switch (func) {
        .count => return,
        .sum => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (!(t == .int or t == .bigint or t == .boolean or t == .float or t == .double)) {
                return Error.AggregateUnsupportedType;
            }
        },
        .min, .max => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (!(t == .int or t == .bigint or t == .boolean or t == .float or t == .double)) {
                return Error.AggregateUnsupportedType;
            }
        },
    }
}

fn updateState(
    s: *AccState,
    func: AggFunc,
    batch: Batch,
    col_idx: ?usize,
    row_start: u32,
    row_end: u32,
) !void {
    switch (func) {
        .count => {
            // COUNT(*) counts every row. COUNT(col) skips NULLs.
            if (col_idx) |idx| {
                const view = batch.values[idx];
                if (view.nulls == null) {
                    s.count += @as(u64, row_end - row_start);
                } else {
                    var r: u32 = row_start;
                    while (r < row_end) : (r += 1) {
                        if (view.isValid(r)) s.count += 1;
                    }
                }
            } else {
                s.count += @as(u64, row_end - row_start);
            }
        },
        .sum => {
            const idx = col_idx.?;
            const view = batch.values[idx];
            switch (view.data) {
                .int => |s_int| for (s_int[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .bigint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .boolean => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .float => |s_f| for (s_f[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_float += v;
                },
                .double => |s_d| for (s_d[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_float += v;
                },
                else => unreachable,
            }
        },
        .min => {
            const idx = col_idx.?;
            const view = batch.values[idx];
            switch (view.data) {
                .int => |s_int| for (s_int[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .bigint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.min_int == null or v < s.min_int.?) s.min_int = v;
                },
                .boolean => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .float => |s_f| for (s_f[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const fv: f64 = v;
                    if (s.min_float == null or fv < s.min_float.?) s.min_float = fv;
                },
                .double => |s_d| for (s_d[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.min_float == null or v < s.min_float.?) s.min_float = v;
                },
                else => unreachable,
            }
        },
        .max => {
            const idx = col_idx.?;
            const view = batch.values[idx];
            switch (view.data) {
                .int => |s_int| for (s_int[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .bigint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.max_int == null or v > s.max_int.?) s.max_int = v;
                },
                .boolean => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .float => |s_f| for (s_f[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const fv: f64 = v;
                    if (s.max_float == null or fv > s.max_float.?) s.max_float = fv;
                },
                .double => |s_d| for (s_d[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.max_float == null or v > s.max_float.?) s.max_float = v;
                },
                else => unreachable,
            }
        },
    }
}

fn appendAccToColumn(
    allocator: Allocator,
    func: AggFunc,
    state: AccState,
    col: *ColumnStore,
    out_type: Type,
) !void {
    switch (func) {
        .count => {
            // BIGINT output
            try col.data.bigint.append(allocator, @intCast(state.count));
        },
        .sum => switch (state) {
            .sum_int => |total| {
                if (total > std.math.maxInt(i64) or total < std.math.minInt(i64)) {
                    return Error.ArithmeticOverflow;
                }
                try col.data.bigint.append(allocator, @intCast(total));
            },
            .sum_float => |total| try col.data.double.append(allocator, total),
            else => unreachable,
        },
        .min, .max => switch (state) {
            .min_int, .max_int => {
                const v: i64 = if (func == .min) (state.min_int orelse 0) else (state.max_int orelse 0);
                switch (out_type) {
                    .int => try col.data.int.append(allocator, @intCast(v)),
                    .bigint => try col.data.bigint.append(allocator, v),
                    .boolean => try col.data.boolean.append(allocator, @intCast(v)),
                    else => unreachable,
                }
            },
            .min_float, .max_float => {
                const v: f64 = if (func == .min) (state.min_float orelse 0.0) else (state.max_float orelse 0.0);
                switch (out_type) {
                    .float => try col.data.float.append(allocator, @floatCast(v)),
                    .double => try col.data.double.append(allocator, v),
                    else => unreachable,
                }
            },
            else => unreachable,
        },
    }
}

/// Pack the group-by columns of the current batch row into a byte buffer for
/// use as a hashmap key. Same encoding scheme as `applyUpsertResolution`:
///   - INT (i32):    4 bytes LE
///   - BIGINT (i64): 8 bytes LE
///   - BOOLEAN (u8): 1 byte
///   - VARCHAR/STRING: 4-byte LE length + bytes
fn compoundGroupKey(
    aa: Allocator,
    batch: Batch,
    group_col_indices: []const usize,
    row: u32,
) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    for (group_col_indices) |ci| {
        const view = batch.values[ci];
        switch (view.data) {
            .int => |s| try storage.format.appendI32(aa, &buf, s[row]),
            .bigint => |s| try storage.format.appendI64(aa, &buf, s[row]),
            .boolean => |s| try buf.append(aa, s[row]),
            .varchar => |sv| {
                const bytes = sv.rowBytes(row);
                try storage.format.appendU32(aa, &buf, @intCast(bytes.len));
                try buf.appendSlice(aa, bytes);
            },
            .string => |sv| {
                const bytes = sv.rowBytes(row);
                try storage.format.appendU32(aa, &buf, @intCast(bytes.len));
                try buf.appendSlice(aa, bytes);
            },
            .float => |s| {
                var b: [4]u8 = undefined;
                storage.format.writeF32(&b, s[row]);
                try buf.appendSlice(aa, &b);
            },
            .double => |s| {
                var b: [8]u8 = undefined;
                storage.format.writeF64(&b, s[row]);
                try buf.appendSlice(aa, &b);
            },
        }
    }
    return buf.toOwnedSlice(aa);
}

/// Decode a packed group key back into the output columns (one value per
/// group column). Mirrors the encoding in `compoundGroupKey`.
fn appendGroupKey(
    allocator: Allocator,
    key_bytes: []const u8,
    group_col_indices: []const usize,
    up_schema: []const Column,
    out_cols: []ColumnStore,
) !void {
    var cursor: usize = 0;
    for (group_col_indices, 0..) |src_idx, i| {
        const t = up_schema[src_idx].type;
        switch (t) {
            .int => {
                const v = storage.format.readI32(key_bytes[cursor .. cursor + 4]);
                cursor += 4;
                try out_cols[i].data.int.append(allocator, v);
            },
            .bigint => {
                const v = storage.format.readI64(key_bytes[cursor .. cursor + 8]);
                cursor += 8;
                try out_cols[i].data.bigint.append(allocator, v);
            },
            .boolean => {
                try out_cols[i].data.boolean.append(allocator, key_bytes[cursor]);
                cursor += 1;
            },
            .varchar, .string => {
                const len = storage.format.readU32(key_bytes[cursor .. cursor + 4]);
                cursor += 4;
                const bytes = key_bytes[cursor .. cursor + len];
                cursor += len;
                const ss: *engine.StringStore = switch (out_cols[i].data) {
                    .varchar => |*x| x,
                    .string => |*x| x,
                    else => unreachable,
                };
                try ss.appendValue(allocator, bytes);
            },
            .float => {
                const v = storage.format.readF32(key_bytes[cursor .. cursor + 4]);
                cursor += 4;
                try out_cols[i].data.float.append(allocator, v);
            },
            .double => {
                const v = storage.format.readF64(key_bytes[cursor .. cursor + 8]);
                cursor += 8;
                try out_cols[i].data.double.append(allocator, v);
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Top-level helper: build a scan query from a Table
// ---------------------------------------------------------------------------

pub fn scan(allocator: Allocator, table: *Table) !Query {
    return Scan.create(allocator, table);
}

/// Returns true if the row-group stats could contain rows matching `op val`.
/// Used by Scan and DELETE to decide whether to skip a row group entirely.
pub fn statsOverlapPredicate(s: storage.format.Stats, op: PredicateOp, v: Value) bool {
    const wanted: i64 = switch (v) {
        .int => |x| x,
        .bigint => |x| x,
        .boolean => |x| @intFromBool(x),
        .text, .float, .double => return true, // no stats on strings/floats yet
    };
    return switch (op) {
        .eq => wanted >= s.min and wanted <= s.max,
        .neq => !(s.min == s.max and s.min == wanted),
        .lt => s.min < wanted,
        .lte => s.min <= wanted,
        .gt => s.max > wanted,
        .gte => s.max >= wanted,
    };
}


test {
    _ = @import("exec_test.zig");
}
