//! Generic state-backed aggregate operator for trusted Zig UDAFs.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const udf_mod = @import("../udf.zig");

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const aggregate_mod = @import("aggregate.zig");
const AggSpec = aggregate_mod.AggSpec;
const AccState = aggregate_mod.AccState;
const makeQuery = exec.makeQuery;

const UdfAggPlan = struct {
    spec: AggSpec,
    entry: udf_mod.AggregateEntry,
    arg_indices: []const usize,
};

const BuiltinAggPlan = struct {
    spec: AggSpec,
    col_index: ?usize,
    input_type: ?Type,
    output_type: Type,
};

const AggPlan = union(enum) {
    udf: UdfAggPlan,
    builtin: BuiltinAggPlan,
};

const UdfStateSlot = struct {
    bytes: []align(16) u8,
    entry: udf_mod.AggregateEntry,
    initialized: bool = false,
};

const StateSlot = union(enum) {
    udf: UdfStateSlot,
    builtin: AccState,
};

pub const UdfAggregate = struct {
    allocator: Allocator,
    state_arena: std.heap.ArenaAllocator,
    upstream: Query,
    registry: *const udf_mod.UdfRegistry,

    group_col_indices: []const usize,
    plans: []AggPlan,
    output_schema: []Column,
    output_columns: []ColumnStore,
    views: []ColumnView,

    group_map: std.StringHashMapUnmanaged(u32) = .empty,
    group_keys: std.ArrayList([]u8) = .empty,
    states: std.ArrayList(StateSlot) = .empty,
    key_scratch: std.ArrayList(u8) = .empty,

    emitted: bool = false,

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        group_cols: []const []const u8,
        aggs: []const AggSpec,
        registry: *const udf_mod.UdfRegistry,
    ) !Query {
        if (aggs.len == 0) return Error.AggregateNoSpecs;
        const up_schema = upstream.outputSchema();

        const group_col_indices = try allocator.alloc(usize, group_cols.len);
        errdefer allocator.free(group_col_indices);
        for (group_cols, 0..) |name, i| {
            group_col_indices[i] = types.findColumn(up_schema, name) orelse return Error.ColumnNotFound;
        }

        const plans = try allocator.alloc(AggPlan, aggs.len);
        var plans_inited: usize = 0;
        errdefer {
            for (plans[0..plans_inited]) |p| switch (p) {
                .udf => |u| allocator.free(u.arg_indices),
                .builtin => {},
            };
            allocator.free(plans);
        }
        for (aggs, plans) |a, *plan| {
            if (a.func == .udf) {
                const name = a.udf_name orelse return Error.AggregateUnsupportedType;
                const arg_cols = if (a.udf_arg_cols.len > 0)
                    a.udf_arg_cols
                else if (a.col) |c|
                    &[_][]const u8{c}
                else
                    &.{};

                const arg_indices = try allocator.alloc(usize, arg_cols.len);
                errdefer allocator.free(arg_indices);
                var arg_types = try allocator.alloc(Type, arg_cols.len);
                defer allocator.free(arg_types);
                for (arg_cols, 0..) |col, i| {
                    const idx = types.findColumn(up_schema, col) orelse return Error.ColumnNotFound;
                    arg_indices[i] = idx;
                    arg_types[i] = up_schema[idx].type;
                }
                const entry = registry.resolveAggregateExact(name, arg_types) orelse return Error.AggregateUnsupportedType;
                plan.* = .{ .udf = .{
                    .spec = a,
                    .entry = entry,
                    .arg_indices = arg_indices,
                } };
            } else {
                const col_index: ?usize = if (a.col) |name|
                    (types.findColumn(up_schema, name) orelse return Error.ColumnNotFound)
                else
                    null;
                const input_type: ?Type = if (col_index) |idx| up_schema[idx].type else null;
                try aggregate_mod.validateAggFn(a.func, input_type, a.params);
                plan.* = .{ .builtin = .{
                    .spec = a,
                    .col_index = col_index,
                    .input_type = input_type,
                    .output_type = try aggregate_mod.aggOutputTypeFor(a, input_type),
                } };
            }
            plans_inited += 1;
        }

        const output_schema = try allocator.alloc(Column, group_cols.len + aggs.len);
        errdefer allocator.free(output_schema);
        for (group_col_indices, 0..) |src_idx, i| output_schema[i] = up_schema[src_idx];
        for (plans, 0..) |p, i| {
            output_schema[group_cols.len + i] = switch (p) {
                .udf => |u| .{
                    .name = u.spec.as,
                    .type = u.entry.return_type,
                    .nullable = true,
                },
                .builtin => |b| .{
                    .name = b.spec.as,
                    .type = b.output_type,
                },
            };
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

        const self = try allocator.create(UdfAggregate);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .state_arena = std.heap.ArenaAllocator.init(allocator),
            .upstream = upstream,
            .registry = registry,
            .group_col_indices = group_col_indices,
            .plans = plans,
            .output_schema = output_schema,
            .output_columns = output_columns,
            .views = views,
        };
        errdefer self.deinit();

        if (group_cols.len == 0) _ = try self.addGroup(&.{}, null, 0);
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *UdfAggregate) void {
        for (self.states.items) |*slot| {
            switch (slot.*) {
                .udf => |*u| {
                    if (u.initialized) {
                        const ctx = udf_mod.AggregateContext{ .allocator = self.allocator, .user_data = u.entry.user_data };
                        if (u.entry.destroy) |destroy| destroy(&ctx, @ptrCast(u.bytes.ptr));
                    }
                    self.allocator.free(u.bytes);
                },
                .builtin => {},
            }
        }
        self.states.deinit(self.allocator);
        self.state_arena.deinit();
        for (self.group_keys.items) |k| self.allocator.free(k);
        self.group_keys.deinit(self.allocator);
        self.group_map.deinit(self.allocator);
        self.key_scratch.deinit(self.allocator);
        for (self.plans) |p| switch (p) {
            .udf => |u| self.allocator.free(u.arg_indices),
            .builtin => {},
        };
        self.allocator.free(self.plans);
        self.allocator.free(self.group_col_indices);
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.views);
        self.upstream.deinit();
        self.allocator.destroy(self);
    }

    pub fn outputSchema(self: *UdfAggregate) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *UdfAggregate, pred: exec.Predicate) !void {
        try self.upstream.addPrune(pred);
    }

    pub fn stats(self: *UdfAggregate) exec.PipelineStats {
        const upstream_stats = self.upstream.stats();
        return .{
            .upper_rows = if (self.group_col_indices.len == 0) 1 else upstream_stats.upper_rows,
        };
    }

    pub fn accountant(self: *UdfAggregate) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *UdfAggregate, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "UdfAggregate");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    pub fn next(self: *UdfAggregate) !?Batch {
        if (self.emitted) return null;
        self.emitted = true;

        while (try self.upstream.next()) |batch| {
            try self.accumulateBatch(batch);
        }
        try self.finalizeGroups();

        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        const row_count = if (self.output_columns.len == 0) 0 else self.output_columns[0].rowCount();
        return .{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = row_count,
        };
    }

    fn addGroup(self: *UdfAggregate, key: []const u8, batch: ?Batch, row: usize) !u32 {
        const gid: u32 = @intCast(self.group_keys.items.len);
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        try self.group_keys.append(self.allocator, owned_key);
        if (batch) |b| {
            for (self.group_col_indices, 0..) |idx, out_i| {
                try appendCellFromView(self.allocator, &self.output_columns[out_i], b.values[idx], row);
            }
        }

        for (self.plans) |p| {
            switch (p) {
                .udf => |u| {
                    const bytes = try self.allocator.alignedAlloc(u8, .@"16", u.entry.state_size);
                    errdefer self.allocator.free(bytes);
                    var slot = UdfStateSlot{ .bytes = bytes, .entry = u.entry };
                    const ctx = udf_mod.AggregateContext{ .allocator = self.allocator, .user_data = u.entry.user_data };
                    try u.entry.init(&ctx, @ptrCast(bytes.ptr));
                    slot.initialized = true;
                    var appended = false;
                    errdefer if (!appended and slot.initialized) {
                        if (slot.entry.destroy) |destroy| destroy(&ctx, @ptrCast(slot.bytes.ptr));
                    };
                    try self.states.append(self.allocator, .{ .udf = slot });
                    appended = true;
                },
                .builtin => |b| {
                    try self.states.append(self.allocator, .{ .builtin = aggregate_mod.initialState(b.spec.func, b.input_type) });
                },
            }
        }
        return gid;
    }

    fn accumulateBatch(self: *UdfAggregate, batch: Batch) !void {
        if (self.group_col_indices.len == 0) {
            try self.updateGroupRows(0, batch, 0, batch.row_count);
            return;
        }

        var row: usize = 0;
        while (row < batch.row_count) : (row += 1) {
            self.key_scratch.clearRetainingCapacity();
            try serializeGroupKey(self.allocator, &self.key_scratch, batch, self.group_col_indices, row);
            const gop = try self.group_map.getOrPut(self.allocator, self.key_scratch.items);
            const gid = if (gop.found_existing) gop.value_ptr.* else blk: {
                const new_gid = try self.addGroup(self.key_scratch.items, batch, row);
                gop.key_ptr.* = self.group_keys.items[new_gid];
                gop.value_ptr.* = new_gid;
                break :blk new_gid;
            };
            try self.updateGroupRow(gid, batch, row);
        }
    }

    fn updateGroupRows(self: *UdfAggregate, gid: u32, batch: Batch, start: usize, end: usize) !void {
        for (self.plans, 0..) |p, ai| {
            const slot = &self.states.items[@as(usize, gid) * self.plans.len + ai];
            switch (p) {
                .udf => |u| {
                    var arg_buf: [16]ColumnView = undefined;
                    if (u.arg_indices.len > arg_buf.len) return Error.AggregateUnsupportedType;
                    const args = arg_buf[0..u.arg_indices.len];
                    for (u.arg_indices, args) |idx, *v| v.* = batch.values[idx];
                    const udf_slot = switch (slot.*) {
                        .udf => |*s| s,
                        .builtin => return Error.AggregateUnsupportedType,
                    };
                    const ctx = udf_mod.AggregateContext{ .allocator = self.allocator, .user_data = u.entry.user_data };
                    if (start == 0 and end == batch.row_count) {
                        if (u.entry.update_batch) |update_batch| {
                            try update_batch(&ctx, @ptrCast(udf_slot.bytes.ptr), args, batch.row_count);
                            continue;
                        }
                    }
                    var row = start;
                    while (row < end) : (row += 1) {
                        try u.entry.update_one(&ctx, @ptrCast(udf_slot.bytes.ptr), args, row);
                    }
                },
                .builtin => |b| {
                    const builtin_slot = switch (slot.*) {
                        .builtin => |*s| s,
                        .udf => return Error.AggregateUnsupportedType,
                    };
                    try aggregate_mod.updateState(
                        self.state_arena.allocator(),
                        builtin_slot,
                        b.spec,
                        batch,
                        b.col_index,
                        @intCast(start),
                        @intCast(end),
                    );
                },
            }
        }
    }

    fn updateGroupRow(self: *UdfAggregate, gid: u32, batch: Batch, row: usize) !void {
        try self.updateGroupRows(gid, batch, row, row + 1);
    }

    fn finalizeGroups(self: *UdfAggregate) !void {
        const n_groups = self.group_keys.items.len;
        var gid: usize = 0;
        while (gid < n_groups) : (gid += 1) {
            for (self.plans, 0..) |p, ai| {
                const out = &self.output_columns[self.group_col_indices.len + ai];
                const before = out.rowCount();
                const slot = &self.states.items[gid * self.plans.len + ai];
                switch (p) {
                    .udf => |u| {
                        const udf_slot = switch (slot.*) {
                            .udf => |*s| s,
                            .builtin => return Error.AggregateUnsupportedType,
                        };
                        const ctx = udf_mod.AggregateContext{ .allocator = self.allocator, .user_data = u.entry.user_data };
                        try u.entry.finalize(&ctx, @ptrCast(udf_slot.bytes.ptr), out);
                        try ensureFinalizeValidity(self.allocator, out, before);
                    },
                    .builtin => |b| {
                        const builtin_slot = switch (slot.*) {
                            .builtin => |s| s,
                            .udf => return Error.AggregateUnsupportedType,
                        };
                        try aggregate_mod.appendAccToColumn(self.allocator, b.spec, builtin_slot, out, b.output_type, null);
                    },
                }
                if (out.rowCount() != before + 1) return Error.AggregateUnsupportedType;
            }
        }
    }
};

fn ensureFinalizeValidity(allocator: Allocator, out: *ColumnStore, row: usize) !void {
    const nulls = out.nulls orelse return;
    if (nulls.items.len * 8 <= row) try out.appendValidBit(allocator, row, true);
}

fn serializeGroupKey(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    batch: Batch,
    group_col_indices: []const usize,
    row: usize,
) !void {
    for (group_col_indices) |idx| {
        const view = batch.values[idx];
        const valid = view.isValid(row);
        try out.append(allocator, @intFromBool(valid));
        if (!valid) continue;
        switch (view.data) {
            .int => |s| try storage.format.appendI32(allocator, out, s[row]),
            .bigint => |s| try storage.format.appendI64(allocator, out, s[row]),
            .boolean => |s| try out.append(allocator, s[row]),
            .varchar => |sv| try appendKeyString(allocator, out, sv.rowBytes(row)),
            .string => |sv| try appendKeyString(allocator, out, sv.rowBytes(row)),
            .float => |s| {
                var b: [4]u8 = undefined;
                storage.format.writeF32(&b, s[row]);
                try out.appendSlice(allocator, &b);
            },
            .double => |s| {
                var b: [8]u8 = undefined;
                storage.format.writeF64(&b, s[row]);
                try out.appendSlice(allocator, &b);
            },
            .date => |s| try storage.format.appendI32(allocator, out, s[row]),
            .datetime => |s| try storage.format.appendI64(allocator, out, s[row]),
            .tinyint => |s| try out.append(allocator, @bitCast(s[row])),
            .smallint => |s| {
                var b: [2]u8 = undefined;
                std.mem.writeInt(i16, &b, s[row], .little);
                try out.appendSlice(allocator, &b);
            },
            .largeint => |s| {
                var b: [16]u8 = undefined;
                std.mem.writeInt(i128, &b, s[row], .little);
                try out.appendSlice(allocator, &b);
            },
            .char => |sv| try appendKeyString(allocator, out, sv.rowBytes(row)),
            .decimal64 => |s| try storage.format.appendI64(allocator, out, s[row]),
            .decimal128 => |s| {
                var b: [16]u8 = undefined;
                std.mem.writeInt(i128, &b, s[row], .little);
                try out.appendSlice(allocator, &b);
            },
            .uuid => |s| {
                var b: [16]u8 = undefined;
                std.mem.writeInt(u128, &b, s[row], .little);
                try out.appendSlice(allocator, &b);
            },
        }
    }
}

fn appendKeyString(allocator: Allocator, out: *std.ArrayList(u8), bytes: []const u8) !void {
    try storage.format.appendU32(allocator, out, @intCast(bytes.len));
    try out.appendSlice(allocator, bytes);
}

fn appendCellFromView(allocator: Allocator, dst: *ColumnStore, src: ColumnView, src_idx: usize) !void {
    if (!src.isValid(src_idx)) {
        try dst.data.appendNullPlaceholder(allocator);
        try dst.appendValidBit(allocator, dst.rowCount() - 1, false);
        return;
    }
    switch (dst.data) {
        .int => |*l| try l.append(allocator, src.data.int[src_idx]),
        .bigint => |*l| try l.append(allocator, src.data.bigint[src_idx]),
        .smallint => |*l| try l.append(allocator, src.data.smallint[src_idx]),
        .tinyint => |*l| try l.append(allocator, src.data.tinyint[src_idx]),
        .largeint => |*l| try l.append(allocator, src.data.largeint[src_idx]),
        .float => |*l| try l.append(allocator, src.data.float[src_idx]),
        .double => |*l| try l.append(allocator, src.data.double[src_idx]),
        .boolean => |*l| try l.append(allocator, src.data.boolean[src_idx]),
        .date => |*l| try l.append(allocator, src.data.date[src_idx]),
        .datetime => |*l| try l.append(allocator, src.data.datetime[src_idx]),
        .decimal64 => |*l| try l.append(allocator, src.data.decimal64[src_idx]),
        .decimal128 => |*l| try l.append(allocator, src.data.decimal128[src_idx]),
        .uuid => |*l| try l.append(allocator, src.data.uuid[src_idx]),
        .varchar => |*s| try s.appendValue(allocator, stringBytes(src, src_idx)),
        .string => |*s| try s.appendValue(allocator, stringBytes(src, src_idx)),
        .char => |*s| try s.appendValue(allocator, stringBytes(src, src_idx)),
    }
    try dst.appendValidBit(allocator, dst.rowCount() - 1, true);
}

fn stringBytes(src: ColumnView, row: usize) []const u8 {
    return switch (src.data) {
        .varchar => |sv| sv.rowBytes(row),
        .string => |sv| sv.rowBytes(row),
        .char => |sv| sv.rowBytes(row),
        else => unreachable,
    };
}
