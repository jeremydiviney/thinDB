//! Aggregate / GROUP BY operator. Drains the upstream into a per-aggregate
//! accumulator (single global slot, or hash-keyed per group), then emits
//! one batch with the final results.
//!
//! Supported aggregates: COUNT, SUM, MIN, MAX, AVG. SUM/MIN/MAX/AVG dispatch
//! over input column type to choose the right accumulator state.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

pub const AggFunc = enum { count, sum, min, max, avg };

pub const AggSpec = struct {
    func: AggFunc,
    /// Column to aggregate. `null` is only valid for `COUNT(*)`.
    col: ?[]const u8 = null,
    /// Output column name.
    as: []const u8,
};

/// Per-aggregate accumulator state. Integer types accumulate into i64
/// (MIN/MAX) or i128 (SUM); float/double types accumulate into f64; LARGEINT
/// gets dedicated i128 min/max variants. The final value is cast back to the
/// declared output column type.
const AccState = union(enum) {
    count: u64,
    sum_int: i128,
    sum_float: f64,
    min_int: ?i64,
    max_int: ?i64,
    min_float: ?f64,
    max_float: ?f64,
    /// Separate i128 min/max variants for LARGEINT inputs (don't fit in i64).
    min_large: ?i128,
    max_large: ?i128,
    avg: AvgAcc,
};

const AvgAcc = struct {
    sum: f64,
    count: u64,
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

        for (group_col_indices, 0..) |src_idx, i| {
            output_schema[i] = up_schema[src_idx];
        }

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

        while (try self.upstream.next()) |batch| {
            try self.accumulateBatch(batch);
        }

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
        for (self.aggs, 0..) |a, ai| {
            try appendAccToColumn(self.allocator, a.func, self.single_state[ai], &self.output_columns[ai], self.output_schema[ai].type);
        }
    }

    fn appendGroupedResults(self: *Aggregate) !void {
        var it = self.groups.iterator();
        while (it.next()) |entry| {
            const key_bytes = entry.key_ptr.*;
            const state = entry.value_ptr.*;

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
        else if (in != null and in.? == .largeint)
            .{ .min_large = null }
        else
            .{ .min_int = null },
        .max => if (in != null and in.?.isFloat())
            .{ .max_float = null }
        else if (in != null and in.? == .largeint)
            .{ .max_large = null }
        else
            .{ .max_int = null },
        .avg => .{ .avg = .{ .sum = 0.0, .count = 0 } },
    };
}

fn aggOutputType(func: AggFunc, in: ?Type) !Type {
    return switch (func) {
        .count => .bigint,
        .sum => blk: {
            const t = in orelse return Error.AggregateColumnRequired;
            break :blk if (t.isFloat()) .double else if (t == .largeint) .largeint else .bigint;
        },
        .min, .max => in orelse return Error.AggregateNoSpecs,
        .avg => .double,
    };
}

fn validateAggFn(func: AggFunc, in: ?Type) !void {
    switch (func) {
        .count => return,
        .sum, .avg => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (!(t.isInteger() or t == .boolean or t == .float or t == .double)) {
                return Error.AggregateUnsupportedType;
            }
        },
        .min, .max => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (!(t.isInteger() or t == .boolean or t == .float or t == .double or t == .date or t == .datetime)) {
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
                .tinyint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .smallint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int += v;
                },
                .largeint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
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
                .int, .date => |s_int| for (s_int[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .bigint, .datetime => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.min_int == null or v < s.min_int.?) s.min_int = v;
                },
                .boolean => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .tinyint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .smallint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .largeint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.min_large == null or v < s.min_large.?) s.min_large = v;
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
                .int, .date => |s_int| for (s_int[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .bigint, .datetime => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.max_int == null or v > s.max_int.?) s.max_int = v;
                },
                .boolean => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .tinyint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .smallint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .largeint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.max_large == null or v > s.max_large.?) s.max_large = v;
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
        .avg => {
            const idx = col_idx.?;
            const view = batch.values[idx];
            switch (view.data) {
                .int, .bigint, .boolean, .tinyint, .smallint => {
                    avgUpdateInt(s, view, row_start, row_end);
                },
                .largeint => |slice| for (slice[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.avg.sum += @as(f64, @floatFromInt(v));
                    s.avg.count += 1;
                },
                .float => |slice| for (slice[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.avg.sum += v;
                    s.avg.count += 1;
                },
                .double => |slice| for (slice[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.avg.sum += v;
                    s.avg.count += 1;
                },
                else => unreachable,
            }
        },
    }
}

fn avgUpdateInt(s: *AccState, view: ColumnView, row_start: u32, row_end: u32) void {
    switch (view.data) {
        inline .int, .bigint, .boolean, .tinyint, .smallint => |slice| {
            for (slice[row_start..row_end], row_start..) |v, r| {
                if (!view.isValid(r)) continue;
                s.avg.sum += @as(f64, @floatFromInt(v));
                s.avg.count += 1;
            }
        },
        else => unreachable,
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
            try col.data.bigint.append(allocator, @intCast(state.count));
        },
        .sum => switch (state) {
            .sum_int => |total| switch (out_type) {
                .largeint => try col.data.largeint.append(allocator, total),
                else => {
                    if (total > std.math.maxInt(i64) or total < std.math.minInt(i64)) {
                        return Error.ArithmeticOverflow;
                    }
                    try col.data.bigint.append(allocator, @intCast(total));
                },
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
                    .date => try col.data.date.append(allocator, @intCast(v)),
                    .datetime => try col.data.datetime.append(allocator, v),
                    .tinyint => try col.data.tinyint.append(allocator, @intCast(v)),
                    .smallint => try col.data.smallint.append(allocator, @intCast(v)),
                    else => unreachable,
                }
            },
            .min_large, .max_large => {
                const v: i128 = if (func == .min) (state.min_large orelse 0) else (state.max_large orelse 0);
                switch (out_type) {
                    .largeint => try col.data.largeint.append(allocator, v),
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
        .avg => {
            const a = state.avg;
            // AVG over an empty set → 0.0 (we don't surface aggregate-result
            // NULLs yet). Guard against div-by-zero.
            const v: f64 = if (a.count == 0) 0.0 else a.sum / @as(f64, @floatFromInt(a.count));
            try col.data.double.append(allocator, v);
        },
    }
}

/// Pack the group-by columns of the current batch row into a byte buffer
/// for hashing. Layout per type matches `comparison.appendColumnValueBytes`.
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
            .date => |s| try storage.format.appendI32(aa, &buf, s[row]),
            .datetime => |s| try storage.format.appendI64(aa, &buf, s[row]),
            .tinyint => |s| try buf.append(aa, @bitCast(s[row])),
            .smallint => |s| {
                var b: [2]u8 = undefined;
                std.mem.writeInt(i16, &b, s[row], .little);
                try buf.appendSlice(aa, &b);
            },
            .largeint => |s| {
                var b: [16]u8 = undefined;
                std.mem.writeInt(i128, &b, s[row], .little);
                try buf.appendSlice(aa, &b);
            },
            .char => |sv| {
                const bytes = sv.rowBytes(row);
                try storage.format.appendU32(aa, &buf, @intCast(bytes.len));
                try buf.appendSlice(aa, bytes);
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
            .varchar, .string, .char => {
                const len = storage.format.readU32(key_bytes[cursor .. cursor + 4]);
                cursor += 4;
                const bytes = key_bytes[cursor .. cursor + len];
                cursor += len;
                const ss: *engine.StringStore = switch (out_cols[i].data) {
                    .varchar => |*x| x,
                    .string => |*x| x,
                    .char => |*x| x,
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
            .date => {
                const v = storage.format.readI32(key_bytes[cursor .. cursor + 4]);
                cursor += 4;
                try out_cols[i].data.date.append(allocator, v);
            },
            .datetime => {
                const v = storage.format.readI64(key_bytes[cursor .. cursor + 8]);
                cursor += 8;
                try out_cols[i].data.datetime.append(allocator, v);
            },
            .tinyint => {
                const v: i8 = @bitCast(key_bytes[cursor]);
                cursor += 1;
                try out_cols[i].data.tinyint.append(allocator, v);
            },
            .smallint => {
                const v = std.mem.readInt(i16, key_bytes[cursor..][0..2], .little);
                cursor += 2;
                try out_cols[i].data.smallint.append(allocator, v);
            },
            .largeint => {
                const v = std.mem.readInt(i128, key_bytes[cursor..][0..16], .little);
                cursor += 16;
                try out_cols[i].data.largeint.append(allocator, v);
            },
        }
    }
}
