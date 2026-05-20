//! Project (column subset / reorder) and Limit (row cap) operators. Both
//! are zero-copy pass-throughs: Project rearranges ColumnView pointers,
//! Limit slices the views to truncate. They share a file because each is
//! small and they have nearly identical lifecycles.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

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
            const idx = types.findColumn(up_schema, name) orelse return Error.ColumnNotFound;
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

    /// Project keeps row count unchanged. Sort state survives as long
    /// as every key in the upstream's sort_state.keys is still in the
    /// output schema; otherwise the sort claim is truncated to the
    /// leading prefix that survived (a sort by `(a,b,c)` with `b`
    /// projected away yields a stream sorted only by `a`).
    pub fn stats(self: *Project) exec.PipelineStats {
        const up = self.upstream.stats();
        var kept_prefix_len: usize = 0;
        outer: for (up.sort_state.keys) |key| {
            for (self.output_schema) |c| {
                if (std.mem.eql(u8, c.name, key)) {
                    kept_prefix_len += 1;
                    continue :outer;
                }
            }
            break; // first missing key truncates the sort claim
        }
        return .{
            .upper_rows = up.upper_rows,
            .sort_state = .{
                .keys = up.sort_state.keys[0..kept_prefix_len],
                .global = up.sort_state.global,
            },
        };
    }

    pub fn accountant(self: *Project) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
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

pub const Limit = struct {
    allocator: Allocator,
    upstream: Query,
    remaining: usize,
    /// We materialize truncated batches into local buffers when the upstream
    /// batch overshoots. We slice the view's slices.
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

    /// Limit clamps row count to `min(n, upstream.upper)`. Sort state
    /// preserved (Limit just truncates, doesn't reorder).
    pub fn stats(self: *Limit) exec.PipelineStats {
        const up = self.upstream.stats();
        return .{
            .upper_rows = @min(@as(u64, self.remaining), up.upper_rows),
            .sort_state = up.sort_state,
        };
    }

    pub fn accountant(self: *Limit) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn next(self: *Limit) !?Batch {
        if (self.remaining == 0) return null;
        const batch = (try self.upstream.next()) orelse return null;

        if (batch.row_count <= self.remaining) {
            self.remaining -= batch.row_count;
            return batch;
        }

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
            .date => |s| .{ .date = s[0..n] },
            .datetime => |s| .{ .datetime = s[0..n] },
            .tinyint => |s| .{ .tinyint = s[0..n] },
            .smallint => |s| .{ .smallint = s[0..n] },
            .largeint => |s| .{ .largeint = s[0..n] },
            .char => |sv| .{ .char = .{
                .offsets = sv.offsets[0 .. n + 1],
                .bytes = sv.bytes[0..sv.offsets[n]],
            } },
            .decimal64 => |s| .{ .decimal64 = s[0..n] },
            .decimal128 => |s| .{ .decimal128 = s[0..n] },
            .uuid => |s| .{ .uuid = s[0..n] },
        };
        return .{ .data = new_data, .nulls = if (view.nulls) |b| b[0..storage.column.bitmapBytes(n)] else null };
    }
};
