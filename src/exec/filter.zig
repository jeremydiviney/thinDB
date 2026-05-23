//! Filter operator. Walks the upstream batch-by-batch, evaluates a
//! `PredicateExpr` per row, and emits batches containing only matching
//! rows. Pushes leaves of top-level ANDs down to the upstream so Scan can
//! prune row groups via min/max stats.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

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
const PredicateExpr = predicate.PredicateExpr;

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

        // Validate in place against the operator-owned predicate so any
        // integer-literal widening mutations land in self.expr (and
        // therefore in the eval path).
        try predicate.validateExpr(&self.expr, schema);

        // Push leaves through top-level ANDs down to Scan for row-group prune.
        var up = self.upstream;
        predicate.pushExprDown(&up, self.expr) catch |err| switch (err) {
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

    /// Filter only restricts rows — upper bound is unchanged
    /// (selectivity unknown without scanning). Sort state preserved
    /// (Filter doesn't reorder).
    pub fn stats(self: *Filter) exec.PipelineStats {
        return self.upstream.stats();
    }

    pub fn accountant(self: *Filter) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *Filter, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "Filter");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    pub fn next(self: *Filter) !?Batch {
        while (true) {
            const upstream_batch = (try self.upstream.next()) orelse return null;

            const n = upstream_batch.row_count;
            const mask = try self.allocator.alloc(bool, n);
            defer self.allocator.free(mask);
            try self.evaluateExpr(self.expr, upstream_batch, mask, null);

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

    /// Evaluate `expr` into `out`. `active`, when non-null, marks rows still
    /// worth testing — an earlier conjunct already eliminated the rest, so
    /// expensive leaves (LIKE) skip them. Inactive rows in `out` are
    /// don't-care: the caller's conjunction AND masks them off.
    fn evaluateExpr(self: *Filter, expr: PredicateExpr, batch: Batch, out: []bool, active: ?[]const bool) anyerror!void {
        switch (expr) {
            .leaf => |p| {
                const col_idx = types.findColumn(self.schema, p.col) orelse return Error.ColumnNotFound;
                try predicate.evaluateMaskWithPred(batch.values[col_idx], p, batch.row_count, out);
                const view = batch.values[col_idx];
                if (view.nulls != null) {
                    for (0..batch.row_count) |i| if (!view.isValid(i)) {
                        out[i] = false;
                    };
                }
            },
            .leaf_col_col => |lc| {
                const li = types.findColumn(self.schema, lc.left) orelse return Error.ColumnNotFound;
                const ri = types.findColumn(self.schema, lc.right) orelse return Error.ColumnNotFound;
                try predicate.evaluateColColMask(batch.values[li], batch.values[ri], lc.op, batch.row_count, out);
            },
            .is_null => |col_name| {
                const col_idx = types.findColumn(self.schema, col_name) orelse return Error.ColumnNotFound;
                const view = batch.values[col_idx];
                for (0..batch.row_count) |i| out[i] = !view.isValid(i);
            },
            .is_not_null => |col_name| {
                const col_idx = types.findColumn(self.schema, col_name) orelse return Error.ColumnNotFound;
                const view = batch.values[col_idx];
                for (0..batch.row_count) |i| out[i] = view.isValid(i);
            },
            .like => |lp| {
                const col_idx = types.findColumn(self.schema, lp.col) orelse return Error.ColumnNotFound;
                try predicate.evaluateLikeMask(batch.values[col_idx], lp.pattern, batch.row_count, out, active);
            },
            .@"and" => |children| {
                if (children.len == 0) {
                    @memset(out, true);
                    return;
                }
                // Conjunction is mask-guided: each conjunct after the first is
                // evaluated only on rows still alive in the running mask, so an
                // expensive predicate (LIKE) runs on the few survivors of the
                // selective ones rather than every row.
                try self.evaluateExpr(children[0], batch, out, active);
                if (children.len == 1) return;
                const scratch = try self.allocator.alloc(bool, out.len);
                defer self.allocator.free(scratch);
                for (children[1..]) |child| {
                    try self.evaluateExpr(child, batch, scratch, out);
                    for (out, scratch) |*o, s| o.* = o.* and s;
                }
            },
            .@"or" => |children| {
                if (children.len == 0) {
                    @memset(out, false);
                    return;
                }
                // Disjunction widens, so it can't narrow the active set; it just
                // forwards the incoming `active` (rows eliminated upstream stay
                // eliminated) to every child.
                try self.evaluateExpr(children[0], batch, out, active);
                if (children.len == 1) return;
                const scratch = try self.allocator.alloc(bool, out.len);
                defer self.allocator.free(scratch);
                for (children[1..]) |child| {
                    try self.evaluateExpr(child, batch, scratch, active);
                    for (out, scratch) |*o, s| o.* = o.* or s;
                }
            },
            .not => |child| {
                try self.evaluateExpr(child.*, batch, out, active);
                for (out) |*o| o.* = !o.*;
            },
            // Resolved away by the pre-compile pass.
            .scalar_subquery, .exists_subquery, .in_subquery => return Error.PredicateTypeMismatch,
            .always => |b| @memset(out, b),
            .in_set => |s| {
                const col_idx = types.findColumn(self.schema, s.col) orelse return Error.ColumnNotFound;
                try predicate.evaluateInSetMask(batch.values[col_idx], s.values, s.negate, batch.row_count, out);
            },
            .correlated_set => |s| try predicate.evaluateCorrelatedSetMask(s, self.schema, batch, out),
            .correlated_scalar => |s| try predicate.evaluateCorrelatedScalarMask(s, self.schema, batch, out),
            .correlated_range => |s| try predicate.evaluateCorrelatedRangeMask(s, self.schema, batch, out),
            .leaf_var => return Error.PredicateTypeMismatch,
        }
    }
};
