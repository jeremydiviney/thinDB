//! Compute operator — adds derived columns to a batch using scalar
//! functions.
//!
//! v1 scope (incremental):
//!   - Each derived column's `Expr` is either:
//!       * a `.col_ref` (rename) — copies an upstream column under a
//!         new name without invoking any function.
//!       * a `.call` whose args are themselves `.col_ref`s (no nested
//!         calls in v1; nested fall back to multiple Compute layers).
//!   - Output schema = upstream schema + derived columns appended in
//!     the order given. Downstream operators see both.
//!   - Null handling: per `NullStrategy` of the resolved function.
//!     `.propagates` → if ANY arg is null at row i, output is null.
//!     `.absorbs` → kernel handles nulls itself.
//!
//! Future (post-v1): nested calls (`upper(lower(x))`), literal args,
//! constant folding, expression compile to a flat op list with shared
//! intermediates.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;

const expr_mod = @import("expr.zig");
const Expr = expr_mod.Expr;
const scalar_fn = @import("scalar_fn.zig");
const ScalarFn = scalar_fn.ScalarFn;
const NullStrategy = scalar_fn.NullStrategy;

const exec = @import("exec.zig");
const Batch = exec.Batch;
const Query = exec.Query;
const Predicate = exec.Predicate;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

/// One derived column on a Compute operator.
pub const Derived = struct {
    name: []const u8,
    expr: Expr,
};

/// Resolved per-derived plan: either a function call with arg column
/// indices, or a simple column rename.
const ResolvedDerived = struct {
    name: []const u8,
    output_type: Type,
    kind: union(enum) {
        rename: struct { src_idx: usize },
        call: struct { func: ScalarFn, arg_indices: []const usize },
    },
};

pub const Compute = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    upstream: Query,

    derived: []ResolvedDerived,
    /// One ColumnStore per derived column. Cleared + refilled each
    /// `next()` from the upstream batch.
    derived_cols: []ColumnStore,

    /// Combined output schema: upstream schema followed by derived
    /// columns. Allocated once at create.
    output_schema: []Column,
    /// Reusable views slice (upstream views + derived views), sized at
    /// create. Rewired per batch.
    views: []ColumnView,

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        derived: []const Derived,
    ) !Query {
        if (derived.len == 0) return Error.ComputeNoColumns;
        const up_schema = upstream.outputSchema();

        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const aa = arena.allocator();

        const resolved = try aa.alloc(ResolvedDerived, derived.len);
        for (derived, resolved) |d, *r| r.* = try resolveDerived(aa, d, up_schema);

        // Validate no duplicate derived names AND no collision with
        // upstream column names (downstream wouldn't be able to
        // disambiguate).
        for (resolved, 0..) |r, i| {
            for (up_schema) |uc| {
                if (std.mem.eql(u8, uc.name, r.name)) return Error.ComputeNameCollision;
            }
            for (resolved[0..i]) |prior| {
                if (std.mem.eql(u8, prior.name, r.name)) return Error.ComputeNameCollision;
            }
        }

        // Output schema = upstream + derived
        const output_schema = try allocator.alloc(Column, up_schema.len + resolved.len);
        errdefer allocator.free(output_schema);
        for (up_schema, 0..) |c, i| output_schema[i] = c;
        for (resolved, up_schema.len..) |r, i| {
            // Nullable: if propagates AND any input column is nullable
            // → derived is nullable. If absorbs → also nullable (the
            // function can still produce null if all inputs are null).
            // Renames inherit nullability from the source.
            const nullable = derivedNullable(r, up_schema);
            output_schema[i] = .{ .name = r.name, .type = r.output_type, .nullable = nullable };
        }

        // One ColumnStore per derived column. Re-initialized each batch
        // is overkill; instead clearRetainingCapacity at the top of
        // next(). We still rebuild the validity bitmap fresh per batch.
        const derived_cols = try allocator.alloc(ColumnStore, resolved.len);
        errdefer allocator.free(derived_cols);
        var inited: usize = 0;
        errdefer for (derived_cols[0..inited]) |*c| c.deinit(allocator);
        for (resolved, derived_cols, 0..) |r, *col, idx| {
            const out_col = output_schema[up_schema.len + idx];
            col.* = try ColumnStore.init(allocator, r.output_type, out_col.nullable);
            inited += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        errdefer allocator.free(views);

        const self = try allocator.create(Compute);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .arena = arena,
            .upstream = upstream,
            .derived = resolved,
            .derived_cols = derived_cols,
            .output_schema = output_schema,
            .views = views,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Compute) void {
        var up = self.upstream;
        up.deinit();
        for (self.derived_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.derived_cols);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.views);
        self.arena.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Compute) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *Compute, pred: Predicate) !void {
        // Pushdown is safe only for predicates referencing upstream
        // columns (not derived ones). For v1 simplicity, just forward
        // — the Filter operator above us validates predicate columns
        // against our output schema, which includes both upstream and
        // derived, so it'll only push down what's pushable through
        // its own check. Anything Filter pushes here we forward to
        // upstream blindly; upstream's addPrune does its own column
        // check.
        return self.upstream.addPrune(pred);
    }

    /// Compute preserves row count (adds columns, doesn't drop rows).
    /// Sort state preserved as long as the sort-state columns aren't
    /// derived (derived columns can't be in upstream's sort_state, so
    /// they can't appear in it; thus the upstream's claim is still
    /// fully valid in our output schema).
    pub fn stats(self: *Compute) exec.PipelineStats {
        return self.upstream.stats();
    }

    pub fn next(self: *Compute) !?Batch {
        const in = (try self.upstream.next()) orelse return null;
        const n = in.row_count;

        for (self.derived, self.derived_cols, 0..) |r, *out_col, di| {
            out_col.clear();
            switch (r.kind) {
                .rename => |rn| try appendCopiedColumn(self.allocator, out_col, in.values[rn.src_idx], n),
                .call => |c| {
                    // Gather arg views.
                    var arg_views_buf: [16]ColumnView = undefined;
                    if (c.arg_indices.len > arg_views_buf.len) return Error.ComputeTooManyArgs;
                    const arg_views = arg_views_buf[0..c.arg_indices.len];
                    for (c.arg_indices, arg_views) |src_idx, *view| view.* = in.values[src_idx];

                    try c.func.kernel(self.allocator, arg_views, out_col, n);

                    // Null bookkeeping. Skip when output isn't nullable
                    // (e.g. neither input nullable, propagates strategy).
                    if (out_col.nulls != null) {
                        switch (c.func.null_strategy) {
                            .propagates => try writePropagatedNulls(self.allocator, out_col, arg_views, n),
                            .absorbs => try writeAbsorbedNulls(self.allocator, out_col, arg_views, n),
                            .kernel_managed => {}, // kernel already wrote the bitmap
                        }
                    }
                },
            }
            _ = di;
        }

        for (in.values, 0..) |v, i| self.views[i] = v;
        for (self.derived_cols, in.values.len..) |c, i| self.views[i] = c.view();

        return Batch{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = n,
        };
    }
};

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

fn resolveDerived(aa: Allocator, d: Derived, up_schema: []const Column) !ResolvedDerived {
    switch (d.expr) {
        .col_ref => |name| {
            const idx = columnIndex(up_schema, name) orelse return Error.ColumnNotFound;
            return .{
                .name = try aa.dupe(u8, d.name),
                .output_type = up_schema[idx].type,
                .kind = .{ .rename = .{ .src_idx = idx } },
            };
        },
        .lit => return Error.ComputeUnsupportedExpr, // v1: no literal-only derived
        .call => |c| {
            // v1: only flat calls. Every arg must be .col_ref.
            const arg_indices = try aa.alloc(usize, c.args.len);
            const arg_types = try aa.alloc(Type, c.args.len);
            for (c.args, 0..) |arg, i| {
                switch (arg) {
                    .col_ref => |aname| {
                        const idx = columnIndex(up_schema, aname) orelse return Error.ColumnNotFound;
                        arg_indices[i] = idx;
                        arg_types[i] = up_schema[idx].type;
                    },
                    else => return Error.ComputeUnsupportedExpr, // v1: nested + lits not yet
                }
            }
            const func = scalar_fn.resolve(c.fn_name, arg_types) orelse return Error.ComputeNoSuchOverload;
            return .{
                .name = try aa.dupe(u8, d.name),
                .output_type = func.return_type,
                .kind = .{ .call = .{ .func = func, .arg_indices = arg_indices } },
            };
        },
    }
}

fn columnIndex(schema: []const Column, name: []const u8) ?usize {
    for (schema, 0..) |c, i| {
        if (std.mem.eql(u8, c.name, name)) return i;
    }
    return null;
}

fn derivedNullable(r: ResolvedDerived, up_schema: []const Column) bool {
    return switch (r.kind) {
        .rename => |rn| up_schema[rn.src_idx].nullable,
        .call => |c| blk: {
            // .absorbs: can produce null from all-null inputs.
            // .kernel_managed: kernel decides per-row, may emit null
            // even on non-null inputs (e.g. nullif).
            // Either way → always nullable.
            switch (c.func.null_strategy) {
                .absorbs, .kernel_managed => break :blk true,
                .propagates => {},
            }
            for (c.arg_indices) |idx| {
                if (up_schema[idx].nullable) break :blk true;
            }
            break :blk false;
        },
    };
}

// ---------------------------------------------------------------------------
// Null bookkeeping
// ---------------------------------------------------------------------------

fn writePropagatedNulls(
    allocator: Allocator,
    out: *ColumnStore,
    arg_views: []const ColumnView,
    n: usize,
) !void {
    // Output row i valid iff ALL arg rows i are valid.
    const base = out.data.rowCount() - n;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var valid = true;
        for (arg_views) |v| {
            if (!v.isValid(i)) {
                valid = false;
                break;
            }
        }
        try out.appendValidBit(allocator, base + i, valid);
    }
}

fn writeAbsorbedNulls(
    allocator: Allocator,
    out: *ColumnStore,
    arg_views: []const ColumnView,
    n: usize,
) !void {
    // Default rule for absorbs functions: output null iff ALL args
    // are null. (Matches coalesce semantics — first non-null wins.)
    const base = out.data.rowCount() - n;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var any_valid = false;
        for (arg_views) |v| {
            if (v.isValid(i)) {
                any_valid = true;
                break;
            }
        }
        try out.appendValidBit(allocator, base + i, any_valid);
    }
}

// ---------------------------------------------------------------------------
// Rename helper
// ---------------------------------------------------------------------------

fn appendCopiedColumn(
    allocator: Allocator,
    out: *ColumnStore,
    src: ColumnView,
    n: usize,
) !void {
    // Bulk-copy via existing memtable.transform helper. Same op as
    // appendAllColumn used by Sort/Filter for output staging.
    try @import("../engine/transform.zig").appendAllColumn(allocator, src, out);
    _ = n;
}
