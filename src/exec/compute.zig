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

const cast = @import("cast.zig");
const CastKernel = cast.CastKernel;

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

/// Where a function-call arg's row data comes from at exec time.
const ArgSource = union(enum) {
    /// Index into the upstream batch's values.
    col_idx: usize,
    /// Index into the call's `lit_buffers` — a ColumnStore that's
    /// refilled with row_count copies of the literal each batch.
    lit_idx: usize,
};

/// Resolved per-derived plan: either a function call (each arg sourced
/// from an upstream col or a literal-replicated buffer), or a simple
/// column rename.
///
/// `arg_casts` is populated only when at least one arg needs implicit
/// coercion (e.g. `mod(int_col, bigint_col)` routes to the (bigint,
/// bigint) overload via a cast on arg 0). Per-arg slot is null when
/// that arg type already matches the overload — only the args that
/// differ pay any runtime cost.
///
/// `cast_buffers` parallels `arg_casts`: a scratch ColumnStore is
/// allocated once per coerced arg at Compute init time, then refilled
/// (clear + cast kernel) per batch. Non-coerced slots stay empty.
///
/// `lit_buffers` parallels `arg_sources` for `.lit_idx` entries: a
/// long-lived ColumnStore (typed to the literal) is refilled with
/// row_count copies of the literal value at each batch.
const ResolvedDerived = struct {
    name: []const u8,
    output_type: Type,
    kind: union(enum) {
        rename: struct { src_idx: usize },
        call: struct {
            func: ScalarFn,
            arg_sources: []const ArgSource,
            arg_casts: ?[]const ?CastKernel,
            cast_buffers: ?[]?ColumnStore,
            /// Indexed by `ArgSource.lit_idx`; the parallel `lit_values`
            /// holds the literal payloads to replicate each batch.
            lit_buffers: []ColumnStore,
            lit_values: []const types.Value,
        },
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
        for (derived, resolved) |d, *r| r.* = try resolveDerived(allocator, aa, d, up_schema);

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
        for (self.derived) |r| {
            switch (r.kind) {
                .call => |c| {
                    if (c.cast_buffers) |buffers| {
                        for (buffers) |*slot| {
                            if (slot.*) |*cs| cs.deinit(self.allocator);
                        }
                        self.allocator.free(buffers);
                    }
                    for (c.lit_buffers) |*buf| buf.deinit(self.allocator);
                    self.allocator.free(c.lit_buffers);
                },
                else => {},
            }
        }
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

    pub fn accountant(self: *Compute) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn next(self: *Compute) !?Batch {
        const in = (try self.upstream.next()) orelse return null;
        const n = in.row_count;

        for (self.derived, self.derived_cols, 0..) |r, *out_col, di| {
            out_col.clear();
            switch (r.kind) {
                .rename => |rn| try appendCopiedColumn(self.allocator, out_col, in.values[rn.src_idx], n),
                .call => |c| {
                    // Refill literal buffers for this batch: each one
                    // holds `n` copies of its literal value so the
                    // kernel sees a uniformly-sized column view.
                    for (c.lit_buffers, c.lit_values) |*buf, v| {
                        buf.clear();
                        try fillLiteralColumn(self.allocator, buf, v, n);
                    }

                    // Gather arg views — sources are either upstream
                    // columns or refilled literal buffers.
                    var arg_views_buf: [16]ColumnView = undefined;
                    if (c.arg_sources.len > arg_views_buf.len) return Error.ComputeTooManyArgs;
                    const arg_views = arg_views_buf[0..c.arg_sources.len];
                    for (c.arg_sources, arg_views) |src, *view| {
                        view.* = switch (src) {
                            .col_idx => |idx| in.values[idx],
                            .lit_idx => |idx| c.lit_buffers[idx].view(),
                        };
                    }

                    // Apply implicit casts in place: for each coerced arg,
                    // refill the scratch ColumnStore from the upstream
                    // view, then swap the cast view into arg_views.
                    if (c.arg_casts) |casts| {
                        const buffers = c.cast_buffers.?;
                        var one_cast_view: [1]ColumnView = undefined;
                        for (casts, buffers, 0..) |kfn, *buf_slot, arg_i| {
                            const k = kfn orelse continue;
                            const buf = &buf_slot.*.?;
                            buf.clear();
                            one_cast_view[0] = arg_views[arg_i];
                            try k(self.allocator, &one_cast_view, buf, n);
                            arg_views[arg_i] = buf.view();
                        }
                    }

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

fn resolveDerived(
    runtime_allocator: Allocator,
    aa: Allocator,
    d: Derived,
    up_schema: []const Column,
) !ResolvedDerived {
    switch (d.expr) {
        .col_ref => |name| {
            const idx = columnIndex(up_schema, name) orelse return Error.ColumnNotFound;
            return .{
                .name = try aa.dupe(u8, d.name),
                .output_type = up_schema[idx].type,
                .kind = .{ .rename = .{ .src_idx = idx } },
            };
        },
        .lit => return Error.ComputeUnsupportedExpr, // v1: no literal-only derived (use a wrapping function call)
        .call => |c| {
            // v1 of the call path: each arg is either a column ref OR
            // a literal. Nested calls still rejected (task #154 covers
            // the recursive evaluator).
            const arg_sources = try aa.alloc(ArgSource, c.args.len);
            const arg_types = try aa.alloc(Type, c.args.len);
            var lit_values: std.ArrayList(types.Value) = .empty;
            for (c.args, 0..) |arg, i| {
                switch (arg) {
                    .col_ref => |aname| {
                        const idx = columnIndex(up_schema, aname) orelse return Error.ColumnNotFound;
                        arg_sources[i] = .{ .col_idx = idx };
                        arg_types[i] = up_schema[idx].type;
                    },
                    .lit => |v| {
                        const lit_idx = lit_values.items.len;
                        try lit_values.append(aa, v);
                        arg_sources[i] = .{ .lit_idx = lit_idx };
                        arg_types[i] = literalType(v);
                    },
                    .call => return Error.ComputeUnsupportedExpr, // nested calls: task #154
                }
            }
            const r = (try scalar_fn.resolve(aa, c.fn_name, arg_types)) orelse return Error.ComputeNoSuchOverload;

            // Long-lived literal buffers — refilled per batch.
            const lit_values_slice = try lit_values.toOwnedSlice(aa);
            const lit_buffers = try runtime_allocator.alloc(ColumnStore, lit_values_slice.len);
            errdefer runtime_allocator.free(lit_buffers);
            for (lit_values_slice, lit_buffers) |v, *buf| {
                buf.* = try ColumnStore.init(runtime_allocator, literalType(v), false);
            }

            // Allocate scratch ColumnStores only for the args that
            // actually need coercion. The buffers themselves live on
            // the runtime allocator (the long-lived parent of `aa`)
            // so they outlive the resolve phase; `cast_buffers`
            // metadata is in the arena.
            var cast_buffers: ?[]?ColumnStore = null;
            if (r.arg_casts) |casts| {
                const buffers = try runtime_allocator.alloc(?ColumnStore, casts.len);
                for (casts, r.func.arg_types, arg_sources, buffers) |k, declared, src, *slot| {
                    if (k == null) {
                        slot.* = null;
                        continue;
                    }
                    const src_nullable = switch (src) {
                        .col_idx => |idx| up_schema[idx].nullable,
                        .lit_idx => false, // literals are always non-null
                    };
                    slot.* = try ColumnStore.init(runtime_allocator, declared, src_nullable);
                }
                cast_buffers = buffers;
            }
            return .{
                .name = try aa.dupe(u8, d.name),
                .output_type = r.func.return_type,
                .kind = .{ .call = .{
                    .func = r.func,
                    .arg_sources = arg_sources,
                    .arg_casts = r.arg_casts,
                    .cast_buffers = cast_buffers,
                    .lit_buffers = lit_buffers,
                    .lit_values = lit_values_slice,
                } },
            };
        },
    }
}

/// Infer the ColumnStore-compatible Type for a literal Value. Mirrors
/// the active union tag — int literals stay int (not promoted to bigint);
/// promotion happens via the existing implicit-cast machinery if the
/// resolved overload requires it.
fn literalType(v: types.Value) Type {
    return switch (v) {
        .int => .int,
        .bigint => .bigint,
        .smallint => .smallint,
        .tinyint => .tinyint,
        .largeint => .largeint,
        .float => .float,
        .double => .double,
        .boolean => .boolean,
        .text => .string,
        .date => .date,
        .datetime => .datetime,
        // Decimal Values carry just the raw int payload (no precision/
        // scale). Compute can't materialize a typed decimal column
        // without those — caller must explicitly cast / project.
        .decimal64, .decimal128 => @panic("Compute: decimal literal args not supported"),
        .uuid => .uuid,
    };
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
            for (c.arg_sources) |src| {
                switch (src) {
                    .col_idx => |idx| if (up_schema[idx].nullable) break :blk true,
                    .lit_idx => {}, // literals are non-null
                }
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

/// Append `n` copies of `v` into `buf`. Used by Compute's call path to
/// materialize a constant-valued column matching the current batch's
/// row count, so scalar kernels see uniform-width arg slices.
fn fillLiteralColumn(allocator: Allocator, buf: *ColumnStore, v: types.Value, n: usize) !void {
    var i: usize = 0;
    switch (v) {
        .int => |x| while (i < n) : (i += 1) try buf.data.int.append(allocator, x),
        .bigint => |x| while (i < n) : (i += 1) try buf.data.bigint.append(allocator, x),
        .smallint => |x| while (i < n) : (i += 1) try buf.data.smallint.append(allocator, x),
        .tinyint => |x| while (i < n) : (i += 1) try buf.data.tinyint.append(allocator, x),
        .largeint => |x| while (i < n) : (i += 1) try buf.data.largeint.append(allocator, x),
        .float => |x| while (i < n) : (i += 1) try buf.data.float.append(allocator, x),
        .double => |x| while (i < n) : (i += 1) try buf.data.double.append(allocator, x),
        .boolean => |x| while (i < n) : (i += 1) try buf.data.boolean.append(allocator, @intFromBool(x)),
        .text => |s| while (i < n) : (i += 1) try buf.data.string.appendValue(allocator, s),
        .date => |x| while (i < n) : (i += 1) try buf.data.date.append(allocator, x),
        .datetime => |x| while (i < n) : (i += 1) try buf.data.datetime.append(allocator, x),
        .decimal64, .decimal128 => unreachable, // literalType() panics before we get here
        .uuid => |x| while (i < n) : (i += 1) try buf.data.uuid.append(allocator, x),
    }
}
