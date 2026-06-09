//! Algebraic aggregate reduction: collapse aggregates whose argument is an
//! affine transform of one base column onto a small shared base set, then derive
//! every original output once from that base — "compute one, late-materialize
//! the rest". ClickBench Q29 is the motivating case: 90 `SUM(ResolutionWidth +
//! k)` become a single `SUM(ResolutionWidth)` + `COUNT(ResolutionWidth)`, with
//! each output recovered as `SUM + k·COUNT`.
//!
//! `reduce` is handler-agnostic: it takes the aggregate list (plus any pre-agg
//! Compute that produced the aggregate arguments) and returns the reduced base
//! aggregate set, the pre-agg Computes to keep, the post-agg derivations, and
//! the final output column order. The V2 builders feed the base set to whichever
//! parallel reducer matches and layer the derivations as an ordinary Compute +
//! Project; the V1 path (net/local.zig) assembles the same pieces into a Query.
//!
//! ## Overflow fidelity (SUM)
//! The direct path computes `Σ(a·col+b)` by accumulating each per-row
//! `a·col+b` (which may itself WRAP its argument arithmetic type) in i128,
//! narrowing once at the i64 output. The reduction is bit-identical only when:
//!   1. The base SUM is pinned to LARGEINT (i128) via `out_type_override`, so it
//!      never narrows mid-flight, and
//!   2. `a·col+b` provably cannot wrap its arg arithmetic type for ANY value in
//!      the base column's declared range (`affineNoWrap`), so `Σ(a·col+b) =
//!      a·Σcol + b·n` holds exactly in i128. The derivation runs in i128 and
//!      narrows with `__narrow_bigint`, whose range check matches SUM finalize.
//! Any aggregate failing the guard is left direct.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const ir = @import("../ir/ir.zig");
const aggregate = @import("aggregate.zig");
const scalar_fn = @import("scalar_fn.zig");

const AggFunc = aggregate.AggFunc;
const AggSpec = aggregate.AggSpec;
const Derived = ir.Derived;
const Expr = ir.Expr;

/// One base column's affine decomposition: `value = a·col + b` over the named
/// base column. For a plain `col` reference a=1, b=0 and `arg_type` is null (no
/// wrapping arg).
pub const AffineArg = struct {
    base_col: []const u8,
    a: i128,
    b: i128,
    /// Declared type of the base column (for the overflow bound on SUM).
    base_type: types.Type,
    /// Arithmetic type the direct `a·col+b` would evaluate (and wrap) in; null
    /// when the arg is a plain column (no intermediate arithmetic).
    arg_type: ?types.Type,
};

pub fn typeMinI128(t: types.Type) ?i128 {
    return switch (t) {
        .tinyint => std.math.minInt(i8),
        .smallint => std.math.minInt(i16),
        .int => std.math.minInt(i32),
        .bigint => std.math.minInt(i64),
        .largeint => std.math.minInt(i128),
        .boolean => 0,
        .decimal64 => std.math.minInt(i64),
        .decimal128 => std.math.minInt(i128),
        else => null,
    };
}

pub fn typeMaxI128(t: types.Type) ?i128 {
    return switch (t) {
        .tinyint => std.math.maxInt(i8),
        .smallint => std.math.maxInt(i16),
        .int => std.math.maxInt(i32),
        .bigint => std.math.maxInt(i64),
        .largeint => std.math.maxInt(i128),
        .boolean => 1,
        .decimal64 => std.math.maxInt(i64),
        .decimal128 => std.math.maxInt(i128),
        else => null,
    };
}

/// Type of an integer-family literal — matches the parser's lowering so the
/// arithmetic-type resolution sees the same overload the direct path picked.
fn litValueType(v: types.Value) types.Type {
    return switch (v) {
        .tinyint => .tinyint,
        .smallint => .smallint,
        .int => .int,
        .bigint => .bigint,
        .largeint => .largeint,
        .boolean => .boolean,
        else => .double,
    };
}

/// Extract the constant integer multiplier/offset from an affine arg literal.
/// Only integer-valued literals qualify (a float literal would taint the
/// arithmetic type / rounding). Returns null for anything else.
fn affineLiteralI128(v: types.Value) ?i128 {
    return switch (v) {
        .tinyint => |x| x,
        .smallint => |x| x,
        .int => |x| x,
        .bigint => |x| x,
        .largeint => |x| x,
        .boolean => |x| @intFromBool(x),
        else => null,
    };
}

/// Decompose `e` into `a·base_col + b` if it is an affine transform of a single
/// plain column: `col`, `col±c`, `c±col`, `c·col`, `col·c`. Otherwise null.
/// `up_schema` resolves the base column's declared type; `arg_type` is the
/// resolved arithmetic type of the (non-trivial) expression.
pub fn affineDecompose(aa: Allocator, up_schema: []const types.Column, e: Expr) !?AffineArg {
    switch (e) {
        .col_ref => |name| {
            const idx = types.findColumn(up_schema, name) orelse return null;
            return AffineArg{ .base_col = name, .a = 1, .b = 0, .base_type = up_schema[idx].type, .arg_type = null };
        },
        .call => |c| {
            if (c.args.len != 2) return null;
            const is_add = std.mem.eql(u8, c.fn_name, "add");
            const is_sub = std.mem.eql(u8, c.fn_name, "sub");
            const is_mul = std.mem.eql(u8, c.fn_name, "mul");
            if (!(is_add or is_sub or is_mul)) return null;

            const l = c.args[0];
            const r = c.args[1];
            var col_name: []const u8 = undefined;
            var lit_v: types.Value = undefined;
            var col_left: bool = undefined;
            switch (l) {
                .col_ref => |n| switch (r) {
                    .lit => |v| {
                        col_name = n;
                        lit_v = v;
                        col_left = true;
                    },
                    else => return null,
                },
                .lit => |v| switch (r) {
                    .col_ref => |n| {
                        col_name = n;
                        lit_v = v;
                        col_left = false;
                    },
                    else => return null,
                },
                else => return null,
            }
            const k = affineLiteralI128(lit_v) orelse return null;
            const idx = types.findColumn(up_schema, col_name) orelse return null;
            const base_type = up_schema[idx].type;

            var a: i128 = undefined;
            var b: i128 = undefined;
            if (is_add) {
                a = 1;
                b = k;
            } else if (is_sub) {
                if (col_left) {
                    a = 1;
                    b = -k; // col - k
                } else {
                    a = -1;
                    b = k; // k - col
                }
            } else { // mul
                a = k;
                b = 0; // c·col == col·c
            }

            var arg_types: [2]types.Type = undefined;
            arg_types[0] = if (col_left) base_type else litValueType(lit_v);
            arg_types[1] = if (col_left) litValueType(lit_v) else base_type;
            const resolved = (try scalar_fn.resolve(aa, c.fn_name, &arg_types)) orelse return null;
            return AffineArg{ .base_col = col_name, .a = a, .b = b, .base_type = base_type, .arg_type = resolved.func.return_type };
        },
        else => return null,
    }
}

/// True when `a·col+b` provably stays inside `arg_type` for every `col` in
/// `base_type`'s declared range — so the direct path's wrapping arithmetic never
/// fires and `Σ(a·col+b) == a·Σcol + b·n` exactly in i128.
pub fn affineNoWrap(arg: AffineArg) bool {
    const at = arg.arg_type orelse return true; // plain col: no arithmetic
    const lo_base = typeMinI128(arg.base_type) orelse return false;
    const hi_base = typeMaxI128(arg.base_type) orelse return false;
    const arg_lo = typeMinI128(at) orelse return false;
    const arg_hi = typeMaxI128(at) orelse return false;
    const e1 = std.math.mul(i128, arg.a, lo_base) catch return false;
    const e2 = std.math.mul(i128, arg.a, hi_base) catch return false;
    const v1 = std.math.add(i128, e1, arg.b) catch return false;
    const v2 = std.math.add(i128, e2, arg.b) catch return false;
    const lo = @min(v1, v2);
    const hi = @max(v1, v2);
    return lo >= arg_lo and hi <= arg_hi;
}

/// Aggregate family for base-set sharing. SUM-family bases need {SUM,COUNT};
/// MIN / MAX each need just their own extreme over the base column.
pub const AggFamily = enum { sum, min, max };

pub fn familyOf(f: AggFunc) ?AggFamily {
    return switch (f) {
        .sum => .sum,
        .min => .min,
        .max => .max,
        else => null,
    };
}

fn isProtected(name: []const u8, protected: []const []const u8) bool {
    for (protected) |p| if (types.columnNameEql(p, name)) return true;
    return false;
}

/// Per-original-aggregate reduction plan.
pub const ReducedAgg = struct {
    out_name: []const u8,
    out_type: types.Type,
    family: AggFamily,
    arg: AffineArg,
};

/// A base aggregate to compute once (deduped across reductions sharing a
/// `(family, base_col)`).
pub const BaseAgg = struct {
    family: AggFamily,
    base_col: []const u8,
    sum_name: []const u8,
    count_name: []const u8,
    min_name: []const u8,
    max_name: []const u8,
};

/// The IR Expr deriving one original output from its base aggregate(s). SUM:
/// `__narrow_bigint(base_sum·a + base_cnt·b)` in i128 (stays largeint when the
/// output is largeint). MIN/MAX: `a·base+b` in the arg arithmetic type.
pub fn buildDerivedExpr(arena: Allocator, r: ReducedAgg, base: BaseAgg) !Expr {
    switch (r.family) {
        .sum => {
            const sum_ref = try arena.create(Expr);
            sum_ref.* = .{ .col_ref = base.sum_name };
            const cnt_ref = try arena.create(Expr);
            cnt_ref.* = .{ .col_ref = base.count_name };

            const a_term = try arena.alloc(Expr, 2);
            a_term[0] = sum_ref.*;
            a_term[1] = .{ .lit = .{ .largeint = r.arg.a } };
            const b_term = try arena.alloc(Expr, 2);
            b_term[0] = cnt_ref.*;
            b_term[1] = .{ .lit = .{ .largeint = r.arg.b } };

            const sum_args = try arena.alloc(Expr, 2);
            sum_args[0] = .{ .call = .{ .fn_name = "mul", .args = a_term } };
            sum_args[1] = .{ .call = .{ .fn_name = "mul", .args = b_term } };
            const inner: Expr = .{ .call = .{ .fn_name = "add", .args = sum_args } };

            if (r.out_type == .largeint) return inner;
            const narrow_args = try arena.alloc(Expr, 1);
            narrow_args[0] = inner;
            return .{ .call = .{ .fn_name = "__narrow_bigint", .args = narrow_args } };
        },
        .min, .max => {
            const base_name = if (r.family == .min) base.min_name else base.max_name;
            const col_ref = try arena.create(Expr);
            col_ref.* = .{ .col_ref = base_name };
            var scaled: Expr = col_ref.*;
            if (r.arg.a != 1) {
                const a_args = try arena.alloc(Expr, 2);
                a_args[0] = col_ref.*;
                a_args[1] = .{ .lit = litForI128(r.arg.a) };
                scaled = .{ .call = .{ .fn_name = "mul", .args = a_args } };
            }
            if (r.arg.b == 0) return scaled;
            const b_args = try arena.alloc(Expr, 2);
            b_args[0] = scaled;
            b_args[1] = .{ .lit = litForI128(r.arg.b) };
            return .{ .call = .{ .fn_name = "add", .args = b_args } };
        },
    }
}

/// Smallest integer-family Value holding `v` — matches the parser's literal
/// typing so coercion in the derived Compute picks the direct path's width.
pub fn litForI128(v: i128) types.Value {
    if (v >= std.math.minInt(i32) and v <= std.math.maxInt(i32)) return .{ .int = @intCast(v) };
    if (v >= std.math.minInt(i64) and v <= std.math.maxInt(i64)) return .{ .bigint = @intCast(v) };
    return .{ .largeint = v };
}

/// `desired` if unused among `taken`, else a `desired__<idx>` suffix that is.
/// Preserves the SELECT alias when unique (downstream ORDER BY / HAVING still
/// binds); only collapsed identically-labeled aggregates get suffixed.
pub fn uniqueOutputName(arena: Allocator, taken: []const []const u8, desired: []const u8, idx: usize) ![]const u8 {
    var clash = false;
    for (taken) |t| {
        if (types.columnNameEql(t, desired)) {
            clash = true;
            break;
        }
    }
    if (!clash) return desired;
    return std.fmt.allocPrint(arena, "{s}__{d}", .{ desired, idx });
}

/// The canonical output type the direct `<func>(arg)` would produce. SUM(int) →
/// bigint (or largeint when the base is largeint); MIN/MAX → the arg arithmetic
/// type (or the base type for a plain column).
pub fn aggOutTypeForReduction(family: AggFamily, base_type: types.Type, aff: AffineArg) !types.Type {
    switch (family) {
        .sum => return if (base_type == .largeint) .largeint else .bigint,
        .min, .max => return aff.arg_type orelse base_type,
    }
}

/// The pieces of an affine reduction, handler-agnostic. The caller computes
/// `base_aggs` (parallel), keeps `pre_derived` below the reducer (group keys +
/// direct-aggregate args), layers `post_derived` as a Compute over the reduced
/// output, then projects `output_names` (group cols + one column per original
/// aggregate, in SELECT order). All slices are arena-owned.
pub const Reduction = struct {
    base_aggs: []const AggSpec,
    pre_derived: []const Derived,
    post_derived: []const Derived,
    output_names: []const []const u8,
};

/// Reduce affine-linked aggregates onto a shared base set. Returns null when
/// nothing collapses (or the rewrite wouldn't shrink the aggregate set), so the
/// caller runs the original aggregates unchanged.
///
/// `up_schema` is the schema the aggregates see (scan / pre-agg-Compute output).
/// `agg_arg_derived` maps a synthetic aggregate-argument column to its
/// expression (empty when arguments are plain columns); `group_cols` names that
/// are computed appear there too and are preserved in `pre_derived`.
///
/// `protected` lists output aliases that must NOT be reduced — aggregates a
/// grouped query consumes in ORDER BY / HAVING, whose value the parallel core
/// needs in-hand to rank/filter (the derivation runs after the core). The
/// global path, with no ranking, passes an empty list.
pub fn reduce(
    arena: Allocator,
    up_schema: []const types.Column,
    group_cols: []const []const u8,
    in_aggs: []const AggSpec,
    agg_arg_derived: []const Derived,
    protected: []const []const u8,
) !?Reduction {
    if (in_aggs.len == 0) return null;

    const reduced = try arena.alloc(?ReducedAgg, in_aggs.len);
    var any_reduced = false;
    for (in_aggs, 0..) |a, i| {
        reduced[i] = null;
        const family = familyOf(a.func) orelse continue;
        if (isProtected(a.as, protected)) continue;
        const col_name = a.col orelse continue; // COUNT(*) — no arg
        var arg_expr: Expr = .{ .col_ref = col_name };
        for (agg_arg_derived) |d| {
            if (types.columnNameEql(d.name, col_name)) {
                arg_expr = d.expr;
                break;
            }
        }
        const aff = (try affineDecompose(arena, up_schema, arg_expr)) orelse continue;

        if (family == .sum) {
            // Integer SUM only; float/decimal SUM scale handling stays direct.
            if (!aff.base_type.isInteger() and aff.base_type != .boolean) continue;
            if (!affineNoWrap(aff)) continue;
        } else {
            if (!affineNoWrap(aff)) continue;
        }

        const base_idx = types.findColumn(up_schema, aff.base_col) orelse continue;
        // a<0 flips MIN↔MAX: MIN(a·col+b) = a·MAX(col)+b for a<0.
        var fam = family;
        if (family == .min and aff.a < 0) fam = .max;
        if (family == .max and aff.a < 0) fam = .min;

        reduced[i] = .{
            .out_name = a.as,
            .out_type = try aggOutTypeForReduction(family, up_schema[base_idx].type, aff),
            .family = fam,
            .arg = aff,
        };
        any_reduced = true;
    }
    if (!any_reduced) return null;

    // Deduped base set keyed by (effective family, base col).
    var bases: std.ArrayListUnmanaged(BaseAgg) = .empty;
    const base_idx_of = try arena.alloc(usize, in_aggs.len);
    var counter: usize = 0;
    for (reduced, 0..) |maybe, i| {
        const r = maybe orelse continue;
        var found: ?usize = null;
        for (bases.items, 0..) |b, bi| {
            if (b.family == r.family and types.columnNameEql(b.base_col, r.arg.base_col)) {
                found = bi;
                break;
            }
        }
        if (found) |bi| {
            base_idx_of[i] = bi;
        } else {
            const tag = try std.fmt.allocPrint(arena, "{d}", .{counter});
            counter += 1;
            try bases.append(arena, .{
                .family = r.family,
                .base_col = r.arg.base_col,
                .sum_name = try std.fmt.allocPrint(arena, "__base_sum_{s}", .{tag}),
                .count_name = try std.fmt.allocPrint(arena, "__base_cnt_{s}", .{tag}),
                .min_name = try std.fmt.allocPrint(arena, "__base_min_{s}", .{tag}),
                .max_name = try std.fmt.allocPrint(arena, "__base_max_{s}", .{tag}),
            });
            base_idx_of[i] = bases.items.len - 1;
        }
    }

    var base_agg_count: usize = 0;
    for (bases.items) |b| base_agg_count += if (b.family == .sum) @as(usize, 2) else 1;
    var direct_count: usize = 0;
    for (reduced) |m| {
        if (m == null) direct_count += 1;
    }
    // Only fire when it strictly shrinks the aggregate set.
    if (base_agg_count + direct_count >= in_aggs.len) return null;

    // Base set first, then the direct (unreduced) aggregates in original order.
    var aggs: std.ArrayListUnmanaged(AggSpec) = .empty;
    for (bases.items) |b| {
        switch (b.family) {
            .sum => {
                try aggs.append(arena, .{ .func = .sum, .col = b.base_col, .as = b.sum_name, .out_type_override = .largeint });
                try aggs.append(arena, .{ .func = .count, .col = b.base_col, .as = b.count_name });
            },
            .min => try aggs.append(arena, .{ .func = .min, .col = b.base_col, .as = b.min_name }),
            .max => try aggs.append(arena, .{ .func = .max, .col = b.base_col, .as = b.max_name }),
        }
    }
    // Direct aggregates keep their original spec (and their __agg_arg derived).
    var keep_derived: std.ArrayListUnmanaged(Derived) = .empty;
    for (in_aggs, 0..) |a, i| {
        if (reduced[i] != null) continue;
        try aggs.append(arena, a);
        if (a.col) |cn| {
            for (agg_arg_derived) |d| {
                if (types.columnNameEql(d.name, cn)) {
                    try keep_derived.append(arena, d);
                    break;
                }
            }
        }
    }

    // Pre-agg Computes to keep below the reducer: computed group keys (always)
    // plus the agg-arg derived still referenced by a direct aggregate.
    var pre: std.ArrayListUnmanaged(Derived) = .empty;
    for (agg_arg_derived) |d| {
        var is_group_key = false;
        for (group_cols) |gc| {
            if (types.columnNameEql(gc, d.name)) is_group_key = true;
        }
        if (is_group_key) {
            try pre.append(arena, d);
            continue;
        }
        for (keep_derived.items) |kd| {
            if (types.columnNameEql(kd.name, d.name)) {
                try pre.append(arena, d);
                break;
            }
        }
    }

    // Post-agg derivation + final output order.
    var post: std.ArrayListUnmanaged(Derived) = .empty;
    const out_names = try arena.alloc([]const u8, group_cols.len + in_aggs.len);
    for (group_cols, 0..) |gc, i| out_names[i] = gc;
    for (reduced, 0..) |maybe, i| {
        const out_slot = group_cols.len + i;
        if (maybe) |r| {
            const b = bases.items[base_idx_of[i]];
            const name = try uniqueOutputName(arena, out_names[0..out_slot], r.out_name, i);
            try post.append(arena, .{ .name = name, .expr = try buildDerivedExpr(arena, r, b) });
            out_names[out_slot] = name;
        } else {
            out_names[out_slot] = in_aggs[i].as;
        }
    }

    return .{
        .base_aggs = try aggs.toOwnedSlice(arena),
        .pre_derived = try pre.toOwnedSlice(arena),
        .post_derived = try post.toOwnedSlice(arena),
        .output_names = out_names,
    };
}

// ---------------------------------------------------------------------------

test "affineDecompose recognizes col, col+k, k-col, c*col" {
    const cols = [_]types.Column{.{ .name = "x", .type = .int, .nullable = false }};
    const mk = struct {
        fn call(a: Allocator, name: []const u8, l: Expr, r: Expr) Expr {
            const args = a.alloc(Expr, 2) catch unreachable;
            args[0] = l;
            args[1] = r;
            return .{ .call = .{ .fn_name = name, .args = args } };
        }
    }.call;
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const a = arena_inst.allocator();

    const plain = (try affineDecompose(a, &cols, .{ .col_ref = "x" })).?;
    try std.testing.expectEqual(@as(i128, 1), plain.a);
    try std.testing.expectEqual(@as(i128, 0), plain.b);

    const add = (try affineDecompose(a, &cols, mk(a, "add", .{ .col_ref = "x" }, .{ .lit = .{ .int = 5 } }))).?;
    try std.testing.expectEqual(@as(i128, 1), add.a);
    try std.testing.expectEqual(@as(i128, 5), add.b);

    const kminus = (try affineDecompose(a, &cols, mk(a, "sub", .{ .lit = .{ .int = 5 } }, .{ .col_ref = "x" }))).?;
    try std.testing.expectEqual(@as(i128, -1), kminus.a);
    try std.testing.expectEqual(@as(i128, 5), kminus.b);

    const scaled = (try affineDecompose(a, &cols, mk(a, "mul", .{ .col_ref = "x" }, .{ .lit = .{ .int = 3 } }))).?;
    try std.testing.expectEqual(@as(i128, 3), scaled.a);
    try std.testing.expectEqual(@as(i128, 0), scaled.b);

    // Two columns isn't affine over a single base.
    try std.testing.expect((try affineDecompose(a, &cols, mk(a, "add", .{ .col_ref = "x" }, .{ .col_ref = "x" }))) == null);
}

test "reduce collapses SUM(x), SUM(x+1), SUM(x+2) to one base set" {
    // smallint base: x+k widens to int and provably can't overflow, so the
    // overflow guard admits the reduction (an int base would decline x+k — it
    // can wrap — which is the correct, fidelity-preserving behavior).
    const cols = [_]types.Column{.{ .name = "x", .type = .smallint, .nullable = false }};
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const a = arena_inst.allocator();

    // Pre-agg Computes for the +1 / +2 arguments, as the planner produces them.
    const d1args = try a.alloc(Expr, 2);
    d1args[0] = .{ .col_ref = "x" };
    d1args[1] = .{ .lit = .{ .int = 1 } };
    const d2args = try a.alloc(Expr, 2);
    d2args[0] = .{ .col_ref = "x" };
    d2args[1] = .{ .lit = .{ .int = 2 } };
    const derived = [_]Derived{
        .{ .name = "__a1", .expr = .{ .call = .{ .fn_name = "add", .args = d1args } } },
        .{ .name = "__a2", .expr = .{ .call = .{ .fn_name = "add", .args = d2args } } },
    };
    const aggs = [_]AggSpec{
        .{ .func = .sum, .col = "x", .as = "s0" },
        .{ .func = .sum, .col = "__a1", .as = "s1" },
        .{ .func = .sum, .col = "__a2", .as = "s2" },
    };

    const red = (try reduce(a, &cols, &.{}, &aggs, &derived, &.{})).?;
    // Base set is exactly {SUM(x), COUNT(x)} — 2 aggs replacing 3.
    try std.testing.expectEqual(@as(usize, 2), red.base_aggs.len);
    try std.testing.expectEqual(AggFunc.sum, red.base_aggs[0].func);
    try std.testing.expectEqual(types.Type.largeint, red.base_aggs[0].out_type_override.?);
    try std.testing.expectEqual(AggFunc.count, red.base_aggs[1].func);
    // One derivation per original output, projected in order.
    try std.testing.expectEqual(@as(usize, 3), red.post_derived.len);
    try std.testing.expectEqual(@as(usize, 3), red.output_names.len);

    // A single SUM(x) must NOT reduce — base {SUM,COUNT} wouldn't shrink it.
    const one = [_]AggSpec{.{ .func = .sum, .col = "x", .as = "s" }};
    try std.testing.expect((try reduce(a, &cols, &.{}, &one, &.{}, &.{})) == null);
}
