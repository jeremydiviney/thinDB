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
//! ## Overflow fidelity
//! Integer arithmetic and integer SUM both wrap (DESIGN.md §3.4): a per-row
//! `a·col+b` either fits its result type or is at least BIGINT wide and wraps
//! mod 2^64, and SUM returns its exact total mod 2^64 as a BIGINT. Wrapping is
//! a ring homomorphism, so `Σ(a·col+b) ≡ a·Σcol + b·n (mod 2^64)` for every
//! input, and the derivation `SUM(col)·a + COUNT(col)·b`, evaluated in
//! wrapping BIGINT arithmetic, equals the direct SUM bit for bit. A LARGEINT
//! base or argument stays direct: its i128 SUM has no wrapping definition to
//! lean on.
//!
//! MIN/MAX do not commute with a wrap, so they reduce only when `a·col+b`
//! provably stays inside its result type for every value of the base column
//! (`affineCannotOverflow`); the derivation then re-applies the original call
//! to the base extreme.

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
/// base column. For a plain `col` reference a=1, b=0 and `arg_type` and
/// `call` are null (no arithmetic).
pub const AffineArg = struct {
    base_col: []const u8,
    a: i128,
    b: i128,
    /// Declared type of the base column (for the MIN/MAX overflow bound).
    base_type: types.Type,
    /// Result type of the direct `a·col+b` call.
    arg_type: ?types.Type,
    /// The direct call, re-applied to a base extreme to derive MIN/MAX.
    call: ?AffineCall,
};

/// `fn_name(col, lit)` (or `fn_name(lit, col)` when `!col_left`).
pub const AffineCall = struct {
    fn_name: []const u8,
    lit: types.Value,
    col_left: bool,
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

/// Type of an integer-family literal, as Compute types it, so the
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
            return AffineArg{ .base_col = name, .a = 1, .b = 0, .base_type = up_schema[idx].type, .arg_type = null, .call = null };
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
            const lit_idx: usize = if (col_left) 1 else 0;
            arg_types[1 - lit_idx] = base_type;
            arg_types[lit_idx] = litValueType(lit_v);
            arg_types[lit_idx] = litValueType(scalar_fn.arithOperandLiteral(c.fn_name, &arg_types, lit_v));
            const resolved = (try scalar_fn.resolve(aa, c.fn_name, &arg_types)) orelse return null;
            return AffineArg{
                .base_col = col_name,
                .a = a,
                .b = b,
                .base_type = base_type,
                .arg_type = resolved.func.return_type,
                .call = .{ .fn_name = c.fn_name, .lit = lit_v, .col_left = col_left },
            };
        },
        else => return null,
    }
}

/// True when `a·col+b` provably stays inside `arg_type` for every `col` in
/// `base_type`'s declared range — so no row wraps and the map is monotonic.
pub fn affineCannotOverflow(arg: AffineArg) bool {
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
/// `base_sum·a + base_cnt·b` in wrapping BIGINT arithmetic (a and b reduced
/// mod 2^64, which the congruence allows). MIN/MAX: the direct call applied to
/// the base extreme, so the result type is the direct one.
pub fn buildDerivedExpr(arena: Allocator, r: ReducedAgg, base: BaseAgg) !Expr {
    switch (r.family) {
        .sum => {
            const a_term = try arena.alloc(Expr, 2);
            a_term[0] = .{ .col_ref = base.sum_name };
            a_term[1] = .{ .lit = .{ .bigint = @truncate(r.arg.a) } };
            const b_term = try arena.alloc(Expr, 2);
            b_term[0] = .{ .col_ref = base.count_name };
            b_term[1] = .{ .lit = .{ .bigint = @truncate(r.arg.b) } };

            const sum_args = try arena.alloc(Expr, 2);
            sum_args[0] = .{ .call = .{ .fn_name = "mul", .args = a_term } };
            sum_args[1] = .{ .call = .{ .fn_name = "mul", .args = b_term } };
            return .{ .call = .{ .fn_name = "add", .args = sum_args } };
        },
        .min, .max => {
            const extreme: Expr = .{ .col_ref = if (r.family == .min) base.min_name else base.max_name };
            const call = r.arg.call orelse return extreme;
            const args = try arena.alloc(Expr, 2);
            args[0] = if (call.col_left) extreme else .{ .lit = call.lit };
            args[1] = if (call.col_left) .{ .lit = call.lit } else extreme;
            return .{ .call = .{ .fn_name = call.fn_name, .args = args } };
        },
    }
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

/// The canonical output type the direct `<func>(arg)` would produce: SUM of a
/// (non-LARGEINT) integer → bigint; MIN/MAX → the arg arithmetic type (or the
/// base type for a plain column).
pub fn aggOutTypeForReduction(family: AggFamily, base_type: types.Type, aff: AffineArg) types.Type {
    return switch (family) {
        .sum => .bigint,
        .min, .max => aff.arg_type orelse base_type,
    };
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
            if (aff.base_type == .largeint) continue;
            if (aff.arg_type) |t| if (t == .largeint) continue;
        } else {
            if (!affineCannotOverflow(aff)) continue;
        }

        const base_idx = types.findColumn(up_schema, aff.base_col) orelse continue;
        // a<0 flips MIN↔MAX: MIN(a·col+b) = a·MAX(col)+b for a<0.
        var fam = family;
        if (family == .min and aff.a < 0) fam = .max;
        if (family == .max and aff.a < 0) fam = .min;

        reduced[i] = .{
            .out_name = a.as,
            .out_type = aggOutTypeForReduction(family, up_schema[base_idx].type, aff),
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
                try aggs.append(arena, .{ .func = .sum, .col = b.base_col, .as = b.sum_name });
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

test "reduce collapses SUM(x), SUM(x+1), SUM(x+2) to one base set for every wrapping width" {
    // SUM wraps mod 2^64 exactly as `x+k` does at BIGINT, so the reduction
    // holds even where x+k can overflow; only a LARGEINT base stays direct.
    const cases = .{
        .{ .base = types.Type.smallint, .reduces = true },
        .{ .base = types.Type.int, .reduces = true },
        .{ .base = types.Type.bigint, .reduces = true },
        .{ .base = types.Type.largeint, .reduces = false },
    };
    inline for (cases) |c| {
        const cols = [_]types.Column{.{ .name = "x", .type = c.base, .nullable = false }};
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

        const maybe_red = try reduce(a, &cols, &.{}, &aggs, &derived, &.{});
        if (!c.reduces) {
            try std.testing.expect(maybe_red == null);
        } else {
            const red = maybe_red.?;
            // Base set is exactly {SUM(x), COUNT(x)} — 2 aggs replacing 3 —
            // and the base SUM keeps the canonical BIGINT type.
            try std.testing.expectEqual(@as(usize, 2), red.base_aggs.len);
            try std.testing.expectEqual(AggFunc.sum, red.base_aggs[0].func);
            try std.testing.expect(red.base_aggs[0].out_type_override == null);
            try std.testing.expectEqual(AggFunc.count, red.base_aggs[1].func);
            // One derivation per original output, projected in order.
            try std.testing.expectEqual(@as(usize, 3), red.post_derived.len);
            try std.testing.expectEqual(@as(usize, 3), red.output_names.len);
        }

        // A single SUM(x) must NOT reduce — base {SUM,COUNT} wouldn't shrink it.
        const one = [_]AggSpec{.{ .func = .sum, .col = "x", .as = "s" }};
        try std.testing.expect((try reduce(a, &cols, &.{}, &one, &.{}, &.{})) == null);
    }
}

test "reduce keeps MIN/MAX(x+k) direct when x+k can wrap" {
    // INT + TINYINT is BIGINT: never wraps, so MIN collapses. BIGINT + TINYINT
    // stays BIGINT and can wrap, which MIN does not commute with.
    const cases = .{
        .{ .base = types.Type.int, .reduces = true },
        .{ .base = types.Type.bigint, .reduces = false },
    };
    inline for (cases) |c| {
        const cols = [_]types.Column{.{ .name = "x", .type = c.base, .nullable = false }};
        var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena_inst.deinit();
        const a = arena_inst.allocator();

        const d1args = try a.alloc(Expr, 2);
        d1args[0] = .{ .col_ref = "x" };
        d1args[1] = .{ .lit = .{ .int = 1 } };
        const derived = [_]Derived{.{ .name = "__a1", .expr = .{ .call = .{ .fn_name = "add", .args = d1args } } }};
        const aggs = [_]AggSpec{
            .{ .func = .min, .col = "x", .as = "m0" },
            .{ .func = .min, .col = "__a1", .as = "m1" },
        };

        const maybe_red = try reduce(a, &cols, &.{}, &aggs, &derived, &.{});
        try std.testing.expectEqual(c.reduces, maybe_red != null);
        if (maybe_red) |red| {
            // The derivation re-applies the direct call to the base MIN.
            const d = red.post_derived[1].expr.call;
            try std.testing.expectEqualStrings("add", d.fn_name);
            try std.testing.expectEqualStrings(red.base_aggs[0].as, d.args[0].col_ref);
        }
    }
}
