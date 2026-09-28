//! Expression IR for derived columns. Consumed by the Compute operator
//! (`exec/compute.zig`) and built by users via helper functions defined
//! alongside each registered scalar function (`exec/scalar_fn.zig`).
//!
//! An `Expr` is a tree of:
//!   - col_ref: refer to an upstream column by name
//!   - lit:     a constant value
//!   - call:    invoke a registered scalar function on N argument exprs
//!
//! Lifetimes: when constructed via the builder helpers
//! (`thindb.expr.col(name)`, `thindb.expr.call(arena, name, args)`),
//! every child slice + string is duped into the supplied arena. The
//! caller passes the Query's arena so the tree lives as long as the
//! query plan.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const scalar_fn_common = @import("scalar_fn_common.zig");
const Value = types.Value;
const Type = types.Type;

const predicate_mod = @import("predicate.zig");
const PredicateExpr = predicate_mod.PredicateExpr;

pub const Expr = union(enum) {
    /// Reference to an upstream column by name.
    col_ref: []const u8,
    /// Constant value. Type comes from the active union tag.
    lit: Value,
    /// SQL NULL with a resolved storage type.
    null_lit: Type,
    /// Function invocation. `fn_name` is matched against the registry
    /// at plan time. `args` may be empty (for nullary functions).
    call: Call,
    /// SQL searched CASE expression. Branches evaluated in order;
    /// first branch whose `cond` is true contributes its `then`
    /// expression to the row. When no branch matches the optional
    /// `else_branch` wins (NULL if absent). Branch `then` results must
    /// resolve to one common type through implicit widening.
    case: Case,
    /// Uncorrelated scalar subquery — `(SELECT single_col FROM ...)`.
    /// The pointer is `*const ir.Op`; opaqued here to break the
    /// expr → ir → expr import cycle. A pre-compile resolution pass
    /// runs the subquery once, extracts the single value, and
    /// substitutes a `.lit` node before any operator is built —
    /// operators never see this variant.
    scalar_subquery: *const anyopaque,
    /// `EXISTS (SELECT ...)` in projection / expression position.
    /// Pre-compile pass runs the inner once and rewrites this into
    /// a `.lit = .{ .boolean = true|false }`. Opaque pointer for
    /// the same cycle reason.
    exists_subquery: *const anyopaque,
    /// MySQL-style user-defined variable reference `@name`. The
    /// pre-compile pass looks up the value in the active Session's
    /// vars map and rewrites this into a `.lit`. Operators never
    /// see this variant.
    var_ref: []const u8,

    pub const Call = struct {
        fn_name: []const u8,
        args: []const Expr,
        /// Its text constants come from the statement, as a
        /// `Predicate.from_statement` value does: one it compares with a
        /// DATE or DATETIME that no date reads fails the statement (NULLIF).
        /// A bound parameter's text, or an API caller's, never equals one.
        from_statement: bool = false,
    };

    pub const Branch = struct {
        cond: PredicateExpr,
        then: Expr,
    };

    pub const Case = struct {
        branches: []const Branch,
        else_branch: ?*const Expr,
        /// Computed operands the conditions compare by name. The CASE
        /// computes each one at the first WHEN that reads it, over only the
        /// rows no earlier WHEN took, so an operand never raises on a row it
        /// can't decide. The names are the CASE's own: nothing outside its
        /// conditions reads them.
        operands: []const Operand = &.{},

        pub fn operandNamed(self: Case, name: []const u8) bool {
            return operandListed(self.operands, name);
        }
    };

    pub const Operand = struct {
        name: []const u8,
        expr: Expr,
    };
};

pub fn operandListed(operands: []const Expr.Operand, name: []const u8) bool {
    for (operands) |o| if (types.columnNameEql(o.name, name)) return true;
    return false;
}

/// The name of a CASE's operand `k` (`Expr.Case.operands`). Positional, so
/// two copies of one CASE are the same tree.
pub fn caseOperandName(arena: Allocator, k: usize) Allocator.Error![]const u8 {
    return std.fmt.allocPrint(arena, "__case_operand_{d}", .{k});
}

/// The columns a CASE condition reads from the CASE's input: every name it
/// compares except the CASE's own operands.
pub fn collectCaseConditionRefs(allocator: Allocator, out: *std.ArrayListUnmanaged([]const u8), cs: Expr.Case, cond: PredicateExpr) Allocator.Error!void {
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer names.deinit(allocator);
    try predicate_mod.collectColumnNames(allocator, &names, cond);
    for (names.items) |name| if (!cs.operandNamed(name)) try out.append(allocator, name);
}

/// Build a column-reference expression. String borrowed from caller —
/// stable through the lifetime of the resulting Expr. For inline use:
/// `expr.col("name")` works because the string literal has static
/// lifetime.
pub fn col(name: []const u8) Expr {
    return .{ .col_ref = name };
}

/// Build a literal expression. `Value` is value-typed (no allocations);
/// the only non-trivial case is `.text` which carries a borrowed
/// `[]const u8` — caller-owned.
pub fn lit(v: Value) Expr {
    return .{ .lit = v };
}

/// Build a function-call expression. Allocates an arena-owned copy of
/// the `args` slice + dups the `fn_name` so the returned Expr has no
/// borrowed pointers into caller storage past this call. Use this for
/// any call constructed with dynamic args.
///
/// For the common builder-helper case (e.g. `expr.upper(arena, x)`),
/// the helpers in `scalar_fn.zig` are thin wrappers around this.
pub fn call(arena: Allocator, fn_name: []const u8, args: []const Expr) !Expr {
    const name_dup = try arena.dupe(u8, fn_name);
    const args_dup = try arena.alloc(Expr, args.len);
    @memcpy(args_dup, args);
    return .{ .call = .{ .fn_name = name_dup, .args = args_dup } };
}

/// Walk the tree, dup all strings + slice backings into `out_arena`.
/// Used when an Expr built with borrowed slices needs to outlive its
/// source. Resolution copies the user-built tree into the operator's
/// own arena via this.
pub fn deepClone(out_arena: Allocator, e: Expr) Allocator.Error!Expr {
    return deepCloneRenamed(out_arena, e, &.{});
}

/// deepClone with column-reference substitution (CASE branch conditions
/// included): used to push an expression through a renaming projection,
/// where every ref must re-bind to the source column's label.
pub fn deepCloneRenamed(out_arena: Allocator, e: Expr, renames: []const predicate_mod.ColRename) Allocator.Error!Expr {
    return switch (e) {
        .col_ref => |name| .{ .col_ref = try out_arena.dupe(u8, predicate_mod.renameOf(renames, name)) },
        .lit => |v| .{ .lit = try cloneValue(out_arena, v) },
        .null_lit => |t| .{ .null_lit = t },
        .call => |c| blk: {
            const name_dup = try out_arena.dupe(u8, c.fn_name);
            const args_dup = try out_arena.alloc(Expr, c.args.len);
            for (c.args, 0..) |child, i| args_dup[i] = try deepCloneRenamed(out_arena, child, renames);
            break :blk .{ .call = .{ .fn_name = name_dup, .args = args_dup, .from_statement = c.from_statement } };
        },
        .case => |cs| blk: {
            const branches_dup = try out_arena.alloc(Expr.Branch, cs.branches.len);
            for (cs.branches, 0..) |br, i| branches_dup[i] = .{
                .cond = try predicate_mod.deepClonePredicateRenamed(out_arena, br.cond, renames),
                .then = try deepCloneRenamed(out_arena, br.then, renames),
            };
            var else_dup: ?*const Expr = null;
            if (cs.else_branch) |eb| {
                const eb_owned = try out_arena.create(Expr);
                eb_owned.* = try deepCloneRenamed(out_arena, eb.*, renames);
                else_dup = eb_owned;
            }
            const operands_dup = try out_arena.alloc(Expr.Operand, cs.operands.len);
            for (cs.operands, operands_dup) |o, *dst| dst.* = .{
                .name = try out_arena.dupe(u8, o.name),
                .expr = try deepCloneRenamed(out_arena, o.expr, renames),
            };
            break :blk .{ .case = .{ .branches = branches_dup, .else_branch = else_dup, .operands = operands_dup } };
        },
        // Opaque pointer aliased — the IR arena owns the pointee.
        .scalar_subquery => |p| .{ .scalar_subquery = p },
        .exists_subquery => |p| .{ .exists_subquery = p },
        .var_ref => |name| .{ .var_ref = try out_arena.dupe(u8, name) },
    };
}

/// Structural equality: the same tree, with column references and
/// function names matched case-insensitively and literals by value.
/// Subquery nodes never compare equal — each is its own evaluation.
pub fn eql(a: Expr, b: Expr) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .col_ref => |name| types.columnNameEql(name, b.col_ref),
        .lit => |v| v.eql(b.lit),
        .null_lit => |t| std.meta.eql(t, b.null_lit),
        .call => |c| blk: {
            const o = b.call;
            if (c.args.len != o.args.len or !std.ascii.eqlIgnoreCase(c.fn_name, o.fn_name)) break :blk false;
            for (c.args, o.args) |x, y| if (!eql(x, y)) break :blk false;
            break :blk true;
        },
        .case => |cs| blk: {
            const o = b.case;
            if (cs.branches.len != o.branches.len) break :blk false;
            if ((cs.else_branch == null) != (o.else_branch == null)) break :blk false;
            for (cs.branches, o.branches) |x, y| {
                if (!predicate_mod.eql(x.cond, y.cond) or !eql(x.then, y.then)) break :blk false;
            }
            if (cs.else_branch) |eb| if (!eql(eb.*, o.else_branch.?.*)) break :blk false;
            if (cs.operands.len != o.operands.len) break :blk false;
            for (cs.operands, o.operands) |x, y| {
                if (!types.columnNameEql(x.name, y.name) or !eql(x.expr, y.expr)) break :blk false;
            }
            break :blk true;
        },
        .scalar_subquery, .exists_subquery => false,
        .var_ref => |name| std.mem.eql(u8, name, b.var_ref),
    };
}

/// A decimal constant as the tree carries it: its digits cast to its own
/// type, `to_decimal:<p>:<s>('<digits>')`. A decimal Value holds only a
/// mantissa, so the digits keep the scale. The parser types a fractional
/// literal this way (`1.10` is DECIMAL(3,2)), subquery resolution re-enters
/// a decimal result this way, and Compute folds it into one typed constant.
pub const DecimalLiteral = struct {
    digits: []const u8,
    p: u8,
    s: u8,
};

pub fn decimalLiteral(e: Expr) ?DecimalLiteral {
    const c = switch (e) {
        .call => |c| c,
        else => return null,
    };
    if (c.args.len != 1) return null;
    const digits = switch (c.args[0]) {
        .lit => |v| switch (v) {
            .text => |t| t,
            else => return null,
        },
        else => return null,
    };
    var it = std.mem.splitScalar(u8, c.fn_name, ':');
    if (!std.mem.eql(u8, it.next() orelse return null, "to_decimal")) return null;
    const p = std.fmt.parseInt(u8, it.next() orelse return null, 10) catch return null;
    const s = std.fmt.parseInt(u8, it.next() orelse return null, 10) catch return null;
    if (it.next() != null) return null;
    return .{ .digits = digits, .p = p, .s = s };
}

pub fn decimalLiteralExpr(arena: Allocator, digits: []const u8, p: u8, s: u8) Allocator.Error!Expr {
    const args = try arena.alloc(Expr, 1);
    args[0] = .{ .lit = .{ .text = try arena.dupe(u8, digits) } };
    return .{ .call = .{ .fn_name = try std.fmt.allocPrint(arena, "to_decimal:{d}:{d}", .{ p, s }), .args = args } };
}

/// The value of a literal operand for a consumer that takes one where a
/// DOUBLE serves, such as a percentile or a session variable. A decimal
/// constant gives the double nearest its digits, except a whole number (an
/// integer literal past BIGINT), which is LARGEINT: its double would drop
/// digits past 2^53, and a LARGEINT meets any numeric column exactly. A
/// comparison takes `exactLiteralValue` instead.
pub fn literalValue(e: Expr) ?Value {
    if (e == .lit) return e.lit;
    if (hexLiteralBytes(e)) |bytes| return .{ .text = bytes };
    const d = decimalLiteral(e) orelse return null;
    if (d.s == 0) if (std.fmt.parseInt(i128, d.digits, 10)) |v| return .{ .largeint = v } else |_| {};
    return .{ .double = std.fmt.parseFloat(f64, d.digits) catch return null };
}

/// MySQL's hex literal (`0x41`, `X'41'`) as the tree carries it,
/// `__hex_literal('<bytes>')`. It is its bytes where text is read and its
/// big-endian integer where a number is (`0x41 = 65`, `0x41 + 0`), and
/// which applies turns on where it lands: Compute decides when it plans the
/// call reading it, a comparison when it meets the other side.
pub const HEX_LITERAL_FN = "__hex_literal";

/// Internal: `__hex_literal_as(x, '<bytes>')`, a hex literal read as a
/// comparison with `x` reads it: its integer when `x` is a number, else its
/// bytes.
pub const HEX_LITERAL_AS_FN = "__hex_literal_as";

pub fn hexLiteralExpr(arena: Allocator, bytes: []const u8) Allocator.Error!Expr {
    const args = try arena.alloc(Expr, 1);
    args[0] = .{ .lit = .{ .text = try arena.dupe(u8, bytes) } };
    return .{ .call = .{ .fn_name = HEX_LITERAL_FN, .args = args } };
}

pub fn hexLiteralBytes(e: Expr) ?[]const u8 {
    const c = switch (e) {
        .call => |c| c,
        else => return null,
    };
    if (c.args.len != 1 or !std.mem.eql(u8, c.fn_name, HEX_LITERAL_FN)) return null;
    return switch (c.args[0]) {
        .lit => |v| switch (v) {
            .text => |t| t,
            else => null,
        },
        else => null,
    };
}

pub fn hexLiteralAsExpr(arena: Allocator, other: Expr, bytes: []const u8) Allocator.Error!Expr {
    const args = try arena.alloc(Expr, 2);
    args[0] = other;
    args[1] = .{ .lit = .{ .text = try arena.dupe(u8, bytes) } };
    return .{ .call = .{ .fn_name = HEX_LITERAL_AS_FN, .args = args } };
}

/// A hex literal's bytes as the unsigned big-endian integer MySQL reads in a
/// numeric context. Past eight bytes it reads as 0, as in MySQL.
pub fn hexNumber(bytes: []const u8) u64 {
    if (bytes.len > 8) return 0;
    var n: u64 = 0;
    for (bytes) |b| n = n << 8 | b;
    return n;
}

/// A hex literal's integer is BIGINT UNSIGNED in MySQL, held in a LARGEINT
/// at any size, so arithmetic past 2^63 - 1 stays positive
/// (`0x7FFFFFFFFFFFFFFF + 1`), as a bit operator's result does.
pub fn hexLiteralNumber(bytes: []const u8) Value {
    return .{ .largeint = hexNumber(bytes) };
}

/// A hex literal stored in a column of type `ty`, as MySQL stores one: its
/// integer in a numeric column (`0x3132` stores 12594 in an INT), its bytes
/// in any other.
pub fn hexStoredValue(bytes: []const u8, ty: Type) Value {
    return if (storesHexAsNumber(ty)) hexLiteralNumber(bytes) else .{ .text = bytes };
}

pub fn storesHexAsNumber(ty: Type) bool {
    return ty.isInteger() or ty.isFloat() or ty.isDecimal() or ty == .boolean;
}

/// A literal operand's value for a comparison leaf, or null when no Value
/// holds it exactly. A leaf places a double on a decimal or integer column
/// by its shortest digits, so a fraction is its DOUBLE only when those are
/// the literal's own digits: always for up to 15 significant digits. A
/// longer one (`123456789012345678.5`) stays the decimal constant it is,
/// which the comparison evaluates exactly.
pub fn exactLiteralValue(e: Expr) ?Value {
    const v = literalValue(e) orelse return null;
    if (e == .lit or v != .double) return v;
    return if (doubleHoldsDigits(decimalLiteral(e).?.digits, v.double)) v else null;
}

/// Whether a fractional literal token (`text`, lexed as `value`) keeps its
/// value as a DOUBLE: an exponent form or one past 38 digits is a DOUBLE
/// literal, and a plain decimal one is when the double holds its digits.
pub fn fractionFitsDouble(text: []const u8, value: f64) bool {
    if (std.mem.indexOfScalar(u8, text, '.') == null or std.mem.indexOfAny(u8, text, "eE") != null or text.len - 1 > 38) return true;
    return doubleHoldsDigits(text, value);
}

fn doubleHoldsDigits(digits: []const u8, v: f64) bool {
    const exact = switch (scalar_fn_common.textNumber(digits) orelse return false) {
        .exact => |x| x,
        .float => return false,
    };
    const shortest = scalar_fn_common.floatDigits(v) orelse return false;
    return std.meta.eql(withoutTrailingZeros(exact), withoutTrailingZeros(shortest));
}

fn withoutTrailingZeros(x: scalar_fn_common.ScaledInt) scalar_fn_common.ScaledInt {
    var out = x;
    while (out.s > 0 and @rem(out.m, 10) == 0) {
        out.m = @divTrunc(out.m, 10);
        out.s -= 1;
    }
    return out;
}

fn cloneValue(out_arena: Allocator, v: Value) Allocator.Error!Value {
    return switch (v) {
        .text => |s| .{ .text = try out_arena.dupe(u8, s) },
        else => v,
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "expr: col + lit + call construction" {
    const e = Expr{ .call = .{
        .fn_name = "upper",
        .args = &.{col("name")},
    } };
    try std.testing.expect(e == .call);
    try std.testing.expectEqualStrings("upper", e.call.fn_name);
    try std.testing.expect(e.call.args[0] == .col_ref);
    try std.testing.expectEqualStrings("name", e.call.args[0].col_ref);
}

test "expr: eql is structural, case-insensitive on identifiers, exact on literals" {
    const a = Expr{ .call = .{ .fn_name = "lower", .args = &.{col("Name")} } };
    const b = Expr{ .call = .{ .fn_name = "LOWER", .args = &.{col("name")} } };
    try std.testing.expect(eql(a, b));
    try std.testing.expect(!eql(a, Expr{ .call = .{ .fn_name = "upper", .args = &.{col("name")} } }));
    try std.testing.expect(eql(lit(.{ .text = "x" }), lit(.{ .text = "x" })));
    try std.testing.expect(!eql(lit(.{ .text = "x" }), lit(.{ .text = "X" })));
    try std.testing.expect(!eql(lit(.{ .int = 1 }), lit(.{ .bigint = 1 })));

    const cond = PredicateExpr{ .leaf = .{ .col = "qty", .op = .gt, .val = .{ .int = 1 } } };
    const cond_other = PredicateExpr{ .leaf = .{ .col = "qty", .op = .gte, .val = .{ .int = 1 } } };
    const case_a = Expr{ .case = .{ .branches = &.{.{ .cond = cond, .then = lit(.{ .text = "big" }) }}, .else_branch = null } };
    const case_b = Expr{ .case = .{ .branches = &.{.{ .cond = cond, .then = lit(.{ .text = "big" }) }}, .else_branch = null } };
    const case_c = Expr{ .case = .{ .branches = &.{.{ .cond = cond_other, .then = lit(.{ .text = "big" }) }}, .else_branch = null } };
    const else_lit = lit(.{ .text = "small" });
    const case_d = Expr{ .case = .{ .branches = &.{.{ .cond = cond, .then = lit(.{ .text = "big" }) }}, .else_branch = &else_lit } };
    try std.testing.expect(eql(case_a, case_b));
    try std.testing.expect(!eql(case_a, case_c));
    try std.testing.expect(!eql(case_a, case_d));
}

test "expr: deepClone produces an owned tree" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const source_name = try std.testing.allocator.dupe(u8, "name");
    defer std.testing.allocator.free(source_name);

    const orig = Expr{ .call = .{
        .fn_name = "upper",
        .args = &.{Expr{ .col_ref = source_name }},
    } };
    const cloned = try deepClone(arena.allocator(), orig);

    try std.testing.expectEqualStrings("upper", cloned.call.fn_name);
    try std.testing.expectEqualStrings("name", cloned.call.args[0].col_ref);
    // The clone's strings live in the arena, not in the originals.
    try std.testing.expect(cloned.call.fn_name.ptr != orig.call.fn_name.ptr);
}

test "expr: a hex literal reads as its big-endian BIGINT UNSIGNED, 0 past eight bytes" {
    const cases = .{
        .{ "A", 65 },
        .{ "12", 12594 },
        .{ "", 0 },
        .{ "\x01\x02\x03\x04\x05\x06\x07\x08", 72623859790382856 },
        .{ "\x7f\xff\xff\xff\xff\xff\xff\xff", std.math.maxInt(i64) },
        .{ "\xff\xff\xff\xff\xff\xff\xff\xff", std.math.maxInt(u64) },
        .{ "ABCDEFGHI", 0 },
    };
    inline for (cases) |c| try std.testing.expectEqual(Value{ .largeint = c[1] }, hexLiteralNumber(c[0]));

    const stored = .{
        .{ Type.int, Value{ .largeint = 12594 } },
        .{ Type.double, Value{ .largeint = 12594 } },
        .{ Type.boolean, Value{ .largeint = 12594 } },
        .{ Type.string, Value{ .text = "12" } },
        .{ Type.date, Value{ .text = "12" } },
    };
    inline for (stored) |c| try std.testing.expectEqualDeep(c[1], hexStoredValue("12", c[0]));

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const e = try hexLiteralExpr(arena.allocator(), "A");
    try std.testing.expectEqualStrings("A", hexLiteralBytes(e).?);
    try std.testing.expectEqualStrings("A", literalValue(e).?.text);
    try std.testing.expect(hexLiteralBytes(.{ .lit = .{ .text = "A" } }) == null);
}
