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
const Value = types.Value;
const Type = types.Type;

const predicate_mod = @import("predicate.zig");
const PredicateExpr = predicate_mod.PredicateExpr;

pub const Expr = union(enum) {
    /// Reference to an upstream column by name.
    col_ref: []const u8,
    /// Constant value. Type comes from the active union tag.
    lit: Value,
    /// Function invocation. `fn_name` is matched against the registry
    /// at plan time. `args` may be empty (for nullary functions).
    call: Call,
    /// SQL searched CASE expression. Branches evaluated in order;
    /// first branch whose `cond` is true contributes its `then`
    /// expression to the row. When no branch matches the optional
    /// `else_branch` wins (NULL if absent). All branch `then` results
    /// must resolve to the same type.
    case: Case,

    pub const Call = struct {
        fn_name: []const u8,
        args: []const Expr,
    };

    pub const Branch = struct {
        cond: PredicateExpr,
        then: Expr,
    };

    pub const Case = struct {
        branches: []const Branch,
        else_branch: ?*const Expr,
    };
};

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
    return switch (e) {
        .col_ref => |name| .{ .col_ref = try out_arena.dupe(u8, name) },
        .lit => |v| .{ .lit = try cloneValue(out_arena, v) },
        .call => |c| blk: {
            const name_dup = try out_arena.dupe(u8, c.fn_name);
            const args_dup = try out_arena.alloc(Expr, c.args.len);
            for (c.args, 0..) |child, i| args_dup[i] = try deepClone(out_arena, child);
            break :blk .{ .call = .{ .fn_name = name_dup, .args = args_dup } };
        },
        .case => |cs| blk: {
            const branches_dup = try out_arena.alloc(Expr.Branch, cs.branches.len);
            for (cs.branches, 0..) |br, i| branches_dup[i] = .{
                .cond = try predicate_mod.deepClonePredicate(out_arena, br.cond),
                .then = try deepClone(out_arena, br.then),
            };
            var else_dup: ?*const Expr = null;
            if (cs.else_branch) |eb| {
                const eb_owned = try out_arena.create(Expr);
                eb_owned.* = try deepClone(out_arena, eb.*);
                else_dup = eb_owned;
            }
            break :blk .{ .case = .{ .branches = branches_dup, .else_branch = else_dup } };
        },
    };
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
