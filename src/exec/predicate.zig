//! Predicate type system + leaf evaluation.
//!
//! `PredicateExpr` is the boolean expression tree consumed by `Filter`.
//! `evaluateMaskWithPred` is the per-leaf row-mask kernel. `validateExpr`
//! type-checks an expression against a schema before evaluation.
//! `statsOverlapPredicate` is the row-group prune helper used by `Scan` and
//! by `Table.delete`.

const std = @import("std");

const types = @import("../types.zig");
const Column = types.Column;
const Value = types.Value;
const ValueTag = types.ValueTag;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const exec = @import("exec.zig");
const Error = exec.Error;

pub const PredicateOp = enum { eq, neq, lt, lte, gt, gte };

pub const Predicate = struct {
    col: []const u8,
    op: PredicateOp,
    val: Value,
};

/// Boolean expression over Predicates.
///
///   - `.leaf`       — a single column-op-value comparison
///   - `.is_null`    — column value is NULL
///   - `.is_not_null`— column value is non-NULL
///   - `.@"and"`     — all children must match
///   - `.@"or"`      — at least one child must match
///   - `.not`        — child must NOT match
pub const PredicateExpr = union(enum) {
    leaf: Predicate,
    is_null: []const u8,
    is_not_null: []const u8,
    @"and": []const PredicateExpr,
    @"or": []const PredicateExpr,
    not: *const PredicateExpr,
};

/// Build a leaf predicate expression. Shorthand for `.{ .leaf = ... }`.
pub fn leafExpr(col: []const u8, op: PredicateOp, val: Value) PredicateExpr {
    return .{ .leaf = .{ .col = col, .op = op, .val = val } };
}

pub fn isNullExpr(col: []const u8) PredicateExpr {
    return .{ .is_null = col };
}

pub fn isNotNullExpr(col: []const u8) PredicateExpr {
    return .{ .is_not_null = col };
}

/// Deep-clone a PredicateExpr into `out_arena`. Mirrors
/// `expr.deepClone` for the boolean side of a CASE/WHERE expression;
/// used when an Expr.case needs to outlive its source arena.
pub fn deepClonePredicate(out_arena: std.mem.Allocator, p: PredicateExpr) std.mem.Allocator.Error!PredicateExpr {
    return switch (p) {
        .leaf => |lf| .{ .leaf = .{
            .col = try out_arena.dupe(u8, lf.col),
            .op = lf.op,
            .val = try cloneValue(out_arena, lf.val),
        } },
        .is_null => |c| .{ .is_null = try out_arena.dupe(u8, c) },
        .is_not_null => |c| .{ .is_not_null = try out_arena.dupe(u8, c) },
        .@"and" => |kids| blk: {
            const dup = try out_arena.alloc(PredicateExpr, kids.len);
            for (kids, 0..) |k, i| dup[i] = try deepClonePredicate(out_arena, k);
            break :blk .{ .@"and" = dup };
        },
        .@"or" => |kids| blk: {
            const dup = try out_arena.alloc(PredicateExpr, kids.len);
            for (kids, 0..) |k, i| dup[i] = try deepClonePredicate(out_arena, k);
            break :blk .{ .@"or" = dup };
        },
        .not => |child| blk: {
            const dup = try out_arena.create(PredicateExpr);
            dup.* = try deepClonePredicate(out_arena, child.*);
            break :blk .{ .not = dup };
        },
    };
}

fn cloneValue(out_arena: std.mem.Allocator, v: Value) std.mem.Allocator.Error!Value {
    return switch (v) {
        .text => |s| .{ .text = try out_arena.dupe(u8, s) },
        else => v,
    };
}

/// Type-check a PredicateExpr against a schema. Every leaf must reference an
/// existing column with a value-tag matching that column's type. String
/// columns only accept `.eq` and `.neq`.
///
/// Performs lossless integer-literal widening when the column type is
/// wider than the literal (e.g. column BIGINT, literal `.int` → mutate to
/// `.bigint`). Pure narrowing isn't done — we don't want silent data
/// loss in the predicate semantics.
pub fn validateExpr(expr: *PredicateExpr, schema: []const Column) !void {
    switch (expr.*) {
        .leaf => |*p| {
            const col_idx = blk: {
                for (schema, 0..) |c, i| {
                    if (std.mem.eql(u8, c.name, p.col)) break :blk i;
                }
                return Error.ColumnNotFound;
            };
            const col_type = schema[col_idx].type;
            const col_tag = ValueTag.fromType(col_type);
            const val_tag = std.meta.activeTag(p.val);
            if (col_tag != val_tag) {
                tryWidenLiteral(&p.val, col_tag) catch return Error.PredicateTypeMismatch;
            }
            if (col_type.isString() and p.op != .eq and p.op != .neq) {
                return Error.UnsupportedOperatorForType;
            }
        },
        .is_null, .is_not_null => |col_name| {
            for (schema) |c| {
                if (std.mem.eql(u8, c.name, col_name)) return;
            }
            return Error.ColumnNotFound;
        },
        .@"and" => |children| {
            for (children) |*c| try validateExpr(@constCast(c), schema);
        },
        .@"or" => |children| {
            for (children) |*c| try validateExpr(@constCast(c), schema);
        },
        .not => |child| try validateExpr(@constCast(child), schema),
    }
}

/// Lossless widening for an integer / float literal to match a wider
/// column type. Errors when the source literal can't be losslessly
/// represented in the target type (caller treats that as a type
/// mismatch).
fn tryWidenLiteral(val: *Value, target: ValueTag) error{NoWidening}!void {
    switch (val.*) {
        .tinyint => |v| switch (target) {
            .smallint => val.* = .{ .smallint = v },
            .int => val.* = .{ .int = v },
            .bigint => val.* = .{ .bigint = v },
            .largeint => val.* = .{ .largeint = v },
            else => return error.NoWidening,
        },
        .smallint => |v| switch (target) {
            .int => val.* = .{ .int = v },
            .bigint => val.* = .{ .bigint = v },
            .largeint => val.* = .{ .largeint = v },
            else => return error.NoWidening,
        },
        .int => |v| switch (target) {
            .bigint => val.* = .{ .bigint = v },
            .largeint => val.* = .{ .largeint = v },
            else => return error.NoWidening,
        },
        .bigint => |v| switch (target) {
            .largeint => val.* = .{ .largeint = v },
            else => return error.NoWidening,
        },
        .float => |v| switch (target) {
            .double => val.* = .{ .double = v },
            else => return error.NoWidening,
        },
        else => return error.NoWidening,
    }
}

/// Push every leaf reachable through top-level ANDs down to the upstream so
/// Scan can use them for row-group min/max pruning. OR/NOT branches are
/// skipped — they don't have monotonic stats overlap semantics.
pub fn pushExprDown(upstream: *exec.Query, expr: PredicateExpr) !void {
    switch (expr) {
        .leaf => |p| {
            upstream.addPrune(p) catch |err| switch (err) {
                error.ColumnNotFound => {},
                else => return err,
            };
        },
        .@"and" => |children| {
            for (children) |c| try pushExprDown(upstream, c);
        },
        else => {},
    }
}

/// Evaluate a full boolean predicate over a Batch (typed columns +
/// nulls + AND/OR/NOT). Writes per-row match bits into `out`. The
/// caller supplies an allocator for AND/OR scratch (one per recursive
/// level). NULL never matches a comparison; `IS NULL` / `IS NOT NULL`
/// inspect the validity bitmap.
pub fn evaluatePredicate(
    allocator: std.mem.Allocator,
    expr: PredicateExpr,
    schema: []const Column,
    batch: anytype,
    out: []bool,
) anyerror!void {
    switch (expr) {
        .leaf => |p| {
            const col_idx = blk: {
                for (schema, 0..) |c, i| {
                    if (std.mem.eql(u8, c.name, p.col)) break :blk i;
                }
                return Error.ColumnNotFound;
            };
            try evaluateMaskWithPred(batch.values[col_idx], p, batch.row_count, out);
            // Two-valued logic: NULL never matches a comparison.
            const view = batch.values[col_idx];
            if (view.nulls != null) {
                for (0..batch.row_count) |i| {
                    if (!view.isValid(i)) out[i] = false;
                }
            }
        },
        .is_null => |col_name| {
            const col_idx = blk: {
                for (schema, 0..) |c, i| {
                    if (std.mem.eql(u8, c.name, col_name)) break :blk i;
                }
                return Error.ColumnNotFound;
            };
            const view = batch.values[col_idx];
            for (0..batch.row_count) |i| out[i] = !view.isValid(i);
        },
        .is_not_null => |col_name| {
            const col_idx = blk: {
                for (schema, 0..) |c, i| {
                    if (std.mem.eql(u8, c.name, col_name)) break :blk i;
                }
                return Error.ColumnNotFound;
            };
            const view = batch.values[col_idx];
            for (0..batch.row_count) |i| out[i] = view.isValid(i);
        },
        .@"and" => |children| {
            if (children.len == 0) {
                @memset(out, true);
                return;
            }
            try evaluatePredicate(allocator, children[0], schema, batch, out);
            if (children.len == 1) return;
            const scratch = try allocator.alloc(bool, out.len);
            defer allocator.free(scratch);
            for (children[1..]) |child| {
                try evaluatePredicate(allocator, child, schema, batch, scratch);
                for (out, scratch) |*o, s| o.* = o.* and s;
            }
        },
        .@"or" => |children| {
            if (children.len == 0) {
                @memset(out, false);
                return;
            }
            try evaluatePredicate(allocator, children[0], schema, batch, out);
            if (children.len == 1) return;
            const scratch = try allocator.alloc(bool, out.len);
            defer allocator.free(scratch);
            for (children[1..]) |child| {
                try evaluatePredicate(allocator, child, schema, batch, scratch);
                for (out, scratch) |*o, s| o.* = o.* or s;
            }
        },
        .not => |child| {
            try evaluatePredicate(allocator, child.*, schema, batch, out);
            for (out) |*o| o.* = !o.*;
        },
    }
}

/// Evaluate a single leaf predicate against a column view, writing per-row
/// match bits into `mask`. Two-valued logic: NULL never matches.
pub fn evaluateMaskWithPred(view: ColumnView, p: Predicate, n: usize, mask: []bool) !void {
    const op = p.op;
    switch (view.data) {
        .int => |s| {
            const want = p.val.int;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i32, v, want, op);
        },
        .bigint => |s| {
            const want = p.val.bigint;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i64, v, want, op);
        },
        .boolean => |s| {
            const want = @intFromBool(p.val.boolean);
            for (s[0..n], 0..) |v, i| mask[i] = cmp(u8, v, want, op);
        },
        .varchar => |sv| {
            if (op != .eq and op != .neq) return Error.UnsupportedOperatorForType;
            for (0..n) |i| {
                const eq = std.mem.eql(u8, sv.rowBytes(i), p.val.text);
                mask[i] = if (op == .eq) eq else !eq;
            }
        },
        .string => |sv| {
            if (op != .eq and op != .neq) return Error.UnsupportedOperatorForType;
            for (0..n) |i| {
                const eq = std.mem.eql(u8, sv.rowBytes(i), p.val.text);
                mask[i] = if (op == .eq) eq else !eq;
            }
        },
        .float => |s| {
            const want = p.val.float;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(f32, v, want, op);
        },
        .double => |s| {
            const want = p.val.double;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(f64, v, want, op);
        },
        .date => |s| {
            const want = p.val.date;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i32, v, want, op);
        },
        .datetime => |s| {
            const want = p.val.datetime;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i64, v, want, op);
        },
        .tinyint => |s| {
            const want = p.val.tinyint;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i8, v, want, op);
        },
        .smallint => |s| {
            const want = p.val.smallint;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i16, v, want, op);
        },
        .largeint => |s| {
            const want = p.val.largeint;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i128, v, want, op);
        },
        .char => |sv| {
            if (op != .eq and op != .neq) return Error.UnsupportedOperatorForType;
            for (0..n) |i| {
                const eq = std.mem.eql(u8, sv.rowBytes(i), p.val.text);
                mask[i] = if (op == .eq) eq else !eq;
            }
        },
        .decimal64 => |s| {
            const want = p.val.decimal64;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i64, v, want, op);
        },
        .decimal128 => |s| {
            const want = p.val.decimal128;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i128, v, want, op);
        },
        .uuid => |s| {
            const want = p.val.uuid;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(u128, v, want, op);
        },
    }
    // Two-valued logic: a NULL value never matches a comparison.
    if (view.nulls != null) {
        for (0..n) |i| {
            if (!view.isValid(i)) mask[i] = false;
        }
    }
}

fn cmp(comptime T: type, a: T, b: T, op: PredicateOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .lte => a <= b,
        .gt => a > b,
        .gte => a >= b,
    };
}

/// Returns true if the row-group stats could contain rows matching `op val`.
/// Used by Scan and DELETE to decide whether to skip a row group entirely.
///
/// Stats are i128 with per-type encoding (see `format.Stats`). The
/// predicate value is encoded with the same scheme so signed i128
/// comparison gives the right answer for every type.
///
/// String predicates use the 16-byte prefix encoding; range ops are
/// rejected by `validateExpr` so this only handles eq/neq for strings.
/// Tied prefixes (`>16` byte strings sharing the same first 16 bytes)
/// stay on the conservative side — eq keeps the row group, neq never
/// prunes.
pub fn statsOverlapPredicate(s: storage.format.Stats, op: PredicateOp, v: Value) bool {
    const wanted: i128 = switch (v) {
        .int => |x| x,
        .bigint => |x| x,
        .boolean => |x| @intFromBool(x),
        .date => |x| x,
        .datetime => |x| x,
        .tinyint => |x| x,
        .smallint => |x| x,
        .decimal64 => |x| x,
        .largeint, .decimal128 => |x| x,
        .uuid => |x| storage.format.encodeUnsignedU128(x),
        .text => |x| {
            if (op == .neq) return true;
            if (op != .eq) return true;
            const enc = storage.format.encodeStringPrefix(x);
            return enc >= s.min and enc <= s.max;
        },
        // Floats still carry no stats — keep conservatively.
        .float, .double => return true,
    };
    return switch (op) {
        .eq => wanted >= s.min and wanted <= s.max,
        .neq => !(s.min == s.max and s.min == wanted),
        .lt => s.min < wanted,
        .lte => s.min <= wanted,
        .gt => s.max > wanted,
        .gte => s.max >= wanted,
    };
}
