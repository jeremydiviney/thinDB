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

/// Type-check a PredicateExpr against a schema. Every leaf must reference an
/// existing column with a value-tag matching that column's type. String
/// columns only accept `.eq` and `.neq`.
pub fn validateExpr(expr: PredicateExpr, schema: []const Column) !void {
    switch (expr) {
        .leaf => |p| {
            const col_idx = blk: {
                for (schema, 0..) |c, i| {
                    if (std.mem.eql(u8, c.name, p.col)) break :blk i;
                }
                return Error.ColumnNotFound;
            };
            const col_type = schema[col_idx].type;
            if (ValueTag.fromType(col_type) != std.meta.activeTag(p.val)) {
                return Error.PredicateTypeMismatch;
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
            for (children) |c| try validateExpr(c, schema);
        },
        .@"or" => |children| {
            for (children) |c| try validateExpr(c, schema);
        },
        .not => |child| try validateExpr(child.*, schema),
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
pub fn statsOverlapPredicate(s: storage.format.Stats, op: PredicateOp, v: Value) bool {
    const wanted: i64 = switch (v) {
        .int => |x| x,
        .bigint => |x| x,
        .boolean => |x| @intFromBool(x),
        .date => |x| x,
        .datetime => |x| x,
        .tinyint => |x| x,
        .smallint => |x| x,
        // No stats on strings/floats/largeint — can't fit i128 in the i64 stats slot.
        .text, .float, .double, .largeint => return true,
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
