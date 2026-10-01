//! Error-code mapping from thinDB's internal error set to MySQL wire codes.

const std = @import("std");
const error_map = @import("../error_map.zig");
const predicate = @import("../../exec/predicate.zig");
const types = @import("../../types.zig");

pub const Mapped = struct {
    code: u16,
    sqlstate: [5]u8,
    message: []const u8,
};

/// Map any internal error to a MySQL (code, sqlstate, message) triple, the
/// code MySQL 8.4 gives the same class of failure (issue #490). Only the
/// parser's own errors are 1064 (ER_PARSE_ERROR); an error nothing here
/// recognizes is 1105 (ER_UNKNOWN_ERROR), so a client never reads an
/// execution failure as a syntax error. `fallback_msg` is that 1105's
/// message; null uses `@errorName(err)`.
pub fn mapInternal(err: anyerror, fallback_msg: ?[]const u8) Mapped {
    return switch (error_map.classify(@errorName(err))) {
        .table_not_found => .{ .code = 1146, .sqlstate = "42S02".*, .message = "Table not found" },
        .database_not_found => .{ .code = 1049, .sqlstate = "42000".*, .message = "Unknown database" },
        .database_already_exists => .{ .code = 1007, .sqlstate = "HY000".*, .message = "Database exists" },
        .invalid_database_name => .{ .code = 1102, .sqlstate = "42000".*, .message = "Incorrect database name" },
        .no_database_selected => .{ .code = 1046, .sqlstate = "3D000".*, .message = "No database selected" },
        .schema_not_found => .{ .code = 1146, .sqlstate = "42S02".*, .message = "Schema not found" },
        .schema_already_exists => .{ .code = 1050, .sqlstate = "42S01".*, .message = "Schema exists" },
        .table_already_exists => .{ .code = 1050, .sqlstate = "42S01".*, .message = "Table exists" },
        .column_not_found => .{ .code = 1054, .sqlstate = "42S22".*, .message = "Unknown column" },
        .ambiguous_column => .{ .code = 1052, .sqlstate = "23000".*, .message = "Column in on clause is ambiguous" },
        .query_cancelled => .{ .code = 1317, .sqlstate = "70100".*, .message = "Query execution was interrupted" },
        .numeric_out_of_range => .{ .code = 1690, .sqlstate = "22003".*, .message = "Numeric value out of range" },
        .value_out_of_range => .{ .code = 1264, .sqlstate = "22003".*, .message = "Out of range value for column" },
        .subquery_multiple_rows => .{ .code = 1242, .sqlstate = "21000".*, .message = "Subquery returns more than 1 row" },
        .recursion_depth_exceeded => .{ .code = 3636, .sqlstate = "HY000".*, .message = "Recursive query aborted after 1001 iterations. Try increasing @@cte_max_recursion_depth to a larger value." },
        .invalid_temporal_literal => .{ .code = 1525, .sqlstate = "HY000".*, .message = predicate.takeInvalidTemporalMessage() orelse "Incorrect DATE or DATETIME value" },
        .wrong_arguments => .{ .code = 1210, .sqlstate = "HY000".*, .message = @errorName(err) },
        // ER_CAPACITY_EXCEEDED: MySQL's error for a statement past a
        // configured memory cap, such as parser_max_mem_size.
        .memory_budget_exceeded => .{ .code = 3170, .sqlstate = "HY000".*, .message = @errorName(err) },
        .out_of_memory => .{ .code = 1037, .sqlstate = "HY001".*, .message = @errorName(err) },
        .not_supported => .{ .code = 1235, .sqlstate = "42000".*, .message = @errorName(err) },
        .syntax_error => .{ .code = 1064, .sqlstate = "42000".*, .message = @errorName(err) },
        .unknown => .{ .code = 1105, .sqlstate = "HY000".*, .message = fallback_msg orelse @errorName(err) },
    };
}

test "mapInternal recognizes catalog errors" {
    const m = mapInternal(error.TableNotFound, "x");
    try std.testing.expectEqual(@as(u16, 1146), m.code);
    try std.testing.expectEqualStrings("42S02", &m.sqlstate);
}

test "mapInternal reports a missing current database and a reserved name as MySQL does" {
    const none = mapInternal(error.NoDatabaseSelected, null);
    try std.testing.expectEqual(@as(u16, 1046), none.code);
    try std.testing.expectEqualStrings("3D000", &none.sqlstate);
    try std.testing.expectEqualStrings("No database selected", none.message);
    const reserved = mapInternal(error.InvalidDatabaseName, null);
    try std.testing.expectEqual(@as(u16, 1102), reserved.code);
    try std.testing.expectEqualStrings("42000", &reserved.sqlstate);
}

test "mapInternal gives each runtime class MySQL 8.4's code, and only parse errors 1064 (issue #490)" {
    const fixed_message = .{
        .{ error.QueryCancelled, 1317, "70100" },
        .{ error.ArithmeticOverflow, 1690, "22003" },
        .{ error.ValueOutOfRange, 1264, "22003" },
        .{ error.NumericOverflow, 1264, "22003" },
    };
    inline for (fixed_message) |c| {
        const m = mapInternal(c[0], null);
        try std.testing.expectEqual(@as(u16, c[1]), m.code);
        try std.testing.expectEqualStrings(c[2], &m.sqlstate);
    }
    const named = .{
        .{ error.MemoryBudgetExceeded, 3170, "HY000" },
        .{ error.OutOfMemory, 1037, "HY001" },
        .{ error.UnsupportedQueryShape, 1235, "42000" },
        .{ error.SqlExpectedFrom, 1064, "42000" },
        .{ error.LexUnterminatedString, 1064, "42000" },
        .{ error.IncorrectArgumentsToSleep, 1210, "HY000" },
        .{ error.SqlFoundRowsUnsupported, 1235, "42000" },
        .{ error.TypeMismatch, 1105, "HY000" },
        .{ error.JsonPathInvalid, 1105, "HY000" },
    };
    inline for (named) |c| {
        const m = mapInternal(c[0], null);
        try std.testing.expectEqual(@as(u16, c[1]), m.code);
        try std.testing.expectEqualStrings(c[2], &m.sqlstate);
        try std.testing.expectEqualStrings(@errorName(c[0]), m.message);
    }
}

test "mapInternal names the constant a DATE comparison rejected" {
    const schema = [_]types.Column{.{ .name = "d", .type = .date, .nullable = true }};
    var expr: predicate.PredicateExpr = .{ .leaf = .{ .col = "d", .op = .lt, .val = .{ .text = "2026-09-31" }, .from_statement = true } };
    try std.testing.expectError(error.InvalidTemporalLiteral, predicate.validateExpr(&expr, &schema));
    const m = mapInternal(error.InvalidTemporalLiteral, null);
    try std.testing.expectEqual(@as(u16, 1525), m.code);
    try std.testing.expectEqualStrings("HY000", &m.sqlstate);
    try std.testing.expectEqualStrings("Incorrect DATE value: '2026-09-31'", m.message);
    try std.testing.expectEqualStrings("Incorrect DATE or DATETIME value", mapInternal(error.InvalidTemporalLiteral, null).message);
}

test "mapInternal falls back to 1105 with the caller's message or the error's name" {
    const m = mapInternal(error.NotARealError, "fallback");
    try std.testing.expectEqual(@as(u16, 1105), m.code);
    try std.testing.expectEqualStrings("HY000", &m.sqlstate);
    try std.testing.expectEqualStrings("fallback", m.message);
    try std.testing.expectEqualStrings("NotARealError", mapInternal(error.NotARealError, null).message);
}
