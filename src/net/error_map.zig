//! Maps thinDB internal errors to a canonical category that each wire
//! protocol's error encoder translates into its native code/sqlstate.

const std = @import("std");

pub const Category = enum {
    table_not_found,
    table_already_exists,
    database_not_found,
    database_already_exists,
    schema_not_found,
    schema_already_exists,
    column_not_found,
    ambiguous_column,
    query_cancelled,
    numeric_out_of_range,
    unknown,
};

pub fn classify(err_name: []const u8) Category {
    if (std.mem.eql(u8, err_name, "TableNotFound")) return .table_not_found;
    if (std.mem.eql(u8, err_name, "TableAlreadyExists")) return .table_already_exists;
    if (std.mem.eql(u8, err_name, "DatabaseNotFound")) return .database_not_found;
    if (std.mem.eql(u8, err_name, "DatabaseAlreadyExists")) return .database_already_exists;
    if (std.mem.eql(u8, err_name, "SchemaNotFound")) return .schema_not_found;
    if (std.mem.eql(u8, err_name, "SchemaAlreadyExists")) return .schema_already_exists;
    if (std.mem.eql(u8, err_name, "ColumnNotFound")) return .column_not_found;
    if (std.mem.eql(u8, err_name, "SqlOnColumnAmbiguous")) return .ambiguous_column;
    if (std.mem.eql(u8, err_name, "QueryCancelled")) return .query_cancelled;
    if (std.mem.eql(u8, err_name, "ArithmeticOverflow")) return .numeric_out_of_range;
    return .unknown;
}

test "classify recognizes known errors" {
    try std.testing.expectEqual(Category.table_not_found, classify("TableNotFound"));
    try std.testing.expectEqual(Category.database_already_exists, classify("DatabaseAlreadyExists"));
    try std.testing.expectEqual(Category.query_cancelled, classify("QueryCancelled"));
    try std.testing.expectEqual(Category.numeric_out_of_range, classify("ArithmeticOverflow"));
    try std.testing.expectEqual(Category.ambiguous_column, classify("SqlOnColumnAmbiguous"));
    try std.testing.expectEqual(Category.unknown, classify("NotARealError"));
}
