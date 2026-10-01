//! Maps thinDB internal errors to a canonical category that each wire
//! protocol's error encoder translates into its native code/sqlstate.

const std = @import("std");
const sql = @import("../sql/sql.zig");

pub const Category = enum {
    table_not_found,
    table_already_exists,
    database_not_found,
    database_already_exists,
    invalid_database_name,
    no_database_selected,
    schema_not_found,
    schema_already_exists,
    column_not_found,
    ambiguous_column,
    query_cancelled,
    numeric_out_of_range,
    value_out_of_range,
    subquery_multiple_rows,
    recursion_depth_exceeded,
    invalid_temporal_literal,
    /// A function given an argument outside its domain, such as a negative
    /// or NULL SLEEP duration.
    wrong_arguments,
    /// The statement ran past `Config.query_memory_budget`.
    memory_budget_exceeded,
    /// An allocation failed.
    out_of_memory,
    /// A statement form thinDB doesn't run yet, found once it binds.
    not_supported,
    /// An error of the SQL parser's own set (`sql.ParseError`): the
    /// statement text didn't parse, or the parser refused its shape.
    syntax_error,
    /// Anything else, which a wire reports as a general execution failure,
    /// never as a parse error.
    unknown,
};

pub fn classify(err_name: []const u8) Category {
    if (std.mem.eql(u8, err_name, "TableNotFound")) return .table_not_found;
    if (std.mem.eql(u8, err_name, "TableAlreadyExists")) return .table_already_exists;
    if (std.mem.eql(u8, err_name, "DatabaseNotFound")) return .database_not_found;
    if (std.mem.eql(u8, err_name, "DatabaseAlreadyExists")) return .database_already_exists;
    if (std.mem.eql(u8, err_name, "InvalidDatabaseName")) return .invalid_database_name;
    if (std.mem.eql(u8, err_name, "NoDatabaseSelected")) return .no_database_selected;
    if (std.mem.eql(u8, err_name, "SchemaNotFound")) return .schema_not_found;
    if (std.mem.eql(u8, err_name, "SchemaAlreadyExists")) return .schema_already_exists;
    if (std.mem.eql(u8, err_name, "ColumnNotFound")) return .column_not_found;
    if (std.mem.eql(u8, err_name, "SqlOnColumnAmbiguous")) return .ambiguous_column;
    if (std.mem.eql(u8, err_name, "QueryCancelled")) return .query_cancelled;
    if (std.mem.eql(u8, err_name, "ArithmeticOverflow")) return .numeric_out_of_range;
    if (std.mem.eql(u8, err_name, "ValueOutOfRange")) return .value_out_of_range;
    // A value an UPDATE converts into its column's narrower integer type.
    if (std.mem.eql(u8, err_name, "NumericOverflow")) return .value_out_of_range;
    if (std.mem.eql(u8, err_name, "SubqueryMultipleRows")) return .subquery_multiple_rows;
    if (std.mem.eql(u8, err_name, "RecursiveCteDepthExceeded")) return .recursion_depth_exceeded;
    if (std.mem.eql(u8, err_name, "InvalidTemporalLiteral")) return .invalid_temporal_literal;
    if (std.mem.eql(u8, err_name, "IncorrectArgumentsToSleep")) return .wrong_arguments;
    if (std.mem.eql(u8, err_name, "MemoryBudgetExceeded")) return .memory_budget_exceeded;
    if (std.mem.eql(u8, err_name, "OutOfMemory")) return .out_of_memory;
    for (NOT_SUPPORTED) |name| if (std.mem.eql(u8, err_name, name)) return .not_supported;
    if (isParseError(err_name)) return .syntax_error;
    return .unknown;
}

/// Errors that refuse a statement form MySQL would run, so a client reads
/// "not supported yet" rather than a syntax error or a failure. The
/// parser's own refusals are listed too: they parse in MySQL, so they
/// aren't syntax errors. Listed by name because other errors share the
/// "Unsupported" spelling for unrelated reasons (a newer on-disk format
/// version, or an ALTER that names an existing column).
const NOT_SUPPORTED = [_][]const u8{
    "UnsupportedQueryShape",
    "UnsupportedCorrelatedSubquery",
    "WindowUnsupported",
    "JoinUnsupportedType",
    "ComputeUnsupportedExpr",
    "FileScanUnsupportedParquet",
    "CopyFileNotSupported",
    "SqlOnNonEquiUnsupported",
    "SqlCopyFileNotSupported",
    "SqlCopyUnsupportedFormat",
    "SqlUnsupportedFileFunction",
    "SqlUnsupportedFileOption",
    "SqlSetOpAllUnsupported",
    "SqlColumnPositionUnsupported",
    "SqlFoundRowsUnsupported",
    "SqlPrepareExecuteUnsupported",
    "LexCharsetUnsupported",
};

fn isParseError(err_name: []const u8) bool {
    inline for (@typeInfo(sql.ParseError).error_set.?) |e| {
        if (std.mem.eql(u8, err_name, e.name)) return true;
    }
    return false;
}

test "classify recognizes known errors" {
    const cases = .{
        .{ "TableNotFound", Category.table_not_found },
        .{ "DatabaseAlreadyExists", Category.database_already_exists },
        .{ "InvalidDatabaseName", Category.invalid_database_name },
        .{ "NoDatabaseSelected", Category.no_database_selected },
        .{ "QueryCancelled", Category.query_cancelled },
        .{ "ArithmeticOverflow", Category.numeric_out_of_range },
        .{ "ValueOutOfRange", Category.value_out_of_range },
        .{ "NumericOverflow", Category.value_out_of_range },
        .{ "SubqueryMultipleRows", Category.subquery_multiple_rows },
        .{ "RecursiveCteDepthExceeded", Category.recursion_depth_exceeded },
        .{ "InvalidTemporalLiteral", Category.invalid_temporal_literal },
        .{ "IncorrectArgumentsToSleep", Category.wrong_arguments },
        .{ "SqlOnColumnAmbiguous", Category.ambiguous_column },
        .{ "MemoryBudgetExceeded", Category.memory_budget_exceeded },
        .{ "OutOfMemory", Category.out_of_memory },
        .{ "UnsupportedQueryShape", Category.not_supported },
        .{ "UnsupportedCorrelatedSubquery", Category.not_supported },
        .{ "WindowUnsupported", Category.not_supported },
        .{ "SqlFoundRowsUnsupported", Category.not_supported },
        .{ "SqlExpectedFrom", Category.syntax_error },
        .{ "LexUnterminatedString", Category.syntax_error },
        .{ "UnsupportedAlterOp", Category.unknown },
        .{ "ManifestUnsupportedVersion", Category.unknown },
        .{ "NotARealError", Category.unknown },
    };
    inline for (cases) |c| try std.testing.expectEqual(c[1], classify(c[0]));
}

test "classify reads the parser's errors as syntax errors, except its refusals of forms MySQL runs (issue #490)" {
    inline for (@typeInfo(sql.ParseError).error_set.?) |e| {
        const refusal = std.mem.indexOf(u8, e.name, "Unsupported") != null or std.mem.indexOf(u8, e.name, "NotSupported") != null;
        const want: Category = if (std.mem.eql(u8, e.name, "SqlOnColumnAmbiguous"))
            .ambiguous_column
        else if (std.mem.eql(u8, e.name, "OutOfMemory"))
            .out_of_memory
        else if (refusal)
            .not_supported
        else
            .syntax_error;
        try std.testing.expectEqual(want, classify(e.name));
    }
    const runtime = .{ "MemoryBudgetExceeded", "TypeMismatch", "ComputeNoSuchOverload", "JsonInvalid", "TableFnInputMismatch" };
    inline for (runtime) |name| try std.testing.expect(classify(name) != .syntax_error);
}
