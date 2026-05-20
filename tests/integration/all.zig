//! Integration test aggregator. Each `_ = @import(...);` pulls in its tests.

const std = @import("std");
const thindb = @import("thindb");

test {
    _ = @import("roundtrip.zig");
    _ = @import("snapshot_test.zig");
    _ = @import("wal_test.zig");
    _ = @import("durability_test.zig");
    _ = @import("ddl_test.zig");
    _ = @import("uuid_test.zig");
    _ = @import("compute_test.zig");
    _ = @import("compute_scalar_test.zig");
    _ = @import("join_test.zig");
    _ = @import("aggregate_test.zig");
    _ = @import("plan_test.zig");
    _ = @import("sql_test.zig");
    _ = @import("sql_namespace_test.zig");
    _ = @import("sql_ddl_test.zig");
    _ = @import("multi_statement_test.zig");
    _ = @import("catalog_test.zig");
    _ = @import("temp_tables_test.zig");
    _ = @import("window_test.zig");
    _ = @import("window_matrix_test.zig");
    _ = @import("binary_arith_test.zig");
    _ = @import("column_defaults_test.zig");
    _ = @import("auto_increment_test.zig");
    _ = @import("case_when_test.zig");
    _ = @import("between_test.zig");
    _ = @import("like_test.zig");
    _ = @import("in_list_test.zig");
    _ = @import("extract_test.zig");
    _ = @import("having_test.zig");
    _ = @import("interval_test.zig");
    _ = @import("union_test.zig");
    _ = @import("ctas_insert_select_test.zig");
    _ = @import("scalar_subquery_test.zig");
    _ = @import("exists_test.zig");
    _ = @import("in_subquery_test.zig");
    _ = @import("col_col_predicate_test.zig");
    _ = @import("correlated_subquery_test.zig");
    _ = @import("correlated_scalar_test.zig");
    _ = @import("typed_temporal_literal_test.zig");
    _ = @import("literal_lhs_test.zig");
    _ = @import("bug_repro_test.zig");
}

test "integration entry exists" {
    try std.testing.expect(thindb.version.len > 0);
}
