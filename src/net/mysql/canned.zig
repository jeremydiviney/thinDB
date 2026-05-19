//! Canned responses for MySQL probe queries — the metadata round-trips
//! mysql CLI and most drivers fire immediately after handshake. Matching
//! them server-side keeps the response time-tight and avoids routing
//! through the SQL parser for things it doesn't support (`@@var`,
//! `SHOW VARIABLES`, etc.).

const std = @import("std");
const Allocator = std.mem.Allocator;

const handshake = @import("handshake.zig");
const sql_text = @import("../sql_text.zig");

/// Canned outcome to surface back at the wire layer.
pub const Outcome = union(enum) {
    /// Reply with an OK_Packet. Used for SET, USE-style statements we
    /// silently accept.
    ok_packet,
    /// Reply with a single-row, single-column result set named by `col`
    /// whose value is `val`.
    single_value: struct { col: []const u8, val: []const u8 },
    /// Reply with a single-row, single-column result set whose value
    /// is SQL NULL. `col` is the column name.
    single_null: []const u8,
    /// Reply with `SHOW VARIABLES`-style two-column row.
    variable_row: struct { name: []const u8, value: []const u8 },
    /// Reply with an empty two-column result set (Variable_name, Value).
    empty_variables,
    /// `KILL [QUERY|CONNECTION] <id>` — wire layer looks up the
    /// target in the connection registry, sets its cancel flag, and
    /// replies OK (or ERR 1094 if no such id). We don't distinguish
    /// KILL QUERY vs KILL CONNECTION yet; both abort the current
    /// query at the next batch boundary and leave the connection
    /// open.
    kill: u32,
    /// `RESET CONNECTION` — the wire layer drops the session's temp
    /// namespace (if any), clears the txn flag, reverts the current
    /// schema to defaults, then replies OK.
    reset_connection,
};

/// Returns null if `sql` is not a probe query we recognize.
/// `current_schema` (possibly empty) is the value reported by DATABASE().
pub fn match(
    allocator: Allocator,
    sql: []const u8,
    current_schema: []const u8,
) !?Outcome {
    const lc = try sql_text.normalizeForCannedMatch(allocator, sql);
    defer allocator.free(lc);

    if (lc.len == 0) return Outcome{ .ok_packet = {} };

    if (std.mem.startsWith(u8, lc, "set ") or std.mem.eql(u8, lc, "set")) {
        return Outcome{ .ok_packet = {} };
    }

    if (std.mem.eql(u8, lc, "reset connection")) return Outcome{ .reset_connection = {} };

    // KILL [QUERY|CONNECTION] <id> — strip the optional verb, then
    // parse the trailing integer.
    if (std.mem.startsWith(u8, lc, "kill ")) {
        var rest: []const u8 = lc[5..];
        if (std.mem.startsWith(u8, rest, "query ")) {
            rest = rest[6..];
        } else if (std.mem.startsWith(u8, rest, "connection ")) {
            rest = rest[11..];
        }
        rest = std.mem.trim(u8, rest, " \t\r\n");
        const id = std.fmt.parseInt(u32, rest, 10) catch return null;
        return Outcome{ .kill = id };
    }

    if (std.mem.eql(u8, lc, "select 1"))
        return Outcome{ .single_value = .{ .col = "1", .val = "1" } };

    if (std.mem.eql(u8, lc, "select @@version_comment") or
        std.mem.eql(u8, lc, "select @@version_comment limit 1"))
        return Outcome{ .single_value = .{ .col = "@@version_comment", .val = "thinDB" } };

    if (std.mem.eql(u8, lc, "select @@version"))
        return Outcome{ .single_value = .{ .col = "@@version", .val = handshake.server_version } };

    if (std.mem.eql(u8, lc, "select version()"))
        return Outcome{ .single_value = .{ .col = "VERSION()", .val = handshake.server_version } };

    if (std.mem.eql(u8, lc, "select @@version_compile_os"))
        return Outcome{ .single_value = .{ .col = "@@version_compile_os", .val = osLabel() } };

    if (std.mem.eql(u8, lc, "select @@max_allowed_packet"))
        return Outcome{ .single_value = .{ .col = "@@max_allowed_packet", .val = "16777216" } };

    if (std.mem.eql(u8, lc, "select @@tx_isolation") or
        std.mem.eql(u8, lc, "select @@transaction_isolation"))
        return Outcome{ .single_value = .{ .col = "@@tx_isolation", .val = "REPEATABLE-READ" } };

    if (std.mem.eql(u8, lc, "select @@session.auto_increment_increment"))
        return Outcome{ .single_value = .{ .col = "@@session.auto_increment_increment", .val = "1" } };

    if (std.mem.eql(u8, lc, "select @@character_set_client"))
        return Outcome{ .single_value = .{ .col = "@@character_set_client", .val = "utf8mb4" } };
    if (std.mem.eql(u8, lc, "select @@character_set_connection"))
        return Outcome{ .single_value = .{ .col = "@@character_set_connection", .val = "utf8mb4" } };
    if (std.mem.eql(u8, lc, "select @@character_set_results"))
        return Outcome{ .single_value = .{ .col = "@@character_set_results", .val = "utf8mb4" } };
    if (std.mem.eql(u8, lc, "select @@collation_connection"))
        return Outcome{ .single_value = .{ .col = "@@collation_connection", .val = "utf8mb4_general_ci" } };
    if (std.mem.eql(u8, lc, "select @@collation_server"))
        return Outcome{ .single_value = .{ .col = "@@collation_server", .val = "utf8mb4_general_ci" } };
    if (std.mem.eql(u8, lc, "select @@time_zone"))
        return Outcome{ .single_value = .{ .col = "@@time_zone", .val = "SYSTEM" } };

    if (std.mem.eql(u8, lc, "select database()")) {
        if (current_schema.len == 0) return Outcome{ .single_null = "DATABASE()" };
        return Outcome{ .single_value = .{ .col = "DATABASE()", .val = current_schema } };
    }
    if (std.mem.eql(u8, lc, "select user()") or std.mem.eql(u8, lc, "select current_user()"))
        return Outcome{ .single_value = .{ .col = "USER()", .val = "thindb@localhost" } };

    if (std.mem.eql(u8, lc, "show variables like 'sql_mode'"))
        return Outcome{ .variable_row = .{ .name = "sql_mode", .value = "STRICT_TRANS_TABLES" } };
    if (std.mem.eql(u8, lc, "show variables like 'character_set_results'"))
        return Outcome{ .variable_row = .{ .name = "character_set_results", .value = "utf8mb4" } };
    if (std.mem.eql(u8, lc, "show variables like 'lower_case_table_names'"))
        return Outcome{ .variable_row = .{ .name = "lower_case_table_names", .value = "1" } };

    if (std.mem.startsWith(u8, lc, "show variables")) {
        return Outcome{ .empty_variables = {} };
    }

    return null;
}

fn osLabel() []const u8 {
    return switch (@import("builtin").os.tag) {
        .windows => "Windows",
        .macos => "macOS",
        else => "Linux",
    };
}

test "canned matches version comment with limit" {
    const allocator = std.testing.allocator;
    const m = try match(allocator, "SELECT @@version_comment LIMIT 1;", "");
    try std.testing.expect(m != null);
    switch (m.?) {
        .single_value => |sv| try std.testing.expectEqualStrings("thinDB", sv.val),
        else => return error.TestUnexpectedResult,
    }
}

test "canned accepts arbitrary SET as OK" {
    const allocator = std.testing.allocator;
    const m = try match(allocator, "SET autocommit=1", "");
    try std.testing.expect(m != null);
    try std.testing.expectEqual(@as(std.meta.Tag(Outcome), .ok_packet), std.meta.activeTag(m.?));
}

test "canned database() with empty schema yields NULL" {
    const allocator = std.testing.allocator;
    const m = try match(allocator, "SELECT DATABASE()", "");
    try std.testing.expect(m != null);
    switch (m.?) {
        .single_null => |c| try std.testing.expectEqualStrings("DATABASE()", c),
        else => return error.TestUnexpectedResult,
    }
}
