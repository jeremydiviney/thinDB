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
    /// Reply with an empty Workbench/driver metadata result set.
    empty_result: EmptyResultKind,
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

pub const EmptyResultKind = enum {
    warnings,
    engines,
    plugins,
    collations,
    character_sets,
    tables,
    full_tables,
    table_status,
    create_table,
    columns,
    indexes,
    grants,
    processlist,
    generic_status,
};

/// Returns null if `sql` is not a probe query we recognize.
/// `current_schema` (possibly empty) is the value reported by DATABASE().
/// True for `SET @name = ...` (a MySQL user-defined variable), false for
/// system/session vars (`SET names ...`, `SET @@global.x`, `SET autocommit`).
/// `lc` is the normalized-lowercased statement. A single leading `@` after the
/// `SET ` keyword marks a user variable; `@@` marks a system variable.
fn isUserVarSet(lc: []const u8) bool {
    if (!std.mem.startsWith(u8, lc, "set ")) return false;
    const rest = std.mem.trimStart(u8, lc[4..], " \t");
    return rest.len >= 1 and rest[0] == '@' and !(rest.len >= 2 and rest[1] == '@');
}

/// Column label for a canned `SELECT <expr>` reply. Real MySQL echoes the
/// expression text EXACTLY as the client typed it — `select version()` names
/// the column `version()`, `SELECT VeRsIoN()` names it `VeRsIoN()` — and
/// clients (TypeORM, mysql2, JDBC) read result fields by that literal text,
/// so a hardcoded label breaks them. Slice the expression from the original
/// statement: everything after the leading SELECT, trailing `;`/whitespace
/// and an optional LIMIT clause trimmed. The slice borrows from `sql`, which
/// outlives the Outcome (the caller holds the query payload while replying).
fn selectExprLabel(sql: []const u8, fallback: []const u8) []const u8 {
    var s = std.mem.trim(u8, sql, " \t\r\n;");
    if (s.len < 8 or !std.ascii.eqlIgnoreCase(s[0..6], "select")) return fallback;
    if (s[6] != ' ' and s[6] != '\t') return fallback;
    s = std.mem.trim(u8, s[7..], " \t\r\n");
    if (std.ascii.indexOfIgnoreCase(s, " limit ")) |i| s = std.mem.trimEnd(u8, s[0..i], " \t");
    return if (s.len == 0) fallback else s;
}

pub fn match(
    allocator: Allocator,
    sql: []const u8,
    current_schema: []const u8,
) !?Outcome {
    const lc = try sql_text.normalizeForCannedMatch(allocator, sql);
    defer allocator.free(lc);

    if (lc.len == 0) return Outcome{ .ok_packet = {} };

    // System-variable / session SETs (`SET names`, `SET autocommit=1`,
    // `SET @@session.x=y`) are no-ops we ack with OK. But `SET @user_var = ...`
    // is a real user-defined variable the engine must store — let it through to
    // the compile path so the value persists for later statements.
    if ((std.mem.startsWith(u8, lc, "set ") or std.mem.eql(u8, lc, "set")) and !isUserVarSet(lc)) {
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
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "1"), .val = "1" } };

    if (std.mem.eql(u8, lc, "select @@version_comment") or
        std.mem.eql(u8, lc, "select @@version_comment limit 1"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "@@version_comment"), .val = "thinDB" } };

    if (std.mem.eql(u8, lc, "select @@version"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "@@version"), .val = handshake.server_version } };

    if (std.mem.eql(u8, lc, "select version()"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "version()"), .val = handshake.server_version } };

    if (std.mem.eql(u8, lc, "select @@version_compile_os"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "@@version_compile_os"), .val = osLabel() } };

    if (std.mem.eql(u8, lc, "select @@max_allowed_packet"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "@@max_allowed_packet"), .val = "16777216" } };

    if (std.mem.eql(u8, lc, "select @@tx_isolation") or
        std.mem.eql(u8, lc, "select @@transaction_isolation"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "@@tx_isolation"), .val = "REPEATABLE-READ" } };

    if (std.mem.eql(u8, lc, "select @@session.auto_increment_increment"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "@@session.auto_increment_increment"), .val = "1" } };

    if (std.mem.eql(u8, lc, "select @@character_set_client"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "@@character_set_client"), .val = "utf8mb4" } };
    if (std.mem.eql(u8, lc, "select @@character_set_connection"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "@@character_set_connection"), .val = "utf8mb4" } };
    if (std.mem.eql(u8, lc, "select @@character_set_results"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "@@character_set_results"), .val = "utf8mb4" } };
    if (std.mem.eql(u8, lc, "select @@collation_connection"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "@@collation_connection"), .val = "utf8mb4_general_ci" } };
    if (std.mem.eql(u8, lc, "select @@collation_server"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "@@collation_server"), .val = "utf8mb4_general_ci" } };
    if (std.mem.eql(u8, lc, "select @@time_zone"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "@@time_zone"), .val = "SYSTEM" } };

    if (std.mem.eql(u8, lc, "select database()")) {
        if (current_schema.len == 0) return Outcome{ .single_null = selectExprLabel(sql, "database()") };
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "database()"), .val = current_schema } };
    }
    if (std.mem.eql(u8, lc, "select user()") or std.mem.eql(u8, lc, "select current_user()"))
        return Outcome{ .single_value = .{ .col = selectExprLabel(sql, "user()"), .val = "thindb@localhost" } };

    if (isShowVariables(lc)) {
        if (std.mem.indexOf(u8, lc, "sql_mode") != null)
            return Outcome{ .variable_row = .{ .name = "sql_mode", .value = "STRICT_TRANS_TABLES" } };
        if (std.mem.indexOf(u8, lc, "character_set_results") != null)
            return Outcome{ .variable_row = .{ .name = "character_set_results", .value = "utf8mb4" } };
        if (std.mem.indexOf(u8, lc, "lower_case_table_names") != null)
            return Outcome{ .variable_row = .{ .name = "lower_case_table_names", .value = "1" } };
        if (std.mem.indexOf(u8, lc, "version") != null)
            return Outcome{ .variable_row = .{ .name = "version", .value = handshake.server_version } };
        return Outcome{ .empty_variables = {} };
    }

    if (std.mem.eql(u8, lc, "show variables like 'sql_mode'"))
        return Outcome{ .variable_row = .{ .name = "sql_mode", .value = "STRICT_TRANS_TABLES" } };
    if (std.mem.eql(u8, lc, "show variables like 'character_set_results'"))
        return Outcome{ .variable_row = .{ .name = "character_set_results", .value = "utf8mb4" } };
    if (std.mem.eql(u8, lc, "show variables like 'lower_case_table_names'"))
        return Outcome{ .variable_row = .{ .name = "lower_case_table_names", .value = "1" } };

    if (isShowStatus(lc)) return Outcome{ .empty_variables = {} };
    if (std.mem.startsWith(u8, lc, "show warnings") or
        std.mem.startsWith(u8, lc, "show errors"))
        return Outcome{ .empty_result = .warnings };
    if (std.mem.startsWith(u8, lc, "show engines"))
        return Outcome{ .empty_result = .engines };
    if (std.mem.startsWith(u8, lc, "show plugins"))
        return Outcome{ .empty_result = .plugins };
    if (std.mem.startsWith(u8, lc, "show collation") or
        std.mem.startsWith(u8, lc, "show collations"))
        return Outcome{ .empty_result = .collations };
    if (std.mem.startsWith(u8, lc, "show character set") or
        std.mem.startsWith(u8, lc, "show charset"))
        return Outcome{ .empty_result = .character_sets };
    if (std.mem.startsWith(u8, lc, "show full tables"))
        return Outcome{ .empty_result = .full_tables };
    // Handled on the wire (not the engine parser) so the MySQL-dialect
    // forms — FROM/IN a flattened `db__schema` name, LIKE patterns, the
    // `Tables_in_<db>` column header — all behave like real MySQL.
    if (std.mem.startsWith(u8, lc, "show tables"))
        return Outcome{ .empty_result = .tables };
    if (std.mem.startsWith(u8, lc, "show table status"))
        return Outcome{ .empty_result = .table_status };
    if (std.mem.startsWith(u8, lc, "show create table "))
        return Outcome{ .empty_result = .create_table };
    if (std.mem.startsWith(u8, lc, "show full columns") or
        std.mem.startsWith(u8, lc, "show columns") or
        std.mem.startsWith(u8, lc, "show fields") or
        std.mem.startsWith(u8, lc, "desc ") or
        std.mem.startsWith(u8, lc, "describe "))
        return Outcome{ .empty_result = .columns };
    if (std.mem.startsWith(u8, lc, "show index") or
        std.mem.startsWith(u8, lc, "show indexes") or
        std.mem.startsWith(u8, lc, "show keys"))
        return Outcome{ .empty_result = .indexes };
    if (std.mem.startsWith(u8, lc, "show grants"))
        return Outcome{ .empty_result = .grants };
    if (std.mem.startsWith(u8, lc, "show processlist") or
        std.mem.startsWith(u8, lc, "show full processlist"))
        return Outcome{ .empty_result = .processlist };
    if (std.mem.startsWith(u8, lc, "show procedure status") or
        std.mem.startsWith(u8, lc, "show triggers") or
        std.mem.startsWith(u8, lc, "show events") or
        std.mem.startsWith(u8, lc, "show master status") or
        std.mem.startsWith(u8, lc, "show slave status") or
        std.mem.startsWith(u8, lc, "show replica status"))
        return Outcome{ .empty_result = .generic_status };

    return null;
}

fn isShowVariables(lc: []const u8) bool {
    return std.mem.startsWith(u8, lc, "show variables") or
        std.mem.startsWith(u8, lc, "show session variables") or
        std.mem.startsWith(u8, lc, "show global variables");
}

fn isShowStatus(lc: []const u8) bool {
    return std.mem.startsWith(u8, lc, "show status") or
        std.mem.startsWith(u8, lc, "show session status") or
        std.mem.startsWith(u8, lc, "show global status");
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

test "canned lets user-variable SET through to the engine, swallows system vars" {
    const allocator = std.testing.allocator;
    // `SET @x = ...` is a real user variable — must NOT be swallowed (null →
    // flows to the compile path so the value persists for later statements).
    try std.testing.expect((try match(allocator, "SET @projectId = 1000054", "")) == null);
    try std.testing.expect((try match(allocator, "SET @x=1, @y=2", "")) == null);
    // System/session vars stay canned-OK.
    try std.testing.expect((try match(allocator, "SET NAMES utf8mb4", "")) != null);
    try std.testing.expect((try match(allocator, "SET @@session.sql_mode = ''", "")) != null);
}

test "canned accepts Workbench SHOW SESSION VARIABLES probe" {
    const allocator = std.testing.allocator;
    const m = try match(allocator, "SHOW SESSION VARIABLES LIKE 'lower_case_table_names'", "");
    try std.testing.expect(m != null);
    switch (m.?) {
        .variable_row => |vr| try std.testing.expectEqualStrings("lower_case_table_names", vr.name),
        else => return error.TestUnexpectedResult,
    }
}

test "canned accepts Workbench SHOW FULL TABLES probe" {
    const allocator = std.testing.allocator;
    const m = try match(allocator, "SHOW FULL TABLES FROM `main__public`", "");
    try std.testing.expect(m != null);
    try std.testing.expectEqual(@as(std.meta.Tag(Outcome), .empty_result), std.meta.activeTag(m.?));
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
