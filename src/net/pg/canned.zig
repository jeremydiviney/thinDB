//! Canned responses for psql / libpq probe queries that don't map cleanly
//! onto our engine (most touch the `pg_catalog` virtual schema). Matching
//! them at the wire layer keeps psql happy without us implementing the
//! catalog tables. Matched against a normalized (trimmed, lowercased,
//! trailing-`;` stripped) form of the query text.

const std = @import("std");
const Allocator = std.mem.Allocator;

const startup = @import("startup.zig");
const sql_text = @import("../sql_text.zig");

pub const Probe = union(enum) {
    /// Silently accept (no rows, no side-effects). Tag is the literal
    /// command-complete payload (e.g. "SET", "BEGIN").
    accept: []const u8,
    /// `DISCARD ALL` / `DISCARD TEMP` / `DISCARD TEMPORARY` — wire layer
    /// drops the session's temp namespace before replying with the
    /// CommandComplete tag below. `RESET ALL` does NOT discard temps
    /// per PG spec; only DISCARD does.
    discard_temp: []const u8,
    /// Reply with a single-row, single-column SELECT result. Column
    /// header is `col`; value is `val` (NULL if `val` is null).
    single_value: struct { col: []const u8, val: ?[]const u8 },
    /// Reply with a multi-row, multi-column SELECT result. Each row's
    /// cells must match `cols.len`. Built once at match time.
    static_rows: StaticRows,
    /// `SET search_path TO <schema>[, ...]` — apply the first schema as the
    /// session's current schema (thinDB tracks a single schema, not a list).
    /// The payload is owned (allocator-dup'd in `match`); the dispatcher
    /// frees it after applying.
    set_search_path: []const u8,
    /// `pg_cancel_backend(<pid>)` — wire layer looks up the target in
    /// the connection registry, sets its cancel flag, and replies a
    /// single-row "t"/"f" boolean. `pg_terminate_backend` shares the
    /// same shape; we don't distinguish (we have no way to forcibly
    /// close the target's socket).
    cancel_backend: u32,
};

pub const StaticRows = struct {
    col_names: []const []const u8,
    rows: []const []const ?[]const u8,
};

/// Extract the `<pid>` from a normalized SQL string of the form
/// `<prefix><pid>)` where prefix is e.g. "select pg_cancel_backend(".
/// Returns null on any parse failure.
fn parseCancelBackend(lc: []const u8, prefix: []const u8) ?u32 {
    if (!std.mem.startsWith(u8, lc, prefix)) return null;
    if (lc.len <= prefix.len or lc[lc.len - 1] != ')') return null;
    const inner = std.mem.trim(u8, lc[prefix.len .. lc.len - 1], " \t");
    return std.fmt.parseInt(u32, inner, 10) catch null;
}

/// Returns null if `sql` is not a probe query we recognize.
pub fn match(
    allocator: Allocator,
    sql: []const u8,
    current_db: []const u8,
    current_schema: []const u8,
) !?Probe {
    const lc = try sql_text.normalizeForCannedMatch(allocator, sql);
    defer allocator.free(lc);

    if (lc.len == 0) return Probe{ .accept = "" };

    // `SET search_path TO/= x[, ...]` actually switches the session schema
    // (the first listed schema); every other SET stays a silent no-op.
    if (std.mem.startsWith(u8, lc, "set search_path")) {
        var rest = std.mem.trim(u8, lc["set search_path".len..], " \t");
        if (std.mem.startsWith(u8, rest, "to")) {
            rest = std.mem.trim(u8, rest[2..], " \t");
        } else if (std.mem.startsWith(u8, rest, "=")) {
            rest = std.mem.trim(u8, rest[1..], " \t");
        }
        var end: usize = 0;
        while (end < rest.len and rest[end] != ',' and rest[end] != ' ') end += 1;
        var schema = rest[0..end];
        if (schema.len >= 2 and schema[0] == '"' and schema[schema.len - 1] == '"')
            schema = schema[1 .. schema.len - 1];
        // Skip the PG `"$user"` placeholder if it leads the list.
        if (schema.len > 0 and schema[0] != '$')
            return Probe{ .set_search_path = try allocator.dupe(u8, schema) };
    }
    if (std.mem.startsWith(u8, lc, "set ") or std.mem.eql(u8, lc, "set"))
        return Probe{ .accept = "SET" };
    if (std.mem.eql(u8, lc, "begin") or std.mem.startsWith(u8, lc, "begin "))
        return Probe{ .accept = "BEGIN" };
    // PG accepts START TRANSACTION as a synonym for BEGIN. The
    // CommandComplete tag PG itself emits is "BEGIN" in both cases.
    if (std.mem.eql(u8, lc, "start transaction") or std.mem.startsWith(u8, lc, "start transaction "))
        return Probe{ .accept = "BEGIN" };
    if (std.mem.eql(u8, lc, "commit") or std.mem.startsWith(u8, lc, "commit "))
        return Probe{ .accept = "COMMIT" };
    if (std.mem.eql(u8, lc, "rollback") or std.mem.startsWith(u8, lc, "rollback "))
        return Probe{ .accept = "ROLLBACK" };

    // pg_cancel_backend(<pid>) / pg_terminate_backend(<pid>) — both
    // route to the connection registry's requestCancel. PG returns a
    // bool indicating success.
    if (parseCancelBackend(lc, "select pg_cancel_backend(")) |pid|
        return Probe{ .cancel_backend = pid };
    if (parseCancelBackend(lc, "select pg_terminate_backend(")) |pid|
        return Probe{ .cancel_backend = pid };
    if (std.mem.eql(u8, lc, "discard all"))
        return Probe{ .discard_temp = "DISCARD ALL" };
    if (std.mem.eql(u8, lc, "discard temp") or std.mem.eql(u8, lc, "discard temporary"))
        return Probe{ .discard_temp = "DISCARD TEMP" };
    if (std.mem.eql(u8, lc, "discard plans"))
        return Probe{ .accept = "DISCARD PLANS" };
    if (std.mem.eql(u8, lc, "reset all"))
        return Probe{ .accept = "RESET ALL" };

    if (std.mem.eql(u8, lc, "select version()") or std.mem.eql(u8, lc, "select pg_catalog.version()"))
        return Probe{ .single_value = .{ .col = "version", .val = "PostgreSQL 16.0 (thinDB)" } };

    if (std.mem.eql(u8, lc, "show server_version"))
        return Probe{ .single_value = .{ .col = "server_version", .val = startup.server_version } };
    if (std.mem.eql(u8, lc, "show server_encoding"))
        return Probe{ .single_value = .{ .col = "server_encoding", .val = "UTF8" } };
    if (std.mem.eql(u8, lc, "show client_encoding"))
        return Probe{ .single_value = .{ .col = "client_encoding", .val = "UTF8" } };
    if (std.mem.eql(u8, lc, "show datestyle"))
        return Probe{ .single_value = .{ .col = "DateStyle", .val = "ISO, MDY" } };
    if (std.mem.eql(u8, lc, "show timezone") or std.mem.eql(u8, lc, "show time zone"))
        return Probe{ .single_value = .{ .col = "TimeZone", .val = "UTC" } };
    if (std.mem.eql(u8, lc, "show standard_conforming_strings"))
        return Probe{ .single_value = .{ .col = "standard_conforming_strings", .val = "on" } };
    if (std.mem.eql(u8, lc, "show transaction_isolation"))
        return Probe{ .single_value = .{ .col = "transaction_isolation", .val = "read committed" } };
    if (std.mem.eql(u8, lc, "show search_path"))
        return Probe{ .single_value = .{ .col = "search_path", .val = current_schema } };
    if (std.mem.eql(u8, lc, "select pg_backend_pid()"))
        return Probe{ .single_value = .{ .col = "pg_backend_pid", .val = "1" } };

    if (std.mem.eql(u8, lc, "select current_database()"))
        return Probe{ .single_value = .{ .col = "current_database", .val = current_db } };
    if (std.mem.eql(u8, lc, "select current_schema()") or std.mem.eql(u8, lc, "select current_schema"))
        return Probe{ .single_value = .{ .col = "current_schema", .val = current_schema } };
    if (std.mem.eql(u8, lc, "select current_user") or std.mem.eql(u8, lc, "select user") or std.mem.eql(u8, lc, "select session_user"))
        return Probe{ .single_value = .{ .col = "current_user", .val = "thindb" } };

    if (std.mem.eql(u8, lc, "select 1"))
        return Probe{ .single_value = .{ .col = "?column?", .val = "1" } };

    // pg_catalog relations (pg_class / pg_namespace / pg_database / ...) are
    // served by the engine's virtual tables (see net/pg_catalog.zig), so we
    // deliberately do NOT intercept them here — letting the query compile
    // gives real, JOIN-able results instead of a single-column listing.
    return null;
}

test "match recognizes version() probe" {
    const allocator = std.testing.allocator;
    const m = try match(allocator, "SELECT version();", "main", "public");
    try std.testing.expect(m != null);
    switch (m.?) {
        .single_value => |sv| try std.testing.expectEqualStrings("PostgreSQL 16.0 (thinDB)", sv.val.?),
        else => return error.TestUnexpectedResult,
    }
}

test "match treats arbitrary SET as accept" {
    const allocator = std.testing.allocator;
    const m = try match(allocator, "SET client_encoding = 'UTF8'", "main", "public");
    try std.testing.expect(m != null);
    switch (m.?) {
        .accept => |tag| try std.testing.expectEqualStrings("SET", tag),
        else => return error.TestUnexpectedResult,
    }
}

test "match applies SET search_path to the first listed schema" {
    const allocator = std.testing.allocator;
    const m = try match(allocator, "SET search_path TO analytics, public", "main", "public");
    try std.testing.expect(m != null);
    switch (m.?) {
        .set_search_path => |sc| {
            try std.testing.expectEqualStrings("analytics", sc);
            allocator.free(sc);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "match does not intercept pg_catalog relations (engine vtables serve them)" {
    const allocator = std.testing.allocator;
    const m = try match(allocator, "SELECT datname FROM pg_catalog.pg_database ORDER BY 1", "main", "public");
    try std.testing.expect(m == null);
}

test "match recognizes BEGIN / COMMIT" {
    const allocator = std.testing.allocator;
    const m1 = try match(allocator, "BEGIN", "main", "public");
    const m2 = try match(allocator, "COMMIT", "main", "public");
    try std.testing.expect(m1 != null);
    try std.testing.expect(m2 != null);
    switch (m1.?) {
        .accept => |tag| try std.testing.expectEqualStrings("BEGIN", tag),
        else => return error.TestUnexpectedResult,
    }
    switch (m2.?) {
        .accept => |tag| try std.testing.expectEqualStrings("COMMIT", tag),
        else => return error.TestUnexpectedResult,
    }
}
