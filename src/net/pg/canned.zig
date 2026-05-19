//! Canned responses for psql / libpq probe queries that don't map cleanly
//! onto our engine (most touch the `pg_catalog` virtual schema). Matching
//! them at the wire layer keeps psql happy without us implementing the
//! catalog tables. Matched against a normalized (trimmed, lowercased,
//! trailing-`;` stripped) form of the query text.

const std = @import("std");
const Allocator = std.mem.Allocator;

const startup = @import("startup.zig");

pub const Probe = union(enum) {
    /// Silently accept (no rows, no side-effects). Tag is the literal
    /// command-complete payload (e.g. "SET", "BEGIN").
    accept: []const u8,
    /// Reply with a single-row, single-column SELECT result. Column
    /// header is `col`; value is `val` (NULL if `val` is null).
    single_value: struct { col: []const u8, val: ?[]const u8 },
    /// Reply with a single-column SELECT result whose rows are taken
    /// from the catalog at dispatch time (databases / schemas /
    /// tables). `col` is the column header; `kind` selects the source.
    catalog_listing: struct { col: []const u8, kind: ListingKind },
    /// Reply with a multi-row, multi-column SELECT result. Each row's
    /// cells must match `cols.len`. Built once at match time.
    static_rows: StaticRows,
};

pub const ListingKind = enum { databases, schemas, tables };

pub const StaticRows = struct {
    col_names: []const []const u8,
    rows: []const []const ?[]const u8,
};

fn normalize(allocator: Allocator, sql: []const u8) ![]u8 {
    var s = std.mem.trim(u8, sql, " \t\r\n");
    while (s.len > 0 and s[s.len - 1] == ';') s = std.mem.trim(u8, s[0 .. s.len - 1], " \t\r\n");
    const out = try allocator.alloc(u8, s.len);
    for (s, 0..) |c, i| out[i] = std.ascii.toLower(c);
    return out;
}

/// Returns null if `sql` is not a probe query we recognize.
pub fn match(
    allocator: Allocator,
    sql_text: []const u8,
    current_db: []const u8,
    current_schema: []const u8,
) !?Probe {
    const lc = try normalize(allocator, sql_text);
    defer allocator.free(lc);

    if (lc.len == 0) return Probe{ .accept = "" };

    if (std.mem.startsWith(u8, lc, "set ") or std.mem.eql(u8, lc, "set"))
        return Probe{ .accept = "SET" };
    if (std.mem.eql(u8, lc, "begin") or std.mem.startsWith(u8, lc, "begin "))
        return Probe{ .accept = "BEGIN" };
    if (std.mem.eql(u8, lc, "commit") or std.mem.startsWith(u8, lc, "commit "))
        return Probe{ .accept = "COMMIT" };
    if (std.mem.eql(u8, lc, "rollback") or std.mem.startsWith(u8, lc, "rollback "))
        return Probe{ .accept = "ROLLBACK" };
    if (std.mem.eql(u8, lc, "discard all"))
        return Probe{ .accept = "DISCARD ALL" };

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

    if (std.mem.eql(u8, lc, "select current_database()"))
        return Probe{ .single_value = .{ .col = "current_database", .val = current_db } };
    if (std.mem.eql(u8, lc, "select current_schema()") or std.mem.eql(u8, lc, "select current_schema"))
        return Probe{ .single_value = .{ .col = "current_schema", .val = current_schema } };
    if (std.mem.eql(u8, lc, "select current_user") or std.mem.eql(u8, lc, "select user") or std.mem.eql(u8, lc, "select session_user"))
        return Probe{ .single_value = .{ .col = "current_user", .val = "thindb" } };

    if (std.mem.eql(u8, lc, "select 1"))
        return Probe{ .single_value = .{ .col = "?column?", .val = "1" } };

    if (std.mem.eql(u8, lc, "select datname from pg_catalog.pg_database order by 1") or
        std.mem.eql(u8, lc, "select datname from pg_database order by 1") or
        std.mem.eql(u8, lc, "select datname from pg_catalog.pg_database"))
    {
        return Probe{ .catalog_listing = .{ .col = "datname", .kind = .databases } };
    }

    if (std.mem.indexOf(u8, lc, "from pg_catalog.pg_namespace") != null or
        std.mem.indexOf(u8, lc, "from pg_namespace") != null)
    {
        return Probe{ .catalog_listing = .{ .col = "nspname", .kind = .schemas } };
    }

    if (std.mem.indexOf(u8, lc, "from pg_catalog.pg_class") != null or
        std.mem.indexOf(u8, lc, "from pg_class") != null)
    {
        return Probe{ .catalog_listing = .{ .col = "Name", .kind = .tables } };
    }

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
    const m = try match(allocator, "SET search_path = analytics", "main", "public");
    try std.testing.expect(m != null);
    switch (m.?) {
        .accept => |tag| try std.testing.expectEqualStrings("SET", tag),
        else => return error.TestUnexpectedResult,
    }
}

test "match returns catalog listing for pg_database probe" {
    const allocator = std.testing.allocator;
    const m = try match(allocator, "SELECT datname FROM pg_catalog.pg_database ORDER BY 1", "main", "public");
    try std.testing.expect(m != null);
    switch (m.?) {
        .catalog_listing => |cl| try std.testing.expectEqual(ListingKind.databases, cl.kind),
        else => return error.TestUnexpectedResult,
    }
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
