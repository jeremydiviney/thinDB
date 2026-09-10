//! End-to-end coverage for `WITH KEYED BY (...)` pipeline regions: the
//! declared-block builder compiling real SQL through the region path,
//! validated by value-equality against the identical pipeline without the
//! declaration (mono engine). Data is tie-free within each key partition so
//! both paths are deterministic and comparable row-for-row. Also covers the
//! hard-decline contract (a declared block that can't compile is a query
//! error, never a silent fallback) and NULL-key rows.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    return setup_with_dop(allocator, io, dir, 1);
}

fn setup_with_dop(allocator: std.mem.Allocator, io: anytype, dir: anytype, dop: usize) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{ .max_dop = dop });
    errdefer db.close();
    try helpers.exec(allocator, db,
        \\CREATE TABLE inv (
        \\  id BIGINT PRIMARY KEY,
        \\  projectId BIGINT,
        \\  custLC VARCHAR(32),
        \\  month INT,
        \\  amount BIGINT
        \\)
    );

    // 2 projects x 8 customers x 5 months (with per-customer month gaps so
    // rank/order paths see uneven partitions). Amounts are unique per row;
    // months are unique within each (project, customer) partition — no ties.
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    try sql.appendSlice(allocator, "INSERT INTO inv (id, projectId, custLC, month, amount) VALUES ");
    var id: i64 = 1;
    var first = true;
    for (0..2) |p| {
        for (0..8) |c| {
            for (0..5) |m| {
                if ((c + m) % 7 == 3) continue; // month gaps
                if (!first) try sql.appendSlice(allocator, ",");
                first = false;
                // A few rows carry a NULL customer key.
                if (c == 5 and m == 1) {
                    try sql.print(allocator, "({d},{d},NULL,{d},{d})", .{
                        id, 100 + p, @as(i64, @intCast(m + 1)), id * 7 + 3,
                    });
                } else {
                    try sql.print(allocator, "({d},{d},'cust_{d}',{d},{d})", .{
                        id, 100 + p, c, @as(i64, @intCast(m + 1)), id * 7 + 3,
                    });
                }
                id += 1;
            }
        }
    }
    try helpers.exec(allocator, db, sql.items);
    const t = try db.openTable("inv", .{});
    try t.flush();
    return db;
}

/// Drain a query and render every row as one line ("a|b|c", NULL as "~"),
/// so keyed and mono results compare with expectEqualStrings and a failure
/// shows the exact diverging row.
fn runToText(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]u8 {
    return run_to_text(allocator, db, sql, null);
}

fn run_to_text(allocator: std.mem.Allocator, db: anytype, sql: []const u8, region_column: ?[]const u8) ![]u8 {
    var q = try helpers.runSql(allocator, db, sql);
    defer q.deinit();
    if (std.mem.indexOf(u8, sql, "KEYED BY") != null) {
        const staged = thindb.exec.queryAs(thindb.exec.mat_stage.StagedRoot, q.cq.query) orelse
            return error.TestExpectedEqual;
        var region_found = false;
        for (staged.set.stages.items) |stage| {
            if (region_column) |name| {
                if (thindb.types.findColumn(stage.schema, name) == null) continue;
            }
            if (stage.is_keyed_region) region_found = true;
        }
        try std.testing.expect(region_found);
    }
    const schema = q.outputSchema();
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (schema) |col| try out.print(allocator, "{s}:{any}\n", .{ col.name, col.type });
    while (try q.next()) |batch| {
        for (0..batch.row_count) |r| {
            for (schema, 0..) |col, ci| {
                if (ci != 0) try out.appendSlice(allocator, "|");
                const v = batch.values[ci];
                if (!v.isValid(r)) {
                    try out.appendSlice(allocator, "~");
                    continue;
                }
                switch (col.type) {
                    .bigint => try out.print(allocator, "{d}", .{v.data.bigint[r]}),
                    .largeint => try out.print(allocator, "{d}", .{v.data.largeint[r]}),
                    .int => try out.print(allocator, "{d}", .{v.data.int[r]}),
                    .double => try out.print(allocator, "{d}", .{v.data.double[r]}),
                    .string => try out.appendSlice(allocator, v.data.string.rowBytes(r)),
                    .varchar => try out.appendSlice(allocator, v.data.varchar.rowBytes(r)),
                    .char => try out.appendSlice(allocator, v.data.char.rowBytes(r)),
                    else => {
                        std.debug.print("unhandled column type in test: {s}\n", .{@tagName(col.type)});
                        return error.UnhandledColumnTypeInTest;
                    },
                }
            }
            try out.appendSlice(allocator, "\n");
        }
    }
    return out.toOwnedSlice(allocator);
}

const keyed_pipeline =
    \\WITH KEYED BY (projectId, custLC)
    \\r AS (
    \\  SELECT projectId, custLC, month, amount,
    \\         ROW_NUMBER() OVER (PARTITION BY projectId, custLC ORDER BY month) AS rn
    \\  FROM inv WHERE projectId >= 100
    \\),
    \\m AS (
    \\  SELECT projectId, custLC, month, SUM(amount) AS amt, MAX(rn) AS mrn
    \\  FROM r GROUP BY projectId, custLC, month
    \\)
    \\SELECT projectId, custLC, COUNT(*) AS n, SUM(amt) AS total, SUM(mrn) AS msum
    \\FROM m
    \\GROUP BY projectId, custLC
    \\ORDER BY projectId ASC, custLC ASC
;

/// The same statement minus the `KEYED BY (...)` declaration — the mono
/// engine reference.
const mono_pipeline = blk: {
    const marker = "KEYED BY (projectId, custLC)\n";
    const at = std.mem.indexOf(u8, keyed_pipeline, marker).?;
    break :blk keyed_pipeline[0..at] ++ keyed_pipeline[at + marker.len ..];
};

test "keyed region: group + rank pipeline matches mono value-for-value (incl NULL keys)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const keyed = try runToText(allocator, db, keyed_pipeline);
    defer allocator.free(keyed);
    const mono = try runToText(allocator, db, mono_pipeline);
    defer allocator.free(mono);

    try std.testing.expect(keyed.len > 0);
    try std.testing.expectEqualStrings(mono, keyed);
}

test "keyed region: filter below the block composes and matches mono" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const keyed_sql =
        \\WITH KEYED BY (custLC)
        \\r AS (
        \\  SELECT custLC, month, amount,
        \\         ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\  FROM inv WHERE projectId = 100 AND month >= 2
        \\),
        \\m AS (
        \\  SELECT custLC, month, SUM(amount) AS amt, MAX(rn) AS mrn
        \\  FROM r GROUP BY custLC, month
        \\)
        \\SELECT custLC, SUM(amt) AS total, SUM(mrn) AS s FROM m
        \\GROUP BY custLC ORDER BY custLC ASC
    ;
    const mono_sql =
        \\WITH r AS (
        \\  SELECT custLC, month, amount,
        \\         ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\  FROM inv WHERE projectId = 100 AND month >= 2
        \\),
        \\m AS (
        \\  SELECT custLC, month, SUM(amount) AS amt, MAX(rn) AS mrn
        \\  FROM r GROUP BY custLC, month
        \\)
        \\SELECT custLC, SUM(amt) AS total, SUM(mrn) AS s FROM m
        \\GROUP BY custLC ORDER BY custLC ASC
    ;
    const keyed = try runToText(allocator, db, keyed_sql);
    defer allocator.free(keyed);
    const mono = try runToText(allocator, db, mono_sql);
    defer allocator.free(mono);

    try std.testing.expect(keyed.len > 0);
    try std.testing.expectEqualStrings(mono, keyed);
}

test "keyed region: declared block that violates the key contract is a hard error" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // The window partitions by a non-key column: per-key execution cannot
    // honor it, and a DECLARED block must fail the query — no silent
    // fallback to the mono engine.
    try helpers.expectRunError(allocator, db,
        \\WITH KEYED BY (custLC)
        \\r AS (
        \\  SELECT custLC, month,
        \\         ROW_NUMBER() OVER (PARTITION BY month ORDER BY custLC) AS rn
        \\  FROM inv
        \\)
        \\SELECT custLC, SUM(rn) AS s FROM r GROUP BY custLC ORDER BY custLC ASC
    , error.RegionKeyContractViolation);
}

fn expect_keyed_matches(allocator: std.mem.Allocator, db: *thindb.Database, comptime body: []const u8, region_column: []const u8) !void {
    const mono = try runToText(allocator, db, "WITH " ++ body);
    defer allocator.free(mono);
    try std.testing.expect(mono.len > 0);
    for (0..2) |_| {
        const keyed = try run_to_text(allocator, db, "WITH KEYED BY (custLC) " ++ body, region_column);
        defer allocator.free(keyed);
        try std.testing.expectEqualStrings(mono, keyed);
    }
}

test "keyed region: boundaries below joins retain duplicate matches and unmatched rows" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    inline for (.{
        .{ "FULL", "p.custLC = d.custLC" },
        .{ "INNER", "p.custLC = d.custLC AND p.month < d.month" },
        .{ "INNER", "p.month = d.month" },
    }) |join_case| {
        try expect_keyed_matches(allocator, db,
            \\r AS (
            \\  SELECT custLC, month, amount AS amt,
            \\         ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
            \\  FROM inv WHERE projectId = 100
            \\), j AS (
            \\  SELECT p.custLC, p.month, p.amt, p.rn, d.month AS other_month
            \\  FROM r p
        ++ " " ++ join_case[0] ++ " JOIN inv d ON " ++ join_case[1] ++ "\n" ++
            \\)
            \\SELECT custLC, month, amt, rn, other_month FROM j
            \\ORDER BY custLC, month, other_month
        , "rn");
    }
}

test "keyed region: cached inner boundaries preserve fresh outer joins and changed declarations" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE sides (id INT PRIMARY KEY, custLC VARCHAR(32), extra INT)");
    try helpers.exec(allocator, db, "INSERT INTO sides VALUES (1,'cust_0',7),(2,'outside',9)");
    const side = try db.openTable("sides", .{});
    try side.flush();
    const body =
        \\r AS (
        \\  SELECT custLC, month, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\  FROM inv WHERE projectId = 100
        \\), j AS (
        \\  SELECT p.custLC, p.month, p.rn, d.extra
        \\  FROM r p FULL JOIN sides d ON p.custLC = d.custLC
        \\)
        \\SELECT custLC, month, rn, extra FROM j ORDER BY custLC, month, extra
    ;
    const before = try run_to_text(allocator, db, "WITH KEYED BY (custLC) " ++ body, "rn");
    defer allocator.free(before);
    try expect_keyed_matches(allocator, db, body, "rn");
    try helpers.exec(allocator, db, "INSERT INTO sides VALUES (1,'cust_0',17),(3,'cust_0',27)");
    try side.flush();
    try expect_keyed_matches(allocator, db, body, "rn");
    const after = try run_to_text(allocator, db, "WITH KEYED BY (custLC) " ++ body, "rn");
    defer allocator.free(after);
    try std.testing.expect(!std.mem.eql(u8, before, after));
    try helpers.expectRunError(allocator, db, "WITH KEYED BY (month) " ++ body, error.RegionKeyContractViolation);
    try helpers.exec(allocator, db, "ALTER TABLE sides ADD COLUMN more INT DEFAULT 5");
    try expect_keyed_matches(allocator, db, body, "rn");
    try expect_keyed_matches(allocator, db,
        \\r AS (
        \\  SELECT custLC, month, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\  FROM inv WHERE projectId = 100
        \\), j AS (
        \\  SELECT p.custLC, p.month, p.rn, d.rn AS other_rn
        \\  FROM r p LEFT JOIN r d ON p.custLC = d.custLC
        \\)
        \\SELECT custLC, month, rn, other_rn FROM j ORDER BY custLC, month, other_rn
    , "rn");
}

test "keyed region: broadcast branches share windowed CTEs and refresh changed source values" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE lookup (id INT PRIMARY KEY, month INT, value BIGINT)");
    try helpers.exec(allocator, db, "INSERT INTO lookup VALUES (1,1,10),(2,2,20),(3,3,30),(4,4,40),(5,5,50)");
    const lookup = try db.openTable("lookup", .{});
    try lookup.flush();
    const body =
        \\ranked AS (
        \\  SELECT id, month, value, ROW_NUMBER() OVER (PARTITION BY month ORDER BY id) AS rn
        \\  FROM lookup
        \\), side AS (
        \\  SELECT a.month, a.value + b.value AS extra
        \\  FROM ranked a INNER JOIN ranked b ON a.month = b.month AND a.rn = b.rn
        \\), r AS (
        \\  SELECT custLC, month, amount, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\  FROM inv WHERE projectId = 100
        \\), j AS (
        \\  SELECT p.custLC, p.month, p.rn, p.amount + d.extra AS total
        \\  FROM r p LEFT JOIN side d ON p.month = d.month
        \\)
        \\SELECT custLC, month, rn, total FROM j ORDER BY custLC, month
    ;
    const before = try run_to_text(allocator, db, "WITH KEYED BY (custLC) " ++ body, "total");
    defer allocator.free(before);
    try expect_keyed_matches(allocator, db, body, "total");
    try helpers.exec(allocator, db, "INSERT INTO lookup VALUES (3,3,300)");
    try lookup.flush();
    try expect_keyed_matches(allocator, db, body, "total");
    const after = try run_to_text(allocator, db, "WITH KEYED BY (custLC) " ++ body, "total");
    defer allocator.free(after);
    try std.testing.expect(!std.mem.eql(u8, before, after));
}

test "keyed region: MAX_BY preserves computed ranking keys NULLs and extreme order keys" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE choices (id INT PRIMARY KEY, custLC VARCHAR(32), grp INT, ord BIGINT, amount DOUBLE, label VARCHAR(32))");
    try helpers.exec(allocator, db,
        \\INSERT INTO choices VALUES
        \\(1,'a',1,-9223372036854775807,1.25,'low'),
        \\(2,'a',1,9223372036854775807,9.5,'high'),
        \\(3,'a',1,NULL,3.0,'no-key'),
        \\(4,'a',1,-5,NULL,NULL),
        \\(5,'a',2,NULL,5.0,'no-key'),
        \\(6,'a',2,1,NULL,NULL),
        \\(7,'b',1,-7,2.5,'only'),
        \\(8,NULL,1,-2,4.5,'null-group')
    );
    const choices = try db.openTable("choices", .{});
    try choices.flush();
    try expect_keyed_matches(allocator, db,
        \\r AS (
        \\ SELECT custLC, grp, ord, amount, label,
        \\ ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY id) AS rn FROM choices
        \\), grouped AS (
        \\ SELECT custLC, grp, MAX_BY(amount, -rn) AS low_amount, MAX_BY(amount, ord) AS high_amount,
        \\ MAX_BY(label, -rn) AS low_label, MAX_BY(label, ord) AS high_label, MAX(rn) AS last_rank
        \\ FROM r GROUP BY custLC, grp
        \\)
        \\SELECT * FROM grouped ORDER BY custLC, grp
    , "low_amount");
}

test "keyed region: broadcast joins preserve full width composite integer keys and NULLs" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE probe_keys (id INT PRIMARY KEY, custLC VARCHAR(32), a BIGINT, b BIGINT)");
    try helpers.exec(allocator, db, "CREATE TABLE build_keys (id INT PRIMARY KEY, a BIGINT, b BIGINT, extra INT)");
    try helpers.exec(allocator, db, "INSERT INTO probe_keys VALUES (1,'p',0,4294967296),(2,'p',1,0),(3,'p',9223372036854775807,0),(4,'p',-9223372036854775807,-1),(5,'p',4,NULL)");
    try helpers.exec(allocator, db, "INSERT INTO build_keys VALUES (1,1,0,10),(2,9223372036854775807,0,20),(3,-9223372036854775807,-1,30),(4,4,NULL,40)");
    const probes = try db.openTable("probe_keys", .{});
    try probes.flush();
    const builds = try db.openTable("build_keys", .{});
    try builds.flush();
    inline for (.{ "LEFT", "INNER" }) |join_type| {
        try expect_keyed_matches(allocator, db,
            \\r AS (
            \\ SELECT id, custLC, a, b, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY id) AS rn
            \\ FROM probe_keys WHERE id > 0
            \\), j AS (
            \\ SELECT p.id, p.custLC, p.a, p.b, p.rn, d.extra FROM r p
        ++ " " ++ join_type ++ " JOIN build_keys d ON p.a = d.a AND p.b = d.b\n" ++
            \\)
            \\SELECT * FROM j ORDER BY id
        , "extra");
    }
}

test "keyed region: broadcast joins honor pinned string keys" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE pinned_lookup (id INT PRIMARY KEY, custLC VARCHAR(32), extra INT)");
    try helpers.exec(allocator, db, "INSERT INTO pinned_lookup VALUES (1,'cust_0',7),(2,'other',9)");
    const lookup = try db.openTable("pinned_lookup", .{});
    try lookup.flush();
    try expect_keyed_matches(allocator, db,
        \\r AS (
        \\ SELECT custLC, month, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\ FROM inv WHERE projectId = 100 AND custLC = 'cust_0'
        \\), j AS (
        \\ SELECT p.custLC, p.month, p.rn, d.extra FROM r p LEFT JOIN pinned_lookup d ON p.custLC = d.custLC
        \\)
        \\SELECT * FROM j ORDER BY month
    , "extra");
}

test "keyed region: broadcast joins retain nullable LARGEINT aggregate payloads" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE broadcast_values (id INT PRIMARY KEY, month INT, amount BIGINT)");
    try helpers.exec(allocator, db, "INSERT INTO broadcast_values VALUES (1,1,9000000000000000000),(2,1,9000000000000000000),(3,2,NULL),(4,3,-9000000000000000000)");
    const values = try db.openTable("broadcast_values", .{});
    try values.flush();
    try expect_keyed_matches(allocator, db,
        \\r AS (
        \\ SELECT custLC, month, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\ FROM inv WHERE projectId = 100
        \\), side AS (
        \\ SELECT month, SUM(amount) AS wide FROM broadcast_values GROUP BY month
        \\), j AS (
        \\ SELECT p.custLC, p.month, p.rn, d.wide FROM r p LEFT JOIN side d ON p.month = d.month
        \\)
        \\SELECT * FROM j ORDER BY custLC, month
    , "wide");
}

test "keyed region: qualified projection aliases follow computed output replacement" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try expect_keyed_matches(allocator, db,
        \\r AS (
        \\ SELECT custLC, month, amount, custLC AS status,
        \\ ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn FROM inv WHERE projectId = 100
        \\), z AS (
        \\ SELECT r.custLC, r.month, r.rn, r.amount + 100 AS amount, r.amount AS original_amount, r.amount + 0 AS input_amount,
        \\ CAST(CASE WHEN r.month = 1 THEN 'changed' ELSE r.status END AS VARCHAR(50)) AS status,
        \\ r.status AS original_status FROM r r
        \\)
        \\SELECT * FROM z ORDER BY custLC, month
    , "original_status");
    try expect_keyed_matches(allocator, db,
        \\r AS (
        \\ SELECT custLC, month, amount, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\ FROM inv WHERE projectId = 100
        \\), changed AS (
        \\ SELECT r.*, r.amount + 100 AS amount, r.amount AS copied FROM r r
        \\)
        \\SELECT * FROM changed ORDER BY custLC, month
    , "copied");
    try helpers.exec(allocator, db, "CREATE TABLE projection_lookup (id BIGINT PRIMARY KEY, extra INT)");
    try helpers.exec(allocator, db, "INSERT INTO projection_lookup VALUES (100,7),(101,17)");
    const lookup = try db.openTable("projection_lookup", .{});
    try lookup.flush();
    try expect_keyed_matches(allocator, db,
        \\r AS (
        \\ SELECT projectId, custLC, month, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\ FROM inv WHERE projectId = 100
        \\), changed AS (
        \\ SELECT projectId + 1 AS projectId, custLC, month, rn FROM r
        \\), j AS (
        \\ SELECT p.custLC, p.month, p.rn, d.extra FROM changed p LEFT JOIN projection_lookup d ON p.projectId = d.id
        \\)
        \\SELECT * FROM j ORDER BY custLC, month
    , "extra");
}

test "keyed region: entry computes replace input names without losing their source values" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try expect_keyed_matches(allocator, db,
        \\r AS (
        \\  SELECT LOWER(custLC) AS custLC, month, amount + 9 AS amount,
        \\         amount AS original_amount
        \\  FROM inv WHERE projectId = 100
        \\), w AS (
        \\  SELECT *, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn FROM r
        \\), m AS (
        \\  SELECT custLC, month, SUM(amount) AS amt, SUM(original_amount) AS original, MAX(rn) AS rn
        \\  FROM w GROUP BY custLC, month
        \\)
        \\SELECT custLC, month, amt, original, rn FROM m ORDER BY custLC, month
    , "amt");
}

test "keyed region: consecutive entry projections preserve aliases and compute order" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    inline for (.{
        .{ .suffix = "", .region_column = "prior" },
        .{ .suffix = " WHERE amount > 20", .region_column = "amount" },
    }) |case| {
        try expect_keyed_matches(allocator, db,
            \\base AS (
            \\  SELECT custLC, month, amount AS base_amount FROM inv WHERE projectId = 100
            \\), adjusted AS (
            \\  SELECT custLC, month, base_amount + 9 AS amount FROM base
            \\), projected AS (
            \\  SELECT custLC, month, amount AS total FROM adjusted
        ++ case.suffix ++ "\n" ++
            \\), w AS (
            \\  SELECT custLC, month, total,
            \\    LAG(total) OVER (PARTITION BY custLC ORDER BY month) AS prior
            \\  FROM projected
            \\)
            \\SELECT custLC, month, total, prior FROM w ORDER BY custLC, month
        , case.region_column);
    }
}

test "keyed region: flexible TVF lets downstream windows choose finer ranges" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    const tdb = thindb.tdb;
    const adjust = struct {
        pub const spec = tdb.TableFnSpec{ .name = "adjust", .execution = .either, .row_aligned = true };
        pub const Input = struct { amount: ?i64 };
        pub const Carry = struct { projectId: ?i64, custLC: ?[]const u8, month: ?i32 };
        pub const Output = struct { projectId: ?i64, custLC: ?[]const u8, month: ?i32, adjusted: ?i64 };
        pub const Computed = struct { adjusted: ?i64 };
        pub const passthrough = .{ "projectId", "custLC", "month" };

        pub fn process(_: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Computed)) !void {
            var rows = p.iter();
            while (rows.next()) |row| {
                try out.row(.{ .adjusted = if (row.amount) |amount| amount + 7 else null });
            }
        }
    };
    try db.registerTableFn(adjust);
    inline for (.{ " WHERE projectId >= 100", " WHERE projectId = 100" }) |filter| {
        inline for (.{ "projectId, custLC", "custLC" }) |partition| {
            try expect_keyed_matches(allocator, db,
                \\adjusted AS (
                \\  SELECT * FROM TABLE(adjust((SELECT amount, projectId, custLC, month FROM inv
            ++ filter ++
                \\)) PARTITION BY custLC)
                \\), w AS (
                \\  SELECT projectId, custLC, month, adjusted,
                \\    LAG(adjusted) OVER (PARTITION BY
            ++ " " ++ partition ++
                \\ ORDER BY month, projectId) AS prior
                \\  FROM adjusted
                \\)
                \\SELECT * FROM w ORDER BY projectId, custLC, month
            , "prior");
        }
    }
}

test "keyed region: TVF passthrough binds sources before computed output aliases" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    const tdb = thindb.tdb;
    const adjust = struct {
        pub const spec = tdb.TableFnSpec{ .name = "adjust_alias", .execution = .either, .row_aligned = true };
        pub const Input = struct { amount: ?i64 };
        pub const Carry = struct { custLC: ?[]const u8, month: ?i32, original: ?i64 };
        pub const Output = struct { amount: ?i64, custLC: ?[]const u8, month: ?i32, original: ?i64 };
        pub const Computed = struct { amount: ?i64 };
        pub const passthrough = .{ "custLC", "month", "original" };
        pub fn process(_: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Computed)) !void {
            var rows = p.iter();
            while (rows.next()) |row|
                try out.row(.{ .amount = if (row.amount) |amount| amount + 7 else null });
        }
    };
    var descriptor = tdb.descriptorFor(adjust);
    var pairs: [3]thindb.udf.PassPair = undefined;
    @memcpy(&pairs, descriptor.passthrough);
    for (&pairs) |*pair| {
        if (pair.out_idx == 3) pair.in_idx = 0;
    }
    descriptor.passthrough = &pairs;
    try db.registerTableUdf(descriptor);
    try expect_keyed_matches(allocator, db,
        \\r AS (
        \\ SELECT * FROM TABLE(adjust_alias((
        \\   SELECT amount, custLC, month, amount + 100 AS original FROM inv WHERE projectId = 100
        \\ )) PARTITION BY custLC)
        \\), w AS (
        \\ SELECT custLC, month, amount, original,
        \\        LAG(original) OVER (PARTITION BY custLC ORDER BY month) AS prior
        \\ FROM r
        \\)
        \\SELECT * FROM w ORDER BY custLC, month
    , "prior");
}

test "keyed region: LAG honors partition boundaries offsets source NULLs and independent orders" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    try helpers.exec(allocator, db, "INSERT INTO inv VALUES (1001,100,'cust_0',9,NULL),(1002,100,'cust_0',8,400)");
    const table = try db.openTable("inv", .{});
    try table.flush();
    inline for (.{ "0", "1", "3", "99" }) |offset| {
        try expect_keyed_matches(allocator, db,
            \\w AS (
            \\  SELECT custLC, month, amount,
            \\    ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn,
            \\    LAG(amount,
        ++ offset ++
            \\) OVER (PARTITION BY custLC ORDER BY month DESC) AS lagged,
            \\    LAG(custLC) OVER (PARTITION BY custLC ORDER BY amount, month DESC) AS prior_customer
            \\  FROM inv WHERE projectId = 100
            \\)
            \\SELECT custLC, month, amount, rn, lagged, prior_customer FROM w ORDER BY custLC, month
        , "lagged");
    }
}

test "keyed region: BIGINT sums preserve widening and values beyond i64" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try helpers.exec(
        allocator,
        db,
        "INSERT INTO inv VALUES (1001,100,'wide',1,9223372036854775807),(1002,100,'wide',1,9223372036854775807)",
    );
    const table = try db.openTable("inv", .{});
    try table.flush();
    try expect_keyed_matches(allocator, db,
        \\r AS (
        \\  SELECT custLC, month, amount,
        \\    ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY id) AS rn
        \\  FROM inv WHERE projectId = 100
        \\), m AS (
        \\  SELECT custLC, month, SUM(amount) AS total, MAX(rn) AS rn
        \\  FROM r GROUP BY custLC, month
        \\)
        \\SELECT custLC, month, total, rn FROM m ORDER BY custLC, month
    , "total");
}

test "keyed region: shadowed filter keys do not retain stale literal values" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try expect_keyed_matches(allocator, db,
        \\r AS (
        \\  SELECT projectId + 1 AS projectId, custLC, month, amount
        \\  FROM inv WHERE projectId = 100
        \\), w AS (
        \\  SELECT projectId, custLC, month, amount,
        \\    ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\  FROM r
        \\), j AS (
        \\  SELECT p.custLC, p.month, p.amount, p.rn, d.amount AS other_amount
        \\  FROM w p LEFT JOIN inv d ON p.custLC = d.custLC AND p.projectId = d.projectId AND p.month = d.month
        \\)
        \\SELECT custLC, month, amount, rn, other_amount FROM j ORDER BY custLC, month
    , "other_amount");
}

test "keyed region: LAG uses substituted offsets across cached executions" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    const body =
        \\w AS (
        \\  SELECT custLC, month, LAG(amount, @distance) OVER (PARTITION BY custLC ORDER BY month) AS prior
        \\  FROM inv WHERE projectId = 100
        \\)
        \\SELECT custLC, month, prior FROM w ORDER BY custLC, month
    ;
    inline for (.{ "1", "3", "3", "1" }) |offset| {
        const prefix = "SET @distance = " ++ offset ++ "; WITH ";
        const mono = try runToText(allocator, db, prefix ++ body);
        defer allocator.free(mono);
        const keyed = try run_to_text(allocator, db, prefix ++ "KEYED BY (custLC) " ++ body, "prior");
        defer allocator.free(keyed);
        try std.testing.expectEqualStrings(mono, keyed);
    }
}

test "keyed region: UNION ALL preserves overlapping rows NULLs and downstream groups" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "INSERT INTO inv VALUES (1001,100,NULL,9,NULL),(1002,101,'cust_0',9,NULL)");
    const table = try db.openTable("inv", .{});
    try table.flush();
    inline for (.{ "projectId = 100", "projectId = 101", "projectId = 999" }) |right_filter| {
        try expect_keyed_matches(allocator, db,
            \\u AS (
            \\  SELECT custLC, id, month, amount FROM inv WHERE projectId = 100
            \\  UNION ALL
            \\  SELECT custLC, id, month, amount FROM inv WHERE
        ++ " " ++ right_filter ++
            \\), w AS (
            \\  SELECT *, LAG(amount) OVER (PARTITION BY custLC, month ORDER BY id) AS prior FROM u
            \\), g AS (
            \\  SELECT custLC, month, SUM(amount) AS total, SUM(prior) AS previous
            \\  FROM w GROUP BY custLC, month
            \\)
            \\SELECT * FROM g ORDER BY custLC, month
        , "total");
    }
    const counts = try run_to_text(allocator, db,
        \\WITH KEYED BY (custLC) u AS (
        \\  SELECT custLC, month, amount FROM inv WHERE id = 1001
        \\  UNION ALL SELECT custLC, month, amount FROM inv WHERE id = 1001
        \\), g AS (SELECT custLC, SUM(month) AS n, SUM(amount) AS total FROM u GROUP BY custLC)
        \\SELECT n, total FROM g
    , "n");
    defer allocator.free(counts);
    try std.testing.expect(std.mem.endsWith(u8, counts, "18|~\n"));
}

test "keyed region: UNION ALL positional names numeric widening and ordered entry expressions" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    inline for (.{ "projectId = 100", "projectId = 999" }) |left_filter| {
        inline for (.{ "projectId = 101", "projectId = 999" }) |right_filter| {
            try expect_keyed_matches(allocator, db,
                \\u AS (
                \\  SELECT custLC, id, month AS amount FROM inv WHERE
            ++ " " ++ left_filter ++
                \\  UNION ALL
                \\  SELECT custLC AS other_key, id AS other_id, amount AS other_value FROM inv WHERE
            ++ " " ++ right_filter ++
                \\  UNION ALL
                \\  SELECT custLC, id, amount FROM inv WHERE projectId = 999
                \\), adjusted AS (
                \\  SELECT custLC, id, amount + 7 AS amount, amount AS original FROM u
                \\), filtered AS (
                \\  SELECT custLC, id, amount AS total, original FROM adjusted WHERE amount > 8
                \\), w AS (
                \\  SELECT *, LAG(total) OVER (PARTITION BY custLC ORDER BY id) AS prior FROM filtered
                \\)
                \\SELECT * FROM w ORDER BY custLC, id
            , "prior");
        }
    }
}

test "keyed region: UNION ALL shared materialized branches retain their SQL results" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    inline for (.{ "", "MATERIALIZED " }) |hint| {
        try expect_keyed_matches(allocator, db, "base AS " ++ hint ++
            \\(
            \\  SELECT custLC, id, amount,
            \\    ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY id) AS original_rank
            \\  FROM inv WHERE projectId = 100
            \\), u AS (
            \\  SELECT custLC, id, amount, original_rank FROM base WHERE id < 20
            \\  UNION ALL
            \\  SELECT custLC, id, amount, original_rank FROM base WHERE id >= 20
            \\), w AS (
            \\  SELECT *, LAG(amount) OVER (PARTITION BY custLC ORDER BY id DESC) AS prior FROM u
            \\)
            \\SELECT * FROM w ORDER BY custLC, id
        , "prior");
    }
}

test "keyed region: UNION ALL cached executions refresh both branches and their schemas" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE right_arm (id BIGINT PRIMARY KEY, custLC VARCHAR(32), amount INT)");
    try helpers.exec(allocator, db, "INSERT INTO right_arm VALUES (1001,'cust_0',900)");
    const right = try db.openTable("right_arm", .{});
    try right.flush();
    const body =
        \\u AS (
        \\  SELECT custLC, id, month AS amount FROM inv WHERE projectId = 100
        \\  UNION ALL SELECT custLC, id, amount FROM right_arm
        \\), w AS (
        \\  SELECT *, LAG(amount) OVER (PARTITION BY custLC ORDER BY id DESC) AS prior FROM u
        \\)
        \\SELECT * FROM w ORDER BY custLC, id
    ;
    try expect_keyed_matches(allocator, db, body, "prior");
    try helpers.exec(allocator, db, "INSERT INTO right_arm VALUES (1002,'cust_0',NULL),(1003,NULL,901)");
    try right.flush();
    try expect_keyed_matches(allocator, db, body, "prior");
    try helpers.exec(allocator, db, "INSERT INTO inv VALUES (2001,100,'cust_0',9,1234)");
    const left = try db.openTable("inv", .{});
    try left.flush();
    try expect_keyed_matches(allocator, db, body, "prior");
    try helpers.exec(allocator, db, "ALTER TABLE right_arm ADD COLUMN extra BIGINT DEFAULT 4");
    try expect_keyed_matches(allocator, db, body, "prior");
    try helpers.exec(allocator, db, "DROP TABLE right_arm");
    try helpers.exec(allocator, db, "CREATE TABLE right_arm (id BIGINT PRIMARY KEY, custLC VARCHAR(32), amount BIGINT)");
    try helpers.exec(allocator, db, "INSERT INTO right_arm VALUES (1001,'cust_0',9223372036854775807)");
    const replacement = try db.openTable("right_arm", .{});
    try replacement.flush();
    try expect_keyed_matches(allocator, db, body, "prior");
    try helpers.expectRunError(allocator, db, "WITH KEYED BY (id) " ++ body, error.RegionUnsupportedConstruct);
    try helpers.expectRunError(allocator, db,
        \\WITH KEYED BY (custLC) u AS (
        \\  SELECT custLC, amount FROM inv UNION ALL SELECT custLC FROM right_arm
        \\), g AS (SELECT custLC, SUM(amount) AS total FROM u GROUP BY custLC)
        \\SELECT * FROM g
    , error.RegionUnsupportedConstruct);
}
