//! End-to-end coverage for `WITH KEYED BY (...)` pipeline regions: the
//! declared-block builder compiling real SQL through the region path,
//! validated by value-equality against the identical pipeline without the
//! declaration (mono engine). Data is tie-free within each key partition so
//! both paths are deterministic and comparable row-for-row. Also covers the
//! ordinary fallback for incompatible partitions, regional reentry across
//! CTE boundaries, explicit frame results, and NULL-key rows.

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
    return run_to_text_checked(allocator, db, sql, region_column, true);
}

fn run_to_text_checked(allocator: std.mem.Allocator, db: anytype, sql: []const u8, region_column: ?[]const u8, require_region: bool) ![]u8 {
    return run_to_text_program(allocator, db, sql, region_column, require_region, null);
}

const RegionProgram = *const thindb.exec.region_exec.Program;

/// `program` receives the region's compiled program: the same one for runs
/// that share a cached program.
fn run_to_text_program(allocator: std.mem.Allocator, db: anytype, sql: []const u8, region_column: ?[]const u8, require_region: bool, program: ?*?RegionProgram) ![]u8 {
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
            if (!stage.is_keyed_region) continue;
            region_found = true;
            if (program) |out| {
                try std.testing.expect(stage.query_alive);
                const op = thindb.exec.queryAs(thindb.exec.region_exec.RegionExecOp, stage.query) orelse
                    return error.TestExpectedEqual;
                out.* = op.prog;
            }
        }
        try std.testing.expectEqual(require_region, region_found);
    }
    const schema = q.outputSchema();
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (schema) |col| try out.print(allocator, "{s}:{any}{s}\n", .{ col.name, col.type, if (col.nullable) "" else " NOT NULL" });
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
                    .date => try out.print(allocator, "{d}", .{v.data.date[r]}),
                    .datetime => try out.print(allocator, "{d}", .{v.data.datetime[r]}),
                    .boolean => try out.print(allocator, "{d}", .{v.data.boolean[r]}),
                    .double => try out.print(allocator, "{d}", .{v.data.double[r]}),
                    .decimal64 => try out.print(allocator, "{d}", .{v.data.decimal64[r]}),
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

test "keyed region: a DATETIME bound on a DATE column prunes the entry scan in the column's type" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE ev (id BIGINT PRIMARY KEY, custLC STRING, day DATE, amount BIGINT)");
    try helpers.exec(allocator, db,
        \\INSERT INTO ev VALUES
        \\ (1,'cust_0','2025-11-30',10),(2,'cust_0','2025-12-01',20),(3,'cust_0','2026-01-15',30),
        \\ (4,'cust_1','2025-12-20',40),(5,'cust_1','2026-02-28',50),(6,NULL,'2026-01-10',60)
    );
    const ev = try db.openTable("ev", .{});
    try ev.flush();
    // Segment and row-group stats hold days; an unplaced DATETIME bound
    // would read as microseconds and prune every row group.
    inline for (.{
        "day >= DATE_ADD('2025-12-01', INTERVAL 0 MONTH)",
        "day >= DATE_ADD('2025-11-30 12:00:00', INTERVAL 0 DAY) AND day <= DATE_ADD('2026-02-28', INTERVAL 0 DAY)",
        "day BETWEEN DATE_ADD('2026-02-01', INTERVAL -2 MONTH) AND DATE_ADD('2026-02-01', INTERVAL 0 MONTH)",
    }) |cond| {
        const body =
            \\base AS (
            \\ SELECT custLC, day, amount FROM ev WHERE
        ++ " " ++ cond ++
            \\
            \\), w AS (
            \\ SELECT custLC, day, amount, LAG(amount) OVER (PARTITION BY custLC ORDER BY day) AS prior FROM base
            \\)
            \\SELECT * FROM w ORDER BY custLC, day
        ;
        const mono = try runToText(allocator, db, "WITH " ++ body);
        defer allocator.free(mono);
        try std.testing.expect(std.mem.count(u8, mono, "\n") >= 4 + 3);
        try expect_keyed_matches(allocator, db, body, "prior");
    }
}

/// The keyed result of `body` with `{lo}`/`{hi}`/`{project}` filled in, after
/// checking it against ordinary execution; `program` receives the region's
/// compiled program.
fn keyed_range_run(allocator: std.mem.Allocator, db: *thindb.Database, comptime body: []const u8, project: u32, lo: u8, hi: u8, program: *?RegionProgram) !void {
    var buf: [2048]u8 = undefined;
    var filled: std.Io.Writer = .fixed(&buf);
    var rest: []const u8 = body;
    while (std.mem.indexOfScalar(u8, rest, '{')) |open| {
        const close = std.mem.indexOfScalarPos(u8, rest, open, '}').?;
        try filled.writeAll(rest[0..open]);
        const name = rest[open + 1 .. close];
        if (std.mem.eql(u8, name, "lo")) try filled.print("'cust_{c}'", .{lo});
        if (std.mem.eql(u8, name, "hi")) try filled.print("'cust_{c}'", .{hi});
        if (std.mem.eql(u8, name, "project")) try filled.print("{d}", .{project});
        rest = rest[close + 1 ..];
    }
    try filled.writeAll(rest);
    const sql = filled.buffered();
    var mono_buf: [2100]u8 = undefined;
    const mono = try runToText(allocator, db, try std.fmt.bufPrint(&mono_buf, "WITH {s}", .{sql}));
    defer allocator.free(mono);
    var keyed_buf: [2100]u8 = undefined;
    const keyed_sql = try std.fmt.bufPrint(&keyed_buf, "WITH KEYED BY (custLC) {s}", .{sql});
    const keyed = try run_to_text_program(allocator, db, keyed_sql, "prior", true, program);
    defer allocator.free(keyed);
    try std.testing.expectEqualStrings(mono, keyed);
}

test "keyed region: runs differing only in the entry scan's range share one program" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();

    const plain =
        \\base AS (
        \\  SELECT custLC, month, amount FROM inv
        \\  WHERE projectId = {project} AND custLC >= {lo} AND custLC < {hi}
        \\), w AS (
        \\  SELECT custLC, month, amount, LAG(amount) OVER (PARTITION BY custLC ORDER BY month) AS prior FROM base
        \\)
        \\SELECT * FROM w ORDER BY custLC, month
    ;
    var first: ?RegionProgram = null;
    var second: ?RegionProgram = null;
    var again: ?RegionProgram = null;
    var pinned: ?RegionProgram = null;
    try keyed_range_run(allocator, db, plain, 100, '0', '3', &first);
    try keyed_range_run(allocator, db, plain, 100, '3', '8', &second);
    try std.testing.expectEqual(first.?, second.?);
    // A repeated range comes back through its remembered boundary.
    try keyed_range_run(allocator, db, plain, 100, '0', '3', &again);
    try std.testing.expectEqual(first.?, again.?);
    // An equality pin is baked into the program, so it stays in the key.
    try keyed_range_run(allocator, db, plain, 101, '3', '8', &pinned);
    try std.testing.expect(pinned.? != first.?);

    // Subtrees read at compile time — a co-partitioned side and a broadcast
    // join — keep the rows of the range they read: no sharing.
    inline for (.{
        \\base AS (
        \\  SELECT custLC, month, amount FROM inv
        \\  WHERE projectId = {project} AND custLC >= {lo} AND custLC < {hi}
        \\), tot AS (
        \\  SELECT custLC, SUM(amount) AS total FROM base GROUP BY custLC
        \\), w AS (
        \\  SELECT b.custLC, b.month, b.amount, t.total,
        \\         LAG(b.amount) OVER (PARTITION BY b.custLC ORDER BY b.month) AS prior
        \\  FROM base b LEFT JOIN tot t ON b.custLC = t.custLC
        \\)
        \\SELECT * FROM w ORDER BY custLC, month
        ,
        \\base AS (
        \\  SELECT custLC, month, amount FROM inv
        \\  WHERE projectId = {project} AND custLC >= {lo} AND custLC < {hi}
        \\), mt AS (
        \\  SELECT month, SUM(amount) AS mtotal FROM base GROUP BY month
        \\), w AS (
        \\  SELECT b.custLC, b.month, b.amount, m.mtotal,
        \\         LAG(b.amount) OVER (PARTITION BY b.custLC ORDER BY b.month) AS prior
        \\  FROM base b LEFT JOIN mt m ON b.month = m.month
        \\)
        \\SELECT * FROM w ORDER BY custLC, month
    }) |body| {
        var a: ?RegionProgram = null;
        var b: ?RegionProgram = null;
        try keyed_range_run(allocator, db, body, 100, '0', '3', &a);
        try keyed_range_run(allocator, db, body, 100, '3', '8', &b);
        try std.testing.expect(a.? != b.?);
    }
}

test "keyed region: incompatible partition uses ordinary execution" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    try expect_fallback_matches(allocator, db, "custLC",
        \\r AS (
        \\  SELECT custLC, month,
        \\         ROW_NUMBER() OVER (PARTITION BY month ORDER BY custLC) AS rn
        \\  FROM inv
        \\)
        \\SELECT custLC, SUM(rn) AS s FROM r GROUP BY custLC ORDER BY custLC ASC
    );
}

fn expect_fallback_matches(allocator: std.mem.Allocator, db: *thindb.Database, comptime keys: []const u8, comptime body: []const u8) !void {
    const mono = try runToText(allocator, db, "WITH " ++ body);
    defer allocator.free(mono);
    for (0..2) |_| {
        const keyed = try run_to_text_checked(allocator, db, "WITH KEYED BY (" ++ keys ++ ") " ++ body, null, false);
        defer allocator.free(keyed);
        try std.testing.expectEqualStrings(mono, keyed);
    }
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
    try expect_fallback_matches(allocator, db, "month", body);
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
        \\(9,'a',2,0,7.0,'mid'),
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

// DATE_ADD over a DATE yields a DATETIME, so a previous-month lookup pairs a
// DATETIME probe with a DATE build column: days against microseconds unless
// the region meets them at midnight as ordinary execution does.
test "keyed region: DATE and DATETIME join keys meet at midnight" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE mrr (id BIGINT PRIMARY KEY, custLC VARCHAR(32), month DATE, amount BIGINT)");
    try helpers.exec(allocator, db, "CREATE TABLE mrr_at (id BIGINT PRIMARY KEY, custLC VARCHAR(32), at DATETIME, amount BIGINT)");
    try helpers.exec(allocator, db, "CREATE TABLE cal (id INT PRIMARY KEY, month DATE, at DATETIME, label INT)");
    try helpers.exec(allocator, db,
        \\INSERT INTO mrr VALUES
        \\ (1,'cust_0','2025-10-01',10),(2,'cust_0','2025-11-01',20),(3,'cust_0','2026-01-01',30),
        \\ (4,'cust_1','2025-11-01',40),(5,'cust_1','2025-12-01',50),(6,'cust_1','2026-01-01',60),
        \\ (7,'cust_2','2025-12-01',70),(8,NULL,'2025-12-01',80),(9,'cust_3',NULL,90)
    );
    try helpers.exec(allocator, db,
        \\INSERT INTO mrr_at VALUES
        \\ (1,'cust_0','2025-11-01 00:00:00',11),(2,'cust_1','2025-12-01 06:00:00',12),
        \\ (3,'cust_1','2026-01-01 00:00:00',13),(4,'cust_2','2025-12-01 00:00:00',14)
    );
    try helpers.exec(allocator, db,
        \\INSERT INTO cal VALUES
        \\ (1,'2025-09-01','2025-10-01 00:00:00',1),(2,'2025-10-01','2025-11-01 00:00:00',2),
        \\ (3,'2025-11-01','2025-12-01 06:00:00',3),(4,'2025-12-01','2026-01-01 00:00:00',4)
    );
    inline for (.{ "mrr", "mrr_at", "cal" }) |name| {
        const t = try db.openTable(name, .{});
        try t.flush();
    }

    const base =
        \\r AS (
        \\ SELECT custLC, month, amount, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\ FROM mrr WHERE id > 0
        \\), totals AS (
        \\ SELECT custLC, month, amount FROM mrr WHERE id > 0
        \\), totals_at AS (
        \\ SELECT custLC, at, amount FROM mrr_at WHERE id > 0
        \\), days AS (
        \\ SELECT month, at, label FROM cal WHERE id > 0
        \\), j AS (
        \\ SELECT p.custLC, p.month, p.rn,
    ;
    const tail =
        \\
        \\)
        \\SELECT * FROM j ORDER BY custLC, month
    ;
    inline for (.{
        // Co-partitioned side, DATETIME probe against a DATE build.
        .{ " x.amount AS v FROM r p", " LEFT JOIN totals x ON x.custLC = p.custLC AND x.month = DATE_ADD(p.month, INTERVAL -1 MONTH)" },
        // Co-partitioned side, DATE probe against a DATETIME build with a row off midnight.
        .{ " x.amount AS v FROM r p", " LEFT JOIN totals_at x ON x.custLC = p.custLC AND x.at = p.month" },
        // Broadcast sides, single and composite keys, both directions.
        .{ " x.label AS v FROM r p", " LEFT JOIN days x ON x.month = DATE_ADD(p.month, INTERVAL -1 MONTH)" },
        .{ " x.label AS v FROM r p", " LEFT JOIN days x ON x.at = p.month" },
        .{ " x.label AS v FROM r p", " LEFT JOIN days x ON x.at = p.month AND x.label = p.rn" },
        .{ " x.label AS v FROM r p", " INNER JOIN days x ON x.at = p.month" },
    }) |case| {
        const body = base ++ case[0] ++ case[1] ++ tail;
        const mono = try runToText(allocator, db, "WITH " ++ body);
        defer allocator.free(mono);
        try std.testing.expect(std.mem.count(u8, mono, "\n") > 2);
        try expect_keyed_matches(allocator, db, body, "v");
    }
}

// The exchange hashes each side's route cell bytes; an INT route key and a
// BIGINT side column hold equal keys in different widths.
test "keyed region: co-partitioned sides with differently typed route keys keep every match" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE narrow_keys (id BIGINT PRIMARY KEY, cust INT, month INT, amount BIGINT)");
    try helpers.exec(allocator, db, "CREATE TABLE wide_keys (id BIGINT PRIMARY KEY, cust BIGINT, month INT, extra BIGINT)");
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    var wide: std.ArrayList(u8) = .empty;
    defer wide.deinit(allocator);
    try sql.appendSlice(allocator, "INSERT INTO narrow_keys VALUES ");
    try wide.appendSlice(allocator, "INSERT INTO wide_keys VALUES ");
    var id: i64 = 1;
    for (0..40) |c| {
        for (0..3) |m| {
            if (id > 1) {
                try sql.appendSlice(allocator, ",");
                try wide.appendSlice(allocator, ",");
            }
            try sql.print(allocator, "({d},{d},{d},{d})", .{ id, c, m, id * 3 });
            try wide.print(allocator, "({d},{d},{d},{d})", .{ id, c, m, id * 5 });
            id += 1;
        }
    }
    try helpers.exec(allocator, db, sql.items);
    try helpers.exec(allocator, db, wide.items);
    inline for (.{ "narrow_keys", "wide_keys" }) |name| {
        const t = try db.openTable(name, .{});
        try t.flush();
    }
    const body =
        \\r AS (
        \\ SELECT cust, month, amount, ROW_NUMBER() OVER (PARTITION BY cust ORDER BY month) AS rn
        \\ FROM narrow_keys WHERE id > 0
        \\), w AS (
        \\ SELECT cust, month, extra FROM wide_keys WHERE id > 0
        \\), j AS (
        \\ SELECT p.cust, p.month, p.rn, x.extra AS v FROM r p
        \\ LEFT JOIN w x ON x.cust = p.cust AND x.month = p.month
        \\)
        \\SELECT * FROM j ORDER BY cust, month
    ;
    const mono = try runToText(allocator, db, "WITH " ++ body);
    defer allocator.free(mono);
    const keyed = try run_to_text_checked(allocator, db, "WITH KEYED BY (cust) " ++ body, null, true);
    defer allocator.free(keyed);
    try std.testing.expectEqualStrings(mono, keyed);
}

// SUM(amount) for month 1 wraps past BIGINT max; both paths must wrap alike.
test "keyed region: broadcast joins retain nullable wrapped BIGINT SUM payloads" {
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
    // The entry filter's pin on projectId must not follow the name to the
    // column renamed onto it.
    try expect_keyed_matches(allocator, db,
        \\r AS (
        \\ SELECT projectId, custLC, month, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\ FROM inv WHERE projectId = 100
        \\), changed AS (
        \\ SELECT custLC, month, rn, rn + 99 AS k FROM r
        \\), renamed AS (
        \\ SELECT custLC, month, rn, k AS projectId FROM changed
        \\), j AS (
        \\ SELECT p.custLC, p.month, p.rn, d.extra FROM renamed p LEFT JOIN projection_lookup d ON p.projectId = d.id
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
        .{ .suffix = " WHERE amount > 20", .region_column = "prior" },
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

const month_step = struct {
    const tdb = thindb.tdb;
    pub const spec = tdb.TableFnSpec{ .name = "month_step", .execution = .either, .row_aligned = true };
    pub const Input = struct { amount: ?i64, day: ?tdb.Date };
    pub const Carry = struct { projectId: ?i64, custLC: ?[]const u8, month: ?i32 };
    pub const Output = struct { projectId: ?i64, custLC: ?[]const u8, month: ?i32, day: ?tdb.Date, stepped: ?i64 };
    pub const Computed = struct { stepped: ?i64 };
    pub const passthrough = .{ "projectId", "custLC", "month", "day" };
    pub fn process(_: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Computed)) !void {
        var rows = p.iter();
        while (rows.next()) |row| {
            const amount = row.amount orelse {
                try out.row(.{ .stepped = null });
                continue;
            };
            const day = row.day orelse {
                try out.row(.{ .stepped = null });
                continue;
            };
            try out.row(.{ .stepped = amount * 100_000 + day.days() });
        }
    }
};

test "keyed region: TVF inputs convert to their declared types as in ordinary execution" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try db.registerTableFn(month_step);
    const tail =
        \\    FROM inv WHERE projectId >= 100
        \\  )) PARTITION BY custLC)
        \\), w AS (
        \\  SELECT projectId, custLC, month, day, stepped,
        \\    LAG(stepped) OVER (PARTITION BY custLC ORDER BY month, projectId) AS prior
        \\  FROM stepped
        \\)
        \\SELECT * FROM w ORDER BY projectId, custLC, month
    ;
    // `day` is a DATETIME (DATE_ADD on a DATE) declared DATE: the region
    // converts it with the operator's cast.
    try expect_keyed_matches(allocator, db,
        \\stepped AS (
        \\  SELECT * FROM TABLE(month_step((
        \\    SELECT amount, DATE_ADD(DATE '2026-01-31', INTERVAL month DAY) AS day, projectId, custLC, month
        \\
    ++ tail, "prior");
    // Text that may not be a date (checked value by value) and a SMALLINT
    // into INT (converted as INSERT does, row by row) convert in the
    // ordinary operator; the keyed statement runs there too.
    try expect_fallback_matches(allocator, db, "custLC",
        \\stepped AS (
        \\  SELECT * FROM TABLE(month_step((
        \\    SELECT amount, CONCAT('2026-01-0', CAST(month AS VARCHAR(2))) AS day, projectId, custLC, month
        \\
    ++ tail);
    try expect_fallback_matches(allocator, db, "custLC",
        \\stepped AS (
        \\  SELECT * FROM TABLE(month_step((
        \\    SELECT amount, DATE_ADD(DATE '2026-01-31', INTERVAL month DAY) AS day, projectId, custLC,
        \\           CAST(month AS SMALLINT) AS month
        \\
    ++ tail);
    // A refused conversion (BIGINT into DATE) fails either way.
    inline for (.{ "WITH ", "WITH KEYED BY (custLC) " }) |head| {
        var q = helpers.runSql(allocator, db, head ++
            \\stepped AS (
            \\  SELECT * FROM TABLE(month_step((
            \\    SELECT amount, amount AS day, projectId, custLC, month FROM inv WHERE projectId >= 100
            \\  )) PARTITION BY custLC)
            \\)
            \\SELECT * FROM stepped
        );
        if (q) |*ok| {
            ok.deinit();
            return error.TestUnexpectedSuccess;
        } else |err| try std.testing.expectEqual(thindb.exec.Error.TableFnInputMismatch, err);
    }
}

/// The call shape of a production up/down chain: `.either`, row-aligned,
/// a DATE `month` order key, partitioned finer than the declared key.
const month_chain = struct {
    const tdb = thindb.tdb;
    pub const spec = tdb.TableFnSpec{ .name = "month_chain", .execution = .either, .row_aligned = true };
    pub const Input = struct { projectId: ?i64, custLC: ?[]const u8, month: ?tdb.Date, amount: ?i64 };
    pub const Output = struct { projectId: ?i64, custLC: ?[]const u8, month: ?tdb.Date, amount: ?i64, lastAmount: ?i64, firstMonth: ?tdb.Date };
    pub const Computed = struct { lastAmount: ?i64, firstMonth: ?tdb.Date };
    pub const passthrough = .{ "projectId", "custLC", "month", "amount" };
    pub fn process(_: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Computed)) !void {
        const months = p.col(.month);
        const amounts = p.col(.amount);
        var last: ?i64 = null;
        for (0..p.len) |i| {
            try out.row(.{ .lastAmount = last, .firstMonth = months.get(0) });
            last = amounts.get(i);
        }
    }
};

test "keyed region: a DATETIME month converted to a DATE order key keeps the region" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try db.registerTableFn(month_chain);
    {
        var q = try helpers.runSql(allocator, db, "SELECT DATE_ADD(DATE '2025-12-01', INTERVAL 1 MONTH) AS m");
        defer q.deinit();
        try std.testing.expectEqual(thindb.types.Type.datetime, q.outputSchema()[0].type);
    }
    // The region sorts each range by the supplied month and converts it
    // after: a DATETIME's day keeps that order, as a DATE month has it.
    inline for (.{
        "DATE_ADD(DATE '2025-12-01', INTERVAL month MONTH)",
        "CAST(DATE_ADD(DATE '2025-12-01', INTERVAL month MONTH) AS DATE)",
    }) |month| {
        try expect_keyed_matches(allocator, db,
            \\chain AS (
            \\  SELECT * FROM TABLE(month_chain((
            \\    SELECT projectId, custLC,
        ++ " " ++ month ++ " AS month, " ++
            \\amount FROM inv WHERE projectId >= 100
            \\  )) PARTITION BY projectId, custLC ORDER BY month)
            \\), w AS (
            \\  SELECT projectId, custLC, month, amount, lastAmount, firstMonth,
            \\    LAG(lastAmount) OVER (PARTITION BY custLC ORDER BY month, projectId) AS prior
            \\  FROM chain
            \\)
            \\SELECT * FROM w ORDER BY projectId, custLC, month
        , "prior");
    }
}

/// A row-aligned kernel with a broadcast lookup keyed by a declared string.
const broadcast_scale = struct {
    const tdb = thindb.tdb;
    pub const spec = tdb.TableFnSpec{ .name = "broadcast_scale", .execution = .either, .row_aligned = true, .broadcast_inputs = &.{1} };
    pub const Input = struct { custLC: ?[]const u8, amount: ?i64 };
    pub const Carry = struct { projectId: ?i64, month: ?i32 };
    pub const Input2 = struct { code: ?[]const u8, mult: ?i64 };
    pub const Output = struct { projectId: ?i64, custLC: ?[]const u8, month: ?i32, scaled: ?i64 };
    pub const Computed = struct { scaled: ?i64 };
    pub const passthrough = .{ "projectId", "custLC", "month" };
    pub fn process(_: *tdb.Ctx, p: tdb.Partition(Input), scales: tdb.Partition(Input2), out: *tdb.Writer(Computed)) !void {
        const codes = scales.col(.code);
        const mults = scales.col(.mult);
        var rows = p.iter();
        while (rows.next()) |row| {
            var mult: ?i64 = null;
            if (row.custLC) |lc| {
                for (0..scales.len) |i| {
                    const code = codes.get(i) orelse continue;
                    if (std.mem.eql(u8, code, lc)) mult = mults.get(i);
                }
            }
            const amount = row.amount orelse {
                try out.row(.{ .scaled = null });
                continue;
            };
            try out.row(.{ .scaled = if (mult) |m| amount * m else null });
        }
    }
};

test "keyed region: broadcast inputs bind VARCHAR and TEXT columns to a declared string as in ordinary execution" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try db.registerTableFn(broadcast_scale);
    try helpers.exec(allocator, db, "CREATE TABLE scale_varchar (code VARCHAR(16) PRIMARY KEY, mult BIGINT)");
    try helpers.exec(allocator, db, "CREATE TABLE scale_text (id INT PRIMARY KEY, code TEXT, mult BIGINT)");
    // cust_3 has no scale; cust_6 has a NULL one.
    try helpers.exec(allocator, db, "INSERT INTO scale_varchar VALUES ('cust_0',2),('cust_1',3),('cust_2',4),('cust_4',6),('cust_5',7),('cust_6',NULL),('cust_7',9)");
    try helpers.exec(allocator, db, "INSERT INTO scale_text VALUES (1,'cust_0',2),(2,'cust_1',3),(3,'cust_2',4),(4,'cust_4',6),(5,'cust_5',7),(6,'cust_6',NULL),(7,'cust_7',9)");
    inline for (.{ "scale_varchar", "scale_text" }) |table| {
        try expect_keyed_matches(allocator, db,
            \\scaled AS (
            \\  SELECT * FROM TABLE(broadcast_scale(
            \\    (SELECT custLC, amount, projectId, month FROM inv WHERE projectId >= 100),
        ++ " (SELECT code, mult FROM " ++ table ++ ")" ++
            \\  ) PARTITION BY custLC)
            \\), w AS (
            \\  SELECT projectId, custLC, month, scaled,
            \\    LAG(scaled) OVER (PARTITION BY custLC ORDER BY month, projectId) AS prior
            \\  FROM scaled
            \\)
            \\SELECT * FROM w ORDER BY projectId, custLC, month
        , "prior");
    }
}

test "keyed region: route provenance survives replacing TVFs and aggregation in one region" {
    const allocator = std.testing.allocator;
    const tdb = thindb.tdb;
    const expand_rows = struct {
        pub const spec = tdb.TableFnSpec{ .name = "expand_rows", .execution = .partitioned, .ordered_output = true };
        pub const Input = struct { custLC: ?[]const u8, month: ?i32, amount: ?i64 };
        pub const Output = Input;
        pub fn process(_: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Output)) !void {
            var rows = p.iter();
            while (rows.next()) |row| {
                const month = if (row.month) |m| m * 2 else null;
                try out.row(.{ .custLC = row.custLC, .month = month, .amount = row.amount });
                try out.row(.{ .custLC = row.custLC, .month = if (month) |m| m + 1 else null, .amount = null });
            }
        }
    };
    const body =
        \\seed AS (
        \\ SELECT custLC, month, amount,
        \\   ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS seed_rank
        \\ FROM inv WHERE projectId = 100
        \\), expanded AS (
        \\ SELECT * FROM TABLE(expand_rows((SELECT custLC, month, amount + seed_rank AS amount FROM seed))
        \\   PARTITION BY custLC ORDER BY month)
        \\), ranked AS (
        \\ SELECT *, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS mid_rank FROM expanded
        \\), grouped AS (
        \\ SELECT custLC, month, MAX(amount) AS amount, MAX(mid_rank) AS mid_rank
        \\ FROM ranked GROUP BY custLC, month
        \\), joined AS (
        \\ SELECT g.custLC, g.month, g.amount + COALESCE(l.bonus, 0) AS amount, g.mid_rank
        \\ FROM grouped g LEFT JOIN (SELECT * FROM route_lookup WHERE id > 0) l ON g.custLC = l.custLC
        \\), lagged AS (
        \\ SELECT custLC, month, LAG(amount, 1, 0) OVER (PARTITION BY custLC ORDER BY month) + mid_rank AS amount
        \\ FROM joined
        \\), expanded_again AS (
        \\ SELECT * FROM TABLE(expand_rows((SELECT custLC, month, amount FROM lagged))
        \\   PARTITION BY custLC ORDER BY month)
        \\), final_window AS (
        \\ SELECT *, LEAD(amount, 1, 0) OVER (PARTITION BY custLC ORDER BY month) AS after_udf FROM expanded_again
        \\)
        \\SELECT * FROM final_window ORDER BY custLC, month
    ;
    for ([_]usize{ 1, 4 }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, dop);
        defer db.close();
        try db.registerTableFn(expand_rows);
        try helpers.exec(allocator, db, "CREATE TABLE route_lookup (id INT PRIMARY KEY, custLC VARCHAR(32), bonus BIGINT)");
        try helpers.exec(allocator, db, "INSERT INTO route_lookup VALUES (1,'cust_0',9),(2,'cust_3',11),(3,NULL,13)");
        const lookup = try db.openTable("route_lookup", .{});
        try lookup.flush();
        for (0..2) |_| {
            var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ body);
            defer query.deinit();
            const root = thindb.exec.queryAs(thindb.exec.mat_stage.StagedRoot, query.cq.query).?;
            var found = false;
            for (root.set.stages.items) |stage| {
                if (!stage.is_keyed_region or thindb.types.findColumn(stage.schema, "after_udf") == null) continue;
                const region = thindb.exec.queryAs(thindb.exec.region_exec.RegionExecOp, stage.query).?;
                var windows: usize = 0;
                var replacements: usize = 0;
                var groups: usize = 0;
                for (region.prog.ops) |op| switch (op) {
                    .window => windows += 1,
                    .tvf_grouped => |tvf| if (!tvf.aligned_append and !tvf.union_append) {
                        replacements += 1;
                    },
                    .group_agg => groups += 1,
                    else => {},
                };
                try std.testing.expectEqual(@as(usize, 4), windows);
                try std.testing.expectEqual(@as(usize, 2), replacements);
                try std.testing.expectEqual(@as(usize, 1), groups);
                try std.testing.expectEqual(@as(usize, 1), region.sides.len);
                found = true;
            }
            try std.testing.expect(found);
            while (try query.next()) |_| {}
        }
        try expect_keyed_matches(allocator, db, body, "after_udf");
    }
}

test "keyed region: replacing TVF without a key preservation contract falls back" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    const tdb = thindb.tdb;
    const rekey = struct {
        pub const spec = tdb.TableFnSpec{ .name = "rekey", .execution = .partitioned };
        pub const Input = struct { custLC: ?[]const u8, month: ?i32, amount: ?i64 };
        pub const Output = Input;
        pub fn process(_: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Output)) !void {
            var rows = p.iter();
            while (rows.next()) |row| try out.row(.{ .custLC = "merged", .month = row.month, .amount = row.amount });
        }
    };
    try db.registerTableFn(rekey);
    try expect_fallback_matches(allocator, db, "custLC",
        \\changed AS (
        \\ SELECT * FROM TABLE(rekey((SELECT custLC, month, amount FROM inv WHERE projectId = 100))
        \\   PARTITION BY custLC ORDER BY month)
        \\), w AS (
        \\ SELECT *, LAG(amount) OVER (PARTITION BY custLC ORDER BY amount) AS prior FROM changed
        \\)
        \\SELECT * FROM w ORDER BY amount
    );
}

test "keyed region: computed replacement after aggregation does not inherit route provenance" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    const body =
        \\r AS (
        \\ SELECT custLC, month, amount, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\ FROM inv WHERE projectId = 100
        \\), g AS (
        \\ SELECT custLC, month, MAX(amount) AS amount, MAX(rn) AS rn FROM r GROUP BY custLC, month
        \\), changed AS (
        \\ SELECT CAST('merged' AS VARCHAR(32)) AS custLC, amount + rn AS amount FROM g
        \\), w AS (
        \\ SELECT *, LAG(amount) OVER (PARTITION BY custLC ORDER BY amount) AS prior FROM changed
        \\)
        \\SELECT * FROM w ORDER BY amount
    ;
    const mono = try runToText(allocator, db, "WITH " ++ body);
    defer allocator.free(mono);
    for (0..2) |_| {
        const keyed = try run_to_text_checked(allocator, db, "WITH KEYED BY (custLC) " ++ body, "prior", false);
        defer allocator.free(keyed);
        try std.testing.expectEqualStrings(mono, keyed);
    }
}

test "keyed region: replaced frames discard stale pinned values" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    const tdb = thindb.tdb;
    const rewrite_project = struct {
        pub const spec = tdb.TableFnSpec{ .name = "rewrite_project", .execution = .partitioned, .ordered_output = true };
        pub const Input = struct { projectId: ?i64, custLC: ?[]const u8, month: ?i32, amount: ?i64 };
        pub const Output = Input;
        pub fn process(_: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Output)) !void {
            var rows = p.iter();
            while (rows.next()) |row| try out.row(.{
                .projectId = if (row.projectId) |project| project + 1 else null,
                .custLC = row.custLC,
                .month = row.month,
                .amount = row.amount,
            });
        }
    };
    try db.registerTableFn(rewrite_project);
    try helpers.exec(allocator, db, "CREATE TABLE project_lookup (id BIGINT PRIMARY KEY, extra INT)");
    try helpers.exec(allocator, db, "INSERT INTO project_lookup VALUES (1,21),(2,22),(3,23),(4,24),(5,25),(100,7),(101,17)");
    const lookup = try db.openTable("project_lookup", .{});
    try lookup.flush();
    inline for (.{
        \\changed AS (
        \\ SELECT * FROM TABLE(rewrite_project((SELECT projectId, custLC, month, amount FROM inv WHERE projectId = 100))
        \\   PARTITION BY custLC ORDER BY month)
        \\)
        ,
        \\r AS (
        \\ SELECT custLC, month, amount, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month) AS rn
        \\ FROM inv WHERE projectId = 100
        \\), changed AS (
        \\ SELECT custLC, month, MAX(rn) AS projectId, MAX(amount) AS amount
        \\ FROM r GROUP BY custLC, month
        \\)
        ,
    }) |prefix| {
        try expect_keyed_matches(allocator, db, prefix ++
            \\, w AS (
            \\ SELECT projectId, custLC, month, amount, LAG(amount) OVER (PARTITION BY custLC ORDER BY month) AS prior
            \\ FROM changed
            \\), j AS (
            \\ SELECT t.projectId, t.custLC, t.month, t.amount, t.prior, l.extra
            \\ FROM w t LEFT JOIN project_lookup l ON t.projectId = l.id
            \\)
            \\SELECT * FROM j ORDER BY custLC, month
        , "extra");
    }
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

test "keyed region: SQL window probe LAG defaults and ordered LAST_VALUE" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();

    inline for (.{
        "LAG(amount, 1, 0) OVER (PARTITION BY custLC ORDER BY month, id)",
        "LAST_VALUE(amount) OVER (PARTITION BY custLC ORDER BY month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
    }) |window| {
        try expect_keyed_matches(allocator, db, "w AS (SELECT id, custLC, " ++ window ++ " AS probe FROM inv WHERE projectId = 100) " ++
            "SELECT id, custLC, probe FROM w ORDER BY id", "probe");
    }
}

test "keyed region: SQL windows share all function families and multiple partition orders" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "INSERT INTO inv VALUES (1001,100,'cust_0',9,NULL),(1002,100,'cust_0',8,400)");
    const table = try db.openTable("inv", .{});
    try table.flush();

    inline for (.{
        "ROW_NUMBER()",        "RANK()",             "DENSE_RANK()",         "NTILE(3)",                         "PERCENT_RANK()",                  "CUME_DIST()",
        "LAG(amount)",         "LEAD(amount, 2, 0)", "LAG(amount, 99, id)",  "LEAD(custLC, 1, 'missing')",       "LAG(amount) IGNORE NULLS",        "LEAD(amount) IGNORE NULLS",
        "FIRST_VALUE(amount)", "LAST_VALUE(amount)", "NTH_VALUE(amount, 2)", "FIRST_VALUE(amount) IGNORE NULLS", "LAST_VALUE(amount) IGNORE NULLS", "NTH_VALUE(amount, 2) IGNORE NULLS",
        "SUM(amount)",         "AVG(amount)",        "COUNT(*)",             "COUNT(amount)",                    "MIN(amount)",                     "MAX(amount)",
        "MIN(custLC)",         "MAX(custLC)",
    }) |call| {
        try expect_keyed_matches(allocator, db, "w AS (SELECT id, projectId, custLC, " ++ call ++
            " OVER (PARTITION BY projectId, custLC ORDER BY month DESC, id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS probe, " ++
            "SUM(amount) OVER (PARTITION BY custLC) AS all_projects, " ++
            "ROW_NUMBER() OVER (PARTITION BY custLC, month ORDER BY id DESC) AS month_rank FROM inv) " ++
            "SELECT id, probe, all_projects, month_rank FROM w ORDER BY id", "probe");
    }
}

test "keyed region: SQL window frames have explicit peer and empty-frame results" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE frames (id BIGINT PRIMARY KEY, custLC VARCHAR(32), ord INT, amount BIGINT)");
    try helpers.exec(allocator, db, "INSERT INTO frames VALUES (1,'a',1,10),(2,'a',1,20),(3,'a',3,NULL),(4,'a',4,40),(5,'b',1,100),(6,NULL,1,200)");
    const table = try db.openTable("frames", .{});
    try table.flush();

    inline for (.{
        .{ "SUM(amount) OVER (PARTITION BY custLC ORDER BY ord)", [4]i64{ 30, 30, 30, 70 } },
        .{ "SUM(amount) OVER (PARTITION BY custLC ORDER BY ord RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)", [4]i64{ 30, 30, -99, 40 } },
        .{ "SUM(amount) OVER (PARTITION BY custLC ORDER BY decimal_ord RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)", [4]i64{ 30, 30, -99, 40 } },
        .{ "SUM(amount) OVER (PARTITION BY custLC ORDER BY double_ord RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)", [4]i64{ 30, 30, -99, 40 } },
        .{ "SUM(amount) OVER (PARTITION BY custLC ORDER BY double_ord RANGE BETWEEN 9223372036854775807 PRECEDING AND CURRENT ROW)", [4]i64{ 30, 30, 30, 70 } },
        .{ "SUM(amount) OVER (PARTITION BY custLC ORDER BY double_ord RANGE BETWEEN CURRENT ROW AND 9223372036854775807 FOLLOWING)", [4]i64{ 70, 70, 40, 40 } },
        .{ "SUM(amount) OVER (PARTITION BY custLC ORDER BY decimal_ord RANGE BETWEEN 9223372036854775807 PRECEDING AND CURRENT ROW)", [4]i64{ 30, 30, 30, 70 } },
        .{ "SUM(amount) OVER (PARTITION BY custLC ORDER BY ord DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)", [4]i64{ 30, 30, 40, 40 } },
        .{ "SUM(amount) OVER (PARTITION BY custLC ORDER BY ord GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW)", [4]i64{ 30, 30, 30, 40 } },
        .{ "SUM(amount) OVER (PARTITION BY custLC ORDER BY ord DESC GROUPS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)", [4]i64{ -99, -99, 30, -99 } },
        .{ "SUM(amount) OVER (PARTITION BY custLC ORDER BY ord, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)", [4]i64{ 10, 30, 20, 40 } },
        .{ "LAST_VALUE(amount) OVER (PARTITION BY custLC ORDER BY ord)", [4]i64{ 20, 20, -99, 40 } },
        .{ "FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY custLC ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)", [4]i64{ 10, 20, 40, 40 } },
        .{ "FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY custLC ORDER BY amount)", [4]i64{ 10, 10, -99, 10 } },
        .{ "FIRST_VALUE(amount) OVER (PARTITION BY custLC ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)", [4]i64{ 20, -99, 40, -99 } },
        .{ "NTH_VALUE(amount, 2) OVER (PARTITION BY custLC ORDER BY id)", [4]i64{ -99, 20, 20, 20 } },
        .{ "LEAD(amount, 9223372036854775807, 7) OVER (PARTITION BY custLC ORDER BY id)", [4]i64{ 7, 7, 7, 7 } },
        .{ "LAG(amount, 9223372036854775807, 7) OVER (PARTITION BY custLC ORDER BY id)", [4]i64{ 7, 7, 7, 7 } },
        .{ "COUNT(*) OVER (PARTITION BY custLC ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING)", [4]i64{ 2, 1, 0, 0 } },
        .{ "SUM(amount) OVER (PARTITION BY custLC ORDER BY ord RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING)", [4]i64{ -99, -99, 40, -99 } },
    }) |case| {
        const body = "frame_input AS (SELECT *, CAST(ord AS DECIMAL(10,2)) AS decimal_ord, CAST(ord AS DOUBLE) AS double_ord FROM frames), " ++
            "w AS (SELECT id, custLC, " ++ case[0] ++ " AS probe FROM frame_input) " ++
            "SELECT COALESCE(probe, -99) AS result FROM w WHERE custLC = 'a' ORDER BY id";
        try expect_keyed_matches(allocator, db, body, "probe");
        const actual = try helpers.collectBigints(allocator, db, "WITH " ++ body);
        defer allocator.free(actual);
        const expected = case[1];
        try std.testing.expectEqualSlices(i64, &expected, actual);
    }
    inline for (.{ "", "KEYED BY (custLC) " }) |declaration| {
        try helpers.expectRunError(allocator, db, "WITH " ++ declaration ++ "w AS (SELECT SUM(amount) OVER (PARTITION BY custLC ORDER BY ord, id RANGE 1 PRECEDING) AS probe FROM frames) SELECT * FROM w", error.WindowUnsupported);
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
    try expect_fallback_matches(allocator, db, "id", body);
    try helpers.expectRunError(allocator, db,
        \\WITH KEYED BY (custLC) u AS (
        \\  SELECT custLC, amount FROM inv UNION ALL SELECT custLC FROM right_arm
        \\), g AS (SELECT custLC, SUM(amount) AS total FROM u GROUP BY custLC)
        \\SELECT * FROM g
    , error.TypeMismatch);
}

fn region_count(query: thindb.exec.Query) usize {
    if (thindb.exec.queryAs(thindb.exec.mat_stage.StagedRoot, query)) |root| {
        var count: usize = 0;
        for (root.set.stages.items) |stage| if (stage.query_alive) {
            count += region_count(stage.query);
        };
        return count;
    }
    if (thindb.exec.queryAs(thindb.exec.region_exec.RegionExecOp, query)) |region| {
        var count: usize = 1;
        for (region.sources) |source| count += region_count(source);
        return count;
    }
    return 0;
}

fn regional_op_count(query: thindb.exec.Query, tag: std.meta.Tag(thindb.exec.region_exec.RegionOp)) usize {
    var count: usize = 0;
    if (thindb.exec.queryAs(thindb.exec.mat_stage.StagedRoot, query)) |root| {
        for (root.set.stages.items) |stage| if (stage.query_alive) {
            count += regional_op_count(stage.query, tag);
        };
    }
    if (thindb.exec.queryAs(thindb.exec.region_exec.RegionExecOp, query)) |region| {
        for (region.prog.ops) |op| if (std.meta.activeTag(op) == tag) {
            count += 1;
        };
        for (region.sources) |source| count += regional_op_count(source, tag);
    }
    return count;
}

test "keyed region: shared SQL branches use one exchange around independent windows" {
    const allocator = std.testing.allocator;
    inline for (.{ 1, 4 }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, dop);
        defer db.close();
        try helpers.exec(allocator, db, "INSERT INTO inv VALUES (2001,100,'cust_0',12,NULL),(2002,100,NULL,3,11)");
        const table = try db.openTable("inv", .{});
        try table.flush();
        const body =
            \\base AS (
            \\ SELECT id, custLC, month, amount,
            \\   LAST_VALUE(amount) OVER (PARTITION BY custLC, month ORDER BY id
            \\     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest FROM inv
            \\), combined AS (
            \\ SELECT custLC, id, amount, latest, 1 AS arm FROM base WHERE month <= 3
            \\ UNION ALL
            \\ SELECT custLC, id, amount * 2 AS amount, latest, 2 AS arm FROM base WHERE month >= 3 OR amount IS NULL
            \\), result AS (
            \\ SELECT *, LAG(amount, 1, 0) OVER (PARTITION BY custLC ORDER BY id) AS prior FROM combined
            \\)
            \\SELECT * FROM result ORDER BY custLC, id, arm
        ;
        for (0..2) |_| {
            var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ body);
            defer query.deinit();
            try std.testing.expectEqual(@as(usize, 1), region_count(query.cq.query));
            try std.testing.expectEqual(@as(usize, 1), regional_op_count(query.cq.query, .union_all));
            try std.testing.expectEqual(@as(usize, 2), regional_op_count(query.cq.query, .window));
            while (try query.next()) |_| {}
        }
        try expect_keyed_matches(allocator, db, body, "prior");
        try helpers.exec(allocator, db, "INSERT INTO inv VALUES (3001,100,'cust_0',3,701)");
        try table.flush();
        try expect_keyed_matches(allocator, db, body, "prior");
    }
}

test "keyed region: three SQL branches preserve positional widening duplicates and empty ranges" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    inline for (.{ "month <= 3", "month > 100" }) |first_filter| {
        const body =
            \\base AS (SELECT * FROM inv), combined AS (
            \\ SELECT custLC, id, month AS value, 1 AS arm FROM base WHERE
        ++ " " ++ first_filter ++
            \\ UNION ALL
            \\ SELECT custLC AS other_key, id AS other_id, amount AS other_value, 2 AS other_arm FROM base WHERE month >= 3
            \\ UNION ALL
            \\ SELECT custLC, id, amount, 3 FROM base WHERE month = 3
            \\), result AS (
            \\ SELECT *, LAG(value, 1, 0) OVER (PARTITION BY custLC ORDER BY id) AS prior FROM combined
            \\)
            \\SELECT * FROM result ORDER BY custLC, id, arm
        ;
        var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ body);
        defer query.deinit();
        try std.testing.expectEqual(@as(usize, 1), region_count(query.cq.query));
        try std.testing.expectEqual(@as(usize, 2), regional_op_count(query.cq.query, .union_all));
        while (try query.next()) |_| {}
        try expect_keyed_matches(allocator, db, body, "prior");
    }
}

test "keyed region: boolean payloads cross SQL union and window regions" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "INSERT INTO inv VALUES (2001,100,'cust_0',12,NULL),(2002,100,'cust_0',13,0)");
    const table = try db.openTable("inv", .{});
    try table.flush();
    const body =
        \\base AS (
        \\ SELECT id, custLC, month, CAST(amount AS BOOLEAN) AS active,
        \\   ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY id) AS rn FROM inv
        \\), combined AS (
        \\ SELECT * FROM base WHERE month <= 3 UNION ALL SELECT * FROM base WHERE month >= 3
        \\), result AS (
        \\ SELECT *, SUM(rn) OVER (PARTITION BY custLC) AS total FROM combined
        \\)
        \\SELECT * FROM result ORDER BY id
    ;
    try expect_keyed_matches(allocator, db, body, "total");
}

test "keyed region: shared SQL branches preserve independent window frames and dimension joins" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE dimension (id INT PRIMARY KEY, month INT, label VARCHAR(32))");
    try helpers.exec(allocator, db, "INSERT INTO dimension VALUES (1,1,'first'),(2,2,NULL),(3,3,'third')");
    const dimension = try db.openTable("dimension", .{});
    try dimension.flush();
    const body =
        \\base AS (SELECT * FROM inv), a AS (
        \\ SELECT *, SUM(amount) OVER (PARTITION BY custLC ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS value
        \\ FROM base WHERE month <= 3
        \\), b AS (
        \\ SELECT *, LEAD(amount, 1, 0) OVER (PARTITION BY custLC ORDER BY id DESC) AS value
        \\ FROM base WHERE month >= 3
        \\), combined AS (
        \\ SELECT a.custLC, a.id, a.value, d.label, 1 AS arm FROM a LEFT JOIN dimension d ON a.month = d.month
        \\ UNION ALL
        \\ SELECT b.custLC, b.id, b.value, d.label, 2 AS arm FROM b LEFT JOIN dimension d ON b.month = d.month
        \\), result AS (
        \\ SELECT *, LAG(value, 1, 0) OVER (PARTITION BY custLC ORDER BY id, arm) AS prior FROM combined
        \\)
        \\SELECT * FROM result ORDER BY custLC, id, arm
    ;
    for (0..2) |_| {
        var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ body);
        defer query.deinit();
        try std.testing.expectEqual(@as(usize, 1), region_count(query.cq.query));
        try std.testing.expectEqual(@as(usize, 1), regional_op_count(query.cq.query, .union_all));
        try std.testing.expectEqual(@as(usize, 3), regional_op_count(query.cq.query, .window));
        while (try query.next()) |_| {}
        try expect_keyed_matches(allocator, db, body, "prior");
        try helpers.exec(allocator, db, "DELETE FROM dimension WHERE month = 2");
        try dimension.flush();
    }
}

test "keyed region: shared SQL branches feed grouped reductions" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    inline for (.{ "id <= 5", "id < 0" }) |filter| {
        const body =
            \\base AS (SELECT * FROM inv), combined AS (
            \\ SELECT custLC, amount FROM base WHERE
        ++ " " ++ filter ++
            \\ UNION ALL SELECT custLC, amount * 2 AS amount FROM base WHERE
        ++ " " ++ filter ++
            \\), result AS (
            \\ SELECT custLC, MAX(amount) AS largest, SUM(amount) AS total FROM combined GROUP BY custLC
            \\)
            \\SELECT * FROM result ORDER BY custLC
        ;
        var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ body);
        defer query.deinit();
        try std.testing.expectEqual(@as(usize, 1), regional_op_count(query.cq.query, .union_all));
        while (try query.next()) |_| {}
        try expect_keyed_matches(allocator, db, body, "total");
    }
}

test "keyed region: shared SQL branches preserve multiplying and filtering joins" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE sides (id INT PRIMARY KEY, custLC VARCHAR(32), extra BIGINT)");
    try helpers.exec(allocator, db, "INSERT INTO sides VALUES (1,'cust_0',11),(2,'cust_0',12),(3,NULL,13),(4,'cust_1',NULL)");
    const sides = try db.openTable("sides", .{});
    try sides.flush();
    inline for (.{
        .{ .join_type = "LEFT", .source = "(SELECT * FROM sides WHERE id <= 3)", .predicate = "", .fused = true },
        .{ .join_type = "INNER", .source = "(SELECT * FROM sides WHERE id = 1)", .predicate = "", .fused = true },
        .{ .join_type = "LEFT", .source = "sides", .predicate = "", .fused = false },
        .{ .join_type = "INNER", .source = "sides", .predicate = " AND b.amount < s.extra", .fused = false },
        .{ .join_type = "LEFT", .source = "sides", .predicate = " AND b.amount > 0", .fused = false },
    }) |case| {
        const body =
            \\base AS (SELECT * FROM inv), combined AS (
            \\ SELECT b.custLC, b.id, s.extra AS value, 1 AS arm FROM base b
        ++ " " ++ case.join_type ++ " JOIN " ++ case.source ++
            \\ s ON b.custLC = s.custLC
        ++ case.predicate ++
            \\ UNION ALL SELECT custLC, id, amount AS value, 2 AS arm FROM base WHERE month < 3
            \\), result AS (
            \\ SELECT *, LAG(value, 1, 0) OVER (PARTITION BY custLC ORDER BY id, arm, value) AS prior FROM combined
            \\)
            \\SELECT * FROM result ORDER BY custLC, id, arm, value
        ;
        var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ body);
        defer query.deinit();
        try std.testing.expectEqual(@as(usize, if (case.fused) 1 else 0), regional_op_count(query.cq.query, .union_all));
        while (try query.next()) |_| {}
        try expect_keyed_matches(allocator, db, body, "prior");
    }
}

test "keyed region: shared SQL branches reject unsafe fusion while retaining staged ingress" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    inline for (.{
        .{ .base = "base AS MATERIALIZED (SELECT * FROM inv)", .left = "SELECT custLC, id, amount FROM base", .right = "SELECT custLC, id, amount FROM base" },
        .{ .base = "base AS NOT MATERIALIZED (SELECT * FROM inv)", .left = "SELECT custLC, id, amount FROM base", .right = "SELECT custLC, id, amount FROM base" },
        .{ .base = "base AS (SELECT * FROM inv)", .left = "SELECT custLC, id, amount FROM base", .right = "SELECT 'changed' AS custLC, id, amount FROM base" },
        .{ .base = "base AS (SELECT * FROM inv)", .left = "SELECT custLC, id, amount FROM base", .right = "SELECT custLC, id, SUM(amount) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS amount FROM base" },
        .{ .base = "base AS (SELECT * FROM inv), barrier AS MATERIALIZED (SELECT * FROM base)", .left = "SELECT custLC, id, amount FROM barrier", .right = "SELECT custLC, id, amount FROM base" },
        .{ .base = "base AS (SELECT * FROM inv)", .left = "SELECT custLC, id, amount FROM base", .right = "SELECT b.custLC, b.id, other.amount FROM base b JOIN base other ON b.id = other.id" },
    }) |case| {
        const body = case.base ++ ", combined AS (" ++ case.left ++ " UNION ALL " ++ case.right ++
            \\), result AS (
            \\ SELECT *, LAG(amount, 1, 0) OVER (PARTITION BY custLC ORDER BY id, amount) AS prior FROM combined
            \\)
            \\SELECT * FROM result ORDER BY custLC, id, amount, prior
        ;
        var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ body);
        defer query.deinit();
        try std.testing.expectEqual(@as(usize, 0), regional_op_count(query.cq.query, .union_all));
        while (try query.next()) |_| {}
        try expect_keyed_matches(allocator, db, body, "prior");
    }
}

test "keyed region: an unsupported UNION ALL branch leaves the CTE both branches read regional" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try expect_keyed_matches(allocator, db,
        \\base AS (
        \\ SELECT id, custLC, month, amount, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY month, id) AS rn FROM inv
        \\), treated AS (
        \\ SELECT id, custLC, month, SUM(amount) OVER (ORDER BY custLC, month, id ROWS UNBOUNDED PRECEDING) AS amount, rn
        \\ FROM base WHERE month >= 3
        \\), combined AS (
        \\ SELECT id, custLC, month, amount, rn FROM treated
        \\ UNION ALL SELECT id, custLC, month, amount, rn FROM base WHERE month < 3
        \\)
        \\SELECT * FROM combined ORDER BY custLC, month, id
    , "rn");
}

test "keyed region: constant-empty SQL branches retain ordinary pruning and later regions" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE lookup (id INT PRIMARY KEY, month INT, value BIGINT)");
    try helpers.exec(allocator, db, "INSERT INTO lookup VALUES (1,1,100),(2,2,200)");
    const lookup = try db.openTable("lookup", .{});
    try lookup.flush();
    inline for (.{ false, true, false }) |enabled| {
        const body =
            \\base AS (SELECT * FROM inv), combined AS (
            \\ SELECT b.custLC, b.id, l.value, 1 AS arm FROM base b LEFT JOIN lookup l ON b.month = l.month
        ++ (if (enabled) " WHERE 1=1" else " WHERE 1=0") ++
            \\ UNION ALL SELECT custLC, id, amount AS value, 2 AS arm FROM base WHERE month >= 3
            \\), result AS (
            \\ SELECT *, LAG(value, 1, 0) OVER (PARTITION BY custLC ORDER BY id, arm) AS prior FROM combined
            \\)
            \\SELECT * FROM result ORDER BY custLC, id, arm
        ;
        for (0..2) |_| {
            var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ body);
            defer query.deinit();
            try std.testing.expectEqual(@as(usize, @intFromBool(enabled)), regional_op_count(query.cq.query, .union_all));
            try std.testing.expectEqual(@as(usize, 1), region_count(query.cq.query));
            while (try query.next()) |_| {}
            try expect_keyed_matches(allocator, db, body, "prior");
        }
    }
}

test "keyed region: rejected SQL fusion retries after changed lookup data and schema" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE lookup (id INT PRIMARY KEY, month INT, value BIGINT)");
    try helpers.exec(allocator, db, "INSERT INTO lookup VALUES (1,1,100),(2,1,200)");
    var lookup = try db.openTable("lookup", .{});
    try lookup.flush();
    const body =
        \\base AS (SELECT * FROM inv), combined AS (
        \\ SELECT b.custLC, b.id, l.value, 1 AS arm FROM base b LEFT JOIN lookup l ON b.month = l.month
        \\ UNION ALL SELECT custLC, id, amount AS value, 2 AS arm FROM base WHERE month >= 3
        \\), result AS (
        \\ SELECT *, LAG(value, 1, 0) OVER (PARTITION BY custLC ORDER BY id, arm, value) AS prior FROM combined
        \\)
        \\SELECT * FROM result ORDER BY custLC, id, arm, value
    ;
    for (0..5) |stage| {
        if (stage == 1) {
            try helpers.exec(allocator, db, "DELETE FROM lookup WHERE id = 2");
            try lookup.flush();
        } else if (stage == 2) {
            try helpers.exec(allocator, db, "INSERT INTO lookup VALUES (2,1,300)");
            try lookup.flush();
        } else if (stage == 3) {
            try helpers.exec(allocator, db, "ALTER TABLE lookup ADD COLUMN extra INT DEFAULT 7");
        } else if (stage == 4) {
            try helpers.exec(allocator, db, "DROP TABLE lookup");
            try helpers.exec(allocator, db, "CREATE TABLE lookup (id INT PRIMARY KEY, month INT, value INT)");
            try helpers.exec(allocator, db, "INSERT INTO lookup VALUES (1,1,400)");
            lookup = try db.openTable("lookup", .{});
            try lookup.flush();
        }
        for (0..3) |_| {
            var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ body);
            defer query.deinit();
            try std.testing.expectEqual(@as(usize, if (stage == 1 or stage == 4) 1 else 0), regional_op_count(query.cq.query, .union_all));
            while (try query.next()) |_| {}
            try expect_keyed_matches(allocator, db, body, "prior");
        }
    }
}

test "keyed region: shared SQL branches retain externally consumed CTE stages" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    const ctes =
        \\base AS (SELECT * FROM inv), combined AS (
        \\ SELECT custLC, id, amount FROM base WHERE month <= 3
        \\ UNION ALL SELECT custLC, id, amount * 2 AS amount FROM base WHERE month >= 3
        \\), result AS (
        \\ SELECT *, LAG(amount, 1, 0) OVER (PARTITION BY custLC ORDER BY id, amount) AS prior FROM combined
        \\)
    ;
    inline for (.{ false, true, false }) |external| {
        const body = ctes ++ if (external)
            \\SELECT r.custLC, r.id, r.amount, r.prior, b.amount AS original
            \\FROM result r JOIN base b ON r.id = b.id ORDER BY r.custLC, r.id, r.amount
        else
            "SELECT * FROM result ORDER BY custLC, id, amount";
        var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ body);
        defer query.deinit();
        try std.testing.expectEqual(@as(usize, if (external) 0 else 1), regional_op_count(query.cq.query, .union_all));
        while (try query.next()) |_| {}
        try expect_keyed_matches(allocator, db, body, "prior");
    }
}

test "keyed region: overlapping join payload names preserve the left columns" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE dimension (id INT PRIMARY KEY, projectId BIGINT, month INT, amount INT)");
    try helpers.exec(allocator, db, "INSERT INTO dimension VALUES (1,100,1,999),(2,100,2,NULL)");
    const dimension = try db.openTable("dimension", .{});
    try dimension.flush();
    const body =
        \\base AS (
        \\ SELECT *, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY id) AS rn FROM inv
        \\), joined AS (
        \\ SELECT base.id, base.custLC, base.projectId, base.month, base.amount, base.rn,
        \\   dimension.amount AS other_amount, dimension.projectId AS other_project,
        \\   dimension.month AS other_month
        \\ FROM base LEFT JOIN dimension dimension
        \\ ON dimension.projectId = base.projectId AND dimension.month = base.month
        \\)
        \\SELECT * FROM joined ORDER BY id
    ;
    try expect_keyed_matches(allocator, db, body, "other_amount");
    try helpers.exec(allocator, db, "DELETE FROM dimension");
    try dimension.flush();
    try expect_keyed_matches(allocator, db, body, "other_amount");
}

test "keyed region: SQL windows reenter regions around a global window CTE" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    const body =
        \\local_window AS (
        \\ SELECT id, custLC, month, amount,
        \\   LAG(amount, 1, 0) OVER (PARTITION BY custLC ORDER BY id) AS prior
        \\ FROM inv
        \\), global_window AS (
        \\ SELECT *, SUM(amount) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS global_sum
        \\ FROM local_window
        \\), local_again AS (
        \\ SELECT *, LEAD(global_sum, 1, 0) OVER (PARTITION BY custLC ORDER BY id) AS next_sum
        \\ FROM global_window
        \\)
        \\SELECT id, custLC, prior, global_sum, next_sum FROM local_again ORDER BY id
    ;
    for (0..2) |_| {
        var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ body);
        defer query.deinit();
        try std.testing.expectEqual(@as(usize, 2), region_count(query.cq.query));
        while (try query.next()) |_| {}
    }
    try expect_keyed_matches(allocator, db, body, "next_sum");
    try helpers.exec(allocator, db, "INSERT INTO inv VALUES (2001,100,'cust_0',12,321)");
    const table = try db.openTable("inv", .{});
    try table.flush();
    try expect_keyed_matches(allocator, db, body, "next_sum");
}

/// A row-generating kernel: one row per call, a month past the call's last
/// input row, carrying the call's total.
const project_month = struct {
    const tdb = thindb.tdb;
    pub const spec = tdb.TableFnSpec{ .name = "project_month", .execution = .partitioned };
    pub const Input = struct { custLC: ?[]const u8, day: ?tdb.Date, amount: ?i64 };
    pub const Output = Input;
    pub fn process(_: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Output)) !void {
        if (p.len == 0) return;
        const days = p.col(.day);
        const amounts = p.col(.amount);
        var total: i64 = 1_000_000;
        for (0..p.len) |i| total += amounts.get(i) orelse 0;
        const last_day = days.get(p.len - 1) orelse return;
        try out.row(.{
            .custLC = p.col(.custLC).get(p.len - 1),
            .day = tdb.Date.fromDays(last_day.days() + 31),
            .amount = total,
        });
    }
};

test "keyed region: a UNION ALL table-function arm reads its window bounds in the filtered column's type" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try db.registerTableFn(project_month);
    try helpers.exec(allocator, db, "CREATE TABLE ev (id BIGINT PRIMARY KEY, custLC STRING, day DATE, amount BIGINT)");
    try helpers.exec(allocator, db,
        \\INSERT INTO ev VALUES
        \\ (1,'cust_0','2025-11-30',10),(2,'cust_0','2025-12-01',20),(3,'cust_0','2026-01-15',30),
        \\ (4,'cust_1','2025-12-20',40),(5,'cust_1','2026-02-28',50),(6,'cust_1','2026-03-01',60),
        \\ (7,'cust_2','2026-03-05',70),(8,'cust_3','2026-02-01',80),(9,NULL,'2026-01-10',90)
    );
    const ev = try db.openTable("ev", .{});
    try ev.flush();
    {
        var q = try helpers.runSql(allocator, db, "SELECT DATE_ADD('2026-02-01', INTERVAL -2 MONTH) AS lo");
        defer q.deinit();
        try std.testing.expectEqual(thindb.types.Type.datetime, q.outputSchema()[0].type);
    }
    // A DATETIME bound on the DATE column, at midnight and with a time of
    // day (which excludes 2025-12-01), beside DATE bounds.
    inline for (.{
        "DATE_ADD('2026-02-01', INTERVAL -2 MONTH) AND LAST_DAY('2026-02-01')",
        "DATE_ADD('2025-12-01 12:00:00', INTERVAL 0 DAY) AND DATE_ADD('2026-02-28', INTERVAL 0 DAY)",
        "DATE '2025-12-01' AND DATE '2026-02-28'",
    }) |window| {
        const body =
            \\base AS (
            \\ SELECT custLC, day, amount FROM ev WHERE id > 0
            \\), projected AS (
            \\ SELECT custLC, day, amount FROM TABLE(project_month((
            \\   SELECT custLC, day, amount FROM base WHERE day BETWEEN
        ++ " " ++ window ++
            \\
            \\ )) PARTITION BY custLC ORDER BY day)
            \\), combined AS (
            \\ SELECT * FROM base
            \\ UNION ALL
            \\ SELECT * FROM projected
            \\), w AS (
            \\ SELECT custLC, day, amount, LAG(amount) OVER (PARTITION BY custLC ORDER BY day, amount) AS prior
            \\ FROM combined
            \\)
            \\SELECT * FROM w ORDER BY custLC, day, amount
        ;
        {
            var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ body);
            defer query.deinit();
            try std.testing.expectEqual(@as(usize, 1), regional_op_count(query.cq.query, .tvf_grouped));
            while (try query.next()) |_| {}
        }
        try expect_keyed_matches(allocator, db, body, "prior");
    }
}

/// A row-generating kernel for a `base UNION ALL TABLE(f(base))` arm, as a
/// raw descriptor so its key and `v` columns can be declared in any type: one
/// row per call, the call's last input row with `seq` moved past the table's
/// and `v` written as `value` in the declared output type.
fn union_arm(comptime name: []const u8, comptime key_type: thindb.Type, comptime frame_type: thindb.Type, comptime out_type: thindb.Type, comptime value: anytype) thindb.udf.TableUdf {
    const kernel = struct {
        const input = [_]thindb.Column{
            .{ .name = "custLC", .type = key_type, .nullable = true },
            .{ .name = "seq", .type = .bigint, .nullable = true },
            .{ .name = "v", .type = frame_type, .nullable = true },
        };
        const output = [_]thindb.Column{
            .{ .name = "custLC", .type = key_type, .nullable = true },
            .{ .name = "seq", .type = .bigint, .nullable = true },
            .{ .name = "v", .type = out_type, .nullable = true },
        };

        fn process(_: *const thindb.udf.TvfContext, parts: []const thindb.udf.TvfPartition, out: *thindb.udf.TvfOutput) !void {
            const part = &parts[0];
            if (part.row_count == 0) return;
            const last = part.row_count - 1;
            const alloc = out.allocator;
            try thindb.engine.store.appendViewRange(alloc, out.columns[0], part.columns[0], last, last + 1);
            const seq = out.columns[1];
            try seq.data.bigint.append(alloc, part.columns[1].data.bigint[last] + 100);
            try seq.appendValidBit(alloc, seq.rowCount() - 1, true);
            const v = out.columns[2];
            if (comptime out_type.isString()) {
                switch (v.data) {
                    .varchar, .string, .char, .json => |*text| try text.appendValue(alloc, value),
                    else => return error.TableFnBadOutput,
                }
            } else {
                if (std.meta.activeTag(v.data) != std.meta.activeTag(out_type)) return error.TableFnBadOutput;
                try @field(v.data, @tagName(out_type)).append(alloc, value);
            }
            try v.appendValidBit(alloc, v.rowCount() - 1, true);
        }
    };
    return .{
        .name = name,
        .input_schemas = &.{&kernel.input},
        .output_schema = &kernel.output,
        .execution = .partitioned,
        .process = kernel.process,
    };
}

/// `base UNION ALL` the rows `function` generates from it, in either arm
/// order, under a window. `column` of `pairs` is the union's `v`.
fn union_arm_body(comptime function: []const u8, comptime column: []const u8, comptime base_first: bool) []const u8 {
    return "base AS (\n SELECT custLC, seq, " ++ column ++ " AS v FROM pairs WHERE id > 0\n" ++
        "), projected AS (\n SELECT custLC, seq, v FROM TABLE(" ++ function ++ "((\n" ++
        "   SELECT custLC, seq, v FROM base WHERE seq BETWEEN 2 AND 8\n" ++
        " )) PARTITION BY custLC ORDER BY seq)\n), combined AS (\n" ++
        (if (base_first) " SELECT * FROM base UNION ALL SELECT * FROM projected\n" else " SELECT * FROM projected UNION ALL SELECT * FROM base\n") ++
        "), w AS (\n SELECT custLC, seq, v, LAG(seq) OVER (PARTITION BY custLC ORDER BY seq) AS prior FROM combined\n)\n" ++
        "SELECT * FROM w ORDER BY custLC, seq";
}

fn setup_pairs(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    try helpers.exec(allocator, db,
        \\CREATE TABLE pairs (
        \\  id BIGINT PRIMARY KEY, custLC VARCHAR(32), seq BIGINT NOT NULL,
        \\  code VARCHAR(8), label STRING, day DATE, at DATETIME,
        \\  small INT, big BIGINT, price DECIMAL(10,2), wide DECIMAL(12,4)
        \\)
    );
    try helpers.exec(allocator, db,
        \\INSERT INTO pairs VALUES
        \\ (1,'cust_0',1,'a','alpha','2025-11-30','2025-11-30 10:00:00',11,5000000001,1.25,1.1234),
        \\ (2,'cust_0',2,'b','bravo','2025-12-01','2025-12-01 11:30:00',12,5000000002,2.50,2.2345),
        \\ (3,'cust_0',3,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
        \\ (4,'cust_1',4,'d','delta','2025-12-20','2025-12-20 13:00:00',14,5000000004,4.00,4.4567),
        \\ (5,'cust_1',5,'e','echo','2026-02-28','2026-02-28 14:45:00',15,5000000005,5.25,5.5678),
        \\ (6,'cust_1',9,'f','foxtrot','2026-03-01','2026-03-01 15:00:00',16,5000000006,6.50,6.6789),
        \\ (7,'cust_2',7,'g','golf','2026-03-05','2026-03-05 16:00:00',17,5000000007,7.75,7.7891),
        \\ (8,NULL,8,'h','hotel','2026-02-01','2026-02-01 17:00:00',18,5000000008,8.00,8.8912)
    );
    const pairs = try db.openTable("pairs", .{});
    try pairs.flush();
}

/// The types both the plain and the keyed statement report for the union's
/// key and its `v`.
fn expect_union_types(allocator: std.mem.Allocator, db: *thindb.Database, comptime body: []const u8, key: thindb.Type, expected: thindb.Type) !void {
    inline for (.{ "WITH ", "WITH KEYED BY (custLC) " }) |head| {
        var query = try helpers.runSql(allocator, db, head ++ body);
        defer query.deinit();
        const schema = query.outputSchema();
        try std.testing.expectEqual(key, schema[0].type);
        try std.testing.expectEqual(expected, schema[2].type);
        while (try query.next()) |_| {}
    }
}

// Issue #495. The kernel's rows are a UNION ALL arm, so each column is the
// union's result type whichever path runs it. `seq` is NOT NULL in the
// table and nullable in the kernel's declaration.
test "keyed region: a UNION ALL table-function arm reports the union's text type" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try setup_pairs(allocator, db);
    inline for (.{
        .{ "arm_text_string", "code", thindb.Type{ .string = {} }, thindb.Type{ .varchar = 8 }, thindb.Type{ .string = {} }, thindb.Type{ .string = {} } },
        .{ "arm_text_bounded", "label", thindb.Type{ .varchar = 32 }, thindb.Type{ .string = {} }, thindb.Type{ .varchar = 32 }, thindb.Type{ .string = {} } },
        .{ "arm_text_longer", "code", thindb.Type{ .varchar = 64 }, thindb.Type{ .varchar = 8 }, thindb.Type{ .varchar = 32 }, thindb.Type{ .varchar = 32 } },
    }) |case| {
        try db.registerTableUdf(union_arm(case[0], case[2], case[3], case[4], "generated"));
        inline for (.{ true, false }) |base_first| {
            const body = comptime union_arm_body(case[0], case[1], base_first);
            {
                var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ body);
                defer query.deinit();
                try std.testing.expectEqual(@as(usize, 1), regional_op_count(query.cq.query, .tvf_grouped));
                while (try query.next()) |_| {}
            }
            try expect_union_types(allocator, db, body, case[2], case[5]);
            // Run twice: the second is the cached program.
            try expect_keyed_matches(allocator, db, body, "prior");
        }
    }
}

/// A kernel whose declared `v` the frame column meets only by converting:
/// the frame's stores can't take its rows, so the keyed statement runs the
/// union in the ordinary operator and returns what the plain one does.
/// `generated` is the kernel's first row as the union returns it.
fn expect_converting_arm(comptime name: []const u8, comptime column: []const u8, comptime frame_type: thindb.Type, comptime out_type: thindb.Type, comptime value: anytype, expected: thindb.Type, generated: []const u8) !void {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try setup_pairs(allocator, db);
    // The key is declared as the table has it, so `v` is the only column the
    // two arms disagree on.
    const key: thindb.Type = .{ .varchar = 32 };
    try db.registerTableUdf(union_arm(name, key, frame_type, out_type, value));
    inline for (.{ true, false }) |base_first| {
        const body = comptime union_arm_body(name, column, base_first);
        try expect_union_types(allocator, db, body, key, expected);
        const plain = try runToText(allocator, db, "WITH " ++ body);
        defer allocator.free(plain);
        try std.testing.expect(std.mem.indexOf(u8, plain, generated) != null);
        try expect_fallback_matches(allocator, db, "custLC", body);
    }
}

test "keyed region: a UNION ALL table-function arm declaring DATETIME over a DATE column runs in the ordinary operator" {
    try expect_converting_arm("arm_stamp", "day", .date, .datetime, @as(i64, 1_772_366_400_000_000), .datetime, "cust_0|103|1772366400000000|3");
}

test "keyed region: a UNION ALL table-function arm declaring DATE over a DATETIME column runs in the ordinary operator" {
    try expect_converting_arm("arm_day", "at", .datetime, .date, @as(i32, 20513), .datetime, "cust_0|103|1772323200000000|3");
}

test "keyed region: a UNION ALL table-function arm declaring BIGINT over an INT column runs in the ordinary operator" {
    try expect_converting_arm("arm_wider", "small", .int, .bigint, @as(i64, 5_000_000_000), .bigint, "cust_0|103|5000000000|3");
}

test "keyed region: a UNION ALL table-function arm declaring INT over a BIGINT column runs in the ordinary operator" {
    try expect_converting_arm("arm_narrower", "big", .bigint, .int, @as(i32, 7), .bigint, "cust_0|103|7|3");
}

test "keyed region: a UNION ALL table-function arm declaring DECIMAL(12,4) over a DECIMAL(10,2) column runs in the ordinary operator" {
    try expect_converting_arm("arm_finer", "price", .{ .decimal64 = .{ .p = 10, .s = 2 } }, .{ .decimal64 = .{ .p = 12, .s = 4 } }, @as(i64, 12345), .{ .decimal64 = .{ .p = 12, .s = 4 } }, "cust_0|103|12345|3");
}

test "keyed region: a UNION ALL table-function arm declaring DECIMAL(10,2) over a DECIMAL(12,4) column runs in the ordinary operator" {
    try expect_converting_arm("arm_coarser", "wide", .{ .decimal64 = .{ .p = 12, .s = 4 } }, .{ .decimal64 = .{ .p = 10, .s = 2 } }, @as(i64, 250), .{ .decimal64 = .{ .p = 12, .s = 4 } }, "cust_0|103|25000|3");
}

test "keyed region: SQL UNION ALL branches report the union's column types" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try setup_pairs(allocator, db);
    inline for (.{
        .{ "code", "label", thindb.Type{ .string = {} } },
        .{ "price", "wide", thindb.Type{ .decimal64 = .{ .p = 12, .s = 4 } } },
        .{ "day", "at", thindb.Type{ .datetime = {} } },
        .{ "small", "big", thindb.Type{ .bigint = {} } },
    }) |case| {
        inline for (.{ .{ case[0], case[1] }, .{ case[1], case[0] } }) |arms| {
            const body = "base AS (SELECT custLC, seq, code, label, day, at, small, big, price, wide FROM pairs WHERE id > 0), combined AS (" ++
                " SELECT custLC, seq, " ++ arms[0] ++ " AS v FROM base WHERE seq < 5" ++
                " UNION ALL SELECT custLC, seq, " ++ arms[1] ++ " AS v FROM base WHERE seq >= 5" ++
                "), w AS (SELECT custLC, seq, v, LAG(seq) OVER (PARTITION BY custLC ORDER BY seq) AS prior FROM combined)" ++
                " SELECT * FROM w ORDER BY custLC, seq";
            inline for (.{ "WITH ", "WITH KEYED BY (custLC) " }) |head| {
                var query = try helpers.runSql(allocator, db, head ++ body);
                defer query.deinit();
                try std.testing.expectEqual(case[2], query.outputSchema()[2].type);
                while (try query.next()) |_| {}
            }
            try expect_keyed_matches(allocator, db, body, "prior");
        }
    }
}

// Issue #498. A region's stores carry null bitmaps wherever a NULL can be
// appended, which says nothing about the values: the statement reports
// each column as the ordinary operators do.
fn setup_events(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    try helpers.exec(allocator, db,
        \\CREATE TABLE ev (
        \\  id BIGINT PRIMARY KEY, custLC VARCHAR(32) NOT NULL, day DATE NOT NULL,
        \\  amount BIGINT NOT NULL, kind BIGINT NOT NULL, note VARCHAR(16)
        \\)
    );
    try helpers.exec(allocator, db,
        \\INSERT INTO ev VALUES
        \\ (1,'cust_0','2025-11-30',10,1,'first'),(2,'cust_0','2025-12-01',20,2,NULL),
        \\ (3,'cust_0','2026-01-15',30,3,'third'),(4,'cust_1','2025-12-20',40,1,NULL),
        \\ (5,'cust_1','2026-02-28',50,2,'fifth'),(6,'cust_1','2026-03-01',60,3,'sixth'),
        \\ (7,'cust_2','2026-03-05',70,1,'seventh'),(8,'cust_3','2026-02-01',80,2,NULL)
    );
    const ev = try db.openTable("ev", .{});
    try ev.flush();
    // No row for kind 3: an inner join drops it, a left join keeps it.
    try helpers.exec(allocator, db, "CREATE TABLE kinds (kind BIGINT PRIMARY KEY, label VARCHAR(8) NOT NULL, hint VARCHAR(8))");
    try helpers.exec(allocator, db, "INSERT INTO kinds VALUES (1,'one','a'),(2,'two',NULL)");
    const kinds = try db.openTable("kinds", .{});
    try kinds.flush();
}

fn expect_nullable(allocator: std.mem.Allocator, db: *thindb.Database, comptime body: []const u8, expected: []const bool) !void {
    inline for (.{ "WITH ", "WITH KEYED BY (custLC) " }) |head| {
        var query = try helpers.runSql(allocator, db, head ++ body);
        defer query.deinit();
        const schema = query.outputSchema();
        try std.testing.expectEqual(expected.len, schema.len);
        for (schema, expected) |col, nullable| try std.testing.expectEqual(nullable, col.nullable);
        while (try query.next()) |_| {}
    }
}

test "keyed region: NOT NULL columns stay NOT NULL through a region" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    try setup_events(allocator, db);
    const body =
        \\w AS (
        \\  SELECT id, custLC, day, amount, note,
        \\         LAG(amount) OVER (PARTITION BY custLC ORDER BY day, amount) AS prior
        \\  FROM ev
        \\)
        \\SELECT * FROM w ORDER BY custLC, day, amount
    ;
    try expect_nullable(allocator, db, body, &.{ false, false, false, false, true, true });
    try expect_keyed_matches(allocator, db, body, "prior");
}

test "keyed region: computes, literals, aggregates, joins and unions report what the plain statement does" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup_with_dop(allocator, std.testing.io, tmp.dir, 4);
    defer db.close();
    try setup_events(allocator, db);
    const window = "), w AS (SELECT *, LAG(amount) OVER (PARTITION BY custLC ORDER BY day, amount) AS prior FROM c) SELECT * FROM w ORDER BY custLC, day, amount";
    // Each shape names the region op that carries it.
    inline for (.{
        // A literal and a compute over NOT NULL inputs are NOT NULL; one
        // over a nullable input is not. At the region's entry they are
        // computed as the rows are scattered; above a window they are ops.
        .{ "c AS (SELECT custLC, day, amount, 1 AS arm, amount * 2 AS twice, CONCAT(note, '!') AS loud FROM ev" ++ window, &[_]bool{ false, false, false, false, false, true, true }, .window },
        .{ "r AS (SELECT custLC, day, amount, note, ROW_NUMBER() OVER (PARTITION BY custLC ORDER BY day, amount) AS rn FROM ev), " ++
            "c AS (SELECT custLC, day, amount, 1 AS arm, rn + amount AS mixed, CONCAT(note, '!') AS loud FROM r" ++ window, &[_]bool{ false, false, false, false, false, true, true }, .const_cols },
        // A group key is as its input, an aggregate by its function.
        .{ "c AS (SELECT custLC, day, SUM(amount) AS amount, MAX(kind) AS top, ANY_VALUE(note) AS note FROM ev GROUP BY custLC, day" ++ window, &[_]bool{ false, false, true, true, true, true }, .group_agg },
        // An inner join keeps the payload as declared; a left join makes
        // it nullable.
        .{ "c AS (SELECT e.custLC, e.day, e.amount, k.label, k.hint FROM ev e INNER JOIN kinds k ON e.kind = k.kind" ++ window, &[_]bool{ false, false, false, false, true, true }, .hash_probe },
        .{ "c AS (SELECT e.custLC, e.day, e.amount, k.label, k.hint FROM ev e LEFT JOIN kinds k ON e.kind = k.kind" ++ window, &[_]bool{ false, false, false, true, true, true }, .hash_probe },
        // A union column is nullable when either arm's is.
        .{ "base AS (SELECT custLC, day, amount, kind FROM ev), " ++
            "c AS (SELECT custLC, day, amount, kind AS tag FROM base WHERE amount < 40 UNION ALL SELECT custLC, day, amount, amount AS tag FROM base WHERE amount >= 40" ++ window, &[_]bool{ false, false, false, false, true }, .union_all },
        .{ "base AS (SELECT custLC, day, amount, kind FROM ev), " ++
            "c AS (SELECT custLC, day, amount, kind AS tag FROM base WHERE amount < 40 UNION ALL SELECT custLC, day, amount, NULLIF(amount, 50) AS tag FROM base WHERE amount >= 40" ++ window, &[_]bool{ false, false, false, true, true }, .union_all },
    }) |case| {
        {
            var query = try helpers.runSql(allocator, db, "WITH KEYED BY (custLC) " ++ case[0]);
            defer query.deinit();
            try std.testing.expect(regional_op_count(query.cq.query, case[2]) != 0);
            while (try query.next()) |_| {}
        }
        try expect_nullable(allocator, db, case[0], case[1]);
        try expect_keyed_matches(allocator, db, case[0], "prior");
    }
}

// The second run of a statement reuses the compiled program. One compiled
// over a NOT NULL column copies it out without a bitmap, so it must not
// outlive the column's declaration.
test "keyed region: a cached program follows a column that became nullable" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try setup(allocator, std.testing.io, tmp.dir);
    defer db.close();
    const body =
        \\w AS (
        \\  SELECT custLC, seq, amount, LAG(seq) OVER (PARTITION BY custLC ORDER BY seq) AS prior
        \\  FROM facts
        \\)
        \\SELECT * FROM w ORDER BY custLC, seq
    ;
    try helpers.exec(allocator, db, "CREATE TABLE facts (seq BIGINT PRIMARY KEY, custLC VARCHAR(32) NOT NULL, amount BIGINT NOT NULL)");
    try helpers.exec(allocator, db, "INSERT INTO facts VALUES (1,'cust_0',10),(2,'cust_0',20),(3,'cust_1',30)");
    const declared = try db.openTable("facts", .{});
    try declared.flush();
    try expect_nullable(allocator, db, body, &.{ false, false, false, true });
    try expect_keyed_matches(allocator, db, body, "prior");

    try helpers.exec(allocator, db, "DROP TABLE facts");
    try helpers.exec(allocator, db, "CREATE TABLE facts (seq BIGINT PRIMARY KEY, custLC VARCHAR(32) NOT NULL, amount BIGINT)");
    try helpers.exec(allocator, db, "INSERT INTO facts VALUES (1,'cust_0',10),(2,'cust_0',NULL),(3,'cust_1',NULL)");
    const relaxed = try db.openTable("facts", .{});
    try relaxed.flush();
    try expect_nullable(allocator, db, body, &.{ false, false, true, true });
    try expect_keyed_matches(allocator, db, body, "prior");
    const keyed = try run_to_text(allocator, db, "WITH KEYED BY (custLC) " ++ body, "prior");
    defer allocator.free(keyed);
    try std.testing.expect(std.mem.indexOf(u8, keyed, "cust_0|2|~|1") != null);
}
