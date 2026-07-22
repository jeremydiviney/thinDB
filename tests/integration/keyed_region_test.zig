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
    const db = try thindb.Database.open(allocator, io, dir, .{});
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
    var q = try helpers.runSql(allocator, db, sql);
    defer q.deinit();
    const schema = q.outputSchema();
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
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
