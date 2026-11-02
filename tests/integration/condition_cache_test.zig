//! The condition cache (DESIGN.md §6.4) changes no result. Two databases take
//! the same writes, one with the cache off, and answer every query alike:
//! queries run twice so the second reads the records the first kept, filters
//! share AND arms so one filter starts from another's records, deletes land
//! after the records were kept, rows stay unflushed, and compaction writes
//! new segments. Filters over computed values (functions, regular
//! expressions, UDFs) are kept too, and a repeat computes none of them. A
//! top-N over row groups in key order keeps and reads records too.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

const queries = [_][]const u8{
    "SELECT COUNT(*) FROM t WHERE s LIKE '%ab%'",
    "SELECT id FROM t WHERE s LIKE '%ab%' ORDER BY id",
    "SELECT id FROM t WHERE s LIKE '%ab%' AND k < 4 ORDER BY id",
    "SELECT id FROM t WHERE k < 4 AND s LIKE '%ab%' AND u NOT LIKE '%c%' ORDER BY id",
    "SELECT id FROM t WHERE u NOT LIKE '%c%' ORDER BY id",
    "SELECT COUNT(*) FROM t WHERE u NOT LIKE '%c%'",
    "SELECT COUNT(*) FROM t WHERE u NOT LIKE '%c%' AND s LIKE '%ab%'",
    "SELECT COUNT(*) FROM t WHERE s LIKE '%ab%' GROUP BY u ORDER BY 1",
    "SELECT id FROM t WHERE s = 'abc' ORDER BY id",
    "SELECT id FROM t WHERE s <> '' AND s LIKE 'a%' ORDER BY id",
    "SELECT COUNT(*) FROM t WHERE s IN ('abc', 'xab') AND u LIKE '%b%'",
    "SELECT id FROM t WHERE s LIKE '%ab%' OR u LIKE '%zz%' ORDER BY id",
    "SELECT MAX(id) FROM t WHERE u LIKE '%b%' AND s NOT LIKE '%x%'",
    "SELECT id FROM t WHERE UPPER(s) = 'ABC' ORDER BY id",
    "SELECT COUNT(*) FROM t WHERE LENGTH(s) > 2 AND k < 4",
    "SELECT id FROM t WHERE LENGTH(s) = k ORDER BY id",
    "SELECT COUNT(*) FROM t WHERE LOWER(s) = LOWER(u)",
    "SELECT id FROM t WHERE REGEXP_LIKE(s, '^a.b') ORDER BY id",
    "SELECT SUM(k) FROM t WHERE REGEXP_LIKE(u, 'a|z') AND s LIKE '%b%'",
    "SELECT COUNT(*) FROM t WHERE CONCAT(s, u) LIKE '%bx%'",
    "SELECT id FROM t WHERE UPPER(s) IN ('ABC', 'XAB') AND u LIKE '%a%' ORDER BY id",
    "SELECT COUNT(*) FROM t WHERE CASE WHEN k < 3 THEN s ELSE u END LIKE '%a%'",
    "SELECT id, UPPER(u) FROM t WHERE LENGTH(u) = 2 AND UPPER(s) LIKE 'A%' ORDER BY id",
    "SELECT k FROM t WHERE LENGTH(u) = 2 GROUP BY k ORDER BY k",
    "SELECT MAX(id) FROM t WHERE id % 7 = 3 AND LENGTH(s) < 3",
    "SELECT id FROM t WHERE bucket3(k) = 1 AND s LIKE '%a%' ORDER BY id",
    "SELECT COUNT(*) FROM t WHERE bucket3(id) = 2",
    "SELECT COUNT(*) FROM t WHERE vbucket3(id) = 2",
    "SELECT COUNT(*) FROM t WHERE bucket3(id) = 0 AND k > 1 GROUP BY u ORDER BY 1 DESC LIMIT 5",
    "SELECT SUM(id) FROM t WHERE REGEXP_LIKE(u, 'b.') GROUP BY k ORDER BY 1",
    "SELECT k FROM t WHERE UPPER(s) LIKE '%B%' AND LENGTH(u) > 1 GROUP BY k ORDER BY k",
    "SELECT id FROM t WHERE s LIKE '%ab%' ORDER BY id DESC LIMIT 7",
    "SELECT id, k FROM t WHERE u NOT LIKE '%c%' AND k < 6 ORDER BY k, id LIMIT 9",
    "SELECT * FROM t WHERE s LIKE '%a%' ORDER BY id LIMIT 10 OFFSET 3",
};

/// `bucket3(x)`: x mod 3, counting the rows it is called on.
const Bucket = struct {
    rows: std.atomic.Value(usize) = .init(0),

    fn kernel(ctx: *const thindb.udf.ScalarContext, args: []const thindb.storage.ColumnView, out: *thindb.engine.ColumnStore, count: usize) !void {
        const self: *Bucket = @ptrCast(@alignCast(ctx.user_data.?));
        _ = self.rows.fetchAdd(count, .monotonic);
        for (args[0].data.bigint[0..count]) |x| try out.data.bigint.append(ctx.allocator, @mod(x, 3));
    }

    fn register(self: *Bucket, db: *thindb.Database, name: []const u8, volatility: thindb.udf.Volatility) !void {
        try db.registerScalarUdf(.{
            .name = name,
            .arg_types = &.{.bigint},
            .return_type = .bigint,
            .volatility = volatility,
            .kernel = kernel,
            .user_data = self,
        });
    }
};

const Row = struct { id: i64, k: i64, s: []const u8, u: []const u8 };

const ALPHABET = "abcxz";

fn word(random: std.Random, buf: []u8) []const u8 {
    const len = random.uintLessThan(usize, buf.len + 1);
    for (buf[0..len]) |*c| c.* = ALPHABET[random.uintLessThan(usize, ALPHABET.len)];
    return buf[0..len];
}

fn insertRows(allocator: std.mem.Allocator, dbs: anytype, random: std.Random, first_id: i64, n: usize) !void {
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    const text = try allocator.alloc(u8, n * 8);
    defer allocator.free(text);
    for (rows, 0..) |*r, i| r.* = .{
        .id = first_id + @as(i64, @intCast(i)),
        .k = random.intRangeLessThan(i64, 0, 8),
        .s = word(random, text[i * 8 ..][0..4]),
        .u = word(random, text[i * 8 + 4 ..][0..4]),
    };
    for (dbs) |db| try (try db.openTable("t", .{})).insert(rows);
}

test "the condition cache answers as a scan with no cache does, through repeats, shared arms, deletes, unflushed rows and compaction" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0xc0dec0de);
    const random = prng.random();

    inline for (.{ 1, 4 }) |dop| {
        var tmp_on = std.testing.tmpDir(.{});
        defer tmp_on.cleanup();
        var tmp_off = std.testing.tmpDir(.{});
        defer tmp_off.cleanup();
        const on = try thindb.Database.open(allocator, std.testing.io, tmp_on.dir, .{ .row_group_size = 64, .max_dop = dop, .auto_flush_secs = 0 });
        defer on.close();
        const off = try thindb.Database.open(allocator, std.testing.io, tmp_off.dir, .{ .row_group_size = 64, .max_dop = dop, .auto_flush_secs = 0, .condition_cache = false });
        defer off.close();
        const dbs = [_]*thindb.Database{ on, off };
        var bucket: Bucket = .{};
        for (dbs) |db| {
            try bucket.register(db, "bucket3", .immutable);
            try bucket.register(db, "vbucket3", .@"volatile");
            try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, s VARCHAR(8) NOT NULL, u VARCHAR(8) NOT NULL)");
        }

        var next_id: i64 = 0;
        try insertRows(allocator, dbs, random, next_id, 3000);
        next_id += 3000;
        for (dbs) |db| try (try db.openTable("t", .{})).flush();

        var sql: std.ArrayList(u8) = .empty;
        defer sql.deinit(allocator);
        for (0..6) |round| {
            for (queries) |q| for (0..2) |_| {
                const want = try collectBigints(allocator, off, q);
                defer allocator.free(want);
                const got = try collectBigints(allocator, on, q);
                defer allocator.free(got);
                std.testing.expectEqualSlices(i64, want, got) catch |err| {
                    std.debug.print("round {d}, dop {d}: {s}\n", .{ round, dop, q });
                    return err;
                };
            };

            sql.clearRetainingCapacity();
            try sql.appendSlice(allocator, "DELETE FROM t WHERE id IN (");
            for (0..40) |i| {
                if (i > 0) try sql.appendSlice(allocator, ", ");
                try sql.print(allocator, "{d}", .{random.intRangeLessThan(i64, 0, next_id)});
            }
            try sql.appendSlice(allocator, ")");
            for (dbs) |db| try exec(allocator, db, sql.items);

            try insertRows(allocator, dbs, random, next_id, 500);
            next_id += 500;
            for (dbs) |db| try (try db.openTable("t", .{})).flush();
            try insertRows(allocator, dbs, random, next_id, 30);
            next_id += 30;
            if (round == 3) for (dbs) |db| try (try db.openTable("t", .{})).compact();
        }
    }
}

/// `slen(s)`: s's length in bytes, counting the rows it is called on.
const Slen = struct {
    rows: std.atomic.Value(usize) = .init(0),

    fn kernel(ctx: *const thindb.udf.ScalarContext, args: []const thindb.storage.ColumnView, out: *thindb.engine.ColumnStore, count: usize) !void {
        const self: *Slen = @ptrCast(@alignCast(ctx.user_data.?));
        _ = self.rows.fetchAdd(count, .monotonic);
        for (0..count) |i| try out.data.bigint.append(ctx.allocator, @intCast(args[0].data.string.rowBytes(i).len));
    }
};

test "a computed filter over a dictionary block computes once per entry, NULL included, or over just the rows left, and answers as row by row" {
    const allocator = std.testing.allocator;
    const words = [_][]const u8{ "ab", "Abc", "x", "zzzz", "" };
    const rows = 12288;

    inline for (.{ 1, 4 }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .row_group_size = 4096, .max_dop = dop, .auto_flush_secs = 0, .condition_cache = false });
        defer db.close();
        var slen: Slen = .{};
        try db.registerScalarUdf(.{ .name = "slen", .arg_types = &.{.string}, .return_type = .bigint, .volatility = .immutable, .kernel = Slen.kernel, .user_data = &slen });
        try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, s STRING NOT NULL, n STRING, u STRING NOT NULL)");
        var sql: std.ArrayList(u8) = .empty;
        defer sql.deinit(allocator);
        var id: usize = 0;
        while (id < rows) {
            sql.clearRetainingCapacity();
            try sql.appendSlice(allocator, "INSERT INTO t VALUES ");
            for (0..1024) |b| {
                if (b > 0) try sql.appendSlice(allocator, ", ");
                try sql.print(allocator, "({d}, {d}, '{s}', ", .{ id, id % 100, words[id * 7 % words.len] });
                if (id % 3 == 0) try sql.appendSlice(allocator, "NULL") else try sql.print(allocator, "'{s}'", .{words[id % 4]});
                try sql.print(allocator, ", 'u{d}')", .{id % 1000});
                id += 1;
            }
            try exec(allocator, db, sql.items);
        }
        try (try db.openTable("t", .{})).flush();

        var want: [6]std.ArrayList(i64) = @splat(.empty);
        defer for (&want) |*w| w.deinit(allocator);
        for (0..rows) |r| {
            const s = words[r * 7 % words.len];
            const n: ?[]const u8 = if (r % 3 == 0) null else words[r % 4];
            var u_buf: [8]u8 = undefined;
            const u = try std.fmt.bufPrint(&u_buf, "u{d}", .{r % 1000});
            const passes = [6]bool{
                s.len == 2,
                n == null,
                n != null and std.ascii.eqlIgnoreCase(n.?, "abc"),
                n == null or n.?.len > 2,
                r % 100 == 7 and std.mem.endsWith(u8, u, "07"),
                std.mem.indexOfScalar(u8, s, 'b') != null or (n != null and std.mem.indexOfScalar(u8, n.?, 'b') != null),
            };
            for (passes, &want) |p, *w| if (p) try w.append(allocator, @intCast(r));
        }

        const cases = [6][]const u8{
            "SELECT id FROM t WHERE slen(s) = 2 ORDER BY id",
            "SELECT id FROM t WHERE COALESCE(n, 'nil') = 'nil' ORDER BY id",
            "SELECT id FROM t WHERE UPPER(n) = 'ABC' ORDER BY id",
            "SELECT id FROM t WHERE n IS NULL OR LENGTH(n) > 2 ORDER BY id",
            "SELECT id FROM t WHERE k = 7 AND RIGHT(u, 2) = '07' ORDER BY id",
            "SELECT id FROM t WHERE CONCAT(s, COALESCE(n, '-')) LIKE '%b%' ORDER BY id",
        };
        for (cases, want) |q, w| {
            const before = slen.rows.load(.monotonic);
            const got = try collectBigints(allocator, db, q);
            defer allocator.free(got);
            std.testing.expectEqualSlices(i64, w.items, got) catch |err| {
                std.debug.print("dop {d}: {s}\n", .{ dop, q });
                return err;
            };
            // Each row group's dictionary holds five values.
            if (std.mem.indexOf(u8, q, "slen") != null) try std.testing.expect(slen.rows.load(.monotonic) - before <= 3 * words.len);
        }
    }
}

test "a computed filter over row groups larger than its chunk keeps exactly the rows its expression passes" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x5eedc4a1);
    const random = prng.random();

    inline for (.{ 1, 4 }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .row_group_size = 20000, .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        var bucket: Bucket = .{};
        try bucket.register(db, "bucket3", .immutable);
        try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, s VARCHAR(8) NOT NULL, u VARCHAR(8) NOT NULL)");
        try insertRows(allocator, [_]*thindb.Database{db}, random, 0, 45000);
        try (try db.openTable("t", .{})).flush();

        const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id");
        defer allocator.free(ids);
        const ks = try collectBigints(allocator, db, "SELECT k FROM t ORDER BY id");
        defer allocator.free(ks);
        var want_all: std.ArrayList(i64) = .empty;
        defer want_all.deinit(allocator);
        var want_low_k: std.ArrayList(i64) = .empty;
        defer want_low_k.deinit(allocator);
        for (ids, ks) |id, k| {
            if (@mod(id, 3) != 1) continue;
            try want_all.append(allocator, id);
            if (k < 4) try want_low_k.append(allocator, id);
        }

        const cases = .{
            .{ "SELECT id FROM t WHERE bucket3(id) = 1 ORDER BY id", want_all.items },
            .{ "SELECT id FROM t WHERE bucket3(id) = 1 AND k < 4 ORDER BY id", want_low_k.items },
        };
        inline for (cases) |c| {
            const got = try collectBigints(allocator, db, c[0]);
            defer allocator.free(got);
            std.testing.expectEqualSlices(i64, c[1], got) catch |err| {
                std.debug.print("dop {d}: {s}\n", .{ dop, c[0] });
                return err;
            };
        }
    }
}

test "a repeated filter over a computed value reads its verdicts back: an immutable UDF is not called again, a volatile one is" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x0b5e55ed);
    const random = prng.random();

    inline for (.{ 1, 4 }) |dop| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .row_group_size = 64, .max_dop = dop, .auto_flush_secs = 0 });
        defer db.close();
        var immutable: Bucket = .{};
        try immutable.register(db, "bucket3", .immutable);
        var volatile_fn: Bucket = .{};
        try volatile_fn.register(db, "vbucket3", .@"volatile");
        try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, s VARCHAR(8) NOT NULL, u VARCHAR(8) NOT NULL)");
        const dbs = [_]*thindb.Database{db};
        try insertRows(allocator, dbs, random, 0, 2000);
        try (try db.openTable("t", .{})).flush();

        // The first run computes once per row the plain conjuncts leave.
        const all = "SELECT COUNT(*) FROM t";
        const cases = .{
            .{ .sql = "SELECT id FROM t WHERE bucket3(k) = 1 ORDER BY id", .probe = &immutable, .reaching = all, .repeat_calls = 0 },
            .{ .sql = "SELECT COUNT(*) FROM t WHERE bucket3(id) = 2 AND s LIKE '%a%'", .probe = &immutable, .reaching = "SELECT COUNT(*) FROM t WHERE s LIKE '%a%'", .repeat_calls = 0 },
            .{ .sql = "SELECT COUNT(*) FROM t WHERE vbucket3(id) = 2", .probe = &volatile_fn, .reaching = all, .repeat_calls = 2000 },
            .{ .sql = "SELECT k FROM t WHERE bucket3(id) = 1 GROUP BY k ORDER BY k", .probe = &immutable, .reaching = all, .repeat_calls = 0 },
            .{ .sql = "SELECT COUNT(*) FROM t WHERE bucket3(id) = 0 GROUP BY u ORDER BY 1 DESC LIMIT 5", .probe = &immutable, .reaching = all, .repeat_calls = 0 },
        };
        inline for (cases) |c| {
            const before = c.probe.rows.load(.monotonic);
            const first = try collectBigints(allocator, db, c.sql);
            defer allocator.free(first);
            const first_calls = c.probe.rows.load(.monotonic) - before;
            const reaching = try collectBigints(allocator, db, c.reaching);
            defer allocator.free(reaching);
            try std.testing.expectEqual(@as(usize, @intCast(reaching[0])), first_calls);
            const second = try collectBigints(allocator, db, c.sql);
            defer allocator.free(second);
            try std.testing.expectEqualSlices(i64, first, second);
            std.testing.expectEqual(@as(usize, c.repeat_calls), c.probe.rows.load(.monotonic) - before - first_calls) catch |err| {
                std.debug.print("dop {d}: {s}\n", .{ dop, c.sql });
                return err;
            };
        }
    }
}

test "under a random digest key, string and composite groups, distinct strings and kept filters answer as under the default key" {
    const allocator = std.testing.allocator;
    const keyed = queries ++ [_][]const u8{
        "SELECT COUNT(*) FROM t GROUP BY s ORDER BY 1",
        "SELECT MIN(id) FROM t GROUP BY s, u ORDER BY 1",
        "SELECT COUNT(*) FROM t GROUP BY u, k, s ORDER BY 1 DESC LIMIT 20",
        "SELECT COUNT(DISTINCT s) FROM t",
        "SELECT COUNT(DISTINCT u) FROM t WHERE k < 5",
        "SELECT COUNT(*) FROM t GROUP BY REPEAT(s, 80) ORDER BY 1",
        "SELECT COUNT(DISTINCT REPEAT(u, 80)) FROM t",
    };
    var want: [keyed.len][]i64 = undefined;
    var kept: usize = 0;
    defer for (want[0..kept]) |w| allocator.free(w);
    defer thindb.exec.setDigestSeed(0);

    // A key holds for the whole of a database's life, so each key gets its own.
    for ([_]u64{ 0, 0x9d1e_57a5_3c0f_b7e1 }) |seed| {
        thindb.exec.setDigestSeed(seed);
        var prng = std.Random.DefaultPrng.init(0xd16e57);
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .row_group_size = 64, .max_dop = 4, .auto_flush_secs = 0 });
        defer db.close();
        var bucket: Bucket = .{};
        try bucket.register(db, "bucket3", .immutable);
        try bucket.register(db, "vbucket3", .@"volatile");
        try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, s VARCHAR(8) NOT NULL, u VARCHAR(8) NOT NULL)");
        try insertRows(allocator, [_]*thindb.Database{db}, prng.random(), 0, 3000);
        try (try db.openTable("t", .{})).flush();
        try insertRows(allocator, [_]*thindb.Database{db}, prng.random(), 3000, 30);

        for (keyed, 0..) |q, i| for (0..2) |_| {
            const got = try collectBigints(allocator, db, q);
            if (kept <= i) {
                want[i] = got;
                kept += 1;
                continue;
            }
            defer allocator.free(got);
            std.testing.expectEqualSlices(i64, want[i], got) catch |err| {
                std.debug.print("seed {x}: {s}\n", .{ seed, q });
                return err;
            };
        };
    }
}
