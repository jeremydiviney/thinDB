//! thinDB v0.1 bench suite. Run via `zig build bench -Doptimize=ReleaseFast`.
//!
//! Each bench:
//!   1. Wipes its scratch directory under `.bench-data/<name>/`
//!   2. Generates the input data (allocation excluded from timing)
//!   3. Times the measured operation with std.time.Timer
//!   4. Reports rows, elapsed, rows/sec, ns/row, optional bytes/sec

const std = @import("std");
const thindb = @import("thindb");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const default_rows: usize = 1_000_000;

const Row = struct {
    id: i64,
    qty: i32,
    active: bool,
    tag: []const u8,
};

const schema = thindb.Schema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "qty", .type = .int },
        .{ .name = "active", .type = .boolean },
        .{ .name = "tag", .type = .{ .varchar = 16 } },
    },
    .order_key = &.{"id"},
    .unique = false,
};

const order_key = [_][]const u8{"id"};
const options = thindb.TableOptions{
    .order_key = &order_key,
    .unique = false,
    .row_group_size = 65_536,
};

const tag_pool = [_][]const u8{ "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta" };

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = Io.Threaded.global_single_threaded.io();

    const n_rows: usize = default_rows;

    std.debug.print("\nthinDB v{s} bench  ({d} rows)\n", .{ thindb.version, n_rows });
    std.debug.print("--------------------------------------------------------------------------------\n", .{});

    try benchInsertMemtable(allocator, io, n_rows);
    try benchInsertAndFlush(allocator, io, n_rows);
    try benchFlushPhases(allocator, io, n_rows);
    try benchSustainedInsert(allocator, io, n_rows);
    try benchScan(allocator, io, n_rows);
    try benchScanColdVsWarm(allocator, io, n_rows);
    try benchScanFilterNonOrderKey(allocator, io, n_rows);
    try benchScanFilterOrderKeyNarrow(allocator, io, n_rows);
    try benchScanFilterOrderKeyMid(allocator, io, n_rows);
    try benchAggregateGlobal(allocator, io, n_rows);
    try benchGroupByTag(allocator, io, n_rows);

    std.debug.print("--------------------------------------------------------------------------------\n\n", .{});
}

// ----------------------------------------------------------------------------
// Benchmarks
// ----------------------------------------------------------------------------

fn benchInsertMemtable(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/insert_memtable");
    defer dir.close(io);

    // Disable auto-flush so this bench measures raw memtable append speed,
    // not insert+flush combined. The "insert + flush" bench below covers
    // the combined path explicitly.
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();
    const t = try db.table("t", schema, options);

    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);

    const t0 = Io.Clock.awake.now(io);
    try t.insert(rows);
    const elapsed = elapsedNs(io, t0);

    try report("insert memtable", n_rows, elapsed, null);
    // Final manual flush so the data the bench produced lands on disk in case
    // anything in this process tries to read from it later (no monitoring/
    // background-flush service yet).
    try t.flush();
}

fn benchInsertAndFlush(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/insert_and_flush");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    const t = try db.table("t", schema, options);

    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);

    const t0 = Io.Clock.awake.now(io);
    try t.insert(rows);
    try t.flush();
    const elapsed = elapsedNs(io, t0);

    try report("insert + flush", n_rows, elapsed, null);
}

fn benchScan(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/scan");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    const t = try db.table("t", schema, options);

    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);
    try t.flush();

    var checksum: i64 = 0;
    var scanned: usize = 0;

    const t0 = Io.Clock.awake.now(io);
    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    while (try q.next()) |batch| {
        scanned += batch.row_count;
        const ids = batch.values[0].data.bigint;
        for (ids) |v| checksum +%= v;
    }
    const elapsed = elapsedNs(io, t0);

    std.mem.doNotOptimizeAway(&checksum);
    if (scanned != n_rows) return error.RowCountMismatch;

    try report("scan (flushed)", n_rows, elapsed, null);
}

/// Filter on `qty` — not the order key. Min/max stats on qty are unhelpful
/// because qty cycles 0..99 inside every row group (every RG has min=0, max=99),
/// so no row group can be pruned. Forces a full scan.
fn benchScanFilterNonOrderKey(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/scan_filter_non_order_key");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    const t = try db.table("t", schema, options);

    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);
    try t.flush();

    var checksum: i64 = 0;
    var matched: usize = 0;

    const t0 = Io.Clock.awake.now(io);
    var base = try thindb.scan(allocator, t);
    var q = try base.filter(thindb.leafExpr("qty", .gt, .{ .int = 50 }));
    defer q.deinit();
    while (try q.next()) |batch| {
        matched += batch.row_count;
        const ids = batch.values[0].data.bigint;
        for (ids) |v| checksum +%= v;
    }
    const elapsed = elapsedNs(io, t0);

    std.mem.doNotOptimizeAway(&checksum);

    var label_buf: [80]u8 = undefined;
    const label = try std.fmt.bufPrint(&label_buf, "filter qty>50 [non-order-key] ({d})", .{matched});
    try report(label, n_rows, elapsed, null);
}

/// Filter on `id` — the order key. With row_group_size=65536, each row group
/// covers 65536 consecutive ids, so a tight range like `id < 50_000` hits at
/// most one row group; range pruning skips the other 15.
fn benchScanFilterOrderKeyNarrow(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/scan_filter_order_key_narrow");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    const t = try db.table("t", schema, options);

    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);
    try t.flush();

    var checksum: i64 = 0;
    var matched: usize = 0;

    const t0 = Io.Clock.awake.now(io);
    var base = try thindb.scan(allocator, t);
    var q = try base.filter(thindb.leafExpr("id", .lt, .{ .bigint = 50_000 }));
    defer q.deinit();
    while (try q.next()) |batch| {
        matched += batch.row_count;
        const ids = batch.values[0].data.bigint;
        for (ids) |v| checksum +%= v;
    }
    const elapsed = elapsedNs(io, t0);

    std.mem.doNotOptimizeAway(&checksum);

    var label_buf: [80]u8 = undefined;
    const label = try std.fmt.bufPrint(&label_buf, "filter id<50k [order key, narrow] ({d})", .{matched});
    try report(label, n_rows, elapsed, null);
}

/// Filter on `id` covering ~half the rows. Pruning skips roughly half the
/// row groups but still does real scan work on the rest.
fn benchScanFilterOrderKeyMid(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/scan_filter_order_key_mid");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    const t = try db.table("t", schema, options);

    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);
    try t.flush();

    var checksum: i64 = 0;
    var matched: usize = 0;

    const half: i64 = @intCast(n_rows / 2);
    const t0 = Io.Clock.awake.now(io);
    var base = try thindb.scan(allocator, t);
    var q = try base.filter(thindb.leafExpr("id", .gte, .{ .bigint = half }));
    defer q.deinit();
    while (try q.next()) |batch| {
        matched += batch.row_count;
        const ids = batch.values[0].data.bigint;
        for (ids) |v| checksum +%= v;
    }
    const elapsed = elapsedNs(io, t0);

    std.mem.doNotOptimizeAway(&checksum);

    var label_buf: [80]u8 = undefined;
    const label = try std.fmt.bufPrint(&label_buf, "filter id>=N/2 [order key, half] ({d})", .{matched});
    try report(label, n_rows, elapsed, null);
}

/// Many small batches in a tight loop, exercising the auto-flush trigger.
/// Demonstrates the "sustained ingest" pattern where data lands in segments
/// continuously rather than via one giant batch.
/// Break out the cost of `flush()` into its three logical phases:
///   1. sort the memtable into order-key order
///   2. flate-compress the columnar payload
///   3. write the resulting bytes to disk + update the manifest
/// Used to identify the single-core upper bound on flush throughput.
fn benchFlushPhases(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/flush_phases");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();
    const t = try db.table("t", schema, options);

    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);

    // ---- Phase 1: sort (buildSortedSnapshot) ----
    const t_sort_start = Io.Clock.awake.now(io);
    var snapshot = try t.memtable.buildSortedSnapshot(allocator, t.order_key_indices);
    defer snapshot.deinit();
    const sort_ns = elapsedNs(io, t_sort_start);

    // ---- Compute the uncompressed payload size ----
    var raw_bytes: u64 = 0;
    for (snapshot.views) |v| {
        raw_bytes += switch (v.data) {
            .int => |s| s.len * 4,
            .bigint => |s| s.len * 8,
            .boolean => |s| s.len,
            .varchar => |sv| sv.offsets.len * 4 + sv.bytes.len,
            .string => |sv| sv.offsets.len * 4 + sv.bytes.len,
            .float => |s| s.len * 4,
            .double => |s| s.len * 8,
            .date => |s| s.len * 4,
            .datetime => |s| s.len * 8,
        };
    }

    // ---- Phase 2: serialize + compress (by writing the segment to memory) ----
    // We can't easily isolate compression from disk write without changing the
    // writer, so do the segment write twice: once to a scratch dir (this is
    // the compress + write combo), once to /dev/null-equivalent by writing
    // then deleting. The difference is dominated by disk I/O.
    var name_buf: [32]u8 = undefined;
    const file_name = try std.fmt.bufPrint(&name_buf, "phases.dat", .{});

    const t_write_start = Io.Clock.awake.now(io);
    var info = try thindb.storage.writeSegment(
        allocator,
        io,
        dir,
        file_name,
        schema,
        999_999,
        t.schema_fingerprint,
        65_536,
        snapshot.views,
    );
    defer info.deinit(allocator);
    const compress_and_write_ns = elapsedNs(io, t_write_start);

    // ---- Report ----
    const total_ns = sort_ns + compress_and_write_ns;
    const seconds = @as(f64, @floatFromInt(total_ns)) / 1e9;
    const mb_per_s = (@as(f64, @floatFromInt(raw_bytes)) / 1_048_576.0) / seconds;
    std.debug.print(
        "  flush phases                        {d} rows  raw={d:.1}MB  sort={d:.1}ms  compress+write={d:.1}ms  total={d:.1}ms  {d:.1} MB/s (raw)\n",
        .{
            n_rows,
            @as(f64, @floatFromInt(raw_bytes)) / 1_048_576.0,
            @as(f64, @floatFromInt(sort_ns)) / 1e6,
            @as(f64, @floatFromInt(compress_and_write_ns)) / 1e6,
            @as(f64, @floatFromInt(total_ns)) / 1e6,
            mb_per_s,
        },
    );

    // Now time JUST a flate compress of the equivalent raw byte stream so we
    // can subtract the disk-write part. Build a single flat buffer of all
    // column blocks concatenated (matching what writeSegment internally feeds
    // to compression_mod.compress, minus the per-block boundaries).
    const concat = try allocator.alloc(u8, raw_bytes);
    defer allocator.free(concat);
    var cur: usize = 0;
    for (snapshot.views) |v| {
        switch (v.data) {
            .int => |s| {
                for (s) |x| {
                    std.mem.writeInt(i32, concat[cur..][0..4], x, .little);
                    cur += 4;
                }
            },
            .bigint => |s| {
                for (s) |x| {
                    std.mem.writeInt(i64, concat[cur..][0..8], x, .little);
                    cur += 8;
                }
            },
            .boolean => |s| {
                @memcpy(concat[cur..][0..s.len], s);
                cur += s.len;
            },
            .varchar, .string => |sv| {
                const off_bytes = sv.offsets.len * 4;
                @memcpy(concat[cur..][0..off_bytes], std.mem.sliceAsBytes(sv.offsets));
                cur += off_bytes;
                @memcpy(concat[cur..][0..sv.bytes.len], sv.bytes);
                cur += sv.bytes.len;
            },
            else => {},
        }
    }

    const t_comp_start = Io.Clock.awake.now(io);
    const compressed = try thindb.storage.compression.compress(allocator, concat);
    defer allocator.free(compressed);
    const compress_only_ns = elapsedNs(io, t_comp_start);

    const compress_seconds = @as(f64, @floatFromInt(compress_only_ns)) / 1e9;
    const compress_mb_per_s = (@as(f64, @floatFromInt(raw_bytes)) / 1_048_576.0) / compress_seconds;
    std.debug.print(
        "  zstd compress (level 3, one shot)   {d:.1}MB → {d:.1}MB  {d:.1}ms  {d:.1} MB/s (input)  ratio={d:.2}\n",
        .{
            @as(f64, @floatFromInt(raw_bytes)) / 1_048_576.0,
            @as(f64, @floatFromInt(compressed.len)) / 1_048_576.0,
            @as(f64, @floatFromInt(compress_only_ns)) / 1e6,
            compress_mb_per_s,
            @as(f64, @floatFromInt(raw_bytes)) / @as(f64, @floatFromInt(compressed.len)),
        },
    );

    // Implied write-only time = compress+write − compress-only (approx, since
    // segment writer slices compression by row group rather than one shot).
    const implied_write_ns = if (compress_and_write_ns > compress_only_ns)
        compress_and_write_ns - compress_only_ns
    else
        0;
    const write_seconds = @as(f64, @floatFromInt(implied_write_ns)) / 1e9;
    const write_mb_per_s = if (write_seconds > 0)
        (@as(f64, @floatFromInt(compressed.len)) / 1_048_576.0) / write_seconds
    else
        0;
    std.debug.print(
        "  disk write (implied: compr+wr − compr-only)  {d:.1}ms  {d:.1} MB/s (compressed bytes)\n",
        .{ @as(f64, @floatFromInt(implied_write_ns)) / 1e6, write_mb_per_s },
    );
}

fn benchSustainedInsert(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/sustained_insert");
    defer dir.close(io);

    const batch_size: usize = 1_000;
    // Set auto-flush at every 10 batches → segments accumulate during the bench.
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = 10 * batch_size,
        .auto_flush_secs = 0,
    });
    defer db.close();
    const t = try db.table("t", schema, options);

    const batch = try buildRows(allocator, batch_size);
    defer allocator.free(batch);

    const t0 = Io.Clock.awake.now(io);
    var done: usize = 0;
    while (done + batch_size <= n_rows) : (done += batch_size) {
        for (batch, 0..) |*r, i| r.id = @intCast(done + i);
        try t.insert(batch);
    }
    try t.flush(); // ensure final memtable lands on disk
    const elapsed = elapsedNs(io, t0);

    var label_buf: [80]u8 = undefined;
    const label = try std.fmt.bufPrint(&label_buf, "sustained insert ({d}×{d} → {d} segs)", .{
        n_rows / batch_size, batch_size, t.segmentCount(),
    });
    try report(label, n_rows, elapsed, null);
}

/// Same scan run twice against the same Database — first call populates the
/// LRU decompressed-block cache, second hits it.
fn benchScanColdVsWarm(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/scan_warm");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    const t = try db.table("t", schema, options);

    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);
    try t.flush();

    // Cold pass: cache empty, every column block decompresses.
    const t_cold = Io.Clock.awake.now(io);
    {
        var checksum: i64 = 0;
        var q = try thindb.scan(allocator, t);
        defer q.deinit();
        while (try q.next()) |b| for (b.values[0].data.bigint) |v| {
            checksum +%= v;
        };
        std.mem.doNotOptimizeAway(&checksum);
    }
    const elapsed_cold = elapsedNs(io, t_cold);

    // Warm pass: same data, cache hits skip the decompress.
    const t_warm = Io.Clock.awake.now(io);
    {
        var checksum: i64 = 0;
        var q = try thindb.scan(allocator, t);
        defer q.deinit();
        while (try q.next()) |b| for (b.values[0].data.bigint) |v| {
            checksum +%= v;
        };
        std.mem.doNotOptimizeAway(&checksum);
    }
    const elapsed_warm = elapsedNs(io, t_warm);

    try report("scan cold (cache populating)", n_rows, elapsed_cold, null);
    try report("scan warm (cache hits)", n_rows, elapsed_warm, null);
}

/// COUNT + SUM + MIN + MAX over all rows, no grouping.
fn benchAggregateGlobal(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/aggregate_global");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    const t = try db.table("t", schema, options);

    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);
    try t.flush();

    const t0 = Io.Clock.awake.now(io);
    var base = try thindb.scan(allocator, t);
    var q = try base.aggregate(&.{
        .{ .func = .count, .as = "n" },
        .{ .func = .sum, .col = "qty", .as = "total" },
        .{ .func = .min, .col = "qty", .as = "lo" },
        .{ .func = .max, .col = "qty", .as = "hi" },
    });
    defer q.deinit();
    const b = (try q.next()).?;
    const checksum = b.values[0].data.bigint[0] + b.values[1].data.bigint[0] + b.values[2].data.int[0] + b.values[3].data.int[0];
    std.mem.doNotOptimizeAway(&checksum);
    const elapsed = elapsedNs(io, t0);

    try report("aggregate count+sum+min+max", n_rows, elapsed, null);
}

/// GROUP BY tag (~8 groups), aggregating COUNT + SUM(qty).
fn benchGroupByTag(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/group_by_tag");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    const t = try db.table("t", schema, options);

    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);
    try t.flush();

    const t0 = Io.Clock.awake.now(io);
    var base = try thindb.scan(allocator, t);
    var q = try base.groupBy(&.{"tag"}, &.{
        .{ .func = .count, .as = "n" },
        .{ .func = .sum, .col = "qty", .as = "total" },
    });
    defer q.deinit();
    var n_groups: usize = 0;
    var checksum: i64 = 0;
    while (try q.next()) |b| {
        n_groups += b.row_count;
        for (b.values[1].data.bigint) |v| checksum +%= v;
    }
    std.mem.doNotOptimizeAway(&checksum);
    const elapsed = elapsedNs(io, t0);

    var label_buf: [80]u8 = undefined;
    const label = try std.fmt.bufPrint(&label_buf, "groupBy tag count+sum ({d} groups)", .{n_groups});
    try report(label, n_rows, elapsed, null);
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

fn buildRows(allocator: Allocator, n: usize) ![]Row {
    const rows = try allocator.alloc(Row, n);
    for (rows, 0..) |*r, i| {
        r.* = .{
            .id = @intCast(i),
            .qty = @intCast(i % 100),
            .active = (i & 1) == 0,
            .tag = tag_pool[i % tag_pool.len],
        };
    }
    return rows;
}

fn elapsedNs(io: Io, t0: Io.Timestamp) u64 {
    const t1 = Io.Clock.awake.now(io);
    const dur = t0.durationTo(t1);
    return @intCast(dur.toNanoseconds());
}

fn freshDir(io: Io, sub_path: []const u8) !Io.Dir {
    const cwd = Io.Dir.cwd();
    try cwd.deleteTree(io, sub_path);
    return cwd.createDirPathOpen(io, sub_path, .{});
}

fn report(name: []const u8, rows: u64, elapsed_ns: u64, bytes: ?u64) !void {
    const seconds = @as(f64, @floatFromInt(elapsed_ns)) / 1e9;
    const ms = seconds * 1000.0;
    const rps_m = (@as(f64, @floatFromInt(rows)) / seconds) / 1e6;
    const ns_per_row = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(rows));

    if (bytes) |b| {
        const mb_per_sec = (@as(f64, @floatFromInt(b)) / seconds) / (1024.0 * 1024.0);
        const kb = @as(f64, @floatFromInt(b)) / 1024.0;
        std.debug.print(
            "  {s:<32} {d:>10} rows  {d:>8.2} ms  {d:>7.2} M rows/s  {d:>6.1} ns/row  {d:>8.1} KB  {d:>6.1} MB/s\n",
            .{ name, rows, ms, rps_m, ns_per_row, kb, mb_per_sec },
        );
    } else {
        std.debug.print(
            "  {s:<32} {d:>10} rows  {d:>8.2} ms  {d:>7.2} M rows/s  {d:>6.1} ns/row\n",
            .{ name, rows, ms, rps_m, ns_per_row },
        );
    }
}
