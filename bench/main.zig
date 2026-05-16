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
    // Real multi-threaded Io so concurrent writers in benchGroupCommit can
    // actually overlap file syscalls (the `global_single_threaded` instance
    // serializes all Io ops through one queue, which masks any parallelism).
    var threaded: Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

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

    std.debug.print("\nCompaction scenarios\n", .{});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    try benchManySegmentsNoCompact(allocator, io);
    try benchManySegmentsWithCompact(allocator, io);
    try benchTombstonePressureCompact(allocator, io);
    try benchTierFillUp(allocator, io);

    std.debug.print("\nDurability (sync_mode)\n", .{});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    try benchDurabilityCost(allocator, io);

    std.debug.print("\nWAL (wal_enabled)\n", .{});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    try benchWalCost(allocator, io);

    std.debug.print("\nGroup commit (concurrent writers, wal=true, sync=per_flush)\n", .{});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    try benchGroupCommit(allocator, io);

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
            .tinyint => |s| s.len,
            .smallint => |s| s.len * 2,
            .largeint => |s| s.len * 16,
            .char => |sv| sv.offsets.len * 4 + sv.bytes.len,
            .decimal64 => |s| s.len * 8,
            .decimal128 => |s| s.len * 16,
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
        false, // no fsync — measuring compress+write throughput, not durability
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

    // ---- Isolated disk-write measurement ----
    // Read the segment we already wrote back into memory, then re-write it to
    // a fresh file and time only the writeFile call. This measures the syscall
    // path with no compression interleaved.
    const seg_bytes = try dir.readFileAlloc(io, file_name, allocator, .unlimited);
    defer allocator.free(seg_bytes);

    // One-shot write of the actual segment-sized buffer.
    const t_write_seg = Io.Clock.awake.now(io);
    try dir.writeFile(io, .{ .sub_path = "write_only_seg.dat", .data = seg_bytes });
    const write_seg_ns = elapsedNs(io, t_write_seg);
    const seg_mb = @as(f64, @floatFromInt(seg_bytes.len)) / 1_048_576.0;
    const seg_mb_per_s = seg_mb / (@as(f64, @floatFromInt(write_seg_ns)) / 1e9);
    std.debug.print(
        "  disk write (isolated, segment-sized)         {d:.2}MB  {d:.2}ms  {d:.1} MB/s\n",
        .{ seg_mb, @as(f64, @floatFromInt(write_seg_ns)) / 1e6, seg_mb_per_s },
    );

    // Larger one-shot writes to expose actual disk bandwidth, free of syscall
    // overhead. Use the segment bytes as filler, repeated.
    const sizes = [_]usize{ 16 * 1024 * 1024, 64 * 1024 * 1024, 256 * 1024 * 1024 };
    inline for (sizes) |target_size| {
        const big = try allocator.alloc(u8, target_size);
        defer allocator.free(big);
        // Fill so the FS / OS can't just zero-page-trick the write.
        var off: usize = 0;
        while (off + seg_bytes.len <= big.len) : (off += seg_bytes.len) {
            @memcpy(big[off..][0..seg_bytes.len], seg_bytes);
        }
        if (off < big.len) @memset(big[off..], 0xab);

        var name_buf2: [32]u8 = undefined;
        const big_name = try std.fmt.bufPrint(&name_buf2, "write_only_{d}.dat", .{target_size});

        const t_big = Io.Clock.awake.now(io);
        try dir.writeFile(io, .{ .sub_path = big_name, .data = big });
        const big_ns = elapsedNs(io, t_big);
        const big_mb = @as(f64, @floatFromInt(big.len)) / 1_048_576.0;
        const big_mb_per_s = big_mb / (@as(f64, @floatFromInt(big_ns)) / 1e9);
        std.debug.print(
            "  disk write (isolated, {d:>3}MB)                {d:.2}MB  {d:.2}ms  {d:.1} MB/s\n",
            .{ target_size / (1024 * 1024), big_mb, @as(f64, @floatFromInt(big_ns)) / 1e6, big_mb_per_s },
        );
    }
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
// Compaction scenarios
// ----------------------------------------------------------------------------

/// 200 small flushes, no compaction. Establishes how a many-segments table
/// degrades scans and ingest steady-state.
fn benchManySegmentsNoCompact(allocator: Allocator, io: Io) !void {
    const batches: usize = 200;
    const batch_size: usize = 1_000;
    const total_rows: usize = batches * batch_size;

    var dir = try freshDir(io, ".bench-data/many_segments_no_compact");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();
    const t = try db.table("t", schema, options);

    const batch = try buildRows(allocator, batch_size);
    defer allocator.free(batch);

    const t_ingest = Io.Clock.awake.now(io);
    var done: usize = 0;
    while (done < total_rows) : (done += batch_size) {
        for (batch, 0..) |*r, i| r.id = @intCast(done + i);
        try t.insert(batch);
        try t.flush();
    }
    const ingest_ns = elapsedNs(io, t_ingest);

    // Full scan over all segments (no compaction → 200 segments).
    var checksum: i64 = 0;
    var scanned: usize = 0;
    const t_scan = Io.Clock.awake.now(io);
    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    while (try q.next()) |b| {
        scanned += b.row_count;
        for (b.values[0].data.bigint) |v| checksum +%= v;
    }
    const scan_ns = elapsedNs(io, t_scan);

    std.mem.doNotOptimizeAway(&checksum);
    std.debug.print(
        "  many segs, NO compact ({d} segs)        ingest={d:.1}ms  scan={d:.1}ms  {d:.2} M rows/s scan  segs={d}\n",
        .{
            t.segmentCount(),
            @as(f64, @floatFromInt(ingest_ns)) / 1e6,
            @as(f64, @floatFromInt(scan_ns)) / 1e6,
            (@as(f64, @floatFromInt(scanned)) / (@as(f64, @floatFromInt(scan_ns)) / 1e9)) / 1e6,
            t.segmentCount(),
        },
    );
}

/// Same ingest pattern, but `backgroundCompactSweep` runs after every flush.
/// Should show: lower steady-state segment count, faster scans, slightly
/// higher ingest cost from the merge work.
fn benchManySegmentsWithCompact(allocator: Allocator, io: Io) !void {
    const batches: usize = 200;
    const batch_size: usize = 1_000;
    const total_rows: usize = batches * batch_size;

    var dir = try freshDir(io, ".bench-data/many_segments_with_compact");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
        // Aggressive tier threshold so compactions fire often.
        .compact_min_segments = 1,
        .compact_tombstone_threshold = 2.0, // disable tomb trigger
    });
    defer db.close();
    const t = try db.table("t", schema, options);

    const batch = try buildRows(allocator, batch_size);
    defer allocator.free(batch);

    const t_ingest = Io.Clock.awake.now(io);
    var done: usize = 0;
    while (done < total_rows) : (done += batch_size) {
        for (batch, 0..) |*r, i| r.id = @intCast(done + i);
        try t.insert(batch);
        try t.flush();
        try db.backgroundCompactSweep();
    }
    const ingest_ns = elapsedNs(io, t_ingest);

    // Full scan against the post-compaction segment layout.
    var checksum: i64 = 0;
    var scanned: usize = 0;
    const t_scan = Io.Clock.awake.now(io);
    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    while (try q.next()) |b| {
        scanned += b.row_count;
        for (b.values[0].data.bigint) |v| checksum +%= v;
    }
    const scan_ns = elapsedNs(io, t_scan);

    std.mem.doNotOptimizeAway(&checksum);
    std.debug.print(
        "  many segs, WITH compact ({d} segs)        ingest={d:.1}ms  scan={d:.1}ms  {d:.2} M rows/s scan  segs={d}\n",
        .{
            t.segmentCount(),
            @as(f64, @floatFromInt(ingest_ns)) / 1e6,
            @as(f64, @floatFromInt(scan_ns)) / 1e6,
            (@as(f64, @floatFromInt(scanned)) / (@as(f64, @floatFromInt(scan_ns)) / 1e9)) / 1e6,
            t.segmentCount(),
        },
    );
}

/// One large segment with ~50% of rows deleted. The tombstone-pressure
/// trigger should rewrite it. Measures the cost of that rewrite vs. the
/// equivalent full-segment recompress.
fn benchTombstonePressureCompact(allocator: Allocator, io: Io) !void {
    const n_rows: usize = 100_000;

    var dir = try freshDir(io, ".bench-data/tomb_pressure");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{
        .compact_min_segments = 1_000_000, // disable count-based trigger
        .compact_tombstone_threshold = 0.30,
        .auto_flush_secs = 0,
    });
    defer db.close();
    const t = try db.table("t", schema, options);

    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);
    try t.flush();

    // Delete ~half the rows by qty bound. qty = i % 100, so qty < 50 → 50%.
    const t_del = Io.Clock.awake.now(io);
    const deleted = try t.delete(.{ .col = "qty", .op = .lt, .val = .{ .int = 50 } });
    const del_ns = elapsedNs(io, t_del);

    // Now run the tombstone-triggered compaction.
    const t_compact = Io.Clock.awake.now(io);
    try db.backgroundCompactSweep();
    const compact_ns = elapsedNs(io, t_compact);

    std.debug.print(
        "  tombstone-pressure compact           delete={d} rows in {d:.1}ms  compact={d:.1}ms  segs_after={d}\n",
        .{
            deleted,
            @as(f64, @floatFromInt(del_ns)) / 1e6,
            @as(f64, @floatFromInt(compact_ns)) / 1e6,
            t.segmentCount(),
        },
    );
}

/// Watch the segment count + per-tier distribution evolve over many
/// flushes with periodic tiered compaction. Reports a snapshot every
/// 50 batches so you can see steady-state behavior.
fn benchTierFillUp(allocator: Allocator, io: Io) !void {
    const batches: usize = 400;
    const batch_size: usize = 1_000;

    var dir = try freshDir(io, ".bench-data/tier_fillup");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
        .compact_min_segments = 1,
        .compact_tombstone_threshold = 2.0,
    });
    defer db.close();
    const t = try db.table("t", schema, options);

    const batch = try buildRows(allocator, batch_size);
    defer allocator.free(batch);

    std.debug.print(
        "  tier fill-up: 400 batches x 1k rows, snapshot every 100 batches\n",
        .{},
    );
    std.debug.print("    {s:>8}  {s:>10}  {s:>10}  {s:>6}\n", .{ "batch", "ingest ms", "scan ms", "segs" });

    var done: usize = 0;
    var batch_idx: usize = 0;
    while (batch_idx < batches) : (batch_idx += 1) {
        for (batch, 0..) |*r, i| r.id = @intCast(done + i);
        done += batch_size;

        const t_ingest = Io.Clock.awake.now(io);
        try t.insert(batch);
        try t.flush();
        try db.backgroundCompactSweep();
        const ingest_ns = elapsedNs(io, t_ingest);

        if ((batch_idx + 1) % 100 == 0) {
            // Quick scan to measure read latency at this segment count.
            var checksum: i64 = 0;
            const t_scan = Io.Clock.awake.now(io);
            var q = try thindb.scan(allocator, t);
            defer q.deinit();
            while (try q.next()) |b| {
                for (b.values[0].data.bigint) |v| checksum +%= v;
            }
            const scan_ns = elapsedNs(io, t_scan);
            std.mem.doNotOptimizeAway(&checksum);

            std.debug.print(
                "    {d:>8}  {d:>10.2}  {d:>10.2}  {d:>6}\n",
                .{
                    batch_idx + 1,
                    @as(f64, @floatFromInt(ingest_ns)) / 1e6,
                    @as(f64, @floatFromInt(scan_ns)) / 1e6,
                    t.segmentCount(),
                },
            );
        }
    }
}

/// Compare ingest throughput under sync_mode .none vs .per_flush.
/// Same workload run twice — fsync overhead per flush is what we're isolating.
fn benchDurabilityCost(allocator: Allocator, io: Io) !void {
    const total_rows: usize = 1_000_000;

    // -------- .none: no fsync, fast path --------
    {
        var dir = try freshDir(io, ".bench-data/dur_none");
        defer dir.close(io);
        var db = try thindb.Database.open(allocator, io, dir, .{
            .sync_mode = .none,
            .auto_flush_secs = 0,
        });
        defer db.close();
        const t = try db.table("t", schema, options);
        const rows = try buildRows(allocator, total_rows);
        defer allocator.free(rows);

        const t0 = Io.Clock.awake.now(io);
        try t.insert(rows);
        try t.flush();
        const elapsed = elapsedNs(io, t0);
        try report("insert + flush  (sync=.none)    ", total_rows, elapsed, null);
    }

    // -------- .per_flush: one fsync on segment, one on manifest --------
    {
        var dir = try freshDir(io, ".bench-data/dur_per_flush");
        defer dir.close(io);
        var db = try thindb.Database.open(allocator, io, dir, .{
            .sync_mode = .per_flush,
            .auto_flush_secs = 0,
        });
        defer db.close();
        const t = try db.table("t", schema, options);
        const rows = try buildRows(allocator, total_rows);
        defer allocator.free(rows);

        const t0 = Io.Clock.awake.now(io);
        try t.insert(rows);
        try t.flush();
        const elapsed = elapsedNs(io, t0);
        try report("insert + flush  (sync=.per_flush)", total_rows, elapsed, null);
    }

    // -------- sustained ingest, .none --------
    {
        const batches: usize = 100;
        const batch_size: usize = 1_000;
        var dir = try freshDir(io, ".bench-data/dur_sus_none");
        defer dir.close(io);
        var db = try thindb.Database.open(allocator, io, dir, .{
            .sync_mode = .none,
            .auto_flush_secs = 0,
            .auto_flush_rows = std.math.maxInt(u64),
            .auto_flush_bytes = std.math.maxInt(usize),
        });
        defer db.close();
        const t = try db.table("t", schema, options);
        const batch = try buildRows(allocator, batch_size);
        defer allocator.free(batch);

        const t0 = Io.Clock.awake.now(io);
        var done: usize = 0;
        while (done < batches * batch_size) : (done += batch_size) {
            for (batch, 0..) |*r, i| r.id = @intCast(done + i);
            try t.insert(batch);
            try t.flush();
        }
        const elapsed = elapsedNs(io, t0);
        try report("sustained 100 flushes (sync=.none)    ", batches * batch_size, elapsed, null);
    }

    // -------- sustained ingest, .per_flush --------
    {
        const batches: usize = 100;
        const batch_size: usize = 1_000;
        var dir = try freshDir(io, ".bench-data/dur_sus_per_flush");
        defer dir.close(io);
        var db = try thindb.Database.open(allocator, io, dir, .{
            .sync_mode = .per_flush,
            .auto_flush_secs = 0,
            .auto_flush_rows = std.math.maxInt(u64),
            .auto_flush_bytes = std.math.maxInt(usize),
        });
        defer db.close();
        const t = try db.table("t", schema, options);
        const batch = try buildRows(allocator, batch_size);
        defer allocator.free(batch);

        const t0 = Io.Clock.awake.now(io);
        var done: usize = 0;
        while (done < batches * batch_size) : (done += batch_size) {
            for (batch, 0..) |*r, i| r.id = @intCast(done + i);
            try t.insert(batch);
            try t.flush();
        }
        const elapsed = elapsedNs(io, t0);
        try report("sustained 100 flushes (sync=.per_flush)", batches * batch_size, elapsed, null);
    }
}

/// Compare ingest under `wal_enabled = false` vs `true`. Each insert() call
/// fsyncs the WAL once. A single 1M-row insert is one fsync. 1000 batches
/// of 1k rows is 1000 fsyncs.
fn benchWalCost(allocator: Allocator, io: Io) !void {
    // ---- one big insert (1M rows) ----
    inline for ([_]bool{ false, true }) |wal| {
        var dir = try freshDir(io, if (wal) ".bench-data/wal_big_on" else ".bench-data/wal_big_off");
        defer dir.close(io);
        var db = try thindb.Database.open(allocator, io, dir, .{
            .wal_enabled = wal,
            .auto_flush_secs = 0,
        });
        defer db.close();
        const t = try db.table("t", schema, options);
        const rows = try buildRows(allocator, 1_000_000);
        defer allocator.free(rows);

        const t0 = Io.Clock.awake.now(io);
        try t.insert(rows);
        const elapsed = elapsedNs(io, t0);
        const label = if (wal) "insert 1M rows  (wal=true) " else "insert 1M rows  (wal=false)";
        try report(label, 1_000_000, elapsed, null);
    }

    // ---- 1000 batches of 1k rows ----
    inline for ([_]bool{ false, true }) |wal| {
        const batches: usize = 1_000;
        const batch_size: usize = 1_000;

        var dir = try freshDir(io, if (wal) ".bench-data/wal_sus_on" else ".bench-data/wal_sus_off");
        defer dir.close(io);
        var db = try thindb.Database.open(allocator, io, dir, .{
            .wal_enabled = wal,
            .auto_flush_secs = 0,
            .auto_flush_rows = std.math.maxInt(u64),
            .auto_flush_bytes = std.math.maxInt(usize),
        });
        defer db.close();
        const t = try db.table("t", schema, options);
        const batch = try buildRows(allocator, batch_size);
        defer allocator.free(batch);

        const t0 = Io.Clock.awake.now(io);
        var done: usize = 0;
        while (done < batches * batch_size) : (done += batch_size) {
            for (batch, 0..) |*r, i| r.id = @intCast(done + i);
            try t.insert(batch);
        }
        const elapsed = elapsedNs(io, t0);
        const label = if (wal) "1000 inserts x 1k rows (wal=true) " else "1000 inserts x 1k rows (wal=false)";
        try report(label, batches * batch_size, elapsed, null);
    }
}

/// Spawn N OS threads, each doing M small inserts under sync_mode=.per_flush
/// + wal_enabled=true. Reports total time, throughput, and the leader fsync
/// count vs. total inserts (the group-commit amortization ratio).
fn benchGroupCommit(allocator: Allocator, io: Io) !void {
    const inserts_per_thread: usize = 250;
    inline for ([_]usize{ 1, 2, 4, 8 }) |n_threads| {
        var dir_name_buf: [64]u8 = undefined;
        const dir_name = try std.fmt.bufPrint(&dir_name_buf, ".bench-data/gc_{d}", .{n_threads});
        var dir = try freshDir(io, dir_name);
        defer dir.close(io);

        var db = try thindb.Database.open(allocator, io, dir, .{
            .wal_enabled = true,
            .sync_mode = .per_flush,
            .auto_flush_secs = 0,
            .auto_flush_rows = std.math.maxInt(u64),
            .auto_flush_bytes = std.math.maxInt(usize),
        });
        defer db.close();
        const t = try db.table("t", schema, options);

        const Ctx = struct {
            t: *thindb.Table,
            base: i64,
            n: usize,
            errs: *std.atomic.Value(usize),

            fn run(self: @This()) void {
                var row: Row = .{ .id = 0, .qty = 1, .active = true, .tag = "x" };
                var i: usize = 0;
                while (i < self.n) : (i += 1) {
                    row.id = self.base + @as(i64, @intCast(i));
                    self.t.insert(&.{row}) catch {
                        _ = self.errs.fetchAdd(1, .release);
                        return;
                    };
                }
            }
        };

        var errs: std.atomic.Value(usize) = .init(0);
        var threads: [n_threads]std.Thread = undefined;

        const t0 = Io.Clock.awake.now(io);
        for (&threads, 0..) |*thr, ti| {
            const ctx = Ctx{
                .t = t,
                .base = @as(i64, @intCast(ti)) * 1_000_000,
                .n = inserts_per_thread,
                .errs = &errs,
            };
            thr.* = try std.Thread.spawn(.{}, Ctx.run, .{ctx});
        }
        for (&threads) |*thr| thr.join();
        const elapsed = elapsedNs(io, t0);

        if (errs.load(.acquire) != 0) {
            std.debug.print("  ERROR: {d} insert failures across threads\n", .{errs.load(.acquire)});
        } else {
            const total_inserts = n_threads * inserts_per_thread;
            const fsyncs = if (t.wal) |*w| w.fsync_count else 0;
            const coalesces = if (t.wal) |*w| w.coalesce_count else 0;
            const amort = if (fsyncs == 0) 0.0 else @as(f64, @floatFromInt(total_inserts)) / @as(f64, @floatFromInt(fsyncs));

            var label_buf: [64]u8 = undefined;
            const label = try std.fmt.bufPrint(&label_buf, "{d} thread(s) x 250 inserts", .{n_threads});
            try report(label, total_inserts, elapsed, null);
            std.debug.print("    fsyncs={d}  coalesce_pauses={d}  inserts/fsync={d:.2}\n", .{ fsyncs, coalesces, amort });
        }
    }
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
