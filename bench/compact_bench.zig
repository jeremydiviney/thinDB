//! Compaction scenarios: many-segs without/with compact, tombstone-pressure
//! trigger, tier fill-up snapshot. Each function is `pub` and called from
//! `main.zig`.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");

const Allocator = common.Allocator;
const Io = common.Io;
const schema = common.schema;
const options = common.options;
const buildRows = common.buildRows;
const elapsedNs = common.elapsedNs;
const freshDir = common.freshDir;

pub fn benchManySegmentsNoCompact(allocator: Allocator, io: Io) !void {
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
pub fn benchManySegmentsWithCompact(allocator: Allocator, io: Io) !void {
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
pub fn benchTombstonePressureCompact(allocator: Allocator, io: Io) !void {
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
pub fn benchTierFillUp(allocator: Allocator, io: Io) !void {
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
