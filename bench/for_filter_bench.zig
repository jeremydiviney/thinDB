//! Phase 2B proof bench: FOR-encoded predicate evaluation vs a raw/native
//! equivalent, at a scale where the NATIVE column exceeds a deliberately-small
//! decompressed-block cache while the narrow FOR column still fits.
//!
//! Construction (one table, two columns, identical per-row selectivity):
//!   - `vfor`  BIGINT in [0, SPAN]              → FOR width 2 (2 bytes/row)
//!   - `vraw`  BIGINT = vfor * SCALE (monotone) → un-narrowable, stays raw (8 B)
//! Because `vraw` is strictly monotone in `vfor`, `vraw > T*SCALE` selects
//! exactly the rows `vfor > T`, so both filters touch the same survivor set —
//! the ONLY difference is the in-cache column width (2 B vs 8 B). With the
//! cache pinned small, the 8-byte raw column thrashes (re-decompress per pass)
//! while the 2-byte FOR column stays resident; repeated selective scans expose
//! the cache-bandwidth win.
//!
//! Run (the whole bench suite builds ReleaseFast): `zig build bench`. This is
//! the first bench printed. To run it alone, copy this file's `run` body into a
//! throwaway `pub fn main` and `zig run` it against the ReleaseFast thindb
//! module — or just read the FOR-vs-raw line at the top of `zig build bench`.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");

const Allocator = common.Allocator;
const Io = common.Io;
const elapsedNs = common.elapsedNs;
const freshDir = common.freshDir;

const ROWS: usize = 4_000_000; // 8-byte vraw ≈ 32 MB raw payload, > the 16 MB cache below
const SPAN: i64 = 60_000; // → u16 FOR deltas (2-byte codes)
const SCALE: i64 = 137_438_953; // spreads vfor across the i64 range → vraw stays raw
const CACHE_BYTES: usize = 16 * 1024 * 1024; // small: native column won't fit, FOR will
const PASSES: usize = 8;

const Row = struct { id: i64, vfor: i64, vraw: i64 };

const schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "vfor", .type = .bigint },
        .{ .name = "vraw", .type = .bigint },
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

fn vforOf(i: usize) i64 {
    return @mod(@as(i64, @intCast(i)) * 2_654_435_761, SPAN + 1);
}

/// Run `vfor`/`vraw` filters `PASSES` times each, alternating so neither warms
/// the other's blocks, and report the elapsed ms + survivor count for each.
pub fn run(allocator: Allocator, io: Io) !void {
    std.debug.print("\nPhase 2B: FOR-encoded vs raw predicate eval (cache pinned {d} MB)\n", .{CACHE_BYTES / (1024 * 1024)});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});

    var dir = try freshDir(io, ".bench-data/for_filter");
    defer dir.close(io);

    var db = try thindb.Database.open(allocator, io, dir, .{ .cache_size_bytes = CACHE_BYTES });
    defer db.close();
    const t = try db.table("t", schema, options);

    const rows = try allocator.alloc(Row, ROWS);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| {
        const v = vforOf(i);
        r.* = .{ .id = @intCast(i), .vfor = v, .vraw = v * SCALE };
    }
    try t.insert(rows);
    try t.flush();

    // Threshold selecting ~10% of rows (vfor > 0.9 * SPAN); the raw threshold
    // is the same logical cutoff scaled, so both filters share survivor sets.
    const t_for: i64 = @divTrunc(SPAN * 9, 10);
    const t_raw: i64 = t_for * SCALE;

    var ns_for: u64 = 0;
    var ns_raw: u64 = 0;
    var matched_for: usize = 0;
    var matched_raw: usize = 0;

    var pass: usize = 0;
    while (pass < PASSES) : (pass += 1) {
        matched_for = try timedFilter(allocator, t, "vfor", t_for, io, &ns_for);
        matched_raw = try timedFilter(allocator, t, "vraw", t_raw, io, &ns_raw);
    }

    const ms_for = @as(f64, @floatFromInt(ns_for)) / 1e6;
    const ms_raw = @as(f64, @floatFromInt(ns_raw)) / 1e6;
    std.debug.print(
        "  FOR  vfor>{d:<12} ({d} matched)  {d:>8.2} ms total over {d} passes  ({d:.2} ms/pass)\n",
        .{ t_for, matched_for, ms_for, PASSES, ms_for / @as(f64, @floatFromInt(PASSES)) },
    );
    std.debug.print(
        "  RAW  vraw>{d:<12} ({d} matched)  {d:>8.2} ms total over {d} passes  ({d:.2} ms/pass)\n",
        .{ t_raw, matched_raw, ms_raw, PASSES, ms_raw / @as(f64, @floatFromInt(PASSES)) },
    );
    if (ns_for > 0) {
        std.debug.print("  speedup (raw/for): {d:.2}x\n", .{ms_raw / ms_for});
    }
    if (matched_for != matched_raw) {
        std.debug.print("  WARNING: survivor counts differ ({d} vs {d}) — selectivity not matched\n", .{ matched_for, matched_raw });
    }
}

fn timedFilter(allocator: Allocator, t: *thindb.Table, col: []const u8, threshold: i64, io: Io, acc_ns: *u64) !usize {
    var checksum: i64 = 0;
    var matched: usize = 0;
    const t0 = Io.Clock.awake.now(io);
    var base = try thindb.scan(allocator, t);
    var q = try base.filter(thindb.leafExpr(col, .gt, .{ .bigint = threshold }));
    defer q.deinit();
    while (try q.next()) |batch| {
        matched += batch.row_count;
        for (batch.values[0].data.bigint) |v| checksum +%= v;
    }
    acc_ns.* += elapsedNs(io, t0);
    std.mem.doNotOptimizeAway(&checksum);
    return matched;
}
