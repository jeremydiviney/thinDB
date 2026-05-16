//! Durability-related benches: sync_mode .none vs .per_flush cost,
//! WAL append cost, group-commit amortization under concurrent writers.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");

const Allocator = common.Allocator;
const Io = common.Io;
const Row = common.Row;
const schema = common.schema;
const options = common.options;
const buildRows = common.buildRows;
const elapsedNs = common.elapsedNs;
const freshDir = common.freshDir;
const report = common.report;

pub fn benchDurabilityCost(allocator: Allocator, io: Io) !void {
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
pub fn benchWalCost(allocator: Allocator, io: Io) !void {
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
pub fn benchGroupCommit(allocator: Allocator, io: Io) !void {
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
