//! ClickBench loader entry point.
//!
//! Stage 1 (this commit): just verify the load path. Open a fresh
//! DB at `.clickbench-db/`, create the `hits` table, stream rows
//! from a TSV file, report load throughput + sanity counts.
//!
//! Query benchmarking lands in a follow-up.

const std = @import("std");
const thindb = @import("thindb");

const schema_mod = @import("schema.zig");
const loader = @import("loader.zig");

pub fn main(init: std.process.Init) !u8 {
    const allocator = init.gpa;

    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Default args: data/hits.tsv, no row cap, fresh DB at .clickbench-db/.
    var tsv_path: []const u8 = "bench/clickbench/data/hits.tsv";
    var max_rows: usize = 0;
    const db_dir_path: []const u8 = ".clickbench-db";

    // Trivial CLI parsing — first positional is TSV path, second is
    // max rows. Both optional.
    var args_iter = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args_iter.deinit();
    _ = args_iter.skip(); // program name
    if (args_iter.next()) |p| tsv_path = p;
    if (args_iter.next()) |s| {
        max_rows = std.fmt.parseInt(usize, s, 10) catch 0;
    }

    std.debug.print("\nthinDB ClickBench loader v{s}\n", .{thindb.version});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    std.debug.print("  TSV path     : {s}\n", .{tsv_path});
    std.debug.print("  DB path      : {s}\n", .{db_dir_path});
    if (max_rows > 0) std.debug.print("  Max rows     : {d}\n", .{max_rows});

    // Wipe the DB dir for a clean run. deleteTree is a no-op if the
    // path doesn't exist already.
    const cwd = std.Io.Dir.cwd();
    try cwd.deleteTree(io, db_dir_path);
    var db_dir = try cwd.createDirPathOpen(io, db_dir_path, .{});
    defer db_dir.close(io);

    var db = try thindb.Database.open(allocator, io, db_dir, .{});
    defer db.close();
    const t = try db.table("hits", schema_mod.table_schema, schema_mod.table_options);

    std.debug.print("\nSchema    : {d} columns, order key on ({s}, {s}, {s}, {s}, {s})\n", .{
        schema_mod.columns.len,
        schema_mod.order_key[0],
        schema_mod.order_key[1],
        schema_mod.order_key[2],
        schema_mod.order_key[3],
        schema_mod.order_key[4],
    });
    std.debug.print("Loading…  (progress prints every 500K rows)\n\n", .{});

    const stats = loader.loadTsv(allocator, io, t, tsv_path, .{
        .max_rows = max_rows,
        .batch_rows = 65_536,
        .progress_every = 500_000,
    }) catch |err| {
        std.debug.print("load error: {t}\n", .{err});
        return err;
    };

    // Final flush to disk so the data survives this process exit.
    try t.flush();

    const seconds = @as(f64, @floatFromInt(stats.elapsed_ns)) / 1e9;
    const rows_per_sec = @as(f64, @floatFromInt(stats.rows_loaded)) / seconds;
    const mb_per_sec = (@as(f64, @floatFromInt(stats.bytes_read)) / seconds) / (1024.0 * 1024.0);

    std.debug.print("\n--------------------------------------------------------------------------------\n", .{});
    std.debug.print("Loaded     : {d} rows  ({d} rejected)\n", .{ stats.rows_loaded, stats.rejected_rows });
    std.debug.print("Bytes read : {d:.2} MB\n", .{@as(f64, @floatFromInt(stats.bytes_read)) / (1024.0 * 1024.0)});
    std.debug.print("Wall time  : {d:.2} s\n", .{seconds});
    std.debug.print("Throughput : {d:.0} rows/s,  {d:.1} MB/s\n", .{ rows_per_sec, mb_per_sec });
    std.debug.print("Segments   : {d}\n", .{t.segmentCount()});
    std.debug.print("--------------------------------------------------------------------------------\n\n", .{});
    return 0;
}
