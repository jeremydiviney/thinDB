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

    // Defaults. CLI: first positional = TSV path, second = max rows.
    // Flags: --data-dir PATH, --database NAME, --wipe / --no-wipe.
    var tsv_path: []const u8 = "bench/clickbench/data/hits.tsv";
    var max_rows: usize = 0;
    var data_dir_path: []const u8 = ".clickbench-db";
    var database_name: []const u8 = "clickbench";
    var wipe: bool = true;

    var args_iter = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args_iter.deinit();
    _ = args_iter.skip(); // program name
    var positional: usize = 0;
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--data-dir")) {
            data_dir_path = args_iter.next() orelse return error.MissingFlagValue;
        } else if (std.mem.eql(u8, arg, "--database")) {
            database_name = args_iter.next() orelse return error.MissingFlagValue;
        } else if (std.mem.eql(u8, arg, "--no-wipe")) {
            wipe = false;
        } else if (std.mem.eql(u8, arg, "--wipe")) {
            wipe = true;
        } else if (std.mem.startsWith(u8, arg, "--")) {
            std.debug.print("clickbench: unknown flag '{s}'\n", .{arg});
            return 1;
        } else switch (positional) {
            0 => {
                tsv_path = arg;
                positional += 1;
            },
            1 => {
                max_rows = std.fmt.parseInt(usize, arg, 10) catch 0;
                positional += 1;
            },
            else => {
                std.debug.print("clickbench: unexpected positional '{s}'\n", .{arg});
                return 1;
            },
        }
    }

    std.debug.print("\nthinDB ClickBench loader v{s}\n", .{thindb.version});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    std.debug.print("  TSV path     : {s}\n", .{tsv_path});
    std.debug.print("  Data dir     : {s}\n", .{data_dir_path});
    std.debug.print("  Target       : {s}.public.hits\n", .{database_name});
    std.debug.print("  Wipe data dir: {}\n", .{wipe});
    if (max_rows > 0) std.debug.print("  Max rows     : {d}\n", .{max_rows});

    const cwd = std.Io.Dir.cwd();
    if (wipe) try cwd.deleteTree(io, data_dir_path);
    var data_root = try cwd.createDirPathOpen(io, data_dir_path, .{});
    defer data_root.close(io);

    const catalog = try thindb.Catalog.open(allocator, io, data_root, .{});
    defer catalog.close();
    const db = try catalog.createOrOpenDatabase(database_name);
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
