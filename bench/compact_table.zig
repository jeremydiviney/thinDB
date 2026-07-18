//! Force compaction sweeps on one database dir until quiescent — offline
//! layout experiment tool (#165). Opens the database with aggressive
//! compaction thresholds and drives `backgroundCompactSweep` to a fixed
//! point, printing per-sweep progress.
//! Usage: compact-table -- <data_dir> <database_name> [table_name]

const std = @import("std");
const thindb = @import("thindb");

pub fn main(init: std.process.Init) !u8 {
    const allocator = init.gpa;
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var args_iter = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args_iter.deinit();
    _ = args_iter.skip();
    const data_path = args_iter.next() orelse {
        std.debug.print("usage: compact-table <data_dir> <database_name> [table_name]\n", .{});
        return 1;
    };
    const db_name = args_iter.next() orelse {
        std.debug.print("usage: compact-table <data_dir> <database_name> [table_name]\n", .{});
        return 1;
    };
    const table_name = args_iter.next();

    const cwd = std.Io.Dir.cwd();
    var data_dir = try cwd.openDir(io, data_path, .{});
    defer data_dir.close(io);

    // Open through the catalog so a server-layout data dir (databases as
    // subdirs) resolves the right database, not the back-compat "main".
    var root_db = try thindb.Database.open(allocator, io, data_dir, .{
        .compact_min_segments = 2,
        .compact_tombstone_threshold = 0.001,
    });
    defer root_db.close();
    const db = try root_db.owned_catalog.?.createOrOpenDatabase(db_name);

    if (table_name) |name| {
        // Named table: full merge-all compaction to a single segment (the
        // order-aligned region fast path wants one global sorted run).
        const t = try db.openTable(name, .{});
        std.debug.print("{s}: {d} segments before\n", .{ name, t.manifest.segments.items.len });
        try t.compact();
        std.debug.print("{s}: {d} segments after\n", .{ name, t.manifest.segments.items.len });
    } else {
        var sweeps: usize = 0;
        while (try db.backgroundCompactSweep()) {
            sweeps += 1;
            std.debug.print("sweep {d} merged something\n", .{sweeps});
            if (sweeps > 200) break; // runaway guard
        }
        std.debug.print("quiescent after {d} sweeps\n", .{sweeps});
    }
    return 0;
}
