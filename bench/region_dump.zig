//! Region recognizer IR-dump tool: compile a SQL file against the
//! wayroll bench catalog so compileStaged's THINDB_REGION_DUMP hook prints
//! the post-pass IR the region recognizer will receive. Never executes the
//! query. Server must be STOPPED (opens .wayroll-bench-db embedded).
//!
//!   THINDB_REGION_DUMP=1 ./zig-out/bin/region_dump.exe <sql-file>

const std = @import("std");
const thindb = @import("thindb");

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

pub fn main(init: std.process.Init) !u8 {
    // std.process.Init io carries the process environ, so the catalog's
    // zig-CLI subprocesses (LANGUAGE zig function compiles) inherit
    // APPDATA — a bare Io.Threaded spawns children with an empty env.
    const allocator = init.gpa;
    const io = init.io;

    std.debug.print("env check: APPDATA={?s} LOCALAPPDATA={?s} ZIG_GLOBAL_CACHE_DIR={?s}\n", .{
        if (getenv("APPDATA")) |v| std.mem.span(v) else null,
        if (getenv("LOCALAPPDATA")) |v| std.mem.span(v) else null,
        if (getenv("ZIG_GLOBAL_CACHE_DIR")) |v| std.mem.span(v) else null,
    });
    const sql_path: []const u8 = if (getenv("RF_SQL")) |v| std.mem.span(v) else return error.MissingRfSqlEnv;
    const sql = try std.Io.Dir.cwd().readFileAlloc(io, sql_path, allocator, .limited(16 << 20));
    defer allocator.free(sql);

    const db_path: []const u8 = if (getenv("RF_DB")) |v| std.mem.span(v) else ".wayroll-bench-db";
    var data_root = try std.Io.Dir.cwd().openDir(io, db_path, .{ .iterate = true });
    defer data_root.close(io);
    const catalog = try thindb.Catalog.open(allocator, io, data_root, .{ .max_dop = 12, .data_root_path = db_path });
    defer catalog.close();
    const db = catalog.database("wayroll_prod") orelse return error.DatabaseNotFound;

    const cat = db.catalog orelse db.owned_catalog.?;
    const passes: usize = if (getenv("RF_PASSES")) |v| std.fmt.parseInt(usize, std.mem.span(v), 10) catch 1 else 1;
    const prof = thindb.exec.prof;
    for (0..passes) |pass| {
        var pass_arena = std.heap.ArenaAllocator.init(allocator);
        defer pass_arena.deinit();
        const tp = prof.nowTicks();
        const proot = try thindb.sql.parseWithContext(pass_arena.allocator(), sql, .neutral, &cat.udfs, .{
            .registry = &cat.sql_fns,
            .db = "wayroll_prod",
            .views = &cat.views,
        });
        const t0 = prof.nowTicks();
        std.debug.print("pass {d}: parse={d:.1}ms\n", .{ pass, prof.ticksToMs(t0 - tp) });
        var cq = try thindb.net.compileWithSession(allocator, db, .{ .current_db = "wayroll_prod" }, proot);
        defer cq.deinit();
        defer thindb.net.CompiledQuery.freeSessionVars(allocator, cq.sessionValue().vars);
        const t1 = prof.nowTicks();
        if (getenv("RF_COMPILE_ONLY") != null) {
            std.debug.print("pass {d}: compile={d:.1}ms (compile only)\n", .{ pass, prof.ticksToMs(t1 - t0) });
            continue;
        }
        var rows: usize = 0;
        while (try cq.next()) |batch| {
            for (0..batch.row_count) |r| {
                rows += 1;
                for (batch.schema, batch.values) |col, v| {
                    std.debug.print("{s}=", .{col.name});
                    if (!v.isValid(r)) {
                        std.debug.print("NULL\n", .{});
                        continue;
                    }
                    switch (v.data) {
                        .tinyint => |s| std.debug.print("{d}\n", .{s[r]}),
                        .smallint => |s| std.debug.print("{d}\n", .{s[r]}),
                        .int => |s| std.debug.print("{d}\n", .{s[r]}),
                        .bigint => |s| std.debug.print("{d}\n", .{s[r]}),
                        .date => |s| std.debug.print("{d}\n", .{s[r]}),
                        .datetime => |s| std.debug.print("{d}\n", .{s[r]}),
                        .float => |s| std.debug.print("{d}\n", .{s[r]}),
                        .double => |s| std.debug.print("{d:.9}\n", .{s[r]}),
                        .varchar, .string, .char, .json => |s| std.debug.print("{s}\n", .{s.rowBytes(r)}),
                        .largeint => |s| std.debug.print("{d}\n", .{s[r]}),
                        .decimal64 => |s| std.debug.print("dec64:{d}\n", .{s[r]}),
                        .decimal128 => |s| std.debug.print("dec128:{d}\n", .{s[r]}),
                        else => std.debug.print("?\n", .{}),
                    }
                }
            }
        }
        const t2 = prof.nowTicks();
        std.debug.print("pass {d}: rows={d} compile={d:.1}ms exec={d:.1}ms\n", .{
            pass, rows, prof.ticksToMs(t1 - t0), prof.ticksToMs(t2 - t1),
        });
    }
    return 0;
}
