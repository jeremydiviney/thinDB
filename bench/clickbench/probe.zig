//! Diagnostic: open .clickbench-db with Catalog.open and dump what
//! gets adopted. Used to debug discovery; not wired into build.zig
//! by default.

const std = @import("std");
const thindb = @import("thindb");

fn tierOf(rows: u64) u8 {
    if (rows == 0) return 0;
    var t: u8 = 0;
    var cap: u64 = 65536;
    while (rows >= cap and t < 31) : (t += 1) cap *|= 4;
    return t;
}

pub fn main(init: std.process.Init) !u8 {
    const allocator = init.gpa;
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const cwd = std.Io.Dir.cwd();
    var data_root = try cwd.createDirPathOpen(io, ".clickbench-db", .{});
    defer data_root.close(io);

    const catalog = try thindb.Catalog.open(allocator, io, data_root, .{});
    defer catalog.close();

    const db_names = try catalog.listDatabases(allocator);
    defer {
        for (db_names) |n| allocator.free(n);
        allocator.free(db_names);
    }
    std.debug.print("Databases ({d}):\n", .{db_names.len});
    for (db_names) |dn| {
        std.debug.print("  - {s}\n", .{dn});
        const db = catalog.database(dn).?;
        const sc_names = try db.listSchemas(allocator);
        defer {
            for (sc_names) |n| allocator.free(n);
            allocator.free(sc_names);
        }
        for (sc_names) |sn| {
            std.debug.print("      schema: {s}\n", .{sn});
            const sc = db.schema(sn).?;
            const tbl_names = try sc.listTables(allocator);
            defer {
                for (tbl_names) |n| allocator.free(n);
                allocator.free(tbl_names);
            }
            for (tbl_names) |tn| {
                std.debug.print("        table: {s}\n", .{tn});
                const t = sc.openTable(tn, .{}) catch |err| {
                    std.debug.print("          (openTable failed: {t})\n", .{err});
                    continue;
                };
                const segs = t.manifest.segments.items;
                if (segs.len > 0) {
                    var minr: u64 = std.math.maxInt(u64);
                    var maxr: u64 = 0;
                    var total: u64 = 0;
                    var tiers = [_]u32{0} ** 10;
                    for (segs) |s| {
                        if (s.row_count < minr) minr = s.row_count;
                        if (s.row_count > maxr) maxr = s.row_count;
                        total += s.row_count;
                        const ti = tierOf(s.row_count);
                        if (ti < 10) tiers[ti] += 1;
                    }
                    std.debug.print("          {d} cols | SEGS={d} total_rows={d} avg={d} min={d} max={d}\n", .{ t.schema.columns.len, segs.len, total, total / segs.len, minr, maxr });
                    std.debug.print("          current tiers (base 64K): ", .{});
                    for (tiers, 0..) |c, ti| if (c > 0) std.debug.print("t{d}={d} ", .{ ti, c });
                    std.debug.print("\n", .{});
                }
            }
        }
    }
    return 0;
}
