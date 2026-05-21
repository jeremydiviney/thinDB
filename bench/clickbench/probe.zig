//! Diagnostic: open .clickbench-db with Catalog.open and dump what
//! gets adopted. Used to debug discovery; not wired into build.zig
//! by default.

const std = @import("std");
const thindb = @import("thindb");

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
                std.debug.print("          {d} columns:\n", .{t.schema.columns.len});
                for (t.schema.columns, 0..) |c, i| {
                    std.debug.print("            [{d}] {s}\n", .{ i, c.name });
                }
            }
        }
    }
    return 0;
}
