const std = @import("std");
const Io = std.Io;
const api = @import("../api/api.zig");
const exec = @import("../exec/exec.zig");

const DirectorySyncFault = struct {
    threaded: Io.Threaded,
    vtable: Io.VTable = undefined,
    remaining: usize = 0,
    rename_failures: usize = 0,
    rename_calls: usize = 0,
    /// When set, only renames whose source path starts with this prefix are
    /// refused; everything else reaches the real backend.
    rename_fail_prefix: ?[]const u8 = null,

    fn io(self: *@This()) Io {
        const base = self.threaded.io();
        self.vtable = base.vtable.*;
        self.vtable.fileSync = sync;
        self.vtable.dirRename = rename;
        return .{ .userdata = base.userdata, .vtable = &self.vtable };
    }

    fn sync(userdata: ?*anyopaque, file: Io.File) Io.File.SyncError!void {
        const threaded: *Io.Threaded = @ptrCast(@alignCast(userdata.?));
        const self: *DirectorySyncFault = @fieldParentPtr("threaded", threaded);
        const base = threaded.io();
        if (self.remaining > 0) {
            const stat = file.stat(base) catch return error.Unexpected;
            if (stat.kind == .directory) {
                self.remaining -= 1;
                if (self.remaining == 0) return error.InputOutput;
            }
        }
        return base.vtable.fileSync(base.userdata, file);
    }

    fn rename(userdata: ?*anyopaque, old_dir: Io.Dir, old_path: []const u8, new_dir: Io.Dir, new_path: []const u8) Io.Dir.RenameError!void {
        const threaded: *Io.Threaded = @ptrCast(@alignCast(userdata.?));
        const self: *@This() = @fieldParentPtr("threaded", threaded);
        self.rename_calls += 1;
        const targeted = if (self.rename_fail_prefix) |prefix| std.mem.startsWith(u8, old_path, prefix) else true;
        if (targeted and self.rename_failures > 0) {
            self.rename_failures -= 1;
            return error.AccessDenied;
        }
        const base = threaded.io();
        return base.vtable.dirRename(base.userdata, old_dir, old_path, new_dir, new_path);
    }
};

test "durability: atomic publication retries temporary Windows refusal and preserves persistent failure" {
    if (@import("builtin").os.tag != .windows) return error.SkipZigTest;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var fault = DirectorySyncFault{ .threaded = .init(a, .{}), .rename_failures = 2 };
    defer fault.threaded.deinit();
    const io = fault.io();
    try tmp.dir.writeFile(io, .{ .sub_path = "state", .data = "old" });
    try @import("../storage/storage.zig").writeFileAtomic(io, tmp.dir, "state.tmp", "state", "new", false);
    const published = try tmp.dir.readFileAlloc(io, "state", a, .limited(16));
    defer a.free(published);
    try std.testing.expectEqualStrings("new", published);
    try std.testing.expect(fault.rename_calls > 2);
    fault.rename_failures = std.math.maxInt(usize);
    try std.testing.expectError(error.AccessDenied, @import("../storage/storage.zig").writeFileAtomic(io, tmp.dir, "state.tmp", "state", "rejected", false));
    const retained = try tmp.dir.readFileAlloc(io, "state", a, .limited(16));
    defer a.free(retained);
    try std.testing.expectEqualStrings("new", retained);
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "state.tmp", .{}));
}

test "durability: post-rename directory sync failure fences writes until reopen" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var fault = DirectorySyncFault{ .threaded = .init(a, .{}) };
    defer fault.threaded.deinit();
    {
        const db = try api.Database.open(a, fault.io(), tmp.dir, .{ .sync_mode = .per_flush });
        defer db.close();
        const table = try db.table("t", .{
            .columns = &.{.{ .name = "id", .type = .bigint }},
            .order_key = &.{"id"},
            .unique = false,
        }, .{ .order_key = &.{"id"} });
        try table.insert(&.{.{ .id = @as(i64, 7) }});
        // Sync the segment directory; fail the table-directory sync after
        // manifest replacement. File syncs continue using the real backend.
        fault.remaining = 2;
        try std.testing.expectError(error.DurabilityUncertain, table.flush());
        try std.testing.expectError(error.RecoveryRequired, table.insert(&.{.{ .id = @as(i64, 8) }}));
        try std.testing.expectError(error.RecoveryRequired, exec.scan(a, table));
    }
    const db = try api.Database.open(a, std.testing.io, tmp.dir, .{});
    defer db.close();
    var query = try exec.scan(a, try db.openTable("t", .{}));
    defer query.deinit();
    var rows: usize = 0;
    while (try query.next()) |batch| {
        for (batch.values[0].data.bigint[0..batch.row_count]) |value| try std.testing.expectEqual(@as(i64, 7), value);
        rows += batch.row_count;
    }
    try std.testing.expectEqual(@as(usize, 1), rows);
}

test "durability: alter swap retries a transient rename refusal and fences the table on a persistent one" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // Refuse only the swap's `__alter_<name>` -> `<name>` rename; the manifest
    // and schema publications inside the rewrite keep the real backend.
    var fault = DirectorySyncFault{ .threaded = .init(a, .{}), .rename_fail_prefix = "__alter_" };
    defer fault.threaded.deinit();
    const db = try api.Database.open(a, fault.io(), tmp.dir, .{});
    defer db.close();
    const table_def: @import("../types.zig").TableSchema = .{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    const add_note: api.AlterOp = .{ .add = .{ .name = "note", .type = .bigint, .nullable = true } };

    if (@import("builtin").os.tag == .windows) {
        const retried = try db.table("t", table_def, .{ .order_key = &.{"id"} });
        try retried.insert(&.{.{ .id = @as(i64, 7) }});
        try retried.flush();
        fault.rename_failures = 2;
        try db.alterTable("t", &.{add_note});
        try std.testing.expectEqual(@as(usize, 0), fault.rename_failures);
        try std.testing.expect(retried.schema.columnIndex("note") != null);
    }

    const fenced = try db.table("u", table_def, .{ .order_key = &.{"id"} });
    try fenced.insert(&.{.{ .id = @as(i64, 7) }});
    try fenced.flush();
    fault.rename_failures = std.math.maxInt(usize);
    try std.testing.expectError(error.AccessDenied, db.alterTable("u", &.{add_note}));
    try std.testing.expectError(error.RecoveryRequired, fenced.insert(&.{.{ .id = @as(i64, 8) }}));
    try std.testing.expectError(error.RecoveryRequired, exec.scan(a, fenced));
    // `db.close()` must skip the directory handles the failed swap closed.
}
