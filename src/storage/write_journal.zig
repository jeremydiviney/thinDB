const std = @import("std");
const storage = @import("storage.zig");
const Io = std.Io;
const Allocator = std.mem.Allocator;

pub const Metadata = struct { xid: []const u8, tables: []const []const u8 };

/// One catalog commit runs at a time. Immutable segments are retained while
/// this undo record owns the pre-commit manifests, WALs and tombstones.
pub const WriteJournal = struct {
    allocator: Allocator,
    io: Io,
    root: Io.Dir,
    parent: Io.Dir,
    dir: Io.Dir,

    pub fn begin(allocator: Allocator, io: Io, root: Io.Dir, metadata: Metadata) !WriteJournal {
        const parent = try root.openDir(io, "_xa", .{ .iterate = true });
        errdefer parent.close(io);
        try removeResolved(io, parent);
        try parent.createDir(io, "commit", .default_dir);
        const dir = try parent.openDir(io, "commit", .{ .iterate = true });
        errdefer dir.close(io);
        const self: WriteJournal = .{ .allocator = allocator, .io = io, .root = root, .parent = parent, .dir = dir };
        for (metadata.tables, 0..) |path, i| {
            try validatePath(path);
            const source = try root.openDir(io, path, .{ .iterate = true });
            defer source.close(io);
            var buf: [32]u8 = undefined;
            const name = try std.fmt.bufPrint(&buf, "{d}", .{i});
            const backup = try dir.createDirPathOpen(io, name, .{ .open_options = .{ .iterate = true } });
            defer backup.close(io);
            _ = try copyIfPresent(io, source, "manifest", backup, "manifest");
            _ = try copyIfPresent(io, source, "wal", backup, "wal");
            const tombs = try backup.createDirPathOpen(io, "tombs", .{ .open_options = .{ .iterate = true } });
            defer tombs.close(io);
            const segments = try source.openDir(io, "segments", .{ .iterate = true });
            defer segments.close(io);
            var it = segments.iterate();
            while (try it.next(io)) |entry| {
                if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".tomb"))
                    _ = try copyIfPresent(io, segments, entry.name, tombs, entry.name);
            }
            try storage.syncDirectory(io, tombs);
            try storage.syncDirectory(io, backup);
        }
        var json_writer: Io.Writer.Allocating = .init(allocator);
        defer json_writer.deinit();
        try std.json.Stringify.value(metadata, .{}, &json_writer.writer);
        try storage.writeFileAtomic(io, dir, "metadata.tmp", "metadata", json_writer.written(), true);
        try storage.syncDirectory(io, parent);
        try storage.writeFileAtomic(io, dir, "ready.tmp", "ready", "ready", true);
        return self;
    }

    pub fn deinit(self: *WriteJournal) void {
        self.dir.close(self.io);
        self.parent.close(self.io);
    }

    pub fn markCommitted(self: *WriteJournal) !void {
        try storage.writeFileAtomic(self.io, self.dir, "committed.tmp", "committed", "committed", true);
    }

    pub fn restore(self: *WriteJournal, paths: []const []const u8) !void {
        for (paths, 0..) |path, i| {
            try validatePath(path);
            const target = try self.root.openDir(self.io, path, .{ .iterate = true });
            defer target.close(self.io);
            var buf: [32]u8 = undefined;
            const name = try std.fmt.bufPrint(&buf, "{d}", .{i});
            const backup = try self.dir.openDir(self.io, name, .{});
            defer backup.close(self.io);
            const segments = try target.openDir(self.io, "segments", .{ .iterate = true });
            defer segments.close(self.io);
            const tombs = try backup.openDir(self.io, "tombs", .{ .iterate = true });
            defer tombs.close(self.io);
            var current = segments.iterate();
            while (try current.next(self.io)) |entry| {
                if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".tomb"))
                    try segments.deleteFile(self.io, entry.name);
            }
            var old = tombs.iterate();
            while (try old.next(self.io)) |entry| {
                if (entry.kind == .file) _ = try copyIfPresent(self.io, tombs, entry.name, segments, entry.name);
            }
            try storage.syncDirectory(self.io, segments);
            inline for (.{ "wal", "manifest" }) |file| {
                if (!try copyIfPresent(self.io, backup, file, target, file)) try removeFile(self.io, target, file);
            }
            try storage.syncDirectory(self.io, target);
        }
    }

    /// Retire the complete journal before deleting any of its files. A crash
    /// during recursive cleanup must not turn a committed record into undo.
    pub fn retire(self: *WriteJournal) !void {
        const replacement = try self.parent.openDir(self.io, ".", .{ .iterate = true });
        self.dir.close(self.io);
        self.dir = replacement;
        try Io.Dir.rename(self.parent, "commit", self.parent, "resolved", self.io);
        try storage.syncDirectory(self.io, self.parent);
        try removeResolved(self.io, self.parent);
    }
};

pub fn recover(allocator: Allocator, io: Io, root: Io.Dir) !void {
    const parent = root.openDir(io, "_xa", .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer parent.close(io);
    try removeResolved(io, parent);
    const dir = parent.openDir(io, "commit", .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    var journal: WriteJournal = .{ .allocator = allocator, .io = io, .root = root, .parent = parent, .dir = dir };
    defer journal.dir.close(io);
    if (try hasMarker(allocator, io, dir, "ready")) {
        const bytes = try dir.readFileAlloc(io, "metadata", allocator, .limited(64 << 20));
        defer allocator.free(bytes);
        const parsed = try std.json.parseFromSlice(Metadata, allocator, bytes, .{});
        defer parsed.deinit();
        if (try hasMarker(allocator, io, dir, "committed")) {
            var buf: [520]u8 = undefined;
            const name = branchFileName(&buf, parsed.value.xid) orelse return error.Corrupt;
            try removeFile(io, parent, name);
            try storage.syncDirectory(io, parent);
        } else {
            try journal.restore(parsed.value.tables);
        }
    }
    try journal.retire();
}

pub fn branchFileName(buf: []u8, xid: []const u8) ?[]const u8 {
    if (xid.len > 124) {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(xid, &digest, .{});
        return std.fmt.bufPrint(buf, "h-{x}.xa", .{digest}) catch null;
    }
    if (xid.len * 2 + 3 > buf.len) return null;
    const hex = "0123456789abcdef";
    for (xid, 0..) |b, i| {
        buf[i * 2] = hex[b >> 4];
        buf[i * 2 + 1] = hex[b & 0xf];
    }
    @memcpy(buf[xid.len * 2 ..][0..3], ".xa");
    return buf[0 .. xid.len * 2 + 3];
}

fn hasMarker(allocator: Allocator, io: Io, dir: Io.Dir, name: []const u8) !bool {
    const data = dir.readFileAlloc(io, name, allocator, .limited(64)) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer allocator.free(data);
    if (!std.mem.eql(u8, data, name)) return error.Corrupt;
    return true;
}

fn copyIfPresent(io: Io, source: Io.Dir, source_path: []const u8, target: Io.Dir, target_path: []const u8) !bool {
    if (!try exists(io, source, source_path)) return false;
    try Io.Dir.copyFile(source, source_path, target, target_path, io, .{ .replace = true });
    const file = try target.openFile(io, target_path, .{ .mode = .read_write });
    defer file.close(io);
    try file.sync(io);
    return true;
}

fn exists(io: Io, dir: Io.Dir, path: []const u8) !bool {
    dir.access(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

fn removeFile(io: Io, dir: Io.Dir, path: []const u8) !void {
    dir.deleteFile(io, path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
}

fn removeResolved(io: Io, parent: Io.Dir) !void {
    try parent.deleteTree(io, "resolved");
}

fn validatePath(path: []const u8) !void {
    if (path.len == 0 or std.fs.path.isAbsolute(path)) return error.Corrupt;
    var parts = std.mem.tokenizeAny(u8, path, "/\\");
    while (parts.next()) |part| {
        if (std.mem.eql(u8, part, "..") or std.mem.eql(u8, part, ".") or std.mem.indexOfScalar(u8, part, ':') != null)
            return error.Corrupt;
    }
}

test "xa journal recovery chooses rollback or completion and is repeatable" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    inline for (.{ false, true }) |committed| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.createDir(io, "_xa", .default_dir);
        const table = try tmp.dir.createDirPathOpen(io, "main/public/t", .{ .open_options = .{ .iterate = true } });
        defer table.close(io);
        try table.createDir(io, "segments", .default_dir);
        inline for (.{ "manifest", "wal", "segments/1.tomb" }) |file|
            try table.writeFile(io, .{ .sub_path = file, .data = "before" });
        var branch_buf: [64]u8 = undefined;
        const branch_file = branchFileName(&branch_buf, "x").?;
        const xa = try tmp.dir.openDir(io, "_xa", .{});
        defer xa.close(io);
        try xa.writeFile(io, .{ .sub_path = branch_file, .data = "prepared" });
        {
            var journal = try WriteJournal.begin(a, io, tmp.dir, .{ .xid = "x", .tables = &.{"main/public/t"} });
            defer journal.deinit();
            inline for (.{ "manifest", "wal", "segments/1.tomb" }) |file|
                try table.writeFile(io, .{ .sub_path = file, .data = "after" });
            try table.writeFile(io, .{ .sub_path = "segments/2.tomb", .data = "new" });
            if (committed) try journal.markCommitted();
        }
        try recover(a, io, tmp.dir);
        try recover(a, io, tmp.dir);
        inline for (.{ "manifest", "wal", "segments/1.tomb" }) |file| {
            const bytes = try table.readFileAlloc(io, file, a, .unlimited);
            defer a.free(bytes);
            try std.testing.expectEqualStrings(if (committed) "after" else "before", bytes);
        }
        try std.testing.expectEqual(committed, try exists(io, table, "segments/2.tomb"));
        try std.testing.expectEqual(!committed, try exists(io, xa, branch_file));
    }
}
