//! XA transaction manager — the exactly-once core for Flink's JDBC sink.
//!
//! Flink's `sink.semantic=exactly-once` drives the DB through XA:
//!   XA START xid → (staged DML) → XA END xid → XA PREPARE xid → XA COMMIT xid
//! and on recovery `XA RECOVER` lists prepared-but-uncommitted xids to re-commit.
//!
//! A branch buffers each staged statement as encoded IR (`ir.encode`); COMMIT
//! decodes + applies them atomically. `xid` is an opaque dedup key: committing
//! a xid that isn't present is a no-op success, so Flink's commit retries are
//! idempotent.
//!
//! Durability (stage 2): on PREPARE a branch is written to `<root>/_xa/<hex-
//! xid>.xa` and fsync-flushed, so a crash between PREPARE and COMMIT survives —
//! `loadAll` re-reads them on open and `XA RECOVER` reports them. COMMIT /
//! ROLLBACK unlink the file. ACTIVE (un-prepared) branches are intentionally NOT
//! persisted: a crash before PREPARE loses them, which is correct — Flink
//! reproduces that data from the previous checkpoint.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

pub const Error = error{
    XaBranchExists,
    XaBranchUnknown,
    XaBranchBusy,
    XaProtocol,
} || Allocator.Error;

pub const State = enum { active, ended, prepared };

/// One XA branch: buffered statements (encoded IR) + lifecycle state. Its arena
/// owns the encoded bytes + the db name so they outlive the request that staged
/// them.
pub const Branch = struct {
    arena: std.heap.ArenaAllocator,
    /// Database the staged statements target (recorded at XA START, so a commit
    /// from another connection / after a restart resolves the right tables).
    db: []const u8,
    state: State = .active,
    stmts: std.ArrayListUnmanaged([]const u8) = .empty,

    fn deinit(self: *Branch) void {
        self.arena.deinit();
    }
};

/// Catalog-owned, keyed by xid. Thread-safe; prepared branches survive the
/// connection that prepared them (any connection may commit/rollback them) and,
/// with storage configured, a process restart.
pub const XaManager = struct {
    allocator: Allocator,
    map: std.StringHashMapUnmanaged(*Branch) = .empty,
    mutex: std.atomic.Mutex = .unlocked,
    /// Durable store for PREPARED branches. Null = in-memory only (tests).
    io: ?Io = null,
    dir: ?Io.Dir = null,

    pub fn init(allocator: Allocator) XaManager {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *XaManager) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            e.value_ptr.*.deinit();
            self.allocator.destroy(e.value_ptr.*);
        }
        self.map.deinit(self.allocator);
        if (self.dir) |*d| if (self.io) |io| d.close(io);
        self.* = undefined;
    }

    /// Point the manager at a persistent `_xa/` dir (opened/created under the
    /// catalog root) and load any prepared branches left by a prior run.
    pub fn setStorage(self: *XaManager, io: Io, root: Io.Dir) void {
        const dir = root.openDir(io, "_xa", .{ .iterate = true }) catch
            (root.createDirPathOpen(io, "_xa", .{}) catch return);
        self.io = io;
        self.dir = dir;
        self.loadAll() catch {};
    }

    fn lock(self: *XaManager) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// XA START — open a new ACTIVE branch targeting `db`. Errors on collision.
    pub fn begin(self: *XaManager, xid: []const u8, db: []const u8) Error!void {
        self.lock();
        defer self.mutex.unlock();
        if (self.map.get(xid) != null) return Error.XaBranchExists;
        const key = try self.allocator.dupe(u8, xid);
        errdefer self.allocator.free(key);
        const branch = try self.allocator.create(Branch);
        errdefer self.allocator.destroy(branch);
        branch.* = .{ .arena = std.heap.ArenaAllocator.init(self.allocator), .db = "" };
        branch.db = try branch.arena.allocator().dupe(u8, db);
        try self.map.put(self.allocator, key, branch);
    }

    /// Buffer one statement's encoded IR into an ACTIVE branch.
    pub fn stage(self: *XaManager, xid: []const u8, encoded: []const u8) Error!void {
        self.lock();
        defer self.mutex.unlock();
        const branch = self.map.get(xid) orelse return Error.XaBranchUnknown;
        if (branch.state != .active) return Error.XaProtocol;
        const a = branch.arena.allocator();
        try branch.stmts.append(a, try a.dupe(u8, encoded));
    }

    pub fn end(self: *XaManager, xid: []const u8) Error!void {
        self.lock();
        defer self.mutex.unlock();
        const branch = self.map.get(xid) orelse return Error.XaBranchUnknown;
        if (branch.state != .active) return Error.XaProtocol;
        branch.state = .ended;
    }

    /// XA PREPARE — mark prepared and persist durably (crash-safe from here).
    pub fn prepare(self: *XaManager, xid: []const u8) Error!void {
        self.lock();
        defer self.mutex.unlock();
        const branch = self.map.get(xid) orelse return Error.XaBranchUnknown;
        if (branch.state != .ended) return Error.XaProtocol;
        branch.state = .prepared;
        self.persist(xid, branch) catch {}; // best-effort; commit still works in-memory
    }

    /// Detach a branch for COMMIT and remove its durable record. Returns null
    /// when the xid is absent — an already-committed xid re-committed by a Flink
    /// retry (idempotent no-op). Caller applies `branch.stmts`, then `finishCommit`.
    pub fn takeForCommit(self: *XaManager, xid: []const u8) ?*Branch {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.map.fetchRemove(xid) orelse return null;
        self.allocator.free(entry.key);
        self.unlink(xid);
        return entry.value;
    }

    pub fn finishCommit(self: *XaManager, branch: *Branch) void {
        branch.deinit();
        self.allocator.destroy(branch);
    }

    pub fn rollback(self: *XaManager, xid: []const u8) void {
        self.lock();
        defer self.mutex.unlock();
        if (self.map.fetchRemove(xid)) |entry| {
            self.allocator.free(entry.key);
            self.unlink(xid);
            entry.value.deinit();
            self.allocator.destroy(entry.value);
        }
    }

    /// Prepared xids (owned copies), for `XA RECOVER`.
    pub fn preparedXids(self: *XaManager, allocator: Allocator) Error![][]const u8 {
        self.lock();
        defer self.mutex.unlock();
        var out: std.ArrayListUnmanaged([]const u8) = .empty;
        errdefer {
            for (out.items) |x| allocator.free(x);
            out.deinit(allocator);
        }
        var it = self.map.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.*.state != .prepared) continue;
            try out.append(allocator, try allocator.dupe(u8, e.key_ptr.*));
        }
        return out.toOwnedSlice(allocator);
    }

    // ---- durability ----

    fn fileName(buf: []u8, xid: []const u8) ?[]const u8 {
        // hex(xid) + ".xa" — xids carry binary bytes, so keep the name safe.
        if (xid.len * 2 + 3 > buf.len) return null;
        const hex = "0123456789abcdef";
        var i: usize = 0;
        for (xid) |b| {
            buf[i] = hex[b >> 4];
            buf[i + 1] = hex[b & 0xf];
            i += 2;
        }
        @memcpy(buf[i .. i + 3], ".xa");
        return buf[0 .. i + 3];
    }

    /// Serialize {xid, db, stmts} and write it to the branch's `.xa` file.
    fn persist(self: *XaManager, xid: []const u8, branch: *Branch) !void {
        const io = self.io orelse return;
        const dir = self.dir orelse return;
        var namebuf: [520]u8 = undefined;
        const fname = fileName(&namebuf, xid) orelse return;

        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(self.allocator);
        try appendU32(self.allocator, &body, @intCast(xid.len));
        try body.appendSlice(self.allocator, xid);
        try appendU32(self.allocator, &body, @intCast(branch.db.len));
        try body.appendSlice(self.allocator, branch.db);
        try appendU32(self.allocator, &body, @intCast(branch.stmts.items.len));
        for (branch.stmts.items) |s| {
            try appendU32(self.allocator, &body, @intCast(s.len));
            try body.appendSlice(self.allocator, s);
        }
        try dir.writeFile(io, .{ .sub_path = fname, .data = body.items });
    }

    fn unlink(self: *XaManager, xid: []const u8) void {
        const io = self.io orelse return;
        const dir = self.dir orelse return;
        var namebuf: [520]u8 = undefined;
        const fname = fileName(&namebuf, xid) orelse return;
        dir.deleteFile(io, fname) catch {};
    }

    /// Re-read all persisted prepared branches on open.
    fn loadAll(self: *XaManager) !void {
        const io = self.io orelse return;
        var dir = self.dir orelse return;
        var it = dir.iterate();
        while (try it.next(io)) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".xa")) continue;
            const bytes = dir.readFileAlloc(io, entry.name, self.allocator, .limited(64 << 20)) catch continue;
            defer self.allocator.free(bytes);
            self.loadOne(bytes) catch continue;
        }
    }

    fn loadOne(self: *XaManager, bytes: []const u8) !void {
        var c: usize = 0;
        const xid = try readSlice(bytes, &c);
        const db = try readSlice(bytes, &c);
        if (c + 4 > bytes.len) return error.Corrupt;
        const n = readU32(bytes[c .. c + 4]);
        c += 4;

        const key = try self.allocator.dupe(u8, xid);
        errdefer self.allocator.free(key);
        const branch = try self.allocator.create(Branch);
        errdefer self.allocator.destroy(branch);
        branch.* = .{ .arena = std.heap.ArenaAllocator.init(self.allocator), .db = "", .state = .prepared };
        const a = branch.arena.allocator();
        branch.db = try a.dupe(u8, db);
        var i: u32 = 0;
        while (i < n) : (i += 1) {
            const s = try readSlice(bytes, &c);
            try branch.stmts.append(a, try a.dupe(u8, s));
        }
        self.lock();
        defer self.mutex.unlock();
        try self.map.put(self.allocator, key, branch);
    }
};

fn appendU32(allocator: Allocator, out: *std.ArrayList(u8), v: u32) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(u32, &b, v, .little);
    try out.appendSlice(allocator, &b);
}

fn readU32(b: []const u8) u32 {
    return std.mem.readInt(u32, b[0..4], .little);
}

fn readSlice(bytes: []const u8, c: *usize) ![]const u8 {
    if (c.* + 4 > bytes.len) return error.Corrupt;
    const len = readU32(bytes[c.* .. c.* + 4]);
    c.* += 4;
    if (c.* + len > bytes.len) return error.Corrupt;
    const s = bytes[c.* .. c.* + len];
    c.* += len;
    return s;
}

test "xa branch lifecycle: begin/stage/end/prepare/commit + idempotent" {
    const testing = std.testing;
    var mgr = XaManager.init(testing.allocator);
    defer mgr.deinit();

    try mgr.begin("x1", "main");
    try testing.expectError(Error.XaBranchExists, mgr.begin("x1", "main"));
    try mgr.stage("x1", &.{ 1, 2, 3 });
    try mgr.stage("x1", &.{ 4, 5 });
    try mgr.end("x1");
    try mgr.prepare("x1");

    const prepared = try mgr.preparedXids(testing.allocator);
    defer {
        for (prepared) |x| testing.allocator.free(x);
        testing.allocator.free(prepared);
    }
    try testing.expectEqual(@as(usize, 1), prepared.len);

    const branch = mgr.takeForCommit("x1").?;
    try testing.expectEqual(@as(usize, 2), branch.stmts.items.len);
    try testing.expectEqualStrings("main", branch.db);
    mgr.finishCommit(branch);

    try testing.expect(mgr.takeForCommit("x1") == null); // idempotent
}

test "xa rollback discards" {
    const testing = std.testing;
    var mgr = XaManager.init(testing.allocator);
    defer mgr.deinit();
    try mgr.begin("r1", "main");
    try mgr.stage("r1", &.{9});
    mgr.rollback("r1");
    try testing.expect(mgr.takeForCommit("r1") == null);
}
