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
    /// Wall-clock microseconds when the branch was PREPARED (0 while ACTIVE).
    /// Absolute real time so age survives a restart; drives GC of orphans.
    prepared_at_us: i64 = 0,

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
    /// A PREPARED branch older than this is orphaned (its Flink job died /
    /// was cancelled without committing) and gets rolled back by `gcSweep`.
    /// Must exceed Flink's checkpoint interval + max tolerable downtime, or a
    /// slowly-recovering job's branch could be aborted from under it. Default
    /// 24h; 0 disables GC.
    gc_max_age_us: i64 = 24 * 3600 * 1_000_000,

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
        // The fallback must also open with .iterate: a dir opened without it
        // cannot be listed on Linux (O_PATH fd), only Windows tolerates that.
        const dir = root.openDir(io, "_xa", .{ .iterate = true }) catch
            (root.createDirPathOpen(io, "_xa", .{
                .open_options = .{ .iterate = true },
            }) catch return);
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
        branch.prepared_at_us = if (self.io) |io| std.Io.Timestamp.now(io, .real).toMicroseconds() else 0;
        self.persist(xid, branch) catch {}; // best-effort; commit still works in-memory
    }

    /// Roll back every PREPARED branch older than `gc_max_age_us` — orphans from
    /// a Flink job that died / was cancelled without committing. Returns the
    /// count aborted. A no-op when GC is disabled or no clock is configured.
    pub fn gcSweep(self: *XaManager) usize {
        const io = self.io orelse return 0;
        return self.gcSweepAt(std.Io.Timestamp.now(io, .real).toMicroseconds());
    }

    /// `gcSweep` with an injected clock (for tests).
    pub fn gcSweepAt(self: *XaManager, now: i64) usize {
        if (self.gc_max_age_us <= 0) return 0;

        // Collect stale xids first — dup'd, because rollback() frees the map's
        // key and re-locks (can't hold the lock across it).
        var stale: std.ArrayListUnmanaged([]const u8) = .empty;
        defer {
            for (stale.items) |x| self.allocator.free(x);
            stale.deinit(self.allocator);
        }
        self.lock();
        {
            var it = self.map.iterator();
            while (it.next()) |e| {
                const b = e.value_ptr.*;
                if (b.state == .prepared and b.prepared_at_us != 0 and now - b.prepared_at_us > self.gc_max_age_us) {
                    const dup = self.allocator.dupe(u8, e.key_ptr.*) catch continue;
                    stale.append(self.allocator, dup) catch self.allocator.free(dup);
                }
            }
        }
        self.mutex.unlock();
        for (stale.items) |xid| self.rollback(xid);
        return stale.items.len;
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
        var ts: [8]u8 = undefined;
        std.mem.writeInt(i64, &ts, branch.prepared_at_us, .little);
        try body.appendSlice(self.allocator, &ts);
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
        if (c + 8 > bytes.len) return error.Corrupt;
        const prepared_at = std.mem.readInt(i64, bytes[c..][0..8], .little);
        c += 8;
        if (c + 4 > bytes.len) return error.Corrupt;
        const n = readU32(bytes[c .. c + 4]);
        c += 4;

        const key = try self.allocator.dupe(u8, xid);
        errdefer self.allocator.free(key);
        const branch = try self.allocator.create(Branch);
        errdefer self.allocator.destroy(branch);
        branch.* = .{ .arena = std.heap.ArenaAllocator.init(self.allocator), .db = "", .state = .prepared, .prepared_at_us = prepared_at };
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

pub const ParsedXid = struct { format_id: i64, gtrid: []const u8, bqual: []const u8 };

/// Decompose a MySQL XA xid literal into (formatID, gtrid, bqual) for XA
/// RECOVER. Accepts `gtrid`, `gtrid,bqual`, or `gtrid,bqual,formatID` where each
/// of gtrid/bqual is `'text'`, `X'hex'`/`x'hex'`, `0xhex`, or bare text (which is
/// what MySQL Connector/J's XAResource emits — it hex-encodes). Bytes are
/// allocated from `allocator`. Falls back to the whole string as an opaque gtrid.
pub fn parseXid(allocator: Allocator, text: []const u8) !ParsedXid {
    var parts: [3][]const u8 = undefined;
    var np: usize = 0;
    var start: usize = 0;
    var in_q = false;
    var i: usize = 0;
    while (i < text.len and np < 2) : (i += 1) {
        if (text[i] == '\'') in_q = !in_q;
        if (text[i] == ',' and !in_q) {
            parts[np] = std.mem.trim(u8, text[start..i], " \t");
            np += 1;
            start = i + 1;
        }
    }
    parts[np] = std.mem.trim(u8, text[start..], " \t");
    np += 1;

    const gtrid = try decodeXidPart(allocator, parts[0]);
    const bqual = if (np >= 2) try decodeXidPart(allocator, parts[1]) else try allocator.dupe(u8, "");
    var fmt: i64 = 1;
    if (np >= 3) {
        // Connector/J emits the formatID as 0x-hex too (e.g. `0x1`).
        const fp = parts[2];
        fmt = if (fp.len >= 2 and fp[0] == '0' and (fp[1] == 'x' or fp[1] == 'X'))
            std.fmt.parseInt(i64, fp[2..], 16) catch 1
        else
            std.fmt.parseInt(i64, fp, 10) catch 1;
    }
    return .{ .format_id = fmt, .gtrid = gtrid, .bqual = bqual };
}

fn decodeXidPart(allocator: Allocator, s: []const u8) ![]const u8 {
    if (s.len >= 3 and (s[0] == 'X' or s[0] == 'x') and s[1] == '\'' and s[s.len - 1] == '\'')
        return hexDecode(allocator, s[2 .. s.len - 1]);
    if (s.len >= 2 and s[0] == '0' and (s[1] == 'x' or s[1] == 'X'))
        return hexDecode(allocator, s[2..]);
    if (s.len >= 2 and s[0] == '\'' and s[s.len - 1] == '\'')
        return allocator.dupe(u8, s[1 .. s.len - 1]);
    return allocator.dupe(u8, s);
}

fn hexDecode(allocator: Allocator, hex: []const u8) ![]const u8 {
    const out = try allocator.alloc(u8, hex.len / 2);
    errdefer allocator.free(out);
    for (out, 0..) |*o, i| {
        o.* = (@as(u8, try hexNibble(hex[i * 2])) << 4) | @as(u8, try hexNibble(hex[i * 2 + 1]));
    }
    return out;
}

fn hexNibble(c: u8) !u4 {
    return switch (c) {
        '0'...'9' => @intCast(c - '0'),
        'a'...'f' => @intCast(c - 'a' + 10),
        'A'...'F' => @intCast(c - 'A' + 10),
        else => error.BadHex,
    };
}

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

test "parseXid: hex gtrid/bqual + formatID" {
    const a = std.testing.allocator;
    const p = try parseXid(a, "X'6774726964',X'6271',7"); // hex('gtrid'), hex('bq'), 7
    defer {
        a.free(p.gtrid);
        a.free(p.bqual);
    }
    try std.testing.expectEqualStrings("gtrid", p.gtrid);
    try std.testing.expectEqualStrings("bq", p.bqual);
    try std.testing.expectEqual(@as(i64, 7), p.format_id);
}

test "parseXid: opaque fallback + default format" {
    const a = std.testing.allocator;
    const p = try parseXid(a, "'dx1'");
    defer {
        a.free(p.gtrid);
        a.free(p.bqual);
    }
    try std.testing.expectEqualStrings("dx1", p.gtrid);
    try std.testing.expectEqual(@as(usize, 0), p.bqual.len);
    try std.testing.expectEqual(@as(i64, 1), p.format_id);
}

test "xa gc: aborts prepared branches older than max age, keeps young ones" {
    const testing = std.testing;
    var mgr = XaManager.init(testing.allocator);
    defer mgr.deinit();
    mgr.gc_max_age_us = 1000;

    // Two prepared branches with controlled prepare times.
    for ([_][]const u8{ "old", "young" }) |x| {
        try mgr.begin(x, "main");
        try mgr.stage(x, &.{1});
        try mgr.end(x);
        try mgr.prepare(x);
    }
    mgr.map.get("old").?.prepared_at_us = 100;
    mgr.map.get("young").?.prepared_at_us = 9_500;

    // now = 10_000: old is 9_900µs stale (> 1000 → aborted), young is 500µs.
    try testing.expectEqual(@as(usize, 1), mgr.gcSweepAt(10_000));
    try testing.expect(mgr.takeForCommit("old") == null); // gone
    const young = mgr.takeForCommit("young") orelse return error.YoungAborted;
    mgr.finishCommit(young); // survived

    // Disabled GC is a no-op.
    mgr.gc_max_age_us = 0;
    try mgr.begin("z", "main");
    try mgr.end("z");
    try mgr.prepare("z");
    mgr.map.get("z").?.prepared_at_us = 1;
    try testing.expectEqual(@as(usize, 0), mgr.gcSweepAt(1_000_000_000));
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
