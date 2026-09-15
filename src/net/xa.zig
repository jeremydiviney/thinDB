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
//! PREPARE atomically publishes and syncs the bounded branch record. Long XIDs
//! use a SHA-256 filename; short XIDs retain the older hex naming. COMMIT is
//! orchestrated by xa_exec.zig with an undo journal and exclusive visibility
//! lease. Startup resolves that journal before loading prepared branches.
//! ACTIVE branches are volatile, and reads do not see their staged writes.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const storage = @import("../storage/storage.zig");

const max_branch_bytes = 64 << 20;
const max_xid_bytes = 1024;

pub const Error = error{
    XaBranchExists,
    XaBranchUnknown,
    XaBranchBusy,
    XaProtocol,
    XaBranchTooLarge,
    XaInvalidXid,
} || Allocator.Error;

pub const State = enum { active, ended, prepared, committing };

/// One XA branch: buffered statements (encoded IR) + lifecycle state. Its arena
/// owns the encoded bytes + the db name so they outlive the request that staged
/// them.
pub const Branch = struct {
    arena: std.heap.ArenaAllocator,
    /// Database the staged statements target (recorded at XA START, so a commit
    /// from another connection / after a restart resolves the right tables).
    db: []const u8,
    schema: []const u8 = "public",
    state: State = .active,
    stmts: std.ArrayListUnmanaged([]const u8) = .empty,
    /// Wall-clock microseconds when the branch was PREPARED (0 while ACTIVE).
    /// Absolute real time so age survives a restart; drives GC of orphans.
    prepared_at_us: i64 = 0,
    encoded_bytes: usize = 0,

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
    /// slowly-recovering job's branch could be aborted from under it. Disabled
    /// by default: elapsed time is not a coordinator rollback decision.
    gc_max_age_us: i64 = 0,

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
    pub fn setStorage(self: *XaManager, io: Io, root: Io.Dir) !void {
        // The fallback must also open with .iterate: a dir opened without it
        // cannot be listed on Linux (O_PATH fd), only Windows tolerates that.
        const dir = root.openDir(io, "_xa", .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => try root.createDirPathOpen(io, "_xa", .{
                .open_options = .{ .iterate = true },
            }),
            else => return err,
        };
        self.io = io;
        self.dir = dir;
        try storage.syncDirectory(io, root);
        try self.loadAll();
    }

    fn lock(self: *XaManager) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// XA START — open a new ACTIVE branch targeting `db`. Errors on collision.
    pub fn begin(self: *XaManager, xid: []const u8, db: []const u8) Error!void {
        return self.beginInSchema(xid, db, "public");
    }

    pub fn beginInSchema(self: *XaManager, xid: []const u8, db: []const u8, schema: []const u8) Error!void {
        if (xid.len == 0 or xid.len > max_xid_bytes) return Error.XaInvalidXid;
        if (db.len > max_branch_bytes - 24 - xid.len or schema.len > max_branch_bytes - 24 - xid.len - db.len) return Error.XaBranchTooLarge;
        self.lock();
        defer self.mutex.unlock();
        if (self.map.get(xid) != null) return Error.XaBranchExists;
        const key = try self.allocator.dupe(u8, xid);
        errdefer self.allocator.free(key);
        const branch = try self.allocator.create(Branch);
        errdefer self.allocator.destroy(branch);
        branch.* = .{ .arena = std.heap.ArenaAllocator.init(self.allocator), .db = "" };
        errdefer branch.deinit();
        branch.db = try branch.arena.allocator().dupe(u8, db);
        branch.schema = try branch.arena.allocator().dupe(u8, schema);
        branch.encoded_bytes = 24 + xid.len + db.len + schema.len;
        try self.map.put(self.allocator, key, branch);
    }

    /// Buffer one statement's encoded IR into an ACTIVE branch.
    pub fn stage(self: *XaManager, xid: []const u8, encoded: []const u8) Error!void {
        self.lock();
        defer self.mutex.unlock();
        const branch = self.map.get(xid) orelse return Error.XaBranchUnknown;
        if (branch.state != .active) return Error.XaProtocol;
        if (branch.encoded_bytes > max_branch_bytes - 4 or encoded.len > max_branch_bytes - branch.encoded_bytes - 4) return Error.XaBranchTooLarge;
        const a = branch.arena.allocator();
        try branch.stmts.append(a, try a.dupe(u8, encoded));
        branch.encoded_bytes += 4 + encoded.len;
    }

    pub fn end(self: *XaManager, xid: []const u8) Error!void {
        self.lock();
        defer self.mutex.unlock();
        const branch = self.map.get(xid) orelse return Error.XaBranchUnknown;
        if (branch.state != .active) return Error.XaProtocol;
        branch.state = .ended;
    }

    /// XA PREPARE — mark prepared and persist durably (crash-safe from here).
    pub fn prepare(self: *XaManager, xid: []const u8) !void {
        self.lock();
        defer self.mutex.unlock();
        const branch = self.map.get(xid) orelse return Error.XaBranchUnknown;
        if (branch.state != .ended) return Error.XaProtocol;
        branch.prepared_at_us = if (self.io) |io| std.Io.Timestamp.now(io, .real).toMicroseconds() else 0;
        try self.persist(xid, branch);
        branch.state = .prepared;
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
        var removed: usize = 0;
        for (stale.items) |xid| {
            self.rollback(xid) catch continue;
            removed += 1;
        }
        return removed;
    }

    pub fn beginCommit(self: *XaManager, xid: []const u8) Error!?*Branch {
        self.lock();
        defer self.mutex.unlock();
        const branch = self.map.get(xid) orelse return null;
        if (branch.state == .committing) return Error.XaBranchBusy;
        if (branch.state != .prepared) return Error.XaProtocol;
        branch.state = .committing;
        return branch;
    }

    pub fn cancelCommit(self: *XaManager, xid: []const u8) void {
        self.lock();
        defer self.mutex.unlock();
        if (self.map.get(xid)) |branch| {
            std.debug.assert(branch.state == .committing);
            branch.state = .prepared;
        }
    }

    pub fn finishCommit(self: *XaManager, xid: []const u8) !void {
        self.lock();
        defer self.mutex.unlock();
        const branch = self.map.get(xid) orelse return Error.XaBranchUnknown;
        if (branch.state != .committing) return Error.XaProtocol;
        try self.unlink(xid);
        self.removeBranch(xid);
    }

    pub fn rollback(self: *XaManager, xid: []const u8) !void {
        self.lock();
        defer self.mutex.unlock();
        const branch = self.map.get(xid) orelse return;
        if (branch.state == .committing) return Error.XaBranchBusy;
        try self.unlink(xid);
        self.removeBranch(xid);
    }

    fn removeBranch(self: *XaManager, xid: []const u8) void {
        if (self.map.fetchRemove(xid)) |entry| {
            self.allocator.free(entry.key);
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
        return @import("../storage/write_journal.zig").branchFileName(buf, xid);
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
        try appendU32(self.allocator, &body, @intCast(branch.schema.len));
        try body.appendSlice(self.allocator, branch.schema);
        var tempbuf: [528]u8 = undefined;
        const temporary = try std.fmt.bufPrint(&tempbuf, "{s}.tmp", .{fname});
        try storage.writeFileAtomic(io, dir, temporary, fname, body.items, true);
    }

    fn unlink(self: *XaManager, xid: []const u8) !void {
        const io = self.io orelse return;
        const dir = self.dir orelse return;
        var namebuf: [520]u8 = undefined;
        const fname = fileName(&namebuf, xid) orelse return;
        dir.deleteFile(io, fname) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        try storage.syncDirectory(io, dir);
    }

    /// Re-read all persisted prepared branches on open.
    fn loadAll(self: *XaManager) !void {
        const io = self.io orelse return;
        var dir = self.dir orelse return;
        var it = dir.iterate();
        while (try it.next(io)) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".xa")) continue;
            const bytes = try dir.readFileAlloc(io, entry.name, self.allocator, .limited(max_branch_bytes));
            defer self.allocator.free(bytes);
            var cursor: usize = 0;
            const xid = try readSlice(bytes, &cursor);
            if (xid.len == 0 or xid.len > max_xid_bytes) return error.Corrupt;
            var namebuf: [520]u8 = undefined;
            const expected = fileName(&namebuf, xid) orelse return error.Corrupt;
            if (!std.mem.eql(u8, entry.name, expected)) return error.Corrupt;
            try self.loadOne(bytes);
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
        errdefer branch.deinit();
        const a = branch.arena.allocator();
        branch.db = try a.dupe(u8, db);
        var i: u32 = 0;
        while (i < n) : (i += 1) {
            const s = try readSlice(bytes, &c);
            try branch.stmts.append(a, try a.dupe(u8, s));
        }
        if (c < bytes.len) branch.schema = try a.dupe(u8, try readSlice(bytes, &c));
        if (c != bytes.len) return error.Corrupt;
        branch.encoded_bytes = bytes.len;
        self.lock();
        defer self.mutex.unlock();
        if (self.map.contains(key)) return Error.XaBranchExists;
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

    const branch = (try mgr.beginCommit("x1")).?;
    try testing.expectEqual(@as(usize, 2), branch.stmts.items.len);
    try testing.expectEqualStrings("main", branch.db);
    try testing.expectError(Error.XaBranchBusy, mgr.rollback("x1"));
    try mgr.finishCommit("x1");

    try testing.expect(try mgr.beginCommit("x1") == null);
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
    try testing.expect(try mgr.beginCommit("old") == null);
    try testing.expect(try mgr.beginCommit("young") != null);
    try mgr.finishCommit("young");

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
    try mgr.rollback("r1");
    try testing.expect(try mgr.beginCommit("r1") == null);
}

test "xa prepare fails without publishing state when its file cannot be written" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var mgr = XaManager.init(a);
    defer mgr.deinit();
    try mgr.setStorage(io, tmp.dir);
    try mgr.begin("blocked", "main");
    try mgr.stage("blocked", "statement");
    try mgr.end("blocked");
    var namebuf: [520]u8 = undefined;
    const name = XaManager.fileName(&namebuf, "blocked").?;
    try mgr.dir.?.createDir(io, name, .default_dir);
    var failed = false;
    mgr.prepare("blocked") catch {
        failed = true;
    };
    try std.testing.expect(failed);
    try std.testing.expectEqual(State.ended, mgr.map.get("blocked").?.state);
    try mgr.dir.?.deleteDir(io, name);
    try mgr.prepare("blocked");
    try std.testing.expectEqual(State.prepared, mgr.map.get("blocked").?.state);
}

test "xa prepared statements and long XIDs survive reopening" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const xid = "a" ** 300;
    {
        var mgr = XaManager.init(a);
        defer mgr.deinit();
        try mgr.setStorage(io, tmp.dir);
        try mgr.begin(xid, "main");
        try mgr.stage(xid, "one");
        try mgr.stage(xid, "two");
        try mgr.end(xid);
        try mgr.prepare(xid);
    }
    var recovered = XaManager.init(a);
    defer recovered.deinit();
    try recovered.setStorage(io, tmp.dir);
    const branch = recovered.map.get(xid).?;
    try std.testing.expectEqual(State.prepared, branch.state);
    try std.testing.expectEqualStrings("main", branch.db);
    try std.testing.expectEqual(@as(usize, 2), branch.stmts.items.len);
    try std.testing.expectEqualStrings("two", branch.stmts.items[1]);
}

test "xa recovery reports corrupt durable records" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDir(io, "_xa", .default_dir);
    try tmp.dir.writeFile(io, .{ .sub_path = "_xa/bad.xa", .data = "truncated" });
    var mgr = XaManager.init(a);
    defer mgr.deinit();
    try std.testing.expectError(error.Corrupt, mgr.setStorage(io, tmp.dir));
}

test "xa staging enforces the recovery size limit before retaining bytes" {
    var mgr = XaManager.init(std.testing.allocator);
    defer mgr.deinit();
    try mgr.begin("bounded", "main");
    const branch = mgr.map.get("bounded").?;
    branch.encoded_bytes = max_branch_bytes - 4;
    try std.testing.expectError(Error.XaBranchTooLarge, mgr.stage("bounded", "x"));
    try std.testing.expectEqual(@as(usize, 0), branch.stmts.items.len);
}
