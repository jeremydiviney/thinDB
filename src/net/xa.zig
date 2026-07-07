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
//! Stage 1 is in-memory (no crash durability yet — a prepared branch is lost on
//! restart, which is safe: Flink reproduces it from the previous checkpoint).
//! Stage 2 will persist prepared branches so a mid-2PC crash survives.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Error = error{
    XaBranchExists,
    XaBranchUnknown,
    XaBranchBusy,
    XaProtocol,
} || Allocator.Error;

pub const State = enum { active, ended, prepared };

/// One XA branch: buffered statements (encoded IR) + lifecycle state. Its arena
/// owns the encoded bytes so they outlive the request that staged them.
pub const Branch = struct {
    arena: std.heap.ArenaAllocator,
    state: State = .active,
    stmts: std.ArrayListUnmanaged([]const u8) = .empty,

    fn deinit(self: *Branch) void {
        self.arena.deinit();
    }
};

/// Catalog-owned, keyed by xid. Thread-safe; prepared branches survive the
/// connection that prepared them (any connection may commit/rollback them).
pub const XaManager = struct {
    allocator: Allocator,
    map: std.StringHashMapUnmanaged(*Branch) = .empty,
    mutex: std.atomic.Mutex = .unlocked,

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
        self.* = undefined;
    }

    fn lock(self: *XaManager) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// XA START — open a new ACTIVE branch. Errors if the xid already exists.
    pub fn begin(self: *XaManager, xid: []const u8) Error!void {
        self.lock();
        defer self.mutex.unlock();
        if (self.map.get(xid) != null) return Error.XaBranchExists;
        const key = try self.allocator.dupe(u8, xid);
        errdefer self.allocator.free(key);
        const branch = try self.allocator.create(Branch);
        errdefer self.allocator.destroy(branch);
        branch.* = .{ .arena = std.heap.ArenaAllocator.init(self.allocator) };
        try self.map.put(self.allocator, key, branch);
    }

    /// Buffer one statement's encoded IR into an ACTIVE branch.
    pub fn stage(self: *XaManager, xid: []const u8, encoded: []const u8) Error!void {
        self.lock();
        defer self.mutex.unlock();
        const branch = self.map.get(xid) orelse return Error.XaBranchUnknown;
        if (branch.state != .active) return Error.XaProtocol;
        const a = branch.arena.allocator();
        const copy = try a.dupe(u8, encoded);
        try branch.stmts.append(a, copy);
    }

    pub fn end(self: *XaManager, xid: []const u8) Error!void {
        self.lock();
        defer self.mutex.unlock();
        const branch = self.map.get(xid) orelse return Error.XaBranchUnknown;
        if (branch.state != .active) return Error.XaProtocol;
        branch.state = .ended;
    }

    pub fn prepare(self: *XaManager, xid: []const u8) Error!void {
        self.lock();
        defer self.mutex.unlock();
        const branch = self.map.get(xid) orelse return Error.XaBranchUnknown;
        if (branch.state != .ended) return Error.XaProtocol;
        branch.state = .prepared;
    }

    /// Detach a branch for COMMIT. Returns null when the xid is absent — an
    /// already-committed xid re-committed by a Flink retry (idempotent no-op).
    /// The caller applies `branch.stmts` then calls `finishCommit`.
    pub fn takeForCommit(self: *XaManager, xid: []const u8) ?*Branch {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.map.fetchRemove(xid) orelse return null;
        self.allocator.free(entry.key);
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
};

test "xa branch lifecycle: begin/stage/end/prepare/commit + idempotent" {
    const testing = std.testing;
    var mgr = XaManager.init(testing.allocator);
    defer mgr.deinit();

    try mgr.begin("x1");
    try testing.expectError(Error.XaBranchExists, mgr.begin("x1"));
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
    mgr.finishCommit(branch);

    // Idempotent: re-commit of a gone xid is a no-op (null).
    try testing.expect(mgr.takeForCommit("x1") == null);
}

test "xa rollback discards" {
    const testing = std.testing;
    var mgr = XaManager.init(testing.allocator);
    defer mgr.deinit();
    try mgr.begin("r1");
    try mgr.stage("r1", &.{9});
    mgr.rollback("r1");
    try testing.expect(mgr.takeForCommit("r1") == null);
}
