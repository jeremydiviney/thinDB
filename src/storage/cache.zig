//! LRU byte-cache for decompressed column blocks.
//!
//! Keyed by `(segment_id, row_group_idx, column_idx)`. Value is the raw,
//! decompressed (but not decoded) bytes of one column block. A cache hit
//! still pays a cheap decode step but skips the expensive flate decompress.
//!
//! Bounded by total cached bytes (`capacity_bytes`). Single-threaded; no
//! refcounting needed — entries are stable byte slices owned by the cache.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Key = struct {
    segment_id: u64,
    row_group_idx: u32,
    column_idx: u32,
};

pub const Cache = struct {
    allocator: Allocator,
    capacity_bytes: usize,
    current_bytes: usize = 0,

    map: std.AutoHashMapUnmanaged(Key, *Entry) = .empty,
    /// Doubly-linked LRU list. Head = most recently used; tail = LRU (next evict).
    head: ?*Entry = null,
    tail: ?*Entry = null,

    /// Stats (read-only, for benches and debugging).
    hits: u64 = 0,
    misses: u64 = 0,
    evictions: u64 = 0,

    pub const Entry = struct {
        key: Key,
        bytes: []u8,
        prev: ?*Entry,
        next: ?*Entry,
    };

    pub fn init(allocator: Allocator, capacity_bytes: usize) Cache {
        return .{
            .allocator = allocator,
            .capacity_bytes = capacity_bytes,
        };
    }

    pub fn deinit(self: *Cache) void {
        var cur = self.head;
        while (cur) |e| {
            const nxt = e.next;
            self.allocator.free(e.bytes);
            self.allocator.destroy(e);
            cur = nxt;
        }
        self.map.deinit(self.allocator);
        self.* = undefined;
    }

    /// Returns a borrowed byte slice if `key` is present. The slice is valid
    /// until the next call that may evict it (insert/get on a different key).
    /// In thinDB's single-threaded use, callers consume hits inline.
    pub fn get(self: *Cache, key: Key) ?[]const u8 {
        if (self.map.get(key)) |entry| {
            self.touch(entry);
            self.hits += 1;
            return entry.bytes;
        }
        self.misses += 1;
        return null;
    }

    /// Insert `bytes` (caller transfers ownership). The cache may immediately
    /// evict other entries to stay within the byte budget. If the new entry
    /// itself is larger than the budget, it is freed and not stored.
    pub fn put(self: *Cache, key: Key, bytes: []u8) !void {
        if (self.map.get(key) != null) {
            // Already present — free the new bytes; caller's responsibility.
            self.allocator.free(bytes);
            return;
        }
        if (bytes.len > self.capacity_bytes) {
            self.allocator.free(bytes);
            return;
        }

        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{ .key = key, .bytes = bytes, .prev = null, .next = null };

        try self.map.put(self.allocator, key, entry);
        errdefer _ = self.map.remove(key);

        self.linkHead(entry);
        self.current_bytes += bytes.len;
        try self.evictUntilWithinBudget();
    }

    fn evictUntilWithinBudget(self: *Cache) !void {
        while (self.current_bytes > self.capacity_bytes) {
            const victim = self.tail orelse break;
            self.unlink(victim);
            _ = self.map.remove(victim.key);
            self.current_bytes -= victim.bytes.len;
            self.allocator.free(victim.bytes);
            self.allocator.destroy(victim);
            self.evictions += 1;
        }
    }

    fn touch(self: *Cache, entry: *Entry) void {
        if (self.head == entry) return;
        self.unlink(entry);
        self.linkHead(entry);
    }

    fn linkHead(self: *Cache, entry: *Entry) void {
        entry.prev = null;
        entry.next = self.head;
        if (self.head) |h| h.prev = entry;
        self.head = entry;
        if (self.tail == null) self.tail = entry;
    }

    fn unlink(self: *Cache, entry: *Entry) void {
        if (entry.prev) |p| p.next = entry.next else self.head = entry.next;
        if (entry.next) |n| n.prev = entry.prev else self.tail = entry.prev;
        entry.prev = null;
        entry.next = null;
    }
};

// ---------- tests --------------------------------------------------------

test "cache get/put round-trip + hit counter" {
    const allocator = std.testing.allocator;
    var c: Cache = .init(allocator, 1024);
    defer c.deinit();

    const k = Key{ .segment_id = 1, .row_group_idx = 0, .column_idx = 0 };
    try std.testing.expect(c.get(k) == null);
    try std.testing.expectEqual(@as(u64, 1), c.misses);

    const buf = try allocator.dupe(u8, "hello");
    try c.put(k, buf);

    const got = c.get(k).?;
    try std.testing.expectEqualStrings("hello", got);
    try std.testing.expectEqual(@as(u64, 1), c.hits);
}

test "cache evicts oldest when over budget" {
    const allocator = std.testing.allocator;
    var c: Cache = .init(allocator, 30);
    defer c.deinit();

    // Three 10-byte entries — third pushes us over 30, evicting the first.
    const buf1 = try allocator.dupe(u8, "0123456789");
    try c.put(.{ .segment_id = 1, .row_group_idx = 0, .column_idx = 0 }, buf1);

    const buf2 = try allocator.dupe(u8, "abcdefghij");
    try c.put(.{ .segment_id = 1, .row_group_idx = 1, .column_idx = 0 }, buf2);

    const buf3 = try allocator.dupe(u8, "ABCDEFGHIJ");
    try c.put(.{ .segment_id = 1, .row_group_idx = 2, .column_idx = 0 }, buf3);

    // First should still be present (we just put it last; let me re-think test logic).
    // Actually buf1 was first. After 3 puts each 10 bytes, current_bytes = 30 which is
    // within budget (30 <= 30). No eviction yet. Add one more.
    const buf4 = try allocator.dupe(u8, "wxyz123456");
    try c.put(.{ .segment_id = 1, .row_group_idx = 3, .column_idx = 0 }, buf4);

    // Now 40 bytes; cap 30; evict from tail (LRU). buf1 was head most recently? No —
    // after puts in order, head=buf4, then buf3, buf2, buf1=tail. So buf1 should be evicted.
    try std.testing.expect(c.get(.{ .segment_id = 1, .row_group_idx = 0, .column_idx = 0 }) == null);
    try std.testing.expect(c.get(.{ .segment_id = 1, .row_group_idx = 3, .column_idx = 0 }) != null);
    try std.testing.expectEqual(@as(u64, 1), c.evictions);
}

test "cache LRU order updated on get" {
    const allocator = std.testing.allocator;
    var c: Cache = .init(allocator, 20);
    defer c.deinit();

    const k1 = Key{ .segment_id = 1, .row_group_idx = 0, .column_idx = 0 };
    const k2 = Key{ .segment_id = 1, .row_group_idx = 1, .column_idx = 0 };
    try c.put(k1, try allocator.dupe(u8, "0123456789"));
    try c.put(k2, try allocator.dupe(u8, "abcdefghij"));

    // Access k1 → moves it to head.
    _ = c.get(k1);

    // Now inserting another 10-byte entry: cap is 20, putting another 10 = 30; evict tail.
    // Tail should be k2 (because k1 was just touched).
    const k3 = Key{ .segment_id = 1, .row_group_idx = 2, .column_idx = 0 };
    try c.put(k3, try allocator.dupe(u8, "XXXXXXXXXX"));

    try std.testing.expect(c.get(k2) == null);
    try std.testing.expect(c.get(k1) != null);
    try std.testing.expect(c.get(k3) != null);
}

test "cache rejects entries larger than the whole budget" {
    const allocator = std.testing.allocator;
    var c: Cache = .init(allocator, 10);
    defer c.deinit();

    const big = try allocator.dupe(u8, "0123456789ABCDEF"); // 16 bytes > 10
    try c.put(.{ .segment_id = 1, .row_group_idx = 0, .column_idx = 0 }, big);

    try std.testing.expectEqual(@as(usize, 0), c.current_bytes);
    try std.testing.expect(c.get(.{ .segment_id = 1, .row_group_idx = 0, .column_idx = 0 }) == null);
}
