//! Open-addressing group tables for the hash Aggregate's key→gid map, built
//! to support a software-prefetch probe pipeline. The Aggregate's accumulator
//! state (`gstate`), per-group key storage (`gkeys` / `gkeys_int`), and emit
//! pass are unchanged — these tables only replace the `key→gid` lookup that
//! sits in the accumulate inner loop.
//!
//! Two variants:
//!   * `ByteGroupTable` — compound/string/mixed keys. Slots hold
//!     `{ hash, gid }`; the key bytes live in the operator's `gkeys` arena
//!     (indexed by gid). A probe compares the stored hash first, then
//!     `std.mem.eql` on the bytes only on a hash collision.
//!   * `IntGroupTable` — all-fixed-width-integer keys packed into a `u128`.
//!     Slots hold `{ key, gid }` inline; comparison is a single `u128` ==.
//!
//! Both use power-of-two capacity, linear probing, a `gid == EMPTY` sentinel
//! (so every key value — including 0 and all-ones — is storable), and grow by
//! doubling at a 0.75 load factor. The Aggregate presizes from the cardinality
//! estimate, so a grow rarely fires mid-run; when it does, the operator grows
//! *before* probing a batch, keeping every slot address stable across the
//! batch's prefetch look-ahead.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// gid sentinel marking an empty slot. `n_groups` can never reach this value
/// (the table is capped far below 2^32-1 entries by the row limit), so it is a
/// safe "no group here" marker independent of the key bits.
pub const EMPTY: u32 = std.math.maxInt(u32);

/// Result of a getOrPut: the group id plus whether it already existed. On a new
/// group the caller assigns the next gid and writes it back via `commit`.
pub const Probe = struct {
    /// Index of the resolved slot (matching group, or the first empty slot on
    /// the probe chain when the group is new).
    slot: usize,
    found: bool,
    /// Valid only when `found` — the existing group's id.
    gid: u32,
};

/// 64-bit integer mixer (splitmix64 finalizer). Used to hash the packed u128
/// key (two mixed halves XOR-folded) — cheap, no byte serialization, and
/// scatters the low-entropy compound keys that integer group columns produce.
inline fn mix64(x: u64) u64 {
    var z = x;
    z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
    return z ^ (z >> 31);
}

pub inline fn hashU128(key: u128) u64 {
    const lo: u64 = @truncate(key);
    const hi: u64 = @truncate(key >> 64);
    return mix64(lo ^ mix64(hi));
}

/// `ByteGroupTable` slot. `gid == EMPTY` ⟺ unoccupied; the key bytes for an
/// occupied slot live in the operator's `gkeys[gid]`.
const ByteSlot = struct {
    hash: u64,
    gid: u32,
};

pub const ByteGroupTable = struct {
    slots: []ByteSlot,
    mask: usize,
    len: usize,

    /// Build a table sized to hold `expected` groups under the load factor
    /// without growing. `expected == 0` yields a small initial table.
    pub fn init(allocator: Allocator, expected: usize) !ByteGroupTable {
        const cap = capacityFor(expected);
        const slots = try allocator.alloc(ByteSlot, cap);
        for (slots) |*s| s.gid = EMPTY;
        return .{ .slots = slots, .mask = cap - 1, .len = 0 };
    }

    pub inline fn bucketOf(self: ByteGroupTable, hash: u64) usize {
        return @as(usize, @truncate(hash)) & self.mask;
    }

    pub inline fn slotAddr(self: *ByteGroupTable, bucket: usize) *ByteSlot {
        return &self.slots[bucket];
    }

    /// Locate `key` (with precomputed `hash`). On a hit returns its gid; on a
    /// miss returns the first empty slot on the probe chain so the caller can
    /// `commit` a freshly-assigned gid there. `gkeys[gid]` supplies the bytes
    /// for the `eql` tie-break on a hash collision.
    pub fn getOrPut(self: *ByteGroupTable, hash: u64, key: []const u8, gkeys: []const []const u8) Probe {
        var i = self.bucketOf(hash);
        while (true) : (i = (i + 1) & self.mask) {
            const s = self.slots[i];
            if (s.gid == EMPTY) return .{ .slot = i, .found = false, .gid = 0 };
            if (s.hash == hash and std.mem.eql(u8, gkeys[s.gid], key)) {
                return .{ .slot = i, .found = true, .gid = s.gid };
            }
        }
    }

    /// Write a new group's `gid` into the slot a prior `getOrPut` returned.
    pub fn commit(self: *ByteGroupTable, slot: usize, hash: u64, gid: u32) void {
        self.slots[slot] = .{ .hash = hash, .gid = gid };
        self.len += 1;
    }

    /// True if inserting `additional` new groups would push past the load
    /// factor. Used to grow before a batch so slot addresses stay stable for
    /// the batch's prefetch look-ahead.
    pub fn needsGrow(self: ByteGroupTable, additional: usize) bool {
        return (self.len + additional) * 4 >= self.slots.len * 3;
    }

    /// Double (or more) capacity and re-insert every live entry. Re-insertion
    /// needs no key bytes — the stored hash determines the new bucket and gids
    /// are unique, so there are no equality checks.
    pub fn grow(self: *ByteGroupTable, allocator: Allocator, additional: usize) !void {
        var new_cap = self.slots.len;
        while ((self.len + additional) * 4 >= new_cap * 3) new_cap *= 2;
        const slots = try allocator.alloc(ByteSlot, new_cap);
        for (slots) |*s| s.gid = EMPTY;
        const new_mask = new_cap - 1;
        for (self.slots) |old| {
            if (old.gid == EMPTY) continue;
            var i = @as(usize, @truncate(old.hash)) & new_mask;
            while (slots[i].gid != EMPTY) : (i = (i + 1) & new_mask) {}
            slots[i] = old;
        }
        allocator.free(self.slots);
        self.slots = slots;
        self.mask = new_mask;
    }

    pub fn deinit(self: *ByteGroupTable, allocator: Allocator) void {
        allocator.free(self.slots);
        self.* = undefined;
    }
};

/// `IntGroupTable` slot. `gid == EMPTY` ⟺ unoccupied. The packed key is stored
/// inline; with the sentinel on `gid`, every `u128` key value is storable.
const IntSlot = struct {
    key: u128,
    gid: u32,
};

pub const IntGroupTable = struct {
    slots: []IntSlot,
    mask: usize,
    len: usize,

    pub fn init(allocator: Allocator, expected: usize) !IntGroupTable {
        const cap = capacityFor(expected);
        const slots = try allocator.alloc(IntSlot, cap);
        for (slots) |*s| s.gid = EMPTY;
        return .{ .slots = slots, .mask = cap - 1, .len = 0 };
    }

    pub inline fn bucketOf(self: IntGroupTable, hash: u64) usize {
        return @as(usize, @truncate(hash)) & self.mask;
    }

    pub inline fn slotAddr(self: *IntGroupTable, bucket: usize) *IntSlot {
        return &self.slots[bucket];
    }

    /// Locate the packed `key` (with precomputed `hash`). The key is inline, so
    /// a hit is a single `u128` compare — no out-of-table indirection.
    pub fn getOrPut(self: *IntGroupTable, hash: u64, key: u128) Probe {
        var i = self.bucketOf(hash);
        while (true) : (i = (i + 1) & self.mask) {
            const s = self.slots[i];
            if (s.gid == EMPTY) return .{ .slot = i, .found = false, .gid = 0 };
            if (s.key == key) return .{ .slot = i, .found = true, .gid = s.gid };
        }
    }

    pub fn commit(self: *IntGroupTable, slot: usize, key: u128, gid: u32) void {
        self.slots[slot] = .{ .key = key, .gid = gid };
        self.len += 1;
    }

    pub fn needsGrow(self: IntGroupTable, additional: usize) bool {
        return (self.len + additional) * 4 >= self.slots.len * 3;
    }

    pub fn grow(self: *IntGroupTable, allocator: Allocator, additional: usize) !void {
        var new_cap = self.slots.len;
        while ((self.len + additional) * 4 >= new_cap * 3) new_cap *= 2;
        const slots = try allocator.alloc(IntSlot, new_cap);
        for (slots) |*s| s.gid = EMPTY;
        const new_mask = new_cap - 1;
        for (self.slots) |old| {
            if (old.gid == EMPTY) continue;
            var i = @as(usize, @truncate(hashU128(old.key))) & new_mask;
            while (slots[i].gid != EMPTY) : (i = (i + 1) & new_mask) {}
            slots[i] = old;
        }
        allocator.free(self.slots);
        self.slots = slots;
        self.mask = new_mask;
    }

    pub fn deinit(self: *IntGroupTable, allocator: Allocator) void {
        allocator.free(self.slots);
        self.* = undefined;
    }
};

/// Smallest power-of-two capacity that holds `expected` entries under the 0.75
/// load factor, floored at 16. `expected * 4 / 3` is the minimum live-capacity;
/// round it up to the next power of two.
fn capacityFor(expected: usize) usize {
    const need = (expected *| 4) / 3 + 1;
    var cap: usize = 16;
    while (cap < need) cap *= 2;
    return cap;
}

test "capacityFor rounds up to power of two above load factor" {
    try std.testing.expectEqual(@as(usize, 16), capacityFor(0));
    try std.testing.expectEqual(@as(usize, 16), capacityFor(10));
    try std.testing.expectEqual(@as(usize, 32), capacityFor(13));
    try std.testing.expect(capacityFor(1000) >= 1000 * 4 / 3);
    try std.testing.expect(std.math.isPowerOfTwo(capacityFor(1_000_000)));
}

test "IntGroupTable getOrPut handles key 0 and all-ones" {
    const allocator = std.testing.allocator;
    var t = try IntGroupTable.init(allocator, 8);
    defer t.deinit(allocator);

    const keys = [_]u128{ 0, std.math.maxInt(u128), 1, 42, std.math.maxInt(u128) - 1 };
    var next_gid: u32 = 0;
    for (keys) |k| {
        const p = t.getOrPut(hashU128(k), k);
        try std.testing.expect(!p.found);
        t.commit(p.slot, k, next_gid);
        next_gid += 1;
    }
    try std.testing.expectEqual(@as(usize, keys.len), t.len);
    // Re-probing returns the same gids, in insertion order.
    for (keys, 0..) |k, i| {
        const p = t.getOrPut(hashU128(k), k);
        try std.testing.expect(p.found);
        try std.testing.expectEqual(@as(u32, @intCast(i)), p.gid);
    }
}

test "IntGroupTable grows and preserves all entries" {
    const allocator = std.testing.allocator;
    var t = try IntGroupTable.init(allocator, 0);
    defer t.deinit(allocator);

    const n: u128 = 5000;
    var k: u128 = 0;
    var gid: u32 = 0;
    while (k < n) : (k += 1) {
        if (t.needsGrow(1)) try t.grow(allocator, 1);
        const key = k *% 0x9E3779B97F4A7C15; // spread keys out
        const p = t.getOrPut(hashU128(key), key);
        try std.testing.expect(!p.found);
        t.commit(p.slot, key, gid);
        gid += 1;
    }
    try std.testing.expectEqual(@as(usize, n), t.len);
    k = 0;
    gid = 0;
    while (k < n) : (k += 1) {
        const key = k *% 0x9E3779B97F4A7C15;
        const p = t.getOrPut(hashU128(key), key);
        try std.testing.expect(p.found);
        try std.testing.expectEqual(gid, p.gid);
        gid += 1;
    }
}

test "ByteGroupTable getOrPut + grow with external keys" {
    const allocator = std.testing.allocator;
    var t = try ByteGroupTable.init(allocator, 0);
    defer t.deinit(allocator);

    var gkeys: std.ArrayList([]const u8) = .empty;
    defer {
        for (gkeys.items) |g| allocator.free(g);
        gkeys.deinit(allocator);
    }

    const n: usize = 3000;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "group-{d}", .{i}) catch unreachable;
        const h = std.hash.Wyhash.hash(0, key);
        const p = t.getOrPut(h, key, gkeys.items);
        try std.testing.expect(!p.found);
        const dup = try allocator.dupe(u8, key);
        try gkeys.append(allocator, dup);
        if (t.needsGrow(1)) try t.grow(allocator, 1);
        // re-resolve the slot after a possible grow before committing
        const p2 = t.getOrPut(h, key, gkeys.items);
        t.commit(p2.slot, h, @intCast(i));
    }
    try std.testing.expectEqual(@as(usize, n), t.len);
    i = 0;
    while (i < n) : (i += 1) {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "group-{d}", .{i}) catch unreachable;
        const h = std.hash.Wyhash.hash(0, key);
        const p = t.getOrPut(h, key, gkeys.items);
        try std.testing.expect(p.found);
        try std.testing.expectEqual(@as(u32, @intCast(i)), p.gid);
    }
}
