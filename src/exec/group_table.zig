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
    /// When non-zero, the first `grow` jumps straight to (at least) this slot
    /// capacity instead of doubling. The Aggregate sets it to `capacityFor` its
    /// provable group-count ceiling, so one grow off the modest initial size
    /// reaches a table that can hold every group without growing again.
    grow_target: usize = 0,

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
        new_cap = @max(new_cap, self.grow_target);
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

/// All-fixed-width-integer group keys pack into a `u128` (column 0 in the low
/// bits). The probe is memory-bound, and most keys are far narrower than 128
/// bits, so the slot is split into three size tiers routed by the key's summed
/// column bit width. A narrower slot fits more entries per cache line, so fewer
/// bytes move per probe:
///
///   * `max_bits <= 32`  → 8-byte slot `{ key32: u32, gid: u32 }`
///   * `max_bits <= 96`  → 16-byte slot `{ lo: u64, hi: u32, gid: u32 }`
///     (a *split* key — `@sizeOf(u96)` rounds to 16 and a `{u96,u32}` slot
///     balloons to 32, defeating the win. The flat 3-field shape is exactly 16
///     bytes and holds 96 key bits + the gid.)
///   * `max_bits <= 128` → 16+ byte slot `{ key: u128, gid: u32 }` (`@sizeOf`
///     rounds to 32; the historical full-width tier)
///
/// Every tier takes a `u128` key in `getOrPut`/`commit` and stores it
/// truncated/split losslessly (the layout summed the widths, so the key fits
/// its tier by construction). The hash is consistent between insert, probe, and
/// grow-rehash within a tier. The 128-bit tier is exported as `IntGroupTable`
/// for callers (e.g. the combined COUNT(DISTINCT) set) that always need the
/// full width.
pub fn IntKeyTable(comptime max_bits: u16) type {
    const tier32 = max_bits <= 32;
    const tier96 = !tier32 and max_bits <= 96;

    const Slot = if (tier32)
        struct { key32: u32, gid: u32 }
    else if (tier96)
        struct { lo: u64, hi: u32, gid: u32 }
    else
        struct { key: u128, gid: u32 };

    return struct {
        const Self = @This();

        slots: []Slot,
        mask: usize,
        len: usize,
        /// When non-zero, the first `grow` jumps straight to (at least) this slot
        /// capacity instead of doubling. The Aggregate sets it to `capacityFor`
        /// its provable group-count ceiling, so one grow off the modest initial
        /// size reaches a table that can hold every group without growing again.
        grow_target: usize = 0,

        /// Hash a full-width `u128` key for this tier. Callers precompute it for
        /// the prefetch-pipelined probe; it must match `hashStored` (used by
        /// grow-rehash) for the same key bits, so both route through here.
        pub inline fn hashKey(key: u128) u64 {
            return hashSlotKey(key);
        }

        /// Hash the truncated/split key. Consistent across insert, probe, and
        /// grow within a tier (each derives it from the same stored bits).
        inline fn hashSlotKey(key: u128) u64 {
            if (tier32) {
                return mix64(@as(u64, @as(u32, @truncate(key))));
            } else if (tier96) {
                const lo: u64 = @truncate(key);
                const hi: u32 = @truncate(key >> 64);
                return mix64(lo ^ mix64(@as(u64, hi)));
            } else {
                return hashU128(key);
            }
        }

        inline fn hashStored(s: Slot) u64 {
            if (tier32) {
                return mix64(@as(u64, s.key32));
            } else if (tier96) {
                return mix64(s.lo ^ mix64(@as(u64, s.hi)));
            } else {
                return hashU128(s.key);
            }
        }

        inline fn storeKey(key: u128, gid: u32) Slot {
            if (tier32) {
                return .{ .key32 = @truncate(key), .gid = gid };
            } else if (tier96) {
                return .{ .lo = @truncate(key), .hi = @truncate(key >> 64), .gid = gid };
            } else {
                return .{ .key = key, .gid = gid };
            }
        }

        inline fn keyEq(s: Slot, key: u128) bool {
            if (tier32) {
                return s.key32 == @as(u32, @truncate(key));
            } else if (tier96) {
                return s.lo == @as(u64, @truncate(key)) and s.hi == @as(u32, @truncate(key >> 64));
            } else {
                return s.key == key;
            }
        }

        pub fn init(allocator: Allocator, expected: usize) !Self {
            const cap = capacityFor(expected);
            const slots = try allocator.alloc(Slot, cap);
            for (slots) |*s| s.gid = EMPTY;
            return .{ .slots = slots, .mask = cap - 1, .len = 0 };
        }

        pub inline fn bucketOf(self: Self, hash: u64) usize {
            return @as(usize, @truncate(hash)) & self.mask;
        }

        pub inline fn slotAddr(self: *Self, bucket: usize) *Slot {
            return &self.slots[bucket];
        }

        /// Locate the packed `key` (with precomputed `hash`). The key is inline,
        /// so a hit is a single (tier-width) compare — no out-of-table indirection.
        pub fn getOrPut(self: *Self, hash: u64, key: u128) Probe {
            var i = self.bucketOf(hash);
            while (true) : (i = (i + 1) & self.mask) {
                const s = self.slots[i];
                if (s.gid == EMPTY) return .{ .slot = i, .found = false, .gid = 0 };
                if (keyEq(s, key)) return .{ .slot = i, .found = true, .gid = s.gid };
            }
        }

        pub fn commit(self: *Self, slot: usize, key: u128, gid: u32) void {
            self.slots[slot] = storeKey(key, gid);
            self.len += 1;
        }

        pub fn needsGrow(self: Self, additional: usize) bool {
            return (self.len + additional) * 4 >= self.slots.len * 3;
        }

        pub fn grow(self: *Self, allocator: Allocator, additional: usize) !void {
            var new_cap = self.slots.len;
            while ((self.len + additional) * 4 >= new_cap * 3) new_cap *= 2;
            new_cap = @max(new_cap, self.grow_target);
            const slots = try allocator.alloc(Slot, new_cap);
            for (slots) |*s| s.gid = EMPTY;
            const new_mask = new_cap - 1;
            for (self.slots) |old| {
                if (old.gid == EMPTY) continue;
                var i = @as(usize, @truncate(hashStored(old))) & new_mask;
                while (slots[i].gid != EMPTY) : (i = (i + 1) & new_mask) {}
                slots[i] = old;
            }
            allocator.free(self.slots);
            self.slots = slots;
            self.mask = new_mask;
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            allocator.free(self.slots);
            self.* = undefined;
        }
    };
}

/// Full-width (128-bit) integer group table. The combined COUNT(DISTINCT int)
/// set keys on a `pack(gid, value)` that can use the full width, so it always
/// takes this tier.
pub const IntGroupTable = IntKeyTable(128);

/// Open-addressing membership set over u64 keys, for exact COUNT(DISTINCT) on
/// integer columns ≤64 bits (the value bits zero-extend injectively into a
/// u64). Key-only 8-byte slots — no gid tax, so the slot is half (or a quarter)
/// the width of the equivalent `IntKeyTable` tier and far fewer bytes move per
/// probe, which dominates this memory-bound insert. `SENTINEL` marks an empty
/// slot; the one real key that collides with it (a 64-bit column's all-ones
/// value, e.g. `bigint` -1) is counted separately via `has_sentinel`. Built for
/// a software-prefetch insert pipeline: `ensureFor` grows before a batch so slot
/// addresses stay stable across the look-ahead window, then the caller
/// interleaves `prefetch(look-ahead key)` with `insert(current key)`.
pub const DistinctU64Set = struct {
    /// Power-of-two-sized open-addressing table; `&.{}` ⟺ not yet allocated.
    /// The probe mask is derived as `slots.len - 1` rather than stored, keeping
    /// the struct at 24 bytes so it fits the Aggregate's `AccState` union
    /// without widening it — a wider union would bloat the per-group `gstate`
    /// array (5M entries for a high-card GROUP BY).
    slots: []u64,
    len: u32,
    has_sentinel: bool,

    pub const SENTINEL: u64 = std.math.maxInt(u64);

    pub const empty: DistinctU64Set = .{ .slots = &.{}, .len = 0, .has_sentinel = false };

    pub fn init(allocator: Allocator, expected: usize) !DistinctU64Set {
        const cap = capacityFor(expected);
        const slots = try allocator.alloc(u64, cap);
        @memset(slots, SENTINEL);
        return .{ .slots = slots, .len = 0, .has_sentinel = false };
    }

    pub fn deinit(self: *DistinctU64Set, allocator: Allocator) void {
        if (self.slots.len != 0) allocator.free(self.slots);
        self.* = undefined;
    }

    /// Reserve room for `additional` more keys so no grow fires mid-batch (slot
    /// addresses must stay stable across the caller's prefetch look-ahead).
    /// Allocates lazily on first use from the `empty` state.
    pub fn ensureFor(self: *DistinctU64Set, allocator: Allocator, additional: usize) !void {
        if (self.slots.len == 0) {
            const cap = capacityFor(additional);
            const slots = try allocator.alloc(u64, cap);
            @memset(slots, SENTINEL);
            self.slots = slots;
            return;
        }
        if ((@as(usize, self.len) + additional) * 4 >= self.slots.len * 3) try self.grow(allocator, additional);
    }

    fn grow(self: *DistinctU64Set, allocator: Allocator, additional: usize) !void {
        var new_cap = self.slots.len;
        while ((@as(usize, self.len) + additional) * 4 >= new_cap * 3) new_cap *= 2;
        const slots = try allocator.alloc(u64, new_cap);
        @memset(slots, SENTINEL);
        const new_mask = new_cap - 1;
        for (self.slots) |k| {
            if (k == SENTINEL) continue;
            var i = @as(usize, @truncate(mix64(k))) & new_mask;
            while (slots[i] != SENTINEL) : (i = (i + 1) & new_mask) {}
            slots[i] = k;
        }
        allocator.free(self.slots);
        self.slots = slots;
    }

    /// `@prefetch` the slot a future `insert(key)` will probe first.
    pub inline fn prefetch(self: *DistinctU64Set, key: u64) void {
        @prefetch(&self.slots[@as(usize, @truncate(mix64(key))) & (self.slots.len - 1)], .{ .rw = .write, .locality = 1 });
    }

    /// Insert `key`; no-op if already present. The caller must have reserved
    /// space via `ensureFor` for the whole batch so no grow fires here.
    pub inline fn insert(self: *DistinctU64Set, key: u64) void {
        if (key == SENTINEL) {
            self.has_sentinel = true;
            return;
        }
        const mask = self.slots.len - 1;
        var b = @as(usize, @truncate(mix64(key))) & mask;
        while (true) : (b = (b + 1) & mask) {
            const s = self.slots[b];
            if (s == SENTINEL) {
                self.slots[b] = key;
                self.len += 1;
                return;
            }
            if (s == key) return;
        }
    }

    pub fn count(self: DistinctU64Set) usize {
        return @as(usize, self.len) + @intFromBool(self.has_sentinel);
    }
};

/// Open-addressing table that holds the COUNT(*) *inside* the slot, for the
/// `GROUP BY <single int col> … COUNT(*)` fast path. The classic two-array
/// shape (a `{key,gid}` group table plus a separate per-gid count array) costs
/// two random cache misses per row — one to resolve the gid, one to bump the
/// count. Folding the count into a `{ key, count }` slot collapses that to a
/// single miss per row: one probe finds (or creates) the slot and bumps its
/// count in place. `SENTINEL` (all-ones u64) marks an empty slot; the one real
/// key that collides with it (a 64-bit column's all-ones value, e.g. bigint -1)
/// is counted separately via `has_sentinel` / `sentinel_count`. Built for the
/// same software-prefetch insert pipeline as `DistinctU64Set`: `ensureFor` grows
/// before a batch so slot addresses stay stable across the look-ahead window,
/// then the caller interleaves `prefetch(look-ahead key)` with `insert(current
/// key)`. The Aggregate later walks the occupied slots to lower each `{key,
/// count}` into its `gkeys_int` / `gstate` arrays, so emit + top-k are untouched.
pub const CountSlotTable = struct {
    const Slot = struct { key: u64, count: u64 };

    /// Power-of-two-sized open-addressing table; `&.{}` ⟺ not yet allocated.
    /// The probe mask is derived as `slots.len - 1` rather than stored.
    slots: []Slot,
    /// Distinct non-sentinel keys (occupied slots).
    len: u32,
    has_sentinel: bool,
    sentinel_count: u64,

    pub const SENTINEL: u64 = std.math.maxInt(u64);

    pub const empty: CountSlotTable = .{ .slots = &.{}, .len = 0, .has_sentinel = false, .sentinel_count = 0 };

    pub fn init(allocator: Allocator, expected: usize) !CountSlotTable {
        const cap = capacityFor(expected);
        const slots = try allocator.alloc(Slot, cap);
        for (slots) |*s| s.key = SENTINEL;
        return .{ .slots = slots, .len = 0, .has_sentinel = false, .sentinel_count = 0 };
    }

    pub fn deinit(self: *CountSlotTable, allocator: Allocator) void {
        if (self.slots.len != 0) allocator.free(self.slots);
        self.* = undefined;
    }

    /// Reserve room for `additional` more distinct keys so no grow fires
    /// mid-batch (slot addresses must stay stable across the caller's prefetch
    /// look-ahead). Allocates lazily on first use from the `empty` state.
    pub fn ensureFor(self: *CountSlotTable, allocator: Allocator, additional: usize) !void {
        if (self.slots.len == 0) {
            const cap = capacityFor(additional);
            const slots = try allocator.alloc(Slot, cap);
            for (slots) |*s| s.key = SENTINEL;
            self.slots = slots;
            return;
        }
        if ((@as(usize, self.len) + additional) * 4 >= self.slots.len * 3) try self.grow(allocator, additional);
    }

    fn grow(self: *CountSlotTable, allocator: Allocator, additional: usize) !void {
        var new_cap = self.slots.len;
        while ((@as(usize, self.len) + additional) * 4 >= new_cap * 3) new_cap *= 2;
        const slots = try allocator.alloc(Slot, new_cap);
        for (slots) |*s| s.key = SENTINEL;
        const new_mask = new_cap - 1;
        for (self.slots) |old| {
            if (old.key == SENTINEL) continue;
            var i = @as(usize, @truncate(mix64(old.key))) & new_mask;
            while (slots[i].key != SENTINEL) : (i = (i + 1) & new_mask) {}
            slots[i] = old;
        }
        allocator.free(self.slots);
        self.slots = slots;
    }

    /// `@prefetch` the slot a future `insert(key)` will probe first.
    pub inline fn prefetch(self: *CountSlotTable, key: u64) void {
        @prefetch(&self.slots[@as(usize, @truncate(mix64(key))) & (self.slots.len - 1)], .{ .rw = .write, .locality = 1 });
    }

    /// Bump `key`'s count in place, creating its slot at count 1 on first
    /// sighting. The caller must have reserved space via `ensureFor` for the
    /// whole batch so no grow fires here.
    pub inline fn insert(self: *CountSlotTable, key: u64) void {
        if (key == SENTINEL) {
            self.has_sentinel = true;
            self.sentinel_count += 1;
            return;
        }
        const mask = self.slots.len - 1;
        var b = @as(usize, @truncate(mix64(key))) & mask;
        while (true) : (b = (b + 1) & mask) {
            const s = &self.slots[b];
            if (s.key == SENTINEL) {
                s.* = .{ .key = key, .count = 1 };
                self.len += 1;
                return;
            }
            if (s.key == key) {
                s.count += 1;
                return;
            }
        }
    }

    pub fn count(self: CountSlotTable) usize {
        return @as(usize, self.len) + @intFromBool(self.has_sentinel);
    }
};

/// Smallest power-of-two capacity that holds `expected` entries under the 0.75
/// load factor, floored at 16. `expected * 4 / 3` is the minimum live-capacity;
/// round it up to the next power of two.
pub fn capacityFor(expected: usize) usize {
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

test "grow_target makes the first grow jump straight to the ceiling" {
    const allocator = std.testing.allocator;

    // Modest initial table; ceiling provably holds 10_000 entries. The first
    // grow must reach `grow_target` in one shot — not the doubling that fitting
    // the immediate `additional` alone would pick.
    const ceiling = capacityFor(10_000);
    var t = try IntGroupTable.init(allocator, 64);
    t.grow_target = ceiling;
    defer t.deinit(allocator);

    const start = t.slots.len;
    try std.testing.expect(start < ceiling);

    try t.grow(allocator, 1);
    try std.testing.expectEqual(ceiling, t.slots.len);

    // ByteGroupTable honors the target identically.
    var b = try ByteGroupTable.init(allocator, 64);
    b.grow_target = ceiling;
    defer b.deinit(allocator);
    try std.testing.expect(b.slots.len < ceiling);
    try b.grow(allocator, 1);
    try std.testing.expectEqual(ceiling, b.slots.len);

    // A grow whose immediate fit already exceeds the target keeps the larger
    // size (the target is a floor, never a cap that would lose entries).
    var t2 = try IntGroupTable.init(allocator, 64);
    t2.grow_target = capacityFor(100);
    defer t2.deinit(allocator);
    try t2.grow(allocator, 50_000);
    try std.testing.expect(t2.slots.len >= capacityFor(50_000));
}

test "IntKeyTable tiers store key 0 and the all-ones value within their width" {
    const allocator = std.testing.allocator;
    // (table type, the max key value that fits its width)
    const cases = .{
        .{ .Table = IntKeyTable(32), .full = @as(u128, std.math.maxInt(u32)) },
        .{ .Table = IntKeyTable(96), .full = (@as(u128, 1) << 96) - 1 },
        .{ .Table = IntKeyTable(128), .full = std.math.maxInt(u128) },
    };
    inline for (cases) |c| {
        var t = try c.Table.init(allocator, 8);
        defer t.deinit(allocator);

        const keys = [_]u128{ 0, c.full, 1, 42, c.full - 1, c.full >> 1 };
        var next_gid: u32 = 0;
        for (keys) |k| {
            const p = t.getOrPut(c.Table.hashKey(k), k);
            try std.testing.expect(!p.found);
            t.commit(p.slot, k, next_gid);
            next_gid += 1;
        }
        try std.testing.expectEqual(@as(usize, keys.len), t.len);
        for (keys, 0..) |k, i| {
            const p = t.getOrPut(c.Table.hashKey(k), k);
            try std.testing.expect(p.found);
            try std.testing.expectEqual(@as(u32, @intCast(i)), p.gid);
        }
    }
}

test "IntKeyTable tiers grow and preserve every gid" {
    const allocator = std.testing.allocator;
    const cases = .{
        // mask keeps each tier's keys inside its width so the truncating store is lossless.
        .{ .Table = IntKeyTable(32), .mask = @as(u128, std.math.maxInt(u32)) },
        .{ .Table = IntKeyTable(96), .mask = (@as(u128, 1) << 96) - 1 },
        .{ .Table = IntKeyTable(128), .mask = std.math.maxInt(u128) },
    };
    inline for (cases) |c| {
        var t = try c.Table.init(allocator, 0);
        defer t.deinit(allocator);

        const n: u32 = 5000;
        var i: u32 = 0;
        while (i < n) : (i += 1) {
            if (t.needsGrow(1)) try t.grow(allocator, 1);
            const key = (@as(u128, i) *% 0x9E3779B97F4A7C15) & c.mask;
            const p = t.getOrPut(c.Table.hashKey(key), key);
            // Distinct i may collide after masking on the narrow tiers; only
            // commit genuinely-new keys so gids stay 1:1 with stored keys.
            if (!p.found) t.commit(p.slot, key, i);
        }
        i = 0;
        while (i < n) : (i += 1) {
            const key = (@as(u128, i) *% 0x9E3779B97F4A7C15) & c.mask;
            const p = t.getOrPut(c.Table.hashKey(key), key);
            try std.testing.expect(p.found);
        }
    }
}

test "DistinctU64Set counts distinct keys, handles the sentinel value, grows" {
    const allocator = std.testing.allocator;
    var set = DistinctU64Set.empty;
    defer set.deinit(allocator);

    // 5000 distinct values, each inserted 3× — count must be 5000.
    const n: u64 = 5000;
    var pass: usize = 0;
    while (pass < 3) : (pass += 1) {
        try set.ensureFor(allocator, n);
        var i: u64 = 0;
        while (i < n) : (i += 1) set.insert((i +% 1) *% 0x9E3779B97F4A7C15);
    }
    try std.testing.expectEqual(@as(usize, n), set.count());

    // The sentinel value (all-ones) is counted exactly once via the flag.
    try std.testing.expect(!set.has_sentinel);
    try set.ensureFor(allocator, 2);
    set.insert(DistinctU64Set.SENTINEL);
    set.insert(DistinctU64Set.SENTINEL);
    try std.testing.expect(set.has_sentinel);
    try std.testing.expectEqual(@as(usize, n + 1), set.count());

    // Key 0 is a normal storable key (distinct from empty slots).
    try set.ensureFor(allocator, 1);
    set.insert(0);
    try std.testing.expectEqual(@as(usize, n + 2), set.count());
}

test "CountSlotTable counts repeated keys, the sentinel value, and grows" {
    const allocator = std.testing.allocator;
    var t = CountSlotTable.empty;
    defer t.deinit(allocator);

    // 5000 distinct keys, key i inserted (i % 4) + 1 times. Spread the keys so
    // the table grows several times across the batches.
    const n: u64 = 5000;
    var i: u64 = 0;
    while (i < n) : (i += 1) {
        const reps: u64 = (i % 4) + 1;
        try t.ensureFor(allocator, 1);
        var r: u64 = 0;
        while (r < reps) : (r += 1) t.insert((i +% 1) *% 0x9E3779B97F4A7C15);
    }
    try std.testing.expectEqual(@as(u32, n), t.len);
    try std.testing.expectEqual(@as(usize, n), t.count());

    // Each key's count matches its repeat schedule.
    i = 0;
    while (i < n) : (i += 1) {
        const key = (i +% 1) *% 0x9E3779B97F4A7C15;
        const mask = t.slots.len - 1;
        var b = @as(usize, @truncate(mix64(key))) & mask;
        const found = while (true) : (b = (b + 1) & mask) {
            if (t.slots[b].key == key) break t.slots[b].count;
            if (t.slots[b].key == CountSlotTable.SENTINEL) break @as(?u64, null);
        };
        try std.testing.expectEqual(@as(?u64, (i % 4) + 1), found);
    }

    // The sentinel value (all-ones) is tracked once via the flag, counted thrice.
    try std.testing.expect(!t.has_sentinel);
    try t.ensureFor(allocator, 1);
    t.insert(CountSlotTable.SENTINEL);
    t.insert(CountSlotTable.SENTINEL);
    t.insert(CountSlotTable.SENTINEL);
    try std.testing.expect(t.has_sentinel);
    try std.testing.expectEqual(@as(u64, 3), t.sentinel_count);
    try std.testing.expectEqual(@as(usize, n + 1), t.count());

    // Key 0 is a normal storable key (distinct from empty slots).
    try t.ensureFor(allocator, 1);
    t.insert(0);
    t.insert(0);
    try std.testing.expectEqual(@as(usize, n + 2), t.count());
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
