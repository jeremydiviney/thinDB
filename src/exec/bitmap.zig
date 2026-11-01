//! BITMAP values: sets of unsigned 64-bit integers, as StarRocks' BITMAP
//! type holds. In memory a `Bitmap` is a Roaring bitmap: values group into
//! chunks by their high 48 bits, and each chunk holds its values' low 16
//! bits in a container — a sorted array while it holds at most `ARRAY_MAX`
//! values, a 65,536-bit set past that.
//!
//! A stored value is StarRocks' serialized form, so `bitmap_to_binary`
//! gives and `bitmap_from_binary` takes the bytes StarRocks does. The first
//! byte names the layout (integers little-endian):
//!
//!   EMPTY     [0]
//!   SINGLE32  [1][u32 value]
//!   SINGLE64  [3][u64 value]
//!   SET       [10][u32 count][u64 value * count]
//!   BITMAP32  [2][portable Roaring]
//!   BITMAP64  [4][varint bucket count]([u32 high 32 bits][portable Roaring])*
//!
//! Portable Roaring is the format CRoaring and the Java library share, over
//! 32-bit values. `serialize` writes the layout StarRocks writes for a set
//! it builds value by value: EMPTY, a SINGLE, a SET of up to `SET_MAX`
//! values, then Roaring with each container run-length encoded where that
//! is smaller. Equal sets so have equal bytes. Reading accepts every
//! layout, since StarRocks writes a set built another way (a union, a
//! parsed string) in another one.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Error = error{BitmapInvalid} || Allocator.Error;

/// The most values a container keeps as a sorted array; past it the 8 KiB
/// bitset is smaller.
const ARRAY_MAX = 4096;
const BITSET_WORDS = 1024;
/// The most values StarRocks writes as a SET.
const SET_MAX = 32;
/// The values a `Builder` queues before merging them in.
const PENDING_MAX = 4096;

const Layout = enum(u8) {
    empty = 0,
    single32 = 1,
    bitmap32 = 2,
    single64 = 3,
    bitmap64 = 4,
    set = 10,
};

const COOKIE_NO_RUNS: u32 = 12346;
const COOKIE_RUNS: u32 = 12347;
/// Portable Roaring with run containers lists container offsets only from
/// this many containers up; without runs it always lists them.
const NO_OFFSET_THRESHOLD = 4;

pub const Op = enum { @"or", @"and", and_not, xor };

const Bitset = struct {
    words: [BITSET_WORDS]u64,
    card: u32,

    fn has(b: *const Bitset, low: u16) bool {
        return b.words[low >> 6] & bitOf(low) != 0;
    }

    fn set(b: *Bitset, low: u16) void {
        const w = &b.words[low >> 6];
        if (w.* & bitOf(low) != 0) return;
        w.* |= bitOf(low);
        b.card += 1;
    }

    fn unset(b: *Bitset, low: u16) void {
        const w = &b.words[low >> 6];
        if (w.* & bitOf(low) == 0) return;
        w.* &= ~bitOf(low);
        b.card -= 1;
    }

    fn recount(b: *Bitset) void {
        var n: u32 = 0;
        for (b.words) |w| n += @popCount(w);
        b.card = n;
    }

    /// The values at most `low`.
    fn rank(b: *const Bitset, low: u16) u32 {
        var n: u32 = 0;
        for (b.words[0 .. low >> 6]) |w| n += @popCount(w);
        const through: u6 = @truncate(low);
        const mask = if (through == 63) ~@as(u64, 0) else (@as(u64, 1) << (through + 1)) - 1;
        return n + @popCount(b.words[low >> 6] & mask);
    }
};

fn bitOf(low: u16) u64 {
    return @as(u64, 1) << @as(u6, @truncate(low));
}

fn lowerBound(comptime T: type, items: []const T, v: T) usize {
    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (items[mid] < v) lo = mid + 1 else hi = mid;
    }
    return lo;
}

/// A chunk's low 16 bits. Every operation keeps arrays at most `ARRAY_MAX`
/// long and bitsets past it, the split portable Roaring's readers assume.
const Container = union(enum) {
    array: std.ArrayList(u16),
    bitset: *Bitset,

    fn deinit(c: *Container, gpa: Allocator) void {
        switch (c.*) {
            .array => |*a| a.deinit(gpa),
            .bitset => |b| gpa.destroy(b),
        }
    }

    fn clone(c: Container, gpa: Allocator) Allocator.Error!Container {
        return switch (c) {
            .array => |a| .{ .array = .fromOwnedSlice(try gpa.dupe(u16, a.items)) },
            .bitset => |b| blk: {
                const copy = try gpa.create(Bitset);
                copy.* = b.*;
                break :blk .{ .bitset = copy };
            },
        };
    }

    fn cardinality(c: Container) u32 {
        return switch (c) {
            .array => |a| @intCast(a.items.len),
            .bitset => |b| b.card,
        };
    }

    fn contains(c: Container, low: u16) bool {
        return switch (c) {
            .array => |a| blk: {
                const at = lowerBound(u16, a.items, low);
                break :blk at < a.items.len and a.items[at] == low;
            },
            .bitset => |b| b.has(low),
        };
    }

    fn rank(c: Container, low: u16) u32 {
        return switch (c) {
            .array => |a| @intCast(lowerBound(u16, a.items, low) + @intFromBool(c.contains(low))),
            .bitset => |b| b.rank(low),
        };
    }

    fn min(c: Container) u16 {
        return switch (c) {
            .array => |a| a.items[0],
            .bitset => |b| for (b.words, 0..) |w, i| {
                if (w != 0) break @intCast(i * 64 + @ctz(w));
            } else unreachable,
        };
    }

    fn max(c: Container) u16 {
        return switch (c) {
            .array => |a| a.items[a.items.len - 1],
            .bitset => |b| blk: {
                var i: usize = BITSET_WORDS;
                while (i > 0) {
                    i -= 1;
                    if (b.words[i] != 0) break :blk @intCast(i * 64 + 63 - @clz(b.words[i]));
                }
                unreachable;
            },
        };
    }

    fn add(c: *Container, gpa: Allocator, low: u16) Allocator.Error!void {
        switch (c.*) {
            .bitset => |b| b.set(low),
            .array => |*a| {
                const at = if (a.items.len == 0 or a.items[a.items.len - 1] < low) a.items.len else lowerBound(u16, a.items, low);
                if (at < a.items.len and a.items[at] == low) return;
                if (a.items.len < ARRAY_MAX) return a.insert(gpa, at, low);
                const b = try bitsetOf(gpa, a.items);
                b.set(low);
                a.deinit(gpa);
                c.* = .{ .bitset = b };
            },
        }
    }

    fn remove(c: *Container, gpa: Allocator, low: u16) Allocator.Error!void {
        switch (c.*) {
            .array => |*a| {
                const at = lowerBound(u16, a.items, low);
                if (at < a.items.len and a.items[at] == low) _ = a.orderedRemove(at);
            },
            .bitset => |b| {
                b.unset(low);
                if (b.card <= ARRAY_MAX) c.* = try normalized(gpa, b);
            },
        }
    }

    /// ORs `other` in place; `other` is only read.
    fn unionInto(c: *Container, gpa: Allocator, other: Container) Allocator.Error!void {
        switch (c.*) {
            .bitset => |dst| switch (other) {
                .bitset => |src| {
                    for (&dst.words, src.words) |*d, s| d.* |= s;
                    dst.recount();
                },
                .array => |src| for (src.items) |v| dst.set(v),
            },
            .array => |*dst| switch (other) {
                .bitset => |src| {
                    const b = try gpa.create(Bitset);
                    b.* = src.*;
                    for (dst.items) |v| b.set(v);
                    dst.deinit(gpa);
                    c.* = .{ .bitset = b };
                },
                .array => |src| {
                    const n = unionCount(dst.items, src.items);
                    if (n > ARRAY_MAX) {
                        const b = try bitsetOf(gpa, dst.items);
                        for (src.items) |v| b.set(v);
                        dst.deinit(gpa);
                        c.* = .{ .bitset = b };
                        return;
                    }
                    var i = dst.items.len;
                    try dst.resize(gpa, n);
                    // Merging from the back writes each slot only after its
                    // old value has moved, so no copy is needed.
                    var j = src.items.len;
                    var k = n;
                    while (j > 0) {
                        k -= 1;
                        if (i > 0 and dst.items[i - 1] >= src.items[j - 1]) {
                            if (dst.items[i - 1] == src.items[j - 1]) j -= 1;
                            dst.items[k] = dst.items[i - 1];
                            i -= 1;
                        } else {
                            dst.items[k] = src.items[j - 1];
                            j -= 1;
                        }
                    }
                },
            },
        }
    }

    /// The bitset `c` covers: its own, or `buf` filled from its array.
    fn words(c: Container, buf: *[BITSET_WORDS]u64) *const [BITSET_WORDS]u64 {
        switch (c) {
            .bitset => |b| return &b.words,
            .array => |a| {
                @memset(buf, 0);
                for (a.items) |v| buf[v >> 6] |= bitOf(v);
                return buf;
            },
        }
    }

    fn iterator(c: Container) ContainerIterator {
        return .{ .container = c };
    }

    /// Runs of consecutive values.
    fn runCount(c: Container) u32 {
        switch (c) {
            .array => |a| {
                var n: u32 = 0;
                for (a.items, 0..) |v, i| n += @intFromBool(i == 0 or a.items[i - 1] + 1 != v);
                return n;
            },
            .bitset => |b| {
                var n: u32 = 0;
                var carry: u64 = 0;
                for (b.words) |w| {
                    n += @popCount(w & ~((w << 1) | carry));
                    carry = w >> 63;
                }
                return n;
            },
        }
    }
};

fn unionCount(a: []const u16, b: []const u16) usize {
    var i: usize = 0;
    var j: usize = 0;
    var n: usize = 0;
    while (i < a.len and j < b.len) : (n += 1) {
        if (a[i] < b[j]) {
            i += 1;
        } else if (b[j] < a[i]) {
            j += 1;
        } else {
            i += 1;
            j += 1;
        }
    }
    return n + (a.len - i) + (b.len - j);
}

fn bitsetOf(gpa: Allocator, values: []const u16) Allocator.Error!*Bitset {
    const b = try gpa.create(Bitset);
    @memset(&b.words, 0);
    b.card = 0;
    for (values) |v| b.set(v);
    return b;
}

/// `b` as the container its cardinality calls for; takes `b`.
fn normalized(gpa: Allocator, b: *Bitset) Allocator.Error!Container {
    if (b.card > ARRAY_MAX) return .{ .bitset = b };
    defer gpa.destroy(b);
    var list = try std.ArrayList(u16).initCapacity(gpa, b.card);
    var it = (Container{ .bitset = b }).iterator();
    while (it.next()) |v| list.appendAssumeCapacity(v);
    return .{ .array = list };
}

/// `a op b` as a new container, empty when no value survives.
fn combineContainers(gpa: Allocator, op: Op, a: Container, b: Container) Allocator.Error!Container {
    if (a == .array and b == .array) return combineArrays(gpa, op, a.array.items, b.array.items);
    var a_buf: [BITSET_WORDS]u64 = undefined;
    var b_buf: [BITSET_WORDS]u64 = undefined;
    const aw = a.words(&a_buf);
    const bw = b.words(&b_buf);
    const out = try gpa.create(Bitset);
    for (&out.words, aw, bw) |*o, x, y| o.* = switch (op) {
        .@"or" => x | y,
        .@"and" => x & y,
        .and_not => x & ~y,
        .xor => x ^ y,
    };
    out.recount();
    return normalized(gpa, out);
}

fn combineArrays(gpa: Allocator, op: Op, a: []const u16, b: []const u16) Allocator.Error!Container {
    const cap = switch (op) {
        .@"or", .xor => a.len + b.len,
        .@"and" => @min(a.len, b.len),
        .and_not => a.len,
    };
    var out = try std.ArrayList(u16).initCapacity(gpa, cap);
    errdefer out.deinit(gpa);
    const keep_a = op != .@"and";
    const keep_b = op == .@"or" or op == .xor;
    const keep_both = op == .@"or" or op == .@"and";
    var i: usize = 0;
    var j: usize = 0;
    while (i < a.len and j < b.len) {
        if (a[i] < b[j]) {
            if (keep_a) out.appendAssumeCapacity(a[i]);
            i += 1;
        } else if (b[j] < a[i]) {
            if (keep_b) out.appendAssumeCapacity(b[j]);
            j += 1;
        } else {
            if (keep_both) out.appendAssumeCapacity(a[i]);
            i += 1;
            j += 1;
        }
    }
    if (keep_a) out.appendSliceAssumeCapacity(a[i..]);
    if (keep_b) out.appendSliceAssumeCapacity(b[j..]);
    if (out.items.len <= ARRAY_MAX) return .{ .array = out };
    const bits = try bitsetOf(gpa, out.items);
    out.deinit(gpa);
    return .{ .bitset = bits };
}

const ContainerIterator = struct {
    container: Container,
    /// The next array index, or the next bit to look at.
    pos: u32 = 0,

    fn next(it: *ContainerIterator) ?u16 {
        switch (it.container) {
            .array => |a| {
                if (it.pos >= a.items.len) return null;
                defer it.pos += 1;
                return a.items[it.pos];
            },
            .bitset => |b| {
                while (it.pos < BITSET_WORDS * 64) {
                    const wi = it.pos >> 6;
                    const w = b.words[wi] >> @as(u6, @truncate(it.pos));
                    if (w == 0) {
                        it.pos = (wi + 1) << 6;
                        continue;
                    }
                    const at = it.pos + @ctz(w);
                    it.pos = at + 1;
                    return @intCast(at);
                }
                return null;
            },
        }
    }
};

const Chunk = struct {
    /// The values' high 48 bits.
    key: u64,
    container: Container,
};

fn chunkIndex(chunks: []const Chunk, key: u64) usize {
    if (chunks.len > 0 and chunks[chunks.len - 1].key < key) return chunks.len;
    var lo: usize = 0;
    var hi: usize = chunks.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (chunks[mid].key < key) lo = mid + 1 else hi = mid;
    }
    return lo;
}

pub const Bitmap = struct {
    /// Sorted by key; none is empty.
    chunks: std.ArrayList(Chunk) = .empty,

    pub fn deinit(self: *Bitmap, gpa: Allocator) void {
        for (self.chunks.items) |*ch| ch.container.deinit(gpa);
        self.chunks.deinit(gpa);
        self.* = undefined;
    }

    pub fn clone(self: *const Bitmap, gpa: Allocator) Allocator.Error!Bitmap {
        var out: Bitmap = .{};
        errdefer out.deinit(gpa);
        try out.chunks.ensureTotalCapacity(gpa, self.chunks.items.len);
        for (self.chunks.items) |ch| out.chunks.appendAssumeCapacity(.{ .key = ch.key, .container = try ch.container.clone(gpa) });
        return out;
    }

    pub fn cardinality(self: *const Bitmap) u64 {
        var n: u64 = 0;
        for (self.chunks.items) |ch| n += ch.container.cardinality();
        return n;
    }

    pub fn min(self: *const Bitmap) ?u64 {
        if (self.chunks.items.len == 0) return null;
        const ch = self.chunks.items[0];
        return (ch.key << 16) | ch.container.min();
    }

    pub fn max(self: *const Bitmap) ?u64 {
        if (self.chunks.items.len == 0) return null;
        const ch = self.chunks.items[self.chunks.items.len - 1];
        return (ch.key << 16) | ch.container.max();
    }

    pub fn contains(self: *const Bitmap, v: u64) bool {
        const items = self.chunks.items;
        const at = chunkIndex(items, v >> 16);
        return at < items.len and items[at].key == v >> 16 and items[at].container.contains(@truncate(v));
    }

    /// The values at most `v`.
    pub fn rank(self: *const Bitmap, v: u64) u64 {
        var n: u64 = 0;
        for (self.chunks.items) |ch| {
            if (ch.key > v >> 16) break;
            if (ch.key < v >> 16) {
                n += ch.container.cardinality();
                continue;
            }
            n += ch.container.rank(@truncate(v));
            break;
        }
        return n;
    }

    pub fn add(self: *Bitmap, gpa: Allocator, v: u64) Allocator.Error!void {
        const items = self.chunks.items;
        const at = chunkIndex(items, v >> 16);
        if (at < items.len and items[at].key == v >> 16) return items[at].container.add(gpa, @truncate(v));
        var list = try std.ArrayList(u16).initCapacity(gpa, 1);
        errdefer list.deinit(gpa);
        list.appendAssumeCapacity(@truncate(v));
        try self.chunks.insert(gpa, at, .{ .key = v >> 16, .container = .{ .array = list } });
    }

    pub fn remove(self: *Bitmap, gpa: Allocator, v: u64) Allocator.Error!void {
        const items = self.chunks.items;
        const at = chunkIndex(items, v >> 16);
        if (at == items.len or items[at].key != v >> 16) return;
        try items[at].container.remove(gpa, @truncate(v));
        if (items[at].container.cardinality() > 0) return;
        items[at].container.deinit(gpa);
        _ = self.chunks.orderedRemove(at);
    }

    /// Adds `container` as the chunk past every other, taking it; an empty
    /// one is dropped.
    fn appendChunk(self: *Bitmap, gpa: Allocator, key: u64, container: Container) Allocator.Error!void {
        var c = container;
        errdefer c.deinit(gpa);
        if (c.cardinality() == 0) return c.deinit(gpa);
        try self.chunks.append(gpa, .{ .key = key, .container = c });
    }

    /// ORs one chunk in; `container` is only read.
    fn unionChunk(self: *Bitmap, gpa: Allocator, key: u64, container: Container) Allocator.Error!void {
        const items = self.chunks.items;
        const at = chunkIndex(items, key);
        if (at < items.len and items[at].key == key) return items[at].container.unionInto(gpa, container);
        var copy = try container.clone(gpa);
        errdefer copy.deinit(gpa);
        try self.chunks.insert(gpa, at, .{ .key = key, .container = copy });
    }

    /// ORs `other` in place.
    pub fn unionWith(self: *Bitmap, gpa: Allocator, other: *const Bitmap) Allocator.Error!void {
        // Chunks only `other` has are copied out first and merged in at
        // once, rather than each inserted into the middle of `chunks`.
        var extra: std.ArrayList(Chunk) = .empty;
        defer extra.deinit(gpa);
        errdefer for (extra.items) |*ch| ch.container.deinit(gpa);
        var i: usize = 0;
        for (other.chunks.items) |ch| {
            while (i < self.chunks.items.len and self.chunks.items[i].key < ch.key) i += 1;
            if (i < self.chunks.items.len and self.chunks.items[i].key == ch.key) {
                try self.chunks.items[i].container.unionInto(gpa, ch.container);
                continue;
            }
            try extra.ensureUnusedCapacity(gpa, 1);
            extra.appendAssumeCapacity(.{ .key = ch.key, .container = try ch.container.clone(gpa) });
        }
        if (extra.items.len == 0) return;
        var merged = try std.ArrayList(Chunk).initCapacity(gpa, self.chunks.items.len + extra.items.len);
        const own = self.chunks.items;
        i = 0;
        var j: usize = 0;
        while (i < own.len or j < extra.items.len) {
            if (j == extra.items.len or (i < own.len and own[i].key < extra.items[j].key)) {
                merged.appendAssumeCapacity(own[i]);
                i += 1;
            } else {
                merged.appendAssumeCapacity(extra.items[j]);
                j += 1;
            }
        }
        self.chunks.deinit(gpa);
        self.chunks = merged;
    }

    /// `a op b` as a new bitmap.
    pub fn combine(gpa: Allocator, op: Op, a: *const Bitmap, b: *const Bitmap) Allocator.Error!Bitmap {
        var out: Bitmap = .{};
        errdefer out.deinit(gpa);
        const ac = a.chunks.items;
        const bc = b.chunks.items;
        var i: usize = 0;
        var j: usize = 0;
        while (i < ac.len or j < bc.len) {
            if (j == bc.len or (i < ac.len and ac[i].key < bc[j].key)) {
                if (op != .@"and") try out.appendChunk(gpa, ac[i].key, try ac[i].container.clone(gpa));
                i += 1;
            } else if (i == ac.len or bc[j].key < ac[i].key) {
                if (op == .@"or" or op == .xor) try out.appendChunk(gpa, bc[j].key, try bc[j].container.clone(gpa));
                j += 1;
            } else {
                try out.appendChunk(gpa, ac[i].key, try combineContainers(gpa, op, ac[i].container, bc[j].container));
                i += 1;
                j += 1;
            }
        }
        return out;
    }

    /// True when the two share a value.
    pub fn intersects(a: *const Bitmap, b: *const Bitmap) bool {
        const ac = a.chunks.items;
        const bc = b.chunks.items;
        var i: usize = 0;
        var j: usize = 0;
        while (i < ac.len and j < bc.len) {
            if (ac[i].key < bc[j].key) {
                i += 1;
            } else if (bc[j].key < ac[i].key) {
                j += 1;
            } else {
                var it = ac[i].container.iterator();
                while (it.next()) |v| if (bc[j].container.contains(v)) return true;
                i += 1;
                j += 1;
            }
        }
        return false;
    }

    /// The values whose 0-based ascending ranks are in `[from, to)`.
    pub fn rankRange(self: *const Bitmap, gpa: Allocator, from: u64, to: u64) Allocator.Error!Bitmap {
        var out: Bitmap = .{};
        errdefer out.deinit(gpa);
        var seen: u64 = 0;
        for (self.chunks.items) |ch| {
            if (seen >= to) break;
            const card = ch.container.cardinality();
            defer seen += card;
            if (seen + card <= from) continue;
            if (seen >= from and seen + card <= to) {
                try out.appendChunk(gpa, ch.key, try ch.container.clone(gpa));
                continue;
            }
            var it = ch.container.iterator();
            var r = seen;
            while (it.next()) |low| : (r += 1) {
                if (r >= to) break;
                if (r >= from) try out.add(gpa, (ch.key << 16) | low);
            }
        }
        return out;
    }

    pub fn iterator(self: *const Bitmap) Iterator {
        return .{ .chunks = self.chunks.items };
    }

    /// The values in ascending order, comma-separated, as
    /// `bitmap_to_string` prints them.
    pub fn appendText(self: *const Bitmap, gpa: Allocator, out: *std.ArrayList(u8)) Allocator.Error!void {
        var it = self.iterator();
        var first = true;
        while (it.next()) |v| {
            if (!first) try out.append(gpa, ',');
            first = false;
            try out.print(gpa, "{d}", .{v});
        }
    }

    pub fn serialize(self: *const Bitmap, gpa: Allocator, out: *std.ArrayList(u8)) Allocator.Error!void {
        const card = self.cardinality();
        if (card == 0) return out.append(gpa, @intFromEnum(Layout.empty));
        const largest = self.max().?;
        if (card == 1) {
            if (largest <= std.math.maxInt(u32)) {
                try out.append(gpa, @intFromEnum(Layout.single32));
                return writeInt(gpa, out, u32, @intCast(largest));
            }
            try out.append(gpa, @intFromEnum(Layout.single64));
            return writeInt(gpa, out, u64, largest);
        }
        if (card <= SET_MAX) {
            try out.append(gpa, @intFromEnum(Layout.set));
            try writeInt(gpa, out, u32, @intCast(card));
            var it = self.iterator();
            while (it.next()) |v| try writeInt(gpa, out, u64, v);
            return;
        }
        const chunks = self.chunks.items;
        if (largest <= std.math.maxInt(u32)) {
            try out.append(gpa, @intFromEnum(Layout.bitmap32));
            return writePortable(gpa, out, chunks);
        }
        try out.append(gpa, @intFromEnum(Layout.bitmap64));
        var buckets: u64 = 0;
        for (chunks, 0..) |ch, i| buckets += @intFromBool(i == 0 or chunks[i - 1].key >> 16 != ch.key >> 16);
        try writeVarint(gpa, out, buckets);
        var i: usize = 0;
        while (i < chunks.len) {
            const high = chunks[i].key >> 16;
            var j = i + 1;
            while (j < chunks.len and chunks[j].key >> 16 == high) j += 1;
            try writeInt(gpa, out, u32, @intCast(high));
            try writePortable(gpa, out, chunks[i..j]);
            i = j;
        }
    }

    /// Reads any of StarRocks' layouts.
    pub fn deserialize(gpa: Allocator, bytes: []const u8) Error!Bitmap {
        var out: Bitmap = .{};
        errdefer out.deinit(gpa);
        try parse(bytes, Decoder{ .gpa = gpa, .bitmap = &out });
        return out;
    }

    /// The set a comma-separated list of values spells, as
    /// `bitmap_from_string` reads it: empty items are skipped, spaces may
    /// pad a value, and anything else that is not an unsigned 64-bit
    /// decimal makes the whole text null.
    pub fn parseText(gpa: Allocator, text: []const u8) Allocator.Error!?Bitmap {
        var values: std.ArrayList(u64) = .empty;
        defer values.deinit(gpa);
        var items = std.mem.splitScalar(u8, text, ',');
        while (items.next()) |item| {
            if (item.len == 0) continue;
            try values.append(gpa, parseMember(item) orelse return null);
        }
        return try fromValues(gpa, values.items);
    }

    /// The set of `values`, which it sorts.
    pub fn fromValues(gpa: Allocator, values: []u64) Allocator.Error!Bitmap {
        std.mem.sort(u64, values, {}, std.sort.asc(u64));
        var out: Bitmap = .{};
        errdefer out.deinit(gpa);
        for (values) |v| try out.add(gpa, v);
        return out;
    }
};

pub const Iterator = struct {
    chunks: []const Chunk,
    index: usize = 0,
    inner: ?ContainerIterator = null,

    pub fn next(it: *Iterator) ?u64 {
        while (it.index < it.chunks.len) {
            const ch = it.chunks[it.index];
            if (it.inner == null) it.inner = ch.container.iterator();
            if (it.inner.?.next()) |low| return (ch.key << 16) | low;
            it.inner = null;
            it.index += 1;
        }
        return null;
    }
};

/// Gathers values and serialized bitmaps into one set, as the BITMAP
/// aggregates do. Single values queue and merge in sorted batches, so
/// values spread over many chunks don't each pay an insert into the middle
/// of the chunk list.
pub const Builder = struct {
    bitmap: Bitmap = .{},
    pending: std.ArrayList(u64) = .empty,

    pub fn deinit(b: *Builder, gpa: Allocator) void {
        b.bitmap.deinit(gpa);
        b.pending.deinit(gpa);
    }

    pub fn add(b: *Builder, gpa: Allocator, v: u64) Allocator.Error!void {
        try b.pending.append(gpa, v);
        if (b.pending.items.len >= PENDING_MAX) try b.flush(gpa);
    }

    /// ORs in a stored bitmap without decoding it into a `Bitmap` first.
    pub fn addSerialized(b: *Builder, gpa: Allocator, bytes: []const u8) Error!void {
        try parse(bytes, Merger{ .gpa = gpa, .builder = b });
    }

    /// The set gathered so far, as a new bitmap of `out_gpa`'s. The builder
    /// is left as it is, so it may live in another allocator (an
    /// aggregate's arena).
    pub fn merged(b: *const Builder, out_gpa: Allocator) Allocator.Error!Bitmap {
        if (b.pending.items.len == 0) return b.bitmap.clone(out_gpa);
        const values = try out_gpa.dupe(u64, b.pending.items);
        defer out_gpa.free(values);
        var batch = try Bitmap.fromValues(out_gpa, values);
        defer batch.deinit(out_gpa);
        return Bitmap.combine(out_gpa, .@"or", &b.bitmap, &batch);
    }

    fn flush(b: *Builder, gpa: Allocator) Allocator.Error!void {
        if (b.pending.items.len == 0) return;
        var batch = try Bitmap.fromValues(gpa, b.pending.items);
        defer batch.deinit(gpa);
        try b.bitmap.unionWith(gpa, &batch);
        b.pending.clearRetainingCapacity();
    }
};

/// True when `bytes` is a value of one of StarRocks' layouts.
pub fn isWellFormed(bytes: []const u8) bool {
    parse(bytes, Validator{}) catch return false;
    return true;
}

/// Writes `bytes`, a value in any of StarRocks' layouts, in the one layout
/// `serialize` writes for its set.
pub fn canonicalize(gpa: Allocator, bytes: []const u8, out: *std.ArrayList(u8)) Error!void {
    var bm = try Bitmap.deserialize(gpa, bytes);
    defer bm.deinit(gpa);
    try bm.serialize(gpa, out);
}

/// The number of values in a stored bitmap, read without decoding it.
pub fn cardinalityOf(bytes: []const u8) error{BitmapInvalid}!u64 {
    var n: u64 = 0;
    try parse(bytes, Counter{ .n = &n });
    return n;
}

/// Appends the stored form of the set `{v}`, as `to_bitmap(v)` gives.
pub fn appendSingle(gpa: Allocator, out: *std.ArrayList(u8), v: u64) Allocator.Error!void {
    if (v <= std.math.maxInt(u32)) {
        try out.append(gpa, @intFromEnum(Layout.single32));
        return writeInt(gpa, out, u32, @intCast(v));
    }
    try out.append(gpa, @intFromEnum(Layout.single64));
    try writeInt(gpa, out, u64, v);
}

/// The stored form of the empty set.
pub const EMPTY_BYTES = [_]u8{@intFromEnum(Layout.empty)};

/// The member a text spells: an unsigned 64-bit decimal, spaces around it
/// allowed.
pub fn parseMember(text: []const u8) ?u64 {
    return parseDecimal(std.mem.trim(u8, text, " \t\r\n"));
}

fn parseDecimal(digits: []const u8) ?u64 {
    if (digits.len == 0) return null;
    var v: u64 = 0;
    for (digits) |ch| {
        if (ch < '0' or ch > '9') return null;
        v = std.math.mul(u64, v, 10) catch return null;
        v = std.math.add(u64, v, ch - '0') catch return null;
    }
    return v;
}

fn writeInt(gpa: Allocator, out: *std.ArrayList(u8), comptime T: type, v: T) Allocator.Error!void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, v, .little);
    try out.appendSlice(gpa, &buf);
}

fn writeVarint(gpa: Allocator, out: *std.ArrayList(u8), v: u64) Allocator.Error!void {
    var rest = v;
    while (rest >= 0x80) : (rest >>= 7) try out.append(gpa, @as(u8, @truncate(rest)) | 0x80);
    try out.append(gpa, @intCast(rest));
}

/// One portable Roaring bitmap of `chunks`, which share their high 32 bits.
fn writePortable(gpa: Allocator, out: *std.ArrayList(u8), chunks: []const Chunk) Allocator.Error!void {
    const n = chunks.len;
    // A container's run count when runs encode it smaller, else 0.
    const runs = try gpa.alloc(u32, n);
    defer gpa.free(runs);
    var any_runs = false;
    for (chunks, runs) |ch, *r| {
        const count = ch.container.runCount();
        r.* = if (2 + 4 * count < plainBytes(ch.container.cardinality())) count else 0;
        any_runs = any_runs or r.* != 0;
    }
    const start = out.items.len;
    if (any_runs) {
        try writeInt(gpa, out, u32, COOKIE_RUNS | @as(u32, @intCast(n - 1)) << 16);
        const flags_at = out.items.len;
        try out.appendNTimes(gpa, 0, (n + 7) / 8);
        for (runs, 0..) |r, i| {
            if (r != 0) out.items[flags_at + i / 8] |= @as(u8, 1) << @intCast(i % 8);
        }
    } else {
        try writeInt(gpa, out, u32, COOKIE_NO_RUNS);
        try writeInt(gpa, out, u32, @intCast(n));
    }
    for (chunks) |ch| {
        try writeInt(gpa, out, u16, @truncate(ch.key));
        try writeInt(gpa, out, u16, @intCast(ch.container.cardinality() - 1));
    }
    if (!any_runs or n >= NO_OFFSET_THRESHOLD) {
        var offset: u32 = @intCast(out.items.len - start + 4 * n);
        for (chunks, runs) |ch, r| {
            try writeInt(gpa, out, u32, offset);
            offset += if (r != 0) 2 + 4 * r else plainBytes(ch.container.cardinality());
        }
    }
    for (chunks, runs) |ch, r| {
        if (r != 0) {
            try writeInt(gpa, out, u16, @intCast(r));
            var it = ch.container.iterator();
            var run_start = it.next().?;
            var run_end = run_start;
            while (it.next()) |v| {
                if (v == run_end + 1) {
                    run_end = v;
                    continue;
                }
                try writeInt(gpa, out, u16, run_start);
                try writeInt(gpa, out, u16, run_end - run_start);
                run_start = v;
                run_end = v;
            }
            try writeInt(gpa, out, u16, run_start);
            try writeInt(gpa, out, u16, run_end - run_start);
            continue;
        }
        switch (ch.container) {
            .array => |a| for (a.items) |v| try writeInt(gpa, out, u16, v),
            .bitset => |b| for (b.words) |w| try writeInt(gpa, out, u64, w),
        }
    }
}

/// The bytes an array or bitset container of `card` values takes.
fn plainBytes(card: u32) u32 {
    return if (card <= ARRAY_MAX) 2 * card else BITSET_WORDS * 8;
}

const Reader = struct {
    bytes: []const u8,
    pos: usize = 0,

    fn take(r: *Reader, n: usize) error{BitmapInvalid}![]const u8 {
        if (r.bytes.len - r.pos < n) return error.BitmapInvalid;
        defer r.pos += n;
        return r.bytes[r.pos..][0..n];
    }

    fn int(r: *Reader, comptime T: type) error{BitmapInvalid}!T {
        return std.mem.readInt(T, (try r.take(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }

    fn varint(r: *Reader) error{BitmapInvalid}!u64 {
        var v: u64 = 0;
        var shift: u32 = 0;
        while (shift < 64) : (shift += 7) {
            const b = (try r.take(1))[0];
            v |= @as(u64, b & 0x7f) << @intCast(shift);
            if (b & 0x80 == 0) return v;
        }
        return error.BitmapInvalid;
    }
};

/// Walks a stored bitmap, handing `visitor` each value of the small layouts
/// (`value(u64)`) and each container of the Roaring ones (`chunk(key,
/// Container)`), in ascending key order. A chunk's container lives in this
/// call's buffers, so a visitor that keeps it copies it.
fn parse(bytes: []const u8, visitor: anytype) !void {
    var r: Reader = .{ .bytes = bytes };
    const layout = std.enums.fromInt(Layout, try r.int(u8)) orelse return error.BitmapInvalid;
    switch (layout) {
        .empty => {},
        .single32 => try visitor.value(try r.int(u32)),
        .single64 => try visitor.value(try r.int(u64)),
        .set => {
            const n = try r.int(u32);
            const values = try r.take(@as(usize, n) * 8);
            var i: usize = 0;
            while (i < values.len) : (i += 8) try visitor.value(std.mem.readInt(u64, values[i..][0..8], .little));
        },
        .bitmap32 => try parsePortable(&r, 0, visitor),
        .bitmap64 => {
            const n = try r.varint();
            var prev: ?u32 = null;
            var i: u64 = 0;
            while (i < n) : (i += 1) {
                const high = try r.int(u32);
                if (prev) |p| if (high <= p) return error.BitmapInvalid;
                prev = high;
                try parsePortable(&r, high, visitor);
            }
        },
    }
    if (r.pos != bytes.len) return error.BitmapInvalid;
}

fn parsePortable(r: *Reader, high: u32, visitor: anytype) !void {
    const cookie = try r.int(u32);
    var count: usize = undefined;
    var run_flags: ?[]const u8 = null;
    if (cookie & 0xffff == COOKIE_RUNS) {
        count = (cookie >> 16) + 1;
        run_flags = try r.take((count + 7) / 8);
    } else if (cookie == COOKIE_NO_RUNS) {
        count = try r.int(u32);
    } else return error.BitmapInvalid;
    const descriptors = try r.take(count * 4);
    // Offsets only help a reader that seeks; this one reads in order.
    if (run_flags == null or count >= NO_OFFSET_THRESHOLD) _ = try r.take(count * 4);
    var values: [ARRAY_MAX]u16 = undefined;
    var bits: Bitset = undefined;
    for (0..count) |i| {
        const key = std.mem.readInt(u16, descriptors[i * 4 ..][0..2], .little);
        if (i > 0 and key <= std.mem.readInt(u16, descriptors[(i - 1) * 4 ..][0..2], .little)) return error.BitmapInvalid;
        const card = @as(u32, std.mem.readInt(u16, descriptors[i * 4 + 2 ..][0..2], .little)) + 1;
        const is_run = if (run_flags) |f| f[i / 8] & (@as(u8, 1) << @intCast(i % 8)) != 0 else false;
        const container = if (is_run)
            try readRuns(r, card, &values, &bits)
        else if (card <= ARRAY_MAX)
            try readArray(r, card, &values)
        else
            try readBitset(r, card, &bits);
        try visitor.chunk((@as(u64, high) << 16) | key, container);
    }
}

fn readArray(r: *Reader, card: u32, values: *[ARRAY_MAX]u16) error{BitmapInvalid}!Container {
    const raw = try r.take(@as(usize, card) * 2);
    for (values[0..card], 0..) |*v, i| {
        v.* = std.mem.readInt(u16, raw[i * 2 ..][0..2], .little);
        if (i > 0 and v.* <= values[i - 1]) return error.BitmapInvalid;
    }
    return .{ .array = .{ .items = values[0..card], .capacity = card, .pointer_stability = .{} } };
}

fn readBitset(r: *Reader, card: u32, bits: *Bitset) error{BitmapInvalid}!Container {
    const raw = try r.take(BITSET_WORDS * 8);
    for (&bits.words, 0..) |*w, i| w.* = std.mem.readInt(u64, raw[i * 8 ..][0..8], .little);
    bits.recount();
    if (bits.card != card) return error.BitmapInvalid;
    return .{ .bitset = bits };
}

fn readRuns(r: *Reader, card: u32, values: *[ARRAY_MAX]u16, bits: *Bitset) error{BitmapInvalid}!Container {
    const n = try r.int(u16);
    const raw = try r.take(@as(usize, n) * 4);
    var total: u32 = 0;
    var next_free: u32 = 0;
    for (0..n) |k| {
        const start: u32 = std.mem.readInt(u16, raw[k * 4 ..][0..2], .little);
        const len: u32 = @as(u32, std.mem.readInt(u16, raw[k * 4 + 2 ..][0..2], .little)) + 1;
        if (start < next_free or start + len > 1 << 16) return error.BitmapInvalid;
        next_free = start + len;
        total += len;
    }
    if (total != card) return error.BitmapInvalid;
    if (card <= ARRAY_MAX) {
        var at: usize = 0;
        for (0..n) |k| {
            const start: u32 = std.mem.readInt(u16, raw[k * 4 ..][0..2], .little);
            const len: u32 = @as(u32, std.mem.readInt(u16, raw[k * 4 + 2 ..][0..2], .little)) + 1;
            for (start..start + len) |v| {
                values[at] = @intCast(v);
                at += 1;
            }
        }
        return .{ .array = .{ .items = values[0..card], .capacity = card, .pointer_stability = .{} } };
    }
    @memset(&bits.words, 0);
    for (0..n) |k| {
        const start: u32 = std.mem.readInt(u16, raw[k * 4 ..][0..2], .little);
        const len: u32 = @as(u32, std.mem.readInt(u16, raw[k * 4 + 2 ..][0..2], .little)) + 1;
        for (start..start + len) |v| bits.words[v >> 6] |= bitOf(@intCast(v));
    }
    bits.card = card;
    return .{ .bitset = bits };
}

const Decoder = struct {
    gpa: Allocator,
    bitmap: *Bitmap,

    fn value(d: Decoder, v: u64) Allocator.Error!void {
        try d.bitmap.add(d.gpa, v);
    }

    fn chunk(d: Decoder, key: u64, c: Container) Allocator.Error!void {
        try d.bitmap.appendChunk(d.gpa, key, try c.clone(d.gpa));
    }
};

const Merger = struct {
    gpa: Allocator,
    builder: *Builder,

    fn value(m: Merger, v: u64) Allocator.Error!void {
        try m.builder.add(m.gpa, v);
    }

    fn chunk(m: Merger, key: u64, c: Container) Allocator.Error!void {
        try m.builder.bitmap.unionChunk(m.gpa, key, c);
    }
};

const Validator = struct {
    fn value(_: Validator, _: u64) error{}!void {}
    fn chunk(_: Validator, _: u64, _: Container) error{}!void {}
};

const Counter = struct {
    n: *u64,

    fn value(c: Counter, _: u64) error{}!void {
        c.n.* += 1;
    }

    fn chunk(c: Counter, _: u64, container: Container) error{}!void {
        c.n.* += container.cardinality();
    }
};

const testing = std.testing;

fn testBitmapOf(values: []const u64) !Bitmap {
    const copy = try testing.allocator.dupe(u64, values);
    defer testing.allocator.free(copy);
    return Bitmap.fromValues(testing.allocator, copy);
}

fn expectValues(bm: *const Bitmap, expected: []const u64) !void {
    var got: std.ArrayList(u64) = .empty;
    defer got.deinit(testing.allocator);
    var it = bm.iterator();
    while (it.next()) |v| try got.append(testing.allocator, v);
    try testing.expectEqualSlices(u64, expected, got.items);
    try testing.expectEqual(@as(u64, expected.len), bm.cardinality());
}

fn expectHex(bm: *const Bitmap, hex: []const u8) !void {
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(testing.allocator);
    try bm.serialize(testing.allocator, &bytes);
    const got = try std.fmt.allocPrint(testing.allocator, "{X}", .{bytes.items});
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(hex, got);
}

fn expectRoundTrip(bm: *const Bitmap) !void {
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(testing.allocator);
    try bm.serialize(testing.allocator, &bytes);
    try testing.expect(isWellFormed(bytes.items));
    try testing.expectEqual(bm.cardinality(), try cardinalityOf(bytes.items));
    var back = try Bitmap.deserialize(testing.allocator, bytes.items);
    defer back.deinit(testing.allocator);
    var a = bm.iterator();
    var b = back.iterator();
    while (a.next()) |v| try testing.expectEqual(v, b.next().?);
    try testing.expectEqual(@as(?u64, null), b.next());
}

fn rangeValues(gpa: Allocator, from: u64, to: u64) ![]u64 {
    const values = try gpa.alloc(u64, to - from + 1);
    for (values, from..) |*v, x| v.* = x;
    return values;
}

test "bitmap: serializes in StarRocks' layouts" {
    const gpa = testing.allocator;
    var empty: Bitmap = .{};
    try expectHex(&empty, "00");
    var one = try testBitmapOf(&.{1});
    defer one.deinit(gpa);
    try expectHex(&one, "0101000000");
    var wide = try testBitmapOf(&.{17179869184});
    defer wide.deinit(gpa);
    try expectHex(&wide, "030000000004000000");
    var five = try testBitmapOf(&.{ 5, 3, 1, 4, 2 });
    defer five.deinit(gpa);
    try expectHex(&five, "0A0500000001000000000000000200000000000000030000000000000004000000000000000500000000000000");

    const forty = try rangeValues(gpa, 1, 40);
    defer gpa.free(forty);
    var run = try Bitmap.fromValues(gpa, forty);
    defer run.deinit(gpa);
    try expectHex(&run, "023B3000000100002700010001002700");

    const low = try rangeValues(gpa, 1, 200);
    defer gpa.free(low);
    var two_runs = try Bitmap.fromValues(gpa, low);
    defer two_runs.deinit(gpa);
    for (900..911) |v| try two_runs.add(gpa, v);
    try expectHex(&two_runs, "023B300000010000D20002000100C70084030A00");
}

test "bitmap: serializes past 32 bits by high-bits bucket" {
    const gpa = testing.allocator;
    const values = try rangeValues(gpa, 17179869184, 17179869284);
    defer gpa.free(values);
    var bm = try Bitmap.fromValues(gpa, values);
    defer bm.deinit(gpa);
    try expectHex(&bm, "0401040000003B3000000100006400010000006400");
}

test "bitmap: reads Roaring that StarRocks writes without runs" {
    const gpa = testing.allocator;
    const bytes = [_]u8{ 0x02, 0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00 };
    var bm = try Bitmap.deserialize(gpa, &bytes);
    defer bm.deinit(gpa);
    try expectValues(&bm, &.{ 1, 2, 3 });
    try testing.expectEqual(@as(u64, 3), try cardinalityOf(&bytes));
}

test "bitmap: reads a 64-bit bitmap with an array bucket" {
    const gpa = testing.allocator;
    var hex = "0402000000003A3000000100000000001300100000000100020003000400050006000700080009000A000B000C000D000E000F0010001100120013001400040000003B3000000100006400010000006400".*;
    var bytes: [hex.len / 2]u8 = undefined;
    _ = try std.fmt.hexToBytes(&bytes, &hex);
    var bm = try Bitmap.deserialize(gpa, &bytes);
    defer bm.deinit(gpa);
    try testing.expectEqual(@as(u64, 121), bm.cardinality());
    try testing.expectEqual(@as(?u64, 1), bm.min());
    try testing.expectEqual(@as(?u64, 17179869284), bm.max());
    try testing.expect(bm.contains(20) and !bm.contains(21) and bm.contains(17179869184));
}

test "bitmap: refuses malformed bytes" {
    const cases = [_][]const u8{
        "",
        &.{0x07},
        &.{ 0x01, 0x01, 0x00 },
        &.{ 0x00, 0x00 },
        &.{ 0x0A, 0x02, 0x00, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0, 0 },
        // An array container whose values are not ascending.
        &.{ 0x02, 0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00 },
        // A run container whose runs cover more values than it claims.
        &.{ 0x02, 0x3B, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x05, 0x00 },
        "1,2,3",
    };
    for (cases) |c| try testing.expect(!isWellFormed(c));
}

test "bitmap: round-trips sets of every container shape" {
    const gpa = testing.allocator;
    const dense = try rangeValues(gpa, 0, 70000);
    defer gpa.free(dense);
    var a = try Bitmap.fromValues(gpa, dense);
    defer a.deinit(gpa);
    try expectRoundTrip(&a);

    var sparse: Bitmap = .{};
    defer sparse.deinit(gpa);
    var v: u64 = 7;
    for (0..6000) |_| {
        try sparse.add(gpa, v);
        v += 3;
    }
    try sparse.add(gpa, std.math.maxInt(u64));
    try sparse.add(gpa, 1 << 40);
    try expectRoundTrip(&sparse);
    try testing.expectEqual(@as(u64, 6002), sparse.cardinality());

    var many_chunks: Bitmap = .{};
    defer many_chunks.deinit(gpa);
    for (0..40) |i| try many_chunks.add(gpa, @as(u64, i) << 16);
    try expectRoundTrip(&many_chunks);
}

test "bitmap: set operations" {
    const gpa = testing.allocator;
    var a = try testBitmapOf(&.{ 1, 2, 3, 1 << 33 });
    defer a.deinit(gpa);
    var b = try testBitmapOf(&.{ 2, 3, 4 });
    defer b.deinit(gpa);
    const cases = .{
        .{ Op.@"or", &[_]u64{ 1, 2, 3, 4, 1 << 33 } },
        .{ Op.@"and", &[_]u64{ 2, 3 } },
        .{ Op.and_not, &[_]u64{ 1, 1 << 33 } },
        .{ Op.xor, &[_]u64{ 1, 4, 1 << 33 } },
    };
    inline for (cases) |c| {
        var out = try Bitmap.combine(gpa, c[0], &a, &b);
        defer out.deinit(gpa);
        try expectValues(&out, c[1]);
    }
    try testing.expect(a.intersects(&b));
    var c = try testBitmapOf(&.{ 9, 10 });
    defer c.deinit(gpa);
    try testing.expect(!a.intersects(&c));
}

test "bitmap: container shapes switch as sets grow and shrink" {
    const gpa = testing.allocator;
    const evens = try gpa.alloc(u64, ARRAY_MAX);
    defer gpa.free(evens);
    for (evens, 0..) |*v, i| v.* = 2 * i;
    var a = try Bitmap.fromValues(gpa, evens);
    defer a.deinit(gpa);
    try testing.expect(a.chunks.items[0].container == .array);
    try a.add(gpa, 1);
    try testing.expect(a.chunks.items[0].container == .bitset);
    try a.remove(gpa, 1);
    try testing.expect(a.chunks.items[0].container == .array);
    try expectValues(&a, evens);

    const odds_list = try gpa.alloc(u64, ARRAY_MAX);
    defer gpa.free(odds_list);
    for (odds_list, 0..) |*v, i| v.* = 2 * i + 1;
    var odds = try Bitmap.fromValues(gpa, odds_list);
    defer odds.deinit(gpa);
    var both = try a.clone(gpa);
    defer both.deinit(gpa);
    try both.unionWith(gpa, &odds);
    try testing.expectEqual(@as(u64, 2 * ARRAY_MAX), both.cardinality());
    try testing.expect(both.chunks.items[0].container == .bitset);
    var back = try Bitmap.combine(gpa, .and_not, &both, &odds);
    defer back.deinit(gpa);
    try testing.expect(back.chunks.items[0].container == .array);
    try expectValues(&back, evens);
}

test "bitmap: union in place merges arrays and new chunks" {
    const gpa = testing.allocator;
    var a = try testBitmapOf(&.{ 1, 5, 9, 1 << 20 });
    defer a.deinit(gpa);
    var b = try testBitmapOf(&.{ 0, 5, 7, 10, 1 << 18, 1 << 40 });
    defer b.deinit(gpa);
    try a.unionWith(gpa, &b);
    try expectValues(&a, &.{ 0, 1, 5, 7, 9, 10, 1 << 18, 1 << 20, 1 << 40 });
}

test "bitmap: rank and rank ranges" {
    const gpa = testing.allocator;
    var a = try testBitmapOf(&.{ 0, 1, 2, 3, 5, 1 << 20 });
    defer a.deinit(gpa);
    try testing.expectEqual(@as(u64, 1), a.rank(0));
    try testing.expectEqual(@as(u64, 4), a.rank(4));
    try testing.expectEqual(@as(u64, 5), a.rank(1 << 19));
    try testing.expectEqual(@as(u64, 6), a.rank(std.math.maxInt(u64)));
    var mid = try a.rankRange(gpa, 3, 5);
    defer mid.deinit(gpa);
    try expectValues(&mid, &.{ 3, 5 });
    var all = try a.rankRange(gpa, 0, 100);
    defer all.deinit(gpa);
    try expectValues(&all, &.{ 0, 1, 2, 3, 5, 1 << 20 });
}

test "bitmap: parses text as bitmap_from_string does" {
    const gpa = testing.allocator;
    var a = (try Bitmap.parseText(gpa, "1,0,1, 2 ,3,,5")).?;
    defer a.deinit(gpa);
    try expectValues(&a, &.{ 0, 1, 2, 3, 5 });
    var empty = (try Bitmap.parseText(gpa, "")).?;
    defer empty.deinit(gpa);
    try testing.expectEqual(@as(u64, 0), empty.cardinality());
    inline for (.{ "abc", "-1", "1,x", "18446744073709551616", "1_0" }) |text| {
        try testing.expectEqual(@as(?Bitmap, null), try Bitmap.parseText(gpa, text));
    }
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(gpa);
    try a.appendText(gpa, &text);
    try testing.expectEqualStrings("0,1,2,3,5", text.items);
}

test "bitmap: builder gathers values and stored bitmaps" {
    const gpa = testing.allocator;
    var b: Builder = .{};
    defer b.deinit(gpa);
    var v: u64 = 0;
    for (0..10000) |_| {
        try b.add(gpa, v);
        v = (v + 7919) % 1000003;
    }
    var stored: std.ArrayList(u8) = .empty;
    defer stored.deinit(gpa);
    var extra = try testBitmapOf(&.{ 1 << 50, 3 });
    defer extra.deinit(gpa);
    try extra.serialize(gpa, &stored);
    try b.addSerialized(gpa, stored.items);
    const values = try gpa.alloc(u64, 50);
    defer gpa.free(values);
    for (values, 0..) |*x, i| x.* = 2000000 + i;
    var run = try Bitmap.fromValues(gpa, values);
    defer run.deinit(gpa);
    stored.clearRetainingCapacity();
    try run.serialize(gpa, &stored);
    try b.addSerialized(gpa, stored.items);
    var got = try b.merged(gpa);
    defer got.deinit(gpa);
    try testing.expectEqual(@as(u64, 10000 + 2 + 50), got.cardinality());
    try testing.expect(got.contains(1 << 50) and got.contains(2000049) and got.contains(7919));
    try b.add(gpa, 1 << 60);
    var again = try b.merged(gpa);
    defer again.deinit(gpa);
    try testing.expectEqual(got.cardinality() + 1, again.cardinality());
}
