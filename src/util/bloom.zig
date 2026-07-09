//! A small Bloom filter over 64-bit key hashes, sized for per-segment primary-key
//! membership. Built once when a segment is written (flush or compaction merge),
//! stored in the segment footer, and queried on the upsert existence probe and
//! PK point lookups: "is this key definitely NOT in this segment?" — a miss lets
//! the caller skip the segment with no decode.
//!
//! Uses Kirsch–Mitzenmacher double hashing: the k probe positions for a key are
//! derived from one 64-bit hash split into two 32-bit halves, so callers only
//! compute a single hash per key.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Default bits-per-key. The upsert probe tests a WHOLE BATCH (~2000 keys) and
/// can skip a segment only if EVERY key misses, so the union false-positive rate
/// over the batch must stay low: P(skip) ≈ (1-fp)^batch. At 10 bits/key (~0.8%
/// fp) that's ~e^-16 ≈ 0 — the segment never skips. 28 bits/key (~1e-6 fp) gives
/// ~e^-0.003 ≈ 99.7% skip for a 2000-key batch, at ~3.5 bytes/key. This is the
/// lever that makes the Bloom actually prune a bulk upsert load (#138).
pub const default_bits_per_key: u32 = 28;

pub const Bloom = struct {
    /// Bit array. `bits.len * 8` addressable bits.
    bits: []u8,
    /// Number of addressable bits (== bits.len * 8, kept explicit for modulo).
    m: u32,
    /// Probe count.
    k: u8,

    /// Optimal k for a given bits-per-key ratio: round(bpk * ln2).
    fn kForBitsPerKey(bpk: u32) u8 {
        const k_f = @as(f64, @floatFromInt(bpk)) * std.math.ln2;
        const k: u32 = @max(1, @min(30, @as(u32, @intFromFloat(@round(k_f)))));
        return @intCast(k);
    }

    /// Build a filter over `hashes` (one 64-bit hash per key). `n_hint` sizes the
    /// bit array; pass the exact count when known. Caller owns the returned bits.
    pub fn build(allocator: Allocator, hashes: []const u64, bits_per_key: u32) !Bloom {
        const n = @max(hashes.len, 1);
        // Round the bit count up to a byte boundary.
        const m_bits: u32 = @intCast(@max(64, (n * bits_per_key + 7) / 8 * 8));
        const bytes = try allocator.alloc(u8, m_bits / 8);
        @memset(bytes, 0);
        var self: Bloom = .{ .bits = bytes, .m = m_bits, .k = kForBitsPerKey(bits_per_key) };
        for (hashes) |h| self.add(h);
        return self;
    }

    pub fn deinit(self: *Bloom, allocator: Allocator) void {
        allocator.free(self.bits);
        self.* = undefined;
    }

    fn positions(h: u64) struct { h1: u32, h2: u32 } {
        const h1: u32 = @truncate(h);
        var h2: u32 = @truncate(h >> 32);
        // A zero delta would probe the same bit k times; force it odd & nonzero.
        h2 |= 1;
        return .{ .h1 = h1, .h2 = h2 };
    }

    pub fn add(self: *Bloom, h: u64) void {
        const p = positions(h);
        var i: u8 = 0;
        var cur: u32 = p.h1;
        while (i < self.k) : (i += 1) {
            const bit = cur % self.m;
            self.bits[bit / 8] |= (@as(u8, 1) << @intCast(bit % 8));
            cur +%= p.h2;
        }
    }

    pub fn mayContain(self: *const Bloom, h: u64) bool {
        const p = positions(h);
        var i: u8 = 0;
        var cur: u32 = p.h1;
        while (i < self.k) : (i += 1) {
            const bit = cur % self.m;
            if ((self.bits[bit / 8] & (@as(u8, 1) << @intCast(bit % 8))) == 0) return false;
            cur +%= p.h2;
        }
        return true;
    }

    // ---- serialization: [m: u32 LE][k: u8][bits...] ----

    pub fn serializedLen(self: *const Bloom) usize {
        return 4 + 1 + self.bits.len;
    }

    pub fn writeTo(self: *const Bloom, out: []u8) usize {
        std.mem.writeInt(u32, out[0..4], self.m, .little);
        out[4] = self.k;
        @memcpy(out[5 .. 5 + self.bits.len], self.bits);
        return self.serializedLen();
    }

    /// Query a serialized filter in place — no allocation, no owning struct.
    /// Used on the hot upsert-probe / point-lookup path against a filter stored
    /// in a manifest entry. Returns false (definitely absent) or true (maybe).
    pub fn mayContainSerialized(buf: []const u8, h: u64) bool {
        if (buf.len < 5) return true; // no filter → can't rule out
        const m = std.mem.readInt(u32, buf[0..4], .little);
        const k = buf[4];
        const bits = buf[5..];
        const p = positions(h);
        var i: u8 = 0;
        var cur: u32 = p.h1;
        while (i < k) : (i += 1) {
            const bit = cur % m;
            if ((bits[bit / 8] & (@as(u8, 1) << @intCast(bit % 8))) == 0) return false;
            cur +%= p.h2;
        }
        return true;
    }

    /// Parse a filter whose bits are copied into caller-owned memory.
    pub fn readFrom(allocator: Allocator, buf: []const u8) !Bloom {
        if (buf.len < 5) return error.BloomTooSmall;
        const m = std.mem.readInt(u32, buf[0..4], .little);
        const k = buf[4];
        const n_bytes = m / 8;
        if (buf.len < 5 + n_bytes) return error.BloomTooSmall;
        const bits = try allocator.dupe(u8, buf[5 .. 5 + n_bytes]);
        return .{ .bits = bits, .m = m, .k = k };
    }
};

/// Sanity-check a serialized filter before trusting it on the probe path —
/// e.g. a sidecar file that a crash could have torn. Header present, nonzero
/// byte-aligned m, bit array fully present (m == 0 would divide-by-zero in
/// the probe; short bits would index out of bounds).
pub fn validSerialized(buf: []const u8) bool {
    if (buf.len < 5) return false;
    const m = std.mem.readInt(u32, buf[0..4], .little);
    if (m < 8 or m % 8 != 0) return false;
    return buf.len >= 5 + m / 8;
}

/// Hash compound-key bytes to a 64-bit value for the filter. Callers must use
/// this exact function on both build and query so probes line up.
pub inline fn keyHash(key_bytes: []const u8) u64 {
    return std.hash.Wyhash.hash(0x7b1d_b100_f117_e400, key_bytes);
}

test "bloom: no false negatives, low false positive rate" {
    const allocator = std.testing.allocator;
    var present: std.ArrayList(u64) = .empty;
    defer present.deinit(allocator);
    var i: u64 = 0;
    while (i < 10_000) : (i += 1) try present.append(allocator, keyHash(std.mem.asBytes(&i)));

    var bf = try Bloom.build(allocator, present.items, default_bits_per_key);
    defer bf.deinit(allocator);

    // No false negatives — every inserted key must test present.
    for (present.items) |h| try std.testing.expect(bf.mayContain(h));

    // False-positive rate on absent keys should be near the ~1% target.
    var fp: usize = 0;
    var j: u64 = 1_000_000;
    while (j < 1_010_000) : (j += 1) {
        if (bf.mayContain(keyHash(std.mem.asBytes(&j)))) fp += 1;
    }
    try std.testing.expect(fp < 300); // < 3%, comfortably above the 1% target

    // Round-trips through serialization.
    const buf = try allocator.alloc(u8, bf.serializedLen());
    defer allocator.free(buf);
    _ = bf.writeTo(buf);
    var bf2 = try Bloom.readFrom(allocator, buf);
    defer bf2.deinit(allocator);
    for (present.items) |h| try std.testing.expect(bf2.mayContain(h));
}
