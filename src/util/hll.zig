//! HyperLogLog cardinality sketch — fixed-size, mergeable distinct-value
//! estimator. Replaces a stored exact count so cardinality merges cleanly
//! across many segments (register-wise max — error doesn't accumulate as
//! segment count grows, unlike summing per-segment counts).
//!
//! Standard Flajolet HLL with linear-counting in the small-cardinality
//! range. precision p=10 ⇒ m=1024 registers ⇒ 1 KB ⇒ ~3.25% standard
//! error. Precision is irrelevant for our use (a hash-vs-sort GROUP BY
//! cutoff near a few thousand groups, where the choice is harmless either
//! way), so we favour the smallest sketch. The hash fed to `add` must be a
//! well-distributed 64-bit value.

const std = @import("std");

pub const precision = 10;
pub const m: usize = 1 << precision;

pub const Hll = struct {
    /// One register per bucket: the max observed `rho` (leading-zero rank).
    registers: [m]u8 = [_]u8{0} ** m,

    pub fn add(self: *Hll, hash: u64) void {
        const idx: usize = @intCast(hash >> (64 - precision));
        // Remaining bits after the index; rho = position of the leftmost
        // set bit (1-indexed). Shifting out the index bits leaves the
        // significant bits at the top, so @clz counts them directly.
        const w: u64 = hash << precision;
        const rho: u8 = if (w == 0) (64 - precision + 1) else @intCast(@clz(w) + 1);
        if (rho > self.registers[idx]) self.registers[idx] = rho;
    }

    /// Merge `other` into `self` (register-wise max). Vectorized.
    pub fn merge(self: *Hll, other: *const Hll) void {
        const V = @Vector(32, u8);
        var i: usize = 0;
        while (i + 32 <= m) : (i += 32) {
            const a: V = self.registers[i..][0..32].*;
            const b: V = other.registers[i..][0..32].*;
            self.registers[i..][0..32].* = @max(a, b);
        }
        while (i < m) : (i += 1) {
            if (other.registers[i] > self.registers[i]) self.registers[i] = other.registers[i];
        }
    }

    pub fn estimate(self: *const Hll) u64 {
        var sum: f64 = 0;
        var zeros: usize = 0;
        for (self.registers) |r| {
            sum += 1.0 / @as(f64, @floatFromInt(@as(u64, 1) << @intCast(r)));
            if (r == 0) zeros += 1;
        }
        const mf: f64 = @floatFromInt(m);
        const alpha = 0.7213 / (1.0 + 1.079 / mf);
        var e = alpha * mf * mf / sum;
        // Small-range correction: linear counting when many registers are
        // still zero (raw HLL is biased low for small cardinalities).
        if (e <= 2.5 * mf and zeros > 0) {
            e = mf * @log(mf / @as(f64, @floatFromInt(zeros)));
        }
        return @intFromFloat(@round(e));
    }

    pub fn bytes(self: *const Hll) []const u8 {
        return &self.registers;
    }

    pub fn fromBytes(b: []const u8) Hll {
        var h: Hll = .{};
        const n = @min(b.len, m);
        @memcpy(h.registers[0..n], b[0..n]);
        return h;
    }
};

fn approxEq(actual: u64, expected: u64, rel: f64) bool {
    const a: f64 = @floatFromInt(actual);
    const e: f64 = @floatFromInt(expected);
    return @abs(a - e) <= rel * e + 2.0;
}

test "hll: estimates small cardinality (linear counting range)" {
    var h: Hll = .{};
    var i: u64 = 0;
    while (i < 50) : (i += 1) h.add(std.hash.Wyhash.hash(0, std.mem.asBytes(&i)));
    // Dups don't move the estimate.
    h.add(std.hash.Wyhash.hash(0, std.mem.asBytes(&@as(u64, 7))));
    try std.testing.expect(approxEq(h.estimate(), 50, 0.10));
}

test "hll: estimates mid cardinality within standard error" {
    var h: Hll = .{};
    var i: u64 = 0;
    while (i < 5000) : (i += 1) h.add(std.hash.Wyhash.hash(0, std.mem.asBytes(&i)));
    try std.testing.expect(approxEq(h.estimate(), 5000, 0.10));
}

test "hll: merge equals union (no error growth across sketches)" {
    var a: Hll = .{};
    var b: Hll = .{};
    var i: u64 = 0;
    while (i < 3000) : (i += 1) a.add(std.hash.Wyhash.hash(0, std.mem.asBytes(&i)));
    // b overlaps a on [1500,3000) and adds [3000,4500) → union = 4500.
    var j: u64 = 1500;
    while (j < 4500) : (j += 1) b.add(std.hash.Wyhash.hash(0, std.mem.asBytes(&j)));
    a.merge(&b);
    try std.testing.expect(approxEq(a.estimate(), 4500, 0.10));
}

test "hll: empty sketch estimates zero" {
    const h: Hll = .{};
    try std.testing.expectEqual(@as(u64, 0), h.estimate());
}
