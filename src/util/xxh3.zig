//! XXH3-128, written from the xxHash specification (doc/xxhash_spec.md in
//! Cyan4973/xxHash). std carries only XXH3-64.

const std = @import("std");

const PRIME32_1: u64 = 0x9E3779B1;
const PRIME32_2: u64 = 0x85EBCA77;
const PRIME32_3: u64 = 0xC2B2AE3D;
const PRIME64_1: u64 = 0x9E3779B185EBCA87;
const PRIME64_2: u64 = 0xC2B2AE3D27D4EB4F;
const PRIME64_3: u64 = 0x165667B19E3779F9;
const PRIME64_4: u64 = 0x85EBCA77C2B2AE63;
const PRIME64_5: u64 = 0x27D4EB2F165667C5;
const PRIME_MX1: u64 = 0x165667919E3779F9;
const PRIME_MX2: u64 = 0x9FB21C651E98DF25;

const SECRET_LEN = 192;
const STRIPE_LEN = 64;
const STRIPES_PER_BLOCK = (SECRET_LEN - STRIPE_LEN) / 8;
const BLOCK_LEN = STRIPE_LEN * STRIPES_PER_BLOCK;

const default_secret = [SECRET_LEN]u8{
    0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c,
    0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f,
    0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21,
    0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c,
    0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3,
    0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8,
    0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d,
    0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64,
    0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb,
    0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e,
    0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce,
    0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e,
};

/// The XXH3-128 digest of `input` under `seed`, its high half in the high
/// 64 bits.
pub fn hash128(seed: u64, input: []const u8) u128 {
    const halves = if (input.len == 0)
        empty(seed, &default_secret)
    else if (input.len <= 3)
        upTo3(seed, input, &default_secret)
    else if (input.len <= 8)
        upTo8(seed, input, &default_secret)
    else if (input.len <= 16)
        upTo16(seed, input, &default_secret)
    else if (input.len <= 128)
        upTo128(seed, input, &default_secret)
    else if (input.len <= 240)
        upTo240(seed, input, &default_secret)
    else
        long(input, &derivedSecret(seed));
    return @as(u128, halves.high) << 64 | halves.low;
}

/// `hash128` of `parts` laid end to end in native byte order, its length
/// known at compile time: a fixed-width composite key digests per row with
/// no length dispatch.
pub fn hash128Parts(seed: u64, comptime k: usize, parts: [k]u128) u128 {
    comptime std.debug.assert(k >= 1 and k <= 8);
    var bytes: [16 * k]u8 = undefined;
    inline for (parts, 0..) |p, i| bytes[i * 16 ..][0..16].* = @bitCast(p);
    const halves = if (k == 1) upTo16(seed, &bytes, &default_secret) else upTo128(seed, &bytes, &default_secret);
    return @as(u128, halves.high) << 64 | halves.low;
}

const Halves = struct { low: u64, high: u64 };

fn read32(bytes: []const u8, at: usize) u64 {
    return std.mem.readInt(u32, bytes[at..][0..4], .little);
}

fn read64(bytes: []const u8, at: usize) u64 {
    return std.mem.readInt(u64, bytes[at..][0..8], .little);
}

fn mul128(a: u64, b: u64) u128 {
    return @as(u128, a) * b;
}

fn foldedMul(a: u64, b: u64) u64 {
    const product = mul128(a, b);
    return @as(u64, @truncate(product)) ^ @as(u64, @truncate(product >> 64));
}

fn avalanche(x: u64) u64 {
    var h = x ^ (x >> 37);
    h *%= PRIME_MX1;
    return h ^ (h >> 32);
}

fn avalancheXxh64(x: u64) u64 {
    var h = x ^ (x >> 33);
    h *%= PRIME64_2;
    h ^= h >> 29;
    h *%= PRIME64_3;
    return h ^ (h >> 32);
}

fn empty(seed: u64, secret: *const [SECRET_LEN]u8) Halves {
    return .{
        .low = avalancheXxh64(seed ^ read64(secret, 64) ^ read64(secret, 72)),
        .high = avalancheXxh64(seed ^ read64(secret, 80) ^ read64(secret, 88)),
    };
}

fn upTo3(seed: u64, input: []const u8, secret: *const [SECRET_LEN]u8) Halves {
    const combined: u32 = @as(u32, input[input.len - 1]) | @as(u32, @intCast(input.len)) << 8 |
        @as(u32, input[0]) << 16 | @as(u32, input[input.len >> 1]) << 24;
    const low = ((read32(secret, 0) ^ read32(secret, 4)) +% seed) ^ combined;
    const high = ((read32(secret, 8) ^ read32(secret, 12)) -% seed) ^ std.math.rotl(u32, @byteSwap(combined), 13);
    return .{ .low = avalancheXxh64(low), .high = avalancheXxh64(high) };
}

fn upTo8(seed: u64, input: []const u8, secret: *const [SECRET_LEN]u8) Halves {
    const first = read32(input, 0);
    const last = read32(input, input.len - 4);
    const modified_seed = seed ^ (@as(u64, @byteSwap(@as(u32, @truncate(seed)))) << 32);
    const value = ((read64(secret, 16) ^ read64(secret, 24)) +% modified_seed) ^ (first | last << 32);
    const product = mul128(value, PRIME64_1 +% (@as(u64, input.len) << 2));
    var high: u64 = @truncate(product >> 64);
    var low: u64 = @truncate(product);
    high +%= low << 1;
    low ^= high >> 3;
    low ^= low >> 35;
    low *%= PRIME_MX2;
    low ^= low >> 28;
    return .{ .low = low, .high = avalanche(high) };
}

fn upTo16(seed: u64, input: []const u8, secret: *const [SECRET_LEN]u8) Halves {
    const first = read64(input, 0);
    const last = read64(input, input.len - 8);
    const val1 = ((read64(secret, 32) ^ read64(secret, 40)) -% seed) ^ first ^ last;
    const val2 = ((read64(secret, 48) ^ read64(secret, 56)) +% seed) ^ last;
    const product = mul128(val1, PRIME64_1);
    var low: u64 = @as(u64, @truncate(product)) +% (@as(u64, input.len - 1) << 54);
    var high: u64 = @as(u64, @truncate(product >> 64)) +% ((val2 >> 32) << 32) +% (val2 & 0xFFFF_FFFF) *% PRIME32_2;
    low ^= @byteSwap(high);
    const product2 = mul128(low, PRIME64_2);
    low = @truncate(product2);
    high = @as(u64, @truncate(product2 >> 64)) +% high *% PRIME64_2;
    return .{ .low = avalanche(low), .high = avalanche(high) };
}

fn mixStep(data: []const u8, secret: []const u8, seed: u64) u64 {
    return foldedMul(read64(data, 0) ^ (read64(secret, 0) +% seed), read64(data, 8) ^ (read64(secret, 8) -% seed));
}

fn mixTwoChunks(acc: *[2]u64, data1: []const u8, data2: []const u8, secret: []const u8, seed: u64) void {
    acc[0] +%= mixStep(data1, secret, seed);
    acc[1] +%= mixStep(data2, secret[16..], seed);
    acc[0] ^= read64(data2, 0) +% read64(data2, 8);
    acc[1] ^= read64(data1, 0) +% read64(data1, 8);
}

fn mediumResult(acc: [2]u64, len: usize, seed: u64) Halves {
    const high = acc[0] *% PRIME64_1 +% acc[1] *% PRIME64_4 +% (@as(u64, len) -% seed) *% PRIME64_2;
    return .{ .low = avalanche(acc[0] +% acc[1]), .high = 0 -% avalanche(high) };
}

fn upTo128(seed: u64, input: []const u8, secret: *const [SECRET_LEN]u8) Halves {
    var acc = [2]u64{ @as(u64, input.len) *% PRIME64_1, 0 };
    var i: usize = (input.len - 1) >> 5;
    while (true) : (i -= 1) {
        const end = input.len - i * 16 - 16;
        mixTwoChunks(&acc, input[i * 16 ..][0..16], input[end..][0..16], secret[i * 32 ..], seed);
        if (i == 0) break;
    }
    return mediumResult(acc, input.len, seed);
}

fn upTo240(seed: u64, input: []const u8, secret: *const [SECRET_LEN]u8) Halves {
    var acc = [2]u64{ @as(u64, input.len) *% PRIME64_1, 0 };
    for (0..4) |i| mixTwoChunks(&acc, input[i * 32 ..][0..16], input[i * 32 + 16 ..][0..16], secret[i * 32 ..], seed);
    acc[0] = avalanche(acc[0]);
    acc[1] = avalanche(acc[1]);
    for (4..input.len >> 5) |i| mixTwoChunks(&acc, input[i * 32 ..][0..16], input[i * 32 + 16 ..][0..16], secret[(i - 4) * 32 + 3 ..], seed);
    mixTwoChunks(&acc, input[input.len - 16 ..][0..16], input[input.len - 32 ..][0..16], secret[103..], 0 -% seed);
    return mediumResult(acc, input.len, seed);
}

/// Above 240 bytes a seed reaches the hash only through the secret.
fn derivedSecret(seed: u64) [SECRET_LEN]u8 {
    var secret = default_secret;
    for (0..SECRET_LEN / 16) |i| {
        std.mem.writeInt(u64, secret[i * 16 ..][0..8], read64(&default_secret, i * 16) +% seed, .little);
        std.mem.writeInt(u64, secret[i * 16 + 8 ..][0..8], read64(&default_secret, i * 16 + 8) -% seed, .little);
    }
    return secret;
}

fn accumulate(acc: *[8]u64, stripe: []const u8, secret: []const u8) void {
    for (0..8) |i| {
        const lane = read64(stripe, i * 8);
        const value = lane ^ read64(secret, i * 8);
        acc[i ^ 1] +%= lane;
        acc[i] +%= (value & 0xFFFF_FFFF) *% (value >> 32);
    }
}

fn finalMerge(acc: [8]u64, init: u64, secret: []const u8) u64 {
    var result = init;
    for (0..4) |i| result +%= foldedMul(acc[i * 2] ^ read64(secret, i * 16), acc[i * 2 + 1] ^ read64(secret, i * 16 + 8));
    return avalanche(result);
}

fn long(input: []const u8, secret: *const [SECRET_LEN]u8) Halves {
    var acc = [8]u64{ PRIME32_3, PRIME64_1, PRIME64_2, PRIME64_3, PRIME64_4, PRIME32_2, PRIME64_5, PRIME32_1 };
    const full_blocks = (input.len - 1) / BLOCK_LEN;
    for (0..full_blocks) |b| {
        const block = input[b * BLOCK_LEN ..][0..BLOCK_LEN];
        for (0..STRIPES_PER_BLOCK) |n| accumulate(&acc, block[n * STRIPE_LEN ..], secret[n * 8 ..]);
        for (&acc, 0..) |*a, i| {
            a.* ^= a.* >> 47;
            a.* ^= read64(secret, SECRET_LEN - STRIPE_LEN + i * 8);
            a.* *%= PRIME32_1;
        }
    }
    const last_block = input[full_blocks * BLOCK_LEN ..];
    for (0..(last_block.len - 1) / STRIPE_LEN) |n| accumulate(&acc, last_block[n * STRIPE_LEN ..], secret[n * 8 ..]);
    accumulate(&acc, input[input.len - STRIPE_LEN ..], secret[SECRET_LEN - 71 ..]);
    return .{
        .low = finalMerge(acc, @as(u64, input.len) *% PRIME64_1, secret[11..]),
        .high = finalMerge(acc, ~(@as(u64, input.len) *% PRIME64_2), secret[SECRET_LEN - 75 ..]),
    };
}

test "XXH3-128 matches StarRocks across every input-length path" {
    // StarRocks' XX_HASH3_128 answers, read as signed LARGEINTs.
    const ab8 = "abcdefgh";
    const cases = .{
        .{ "", -136027654687899461258310042000974591617 },
        .{ "a", -115062932358609980327512201381724926433 },
        .{ "abc", 8891052093862885505146213044715469136 },
        .{ "abcd", -152303692184121546813063104137364687555 },
        .{ ab8, -49502339717015306872520941418082463982 },
        .{ "abcdefghi", -100689228862623767862525847912314381416 },
        .{ "abcdefghijklmnop", 41668111349311858976869800786634171184 },
        .{ "abcdefghijklmnopq", 22636066031784431813857569789036654507 },
        .{ ab8 ** 16, 134018816784972153682010339138580759315 },
        .{ ab8 ** 16 ++ "x", -4590305595231998317837289130799445395 },
        .{ ab8 ** 30, -135041434779996440649634732658357321958 },
        .{ ab8 ** 30 ++ "x", 145367417897179468193453102402462419006 },
        .{ ab8 ** 128, 143766852459482183353646331477400323263 },
        .{ ab8 ** 128 ++ "x", 93967746685416702219370960950665292183 },
        .{ ab8 ** 375, -68405818285202680017503919040926716798 },
    };
    inline for (cases) |c| try std.testing.expectEqual(@as(i128, c[1]), @as(i128, @bitCast(hash128(0, c[0]))));
}

test "XXH3-128 over fixed-width parts matches the concatenated bytes" {
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const random = prng.random();
    inline for (1..9) |k| {
        for (0..64) |_| {
            var parts: [k]u128 = undefined;
            for (&parts) |*p| p.* = random.int(u128);
            const bytes: [16 * k]u8 = @bitCast(parts);
            inline for (.{ 0, 0x9E3779B185EBCA87 }) |seed| {
                try std.testing.expectEqual(hash128(seed, &bytes), hash128Parts(seed, k, parts));
            }
        }
    }
}

test "XXH3-128's low half is XXH3-64 where the specification says so" {
    const ab8 = "abcdefgh";
    inline for (.{ "a", "abc", ab8 ** 30 ++ "x", ab8 ** 375 }) |input| {
        inline for (.{ 0, 7, 0x9E3779B185EBCA87 }) |seed| {
            try std.testing.expectEqual(std.hash.XxHash3.hash(seed, input), @as(u64, @truncate(hash128(seed, input))));
        }
    }
}
