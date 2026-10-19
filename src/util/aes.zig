//! AES (FIPS-197) over 128-, 192- and 256-bit keys, in the modes of NIST
//! SP 800-38A and in GCM (SP 800-38D). std carries the key expansion for
//! 128- and 256-bit keys only, and MySQL and StarRocks also take 192-bit
//! ones, so the expansion lives here while the rounds stay std's, on the
//! CPU's AES instructions where it has them.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Block = std.crypto.core.aes.Block;
const Ghash = std.crypto.onetimeauth.Ghash;

pub const BLOCK_LEN = 16;
pub const GCM_NONCE_LEN = 12;
pub const GCM_TAG_LEN = 16;

const SBOX = sbox: {
    @setEvalBranchQuota(10_000);
    var table: [256]u8 = undefined;
    // p walks the multiplicative group by powers of 3 while q walks it by
    // powers of 3's inverse, so q is p's inverse when the affine map runs.
    var p: u8 = 1;
    var q: u8 = 1;
    while (true) {
        p = p ^ (p << 1) ^ (if (p & 0x80 != 0) @as(u8, 0x1b) else 0);
        q ^= q << 1;
        q ^= q << 2;
        q ^= q << 4;
        if (q & 0x80 != 0) q ^= 0x09;
        table[p] = q ^ std.math.rotl(u8, q, 1) ^ std.math.rotl(u8, q, 2) ^ std.math.rotl(u8, q, 3) ^ std.math.rotl(u8, q, 4) ^ 0x63;
        if (p == 1) break;
    }
    table[0] = 0x63;
    break :sbox table;
};

pub const Cipher = struct {
    rounds: usize,
    enc_keys: [15]Block,
    dec_keys: [15]Block,

    /// `key` is 16, 24 or 32 bytes long.
    pub fn init(key: []const u8) Cipher {
        std.debug.assert(key.len == 16 or key.len == 24 or key.len == 32);
        const key_words = key.len / 4;
        const rounds = key_words + 6;
        var words: [60][4]u8 = undefined;
        for (words[0..key_words], 0..) |*w, i| w.* = key[4 * i ..][0..4].*;
        var rcon: u8 = 1;
        for (key_words..4 * (rounds + 1)) |i| {
            const prev = words[i - 1];
            const t: [4]u8 = if (i % key_words == 0)
                .{ SBOX[prev[1]] ^ rcon, SBOX[prev[2]], SBOX[prev[3]], SBOX[prev[0]] }
            else if (key_words > 6 and i % key_words == 4)
                .{ SBOX[prev[0]], SBOX[prev[1]], SBOX[prev[2]], SBOX[prev[3]] }
            else
                prev;
            if (i % key_words == 0) rcon = (rcon << 1) ^ (if (rcon & 0x80 != 0) @as(u8, 0x1b) else 0);
            for (&words[i], words[i - key_words], t) |*w, a, b| w.* = a ^ b;
        }
        var cipher: Cipher = .{ .rounds = rounds, .enc_keys = undefined, .dec_keys = undefined };
        for (cipher.enc_keys[0 .. rounds + 1], 0..) |*rk, r| {
            var bytes: [BLOCK_LEN]u8 = undefined;
            for (0..4) |j| bytes[4 * j ..][0..4].* = words[4 * r + j];
            rk.* = Block.fromBytes(&bytes);
        }
        // The equivalent inverse cipher's keys (FIPS-197 §5.3.5), which the
        // decryption rounds std runs expect.
        cipher.dec_keys[0] = cipher.enc_keys[rounds];
        for (1..rounds) |r| cipher.dec_keys[r] = cipher.enc_keys[rounds - r].invMixColumns();
        cipher.dec_keys[rounds] = cipher.enc_keys[0];
        return cipher;
    }

    pub fn encryptBlock(cipher: *const Cipher, block: [BLOCK_LEN]u8) [BLOCK_LEN]u8 {
        var t = Block.fromBytes(&block).xorBlocks(cipher.enc_keys[0]);
        for (cipher.enc_keys[1..cipher.rounds]) |rk| t = t.encrypt(rk);
        return t.encryptLast(cipher.enc_keys[cipher.rounds]).toBytes();
    }

    pub fn decryptBlock(cipher: *const Cipher, block: [BLOCK_LEN]u8) [BLOCK_LEN]u8 {
        var t = Block.fromBytes(&block).xorBlocks(cipher.dec_keys[0]);
        for (cipher.dec_keys[1..cipher.rounds]) |rk| t = t.decrypt(rk);
        return t.decryptLast(cipher.dec_keys[cipher.rounds]).toBytes();
    }
};

pub const Chaining = enum { ecb, cbc, cfb1, cfb8, cfb128, ofb, ctr, gcm };

const Direction = enum { encrypt, decrypt };

/// Appends `input` encrypted to `out`. ECB and CBC pad it with PKCS#7. GCM
/// takes the first 12 bytes of `iv` as its nonce and writes the nonce, the
/// ciphertext and the 16-byte tag over `aad`, in that order. The other
/// modes ignore `aad`, and ECB ignores `iv`.
pub fn encrypt(allocator: Allocator, cipher: *const Cipher, chaining: Chaining, input: []const u8, iv: [BLOCK_LEN]u8, aad: []const u8, out: *std.ArrayList(u8)) Allocator.Error!void {
    switch (chaining) {
        .ecb, .cbc => {
            const pad: u8 = @intCast(BLOCK_LEN - input.len % BLOCK_LEN);
            const padded_len = input.len + pad;
            try out.ensureUnusedCapacity(allocator, padded_len);
            var prev = iv;
            var at: usize = 0;
            while (at < padded_len) : (at += BLOCK_LEN) {
                var block: [BLOCK_LEN]u8 = undefined;
                for (&block, at..) |*b, i| b.* = if (i < input.len) input[i] else pad;
                if (chaining == .cbc) xorInto(&block, prev);
                prev = cipher.encryptBlock(block);
                out.appendSliceAssumeCapacity(&prev);
            }
        },
        .gcm => {
            try out.ensureUnusedCapacity(allocator, GCM_NONCE_LEN + input.len + GCM_TAG_LEN);
            const nonce = iv[0..GCM_NONCE_LEN].*;
            out.appendSliceAssumeCapacity(&nonce);
            const sealed = out.unusedCapacitySlice()[0..input.len];
            gcmCounter(cipher, nonce, input, sealed);
            out.items.len += input.len;
            out.appendSliceAssumeCapacity(&gcmTag(cipher, nonce, aad, sealed));
        },
        inline .cfb1, .cfb8, .cfb128, .ofb, .ctr => |mode| {
            try out.ensureUnusedCapacity(allocator, input.len);
            stream(cipher, mode, .encrypt, input, iv, out.unusedCapacitySlice()[0..input.len]);
            out.items.len += input.len;
        },
    }
}

/// Appends `input` decrypted to `out`, reading it as `encrypt` writes it.
/// False, with `out` as it was, for input that `encrypt` can't have
/// written under this key: ECB or CBC of a length that isn't a whole
/// number of blocks or with broken padding, or GCM whose tag doesn't match.
pub fn decrypt(allocator: Allocator, cipher: *const Cipher, chaining: Chaining, input: []const u8, iv: [BLOCK_LEN]u8, aad: []const u8, out: *std.ArrayList(u8)) Allocator.Error!bool {
    switch (chaining) {
        .ecb, .cbc => {
            if (input.len == 0 or input.len % BLOCK_LEN != 0) return false;
            try out.ensureUnusedCapacity(allocator, input.len);
            const start = out.items.len;
            var prev = iv;
            var at: usize = 0;
            while (at < input.len) : (at += BLOCK_LEN) {
                const block = input[at..][0..BLOCK_LEN].*;
                var plain = cipher.decryptBlock(block);
                if (chaining == .cbc) xorInto(&plain, prev);
                prev = block;
                out.appendSliceAssumeCapacity(&plain);
            }
            const pad = out.items[out.items.len - 1];
            const padding_ok = pad >= 1 and pad <= BLOCK_LEN and for (out.items[out.items.len - pad ..]) |b| {
                if (b != pad) break false;
            } else true;
            out.items.len = if (padding_ok) out.items.len - pad else start;
            return padding_ok;
        },
        .gcm => {
            if (input.len < GCM_NONCE_LEN + GCM_TAG_LEN) return false;
            const nonce = input[0..GCM_NONCE_LEN].*;
            const sealed = input[GCM_NONCE_LEN .. input.len - GCM_TAG_LEN];
            const tag = input[input.len - GCM_TAG_LEN ..][0..GCM_TAG_LEN].*;
            if (!std.crypto.timing_safe.eql([GCM_TAG_LEN]u8, gcmTag(cipher, nonce, aad, sealed), tag)) return false;
            try out.ensureUnusedCapacity(allocator, sealed.len);
            gcmCounter(cipher, nonce, sealed, out.unusedCapacitySlice()[0..sealed.len]);
            out.items.len += sealed.len;
            return true;
        },
        inline .cfb1, .cfb8, .cfb128, .ofb, .ctr => |mode| {
            try out.ensureUnusedCapacity(allocator, input.len);
            stream(cipher, mode, .decrypt, input, iv, out.unusedCapacitySlice()[0..input.len]);
            out.items.len += input.len;
            return true;
        },
    }
}

/// The stream modes, which XOR the input with a keystream. CFB feeds its
/// ciphertext back, the output when encrypting and the input when
/// decrypting; OFB feeds back the keystream; CTR counts up from `iv`.
fn stream(cipher: *const Cipher, comptime mode: Chaining, comptime direction: Direction, input: []const u8, iv: [BLOCK_LEN]u8, dest: []u8) void {
    var register = iv;
    switch (mode) {
        .cfb128, .ofb, .ctr => {
            var at: usize = 0;
            while (at < input.len) : (at += BLOCK_LEN) {
                const keystream = cipher.encryptBlock(register);
                const n = @min(BLOCK_LEN, input.len - at);
                for (dest[at..][0..n], input[at..][0..n], keystream[0..n]) |*d, in, k| d.* = in ^ k;
                switch (mode) {
                    .cfb128 => @memcpy(register[0..n], if (direction == .encrypt) dest[at..][0..n] else input[at..][0..n]),
                    .ofb => register = keystream,
                    .ctr => std.mem.writeInt(u128, &register, std.mem.readInt(u128, &register, .big) +% 1, .big),
                    else => comptime unreachable,
                }
            }
        },
        .cfb8 => for (dest, input) |*d, in| {
            d.* = in ^ cipher.encryptBlock(register)[0];
            std.mem.copyForwards(u8, register[0 .. BLOCK_LEN - 1], register[1..]);
            register[BLOCK_LEN - 1] = if (direction == .encrypt) d.* else in;
        },
        .cfb1 => for (dest, input) |*d, in| {
            d.* = 0;
            for (0..8) |k| {
                const shift: u3 = @intCast(7 - k);
                const in_bit = (in >> shift) & 1;
                const out_bit = in_bit ^ (cipher.encryptBlock(register)[0] >> 7);
                d.* |= out_bit << shift;
                shiftInBit(&register, if (direction == .encrypt) out_bit else in_bit);
            }
        },
        else => comptime unreachable,
    }
}

fn shiftInBit(register: *[BLOCK_LEN]u8, bit: u8) void {
    std.mem.writeInt(u128, register, (std.mem.readInt(u128, register, .big) << 1) | bit, .big);
}

fn xorInto(block: *[BLOCK_LEN]u8, other: [BLOCK_LEN]u8) void {
    for (block, other) |*b, o| b.* ^= o;
}

fn gcmCounterBlock(nonce: [GCM_NONCE_LEN]u8, count: u32) [BLOCK_LEN]u8 {
    var block: [BLOCK_LEN]u8 = undefined;
    block[0..GCM_NONCE_LEN].* = nonce;
    std.mem.writeInt(u32, block[GCM_NONCE_LEN..], count, .big);
    return block;
}

/// GCM's keystream, which starts at counter 2 since counter 1 masks the tag.
fn gcmCounter(cipher: *const Cipher, nonce: [GCM_NONCE_LEN]u8, input: []const u8, dest: []u8) void {
    var count: u32 = 2;
    var at: usize = 0;
    while (at < input.len) : (at += BLOCK_LEN) {
        const keystream = cipher.encryptBlock(gcmCounterBlock(nonce, count));
        count +%= 1;
        const n = @min(BLOCK_LEN, input.len - at);
        for (dest[at..][0..n], input[at..][0..n], keystream[0..n]) |*d, in, k| d.* = in ^ k;
    }
}

fn gcmTag(cipher: *const Cipher, nonce: [GCM_NONCE_LEN]u8, aad: []const u8, sealed: []const u8) [GCM_TAG_LEN]u8 {
    const hash_key = cipher.encryptBlock(@splat(0));
    var mac = Ghash.init(&hash_key);
    mac.update(aad);
    mac.pad();
    mac.update(sealed);
    mac.pad();
    var lengths: [BLOCK_LEN]u8 = undefined;
    std.mem.writeInt(u64, lengths[0..8], @as(u64, aad.len) * 8, .big);
    std.mem.writeInt(u64, lengths[8..16], @as(u64, sealed.len) * 8, .big);
    mac.update(&lengths);
    var tag: [GCM_TAG_LEN]u8 = undefined;
    mac.final(&tag);
    xorInto(&tag, cipher.encryptBlock(gcmCounterBlock(nonce, 1)));
    return tag;
}

fn hexBytes(comptime hex: []const u8) [hex.len / 2]u8 {
    var bytes: [hex.len / 2]u8 = undefined;
    _ = std.fmt.hexToBytes(&bytes, hex) catch unreachable;
    return bytes;
}

test "AES-128, -192 and -256 match FIPS-197's examples and invert" {
    const plain = hexBytes("00112233445566778899aabbccddeeff");
    const key = hexBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
    const cases = .{
        .{ 16, "69c4e0d86a7b0430d8cdb78070b4c55a" },
        .{ 24, "dda97ca4864cdfe06eaf70a0ec0d7191" },
        .{ 32, "8ea2b7ca516745bfeafc49904b496089" },
    };
    inline for (cases) |c| {
        const cipher = Cipher.init(key[0..c[0]]);
        const sealed = cipher.encryptBlock(plain);
        try std.testing.expectEqualSlices(u8, &hexBytes(c[1]), &sealed);
        try std.testing.expectEqualSlices(u8, &plain, &cipher.decryptBlock(sealed));
    }
}

test "the SP 800-38A modes match its AES-128 examples" {
    const allocator = std.testing.allocator;
    const cipher = Cipher.init(&hexBytes("2b7e151628aed2a6abf7158809cf4f3c"));
    const iv = hexBytes("000102030405060708090a0b0c0d0e0f");
    const plain = hexBytes("6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e51" ++
        "30c81c46a35ce411e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710");
    const cases = .{
        .{ Chaining.ecb, iv, "3ad77bb40d7a3660a89ecaf32466ef97f5d3d58503b9699de785895a96fdbaaf" ++
            "43b1cd7f598ece23881b00e3ed0306887b0c785e27e8ad3f8223207104725dd4" },
        .{ Chaining.cbc, iv, "7649abac8119b246cee98e9b12e9197d5086cb9b507219ee95db113a917678b2" ++
            "73bed6b8e3c1743b7116e69e222295163ff1caa1681fac09120eca307586e1a7" },
        .{ Chaining.cfb128, iv, "3b3fd92eb72dad20333449f8e83cfb4ac8a64537a0b3a93fcde3cdad9f1ce58b" ++
            "26751f67a3cbb140b1808cf187a4f4dfc04b05357c5d1c0eeac4c66f9ff7f2e6" },
        .{ Chaining.ofb, iv, "3b3fd92eb72dad20333449f8e83cfb4a7789508d16918f03f53c52dac54ed825" ++
            "9740051e9c5fecf64344f7a82260edcc304c6528f659c77866a510d9c1d6ae5e" },
        .{ Chaining.ctr, hexBytes("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff"), "874d6191b620e3261bef6864990db6ce9806f66b7970fdff8617187bb9fffdff" ++
            "5ae4df3edbd5d35e5b4f09020db03eab1e031dda2fbe03d1792170a0f3009cee" },
    };
    inline for (cases) |c| {
        var sealed: std.ArrayList(u8) = .empty;
        defer sealed.deinit(allocator);
        try encrypt(allocator, &cipher, c[0], &plain, c[1], "", &sealed);
        try std.testing.expectEqualSlices(u8, &hexBytes(c[2]), sealed.items[0..plain.len]);
    }
    var cfb8: std.ArrayList(u8) = .empty;
    defer cfb8.deinit(allocator);
    try encrypt(allocator, &cipher, .cfb8, plain[0..18], iv, "", &cfb8);
    try std.testing.expectEqualSlices(u8, &hexBytes("3b79424c9c0dd436bace9e0ed4586a4f32b9"), cfb8.items);
}

test "GCM agrees with std's AES-GCM, and a changed byte fails its tag" {
    const allocator = std.testing.allocator;
    var key: [32]u8 = undefined;
    for (&key, 0..) |*b, i| b.* = @intCast(i * 7 + 3);
    const nonce = hexBytes("cafebabefacedbaddecaf888");
    const plain = "a message longer than two blocks of sixteen bytes";
    const aad = "header";
    var iv: [BLOCK_LEN]u8 = @splat(0);
    iv[0..GCM_NONCE_LEN].* = nonce;
    inline for (.{ std.crypto.aead.aes_gcm.Aes128Gcm, std.crypto.aead.aes_gcm.Aes256Gcm }) |Std| {
        var expected: [plain.len]u8 = undefined;
        var expected_tag: [GCM_TAG_LEN]u8 = undefined;
        Std.encrypt(&expected, &expected_tag, plain, aad, nonce, key[0..Std.key_length].*);
        const cipher = Cipher.init(key[0..Std.key_length]);
        var sealed: std.ArrayList(u8) = .empty;
        defer sealed.deinit(allocator);
        try encrypt(allocator, &cipher, .gcm, plain, iv, aad, &sealed);
        try std.testing.expectEqualSlices(u8, &nonce, sealed.items[0..GCM_NONCE_LEN]);
        try std.testing.expectEqualSlices(u8, &expected, sealed.items[GCM_NONCE_LEN..][0..plain.len]);
        try std.testing.expectEqualSlices(u8, &expected_tag, sealed.items[GCM_NONCE_LEN + plain.len ..]);

        var opened: std.ArrayList(u8) = .empty;
        defer opened.deinit(allocator);
        try std.testing.expect(try decrypt(allocator, &cipher, .gcm, sealed.items, iv, aad, &opened));
        try std.testing.expectEqualStrings(plain, opened.items);
        opened.clearRetainingCapacity();
        try std.testing.expect(!try decrypt(allocator, &cipher, .gcm, sealed.items, iv, "other", &opened));
        sealed.items[GCM_NONCE_LEN] ^= 1;
        try std.testing.expect(!try decrypt(allocator, &cipher, .gcm, sealed.items, iv, aad, &opened));
        try std.testing.expectEqual(@as(usize, 0), opened.items.len);
    }
}

test "every mode and key length decrypts what it encrypts" {
    const allocator = std.testing.allocator;
    var key: [32]u8 = undefined;
    for (&key, 0..) |*b, i| b.* = @intCast(255 - i);
    var plain: [40]u8 = undefined;
    for (&plain, 0..) |*b, i| b.* = @truncate(i * 13);
    const iv = hexBytes("0f0e0d0c0b0a09080706050403020100");
    for ([_]usize{ 16, 24, 32 }) |key_len| {
        const cipher = Cipher.init(key[0..key_len]);
        inline for (comptime std.enums.values(Chaining)) |chaining| {
            for ([_]usize{ 0, 1, 15, 16, 17, 40 }) |len| {
                var sealed: std.ArrayList(u8) = .empty;
                defer sealed.deinit(allocator);
                var opened: std.ArrayList(u8) = .empty;
                defer opened.deinit(allocator);
                try encrypt(allocator, &cipher, chaining, plain[0..len], iv, "aad", &sealed);
                try std.testing.expect(try decrypt(allocator, &cipher, chaining, sealed.items, iv, "aad", &opened));
                try std.testing.expectEqualSlices(u8, plain[0..len], opened.items);
            }
        }
    }
}

test "ECB and CBC refuse a ragged length or broken padding" {
    const allocator = std.testing.allocator;
    const cipher = Cipher.init(&hexBytes("2b7e151628aed2a6abf7158809cf4f3c"));
    const iv: [BLOCK_LEN]u8 = @splat(0);
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    try std.testing.expect(!try decrypt(allocator, &cipher, .ecb, "", iv, "", &out));
    try std.testing.expect(!try decrypt(allocator, &cipher, .cbc, "seventeen bytes!!", iv, "", &out));
    const zero_pad = cipher.encryptBlock(@splat(0));
    try std.testing.expect(!try decrypt(allocator, &cipher, .ecb, &zero_pad, iv, "", &out));
    try std.testing.expectEqual(@as(usize, 0), out.items.len);
}
