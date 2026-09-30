const std = @import("std");
const Allocator = std.mem.Allocator;

/// Copies of the strings that aggregate states keep, carved from the
/// operator's arena.
///
/// `dupe` packs a copy that nothing replaces, such as an ANY_VALUE.
/// `replace` keeps a copy that the state swaps for a later row's, as MAX_BY,
/// MIN/MAX and LAST do. Such a copy takes a block of its length's size class.
/// When the next copy falls in the same class, it overwrites the block in
/// place. Otherwise the block goes on its class's free list, and the next
/// copy of that class takes it. A class never holds more blocks than it once
/// had live copies, so the bank grows with the states' live strings, not
/// with the rows that improved them.
///
/// Every block lives in the arena, so each reset of the arena must be
/// followed by `reset` here.
pub const StringBank = struct {
    chunk: []u8 = &.{},
    used: usize = 0,
    /// Head of each class's free list. A free block's first bytes hold the
    /// next free block of its class.
    free: [CLASS_COUNT]?[*]u8 = @splat(null),
    /// The classes whose free list has had a block, so `reset` clears only
    /// those.
    listed: std.StaticBitSet(CLASS_COUNT) = .initEmpty(),

    /// Small blocks are carved from chunks of the arena, so most copies cost
    /// no arena call. The chunks stay small because the arena already grows
    /// geometrically, an operator can run one bank per partition, and a
    /// request that overflows the arena's current node wastes up to its own
    /// size there.
    const CHUNK_BYTES: usize = 4096;
    /// A block past this size is its own arena allocation.
    const CHUNK_BLOCK_MAX: usize = CHUNK_BYTES / 4;
    const LINK_BYTES = @sizeOf(?[*]u8);

    /// Every multiple of 8 up to 128, then eight classes per doubling, so a
    /// copy longer than 128 bytes wastes under an eighth of its block. The
    /// classes stop at 4 GiB, which a column's strings never reach (their
    /// offsets are u32). A longer copy gets its own allocation and is not
    /// reused.
    const SMALL_CLASSES: usize = 16;
    const MAX_CLASSED: usize = 1 << 32;
    const CLASS_COUNT: usize = SMALL_CLASSES + (32 - 7) * 8;

    comptime {
        std.debug.assert(LINK_BYTES <= 8);
        std.debug.assert(classOf(MAX_CLASSED).? == CLASS_COUNT - 1);
    }

    /// Copy `bytes` for a state that never replaces it.
    pub fn dupe(self: *StringBank, aa: Allocator, bytes: []const u8) ![]const u8 {
        if (bytes.len > CHUNK_BLOCK_MAX) return aa.dupe(u8, bytes);
        const out = try self.carve(aa, bytes.len);
        @memcpy(out, bytes);
        return out;
    }

    /// Copy `bytes` for a state whose previous copy was `old`: a slice an
    /// earlier `replace` on this bank returned, or empty. `old` is no longer
    /// valid afterwards.
    pub fn replace(self: *StringBank, aa: Allocator, old: []const u8, bytes: []const u8) ![]const u8 {
        const old_class = classOf(old.len);
        const new_class = classOf(bytes.len);
        if (new_class != null and old_class != null and new_class.? == old_class.?) {
            const block = @constCast(old.ptr)[0..bytes.len];
            @memcpy(block, bytes);
            return block;
        }
        const out: []const u8 = if (new_class) |class| blk: {
            const block = (try self.take(aa, class))[0..bytes.len];
            @memcpy(block, bytes);
            break :blk block;
        } else if (bytes.len == 0) "" else try aa.dupe(u8, bytes);
        if (old_class) |class| self.release(@constCast(old.ptr), class);
        return out;
    }

    /// Forget every block, after the arena they live in was reset.
    pub fn reset(self: *StringBank) void {
        var listed = self.listed.iterator(.{});
        while (listed.next()) |class| self.free[class] = null;
        self.listed = .initEmpty();
        self.chunk = &.{};
        self.used = 0;
    }

    fn classOf(len: usize) ?usize {
        if (len == 0 or len > MAX_CLASSED) return null;
        if (len <= 128) return (len - 1) / 8;
        const doubling: usize = std.math.log2_int(usize, len - 1);
        const step = ((len - 1) >> @intCast(doubling - 3)) & 7;
        return SMALL_CLASSES + (doubling - 7) * 8 + step;
    }

    fn classBytes(class: usize) usize {
        if (class < SMALL_CLASSES) return (class + 1) * 8;
        const doubling = (class - SMALL_CLASSES) / 8 + 7;
        const step = (class - SMALL_CLASSES) % 8;
        return (9 + step) << @intCast(doubling - 3);
    }

    fn take(self: *StringBank, aa: Allocator, class: usize) ![]u8 {
        const size = classBytes(class);
        if (self.free[class]) |block| {
            var next: ?[*]u8 = undefined;
            @memcpy(std.mem.asBytes(&next), block[0..LINK_BYTES]);
            self.free[class] = next;
            return block[0..size];
        }
        if (size > CHUNK_BLOCK_MAX) return aa.alloc(u8, size);
        return self.carve(aa, size);
    }

    fn release(self: *StringBank, block: [*]u8, class: usize) void {
        @memcpy(block[0..LINK_BYTES], std.mem.asBytes(&self.free[class]));
        self.free[class] = block;
        self.listed.set(class);
    }

    fn carve(self: *StringBank, aa: Allocator, len: usize) ![]u8 {
        if (self.used + len > self.chunk.len) {
            self.chunk = try aa.alloc(u8, CHUNK_BYTES);
            self.used = 0;
        }
        const out = self.chunk[self.used..][0..len];
        self.used += len;
        return out;
    }
};

test "StringBank classes: every multiple of 8 to 128, then eight per doubling" {
    const cases = .{
        .{ 1, 0, 8 },               .{ 8, 0, 8 },        .{ 9, 1, 16 },       .{ 128, 15, 128 },
        .{ 129, 16, 144 },          .{ 144, 16, 144 },   .{ 145, 17, 160 },   .{ 256, 23, 256 },
        .{ 257, 24, 288 },          .{ 1000, 39, 1024 }, .{ 1025, 40, 1152 }, .{ 65536, 87, 65536 },
        .{ 1 << 32, 215, 1 << 32 },
    };
    inline for (cases) |c| {
        const class = StringBank.classOf(c[0]).?;
        try std.testing.expectEqual(@as(usize, c[1]), class);
        try std.testing.expectEqual(@as(usize, c[2]), StringBank.classBytes(class));
    }
    try std.testing.expectEqual(@as(?usize, null), StringBank.classOf(0));
    try std.testing.expectEqual(@as(?usize, null), StringBank.classOf((1 << 32) + 1));
    for (1..5000) |len| {
        const class = StringBank.classOf(len).?;
        const bytes = StringBank.classBytes(class);
        try std.testing.expect(bytes >= len);
        if (class > 0) try std.testing.expect(StringBank.classBytes(class - 1) < len);
    }
}

test "StringBank replace keeps every live copy intact and its memory bounded by the live copies" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    var bank: StringBank = .{};
    var prng = std.Random.DefaultPrng.init(439);
    const random = prng.random();

    const cells = 64;
    var held: [cells][]const u8 = @splat("");
    var want: [cells][300]u8 = undefined;
    var want_len: [cells]usize = @splat(0);
    for (0..200_000) |_| {
        const cell = random.uintLessThan(usize, cells);
        const len = random.uintLessThan(usize, 300);
        for (want[cell][0..len]) |*b| b.* = random.int(u8);
        held[cell] = try bank.replace(aa, held[cell], want[cell][0..len]);
        want_len[cell] = len;
    }
    for (held, want, want_len) |got, w, len| try std.testing.expectEqualSlices(u8, w[0..len], got);
    // A class never holds more blocks than there are cells, 211 KB at most
    // here. A copy per replacement would take about 30 MB.
    try std.testing.expect(arena.queryCapacity() < 512 * 1024);

    const kept = try bank.dupe(aa, "never replaced");
    held[0] = try bank.replace(aa, held[0], "x" ** 100_000);
    const with_large_block = arena.queryCapacity();
    held[0] = try bank.replace(aa, held[0], "y" ** 100_000);
    held[0] = try bank.replace(aa, held[0], "short");
    held[1] = try bank.replace(aa, held[1], "z" ** 100_000);
    try std.testing.expectEqual(with_large_block, arena.queryCapacity());
    try std.testing.expectEqualStrings("short", held[0]);
    try std.testing.expectEqualStrings("z" ** 100_000, held[1]);
    try std.testing.expectEqualStrings("never replaced", kept);

    _ = arena.reset(.retain_capacity);
    bank.reset();
    const fresh = try bank.replace(aa, "", "after reset");
    try std.testing.expectEqualStrings("after reset", fresh);
}
