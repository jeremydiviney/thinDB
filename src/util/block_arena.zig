//! An arena for a handful of large, growing buffers: one sweep frees them
//! all, but each block is its own backing allocation at the size requested,
//! and a block that grows moves through the backing allocator, which frees
//! the buffer it outgrew.
//!
//! Why not `std.heap.ArenaAllocator`: it sizes a new node at 1.5x (previous
//! node + request) and cannot return a block that is not its newest. A column
//! that grows inside one asks for 1.5x what it needs (the list's own growth),
//! gets a node 1.5x larger again, and leaves every buffer it outgrew stranded
//! in an earlier node until the arena dies. Measured on a window operator
//! accumulating its input, that held 2-4x the live bytes: 10.5 GiB charged
//! to the statement for 5.2 GiB ever written.
//!
//! Single owner: one thread at a time. Blocks are found by a linear scan, so
//! this suits an owner with a few blocks (a column store has at most five),
//! not a general-purpose arena of small objects.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;

pub const BlockArena = struct {
    backing: Allocator,
    blocks: std.ArrayList(Block) = .empty,

    const Block = struct {
        ptr: [*]u8,
        len: usize,
        alignment: Alignment,
    };

    pub fn init(backing: Allocator) BlockArena {
        return .{ .backing = backing };
    }

    pub fn deinit(self: *BlockArena) void {
        for (self.blocks.items) |b| self.backing.rawFree(b.ptr[0..b.len], b.alignment, @returnAddress());
        self.blocks.deinit(self.backing);
        self.* = undefined;
    }

    pub fn allocator(self: *BlockArena) Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// Bytes the live blocks hold.
    pub fn queryCapacity(self: BlockArena) usize {
        var n: usize = 0;
        for (self.blocks.items) |b| n += b.len;
        return n;
    }

    const vtable = Allocator.VTable{
        .alloc = allocFn,
        .resize = resizeFn,
        .remap = remapFn,
        .free = freeFn,
    };

    fn indexOf(self: *const BlockArena, ptr: [*]u8) usize {
        for (self.blocks.items, 0..) |b, i| {
            if (b.ptr == ptr) return i;
        }
        unreachable; // the Allocator contract: `ptr` came from this arena
    }

    fn allocFn(ctx: *anyopaque, len: usize, alignment: Alignment, ret_addr: usize) ?[*]u8 {
        const self: *BlockArena = @ptrCast(@alignCast(ctx));
        self.blocks.ensureUnusedCapacity(self.backing, 1) catch return null;
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.blocks.appendAssumeCapacity(.{ .ptr = ptr, .len = len, .alignment = alignment });
        return ptr;
    }

    fn resizeFn(ctx: *anyopaque, memory: []u8, alignment: Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *BlockArena = @ptrCast(@alignCast(ctx));
        const i = self.indexOf(memory.ptr);
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.blocks.items[i].len = new_len;
        return true;
    }

    fn remapFn(ctx: *anyopaque, memory: []u8, alignment: Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *BlockArena = @ptrCast(@alignCast(ctx));
        const i = self.indexOf(memory.ptr);
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.blocks.items[i].ptr = ptr;
        self.blocks.items[i].len = new_len;
        return ptr;
    }

    fn freeFn(ctx: *anyopaque, memory: []u8, alignment: Alignment, ret_addr: usize) void {
        const self: *BlockArena = @ptrCast(@alignCast(ctx));
        _ = self.blocks.swapRemove(self.indexOf(memory.ptr));
        self.backing.rawFree(memory, alignment, ret_addr);
    }
};

test "block arena: a growing list holds one block of its own capacity" {
    var arena = BlockArena.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var list: std.ArrayList(u64) = .empty;
    for (0..100_000) |i| try list.append(a, i);

    try std.testing.expectEqual(@as(usize, 1), arena.blocks.items.len);
    try std.testing.expectEqual(list.capacity * @sizeOf(u64), arena.queryCapacity());
    for (list.items, 0..) |v, i| try std.testing.expectEqual(@as(u64, i), v);
}

test "block arena: deinit frees every live block, free returns one early" {
    var arena = BlockArena.init(std.testing.allocator);
    const a = arena.allocator();

    const kept = try a.alloc(u8, 4096);
    const dropped = try a.alloc(u32, 1024);
    var grown = try a.alloc(u16, 8);
    grown = try a.realloc(grown, 50_000);
    @memset(kept, 7);
    @memset(grown, 9);

    a.free(dropped);
    try std.testing.expectEqual(@as(usize, 2), arena.blocks.items.len);
    try std.testing.expectEqual(kept.len + grown.len * @sizeOf(u16), arena.queryCapacity());

    // std.testing.allocator fails the test if the sweep misses a block.
    arena.deinit();
}

test "block arena: a refused allocation leaves the arena unchanged" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 2 });
    var arena = BlockArena.init(failing.allocator());
    defer arena.deinit();
    const a = arena.allocator();

    const first = try a.alloc(u8, 64);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 64));
    try std.testing.expectEqual(@as(usize, 1), arena.blocks.items.len);
    try std.testing.expectEqual(first.len, arena.queryCapacity());
}
