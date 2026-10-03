//! An arena whose first block is one allocation of a size known up front.
//! For moving finished buffers out of an arena that grew them: carved back
//! to back from the slab, they hold their live bytes and nothing of their
//! growth history. Allocations past the slab go to a
//! `std.heap.ArenaAllocator`, so a slab-less SlabArena is exactly that.
//!
//! The slab is sized by the caller because `std.heap.ArenaAllocator` sizes
//! a node at 1.5x (previous node + request): one exact request still gets
//! half as much again.
//!
//! Single owner: one thread at a time.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;

pub const SlabArena = struct {
    backing: Allocator,
    slab: []u8 = &.{},
    /// Bytes of the slab handed out (the next allocation starts here,
    /// aligned forward).
    end: usize = 0,
    overflow: std.heap.ArenaAllocator,

    const slab_alignment: Alignment = .@"16";

    pub fn init(backing: Allocator) SlabArena {
        return .{ .backing = backing, .overflow = .init(backing) };
    }

    pub fn initSlab(backing: Allocator, len: usize) Allocator.Error!SlabArena {
        var self = init(backing);
        if (len > 0) {
            const ptr = backing.rawAlloc(len, slab_alignment, @returnAddress()) orelse return error.OutOfMemory;
            self.slab = ptr[0..len];
        }
        return self;
    }

    pub fn deinit(self: *SlabArena) void {
        if (self.slab.len > 0) self.backing.rawFree(self.slab, slab_alignment, @returnAddress());
        self.overflow.deinit();
        self.* = undefined;
    }

    pub fn allocator(self: *SlabArena) Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// Bytes held from the backing allocator: the whole slab plus the
    /// overflow arena's nodes.
    pub fn queryCapacity(self: SlabArena) usize {
        return self.slab.len + self.overflow.queryCapacity();
    }

    const vtable = Allocator.VTable{
        .alloc = allocFn,
        .resize = resizeFn,
        .remap = remapFn,
        .free = freeFn,
    };

    fn inSlab(self: *const SlabArena, memory: []u8) bool {
        const p = @intFromPtr(memory.ptr);
        const base = @intFromPtr(self.slab.ptr);
        return p >= base and p < base + self.slab.len;
    }

    fn isNewest(self: *const SlabArena, memory: []u8) bool {
        return @intFromPtr(memory.ptr) + memory.len == @intFromPtr(self.slab.ptr) + self.end;
    }

    fn allocFn(ctx: *anyopaque, len: usize, alignment: Alignment, ret_addr: usize) ?[*]u8 {
        const self: *SlabArena = @ptrCast(@alignCast(ctx));
        const base = @intFromPtr(self.slab.ptr);
        const start = alignment.forward(base + self.end) - base;
        if (start + len <= self.slab.len) {
            self.end = start + len;
            return self.slab.ptr + start;
        }
        return self.overflow.allocator().rawAlloc(len, alignment, ret_addr);
    }

    fn resizeFn(ctx: *anyopaque, memory: []u8, alignment: Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *SlabArena = @ptrCast(@alignCast(ctx));
        if (!self.inSlab(memory)) return self.overflow.allocator().rawResize(memory, alignment, new_len, ret_addr);
        if (!self.isNewest(memory)) return new_len <= memory.len;
        const start = @intFromPtr(memory.ptr) - @intFromPtr(self.slab.ptr);
        if (start + new_len > self.slab.len) return false;
        self.end = start + new_len;
        return true;
    }

    fn remapFn(ctx: *anyopaque, memory: []u8, alignment: Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *SlabArena = @ptrCast(@alignCast(ctx));
        if (!self.inSlab(memory)) return self.overflow.allocator().rawRemap(memory, alignment, new_len, ret_addr);
        return if (resizeFn(ctx, memory, alignment, new_len, ret_addr)) memory.ptr else null;
    }

    fn freeFn(ctx: *anyopaque, memory: []u8, alignment: Alignment, ret_addr: usize) void {
        const self: *SlabArena = @ptrCast(@alignCast(ctx));
        if (!self.inSlab(memory)) return self.overflow.allocator().rawFree(memory, alignment, ret_addr);
        if (self.isNewest(memory)) self.end = @intFromPtr(memory.ptr) - @intFromPtr(self.slab.ptr);
    }
};

test "slab arena: buffers carved to the slab's size leave nothing in the overflow" {
    var arena = try SlabArena.initSlab(std.testing.allocator, 3 * 64 * @sizeOf(u64));
    defer arena.deinit();
    const a = arena.allocator();

    var lists: [3]std.ArrayList(u64) = @splat(.empty);
    for (&lists) |*l| try l.ensureTotalCapacityPrecise(a, 64);
    for (&lists, 0..) |*l, k| {
        for (0..64) |i| l.appendAssumeCapacity(k * 1000 + i);
    }

    try std.testing.expectEqual(arena.slab.len, arena.end);
    try std.testing.expectEqual(arena.slab.len, arena.queryCapacity());
    for (lists, 0..) |l, k| try std.testing.expectEqual(@as(u64, k * 1000 + 63), l.items[63]);
}

test "slab arena: a buffer that outgrows the slab moves to the overflow intact" {
    var arena = try SlabArena.initSlab(std.testing.allocator, 2 * 16 * @sizeOf(u32));
    defer arena.deinit();
    const a = arena.allocator();

    var first: std.ArrayList(u32) = .empty;
    var second: std.ArrayList(u32) = .empty;
    try first.ensureTotalCapacityPrecise(a, 16);
    try second.ensureTotalCapacityPrecise(a, 16);
    for (0..16) |i| first.appendAssumeCapacity(@intCast(i));
    // Not the newest slab allocation: growth cannot happen in place.
    for (16..10_000) |i| try first.append(a, @intCast(i));

    try std.testing.expect(!arena.inSlab(std.mem.sliceAsBytes(first.allocatedSlice())));
    try std.testing.expect(arena.overflow.queryCapacity() > 0);
    for (first.items, 0..) |v, i| try std.testing.expectEqual(@as(u32, @intCast(i)), v);
}

test "slab arena: the newest slab allocation grows, shrinks and frees in place" {
    var arena = try SlabArena.initSlab(std.testing.allocator, 1024);
    defer arena.deinit();
    const a = arena.allocator();

    const kept = try a.alloc(u8, 100);
    var newest = try a.alloc(u8, 100);
    try std.testing.expect(a.resize(newest, 300));
    newest = newest.ptr[0..300];
    try std.testing.expectEqual(@as(usize, 400), arena.end);
    try std.testing.expect(!a.resize(newest, 2000));
    a.free(newest);
    try std.testing.expectEqual(@as(usize, 100), arena.end);
    // Not the newest: a free leaves it in place, a shrink succeeds, growth fails.
    const top = try a.alloc(u8, 8);
    a.free(kept);
    try std.testing.expect(a.resize(kept, 50));
    try std.testing.expect(!a.resize(kept, 101));
    try std.testing.expectEqual(@intFromPtr(top.ptr) + top.len - @intFromPtr(arena.slab.ptr), arena.end);
    try std.testing.expectEqual(@as(usize, 0), arena.overflow.queryCapacity());
}

test "slab arena: without a slab it allocates like a plain arena" {
    var arena = SlabArena.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var list: std.ArrayList(u64) = .empty;
    for (0..1000) |i| try list.append(a, i);
    try std.testing.expectEqual(@as(usize, 0), arena.slab.len);
    try std.testing.expect(arena.queryCapacity() >= list.capacity * @sizeOf(u64));
    try std.testing.expectEqual(@as(u64, 999), list.items[999]);
}
