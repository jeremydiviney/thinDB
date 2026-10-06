//! Test allocator wrapper that overwrites memory as it is freed. The debug
//! allocator leaves a freed slot's bytes in place until its whole page goes
//! back to the OS, so a read through a dangling slice usually still sees the
//! old values; through this wrapper it sees `POISON` instead.

const std = @import("std");

pub const POISON: u8 = 0xAA;

pub const PoisonOnFree = struct {
    child: std.mem.Allocator,

    pub fn allocator(self: *PoisonOnFree) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = std.mem.Allocator.VTable{
        .alloc = allocFn,
        .resize = resizeFn,
        .remap = remapFn,
        .free = freeFn,
    };

    fn allocFn(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *PoisonOnFree = @ptrCast(@alignCast(ctx));
        return self.child.rawAlloc(len, alignment, ret_addr);
    }

    fn resizeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *PoisonOnFree = @ptrCast(@alignCast(ctx));
        if (!self.child.rawResize(memory, alignment, new_len, ret_addr)) return false;
        if (new_len < memory.len) @memset(memory[new_len..], POISON);
        return true;
    }

    // Moving in place would leave the old bytes readable where a stale slice
    // still points, so a remap always falls back to alloc + copy + free.
    fn remapFn(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }

    fn freeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *PoisonOnFree = @ptrCast(@alignCast(ctx));
        @memset(memory, POISON);
        self.child.rawFree(memory, alignment, ret_addr);
    }
};
