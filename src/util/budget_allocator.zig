const std = @import("std");
const MemoryAccountant = @import("../memory.zig").MemoryAccountant;
const Allocator = std.mem.Allocator;

/// A retained pool keeps this wrapper alive between executions and attaches
/// the current query while its buffers are in use. Query-owned wrappers stay
/// attached until their last allocation is freed.
pub const BudgetAllocator = struct {
    child: Allocator,
    active: ?*MemoryAccountant = null,
    live_bytes: std.atomic.Value(usize) = .init(0),

    pub fn init(child: Allocator) BudgetAllocator {
        return .{ .child = child };
    }

    pub fn allocator(self: *BudgetAllocator) Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn attach(self: *BudgetAllocator, accountant: *MemoryAccountant) !void {
        std.debug.assert(self.active == null);
        try accountant.reserveAllocation(self.live_bytes.load(.monotonic));
        self.active = accountant;
    }

    pub fn detach(self: *BudgetAllocator) void {
        const accountant = self.active orelse return;
        self.active = null;
        accountant.releaseAllocation(self.live_bytes.load(.monotonic));
    }

    pub fn accountantOf(alloc: Allocator) ?*MemoryAccountant {
        if (alloc.vtable != &vtable) return null;
        const self: *BudgetAllocator = @ptrCast(@alignCast(alloc.ptr));
        return self.active;
    }

    const vtable: Allocator.VTable = .{
        .alloc = allocFn,
        .resize = resizeFn,
        .remap = remapFn,
        .free = freeFn,
    };

    fn reserve(self: *BudgetAllocator, bytes: usize) bool {
        if (self.active) |accountant| accountant.reserveAllocation(bytes) catch return false;
        _ = self.live_bytes.fetchAdd(bytes, .monotonic);
        return true;
    }

    fn release(self: *BudgetAllocator, bytes: usize) void {
        const accountant = self.active;
        const previous = self.live_bytes.fetchSub(bytes, .monotonic);
        std.debug.assert(previous >= bytes);
        if (accountant) |a| a.releaseAllocation(bytes);
    }

    fn allocFn(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *BudgetAllocator = @ptrCast(@alignCast(ctx));
        if (!self.reserve(len)) return null;
        return self.child.rawAlloc(len, alignment, ret_addr) orelse {
            self.release(len);
            return null;
        };
    }

    fn resizeFn(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *BudgetAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| bytes.len;
        if (growth != 0 and !self.reserve(growth)) return false;
        if (!self.child.rawResize(bytes, alignment, new_len, ret_addr)) {
            if (growth != 0) self.release(growth);
            return false;
        }
        if (new_len < bytes.len) self.release(bytes.len - new_len);
        return true;
    }

    fn remapFn(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *BudgetAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| bytes.len;
        if (growth != 0 and !self.reserve(growth)) return null;
        const result = self.child.rawRemap(bytes, alignment, new_len, ret_addr) orelse {
            if (growth != 0) self.release(growth);
            return null;
        };
        if (new_len < bytes.len) self.release(bytes.len - new_len);
        return result;
    }

    fn freeFn(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *BudgetAllocator = @ptrCast(@alignCast(ctx));
        self.child.rawFree(bytes, alignment, ret_addr);
        self.release(bytes.len);
    }
};
