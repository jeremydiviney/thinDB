const std = @import("std");
const MemoryAccountant = @import("../memory.zig").MemoryAccountant;
const buffer_pool = @import("buffer_pool.zig");
const Allocator = std.mem.Allocator;

/// A retained pool keeps this wrapper alive between executions and attaches
/// the current query while its buffers are in use. Query-owned wrappers stay
/// attached until their last allocation is freed. Each allocation is charged
/// what it holds in the child (`buffer_pool.footprint`), not what was asked.
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

    fn charge(self: *const BudgetAllocator, len: usize, alignment: std.mem.Alignment) usize {
        return buffer_pool.footprint(self.child, len, alignment);
    }

    fn allocFn(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *BudgetAllocator = @ptrCast(@alignCast(ctx));
        const bytes = self.charge(len, alignment);
        if (!self.reserve(bytes)) return null;
        return self.child.rawAlloc(len, alignment, ret_addr) orelse {
            self.release(bytes);
            return null;
        };
    }

    fn resizeFn(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *BudgetAllocator = @ptrCast(@alignCast(ctx));
        const old_charge = self.charge(bytes.len, alignment);
        const new_charge = self.charge(new_len, alignment);
        const growth = new_charge -| old_charge;
        if (growth != 0 and !self.reserve(growth)) return false;
        if (!self.child.rawResize(bytes, alignment, new_len, ret_addr)) {
            if (growth != 0) self.release(growth);
            return false;
        }
        if (new_charge < old_charge) self.release(old_charge - new_charge);
        return true;
    }

    fn remapFn(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *BudgetAllocator = @ptrCast(@alignCast(ctx));
        const old_charge = self.charge(bytes.len, alignment);
        const new_charge = self.charge(new_len, alignment);
        const growth = new_charge -| old_charge;
        if (growth != 0 and !self.reserve(growth)) return null;
        const result = self.child.rawRemap(bytes, alignment, new_len, ret_addr) orelse {
            if (growth != 0) self.release(growth);
            return null;
        };
        if (new_charge < old_charge) self.release(old_charge - new_charge);
        return result;
    }

    fn freeFn(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *BudgetAllocator = @ptrCast(@alignCast(ctx));
        self.child.rawFree(bytes, alignment, ret_addr);
        self.release(self.charge(bytes.len, alignment));
    }
};
