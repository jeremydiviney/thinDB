const std = @import("std");
const MemoryAccountant = @import("../memory.zig").MemoryAccountant;
const buffer_pool = @import("buffer_pool.zig");
const Allocator = std.mem.Allocator;

/// A retained pool keeps this wrapper alive between executions and attaches
/// the current query while its buffers are in use. Query-owned wrappers stay
/// attached until their last allocation is freed. What is built on a retained
/// wrapper outlives the query attached at the time, so it keeps neither that
/// accountant nor anything charged to it (`ownerOf`, `isRetained`). Each
/// allocation is charged what it holds in the child
/// (`buffer_pool.footprint`), not what was asked.
/// A free on a thread collecting `DeferredFrees` drops the charge at once and
/// leaves the memory to the collector.
pub const BudgetAllocator = struct {
    child: Allocator,
    active: ?*MemoryAccountant = null,
    live_bytes: std.atomic.Value(usize) = .init(0),
    /// Set on the wrapper a query creates for itself (`wrapAllocator`).
    query_owned: bool = false,

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

    fn of(alloc: Allocator) ?*BudgetAllocator {
        if (alloc.vtable != &vtable) return null;
        return @ptrCast(@alignCast(alloc.ptr));
    }

    /// The query `alloc` is charged to right now.
    pub fn accountantOf(alloc: Allocator) ?*MemoryAccountant {
        const self = of(alloc) orelse return null;
        return self.active;
    }

    /// The query `alloc` belongs to for as long as the allocator lives: the
    /// only accountant that may be kept past the current call. Null for a
    /// retained wrapper, whose accountant is whichever query is running.
    pub fn ownerOf(alloc: Allocator) ?*MemoryAccountant {
        const self = of(alloc) orelse return null;
        return if (self.query_owned) self.active else null;
    }

    pub fn isRetained(alloc: Allocator) bool {
        const self = of(alloc) orelse return false;
        return !self.query_owned;
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
        const bytes_charged = self.charge(bytes.len, alignment);
        if (self.active) |accountant| if (DeferredFrees.keep(self.child, accountant, bytes, alignment, bytes_charged)) {
            const previous = self.live_bytes.fetchSub(bytes_charged, .monotonic);
            std.debug.assert(previous >= bytes_charged);
            return;
        };
        self.child.rawFree(bytes, alignment, ret_addr);
        self.release(bytes_charged);
    }
};

/// Splits a teardown's frees in two: the charge goes now, the memory later.
/// While a thread collects, each block it frees through a `BudgetAllocator`
/// with an accountant stops counting against that accountant's budget, as a
/// direct free would, but is listed in one of `lane_count` lanes instead of
/// returning to the child allocator. The accountant holds it as retired
/// (`MemoryAccountant.retireAllocation`) until `release` or `releaseDetached`
/// returns it, so the accountant, and the statement that owns it, outlive the
/// memory however late that runs. The lists live outside the blocks, since
/// writing into a block would fault in pages nothing ever touched. Untracked
/// frees, and any the list cannot grow to hold, are freed directly.
pub const DeferredFrees = struct {
    lanes: [MAX_LANES]Lane = [_]Lane{.{}} ** MAX_LANES,
    lane_count: usize = 1,

    pub const MAX_LANES = 16;

    const Lane = struct {
        chunks: ?*Chunk = null,
        bytes: usize = 0,
    };

    const Chunk = struct {
        next: ?*Chunk,
        len: usize = 0,
        entries: [CHUNK_ENTRIES]Entry = undefined,

        const CHUNK_ENTRIES = 256;
    };

    const Entry = struct {
        memory: [*]u8,
        len: usize,
        alignment: std.mem.Alignment,
        child: Allocator,
        accountant: *MemoryAccountant,
        charge: usize,
    };

    const list_allocator = std.heap.c_allocator;

    threadlocal var collecting: ?*DeferredFrees = null;

    /// Collected blocks spread over `lanes` lanes by size, one per thread
    /// that `releaseDetached` starts.
    pub fn init(lanes: usize) DeferredFrees {
        return .{ .lane_count = std.math.clamp(lanes, 1, MAX_LANES) };
    }

    pub fn collect(self: *DeferredFrees) void {
        std.debug.assert(collecting == null);
        collecting = self;
    }

    pub fn stop(self: *DeferredFrees) void {
        std.debug.assert(collecting == self);
        collecting = null;
    }

    pub fn isEmpty(self: DeferredFrees) bool {
        for (self.lanes[0..self.lane_count]) |lane| if (lane.chunks != null) return false;
        return true;
    }

    /// Returns the collected memory on this thread.
    pub fn release(self: DeferredFrees) void {
        for (self.lanes[0..self.lane_count]) |lane| releaseLane(lane.chunks);
    }

    /// Returns each lane's memory on a thread of its own that nothing joins:
    /// the accountants hold the bytes as retired until they are back.
    pub fn releaseDetached(self: DeferredFrees) void {
        for (self.lanes[0..self.lane_count]) |lane| {
            const chunks = lane.chunks orelse continue;
            if (std.Thread.spawn(.{}, releaseLane, .{chunks})) |thread| {
                thread.detach();
            } else |_| releaseLane(chunks);
        }
    }

    /// Hands each accountant its bytes back once per run of entries, after
    /// the run's memory is freed.
    fn releaseLane(first: ?*Chunk) void {
        var next = first;
        while (next) |chunk| {
            next = chunk.next;
            var run_accountant: ?*MemoryAccountant = null;
            var run_bytes: usize = 0;
            for (chunk.entries[0..chunk.len]) |entry| {
                if (run_accountant != entry.accountant) {
                    if (run_accountant) |accountant| accountant.finishRetired(run_bytes);
                    run_accountant = entry.accountant;
                    run_bytes = 0;
                }
                entry.child.rawFree(entry.memory[0..entry.len], entry.alignment, @returnAddress());
                run_bytes += entry.charge;
            }
            if (run_accountant) |accountant| accountant.finishRetired(run_bytes);
            list_allocator.destroy(chunk);
        }
    }

    fn keep(child: Allocator, accountant: *MemoryAccountant, memory: []u8, alignment: std.mem.Alignment, charge: usize) bool {
        const self = collecting orelse return false;
        const lane = self.lightestLane();
        const chunk = if (lane.chunks) |chunk| if (chunk.len < Chunk.CHUNK_ENTRIES) chunk else null else null;
        const open = chunk orelse blk: {
            const fresh = list_allocator.create(Chunk) catch return false;
            fresh.* = .{ .next = lane.chunks };
            lane.chunks = fresh;
            break :blk fresh;
        };
        accountant.retireAllocation(charge);
        open.entries[open.len] = .{ .memory = memory.ptr, .len = memory.len, .alignment = alignment, .child = child, .accountant = accountant, .charge = charge };
        open.len += 1;
        lane.bytes += memory.len;
        return true;
    }

    fn lightestLane(self: *DeferredFrees) *Lane {
        var lightest = &self.lanes[0];
        for (self.lanes[1..self.lane_count]) |*lane| {
            if (lane.bytes < lightest.bytes) lightest = lane;
        }
        return lightest;
    }
};
