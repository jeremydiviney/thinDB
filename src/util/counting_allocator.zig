//! Diagnostic allocator wrapper: counts alloc/resize/remap/free calls, bytes,
//! live/peak bytes, and wall ticks spent inside the child allocator. Used only
//! under `--profile-ops` to break down a query handler's heap activity
//! (operator construction + teardown). Single-threaded use only — the per-query
//! handler thread builds and tears down the operator tree; parallel-scan workers
//! allocate from the table allocator, which this does NOT wrap.

const std = @import("std");
const prof = @import("prof.zig");

pub const Stats = struct {
    n_alloc: u64 = 0,
    n_resize: u64 = 0,
    n_remap: u64 = 0,
    n_free: u64 = 0,
    bytes_alloc: u64 = 0,
    bytes_freed: u64 = 0,
    live: i64 = 0,
    peak: i64 = 0,
    ticks: u64 = 0,
};

pub const CountingAllocator = struct {
    child: std.mem.Allocator,
    stats: *Stats,

    pub fn init(child: std.mem.Allocator, stats: *Stats) CountingAllocator {
        return .{ .child = child, .stats = stats };
    }

    pub fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = std.mem.Allocator.VTable{
        .alloc = allocFn,
        .resize = resizeFn,
        .remap = remapFn,
        .free = freeFn,
    };

    fn bump(self: *CountingAllocator, delta: i64) void {
        self.stats.live += delta;
        if (self.stats.live > self.stats.peak) self.stats.peak = self.stats.live;
    }

    fn allocFn(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const t0 = prof.nowTicks();
        const r = self.child.rawAlloc(len, alignment, ret_addr);
        self.stats.ticks +%= @intCast(prof.nowTicks() - t0);
        if (r != null) {
            self.stats.n_alloc += 1;
            self.stats.bytes_alloc += len;
            self.bump(@intCast(len));
        }
        return r;
    }

    fn resizeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const t0 = prof.nowTicks();
        const ok = self.child.rawResize(memory, alignment, new_len, ret_addr);
        self.stats.ticks +%= @intCast(prof.nowTicks() - t0);
        if (ok) {
            self.stats.n_resize += 1;
            self.bump(@as(i64, @intCast(new_len)) - @as(i64, @intCast(memory.len)));
        }
        return ok;
    }

    fn remapFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const t0 = prof.nowTicks();
        const r = self.child.rawRemap(memory, alignment, new_len, ret_addr);
        self.stats.ticks +%= @intCast(prof.nowTicks() - t0);
        if (r != null) {
            self.stats.n_remap += 1;
            self.bump(@as(i64, @intCast(new_len)) - @as(i64, @intCast(memory.len)));
        }
        return r;
    }

    fn freeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const t0 = prof.nowTicks();
        self.child.rawFree(memory, alignment, ret_addr);
        self.stats.ticks +%= @intCast(prof.nowTicks() - t0);
        self.stats.n_free += 1;
        self.stats.bytes_freed += memory.len;
        self.bump(-@as(i64, @intCast(memory.len)));
    }
};

pub fn dump(label: []const u8, s: Stats) void {
    std.debug.print("[mem] {s}: allocs={d} resizes={d} remaps={d} frees={d}  bytes_alloc={d} bytes_freed={d}  peak_live={d}  in_alloc={d:.3}ms\n", .{
        label,        s.n_alloc, s.n_resize, s.n_remap,
        s.n_free,     s.bytes_alloc, s.bytes_freed, s.peak,
        prof.ticksToMs(@intCast(s.ticks)),
    });
}
