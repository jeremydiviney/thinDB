//! Platform primitives shared by the execution engines and benches:
//! monotonic tick timing (QPC on Windows, CLOCK_MONOTONIC on Linux) and
//! CPU topology / thread pinning. Off-platform fallbacks are graceful:
//! zero ticks, identity CPU order, no-op pinning.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const win = std.os.windows;

pub fn nowTicks() i64 {
    switch (builtin.os.tag) {
        .windows => {
            var c: win.LARGE_INTEGER = 0;
            _ = win.ntdll.RtlQueryPerformanceCounter(&c);
            return c;
        },
        .linux => {
            var ts: std.os.linux.timespec = undefined;
            _ = std.os.linux.clock_gettime(std.os.linux.CLOCK.MONOTONIC, &ts);
            return @as(i64, ts.sec) * 1_000_000_000 + ts.nsec;
        },
        else => return 0,
    }
}

pub fn perfFreq() i64 {
    switch (builtin.os.tag) {
        .windows => {
            var f: win.LARGE_INTEGER = 0;
            _ = win.ntdll.RtlQueryPerformanceFrequency(&f);
            return if (f == 0) 1 else f;
        },
        .linux => return 1_000_000_000,
        else => return 1,
    }
}

pub fn ticksToMs(ticks: i64, freq: i64) f64 {
    return @as(f64, @floatFromInt(ticks)) * 1000.0 / @as(f64, @floatFromInt(freq));
}

/// Linux 5.14's MADV_POPULATE_READ, which `std.os.linux.MADV` lacks.
const MADV_POPULATE_READ: u32 = 22;

/// What `lockExecutable` did with the executable's loaded segments.
pub const ExecutableLock = union(enum) {
    /// Read in and pinned in memory: the bytes locked.
    locked: usize,
    /// Read in, but mlock was refused for some of it (no CAP_IPC_LOCK and an
    /// RLIMIT_MEMLOCK below the segments' size): the bytes read.
    unlocked: usize,
    /// Not Linux: the code loads on demand.
    unsupported,
};

/// Reads the executable's loaded segments into memory and pins them there, as
/// ClickHouse's `mlock_executable` does. A server started over a cold page cache
/// otherwise faults its code in from disk a page at a time, one wait each,
/// inside its first queries, and heavy scan I/O can later evict that code
/// again. When mlock is refused the segments are still read in, without the
/// pin; an older kernel without MADV_POPULATE_READ gets the asynchronous
/// MADV_WILLNEED read-ahead instead.
pub fn lockExecutable() ExecutableLock {
    if (builtin.os.tag != .linux) return .unsupported;
    const linux = std.os.linux;
    const phdrs = std.posix.getSelfPhdrs();
    const base = for (phdrs) |phdr| {
        if (phdr.type == .PHDR) break @intFromPtr(phdrs.ptr) - phdr.vaddr;
    } else return .unsupported;
    const page = std.heap.pageSize();
    var bytes: usize = 0;
    var all_locked = true;
    for (phdrs) |phdr| {
        if (phdr.type != .LOAD or phdr.filesz == 0) continue;
        const start = std.mem.alignBackward(usize, base + phdr.vaddr, page);
        const len = base + phdr.vaddr + phdr.filesz - start;
        const ptr: [*]u8 = @ptrFromInt(start);
        bytes += len;
        if (linux.errno(linux.mlock(ptr, len)) == .SUCCESS) continue;
        all_locked = false;
        if (linux.errno(linux.madvise(ptr, len, MADV_POPULATE_READ)) != .SUCCESS) {
            _ = linux.madvise(ptr, len, linux.MADV.WILLNEED);
        }
    }
    return if (all_locked) .{ .locked = bytes } else .{ .unlocked = bytes };
}

const WinAffinity = if (builtin.os.tag == .windows) struct {
    extern "kernel32" fn GetLogicalProcessorInformation(buf: ?[*]LogicalProcInfo, len: *u32) callconv(.winapi) win.BOOL;
} else struct {};

const RelationProcessorCore: u32 = 0;
const LogicalProcInfo = extern struct {
    processor_mask: usize,
    relationship: u32,
    _pad: u32 = 0,
    _union: [16]u8 = @splat(0),
};

pub const CpuLayout = struct {
    order: []usize,
    physical_count: usize,

    pub fn deinit(self: CpuLayout, allocator: Allocator) void {
        allocator.free(self.order);
    }
};

pub fn cpuLayout(allocator: Allocator) !CpuLayout {
    if (builtin.os.tag != .windows) {
        // No SMT-topology query off-Windows yet: identity order, every logical
        // CPU counted as physical. Good enough for pinning; SMT-aware
        // primary-first ordering is a Linux-port follow-up.
        const n_cpus = std.Thread.getCpuCount() catch 1;
        const order = try allocator.alloc(usize, n_cpus);
        for (order, 0..) |*o, i| o.* = i;
        return .{ .order = order, .physical_count = n_cpus };
    }
    var buf: [256]LogicalProcInfo = undefined;
    var len: u32 = @intCast(@sizeOf(LogicalProcInfo) * buf.len);
    if (!WinAffinity.GetLogicalProcessorInformation(&buf, &len).toBool()) return error.QueryFailed;
    const n = len / @sizeOf(LogicalProcInfo);

    var primaries: std.ArrayListUnmanaged(usize) = .empty;
    errdefer primaries.deinit(allocator);
    var siblings: std.ArrayListUnmanaged(usize) = .empty;
    defer siblings.deinit(allocator);
    for (buf[0..n]) |info| {
        if (info.relationship != RelationProcessorCore) continue;
        var mask = info.processor_mask;
        var first = true;
        while (mask != 0) {
            const bit: usize = @ctz(mask);
            mask &= mask - 1;
            if (first) {
                try primaries.append(allocator, bit);
                first = false;
            } else {
                try siblings.append(allocator, bit);
            }
        }
    }
    const physical_count = primaries.items.len;
    try primaries.appendSlice(allocator, siblings.items);
    return .{
        .order = try primaries.toOwnedSlice(allocator),
        .physical_count = physical_count,
    };
}
