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

const WinAffinity = if (builtin.os.tag == .windows) struct {
    extern "kernel32" fn GetCurrentThread() callconv(.winapi) win.HANDLE;
    extern "kernel32" fn SetThreadAffinityMask(hThread: win.HANDLE, mask: usize) callconv(.winapi) usize;
    extern "kernel32" fn GetLogicalProcessorInformation(buf: ?[*]LogicalProcInfo, len: *u32) callconv(.winapi) win.BOOL;
} else struct {};

const RelationProcessorCore: u32 = 0;
const LogicalProcInfo = extern struct {
    processor_mask: usize,
    relationship: u32,
    _pad: u32 = 0,
    _union: [16]u8 = [_]u8{0} ** 16,
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
    var siblings: std.ArrayListUnmanaged(usize) = .empty;
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
    siblings.deinit(allocator);
    return .{
        .order = try primaries.toOwnedSlice(allocator),
        .physical_count = physical_count,
    };
}

pub fn pinToCpu(cpu: usize) void {
    switch (builtin.os.tag) {
        .windows => {
            if (cpu < @bitSizeOf(usize)) {
                _ = WinAffinity.SetThreadAffinityMask(WinAffinity.GetCurrentThread(), @as(usize, 1) << @intCast(cpu));
            }
        },
        .linux => {
            var set: std.os.linux.cpu_set_t = @splat(0);
            if (cpu < @bitSizeOf(std.os.linux.cpu_set_t)) {
                set[cpu / @bitSizeOf(usize)] |= @as(usize, 1) << @intCast(cpu % @bitSizeOf(usize));
                std.os.linux.sched_setaffinity(0, &set) catch {};
            }
        },
        else => {},
    }
}
