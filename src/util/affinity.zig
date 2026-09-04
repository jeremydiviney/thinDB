//! Cross-platform CPU topology + thread affinity, plus the host-resource
//! probes that size pools (physical cores, usable memory). Used by the engine's
//! CoreScheduler (engine/core_scheduler.zig) to pin query worker threads to
//! physical cores so the OS can't park two of our threads on one core's SMT
//! siblings while another core idles (measured ~40% scaling loss otherwise).
//!
//! Affinity is a PERFORMANCE HINT, never a correctness gate: every entry point
//! is best-effort and degrades to a no-op where the OS doesn't support pinning
//! (notably macOS, which deliberately ignores hard thread-to-core placement).
//!
//! The CoreScheduler only PINS on Windows (core_scheduler.PIN_THREADS): Linux
//! child threads inherit the spawner's affinity mask, so pinning a thread that
//! spawns workers traps them all on its core. pinCurrentThread/unpinCurrentThread
//! keep their Linux arms for explicit child-side pinning (platform.pinToCpu-style
//! callers that pin themselves at worker entry, which is inheritance-safe).
//!
//! Mask model: a "core" is one physical core, identified by the bitmask of the
//! logical CPUs that belong to it (1 bit non-SMT, 2 bits with hyperthreading).
//! Pinning sets the thread's affinity to that whole mask, keeping it on the
//! core's caches while letting the OS use either SMT lane. Capped at 64 logical
//! CPUs (one usize mask); larger machines fall back to no pinning.

const std = @import("std");
const builtin = @import("builtin");

pub const Core = struct {
    /// Bitmask of logical CPUs belonging to this physical core.
    mask: u64,
    /// Hardware threads on this core (popcount of mask) + 1. The +1 is a
    /// deliberate one-thread oversubscription so a core stays saturated across
    /// memory/IO stalls — see CoreScheduler.
    capacity: u8,
};

pub const Topology = struct {
    cores: []Core,
    /// Union of all logical CPUs this process is allowed to run on — the
    /// affinity to restore on unpin.
    all_mask: u64,
    smt: bool,
    /// True when real topology was discovered; false = conservative fallback
    /// (each allowed logical CPU treated as its own core) or pinning unsupported.
    real: bool,

    pub fn deinit(self: Topology, allocator: std.mem.Allocator) void {
        allocator.free(self.cores);
    }
};

/// Discover the physical cores available to THIS process. Always returns a
/// valid Topology — on any failure it falls back to one core per allowed
/// logical CPU (capacity 2), which still pins correctly on non-SMT hardware
/// (Graviton/ARM) and is merely SMT-unaware on hyperthreaded x86.
pub fn detect(allocator: std.mem.Allocator) Topology {
    return switch (builtin.os.tag) {
        .windows => detectWindows(allocator) catch fallback(allocator),
        else => fallback(allocator),
    };
}

/// Pin the calling thread to `mask` (the logical CPUs of one physical core).
/// Best-effort: silently does nothing where unsupported.
pub fn pinCurrentThread(mask: u64) void {
    if (mask == 0) return;
    switch (builtin.os.tag) {
        .windows => {
            _ = SetThreadAffinityMask(GetCurrentThread(), @intCast(mask));
        },
        .linux => setLinuxAffinity(mask),
        else => {},
    }
}

/// Restore the calling thread's affinity to the full allowed set — called when
/// a thread releases its core lease so a pooled/connection thread doesn't carry
/// a stale pin into its next query.
pub fn unpinCurrentThread(all_mask: u64) void {
    if (all_mask == 0) return;
    switch (builtin.os.tag) {
        .windows => {
            _ = SetThreadAffinityMask(GetCurrentThread(), @intCast(all_mask));
        },
        .linux => setLinuxAffinity(all_mask),
        else => {},
    }
}

/// Physical core count, for sizing background work (compaction encoders) —
/// NOT for pinning, which goes through `detect`. Windows: processor-core
/// records from `GetLogicalProcessorInformation`. Linux: unique
/// (physical_package_id, core_id) pairs from sysfs — counts non-SMT parts
/// (Graviton and other ARM server cores) 1:1. macOS: `sysctl hw.physicalcpu`
/// (covers Apple Silicon, where there is no SMT). On any detection failure,
/// falls back to the LOGICAL count — over-counting a hyperthreaded x86 beats
/// halving a non-SMT machine to half its real size.
pub fn physicalCoreCount(allocator: std.mem.Allocator) usize {
    const counted: ?usize = switch (builtin.os.tag) {
        .windows => blk: {
            const topo = detectWindows(allocator) catch break :blk null;
            defer topo.deinit(allocator);
            break :blk topo.cores.len;
        },
        .linux => linuxPhysicalCoreCount(allocator) catch null,
        .macos => macosPhysicalCoreCount() catch null,
        else => null,
    };
    if (counted) |n| {
        if (n >= 1) return n;
    }
    return @max(1, std.Thread.getCpuCount() catch 1);
}

fn fallback(allocator: std.mem.Allocator) Topology {
    const n = @min(@as(usize, std.Thread.getCpuCount() catch 1), 64);
    const cores = allocator.alloc(Core, @max(n, 1)) catch {
        return .{ .cores = &.{}, .all_mask = 0, .smt = false, .real = false };
    };
    var all: u64 = 0;
    for (cores, 0..) |*c, i| {
        const bit = @as(u64, 1) << @intCast(i);
        c.* = .{ .mask = bit, .capacity = 2 };
        all |= bit;
    }
    return .{ .cores = cores, .all_mask = all, .smt = false, .real = false };
}

// ---- Linux ----------------------------------------------------------------

fn setLinuxAffinity(mask: u64) void {
    if (builtin.os.tag != .linux) return;
    const linux = std.os.linux;
    // cpu_set_t is a 1024-bit mask; we only ever touch the low 64.
    var set = [_]u8{0} ** 128;
    std.mem.writeInt(u64, set[0..8], mask, .little);
    _ = linux.syscall3(
        .sched_setaffinity,
        0, // pid 0 = calling thread
        set.len,
        @intFromPtr(&set),
    );
}

/// Unique (package, core) pairs across all sysfs CPU entries. CPUs are
/// iterated until the first missing `cpuN` directory (kernel keeps them
/// contiguous); an entry whose topology files are unreadable (offline CPU)
/// is skipped rather than treated as the end.
fn linuxPhysicalCoreCount(allocator: std.mem.Allocator) !usize {
    if (builtin.os.tag != .linux) return error.QueryFailed;
    var seen: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer seen.deinit(allocator);

    var path_buf: [128]u8 = undefined;
    var cpu: usize = 0;
    while (cpu < 4096) : (cpu += 1) {
        const dir = std.fmt.bufPrintZ(&path_buf, "/sys/devices/system/cpu/cpu{d}", .{cpu}) catch unreachable;
        const dir_fd = linuxOpenReadonly(dir, true) orelse break;
        _ = std.os.linux.close(dir_fd);

        const core_path = std.fmt.bufPrintZ(&path_buf, "/sys/devices/system/cpu/cpu{d}/topology/core_id", .{cpu}) catch unreachable;
        const core_id = readSysfsInt(core_path) orelse continue;
        const pkg_path = std.fmt.bufPrintZ(&path_buf, "/sys/devices/system/cpu/cpu{d}/topology/physical_package_id", .{cpu}) catch unreachable;
        const pkg_id = readSysfsInt(pkg_path) orelse 0;
        try seen.put(allocator, (pkg_id << 32) | (core_id & 0xffff_ffff), {});
    }
    if (seen.count() == 0) return error.QueryFailed;
    return seen.count();
}

fn linuxOpenReadonly(path: [:0]const u8, directory: bool) ?i32 {
    if (builtin.os.tag != .linux) return null;
    const linux = std.os.linux;
    var flags: linux.O = .{ .ACCMODE = .RDONLY };
    flags.DIRECTORY = directory;
    const rc = linux.open(path.ptr, flags, 0);
    if (linux.errno(rc) != .SUCCESS) return null;
    return @intCast(rc);
}

fn readSysfsInt(path: [:0]const u8) ?u64 {
    if (builtin.os.tag != .linux) return null;
    const linux = std.os.linux;
    const fd = linuxOpenReadonly(path, false) orelse return null;
    defer _ = linux.close(fd);
    var buf: [32]u8 = undefined;
    const rc = linux.read(fd, &buf, buf.len);
    if (linux.errno(rc) != .SUCCESS) return null;
    const s = std.mem.trim(u8, buf[0..rc], " \t\r\n");
    return std.fmt.parseInt(u64, s, 10) catch null;
}

/// Memory this process may actually use: physical RAM, capped by the cgroup
/// memory limit when one is set (a container, or a systemd unit with
/// MemoryMax). `sysinfo` reports the HOST's RAM inside either, so pools sized
/// from it alone would plan for memory the kernel will OOM-kill the process
/// for touching. Null when neither source answers.
pub fn totalMemoryBytes() ?u64 {
    const physical: ?u64 = std.process.totalSystemMemory() catch null;
    const limit = cgroupMemoryLimit();
    if (physical) |p| return if (limit) |l| @min(p, l) else p;
    return limit;
}

/// The process's own cgroup and every ancestor, v2 first then the v1 memory
/// controller: a systemd service's cap is written at
/// /sys/fs/cgroup/system.slice/<unit>/memory.max, a slice cap one level up,
/// and a container's at the root of its cgroup namespace. `memory.max` reads
/// "max" when unlimited, which fails the integer parse and is skipped.
fn cgroupMemoryLimit() ?u64 {
    if (builtin.os.tag != .linux) return null;
    var buf: [4096]u8 = undefined;
    const self_cgroup = readSmallFile("/proc/self/cgroup", &buf) orelse "";
    if (cgroupV2SelfPath(self_cgroup)) |path| {
        if (minLimitAlongPath("/sys/fs/cgroup", path, "memory.max")) |v| return v;
    }
    if (cgroupV1MemoryPath(self_cgroup)) |path| {
        if (minLimitAlongPath("/sys/fs/cgroup/memory", path, "memory.limit_in_bytes")) |v| return v;
    }
    // No readable self path: the mount roots still describe a container's own
    // cgroup under a cgroup namespace.
    if (readSysfsInt("/sys/fs/cgroup/memory.max")) |v| return cgroupLimitValue(v);
    if (readSysfsInt("/sys/fs/cgroup/memory/memory.limit_in_bytes")) |v| return cgroupLimitValue(v);
    return null;
}

/// Smallest numeric limit at `path` or any ancestor under `mount`. A child
/// cgroup can never exceed its parent, but the parent's file is where a slice
/// cap is written, so every level counts. Missing or "max" entries are skipped.
fn minLimitAlongPath(mount: []const u8, path: []const u8, file: []const u8) ?u64 {
    var best: ?u64 = null;
    var cur: ?[]const u8 = path;
    while (cur) |p| : (cur = parentCgroupPath(p)) {
        var path_buf: [512]u8 = undefined;
        const dir = if (std.mem.eql(u8, p, "/")) "" else p;
        const full = std.fmt.bufPrintZ(&path_buf, "{s}{s}/{s}", .{ mount, dir, file }) catch break;
        const v = readSysfsInt(full) orelse continue;
        const lim = cgroupLimitValue(v) orelse continue;
        best = if (best) |b| @min(b, lim) else lim;
    }
    return best;
}

/// "/a/b" -> "/a", "/a" -> "/", "/" -> null.
fn parentCgroupPath(p: []const u8) ?[]const u8 {
    if (p.len <= 1) return null;
    const cut = std.mem.lastIndexOfScalar(u8, p, '/') orelse return null;
    return if (cut == 0) p[0..1] else p[0..cut];
}

/// The unified-hierarchy line of /proc/self/cgroup: `0::/system.slice/x.service`.
fn cgroupV2SelfPath(text: []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        if (std.mem.startsWith(u8, line, "0::")) return std.mem.trim(u8, line[3..], " \r");
    }
    return null;
}

/// The v1 memory-controller line: `N:memory:/path` or `N:cpu,memory:/path`.
fn cgroupV1MemoryPath(text: []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        var parts = std.mem.splitScalar(u8, line, ':');
        _ = parts.next() orelse continue;
        const controllers = parts.next() orelse continue;
        const path = parts.rest();
        if (controllers.len == 0 or path.len == 0) continue;
        var names = std.mem.splitScalar(u8, controllers, ',');
        while (names.next()) |n| {
            if (std.mem.eql(u8, n, "memory")) return std.mem.trim(u8, path, " \r");
        }
    }
    return null;
}

fn readSmallFile(path: [:0]const u8, buf: []u8) ?[]const u8 {
    if (builtin.os.tag != .linux) return null;
    const linux = std.os.linux;
    const fd = linuxOpenReadonly(path, false) orelse return null;
    defer _ = linux.close(fd);
    const rc = linux.read(fd, buf.ptr, buf.len);
    if (linux.errno(rc) != .SUCCESS) return null;
    return buf[0..rc];
}

/// v1 reports "no limit" as a page-rounded maxInt(i64); nothing that large is
/// a real cap.
fn cgroupLimitValue(v: u64) ?u64 {
    return if (v >= (@as(u64, 1) << 62)) null else v;
}

fn macosPhysicalCoreCount() !usize {
    if (builtin.os.tag != .macos) return error.QueryFailed;
    var n: c_int = 0;
    var len: usize = @sizeOf(c_int);
    if (std.c.sysctlbyname("hw.physicalcpu", &n, &len, null, 0) != 0) return error.QueryFailed;
    if (n < 1) return error.QueryFailed;
    return @intCast(n);
}

// ---- Windows --------------------------------------------------------------

const win = std.os.windows;

extern "kernel32" fn GetCurrentThread() callconv(.winapi) win.HANDLE;
extern "kernel32" fn SetThreadAffinityMask(hThread: win.HANDLE, mask: usize) callconv(.winapi) usize;
extern "kernel32" fn GetLogicalProcessorInformation(buf: ?[*]LogicalProcInfo, len: *u32) callconv(.winapi) win.BOOL;
extern "kernel32" fn GetCurrentProcess() callconv(.winapi) win.HANDLE;
extern "kernel32" fn GetProcessAffinityMask(h: win.HANDLE, proc: *usize, sys: *usize) callconv(.winapi) win.BOOL;

const RelationProcessorCore: u32 = 0;
const LogicalProcInfo = extern struct {
    processor_mask: usize,
    relationship: u32,
    _pad: u32 = 0,
    _union: [16]u8 = [_]u8{0} ** 16,
};

fn detectWindows(allocator: std.mem.Allocator) !Topology {
    var proc_mask: usize = 0;
    var sys_mask: usize = 0;
    if (!GetProcessAffinityMask(GetCurrentProcess(), &proc_mask, &sys_mask).toBool()) return error.QueryFailed;
    const allowed: u64 = @intCast(proc_mask);

    var buf: [256]LogicalProcInfo = undefined;
    var len: u32 = @intCast(@sizeOf(LogicalProcInfo) * buf.len);
    if (!GetLogicalProcessorInformation(&buf, &len).toBool()) return error.QueryFailed;
    const n = len / @sizeOf(LogicalProcInfo);

    var cores: std.ArrayListUnmanaged(Core) = .empty;
    errdefer cores.deinit(allocator);
    var smt = false;
    for (buf[0..n]) |info| {
        if (info.relationship != RelationProcessorCore) continue;
        const m = @as(u64, @intCast(info.processor_mask)) & allowed;
        if (m == 0) continue; // core has no CPUs this process may use
        const hw_threads = @popCount(m);
        if (hw_threads > 1) smt = true;
        try cores.append(allocator, .{ .mask = m, .capacity = @intCast(hw_threads + 1) });
    }
    if (cores.items.len == 0) return error.QueryFailed;
    return .{
        .cores = try cores.toOwnedSlice(allocator),
        .all_mask = allowed,
        .smt = smt,
        .real = true,
    };
}

test "cgroup self-path parsing: v2 unified line and v1 memory controller" {
    try std.testing.expectEqualStrings("/system.slice/thindb.service", cgroupV2SelfPath("0::/system.slice/thindb.service\n").?);
    try std.testing.expectEqualStrings("/", cgroupV2SelfPath("12:pids:/x\n0::/\n").?);
    try std.testing.expect(cgroupV2SelfPath("11:memory:/docker/abc\n") == null);
    try std.testing.expectEqualStrings("/docker/abc", cgroupV1MemoryPath("12:pids:/x\n11:cpu,memory:/docker/abc\n").?);
    try std.testing.expect(cgroupV1MemoryPath("0::/\n") == null);
}

test "parentCgroupPath walks to the root and stops" {
    try std.testing.expectEqualStrings("/system.slice", parentCgroupPath("/system.slice/thindb.service").?);
    try std.testing.expectEqualStrings("/", parentCgroupPath("/system.slice").?);
    try std.testing.expect(parentCgroupPath("/") == null);
}

test "cgroupLimitValue treats the v1 unlimited sentinel as no cap" {
    try std.testing.expectEqual(@as(?u64, null), cgroupLimitValue(9223372036854771712));
    try std.testing.expectEqual(@as(?u64, null), cgroupLimitValue(std.math.maxInt(u64)));
    try std.testing.expectEqual(@as(?u64, 4294967296), cgroupLimitValue(4294967296));
}

test "totalMemoryBytes reports usable memory" {
    const total = totalMemoryBytes() orelse return error.SkipZigTest;
    try std.testing.expect(total >= 64 * 1024 * 1024);
}

test "physicalCoreCount is sane" {
    const n = physicalCoreCount(std.testing.allocator);
    const logical = std.Thread.getCpuCount() catch 1;
    try std.testing.expect(n >= 1);
    try std.testing.expect(n <= logical);
}

test "detect returns a usable topology" {
    const topo = detect(std.testing.allocator);
    defer topo.deinit(std.testing.allocator);
    try std.testing.expect(topo.cores.len >= 1);
    var union_mask: u64 = 0;
    for (topo.cores) |c| {
        try std.testing.expect(c.capacity >= 2); // hw_threads(>=1) + 1
        union_mask |= c.mask;
    }
    // Every core's CPUs are within the allowed set.
    try std.testing.expectEqual(union_mask, union_mask & topo.all_mask);
}
