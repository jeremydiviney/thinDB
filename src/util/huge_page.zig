//! Cross-platform huge-page-backed slab pool for the row-group cache's
//! decompressed column blocks. Two wins over allocating each block straight
//! from the general allocator, both measured on the 100M ClickBench scan:
//!
//!   1. Bigger pages. Slabs are backed by 2 MiB OS huge pages where available,
//!      so a 20 GiB resident cache needs ~512x fewer TLB entries to map. Once
//!      the cache grows to tens of GiB the scan layer was spending most of its
//!      time on address translation (soft page faults), not decode.
//!   2. No working-set trimming. Huge pages are non-pageable on Windows, and
//!      every slab is VirtualLock/mlock'd, so the OS can't trim cache pages out
//!      of the resident set and force a re-fault on the next scan of that block.
//!
//! Best-effort by design: every OS huge-page path degrades — large pages to a
//! plain locked mapping, a locked mapping to an unlocked one, and an OS mapping
//! to the backing allocator — so the pool is always usable. Huge pages are a
//! performance hint, never a correctness gate (mirrors util/affinity.zig).
//!
//! ## Allocation model
//! Blocks (>= MIN_BLOCK) are sub-allocated from 32 MiB slabs via segregated
//! free lists keyed by a quantized size class (<=25% internal waste; fixed
//! width column blocks are exact powers of two and land on a class boundary
//! with zero waste). A freed block is pushed onto its class list using an
//! intrusive node stored in the block's own memory and reused on the next
//! same-class request. Slabs are never returned to the OS until `deinit`, so
//! the pool's resident footprint is its high-water mark — bounded in practice
//! by the cache's `capacity_bytes`. Sub-MIN_BLOCK requests (entry metadata sized
//! buffers never reach here, but headers/odd sizes might) fall through to the
//! backing allocator unchanged.
//!
//! ## Concurrency
//! Scan workers decompress and insert blocks concurrently (the cache holds its
//! own mutex only for O(1) bookkeeping, never across decompress), so the pool
//! guards its state with its own spinlock. Lock order is always cache.mutex
//! then pool.lock — the pool never calls back into the cache.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;

pub const HUGE_PAGE: usize = 2 * 1024 * 1024;
const SLAB_BYTES: usize = 32 * 1024 * 1024;
const MIN_BLOCK: usize = 64 * 1024;

/// Diagnostic counters, read by the MySQL handler under `--profile-ops` to
/// confirm whether huge pages actually engaged (privilege/THP availability is
/// environment-dependent, so "asked for" != "got").
pub var g_slabs_huge = std.atomic.Value(u64).init(0);
pub var g_slabs_plain = std.atomic.Value(u64).init(0);
pub var g_slab_bytes = std.atomic.Value(u64).init(0);

const SpinLock = struct {
    state: std.atomic.Value(bool) = .{ .raw = false },
    fn lock(self: *SpinLock) void {
        while (self.state.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
    }
    fn unlock(self: *SpinLock) void {
        self.state.store(false, .release);
    }
};

const Node = struct { next: ?*Node };

const Region = struct {
    base: [*]align(16) u8,
    len: usize,
    used: usize,
    huge: bool,
    /// A block larger than a slab gets its own region; freed back to the OS
    /// whole rather than carved into class cells.
    single: bool,
};

/// Round `len` up to a size class with at most 25% internal waste: four
/// sub-classes per power-of-two octave. A power-of-two `len` is returned
/// unchanged, so fixed-width column blocks (65536 rows * {1,2,4,8} bytes) get
/// an exact-fit cell.
fn classSize(len: usize) usize {
    if (len <= MIN_BLOCK) return MIN_BLOCK;
    const octave = 63 - @clz(len); // floor(log2(len))
    const quantum = @as(usize, 1) << @intCast(octave - 2);
    return std.mem.alignForward(usize, len, quantum);
}

/// Resident bytes a block of `len` actually occupies in the pool: `len` for
/// sub-MIN_BLOCK blocks (exact backing allocation) or the rounded class cell
/// otherwise. The cache uses this so its capacity bounds real footprint.
pub fn cellSize(len: usize) usize {
    if (len < MIN_BLOCK) return len;
    return classSize(len);
}

pub const Pool = struct {
    backing: Allocator,
    lock: SpinLock = .{},
    /// Slabs and single-block regions, kept sorted by `base` for a binary-search
    /// ownership test on free.
    regions: std.ArrayListUnmanaged(Region) = .empty,
    /// Free cells by class size. Few distinct sizes (~36), so a hashmap of
    /// intrusive list heads is cheap and avoids size-class index arithmetic.
    free_lists: std.AutoHashMapUnmanaged(usize, ?*Node) = .empty,

    pub fn init(backing: Allocator) Pool {
        return .{ .backing = backing };
    }

    pub fn deinit(self: *Pool) void {
        for (self.regions.items) |r| {
            if (r.huge) osFree(r.base, r.len) else self.backing.rawFree(r.base[0..r.len], .@"16", @returnAddress());
        }
        self.regions.deinit(self.backing);
        self.free_lists.deinit(self.backing);
        self.* = undefined;
    }

    pub fn allocator(self: *Pool) Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = Allocator.VTable{
        .alloc = vtAlloc,
        .resize = vtResize,
        .remap = vtRemap,
        .free = vtFree,
    };

    fn vtAlloc(ctx: *anyopaque, len: usize, alignment: Alignment, ra: usize) ?[*]u8 {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        if (len < MIN_BLOCK or alignment.toByteUnits() > MIN_BLOCK) {
            return self.backing.rawAlloc(len, alignment, ra);
        }
        self.lock.lock();
        defer self.lock.unlock();
        return self.allocBlock(len) catch null;
    }

    fn vtResize(ctx: *anyopaque, mem: []u8, alignment: Alignment, new_len: usize, ra: usize) bool {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        if (mem.len < MIN_BLOCK and new_len < MIN_BLOCK) {
            return self.backing.rawResize(mem, alignment, new_len, ra);
        }
        // In-place only when the new length still fits the same class cell.
        return new_len >= MIN_BLOCK and classSize(new_len) == classSize(mem.len);
    }

    fn vtRemap(ctx: *anyopaque, mem: []u8, alignment: Alignment, new_len: usize, ra: usize) ?[*]u8 {
        if (vtResize(ctx, mem, alignment, new_len, ra)) return mem.ptr;
        return null;
    }

    fn vtFree(ctx: *anyopaque, mem: []u8, alignment: Alignment, ra: usize) void {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        if (mem.len < MIN_BLOCK and alignment.toByteUnits() <= MIN_BLOCK) {
            self.backing.rawFree(mem, alignment, ra);
            return;
        }
        self.lock.lock();
        defer self.lock.unlock();
        self.freeBlock(mem);
    }

    fn allocBlock(self: *Pool, len: usize) !?[*]u8 {
        const size = classSize(len);
        if (self.free_lists.get(size)) |head| {
            if (head) |node| {
                self.free_lists.putAssumeCapacity(size, node.next);
                return @ptrCast(node);
            }
        }
        if (size > SLAB_BYTES) {
            const r = try self.mapRegion(size, true);
            return r.base;
        }
        return try self.carve(size);
    }

    /// Carve `size` bytes off a slab with room, allocating a fresh slab if none
    /// fits. `size` is class-quantized (multiple of >=16 KiB) and slab bases are
    /// >=64 KiB aligned, so every returned cell is 16 KiB-aligned — covering any
    /// block alignment the cache asks for.
    fn carve(self: *Pool, size: usize) !?[*]u8 {
        for (self.regions.items) |*r| {
            if (r.single) continue;
            if (r.len - r.used >= size) {
                const p = r.base + r.used;
                r.used += size;
                return p;
            }
        }
        const r = try self.mapRegion(SLAB_BYTES, false);
        // mapRegion inserted a copy into `regions`; bump that copy, not the local.
        const slab = self.regionAt(r.base) orelse return error.OutOfMemory;
        slab.used = size;
        return slab.base;
    }

    fn freeBlock(self: *Pool, mem: []u8) void {
        const region = self.regionAt(@ptrCast(mem.ptr));
        if (region) |r| {
            if (r.single) {
                const base = r.base;
                const len = r.len;
                const huge = r.huge;
                self.removeRegion(base);
                if (huge) osFree(base, len) else self.backing.rawFree(base[0..len], .@"16", @returnAddress());
                return;
            }
        }
        // Slab cell (or, defensively, an untracked pointer): recycle by class.
        const size = classSize(mem.len);
        const node: *Node = @ptrCast(@alignCast(mem.ptr));
        const gop = self.free_lists.getOrPut(self.backing, size) catch {
            // Can't record the free; leak the cell rather than corrupt state.
            return;
        };
        node.next = if (gop.found_existing) gop.value_ptr.* else null;
        gop.value_ptr.* = node;
    }

    fn mapRegion(self: *Pool, size: usize, single: bool) !Region {
        var huge = true;
        const base = osAlloc(size) orelse blk: {
            huge = false;
            const p = self.backing.rawAlloc(size, .@"16", @returnAddress()) orelse return error.OutOfMemory;
            break :blk @as([*]align(16) u8, @alignCast(p));
        };
        if (huge) {
            _ = g_slabs_huge.fetchAdd(1, .monotonic);
        } else {
            _ = g_slabs_plain.fetchAdd(1, .monotonic);
        }
        _ = g_slab_bytes.fetchAdd(size, .monotonic);
        const region = Region{ .base = base, .len = size, .used = 0, .huge = huge, .single = single };
        errdefer if (huge) osFree(base, size) else self.backing.rawFree(base[0..size], .@"16", @returnAddress());
        try self.insertRegionSorted(region);
        return region;
    }

    fn insertRegionSorted(self: *Pool, region: Region) !void {
        const addr = @intFromPtr(region.base);
        var lo: usize = 0;
        var hi: usize = self.regions.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (@intFromPtr(self.regions.items[mid].base) < addr) lo = mid + 1 else hi = mid;
        }
        try self.regions.insert(self.backing, lo, region);
    }

    /// Binary search for the region whose [base, base+len) contains `p`.
    fn regionAt(self: *Pool, p: [*]u8) ?*Region {
        const addr = @intFromPtr(p);
        var lo: usize = 0;
        var hi: usize = self.regions.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const r = &self.regions.items[mid];
            const start = @intFromPtr(r.base);
            if (addr < start) {
                hi = mid;
            } else if (addr >= start + r.len) {
                lo = mid + 1;
            } else {
                return r;
            }
        }
        return null;
    }

    fn removeRegion(self: *Pool, base: [*]align(16) u8) void {
        const addr = @intFromPtr(base);
        for (self.regions.items, 0..) |r, i| {
            if (@intFromPtr(r.base) == addr) {
                _ = self.regions.orderedRemove(i);
                return;
            }
        }
    }
};

// ---- OS huge-page mapping --------------------------------------------------

/// Map `size` (a multiple of HUGE_PAGE) bytes backed by huge pages and locked
/// into the resident set where the OS allows it. Returns null on any failure so
/// the pool falls back to the backing allocator.
fn osAlloc(size: usize) ?[*]align(16) u8 {
    return switch (builtin.os.tag) {
        .windows => osAllocWindows(size),
        .linux, .macos => osAllocPosix(size),
        else => null,
    };
}

fn osFree(base: [*]align(16) u8, len: usize) void {
    switch (builtin.os.tag) {
        .windows => {
            _ = VirtualFree(base, 0, MEM_RELEASE);
        },
        .linux, .macos => {
            std.posix.munmap(@alignCast(base[0..len]));
        },
        else => {},
    }
}

// ---- Windows ---------------------------------------------------------------

const win = std.os.windows;

const MEM_COMMIT: u32 = 0x1000;
const MEM_RESERVE: u32 = 0x2000;
const MEM_RELEASE: u32 = 0x8000;
const MEM_LARGE_PAGES: u32 = 0x20000000;
const PAGE_READWRITE: u32 = 0x04;

extern "kernel32" fn VirtualAlloc(addr: ?*anyopaque, size: usize, alloc_type: u32, protect: u32) callconv(.winapi) ?*anyopaque;
extern "kernel32" fn VirtualFree(addr: *anyopaque, size: usize, free_type: u32) callconv(.winapi) win.BOOL;
extern "kernel32" fn VirtualLock(addr: *anyopaque, size: usize) callconv(.winapi) win.BOOL;
extern "kernel32" fn GetLargePageMinimum() callconv(.winapi) usize;
extern "kernel32" fn SetProcessWorkingSetSizeEx(process: win.HANDLE, min: usize, max: usize, flags: u32) callconv(.winapi) win.BOOL;

var win_priv_state = std.atomic.Value(u8).init(0); // 0=unknown 1=enabled 2=failed
var win_ws_raised = std.atomic.Value(bool).init(false);

fn osAllocWindows(size: usize) ?[*]align(16) u8 {
    if (enableLockMemoryPrivilege()) {
        const lpm = GetLargePageMinimum();
        if (lpm != 0 and size % lpm == 0) {
            if (VirtualAlloc(null, size, MEM_COMMIT | MEM_RESERVE | MEM_LARGE_PAGES, PAGE_READWRITE)) |p| {
                return @ptrCast(@alignCast(p));
            }
        }
    }
    // No large pages (privilege or pool unavailable): a normal committed
    // mapping, locked so the working set isn't trimmed under memory pressure.
    // VirtualLock is bounded by the process working-set maximum, so raise that
    // ceiling first or a multi-GiB cache can't be pinned.
    raiseWorkingSetQuota();
    const p = VirtualAlloc(null, size, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE) orelse return null;
    _ = VirtualLock(p, size); // best-effort
    return @ptrCast(@alignCast(p));
}

/// Raise this process's working-set maximum once so VirtualLock has quota to
/// pin the cache slabs. Soft limits (flags=0): we lift the ceiling for locking
/// without imposing a hard minimum that would starve the rest of the machine.
fn raiseWorkingSetQuota() void {
    if (win_ws_raised.swap(true, .monotonic)) return;
    const total = std.process.totalSystemMemory() catch return;
    const max_ws: usize = @intCast(total * 90 / 100);
    const min_ws: usize = @min(max_ws, @as(usize, 256) * 1024 * 1024);
    _ = SetProcessWorkingSetSizeEx(GetCurrentProcess(), min_ws, max_ws, 0);
}

const TOKEN_ADJUST_PRIVILEGES: u32 = 0x0020;
const TOKEN_QUERY: u32 = 0x0008;
const SE_PRIVILEGE_ENABLED: u32 = 0x00000002;

const LUID = extern struct { low: u32, high: i32 };
const LUID_AND_ATTRIBUTES = extern struct { luid: LUID, attributes: u32 };
const TOKEN_PRIVILEGES = extern struct { count: u32, privilege: [1]LUID_AND_ATTRIBUTES };

extern "advapi32" fn OpenProcessToken(process: win.HANDLE, access: u32, token: *win.HANDLE) callconv(.winapi) win.BOOL;
extern "advapi32" fn LookupPrivilegeValueW(system: ?win.LPCWSTR, name: win.LPCWSTR, luid: *LUID) callconv(.winapi) win.BOOL;
extern "advapi32" fn AdjustTokenPrivileges(token: win.HANDLE, disable_all: c_int, new: ?*TOKEN_PRIVILEGES, len: u32, prev: ?*TOKEN_PRIVILEGES, ret_len: ?*u32) callconv(.winapi) win.BOOL;
extern "kernel32" fn GetCurrentProcess() callconv(.winapi) win.HANDLE;

/// Enable SeLockMemoryPrivilege for this process once. Succeeds only if the
/// account holds the "Lock pages in memory" right; otherwise large pages stay
/// unavailable and the caller falls back to a locked normal mapping.
fn enableLockMemoryPrivilege() bool {
    switch (win_priv_state.load(.monotonic)) {
        1 => return true,
        2 => return false,
        else => {},
    }
    const ok = tryEnableLockMemoryPrivilege();
    win_priv_state.store(if (ok) 1 else 2, .monotonic);
    return ok;
}

fn tryEnableLockMemoryPrivilege() bool {
    var token: win.HANDLE = undefined;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, &token).toBool()) return false;
    defer win.CloseHandle(token);
    var luid: LUID = undefined;
    const name = std.unicode.utf8ToUtf16LeStringLiteral("SeLockMemoryPrivilege");
    if (!LookupPrivilegeValueW(null, name, &luid).toBool()) return false;
    var tp = TOKEN_PRIVILEGES{ .count = 1, .privilege = .{.{ .luid = luid, .attributes = SE_PRIVILEGE_ENABLED }} };
    if (!AdjustTokenPrivileges(token, 0, &tp, @sizeOf(TOKEN_PRIVILEGES), null, null).toBool()) return false;
    // AdjustTokenPrivileges "succeeds" even when the right isn't held; the real
    // verdict is in GetLastError (ERROR_NOT_ALL_ASSIGNED == 1300).
    return win.GetLastError() == .SUCCESS;
}

// ---- Linux / macOS ---------------------------------------------------------

fn osAllocPosix(size: usize) ?[*]align(16) u8 {
    // Over-map by one huge page so the usable region can be aligned to a 2 MiB
    // boundary — transparent huge pages only collapse aligned, huge-sized spans.
    const over = size + HUGE_PAGE;
    const raw = std.posix.mmap(
        null,
        over,
        .{ .READ = true, .WRITE = true },
        .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
        -1,
        0,
    ) catch return null;
    const raw_addr = @intFromPtr(raw.ptr);
    const aligned = std.mem.alignForward(usize, raw_addr, HUGE_PAGE);
    const head = aligned - raw_addr;
    if (head != 0) std.posix.munmap(@alignCast(raw[0..head]));
    const tail_start = head + size;
    if (over > tail_start) std.posix.munmap(@alignCast(raw[tail_start..over]));
    const region: [*]align(HUGE_PAGE) u8 = @ptrFromInt(aligned);
    if (builtin.os.tag == .linux) {
        std.posix.madvise(region, size, std.os.linux.MADV.HUGEPAGE) catch {};
    }
    mlockBestEffort(region, size);
    return region;
}

fn mlockBestEffort(addr: [*]u8, size: usize) void {
    // Linux only: pins the slab so the resident set isn't trimmed. macOS keeps
    // the plain (madvise-less) mapping — superpages there are opportunistic and
    // mlock would pull in libc, which the engine doesn't otherwise need.
    if (builtin.os.tag == .linux) {
        _ = std.os.linux.mlock(addr, size);
    }
}

// ---- tests -----------------------------------------------------------------

test "classSize keeps fixed-width column blocks exact and bounds waste" {
    try std.testing.expectEqual(@as(usize, 64 * 1024), classSize(64 * 1024)); // u8 col
    try std.testing.expectEqual(@as(usize, 256 * 1024), classSize(256 * 1024)); // u32 col
    try std.testing.expectEqual(@as(usize, 512 * 1024), classSize(512 * 1024)); // u64 col
    // <=25% internal waste mid-octave.
    const s = classSize(300 * 1024);
    try std.testing.expect(s >= 300 * 1024);
    try std.testing.expect(s <= 300 * 1024 * 5 / 4);
}

test "pool round-trips blocks and recycles freed cells" {
    var pool = Pool.init(std.testing.allocator);
    defer pool.deinit();
    const a = pool.allocator();

    const b1 = try a.alignedAlloc(u8, .@"16", 256 * 1024);
    @memset(b1, 0xAB);
    const p1 = b1.ptr;
    a.free(b1);
    // Same-class request should reuse the just-freed cell.
    const b2 = try a.alignedAlloc(u8, .@"16", 256 * 1024);
    try std.testing.expectEqual(p1, b2.ptr);
    a.free(b2);

    // A sub-MIN_BLOCK request bypasses the pool via the backing allocator.
    const small = try a.alloc(u8, 128);
    a.free(small);
}

test "pool serves many concurrent-sized blocks without overlap" {
    var pool = Pool.init(std.testing.allocator);
    defer pool.deinit();
    const a = pool.allocator();

    var blocks: [32][]u8 = undefined;
    for (&blocks, 0..) |*b, i| {
        b.* = try a.alignedAlloc(u8, .@"16", 64 * 1024 + i * 4096);
        @memset(b.*, @intCast(i));
    }
    // Each block keeps its own bytes — no aliasing across cells.
    for (blocks, 0..) |b, i| {
        for (b) |byte| try std.testing.expectEqual(@as(u8, @intCast(i)), byte);
    }
    for (blocks) |b| a.free(b);
}
