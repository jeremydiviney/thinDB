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
//! Blocks (>= MIN_BLOCK) are rounded to a quantized size class (<=25% internal
//! waste; fixed-width column blocks are exact powers of two and land on a class
//! boundary with zero waste). Classes up to MAX_SLAB_CLASS are cells of a slab
//! that serves that one class: 2-32 MiB, sized to hold at least
//! CELLS_PER_SLAB cells. A slab tracks its live cells and keeps its freed
//! cells on its own intrusive list; slabs with a free cell sit on their
//! class's partial list. A larger block gets a region of its own, rounded up to
//! a huge page.
//!
//! The footprint is bounded by the cache's capacity, not by the high-water mark
//! of every size class the workload touched, because `capacity_bytes` only
//! bounds live cells:
//!   - A slab whose last cell is freed (or a big block's region) moves to a
//!     spare reserve that any class needing a region of that length reuses, so
//!     blocks of columns the workload moved away from don't pin their class's
//!     memory once LRU evicts them. The reserve absorbs evict/insert churn
//!     without OS calls (a scan larger than the cache evicts and refills on
//!     every miss, and remapping a locked region costs more than decoding the
//!     block); its oldest regions are unmapped only when a new mapping would
//!     cross `map_limit`.
//!   - Mapped bytes never exceed `map_limit`, a quarter over the capacity. A
//!     request that would cross it with no spare left to unmap (heavy
//!     fragmentation, or pinned blocks holding the cache over capacity) is
//!     served by the backing allocator and freed back there.
//! Sub-MIN_BLOCK requests (entry-metadata-sized buffers never reach here, but
//! headers/odd sizes might) fall through to the backing allocator unchanged.
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
const CELLS_PER_SLAB: usize = 8;
const MAX_SLAB_CLASS: usize = SLAB_BYTES / CELLS_PER_SLAB;
/// Every class is a multiple of 16 KiB and every region base is at least
/// MIN_BLOCK-aligned, so cells honor alignments up to this.
const MAX_CELL_ALIGN: usize = 16 * 1024;
const REGION_ALIGN: Alignment = .fromByteUnits(MIN_BLOCK);

/// Diagnostic counters, read by the MySQL handler under `--profile-ops` to
/// confirm whether huge pages actually engaged (privilege/THP availability is
/// environment-dependent, so "asked for" != "got"). `g_slab_bytes` is what the
/// pools have mapped (spare reserve included); `g_fallback_bytes` is what they
/// hand out from the backing allocator in blocks of MIN_BLOCK or more (past
/// `map_limit`, or at an alignment a cell can't give).
pub var g_slabs_huge = std.atomic.Value(u64).init(0);
pub var g_slabs_plain = std.atomic.Value(u64).init(0);
pub var g_slab_bytes = std.atomic.Value(u64).init(0);
pub var g_fallback_bytes = std.atomic.Value(u64).init(0);

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
    huge: bool,
    /// Size of the cells this region is carved into; 0 while it sits in the
    /// spare reserve. A big block's region is one cell of `len`.
    cell: usize = 0,
    cells: usize = 0,
    /// Cells handed out by bumping; the rest of the region is untouched.
    carved: usize = 0,
    live: usize = 0,
    free: ?*Node = null,
    partial_prev: ?*Region = null,
    partial_next: ?*Region = null,
    in_partial: bool = false,

    fn isFull(self: *const Region) bool {
        return self.free == null and self.carved == self.cells;
    }
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

fn slabLen(class: usize) usize {
    return std.math.clamp(std.mem.alignForward(usize, class * CELLS_PER_SLAB, HUGE_PAGE), HUGE_PAGE, SLAB_BYTES);
}

/// Resident bytes a block of `len` actually occupies in the pool: `len` for
/// sub-MIN_BLOCK blocks (exact backing allocation), the class cell for a slab
/// block, or the huge-page-rounded region for a big block. The cache uses this
/// so its capacity bounds real footprint.
pub fn cellSize(len: usize) usize {
    if (len < MIN_BLOCK) return len;
    const class = classSize(len);
    if (class <= MAX_SLAB_CLASS) return class;
    return std.mem.alignForward(usize, len, HUGE_PAGE);
}

/// Move `g_fallback_bytes` for a backing-allocator block going from `old_len`
/// to `new_len` bytes (0 = not allocated). Shorter blocks than MIN_BLOCK are
/// the backing allocator's by design, so only longer ones count.
fn trackFallback(old_len: usize, new_len: usize) void {
    if (old_len >= MIN_BLOCK) _ = g_fallback_bytes.fetchSub(old_len, .monotonic);
    if (new_len >= MIN_BLOCK) _ = g_fallback_bytes.fetchAdd(new_len, .monotonic);
}

pub const Pool = struct {
    backing: Allocator,
    lock: SpinLock = .{},
    /// Every mapped region (carved, big-block and spare), sorted by `base` for a
    /// binary-search ownership test on free.
    regions: std.ArrayListUnmanaged(*Region) = .empty,
    /// Head of each class's partial list: slabs with at least one free cell.
    /// Few distinct sizes (~36), so a hashmap of list heads is cheap and avoids
    /// size-class index arithmetic. Entries are never removed.
    partial: std.AutoHashMapUnmanaged(usize, ?*Region) = .empty,
    /// Empty regions, oldest first.
    spare: std.ArrayListUnmanaged(*Region) = .empty,
    spare_bytes: usize = 0,
    mapped_bytes: usize = 0,
    map_limit: usize,

    /// `capacity` is the cache's live-byte budget. The pool may map up to a
    /// quarter more for partially filled slabs and the spare reserve, and at
    /// least one full slab.
    pub fn init(backing: Allocator, capacity: usize) Pool {
        return .{ .backing = backing, .map_limit = @max(capacity +| capacity / 4, SLAB_BYTES) };
    }

    pub fn deinit(self: *Pool) void {
        for (self.regions.items) |r| self.releaseRegionMemory(r);
        self.regions.deinit(self.backing);
        self.partial.deinit(self.backing);
        self.spare.deinit(self.backing);
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
        if (len >= MIN_BLOCK and alignment.toByteUnits() <= MAX_CELL_ALIGN) {
            self.lock.lock();
            const block = self.allocBlock(len);
            self.lock.unlock();
            if (block) |p| return p;
        }
        const p = self.backing.rawAlloc(len, alignment, ra) orelse return null;
        trackFallback(0, len);
        return p;
    }

    fn vtResize(ctx: *anyopaque, mem: []u8, alignment: Alignment, new_len: usize, ra: usize) bool {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        if (!self.owns(mem)) {
            if (!self.backing.rawResize(mem, alignment, new_len, ra)) return false;
            trackFallback(mem.len, new_len);
            return true;
        }
        // In place only when the new length still fits the same cell.
        return new_len >= MIN_BLOCK and cellSize(new_len) == cellSize(mem.len);
    }

    fn vtRemap(ctx: *anyopaque, mem: []u8, alignment: Alignment, new_len: usize, ra: usize) ?[*]u8 {
        if (vtResize(ctx, mem, alignment, new_len, ra)) return mem.ptr;
        return null;
    }

    fn vtFree(ctx: *anyopaque, mem: []u8, alignment: Alignment, ra: usize) void {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        if (mem.len >= MIN_BLOCK) {
            self.lock.lock();
            if (self.regionAt(mem.ptr)) |region| {
                self.freeCell(region, mem.ptr);
                self.lock.unlock();
                return;
            }
            self.lock.unlock();
        }
        self.backing.rawFree(mem, alignment, ra);
        trackFallback(mem.len, 0);
    }

    /// A cell is never shorter than MIN_BLOCK, so a shorter block belongs to
    /// the backing allocator without a lookup.
    fn owns(self: *Pool, mem: []u8) bool {
        if (mem.len < MIN_BLOCK) return false;
        self.lock.lock();
        defer self.lock.unlock();
        return self.regionAt(mem.ptr) != null;
    }

    /// Null when the block must come from the backing allocator instead: the
    /// map limit is reached or the OS and backing allocator are both dry.
    fn allocBlock(self: *Pool, len: usize) ?[*]u8 {
        const class = classSize(len);
        if (class > MAX_SLAB_CLASS) {
            const r = self.takeRegion(std.mem.alignForward(usize, len, HUGE_PAGE)) orelse return null;
            r.cell = r.len;
            r.cells = 1;
            r.carved = 1;
            r.live = 1;
            return r.base;
        }
        const head = self.partial.getOrPut(self.backing, class) catch return null;
        if (!head.found_existing) head.value_ptr.* = null;
        if (head.value_ptr.*) |slab| {
            const p = takeCell(slab);
            if (slab.isFull()) self.unlinkPartial(slab);
            return p;
        }
        const slab = self.takeRegion(slabLen(class)) orelse return null;
        slab.cell = class;
        slab.cells = slab.len / class;
        const p = takeCell(slab);
        self.pushPartial(slab);
        return p;
    }

    fn takeCell(slab: *Region) [*]u8 {
        slab.live += 1;
        if (slab.free) |node| {
            slab.free = node.next;
            return @ptrCast(node);
        }
        const p = slab.base + slab.carved * slab.cell;
        slab.carved += 1;
        return p;
    }

    fn freeCell(self: *Pool, region: *Region, p: [*]u8) void {
        region.live -= 1;
        if (region.live == 0) {
            if (region.in_partial) self.unlinkPartial(region);
            self.retire(region);
            return;
        }
        const node: *Node = @ptrCast(@alignCast(p));
        node.next = region.free;
        region.free = node;
        if (!region.in_partial) self.pushPartial(region);
    }

    fn pushPartial(self: *Pool, slab: *Region) void {
        const head = self.partial.getPtr(slab.cell).?;
        slab.partial_prev = null;
        slab.partial_next = head.*;
        if (head.*) |h| h.partial_prev = slab;
        head.* = slab;
        slab.in_partial = true;
    }

    fn unlinkPartial(self: *Pool, slab: *Region) void {
        if (slab.partial_prev) |prev| {
            prev.partial_next = slab.partial_next;
        } else {
            self.partial.getPtr(slab.cell).?.* = slab.partial_next;
        }
        if (slab.partial_next) |next| next.partial_prev = slab.partial_prev;
        slab.partial_prev = null;
        slab.partial_next = null;
        slab.in_partial = false;
    }

    /// Move an empty region to the spare reserve.
    fn retire(self: *Pool, region: *Region) void {
        region.* = .{ .base = region.base, .len = region.len, .huge = region.huge };
        self.spare.append(self.backing, region) catch {
            self.unmapRegion(region);
            return;
        };
        self.spare_bytes += region.len;
    }

    fn popOldestSpare(self: *Pool) *Region {
        const r = self.spare.orderedRemove(0);
        self.spare_bytes -= r.len;
        return r;
    }

    /// An empty region of exactly `len`: the newest spare one when the reserve
    /// has it (leaving the oldest to unmap), else a fresh mapping, unmapping
    /// spares first if that would cross `map_limit`.
    fn takeRegion(self: *Pool, len: usize) ?*Region {
        var i = self.spare.items.len;
        while (i > 0) {
            i -= 1;
            const r = self.spare.items[i];
            if (r.len != len) continue;
            _ = self.spare.orderedRemove(i);
            self.spare_bytes -= len;
            return r;
        }
        while (self.mapped_bytes + len > self.map_limit) {
            if (self.spare.items.len == 0) return null;
            self.unmapRegion(self.popOldestSpare());
        }
        return self.mapRegion(len) catch null;
    }

    fn mapRegion(self: *Pool, len: usize) !*Region {
        const region = try self.backing.create(Region);
        errdefer self.backing.destroy(region);
        var huge = true;
        const base = osAlloc(len) orelse blk: {
            huge = false;
            const p = self.backing.rawAlloc(len, REGION_ALIGN, @returnAddress()) orelse return error.OutOfMemory;
            break :blk @as([*]align(16) u8, @alignCast(p));
        };
        region.* = .{ .base = base, .len = len, .huge = huge };
        errdefer if (huge) osFree(base, len) else self.backing.rawFree(base[0..len], REGION_ALIGN, @returnAddress());
        try self.insertRegionSorted(region);
        if (huge) {
            _ = g_slabs_huge.fetchAdd(1, .monotonic);
        } else {
            _ = g_slabs_plain.fetchAdd(1, .monotonic);
        }
        self.mapped_bytes += len;
        _ = g_slab_bytes.fetchAdd(len, .monotonic);
        return region;
    }

    fn unmapRegion(self: *Pool, region: *Region) void {
        self.removeRegion(region);
        self.mapped_bytes -= region.len;
        self.releaseRegionMemory(region);
    }

    fn releaseRegionMemory(self: *Pool, region: *Region) void {
        _ = g_slab_bytes.fetchSub(region.len, .monotonic);
        if (region.huge) {
            osFree(region.base, region.len);
        } else {
            self.backing.rawFree(region.base[0..region.len], REGION_ALIGN, @returnAddress());
        }
        self.backing.destroy(region);
    }

    fn insertRegionSorted(self: *Pool, region: *Region) !void {
        try self.regions.insert(self.backing, self.lowerBound(@intFromPtr(region.base)), region);
    }

    fn removeRegion(self: *Pool, region: *Region) void {
        const i = self.lowerBound(@intFromPtr(region.base));
        std.debug.assert(self.regions.items[i] == region);
        _ = self.regions.orderedRemove(i);
    }

    fn lowerBound(self: *Pool, addr: usize) usize {
        var lo: usize = 0;
        var hi: usize = self.regions.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (@intFromPtr(self.regions.items[mid].base) < addr) lo = mid + 1 else hi = mid;
        }
        return lo;
    }

    /// Binary search for the region whose [base, base+len) contains `p`.
    fn regionAt(self: *Pool, p: [*]u8) ?*Region {
        const addr = @intFromPtr(p);
        var lo: usize = 0;
        var hi: usize = self.regions.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const r = self.regions.items[mid];
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
    var pool = Pool.init(std.testing.allocator, 64 << 20);
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

test "a freed cell is reused while its slab still holds others" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
    defer pool.deinit();
    const a = pool.allocator();

    const b1 = try a.alignedAlloc(u8, .@"16", 64 * 1024);
    const b2 = try a.alignedAlloc(u8, .@"16", 64 * 1024);
    defer a.free(b2);
    const p1 = b1.ptr;
    a.free(b1);
    const b3 = try a.alignedAlloc(u8, .@"16", 64 * 1024);
    defer a.free(b3);
    try std.testing.expectEqual(p1, b3.ptr);
    try std.testing.expectEqual(HUGE_PAGE, pool.mapped_bytes);
}

test "an emptied slab serves another class" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
    defer pool.deinit();
    const a = pool.allocator();

    const b1 = try a.alignedAlloc(u8, .@"16", 64 * 1024);
    const p1 = b1.ptr;
    a.free(b1);
    try std.testing.expectEqual(HUGE_PAGE, pool.spare_bytes);
    // A 192 KiB class also lives in 2 MiB slabs, so it takes the spare one.
    const b2 = try a.alignedAlloc(u8, .@"16", 192 * 1024);
    defer a.free(b2);
    try std.testing.expectEqual(p1, b2.ptr);
    try std.testing.expectEqual(HUGE_PAGE, pool.mapped_bytes);
    try std.testing.expectEqual(@as(usize, 0), pool.spare_bytes);
}

test "a big block's region is reused by a block rounding to the same length" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
    defer pool.deinit();
    const a = pool.allocator();

    const b1 = try a.alignedAlloc(u8, .@"16", 5 << 20);
    const p1 = b1.ptr;
    a.free(b1);
    const b2 = try a.alignedAlloc(u8, .@"16", (5 << 20) + (512 << 10));
    defer a.free(b2);
    try std.testing.expectEqual(p1, b2.ptr);
    try std.testing.expectEqual(@as(usize, 6 << 20), pool.mapped_bytes);
}

test "spares stay mapped until a new mapping needs their room" {
    var pool = Pool.init(std.testing.allocator, 0);
    defer pool.deinit();
    const a = pool.allocator();
    try std.testing.expectEqual(@as(usize, 32 << 20), pool.map_limit);

    // Each 5 MiB block gets a 6 MiB region of its own.
    var blocks: [5][]align(16) u8 = undefined;
    for (&blocks) |*b| b.* = try a.alignedAlloc(u8, .@"16", 5 << 20);
    for (blocks) |b| a.free(b);
    try std.testing.expectEqual(@as(usize, 30 << 20), pool.spare_bytes);
    try std.testing.expectEqual(pool.spare_bytes, pool.mapped_bytes);

    // An 8 MiB region fits under the limit only once the oldest spare goes.
    const wide = try a.alignedAlloc(u8, .@"16", 7 << 20);
    defer a.free(wide);
    try std.testing.expectEqual(@as(usize, 32 << 20), pool.mapped_bytes);
    try std.testing.expectEqual(@as(usize, 24 << 20), pool.spare_bytes);

    // A 6 MiB request takes the newest spare.
    const again = try a.alignedAlloc(u8, .@"16", 5 << 20);
    defer a.free(again);
    try std.testing.expectEqual(blocks[4].ptr, again.ptr);
    try std.testing.expectEqual(@as(usize, 18 << 20), pool.spare_bytes);
}

test "mapping stops at the map limit and the backing allocator serves the rest" {
    var pool = Pool.init(std.testing.allocator, 0);
    defer pool.deinit();
    const a = pool.allocator();
    try std.testing.expectEqual(@as(usize, 32 << 20), pool.map_limit);

    const fallback_before = g_fallback_bytes.load(.monotonic);
    var blocks: [6][]align(16) u8 = undefined;
    for (&blocks, 0..) |*b, i| {
        b.* = try a.alignedAlloc(u8, .@"16", 5 << 20);
        @memset(b.*, @intCast(i));
    }
    try std.testing.expectEqual(@as(usize, 30 << 20), pool.mapped_bytes);
    try std.testing.expect(pool.regionAt(blocks[5].ptr) == null);
    try std.testing.expectEqual(fallback_before + (5 << 20), g_fallback_bytes.load(.monotonic));
    for (blocks, 0..) |b, i| try std.testing.expectEqual(@as(u8, @intCast(i)), b[b.len - 1]);
    for (blocks) |b| a.free(b);
    try std.testing.expectEqual(fallback_before, g_fallback_bytes.load(.monotonic));
}

test "resize stays in place only within the block's cell" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
    defer pool.deinit();
    const a = pool.allocator();

    // A 100 KiB block sits in a 112 KiB cell.
    var b = try a.alignedAlloc(u8, .@"16", 100 * 1024);
    try std.testing.expect(a.resize(b, 110 * 1024));
    b.len = 110 * 1024;
    try std.testing.expect(!a.resize(b, 120 * 1024));
    try std.testing.expect(!a.resize(b, 1024));
    a.free(b);
}

test "pool serves many concurrent-sized blocks without overlap" {
    var pool = Pool.init(std.testing.allocator, 64 << 20);
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
