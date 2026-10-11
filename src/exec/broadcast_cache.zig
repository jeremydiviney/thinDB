//! Cross-statement cache of table functions' broadcast inputs.
//!
//! A kernel's worker state derives only from its broadcast inputs and call
//! arguments (udf.TvfContext), so statements whose broadcast inputs are the
//! same subtrees over the same table versions can reuse both the drained
//! input columns and every worker state built over them. A state may point
//! into those columns, so an entry keeps both alive together. A state is
//! leased to one worker at a time, which keeps a kernel that fills its state
//! lazily safe.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ColumnStore = @import("../engine/store.zig").ColumnStore;
const memory = @import("../memory.zig");

const MAX_ENTRIES = 16;
/// Free states one entry keeps: a worker per state for the widest statement,
/// plus what concurrent statements built alongside it.
const MAX_STATES = 64;

pub const WorkerState = struct {
    arena: std.heap.ArenaAllocator,
    state: ?*anyopaque = null,
};

/// One broadcast input's drained columns, converted to the declared types.
pub const Input = struct {
    cols: []ColumnStore,
    rows: usize,
};

pub const Entry = struct {
    key: u64,
    inputs: []Input,
    states: std.ArrayListUnmanaged(*WorkerState) = .empty,
    /// Statements holding the entry. Only an unheld entry is evicted.
    refs: usize = 0,
    used: u64 = 0,
    bytes: usize = 0,
};

/// CAS spinlock: every critical section is a few slot flips.
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

/// The cap on retained bytes: `THINDB_BROADCAST_CACHE_MB`, 0 disabling the
/// cache.
pub fn capBytes() usize {
    if (getenv("THINDB_BROADCAST_CACHE_MB")) |v| {
        const mb = std.fmt.parseInt(usize, std.mem.span(v), 10) catch return DEFAULT_CAP;
        return mb << 20;
    }
    return DEFAULT_CAP;
}

const DEFAULT_CAP: usize = 2048 << 20;

pub const Cache = struct {
    alloc: Allocator,
    max_bytes: usize,
    mu: SpinLock = .{},
    entries: [MAX_ENTRIES]?*Entry = @splat(null),
    clock: u64 = 0,
    /// Unheld entries, as the shared pool sees them: they give way when live
    /// queries need the budget.
    idle: memory.IdleSource = .{ .idle_bytes_fn = idleBytesErased, .reclaim_fn = reclaimErased },
    shared: ?*memory.MemoryPool = null,

    pub fn create(alloc: Allocator, max_bytes: usize, pool: ?*memory.MemoryPool) !*Cache {
        const self = try alloc.create(Cache);
        self.* = .{ .alloc = alloc, .max_bytes = max_bytes, .shared = pool };
        if (pool) |p| p.registerIdle(&self.idle);
        return self;
    }

    /// No statement may still hold an entry.
    pub fn destroy(self: *Cache) void {
        if (self.shared) |p| p.unregisterIdle(&self.idle);
        for (self.entries) |slot| if (slot) |entry| self.destroyEntry(entry);
        self.alloc.destroy(self);
    }

    /// The entry under `key`, held until `release`.
    pub fn acquire(self: *Cache, key: u64) ?*Entry {
        self.mu.lock();
        defer self.mu.unlock();
        for (self.entries) |slot| {
            const entry = slot orelse continue;
            if (entry.key != key) continue;
            entry.refs += 1;
            self.clock +%= 1;
            entry.used = self.clock;
            return entry;
        }
        return null;
    }

    /// A free state of the held `entry`, or a new empty one.
    pub fn leaseState(self: *Cache, entry: ?*Entry) !*WorkerState {
        if (entry) |e| {
            self.mu.lock();
            const free = e.states.pop();
            self.mu.unlock();
            if (free) |state| return state;
        }
        const state = try self.alloc.create(WorkerState);
        state.* = .{ .arena = std.heap.ArenaAllocator.init(self.alloc) };
        return state;
    }

    pub fn destroyState(self: *Cache, state: *WorkerState) void {
        state.arena.deinit();
        self.alloc.destroy(state);
    }

    /// Hand back a held entry with the states its statement leased. Without
    /// `keep` the states are dropped: a failed statement may have left them
    /// half built.
    pub fn release(self: *Cache, entry: *Entry, states: []const *WorkerState, keep: bool) void {
        self.mu.lock();
        const kept = if (keep) @min(states.len, MAX_STATES - entry.states.items.len) else 0;
        for (states[0..kept]) |state| entry.states.appendAssumeCapacity(state);
        entry.refs -= 1;
        entry.bytes = entryBytes(entry);
        self.mu.unlock();
        for (states[kept..]) |state| self.destroyState(state);
        self.trim();
    }

    /// Keep a statement's drained broadcast inputs and the states built over
    /// them under `key`. Takes ownership of `inputs` (allocated, with every
    /// column, from `alloc`) and of `states`.
    pub fn publish(self: *Cache, key: u64, inputs: []Input, states: []const *WorkerState) void {
        const entry = self.alloc.create(Entry) catch {
            self.destroyInputs(inputs);
            for (states) |state| self.destroyState(state);
            return;
        };
        entry.* = .{ .key = key, .inputs = inputs };
        entry.states.ensureTotalCapacity(self.alloc, MAX_STATES) catch {
            self.destroyEntry(entry);
            for (states) |state| self.destroyState(state);
            return;
        };
        const kept = @min(states.len, MAX_STATES);
        for (states[0..kept]) |state| entry.states.appendAssumeCapacity(state);
        for (states[kept..]) |state| self.destroyState(state);
        entry.bytes = entryBytes(entry);

        self.mu.lock();
        const slot = blk: {
            var free: ?*?*Entry = null;
            var oldest: ?*?*Entry = null;
            for (&self.entries) |*s| {
                const held = s.* orelse {
                    if (free == null) free = s;
                    continue;
                };
                // A concurrent statement published first; its states point
                // into its own columns, so the two never merge.
                if (held.key == key) break :blk null;
                if (held.refs != 0) continue;
                if (oldest == null or held.used < oldest.?.*.?.used) oldest = s;
            }
            break :blk free orelse oldest;
        };
        const evicted = if (slot) |s| s.* else null;
        if (slot) |s| {
            self.clock +%= 1;
            entry.used = self.clock;
            s.* = entry;
        }
        self.mu.unlock();
        if (slot == null) self.destroyEntry(entry);
        if (evicted) |old| self.destroyEntry(old);
        self.trim();
    }

    fn trim(self: *Cache) void {
        var evicted: [MAX_ENTRIES]*Entry = undefined;
        var n: usize = 0;
        self.mu.lock();
        var total: usize = 0;
        for (self.entries) |slot| if (slot) |entry| {
            total +|= entry.bytes;
        };
        while (total > self.max_bytes) {
            const entry = self.takeOldestIdle() orelse break;
            total -|= entry.bytes;
            evicted[n] = entry;
            n += 1;
        }
        self.mu.unlock();
        for (evicted[0..n]) |entry| self.destroyEntry(entry);
    }

    /// Caller holds `mu`.
    fn takeOldestIdle(self: *Cache) ?*Entry {
        var oldest: ?*?*Entry = null;
        for (&self.entries) |*s| {
            const entry = s.* orelse continue;
            if (entry.refs != 0) continue;
            if (oldest == null or entry.used < oldest.?.*.?.used) oldest = s;
        }
        const s = oldest orelse return null;
        const entry = s.*.?;
        s.* = null;
        return entry;
    }

    fn idleBytesErased(source: *memory.IdleSource) usize {
        const self: *Cache = @fieldParentPtr("idle", source);
        self.mu.lock();
        defer self.mu.unlock();
        var total: usize = 0;
        for (self.entries) |slot| if (slot) |entry| {
            if (entry.refs == 0) total +|= entry.bytes;
        };
        return total;
    }

    fn reclaimErased(source: *memory.IdleSource, want: usize) usize {
        const self: *Cache = @fieldParentPtr("idle", source);
        var evicted: [MAX_ENTRIES]*Entry = undefined;
        var n: usize = 0;
        var released: usize = 0;
        self.mu.lock();
        while (released < want) {
            const entry = self.takeOldestIdle() orelse break;
            released +|= entry.bytes;
            evicted[n] = entry;
            n += 1;
        }
        self.mu.unlock();
        for (evicted[0..n]) |entry| self.destroyEntry(entry);
        return released;
    }

    fn destroyInputs(self: *Cache, inputs: []Input) void {
        for (inputs) |input| {
            for (input.cols) |*c| c.deinit(self.alloc);
            self.alloc.free(input.cols);
        }
        self.alloc.free(inputs);
    }

    fn destroyEntry(self: *Cache, entry: *Entry) void {
        self.destroyInputs(entry.inputs);
        for (entry.states.items) |state| self.destroyState(state);
        entry.states.deinit(self.alloc);
        self.alloc.destroy(entry);
    }
};

/// Free states and columns only: a leased state is in use on a worker.
fn entryBytes(entry: *const Entry) usize {
    var total: usize = 0;
    for (entry.inputs) |input| {
        for (input.cols) |c| total +|= c.heldBytes();
    }
    for (entry.states.items) |state| total +|= state.arena.queryCapacity();
    return total;
}

fn testInputs(alloc: Allocator, values: []const i64) ![]Input {
    const cols = try alloc.alloc(ColumnStore, 1);
    errdefer alloc.free(cols);
    cols[0] = try ColumnStore.init(alloc, .{ .bigint = {} }, false);
    errdefer cols[0].deinit(alloc);
    for (values) |v| try cols[0].data.bigint.append(alloc, v);
    const inputs = try alloc.alloc(Input, 1);
    inputs[0] = .{ .cols = cols, .rows = values.len };
    return inputs;
}

test "broadcast cache hands states back to their entry and never evicts a held one" {
    const alloc = std.testing.allocator;
    const cache = try Cache.create(alloc, std.math.maxInt(usize), null);
    defer cache.destroy();

    const built = try cache.leaseState(null);
    built.state = try built.arena.allocator().create(u64);
    cache.publish(7, try testInputs(alloc, &.{ 1, 2, 3 }), &.{built});

    try std.testing.expect(cache.acquire(8) == null);
    const entry = cache.acquire(7).?;
    try std.testing.expectEqual(@as(usize, 3), entry.inputs[0].rows);
    const reused = try cache.leaseState(entry);
    try std.testing.expectEqual(built, reused);
    const extra = try cache.leaseState(entry);
    try std.testing.expect(extra != built);

    // Full of held and newer entries: a held one is never the victim.
    for (0..MAX_ENTRIES) |i| cache.publish(100 + i, try testInputs(alloc, &.{@intCast(i)}), &.{});
    try std.testing.expect(cache.acquire(100) == null);

    cache.release(entry, &.{ reused, extra }, true);
    const again = cache.acquire(7).?;
    try std.testing.expectEqual(@as(usize, 2), again.states.items.len);
    cache.release(again, &.{}, true);
}

test "broadcast cache drops a failed statement's states and trims to its cap" {
    const alloc = std.testing.allocator;
    const cache = try Cache.create(alloc, 0, null);
    defer cache.destroy();

    cache.publish(1, try testInputs(alloc, &.{ 1, 2 }), &.{});
    try std.testing.expect(cache.acquire(1) == null);

    cache.max_bytes = std.math.maxInt(usize);
    cache.publish(2, try testInputs(alloc, &.{ 1, 2 }), &.{});
    const entry = cache.acquire(2).?;
    const state = try cache.leaseState(entry);
    cache.release(entry, &.{state}, false);
    const again = cache.acquire(2).?;
    try std.testing.expectEqual(@as(usize, 0), again.states.items.len);
    cache.release(again, &.{}, true);

    // A second publish under a live key keeps the first entry.
    const first = cache.acquire(2).?;
    cache.release(first, &.{}, true);
    cache.publish(2, try testInputs(alloc, &.{9}), &.{});
    const kept = cache.acquire(2).?;
    try std.testing.expectEqual(first, kept);
    cache.release(kept, &.{}, true);
}

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;
