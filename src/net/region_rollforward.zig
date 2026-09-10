//! Keyed pipeline regions: the compiler for `WITH KEYED BY (...)` blocks
//! (tasks #184/#185, docs/plans/REGION_PLAN.md §7).
//!
//! The compiler checks partitions against the declared keys before forming
//! a region. Incompatible operators execute through ordinary stages; a
//! later compatible CTE can enter a new region. Each region partitions its
//! input by a declared key's hash, then runs its operator chain
//! shard-locally with zero stage materializations. There is no fixed query
//! shape — the block's IR is collected into an ordered step list and each
//! step dispatches on STRUCTURE and kernel SDK METADATA (execution mode,
//! passthrough, broadcast inputs), never on names. The region's output
//! becomes an ordinary Stage (per-shard chunks adopted zero-copy); the rest
//! of the query compiles normally above it.
//!
//! Semantic preconditions are verified by EXECUTING small subtrees at
//! compile time (same precedent as scalar-subquery resolution): LEFT-join
//! build sides proven empty reduce to typed NULL columns; small join sides
//! and secondary TVF inputs are drained into broadcast maps/partitions.
//!
//! Name discipline: the region frame only ever APPENDS columns (no
//! pruning), so the engine's suffix-based column resolution would misbind
//! refs like `$amount` against stale entry columns. The recognizer instead
//! maintains the engine-visible name map itself (types.findColumn rules,
//! updated per select/alias/group node) and deep-clones every captured
//! expression, rewriting each col_ref to the unique frame-column name it
//! resolves to at that point.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ir = @import("../ir/ir.zig");
const exec = @import("../exec/exec.zig");
const engine_v2 = @import("../exec/engine_v2.zig");
const region = @import("../exec/region_exec.zig");
const mat_stage = @import("../exec/mat_stage.zig");
const compute_mod = @import("../exec/compute.zig");
const expr_mod = @import("../exec/expr.zig");
const predicate_mod = @import("../exec/predicate.zig");
const types = @import("../types.zig");
const udf_mod = @import("../udf.zig");
const cte_stages = @import("cte_stages.zig");
const aggregate_mod = @import("../exec/aggregate.zig");

const Column = types.Column;
const Value = types.Value;
const ColumnStore = @import("../engine/store.zig").ColumnStore;
const ColumnView = @import("../storage/storage.zig").ColumnView;
const Expr = expr_mod.Expr;
const PredicateExpr = predicate_mod.PredicateExpr;
const Derived = compute_mod.Derived;
const Scan = exec.Scan;


extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

const NoMatch = error.RegionNoMatch;

pub const Recognized = struct {
    anchor: *const ir.Op,
    query: exec.Query,
};
// ---------------------------------------------------------------------------
// Declared regions: `WITH KEYED BY (k1, ...)`. Only compatible portions run
// within regions; unsupported portions retain ordinary SQL execution.
// Join right sides and secondary TVF inputs are exempt from the key check:
// they are broadcast /
// empty-proof candidates the region compiler validates by executing them.
// ---------------------------------------------------------------------------

/// Returns null when no declared boundary can run regionally. Ordinary SQL
/// compilation then preserves both query results and semantic errors.
pub fn compileDeclared(input: engine_v2.CompileInput, root: *const ir.Op) anyerror!?Recognized {
    // Find the topmost CTE boundary carrying declared keys.
    var cur = root;
    var depth: usize = 0;
    const top: *const ir.Op = blk: {
        while (depth < 256) : (depth += 1) {
            switch (cur.*) {
                .materialize => |m| {
                    if (m.region_keys != null) break :blk cur;
                    cur = m.upstream;
                },
                else => cur = region_spine_upstream(cur) orelse return null,
            }
        }
        return null;
    };
    const keys = top.materialize.region_keys.?;
    const shape_hash = hashAnchor(top);
    const declaration_hash = hash_declaration(input, top);
    if (try_cached_declaration(input, top, keys, declaration_hash)) |recognized| return recognized;

    // This hint retains no rows or resolved expressions. Rebuild the selected
    // subtree from fresh IR after a data change instead of re-executing outer
    // candidates that previously failed. A stale hint still has to compile.
    if (shape_hash) |shape| if (cacheFor(input.db)) |cache| {
        if (cache.boundary(shape)) |selected_depth| hint: {
            var anchor = top;
            for (0..selected_depth) |_| anchor = region_spine_upstream(anchor) orelse break :hint;
            if (anchor.* != .materialize) break :hint;
            verifyKeyContract(anchor, keys, 0) catch |err| {
                if (err == error.OutOfMemory) return err;
                break :hint;
            };
            const declaration: ?DeclaredBoundary = if (declaration_hash) |hash| .{ .hash = hash, .depth = selected_depth } else null;
            if (buildRegion(input, anchor, keys, hashAnchor(anchor), declaration)) |q| {
                if (getenv("THINDB_REGION_TRACE") != null) std.debug.print("[region] boundary hint rebuilt depth={d}\n", .{selected_depth});
                return .{ .anchor = anchor, .query = q };
            } else |err| {
                if (err == error.OutOfMemory) return err;
            }
        }
    };

    // Compile: try the marked boundary, then boundaries below it — the
    // program anchor can sit under the outermost CTE (e.g. when the final
    // CTE is a plain projection the staged path composes above the region,
    // or when the chain ends in a cross-key rollup like the API's
    // date × upDownType aggregate). The contract is verified PER BOUNDARY:
    // a boundary whose subtree contains a coarser-than-keys partition is
    // not a legal region root, but a boundary BELOW that partition can be —
    // the rollup then consumes the region's output in the normal engine.
    // Incompatible windows may also become ordinary ingress for a region
    // above them; collectPipeline makes that cut before adding the window.
    var any_conforming = false;
    cur = top;
    depth = 0;
    while (depth < 256) : (depth += 1) {
        switch (cur.*) {
            .materialize => |m| {
                const conforms = blk: {
                    verifyKeyContract(cur, keys, 0) catch |e| {
                        if (e == error.OutOfMemory) return e;
                        break :blk false;
                    };
                    break :blk true;
                };
                if (conforms) {
                    any_conforming = true;
                    // Hash ONCE per boundary and share it between the hit
                    // attempt and the store: recognize-time subtree drains
                    // can benignly rewrite IR (scalar resolution), and a
                    // recomputed hash would never match its own store.
                    const bh = hashAnchor(cur);
                    const declaration: ?DeclaredBoundary = if (declaration_hash) |hash| .{ .hash = hash, .depth = depth } else null;
                    if (tryCachedAt(input, cur, keys, bh, declaration)) |q| {
                        if (shape_hash) |shape| if (cacheFor(input.db)) |cache| cache.remember_boundary(shape, depth);
                        return .{ .anchor = cur, .query = q };
                    }
                    if (buildRegion(input, cur, keys, bh, declaration)) |q| {
                        if (shape_hash) |shape| if (cacheFor(input.db)) |cache| cache.remember_boundary(shape, depth);
                        return .{ .anchor = cur, .query = q };
                    } else |e| {
                        if (e == error.OutOfMemory) return e;
                        if (getenv("THINDB_REGION_TRACE") != null) {
                            std.debug.print("[region] declared block declined at {*}: {s}\n", .{ cur, @errorName(e) });
                            if (@errorReturnTrace()) |t| {
                                const n = @min(t.index, t.instruction_addresses.len);
                                const st = std.debug.StackTrace{
                                    .return_addresses = t.instruction_addresses[0..n],
                                    .skipped = .none,
                                };
                                std.debug.dumpStackTrace(&st);
                            }
                        }
                    }
                }
                cur = m.upstream;
            },
            else => cur = region_spine_upstream(cur) orelse break,
        }
    }
    if (getenv("THINDB_REGION_TRACE") != null) {
        std.debug.print("[region] ordinary execution: {s}\n", .{if (any_conforming) "no supported region boundary" else "no compatible key boundary"});
    }
    return null;
}

fn region_spine_upstream(op: *const ir.Op) ?*const ir.Op {
    return switch (op.*) {
        .materialize => |m| m.upstream,
        .select, .exclude => |p| p.upstream,
        .filter => |f| f.upstream,
        .group_by => |g| g.upstream,
        .compute => |c| c.upstream,
        .alias => |a| a.upstream,
        .limit => |l| l.upstream,
        .order_by => |o| o.upstream,
        .window => |w| w.upstream,
        .join => |j| j.left,
        .table_fn => |t| if (t.inputs.len > 0) t.inputs[0] else null,
        else => null,
    };
}

/// Static half of the contract: walk the block's pipeline (left/primary
/// spine) until an ordinary ingress boundary is needed. Each regional
/// partition contains the declared keys, so it never needs another shard.
fn verifyKeyContract(op: *const ir.Op, keys: []const []const u8, depth: usize) anyerror!void {
    if (depth > 64) return error.RegionUnsupportedConstruct;
    var cur = op;
    var guard: usize = 0;
    while (guard < 256) : (guard += 1) {
        switch (cur.*) {
            .scan, .single_row, .file_scan => return,
            .materialize => |m| cur = m.upstream,
            .alias => |a| cur = a.upstream,
            .select => |p| cur = p.upstream,
            .exclude => |p| cur = p.upstream,
            .filter => |f| cur = f.upstream,
            .compute => |c| cur = c.upstream,
            .limit => |l| cur = l.upstream,
            .order_by => |o| cur = o.upstream,
            .group_by => |g| {
                if (!contains_keys(keys, g.group_cols)) return;
                cur = g.upstream;
            },
            .window => |w| {
                for (w.specs) |spec| if (!contains_keys(keys, spec.partition_by)) return;
                cur = w.upstream;
            },
            .table_fn => |t| {
                if (t.partition_by.len > 0 and !contains_keys(keys, t.partition_by)) return;
                if (t.inputs.len == 0) return;
                cur = t.inputs[0];
            },
            .join => |j| cur = j.left,
            .set_union => |u| {
                try verifyKeyContract(u.left, keys, depth + 1);
                cur = u.right;
            },
            else => {
                std.debug.print("[region] KEYED BY contract: unsupported construct '{s}' inside the block\n", .{@tagName(std.meta.activeTag(cur.*))});
                return error.RegionUnsupportedConstruct;
            },
        }
    }
    return error.RegionUnsupportedConstruct;
}

fn contains_keys(keys: []const []const u8, cols: []const []const u8) bool {
    outer: for (keys) |k| {
        for (cols) |c| {
            if (std.ascii.eqlIgnoreCase(lastSegment(c), lastSegment(k))) continue :outer;
        }
        return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Cross-run region cache — the probe's pooled-buffer discipline in engine
// form. Bounded entries per Database, keyed by a deterministic deep hash of the
// anchor IR subtree (post-fold, so folded dates/constants are captured). A
// hit revalidates everything the program BAKED at recognize time — broadcast
// and proof tables via a data-version fingerprint, kernel identity via
// process pointers — then rebuilds only the scans against a fresh snapshot
// and reuses the compiled Program plus its RegionPool, so exchange buckets,
// shard buffers, and op stores keep their capacities across queries.
// ---------------------------------------------------------------------------

const Unhashable = error.RegionUnhashable;

const TableVersion = struct { name: []const u8, version: u64 };
const KernelCheck = struct { name: []const u8, process: udf_mod.TvfProcess };
const DeclaredBoundary = struct { hash: u64, depth: usize };
const BoundaryHint = struct { shape_hash: u64, depth: usize, used: u64 };

/// CAS spinlock (std.Thread.Mutex is gone in Zig 0.16; Io.Mutex would drag
/// an Io through the recognizer). Critical sections here are flag flips —
/// version validation runs outside the lock.
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

const Cache = struct {
    alloc: Allocator,
    mu: SpinLock = .{},
    // Bound tiny-program metadata separately from the combined byte budget.
    entries: [32]CacheEntry = @splat(.{}),
    boundaries: [64]?BoundaryHint = @splat(null),
    clock: u64 = 0,
    max_retained_bytes: usize,
    /// In-flight background ctx destroys (eviction/invalidation) — a big
    /// pool frees seconds of allocator work, which must never sit on the
    /// incoming query's critical path. Database close waits for them.
    reaps_pending: std.atomic.Value(usize) = .init(0),

    /// Destroy an evicted/invalidated ctx off-thread. The ctx is exclusively
    /// owned by the caller at this point (it left the cache slot while
    /// busy-held), so the only ordering requirement is that the database —
    /// whose allocator the ctx frees into — outlives the reap, which
    /// deinitErased's wait guarantees.
    fn destroyCtxAsync(self: *Cache, ctx: *Ctx) void {
        _ = self.reaps_pending.fetchAdd(1, .acquire);
        const t = std.Thread.spawn(.{}, reap, .{ self, ctx }) catch {
            Ctx.destroyErased(ctx);
            _ = self.reaps_pending.fetchSub(1, .release);
            return;
        };
        t.detach();
    }

    fn reap(self: *Cache, ctx: *Ctx) void {
        Ctx.destroyErased(ctx);
        _ = self.reaps_pending.fetchSub(1, .release);
    }

    fn deinitErased(p: *anyopaque) void {
        const self: *Cache = @ptrCast(@alignCast(p));
        while (self.reaps_pending.load(.acquire) != 0) {
            std.Thread.yield() catch {};
        }
        const alloc = self.alloc;
        for (&self.entries) |*entry| if (entry.ctx) |ctx| Ctx.destroyErased(ctx);
        alloc.destroy(self);
    }

    fn checkout(self: *Cache, hash: u64) ?*CacheEntry {
        self.mu.lock();
        defer self.mu.unlock();
        for (&self.entries) |*entry| {
            if (entry.ctx == null or entry.busy or entry.hash != hash) continue;
            entry.busy = true;
            self.clock +%= 1;
            entry.used = self.clock;
            return entry;
        }
        return null;
    }

    fn publish(self: *Cache, hash: u64, ctx: *Ctx) ?*CacheEntry {
        self.mu.lock();
        const selected = blk: {
            var oldest: ?*CacheEntry = null;
            var empty: ?*CacheEntry = null;
            for (&self.entries) |*entry| {
                if (entry.busy) continue;
                if (entry.ctx == null) {
                    if (empty == null) empty = entry;
                    continue;
                }
                if (entry.hash == hash) break :blk entry;
                if (oldest == null or entry.used < oldest.?.used) oldest = entry;
            }
            break :blk empty orelse oldest orelse {
                self.mu.unlock();
                return null;
            };
        };
        const old = selected.ctx;
        self.clock +%= 1;
        selected.* = .{
            .owner = self,
            .hash = hash,
            .ctx = ctx,
            .busy = true,
            .used = self.clock,
            .retained_bytes = ctx.retained_bytes(),
        };
        self.mu.unlock();
        if (old) |c| self.destroyCtxAsync(c);
        return selected;
    }

    fn discard(self: *Cache, entry: *CacheEntry) void {
        self.mu.lock();
        const ctx = entry.ctx.?;
        entry.ctx = null;
        entry.busy = false;
        entry.retained_bytes = 0;
        self.mu.unlock();
        self.destroyCtxAsync(ctx);
    }

    fn trim(self: *Cache) void {
        var evicted: [32]*Ctx = undefined;
        var n: usize = 0;
        self.mu.lock();
        var total: usize = 0;
        for (&self.entries) |*entry| total +|= entry.retained_bytes;
        while (total > self.max_retained_bytes) {
            var oldest: ?*CacheEntry = null;
            for (&self.entries) |*entry| {
                if (entry.ctx == null or entry.busy) continue;
                if (oldest == null or entry.used < oldest.?.used) oldest = entry;
            }
            const entry = oldest orelse break;
            evicted[n] = entry.ctx.?;
            n += 1;
            total -|= entry.retained_bytes;
            entry.ctx = null;
            entry.retained_bytes = 0;
        }
        self.mu.unlock();
        for (evicted[0..n]) |ctx| self.destroyCtxAsync(ctx);
    }

    fn boundary(self: *Cache, shape_hash: u64) ?usize {
        self.mu.lock();
        defer self.mu.unlock();
        for (&self.boundaries) |*hint| {
            if (hint.*) |*h| if (h.shape_hash == shape_hash) {
                self.clock +%= 1;
                h.used = self.clock;
                return h.depth;
            };
        }
        return null;
    }

    fn remember_boundary(self: *Cache, shape_hash: u64, depth: usize) void {
        self.mu.lock();
        defer self.mu.unlock();
        var oldest: usize = 0;
        for (self.boundaries, 0..) |hint, i| {
            if (hint == null or hint.?.shape_hash == shape_hash) {
                oldest = i;
                break;
            }
            if (hint.?.used < self.boundaries[oldest].?.used) oldest = i;
        }
        self.clock +%= 1;
        self.boundaries[oldest] = .{ .shape_hash = shape_hash, .depth = depth, .used = self.clock };
    }
};

const CacheEntry = struct {
    owner: *Cache = undefined,
    hash: u64 = 0,
    ctx: ?*Ctx = null,
    busy: bool = false,
    used: u64 = 0,
    retained_bytes: usize = 0,

    fn releaseErased(p: *anyopaque) void {
        const self: *CacheEntry = @ptrCast(@alignCast(p));
        const owner = self.owner;
        const bytes = self.ctx.?.retained_bytes();
        owner.mu.lock();
        self.retained_bytes = bytes;
        self.busy = false;
        owner.mu.unlock();
        owner.trim();
    }
};

test "region cache retains independent programs and never evicts a borrowed entry" {
    const allocator = std.testing.allocator;
    const cache = try allocator.create(Cache);
    cache.* = .{ .alloc = allocator, .max_retained_bytes = std.math.maxInt(usize) };
    defer Cache.deinitErased(cache);
    for (0..cache.entries.len) |i| {
        const ctx = try allocator.create(Ctx);
        ctx.* = .{ .gpa = allocator, .arena = std.heap.ArenaAllocator.init(allocator), .pool = region.RegionPool.init(allocator, 0) };
        const entry = cache.publish(i, ctx).?;
        CacheEntry.releaseErased(entry);
    }
    const first = cache.checkout(0).?;
    try std.testing.expect(cache.checkout(0) == null);
    const second = cache.checkout(1).?;
    CacheEntry.releaseErased(second);
    const next = try allocator.create(Ctx);
    next.* = .{ .gpa = allocator, .arena = std.heap.ArenaAllocator.init(allocator), .pool = region.RegionPool.init(allocator, 0) };
    const published = cache.publish(cache.entries.len, next).?;
    try std.testing.expect(cache.checkout(2) == null);
    try std.testing.expectEqual(@as(u64, 0), first.hash);
    CacheEntry.releaseErased(published);
    CacheEntry.releaseErased(first);
    const reused = cache.checkout(0).?;
    try std.testing.expectEqual(first, reused);
    CacheEntry.releaseErased(reused);
}

test "region cache combined byte budget includes program arenas and preserves structural hints" {
    const allocator = std.testing.allocator;
    const cache = try allocator.create(Cache);
    cache.* = .{ .alloc = allocator, .max_retained_bytes = 0 };
    defer Cache.deinitErased(cache);
    cache.remember_boundary(17, 3);
    const ctx = try allocator.create(Ctx);
    ctx.* = .{ .gpa = allocator, .arena = std.heap.ArenaAllocator.init(allocator), .pool = region.RegionPool.init(allocator, 0) };
    const entry = cache.publish(17, ctx).?;
    _ = try ctx.arena.allocator().alloc(u8, 1024);
    cache.trim();
    try std.testing.expect(entry.ctx != null);
    CacheEntry.releaseErased(entry);
    try std.testing.expect(cache.checkout(17) == null);
    try std.testing.expectEqual(@as(?usize, 3), cache.boundary(17));
    cache.remember_boundary(17, 4);
    try std.testing.expectEqual(@as(?usize, 4), cache.boundary(17));
}

/// Get-or-create the per-database cache slot. Uses the DATABASE allocator —
/// the cache must outlive any single query or connection.
fn cacheFor(db: anytype) ?*Cache {
    db.region_cache_lock.lock();
    defer db.region_cache_lock.unlock();
    if (db.region_cache) |p| return @ptrCast(@alignCast(p));
    const c = db.allocator.create(Cache) catch return null;
    c.* = .{ .alloc = db.allocator, .max_retained_bytes = poolCapBytes() };
    db.region_cache = c;
    db.region_cache_deinit = Cache.deinitErased;
    return c;
}

fn poolCapBytes() usize {
    // Default sized for the rollforward-class region: worker-slot stores
    // ratchet toward n_workers × whale-shard footprint and plateau ~4GB on
    // the 3.6M-row workload — 6GB leaves margin so the retention policy
    // doesn't reset the pool right at steady state.
    if (getenv("THINDB_REGION_POOL_MB")) |v| {
        const mb = std.fmt.parseInt(usize, std.mem.span(v), 10) catch 8192;
        return mb << 20;
    }
    // DOP-24 worker slots plateau ~6.0GB on the 3.6M-row workload; a cap
    // inside the plateau resets the pool at steady state (measured: every
    // run repays cold allocations, 1.4s -> 5.6s warm).
    return 8192 << 20;
}

/// Data-version fingerprint of one table: memtable generation (bumped by
/// every retire-swap — flush/delete/update/alter) + memtable row count
/// (catches appends within a generation) + the segment set. Read under the
/// table mutex so the triple is coherent. Compaction changes the segment set
/// without changing values — a spurious invalidation, which is safe.
fn tableVersionOf(input: engine_v2.CompileInput, name: []const u8) ?u64 {
    var is_temp = false;
    const t = blk: {
        if (input.session.temp_namespace) |ns| {
            if (ns.findTable(name)) |tt| {
                is_temp = true;
                break :blk tt;
            }
        }
        break :blk input.db.openTable(name, .{}) catch return null;
    };
    t.mutex.lockUncancelable(t.io);
    defer t.mutex.unlock(t.io);
    var h = std.hash.Wyhash.init(0x7461626c65);
    if (is_temp) {
        // Recreated small lookup tables can retain a program and its worker
        // state only when their complete schema and ordered contents match.
        // Counters restart, and allocator reuse makes pointer identity unsafe.
        if (t.manifest.segments.items.len != 0 or t.memtable.row_count > 16384 or
            t.memtable.byteSize() > 4 * 1024 * 1024) return null;
        hu(&h, 1);
        hu(&h, t.memtable.row_count);
        hu(&h, @intFromBool(t.schema.unique));
        hu(&h, t.schema.order_key.len);
        for (t.schema.order_key) |key| hstr(&h, key);
        hu(&h, t.schema.columns.len);
        for (t.schema.columns, t.memtable.columns) |col, store| {
            hstr(&h, col.name);
            hashType(&h, col.type);
            hu(&h, @intFromBool(col.nullable));
            const view = store.view();
            if (view.nulls) |bits| {
                hu(&h, 1);
                h.update(bits[0..@intCast((t.memtable.row_count + 7) / 8)]);
            } else hu(&h, 0);
            switch (view.data) {
                .varchar, .string, .char, .json => |strings| {
                    h.update(std.mem.sliceAsBytes(strings.offsets));
                    hu(&h, strings.bytes.len);
                    h.update(strings.bytes);
                },
                inline else => |values| h.update(std.mem.sliceAsBytes(values)),
            }
        }
        return h.final();
    }
    hu(&h, 0);
    hu(&h, t.memtable_gen);
    hu(&h, t.memtable.row_count);
    for (t.manifest.segments.items) |e| {
        hu(&h, e.segment_id);
        hu(&h, e.row_count);
    }
    return h.final();
}

/// Record data-versions for every table a compile-time-executed subtree
/// reads. Capture happens BEFORE the drain: a write landing after capture
/// makes the recorded version stale relative to the drained data, which the
/// next query detects as a mismatch and re-recognizes — the safe direction.
fn recordSubtreeVersions(b: *Builder, node: *const ir.Op) void {
    if (b.ctx.uncacheable) return;
    collectVersions(b, node) catch {
        b.ctx.uncacheable = true;
    };
}

fn collectVersions(b: *Builder, node: *const ir.Op) !void {
    switch (node.*) {
        .scan => |s| try recordTableVersion(b, s.table.name),
        .alias => |x| try collectVersions(b, x.upstream),
        .select, .exclude => |p| try collectVersions(b, p.upstream),
        .filter => |f| try collectVersions(b, f.upstream),
        .group_by => |g| try collectVersions(b, g.upstream),
        .compute => |c| try collectVersions(b, c.upstream),
        .limit => |l| try collectVersions(b, l.upstream),
        .order_by => |o| try collectVersions(b, o.upstream),
        .materialize => |m| try collectVersions(b, m.upstream),
        .window => |w| try collectVersions(b, w.upstream),
        .join => |j| {
            try collectVersions(b, j.left);
            try collectVersions(b, j.right);
        },
        .set_union => |u| {
            try collectVersions(b, u.left);
            try collectVersions(b, u.right);
        },
        .table_fn => |t| for (t.inputs) |inp| try collectVersions(b, inp),
        .single_row => {},
        else => return Unhashable,
    }
}

fn recordTableVersion(b: *Builder, name: []const u8) !void {
    for (b.ctx.versions.items) |v| {
        if (std.ascii.eqlIgnoreCase(v.name, name)) return;
    }
    const ver = tableVersionOf(b.input, name) orelse {
        if (getenv("THINDB_REGION_TRACE") != null) {
            std.debug.print("[region] table '{s}' unversionable — ctx uncacheable\n", .{name});
        }
        return Unhashable;
    };
    try b.ctx.versions.append(b.a, .{ .name = try b.a.dupe(u8, name), .version = ver });
}

/// True when everything the cached program baked at recognize time is still
/// current: kernel identities and consumed-table data versions.
fn cacheValid(input: engine_v2.CompileInput, ctx: *Ctx) bool {
    if (ctx.uncacheable) return false;
    const registry = input.udf_registry orelse return false;
    for (ctx.kernels.items) |k| {
        const e = registry.tableByName(k.name) orelse return false;
        if (e.process != k.process) return false;
    }
    for (ctx.versions.items) |v| {
        const now = tableVersionOf(input, v.name) orelse return false;
        if (now != v.version) return false;
    }
    return true;
}

fn try_cached_declaration(input: engine_v2.CompileInput, top: *const ir.Op, keys: []const []const u8, declaration_hash: ?u64) ?Recognized {
    const hash = declaration_hash orelse {
        if (getenv("THINDB_REGION_TRACE") != null) std.debug.print("[region] declaration unhashable: search boundaries\n", .{});
        return null;
    };
    const cache = cacheFor(input.db) orelse return null;
    const selected = blk: {
        cache.mu.lock();
        defer cache.mu.unlock();
        for (&cache.entries) |*entry| {
            if (entry.busy) continue;
            const ctx = entry.ctx orelse continue;
            const declaration = ctx.declaration orelse continue;
            if (declaration.hash == hash) break :blk .{ .depth = declaration.depth, .anchor_hash = entry.hash };
        }
        return null;
    };
    var anchor = top;
    for (0..selected.depth) |_| anchor = region_spine_upstream(anchor) orelse return null;
    if (anchor.* != .materialize) return null;
    verifyKeyContract(anchor, keys, 0) catch return null;
    // Failed outer candidates can drain whole join inputs. The unchanged
    // declaration identifies the subtree BEFORE those drains rewrite shared
    // IR. Its current anchor hash need not match the post-drain cache hash;
    // tryCachedAt uses the stored hash to identify the program, then checks
    // kernel/table versions and rebuilds scans against fresh snapshots.
    const query = tryCachedAt(input, anchor, keys, selected.anchor_hash, null) orelse return null;
    return .{ .anchor = anchor, .query = query };
}

fn tryCachedAt(input: engine_v2.CompileInput, anchor: *const ir.Op, keys: []const []const u8, anchor_hash: ?u64, declaration: ?DeclaredBoundary) ?exec.Query {
    const hash = anchor_hash orelse return null;
    const cache = cacheFor(input.db) orelse return null;

    const entry = cache.checkout(hash) orelse return null;
    const ctx = entry.ctx.?;
    if (ctx.keys.len != keys.len or !contains_keys(ctx.keys, keys)) {
        CacheEntry.releaseErased(entry);
        return null;
    }
    if (!cacheValid(input, ctx)) {
        cache.discard(entry);
        if (getenv("THINDB_REGION_TRACE") != null) {
            std.debug.print("[region] cache invalidated (data changed)\n", .{});
        }
        return null;
    }

    const q = runCached(input, anchor, ctx) catch |e| {
        CacheEntry.releaseErased(entry);
        if (e != error.OutOfMemory and getenv("THINDB_REGION_TRACE") != null) {
            std.debug.print("[region] cache hit declined: {s}\n", .{@errorName(e)});
        }
        return null;
    };
    const op = exec.queryAs(region.RegionExecOp, q) orelse {
        var qq = q;
        qq.deinit();
        CacheEntry.releaseErased(entry);
        return null;
    };
    if (declaration) |d| ctx.declaration = d;
    op.setOwnedCtx(entry, CacheEntry.releaseErased);
    if (getenv("THINDB_REGION_TRACE") != null) {
        std.debug.print("[region] cache hit — pooled run (retained ~{d}MB)\n", .{ctx.pool.retainedBytes() >> 20});
    }
    return q;
}

/// The hit path: fresh pipeline collection (validates shape, provides the
/// IR-backed scan recipe + entry computes), fresh snapshot scans, then the
/// cached Program + pool. The entry schema is compared against the cached
/// program's — any DDL drift on the scan table declines to a full build.
fn runCached(input: engine_v2.CompileInput, anchor: *const ir.Op, ctx: *Ctx) !exec.Query {
    const qa = input.allocator;
    var pl = try collectPipeline(input.node_arena, anchor, ctx.keys);

    var prune_leaves: std.ArrayListUnmanaged(predicate_mod.Predicate) = .empty;
    defer prune_leaves.deinit(qa);
    try collectAndLeaves(qa, pl.entry_filter, &prune_leaves);

    const table = switch (pl.entry) {
        .scan => |scan| input.db.openTable(scan.table.name, .{}) catch return NoMatch,
        .staged => null,
    };
    // Same entry transformation as the build path — the cached program's
    // entry schema was derived post-hoist.
    if (table) |t| try hoistEntryComputes(input.node_arena, t, &pl);

    const scan_cols_opt = try entry_scan_columns(input.node_arena, pl);
    const n_threads = @max(input.effectiveDop(), 1);
    const bs = switch (pl.entry) {
        .staged => |root| try build_staged_source(input, root, ctx.keys),
        .scan => if (ctx.opts.ordered)
            try buildOrderedSources(input, table.?, prune_leaves.items, pl.entry_filter, scan_cols_opt, ctx.opts.n_threads, lastSegment(anchor.materialize.region_keys.?[0]))
        else
            try buildScanSources(input, table.?, prune_leaves.items, pl.entry_filter, scan_cols_opt, n_threads),
    };
    // Interval count is deterministic given identical data versions (the
    // hit precondition); a drift means the snapshot changed under us.
    if (ctx.opts.ordered and bs.sources.len != ctx.opts.n_shards) {
        for (bs.sources) |*s| s.deinit();
        qa.free(bs.sources);
        return NoMatch;
    }
    var sources_owned = true;
    errdefer if (sources_owned) {
        for (bs.sources) |*s| s.deinit();
        qa.free(bs.sources);
    };

    const scan_schema = bs.sources[0].outputSchema();
    const entry_derived = try rename_entry_outputs(input.node_arena, scan_schema, pl.entry_derived);
    const want = ctx.entry_schema;
    if (scan_schema.len + pl.entry_derived.len != want.len) {
        if (getenv("THINDB_REGION_TRACE") != null) {
            std.debug.print("[region] cached entry drift: widths {d}+{d} vs {d}\n", .{ scan_schema.len, pl.entry_derived.len, want.len });
        }
        return NoMatch;
    }
    for (scan_schema, want[0..scan_schema.len]) |src, w| {
        if (!std.ascii.eqlIgnoreCase(src.name, w.name)) return NoMatch;
        if (!std.meta.eql(src.type, w.type)) return NoMatch;
        if (!w.nullable) return NoMatch; // entry cols are always forced nullable
    }
    for (entry_derived, want[scan_schema.len..]) |d, w| {
        if (!std.ascii.eqlIgnoreCase(d.name, w.name)) return NoMatch;
    }

    var opts = ctx.opts;
    opts.n_threads = n_threads;
    opts.iv_rows_est = bs.iv_rows;

    // Side tables: fresh snapshot scans from the cached recipes, with the
    // same schema-drift guard the entry gets.
    const op_sides = try qa.alloc(region.SideInput, ctx.side_specs.items.len);
    var sides_built: usize = 0;
    var sides_owned = true;
    errdefer if (sides_owned) {
        for (op_sides[0..sides_built]) |*side| {
            for (side.sources) |*sq| sq.deinit();
            qa.free(side.sources);
        }
        qa.free(op_sides);
    };
    for (ctx.side_specs.items, op_sides) |*spec, *s| {
        const st = input.db.openTable(spec.table, .{}) catch return NoMatch;
        const sbs = try buildScanSources(input, st, spec.prune, spec.filter, spec.scan_cols, n_threads);
        s.* = .{
            .scan_schema = spec.scan_schema,
            .pre_schema = spec.pre_schema,
            .schema = spec.schema,
            .sources = sbs.sources,
            .entry_derived = spec.entry_derived,
            .key_col = spec.key_col,
            .agg = spec.agg,
        };
        sides_built += 1;
        const ss = sbs.sources[0].outputSchema();
        if (ss.len != spec.scan_schema.len) return NoMatch;
        for (ss, spec.scan_schema) |src, w| {
            if (!std.ascii.eqlIgnoreCase(src.name, w.name)) return NoMatch;
            if (!std.meta.eql(src.type, w.type)) return NoMatch;
        }
    }

    const q = try region.RegionExecOp.create(
        qa,
        ctx.entry_schema,
        bs.sources,
        entry_derived,
        op_sides,
        &ctx.prog,
        opts,
        &ctx.pool,
        bs.total_rows *| 2,
    );
    sources_owned = false;
    sides_owned = false;
    return q;
}

// ---- anchor-subtree fingerprint -------------------------------------------
// Deterministic deep hash of the post-pass IR the recognizer consumes:
// structure, names, literals, folded constants, expressions. Equal hashes ⇒
// the recognizer builds the identical program (given unchanged consumed
// data, which the cache validates separately). Variants outside the SELECT
// shapes a spine can contain return null — that query is never cached.

fn hashAnchor(anchor: *const ir.Op) ?u64 {
    var h = std.hash.Wyhash.init(0x726567696f6e);
    hashOp(&h, anchor) catch return null;
    return h.final();
}

fn hash_declaration(input: engine_v2.CompileInput, top: *const ir.Op) ?u64 {
    var h = std.hash.Wyhash.init(0x6465636c617265);
    hashOp(&h, top) catch return null;
    // An outer candidate may resolve a data-dependent expression in shared
    // IR before declining. Cover its source tables even when the selected
    // inner program no longer references them after that resolution.
    hash_declaration_sources(input, &h, top) catch return null;
    return h.final();
}

fn hash_declaration_sources(input: engine_v2.CompileInput, h: *std.hash.Wyhash, node: *const ir.Op) error{RegionUnhashable}!void {
    switch (node.*) {
        .scan => |s| {
            if (s.table.database) |db| if (!std.ascii.eqlIgnoreCase(db, input.db.name)) return Unhashable;
            if (s.table.schema) |schema| if (!std.ascii.eqlIgnoreCase(schema, "public")) return Unhashable;
            hu(h, tableVersionOf(input, s.table.name) orelse return Unhashable);
        },
        .materialize => |m| try hash_declaration_sources(input, h, m.upstream),
        .alias => |a| try hash_declaration_sources(input, h, a.upstream),
        .select, .exclude => |p| try hash_declaration_sources(input, h, p.upstream),
        .filter => |f| try hash_declaration_sources(input, h, f.upstream),
        .group_by => |g| try hash_declaration_sources(input, h, g.upstream),
        .compute => |c| try hash_declaration_sources(input, h, c.upstream),
        .limit => |l| try hash_declaration_sources(input, h, l.upstream),
        .order_by => |o| try hash_declaration_sources(input, h, o.upstream),
        .window => |w| try hash_declaration_sources(input, h, w.upstream),
        .join => |j| {
            try hash_declaration_sources(input, h, j.left);
            try hash_declaration_sources(input, h, j.right);
        },
        .set_union => |u| {
            try hash_declaration_sources(input, h, u.left);
            try hash_declaration_sources(input, h, u.right);
        },
        .table_fn => |t| {
            const registry = input.udf_registry orelse return Unhashable;
            const entry = registry.tableByName(t.name) orelse return Unhashable;
            h.update(std.mem.asBytes(&entry.process));
            for (t.inputs) |source| try hash_declaration_sources(input, h, source);
        },
        .single_row => {},
        else => return Unhashable,
    }
}

fn hu(h: *std.hash.Wyhash, v: u64) void {
    h.update(std.mem.asBytes(&v));
}

fn hstr(h: *std.hash.Wyhash, s: []const u8) void {
    hu(h, s.len);
    h.update(s);
}

fn hostr(h: *std.hash.Wyhash, s: ?[]const u8) void {
    if (s) |x| {
        hu(h, 1);
        hstr(h, x);
    } else hu(h, 0);
}

fn hashOp(h: *std.hash.Wyhash, op: *const ir.Op) error{RegionUnhashable}!void {
    hu(h, @intFromEnum(std.meta.activeTag(op.*)));
    switch (op.*) {
        .scan => |s| {
            hostr(h, s.table.database);
            hostr(h, s.table.schema);
            hstr(h, s.table.name);
            hostr(h, s.alias);
        },
        .limit => |l| {
            hu(h, l.n);
            hu(h, l.offset);
            try hashOp(h, l.upstream);
        },
        .select, .exclude => |p| {
            hu(h, p.columns.len);
            for (p.columns) |c| hstr(h, c);
            if (p.outputs) |outs| {
                hu(h, outs.len + 1);
                for (outs) |o| hostr(h, o);
            } else hu(h, 0);
            if (p.replace_on_collision) |rs| {
                hu(h, rs.len + 1);
                for (rs) |r| hu(h, @intFromBool(r));
            } else hu(h, 0);
            hu(h, p.star_skip_trailing);
            try hashOp(h, p.upstream);
        },
        .filter => |f| {
            try hashPred(h, f.predicate);
            try hashOp(h, f.upstream);
        },
        .order_by => |o| {
            hashSorts(h, o.specs);
            try hashOp(h, o.upstream);
        },
        .group_by => |g| {
            // top_k / emit_limit are post-decode planner hints; a group-by
            // carrying them is above a Limit and outside region shapes.
            if (g.top_k != null or g.emit_limit != null) return Unhashable;
            hu(h, g.group_cols.len);
            for (g.group_cols) |c| hstr(h, c);
            hu(h, g.aggs.len);
            for (g.aggs) |spec| {
                if (spec.out_type_override != null) return Unhashable;
                hu(h, @intFromEnum(spec.func));
                hostr(h, spec.udf_name);
                hu(h, spec.udf_arg_cols.len);
                for (spec.udf_arg_cols) |c| hstr(h, c);
                hostr(h, spec.col);
                hostr(h, spec.arg2_col);
                hstr(h, spec.as);
                switch (spec.params) {
                    .none => hu(h, 0),
                    .percentile => |p| {
                        hu(h, 1);
                        hu(h, @bitCast(p));
                    },
                    .separator => |s| {
                        hu(h, 2);
                        hstr(h, s);
                    },
                }
            }
            try hashOp(h, g.upstream);
        },
        .compute => |c| {
            hu(h, c.derived.len);
            for (c.derived) |d| {
                hstr(h, d.name);
                try hashExpr(h, d.expr);
            }
            try hashOp(h, c.upstream);
        },
        .join => |j| {
            hu(h, @intFromEnum(j.algorithm));
            hu(h, @intFromEnum(j.join_type));
            hu(h, j.on.len);
            for (j.on) |p| {
                hstr(h, p.left);
                hstr(h, p.right);
            }
            hu(h, j.ranges.len);
            for (j.ranges) |r| {
                hstr(h, r.left);
                hu(h, @intFromEnum(r.op));
                hstr(h, r.right);
            }
            if (j.extra_predicate) |p| {
                hu(h, 1);
                try hashPred(h, p);
            } else hu(h, 0);
            try hashOp(h, j.left);
            try hashOp(h, j.right);
        },
        .materialize => |m| {
            hu(h, @intFromBool(m.forced));
            if (m.region_keys) |keys| {
                hu(h, keys.len + 1);
                for (keys) |k| hstr(h, k);
            } else hu(h, 0);
            try hashOp(h, m.upstream);
        },
        .window => |w| {
            hu(h, w.specs.len);
            for (w.specs) |spec| {
                hu(h, spec.partition_by.len);
                for (spec.partition_by) |c| hstr(h, c);
                hashSorts(h, spec.order_by);
                hu(h, @intFromEnum(spec.frame.kind));
                hashBound(h, spec.frame.start);
                hashBound(h, spec.frame.end);
            }
            hu(h, w.calls.len);
            for (w.calls) |c| {
                hu(h, c.spec_idx);
                hu(h, @intFromEnum(c.func));
                hu(h, c.args.len);
                for (c.args) |e| try hashExpr(h, e);
                hu(h, @intFromBool(c.ignore_nulls));
                hstr(h, c.output_name);
            }
            try hashOp(h, w.upstream);
        },
        .set_union => |u| {
            hu(h, @intFromBool(u.all));
            try hashOp(h, u.left);
            try hashOp(h, u.right);
        },
        .alias => |a| {
            hstr(h, a.alias);
            try hashOp(h, a.upstream);
        },
        .table_fn => |t| {
            hstr(h, t.name);
            hu(h, t.args.len);
            for (t.args) |arg| {
                if (arg) |v| {
                    hu(h, 1);
                    hashValue(h, v);
                } else hu(h, 0);
            }
            hu(h, t.partition_by.len);
            for (t.partition_by) |c| hstr(h, c);
            hashSorts(h, t.order_by);
            hostr(h, t.alias);
            hu(h, t.inputs.len);
            for (t.inputs) |inp| try hashOp(h, inp);
        },
        .single_row => {},
        else => return Unhashable,
    }
}

fn hashSorts(h: *std.hash.Wyhash, specs: []const ir.SortSpec) void {
    hu(h, specs.len);
    for (specs) |s| {
        hstr(h, s.col);
        hu(h, @intFromBool(s.desc));
    }
}

fn hashBound(h: *std.hash.Wyhash, b: ir.FrameBound) void {
    hu(h, @intFromEnum(std.meta.activeTag(b)));
    switch (b) {
        .preceding, .following => |n| hu(h, n),
        else => {},
    }
}

fn hashValue(h: *std.hash.Wyhash, v: Value) void {
    hu(h, @intFromEnum(std.meta.activeTag(v)));
    switch (v) {
        .tinyint => |x| hu(h, @bitCast(@as(i64, x))),
        .smallint => |x| hu(h, @bitCast(@as(i64, x))),
        .int => |x| hu(h, @bitCast(@as(i64, x))),
        .date => |x| hu(h, @bitCast(@as(i64, x))),
        .bigint => |x| hu(h, @bitCast(x)),
        .datetime => |x| hu(h, @bitCast(x)),
        .decimal64 => |x| hu(h, @bitCast(x)),
        .boolean => |x| hu(h, @intFromBool(x)),
        .float => |x| hu(h, @as(u32, @bitCast(x))),
        .double => |x| hu(h, @bitCast(x)),
        .text => |s| hstr(h, s),
        .largeint => |x| h.update(std.mem.asBytes(&x)),
        .decimal128 => |x| h.update(std.mem.asBytes(&x)),
        .uuid => |x| h.update(std.mem.asBytes(&x)),
    }
}

fn hashType(h: *std.hash.Wyhash, t: types.Type) void {
    std.hash.autoHash(h, t);
}

fn hashExpr(h: *std.hash.Wyhash, e: Expr) error{RegionUnhashable}!void {
    hu(h, @intFromEnum(std.meta.activeTag(e)));
    switch (e) {
        .col_ref => |n| hstr(h, n),
        .lit => |v| hashValue(h, v),
        .null_lit => |t| hashType(h, t),
        .call => |c| {
            hstr(h, c.fn_name);
            hu(h, c.args.len);
            for (c.args) |a| try hashExpr(h, a);
        },
        .case => |c| {
            hu(h, c.branches.len);
            for (c.branches) |br| {
                try hashPred(h, br.cond);
                try hashExpr(h, br.then);
            }
            if (c.else_branch) |eb| {
                hu(h, 1);
                try hashExpr(h, eb.*);
            } else hu(h, 0);
        },
        .scalar_subquery, .exists_subquery, .var_ref => return Unhashable,
    }
}

fn hashPred(h: *std.hash.Wyhash, p: PredicateExpr) error{RegionUnhashable}!void {
    hu(h, @intFromEnum(std.meta.activeTag(p)));
    switch (p) {
        .leaf, .day_leaf => |l| {
            hstr(h, l.col);
            hu(h, @intFromEnum(l.op));
            hashValue(h, l.val);
        },
        .leaf_col_col => |l| {
            hstr(h, l.left);
            hu(h, @intFromEnum(l.op));
            hstr(h, l.right);
        },
        .is_null, .is_not_null => |c| hstr(h, c),
        .like => |l| {
            hstr(h, l.col);
            hstr(h, l.pattern);
        },
        .@"and", .@"or" => |kids| {
            hu(h, kids.len);
            for (kids) |k| try hashPred(h, k);
        },
        .not => |k| try hashPred(h, k.*),
        .always => |b| hu(h, @intFromBool(b)),
        .unknown => {},
        .in_set => |s| {
            hstr(h, s.col);
            hu(h, @intFromBool(s.negate));
            hu(h, s.values.len);
            for (s.values) |v| hashValue(h, v);
        },
        else => return Unhashable,
    }
}

// ---------------------------------------------------------------------------
// Frame + visible-name map (types.findColumn rules over recognizer state).
// ---------------------------------------------------------------------------

const VisEntry = struct { name: []const u8, idx: usize };

const FrameB = struct {
    a: Allocator, // ctx arena
    cols: std.ArrayListUnmanaged(Column) = .empty,
    vis: std.ArrayListUnmanaged(VisEntry) = .empty,
    next_id: usize = 0,

    fn canonName(fb: *FrameB, hint: []const u8) ![]const u8 {
        const buf = try std.fmt.allocPrint(fb.a, "__rg{d}_{s}", .{ fb.next_id, hint });
        fb.next_id += 1;
        for (buf) |*ch| {
            if (ch.* == '.') ch.* = '_';
        }
        return buf;
    }

    /// Append a frame column under a canonical unique name; returns its idx.
    fn addCol(fb: *FrameB, hint: []const u8, t: types.Type, nullable: bool) !usize {
        const idx = fb.cols.items.len;
        try fb.cols.append(fb.a, .{ .name = try fb.canonName(hint), .type = t, .nullable = nullable });
        return idx;
    }

    /// Append a frame column keeping a REAL (entry) name — caller must
    /// guarantee uniqueness across the whole frame.
    fn addColNamed(fb: *FrameB, name: []const u8, t: types.Type, nullable: bool) !usize {
        const idx = fb.cols.items.len;
        try fb.cols.append(fb.a, .{ .name = try fb.a.dupe(u8, name), .type = t, .nullable = nullable });
        return idx;
    }

    fn setVis(fb: *FrameB, name: []const u8, idx: usize) !void {
        for (fb.vis.items) |*e| {
            if (std.ascii.eqlIgnoreCase(e.name, name)) {
                e.idx = idx;
                return;
            }
        }
        try fb.vis.append(fb.a, .{ .name = try fb.a.dupe(u8, name), .idx = idx });
    }

    fn removeVis(fb: *FrameB, name: []const u8) void {
        var i: usize = 0;
        while (i < fb.vis.items.len) {
            if (visRefMatches(fb.vis.items[i].name, name)) {
                _ = fb.vis.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    /// types.findColumn semantics over the visible map.
    fn resolve(fb: *const FrameB, name: []const u8) ?VisEntry {
        for (fb.vis.items) |e| {
            if (std.ascii.eqlIgnoreCase(e.name, name)) return e;
        }
        if (std.mem.lastIndexOfScalar(u8, name, '.')) |dot| {
            const tail = name[dot + 1 ..];
            for (fb.vis.items) |e| {
                if (std.ascii.eqlIgnoreCase(e.name, tail)) return e;
            }
            return null;
        }
        var match: ?VisEntry = null;
        for (fb.vis.items) |e| {
            const d = std.mem.lastIndexOfScalar(u8, e.name, '.') orelse continue;
            if (std.ascii.eqlIgnoreCase(e.name[d + 1 ..], name)) {
                if (match != null) return null; // ambiguous
                match = e;
            }
        }
        return match;
    }
};

fn visRefMatches(vis_name: []const u8, ref: []const u8) bool {
    if (std.ascii.eqlIgnoreCase(vis_name, ref)) return true;
    if (std.mem.lastIndexOfScalar(u8, ref, '.')) |dot| {
        return std.ascii.eqlIgnoreCase(vis_name, ref[dot + 1 ..]);
    }
    return false;
}

// ---------------------------------------------------------------------------
// Recognizer context: owns everything the compiled program borrows. Attached
// to the RegionExecOp and freed at query teardown.
// ---------------------------------------------------------------------------

const NullSide = struct {
    alias: []const u8,
    schema: []const Column, // arena copy of the compiled right-side schema
};

const PinnedCol = struct {
    name: []const u8,
    val: Value,
};

const Ctx = struct {
    gpa: Allocator,
    arena: std.heap.ArenaAllocator,
    prog: region.Program = undefined,
    prog_built: bool = false,
    /// Buffer pool for the region run. When the ctx is cached, the pool —
    /// and every capacity it grew — survives to the next query.
    pool: region.RegionPool,
    /// Cache-entry state (all arena-owned): the program's entry schema for
    /// drift checks, the driver opts to replay, the data-versions of every
    /// table a compile-time drain consumed, and the kernel identities.
    entry_schema: []const Column = &.{},
    opts: region.DriverOpts = undefined,
    declaration: ?DeclaredBoundary = null,
    keys: []const []const u8 = &.{},
    /// Ordered mode: measured per-interval cost, filled by the first run
    /// and frozen — LPT weights for every later hit (arena-owned).
    iv_cost: []i64 = &.{},
    /// Co-partitioned side tables: everything needed to rebuild a side's
    /// scan sources per run (all arena-owned — the sources themselves are
    /// query-lifetime and never cached).
    side_specs: std.ArrayListUnmanaged(SideSpec) = .empty,
    versions: std.ArrayListUnmanaged(TableVersion) = .empty,
    kernels: std.ArrayListUnmanaged(KernelCheck) = .empty,
    uncacheable: bool = false,

    fn retained_bytes(self: *const Ctx) usize {
        return self.arena.queryCapacity() +| self.pool.retainedBytes();
    }

    fn destroyErased(p: *anyopaque) void {
        const self: *Ctx = @ptrCast(@alignCast(p));
        const gpa = self.gpa;
        self.pool.deinit();
        if (self.prog_built) self.prog.deinit();
        var arena = self.arena;
        gpa.destroy(self);
        arena.deinit();
    }
};

/// One co-partitioned side table (ctx-arena-owned rebuild recipe).
const SideSpec = struct {
    table: []const u8,
    prune: []const predicate_mod.Predicate,
    filter: PredicateExpr,
    scan_cols: ?[]const []const u8,
    entry_derived: []const Derived,
    scan_schema: []const Column,
    /// Exchange (pre-agg) schema: scan output ++ entry-derived columns.
    pre_schema: []const Column,
    /// Probe-visible schema (post-agg when `agg` is set; == pre otherwise).
    schema: []const Column,
    /// Route key column in the pre-agg schema.
    key_col: usize,
    /// Per-bin GROUP BY spec (crossplans CMT collapse), indices into
    /// `pre_schema`.
    agg: ?region.SideAgg,
};

const Builder = struct {
    input: engine_v2.CompileInput,
    ctx: *Ctx,
    a: Allocator, // ctx arena allocator
    fb: FrameB,
    ops: std.ArrayListUnmanaged(region.RegionOp) = .empty,
    /// Canonical frame name of the route key (declared_keys[0]) — the side
    /// of a co-partitioned join must bind to it through an ON pair.
    route_name: []const u8 = &.{},
    order_aligned: bool = false,
    /// Side-table dedupe: the IR node each side spec was compiled from
    /// (ctc/ctl reference the SAME materialized CTE — one side, two probes).
    side_nodes: std.ArrayListUnmanaged(*const ir.Op) = .empty,
    /// Semi-joins converted to scan-fused membership filters (applied
    /// pre-exchange by the driver; no program op).
    member_filters: std.ArrayListUnmanaged(region.MemberFilter) = .empty,
    /// Per-side scan sources (query-lifetime; the RegionExecOp takes them).
    side_sources: std.ArrayListUnmanaged([]exec.Query) = .empty,
    /// Consolidation range-key NAMES, resolved against the CURRENT frame at
    /// every partition check — frame-replacing ops (group_agg, replace-TVF)
    /// keep the names by contract, so name resolution survives epochs that
    /// indices would not.
    range_key_names: []const []const u8 = &.{},
    /// Scan-filter eq-literal columns — per-run constants tracked by NAME
    /// (matched on the last segment, ci) so the fact survives frame
    /// replacement. Span merges and probe-pair elimination use them.
    pinned: std.ArrayListUnmanaged(PinnedCol) = .empty,
    /// LEFT-join sides proven all-NULL; refs against them append typed NULL
    /// columns on demand.
    null_sides: std.ArrayListUnmanaged(NullSide) = .empty,
    pending_nulls: std.ArrayListUnmanaged(Derived) = .empty,
    /// Frame columns known constant (folded literal computes): groupings
    /// skip them as subkeys — a constant can't split groups.
    const_idxs: std.ArrayListUnmanaged(usize) = .empty,

    fn resolveIdx(b: *Builder, name: []const u8) !usize {
        if (b.fb.resolve(name)) |e| return e.idx;
        return b.tryNullAppend(name);
    }

    /// A ref that doesn't resolve may target one of the proven-empty join
    /// sides: append a typed NULL frame column for it.
    fn tryNullAppend(b: *Builder, name: []const u8) !usize {
        errdefer if (getenv("THINDB_REGION_TRACE") != null) {
            std.debug.print("[region] unresolved ref '{s}' (vis: ", .{name});
            for (b.fb.vis.items) |e| std.debug.print("{s} ", .{e.name});
            std.debug.print(")\n", .{});
        };
        const dot = std.mem.indexOfScalar(u8, name, '.') orelse return NoMatch;
        const prefix = name[0..dot];
        for (b.null_sides.items) |ns| {
            if (!std.ascii.eqlIgnoreCase(ns.alias, prefix)) continue;
            const ci = types.findColumn(ns.schema, name) orelse return NoMatch;
            const col = ns.schema[ci];
            const idx = try b.fb.addCol(name, col.type, true);
            try b.pending_nulls.append(b.a, .{
                .name = b.fb.cols.items[idx].name,
                .expr = .{ .null_lit = col.type },
            });
            try b.fb.setVis(try visKeyFor(b.a, ns.alias, col.name), idx);
            return idx;
        }
        return NoMatch;
    }

    fn flushPending(b: *Builder) !void {
        if (b.pending_nulls.items.len == 0) return;
        const derived = try b.a.dupe(Derived, b.pending_nulls.items);
        b.pending_nulls.clearRetainingCapacity();
        try b.ops.append(b.a, .{ .compute = .{ .derived = derived } });
    }

    /// Range-key column indices resolved against the CURRENT frame.
    fn rangeKeyIdxs(b: *Builder, out: *[8]usize) ![]const usize {
        if (b.range_key_names.len > 8) return NoMatch;
        for (b.range_key_names, 0..) |n, i| {
            out[i] = (b.fb.resolve(n) orelse return NoMatch).idx;
        }
        return out[0..b.range_key_names.len];
    }

    /// Set-equality of a partition column list against the range keys.
    fn partitionMatchesRangeKeys(b: *Builder, part: []const []const u8) !bool {
        if (part.len != b.range_key_names.len or part.len > 8) return false;
        var buf: [8]usize = undefined;
        const keys = try b.rangeKeyIdxs(&buf);
        var seen = [_]bool{false} ** 8;
        for (part) |p| {
            const idx = (b.fb.resolve(p) orelse return NoMatch).idx;
            var found = false;
            for (keys, 0..) |k, i| {
                if (k == idx and !seen[i]) {
                    seen[i] = true;
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        for (seen[0..part.len]) |s| {
            if (!s) return false;
        }
        return true;
    }

    const PartClass = union(enum) {
        range_exact,
        /// Partition strictly coarser than the range keys: adjacent ranges
        /// equal on this (single non-constant) column merge into one span.
        merged_span: usize,
    };

    fn classifyPartition(b: *Builder, part: []const []const u8) !PartClass {
        if (try b.partitionMatchesRangeKeys(part)) return .range_exact;
        var buf: [8]usize = undefined;
        const keys = try b.rangeKeyIdxs(&buf);
        var merge: ?usize = null;
        for (part) |p| {
            const idx = (b.fb.resolve(p) orelse return NoMatch).idx;
            if (std.mem.indexOfScalar(usize, keys, idx) == null) return NoMatch;
            if (b.pinnedName(p) == null and !b.isConstIdx(idx)) {
                if (merge != null) return NoMatch; // one merge column (runtime limit)
                merge = idx;
            }
        }
        return .{ .merged_span = merge orelse return NoMatch };
    }

    fn pinnedName(b: *Builder, name: []const u8) ?Value {
        const tail = lastSegment(name);
        for (b.pinned.items) |p| {
            if (std.ascii.eqlIgnoreCase(lastSegment(p.name), tail)) return p.val;
        }
        return null;
    }

    fn isConstIdx(b: *Builder, idx: usize) bool {
        return std.mem.indexOfScalar(usize, b.const_idxs.items, idx) != null;
    }

    // ---- expression cloning (col_ref rewrite to canonical frame names) ----

    fn cloneValue(b: *Builder, v: Value) !Value {
        return switch (v) {
            .text => |s| .{ .text = try b.a.dupe(u8, s) },
            else => v,
        };
    }

    fn cloneExpr(b: *Builder, e: Expr) anyerror!Expr {
        return switch (e) {
            .col_ref => |name| blk: {
                const idx = try b.resolveIdx(name);
                break :blk .{ .col_ref = b.fb.cols.items[idx].name };
            },
            .lit => |v| .{ .lit = try b.cloneValue(v) },
            .null_lit => |t| .{ .null_lit = t },
            .call => |c| blk: {
                const args = try b.a.alloc(Expr, c.args.len);
                for (c.args, args) |src, *dst| dst.* = try b.cloneExpr(src);
                break :blk .{ .call = .{ .fn_name = try b.a.dupe(u8, c.fn_name), .args = args } };
            },
            .case => |c| blk: {
                const branches = try b.a.alloc(Expr.Branch, c.branches.len);
                for (c.branches, branches) |src, *dst| {
                    dst.* = .{ .cond = try b.clonePred(src.cond), .then = try b.cloneExpr(src.then) };
                }
                var else_branch: ?*const Expr = null;
                if (c.else_branch) |eb| {
                    const p = try b.a.create(Expr);
                    p.* = try b.cloneExpr(eb.*);
                    else_branch = p;
                }
                break :blk .{ .case = .{ .branches = branches, .else_branch = else_branch } };
            },
            else => NoMatch,
        };
    }

    fn clonePred(b: *Builder, p: PredicateExpr) anyerror!PredicateExpr {
        return switch (p) {
            .leaf => |l| .{ .leaf = try b.cloneLeaf(l) },
            .day_leaf => |l| .{ .day_leaf = try b.cloneLeaf(l) },
            .leaf_col_col => |cc| blk: {
                const li = try b.resolveIdx(cc.left);
                const ri = try b.resolveIdx(cc.right);
                break :blk .{ .leaf_col_col = .{
                    .left = b.fb.cols.items[li].name,
                    .op = cc.op,
                    .right = b.fb.cols.items[ri].name,
                } };
            },
            .is_null => |name| blk: {
                const idx = try b.resolveIdx(name);
                break :blk .{ .is_null = b.fb.cols.items[idx].name };
            },
            .is_not_null => |name| blk: {
                const idx = try b.resolveIdx(name);
                break :blk .{ .is_not_null = b.fb.cols.items[idx].name };
            },
            .like => |l| blk: {
                const idx = try b.resolveIdx(l.col);
                break :blk .{ .like = .{
                    .col = b.fb.cols.items[idx].name,
                    .pattern = try b.a.dupe(u8, l.pattern),
                } };
            },
            .@"and" => |kids| blk: {
                const out = try b.a.alloc(PredicateExpr, kids.len);
                for (kids, out) |src, *dst| dst.* = try b.clonePred(src);
                break :blk .{ .@"and" = out };
            },
            .@"or" => |kids| blk: {
                const out = try b.a.alloc(PredicateExpr, kids.len);
                for (kids, out) |src, *dst| dst.* = try b.clonePred(src);
                break :blk .{ .@"or" = out };
            },
            .not => |child| blk: {
                const out = try b.a.create(PredicateExpr);
                out.* = try b.clonePred(child.*);
                break :blk .{ .not = out };
            },
            .always => |v| .{ .always = v },
            .in_set => |s| blk: {
                const idx = try b.resolveIdx(s.col);
                const vals = try b.a.alloc(Value, s.values.len);
                for (s.values, vals) |src, *dst| dst.* = try b.cloneValue(src);
                break :blk .{ .in_set = .{
                    .col = b.fb.cols.items[idx].name,
                    .values = vals,
                    .negate = s.negate,
                } };
            },
            else => NoMatch,
        };
    }

    fn cloneLeaf(b: *Builder, l: predicate_mod.Predicate) !predicate_mod.Predicate {
        const idx = try b.resolveIdx(l.col);
        return .{ .col = b.fb.cols.items[idx].name, .op = l.op, .val = try b.cloneValue(l.val) };
    }

    // ---- structural op appenders -----------------------------------------

    /// Clone `derived` against the current frame and append one compute op;
    /// each output becomes a fresh canonical frame column shadowing its
    /// visible name. Output TYPES come from a throwaway engine Compute over
    /// the frame schema — the same resolution the runtime instances use, so
    /// later type-driven decisions (sum int vs float, int-family checks)
    /// can never disagree with execution.
    fn pushCompute(b: *Builder, derived: []const Derived) !void {
        // Clone everything first (may append pending null columns) and let
        // the engine type the FULL list once — folded constants then carry
        // exactly the type the engine evaluator would have produced.
        const cloned = try b.a.alloc(Derived, derived.len);
        const replaced = try b.a.alloc(?usize, derived.len);
        for (derived, cloned, replaced) |src, *dst, *prior| {
            prior.* = if (b.fb.resolve(src.name)) |entry| entry.idx else null;
            const expr = try b.cloneExpr(src.expr);
            dst.* = .{ .name = try b.fb.canonName(src.name), .expr = expr };
        }
        try b.flushPending();
        const base = b.fb.cols.items.len;
        const typed = region.computeOutputSchema(
            b.input.allocator,
            b.a,
            b.fb.cols.items,
            cloned,
            b.input.udf_registry,
        ) catch return NoMatch;
        if (typed.len != base + derived.len) return NoMatch;

        // Constant deriveds (literals, typed literal casts, NULL literals)
        // fold into a bulk-fill const_cols op — per-row evaluation for e.g.
        // sixteen zero columns is pure waste — and their frame columns are
        // remembered so groupings can skip them as subkeys.
        var const_cols: std.ArrayListUnmanaged(Column) = .empty;
        var const_vals: std.ArrayListUnmanaged(?Value) = .empty;
        var rest: std.ArrayListUnmanaged(Derived) = .empty;
        var rest_src: std.ArrayListUnmanaged(usize) = .empty;
        for (derived, cloned, 0..) |src, cl, i| {
            const t = typed[base + i].type;
            if (foldConst(cl.expr, t)) |fv| {
                const idx = b.fb.cols.items.len;
                try b.fb.cols.append(b.a, .{ .name = cl.name, .type = t, .nullable = true });
                try const_cols.append(b.a, b.fb.cols.items[idx]);
                try const_vals.append(b.a, fv);
                try b.bind_compute_output(src.name, idx, replaced[i]);
                try b.const_idxs.append(b.a, idx);
            } else {
                try rest.append(b.a, cl);
                try rest_src.append(b.a, i);
            }
        }
        if (const_cols.items.len > 0) {
            try b.ops.append(b.a, .{ .const_cols = .{
                .cols = const_cols.items,
                .values = const_vals.items,
            } });
        }
        if (rest.items.len == 0) return;
        try b.ops.append(b.a, .{ .compute = .{ .derived = rest.items } });
        for (rest.items, rest_src.items) |cl, i| {
            const idx = b.fb.cols.items.len;
            var col = typed[base + i];
            col.name = cl.name;
            try b.fb.cols.append(b.a, col);
            try b.bind_compute_output(derived[i].name, idx, replaced[i]);
        }
    }

    fn bind_compute_output(b: *Builder, name: []const u8, idx: usize, replaced: ?usize) !void {
        // Compute replaces one logical output slot after binding every RHS.
        // Keeping its old qualified alias would let a later Project read the
        // original value even though ordinary SQL now exposes the replacement.
        var bound = false;
        var i: usize = 0;
        while (i < b.fb.vis.items.len) {
            const entry = b.fb.vis.items[i];
            if (replaced != null and entry.idx == replaced.? and std.ascii.eqlIgnoreCase(lastSegment(entry.name), lastSegment(name))) {
                if (bound) {
                    _ = b.fb.vis.orderedRemove(i);
                    continue;
                }
                b.fb.vis.items[i] = .{ .name = name, .idx = idx };
                bound = true;
            }
            i += 1;
        }
        if (!bound) try b.fb.setVis(name, idx);
        i = 0;
        while (i < b.pinned.items.len) {
            if (std.ascii.eqlIgnoreCase(lastSegment(b.pinned.items[i].name), lastSegment(name))) {
                _ = b.pinned.swapRemove(i);
            } else i += 1;
        }
    }

    /// Constant expressions the fill op can represent, coerced to the type
    /// the engine evaluator derived for the derived column: NULL literals,
    /// int-family literals, and single-arg typed casts of them. Returns the
    /// fill value (?? = the outer optional is "not foldable"; the inner is
    /// the NULL fill). Anything else stays with the engine evaluator.
    fn foldConst(e: ir.Expr, t: types.Type) ??Value {
        const lit: ?Value = switch (e) {
            .null_lit => null,
            .lit => |v| v,
            .call => |c| blk: {
                if (c.args.len != 1) return null;
                const known = std.ascii.eqlIgnoreCase(c.fn_name, "to_bigint") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "to_int") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "to_smallint") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "to_double");
                if (!known) return null;
                break :blk switch (c.args[0]) {
                    .null_lit => null,
                    .lit => |v| v,
                    else => return null,
                };
            },
            else => return null,
        };
        const v = lit orelse return @as(??Value, @as(?Value, null));
        const iv = valueI64(v) orelse return null;
        return switch (t) {
            .bigint => @as(?Value, .{ .bigint = iv }),
            .int => @as(?Value, .{ .int = std.math.cast(i32, iv) orelse return null }),
            .smallint => @as(?Value, .{ .smallint = std.math.cast(i16, iv) orelse return null }),
            .tinyint => @as(?Value, .{ .tinyint = std.math.cast(i8, iv) orelse return null }),
            .double => @as(?Value, .{ .double = @floatFromInt(iv) }),
            else => null,
        };
    }

    fn applySelect(b: *Builder, p: *const ir.Op.Project) !void {
        for (p.columns) |col| {
            if (std.mem.eql(u8, col, "*") or std.mem.endsWith(u8, col, ".*")) continue;
            if (b.fb.resolve(col) == null) _ = try b.tryNullAppend(col);
        }
        const schema = try b.a.alloc(Column, b.fb.vis.items.len);
        for (b.fb.vis.items, schema) |visible, *col| {
            col.* = b.fb.cols.items[visible.idx];
            col.name = visible.name;
        }
        const names = try @import("local.zig").resolve_select_project(b.a, schema, p.*);
        defer names.deinit(b.a);
        var new_vis: std.ArrayListUnmanaged(VisEntry) = .empty;
        for (names.sources, names.outputs) |col, output| {
            const e = b.fb.resolve(col) orelse return NoMatch;
            for (new_vis.items) |prior| {
                if (types.columnNameEql(prior.name, output)) return NoMatch;
            }
            try new_vis.append(b.a, .{ .name = try b.a.dupe(u8, output), .idx = e.idx });
        }
        try b.flushPending();
        b.fb.vis = new_vis;
    }

    fn applyAlias(b: *Builder, alias: []const u8) !void {
        // Requalify on the LAST name segment (idempotent): the SQL's refs
        // address alias.col regardless of how many qualifier layers the
        // upstream names accumulated, and any resulting ambiguity declines
        // through the resolver rather than misbinding.
        for (b.fb.vis.items) |*e| {
            e.name = try std.fmt.allocPrint(b.a, "{s}.{s}", .{ alias, lastSegment(e.name) });
        }
    }
};

fn visKeyFor(a: Allocator, alias: []const u8, col_name: []const u8) ![]const u8 {
    // The compiled right side already qualifies names ("ctc.amount"); keep
    // them verbatim, otherwise qualify with the alias.
    if (std.mem.indexOfScalar(u8, col_name, '.') != null) return a.dupe(u8, col_name);
    return std.fmt.allocPrint(a, "{s}.{s}", .{ alias, col_name });
}

// ---------------------------------------------------------------------------
// Compile-time subtree execution (broadcasts + emptiness probes).
// ---------------------------------------------------------------------------

const DrainedBlock = struct {
    schema: []const Column, // arena copy
    rows: usize,
    /// Arena stores per schema column (empty when drain=false).
    stores: []ColumnStore,
};

fn compileAndDrain(b: *Builder, node: *const ir.Op, drain: bool) !DrainedBlock {
    recordSubtreeVersions(b, node);
    // Side branches can share CTEs and window results. The ordinary stage
    // compiler must retain those boundaries even during region preparation.
    var q = cte_stages.compile_region_input(b.input, node) catch |err| {
        if (err == error.OutOfMemory) return err;
        return NoMatch;
    };
    defer q.deinit();

    const src_schema = q.outputSchema();
    const schema = try b.a.alloc(Column, src_schema.len);
    for (src_schema, schema) |src, *dst| {
        dst.* = src;
        dst.name = try b.a.dupe(u8, src.name);
    }

    var stores: []ColumnStore = &.{};
    var rows: usize = 0;
    if (drain) {
        stores = try b.a.alloc(ColumnStore, schema.len);
        for (stores, schema) |*s, col| s.* = try ColumnStore.init(b.a, col.type, true);
        while (q.next() catch return NoMatch) |batch| {
            for (stores, 0..) |*s, ci| {
                try appendViewAll(b.a, s, batch.values[ci], batch.row_count);
            }
            rows += batch.row_count;
        }
    }
    return .{ .schema = schema, .rows = rows, .stores = stores };
}

fn appendViewAll(a: Allocator, store: *ColumnStore, v: ColumnView, n: usize) !void {
    try region.appendViewRange(a, store, v, 0, n);
}

fn i64At(v: ColumnView, i: usize) ?i64 {
    if (!v.isValid(i)) return null;
    return switch (v.data) {
        .tinyint => |s| s[i],
        .smallint => |s| s[i],
        .int => |s| s[i],
        .bigint => |s| s[i],
        .date => |s| s[i],
        .datetime => |s| s[i],
        else => null,
    };
}

/// One row of a drained store as a const-column Value; null = SQL NULL.
/// Errors (never nulls) on types const_cols can't carry, so a real value
/// can't silently degrade to NULL.
fn valueAtRow(v: ColumnView, i: usize) !?Value {
    if (!v.isValid(i)) return null;
    return switch (v.data) {
        .tinyint => |s| .{ .tinyint = s[i] },
        .smallint => |s| .{ .smallint = s[i] },
        .int => |s| .{ .int = s[i] },
        .bigint => |s| .{ .bigint = s[i] },
        .date => |s| .{ .date = s[i] },
        .datetime => |s| .{ .datetime = s[i] },
        .float => |s| .{ .float = s[i] },
        .double => |s| .{ .double = s[i] },
        .largeint => |s| .{ .largeint = s[i] },
        .decimal64 => |s| .{ .decimal64 = s[i] },
        .decimal128 => |s| .{ .decimal128 = s[i] },
        .varchar, .string, .char, .json => |s| .{ .text = s.rowBytes(i) },
        else => NoMatch,
    };
}

// ---- plain deep clones (no frame rewriting) --------------------------------
// Side-table expressions resolve against the SIDE scan — a FLAT single-table
// namespace — so every col_ref normalizes to its last segment (the IR
// qualifies refs with CTE/join aliases the hand-built side has no alias
// nodes to resolve). Cloned into the ctx arena for the cached per-run
// rebuild.

fn cloneValuePlain(a: Allocator, v: Value) !Value {
    return switch (v) {
        .text => |s| .{ .text = try a.dupe(u8, s) },
        else => v,
    };
}

fn cloneExprPlain(a: Allocator, e: Expr) anyerror!Expr {
    return switch (e) {
        .col_ref => |name| .{ .col_ref = try a.dupe(u8, lastSegment(name)) },
        .lit => |v| .{ .lit = try cloneValuePlain(a, v) },
        .null_lit => |t| .{ .null_lit = t },
        .call => |c| blk: {
            const args = try a.alloc(Expr, c.args.len);
            for (c.args, args) |src, *dst| dst.* = try cloneExprPlain(a, src);
            break :blk .{ .call = .{ .fn_name = try a.dupe(u8, c.fn_name), .args = args } };
        },
        .case => |c| blk: {
            const branches = try a.alloc(Expr.Branch, c.branches.len);
            for (c.branches, branches) |src, *dst| {
                dst.* = .{ .cond = try clonePredPlain(a, src.cond), .then = try cloneExprPlain(a, src.then) };
            }
            var else_branch: ?*const Expr = null;
            if (c.else_branch) |eb| {
                const p = try a.create(Expr);
                p.* = try cloneExprPlain(a, eb.*);
                else_branch = p;
            }
            break :blk .{ .case = .{ .branches = branches, .else_branch = else_branch } };
        },
        else => NoMatch,
    };
}

fn clonePredPlain(a: Allocator, p: PredicateExpr) anyerror!PredicateExpr {
    return switch (p) {
        .leaf => |l| .{ .leaf = try cloneLeafPlain(a, l) },
        .day_leaf => |l| .{ .day_leaf = try cloneLeafPlain(a, l) },
        .leaf_col_col => |cc| .{ .leaf_col_col = .{
            .left = try a.dupe(u8, lastSegment(cc.left)),
            .op = cc.op,
            .right = try a.dupe(u8, lastSegment(cc.right)),
        } },
        .is_null => |name| .{ .is_null = try a.dupe(u8, lastSegment(name)) },
        .is_not_null => |name| .{ .is_not_null = try a.dupe(u8, lastSegment(name)) },
        .like => |l| .{ .like = .{
            .col = try a.dupe(u8, lastSegment(l.col)),
            .pattern = try a.dupe(u8, l.pattern),
        } },
        .@"and" => |kids| blk: {
            const out = try a.alloc(PredicateExpr, kids.len);
            for (kids, out) |src, *dst| dst.* = try clonePredPlain(a, src);
            break :blk .{ .@"and" = out };
        },
        .@"or" => |kids| blk: {
            const out = try a.alloc(PredicateExpr, kids.len);
            for (kids, out) |src, *dst| dst.* = try clonePredPlain(a, src);
            break :blk .{ .@"or" = out };
        },
        .not => |child| blk: {
            const out = try a.create(PredicateExpr);
            out.* = try clonePredPlain(a, child.*);
            break :blk .{ .not = out };
        },
        .always => |v| .{ .always = v },
        .in_set => |s| blk: {
            const vals = try a.alloc(Value, s.values.len);
            for (s.values, vals) |src, *dst| dst.* = try cloneValuePlain(a, src);
            break :blk .{ .in_set = .{
                .col = try a.dupe(u8, lastSegment(s.col)),
                .values = vals,
                .negate = s.negate,
            } };
        },
        else => NoMatch,
    };
}

fn cloneLeafPlain(a: Allocator, l: predicate_mod.Predicate) !predicate_mod.Predicate {
    return .{ .col = try a.dupe(u8, lastSegment(l.col)), .op = l.op, .val = try cloneValuePlain(a, l.val) };
}

/// Inline earlier derived outputs into a later expr: the side runs ONE
/// compute over the scan schema, so a col_ref to a sibling derived (the IR
/// had them in separate compute nodes) must become that sibling's
/// expression. Shared subtrees are fine — expressions are read-only.
fn substDerivedRefs(a: Allocator, e: Expr, earlier: []const Derived) anyerror!Expr {
    return switch (e) {
        .col_ref => |name| blk: {
            for (earlier) |d| {
                if (std.ascii.eqlIgnoreCase(d.name, name)) break :blk d.expr;
            }
            break :blk e;
        },
        .call => |c| blk: {
            const args = try a.alloc(Expr, c.args.len);
            for (c.args, args) |src, *dst| dst.* = try substDerivedRefs(a, src, earlier);
            break :blk .{ .call = .{ .fn_name = c.fn_name, .args = args } };
        },
        .case => |c| blk: {
            const branches = try a.alloc(Expr.Branch, c.branches.len);
            for (c.branches, branches) |src, *dst| {
                dst.* = .{ .cond = src.cond, .then = try substDerivedRefs(a, src.then, earlier) };
            }
            var else_branch = c.else_branch;
            if (c.else_branch) |eb| {
                const p = try a.create(Expr);
                p.* = try substDerivedRefs(a, eb.*, earlier);
                else_branch = p;
            }
            break :blk .{ .case = .{ .branches = branches, .else_branch = else_branch } };
        },
        else => e,
    };
}

fn predAlwaysFalse(p: PredicateExpr) bool {
    return switch (p) {
        .always => |v| !v,
        .@"and" => |kids| blk: {
            for (kids) |k| {
                if (predAlwaysFalse(k)) break :blk true;
            }
            break :blk false;
        },
        else => false,
    };
}
// ---------------------------------------------------------------------------
// Pipeline collection: the block's IR walked ONCE into an ordered step list.
// No fixed shape — any interleaving of the supported constructs compiles;
// the builder dispatches the steps bottom-up over the frame.
// ---------------------------------------------------------------------------

const UnionTvf = struct {
    tvf: *const ir.Op.TableFn,
    /// Filter between the TVF's input and the shared base (the kernel's
    /// input rows are the base rows passing it).
    input_filter: ?PredicateExpr,
};

const Step = union(enum) {
    select: *const ir.Op.Project,
    exclude: *const ir.Op.Project,
    compute: []const Derived,
    alias_name: []const u8,
    filt: PredicateExpr,
    group_by: *const ir.Op.GroupBy,
    window: *const ir.WindowOp,
    table_fn: *const ir.Op.TableFn,
    join: *const ir.Op.Join,
    /// `base UNION ALL TVF(SELECT .. FROM base [WHERE f])` over the SAME
    /// base node: the TVF appends rows at each consolidation group's tail
    /// (fused), and its PARTITION BY / ORDER BY define the region's
    /// range/order contract. Must be the bottom-most structural step.
    union_tvf: UnionTvf,
};

const Pipeline = struct {
    /// Top-down (steps[0] nearest the anchor); dispatched in reverse.
    steps: []const Step,
    entry_sel: ?*const ir.Op.Project,
    /// Entry computes in evaluation (bottom-up) order — run per batch
    /// during the scatter, before the exchange.
    entry_derived: []const Derived,
    entry_filter: PredicateExpr,
    entry: union(enum) {
        scan: *const ir.Op.Scan,
        staged: *const ir.Op,
    },
};

fn staged_pipeline(steps: []const Step, structural_end: usize, entry_root: *const ir.Op) !Pipeline {
    if (structural_end == 0) return NoMatch;
    return .{
        .steps = steps[0..structural_end],
        .entry_sel = null,
        .entry_derived = &.{},
        .entry_filter = .{ .@"and" = &.{} },
        .entry = .{ .staged = entry_root },
    };
}

fn collectPipeline(a: Allocator, anchor: *const ir.Op, keys: []const []const u8) !Pipeline {
    if (anchor.* != .materialize) return NoMatch;
    var steps: std.ArrayListUnmanaged(Step) = .empty;
    var cur: *const ir.Op = anchor.materialize.upstream;
    var entry_root = cur;
    var structural_end: usize = 0;
    var guard: usize = 0;
    const scan: *const ir.Op.Scan = blk: while (guard < 512) : (guard += 1) {
        switch (cur.*) {
            .materialize => |m| cur = m.upstream,
            .select => |*p| {
                try steps.append(a, .{ .select = p });
                cur = p.upstream;
            },
            .exclude => |*p| {
                try steps.append(a, .{ .exclude = p });
                cur = p.upstream;
            },
            .compute => |c| {
                try steps.append(a, .{ .compute = c.derived });
                cur = c.upstream;
            },
            .alias => |al| {
                try steps.append(a, .{ .alias_name = al.alias });
                cur = al.upstream;
            },
            .filter => |f| {
                try steps.append(a, .{ .filt = f.predicate });
                cur = f.upstream;
            },
            .group_by => |*g| {
                if (!contains_keys(keys, g.group_cols)) return staged_pipeline(steps.items, structural_end, entry_root);
                try steps.append(a, .{ .group_by = g });
                cur = g.upstream;
                entry_root = cur;
                structural_end = steps.items.len;
            },
            .window => |*w| {
                for (w.specs) |spec| {
                    if (!contains_keys(keys, spec.partition_by)) return staged_pipeline(steps.items, structural_end, entry_root);
                }
                try steps.append(a, .{ .window = w });
                cur = w.upstream;
                entry_root = cur;
                structural_end = steps.items.len;
            },
            .join => |*j| {
                try steps.append(a, .{ .join = j });
                cur = j.left;
                entry_root = cur;
                structural_end = steps.items.len;
            },
            .table_fn => |*t| {
                if (t.inputs.len == 0) return NoMatch;
                if (t.partition_by.len > 0 and !contains_keys(keys, t.partition_by)) return staged_pipeline(steps.items, structural_end, entry_root);
                try steps.append(a, .{ .table_fn = t });
                cur = t.inputs[0];
                entry_root = cur;
                structural_end = steps.items.len;
            },
            .set_union => |*u| {
                const arm = unionTvfArm(u) orelse {
                    if (!u.all or structural_end == 0) return NoMatch;
                    // Keep the entry's projections and filters in their SQL
                    // order, including positional aliases and union casts.
                    return staged_pipeline(steps.items, structural_end, entry_root);
                };
                try steps.append(a, .{ .union_tvf = arm.ut });
                cur = arm.base;
                entry_root = cur;
                structural_end = steps.items.len;
            },
            .scan => |*s| break :blk s,
            .order_by, .limit => return staged_pipeline(steps.items, structural_end, entry_root),
            else => return NoMatch,
        }
    } else return NoMatch;

    // Split off the entry cluster: the trailing run of select/compute/filter
    // steps below the last structural step becomes the scan projection, the
    // scatter-time computes, and the fused scan filter.
    var split = steps.items.len;
    while (split > 0) : (split -= 1) {
        switch (steps.items[split - 1]) {
            .select, .compute, .filt => {},
            else => break,
        }
    }
    var first_projection: ?usize = null;
    var entry_index = steps.items.len;
    while (entry_index > split) : (entry_index -= 1) {
        if (steps.items[entry_index - 1] != .select) continue;
        if (first_projection) |first| {
            // Later projections may rename inputs used by intervening
            // computes or filters. Keep their evaluation order in-region.
            split = first;
            break;
        }
        first_projection = entry_index - 1;
    }
    var entry_sel: ?*const ir.Op.Project = null;
    var entry_derived: std.ArrayListUnmanaged(Derived) = .empty;
    var filters: std.ArrayListUnmanaged(PredicateExpr) = .empty;
    var i = steps.items.len;
    while (i > split) : (i -= 1) {
        switch (steps.items[i - 1]) {
            .select => |p| {
                if (entry_sel != null) return NoMatch; // one entry projection
                entry_sel = p;
            },
            .compute => |d| try entry_derived.appendSlice(a, d),
            .filt => |p| try filters.append(a, p),
            else => unreachable,
        }
    }
    const entry_filter: PredicateExpr = if (filters.items.len == 1)
        filters.items[0]
    else
        .{ .@"and" = filters.items };

    return .{
        .steps = steps.items[0..split],
        .entry_sel = entry_sel,
        .entry_derived = entry_derived.items,
        .entry_filter = entry_filter,
        .entry = .{ .scan = scan },
    };
}

fn unionTvfArm(u: *const ir.SetUnion) ?struct { base: *const ir.Op, ut: UnionTvf } {
    if (!u.all) return null;
    if (matchTvfArm(u.right, u.left)) |ut| return .{ .base = u.left, .ut = ut };
    if (matchTvfArm(u.left, u.right)) |ut| return .{ .base = u.right, .ut = ut };
    return null;
}

fn matchTvfArm(arm: *const ir.Op, base: *const ir.Op) ?UnionTvf {
    var cur = arm;
    var guard: usize = 0;
    const tvf: *const ir.Op.TableFn = blk: while (guard < 8) : (guard += 1) {
        switch (cur.*) {
            .materialize => |m| cur = m.upstream,
            .select => |p| cur = p.upstream,
            .table_fn => |*t| break :blk t,
            else => return null,
        }
    } else return null;
    if (tvf.inputs.len == 0) return null;
    cur = tvf.inputs[0];
    var input_filter: ?PredicateExpr = null;
    guard = 0;
    while (guard < 8) : (guard += 1) {
        if (cur == base) return .{ .tvf = tvf, .input_filter = input_filter };
        switch (cur.*) {
            .materialize => |m| cur = m.upstream,
            .select => |p| cur = p.upstream,
            .filter => |f| {
                if (input_filter != null) return null;
                input_filter = f.predicate;
                cur = f.upstream;
            },
            else => return null,
        }
    }
    return null;
}

// ---------------------------------------------------------------------------
// The general region builder: entry + range contract derivation, then each
// pipeline step dispatched bottom-up. Kernel handling is driven by SDK
// metadata (execution mode, passthrough, broadcast inputs) — never by name.
// ---------------------------------------------------------------------------

/// Hoist scatter-safe computes past bottom-most joins into the entry: a
/// per-row compute whose inputs are all BASE TABLE columns commutes with
/// any join below it (a probe only adds payload columns and filters rows),
/// so it can run at scan time. Without this the plan-selection temp-table
/// join strands LOWER(customerNumber) above itself and the declared route
/// key never reaches the entry frame. Applied identically on the cached
/// path — the entry schema must be reproducible.
fn hoistEntryComputes(na: Allocator, table: anytype, pl: *Pipeline) !void {
    var kept: std.ArrayListUnmanaged(Step) = .empty;
    var hoisted: std.ArrayListUnmanaged(Derived) = .empty;
    var stop = false;
    var j = pl.steps.len;
    while (j > 0) : (j -= 1) {
        const st = pl.steps[j - 1];
        if (!stop) switch (st) {
            .join => {},
            .compute => |d| blk: {
                var refs: std.ArrayListUnmanaged([]const u8) = .empty;
                for (d) |dv| try exprColNames(na, dv.expr, &refs);
                for (refs.items) |n| {
                    if (table.schema.columnIndex(lastSegment(n)) == null) {
                        stop = true;
                        break :blk;
                    }
                }
                try hoisted.appendSlice(na, d);
                continue;
            },
            else => stop = true,
        };
        try kept.append(na, st);
    }
    if (hoisted.items.len > 0) {
        std.mem.reverse(Step, kept.items);
        var merged: std.ArrayListUnmanaged(Derived) = .empty;
        try merged.appendSlice(na, pl.entry_derived);
        try merged.appendSlice(na, hoisted.items);
        pl.steps = kept.items;
        pl.entry_derived = merged.items;
    }
}

fn entry_scan_columns(arena: Allocator, pl: Pipeline) !?[]const []const u8 {
    const sel = pl.entry_sel orelse return null;
    for (sel.columns) |col| {
        if (std.mem.eql(u8, col, "*") or std.mem.endsWith(u8, col, ".*")) return null;
    }
    var columns: std.ArrayListUnmanaged([]const u8) = .empty;
    outer: for (sel.columns) |col| {
        for (pl.entry_derived) |d| {
            if (types.columnNameEql(d.name, col)) continue :outer;
        }
        try columns.append(arena, col);
    }
    // A replacement still reads its original input, even when SELECT only
    // exposes the replacement. Scatter computes need those hidden inputs.
    for (pl.entry_derived) |d| try compute_mod.collectColumnRefs(arena, &columns, d.expr);
    return columns.items;
}

fn rename_entry_outputs(arena: Allocator, scan_schema: []const Column, derived: []const Derived) ![]const Derived {
    if (derived.len == 0) return derived;
    const renamed = try arena.dupe(Derived, derived);
    errdefer arena.free(renamed);
    var next_id: usize = 0;
    for (derived, renamed, 0..) |d, *dst, i| {
        for (derived[0..i]) |prior| {
            if (types.columnNameEql(prior.name, d.name)) return NoMatch;
        }
        if (types.findColumn(scan_schema, d.name) == null) continue;
        // Compute replaces matching output slots; the region frame must
        // retain the original slots and append every scatter-time output.
        while (true) {
            const name = try std.fmt.allocPrint(arena, "__region_entry_{d}", .{next_id});
            next_id += 1;
            const used = blk: {
                if (types.findColumn(scan_schema, name) != null) break :blk true;
                for (derived) |other| {
                    if (types.columnNameEql(other.name, name)) break :blk true;
                }
                break :blk false;
            };
            if (used) {
                arena.free(name);
                continue;
            }
            dst.name = name;
            break;
        }
    }
    return renamed;
}

fn buildRegion(input: engine_v2.CompileInput, anchor: *const ir.Op, declared_keys: []const []const u8, anchor_hash: ?u64, declaration: ?DeclaredBoundary) anyerror!exec.Query {
    var tm: i64 = exec.prof.nowTicks();
    const registry = input.udf_registry orelse return NoMatch;

    // A cacheable build uses the DATABASE allocator for its ctx so the
    // entry can outlive this query/connection; otherwise query-lifetime.
    if (anchor_hash == null and getenv("THINDB_REGION_TRACE") != null) {
        std.debug.print("[region] anchor unhashable — never cached\n", .{});
    }
    const cache: ?*Cache = if (anchor_hash != null) cacheFor(input.db) else null;
    const gpa = if (cache != null) input.db.allocator else input.allocator;
    const qa = input.allocator;
    const ctx = try gpa.create(Ctx);
    ctx.* = .{
        .gpa = gpa,
        .arena = std.heap.ArenaAllocator.init(gpa),
        .pool = region.RegionPool.init(gpa, poolCapBytes()),
        .declaration = declaration,
    };
    errdefer Ctx.destroyErased(ctx);
    const a = ctx.arena.allocator();
    if (cache == null) ctx.uncacheable = true;
    const keys = try a.alloc([]const u8, declared_keys.len);
    for (declared_keys, keys) |key, *copy| copy.* = try a.dupe(u8, key);
    ctx.keys = keys;

    var b = Builder{ .input = input, .ctx = ctx, .a = a, .fb = .{ .a = a } };
    var sides_owned = true;
    errdefer if (sides_owned) {
        for (b.side_sources.items) |srcs| {
            for (srcs) |*sq| sq.deinit();
            qa.free(srcs);
        }
    };

    // Query-lifetime arena: the step list and the entry-derived slice are
    // borrowed by the operator (never by the cached ctx).
    var pl = try collectPipeline(input.node_arena, anchor, declared_keys);
    traceMark("walk", &tm);

    // ---- entry: prune leaves + literal-pinned columns --------------------
    var prune_leaves: std.ArrayListUnmanaged(predicate_mod.Predicate) = .empty;
    try collectAndLeaves(a, pl.entry_filter, &prune_leaves);

    const table = switch (pl.entry) {
        .scan => |scan| input.db.openTable(scan.table.name, .{}) catch return NoMatch,
        .staged => null,
    };

    if (table) |t| try hoistEntryComputes(input.node_arena, t, &pl);

    const scan_cols_opt = try entry_scan_columns(input.node_arena, pl);

    const dop = input.effectiveDop();
    const n_threads = @max(dop, 1);
    // Route partitions, not execution units: the driver LPT-packs nonzero
    // partitions into ~2×threads execution bins, so a finer fan-out isolates
    // whale keys without multiplying program runs (the 192-executed-shards
    // experiment that regressed).
    const n_shards: usize = blk: {
        if (getenv("THINDB_REGION_SHARDS")) |v| {
            break :blk std.fmt.parseInt(usize, std.mem.span(v), 10) catch 256;
        }
        break :blk 256;
    };

    // Order-aligned fast path (#186): the declared keys form a contiguous
    // run of the table's physical order_key immediately after the leading
    // eq-pinned columns, and the route key is a stored column — the table
    // order replaces the hash exchange (key-interval scans arrive
    // pre-grouped; consolidation is a small run merge).
    const order_aligned = blk: {
        // Opt-in while the exec phase trails the hash exchange (~2.0s vs
        // 1.24s on the sorted bench table): correct and value-verified, but
        // the single-sorted-run fast case still pays a gather it doesn't
        // need. Flip default once ops run directly over sorted interval
        // stores.
        if (getenv("THINDB_REGION_ORDERED") == null) break :blk false;
        const t = table orelse break :blk false;
        for (pl.entry_derived) |d| {
            if (std.ascii.eqlIgnoreCase(d.name, lastSegment(declared_keys[0]))) break :blk false;
        }
        const ok = t.schema.order_key;
        var oi: usize = 0;
        outer: while (oi < ok.len) : (oi += 1) {
            for (prune_leaves.items) |l| {
                if (l.op == .eq and std.ascii.eqlIgnoreCase(lastSegment(l.col), ok[oi])) continue :outer;
            }
            break;
        }
        if (ok.len - oi < declared_keys.len) break :blk false;
        for (declared_keys, ok[oi..][0..declared_keys.len]) |dk, okc| {
            if (!std.ascii.eqlIgnoreCase(lastSegment(dk), okc)) break :blk false;
        }
        break :blk true;
    };

    b.order_aligned = order_aligned;
    const bs = switch (pl.entry) {
        .staged => |root| blk: {
            recordSubtreeVersions(&b, root);
            break :blk try build_staged_source(input, root, declared_keys);
        },
        .scan => if (order_aligned)
            try buildOrderedSources(input, table.?, prune_leaves.items, pl.entry_filter, scan_cols_opt, n_threads, lastSegment(declared_keys[0]))
        else
            try buildScanSources(input, table.?, prune_leaves.items, pl.entry_filter, scan_cols_opt, n_threads),
    };
    const sources = bs.sources;
    const total_rows = bs.total_rows;
    const iv_rows_est = bs.iv_rows;
    if (order_aligned and getenv("THINDB_REGION_TRACE") != null) {
        std.debug.print("[region] order-aligned: {d} key intervals ride table order_key\n", .{sources.len});
    }
    var sources_owned = true; // RegionExecOp takes them over on create
    errdefer if (sources_owned) {
        for (sources) |*q| q.deinit();
        qa.free(sources);
    };
    traceMark("scan_build", &tm);

    // ---- entry schema = scan output ++ entry-computed columns ------------
    const scan_schema = sources[0].outputSchema();
    const entry_derived = try rename_entry_outputs(input.node_arena, scan_schema, pl.entry_derived);
    const entry_schema = try a.alloc(Column, scan_schema.len + pl.entry_derived.len);
    for (scan_schema, entry_schema[0..scan_schema.len]) |src, *dst| {
        dst.* = src;
        dst.name = try a.dupe(u8, src.name);
        // Every entry column is nullable: a union-append kernel NULL-pads
        // whatever it doesn't cover, and which columns those are depends on
        // the variant's projection (plans carries invoiceItemId the
        // estimates kernel never writes). The all-valid bulk append keeps
        // the bitmap cost negligible.
        dst.nullable = true;
    }
    const rowloc_entry: ?usize = if (pl.entry == .scan) scan_schema.len - 1 else null;
    if (pl.entry_derived.len > 0) {
        // Engine-exact types for the scatter-time computes; forced nullable
        // (kernel-appended rows NULL-pad every entry-derived column).
        const typed = region.computeOutputSchema(qa, a, scan_schema, entry_derived, registry) catch return NoMatch;
        if (typed.len != scan_schema.len + pl.entry_derived.len) return NoMatch;
        for (entry_derived, typed[scan_schema.len..], entry_schema[scan_schema.len..]) |d, t, *dst| {
            dst.* = .{ .name = try a.dupe(u8, d.name), .type = t.type, .nullable = true };
        }
    }
    for (entry_schema, 0..) |col, i| {
        _ = try b.fb.addColNamed(col.name, col.type, col.nullable);
        if (i == rowloc_entry) continue;
        const visible_name = if (i < scan_schema.len) col.name else pl.entry_derived[i - scan_schema.len].name;
        try b.fb.setVis(visible_name, i);
    }
    ctx.entry_schema = entry_schema;

    traceMark("entry_schema", &tm);
    // Literal-pinned entry columns: an eq-literal conjunct in the scan
    // filter makes the column a per-run constant — groupings and span
    // merges may skip it, and probe key pairs against it eliminate.
    pinned: for (prune_leaves.items) |l| {
        if (l.op != .eq) continue;
        if (b.fb.resolve(l.col) == null) continue;
        for (pl.entry_derived) |d| {
            if (types.columnNameEql(lastSegment(d.name), lastSegment(l.col))) continue :pinned;
        }
        try b.pinned.append(a, .{ .name = try a.dupe(u8, l.col), .val = l.val });
    }

    // ---- range/order contract from the first granularity-sensitive step --
    var range_names: []const []const u8 = declared_keys;
    var order_specs: []const ir.SortSpec = &.{};
    {
        var i = pl.steps.len;
        found: while (i > 0) : (i -= 1) {
            switch (pl.steps[i - 1]) {
                .union_tvf => |u| {
                    // Not necessarily bottom-most: steps below it (e.g. the
                    // plan-selection temp-table probe) become leading ops and
                    // the union-append TVF then runs UNFUSED mid-program
                    // (fusedFirstTail only fuses when it lands at op 0).
                    range_names = u.tvf.partition_by;
                    order_specs = u.tvf.order_by;
                    break :found;
                },
                .table_fn => |t| {
                    if (t.partition_by.len > 0) {
                        range_names = t.partition_by;
                        order_specs = t.order_by;
                        if (registry.tableByName(t.name)) |ent| {
                            // An unordered, row-aligned .either kernel can
                            // run over a shard even when a later operation
                            // requires finer ranges. Its call partition need
                            // not constrain the rest of the pipeline.
                            if (ent.execution == .either and ent.passthrough.len != 0 and t.order_by.len == 0)
                                continue;
                        }
                        break :found;
                    }
                },
                .group_by => |g| {
                    range_names = g.group_cols;
                    break :found;
                },
                .window => |w| {
                    if (w.specs.len > 0) {
                        range_names = w.specs[0].partition_by;
                        break :found;
                    }
                },
                else => {},
            }
        }
    }
    const range_keys = try a.alloc(usize, range_names.len);
    const key_names_owned = try a.alloc([]const u8, range_names.len);
    for (range_names, range_keys, key_names_owned) |n, *dst, *nm| {
        dst.* = (b.fb.resolve(n) orelse {
            if (getenv("THINDB_REGION_TRACE") != null) {
                std.debug.print("[region] range key '{s}' does not resolve in the entry frame (steps:", .{n});
                for (pl.steps) |s| std.debug.print(" {s}", .{@tagName(std.meta.activeTag(s))});
                std.debug.print(")\n", .{});
            }
            return NoMatch;
        }).idx;
        nm.* = try a.dupe(u8, n);
    }
    b.range_key_names = key_names_owned;
    // Routing: any single declared key suffices (every partition in the
    // block contains all of them — the verifier's guarantee), and it must
    // itself be a range key. Floats can't route: the exchange hashes raw
    // value bytes, and float bit patterns diverge from value equality
    // (-0.0 vs 0.0), so prefer any non-float key.
    const route_idx = blk: {
        for (declared_keys) |dk| {
            const idx = (b.fb.resolve(dk) orelse return NoMatch).idx;
            switch (entry_schema[idx].type) {
                .float, .double => continue,
                else => break :blk idx,
            }
        }
        if (getenv("THINDB_REGION_TRACE") != null) {
            std.debug.print("[region] no routable declared key (all float-typed)\n", .{});
        }
        return NoMatch;
    };
    if (std.mem.indexOfScalar(usize, range_keys, route_idx) == null) return NoMatch;
    b.route_name = b.fb.cols.items[route_idx].name;

    // Sort order within the consolidation: DECLARED keys first — every
    // coarser partition (span merge) retains exactly the declared keys, so
    // putting them first keeps its spans adjacent regardless of the SQL's
    // PARTITION BY column order — then the remaining range keys. Pinned
    // constants can't split the order and are dropped.
    var sort_list: std.ArrayListUnmanaged(region.OrderCol) = .empty;
    for (declared_keys) |n| {
        const idx = (b.fb.resolve(n) orelse return NoMatch).idx;
        if (std.mem.indexOfScalar(usize, range_keys, idx) == null) return NoMatch;
        if (b.pinnedName(n) != null) continue;
        try sort_list.append(a, .{ .col = idx, .kind = try orderKind(entry_schema[idx].type) });
    }
    for (range_names, range_keys) |n, k| {
        if (b.pinnedName(n) != null) continue; // constants can't split the order
        var is_declared = false;
        for (sort_list.items) |sc| {
            if (sc.col == k) {
                is_declared = true;
                break;
            }
        }
        if (is_declared) continue;
        try sort_list.append(a, .{ .col = k, .kind = try orderKind(entry_schema[k].type) });
    }
    const group_prefix = sort_list.items.len;
    for (order_specs) |ob| {
        if (ob.desc) return NoMatch;
        const idx = (b.fb.resolve(ob.col) orelse return NoMatch).idx;
        try sort_list.append(a, .{ .col = idx, .kind = try orderKind(entry_schema[idx].type) });
    }
    // Scan chunks need a locator to recover physical tie order. A union
    // enters through one stream; stable consolidation retains its tie order.
    if (rowloc_entry) |loc| try sort_list.append(a, .{ .col = loc, .kind = .int64 });
    const sort_cols = sort_list.items;
    if (getenv("THINDB_REGION_STEPS") != null) {
        std.debug.print("[region] range keys:", .{});
        for (range_names) |name| std.debug.print(" {s}", .{name});
        std.debug.print("; sort prefix {d}:", .{group_prefix});
        for (sort_cols) |col| std.debug.print(" {s}", .{entry_schema[col.col].name});
        std.debug.print("\n", .{});
    }

    traceMark("contract", &tm);
    if (pl.entry_sel) |sel| try b.applySelect(sel);

    // ---- dispatch the pipeline bottom-up ---------------------------------
    {
        var i = pl.steps.len;
        while (i > 0) : (i -= 1) {
            if (getenv("THINDB_REGION_STEPS") != null) {
                std.debug.print("[region] step {d} '{s}'", .{ i, @tagName(std.meta.activeTag(pl.steps[i - 1])) });
                switch (pl.steps[i - 1]) {
                    .exclude, .select => |p| for (p.columns) |c| std.debug.print(" {s}", .{c}),
                    else => {},
                }
                std.debug.print("\n", .{});
            }
            dispatchStep(&b, registry, pl.steps[i - 1], pl.steps[0 .. i - 1]) catch |e| {
                if (getenv("THINDB_REGION_TRACE") != null) {
                    std.debug.print("[region] dispatch declined on step {d}/{d} '{s}': {s}", .{
                        i, pl.steps.len, @tagName(std.meta.activeTag(pl.steps[i - 1])), @errorName(e),
                    });
                    switch (pl.steps[i - 1]) {
                        .join => |jj| {
                            const nm = rightAliasName(jj.right) catch null;
                            std.debug.print(" (right: {s}, type: {s}, extra_pred: {}, on: {d})", .{
                                nm orelse "?", @tagName(jj.join_type), jj.extra_predicate != null, jj.on.len,
                            });
                        },
                        else => {},
                    }
                    std.debug.print("\n", .{});
                }
                return e;
            };
        }
    }
    traceMark("dispatch", &tm);

    const emit_cols = try a.alloc(usize, b.fb.vis.items.len);
    const emit_names = try a.alloc([]const u8, b.fb.vis.items.len);
    for (b.fb.vis.items, emit_cols, emit_names) |e, *ec, *en| {
        ec.* = e.idx;
        en.* = e.name;
    }
    try b.flushPending();
    try b.ops.append(a, .{ .emit = .{ .cols = emit_cols } });

    // ---- compile the program ---------------------------------------------
    ctx.prog = region.Program.build(gpa, entry_schema, b.ops.items, registry) catch |pe| {
        if (getenv("THINDB_REGION_TRACE") != null) {
            std.debug.print("[region] program build failed: {s} (ops:", .{@errorName(pe)});
            for (b.ops.items) |op| std.debug.print(" {s}", .{@tagName(std.meta.activeTag(op))});
            std.debug.print(")\n", .{});
        }
        return NoMatch;
    };
    ctx.prog_built = true;
    traceMark("prog_build", &tm);
    // Emit-column NAMES for the stage schema: the program derives them from
    // the frame (canonical); patch to the SQL-visible names the query above
    // resolves against.
    if (ctx.prog.output_schema.len != emit_names.len) return NoMatch;
    const patched = try a.alloc(Column, ctx.prog.output_schema.len);
    for (ctx.prog.output_schema, emit_names, patched) |src, name, *dst| {
        dst.* = src;
        dst.name = name;
    }
    ctx.prog.output_schema = patched;

    if (order_aligned) {
        ctx.iv_cost = try a.alloc(i64, sources.len);
        @memset(ctx.iv_cost, 0);
    }
    const opts = region.DriverOpts{
        .n_threads = n_threads,
        .n_shards = if (order_aligned) sources.len else n_shards,
        .key_col = route_idx,
        .sort_cols = sort_cols,
        .group_prefix = group_prefix,
        .ordered = order_aligned,
        .loc_col = rowloc_entry orelse 0,
        .iv_rows_est = iv_rows_est,
        .iv_cost_slot = if (order_aligned) ctx.iv_cost else null,
        .member_filters = b.member_filters.items,
    };
    ctx.opts = opts;
    // The estimate slice is query-lifetime (scan-source scratch) — never
    // let the cached ctx carry it across runs; each run supplies its own.
    // (iv_cost_slot IS ctx-owned and deliberately persists.)
    ctx.opts.iv_rows_est = &.{};

    const op_sides = try qa.alloc(region.SideInput, ctx.side_specs.items.len);
    for (ctx.side_specs.items, b.side_sources.items, op_sides) |spec, srcs, *s| {
        s.* = .{
            .scan_schema = spec.scan_schema,
            .pre_schema = spec.pre_schema,
            .schema = spec.schema,
            .sources = srcs,
            .entry_derived = spec.entry_derived,
            .key_col = spec.key_col,
            .agg = spec.agg,
        };
    }
    var q = try region.RegionExecOp.create(
        qa,
        entry_schema,
        sources,
        entry_derived,
        op_sides,
        &ctx.prog,
        opts,
        &ctx.pool,
        total_rows *| 2,
    );
    sources_owned = false; // the query owns sources (and, below, the ctx)
    sides_owned = false;
    const op = exec.queryAs(region.RegionExecOp, q) orelse {
        q.deinit();
        return NoMatch;
    };

    // Publish into the per-database cache when possible: the cache then owns
    // the ctx (program + pool) and the op only releases the busy pin at
    // query teardown. Otherwise the op owns the ctx (one-shot).
    if (cache) |c| blk: {
        if (ctx.uncacheable) break :blk;
        const entry = c.publish(anchor_hash.?, ctx) orelse break :blk;
        op.setOwnedCtx(entry, CacheEntry.releaseErased);
        if (getenv("THINDB_REGION_TRACE") != null) {
            std.debug.print("[region] cache store ({x})\n", .{anchor_hash.?});
        }
        return q;
    }
    op.setOwnedCtx(ctx, Ctx.destroyErased);
    return q;
}

fn dispatchStep(b: *Builder, registry: *const udf_mod.UdfRegistry, step: Step, above: []const Step) anyerror!void {
    switch (step) {
        .select => |p| try b.applySelect(p),
        .exclude => |p| for (p.columns) |col| b.fb.removeVis(col),
        .compute => |d| try b.pushCompute(d),
        .alias_name => |nm| try b.applyAlias(nm),
        .filt => return NoMatch, // mid-stream filters have no region op yet
        .group_by => |g| try pushGroupAggAuto(b, g),
        .window => |w| try dispatchWindow(b, w),
        .union_tvf => |u| try dispatchUnionTvf(b, registry, u),
        .table_fn => |t| try dispatchTvf(b, registry, t),
        .join => |j| try dispatchJoin(b, j, above),
    }
}

/// Column names the remaining (not-yet-dispatched) steps can reference —
/// the probe payload liveness set. Null when the steps above contain a
/// construct whose references can't be enumerated (a kernel's declared
/// input names live in the registry, not the step) — then keep everything.
/// A wrongly-dropped payload can only fail resolution later (a compile
/// decline), never produce wrong values.
fn liveNamesAbove(a: Allocator, steps: []const Step) !?[]const []const u8 {
    var out: std.ArrayListUnmanaged([]const u8) = .empty;
    var any_select = false;
    for (steps) |s| {
        switch (s) {
            .select => |p| {
                any_select = true;
                for (p.columns) |c| try out.append(a, c);
            },
            .exclude => |p| for (p.columns) |c| try out.append(a, c),
            .compute => |d| for (d) |dd| try exprColNames(a, dd.expr, &out),
            .alias_name => {},
            .filt => |p| try predColNames(a, p, &out),
            .group_by => |g| {
                for (g.group_cols) |c| try out.append(a, c);
                for (g.aggs) |spec| {
                    if (spec.col) |c| try out.append(a, c);
                    if (spec.arg2_col) |c| try out.append(a, c);
                    for (spec.udf_arg_cols) |c| try out.append(a, c);
                }
            },
            .window => |w| {
                for (w.specs) |spec| {
                    for (spec.partition_by) |c| try out.append(a, c);
                    for (spec.order_by) |ob| try out.append(a, ob.col);
                }
                for (w.calls) |call| {
                    for (call.args) |e| try exprColNames(a, e, &out);
                }
            },
            .join => |j| for (j.on) |pair| try out.append(a, pair.left),
            .table_fn, .union_tvf => return null,
        }
    }
    if (!any_select) return null; // without a projection, anything may emit
    return out.items;
}

/// Liveness for the not-yet-dispatched steps: the enumerating walk first,
/// the projection-bounded walk when a TVF defeats it.
fn liveNames(a: Allocator, steps: []const Step) !?[]const []const u8 {
    if (try liveNamesAbove(a, steps)) |l| return l;
    return liveNamesBounded(a, steps);
}

/// Fallback liveness when `liveNamesAbove` gives up on a TVF: walk from
/// the step NEAREST the join upward and STOP at the first namespace-closing
/// step — an explicit SELECT projection, a GROUP BY, or a frame-replacing
/// table_fn. SQL scoping means everything higher references columns only
/// THROUGH that step, so the set collected up to and including it is
/// complete. TVF steps contribute their call-site references (partition/
/// order keys + input-subquery projections/filters); a union-append TVF
/// passes the frame through, so the walk continues past it. Same safety
/// contract as liveNamesAbove: a wrongly-dropped payload fails resolution
/// later (compile decline), never wrong values.
fn liveNamesBounded(a: Allocator, steps: []const Step) !?[]const []const u8 {
    var out: std.ArrayListUnmanaged([]const u8) = .empty;
    var i = steps.len;
    while (i > 0) {
        i -= 1;
        switch (steps[i]) {
            .select => |p| {
                for (p.columns) |c| try out.append(a, c);
                return out.items;
            },
            .group_by => |g| {
                for (g.group_cols) |c| try out.append(a, c);
                for (g.aggs) |spec| {
                    if (spec.col) |c| try out.append(a, c);
                    if (spec.arg2_col) |c| try out.append(a, c);
                    for (spec.udf_arg_cols) |c| try out.append(a, c);
                }
                return out.items;
            },
            .table_fn => |t| {
                try tvfRefNames(a, t, &out);
                return out.items;
            },
            .exclude => |p| for (p.columns) |c| try out.append(a, c),
            .compute => |d| for (d) |dd| try exprColNames(a, dd.expr, &out),
            .alias_name => {},
            .filt => |p| try predColNames(a, p, &out),
            .window => |w| {
                for (w.specs) |spec| {
                    for (spec.partition_by) |c| try out.append(a, c);
                    for (spec.order_by) |ob| try out.append(a, ob.col);
                }
                for (w.calls) |call| {
                    for (call.args) |e| try exprColNames(a, e, &out);
                }
            },
            .join => |j| for (j.on) |pair| try out.append(a, pair.left),
            .union_tvf => |u| {
                try tvfRefNames(a, u.tvf, &out);
                if (u.input_filter) |f| try predColNames(a, f, &out);
            },
        }
    }
    return null; // no closing step below the anchor — keep everything
}

/// References a TVF call site makes against the frame: partition/order
/// keys plus each input subquery's projection columns, filters, and
/// derived-column refs, walked toward the shared base. An unrecognized
/// node ends that input's walk — deeper refs are against other sources
/// (broadcast inputs) and cannot name frame columns.
fn tvfRefNames(a: Allocator, tf: *const ir.Op.TableFn, out: *std.ArrayListUnmanaged([]const u8)) anyerror!void {
    for (tf.partition_by) |c| try out.append(a, c);
    for (tf.order_by) |ob| try out.append(a, ob.col);
    for (tf.inputs) |sub| {
        var node: *const ir.Op = sub;
        walk: while (true) {
            switch (node.*) {
                .select, .exclude => |p| {
                    for (p.columns) |c| try out.append(a, c);
                    node = p.upstream;
                },
                .filter => |f| {
                    try predColNames(a, f.predicate, out);
                    node = f.upstream;
                },
                .compute => |c| {
                    for (c.derived) |d| try exprColNames(a, d.expr, out);
                    node = c.upstream;
                },
                else => break :walk,
            }
        }
    }
}

fn exprColNames(a: Allocator, e: Expr, out: *std.ArrayListUnmanaged([]const u8)) anyerror!void {
    switch (e) {
        .col_ref => |n| try out.append(a, n),
        .call => |c| for (c.args) |arg| try exprColNames(a, arg, out),
        .case => |c| {
            for (c.branches) |br| {
                try predColNames(a, br.cond, out);
                try exprColNames(a, br.then, out);
            }
            if (c.else_branch) |eb| try exprColNames(a, eb.*, out);
        },
        else => {},
    }
}

fn predColNames(a: Allocator, p: PredicateExpr, out: *std.ArrayListUnmanaged([]const u8)) anyerror!void {
    switch (p) {
        .leaf, .day_leaf => |l| try out.append(a, l.col),
        .leaf_col_col => |l| {
            try out.append(a, l.left);
            try out.append(a, l.right);
        },
        .is_null, .is_not_null => |c| try out.append(a, c),
        .like => |l| try out.append(a, l.col),
        .@"and", .@"or" => |kids| for (kids) |k| try predColNames(a, k, out),
        .not => |k| try predColNames(a, k.*, out),
        .in_set => |s| try out.append(a, s.col),
        else => {},
    }
}

/// The fused-tail union arm: the kernel appends rows at each consolidation
/// group's tail during the ONE gather. Requires a write-everything kernel.
fn dispatchUnionTvf(b: *Builder, registry: *const udf_mod.UdfRegistry, u: UnionTvf) anyerror!void {
    const ent = registry.tableByName(u.tvf.name) orelse return NoMatch;
    try recordKernel(b, ent);
    if (!kernelReadsAll(ent) or ent.passthrough.len != 0) return NoMatch;
    if (u.tvf.inputs.len != 1) return NoMatch;
    if (!try b.partitionMatchesRangeKeys(u.tvf.partition_by)) return NoMatch;
    const inputs = try tvfInputs(b, ent, ent.input_schemas[0].len);
    const out = try b.a.alloc(Column, inputs.len);
    for (out, inputs) |*o, ci| o.* = b.fb.cols.items[ci];
    const filt = u.input_filter orelse return NoMatch;
    const win = try dateWindow(filt, b);
    try b.ops.append(b.a, .{ .tvf_grouped = .{
        .spec = .{
            .process = ent.process,
            .user_data = ent.user_data,
            .args = try cloneArgs(b, u.tvf.args),
            .inputs = inputs,
            .out = out,
        },
        .union_append = true,
        .input_filter = .{ .col = win.col, .lo = win.lo, .hi = win.hi },
    } });
}

/// Mid-stream TVF. Granularity is a kernel CONTRACT, decided by metadata:
/// partition == range keys → one call per range (`.partitioned` honored);
/// partition coarser → whole-shard call, legal only when the kernel
/// declares `.either` (correct at any granularity). Secondary inputs are
/// compile-time-drained broadcasts.
fn dispatchTvf(b: *Builder, registry: *const udf_mod.UdfRegistry, t: *const ir.Op.TableFn) anyerror!void {
    const ent = registry.tableByName(t.name) orelse return NoMatch;
    try recordKernel(b, ent);

    var extra_parts: []udf_mod.TvfPartition = &.{};
    if (t.inputs.len > 1) {
        if (ent.input_schemas.len != t.inputs.len) return NoMatch;
        if (ent.broadcast_inputs.len != t.inputs.len - 1) return NoMatch;
        extra_parts = try b.a.alloc(udf_mod.TvfPartition, t.inputs.len - 1);
        for (t.inputs[1..], extra_parts, 1..) |inp, *dst, si| {
            const blk = try compileAndDrain(b, inp, true);
            dst.* = try blockPartition(b, ent.input_schemas[si], blk);
        }
    }

    const pc = try b.classifyPartition(t.partition_by);
    switch (pc) {
        .range_exact => {
            try checkRangeOrder(b, t.order_by);
            if (ent.passthrough.len != 0) {
                try pushAlignedTvf(b, ent, extra_parts, t.args, true);
            } else if (kernelReadsAll(ent)) {
                try pushReplaceTvf(b, ent, t);
            } else return NoMatch;
        },
        .merged_span => {
            if (ent.execution != .either) return NoMatch;
            if (t.order_by.len != 0) return NoMatch;
            if (ent.passthrough.len == 0) return NoMatch;
            try pushAlignedTvf(b, ent, extra_parts, t.args, false);
        },
    }
}

/// Frame-replacing kernel (writes every output, no passthrough): per-range
/// calls; the frame becomes the kernel's output schema.
fn pushReplaceTvf(b: *Builder, ent: *const udf_mod.TableEntry, t: *const ir.Op.TableFn) !void {
    // Equal output names alone say nothing about their values. This is the
    // same partition-key preservation contract used by tvfEmitKeys.
    if (!ent.ordered_output or t.inputs.len != 1) return NoMatch;
    const a = b.a;
    const inputs = try tvfInputs(b, ent, ent.input_schemas[0].len);
    const out = try a.alloc(Column, ent.output_schema.len);
    for (ent.output_schema, out) |src, *dst| {
        dst.* = .{ .name = try b.fb.canonName(src.name), .type = src.type, .nullable = true };
    }
    var route_name: ?[]const u8 = null;
    var pinned: std.ArrayListUnmanaged(PinnedCol) = .empty;
    for (t.partition_by) |name| {
        const input_idx = try b.resolveIdx(name);
        const output_idx = types.findColumn(ent.output_schema, name) orelse return NoMatch;
        if (std.mem.eql(u8, b.fb.cols.items[input_idx].name, b.route_name)) route_name = out[output_idx].name;
        if (b.pinnedName(name)) |value| try pinned.append(a, .{ .name = ent.output_schema[output_idx].name, .val = value });
    }
    const next_route = route_name orelse return NoMatch;
    try b.flushPending();
    try b.ops.append(a, .{ .tvf_grouped = .{ .spec = .{
        .process = ent.process,
        .user_data = ent.user_data,
        .args = try cloneArgs(b, t.args),
        .inputs = inputs,
        .out = out,
    } } });
    b.fb.cols.clearRetainingCapacity();
    b.fb.vis.clearRetainingCapacity();
    for (ent.output_schema, out) |src, o| {
        const idx = b.fb.cols.items.len;
        try b.fb.cols.append(a, o);
        try b.fb.setVis(src.name, idx);
    }
    b.route_name = next_route;
    // The output contract preserves partition values, not arbitrary fields
    // that happened to be fixed by the entry filter.
    b.pinned = pinned;
    // Range keys re-resolve by NAME against the new frame (the kernel keeps
    // partition-column names — SDK schema contract); verify they survive.
    var buf: [8]usize = undefined;
    _ = try b.rangeKeyIdxs(&buf);
    // Stale per-frame bookkeeping: forgotten constants become real subkeys
    // (correct, at worst a hair slower).
    b.const_idxs.clearRetainingCapacity();
}

fn dispatchWindow(b: *Builder, w: *const ir.WindowOp) anyerror!void {
    const specs = try b.a.alloc(ir.WindowSpec, w.specs.len);
    for (w.specs, specs) |src, *dst| {
        const part = try b.a.alloc([]const u8, src.partition_by.len);
        var contains_route_key = false;
        for (src.partition_by, part) |name, *physical| {
            physical.* = b.fb.cols.items[try b.resolveIdx(name)].name;
            if (std.mem.eql(u8, physical.*, b.route_name)) contains_route_key = true;
        }
        if (!contains_route_key) return NoMatch;
        const order = try b.a.alloc(ir.SortSpec, src.order_by.len);
        for (src.order_by, order) |ob, *physical| physical.* = .{
            .col = b.fb.cols.items[try b.resolveIdx(ob.col)].name,
            .desc = ob.desc,
        };
        dst.* = .{ .partition_by = part, .order_by = order, .frame = src.frame };
    }
    const calls = try b.a.alloc(ir.WindowCall, w.calls.len);
    const replaced = try b.a.alloc(?usize, w.calls.len);
    for (w.calls, calls, replaced) |src, *dst, *prior| {
        prior.* = if (b.fb.resolve(src.output_name)) |entry| entry.idx else null;
        const args = try b.a.alloc(ir.Expr, src.args.len);
        for (src.args, args) |arg, *cloned| {
            cloned.* = if (arg == .col_ref and std.mem.eql(u8, arg.col_ref, "*")) .{ .col_ref = "*" } else try b.cloneExpr(arg);
        }
        dst.* = src;
        dst.args = args;
        dst.output_name = try b.fb.canonName(src.output_name);
    }
    try b.flushPending();
    const columns = try b.a.alloc(Column, calls.len);
    for (calls, columns) |call, *col| col.* = try @import("../exec/window.zig").output_column(call, b.fb.cols.items);
    try b.ops.append(b.a, .{ .window = .{ .specs = specs, .calls = calls } });
    for (w.calls, columns, replaced) |src, col, prior| {
        const idx = b.fb.cols.items.len;
        try b.fb.cols.append(b.a, col);
        try b.bind_compute_output(src.output_name, idx, prior);
    }
}

fn dispatchJoin(b: *Builder, j: *const ir.Op.Join, above: []const Step) anyerror!void {
    if (j.extra_predicate != null or j.ranges.len != 0) return NoMatch;
    switch (j.join_type) {
        .left => {
            const ralias = rightAliasName(j.right) catch null;
            // Keys sourced from a proven-NULL side can never match: the
            // right side collapses to typed NULL columns without draining.
            if (ralias != null and joinKeyTouchesNullSides(b, j)) {
                const blk = try compileAndDrain(b, j.right, false);
                try b.null_sides.append(b.a, .{ .alias = ralias.?, .schema = blk.schema });
                return;
            }
            // Co-partitioned side table first: a scan-shaped right side with
            // an ON pair on the route key builds+probes shard-locally and
            // never drains through the mono engine (the LIVE
            // customer_monthly_totals class — millions of rows).
            side: {
                trySideJoin(b, j, ralias, try liveNames(b.input.node_arena, above)) catch |e| {
                    if (e == error.OutOfMemory) return e;
                    break :side;
                };
                return;
            }
            const blk = try compileAndDrain(b, j.right, true);
            if (blk.rows == 0) {
                const alias = ralias orelse return NoMatch;
                try b.null_sides.append(b.a, .{ .alias = alias, .schema = blk.schema });
                return;
            }
            if (blk.rows > (1 << 20)) return NoMatch;
            try pushProbe(b, j, ralias, blk, false, try liveNames(b.input.node_arena, above));
        },
        .inner => {
            const blk = try compileAndDrain(b, j.right, true);
            if (blk.rows > (1 << 20)) return NoMatch;
            try pushProbe(b, j, rightAliasName(j.right) catch null, blk, true, try liveNames(b.input.node_arena, above));
        },
        else => return NoMatch,
    }
}

/// Small-side hash probe (both join types). Probe-side key pairs pinned to
/// an entry literal eliminate at build (right rows filtered to the
/// literal); the remaining int keys pack into one i64. Every non-key right
/// column rides as a payload so downstream references resolve.
fn pushProbe(b: *Builder, j: *const ir.Op.Join, ralias: ?[]const u8, blk: DrainedBlock, inner: bool, live: ?[]const []const u8) anyerror!void {
    const a = b.a;
    if (j.on.len == 0) return NoMatch;

    var live_left: std.ArrayListUnmanaged([]const u8) = .empty;
    var live_right: std.ArrayListUnmanaged(usize) = .empty;
    var pin_right: std.ArrayListUnmanaged(usize) = .empty;
    var pin_vals: std.ArrayListUnmanaged(Value) = .empty;
    var key_right = try a.alloc(bool, blk.schema.len);
    @memset(key_right, false);
    for (j.on) |pair| {
        const rci = types.findColumn(blk.schema, pair.right) orelse {
            sideTrace("broadcast build column '{s}' unresolved", .{pair.right});
            return NoMatch;
        };
        key_right[rci] = true;
        if (b.pinnedName(pair.left)) |v| {
            try pin_right.append(a, rci);
            try pin_vals.append(a, v);
        } else {
            try live_left.append(a, pair.left);
            try live_right.append(a, rci);
        }
    }
    // Composite keys retain every component's bits in the general probe.
    // Packing two BIGINTs into one integer can alias distinct SQL keys.
    if (live_left.items.len > 0) {
        var needs_keyed = live_left.items.len > 1;
        for (live_right.items) |rci| {
            if (isStringFamilyType(blk.schema[rci].type)) needs_keyed = true;
        }
        if (needs_keyed) {
            return pushKeyedBroadcast(b, ralias, blk, inner, live, pin_right.items, pin_vals.items, live_left.items, live_right.items, key_right);
        }
        const probe = try b.resolveIdx(live_left.items[0]);
        if (!isIntFamilyType(b.fb.cols.items[probe].type) or !isIntFamilyType(blk.schema[live_right.items[0]].type)) return NoMatch;
    }

    if (live_left.items.len == 0) {
        // Fully-pinned join: every ON pair binds to an entry literal, so
        // every left row sees the SAME matching right rows — the join
        // degenerates to constant columns (e.g. INNER JOIN division ON
        // division.id = r.divisionId under a single-division filter).
        // More than one matching right row would multiply left rows —
        // unsupported; zero matches empties an INNER region (decline to
        // the mono path) while LEFT attaches typed NULLs.
        var match_row: ?usize = null;
        rows: for (0..blk.rows) |i| {
            for (pin_right.items, pin_vals.items) |rci, want| {
                if (!try pinMatches(blk.stores[rci].view(), i, want)) continue :rows;
            }
            if (match_row != null) {
                sideTrace("fully pinned broadcast has multiple matching rows", .{});
                return NoMatch;
            }
            match_row = i;
        }
        const mi = match_row orelse {
            if (inner) return NoMatch;
            const alias = ralias orelse return NoMatch;
            try b.null_sides.append(a, .{ .alias = alias, .schema = blk.schema });
            return;
        };
        var ccols: std.ArrayListUnmanaged(Column) = .empty;
        var cvals: std.ArrayListUnmanaged(?Value) = .empty;
        for (blk.schema, 0..) |col, ci| {
            if (key_right[ci]) continue;
            if (live) |names| {
                const tail = lastSegment(col.name);
                var referenced = false;
                for (names) |n| {
                    if (std.ascii.eqlIgnoreCase(lastSegment(n), tail)) {
                        referenced = true;
                        break;
                    }
                }
                if (!referenced) continue;
            }
            const idx = try b.fb.addCol(col.name, col.type, true);
            try ccols.append(a, b.fb.cols.items[idx]);
            try cvals.append(a, try valueAtRow(blk.stores[ci].view(), mi));
            try b.fb.setVis(col.name, idx);
            if (ralias) |al| try b.fb.setVis(try visKeyFor(a, al, col.name), idx);
        }
        if (ccols.items.len > 0) {
            try b.flushPending();
            try b.ops.append(a, .{ .const_cols = .{
                .cols = ccols.items,
                .values = cvals.items,
            } });
        }
        return;
    }

    // Build the map (and kept-row list) over rows matching every pinned
    // literal, with NULL keys skipped (SQL join semantics).
    const map = try a.create(region.KeyMap);
    map.* = .empty;
    var kept: std.ArrayListUnmanaged(u32) = .empty;
    rows: for (0..blk.rows) |i| {
        for (pin_right.items, pin_vals.items) |rci, want| {
            if (!try pinMatches(blk.stores[rci].view(), i, want)) continue :rows;
        }
        const key = i64At(blk.stores[live_right.items[0]].view(), i) orelse continue :rows;
        const gop = try map.getOrPut(a, key);
        // A broadcast probe emits at most one match. Both INNER and LEFT
        // joins must remain staged when the build side would multiply rows.
        if (gop.found_existing) return NoMatch;
        gop.value_ptr.* = @intCast(kept.items.len);
        try kept.append(a, @intCast(i));
    }

    const probe_idx = try b.resolveIdx(live_left.items[0]);

    // Payloads: every non-key right column the steps above can reference,
    // gathered to kept-row order.
    var payloads: std.ArrayListUnmanaged(region.Payload) = .empty;
    for (blk.schema, 0..) |col, ci| {
        if (key_right[ci]) continue;
        if (live) |names| {
            const tail = lastSegment(col.name);
            var referenced = false;
            for (names) |n| {
                if (std.ascii.eqlIgnoreCase(lastSegment(n), tail)) {
                    referenced = true;
                    break;
                }
            }
            if (!referenced) continue;
        }
        const store = try a.create(ColumnStore);
        store.* = try ColumnStore.init(a, col.type, true);
        const src = blk.stores[ci].view();
        for (kept.items) |ri| {
            try region.appendViewRange(a, store, src, ri, ri + 1);
        }
        const idx = try b.fb.addCol(col.name, col.type, true);
        try payloads.append(a, .{
            .name = b.fb.cols.items[idx].name,
            .view = store.view(),
            .out_type = col.type,
        });
        try b.fb.setVis(col.name, idx);
        if (ralias) |al| try b.fb.setVis(try visKeyFor(a, al, col.name), idx);
    }

    try b.flushPending();
    try b.ops.append(a, .{ .hash_probe = .{
        .probe = probe_idx,
        .map = map,
        .payload = payloads.items,
        .inner = inner,
    } });
}

fn strBytesAt(v: ColumnView, i: usize) ?[]const u8 {
    if (!v.isValid(i)) return null;
    return switch (v.data) {
        .varchar, .string, .char, .json => |s| s.rowBytes(i),
        else => null,
    };
}

fn pinMatches(v: ColumnView, i: usize, want: Value) !bool {
    switch (want) {
        .text => |s| {
            const got = strBytesAt(v, i) orelse return false;
            return std.mem.eql(u8, got, s);
        },
        else => {
            const got = i64At(v, i) orelse return false;
            const want_i = valueI64(want) orelse return NoMatch;
            return got == want_i;
        },
    }
}

/// The broadcast form of keyed_probe: a small compile-time-drained build
/// side with string or composite keys. Map + interners live in the
/// ctx arena beside the drained block, so cache hits replay them for free.
fn pushKeyedBroadcast(
    b: *Builder,
    ralias: ?[]const u8,
    blk: DrainedBlock,
    inner: bool,
    live: ?[]const []const u8,
    pin_right: []const usize,
    pin_vals: []const Value,
    live_left: []const []const u8,
    live_right: []const usize,
    key_right: []const bool,
) anyerror!void {
    const a = b.a;
    if (live_left.len > region.MAX_KEYED_PAIRS) {
        sideTrace("broadcast has {d} key pairs (max {d})", .{ live_left.len, region.MAX_KEYED_PAIRS });
        return NoMatch;
    }

    const views = try a.alloc(ColumnView, blk.schema.len);
    for (blk.stores, views) |*st, *v| v.* = st.view();

    const pairs = try a.alloc(region.KeyedPair, live_left.len);
    const interners = try a.alloc(?*const region.StrInterner, live_left.len);
    const interner_ptrs = try a.alloc(?*region.StrInterner, live_left.len);
    for (live_left, live_right, pairs, interners, interner_ptrs) |ln, rci, *pp, *ip, *mp| {
        const probe = b.resolveIdx(ln) catch |err| {
            sideTrace("broadcast probe column '{s}' unresolved", .{ln});
            return err;
        };
        const pt = b.fb.cols.items[probe].type;
        const bt = blk.schema[rci].type;
        const kind: region.KeyedPairKind = if (isIntFamilyType(pt) and isIntFamilyType(bt))
            .int
        else if (isStringFamilyType(pt) and isStringFamilyType(bt))
            .str
        else if (isIntFamilyType(pt) and isStringFamilyType(bt))
            .int_from_str_build
        else {
            sideTrace("broadcast pair '{s}' type mismatch ({s} vs {s})", .{ ln, @tagName(pt), @tagName(bt) });
            return NoMatch;
        };
        pp.* = .{ .probe = probe, .build = rci, .kind = kind };
        if (kind == .str) {
            const it = try a.create(region.StrInterner);
            it.* = .empty;
            ip.* = it;
            mp.* = it;
        } else {
            ip.* = null;
            mp.* = null;
        }
    }

    const map = try a.create(region.MultiKeyMap);
    map.* = .empty;
    rows: for (0..blk.rows) |i| {
        for (pin_right, pin_vals) |rci, want| {
            if (!try pinMatches(views[rci], i, want)) continue :rows;
        }
        var key: region.MultiKey = @splat(0);
        for (pairs, 0..) |p, pi| {
            const v = views[p.build];
            switch (p.kind) {
                .int => key[pi] = i64At(v, i) orelse continue :rows,
                .int_from_str_build => {
                    const bytes = strBytesAt(v, i) orelse continue :rows;
                    key[pi] = std.fmt.parseInt(i64, bytes, 10) catch continue :rows;
                },
                .str => {
                    const bytes = strBytesAt(v, i) orelse continue :rows;
                    const it = interner_ptrs[pi].?;
                    const gop = try it.getOrPut(a, bytes);
                    if (!gop.found_existing) gop.value_ptr.* = @intCast(it.count());
                    key[pi] = gop.value_ptr.*;
                },
            }
        }
        const gop = try map.getOrPut(a, key);
        if (gop.found_existing) {
            sideTrace("broadcast dup build key (row {d})", .{i});
            return NoMatch; // dup key: 1:N changes row counts
        }
        gop.value_ptr.* = @intCast(i);
    }

    // Scan-fused membership filter: an INNER single-pair probe as the
    // program's FIRST op drops exactly the rows whose key misses the map —
    // pre-cutting those at the exchange scatter is semantics-neutral (the
    // op still runs, now on the all-match fast path, so its restructure
    // emit disappears) and the cut lands before scatter/consolidation.
    // The map build above already proved build keys unique. Ordered mode
    // has no scatter stage to mask — skip.
    var semi_converted = false;
    if (inner and pairs.len == 1 and b.ops.items.len == 0 and
        b.pending_nulls.items.len == 0 and !b.order_aligned)
    {
        semi_converted = true;
        const p = pairs[0];
        const bv = views[p.build];
        if (p.kind == .str) {
            var vals: std.ArrayListUnmanaged([]const u8) = .empty;
            rows: for (0..blk.rows) |i| {
                for (pin_right, pin_vals) |rci, want| {
                    if (!try pinMatches(views[rci], i, want)) continue :rows;
                }
                const bytes = strBytesAt(bv, i) orelse continue :rows;
                try vals.append(a, try a.dupe(u8, bytes));
            }
            std.mem.sortUnstable([]const u8, vals.items, {}, struct {
                fn less(_: void, x: []const u8, y: []const u8) bool {
                    return std.mem.order(u8, x, y) == .lt;
                }
            }.less);
            try b.member_filters.append(a, .{ .col = p.probe, .strs = vals.items, .is_str = true });
        } else {
            var vals: std.ArrayListUnmanaged(i64) = .empty;
            rows: for (0..blk.rows) |i| {
                for (pin_right, pin_vals) |rci, want| {
                    if (!try pinMatches(views[rci], i, want)) continue :rows;
                }
                const key = switch (p.kind) {
                    .int => i64At(bv, i) orelse continue :rows,
                    .int_from_str_build => k: {
                        const bytes = strBytesAt(bv, i) orelse continue :rows;
                        break :k std.fmt.parseInt(i64, bytes, 10) catch continue :rows;
                    },
                    .str => unreachable,
                };
                try vals.append(a, key);
            }
            std.mem.sortUnstable(i64, vals.items, {}, std.sort.asc(i64));
            try b.member_filters.append(a, .{ .col = p.probe, .ints = vals.items, .is_str = false });
        }
        sideTrace("semi member filter registered ({d} build rows)", .{blk.rows});
    }

    var payloads: std.ArrayListUnmanaged(region.KeyedPayload) = .empty;
    for (blk.schema, 0..) |col, ci| {
        if (key_right[ci]) continue;
        if (live) |names| {
            const tail = lastSegment(col.name);
            var referenced = false;
            for (names) |n| {
                if (std.ascii.eqlIgnoreCase(lastSegment(n), tail)) {
                    referenced = true;
                    break;
                }
            }
            if (!referenced) continue;
        }
        const idx = try b.fb.addCol(col.name, col.type, true);
        try payloads.append(a, .{
            .name = b.fb.cols.items[idx].name,
            .src = ci,
            .out_type = col.type,
        });
        try b.fb.setVis(col.name, idx);
        if (ralias) |al| try b.fb.setVis(try visKeyFor(a, al, col.name), idx);
    }

    // Member filter + zero live payloads: the probe op is a complete
    // no-op (the filter already dropped every row it would drop, and it
    // appends nothing) — elide it. This also restores the union-append
    // TVF fusion when the plan-filter join was the only thing below it.
    if (semi_converted and payloads.items.len == 0) {
        sideTrace("semi member filter absorbs probe op (no live payloads)", .{});
        return;
    }

    try b.flushPending();
    try b.ops.append(a, .{ .keyed_probe = .{
        .pairs = pairs,
        .side = .{ .broadcast = .{
            .map = map,
            .interners = interners,
            .views = views,
            .rows = blk.rows,
        } },
        .payload = payloads.items,
        .inner = inner,
    } });
}

fn isIntFamilyType(t: types.Type) bool {
    return switch (t) {
        .tinyint, .smallint, .int, .bigint, .date, .datetime => true,
        else => false,
    };
}

fn isStringFamilyType(t: types.Type) bool {
    return switch (t) {
        .varchar, .string, .char => true,
        else => false,
    };
}

/// Side column resolution: last match wins (entry-derived columns sit after
/// the scan columns and shadow same-named ones — exec.Compute's contract is
/// replace-in-place for same names, so a clash can't actually occur; the
/// backward search just mirrors the frame's precedence rule).
fn sideColIdx(schema: []const Column, name: []const u8) ?usize {
    const tail = lastSegment(name);
    var i = schema.len;
    while (i > 0) : (i -= 1) {
        if (std.ascii.eqlIgnoreCase(lastSegment(schema[i - 1].name), tail)) return i - 1;
    }
    return null;
}

const SideEntry = struct {
    scan: *const ir.Op.Scan,
    top_select: ?[]const []const u8,
    /// Bottom-up evaluation order (below the aggregate when one exists).
    derived: []const Derived,
    filters: []const PredicateExpr,
    /// A single GROUP BY between the top and the scan (the crossplans CMT
    /// collapse). Derived collected ABOVE it land in `above_derived` —
    /// trySideJoin only accepts constants there (the `-2 AS divisionId`
    /// class), pushed below the aggregate as extra group keys.
    agg: ?*const ir.Op.GroupBy = null,
    above_derived: []const Derived = &.{},
};

/// Walk a join right side that is nothing but scan + computes + filters +
/// projections (the rf_customer_monthly_totals class), with at most one
/// GROUP BY on the way down (the crossplans CMT collapse). Anything else
/// structural (join/window/TVF) declines — those need the drain path.
fn collectSideEntry(na: Allocator, node: *const ir.Op) !SideEntry {
    var top_select: ?[]const []const u8 = null;
    var derived: std.ArrayListUnmanaged(Derived) = .empty;
    var above_derived: std.ArrayListUnmanaged(Derived) = .empty;
    var filters: std.ArrayListUnmanaged(PredicateExpr) = .empty;
    var agg: ?*const ir.Op.GroupBy = null;
    var cur = node;
    var guard: usize = 0;
    const scan: *const ir.Op.Scan = blk: while (guard < 64) : (guard += 1) {
        switch (cur.*) {
            .materialize => |m| cur = m.upstream,
            .alias => |al| cur = al.upstream,
            .select => |p| {
                // The topmost projection defines the side's visible set;
                // deeper ones only narrow (SQL validity guarantees they
                // contain everything the top one needs).
                if (top_select == null) top_select = p.columns;
                cur = p.upstream;
            },
            .compute => |c| {
                try derived.appendSlice(na, c.derived);
                cur = c.upstream;
            },
            .filter => |f| {
                try filters.append(na, f.predicate);
                cur = f.upstream;
            },
            .group_by => |*g| {
                if (agg != null) return NoMatch;
                // Filters collected so far sit ABOVE the aggregate — a
                // HAVING, which the per-bin fold can't apply.
                if (filters.items.len > 0) return NoMatch;
                agg = g;
                above_derived = derived;
                derived = .empty;
                cur = g.upstream;
            },
            .scan => |*s| break :blk s,
            else => return NoMatch,
        }
    } else return NoMatch;
    if (filters.items.len == 0) return NoMatch; // unfiltered side scan: not worth a region side
    // Computes were collected top-down; evaluation is bottom-up.
    std.mem.reverse(Derived, derived.items);
    std.mem.reverse(Derived, above_derived.items);
    return .{
        .scan = scan,
        .top_select = top_select,
        .derived = derived.items,
        .filters = filters.items,
        .agg = agg,
        .above_derived = above_derived.items,
    };
}

/// Compile a LEFT join whose right side is a scan-shaped subtree
/// co-partitioned with the region (an ON pair binds the route key): the
/// side gets its own chunked scan + exchange scatter and each shard builds
/// a local multi-key map — the mono engine never materializes it.
fn sideTrace(comptime fmt: []const u8, args: anytype) void {
    if (getenv("THINDB_REGION_TRACE") != null) {
        std.debug.print("[region] side decline: " ++ fmt ++ "\n", args);
    }
}

fn trySideJoin(b: *Builder, j: *const ir.Op.Join, ralias: ?[]const u8, live: ?[]const []const u8) anyerror!void {
    const a = b.a;
    if (b.order_aligned) return NoMatch; // no exchange to co-partition through
    if (j.on.len == 0 or j.on.len > region.MAX_KEYED_PAIRS) return NoMatch;
    const registry = b.input.udf_registry orelse return NoMatch;

    // Co-partition requirement: some ON pair's LEFT is the route key column
    // (scatter hashes ONLY that column, so equal route keys — and therefore
    // every possible match — land in the same shard on both sides).
    const route_idx = types.findColumn(b.fb.cols.items, b.route_name) orelse {
        sideTrace("route name '{s}' unresolved", .{b.route_name});
        if (getenv("THINDB_REGION_STEPS") != null) {
            for (b.fb.vis.items) |e| {
                if (std.ascii.indexOfIgnoreCase(e.name, lastSegment(b.route_name)) != null) {
                    std.debug.print("[region]   vis '{s}' -> {d}\n", .{ e.name, e.idx });
                }
            }
        }
        return NoMatch;
    };
    var route_pair: ?usize = null;
    for (j.on, 0..) |pair, pi| {
        const e = b.fb.resolve(pair.left) orelse {
            sideTrace("ON left '{s}' unresolved", .{pair.left});
            return NoMatch;
        };
        if (e.idx == route_idx) {
            route_pair = pi;
            break;
        }
    }
    if (route_pair == null) {
        sideTrace("no ON pair binds route '{s}' (idx {d})", .{ b.route_name, route_idx });
        return NoMatch;
    }

    // Dedupe: ctc/ctl reference the SAME materialized CTE node — one side
    // spec + one scatter, two probe ops.
    var side_idx: ?usize = null;
    for (b.side_nodes.items, 0..) |n, i| {
        if (n == j.right) {
            side_idx = i;
            break;
        }
    }

    if (side_idx == null) {
        const se = collectSideEntry(b.input.node_arena, j.right) catch |e| {
            sideTrace("right subtree not scan-shaped ({s})", .{@errorName(e)});
            return e;
        };

        // Aggregating side: the effective group-key set is the declared
        // GROUP BY keys plus every above-aggregate derived that is a
        // constant (`-2 AS divisionId`, pushed below the aggregate) or an
        // alias of a key already in the set (the parser's
        // `__join_on_right_N` copies, resolved to their source column —
        // never materialized). Grouping additionally on a constant or a
        // key copy changes nothing. Anything else above declines.
        const AggKey = struct { name: []const u8, src: []const u8 };
        var agg_keys: std.ArrayListUnmanaged(AggKey) = .empty;
        if (se.agg) |g| {
            for (g.group_cols) |c| {
                try agg_keys.append(a, .{ .name = lastSegment(c), .src = lastSegment(c) });
            }
            for (se.above_derived) |d| {
                const src: ?[]const u8 = switch (d.expr) {
                    .lit, .null_lit => d.name,
                    .col_ref => |r| blk: {
                        const t = lastSegment(r);
                        for (agg_keys.items) |k| {
                            if (std.ascii.eqlIgnoreCase(k.name, t)) break :blk k.src;
                        }
                        break :blk null;
                    },
                    else => null,
                };
                if (src == null) {
                    sideTrace("derived '{s}' above side aggregate is neither constant nor a key alias", .{d.name});
                    return NoMatch;
                }
                var present = false;
                for (agg_keys.items) |k| {
                    if (std.ascii.eqlIgnoreCase(k.name, d.name)) {
                        present = true;
                        break;
                    }
                }
                if (!present) try agg_keys.append(a, .{ .name = d.name, .src = src.? });
            }
            // Per-bin aggregation is only globally exact when the route
            // key is a group key (every row of a group is shard-local).
            const rt = lastSegment(j.on[route_pair.?].right);
            var route_grouped = false;
            for (agg_keys.items) |k| {
                if (std.ascii.eqlIgnoreCase(k.name, rt)) {
                    route_grouped = true;
                    break;
                }
            }
            if (!route_grouped) {
                sideTrace("side GROUP BY omits route '{s}'", .{rt});
                return NoMatch;
            }
            for (g.aggs) |sp| {
                if (sp.func != .sum or sp.col == null or sp.arg2_col != null or sp.udf_name != null) {
                    sideTrace("side aggregate '{s}' is not a plain SUM", .{sp.as});
                    return NoMatch;
                }
            }
        }

        var filt_list: std.ArrayListUnmanaged(PredicateExpr) = .empty;
        for (se.filters) |f| {
            const cloned = try clonePredPlain(a, f);
            // A folded-false side (the non-plans variants' 1=0 CMT) belongs
            // to the empty-proof NULL-collapse path, not a side scatter.
            if (predAlwaysFalse(cloned)) return NoMatch;
            try filt_list.append(a, cloned);
        }

        const side_table = b.input.db.openTable(se.scan.table.name, .{}) catch return NoMatch;

        // Pinned ON pairs (left bound to an entry literal) filter the side
        // scan directly when the side column is a stored column; the pair
        // still participates in the key so unpinnable ones stay correct.
        // With a side aggregate the filter runs BELOW the GROUP BY, which
        // only commutes when the pinned column is a group key.
        pin: for (j.on) |pair| {
            const v = b.pinnedName(pair.left) orelse continue;
            const tail = lastSegment(pair.right);
            if (side_table.schema.columnIndex(tail) == null) continue;
            if (se.agg) |g| {
                for (g.group_cols) |kc| {
                    if (std.ascii.eqlIgnoreCase(lastSegment(kc), tail)) {
                        try filt_list.append(a, .{ .leaf = .{
                            .col = try a.dupe(u8, tail),
                            .op = .eq,
                            .val = try cloneValuePlain(a, v),
                        } });
                        continue :pin;
                    }
                }
                continue :pin;
            }
            try filt_list.append(a, .{ .leaf = .{
                .col = try a.dupe(u8, tail),
                .op = .eq,
                .val = try cloneValuePlain(a, v),
            } });
        }

        const side_filter: PredicateExpr = if (filt_list.items.len == 1)
            filt_list.items[0]
        else
            .{ .@"and" = filt_list.items };
        var prune_list: std.ArrayListUnmanaged(predicate_mod.Predicate) = .empty;
        try collectAndLeaves(a, side_filter, &prune_list);

        var side_derived: std.ArrayListUnmanaged(Derived) = .empty;
        for (se.derived) |d| {
            const cloned = try cloneExprPlain(a, d.expr);
            try side_derived.append(a, .{
                .name = try a.dupe(u8, d.name),
                .expr = try substDerivedRefs(a, cloned, side_derived.items),
            });
        }
        // Constant derived above the aggregate (the `-2 AS divisionId`
        // class) evaluate with the scan; key aliases resolve at compile
        // and are never materialized.
        for (se.above_derived) |d| switch (d.expr) {
            .lit, .null_lit => try side_derived.append(a, .{
                .name = try a.dupe(u8, d.name),
                .expr = try cloneExprPlain(a, d.expr),
            }),
            else => {},
        };

        // Scan projection: the visible set minus derived names, plus every
        // stored column the derived exprs read. An aggregating side scans
        // its group-key sources and SUM sources instead of the (post-agg)
        // visible set.
        var scan_cols_opt: ?[]const []const u8 = null;
        scan_cols: {
            var cols: std.ArrayListUnmanaged([]const u8) = .empty;
            if (se.agg) |g| {
                var want: std.ArrayListUnmanaged([]const u8) = .empty;
                for (g.group_cols) |c| try want.append(a, c);
                for (g.aggs) |sp| try want.append(a, sp.col.?);
                outer: for (want.items) |col| {
                    const tail = lastSegment(col);
                    for (side_derived.items) |d| {
                        if (std.ascii.eqlIgnoreCase(d.name, tail)) continue :outer;
                    }
                    if (side_table.schema.columnIndex(tail) == null) {
                        sideTrace("side agg column '{s}' neither stored nor derived", .{col});
                        return NoMatch;
                    }
                    for (cols.items) |c| {
                        if (std.ascii.eqlIgnoreCase(lastSegment(c), tail)) continue :outer;
                    }
                    try cols.append(a, try a.dupe(u8, tail));
                }
            } else if (se.top_select) |sel| {
                outer: for (sel) |col| {
                    for (side_derived.items) |d| {
                        if (std.ascii.eqlIgnoreCase(d.name, lastSegment(col))) continue :outer;
                    }
                    try cols.append(a, try a.dupe(u8, lastSegment(col)));
                }
            } else break :scan_cols;
            for (side_derived.items) |d| {
                var refs: std.ArrayListUnmanaged([]const u8) = .empty;
                try exprColNames(b.input.node_arena, d.expr, &refs);
                ref: for (refs.items) |n| {
                    const tail = lastSegment(n);
                    if (side_table.schema.columnIndex(tail) == null) continue;
                    for (cols.items) |c| {
                        if (std.ascii.eqlIgnoreCase(lastSegment(c), tail)) continue :ref;
                    }
                    try cols.append(a, try a.dupe(u8, tail));
                }
            }
            scan_cols_opt = cols.items;
        }

        recordSubtreeVersions(b, j.right);
        const n_threads = @max(b.input.effectiveDop(), 1);
        const bs = try buildScanSources(b.input, side_table, prune_list.items, side_filter, scan_cols_opt, n_threads);
        var sources_owned = true;
        errdefer if (sources_owned) {
            for (bs.sources) |*q| q.deinit();
            b.input.allocator.free(bs.sources);
        };

        const raw_scan_schema = bs.sources[0].outputSchema();
        const scan_schema = try a.alloc(Column, raw_scan_schema.len);
        for (raw_scan_schema, scan_schema) |src, *dst| {
            dst.* = src;
            dst.name = try a.dupe(u8, src.name);
        }
        var side_schema: []const Column = scan_schema;
        if (side_derived.items.len > 0) {
            const typed = region.computeOutputSchema(b.input.allocator, a, scan_schema, side_derived.items, registry) catch |ce| {
                if (getenv("THINDB_REGION_TRACE") != null) {
                    std.debug.print("[region] side decline: entry compute failed ({s}); derived:", .{@errorName(ce)});
                    for (side_derived.items) |d| std.debug.print(" {s}", .{d.name});
                    std.debug.print("; scan cols:", .{});
                    for (scan_schema) |c| std.debug.print(" {s}", .{c.name});
                    std.debug.print("\n", .{});
                }
                return NoMatch;
            };
            if (typed.len != scan_schema.len + side_derived.items.len) {
                sideTrace("side entry compute replaced a column", .{});
                return NoMatch;
            }
            side_schema = typed;
        }

        // Exchange route column in the PRE schema: alias names (never
        // materialized below an aggregate) resolve to their source.
        var route_src = lastSegment(j.on[route_pair.?].right);
        for (agg_keys.items) |k| {
            if (std.ascii.eqlIgnoreCase(k.name, route_src)) {
                route_src = k.src;
                break;
            }
        }
        const key_col = sideColIdx(side_schema, route_src) orelse {
            sideTrace("route side col '{s}' not in side schema", .{route_src});
            return NoMatch;
        };

        // Aggregating side: probes and payloads resolve against the
        // post-agg schema — group keys (declared ++ pushed-down consts)
        // then the SUM outputs with the engine's canonical widening.
        var probe_schema = side_schema;
        var side_agg: ?region.SideAgg = null;
        if (se.agg) |g| {
            const n_keys = agg_keys.items.len;
            const post = try a.alloc(Column, n_keys + g.aggs.len);
            const group_srcs = try a.alloc(usize, n_keys);
            const agg_srcs = try a.alloc(usize, g.aggs.len);
            // Map keys dedupe to the distinct source columns — alias copies
            // (same pre column twice) add nothing to the grouping.
            var key_srcs: std.ArrayListUnmanaged(usize) = .empty;
            for (agg_keys.items, group_srcs, post[0..n_keys]) |k, *src, *col| {
                const ki = sideColIdx(side_schema, k.src) orelse {
                    sideTrace("side group key '{s}' (src '{s}') not in side schema", .{ k.name, k.src });
                    return NoMatch;
                };
                const kt = side_schema[ki].type;
                if (!(isIntFamilyType(kt) or isStringFamilyType(kt))) {
                    sideTrace("side group key '{s}' has unsupported type {s}", .{ k.name, @tagName(kt) });
                    return NoMatch;
                }
                src.* = ki;
                col.* = .{ .name = try a.dupe(u8, k.name), .type = kt, .nullable = side_schema[ki].nullable };
                var seen = false;
                for (key_srcs.items) |ks| {
                    if (ks == ki) {
                        seen = true;
                        break;
                    }
                }
                if (!seen) try key_srcs.append(a, ki);
            }
            if (key_srcs.items.len > region.MAX_SIDE_GROUP_KEYS) {
                sideTrace("side aggregate has {d} distinct group keys (max {d})", .{ key_srcs.items.len, region.MAX_SIDE_GROUP_KEYS });
                return NoMatch;
            }
            for (g.aggs, agg_srcs, post[n_keys..]) |sp, *src, *col| {
                const si2 = sideColIdx(side_schema, sp.col.?) orelse {
                    sideTrace("side SUM source '{s}' not in side schema", .{sp.col.?});
                    return NoMatch;
                };
                const st2 = side_schema[si2].type;
                switch (st2) {
                    .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .decimal64, .decimal128 => {},
                    else => {
                        sideTrace("side SUM('{s}') has unsupported type {s}", .{ sp.col.?, @tagName(st2) });
                        return NoMatch;
                    },
                }
                const ot = aggregate_mod.aggOutputTypeFor(sp, st2) catch {
                    sideTrace("side SUM('{s}') output type unresolved", .{sp.col.?});
                    return NoMatch;
                };
                src.* = si2;
                col.* = .{ .name = try a.dupe(u8, sp.as), .type = ot, .nullable = true };
            }
            probe_schema = post;
            side_agg = .{ .group_srcs = group_srcs, .key_srcs = key_srcs.items, .agg_srcs = agg_srcs };
        }

        try b.side_nodes.append(a, j.right);
        try b.ctx.side_specs.append(a, .{
            .table = try a.dupe(u8, se.scan.table.name),
            .prune = prune_list.items,
            .filter = side_filter,
            .scan_cols = scan_cols_opt,
            .entry_derived = side_derived.items,
            .scan_schema = scan_schema,
            .pre_schema = side_schema,
            .schema = probe_schema,
            .key_col = key_col,
            .agg = side_agg,
        });
        try b.side_sources.append(a, bs.sources);
        sources_owned = false;
        side_idx = b.side_nodes.items.len - 1;
    }

    const spec = &b.ctx.side_specs.items[side_idx.?];

    // Key pairs: every ON pair keys the map (pinned pairs included — the
    // frame carries their column, constant or not).
    const pairs = try a.alloc(region.KeyedPair, j.on.len);
    const key_build = try a.alloc(bool, spec.schema.len);
    @memset(key_build, false);
    for (j.on, pairs) |pair, *dst| {
        const probe = try b.resolveIdx(pair.left);
        const build = sideColIdx(spec.schema, pair.right) orelse {
            sideTrace("ON right '{s}' not in side schema", .{pair.right});
            return NoMatch;
        };
        const pt = b.fb.cols.items[probe].type;
        const bt = spec.schema[build].type;
        const kind: region.KeyedPairKind = if (isIntFamilyType(pt) and isIntFamilyType(bt))
            .int
        else if (isStringFamilyType(pt) and isStringFamilyType(bt))
            .str
        else if (isIntFamilyType(pt) and isStringFamilyType(bt))
            .int_from_str_build
        else {
            sideTrace("pair '{s}'/'{s}' type mismatch ({s} vs {s})", .{ pair.left, pair.right, @tagName(pt), @tagName(bt) });
            return NoMatch;
        };
        dst.* = .{ .probe = probe, .build = build, .kind = kind };
        key_build[build] = true;
    }

    // Payloads: every non-key side column the steps above can reference. A
    // column whose name matches a join-key tail (the parser leaves the
    // original columns behind its __join_on_right duplicates) gets ONLY the
    // alias-qualified name — bare visibility would shadow the main frame's
    // key column and corrupt every later bare reference (including the
    // route key itself for the NEXT side join).
    var payloads: std.ArrayListUnmanaged(region.KeyedPayload) = .empty;
    for (spec.schema, 0..) |col, ci| {
        if (key_build[ci]) continue;
        const tail = lastSegment(col.name);
        if (live) |names| {
            var referenced = false;
            for (names) |n| {
                if (std.ascii.eqlIgnoreCase(lastSegment(n), tail)) {
                    referenced = true;
                    break;
                }
            }
            if (!referenced) continue;
        }
        var key_tail = false;
        for (j.on) |pair| {
            if (std.ascii.eqlIgnoreCase(tail, lastSegment(pair.left)) or
                std.ascii.eqlIgnoreCase(tail, lastSegment(pair.right)))
            {
                key_tail = true;
                break;
            }
        }
        if (key_tail) {
            // Carry only under an ACTUAL alias-qualified reference — a bare
            // liveness hit comes from the spine's own key column, and even
            // registering the qualified name would make bare resolution of
            // that key ambiguous for every later step.
            const al = ralias orelse continue;
            var qualified_ref = false;
            if (live) |names| {
                for (names) |n| {
                    const dot = std.mem.lastIndexOfScalar(u8, n, '.') orelse continue;
                    if (std.ascii.eqlIgnoreCase(n[0..dot], al) and
                        std.ascii.eqlIgnoreCase(n[dot + 1 ..], tail))
                    {
                        qualified_ref = true;
                        break;
                    }
                }
            }
            // Unknown liveness (a TVF above hides the reference set) must
            // NOT register speculatively: a qualified key-tail entry makes
            // bare resolution of the spine's own key — including the route
            // key — ambiguous for every later step.
            if (!qualified_ref) continue;
        }
        const idx = try b.fb.addCol(col.name, col.type, true);
        try payloads.append(a, .{
            .name = b.fb.cols.items[idx].name,
            .src = ci,
            .out_type = col.type,
        });
        if (!key_tail) try b.fb.setVis(col.name, idx);
        if (ralias) |al| try b.fb.setVis(try visKeyFor(a, al, col.name), idx);
    }

    try b.flushPending();
    try b.ops.append(a, .{ .keyed_probe = .{
        .pairs = pairs,
        .side = .{ .shard = side_idx.? },
        .payload = payloads.items,
        .inner = false,
    } });
}

fn joinKeyTouchesNullSides(b: *Builder, j: *const ir.Op.Join) bool {
    for (b.null_sides.items) |ns| {
        if (joinKeyTouchesAlias(j, ns.alias)) return true;
    }
    return false;
}

fn recordKernel(b: *Builder, ent: *const udf_mod.TableEntry) !void {
    for (b.ctx.kernels.items) |k| {
        if (std.ascii.eqlIgnoreCase(k.name, ent.name)) return;
    }
    try b.ctx.kernels.append(b.a, .{ .name = try b.a.dupe(u8, ent.name), .process = ent.process });
}

// ---------------------------------------------------------------------------
// Helpers for the build pass.
// ---------------------------------------------------------------------------

fn collectAndLeaves(a: Allocator, p: PredicateExpr, out: *std.ArrayListUnmanaged(predicate_mod.Predicate)) !void {
    switch (p) {
        .leaf => |l| try out.append(a, l),
        .@"and" => |kids| for (kids) |k| try collectAndLeaves(a, k, out),
        else => {},
    }
}

const BuiltSources = struct {
    sources: []exec.Query,
    total_rows: u64,
    /// Ordered mode only: per-interval row estimates from the RG quantile
    /// weights (boundary RGs count toward the interval owning their min) —
    /// drives the fused scan+exec LPT assignment.
    iv_rows: []u64 = &.{},
};

fn build_staged_source(input: engine_v2.CompileInput, root: *const ir.Op, keys: []const []const u8) !BuiltSources {
    const sources = try input.allocator.alloc(exec.Query, 1);
    errdefer input.allocator.free(sources);
    const entry = try input.node_arena.create(ir.Op);
    entry.* = .{ .materialize = .{ .upstream = @constCast(root), .region_keys = keys } };
    sources[0] = try cte_stages.compileStaged(input, entry, null);
    return .{ .sources = sources, .total_rows = sources[0].stats().upper_rows };
}

/// Chunked fused-filter scans over the base table (the rf_custom recipe):
/// snapshot once, split row groups into ~4×DOP ranges, prune + fuse the
/// filter into every chunk. Scratch is query-lifetime — nothing here may
/// grow a cached ctx arena. Caller owns `sources` until the op takes them.
fn buildScanSources(
    input: engine_v2.CompileInput,
    table: anytype,
    prune_leaves: []const predicate_mod.Predicate,
    filter: PredicateExpr,
    scan_cols: ?[]const []const u8,
    n_threads: usize,
) !BuiltSources {
    const qa = input.allocator;
    table.ddl_lock.lockSharedUncancelable(table.io);
    defer table.ddl_lock.unlockShared(table.io);
    const snap = try Scan.captureSnapshotAlloc(table, qa);
    defer qa.free(snap.segments);
    var pin_held = true;
    defer if (pin_held) snap.memtable_snap.release();

    var total_rgs: usize = 0;
    var total_rows: u64 = snap.memtable_row_count;
    const seg_start = try qa.alloc(usize, snap.segment_count + 1);
    defer qa.free(seg_start);
    for (snap.segments, 0..) |e, i| {
        seg_start[i] = total_rgs;
        total_rgs += e.row_group_count;
        total_rows += e.row_count;
    }
    seg_start[snap.segment_count] = total_rgs;
    const n_chunks = @max(n_threads, @min(n_threads * 4, @max(total_rgs, 1)));

    const sources = try qa.alloc(exec.Query, n_chunks);
    var built: usize = 0;
    errdefer {
        for (sources[0..built]) |*q| q.deinit();
        qa.free(sources);
    }
    const scans = try qa.alloc(*Scan, n_chunks);
    defer qa.free(scans);
    for (0..n_chunks) |i| {
        const lo = i * total_rgs / n_chunks;
        const hi = if (i == n_chunks - 1) total_rgs else (i + 1) * total_rgs / n_chunks;
        // emit_loc: the physical row locator rides through the exchange as
        // the FINAL consolidation sort key, so (invoiceId, date) ties inside
        // a group keep the table's physical order — the same order the
        // engine's staged path presents to the estimates kernel, whose
        // representative-row picks are input-order-sensitive ("first row
        // wins"). Without it those picks are scatter-arrival nondeterministic.
        const s = Scan.allocWithProjectionLoc(qa, table, input.accountant, scan_cols, true, snap) catch return NoMatch;
        sources[i] = exec.makeQuery(qa, s);
        scans[i] = s;
        built += 1;
        const start = flatToCoord(lo, seg_start, snap.segment_count);
        const end = flatToCoord(hi, seg_start, snap.segment_count);
        s.setRange(start.seg, start.rg, end.seg, end.rg, i == n_chunks - 1);
        for (prune_leaves) |l| s.addPrune(l) catch {};
        const fused = s.tryFuseFilter(filter) catch return NoMatch;
        if (!fused) return NoMatch;
    }
    recutSourcesForPruning(qa, scans, seg_start, snap.segment_count, total_rgs);
    snap.memtable_snap.release();
    pin_held = false;
    return .{ .sources = sources, .total_rows = total_rows };
}

/// ParallelScan.rebalanceChunksForPruning for the region's chunk scans. The
/// even row-group split lands a selective query's few surviving row groups
/// (contiguous under the table order key) on one or two workers while the
/// rest idle — the exchange then runs at the pace of one thread decoding
/// every survivor. Re-cut the same flat index space so each chunk holds
/// ~equal SURVIVING row groups; bounds stay ascending, so chunk emission
/// order (and the emit_loc tie order riding it) is unchanged. Only when at
/// most half the groups survive — above that the even split is the better
/// work model.
fn recutSourcesForPruning(qa: Allocator, scans: []const *Scan, seg_start: []const usize, n_segs: usize, total: usize) void {
    if (scans.len <= 1) return;
    const mask = scans[0].survivingMask(qa) orelse return;
    defer qa.free(mask);
    if (mask.len != total) return;
    var surviving: usize = 0;
    for (mask) |m| surviving += @intFromBool(m);
    if (surviving == 0 or surviving > total / 2) return;

    const n = scans.len;
    var lo: usize = 0;
    var seen: usize = 0;
    var cut: usize = 0;
    for (scans, 0..) |s, c| {
        const target = (c + 1) * surviving / n + @intFromBool((c + 1) * surviving % n != 0);
        while (cut < total and seen < target) : (cut += 1) {
            if (mask[cut]) seen += 1;
        }
        const hi = if (c == n - 1) total else cut;
        const start = flatToCoord(lo, seg_start, n_segs);
        const end = flatToCoord(hi, seg_start, n_segs);
        s.resetRange(start.seg, start.rg, end.seg, end.rg, c == n - 1);
        lo = hi;
    }
    if (getenv("THINDB_REGION_TRACE") != null) {
        std.debug.print("[region] prune-recut: surviving={d}/{d} rgs across {d} chunks\n", .{ surviving, total, n });
    }
}

/// Decode a footer string stat (format.encodeStringPrefix) back to its
/// ≤16-byte prefix string. Safe as an interval boundary: every row of one
/// key shares one full value, so no key group straddles any bound string.
fn statPrefixString(qa: Allocator, stat: i128) ![]const u8 {
    const u: u128 = @as(u128, @bitCast(stat)) ^ (@as(u128, 1) << 127);
    var buf: [16]u8 = undefined;
    std.mem.writeInt(u128, &buf, u, .big);
    var n: usize = 16;
    while (n > 0 and buf[n - 1] == 0) n -= 1;
    return try qa.dupe(u8, buf[0..n]);
}

/// Order-aligned sources (#186): one fused-filter Scan per route-key
/// interval, bounds from row-weighted quantiles of per-RG footer mins.
/// The bounds are BOTH zonemap prunes and row-level conjuncts — boundary
/// row groups hold rows of two intervals, so prunes alone are not
/// row-exact. Interval 0 additionally admits NULL keys (leaf compares
/// fail on NULL; without the OR IS NULL arm those rows would vanish).
/// Each source scans segments sequentially, so its output is per-segment
/// key-sorted runs — no hash exchange.
fn buildOrderedSources(
    input: engine_v2.CompileInput,
    table: anytype,
    prune_leaves: []const predicate_mod.Predicate,
    filter: PredicateExpr,
    scan_cols: ?[]const []const u8,
    n_threads: usize,
    route_name: []const u8,
) !BuiltSources {
    const qa = input.allocator;
    table.ddl_lock.lockSharedUncancelable(table.io);
    defer table.ddl_lock.unlockShared(table.io);
    const snap = try Scan.captureSnapshotAlloc(table, qa);
    defer qa.free(snap.segments);
    var pin_held = true;
    defer if (pin_held) snap.memtable_snap.release();

    const route_phys = table.schema.columnIndex(route_name) orelse return NoMatch;

    // Weight the quantiles by rows that can SURVIVE the eq-pinned prunes
    // (e.g. projectId = N): the table clusters on the pinned prefix, so RG
    // stats decide accurately — without this the boundaries reflect every
    // project's keys and the queried project's rows skew into few intervals.
    const fmt = @import("../storage/format.zig");
    const EqPin = struct { phys: usize, v: i128 };
    var pins: std.ArrayListUnmanaged(EqPin) = .empty;
    defer pins.deinit(qa);
    for (prune_leaves) |l| {
        if (l.op != .eq) continue;
        const phys = table.schema.columnIndex(lastSegment(l.col)) orelse continue;
        const v: i128 = switch (l.val) {
            .tinyint => |x| x,
            .smallint => |x| x,
            .int => |x| x,
            .bigint => |x| x,
            .date => |x| x,
            .datetime => |x| x,
            .text => |t| fmt.encodeStringPrefix(t),
            else => continue,
        };
        try pins.append(qa, .{ .phys = phys, .v = v });
    }

    const RgMin = struct { min: i128, rows: u64 };
    var rgs: std.ArrayListUnmanaged(RgMin) = .empty;
    defer rgs.deinit(qa);
    var total_rows: u64 = snap.memtable_row_count;
    for (snap.segments) |e| {
        const handle = try table.acquireSegment(e.segment_id);
        defer table.releaseSegment(handle);
        rg_loop: for (handle.seg.info.row_groups) |rg| {
            for (pins.items) |p| {
                const s = rg.stats[p.phys];
                if (p.v < s.min or p.v > s.max) continue :rg_loop;
            }
            try rgs.append(qa, .{ .min = rg.stats[route_phys].min, .rows = rg.row_count });
        }
        total_rows += e.row_count;
    }
    if (rgs.items.len == 0) return NoMatch;
    std.mem.sortUnstable(RgMin, rgs.items, {}, struct {
        fn less(_: void, x: RgMin, y: RgMin) bool {
            return x.min < y.min;
        }
    }.less);

    // 2×threads intervals: 4× measured WORSE (per-interval scan setup ~37ms
    // CPU each outweighs the finer LPT quantum; scan_cpu 3.3→4.2s @48).
    const n_iv = @max(1, @min(2 * n_threads, rgs.items.len));
    var bounds: std.ArrayListUnmanaged([]const u8) = .empty;
    defer bounds.deinit(qa);
    var bound_mins: std.ArrayListUnmanaged(i128) = .empty;
    defer bound_mins.deinit(qa);
    {
        var seg_rows: u64 = 0;
        for (rgs.items) |r| seg_rows += r.rows;
        var acc: u64 = 0;
        var next_cut: usize = 1;
        for (rgs.items) |r| {
            acc += r.rows;
            while (next_cut < n_iv and acc >= seg_rows * next_cut / n_iv) {
                const s = try statPrefixString(qa, r.min);
                if (s.len > 0 and (bounds.items.len == 0 or
                    !std.mem.eql(u8, bounds.items[bounds.items.len - 1], s)))
                {
                    try bounds.append(qa, s);
                    try bound_mins.append(qa, r.min);
                } else qa.free(s);
                next_cut += 1;
            }
        }
    }

    const n_src = bounds.items.len + 1;
    const iv_rows = try qa.alloc(u64, n_src);
    @memset(iv_rows, 0);
    {
        var iv: usize = 0;
        for (rgs.items) |r| {
            while (iv < bound_mins.items.len and r.min >= bound_mins.items[iv]) iv += 1;
            iv_rows[iv] += r.rows;
        }
    }
    const sources = try qa.alloc(exec.Query, n_src);
    var built: usize = 0;
    errdefer {
        for (sources[0..built]) |*q| q.deinit();
        qa.free(sources);
    }
    for (0..n_src) |i| {
        const s = Scan.allocWithProjectionLoc(qa, table, input.accountant, scan_cols, true, snap) catch return NoMatch;
        sources[i] = exec.makeQuery(qa, s);
        built += 1;
        for (prune_leaves) |l| s.addPrune(l) catch {};
        var conj: std.ArrayListUnmanaged(PredicateExpr) = .empty;
        try conj.append(qa, filter);
        if (i > 0) {
            const p = predicate_mod.Predicate{ .col = route_name, .op = .gte, .val = .{ .text = bounds.items[i - 1] } };
            s.addPrune(p) catch {};
            try conj.append(qa, .{ .leaf = p });
        }
        if (i < bounds.items.len) {
            const p = predicate_mod.Predicate{ .col = route_name, .op = .lt, .val = .{ .text = bounds.items[i] } };
            if (i > 0) s.addPrune(p) catch {}; // interval 0 keeps NULL-bearing RGs
            if (i == 0) {
                const arms = try qa.alloc(PredicateExpr, 2);
                arms[0] = .{ .leaf = p };
                arms[1] = .{ .is_null = route_name };
                try conj.append(qa, .{ .@"or" = arms });
            } else {
                try conj.append(qa, .{ .leaf = p });
            }
        }
        const fexpr: PredicateExpr = if (conj.items.len == 1) blk: {
            conj.deinit(qa);
            break :blk filter;
        } else .{ .@"and" = try conj.toOwnedSlice(qa) };
        const fused = s.tryFuseFilter(fexpr) catch return NoMatch;
        if (!fused) return NoMatch;
    }
    snap.memtable_snap.release();
    pin_held = false;
    return .{ .sources = sources, .total_rows = total_rows, .iv_rows = iv_rows };
}

const Coord = struct { seg: usize, rg: usize };

fn flatToCoord(flat: usize, seg_start: []const usize, n_segs: usize) Coord {
    var s: usize = 0;
    while (s < n_segs and seg_start[s + 1] <= flat) s += 1;
    return .{ .seg = s, .rg = flat - seg_start[@min(s, n_segs)] };
}

fn orderKind(t: types.Type) !region.OrderKind {
    return switch (t) {
        .tinyint, .smallint, .int, .date => .int32,
        .bigint, .datetime => .int64,
        .varchar, .string, .char => .string,
        else => NoMatch,
    };
}

fn traceMark(name: []const u8, last: *i64) void {
    if (getenv("THINDB_REGION_TRACE") == null) return;
    const now = exec.prof.nowTicks();
    std.debug.print("[region] compile {s}={d:.1}ms\n", .{ name, exec.prof.ticksToMs(now - last.*) });
    last.* = now;
}

fn lastSegment(name: []const u8) []const u8 {
    if (std.mem.lastIndexOfScalar(u8, name, '.')) |d| return name[d + 1 ..];
    return name;
}

fn valueI64(v: Value) ?i64 {
    return switch (v) {
        .tinyint => |x| x,
        .smallint => |x| x,
        .int => |x| x,
        .bigint => |x| x,
        .date => |x| x,
        .datetime => |x| x,
        else => null,
    };
}

fn dupExpr(a: Allocator, e: Expr) ![]Expr {
    const s = try a.alloc(Expr, 1);
    s[0] = e;
    return s;
}

fn cloneArgs(b: *Builder, args: []const ?Value) ![]const ?Value {
    const out = try b.a.alloc(?Value, args.len);
    for (args, out) |src, *dst| {
        dst.* = if (src) |v| try b.cloneValue(v) else null;
    }
    return out;
}

/// Resolve the kernel's declared input-0 columns (first `count`) against
/// the current visible map.
fn tvfInputs(b: *Builder, ent: *const udf_mod.TableEntry, count: usize) ![]const usize {
    if (count > ent.input_schemas[0].len) return NoMatch;
    const inputs = try b.a.alloc(usize, count);
    for (ent.input_schemas[0][0..count], inputs) |col, *dst| {
        dst.* = (b.fb.resolve(col.name) orelse return NoMatch).idx;
    }
    return inputs;
}

const DateWindow = struct { col: usize, lo: i64, hi: i64 };

fn dateWindow(p: PredicateExpr, b: *Builder) !DateWindow {
    if (p != .@"and" or p.@"and".len != 2) return NoMatch;
    var col: ?usize = null;
    var lo: ?i64 = null;
    var hi: ?i64 = null;
    for (p.@"and") |k| {
        if (k != .leaf) return NoMatch;
        const l = k.leaf;
        const idx = (b.fb.resolve(l.col) orelse return NoMatch).idx;
        if (col != null and col.? != idx) return NoMatch;
        col = idx;
        const v = valueI64(l.val) orelse return NoMatch;
        switch (l.op) {
            .gte => lo = v,
            .lte => hi = v,
            else => return NoMatch,
        }
    }
    return .{ .col = col orelse return NoMatch, .lo = lo orelse return NoMatch, .hi = hi orelse return NoMatch };
}

/// Build a broadcast TvfPartition from a drained block, columns mapped to
/// the kernel's declared broadcast-input schema by name.
fn blockPartition(b: *Builder, want: []const Column, blk: DrainedBlock) !udf_mod.TvfPartition {
    const views = try b.a.alloc(ColumnView, want.len);
    for (want, views) |col, *v| {
        const ci = types.findColumn(blk.schema, col.name) orelse return NoMatch;
        if (!std.meta.eql(blk.schema[ci].type, col.type)) return NoMatch;
        v.* = blk.stores[ci].view();
    }
    return .{ .columns = views, .row_count = blk.rows, .keys = &.{} };
}

/// Row-aligned passthrough kernel: inputs = the declared kernel-visible
/// input columns; out = the non-passthrough (computed) outputs. Appends
/// the computed columns; passthrough outputs re-map to their frame sources
/// in the visible map.
fn pushAlignedTvf(
    b: *Builder,
    ent: *const udf_mod.TableEntry,
    extra_parts: []const udf_mod.TvfPartition,
    args: []const ?Value,
    per_range: bool,
) !void {
    const a = b.a;
    const kic: usize = if (ent.kernel_input_cols == 0) ent.input_schemas[0].len else ent.kernel_input_cols;
    const inputs = try tvfInputs(b, ent, kic);
    const pass_sources = try a.alloc(usize, ent.passthrough.len);
    errdefer a.free(pass_sources);
    for (ent.passthrough, pass_sources) |pp, *source| {
        if (pp.in_idx >= ent.input_schemas[0].len) return NoMatch;
        source.* = (b.fb.resolve(ent.input_schemas[0][pp.in_idx].name) orelse return NoMatch).idx;
    }

    var is_pass = try a.alloc(bool, ent.output_schema.len);
    @memset(is_pass, false);
    for (ent.passthrough) |pp| is_pass[pp.out_idx] = true;

    var n_out: usize = 0;
    for (is_pass) |x| {
        if (!x) n_out += 1;
    }
    if (n_out == 0) return NoMatch;
    const out = try a.alloc(Column, n_out);
    var oi: usize = 0;
    for (ent.output_schema, is_pass) |col, pass| {
        if (pass) continue;
        out[oi] = .{ .name = try b.fb.canonName(col.name), .type = col.type, .nullable = true };
        oi += 1;
    }

    try b.flushPending();
    const base = b.fb.cols.items.len;
    if (per_range) {
        try b.ops.append(a, .{ .tvf_grouped = .{
            .spec = .{
                .process = ent.process,
                .user_data = ent.user_data,
                .args = try cloneArgs(b, args),
                .inputs = inputs,
                .extra_parts = extra_parts,
                .out = out,
            },
            .aligned_append = true,
        } });
    } else {
        try b.ops.append(a, .{ .tvf_aligned = .{
            .process = ent.process,
            .user_data = ent.user_data,
            .args = try cloneArgs(b, args),
            .inputs = inputs,
            .extra_parts = extra_parts,
            .out = out,
        } });
    }

    oi = 0;
    for (ent.output_schema, is_pass) |col, pass| {
        if (pass) continue;
        const idx = base + oi;
        try b.fb.cols.append(a, out[oi]);
        try b.fb.setVis(col.name, idx);
        oi += 1;
    }
    var views: std.ArrayListUnmanaged(region.ViewCol) = .empty;
    for (ent.passthrough, pass_sources) |pp, source| {
        const declared = ent.output_schema[pp.out_idx];
        const idx = if (std.meta.eql(b.fb.cols.items[source].type, declared.type)) source else blk: {
            const idx = try b.fb.addCol(declared.name, declared.type, declared.nullable);
            try views.append(a, .{ .src = source, .column = b.fb.cols.items[idx] });
            if (b.isConstIdx(source)) try b.const_idxs.append(a, idx);
            break :blk idx;
        };
        if (std.mem.eql(u8, b.fb.cols.items[source].name, b.route_name)) b.route_name = b.fb.cols.items[idx].name;
        try b.fb.setVis(declared.name, idx);
    }
    if (views.items.len != 0) try b.ops.append(a, .{ .view_cols = views.items });
}

/// Keyed aggregation: group cols must cover every `required` frame column
/// (the span identity — the range keys, or (project, customer) when the
/// span merges adjacent ranges via `merge_on`); the rest become int-family
/// subkeys. The region emits sub-groups subkey-ascending per span, which is
/// exactly the month order downstream ops require.
/// GROUP BY dispatch: group cols covering all range keys aggregate per
/// range (extras become subkeys); a grouping strictly coarser aggregates
/// over merged spans (one non-constant range key carries the merge).
fn pushGroupAggAuto(b: *Builder, g: *const ir.Op.GroupBy) anyerror!void {
    var buf: [8]usize = undefined;
    const keys = try b.rangeKeyIdxs(&buf);
    // Per-range aggregation is only valid when the grouping still
    // distinguishes every range. Three key states:
    //   pinned  — one literal everywhere; never splits, ignore.
    //   degraded — the column the NAME now resolves to is a folded
    //              constant, but the RANGES still split by the original
    //              values (the -2 cross-division literal case). Grouping by
    //              it is COARSER than the ranges → merge path required.
    //   live    — must appear among the group columns for per-range.
    var degraded = false;
    var live_uncovered = false;
    for (b.range_key_names, keys) |n, k| {
        if (b.pinnedName(n) != null) continue;
        if (b.isConstIdx(k)) {
            degraded = true;
            continue;
        }
        var covered = false;
        for (g.group_cols) |gc| {
            const e = b.fb.resolve(gc) orelse return NoMatch;
            if (e.idx == k) {
                covered = true;
                break;
            }
        }
        if (!covered) live_uncovered = true;
    }
    if (!degraded and !live_uncovered) {
        return pushGroupAgg(b, g, keys, null);
    }
    var part: std.ArrayListUnmanaged(usize) = .empty;
    var merge: ?usize = null;
    for (g.group_cols) |gc| {
        const idx = (b.fb.resolve(gc) orelse return NoMatch).idx;
        if (std.mem.indexOfScalar(usize, keys, idx) == null) continue;
        try part.append(b.a, idx);
        if (b.pinnedName(gc) == null and !b.isConstIdx(idx)) {
            if (merge != null) return NoMatch; // one merge column (runtime limit)
            merge = idx;
        }
    }
    if (part.items.len == 0) return NoMatch;
    return pushGroupAgg(b, g, part.items, merge orelse return NoMatch);
}

fn pushGroupAgg(b: *Builder, g: *const ir.Op.GroupBy, required: []const usize, merge_on: ?usize) !void {
    const a = b.a;
    var subkeys: std.ArrayListUnmanaged(usize) = .empty;
    const covered = try a.alloc(bool, required.len);
    @memset(covered, false);
    for (g.group_cols) |gc| {
        const idx = (b.fb.resolve(gc) orelse return NoMatch).idx;
        var is_span_key = false;
        for (required, 0..) |k, i| {
            if (k == idx) {
                covered[i] = true;
                is_span_key = true;
                break;
            }
        }
        if (!is_span_key) {
            var is_const = false;
            for (b.const_idxs.items) |ci| {
                if (ci == idx) {
                    is_const = true;
                    break;
                }
            }
            if (!is_const) try subkeys.append(a, idx);
        }
    }
    for (covered) |c| {
        if (!c) return NoMatch;
    }
    if (subkeys.items.len > 3) {
        if (getenv("THINDB_REGION_TRACE") != null) {
            std.debug.print("[region] pushGroupAgg decline: subkeys={d} group_cols={d} required={d}\n", .{ subkeys.items.len, g.group_cols.len, required.len });
            for (g.group_cols) |gc| {
                const e = b.fb.resolve(gc);
                std.debug.print("[region]   gc '{s}' idx={?d} const={}\n", .{ gc, if (e) |ee| ee.idx else null, if (e) |ee| b.isConstIdx(ee.idx) else false });
            }
        }
        return NoMatch;
    }

    var out: std.ArrayListUnmanaged(region.AggOut) = .empty;
    var new_vis: std.ArrayListUnmanaged(VisEntry) = .empty;
    var new_consts: std.ArrayListUnmanaged(usize) = .empty;
    var new_pinned: std.ArrayListUnmanaged(PinnedCol) = .empty;
    var route_name: ?[]const u8 = null;

    // Group keys first (constant within their sub-group → .first).
    for (g.group_cols) |gc| {
        const e = b.fb.resolve(gc) orelse return NoMatch;
        const name = try nameFor(b, gc);
        if (std.mem.eql(u8, b.fb.cols.items[e.idx].name, b.route_name)) route_name = name;
        if (b.isConstIdx(e.idx)) try new_consts.append(a, out.items.len);
        if (b.pinnedName(gc)) |value| try new_pinned.append(a, .{ .name = gc, .val = value });
        try out.append(a, .{ .name = name, .kind = .{ .first = e.idx } });
        try new_vis.append(a, .{ .name = try a.dupe(u8, gc), .idx = new_vis.items.len });
    }
    const next_route = route_name orelse return NoMatch;
    for (g.aggs) |spec| {
        const kind: @FieldType(region.AggOut, "kind") = switch (spec.func) {
            .any_value => .{ .first = try b.resolveIdx(spec.col orelse return NoMatch) },
            .max_by => .{ .max_by = .{
                .val = try b.resolveIdx(spec.col orelse return NoMatch),
                .ord = try b.resolveIdx(spec.arg2_col orelse return NoMatch),
            } },
            .max => blk: {
                const idx = try b.resolveIdx(spec.col orelse return NoMatch);
                break :blk switch (b.fb.cols.items[idx].type) {
                    .varchar, .string, .char => .{ .max_str = idx },
                    else => .{ .max_int = try intFamilyIdx(b, spec.col orelse return NoMatch) },
                };
            },
            .min => .{ .min_int = try intFamilyIdx(b, spec.col orelse return NoMatch) },
            .sum => blk: {
                const idx = try b.resolveIdx(spec.col orelse return NoMatch);
                const t = b.fb.cols.items[idx].type;
                break :blk switch (t) {
                    .tinyint, .smallint, .int, .date, .datetime => .{ .sum_int = idx },
                    .bigint, .largeint => .{ .sum_large = idx },
                    .float, .double => .{ .sum_float = idx },
                    else => return NoMatch,
                };
            },
            else => return NoMatch,
        };
        try out.append(a, .{ .name = try nameFor(b, spec.as), .kind = kind });
        try new_vis.append(a, .{ .name = try a.dupe(u8, spec.as), .idx = new_vis.items.len });
    }

    try b.flushPending();
    try b.ops.append(a, .{ .group_agg = .{
        .subkeys = try a.dupe(usize, subkeys.items),
        .out = try a.dupe(region.AggOut, out.items),
        .merge_on = merge_on,
    } });

    // Frame replaced by the aggregation output. Types are re-derived by
    // Program.build; our copy mirrors names only (placeholder types for
    // .first/.max_by columns whose sources we know).
    const in_cols = b.fb.cols.items;
    var new_cols: std.ArrayListUnmanaged(Column) = .empty;
    for (out.items) |o| {
        const t: types.Type = switch (o.kind) {
            .first => |c| in_cols[c].type,
            .max_by => |mb| in_cols[mb.val].type,
            .min_int, .max_int, .max_str => |c| in_cols[c].type,
            .sum_int => .bigint,
            .sum_large => .largeint,
            .sum_float => .double,
        };
        try new_cols.append(a, .{ .name = o.name, .type = t, .nullable = true });
    }
    b.fb.cols = new_cols;
    b.fb.vis = new_vis;
    b.route_name = next_route;
    b.const_idxs = new_consts;
    b.pinned = new_pinned;
}

fn nameFor(b: *Builder, hint: []const u8) ![]const u8 {
    return b.fb.canonName(hint);
}

fn intFamilyIdx(b: *Builder, name: []const u8) !usize {
    const idx = try b.resolveIdx(name);
    return switch (b.fb.cols.items[idx].type) {
        .tinyint, .smallint, .int, .bigint, .date, .datetime => idx,
        else => NoMatch,
    };
}

/// Downstream per-range order requirement: single ascending key that is the
/// aggregation's month subkey (the region emits sub-groups month-ascending,
/// so the order already holds — this just verifies the SQL asked for it).
fn checkRangeOrder(b: *Builder, specs: []const ir.SortSpec) !void {
    if (specs.len == 0) return;
    if (specs.len != 1 or specs[0].desc) return NoMatch;
    _ = try b.resolveIdx(specs[0].col);
}

fn kernelReadsAll(ent: *const udf_mod.TableEntry) bool {
    return ent.kernel_input_cols == 0 or ent.kernel_input_cols == ent.input_schemas[0].len;
}

fn rightAliasName(op: *const ir.Op) ![]const u8 {
    var cur = op;
    var depth: usize = 0;
    while (depth < 8) : (depth += 1) {
        switch (cur.*) {
            .alias => |al| return al.alias,
            .compute => |c| cur = c.upstream,
            .select => |p| cur = p.upstream,
            else => return NoMatch,
        }
    }
    return NoMatch;
}

fn joinKeyTouchesAlias(j: *const ir.Op.Join, alias: []const u8) bool {
    for (j.on) |pair| {
        if (std.mem.indexOfScalar(u8, pair.left, '.')) |d| {
            if (std.ascii.eqlIgnoreCase(pair.left[0..d], alias)) return true;
        }
    }
    return false;
}
