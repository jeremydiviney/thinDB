//! Keyed pipeline regions: the compiler for `WITH KEYED BY (...)` blocks
//! (tasks #184/#185, REGION_PLAN.md §7).
//!
//! The declaration is a hard contract: every GROUP BY / window PARTITION BY
//! / TVF PARTITION BY in the block contains the declared keys (statically
//! verified — violations are compile errors, never silent fallback). A
//! verified block compiles into ONE region program: exchange-scatter the
//! base scan by a declared key's hash, then run the whole chain
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

const Column = types.Column;
const Value = types.Value;
const ColumnStore = @import("../engine/store.zig").ColumnStore;
const ColumnView = @import("../storage/storage.zig").ColumnView;
const Expr = expr_mod.Expr;
const PredicateExpr = predicate_mod.PredicateExpr;
const Derived = compute_mod.Derived;
const Scan = exec.Scan;

const StageMap = std.AutoHashMapUnmanaged(*const ir.Op, *mat_stage.Stage);

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

const NoMatch = error.RegionNoMatch;

pub const Recognized = struct {
    anchor: *const ir.Op,
    query: exec.Query,
};
// ---------------------------------------------------------------------------
// Declared regions: `WITH KEYED BY (k1, ...)`. The declaration is a HARD
// contract — the block must verify (every GROUP BY / window PARTITION BY /
// TVF PARTITION BY along the pipeline contains the declared keys) and must
// compile as a region; violations and unsupported constructs are compile
// errors, never a silent fall-back to the staged path. Join right sides and
// secondary TVF inputs are exempt from the key check: they are broadcast /
// empty-proof candidates the region compiler validates by executing them.
// ---------------------------------------------------------------------------

/// Entry for the declared path (no env gate). Returns null when the query
/// declares no keyed block; errors when it declares one that can't run.
pub fn compileDeclared(input: engine_v2.CompileInput, root: *const ir.Op) anyerror!?Recognized {
    // Find the topmost CTE boundary carrying declared keys.
    var cur = root;
    var depth: usize = 0;
    const top: *const ir.Op = blk: {
        while (depth < 16) : (depth += 1) {
            switch (cur.*) {
                .materialize => |m| {
                    if (m.region_keys != null) break :blk cur;
                    cur = m.upstream;
                },
                .select => |p| cur = p.upstream,
                .exclude => |p| cur = p.upstream,
                .filter => |f| cur = f.upstream,
                .group_by => |g| cur = g.upstream,
                .compute => |c| cur = c.upstream,
                .limit => |l| cur = l.upstream,
                .order_by => |o| cur = o.upstream,
                else => return null,
            }
        }
        return null;
    };
    const keys = top.materialize.region_keys.?;

    try verifyKeyContract(top, keys, 0);

    // Compile: try the marked boundary, then boundaries below it — the
    // program anchor can sit under the outermost CTE (e.g. when the final
    // CTE is a plain projection the staged path composes above the region).
    cur = top;
    depth = 0;
    while (depth < 16) : (depth += 1) {
        switch (cur.*) {
            .materialize => |m| {
                if (tryCachedAt(input, cur)) |q| {
                    return .{ .anchor = cur, .query = q };
                }
                if (buildRegion(input, cur, keys)) |q| {
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
                cur = m.upstream;
            },
            .select => |p| cur = p.upstream,
            .exclude => |p| cur = p.upstream,
            .filter => |f| cur = f.upstream,
            .group_by => |g| cur = g.upstream,
            .compute => |c| cur = c.upstream,
            .limit => |l| cur = l.upstream,
            .order_by => |o| cur = o.upstream,
            else => break,
        }
    }
    std.debug.print("[region] KEYED BY block did not compile: no boundary matches a supported region shape\n", .{});
    return error.RegionUnsupportedConstruct;
}

/// Static half of the contract: walk the block's pipeline (left/primary
/// spine) and require every partition-defining construct to contain every
/// declared key. Stages may partition FINER (extra columns), never coarser —
/// that is what guarantees no row ever needs another shard's data.
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
                try requireKeys(keys, g.group_cols, "GROUP BY");
                cur = g.upstream;
            },
            .window => |w| {
                for (w.specs) |spec| try requireKeys(keys, spec.partition_by, "window PARTITION BY");
                cur = w.upstream;
            },
            .table_fn => |t| {
                if (t.partition_by.len > 0) try requireKeys(keys, t.partition_by, "TABLE(...) PARTITION BY");
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

fn requireKeys(keys: []const []const u8, cols: []const []const u8, what: []const u8) !void {
    outer: for (keys) |k| {
        for (cols) |c| {
            if (std.ascii.eqlIgnoreCase(lastSegment(c), lastSegment(k))) continue :outer;
        }
        std.debug.print("[region] KEYED BY contract violation: a {s} does not include declared key '{s}' (partitions on:", .{ what, k });
        for (cols) |c| std.debug.print(" {s}", .{c});
        std.debug.print(")\n", .{});
        return error.RegionKeyContractViolation;
    }
}

// ---------------------------------------------------------------------------
// Cross-run region cache — the probe's pooled-buffer discipline in engine
// form. One entry per Database, keyed by a deterministic deep hash of the
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
    hash: u64 = 0,
    ctx: ?*Ctx = null,
    /// Checked out by a running query (the RegionExecOp releases on deinit).
    /// While busy the entry can be neither reused nor replaced — a second
    /// concurrent identical query just runs one-shot.
    busy: bool = false,

    fn deinitErased(p: *anyopaque) void {
        const self: *Cache = @ptrCast(@alignCast(p));
        const alloc = self.alloc;
        if (self.ctx) |c| Ctx.destroyErased(c);
        alloc.destroy(self);
    }

    fn releaseErased(p: *anyopaque) void {
        const self: *Cache = @ptrCast(@alignCast(p));
        self.mu.lock();
        self.busy = false;
        self.mu.unlock();
    }
};

/// Get-or-create the per-database cache slot. Uses the DATABASE allocator —
/// the cache must outlive any single query or connection.
fn cacheFor(db: anytype) ?*Cache {
    db.region_cache_lock.lock();
    defer db.region_cache_lock.unlock();
    if (db.region_cache) |p| return @ptrCast(@alignCast(p));
    const c = db.allocator.create(Cache) catch return null;
    c.* = .{ .alloc = db.allocator };
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
fn tableVersionOf(db: anytype, name: []const u8) ?u64 {
    const t = db.openTable(name, .{}) catch return null;
    t.mutex.lockUncancelable(t.io);
    defer t.mutex.unlock(t.io);
    var h = std.hash.Wyhash.init(0x7461626c65);
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
    const ver = tableVersionOf(b.input.db, name) orelse return Unhashable;
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
        const now = tableVersionOf(input.db, v.name) orelse return false;
        if (now != v.version) return false;
    }
    return true;
}

fn tryCachedAt(input: engine_v2.CompileInput, anchor: *const ir.Op) ?exec.Query {
    const hash = hashAnchor(anchor) orelse return null;
    const cache = cacheFor(input.db) orelse return null;

    cache.mu.lock();
    if (cache.busy or cache.ctx == null or cache.hash != hash) {
        cache.mu.unlock();
        return null;
    }
    cache.busy = true;
    cache.mu.unlock();

    const ctx = cache.ctx.?;
    if (!cacheValid(input, ctx)) {
        cache.mu.lock();
        cache.ctx = null;
        cache.busy = false;
        cache.mu.unlock();
        Ctx.destroyErased(ctx);
        if (getenv("THINDB_REGION_TRACE") != null) {
            std.debug.print("[region] cache invalidated (data changed)\n", .{});
        }
        return null;
    }

    const q = runCached(input, anchor, ctx) catch |e| {
        Cache.releaseErased(cache);
        if (e != error.OutOfMemory and getenv("THINDB_REGION_TRACE") != null) {
            std.debug.print("[region] cache hit declined: {s}\n", .{@errorName(e)});
        }
        return null;
    };
    const op = exec.queryAs(region.RegionExecOp, q) orelse {
        var qq = q;
        qq.deinit();
        Cache.releaseErased(cache);
        return null;
    };
    op.setOwnedCtx(cache, Cache.releaseErased);
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
    const pl = try collectPipeline(input.node_arena, anchor);

    var prune_leaves: std.ArrayListUnmanaged(predicate_mod.Predicate) = .empty;
    defer prune_leaves.deinit(qa);
    try collectAndLeaves(qa, pl.entry_filter, &prune_leaves);

    var scan_cols: std.ArrayListUnmanaged([]const u8) = .empty;
    defer scan_cols.deinit(qa);
    var scan_cols_opt: ?[]const []const u8 = null;
    if (pl.entry_sel) |sel| {
        outer: for (sel.columns) |col| {
            for (pl.entry_derived) |d| {
                if (std.ascii.eqlIgnoreCase(d.name, col)) continue :outer;
            }
            try scan_cols.append(qa, col);
        }
        scan_cols_opt = scan_cols.items;
    }

    const table = input.db.openTable(pl.scan.table.name, .{}) catch return NoMatch;
    const n_threads = @max(input.effectiveDop(), 1);
    const bs = try buildScanSources(input, table, prune_leaves.items, pl.entry_filter, scan_cols_opt, n_threads);
    var sources_owned = true;
    errdefer if (sources_owned) {
        for (bs.sources) |*s| s.deinit();
        qa.free(bs.sources);
    };

    const scan_schema = bs.sources[0].outputSchema();
    const want = ctx.entry_schema;
    if (scan_schema.len + pl.entry_derived.len != want.len) return NoMatch;
    const rowloc_entry = scan_schema.len - 1;
    for (scan_schema, want[0..scan_schema.len], 0..) |src, w, i| {
        if (!std.ascii.eqlIgnoreCase(src.name, w.name)) return NoMatch;
        if (!std.meta.eql(src.type, w.type)) return NoMatch;
        const want_nullable = if (i == rowloc_entry) true else src.nullable;
        if (w.nullable != want_nullable) return NoMatch;
    }
    for (pl.entry_derived, want[scan_schema.len..]) |d, w| {
        if (!std.ascii.eqlIgnoreCase(d.name, w.name)) return NoMatch;
    }

    var opts = ctx.opts;
    opts.n_threads = n_threads;

    const q = try region.RegionExecOp.create(
        qa,
        ctx.entry_schema,
        bs.sources,
        pl.entry_derived,
        &ctx.prog,
        opts,
        &ctx.pool,
        bs.total_rows * 2,
    );
    sources_owned = false;
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
    versions: std.ArrayListUnmanaged(TableVersion) = .empty,
    kernels: std.ArrayListUnmanaged(KernelCheck) = .empty,
    uncacheable: bool = false,

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

const Builder = struct {
    input: engine_v2.CompileInput,
    ctx: *Ctx,
    a: Allocator, // ctx arena allocator
    fb: FrameB,
    ops: std.ArrayListUnmanaged(region.RegionOp) = .empty,
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
        for (derived, cloned) |src, *dst| {
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
                try b.fb.setVis(src.name, idx);
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
            try b.fb.setVis(derived[i].name, idx);
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
        var new_vis: std.ArrayListUnmanaged(VisEntry) = .empty;
        for (p.columns, 0..) |col, i| {
            if (b.fb.resolve(col) == null) _ = try b.tryNullAppend(col);
            const e = b.fb.resolve(col) orelse return NoMatch;
            var out_name: []const u8 = e.name;
            if (p.outputs) |outs| {
                if (i < outs.len) {
                    if (outs[i]) |o| out_name = o;
                }
            }
            try new_vis.append(b.a, .{ .name = try b.a.dupe(u8, out_name), .idx = e.idx });
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
    var local_map: StageMap = .empty;
    defer local_map.deinit(b.input.allocator);
    var q = cte_stages.compileBlock(b.input, node, &local_map) catch return NoMatch;
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
    for (0..n) |i| {
        if (!v.isValid(i)) {
            try store.appendNulls(a, 1);
            continue;
        }
        switch (v.data) {
            .tinyint => |s| try store.data.tinyint.append(a, s[i]),
            .smallint => |s| try store.data.smallint.append(a, s[i]),
            .int => |s| try store.data.int.append(a, s[i]),
            .bigint => |s| try store.data.bigint.append(a, s[i]),
            .date => |s| try store.data.date.append(a, s[i]),
            .datetime => |s| try store.data.datetime.append(a, s[i]),
            .float => |s| try store.data.float.append(a, s[i]),
            .double => |s| try store.data.double.append(a, s[i]),
            .varchar, .string, .char, .json => |s| switch (store.data) {
                .varchar, .string, .char, .json => |*d| try d.appendValue(a, s.rowBytes(i)),
                else => return NoMatch,
            },
            else => return NoMatch,
        }
        if (store.nulls != null) try store.appendValidBit(a, store.rowCount() - 1, true);
    }
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
    scan: *const ir.Op.Scan,
};

fn collectPipeline(a: Allocator, anchor: *const ir.Op) !Pipeline {
    if (anchor.* != .materialize) return NoMatch;
    var steps: std.ArrayListUnmanaged(Step) = .empty;
    var cur: *const ir.Op = anchor.materialize.upstream;
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
                try steps.append(a, .{ .group_by = g });
                cur = g.upstream;
            },
            .window => |*w| {
                try steps.append(a, .{ .window = w });
                cur = w.upstream;
            },
            .join => |*j| {
                try steps.append(a, .{ .join = j });
                cur = j.left;
            },
            .table_fn => |*t| {
                if (t.inputs.len == 0) return NoMatch;
                try steps.append(a, .{ .table_fn = t });
                cur = t.inputs[0];
            },
            .set_union => |*u| {
                const arm = unionTvfArm(u) orelse return NoMatch;
                try steps.append(a, .{ .union_tvf = arm.ut });
                cur = arm.base;
            },
            .scan => |*s| break :blk s,
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
    if (filters.items.len == 0) return NoMatch; // an unfiltered full scan is never a region win
    const entry_filter: PredicateExpr = if (filters.items.len == 1)
        filters.items[0]
    else
        .{ .@"and" = filters.items };

    return .{
        .steps = steps.items[0..split],
        .entry_sel = entry_sel,
        .entry_derived = entry_derived.items,
        .entry_filter = entry_filter,
        .scan = scan,
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

fn buildRegion(input: engine_v2.CompileInput, anchor: *const ir.Op, declared_keys: []const []const u8) anyerror!exec.Query {
    var tm: i64 = exec.prof.nowTicks();
    const registry = input.udf_registry orelse return NoMatch;

    // A cacheable build uses the DATABASE allocator for its ctx so the
    // entry can outlive this query/connection; otherwise query-lifetime.
    const anchor_hash = hashAnchor(anchor);
    const cache: ?*Cache = if (anchor_hash != null) cacheFor(input.db) else null;
    const gpa = if (cache != null) input.db.allocator else input.allocator;
    const qa = input.allocator;
    const ctx = try gpa.create(Ctx);
    ctx.* = .{
        .gpa = gpa,
        .arena = std.heap.ArenaAllocator.init(gpa),
        .pool = region.RegionPool.init(gpa, poolCapBytes()),
    };
    errdefer Ctx.destroyErased(ctx);
    const a = ctx.arena.allocator();
    if (cache == null) ctx.uncacheable = true;

    var b = Builder{ .input = input, .ctx = ctx, .a = a, .fb = .{ .a = a } };

    // Query-lifetime arena: the step list and the entry-derived slice are
    // borrowed by the operator (never by the cached ctx).
    const pl = try collectPipeline(input.node_arena, anchor);
    traceMark("walk", &tm);

    // ---- entry: prune leaves + literal-pinned columns --------------------
    var prune_leaves: std.ArrayListUnmanaged(predicate_mod.Predicate) = .empty;
    try collectAndLeaves(a, pl.entry_filter, &prune_leaves);

    const table = input.db.openTable(pl.scan.table.name, .{}) catch return NoMatch;

    var scan_cols: std.ArrayListUnmanaged([]const u8) = .empty;
    var scan_cols_opt: ?[]const []const u8 = null;
    if (pl.entry_sel) |sel| {
        outer: for (sel.columns) |col| {
            for (pl.entry_derived) |d| {
                if (std.ascii.eqlIgnoreCase(d.name, col)) continue :outer;
            }
            try scan_cols.append(a, col);
        }
        scan_cols_opt = scan_cols.items;
    }

    const dop = input.effectiveDop();
    const n_threads = @max(dop, 1);
    const n_shards: usize = blk: {
        if (getenv("THINDB_REGION_SHARDS")) |v| {
            break :blk std.fmt.parseInt(usize, std.mem.span(v), 10) catch 64;
        }
        break :blk 64;
    };

    const bs = try buildScanSources(input, table, prune_leaves.items, pl.entry_filter, scan_cols_opt, n_threads);
    const sources = bs.sources;
    const total_rows = bs.total_rows;
    var sources_owned = true; // RegionExecOp takes them over on create
    errdefer if (sources_owned) {
        for (sources) |*q| q.deinit();
        qa.free(sources);
    };
    traceMark("scan_build", &tm);

    // ---- entry schema = scan output ++ entry-computed columns ------------
    const scan_schema = sources[0].outputSchema();
    const entry_schema = try a.alloc(Column, scan_schema.len + pl.entry_derived.len);
    for (scan_schema, entry_schema[0..scan_schema.len]) |src, *dst| {
        dst.* = src;
        dst.name = try a.dupe(u8, src.name);
    }
    // The __rowloc tie-break column: nullable so kernel-generated rows
    // (which have no physical location) NULL-pad it.
    const rowloc_entry = scan_schema.len - 1;
    entry_schema[rowloc_entry].nullable = true;
    if (pl.entry_derived.len > 0) {
        // Engine-exact types for the scatter-time computes; forced nullable
        // (kernel-appended rows NULL-pad every entry-derived column).
        const typed = region.computeOutputSchema(qa, a, scan_schema, pl.entry_derived, registry) catch return NoMatch;
        if (typed.len != scan_schema.len + pl.entry_derived.len) return NoMatch;
        for (pl.entry_derived, typed[scan_schema.len..], entry_schema[scan_schema.len..]) |d, t, *dst| {
            dst.* = .{ .name = try a.dupe(u8, d.name), .type = t.type, .nullable = true };
        }
    }
    for (entry_schema, 0..) |col, i| {
        _ = try b.fb.addColNamed(col.name, col.type, col.nullable);
        try b.fb.setVis(col.name, i);
    }
    ctx.entry_schema = entry_schema;

    // Literal-pinned entry columns: an eq-literal conjunct in the scan
    // filter makes the column a per-run constant — groupings and span
    // merges may skip it, and probe key pairs against it eliminate.
    for (prune_leaves.items) |l| {
        if (l.op != .eq) continue;
        if (b.fb.resolve(l.col) == null) continue;
        try b.pinned.append(a, .{ .name = try a.dupe(u8, l.col), .val = l.val });
    }

    // ---- range/order contract from the bottom-most partitioned step ------
    var range_names: []const []const u8 = declared_keys;
    var order_specs: []const ir.SortSpec = &.{};
    {
        var i = pl.steps.len;
        found: while (i > 0) : (i -= 1) {
            switch (pl.steps[i - 1]) {
                .union_tvf => |u| {
                    if (i != pl.steps.len) return NoMatch; // fused tail must be bottom-most
                    range_names = u.tvf.partition_by;
                    order_specs = u.tvf.order_by;
                    break :found;
                },
                .table_fn => |t| {
                    if (t.partition_by.len > 0) {
                        range_names = t.partition_by;
                        order_specs = t.order_by;
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
        dst.* = (b.fb.resolve(n) orelse return NoMatch).idx;
        nm.* = try a.dupe(u8, n);
    }
    b.range_key_names = key_names_owned;
    // Routing: any single declared key suffices (every partition in the
    // block contains all of them — the verifier's guarantee), and it must
    // itself be a range key.
    const route_idx = (b.fb.resolve(declared_keys[0]) orelse return NoMatch).idx;
    if (std.mem.indexOfScalar(usize, range_keys, route_idx) == null) return NoMatch;

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
    try sort_list.append(a, .{ .col = rowloc_entry, .kind = .int64 });
    const sort_cols = sort_list.items;

    if (pl.entry_sel) |sel| try b.applySelect(sel);

    // ---- dispatch the pipeline bottom-up ---------------------------------
    {
        var i = pl.steps.len;
        while (i > 0) : (i -= 1) {
            try dispatchStep(&b, registry, pl.steps[i - 1], pl.steps[0 .. i - 1]);
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
    ctx.prog = region.Program.build(gpa, entry_schema, b.ops.items, registry) catch return NoMatch;
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

    const opts = region.DriverOpts{
        .n_threads = n_threads,
        .n_shards = n_shards,
        .key_col = route_idx,
        .sort_cols = sort_cols,
        .group_prefix = group_prefix,
    };
    ctx.opts = opts;

    var q = try region.RegionExecOp.create(
        qa,
        entry_schema,
        sources,
        pl.entry_derived,
        &ctx.prog,
        opts,
        &ctx.pool,
        total_rows * 2,
    );
    sources_owned = false; // the query owns sources (and, below, the ctx)
    const op = exec.queryAs(region.RegionExecOp, q) orelse {
        q.deinit();
        return NoMatch;
    };

    // Publish into the per-database cache when possible: the cache then owns
    // the ctx (program + pool) and the op only releases the busy pin at
    // query teardown. Otherwise the op owns the ctx (one-shot).
    if (cache) |c| blk: {
        if (ctx.uncacheable) break :blk;
        c.mu.lock();
        if (c.busy) {
            c.mu.unlock();
            break :blk;
        }
        const old = c.ctx;
        c.ctx = ctx;
        c.hash = anchor_hash.?;
        c.busy = true;
        c.mu.unlock();
        if (old) |o| Ctx.destroyErased(o);
        op.setOwnedCtx(c, Cache.releaseErased);
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
                try pushReplaceTvf(b, ent, t.args);
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
fn pushReplaceTvf(b: *Builder, ent: *const udf_mod.TableEntry, args: []const ?Value) !void {
    const a = b.a;
    const inputs = try tvfInputs(b, ent, ent.input_schemas[0].len);
    const out = try a.alloc(Column, ent.output_schema.len);
    for (ent.output_schema, out) |src, *dst| {
        dst.* = .{ .name = try b.fb.canonName(src.name), .type = src.type, .nullable = true };
    }
    try b.flushPending();
    try b.ops.append(a, .{ .tvf_grouped = .{ .spec = .{
        .process = ent.process,
        .user_data = ent.user_data,
        .args = try cloneArgs(b, args),
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
    // Range keys re-resolve by NAME against the new frame (the kernel keeps
    // partition-column names — SDK schema contract); verify they survive.
    var buf: [8]usize = undefined;
    _ = try b.rangeKeyIdxs(&buf);
    // Stale per-frame bookkeeping: forgotten constants become real subkeys
    // (correct, at worst a hair slower).
    b.const_idxs.clearRetainingCapacity();
}

fn dispatchWindow(b: *Builder, w: *const ir.WindowOp) anyerror!void {
    var all_ranks = true;
    var all_fill = true;
    for (w.calls) |call| {
        if (call.func != .row_number) all_ranks = false;
        if (call.func != .last_value) all_fill = false;
    }
    if (all_ranks) return pushRanksFromWindow(b, w);
    if (all_fill) return pushFillLast(b, w);
    return NoMatch;
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
            const blk = try compileAndDrain(b, j.right, true);
            if (blk.rows == 0) {
                const alias = ralias orelse return NoMatch;
                try b.null_sides.append(b.a, .{ .alias = alias, .schema = blk.schema });
                return;
            }
            if (blk.rows > (1 << 20)) return NoMatch;
            try pushProbe(b, j, ralias, blk, false, try liveNamesAbove(b.input.node_arena, above));
        },
        .inner => {
            const blk = try compileAndDrain(b, j.right, true);
            if (blk.rows > (1 << 20)) return NoMatch;
            try pushProbe(b, j, rightAliasName(j.right) catch null, blk, true, try liveNamesAbove(b.input.node_arena, above));
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
        const rci = types.findColumn(blk.schema, pair.right) orelse return NoMatch;
        key_right[rci] = true;
        if (b.pinnedName(pair.left)) |v| {
            try pin_right.append(a, rci);
            try pin_vals.append(a, v);
        } else {
            try live_left.append(a, pair.left);
            try live_right.append(a, rci);
        }
    }
    if (live_left.items.len == 0 or live_left.items.len > 2) return NoMatch;

    // Build the map (and kept-row list) over rows matching every pinned
    // literal, with NULL keys skipped (SQL join semantics).
    const map = try a.create(region.KeyMap);
    map.* = .empty;
    var kept: std.ArrayListUnmanaged(u32) = .empty;
    rows: for (0..blk.rows) |i| {
        for (pin_right.items, pin_vals.items) |rci, want| {
            const rv = blk.stores[rci].view();
            const got = i64At(rv, i) orelse continue :rows;
            const want_i = valueI64(want) orelse return NoMatch;
            if (got != want_i) continue :rows;
        }
        var key: i64 = 0;
        for (live_right.items) |rci| {
            const kv = i64At(blk.stores[rci].view(), i) orelse continue :rows;
            key = key * 4294967296 + kv;
        }
        const gop = try map.getOrPut(a, key);
        if (gop.found_existing) {
            if (!inner) return NoMatch; // dup key changes LEFT row counts
            continue :rows;
        }
        gop.value_ptr.* = @intCast(kept.items.len);
        try kept.append(a, @intCast(i));
    }

    // Probe key: single live key probes directly; two pack as k1*2^32+k2.
    var probe_idx: usize = undefined;
    if (live_left.items.len == 1) {
        probe_idx = try b.resolveIdx(live_left.items[0]);
    } else {
        const hi_idx = try b.resolveIdx(live_left.items[0]);
        const lo_idx = try b.resolveIdx(live_left.items[1]);
        const mul_args = try a.alloc(Expr, 2);
        mul_args[0] = .{ .call = .{ .fn_name = "to_bigint", .args = try dupExpr(a, .{ .col_ref = b.fb.cols.items[hi_idx].name }) } };
        mul_args[1] = .{ .lit = .{ .bigint = 4294967296 } };
        const add_args = try a.alloc(Expr, 2);
        add_args[0] = .{ .call = .{ .fn_name = "mul", .args = mul_args } };
        add_args[1] = .{ .call = .{ .fn_name = "to_bigint", .args = try dupExpr(a, .{ .col_ref = b.fb.cols.items[lo_idx].name }) } };
        probe_idx = try b.fb.addCol("probe_key", .bigint, true);
        const key_derived = try a.alloc(Derived, 1);
        key_derived[0] = .{ .name = b.fb.cols.items[probe_idx].name, .expr = .{ .call = .{ .fn_name = "add", .args = add_args } } };
        try b.flushPending();
        try b.ops.append(a, .{ .compute = .{ .derived = key_derived } });
    }

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
};

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
        built += 1;
        const start = flatToCoord(lo, seg_start, snap.segment_count);
        const end = flatToCoord(hi, seg_start, snap.segment_count);
        s.setRange(start.seg, start.rg, end.seg, end.rg, i == n_chunks - 1);
        for (prune_leaves) |l| s.addPrune(l) catch {};
        const fused = s.tryFuseFilter(filter) catch return NoMatch;
        if (!fused) return NoMatch;
    }
    snap.memtable_snap.release();
    pin_held = false;
    return .{ .sources = sources, .total_rows = total_rows };
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
    for (ent.passthrough) |pp| {
        if (pp.in_idx >= ent.input_schemas[0].len) return NoMatch;
        const src_name = ent.input_schemas[0][pp.in_idx].name;
        const src = b.fb.resolve(src_name) orelse return NoMatch;
        try b.fb.setVis(ent.output_schema[pp.out_idx].name, src.idx);
    }
}

fn cloneOrder(b: *Builder, specs: []const ir.SortSpec) ![]const region.OrderBy {
    const out = try b.a.alloc(region.OrderBy, specs.len);
    for (specs, out) |src, *dst| {
        dst.* = .{ .col = try b.resolveIdx(src.col), .desc = src.desc };
    }
    return out;
}

/// Every ROW_NUMBER call becomes one ranks op; a partition coarser than the
/// range keys ranks over merged spans.
fn pushRanksFromWindow(b: *Builder, w: *const ir.WindowOp) !void {
    for (w.calls) |call| {
        if (call.func != .row_number or call.args.len != 0) return NoMatch;
        const spec = w.specs[call.spec_idx];
        const merge_on: ?usize = switch (try b.classifyPartition(spec.partition_by)) {
            .range_exact => null,
            .merged_span => |m| m,
        };
        const order = try cloneOrder(b, spec.order_by);
        try b.flushPending();
        const idx = try b.fb.addCol(call.output_name, .bigint, false);
        try b.ops.append(b.a, .{ .ranks = .{
            .name = b.fb.cols.items[idx].name,
            .order = order,
            .merge_on = merge_on,
        } });
        try b.fb.setVis(call.output_name, idx);
    }
}

/// LAST_VALUE(col) with an unbounded-following frame over the range keys →
/// fill_last; a current-row frame is per-row identity (vis re-point only).
fn pushFillLast(b: *Builder, w: *const ir.WindowOp) !void {
    for (w.calls) |call| {
        if (call.func != .last_value or call.args.len != 1) return NoMatch;
        if (call.args[0] != .col_ref) return NoMatch;
        const spec = w.specs[call.spec_idx];
        if (!try b.partitionMatchesRangeKeys(spec.partition_by)) return NoMatch;
        try checkRangeOrder(b, spec.order_by);
        const src = try b.resolveIdx(call.args[0].col_ref);
        if (spec.frame.end != .unbounded_following) {
            try b.fb.setVis(call.output_name, src);
            continue;
        }
        try b.flushPending();
        const idx = try b.fb.addCol(call.output_name, b.fb.cols.items[src].type, true);
        try b.ops.append(b.a, .{ .fill_last = .{ .name = b.fb.cols.items[idx].name, .src = src } });
        try b.fb.setVis(call.output_name, idx);
    }
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
    if (subkeys.items.len == 0 or subkeys.items.len > 3) return NoMatch;

    var out: std.ArrayListUnmanaged(region.AggOut) = .empty;
    var new_vis: std.ArrayListUnmanaged(VisEntry) = .empty;

    // Group keys first (constant within their sub-group → .first).
    for (g.group_cols) |gc| {
        const e = b.fb.resolve(gc) orelse return NoMatch;
        try out.append(a, .{ .name = try nameFor(b, gc), .kind = .{ .first = e.idx } });
        try new_vis.append(a, .{ .name = try a.dupe(u8, gc), .idx = new_vis.items.len });
    }
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
                    .tinyint, .smallint, .int, .bigint, .date, .datetime => .{ .sum_int = idx },
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
            .sum_float => .double,
        };
        try new_cols.append(a, .{ .name = o.name, .type = t, .nullable = true });
    }
    b.fb.cols = new_cols;
    b.fb.vis = new_vis;
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
