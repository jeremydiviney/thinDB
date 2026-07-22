//! V2 low-cardinality grouped aggregate: the hybrid between the global
//! reduction (one group, per-lane partials) and the silo grid (unbounded
//! groups, scatter-by-key staging).
//!
//! When the GROUP BY key set provably has FEW distinct combinations, the
//! silo's stage/scatter is pure overhead: every row's key + aggregate payload
//! is written into per-bucket buffers and re-read, costing more memory traffic
//! than the aggregation itself. This handler instead folds rows DIRECTLY as
//! they are scanned: each worker owns a private packed-key group table small
//! enough to stay cache-resident, so no row ever moves between threads.
//!
//! Merge is two-natured:
//!   - Scalar aggregates (COUNT/SUM/AVG/MIN/MAX): a single-threaded fold of
//!     n_workers small tables — trivially cheap at this cardinality.
//!   - COUNT(DISTINCT): per-worker partial sets overlap heavily across
//!     workers, so a serial union would re-dedup tens of millions of entries
//!     (the bottleneck the global path eliminated). Each worker therefore
//!     scatters its (key, value) composites into P = n_workers sub-sets by
//!     key-hash (Lemire multiply-shift — same partition in every worker), and
//!     the merge runs ONE THREAD PER PARTITION over disjoint key sets, bumping
//!     per-group counts on first sighting. Partition counts sum exactly.
//!
//! Routing: the planner admits a query only when the combined group-key
//! cardinality has a provable-enough upper bound — the saturating PRODUCT of
//! the per-column HLL NDV estimates (clamped to the row-count ceiling), so a
//! compound key is bounded even when its combination cardinality isn't
//! directly known. Unknown NDV on any key column declines (the silo handles
//! it). The estimate only sizes tables — they grow dynamically, so an HLL
//! under-estimate degrades performance, never correctness.
//!
//! String group keys ride the dict-code machinery: a provably low-card,
//! non-nullable string column packs its 32-bit GLOBAL dict code into the key
//! (the scan emits codes via the `Batch.coded` sidecar, skipping the
//! dict→string expansion entirely), decoded back to bytes only at emit. All
//! workers intern into ONE shared `GlobalDict` per coded column — its mutex
//! makes that safe, and it keeps codes comparable across workers, which the
//! scalar merge (packed-key equality) and the distinct partitioning (same
//! composite → same partition in every worker) both rely on. A batch without
//! the sidecar (tombstoned row group, memtable rows, a scan that declined
//! coding) falls back to interning each row's bytes into the same dict.

const std = @import("std");
const Allocator = std.mem.Allocator;

const api = @import("../api/api.zig");
const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;
const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;

const exec = @import("exec.zig");
const aggregate = @import("aggregate.zig");
const Scan = @import("scan.zig").Scan;
const SiloCore = exec.silo_group_core;
const platform = @import("../util/platform.zig");
const core_scheduler = @import("../util/core_scheduler.zig");
const group_table = exec.group_table;

const Batch = exec.Batch;
const Query = exec.Query;
const PredicateExpr = exec.PredicateExpr;
const SortSpec = exec.SortSpec;

pub const Request = @import("v2_shape_group_topn.zig").Request;

const GroupTable = group_table.IntKeyMemsetTable(64);
const DistinctSet = SiloCore.DistinctSet;
const CountSlotTable = group_table.CountSlotTable;

const MAX_KEYS: usize = 8;
const MAX_AGGS: usize = 16;
const TILE_RGS: usize = 16;
const PREFETCH_DIST: usize = 32;
const PREFETCH_DIST_DISTINCT: usize = 24;

// Routing gate. The crossover against the silo is "does one worker's full
// group table stay cache-resident": beyond it, every probe on a 100M-row
// stream is a DRAM miss and the scatter's locality wins. 64K groups at the
// ~32-byte common stride is ~2 MB — comfortably inside a shared-L3 slice per
// worker. The byte cap tightens the gate for wide aggregate programs.
const GATE_GROUPS: u64 = 64 * 1024;
const GATE_STATE_BYTES: u64 = 4 << 20;

const AggOp = enum { count_star, count_col, sum, avg, min, max, count_distinct };

const GlobalDict = exec.GlobalDict;

const KeyPart = struct {
    name: []const u8,
    typ: Type,
    offset: u8,
    width: u8,
    // String key packed as its 32-bit global dict code (decoded at emit).
    coded: bool = false,
};

const AggPlan = struct {
    op: AggOp,
    input_name: ?[]const u8,
    is_float: bool = false,
    output_type: Type,
    // DECIMAL input scale: SUM folds raw mantissas (correct at this scale, so
    // emit is unchanged), AVG divides the mean by 10^input_scale. 0 otherwise.
    input_scale: u8 = 0,
    name: []const u8,
    // count_distinct only: which distinct slot this aggregate owns, the bit
    // width of its value (composite = key << value_bits | value), and the
    // membership-set tier sized to key_bits + value_bits.
    distinct_index: u16 = 0,
    value_bits: u8 = 0,
    tier: DistinctSet.Tier = .u64,
};

fn keyTypeBits(t: Type) ?u8 {
    return switch (t) {
        .boolean, .tinyint => 8,
        .smallint => 16,
        .int, .date => 32,
        .bigint, .datetime, .decimal64 => 64,
        else => null,
    };
}

fn tierFor(bits: u16) DistinctSet.Tier {
    if (bits <= 32) return .u32;
    if (bits <= 64) return .u64;
    if (bits <= 96) return .u96;
    return .u128;
}

fn isFloatType(t: Type) bool {
    return t == .float or t == .double;
}

fn isStringType(t: Type) bool {
    return switch (t) {
        .varchar, .string, .char, .json => true,
        else => false,
    };
}

fn columnType(table: *api.Table, name: []const u8) ?Type {
    const idx = types.findColumn(table.schema.columns, name) orelse return null;
    return table.schema.columns[idx].type;
}

fn columnNullable(table: *api.Table, name: []const u8) bool {
    const idx = types.findColumn(table.schema.columns, name) orelse return false;
    return table.schema.columns[idx].nullable;
}

inline fn truncBits(x: u64, bits: u8) u64 {
    if (bits >= 64) return x;
    return x & ((@as(u64, 1) << @intCast(bits)) - 1);
}

// Same partitioning as the global path: Lemire multiply-shift on the
// tier-sized hash, so a composite lands in the same partition in every worker
// and the set's low-bit bucketing stays independent of the split.
inline fn distinctPartition(tier: DistinctSet.Tier, key: u128, parts: usize) usize {
    const h: u64 = switch (tier) {
        .u32 => group_table.mix64(@as(u64, @as(u32, @truncate(key)))),
        .u64 => group_table.mix64(@truncate(key)),
        .u96 => group_table.Key96.fromU128(key).hash(),
        .u128 => group_table.hashU128(key),
    };
    return @intCast((@as(u128, h) *% @as(u128, parts)) >> 64);
}

pub fn tryBuild(allocator: Allocator, table: *api.Table, request: Request) !?Query {
    if (request.group_cols.len == 0 or request.group_cols.len > MAX_KEYS) return null;
    if (request.aggs.len == 0 or request.aggs.len > MAX_AGGS) return null;
    // HAVING and derived columns stay on the silo path: a derived group key
    // has no stored stats to gate on, and HAVING needs its predicate machinery.
    if (request.having_filter != null) return null;
    if (request.derived.len != 0) return null;

    // Group keys: real integer-family table columns, plus low-card string
    // columns as 32-bit dict codes (codeability proven on the probe below),
    // packing into ≤64 bits.
    var parts: [MAX_KEYS]KeyPart = undefined;
    var key_bits: u16 = 0;
    var n_coded: usize = 0;
    for (request.group_cols, 0..) |name, i| {
        const typ = columnType(table, name) orelse return null;
        // A NULL key slot's batch value is an encoding artifact (FOR base /
        // dict entry 0); the packed key carries no validity bit, so nullable
        // keys decline to the NULL-tagged generic path.
        if (columnNullable(table, name)) return null;
        if (isStringType(typ)) {
            parts[i] = .{ .name = name, .typ = typ, .offset = @intCast(key_bits), .width = 32, .coded = true };
            key_bits += 32;
            n_coded += 1;
        } else {
            const width = keyTypeBits(typ) orelse return null;
            parts[i] = .{ .name = name, .typ = typ, .offset = @intCast(key_bits), .width = width };
            key_bits += width;
        }
    }
    if (key_bits > 64) return null;

    var aggs = try allocator.alloc(AggPlan, request.aggs.len);
    errdefer allocator.free(aggs);
    var n_distinct: u16 = 0;
    for (request.aggs, 0..) |agg, i| {
        switch (agg.func) {
            .count => {
                if (agg.col) |col_name| {
                    if (columnType(table, col_name) == null) return declineFree(allocator, aggs);
                    // The direct kernels are NULL-blind on inputs: COUNT(col)
                    // would count NULL rows and SUM would fold artifact
                    // payloads. Nullable inputs decline to the silo, which
                    // routes them to the validity-aware legacy aggregate.
                    if (columnNullable(table, col_name)) return declineFree(allocator, aggs);
                    aggs[i] = .{ .op = .count_col, .input_name = col_name, .output_type = .bigint, .name = agg.as };
                } else {
                    aggs[i] = .{ .op = .count_star, .input_name = null, .output_type = .bigint, .name = agg.as };
                }
            },
            .sum, .avg, .min, .max => {
                const col_name = agg.col orelse return declineFree(allocator, aggs);
                const typ = columnType(table, col_name) orelse return declineFree(allocator, aggs);
                if (columnNullable(table, col_name)) return declineFree(allocator, aggs);
                if (keyTypeBits(typ) == null and !isFloatType(typ)) return declineFree(allocator, aggs);
                // Temporal SUM/AVG is a dialect error (validateAggFn) — decline
                // so the shape errors consistently instead of this lane quietly
                // summing day/µs ints when the key cardinality happens to be low.
                if ((typ == .date or typ == .datetime) and agg.func != .min and agg.func != .max)
                    return declineFree(allocator, aggs);
                // decimal128 (i128 mantissa) can't fit this path's i64 slots;
                // decline so the shape surfaces an error rather than truncating.
                // decimal64 is fine: SUM/MIN/MAX fold in i64 and widen at emit,
                // AVG divides out the scale (see input_scale below).
                if (typ == .decimal128) return declineFree(allocator, aggs);
                const out_type = aggregate.aggOutputTypeFor(agg, typ) catch return declineFree(allocator, aggs);
                aggs[i] = .{
                    .op = switch (agg.func) {
                        .sum => .sum,
                        .avg => .avg,
                        .min => .min,
                        .max => .max,
                        else => unreachable,
                    },
                    .input_name = col_name,
                    .is_float = isFloatType(typ),
                    .output_type = out_type,
                    .input_scale = if (typ.decimalSpec()) |sp| sp.s else 0,
                    .name = agg.as,
                };
            },
            .count_distinct => {
                const col_name = agg.col orelse return declineFree(allocator, aggs);
                const typ = columnType(table, col_name) orelse return declineFree(allocator, aggs);
                if (columnNullable(table, col_name)) return declineFree(allocator, aggs);
                const vbits = keyTypeBits(typ) orelse return declineFree(allocator, aggs);
                aggs[i] = .{
                    .op = .count_distinct,
                    .input_name = col_name,
                    .output_type = .bigint,
                    .name = agg.as,
                    .distinct_index = n_distinct,
                    .value_bits = vbits,
                    .tier = tierFor(key_bits + vbits),
                };
                n_distinct += 1;
            },
            else => return declineFree(allocator, aggs),
        }
    }

    // A coded key column's batch value is an empty-string placeholder — no
    // aggregate may read it as input.
    if (n_coded > 0) {
        for (aggs) |a| {
            const nm = a.input_name orelse continue;
            for (parts[0..request.group_cols.len]) |p| {
                if (p.coded and types.columnNameEql(p.name, nm)) return declineFree(allocator, aggs);
            }
        }
    }

    // ORDER BY must reference an emitted column (group key or aggregate alias).
    for (request.order_specs) |spec| {
        var found = false;
        for (parts[0..request.group_cols.len]) |p| {
            if (types.columnNameEql(p.name, spec.col)) found = true;
        }
        for (aggs) |a| {
            if (types.columnNameEql(a.name, spec.col)) found = true;
        }
        if (!found) return declineFree(allocator, aggs);
    }

    // Projected columns: keys + aggregate inputs, de-duplicated.
    var needed: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer needed.deinit(allocator);
    for (request.group_cols) |name| try addNeeded(allocator, &needed, name);
    for (aggs) |a| {
        if (a.input_name) |nm| try addNeeded(allocator, &needed, nm);
    }

    // Cardinality gate via a probe scan: the saturating product of per-key
    // HLL NDV bounds, clamped to the row-count ceiling, is a sound upper bound
    // for the COMBINED key cardinality. Unknown NDV on any key → silo. The
    // probe also proves the WHERE fuses, so the per-worker opens can't fail.
    var est_groups: u64 = 0;
    {
        const probe = Scan.allocWithProjectionLoc(allocator, table, null, needed.items, false, null) catch {
            needed.deinit(allocator);
            return declineFree(allocator, aggs);
        };
        defer probe.deinit();
        if (request.where_filter) |w| {
            const fused = probe.tryFuseFilter(w) catch false;
            if (!fused) {
                needed.deinit(allocator);
                return declineFree(allocator, aggs);
            }
        }
        // String keys must be codeable (non-nullable, flushed, exact NDV under
        // the dict-code cap) — otherwise the silo's hash path handles them.
        for (parts[0..request.group_cols.len]) |p| {
            if (p.coded and !probe.canCodeColumn(p.name)) {
                needed.deinit(allocator);
                return declineFree(allocator, aggs);
            }
        }
        const st = probe.stats();
        const schema = probe.outputSchema();
        var product: u64 = 1;
        for (request.group_cols) |name| {
            const idx = types.findColumn(schema, name) orelse {
                needed.deinit(allocator);
                return declineFree(allocator, aggs);
            };
            if (idx >= st.column_stats.len) {
                needed.deinit(allocator);
                return declineFree(allocator, aggs);
            }
            switch (st.column_stats[idx].ndv) {
                .exact => |n| product *|= @max(n, 1),
                .unknown => {
                    needed.deinit(allocator);
                    return declineFree(allocator, aggs);
                },
            }
        }
        est_groups = @max(@min(product, @max(st.upper_rows, 1)), 1);
    }
    const state_stride: u64 = 8 + @as(u64, request.aggs.len) * 24;
    if (est_groups > GATE_GROUPS or est_groups * state_stride > GATE_STATE_BYTES) {
        needed.deinit(allocator);
        return declineFree(allocator, aggs);
    }

    // One shared dict per coded key column, owned by the operator: every
    // worker interns into it, so codes are comparable across workers.
    const dicts = try allocator.alloc(?*GlobalDict, request.group_cols.len);
    errdefer allocator.free(dicts);
    @memset(dicts, null);
    errdefer for (dicts) |md| if (md) |d| {
        d.deinit(allocator);
        allocator.destroy(d);
    };
    for (parts[0..request.group_cols.len], 0..) |p, i| {
        if (!p.coded) continue;
        const d = try allocator.create(GlobalDict);
        d.* = .{};
        dicts[i] = d;
    }

    const needed_owned = try needed.toOwnedSlice(allocator);
    errdefer allocator.free(needed_owned);
    const op = try allocator.create(LowCardGroup);
    errdefer allocator.destroy(op);
    op.* = try LowCardGroup.init(allocator, table, request, parts, aggs, n_distinct, @intCast(key_bits), est_groups, needed_owned, dicts);
    return exec.makeQuery(allocator, op);
}

fn declineFree(allocator: Allocator, aggs: []AggPlan) ?Query {
    allocator.free(aggs);
    return null;
}

fn addNeeded(allocator: Allocator, needed: *std.ArrayListUnmanaged([]const u8), name: []const u8) !void {
    for (needed.items) |existing| {
        if (types.columnNameEql(existing, name)) return;
    }
    try needed.append(allocator, name);
}

// Scan + driver pair per worker; no Compute layer (derived shapes decline).
const ScanSource = struct {
    scan: *Scan,
    drive: Query,

    fn next(self: *ScanSource) !?Batch {
        return self.drive.next();
    }

    fn resetRange(self: *ScanSource, start_seg: usize, start_rg: usize, end_seg: usize, end_rg: usize, scan_memtable: bool) void {
        self.scan.resetRange(start_seg, start_rg, end_seg, end_rg, scan_memtable);
    }

    fn deinit(self: *ScanSource) void {
        self.drive.deinit();
    }
};

fn openScanSource(
    allocator: Allocator,
    table: *api.Table,
    needed: []const []const u8,
    where_filter: ?PredicateExpr,
    snap: ?Scan.Snapshot,
    parts: []const KeyPart,
    dicts: []const ?*GlobalDict,
) !ScanSource {
    const scan = try Scan.allocWithProjectionLoc(allocator, table, null, needed, false, snap);
    errdefer scan.deinit();
    if (where_filter) |w| {
        // tryBuild proved fusibility on the probe scan; never run unfiltered.
        if (!try scan.tryFuseFilter(w)) return error.UnsupportedQueryShape;
    }
    // A decline (e.g. the execute-time snapshot has memtable rows the probe
    // didn't) is fine: the fold's per-row intern fallback covers plain batches.
    for (parts, 0..) |p, i| {
        if (p.coded) _ = scan.setDictCodeColumn(p.name, dicts[i].?);
    }
    return .{ .scan = scan, .drive = exec.makeQuery(allocator, scan) };
}

// One worker's private aggregation state: a packed-key group table plus
// parallel per-group accumulator arrays (stride = n_aggs). Slots are i128 —
// overflow-proof for any integer SUM; floats bit-cast an f64 into the slot.
const WState = struct {
    table: GroupTable,
    counts: std.ArrayListUnmanaged(u64) = .empty,
    slots: std.ArrayListUnmanaged(i128) = .empty,
    ns: std.ArrayListUnmanaged(u64) = .empty,
    // n_distinct × parts membership sets, indexed [d * parts + partition].
    dsets: []DistinctSet = &.{},
    // Per-batch scratch (reused): each row's packed key and resolved gid, so
    // the per-aggregate kernels run over dense arrays instead of re-probing.
    keys_scratch: std.ArrayListUnmanaged(u64) = .empty,
    gids_scratch: std.ArrayListUnmanaged(u32) = .empty,

    fn init(allocator: Allocator, expected_groups: usize, aggs: []const AggPlan, n_distinct: u16, dop_parts: usize) !WState {
        var self: WState = .{ .table = try GroupTable.init(allocator, expected_groups) };
        errdefer self.table.deinit(allocator);
        const dsets = try allocator.alloc(DistinctSet, @as(usize, n_distinct) * dop_parts);
        @memset(dsets, .{});
        for (aggs) |a| {
            if (a.op != .count_distinct) continue;
            for (dsets[@as(usize, a.distinct_index) * dop_parts ..][0..dop_parts]) |*d| d.configure(a.tier);
        }
        self.dsets = dsets;
        return self;
    }

    fn deinit(self: *WState, allocator: Allocator) void {
        for (self.dsets) |*d| d.deinit(allocator);
        if (self.dsets.len > 0) allocator.free(self.dsets);
        self.keys_scratch.deinit(allocator);
        self.gids_scratch.deinit(allocator);
        self.counts.deinit(allocator);
        self.slots.deinit(allocator);
        self.ns.deinit(allocator);
        self.table.deinit(allocator);
    }
};

const Worker = struct {
    index: usize,
    cpu: ?usize,
    source: ScanSource,
    state: WState,
    parts: []const KeyPart,
    dicts: []const ?*GlobalDict,
    aggs: []const AggPlan,
    key_bits: u8,
    dop_parts: usize,
    resolved_keys: [MAX_KEYS]usize = undefined,
    resolved_aggs: [MAX_AGGS]?usize = undefined,
    allocator: Allocator,
    seg_start: []const usize,
    segment_count: usize,
    total_rgs: usize,
    next_rg: *std.atomic.Value(usize),
    err: ?anyerror = null,
};

fn workerMain(w: *Worker) void {
    // Lease a core from the process-global scheduler (round-robins distinct
    // cores across ALL in-flight queries, global per-core cap) rather than
    // pinning to a per-query `cpus[worker_index]` — a fixed assignment makes
    // every single-worker query pin to the same core, serializing N concurrent
    // ones. tryAcquire (non-blocking): workers coordinate, so none may block on
    // a full bucket; excess workers run unpinned.
    var lease = core_scheduler.global().tryAcquire();
    defer lease.release();
    workerRun(w) catch |e| {
        w.err = e;
    };
}

const Coord = struct { seg: usize, rg: usize };

fn flatToCoord(f: usize, seg_start: []const usize, segment_count: usize, total: usize) Coord {
    if (f >= total) return .{ .seg = segment_count, .rg = 0 };
    var seg: usize = 0;
    while (seg + 1 < segment_count and seg_start[seg + 1] <= f) seg += 1;
    return .{ .seg = seg, .rg = f - seg_start[seg] };
}

fn workerRun(w: *Worker) !void {
    var have_resolved = false;
    if (w.total_rgs == 0) {
        if (w.index != 0) return;
        w.source.resetRange(0, 0, w.segment_count, 0, true);
        try driveTile(w, &have_resolved);
        return;
    }
    while (true) {
        const lo = w.next_rg.fetchAdd(TILE_RGS, .monotonic);
        if (lo >= w.total_rgs) break;
        const hi = @min(lo + TILE_RGS, w.total_rgs);
        const start = flatToCoord(lo, w.seg_start, w.segment_count, w.total_rgs);
        const end = flatToCoord(hi, w.seg_start, w.segment_count, w.total_rgs);
        w.source.resetRange(start.seg, start.rg, end.seg, end.rg, hi == w.total_rgs);
        try driveTile(w, &have_resolved);
    }
}

fn driveTile(w: *Worker, have_resolved: *bool) !void {
    while (try w.source.next()) |batch| {
        if (!have_resolved.*) {
            for (w.parts, 0..) |p, i| {
                w.resolved_keys[i] = batch.columnIndex(p.name) orelse return error.UnsupportedQueryShape;
            }
            for (w.aggs, 0..) |a, i| {
                w.resolved_aggs[i] = if (a.input_name) |nm| (batch.columnIndex(nm) orelse return error.UnsupportedQueryShape) else null;
            }
            have_resolved.* = true;
        }
        try foldBatch(w, batch);
    }
}

// OR one key part into every row's packed key, column-wise so each inner loop
// is monomorphic. A coded part normally reads the scan's code sidecar; a batch
// without one (tombstoned row group, memtable rows, a scan that declined
// coding) interns each row's bytes into the same shared dict instead — slower
// but identical codes, so any mix of batch kinds groups consistently.
fn packKeysForPart(w: *Worker, batch: Batch, part_i: usize, keys: []u64) !void {
    const p = w.parts[part_i];
    const ci = w.resolved_keys[part_i];
    const shift: u6 = @intCast(p.offset);
    if (p.coded) {
        const sidecar: ?exec.CodedColumn = if (batch.coded) |sc| sc[ci] else null;
        if (sidecar) |cc| {
            for (keys, cc.codes[0..keys.len]) |*k, code| {
                k.* |= @as(u64, code) << shift;
            }
        } else {
            const dict = w.dicts[part_i].?;
            const sv = switch (batch.values[ci].data) {
                .varchar, .string, .char, .json => |s| s,
                else => return error.UnsupportedQueryShape,
            };
            for (keys, 0..) |*k, r| {
                const code = try dict.intern(w.allocator, sv.rowBytes(r));
                k.* |= @as(u64, code) << shift;
            }
        }
        return;
    }
    switch (batch.values[ci].data) {
        inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |s| {
            const width = p.width;
            for (keys, s[0..keys.len]) |*k, v| {
                const raw: u64 = @bitCast(@as(i64, v));
                k.* |= truncBits(raw, width) << shift;
            }
        },
        else => return error.UnsupportedQueryShape,
    }
}

// The fold runs in passes so each inner loop is monomorphic: pass 0 packs
// every row's key column-wise (pure compute, one monomorphic loop per key
// part), pass 1 probes the group table with a look-ahead prefetch over the
// packed keys, then each aggregate runs ONE specialized kernel over the dense
// gid array — the op dispatch and the ValueView tag switch both hoisted out
// of the row loops.
fn foldBatch(w: *Worker, batch: Batch) !void {
    const n = batch.row_count;
    if (n == 0) return;
    const st = &w.state;
    const allocator = w.allocator;
    const n_aggs = w.aggs.len;

    if (st.table.needsGrow(n)) try st.table.grow(allocator, n);
    try st.counts.ensureUnusedCapacity(allocator, n);
    try st.slots.ensureUnusedCapacity(allocator, n * n_aggs);
    try st.ns.ensureUnusedCapacity(allocator, n * n_aggs);
    try st.keys_scratch.resize(allocator, n);
    try st.gids_scratch.resize(allocator, n);
    const keys = st.keys_scratch.items[0..n];
    const gids = st.gids_scratch.items[0..n];

    @memset(keys, 0);
    for (0..w.parts.len) |pi| try packKeysForPart(w, batch, pi, keys);

    var r: usize = 0;
    while (r < n) {
        const pf = r + PREFETCH_DIST;
        if (pf < n) {
            @prefetch(st.table.slotAddr(st.table.bucketOf(GroupTable.hashKey(keys[pf]))), .{ .rw = .write, .locality = 1 });
        }
        // Adjacent-equal run over the packed keys (the table is physically
        // ordered, so clustered group keys arrive in runs): one probe and one
        // count bump for the whole run.
        const key = keys[r];
        var run_end = r + 1;
        while (run_end < n and keys[run_end] == key) run_end += 1;
        const probe = st.table.getOrPut(GroupTable.hashKey(key), key);
        var gid: u32 = probe.gid;
        if (!probe.found) {
            gid = @intCast(st.counts.items.len);
            st.counts.appendAssumeCapacity(0);
            st.slots.appendNTimesAssumeCapacity(0, n_aggs);
            st.ns.appendNTimesAssumeCapacity(0, n_aggs);
            st.table.commit(probe.slot, key, gid);
        }
        st.counts.items[gid] += @intCast(run_end - r);
        @memset(gids[r..run_end], gid);
        r = run_end;
    }

    const ns = st.ns.items;
    const slots = st.slots.items;
    for (w.aggs, 0..) |a, i| {
        if (a.op == .count_star) continue;
        const view = batch.values[w.resolved_aggs[i].?];
        switch (a.op) {
            .count_star => unreachable,
            .count_col => foldCountCol(view, gids, ns, n_aggs, i),
            .sum, .avg => if (a.is_float)
                foldSumFloat(view, gids, ns, slots, n_aggs, i)
            else
                foldSumInt(view, gids, ns, slots, n_aggs, i),
            .min => if (a.is_float)
                foldExtremeFloat(true, view, gids, ns, slots, n_aggs, i)
            else
                foldExtremeInt(true, view, gids, ns, slots, n_aggs, i),
            .max => if (a.is_float)
                foldExtremeFloat(false, view, gids, ns, slots, n_aggs, i)
            else
                foldExtremeInt(false, view, gids, ns, slots, n_aggs, i),
            .count_distinct => try foldDistinctKernel(
                allocator,
                view,
                keys,
                st.dsets[@as(usize, a.distinct_index) * w.dop_parts ..][0..w.dop_parts],
                w.dop_parts,
                a.value_bits,
                a.tier,
            ),
        }
    }
}

// The fold kernels walk the gid array run-at-a-time: adjacent-equal gids
// (clustered keys arrive in runs) accumulate in registers and touch the
// group's ns/slots entries once per run. Null rows stay inside the run walk.
fn foldCountCol(view: ColumnView, gids: []const u32, ns: []u64, stride: usize, agg_i: usize) void {
    var r: usize = 0;
    while (r < gids.len) {
        const gid = gids[r];
        var cnt: u64 = 0;
        var rr = r;
        while (rr < gids.len and gids[rr] == gid) : (rr += 1) cnt += @intFromBool(view.isValid(rr));
        if (cnt != 0) ns[@as(usize, gid) * stride + agg_i] += cnt;
        r = rr;
    }
}

fn foldSumInt(view: ColumnView, gids: []const u32, ns: []u64, slots: []i128, stride: usize, agg_i: usize) void {
    switch (view.data) {
        inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |s| {
            var r: usize = 0;
            while (r < gids.len) {
                const gid = gids[r];
                var acc: i128 = 0;
                var cnt: u64 = 0;
                var rr = r;
                while (rr < gids.len and gids[rr] == gid) : (rr += 1) {
                    if (!view.isValid(rr)) continue;
                    acc += s[rr];
                    cnt += 1;
                }
                if (cnt != 0) {
                    const o = @as(usize, gid) * stride + agg_i;
                    ns[o] += cnt;
                    slots[o] += acc;
                }
                r = rr;
            }
        },
        else => {},
    }
}

fn foldSumFloat(view: ColumnView, gids: []const u32, ns: []u64, slots: []i128, stride: usize, agg_i: usize) void {
    switch (view.data) {
        inline .float, .double => |s| {
            var r: usize = 0;
            while (r < gids.len) {
                const gid = gids[r];
                var acc: f64 = 0;
                var cnt: u64 = 0;
                var rr = r;
                while (rr < gids.len and gids[rr] == gid) : (rr += 1) {
                    if (!view.isValid(rr)) continue;
                    acc += @as(f64, @floatCast(s[rr]));
                    cnt += 1;
                }
                if (cnt != 0) {
                    const o = @as(usize, gid) * stride + agg_i;
                    ns[o] += cnt;
                    setSlotF64(&slots[o], slotF64(slots[o]) + acc);
                }
                r = rr;
            }
        },
        else => {},
    }
}

fn foldExtremeInt(comptime is_min: bool, view: ColumnView, gids: []const u32, ns: []u64, slots: []i128, stride: usize, agg_i: usize) void {
    switch (view.data) {
        inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |s| {
            var r: usize = 0;
            while (r < gids.len) {
                const gid = gids[r];
                var best: i128 = 0;
                var cnt: u64 = 0;
                var rr = r;
                while (rr < gids.len and gids[rr] == gid) : (rr += 1) {
                    if (!view.isValid(rr)) continue;
                    const v: i128 = s[rr];
                    if (cnt == 0 or (if (is_min) v < best else v > best)) best = v;
                    cnt += 1;
                }
                if (cnt != 0) {
                    const o = @as(usize, gid) * stride + agg_i;
                    const was_empty = ns[o] == 0;
                    ns[o] += cnt;
                    const better = if (is_min) best < slots[o] else best > slots[o];
                    if (was_empty or better) slots[o] = best;
                }
                r = rr;
            }
        },
        else => {},
    }
}

fn foldExtremeFloat(comptime is_min: bool, view: ColumnView, gids: []const u32, ns: []u64, slots: []i128, stride: usize, agg_i: usize) void {
    switch (view.data) {
        inline .float, .double => |s| {
            var r: usize = 0;
            while (r < gids.len) {
                const gid = gids[r];
                var best: f64 = 0;
                var cnt: u64 = 0;
                var rr = r;
                while (rr < gids.len and gids[rr] == gid) : (rr += 1) {
                    if (!view.isValid(rr)) continue;
                    const v: f64 = @floatCast(s[rr]);
                    if (cnt == 0 or (if (is_min) v < best else v > best)) best = v;
                    cnt += 1;
                }
                if (cnt != 0) {
                    const o = @as(usize, gid) * stride + agg_i;
                    const was_empty = ns[o] == 0;
                    ns[o] += cnt;
                    const better = if (is_min) best < slotF64(slots[o]) else best > slotF64(slots[o]);
                    if (was_empty or better) setSlotF64(&slots[o], best);
                }
                r = rr;
            }
        },
        else => {},
    }
}

// COUNT(DISTINCT) fold: scatter composites into the partition sub-sets with a
// look-ahead prefetch — the set probe is the cache-miss bottleneck, and the
// composite for row r+K is computable up front (packed key + value, no group
// probe needed), so the misses of independent rows overlap. A grow between a
// prefetch and its insert only wastes that one hint.
fn foldDistinctKernel(
    allocator: Allocator,
    view: ColumnView,
    keys: []const u64,
    dsets: []DistinctSet,
    parts_n: usize,
    value_bits: u8,
    tier: DistinctSet.Tier,
) !void {
    switch (view.data) {
        inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |s| {
            const n = keys.len;
            var prev_comp: u128 = 0;
            var have_prev = false;
            var r: usize = 0;
            while (r < n) : (r += 1) {
                if (!view.isValid(r)) continue;
                const pf = r + PREFETCH_DIST_DISTINCT;
                if (pf < n and view.isValid(pf)) {
                    const c_pf = compositeOf(keys[pf], @as(i64, s[pf]), value_bits);
                    dsets[distinctPartition(tier, c_pf, parts_n)].prefetchKey(c_pf);
                }
                const comp = compositeOf(keys[r], @as(i64, s[r]), value_bits);
                // Physical order clusters repeated (key, value) pairs into
                // adjacent runs; an identical composite can't be new — skip
                // the cache-missing set probe.
                if (have_prev and comp == prev_comp) continue;
                prev_comp = comp;
                have_prev = true;
                _ = try dsets[distinctPartition(tier, comp, parts_n)].insertIsNew(allocator, comp);
            }
        },
        else => {},
    }
}

inline fn compositeOf(key: u64, value: i64, value_bits: u8) u128 {
    const raw: u64 = @bitCast(value);
    return (@as(u128, key) << @intCast(value_bits)) | truncBits(raw, value_bits);
}

inline fn slotF64(slot: i128) f64 {
    return @bitCast(@as(u64, @truncate(@as(u128, @bitCast(slot)))));
}

inline fn setSlotF64(slot: *i128, v: f64) void {
    slot.* = @bitCast(@as(u128, @as(u64, @bitCast(v))));
}

// One partition of the cross-worker distinct merge: for each distinct
// aggregate, union column `part` of every worker into a fresh set; a first
// sighting bumps the owning group's count in this job's private key→count
// table. Partitions are disjoint by key-hash, so no locks and counts sum.
const PartMergeJob = struct {
    part: usize,
    dop_parts: usize,
    workers: []Worker,
    aggs: []const AggPlan,
    n_distinct: u16,
    allocator: Allocator,
    cpu: ?usize,
    counts: []CountSlotTable,
    err: ?anyerror = null,
};

fn partMergeMain(job: *PartMergeJob) void {
    // Global core lease (non-blocking) instead of a per-query pin — see workerMain.
    var lease = core_scheduler.global().tryAcquire();
    defer lease.release();
    partMergeRun(job) catch |e| {
        job.err = e;
    };
}

fn partMergeRun(job: *PartMergeJob) !void {
    for (job.aggs) |a| {
        if (a.op != .count_distinct) continue;
        const d: usize = a.distinct_index;
        var out: DistinctSet = .{};
        out.configure(a.tier);
        defer out.deinit(job.allocator);
        var total: usize = 0;
        for (job.workers) |*w| total += @intCast(w.state.dsets[d * job.dop_parts + job.part].count());
        if (total == 0) continue;
        try out.ensureForBatch(job.allocator, total);
        const cnt = &job.counts[d];
        for (job.workers) |*w| {
            try mergeSetInto(job.allocator, &out, &w.state.dsets[d * job.dop_parts + job.part], cnt, a.value_bits);
        }
    }
}

// Union `src` into `out`, bumping `cnt[key]` for each first-ever composite
// (key = composite >> value_bits). Iterates the tier's raw slots; the one
// composite equal to the tier sentinel is carried by `has_sentinel` and
// re-materializes as the all-ones composite, whose key bits extract the same.
fn mergeSetInto(allocator: Allocator, out: *DistinctSet, src: *const DistinctSet, cnt: *CountSlotTable, value_bits: u8) !void {
    switch (src.store) {
        .u32 => |s| {
            for (s.slots) |k| {
                if (k != group_table.DistinctU32Set.SENTINEL) try mergeOne(allocator, out, @as(u128, k), cnt, value_bits);
            }
            if (s.has_sentinel) try mergeOne(allocator, out, @as(u128, group_table.DistinctU32Set.SENTINEL), cnt, value_bits);
        },
        .u64 => |s| {
            for (s.slots) |k| {
                if (k != group_table.DistinctU64Set.SENTINEL) try mergeOne(allocator, out, @as(u128, k), cnt, value_bits);
            }
            if (s.has_sentinel) try mergeOne(allocator, out, @as(u128, group_table.DistinctU64Set.SENTINEL), cnt, value_bits);
        },
        .u96 => |s| {
            for (s.slots) |k| {
                if (!k.eql(group_table.DistinctU96Set.SENTINEL)) {
                    const comp = @as(u128, k.w0) | (@as(u128, k.w1) << 32) | (@as(u128, k.w2) << 64);
                    try mergeOne(allocator, out, comp, cnt, value_bits);
                }
            }
            if (s.has_sentinel) try mergeOne(allocator, out, (@as(u128, 1) << 96) - 1, cnt, value_bits);
        },
        .u128 => |s| {
            for (s.slots) |k| {
                if (k != group_table.DistinctU128Set.SENTINEL) try mergeOne(allocator, out, k, cnt, value_bits);
            }
            if (s.has_sentinel) try mergeOne(allocator, out, group_table.DistinctU128Set.SENTINEL, cnt, value_bits);
        },
    }
}

inline fn mergeOne(allocator: Allocator, out: *DistinctSet, composite: u128, cnt: *CountSlotTable, value_bits: u8) !void {
    if (out.insertNewBatch(composite)) {
        try cnt.ensureFor(allocator, 1);
        cnt.insert(@truncate(composite >> @intCast(value_bits)));
    }
}

// The merged result: one group table + accumulator arrays in the same layout
// as a worker's, plus per-(group, distinct-agg) final counts. `keys` is the
// dense gid→packed-key reverse of the table, recorded at commit so sort/emit
// never have to walk slots.
const Merged = struct {
    table: GroupTable,
    keys: std.ArrayListUnmanaged(u64) = .empty,
    counts: std.ArrayListUnmanaged(u64) = .empty,
    slots: std.ArrayListUnmanaged(i128) = .empty,
    ns: std.ArrayListUnmanaged(u64) = .empty,
    dcounts: []u64 = &.{},

    fn deinit(self: *Merged, allocator: Allocator) void {
        if (self.dcounts.len > 0) allocator.free(self.dcounts);
        self.keys.deinit(allocator);
        self.counts.deinit(allocator);
        self.slots.deinit(allocator);
        self.ns.deinit(allocator);
        self.table.deinit(allocator);
    }
};

const LowCardGroup = struct {
    allocator: Allocator,
    table: *api.Table,
    parts: [MAX_KEYS]KeyPart,
    part_count: usize,
    key_bits: u8,
    // Per-key-part shared global dict (null for non-coded parts). Owned.
    dicts: []?*GlobalDict,
    aggs: []AggPlan,
    n_distinct: u16,
    where_filter: ?PredicateExpr,
    order_specs: []const SortSpec,
    limit: usize,
    offset: usize,
    dop: usize,
    est_groups: u64,
    needed: []const []const u8,
    output_schema: []Column,
    output_cols: []ColumnStore,
    views: []ColumnView,
    emitted: bool = false,
    built: bool = false,
    row_count: usize = 0,

    fn init(
        allocator: Allocator,
        table: *api.Table,
        request: Request,
        parts: [MAX_KEYS]KeyPart,
        aggs: []AggPlan,
        n_distinct: u16,
        key_bits: u8,
        est_groups: u64,
        needed: []const []const u8,
        dicts: []?*GlobalDict,
    ) !LowCardGroup {
        const n_out = request.group_cols.len + aggs.len;
        const output_schema = try allocator.alloc(Column, n_out);
        errdefer allocator.free(output_schema);
        for (parts[0..request.group_cols.len], 0..) |p, i| {
            output_schema[i] = .{ .name = p.name, .type = p.typ, .nullable = false };
        }
        for (aggs, 0..) |a, i| {
            // SUM/AVG/MIN/MAX over a group whose every input is NULL is SQL
            // NULL, so those outputs are nullable; the COUNT family is 0+.
            output_schema[request.group_cols.len + i] = .{ .name = a.name, .type = a.output_type, .nullable = switch (a.op) {
                .sum, .avg, .min, .max => true,
                .count_star, .count_col, .count_distinct => false,
            } };
        }

        const output_cols = try allocator.alloc(ColumnStore, n_out);
        errdefer allocator.free(output_cols);
        var built_cols: usize = 0;
        errdefer for (output_cols[0..built_cols]) |*c| c.deinit(allocator);
        for (output_schema, 0..) |col, i| {
            output_cols[i] = try ColumnStore.initCapacity(allocator, col.type, col.nullable, 1, 0);
            built_cols += 1;
        }

        const views = try allocator.alloc(ColumnView, n_out);
        return .{
            .allocator = allocator,
            .table = table,
            .parts = parts,
            .part_count = request.group_cols.len,
            .key_bits = key_bits,
            .dicts = dicts,
            .aggs = aggs,
            .n_distinct = n_distinct,
            .where_filter = request.where_filter,
            .order_specs = request.order_specs,
            .limit = request.limit,
            .offset = request.offset,
            .dop = request.dop,
            .est_groups = est_groups,
            .needed = needed,
            .output_schema = output_schema,
            .output_cols = output_cols,
            .views = views,
        };
    }

    pub fn deinit(self: *LowCardGroup) void {
        for (self.dicts) |md| if (md) |d| {
            d.deinit(self.allocator);
            self.allocator.destroy(d);
        };
        self.allocator.free(self.dicts);
        for (self.output_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_cols);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.aggs);
        self.allocator.free(@constCast(self.needed));
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *LowCardGroup) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(_: *LowCardGroup, _: exec.Predicate) !void {}

    pub fn accountant(_: *LowCardGroup) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn stats(self: *LowCardGroup) exec.PipelineStats {
        return .{ .upper_rows = self.est_groups };
    }

    pub fn explain(self: *LowCardGroup, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "HashAggregate (V2 lowcard direct: private-fold/partition-merge)");
        try exec.explainIndent(out, allocator, depth + 1);
        try out.appendSlice(allocator, "Scan ");
        try out.appendSlice(allocator, self.table.name);
        try out.appendSlice(allocator, " (parallel)\n");
    }

    pub fn next(self: *LowCardGroup) !?Batch {
        if (self.emitted) return null;
        if (!self.built) {
            try self.execute();
            self.built = true;
        }
        self.emitted = true;
        for (self.output_cols, 0..) |c, i| self.views[i] = c.view();
        return .{ .schema = self.output_schema, .values = self.views, .row_count = self.row_count };
    }

    fn execute(self: *LowCardGroup) !void {
        var merged = try self.reduceParallel();
        defer merged.deinit(self.allocator);
        try self.emitSorted(&merged);
    }

    fn reduceParallel(self: *LowCardGroup) !Merged {
        const table = self.table;
        const allocator = self.allocator;
        table.ddl_lock.lockSharedUncancelable(table.io);
        defer table.ddl_lock.unlockShared(table.io);

        const snap = try Scan.captureSnapshotAlloc(table, allocator);
        var pin_held = true;
        defer if (pin_held) snap.memtable_snap.release();
        defer allocator.free(snap.segments);

        const seg_start = try allocator.alloc(usize, snap.segment_count + 1);
        defer allocator.free(seg_start);
        var total_rgs: usize = 0;
        for (snap.segments, 0..) |entry, i| {
            seg_start[i] = total_rgs;
            total_rgs += entry.row_group_count;
        }
        seg_start[snap.segment_count] = total_rgs;

        var layout = platform.cpuLayout(allocator) catch platform.CpuLayout{ .order = &.{}, .physical_count = 0 };
        defer layout.deinit(allocator);
        const cpu_count = @max(@as(usize, 1), layout.order.len);
        var n_workers = @max(@as(usize, 1), @min(self.dop, cpu_count));
        if (total_rgs > 0) n_workers = @min(n_workers, total_rgs);

        // Right-size to the work that survives zone-map pruning, mirroring the
        // silo grid and ParallelScan: fuse the filter into a throwaway probe
        // scan (installing the same prune hints the worker scans get) and
        // count via the shared `Scan.survivingWorkUnits` — one worker per two
        // surviving row groups. Over-spawning costs more here than in the
        // grid: every worker pre-zeroes its own est_groups-slot direct table
        // plus its slice of distinct-set partitions.
        if (self.where_filter) |w| sized: {
            const probe = Scan.allocWithProjectionLoc(allocator, table, null, self.needed, false, snap) catch break :sized;
            defer probe.deinit();
            if (!(probe.tryFuseFilter(w) catch break :sized)) break :sized;
            const surviving = probe.survivingWorkUnits() orelse break :sized;
            n_workers = @max(@as(usize, 1), @min(n_workers, @max((surviving + 1) / 2, 1)));
        }

        const workers = try allocator.alloc(Worker, n_workers);
        defer allocator.free(workers);
        var built: usize = 0;
        defer for (workers[0..built]) |*w| {
            w.source.deinit();
            w.state.deinit(allocator);
        };

        var next_rg = std.atomic.Value(usize).init(0);
        for (workers, 0..) |*w, i| {
            var source = try openScanSource(allocator, table, self.needed, self.where_filter, snap, self.parts[0..self.part_count], self.dicts);
            errdefer source.deinit();
            w.* = .{
                .index = i,
                .cpu = if (layout.order.len == 0) null else layout.order[i % layout.order.len],
                .source = source,
                .state = try WState.init(allocator, @intCast(self.est_groups), self.aggs, self.n_distinct, n_workers),
                .parts = self.parts[0..self.part_count],
                .dicts = self.dicts,
                .aggs = self.aggs,
                .key_bits = self.key_bits,
                .dop_parts = n_workers,
                .allocator = allocator,
                .seg_start = seg_start,
                .segment_count = snap.segment_count,
                .total_rgs = total_rgs,
                .next_rg = &next_rg,
            };
            built += 1;
        }
        snap.memtable_snap.release();
        pin_held = false;

        const threads = try allocator.alloc(std.Thread, n_workers);
        defer allocator.free(threads);
        const spawned = try allocator.alloc(bool, n_workers);
        defer allocator.free(spawned);
        @memset(spawned, false);
        for (workers, 0..) |*w, i| {
            if (std.Thread.spawn(.{}, workerMain, .{w})) |th| {
                threads[i] = th;
                spawned[i] = true;
            } else |_| {}
        }
        for (workers, 0..) |*w, i| if (!spawned[i]) {
            workerRun(w) catch |e| {
                w.err = e;
            };
        };
        for (workers, 0..) |_, i| if (spawned[i]) threads[i].join();
        for (workers) |*w| if (w.err) |e| return e;

        var merged: Merged = .{ .table = try GroupTable.init(allocator, @intCast(self.est_groups)) };
        errdefer merged.deinit(allocator);
        try self.mergeScalars(workers, &merged);
        try self.mergeDistinct(workers, n_workers, layout.order, &merged);
        return merged;
    }

    fn mergeScalars(self: *LowCardGroup, workers: []Worker, merged: *Merged) !void {
        const allocator = self.allocator;
        const n_aggs = self.aggs.len;
        for (workers) |*w| {
            const wst = &w.state;
            for (wst.table.slots) |s| {
                if (s.gid == group_table.EMPTY) continue;
                const key: u64 = s.lo;
                const wgid: usize = s.gid;
                if (merged.table.needsGrow(1)) try merged.table.grow(allocator, 1);
                const probe = merged.table.getOrPut(GroupTable.hashKey(key), key);
                var mgid: u32 = probe.gid;
                if (!probe.found) {
                    mgid = @intCast(merged.counts.items.len);
                    try merged.keys.append(allocator, key);
                    try merged.counts.append(allocator, 0);
                    try merged.slots.appendNTimes(allocator, 0, n_aggs);
                    try merged.ns.appendNTimes(allocator, 0, n_aggs);
                    merged.table.commit(probe.slot, key, mgid);
                }
                merged.counts.items[mgid] += wst.counts.items[wgid];
                for (self.aggs, 0..) |a, i| {
                    const w_ns = wst.ns.items[wgid * n_aggs + i];
                    if (w_ns == 0) continue;
                    const w_slot = wst.slots.items[wgid * n_aggs + i];
                    const m_ns = &merged.ns.items[@as(usize, mgid) * n_aggs + i];
                    const m_slot = &merged.slots.items[@as(usize, mgid) * n_aggs + i];
                    const had = m_ns.*;
                    m_ns.* += w_ns;
                    switch (a.op) {
                        .count_star, .count_col, .count_distinct => {},
                        .sum, .avg => {
                            if (a.is_float) {
                                setSlotF64(m_slot, slotF64(m_slot.*) + slotF64(w_slot));
                            } else {
                                m_slot.* += w_slot;
                            }
                        },
                        .min => {
                            if (a.is_float) {
                                if (had == 0 or slotF64(w_slot) < slotF64(m_slot.*)) m_slot.* = w_slot;
                            } else {
                                if (had == 0 or w_slot < m_slot.*) m_slot.* = w_slot;
                            }
                        },
                        .max => {
                            if (a.is_float) {
                                if (had == 0 or slotF64(w_slot) > slotF64(m_slot.*)) m_slot.* = w_slot;
                            } else {
                                if (had == 0 or w_slot > m_slot.*) m_slot.* = w_slot;
                            }
                        },
                    }
                }
            }
        }
    }

    fn mergeDistinct(self: *LowCardGroup, workers: []Worker, n_workers: usize, cpus: []const usize, merged: *Merged) !void {
        const allocator = self.allocator;
        if (self.n_distinct == 0) return;
        const n_groups = merged.counts.items.len;
        merged.dcounts = try allocator.alloc(u64, n_groups * self.n_distinct);
        @memset(merged.dcounts, 0);
        if (n_groups == 0) return;

        const jobs = try allocator.alloc(PartMergeJob, n_workers);
        defer allocator.free(jobs);
        const count_tables = try allocator.alloc(CountSlotTable, n_workers * self.n_distinct);
        defer allocator.free(count_tables);
        @memset(count_tables, CountSlotTable.empty);
        defer for (count_tables) |*t| {
            if (t.slots.len > 0) t.deinit(allocator);
        };
        for (jobs, 0..) |*j, p| {
            j.* = .{
                .part = p,
                .dop_parts = n_workers,
                .workers = workers,
                .aggs = self.aggs,
                .n_distinct = self.n_distinct,
                .allocator = allocator,
                .cpu = if (cpus.len == 0) null else cpus[p % cpus.len],
                .counts = count_tables[p * self.n_distinct ..][0..self.n_distinct],
            };
        }

        const threads = try allocator.alloc(std.Thread, n_workers);
        defer allocator.free(threads);
        const spawned = try allocator.alloc(bool, n_workers);
        defer allocator.free(spawned);
        @memset(spawned, false);
        for (jobs, 0..) |*j, p| {
            if (std.Thread.spawn(.{}, partMergeMain, .{j})) |th| {
                threads[p] = th;
                spawned[p] = true;
            } else |_| {}
        }
        for (jobs, 0..) |*j, p| if (!spawned[p]) {
            partMergeRun(j) catch |e| {
                j.err = e;
            };
        };
        for (0..n_workers) |p| if (spawned[p]) threads[p].join();
        for (jobs) |*j| if (j.err) |e| return e;

        // Fold each partition's per-group counts onto the merged groups. Every
        // counted key folded scalar state in some worker, so it must resolve.
        for (jobs) |*j| {
            for (self.aggs) |a| {
                if (a.op != .count_distinct) continue;
                const d: usize = a.distinct_index;
                const cnt = &j.counts[d];
                if (cnt.slots.len > 0) {
                    for (cnt.slots) |s| {
                        if (s.key == CountSlotTable.SENTINEL) continue;
                        try addDistinctCount(merged, self.n_distinct, d, s.key, s.count);
                    }
                }
                if (cnt.has_sentinel) {
                    try addDistinctCount(merged, self.n_distinct, d, CountSlotTable.SENTINEL, cnt.sentinel_count);
                }
            }
        }
    }

    fn emitSorted(self: *LowCardGroup, merged: *Merged) !void {
        const allocator = self.allocator;
        const n_groups = merged.counts.items.len;
        const order = try allocator.alloc(u32, n_groups);
        defer allocator.free(order);
        for (order, 0..) |*o, i| o.* = @intCast(i);

        if (self.order_specs.len > 0) {
            const ctx = SortCtx{ .op = self, .merged = merged };
            std.sort.pdq(u32, order, ctx, SortCtx.lessThan);
        }

        const start = @min(self.offset, n_groups);
        const end = if (self.limit == 0) n_groups else @min(start + self.limit, n_groups);
        for (order[start..end]) |gid| {
            try self.emitGroup(merged, gid);
        }
        self.row_count = end - start;
    }

    fn emitGroup(self: *LowCardGroup, merged: *Merged, gid: u32) !void {
        const a = self.allocator;
        const n_aggs = self.aggs.len;
        const key = keyOfGid(merged, gid);
        for (self.parts[0..self.part_count], 0..) |p, i| {
            if (p.coded) {
                const code: u32 = @intCast(truncBits(key >> @intCast(p.offset), 32));
                try appendString(a, &self.output_cols[i], self.dicts[i].?.decode(code));
            } else {
                try appendInt(a, &self.output_cols[i], p.typ, unpackKeyPart(key, p));
            }
        }
        for (self.aggs, 0..) |agg, i| {
            const col = &self.output_cols[self.part_count + i];
            const row = col.data.rowCount();
            const ns = merged.ns.items[@as(usize, gid) * n_aggs + i];
            const slot = merged.slots.items[@as(usize, gid) * n_aggs + i];
            // SUM/AVG/MIN/MAX over a group with zero non-NULL inputs is SQL
            // NULL (the group exists via COUNT(*); its values were all NULL).
            var is_null = false;
            switch (agg.op) {
                .count_star => try col.data.bigint.append(a, @intCast(merged.counts.items[gid])),
                .count_col => try col.data.bigint.append(a, @intCast(ns)),
                .count_distinct => try col.data.bigint.append(a, @intCast(merged.dcounts[@as(usize, gid) * self.n_distinct + agg.distinct_index])),
                .sum => {
                    if (ns == 0) {
                        try col.data.appendNullPlaceholder(a);
                        is_null = true;
                    } else if (agg.is_float) {
                        try appendFloat(a, col, agg.output_type, slotF64(slot));
                    } else {
                        try appendInt(a, col, agg.output_type, slot);
                    }
                },
                .avg => {
                    if (ns == 0) {
                        try col.data.appendNullPlaceholder(a);
                        is_null = true;
                    } else {
                        const avg: f64 = if (agg.is_float)
                            slotF64(slot) / @as(f64, @floatFromInt(ns))
                        else
                            @as(f64, @floatFromInt(slot)) / @as(f64, @floatFromInt(ns)) / std.math.pow(f64, 10.0, @floatFromInt(agg.input_scale));
                        try col.data.double.append(a, avg);
                    }
                },
                .min, .max => {
                    if (ns == 0) {
                        try col.data.appendNullPlaceholder(a);
                        is_null = true;
                    } else if (agg.is_float) {
                        try appendFloat(a, col, agg.output_type, slotF64(slot));
                    } else {
                        try appendInt(a, col, agg.output_type, slot);
                    }
                },
            }
            try col.appendValidBit(a, row, !is_null);
        }
    }
};

fn addDistinctCount(merged: *Merged, n_distinct: u16, d: usize, key: u64, count: u64) !void {
    const probe = merged.table.getOrPut(GroupTable.hashKey(key), key);
    if (!probe.found) return error.UnsupportedOperatorForType;
    merged.dcounts[@as(usize, probe.gid) * n_distinct + d] += count;
}

inline fn keyOfGid(merged: *const Merged, gid: u32) u64 {
    return merged.keys.items[gid];
}

const SortCtx = struct {
    op: *LowCardGroup,
    merged: *Merged,

    fn lessThan(ctx: SortCtx, a_gid: u32, b_gid: u32) bool {
        for (ctx.op.order_specs) |spec| {
            const ord = compareBySpec(ctx, spec, a_gid, b_gid);
            if (ord == .lt) return !spec.desc;
            if (ord == .gt) return spec.desc;
        }
        return a_gid < b_gid;
    }
};

fn compareBySpec(ctx: SortCtx, spec: SortSpec, a_gid: u32, b_gid: u32) std.math.Order {
    const op = ctx.op;
    const merged = ctx.merged;
    for (op.parts[0..op.part_count], 0..) |p, pi| {
        if (types.columnNameEql(p.name, spec.col)) {
            if (p.coded) {
                const dict = op.dicts[pi].?;
                const ac: u32 = @intCast(truncBits(keyOfGid(merged, a_gid) >> @intCast(p.offset), 32));
                const bc: u32 = @intCast(truncBits(keyOfGid(merged, b_gid) >> @intCast(p.offset), 32));
                return std.mem.order(u8, dict.decode(ac), dict.decode(bc));
            }
            const av = unpackKeyPart(keyOfGid(merged, a_gid), p);
            const bv = unpackKeyPart(keyOfGid(merged, b_gid), p);
            return std.math.order(av, bv);
        }
    }
    const n_aggs = op.aggs.len;
    for (op.aggs, 0..) |agg, i| {
        if (!types.columnNameEql(agg.name, spec.col)) continue;
        switch (agg.op) {
            .count_star => return std.math.order(merged.counts.items[a_gid], merged.counts.items[b_gid]),
            .count_col => return std.math.order(merged.ns.items[@as(usize, a_gid) * n_aggs + i], merged.ns.items[@as(usize, b_gid) * n_aggs + i]),
            .count_distinct => return std.math.order(
                merged.dcounts[@as(usize, a_gid) * op.n_distinct + agg.distinct_index],
                merged.dcounts[@as(usize, b_gid) * op.n_distinct + agg.distinct_index],
            ),
            .sum, .min, .max => {
                const as_ = merged.slots.items[@as(usize, a_gid) * n_aggs + i];
                const bs = merged.slots.items[@as(usize, b_gid) * n_aggs + i];
                if (agg.is_float) return std.math.order(slotF64(as_), slotF64(bs));
                return std.math.order(as_, bs);
            },
            .avg => {
                const an = merged.ns.items[@as(usize, a_gid) * n_aggs + i];
                const bn = merged.ns.items[@as(usize, b_gid) * n_aggs + i];
                const as_ = merged.slots.items[@as(usize, a_gid) * n_aggs + i];
                const bs = merged.slots.items[@as(usize, b_gid) * n_aggs + i];
                const af: f64 = if (an == 0) 0 else if (agg.is_float) slotF64(as_) / @as(f64, @floatFromInt(an)) else @as(f64, @floatFromInt(as_)) / @as(f64, @floatFromInt(an));
                const bf: f64 = if (bn == 0) 0 else if (agg.is_float) slotF64(bs) / @as(f64, @floatFromInt(bn)) else @as(f64, @floatFromInt(bs)) / @as(f64, @floatFromInt(bn));
                return std.math.order(af, bf);
            },
        }
    }
    return .eq;
}

// Extract one key part and sign-extend it back to the column's value domain.
inline fn unpackKeyPart(key: u64, p: KeyPart) i128 {
    const raw = truncBits(key >> @intCast(p.offset), p.width);
    if (p.width >= 64) return @as(i64, @bitCast(raw));
    const shift: u6 = @intCast(64 - @as(u16, p.width));
    return (@as(i64, @bitCast(raw << shift))) >> shift;
}

fn appendInt(allocator: Allocator, col: *ColumnStore, out_type: Type, value: i128) !void {
    switch (out_type) {
        .boolean => try col.data.boolean.append(allocator, @intCast(value)),
        .tinyint => try col.data.tinyint.append(allocator, @intCast(value)),
        .smallint => try col.data.smallint.append(allocator, @intCast(value)),
        .int => try col.data.int.append(allocator, @intCast(value)),
        .date => try col.data.date.append(allocator, @intCast(value)),
        .bigint => try col.data.bigint.append(allocator, @intCast(value)),
        .datetime => try col.data.datetime.append(allocator, @intCast(value)),
        .decimal64 => try col.data.decimal64.append(allocator, @intCast(value)),
        .decimal128 => try col.data.decimal128.append(allocator, value),
        .largeint => try col.data.largeint.append(allocator, value),
        .double => try col.data.double.append(allocator, @floatFromInt(value)),
        else => return error.TypeMismatch,
    }
}

fn appendString(allocator: Allocator, col: *ColumnStore, s: []const u8) !void {
    switch (col.data) {
        .varchar, .string, .char, .json => |*ss| try ss.appendValue(allocator, s),
        else => return error.TypeMismatch,
    }
}

fn appendFloat(allocator: Allocator, col: *ColumnStore, out_type: Type, value: f64) !void {
    switch (out_type) {
        .float => try col.data.float.append(allocator, @floatCast(value)),
        .double => try col.data.double.append(allocator, value),
        else => return error.TypeMismatch,
    }
}
