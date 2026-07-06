//! Aggregate / GROUP BY operator. Drains the upstream into a per-aggregate
//! accumulator (single global slot, or hash-keyed per group), then emits
//! one batch with the final results.
//!
//! Supported aggregates: COUNT, SUM, MIN, MAX, AVG. SUM/MIN/MAX/AVG dispatch
//! over input column type to choose the right accumulator state.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

const simd = @import("../util/simd.zig");

const native_endian = @import("builtin").cpu.arch.endian();

/// Phase 4.2 (Option A): a single string group key delivered as global dict
/// codes (via `Batch.coded`) instead of materialized strings. `dict` decodes a
/// code back to its string at emit.
const CodedKey = struct { dict: *exec.GlobalDict };

const group_table = @import("group_table.zig");
const ByteGroupTable = group_table.ByteGroupTable;
const IntGroupTable = group_table.IntGroupTable;
/// Per-tier integer group tables. The active one is selected by
/// `int_layout.?.tier`; the unused two stay `null`.
const IntTable32 = group_table.IntKeyTable(32);
const IntTable96 = group_table.IntKeyTable(96);
const IntTable128 = group_table.IntKeyTable(128);
const DistinctU64Set = group_table.DistinctU64Set;
const DistinctU32Set = group_table.DistinctU32Set;
const CountSlotTable = group_table.CountSlotTable;
/// FOR-narrow inline-state group tables for the single-int-key + single
/// inline aggregate fast path. The state is always `i64` (SUM over ≤32-bit
/// int families stays inside i64 by the eligibility gate; MIN/MAX fold to
/// i64), so only the key width tiers. The active one is selected by
/// `inline_for.?.tier`; the others stay `null`.
const InlineState = i64;
const InlineTable8 = group_table.InlineSlotTable(u8, InlineState);
const InlineTable16 = group_table.InlineSlotTable(u16, InlineState);
const InlineTable32 = group_table.InlineSlotTable(u32, InlineState);
const InlineTable64 = group_table.InlineSlotTable(u64, InlineState);

pub const AggFunc = enum {
    count,
    sum,
    min,
    max,
    avg,
    /// Count rows where a boolean predicate is true.
    count_if,
    /// Boolean conjunction/disjunction over non-null inputs.
    bool_and,
    bool_or,
    /// Return an arbitrary/first/last non-null value.
    any_value,
    first,
    last,
    /// Return the value from the row whose second argument is maximal.
    max_by,
    /// Bitwise aggregates over integer-family inputs.
    bit_and,
    bit_or,
    bit_xor,
    /// DISTINCT numeric SUM/AVG; keeps observed values and dedupes at finalize.
    sum_distinct,
    avg_distinct,
    /// Population stddev. sqrt(sum((x-mean)^2) / n). 0 when n=0.
    stddev_pop,
    /// Sample stddev. sqrt(sum((x-mean)^2) / (n-1)). 0 when n<2.
    stddev_samp,
    /// Population variance. sum((x-mean)^2) / n.
    var_pop,
    /// Sample variance. sum((x-mean)^2) / (n-1).
    var_samp,
    /// Exact distinct count. Hash set of dup'd value bytes; output bigint.
    count_distinct,
    /// Exact continuous percentile. params.percentile in [0, 1].
    /// O(N) memory; sorts at finalize. Output double.
    percentile,
    /// Concatenate string values with a separator. params.separator
    /// is prepended before every value after the first. Output string.
    group_concat,
    /// User-defined aggregate. `AggSpec.udf_name` carries the registry name.
    udf,
};

/// True when every aggregate keeps BOUNDED per-group state — so the bare-LIMIT
/// table cap's shared overflow group (which absorbs all post-cap rows) can't
/// grow without bound. COUNT(DISTINCT) (a per-group set), PERCENTILE (keeps every
/// value), GROUP_CONCAT (keeps every value), and UDAFs (opaque state) are
/// excluded; count/sum/min/max/avg/stddev/variance all keep fixed-size state.
fn aggsAllowGroupCap(aggs: []const AggSpec) bool {
    for (aggs) |a| switch (a.func) {
        .count, .sum, .min, .max, .avg, .count_if, .bool_and, .bool_or, .bit_and, .bit_or, .bit_xor, .stddev_pop, .stddev_samp, .var_pop, .var_samp => {},
        .any_value, .first, .last, .max_by, .sum_distinct, .avg_distinct, .count_distinct, .percentile, .group_concat, .udf => return false,
    };
    return true;
}

/// Per-aggregate parameters not expressible via `col` / `as`. `.none`
/// covers all existing aggregates; percentile and group_concat carry
/// their own payload.
pub const AggParams = union(enum) {
    none,
    percentile: f64,
    separator: []const u8,
};

pub const AggSpec = struct {
    func: AggFunc,
    /// Registry name for `.func = .udf`.
    udf_name: ?[]const u8 = null,
    /// UDAF argument columns. Empty for built-ins and unary UDAFs that
    /// use `.col` for backward-compatible client construction.
    udf_arg_cols: []const []const u8 = &.{},
    /// Column to aggregate. `null` is only valid for `COUNT(*)`.
    col: ?[]const u8 = null,
    /// Secondary key column for MAX_BY(value, key).
    arg2_col: ?[]const u8 = null,
    /// Output column name.
    as: []const u8,
    /// Per-function payload. Defaults to `.none` so existing call
    /// sites compile unchanged.
    params: AggParams = .none,
    /// Forces the output column type, overriding `aggOutputType`. Set by
    /// the affine-aggregate reduction (`local.zig`) to keep an integer SUM
    /// base in i128 (`.largeint`) so the post-aggregate derivation
    /// `a·SUM + b·COUNT` runs in i128 with no intermediate i64 narrowing —
    /// matching the direct path's accumulate-in-i128-then-narrow-once
    /// overflow behavior bit-for-bit. `null` ⇒ the canonical type.
    out_type_override: ?Type = null,
};

/// One ORDER BY key in a top-k hint: an aggregate output column name + its
/// sort direction. Mirrors `ir.SortSpec`; kept dependency-free so `aggregate`
/// need not import `ir` (which already depends on this file's `AggSpec`).
pub const TopKKey = struct {
    col: []const u8,
    desc: bool,
};

/// Planner hint: this hash aggregate sits directly under `ORDER BY <keys>
/// LIMIT k`. When every key resolves to a numeric aggregate output, the
/// operator emits only the top-k groups instead of every group (the downstream
/// OrderBy+Limit then re-sort the small set).
pub const TopKHint = struct {
    k: u32,
    keys: []const TopKKey,
};

/// A comparable order value pulled from a finalized accumulator. The active
/// variant follows the accumulator (integer-family vs float), and every group
/// shares the same variant for a given key — so comparisons only ever match
/// `int`↔`int` or `float`↔`float`.
const OrderVal = union(enum) {
    int: i128,
    float: f64,
};

/// One ORDER BY key after binding its column name to a concrete aggregate
/// index. Direction is per-key (mixed ASC/DESC supported).
const ResolvedKey = struct {
    agg_idx: usize,
    desc: bool,
};

/// `TopKHint` after binding every key. Owns `keys` (allocator-backed; freed in
/// `deinit`). `null` (unresolved) means fall back to emitting all groups.
const ResolvedTopK = struct {
    k: usize,
    keys: []ResolvedKey,
};

/// Maximum ORDER BY keys the fusion handles. Beyond this the hint is left
/// unresolved (full emit) — analytic top-N almost never sorts on more keys, so
/// a small inline value cache beats a per-entry allocation.
const MAX_TOPK_KEYS: usize = 4;

/// One group competing for a top-k slot. `key`/`state` borrow into the group
/// hash table (valid only until the result batch is materialized); `vals`
/// caches each ORDER BY key's order value so the heap comparator does no
/// accumulator decoding in its inner loop. Only `vals[0..keys.len]` is live.
const TopKEntry = struct {
    gid: u32,
    vals: [MAX_TOPK_KEYS]OrderVal,
};

/// Heap path is used only when k is modest; beyond this we emit all groups
/// and let the downstream Limit trim, avoiding a large heap allocation for a
/// degenerate `LIMIT <huge>`.
const TOPK_HEAP_CAP: usize = 1 << 16;

/// Backing state for a string COUNT(DISTINCT): the hash set of arena-dup'd
/// value bytes, plus a flag for whether the (single, ≠ NULL) empty string was
/// seen — counted via the flag rather than hashed + probed for every row.
const DistinctStr = struct {
    set: std.StringHashMapUnmanaged(void) = .empty,
    seen_empty: bool = false,
};

/// Per-aggregate accumulator state. Integer types accumulate into i64
/// (MIN/MAX) or i128 (SUM); float/double types accumulate into f64; LARGEINT
/// gets dedicated i128 min/max variants. The final value is cast back to the
/// declared output column type.
pub const AccState = union(enum) {
    count: u64,
    sum_int: SumIntAcc,
    sum_float: SumFloatAcc,
    min_int: ?i64,
    max_int: ?i64,
    min_float: ?f64,
    max_float: ?f64,
    /// MIN/MAX for LARGEINT/DECIMAL128 inputs (don't fit in i64). Held inline
    /// as a presence-flagged struct rather than `?i128` so the i128 can sit at
    /// 8-byte alignment — that keeps the whole `AccState` union at 32 bytes
    /// instead of 48 (the `?i128`'s 16-byte alignment would dominate).
    min_large: LargeAcc,
    max_large: LargeAcc,
    /// MIN/MAX over string-family columns. Holds the running extreme as
    /// arena-dup'd bytes (the view's bytes are transient per batch).
    min_str: ?[]const u8,
    max_str: ?[]const u8,
    avg: AvgAcc,
    bool_acc: BoolAcc,
    value_acc: ValueAcc,
    max_by: MaxByAcc,
    bitwise: BitwiseAcc,
    distinct_numeric: std.ArrayListUnmanaged(f64),

    /// Welford's online algorithm: numerically stable variance/stddev.
    /// Covers stddev_pop, stddev_samp, var_pop, var_samp.
    welford: WelfordAcc,
    /// Exact distinct count: set of arena-dup'd value bytes, plus a flag that
    /// tracks the empty string. `""` is one distinct value (≠ NULL) but is
    /// hugely common in optional text columns; the flag counts it once instead
    /// of hashing + probing the map for every empty row.
    distinct: DistinctStr,
    /// Exact distinct count over a >64-bit integer-family column (largeint /
    /// decimal128 / uuid): the raw value bits are hashed directly (no per-value
    /// byte serialization or key arena-dup). Columns ≤64 bits take the faster
    /// `distinct_int64` set instead.
    distinct_int: std.AutoHashMapUnmanaged(u128, void),
    /// Exact distinct count over an integer-family column ≤64 bits: a key-only
    /// open-addressing set probed via a software-prefetch pipeline. The slot is
    /// 8 bytes (no gid) and the global insert is pure-latency-bound (no group
    /// probe to overlap it), so prefetch + the narrow slot win big over the
    /// generic `AutoHashMap`. Selected by `initialState` for 33–64-bit int inputs.
    distinct_int64: group_table.DistinctU64Set,
    /// Exact distinct count over an integer-family column ≤32 bits: like
    /// `distinct_int64` but a 4-byte slot, halving the set's footprint and the
    /// bytes touched per probe. Selected by `initialState` for ≤32-bit inputs.
    distinct_int32: group_table.DistinctU32Set,
    /// Exact percentile: keep every observed value (as f64), sort + interpolate at finalize.
    percentile_values: std.ArrayListUnmanaged(f64),
    /// group_concat buffer, lazily boxed (the `ConcatAcc` is 32 bytes — too
    /// wide to sit inline without inflating every other group's state — so it
    /// lives behind a pointer, null until the first value is appended).
    concat: ?*ConcatAcc,
};

fn boolUpdate(s: *AccState, func: AggFunc, view: ColumnView, row_start: u32, row_end: u32) !void {
    const vals = view.data.boolean;
    var r: u32 = row_start;
    while (r < row_end) : (r += 1) {
        if (!view.isValid(r)) continue;
        const v = vals[r] != 0;
        if (!s.bool_acc.seen) {
            s.bool_acc.seen = true;
            s.bool_acc.value = v;
        } else if (func == .bool_and) {
            s.bool_acc.value = s.bool_acc.value and v;
        } else {
            s.bool_acc.value = s.bool_acc.value or v;
        }
    }
}

fn valueFromRow(aa: Allocator, view: ColumnView, row: u32) !types.Value {
    return switch (view.data) {
        .int => |v| .{ .int = v[row] },
        .bigint => |v| .{ .bigint = v[row] },
        .boolean => |v| .{ .boolean = v[row] != 0 },
        .float => |v| .{ .float = v[row] },
        .double => |v| .{ .double = v[row] },
        .date => |v| .{ .date = v[row] },
        .datetime => |v| .{ .datetime = v[row] },
        .tinyint => |v| .{ .tinyint = v[row] },
        .smallint => |v| .{ .smallint = v[row] },
        .largeint => |v| .{ .largeint = v[row] },
        .decimal64 => |v| .{ .decimal64 = v[row] },
        .decimal128 => |v| .{ .decimal128 = v[row] },
        .uuid => |v| .{ .uuid = v[row] },
        .varchar, .string, .char, .json => .{ .text = try aa.dupe(u8, stringRowBytes(view, @intCast(row))) },
    };
}

fn valueUpdate(aa: Allocator, s: *AccState, func: AggFunc, view: ColumnView, row_start: u32, row_end: u32) !void {
    var r: u32 = row_start;
    while (r < row_end) : (r += 1) {
        if (!view.isValid(r)) continue;
        if (func == .last or !s.value_acc.seen) {
            s.value_acc.value = try valueFromRow(aa, view, r);
            s.value_acc.seen = true;
        }
        if (func == .any_value or func == .first) break;
    }
}

/// Compare a view cell against a stored Value of the same underlying type
/// WITHOUT materializing the cell — for string keys this skips the arena
/// dupe `valueFromRow` would pay on every row just to compare.
fn rowVsValue(view: ColumnView, row: u32, val: types.Value) std.math.Order {
    return switch (view.data) {
        .int => |v| std.math.order(v[row], val.int),
        .bigint => |v| std.math.order(v[row], val.bigint),
        .boolean => |v| std.math.order(v[row], @intFromBool(val.boolean)),
        .float => |v| std.math.order(v[row], val.float),
        .double => |v| std.math.order(v[row], val.double),
        .date => |v| std.math.order(v[row], val.date),
        .datetime => |v| std.math.order(v[row], val.datetime),
        .tinyint => |v| std.math.order(v[row], val.tinyint),
        .smallint => |v| std.math.order(v[row], val.smallint),
        .largeint => |v| std.math.order(v[row], val.largeint),
        .decimal64 => |v| std.math.order(v[row], val.decimal64),
        .decimal128 => |v| std.math.order(v[row], val.decimal128),
        .uuid => |v| std.math.order(v[row], val.uuid),
        .varchar, .string, .char, .json => std.mem.order(u8, stringRowBytes(view, @intCast(row)), val.text),
    };
}

fn maxByUpdate(aa: Allocator, s: *AccState, value_view: ColumnView, key_view: ColumnView, row_start: u32, row_end: u32) !void {
    var r: u32 = row_start;
    while (r < row_end) : (r += 1) {
        if (!value_view.isValid(r) or !key_view.isValid(r)) continue;
        if (s.max_by.seen and rowVsValue(key_view, r, s.max_by.key) != .gt) continue;
        s.max_by.key = try valueFromRow(aa, key_view, r);
        s.max_by.value = try valueFromRow(aa, value_view, r);
        s.max_by.seen = true;
    }
}

fn appendValueToColumn(allocator: Allocator, col: *ColumnStore, out_type: Type, value: types.Value) !void {
    switch (out_type) {
        .int => try col.data.int.append(allocator, value.int),
        .bigint => try col.data.bigint.append(allocator, value.bigint),
        .boolean => try col.data.boolean.append(allocator, if (value.boolean) 1 else 0),
        .float => try col.data.float.append(allocator, value.float),
        .double => try col.data.double.append(allocator, value.double),
        .date => try col.data.date.append(allocator, value.date),
        .datetime => try col.data.datetime.append(allocator, value.datetime),
        .tinyint => try col.data.tinyint.append(allocator, value.tinyint),
        .smallint => try col.data.smallint.append(allocator, value.smallint),
        .largeint => try col.data.largeint.append(allocator, value.largeint),
        .decimal64 => try col.data.decimal64.append(allocator, value.decimal64),
        .decimal128 => try col.data.decimal128.append(allocator, value.decimal128),
        .uuid => try col.data.uuid.append(allocator, value.uuid),
        .varchar => try col.data.varchar.appendValue(allocator, value.text),
        .string => try col.data.string.appendValue(allocator, value.text),
        .char => try col.data.char.appendValue(allocator, value.text),
        .json => try col.data.json.appendValue(allocator, value.text),
    }
}

fn rowI64(view: ColumnView, row: usize) i64 {
    return switch (view.data) {
        .boolean => |v| v[row],
        .tinyint => |v| v[row],
        .smallint => |v| v[row],
        .int => |v| v[row],
        .bigint => |v| v[row],
        else => unreachable,
    };
}

fn bitwiseUpdate(s: *AccState, func: AggFunc, view: ColumnView, row_start: u32, row_end: u32) !void {
    var r: u32 = row_start;
    while (r < row_end) : (r += 1) {
        if (!view.isValid(r)) continue;
        const v = rowI64(view, r);
        if (!s.bitwise.seen) {
            s.bitwise.seen = true;
            s.bitwise.value = v;
        } else switch (func) {
            .bit_and => s.bitwise.value &= v,
            .bit_or => s.bitwise.value |= v,
            .bit_xor => s.bitwise.value ^= v,
            else => unreachable,
        }
    }
}

fn rowF64(view: ColumnView, row: usize) f64 {
    return switch (view.data) {
        .boolean => |v| @floatFromInt(v[row]),
        .tinyint => |v| @floatFromInt(v[row]),
        .smallint => |v| @floatFromInt(v[row]),
        .int => |v| @floatFromInt(v[row]),
        .bigint, .decimal64 => |v| @floatFromInt(v[row]),
        .largeint, .decimal128 => |v| @floatFromInt(v[row]),
        .float => |v| v[row],
        .double => |v| v[row],
        else => unreachable,
    };
}

fn distinctNumericUpdate(aa: Allocator, s: *AccState, view: ColumnView, row_start: u32, row_end: u32) !void {
    var r: u32 = row_start;
    while (r < row_end) : (r += 1) {
        if (!view.isValid(r)) continue;
        try s.distinct_numeric.append(aa, rowF64(view, r));
    }
}

/// Per-aggregate accumulator storage, one entry per aggregate. The hybrid
/// Struct-of-Arrays replacement for the old flat `[]AccState gstate`: each
/// aggregate gets ONE typed column sized to the group-table capacity and
/// indexed by gid, so a COUNT/SUM/AVG group costs ~40 B instead of the old
/// 96 B (three 32-B `AccState` cells). The narrow variants hold exactly the
/// same semantic value as the matching `AccState` field; the complex
/// aggregates (string MIN/MAX, 128-bit MIN/MAX, welford, all distinct sets,
/// percentile, group_concat, and combined-distinct aggs) keep a real
/// `[]AccState` so the per-row `updateStateRow` path operates on a stable
/// `*AccState` unchanged.
const AggCol = union(enum) {
    count: []u64,
    sum_int: []SumIntAcc,
    sum_float: []SumFloatAcc,
    avg: []AvgAcc,
    min_int: []?i64,
    max_int: []?i64,
    min_float: []?f64,
    max_float: []?f64,
    other: []AccState,
};

/// Choose the `AggCol` kind for an aggregate, mirroring exactly what
/// `initialState` picks for that aggregate's `AccState` variant. The narrow
/// kinds (count/sum/avg/numeric min-max) are stored in their own typed column;
/// everything else (string min/max, 128-bit min/max, welford, distinct sets,
/// percentile, group_concat) lands in `.other`. Combined-distinct aggregates
/// (`cd[ai] != null`) also use `.other` — their cell value is unused (the count
/// is read from the combined counter), so a default-initialized `AccState` does.
const AggColKind = enum { count, sum_int, sum_float, avg, min_int, max_int, min_float, max_float, other };

fn aggColKind(func: AggFunc, in: ?Type) AggColKind {
    return switch (func) {
        .count, .count_if => .count,
        .sum => if (in != null and in.?.isFloat()) .sum_float else .sum_int,
        .avg => .avg,
        .min => if (in != null and in.?.isFloat())
            .min_float
        else if (in != null and (in.?.isString() or in.? == .largeint or in.? == .decimal128))
            .other
        else
            .min_int,
        .max => if (in != null and in.?.isFloat())
            .max_float
        else if (in != null and (in.?.isString() or in.? == .largeint or in.? == .decimal128))
            .other
        else
            .max_int,
        else => .other,
    };
}

/// Byte width of one aggregate's per-group state cell in the SoA layout — the
/// element size of the `AggCol` column that `aggColKind` selects. The GROUP BY
/// router (net/local.zig) sums these to size the hash table against the budget,
/// so it must track the real column widths rather than a flat per-AccState
/// estimate.
pub fn aggStateWidth(func: AggFunc, in: ?Type) usize {
    return switch (aggColKind(func, in)) {
        .count => @sizeOf(u64),
        .sum_int => @sizeOf(SumIntAcc),
        .sum_float => @sizeOf(SumFloatAcc),
        .avg => @sizeOf(AvgAcc),
        .min_int, .max_int => @sizeOf(?i64),
        .min_float, .max_float => @sizeOf(?f64),
        .other => @sizeOf(AccState),
    };
}

/// Inline MIN/MAX accumulator for 128-bit inputs. `align(8)` on the i128 keeps
/// the enclosing `AccState` union 8-aligned (32 B) rather than 16-aligned.
const LargeAcc = struct { v: i128 align(8) = 0, present: bool = false };

/// SUM accumulators carry a `seen` flag so a group (or global input) whose
/// every value was NULL finalizes to SQL NULL rather than the 0 identity.
/// Same align(8) trick as LargeAcc to keep AccState at 32 B.
const SumIntAcc = struct { v: i128 align(8) = 0, seen: bool = false };
const SumFloatAcc = struct { v: f64 = 0, seen: bool = false };

const AvgAcc = struct {
    sum: f64,
    count: u64,
    /// 10^input_scale for a DECIMAL input. AVG sums raw mantissas, so the final
    /// mean is divided by this to recover the true value; 1.0 for int/float.
    scale_div: f64 = 1.0,
};

/// AVG denominator factor for the input's decimal scale (1.0 for non-decimal).
fn avgScaleDiv(in: ?Type) f64 {
    if (in) |t| if (t.decimalSpec()) |sp| return std.math.pow(f64, 10.0, @floatFromInt(sp.s));
    return 1.0;
}

const BoolAcc = struct {
    seen: bool = false,
    value: bool = false,
};

const ValueAcc = struct {
    seen: bool = false,
    value: types.Value = .{ .int = 0 },
};

const MaxByAcc = struct {
    seen: bool = false,
    key: types.Value = .{ .int = 0 },
    value: types.Value = .{ .int = 0 },
};

const BitwiseAcc = struct {
    seen: bool = false,
    value: i64 = 0,
};

const WelfordAcc = struct {
    mean: f64 = 0.0,
    m2: f64 = 0.0,
    count: u64 = 0,
};

const ConcatAcc = struct {
    buf: std.ArrayListUnmanaged(u8) = .empty,
    nonempty: bool = false,
};

/// Batched COUNT(DISTINCT <int col>) state for one aggregate under a GROUP BY.
/// Replaces the per-row insert into each group's own `AccState.distinct_int`
/// (which scatters one hash map per group across the arena) with a single
/// combined open-addressing set keyed on `pack(gid, value)` plus a flat per-gid
/// counter. One probe per row into one contiguous table — the DuckDB shape.
/// Only built when the distinct value column is int-family ≤64 bits, so the
/// `gid << vbits | value_bits` pack stays bijective and fits the 96-bit tier:
/// gid is a u32 (≤32 bits) and vbits ≤ 64, so the packed key is ≤96 bits — it
/// lands in `hi:u32 = gid`, `lo:u64 = value` with no loss, and distinct
/// (gid, value) pairs map 1:1 to distinct keys (an exact per-group count). A
/// >64-bit value (largeint/decimal128/uuid) can't combine (gid + 128 overflows
/// 128) and stays on the per-group set.
/// Combined (gid, value) membership set for grouped COUNT(DISTINCT int). Two
/// tiers by packed-key width: `narrow` (value ≤32 bits ⇒ `gid<<vbits | value`
/// fits a u64, since gid is a u32) uses a key-only 8-byte-slot `DistinctU64Set`;
/// `wide` (value 33–64 bits ⇒ key ≤96 bits) reuses `IntTable96` (16-byte slot)
/// as a pure set with its `gid` slot field unused. The narrow tier halves the
/// bytes moved per probe on this memory-bound insert.
const CombinedSet = union(enum) {
    narrow: DistinctU64Set,
    wide: IntTable96,
};

const CombinedDistinct = struct {
    set: CombinedSet,
    /// Per-GROUP-gid distinct count, indexed by the aggregate's group gid.
    /// `initGroupCells` appends a 0 for every new group.
    counts: std.ArrayListUnmanaged(u64) = .empty,
    /// Bit width of the value column (`intKeyBits(value_type)`, ≤64): the shift
    /// amount that places gid above the value bits in the combined key.
    vbits: u8,
};

/// FOR-narrow key width tier for the inline-state fast path. The summed slot
/// (`{ KeyW, i64 }`) stays ≤16 bytes for every tier, so a probe touches one
/// cache line. The tier is the smallest width whose `range = max − base`
/// leaves the `maxInt(KeyW)` EMPTY sentinel free (`range < maxInt(KeyW)`).
const InlineKeyTier = enum { w8, w16, w32, w64 };

/// The single inline-able aggregate's fold, captured at plan time so the
/// accumulate inner loop is a comptime-specialized scalar fold (no per-row
/// func switch). The agg input column's stored slice type is carried by
/// `read_tag` so the FOR-key read and the value read both specialize.
const InlineAggKind = enum { sum, min, max };

/// Decision + layout for the single-int-key + single-inline-aggregate fast
/// path. The group column's value FOR-normalizes to `code = value − base`,
/// stored in a `KeyW`-wide slot alongside an `i64` accumulator; emit lowers
/// each occupied slot into `gkeys_int` / `gstate`, reconstructing
/// `value = base + code`. `null` ⟺ ineligible (see `planInlineFor`).
const InlineForPlan = struct {
    /// FOR base = the group column's proven min. `value − base ∈ [0, range]`.
    base: i64,
    tier: InlineKeyTier,
    kind: InlineAggKind,
    /// Stored-slice tag of the group column (drives the FOR-key read).
    key_tag: types.TypeTag,
    /// Stored-slice tag of the aggregate's input column (drives the value read).
    val_tag: types.TypeTag,
};

pub const Aggregate = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    upstream: Query,

    /// Index in the *upstream* schema for each group-by column.
    group_col_indices: []usize,
    /// For each agg, index in upstream schema (or null for COUNT(*)).
    agg_col_indices: []?usize,
    /// Each agg's spec (borrowed from caller).
    aggs: []const AggSpec,

    output_schema: []Column,
    output_columns: []ColumnStore,
    views: []ColumnView,

    /// Used only when there are no group-by columns (single global group).
    single_state: []AccState,
    /// Open-addressing key→gid table for the byte-key (string/mixed/compound
    /// non-integer) path. `int_layout != null` ⟺ the integer path is active and
    /// this table is unused.
    byte_table: ByteGroupTable = undefined,
    /// Open-addressing key→gid table for the integer fast path (packed u128
    /// keys), one per slot-size tier. Exactly the tier `int_layout.?.tier`
    /// selects is non-null; the other two stay null. Splitting by tier lets the
    /// common narrow keys use an 8- or 16-byte slot instead of the 32-byte
    /// u128 slot, so the memory-bound probe moves fewer bytes per row.
    int_table_32: ?IntTable32 = null,
    int_table_96: ?IntTable96 = null,
    int_table_128: ?IntTable128 = null,
    /// `null` ⟺ byte-key path; non-null ⟺ all group columns are fixed-width
    /// integer family summing to ≤128 bits, so keys pack into a u128 (no
    /// per-row byte serialization). Owns `fields` (freed in `deinit`).
    int_layout: ?IntKeyLayout = null,
    /// Provable ceiling on the group count (product of group-key NDVs, capped at
    /// `upper_rows`). The group table presizes to a modest `ADAPTIVE_INITIAL`
    /// and, on its first overflow, jumps straight to this ceiling — one grow
    /// suffices because the actual group count can never exceed it. 0 ⟺ no
    /// known estimate, so the table just doubles from its small initial size.
    group_cap: usize = 0,
    /// Count-in-slot fast path for `GROUP BY <single non-nullable int col> …
    /// COUNT(*)`. Non-null ⟺ the gate in `create` fired: a single ≤64-bit int
    /// group column and a single `COUNT(*)` aggregate. The table holds the
    /// running count *inside* each `{key,count}` slot, so accumulate is one
    /// cache miss per row instead of two (probe + separate count bump). At emit
    /// time `next()` lowers each occupied slot into `gkeys_int`/`gstate` in dense
    /// gid order, after which the existing emit / top-k paths run unchanged.
    /// Arena-owned (no explicit free), like the int tables.
    count_table: ?CountSlotTable = null,
    /// FOR-narrow inline-state fast path for `GROUP BY <single int col> …
    /// {SUM|MIN|MAX}(<int col>)`. Non-null ⟺ `planInlineFor` accepted: a
    /// single ≤64-bit int group column with a known FOR range and one
    /// inline-able aggregate whose `{ KeyW, i64 }` slot stays ≤16 bytes. The
    /// running accumulator lives *inside* each slot next to its FOR-narrowed
    /// key (gid = slot position, no stored gid), so accumulate is one cache
    /// miss per row. At emit `next()` lowers each occupied slot into
    /// `gkeys_int` / `gstate` in dense gid order, after which the existing
    /// emit / top-k paths run unchanged. Arena-owned, like the int tables.
    inline_for: ?InlineForPlan = null,
    /// The active FOR-narrow inline table, one per key-width tier
    /// (`inline_for.?.tier` selects it); the other three stay `null`. Split
    /// by tier so the common narrow keys use a 2/4-byte key slot.
    inline_table_8: ?InlineTable8 = null,
    inline_table_16: ?InlineTable16 = null,
    inline_table_32: ?InlineTable32 = null,
    inline_table_64: ?InlineTable64 = null,
    /// Per-aggregate accumulator columns (hybrid Struct-of-Arrays), one entry
    /// per aggregate. Each entry is a typed slice sized to the group-table
    /// capacity and indexed by gid, so group `g`'s state for aggregate `ai`
    /// is `agg_cols[ai].<kind>[g]`. Replaces the old flat `[]AccState gstate`
    /// (which paid one 32-B `AccState` cell per (group, aggregate)); the narrow
    /// per-aggregate columns cut a COUNT/SUM/AVG group from 96 B to ~40 B.
    /// Arena-owned (the backing slices live in `arena`), like `gstate` was.
    agg_cols: []AggCol = &.{},
    /// Reused scratch (arena, length `aggs.len`) into which the emit paths
    /// gather one group's columns back into `[]AccState`, so the existing
    /// `appendGroupRow` / `topkEntry` (which take `[]AccState`) run unchanged.
    state_scratch: []AccState = &.{},
    /// Per-group key bytes, indexed by gid (arena-owned). Byte-key path only.
    /// Lets emit reconstruct each group's key columns in gid order without
    /// touching the hash table.
    gkeys: std.ArrayListUnmanaged([]const u8) = .empty,
    /// Per-group packed u128 key, indexed by gid. Integer fast path only — the
    /// emit pass unpacks each back into the group output columns.
    gkeys_int: std.ArrayListUnmanaged(u128) = .empty,
    /// Number of distinct groups seen = next gid to assign.
    n_groups: u32 = 0,
    /// Prefetch-pipeline scratch (byte path): per-row hash + key-slice for the
    /// current batch. Phase (a) fills these; phase (b) probes with look-ahead.
    /// Grown lazily to the batch row count; arena-owned.
    pf_hashes: std.ArrayListUnmanaged(u64) = .empty,
    pf_keys: std.ArrayListUnmanaged([]const u8) = .empty,
    /// Compound byte-key phase-(a) scratch: all of a batch's serialized keys are
    /// packed back-to-back into `pf_key_blob` (cleared per batch, not per row),
    /// with each row's (offset,len) in `pf_key_spans`. `pf_keys` slices are
    /// resolved into the blob once it's fully built. This replaces a per-row
    /// arena dup (most of which were wasted on existing-group rows and never
    /// freed until query end); only *new* groups now copy their key into `gkeys`.
    pf_key_blob: std.ArrayList(u8) = .empty,
    pf_key_spans: std.ArrayListUnmanaged([2]u32) = .empty,
    /// Prefetch-pipeline scratch (integer path): per-row hash + packed u128 key.
    pf_int_keys: std.ArrayListUnmanaged(u128) = .empty,
    /// Per-row resolved group id for the current batch. Both key paths fill
    /// this in their phase-(b) probe loop instead of updating accumulators
    /// inline; the batched scatter-update kernels then consume it once per
    /// aggregate. Arena-owned, cleared per batch.
    pf_gids: std.ArrayListUnmanaged(u32) = .empty,
    /// Per-aggregate combined COUNT(DISTINCT int) state, or `null` when the
    /// aggregate stays on its existing path (not count_distinct, no GROUP BY,
    /// or a non-int / >64-bit / float / string distinct column). Indexed by
    /// aggregate index `ai`. Arena-owned (tables + counts), like `gstate`.
    cd: []?CombinedDistinct,
    /// Per-batch scratch for the combined-distinct kernel: each valid row's
    /// packed `(gid, value)` key + its hash. Cleared per batch, reused across
    /// the combined-distinct aggregates within a batch. Arena-owned.
    pf_cd_keys: std.ArrayListUnmanaged(u128) = .empty,
    pf_cd_hashes: std.ArrayListUnmanaged(u64) = .empty,
    /// Reusable buffer for building per-row group keys during accumulate.
    /// Allocated once, grown to max-key-size, cleared+reused per row.
    /// Saves ~1 arena alloc per row in the inner loop.
    key_scratch: std.ArrayList(u8),
    /// True when grouping by exactly one string-typed column. The group key
    /// is then the row's raw string bytes (already decoded in the batch), so
    /// the per-row key build skips the scratch copy + length prefix and the
    /// stored key needs no decoding on output.
    single_str_key: bool = false,

    /// Phase 4.2 (Option A): when set, the single string group key arrives as
    /// global dict CODES via `Batch.coded[group_col_indices[0]]`. The byte path
    /// keys on the 4 code-bytes (the scan already skipped the dict→string
    /// expansion — the win); emit decodes the code back to a string via `dict`.
    /// Set by the compile gate post-construction; null = normal string keys.
    coded_key: ?CodedKey = null,
    /// Per-batch scratch holding global codes interned from a NON-coded (string)
    /// batch under `coded_key` — e.g. a tombstoned segment or the memtable, which
    /// the scan emits as strings. Normalizing those to codes (same GlobalDict)
    /// keeps a query that mixes coded and string batches grouping on ONE code
    /// space (else the same value would split into a code-keyed and a
    /// string-keyed group). Reused across batches; arena-backed.
    coded_scratch: std.ArrayListUnmanaged(u32) = .empty,

    /// Resolved top-k hint, or null to emit every group (the default).
    top_k: ?ResolvedTopK = null,

    /// Emit cap from an unordered `GROUP BY … LIMIT n`: stop after this many
    /// groups (in group-insertion order). null = emit every group. The emitted
    /// groups' counts are exact (every matching row still updates them); only the
    /// emit is bounded. Mutually exclusive with `top_k`.
    emit_limit: ?u32 = null,

    /// Table cap for the `emit_limit` path: once `emit_limit` distinct groups
    /// exist, route every further new key into one never-emitted `overflow_gid`
    /// instead of growing the table to millions of dead groups. Output is
    /// identical (the first n groups still see all their rows), only far cheaper.
    /// Enabled only when every aggregate has bounded per-group state (the
    /// overflow group accumulates too, so distinct/percentile/group_concat — which
    /// grow without bound — disable it and keep the build-everything path).
    cap_groups: bool = false,
    overflow_gid: ?u32 = null,

    emitted: bool = false,
    /// Bytes charged against the query budget for the group hash table /
    /// accumulator state (held in `arena`). Released when the single
    /// result batch has been built — the input is no longer needed.
    reserved_bytes: usize = 0,
    evicted: bool = false,

    /// Propagated per-output-column stats: group-key columns carry their
    /// input stats; aggregate-output columns are bounded by the group count
    /// (≤ `upper_rows`) with unknown min/max. Computed at create from the
    /// upstream stats; arena-owned, borrowed by `stats()`. Empty when the
    /// upstream carries no per-column array.
    cached_stats: []const exec.ColStat = &.{},
    /// Provable upper bound on the output row (group) count, cached at create
    /// so `stats()` doesn't recompute the product. For a global aggregate
    /// (no group columns) this is 1.
    cached_upper_rows: u64 = 1,

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        group_cols: []const []const u8,
        aggs: []const AggSpec,
        top_k: ?TopKHint,
        emit_limit: ?u32,
    ) !Query {
        if (aggs.len == 0) return Error.AggregateNoSpecs;
        const up_schema = upstream.outputSchema();

        // Resolve group-by column indices.
        const group_col_indices = try allocator.alloc(usize, group_cols.len);
        errdefer allocator.free(group_col_indices);
        for (group_cols, 0..) |name, i| {
            group_col_indices[i] = types.findColumn(up_schema, name) orelse return Error.ColumnNotFound;
        }

        // Resolve agg column indices and build output schema.
        const agg_col_indices = try allocator.alloc(?usize, aggs.len);
        errdefer allocator.free(agg_col_indices);

        const output_schema = try allocator.alloc(Column, group_cols.len + aggs.len);
        errdefer allocator.free(output_schema);

        for (group_col_indices, 0..) |src_idx, i| {
            output_schema[i] = up_schema[src_idx];
        }

        for (aggs, 0..) |a, i| {
            agg_col_indices[i] = if (a.col) |name|
                (types.findColumn(up_schema, name) orelse return Error.ColumnNotFound)
            else
                null;

            output_schema[group_cols.len + i] = .{
                .name = a.as,
                .type = try aggOutputTypeFor(a, if (agg_col_indices[i]) |idx| up_schema[idx].type else null),
                // Every aggregate but the COUNT family finalizes to NULL over
                // zero qualifying inputs.
                .nullable = aggOutputNullable(a.func),
            };
        }

        for (aggs, agg_col_indices) |a, maybe_idx| {
            const t = if (maybe_idx) |idx| up_schema[idx].type else null;
            const arg2_t: ?Type = if (a.arg2_col) |name| blk: {
                const idx = types.findColumn(up_schema, name) orelse return Error.ColumnNotFound;
                break :blk up_schema[idx].type;
            } else null;
            try validateAggFn(a.func, t, a.params, arg2_t);
        }

        const output_columns = try allocator.alloc(ColumnStore, output_schema.len);
        errdefer allocator.free(output_columns);
        var inited: usize = 0;
        errdefer for (output_columns[0..inited]) |*c| c.deinit(allocator);
        for (output_schema, 0..) |col, i| {
            output_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        errdefer allocator.free(views);

        const single_state = try allocator.alloc(AccState, aggs.len);
        errdefer allocator.free(single_state);
        for (aggs, agg_col_indices, single_state) |a, idx, *s| {
            const in_t: ?Type = if (idx) |i| up_schema[i].type else null;
            s.* = initialState(a.func, in_t);
        }

        // Per-aggregate combined-distinct slot, populated below for those
        // aggregates the gate selects. Start all-null; the arena-backed table /
        // counts are filled once the arena exists (the `group_col_indices.len >
        // 0` block), since the combined path requires a GROUP BY.
        const cd = try allocator.alloc(?CombinedDistinct, aggs.len);
        errdefer allocator.free(cd);
        for (cd) |*c| c.* = null;

        // Resolve the top-k hint against this aggregate's output. Fuses only
        // when grouping and *every* order key binds to a numeric aggregate
        // output (string MIN/MAX, stddev/variance, percentile, group_concat,
        // and group-key columns have no `OrderVal` — any such key leaves the
        // whole hint unresolved so the operator falls back to a full emit).
        const resolved_top_k = try resolveTopK(allocator, top_k, aggs, group_cols.len, output_schema);
        errdefer if (resolved_top_k) |r| allocator.free(r.keys);

        // Integer fast path: every group column is a fixed-width integer family
        // type whose widths sum to ≤128 bits ⇒ keys pack into a u128, skipping
        // the per-row byte serialization that dominates high-card GROUP BY.
        const int_layout = try planIntKey(allocator, group_col_indices, up_schema, null, &.{});
        errdefer if (int_layout) |l| l.deinit(allocator);

        const self = try allocator.create(Aggregate);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .upstream = upstream,
            .group_col_indices = group_col_indices,
            .agg_col_indices = agg_col_indices,
            .aggs = aggs,
            .output_schema = output_schema,
            .output_columns = output_columns,
            .views = views,
            .single_state = single_state,
            .cd = cd,
            .int_layout = int_layout,
            .key_scratch = .empty,
            // Raw-bytes single-string-key shortcut: a NULL key has no raw-byte
            // representation distinct from '' (and the raw emit writes no
            // validity bits), so nullable keys stay on the tagged compound
            // path. The coded (dict) upgrade keys off this flag, so it is
            // gated too.
            .single_str_key = group_col_indices.len == 1 and
                !up_schema[group_col_indices[0]].nullable and
                switch (up_schema[group_col_indices[0]].type) {
                    .string, .varchar, .char, .json => true,
                    else => false,
                },
            .top_k = resolved_top_k,
            // Only the hash-grouped emit honors the cap; a global aggregate is
            // one row already, and the top-k path owns its own bounded emit.
            .emit_limit = if (group_col_indices.len > 0 and resolved_top_k == null) emit_limit else null,
            .cap_groups = group_col_indices.len > 0 and resolved_top_k == null and emit_limit != null and aggsAllowGroupCap(aggs),
        };

        try self.computeOutputStats(up_schema);

        // No-GROUP-BY COUNT(DISTINCT int≤64): presize the membership set from
        // the value column's cardinality estimate (capped at PRESIZE_CAP) so the
        // 5M-row insert grows+rehashes only a couple of times on its way to ~1M
        // entries. (`DistinctU64Set` stays 24 bytes to fit `AccState`, so it has
        // no grow_target — unlike the grouped combined set below, it can only cap
        // + double.) The cap protects a selective-filter case (Filter.stats pass
        // `upper_rows` through) from a multi-MB zero-fill for a small set.
        if (group_col_indices.len == 0) {
            const st = self.upstream.stats();
            const aa = self.arena.allocator();
            for (aggs, agg_col_indices, single_state) |a, maybe_idx, *s| {
                if (a.func != .count_distinct) continue;
                const idx = maybe_idx orelse continue;
                const vb = intKeyBits(up_schema[idx].type) orelse continue;
                if (vb > 64) continue;
                var set_cap: usize = 0;
                if (idx < st.column_stats.len) switch (st.column_stats[idx].ndv) {
                    .exact => |nd| set_cap = @intCast(@min(nd, @max(st.upper_rows, 1))),
                    .unknown => {},
                };
                set_cap = @min(set_cap, PRESIZE_CAP);
                if (set_cap > 0) {
                    s.* = if (vb <= 32)
                        .{ .distinct_int32 = DistinctU32Set.init(aa, set_cap) catch DistinctU32Set.empty }
                    else
                        .{ .distinct_int64 = DistinctU64Set.init(aa, set_cap) catch DistinctU64Set.empty };
                }
            }
        }

        // Pre-size the group table + flat state from the upstream cardinality
        // estimate so they don't grow+rehash repeatedly as they fill toward
        // their final size (a high-card GROUP BY otherwise rehashes ~log2(N)
        // times, re-moving every live entry each time). The router only sends
        // us here when this count fits the budget, so the up-front allocation
        // is safe. Below the threshold we still init a small table.
        if (group_col_indices.len > 0) {
            // Per-group gather scratch shared by every grouped emit path.
            self.state_scratch = try self.arena.allocator().alloc(AccState, aggs.len);
            const st = self.upstream.stats();
            var est: u64 = 1;
            var known = true;
            for (group_col_indices) |ci| {
                if (ci >= st.column_stats.len) {
                    known = false;
                    break;
                }
                switch (st.column_stats[ci].ndv) {
                    .exact => |nd| est *|= nd,
                    .unknown => {
                        known = false;
                        break;
                    },
                }
            }
            const aa = self.arena.allocator();
            const cap: usize = if (known and est > 1024)
                @intCast(@min(est, @max(st.upper_rows, 1)))
            else
                0;

            // Count-in-slot fast path: one non-nullable ≤64-bit int group column
            // and one `COUNT(*)`. The single group column being non-nullable means
            // there is no NULL group to special-case, so the count can live inside
            // the slot. `int_layout` is already non-null for a single ≤64-bit int
            // column (it summed to ≤128 bits) — confirm the width here so the key
            // injects into a u64 losslessly. When it fires, the generic int group
            // table + flat-state presize below is skipped (the count table owns
            // accumulate); `next()` lowers the slots into `gstate`/`gkeys_int`.
            const count_slot_ok = group_col_indices.len == 1 and
                !up_schema[group_col_indices[0]].nullable and
                (intKeyBits(up_schema[group_col_indices[0]].type) orelse 128) <= 64 and
                aggs.len == 1 and aggs[0].func == .count and agg_col_indices[0] == null;
            // FOR-narrow inline-state fast path (additive to count-in-slot):
            // single int key + one inline SUM/MIN/MAX. Only consulted when the
            // COUNT path didn't fire. `null` ⟺ ineligible → generic int / byte
            // path below.
            self.inline_for = if (count_slot_ok)
                null
            else
                planInlineFor(group_col_indices, agg_col_indices, aggs, up_schema, st);
            // `cap` is the provable group-count ceiling. The group table starts
            // at the modest `ADAPTIVE_INITIAL` (not the ceiling) and, on its
            // first overflow, grows straight to the ceiling — `grow_target`
            // carries that jump into `group_table.grow`. `gstate`/`gkeys`
            // follow the table's capacity (coupled in the accumulate paths after
            // each grow), so they don't repeatedly double-copy as groups append.
            // The count-table fast path (Q15) stays on the ceiling presize.
            self.group_cap = cap;
            const init_cap: usize = if (cap > 0) @min(cap, ADAPTIVE_INITIAL) else 0;
            if (count_slot_ok) {
                self.count_table = CountSlotTable.init(aa, cap) catch CountSlotTable.empty;
            } else if (self.inline_for) |plan| {
                // Presize the inline table to the provable ceiling like the
                // count table — its state rides with the slots through grow, so
                // there is no separate `gstate` array to keep in lockstep.
                switch (plan.tier) {
                    .w8 => self.inline_table_8 = InlineTable8.init(aa, cap) catch InlineTable8.empty,
                    .w16 => self.inline_table_16 = InlineTable16.init(aa, cap) catch InlineTable16.empty,
                    .w32 => self.inline_table_32 = InlineTable32.init(aa, cap) catch InlineTable32.empty,
                    .w64 => self.inline_table_64 = InlineTable64.init(aa, cap) catch InlineTable64.empty,
                }
            } else if (self.int_layout) |layout| {
                const slots: usize = switch (layout.tier) {
                    .bits32 => blk: {
                        self.int_table_32 = IntTable32.init(aa, init_cap) catch try IntTable32.init(aa, 0);
                        if (cap > 0) self.int_table_32.?.grow_target = group_table.capacityFor(cap);
                        break :blk self.int_table_32.?.slots.len;
                    },
                    .bits96 => blk: {
                        self.int_table_96 = IntTable96.init(aa, init_cap) catch try IntTable96.init(aa, 0);
                        if (cap > 0) self.int_table_96.?.grow_target = group_table.capacityFor(cap);
                        break :blk self.int_table_96.?.slots.len;
                    },
                    .bits128 => blk: {
                        self.int_table_128 = IntTable128.init(aa, init_cap) catch try IntTable128.init(aa, 0);
                        if (cap > 0) self.int_table_128.?.grow_target = group_table.capacityFor(cap);
                        break :blk self.int_table_128.?.slots.len;
                    },
                };
                if (init_cap > 0) {
                    try self.allocAggCols(aa, slots);
                    self.gkeys_int.ensureTotalCapacity(aa, init_cap) catch {};
                }
            } else {
                self.byte_table = ByteGroupTable.init(aa, init_cap) catch try ByteGroupTable.init(aa, 0);
                if (cap > 0) self.byte_table.grow_target = group_table.capacityFor(cap);
                if (init_cap > 0) {
                    try self.allocAggCols(aa, self.byte_table.slots.len);
                    self.gkeys.ensureTotalCapacity(aa, init_cap) catch {};
                }
            }

            // Gate the combined COUNT(DISTINCT int) kernel per aggregate. The
            // combined set holds up to one entry per distinct (gid, value) pair,
            // so presize from the value column's cardinality estimate (the
            // dominant term) capped at the upstream row bound. The cap is the
            // load-bearing part: `upper_rows` is the *scan's* estimate and a
            // filter between scan and aggregate doesn't shrink it (Filter.stats
            // passes through), so an unfiltered-but-selective query (e.g.
            // ClickBench Q10/Q11, `WHERE MobilePhoneModel <> ''` keeps ~4% of
            // rows) would otherwise presize the set to the full ~1M-distinct
            // UserID estimate and pay a multi-MB zero-fill for a few-thousand-
            // entry set. Capping at PRESIZE_CAP costs the genuinely-large
            // unfiltered case (Q08/Q09) only a couple of cheap amortized
            // re-grows while keeping the selective case off the cliff.
            for (aggs, agg_col_indices, cd) |a, maybe_idx, *slot| {
                if (a.func != .count_distinct) continue;
                const idx = maybe_idx orelse continue;
                const vt = up_schema[idx].type;
                const vbits = intKeyBits(vt) orelse continue;
                if (vbits > 64) continue;
                // The set holds ≤ one entry per distinct (gid, value) pair, so its
                // size is bounded by the value column's cardinality estimate (the
                // dominant term) plus 10% headroom, and in all cases by the row
                // count — there is never an *unbounded* case at this stage, only
                // "no tighter estimate than the rows". Allocate a modest initial
                // (ADAPTIVE_INITIAL); on first overflow grow straight to that
                // bound in one rehash rather than doubling repeatedly (which cost
                // Q08 ~8 ms). Growing to the *estimate*, not the row count, is
                // load-bearing: a high-card-but-not-unique value (Q08: 5M rows,
                // ~1M distinct UserIDs) would otherwise jump to a 5M-slot / 128 MB
                // table and eat ~16 ms of sentinel-fill.
                const row_ceiling = @max(st.upper_rows, 1);
                var bound: usize = row_ceiling;
                if (idx < st.column_stats.len) {
                    switch (st.column_stats[idx].ndv) {
                        .exact => |nd| bound = @intCast(@min(nd +| nd / 10, row_ceiling)),
                        .unknown => {},
                    }
                }
                const presize = @min(bound, ADAPTIVE_INITIAL);
                if (vbits <= 32) {
                    // value ≤32 bits + u32 gid ⇒ combined key fits a u64: an
                    // 8-byte-slot key-only set (no `grow_target`; it doubles).
                    slot.* = .{
                        .set = .{ .narrow = DistinctU64Set.init(aa, presize) catch DistinctU64Set.empty },
                        .vbits = vbits,
                    };
                } else {
                    var table = IntTable96.init(aa, presize) catch try IntTable96.init(aa, 0);
                    if (bound > presize) table.grow_target = group_table.capacityFor(bound);
                    slot.* = .{ .set = .{ .wide = table }, .vbits = vbits };
                }
                if (cap > 0) slot.*.?.counts.ensureTotalCapacity(aa, cap) catch {};
            }
        }
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Aggregate) void {
        var up = self.upstream;
        up.deinit();
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.group_col_indices);
        self.allocator.free(self.agg_col_indices);
        self.allocator.free(self.single_state);
        self.allocator.free(self.cd);
        if (self.top_k) |r| self.allocator.free(r.keys);
        if (self.int_layout) |l| l.deinit(self.allocator);
        self.key_scratch.deinit(self.allocator);
        self.pf_key_blob.deinit(self.allocator);
        // The group tables, flat state, per-group key lists, and prefetch
        // scratch all live in `arena` — freed wholesale here.
        self.arena.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Aggregate) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *Aggregate, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    /// Global aggregate (no group_cols): always emits exactly 1 row.
    /// Grouped aggregate: emits at most `min(∏ NDV(group keys), input rows)`
    /// rows — the provable group-count bound (cached at create). Output
    /// columns: group-key columns carry their input stats, aggregate outputs
    /// are bounded by the group count with unknown min/max. Sort state:
    /// hash-based aggregate destroys any prior sort.
    pub fn stats(self: *Aggregate) exec.PipelineStats {
        return .{
            .upper_rows = self.cached_upper_rows,
            .column_stats = self.cached_stats,
        };
    }

    /// Build `cached_upper_rows` + `cached_stats` from the upstream stats.
    /// Called once at create. The group-count bound is the saturating
    /// product of the group keys' NDVs, clamped to the input row count; an
    /// unknown key NDV (or a missing per-column array) leaves only the row
    /// ceiling. Output stats are arena-owned.
    fn computeOutputStats(self: *Aggregate, up_schema: []const Column) !void {
        const up = self.upstream.stats();

        if (self.group_col_indices.len == 0) {
            self.cached_upper_rows = 1;
            // A global aggregate emits one row; each aggregate output is a
            // single value (ndv ≤ 1). min/max come from the provable agg bound
            // (COUNT(*) ∈ [0, rows], SUM ∈ [rows·lo, rows·hi], MIN/MAX inherit
            // the source column's range). Output schema here is the agg outputs
            // only (no group keys).
            const aa = self.arena.allocator();
            const out_stats = try aa.alloc(exec.ColStat, self.output_schema.len);
            const have_up = up.column_stats.len == up_schema.len;
            for (self.aggs, self.agg_col_indices, self.output_schema, out_stats) |a, maybe_idx, oc, *s| {
                const src: ?exec.ColStat = if (have_up) (if (maybe_idx) |idx| up.column_stats[idx] else null) else null;
                s.* = aggColStat(a, oc.type, src, up.upper_rows);
                s.ndv = .{ .exact = 1 };
            }
            self.cached_stats = out_stats;
            return;
        }

        var product: u64 = 1;
        var known = up.column_stats.len == up_schema.len;
        if (known) {
            for (self.group_col_indices) |ci| {
                switch (up.column_stats[ci].ndv) {
                    .exact => |nd| product *|= nd,
                    .unknown => {
                        known = false;
                        break;
                    },
                }
            }
        }
        const upper: u64 = if (known) @min(product, up.upper_rows) else up.upper_rows;
        self.cached_upper_rows = upper;

        // Output column stats: group keys keep their input stats; aggregate
        // outputs are bounded by the group count (ndv ≤ upper), min/max
        // unknown. Skip when the upstream carries no full per-column array —
        // fabricating one would change the "empty ⇒ no info" contract.
        if (up.column_stats.len != up_schema.len) return;
        const aa = self.arena.allocator();
        const out_stats = try aa.alloc(exec.ColStat, self.output_schema.len);
        for (self.group_col_indices, 0..) |ci, i| out_stats[i] = up.column_stats[ci];
        const agg_ndv: exec.ColCard = if (upper > std.math.maxInt(u32))
            .unknown
        else
            .{ .exact = @intCast(upper) };
        // Aggregate outputs: ndv bounded by the group count; min/max from the
        // provable per-agg bound over the source column's range.
        const ng = self.group_col_indices.len;
        for (self.aggs, self.agg_col_indices, self.output_schema[ng..], out_stats[ng..]) |a, maybe_idx, oc, *s| {
            const src: ?exec.ColStat = if (maybe_idx) |idx| up.column_stats[idx] else null;
            s.* = aggColStat(a, oc.type, src, up.upper_rows);
            s.ndv = agg_ndv;
        }
        exec.capColStats(out_stats, upper);
        self.cached_stats = out_stats;
    }

    pub fn accountant(self: *Aggregate) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *Aggregate, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        if (self.group_col_indices.len == 0) {
            try exec.explainLine(out, allocator, depth, "Aggregate (global)");
        } else if (self.emit_limit) |cap| {
            const label = try std.fmt.allocPrint(allocator, "HashAggregate (emit-cap {d})", .{cap});
            defer allocator.free(label);
            try exec.explainLine(out, allocator, depth, label);
        } else {
            try exec.explainLine(out, allocator, depth, "HashAggregate");
        }
        try self.upstream.explain(out, allocator, depth + 1);
    }

    pub fn next(self: *Aggregate) !?Batch {
        if (self.emitted) return null;
        self.emitted = true;

        while (try self.upstream.next()) |batch| {
            try self.accumulateBatch(batch);
        }

        // Count-in-slot is purely an accumulate optimization: lower its
        // `{key,count}` slots into the standard `gkeys_int` / `gstate` arrays
        // (dense gid order) so the emit / top-k dispatch below is untouched.
        if (self.count_table != null) try self.lowerCountSlot(self.arena.allocator());
        // Same shape for the FOR-narrow inline path: lower its `{key,state}`
        // slots into `gkeys_int` / `gstate` (dense gid order) so emit / top-k
        // run unchanged.
        if (self.inline_for != null) try self.lowerInlineFor(self.arena.allocator());

        if (self.group_col_indices.len == 0) {
            try self.appendSingleResult();
        } else if (self.top_k) |r| {
            // Use the bounded-heap top-k path only when k is modest and
            // actually smaller than the group count; otherwise emitting every
            // group and letting the downstream Limit trim is cheaper than a
            // large heap that would keep nearly all groups anyway.
            if (r.k <= TOPK_HEAP_CAP and r.k < self.n_groups) {
                try self.appendTopKResults(r);
            } else {
                try self.appendGroupedResults();
            }
        } else {
            try self.appendGroupedResults();
        }

        // Results are now materialized into `output_columns` (allocator-
        // owned, independent of the arena), so the group hash table /
        // accumulator state is no longer a downstream dependency. Free it
        // and hand its budget back before emitting.
        self.evict();

        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        return Batch{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = self.output_columns[0].rowCount(),
        };
    }

    /// Drop the group accumulator arena and release its reserved budget.
    /// Idempotent. The arena is left in a valid (empty) state so the
    /// later `deinit` call remains safe.
    fn evict(self: *Aggregate) void {
        if (self.evicted) return;
        _ = self.arena.reset(.free_all);
        if (self.upstream.accountant()) |a| a.release(.hash_aggregate, self.reserved_bytes);
        self.reserved_bytes = 0;
        self.evicted = true;
    }

    /// Look-ahead distance for the probe pipeline. Before probing row `i` we
    /// `@prefetch` the slot row `i + PREFETCH_DIST` will land in, so the random
    /// cache miss for that bucket is in flight by the time we reach it — the
    /// outstanding misses overlap instead of serializing. ~8–16 is the usual
    /// sweet spot; 12 balances enough in-flight misses against the scratch
    /// footprint per batch (≤1024 rows).
    const PREFETCH_DIST: usize = 12;

    /// Upper bound on the combined COUNT(DISTINCT int) set's presize (entries).
    /// Bounds the per-query zero-fill of the set when the value-NDV / row-count
    /// estimate is large but a selective filter (invisible to this operator's
    /// stats) means few rows actually arrive. 2^18 ≈ a 4 MiB IntSlot table —
    /// large enough that the genuinely-big unfiltered case only re-grows a
    /// couple of (amortized) times, small enough that a selective query's
    /// zero-fill stays off the cache cliff. See the presize site in `create`.
    const PRESIZE_CAP: usize = 1 << 18;

    /// Initial group-table size when a cardinality estimate is known. The
    /// estimate (`group_cap`) is a provable *ceiling*, not a forecast — a
    /// high-NDV group key behind a super-selective filter (ClickBench Q40)
    /// produces a ceiling in the millions but only tens of thousands of actual
    /// groups, so presizing to the ceiling page-faults a ~80 MB table for a
    /// query that fills <1 MB. Instead start here and, on the first overflow,
    /// jump straight to `group_cap` (a single grow — the ceiling always fits).
    /// A genuinely-high-card group-by fills this immediately and pays exactly
    /// that one jump (≈ today's presize), so this is never slower than before
    /// for overflowing queries and free for queries that stay under it.
    const ADAPTIVE_INITIAL: usize = 1 << 16;

    fn accumulateBatch(self: *Aggregate, batch: Batch) !void {
        const n = batch.row_count;
        const aa_state = self.arena.allocator();
        if (self.group_col_indices.len == 0) {
            for (self.aggs, 0..) |a, ai| {
                try updateState(aa_state, &self.single_state[ai], a, batch, self.agg_col_indices[ai], 0, @intCast(n));
            }
            return;
        }
        if (n == 0) return;
        if (self.count_table != null) {
            try self.accumulateCountSlot(batch);
            return;
        }
        if (self.inline_for) |plan| {
            try self.accumulateInlineFor(batch, plan);
            return;
        }
        if (self.int_layout) |layout| {
            // Dispatch on the tier once per batch, then run the comptime-
            // specialized accumulate so the hot phase-a/phase-b loop has no
            // per-row union/tier branch — the table type is fixed at comptime.
            switch (layout.tier) {
                .bits32 => try self.accumulateBatchIntT(batch, IntTable32, &self.int_table_32.?),
                .bits96 => try self.accumulateBatchIntT(batch, IntTable96, &self.int_table_96.?),
                .bits128 => try self.accumulateBatchIntT(batch, IntTable128, &self.int_table_128.?),
            }
        } else {
            try self.accumulateBatchBytes(batch);
        }
    }

    /// Batched accumulator update for a resolved batch of `gids` (one per row,
    /// `gids.len == batch.row_count`). For each aggregate the (func, input
    /// column type) decision is hoisted out of the row loop — taken once here —
    /// so the per-row scatter into `gstate` is a single specialized operation
    /// rather than a per-row, per-aggregate type switch. The simple scalar
    /// aggregates (COUNT/SUM/MIN/MAX/AVG) run through the specialized kernels;
    /// the complex ones (stddev/var/count_distinct/percentile/group_concat)
    /// still take the per-row `updateStateRow` pass so their semantics live in
    /// exactly one place. Results are byte-identical to the prior per-row path.
    fn accumulateAggsBatched(self: *Aggregate, batch: Batch, gids: []const u32) !void {
        const aa_state = self.arena.allocator();
        for (self.aggs, 0..) |a, ai| {
            switch (a.func) {
                .count => self.scatterCount(self.agg_cols[ai].count, gids, self.agg_col_indices[ai], batch),
                .sum => self.scatterSum(ai, gids, batch.values[self.agg_col_indices[ai].?]),
                .min => try self.scatterMinMax(ai, gids, batch.values[self.agg_col_indices[ai].?], aa_state, true),
                .max => try self.scatterMinMax(ai, gids, batch.values[self.agg_col_indices[ai].?], aa_state, false),
                .avg => self.scatterAvg(self.agg_cols[ai].avg, gids, batch.values[self.agg_col_indices[ai].?]),
                // COUNT(DISTINCT int) under a GROUP BY runs the combined
                // (gid, value) kernel; the gate left `cd[ai]` non-null only for
                // those. Every other complex aggregate (stddev/var/percentile/
                // group_concat, plus string/float/128-bit/no-group distinct)
                // keeps the per-row update on its `.other` AccState cell so its
                // (single) definition of semantics is untouched.
                .count_distinct => if (self.cd[ai] != null) {
                    try self.accumulateCombinedDistinct(ai, batch.values[self.agg_col_indices[ai].?], gids);
                } else {
                    const col = self.agg_cols[ai].other;
                    var r: u32 = 0;
                    while (r < gids.len) : (r += 1) {
                        const s = &col[gids[r]];
                        try updateStateRow(aa_state, s, a, batch, self.agg_col_indices[ai], r);
                    }
                },
                else => {
                    const col = self.agg_cols[ai].other;
                    var r: u32 = 0;
                    while (r < gids.len) : (r += 1) {
                        const s = &col[gids[r]];
                        try updateStateRow(aa_state, s, a, batch, self.agg_col_indices[ai], r);
                    }
                },
            }
        }
    }

    /// COUNT scatter. COUNT(*) (`col_idx == null`) bumps every row's group;
    /// COUNT(col) skips NULLs. Mirrors `updateStateRow`/`updateState` .count.
    inline fn scatterCount(self: *Aggregate, counts: []u64, gids: []const u32, col_idx: ?usize, batch: Batch) void {
        _ = self;
        if (col_idx) |idx| {
            const view = batch.values[idx];
            if (view.nulls == null) {
                for (gids) |g| counts[g] += 1;
            } else {
                for (gids, 0..) |g, r| {
                    if (view.isValid(r)) counts[g] += 1;
                }
            }
        } else {
            for (gids) |g| counts[g] += 1;
        }
    }

    /// SUM scatter. Int family widens into `sum_int` (i128); float/double into
    /// `sum_float` (f64); NULLs skipped. Mirrors `updateStateRow`/`updateState`.
    inline fn scatterSum(self: *Aggregate, ai: usize, gids: []const u32, view: ColumnView) void {
        const has_nulls = view.nulls != null;
        switch (view.data) {
            inline .int, .smallint, .tinyint, .boolean, .bigint, .decimal64, .largeint, .decimal128 => |sl| {
                const sums = self.agg_cols[ai].sum_int;
                if (has_nulls) {
                    for (gids, 0..) |g, r| {
                        if (!view.isValid(r)) continue;
                        sums[g].v += sl[r];
                        sums[g].seen = true;
                    }
                } else {
                    for (gids, 0..) |g, r| {
                        sums[g].v += sl[r];
                        sums[g].seen = true;
                    }
                }
            },
            inline .float, .double => |sl| {
                const sums = self.agg_cols[ai].sum_float;
                if (has_nulls) {
                    for (gids, 0..) |g, r| {
                        if (!view.isValid(r)) continue;
                        sums[g].v += sl[r];
                        sums[g].seen = true;
                    }
                } else {
                    for (gids, 0..) |g, r| {
                        sums[g].v += sl[r];
                        sums[g].seen = true;
                    }
                }
            },
            else => unreachable,
        }
    }

    /// AVG scatter. f64 running sum + u64 count; NULLs skipped. Integer values
    /// convert per-row (matching `updateStateRow`'s grouped path, which routed
    /// AVG through `updateState`'s scalar branch — not the no-null i128 fast
    /// path, which is single-group only). Mirrors that scalar accumulation.
    inline fn scatterAvg(self: *Aggregate, avgs: []AvgAcc, gids: []const u32, view: ColumnView) void {
        _ = self;
        const has_nulls = view.nulls != null;
        switch (view.data) {
            inline .int, .smallint, .tinyint, .boolean, .bigint, .decimal64, .largeint, .decimal128 => |sl| {
                if (has_nulls) {
                    for (gids, 0..) |g, r| {
                        if (!view.isValid(r)) continue;
                        const acc = &avgs[g];
                        acc.sum += @as(f64, @floatFromInt(sl[r]));
                        acc.count += 1;
                    }
                } else {
                    for (gids, 0..) |g, r| {
                        const acc = &avgs[g];
                        acc.sum += @as(f64, @floatFromInt(sl[r]));
                        acc.count += 1;
                    }
                }
            },
            inline .float, .double => |sl| {
                if (has_nulls) {
                    for (gids, 0..) |g, r| {
                        if (!view.isValid(r)) continue;
                        const acc = &avgs[g];
                        acc.sum += sl[r];
                        acc.count += 1;
                    }
                } else {
                    for (gids, 0..) |g, r| {
                        const acc = &avgs[g];
                        acc.sum += sl[r];
                        acc.count += 1;
                    }
                }
            },
            else => unreachable,
        }
    }

    /// MIN (`is_min`) / MAX scatter. Numeric int/large/float use the present-
    /// flagged accumulator forms; strings arena-dup the running extreme. NULLs
    /// skipped. Mirrors `updateStateRow`/`updateState` per-row semantics: which
    /// int widths fold into `min_int`/`max_int` (i64) vs `min_large`/`max_large`
    /// (i128), `date`→i64, `datetime`→i64, `decimal64`→i64, etc.
    fn scatterMinMax(self: *Aggregate, ai: usize, gids: []const u32, view: ColumnView, aa: Allocator, comptime is_min: bool) !void {
        const has_nulls = view.nulls != null;
        switch (view.data) {
            // i64-accumulator int families. `date`→i64, `datetime` already i64,
            // booleans/small ints widen to i64 (matching the scalar path).
            inline .int, .date, .bigint, .datetime, .boolean, .tinyint, .smallint, .decimal64 => |sl| {
                const col = if (is_min) self.agg_cols[ai].min_int else self.agg_cols[ai].max_int;
                for (gids, 0..) |g, r| {
                    if (has_nulls and !view.isValid(r)) continue;
                    const iv: i64 = sl[r];
                    const cur = &col[g];
                    if (is_min) {
                        if (cur.* == null or iv < cur.*.?) cur.* = iv;
                    } else {
                        if (cur.* == null or iv > cur.*.?) cur.* = iv;
                    }
                }
            },
            // i128-accumulator families (don't fit in i64). These keep their
            // `min_large`/`max_large` AccState cell on the `.other` column.
            inline .largeint, .decimal128 => |sl| {
                const col = self.agg_cols[ai].other;
                for (gids, 0..) |g, r| {
                    if (has_nulls and !view.isValid(r)) continue;
                    const v: i128 = sl[r];
                    const s = &col[g];
                    if (is_min) {
                        if (!s.min_large.present or v < s.min_large.v) s.min_large = .{ .v = v, .present = true };
                    } else {
                        if (!s.max_large.present or v > s.max_large.v) s.max_large = .{ .v = v, .present = true };
                    }
                }
            },
            inline .float, .double => |sl| {
                const col = if (is_min) self.agg_cols[ai].min_float else self.agg_cols[ai].max_float;
                for (gids, 0..) |g, r| {
                    if (has_nulls and !view.isValid(r)) continue;
                    const fv: f64 = sl[r];
                    const cur = &col[g];
                    if (is_min) {
                        if (cur.* == null or fv < cur.*.?) cur.* = fv;
                    } else {
                        if (cur.* == null or fv > cur.*.?) cur.* = fv;
                    }
                }
            },
            .varchar, .string, .char, .json => |sv| {
                const col = self.agg_cols[ai].other;
                for (gids, 0..) |g, r| {
                    if (has_nulls and !view.isValid(r)) continue;
                    const bytes = sv.rowBytes(r);
                    const s = &col[g];
                    if (is_min) {
                        if (s.min_str == null or std.mem.order(u8, bytes, s.min_str.?) == .lt) {
                            s.min_str = try aa.dupe(u8, bytes);
                        }
                    } else {
                        if (s.max_str == null or std.mem.order(u8, bytes, s.max_str.?) == .gt) {
                            s.max_str = try aa.dupe(u8, bytes);
                        }
                    }
                }
            },
            else => unreachable,
        }
    }

    /// Batched COUNT(DISTINCT int) for aggregate `ai` (gate-confirmed: int value
    /// column ≤64 bits, under a GROUP BY). One probe per valid row into a single
    /// combined set keyed on `pack(gid, value)`, plus a flat per-gid counter
    /// bumped on each first sighting — no per-row dispatch, no per-group
    /// scattered hash map. Two tiers by packed-key width (see `CombinedSet`); the
    /// per-group count is exact for both because the pack is bijective.
    fn accumulateCombinedDistinct(self: *Aggregate, ai: usize, view: ColumnView, gids: []const u32) !void {
        const c = &self.cd[ai].?;
        switch (c.set) {
            .wide => try self.combinedDistinctWide(c, view, gids),
            .narrow => try self.combinedDistinctNarrow(c, view, gids),
        }
    }

    /// Wide tier (value 33–64 bits): the `(gid, value)` pack is a ≤96-bit key in
    /// an `IntTable96` set. The table is grown for the whole batch up front so
    /// slot addresses stay stable across the look-ahead, then phase (a) packs
    /// key+hash per valid row and phase (b) probes with a look-ahead `@prefetch`,
    /// bumping the owning group on each first sighting. NULLs are excluded.
    fn combinedDistinctWide(self: *Aggregate, c: *CombinedDistinct, view: ColumnView, gids: []const u32) !void {
        const table = &c.set.wide;
        const aa = self.arena.allocator();
        const n: usize = gids.len;
        const vbits: u7 = @intCast(c.vbits);
        const has_nulls = view.nulls != null;

        if (table.needsGrow(n)) try table.grow(aa, n);

        self.pf_cd_keys.clearRetainingCapacity();
        self.pf_cd_hashes.clearRetainingCapacity();
        try self.pf_cd_keys.ensureTotalCapacity(aa, n);
        try self.pf_cd_hashes.ensureTotalCapacity(aa, n);
        switch (view.data) {
            inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |sl| {
                const Child = @typeInfo(@TypeOf(sl)).pointer.child;
                const bits: u8 = @bitSizeOf(Child);
                for (0..n) |r| {
                    if (has_nulls and !view.isValid(r)) continue;
                    const value_bits = fieldBits(Child, sl[r], bits);
                    const key = (@as(u128, gids[r]) << vbits) | value_bits;
                    self.pf_cd_keys.appendAssumeCapacity(key);
                    self.pf_cd_hashes.appendAssumeCapacity(IntTable96.hashKey(key));
                }
            },
            else => unreachable,
        }
        const keys = self.pf_cd_keys.items;
        const hashes = self.pf_cd_hashes.items;
        const m = keys.len;

        var i: usize = 0;
        while (i < m) : (i += 1) {
            const pf = i + PREFETCH_DIST;
            if (pf < m) {
                const b = table.bucketOf(hashes[pf]);
                @prefetch(table.slotAddr(b), .{ .rw = .write, .locality = 1 });
            }
            const key = keys[i];
            const probe = table.getOrPut(hashes[i], key);
            if (!probe.found) {
                table.commit(probe.slot, key, 0);
                c.counts.items[@intCast(key >> vbits)] += 1;
            }
        }
    }

    /// Narrow tier (value ≤32 bits): the `gid<<vbits | value` key fits a u64
    /// (gid is a u32, vbits ≤ 32), so a key-only 8-byte-slot `DistinctU64Set`
    /// replaces the 16-byte `IntTable96` — half the bytes touched per probe on
    /// this memory-bound insert. Phase (a) packs the u64 keys into the shared
    /// `pf_cd_keys` scratch; phase (b) reserves for the batch, then probes with a
    /// look-ahead `@prefetch`, bumping the owning group on each first sighting.
    fn combinedDistinctNarrow(self: *Aggregate, c: *CombinedDistinct, view: ColumnView, gids: []const u32) !void {
        const set = &c.set.narrow;
        const aa = self.arena.allocator();
        const n: usize = gids.len;
        const vbits: u6 = @intCast(c.vbits);
        const has_nulls = view.nulls != null;

        self.pf_cd_keys.clearRetainingCapacity();
        try self.pf_cd_keys.ensureTotalCapacity(aa, n);
        switch (view.data) {
            inline .boolean, .tinyint, .smallint, .int, .date => |sl| {
                const Child = @typeInfo(@TypeOf(sl)).pointer.child;
                const bits: u8 = @bitSizeOf(Child);
                for (0..n) |r| {
                    if (has_nulls and !view.isValid(r)) continue;
                    const value_bits: u64 = @intCast(fieldBits(Child, sl[r], bits));
                    self.pf_cd_keys.appendAssumeCapacity((@as(u64, gids[r]) << vbits) | value_bits);
                }
            },
            else => unreachable,
        }
        const keys = self.pf_cd_keys.items;
        const m = keys.len;

        // Reserve for the whole batch up front so no grow fires across the
        // look-ahead window (slot addresses must stay stable for the prefetch).
        try set.ensureFor(aa, m);

        var i: usize = 0;
        while (i < m) : (i += 1) {
            const pf = i + PREFETCH_DIST;
            if (pf < m) set.prefetch(@as(u64, @truncate(keys[pf])));
            const key: u64 = @truncate(keys[i]);
            if (set.insertNew(key)) c.counts.items[@intCast(key >> vbits)] += 1;
        }
    }

    /// Count-in-slot accumulate (gate-confirmed: one non-nullable ≤64-bit int
    /// group column, one `COUNT(*)`). Type-switch once on the single group
    /// column, then run the prefetch pipeline: grow the table for the whole
    /// batch up front (so slot addresses stay stable across the look-ahead),
    /// then interleave `prefetch(look-ahead key)` with `insert(current key)`.
    /// `insert` bumps the count inside the slot — one cache miss per row. The
    /// key is the value's unsigned bits zero-extended to u64 (same bijective
    /// cast as `insertDistinctRange`). The group column is non-nullable, so no
    /// validity branch.
    fn accumulateCountSlot(self: *Aggregate, batch: Batch) !void {
        const n = batch.row_count;
        const aa = self.arena.allocator();
        const view = batch.values[self.group_col_indices[0]];
        switch (view.data) {
            inline .int, .date => |sl| try self.insertCountRange(aa, i32, sl, n),
            inline .bigint, .datetime, .decimal64 => |sl| try self.insertCountRange(aa, i64, sl, n),
            .smallint => |sl| try self.insertCountRange(aa, i16, sl, n),
            .tinyint => |sl| try self.insertCountRange(aa, i8, sl, n),
            .boolean => |sl| try self.insertCountRange(aa, u8, sl, n),
            else => unreachable,
        }
    }

    /// Prefetch-pipelined count-in-slot insert over a non-nullable typed column
    /// slice. Reserves the whole batch up front so no grow fires mid-loop. The
    /// value's two's-complement bits zero-extend injectively into a u64, so
    /// distinct stored values map to distinct keys.
    inline fn insertCountRange(self: *Aggregate, aa: Allocator, comptime T: type, sl: []const T, n: usize) !void {
        const U = std.meta.Int(.unsigned, @bitSizeOf(T));
        const t = &self.count_table.?;
        try t.ensureFor(aa, n);
        var r: usize = 0;
        while (r < n) : (r += 1) {
            if (r + PREFETCH_DIST < n) t.prefetch(@as(u64, @as(U, @bitCast(sl[r + PREFETCH_DIST]))));
            t.insert(@as(u64, @as(U, @bitCast(sl[r]))));
        }
    }

    /// Lower the count-in-slot table into the operator's `gkeys_int` /
    /// `agg_cols` arrays so the existing emit / top-k dispatch runs unchanged.
    /// Walks the occupied slots (plus the sentinel group, if present), assigning
    /// a dense gid 0,1,2,…: `gkeys_int[gid] = key`, `agg_cols[0].count[gid] = c`
    /// (the single aggregate is COUNT, matching `initialState(.count, null)`).
    fn lowerCountSlot(self: *Aggregate, aa: Allocator) !void {
        const t = &self.count_table.?;
        const total = t.count();
        try self.gkeys_int.ensureTotalCapacity(aa, total);
        try self.allocAggCols(aa, total);
        const counts = self.agg_cols[0].count;
        var gid: u32 = 0;
        for (t.slots) |s| {
            if (s.key == CountSlotTable.SENTINEL) continue;
            self.gkeys_int.appendAssumeCapacity(@as(u128, s.key));
            counts[gid] = s.count;
            gid += 1;
        }
        if (t.has_sentinel) {
            self.gkeys_int.appendAssumeCapacity(@as(u128, CountSlotTable.SENTINEL));
            counts[gid] = t.sentinel_count;
            gid += 1;
        }
        self.n_groups = gid;
    }

    /// FOR-narrow inline-state accumulate (gate-confirmed by `planInlineFor`):
    /// one non-nullable ≤64-bit int group column, one inline SUM/MIN/MAX over a
    /// ≤64-bit int column. Resolves the key width, the group/value stored types,
    /// and the fold at comptime, then runs the prefetch-pipelined inner loop with
    /// the table type fixed (no per-row union/tier/func branch). The group column
    /// is non-nullable, so there is no key validity branch; the *value* column
    /// may carry NULLs (SQL excludes them from SUM/MIN/MAX), so the fold skips
    /// invalid rows.
    fn accumulateInlineFor(self: *Aggregate, batch: Batch, plan: InlineForPlan) !void {
        // The tier × kind × key-tag × val-tag inline fan-out exceeds the default
        // comptime branch budget; raise it for this specialization tree.
        @setEvalBranchQuota(10_000);
        const aa = self.arena.allocator();
        const key_view = batch.values[self.group_col_indices[0]];
        const val_view = batch.values[self.agg_col_indices[0].?];
        switch (plan.tier) {
            .w8 => try inlineForTier(aa, plan, key_view, val_view, u8, &self.inline_table_8.?),
            .w16 => try inlineForTier(aa, plan, key_view, val_view, u16, &self.inline_table_16.?),
            .w32 => try inlineForTier(aa, plan, key_view, val_view, u32, &self.inline_table_32.?),
            .w64 => try inlineForTier(aa, plan, key_view, val_view, u64, &self.inline_table_64.?),
        }
    }

    /// Lower the FOR-narrow inline table into `gkeys_int` / `agg_cols` so the
    /// existing emit / top-k dispatch runs unchanged. Walks the occupied slots,
    /// assigning a dense gid 0,1,2,…: reconstructs `value = base + code`, packs
    /// it into the single-column u128 key exactly as `orKeyColumn` would (so
    /// `appendIntGroupKey` decodes it identically), and lowers the i64 state into
    /// the matching narrow column (`.sum_int` / `.min_int` / `.max_int`,
    /// mirroring `initialState`). Both columns are non-nullable (`planInlineFor`
    /// gates), so every occupied slot folded at least one real value.
    fn lowerInlineFor(self: *Aggregate, aa: Allocator) !void {
        const plan = self.inline_for.?;
        const layout = self.int_layout.?;
        const field = layout.fields[0];
        switch (plan.tier) {
            inline .w8, .w16, .w32, .w64 => |tier| {
                const KeyW = inlineKeyWidth(tier);
                const t: *group_table.InlineSlotTable(KeyW, InlineState) = switch (tier) {
                    .w8 => &self.inline_table_8.?,
                    .w16 => &self.inline_table_16.?,
                    .w32 => &self.inline_table_32.?,
                    .w64 => &self.inline_table_64.?,
                };
                const total = t.count();
                try self.gkeys_int.ensureTotalCapacity(aa, total);
                try self.allocAggCols(aa, total);
                const col = self.agg_cols[0];
                const SENTINEL = @TypeOf(t.*).SENTINEL;
                var gid: u32 = 0;
                for (t.slots) |s| {
                    if (s.key == SENTINEL) continue;
                    // `base + code` reconstructs the original value; the gate
                    // proved it fits i64, so the i128 add narrows losslessly.
                    const value: i64 = @intCast(@as(i128, plan.base) + @as(i128, s.key));
                    self.gkeys_int.appendAssumeCapacity(packSingleIntField(field, value));
                    switch (plan.kind) {
                        .sum => col.sum_int[gid] = .{ .v = @as(i128, s.state), .seen = true },
                        .min => col.min_int[gid] = s.state,
                        .max => col.max_int[gid] = s.state,
                    }
                    gid += 1;
                }
                self.n_groups = gid;
            },
        }
    }

    /// Allocate the per-aggregate columns to `capacity` (== the group table's
    /// `slots.len`) and initialize every cell to its aggregate's initial value
    /// (`initialState`'s semantics, mirrored per column). Each column's kind is
    /// chosen by `aggColKind`; `.other` cells get `initialState` directly (so
    /// the per-row `updateStateRow` path sees a correctly-shaped `*AccState`).
    fn allocAggCols(self: *Aggregate, aa: Allocator, capacity: usize) !void {
        const up_schema = self.upstream.outputSchema();
        const cols = try aa.alloc(AggCol, self.aggs.len);
        for (self.aggs, self.agg_col_indices, 0..) |a, maybe_idx, ai| {
            const in_t: ?Type = if (maybe_idx) |i| up_schema[i].type else null;
            cols[ai] = try initAggCol(aa, a.func, in_t, capacity);
        }
        self.agg_cols = cols;
    }

    /// Current cell count of the per-aggregate columns (every column shares it).
    /// 0 when the columns have not been allocated yet.
    fn aggColsLen(self: *const Aggregate) usize {
        if (self.agg_cols.len == 0) return 0;
        return switch (self.agg_cols[0]) {
            inline else => |s| s.len,
        };
    }

    /// Ensure the per-aggregate columns hold at least `capacity` cells, allocating
    /// fresh if unallocated or growing in place otherwise. New cells get the
    /// aggregate's initial value. Idempotent when already large enough.
    fn ensureAggColsCapacity(self: *Aggregate, aa: Allocator, capacity: usize) !void {
        if (self.agg_cols.len == 0) {
            try self.allocAggCols(aa, capacity);
        } else if (self.aggColsLen() < capacity) {
            try self.growAggCols(aa, capacity);
        }
    }

    /// Grow the per-aggregate columns from their current length to
    /// `new_capacity`, initializing the freshly-added region to each
    /// aggregate's initial value. Mirrors a group-table grow: the table jumps
    /// straight to its ceiling, so this normally fires at most once.
    fn growAggCols(self: *Aggregate, aa: Allocator, new_capacity: usize) !void {
        const up_schema = self.upstream.outputSchema();
        for (self.agg_cols, self.aggs, self.agg_col_indices) |*col, a, maybe_idx| {
            const in_t: ?Type = if (maybe_idx) |i| up_schema[i].type else null;
            try growAggCol(aa, col, a.func, in_t, new_capacity);
        }
    }

    /// Mark a freshly-assigned group `gid` as initialized: seed each
    /// combined-distinct aggregate's gid-indexed counter with 0. Every
    /// accumulator cell (narrow column AND `.other` AccState) was already set to
    /// its `initialState` value at column-allocation / grow time — gids are
    /// dense and monotonic (each assigned exactly once, never reused, the table
    /// never shrinks), so the cell at `gid` still holds that fresh initial value
    /// when this fires. No per-group cell write is needed.
    fn initGroupCells(self: *Aggregate, aa: Allocator, gid: u32) !void {
        _ = gid;
        for (self.cd) |*maybe| {
            if (maybe.*) |*c| try c.counts.append(aa, 0);
        }
    }

    /// Gather one group's accumulator cells (gid) from the per-aggregate columns
    /// into `self.state_scratch` as `[]AccState`, wrapping each narrow column
    /// value back into its matching `AccState` variant. Lets the emit paths keep
    /// passing a `[]AccState` to `appendGroupRow` / `topkEntry` unchanged.
    fn readGroupState(self: *Aggregate, gid: u32) []AccState {
        const g: usize = gid;
        const out = self.state_scratch;
        for (self.agg_cols, out) |col, *dst| {
            dst.* = switch (col) {
                .count => |s| .{ .count = s[g] },
                .sum_int => |s| .{ .sum_int = s[g] },
                .sum_float => |s| .{ .sum_float = s[g] },
                .avg => |s| .{ .avg = s[g] },
                .min_int => |s| .{ .min_int = s[g] },
                .max_int => |s| .{ .max_int = s[g] },
                .min_float => |s| .{ .min_float = s[g] },
                .max_float => |s| .{ .max_float = s[g] },
                .other => |s| s[g],
            };
        }
        return out;
    }

    /// Integer fast path: pack each row's group columns into a u128 (phase a),
    /// then probe the tier-`Table` with a prefetch look-ahead (phase b). The
    /// table is grown for the whole batch up front so slot addresses stay stable
    /// across the look-ahead window. Generic over the slot-tier table type so
    /// the per-row probe specializes (no tier branch in the inner loop); the one
    /// runtime tier decision is hoisted to `accumulateBatch`'s switch. `orKeyColumn`
    /// keeps producing the full u128 — the table truncates/splits it losslessly
    /// to its tier on store (the layout guarantees the key fits).
    /// Lazily create the bare-LIMIT overflow group (`cap_groups`): one extra,
    /// never-emitted gid that absorbs every post-cap new key, so the table stops
    /// growing once `emit_limit` real groups exist. Its key is a placeholder
    /// (never read — the emit stops at `emit_limit`, below this gid); its bounded
    /// agg state is updated like any group, then discarded. Idempotent.
    fn ensureOverflowGroup(self: *Aggregate, aa: Allocator, int_path: bool) !u32 {
        if (self.overflow_gid) |g| return g;
        const g = self.n_groups;
        self.n_groups += 1;
        if (int_path) self.gkeys_int.appendAssumeCapacity(0) else try self.gkeys.append(aa, &.{});
        try self.initGroupCells(aa, g);
        self.overflow_gid = g;
        return g;
    }

    fn accumulateBatchIntT(self: *Aggregate, batch: Batch, comptime Table: type, table: *Table) !void {
        const n = batch.row_count;
        const aa = self.arena.allocator();
        const layout = self.int_layout.?;

        if (table.needsGrow(n)) {
            try table.grow(aa, n);
            // Couple the per-aggregate columns + key array to the table's new
            // capacity so the new-group writes below land in bounds (gid <
            // slots.len always) with no per-group reallocation.
            self.gkeys_int.ensureTotalCapacity(aa, table.slots.len) catch {};
        }
        // Ensure the per-aggregate columns are sized to the table's slot count
        // (gids index into them directly). On the `init_cap == 0` path they are
        // unallocated until here; otherwise a grow above may have enlarged the
        // table past their current length.
        try self.ensureAggColsCapacity(aa, table.slots.len);
        // Every row could be a new group; reserve the key array for the worst
        // case up front so the per-row new-group appends below are bounds-free.
        try self.gkeys_int.ensureUnusedCapacity(aa, n);

        // Phase (a): pack every row's key, column-major — the per-column ValueView
        // type switch runs once per group column per batch (in `orKeyColumn`)
        // instead of once per column per row. Keys start at 0; each column ORs in
        // its bit-field contribution.
        self.pf_int_keys.clearRetainingCapacity();
        try self.pf_int_keys.ensureTotalCapacity(aa, n);
        self.pf_int_keys.appendNTimesAssumeCapacity(0, n);
        const keys = self.pf_int_keys.items;
        for (self.group_col_indices, layout.fields) |ci, f| orKeyColumn(keys, batch, ci, f);

        // Phase (b): probe with look-ahead prefetch, recording each row's gid.
        self.pf_gids.clearRetainingCapacity();
        try self.pf_gids.ensureTotalCapacity(aa, n);
        const acct = self.upstream.accountant();
        const approx_per = self.aggs.len * @sizeOf(AccState) + @sizeOf(u128) + 32;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const pf = i + PREFETCH_DIST;
            if (pf < n) {
                const b = table.bucketOf(Table.hashKey(keys[pf]));
                @prefetch(table.slotAddr(b), .{ .rw = .write, .locality = 1 });
            }
            const key = keys[i];
            const probe = table.getOrPut(Table.hashKey(key), key);
            const gid = if (probe.found) probe.gid else if (self.cap_groups and self.n_groups >= self.emit_limit.?)
                try self.ensureOverflowGroup(aa, true)
            else blk: {
                if (acct) |a| try a.reserve(.hash_aggregate, approx_per);
                self.reserved_bytes += approx_per;
                const new_gid = self.n_groups;
                self.n_groups += 1;
                table.commit(probe.slot, key, new_gid);
                self.gkeys_int.appendAssumeCapacity(key);
                try self.initGroupCells(aa, new_gid);
                break :blk new_gid;
            };
            self.pf_gids.appendAssumeCapacity(gid);
        }

        // Phase (c): batched, type-specialized scatter-update over the resolved
        // gids — one tight loop per aggregate instead of a per-row type switch.
        try self.accumulateAggsBatched(batch, self.pf_gids.items);
    }

    /// Byte-key path (string / mixed / wide compound keys): serialize each
    /// row's key + hash (phase a), then probe `byte_table` with a prefetch
    /// look-ahead (phase b). New groups dup their key bytes into the arena
    /// (indexed by gid) so the stored key survives past the batch.
    fn accumulateBatchBytes(self: *Aggregate, batch: Batch) !void {
        const n = batch.row_count;
        const aa = self.arena.allocator();

        if (self.byte_table.needsGrow(n)) {
            try self.byte_table.grow(aa, n);
            // Couple the key array to the table's new capacity so the per-group
            // appends below run amortized-free as groups fill toward the ceiling.
            self.gkeys.ensureTotalCapacity(aa, self.byte_table.slots.len) catch {};
        }
        // Per-aggregate columns are indexed by gid, so they must cover every slot
        // the table can hold (gid < slots.len always). Unallocated on the
        // `init_cap == 0` path until here; a grow above may have enlarged them.
        try self.ensureAggColsCapacity(aa, self.byte_table.slots.len);

        // Single string key: the key is the row's raw string bytes, already
        // sitting decoded in the batch — no scratch copy / length prefix. Its
        // bytes live in the batch (valid for this whole call), so phase (a) can
        // borrow them directly. Compound/mixed keys are serialized into a
        // per-batch arena buffer (one dupe per row) so every key slice in
        // `pf_keys` stays live through phase (b).
        // Phase 4.2: the single string key may arrive as global dict CODES via
        // the sidecar. Key on the 4 code-bytes (in place in the codes array,
        // stable for the batch) — no string materialization.
        const coded_cc: ?exec.CodedColumn = if (self.coded_key != null) blk: {
            const cd = batch.coded orelse break :blk null;
            break :blk cd[self.group_col_indices[0]];
        } else null;

        const str_view: ?storage.StringView = if (coded_cc == null and self.single_str_key)
            switch (batch.values[self.group_col_indices[0]].data) {
                .string, .varchar, .char, .json => |sv| sv,
                else => unreachable,
            }
        else
            null;

        // Phase (a): build per-row key slice + hash. Single-string keys borrow
        // the batch's bytes directly; compound keys are serialized into the
        // reused per-batch `pf_key_blob` (offsets recorded in `pf_key_spans`,
        // then resolved to slices once the blob stops growing) — no per-row dup.
        self.pf_keys.clearRetainingCapacity();
        self.pf_hashes.clearRetainingCapacity();
        try self.pf_keys.ensureTotalCapacity(aa, n);
        try self.pf_hashes.ensureTotalCapacity(aa, n);
        var row: u32 = 0;
        if (coded_cc) |cc| {
            while (row < n) : (row += 1) {
                const kb = std.mem.asBytes(&cc.codes[row])[0..];
                self.pf_keys.appendAssumeCapacity(kb);
                self.pf_hashes.appendAssumeCapacity(std.hash.Wyhash.hash(0, kb));
            }
        } else if (self.coded_key) |ck| {
            // Non-coded (string) batch under coding — a tombstoned segment or the
            // memtable, which the scan emits as strings. Intern each key string
            // into the SAME GlobalDict → global codes, so it groups consistently
            // with the sidecar-coded batches (one code space, no split groups).
            const sv = str_view.?;
            self.coded_scratch.clearRetainingCapacity();
            try self.coded_scratch.ensureTotalCapacity(aa, n);
            while (row < n) : (row += 1) {
                self.coded_scratch.appendAssumeCapacity(try ck.dict.intern(self.allocator, sv.rowBytes(row)));
            }
            row = 0;
            while (row < n) : (row += 1) {
                const kb = std.mem.asBytes(&self.coded_scratch.items[row])[0..];
                self.pf_keys.appendAssumeCapacity(kb);
                self.pf_hashes.appendAssumeCapacity(std.hash.Wyhash.hash(0, kb));
            }
        } else if (str_view) |sv| {
            while (row < n) : (row += 1) {
                const key = sv.rowBytes(row);
                self.pf_keys.appendAssumeCapacity(key);
                self.pf_hashes.appendAssumeCapacity(std.hash.Wyhash.hash(0, key));
            }
        } else {
            self.pf_key_blob.clearRetainingCapacity();
            self.pf_key_spans.clearRetainingCapacity();
            try self.pf_key_spans.ensureTotalCapacity(aa, n);
            while (row < n) : (row += 1) {
                const off: u32 = @intCast(self.pf_key_blob.items.len);
                try buildCompoundGroupKey(self.allocator, &self.pf_key_blob, batch, self.group_col_indices, row);
                const len: u32 = @intCast(self.pf_key_blob.items.len - off);
                self.pf_key_spans.appendAssumeCapacity(.{ off, len });
                self.pf_hashes.appendAssumeCapacity(std.hash.Wyhash.hash(0, self.pf_key_blob.items[off..]));
            }
            // Blob is stable now → resolve each row's key slice into it.
            for (self.pf_key_spans.items) |sp| {
                self.pf_keys.appendAssumeCapacity(self.pf_key_blob.items[sp[0] .. sp[0] + sp[1]]);
            }
        }
        const keys = self.pf_keys.items;
        const hashes = self.pf_hashes.items;

        // Phase (b): probe with look-ahead prefetch, recording each row's gid.
        self.pf_gids.clearRetainingCapacity();
        try self.pf_gids.ensureTotalCapacity(aa, n);
        const acct = self.upstream.accountant();
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const pf = i + PREFETCH_DIST;
            if (pf < n) {
                const b = self.byte_table.bucketOf(hashes[pf]);
                @prefetch(self.byte_table.slotAddr(b), .{ .rw = .write, .locality = 1 });
            }
            const key = keys[i];
            const h = hashes[i];
            const probe = self.byte_table.getOrPut(h, key, self.gkeys.items);
            const gid = if (probe.found) probe.gid else if (self.cap_groups and self.n_groups >= self.emit_limit.?)
                try self.ensureOverflowGroup(aa, false)
            else blk: {
                const approx = key.len + self.aggs.len * @sizeOf(AccState) + 32;
                if (acct) |a| try a.reserve(.hash_aggregate, approx);
                self.reserved_bytes += approx;
                const new_gid = self.n_groups;
                self.n_groups += 1;
                self.byte_table.commit(probe.slot, h, new_gid);
                // `key` borrows transient storage — the batch (single-string) or
                // the reused per-batch blob (compound) — so a new group dups it
                // into the arena to survive past this batch. Only new groups pay
                // this copy now, not every row.
                try self.gkeys.append(aa, try aa.dupe(u8, key));
                try self.initGroupCells(aa, new_gid);
                break :blk new_gid;
            };
            self.pf_gids.appendAssumeCapacity(gid);
        }

        // Phase (c): batched, type-specialized scatter-update over the resolved
        // gids — one tight loop per aggregate instead of a per-row type switch.
        try self.accumulateAggsBatched(batch, self.pf_gids.items);
    }

    fn appendSingleResult(self: *Aggregate) !void {
        // No GROUP BY ⇒ the combined-distinct gate never fires; every distinct
        // aggregate stays on its AccState set, so `cd_count` is always null.
        for (self.aggs, 0..) |a, ai| {
            try appendAccToColumn(self.allocator, a, self.single_state[ai], &self.output_columns[ai], self.output_schema[ai].type, null);
        }
    }

    fn appendGroupedResults(self: *Aggregate) !void {
        // An unordered `GROUP BY … LIMIT n` caps the emit at the first n groups
        // (group-insertion order). The build already consumed all input, so the
        // counts/aggregates of those groups are exact — only the emit stops
        // early, skipping the (string-heavy) row reconstruction for the rest.
        const stop: u32 = if (self.emit_limit) |cap| @min(cap, self.n_groups) else self.n_groups;
        var gid: u32 = 0;
        while (gid < stop) : (gid += 1) {
            try self.appendGroupRow(gid, self.readGroupState(gid));
        }
    }

    /// Materialize one group's key + aggregate values into `output_columns`.
    /// Copies into allocator-owned storage, so the borrowed `state` (which
    /// lives in the group arena) need not outlive this call. The group key is
    /// reconstructed from `gkeys_int[gid]` (integer path) or `gkeys[gid]`
    /// (byte path) — both produce identical output columns.
    fn appendGroupRow(self: *Aggregate, gid: u32, state: []AccState) !void {
        if (self.coded_key) |ck| {
            // Phase 4.2: the stored key is the 4 code-bytes; decode the global
            // code back to its string via the query dictionary.
            const code = std.mem.readInt(u32, self.gkeys.items[gid][0..4], native_endian);
            switch (self.output_columns[0].data) {
                .string, .varchar, .char, .json => |*ss| try ss.appendValue(self.allocator, ck.dict.decode(code)),
                else => unreachable,
            }
        } else if (self.int_layout) |layout| {
            try appendIntGroupKey(self.allocator, self.gkeys_int.items[gid], layout, self.output_columns[0..self.group_col_indices.len]);
        } else if (self.single_str_key) {
            // Raw string bytes — no compound framing to decode.
            switch (self.output_columns[0].data) {
                .string, .varchar, .char, .json => |*ss| try ss.appendValue(self.allocator, self.gkeys.items[gid]),
                else => unreachable,
            }
        } else {
            try appendGroupKey(self.allocator, self.gkeys.items[gid], self.group_col_indices, self.upstream.outputSchema(), self.output_columns[0..self.group_col_indices.len]);
        }

        for (self.aggs, 0..) |a, ai| {
            const out_idx = self.group_col_indices.len + ai;
            const cd_count: ?u64 = if (self.cd[ai]) |c| c.counts.items[gid] else null;
            try appendAccToColumn(self.allocator, a, state[ai], &self.output_columns[out_idx], self.output_schema[out_idx].type, cd_count);
        }
    }

    /// Top-k emit path: a single pass over the group hash table keeps only the
    /// `k` most-preferred groups (by the resolved ORDER BY keys, lexicographic
    /// with per-key direction) in a bounded heap, then materializes just those.
    /// The downstream OrderBy+Limit re-sort the small surviving set, so the heap
    /// need not produce sorted output — it only has to pick the correct k
    /// groups. This avoids building (and string-copying) every group's row only
    /// for TopN to discard them.
    fn appendTopKResults(self: *Aggregate, r: ResolvedTopK) !void {
        const k = r.k;
        const heap = try self.arena.allocator().alloc(TopKEntry, k);
        var len: usize = 0;

        var gid: u32 = 0;
        while (gid < self.n_groups) : (gid += 1) {
            // `topkEntry` caches each key's order value into the entry up front,
            // so reusing the single `state_scratch` across iterations is safe.
            const cand = topkEntry(gid, self.readGroupState(gid), r.keys, self.cd);
            if (len < k) {
                heap[len] = cand;
                len += 1;
                if (len == k) topkBuildHeap(heap, r.keys);
            } else if (topkLessPreferred(heap[0], cand, r.keys)) {
                // The current worst kept group (root) is less preferred than
                // the candidate — evict it.
                heap[0] = cand;
                topkSiftDown(heap, 0, k, r.keys);
            }
        }

        for (heap[0..len]) |w| {
            try self.appendGroupRow(w.gid, self.readGroupState(w.gid));
        }
    }
};

/// Streaming GROUP BY for input already sorted such that equal group keys
/// are adjacent (direction-agnostic). Holds only the *current* group's
/// accumulator state — O(1) in cardinality, unlike the hash Aggregate
/// which holds every group at once. When the key changes, the open group's
/// result row is appended to the output batch and the per-group transient
/// arena is reset. Emits in `batch_size` chunks so operator memory stays
/// bounded regardless of how many groups there are.
///
/// Requires `group_cols.len > 0`. The router in net/local.zig selects this
/// only when `sort_state` proves the group keys are a sorted prefix.
pub const SortedAggregate = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    upstream: Query,

    group_col_indices: []usize,
    agg_col_indices: []?usize,
    aggs: []const AggSpec,

    output_schema: []Column,
    output_columns: []ColumnStore,
    views: []ColumnView,

    /// The open group's key bytes + accumulators. `cur_state` is allocated
    /// once and re-initialized per group; its transient sub-allocations
    /// (string min/max, distinct sets, ...) live in `arena`, reset between
    /// groups.
    cur_key: std.ArrayList(u8),
    cur_state: []AccState,
    open: bool = false,
    /// Scratch for building a candidate row's key to compare against
    /// `cur_key`.
    key_scratch: std.ArrayList(u8),

    /// Resumable input cursor: we may stop mid-batch when the output batch
    /// fills, and continue from here on the next `next()` call.
    cur_batch: ?Batch = null,
    cur_row: u32 = 0,
    upstream_done: bool = false,

    const batch_size: usize = 1024;

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        group_cols: []const []const u8,
        aggs: []const AggSpec,
    ) !Query {
        if (aggs.len == 0) return Error.AggregateNoSpecs;
        // Streaming only makes sense with grouping keys; the no-group
        // (global) case has no sortedness to exploit and stays on Aggregate.
        if (group_cols.len == 0) return Error.AggregateNoSpecs;
        const up_schema = upstream.outputSchema();

        const group_col_indices = try allocator.alloc(usize, group_cols.len);
        errdefer allocator.free(group_col_indices);
        for (group_cols, 0..) |name, i| {
            group_col_indices[i] = types.findColumn(up_schema, name) orelse return Error.ColumnNotFound;
        }

        const agg_col_indices = try allocator.alloc(?usize, aggs.len);
        errdefer allocator.free(agg_col_indices);

        const output_schema = try allocator.alloc(Column, group_cols.len + aggs.len);
        errdefer allocator.free(output_schema);
        for (group_col_indices, 0..) |src_idx, i| output_schema[i] = up_schema[src_idx];
        for (aggs, 0..) |a, i| {
            agg_col_indices[i] = if (a.col) |name|
                (types.findColumn(up_schema, name) orelse return Error.ColumnNotFound)
            else
                null;
            output_schema[group_cols.len + i] = .{
                .name = a.as,
                .type = try aggOutputTypeFor(a, if (agg_col_indices[i]) |idx| up_schema[idx].type else null),
                // Every aggregate but the COUNT family finalizes to NULL over
                // zero qualifying inputs.
                .nullable = aggOutputNullable(a.func),
            };
        }
        for (aggs, agg_col_indices) |a, maybe_idx| {
            const t = if (maybe_idx) |idx| up_schema[idx].type else null;
            const arg2_t: ?Type = if (a.arg2_col) |name| blk: {
                const idx = types.findColumn(up_schema, name) orelse return Error.ColumnNotFound;
                break :blk up_schema[idx].type;
            } else null;
            try validateAggFn(a.func, t, a.params, arg2_t);
        }

        const output_columns = try allocator.alloc(ColumnStore, output_schema.len);
        errdefer allocator.free(output_columns);
        var inited: usize = 0;
        errdefer for (output_columns[0..inited]) |*c| c.deinit(allocator);
        for (output_schema, 0..) |col, i| {
            output_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        errdefer allocator.free(views);

        const cur_state = try allocator.alloc(AccState, aggs.len);
        errdefer allocator.free(cur_state);

        const self = try allocator.create(SortedAggregate);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .upstream = upstream,
            .group_col_indices = group_col_indices,
            .agg_col_indices = agg_col_indices,
            .aggs = aggs,
            .output_schema = output_schema,
            .output_columns = output_columns,
            .views = views,
            .cur_state = cur_state,
            .cur_key = .empty,
            .key_scratch = .empty,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *SortedAggregate) void {
        var up = self.upstream;
        up.deinit();
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.group_col_indices);
        self.allocator.free(self.agg_col_indices);
        self.allocator.free(self.cur_state);
        self.cur_key.deinit(self.allocator);
        self.key_scratch.deinit(self.allocator);
        self.arena.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *SortedAggregate) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *SortedAggregate, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    pub fn stats(self: *SortedAggregate) exec.PipelineStats {
        const up = self.upstream.stats();
        return .{ .upper_rows = up.upper_rows };
    }

    pub fn accountant(self: *SortedAggregate) ?*exec.memory.MemoryAccountant {
        // Bounded memory by construction — no budget reservation needed.
        return self.upstream.accountant();
    }

    pub fn explain(self: *SortedAggregate, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        // A Sort child below means "sorted then streamed"; its absence means
        // the input was already sorted on the group key (no sort needed).
        try exec.explainLine(out, allocator, depth, "StreamAggregate (sorted input)");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    fn beginGroup(self: *SortedAggregate) !void {
        // key_scratch already holds the new group's key bytes.
        self.cur_key.clearRetainingCapacity();
        try self.cur_key.appendSlice(self.allocator, self.key_scratch.items);
        const up_schema = self.upstream.outputSchema();
        for (self.aggs, self.agg_col_indices, self.cur_state) |a, maybe_idx, *s| {
            const in_t: ?Type = if (maybe_idx) |i| up_schema[i].type else null;
            s.* = initialState(a.func, in_t);
        }
        self.open = true;
    }

    fn finalizeGroup(self: *SortedAggregate) !void {
        try appendGroupKey(
            self.allocator,
            self.cur_key.items,
            self.group_col_indices,
            self.upstream.outputSchema(),
            self.output_columns[0..self.group_col_indices.len],
        );
        for (self.aggs, 0..) |a, ai| {
            const out_idx = self.group_col_indices.len + ai;
            // SortedAggregate keeps every distinct aggregate on its per-group
            // AccState set (O(1)-in-cardinality streaming reset between groups),
            // so it never uses the combined-distinct path.
            try appendAccToColumn(self.allocator, a, self.cur_state[ai], &self.output_columns[out_idx], self.output_schema[out_idx].type, null);
        }
        // Group done — drop its transient state, keep the buffer for reuse.
        _ = self.arena.reset(.retain_capacity);
        self.open = false;
    }

    pub fn next(self: *SortedAggregate) !?Batch {
        for (self.output_columns) |*c| c.clear();
        var out_rows: usize = 0;

        outer: while (out_rows < batch_size) {
            if (self.cur_batch == null) {
                if (self.upstream_done) {
                    if (self.open) {
                        try self.finalizeGroup();
                        out_rows += 1;
                    }
                    break;
                }
                self.cur_batch = try self.upstream.next();
                if (self.cur_batch == null) {
                    self.upstream_done = true;
                    continue;
                }
                self.cur_row = 0;
            }
            const batch = self.cur_batch.?;
            while (self.cur_row < batch.row_count) {
                self.key_scratch.clearRetainingCapacity();
                try buildCompoundGroupKey(self.allocator, &self.key_scratch, batch, self.group_col_indices, self.cur_row);

                if (self.open and !std.mem.eql(u8, self.key_scratch.items, self.cur_key.items)) {
                    try self.finalizeGroup();
                    out_rows += 1;
                    if (out_rows == batch_size) {
                        // Output batch is full and the current row hasn't
                        // started its group yet. Emit now; the next call
                        // resumes at this same row (open == false).
                        break :outer;
                    }
                }
                if (!self.open) try self.beginGroup();
                for (self.aggs, 0..) |a, ai| {
                    try updateState(self.arena.allocator(), &self.cur_state[ai], a, batch, self.agg_col_indices[ai], self.cur_row, self.cur_row + 1);
                }
                self.cur_row += 1;
            }
            if (self.cur_row >= batch.row_count) self.cur_batch = null;
        }

        if (out_rows == 0) return null;
        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        return Batch{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = @intCast(out_rows),
        };
    }
};

/// Bind a top-k hint to concrete aggregate indices. Returns `null` (fall back
/// to a full emit) unless grouping and *every* order key names an aggregate
/// whose output is orderable. On success the returned `keys` slice is
/// allocator-owned (freed in `Aggregate.deinit`).
fn resolveTopK(
    allocator: Allocator,
    hint: ?TopKHint,
    aggs: []const AggSpec,
    group_cols_len: usize,
    output_schema: []const Column,
) !?ResolvedTopK {
    const h = hint orelse return null;
    if (group_cols_len == 0 or h.keys.len == 0 or h.keys.len > MAX_TOPK_KEYS) return null;
    const rkeys = try allocator.alloc(ResolvedKey, h.keys.len);
    for (h.keys, rkeys) |hk, *rk| {
        const idx = findAggByName(aggs, hk.col) orelse {
            allocator.free(rkeys);
            return null;
        };
        if (!topkOrderable(aggs[idx].func, output_schema[group_cols_len + idx].type)) {
            allocator.free(rkeys);
            return null;
        }
        rk.* = .{ .agg_idx = idx, .desc = hk.desc };
    }
    return ResolvedTopK{ .k = h.k, .keys = rkeys };
}

fn findAggByName(aggs: []const AggSpec, name: []const u8) ?usize {
    for (aggs, 0..) |a, i| {
        if (std.mem.eql(u8, a.as, name)) return i;
    }
    return null;
}

/// Whether `func` (producing output type `out_t`) yields a value the top-k heap
/// can order. String MIN/MAX, stddev/variance, percentile and group_concat have
/// no numeric `OrderVal`; everything else (count, sum, avg, numeric MIN/MAX)
/// does. Aggregates never surface NULL — empty/all-null groups emit 0/0.0 — so
/// `aggOrderValue` is always defined for an orderable key.
fn topkOrderable(func: AggFunc, out_t: Type) bool {
    return switch (func) {
        .count, .count_if, .count_distinct, .sum, .avg => true,
        .min, .max => !out_t.isString(),
        else => false,
    };
}

/// Extract a comparable order value from a finalized accumulator, mirroring the
/// value `appendAccToColumn` would emit — including the 0/0.0 defaults for
/// empty MIN/MAX/AVG — so the heap orders groups identically to the downstream
/// OrderBy. Only reached for variants `topkOrderable` accepts.
fn aggOrderValue(s: AccState) OrderVal {
    return switch (s) {
        .count => |c| .{ .int = @intCast(c) },
        .sum_int => |v| .{ .int = v.v },
        .sum_float => |v| .{ .float = v.v },
        .min_int, .max_int => |m| .{ .int = m orelse 0 },
        .min_large, .max_large => |m| .{ .int = if (m.present) m.v else 0 },
        .min_float, .max_float => |m| .{ .float = m orelse 0.0 },
        .avg => |a| .{ .float = if (a.count == 0) 0.0 else a.sum / @as(f64, @floatFromInt(a.count)) / a.scale_div },
        .distinct => |d| .{ .int = @intCast(d.set.count() + @as(u32, @intFromBool(d.seen_empty))) },
        .distinct_int => |set| .{ .int = @intCast(set.count()) },
        .distinct_int64 => |set| .{ .int = @intCast(set.count()) },
        .distinct_int32 => |set| .{ .int = @intCast(set.count()) },
        else => unreachable,
    };
}

fn ovOrder(a: OrderVal, b: OrderVal) std.math.Order {
    return switch (a) {
        .int => |x| std.math.order(x, b.int),
        .float => |x| std.math.order(x, b.float),
    };
}

/// Build a heap entry, caching each ORDER BY key's order value up front so the
/// comparator never touches the (large, scattered) accumulator union. A key
/// bound to a combined COUNT(DISTINCT int) aggregate reads its count from the
/// gid-indexed `cd` counter (the AccState set is empty on that path); every
/// other key decodes its `AccState`.
fn topkEntry(gid: u32, state: []AccState, keys: []const ResolvedKey, cd: []const ?CombinedDistinct) TopKEntry {
    var e = TopKEntry{ .gid = gid, .vals = undefined };
    for (keys, 0..) |kk, i| {
        e.vals[i] = if (cd[kk.agg_idx]) |c|
            .{ .int = @intCast(c.counts.items[gid]) }
        else
            aggOrderValue(state[kk.agg_idx]);
    }
    return e;
}

/// Lexicographic comparison of two groups under the resolved ORDER BY keys.
/// `.lt` ⟺ `a` ranks before `b` in the final ordering (i.e. `a` is more
/// preferred / closer to the kept set), honoring each key's direction.
fn topkOrder(a: TopKEntry, b: TopKEntry, keys: []const ResolvedKey) std.math.Order {
    for (keys, 0..) |key, i| {
        const ord = ovOrder(a.vals[i], b.vals[i]);
        if (ord != .eq) return if (key.desc) ord.invert() else ord;
    }
    return .eq;
}

/// True when `a` ranks after `b` (so `a` is the better eviction candidate).
fn topkLessPreferred(a: TopKEntry, b: TopKEntry, keys: []const ResolvedKey) bool {
    return topkOrder(a, b, keys) == .gt;
}

/// Min-heap on preference: the root is the least-preferred kept entry, so a new
/// candidate need only beat the root to earn a slot.
fn topkSiftDown(heap: []TopKEntry, start: usize, n: usize, keys: []const ResolvedKey) void {
    var i = start;
    while (true) {
        const l = 2 * i + 1;
        const r = 2 * i + 2;
        var least = i;
        if (l < n and topkLessPreferred(heap[l], heap[least], keys)) least = l;
        if (r < n and topkLessPreferred(heap[r], heap[least], keys)) least = r;
        if (least == i) break;
        std.mem.swap(TopKEntry, &heap[i], &heap[least]);
        i = least;
    }
}

fn topkBuildHeap(heap: []TopKEntry, keys: []const ResolvedKey) void {
    const n = heap.len;
    if (n < 2) return;
    var i = n / 2;
    while (i > 0) {
        i -= 1;
        topkSiftDown(heap, i, n, keys);
    }
}

// Fold a SIMD-reduced extreme into the running MIN/MAX accumulator. Used by
// the no-null fast paths; `m` comes from `simd.minOf`/`maxOf` over the batch.
fn foldMinInt(s: *AccState, m: i64) void {
    if (s.min_int == null or m < s.min_int.?) s.min_int = m;
}
fn foldMaxInt(s: *AccState, m: i64) void {
    if (s.max_int == null or m > s.max_int.?) s.max_int = m;
}
fn foldMinLarge(s: *AccState, m: i128) void {
    if (!s.min_large.present or m < s.min_large.v) s.min_large = .{ .v = m, .present = true };
}
fn foldMaxLarge(s: *AccState, m: i128) void {
    if (!s.max_large.present or m > s.max_large.v) s.max_large = .{ .v = m, .present = true };
}
fn foldMinFloat(s: *AccState, m: f64) void {
    if (s.min_float == null or m < s.min_float.?) s.min_float = m;
}
fn foldMaxFloat(s: *AccState, m: f64) void {
    if (s.max_float == null or m > s.max_float.?) s.max_float = m;
}

/// Allocate one aggregate's accumulator column to `capacity` cells and fill it
/// with the aggregate's initial value (mirroring `initialState`). The narrow
/// kinds get a typed slice; everything else gets a `[]AccState` (`.other`).
fn initAggCol(aa: Allocator, func: AggFunc, in: ?Type, capacity: usize) !AggCol {
    return switch (aggColKind(func, in)) {
        .count => blk: {
            const s = try aa.alloc(u64, capacity);
            @memset(s, 0);
            break :blk .{ .count = s };
        },
        .sum_int => blk: {
            const s = try aa.alloc(SumIntAcc, capacity);
            @memset(s, .{});
            break :blk .{ .sum_int = s };
        },
        .sum_float => blk: {
            const s = try aa.alloc(SumFloatAcc, capacity);
            @memset(s, .{});
            break :blk .{ .sum_float = s };
        },
        .avg => blk: {
            const s = try aa.alloc(AvgAcc, capacity);
            @memset(s, .{ .sum = 0.0, .count = 0, .scale_div = avgScaleDiv(in) });
            break :blk .{ .avg = s };
        },
        .min_int => blk: {
            const s = try aa.alloc(?i64, capacity);
            @memset(s, null);
            break :blk .{ .min_int = s };
        },
        .max_int => blk: {
            const s = try aa.alloc(?i64, capacity);
            @memset(s, null);
            break :blk .{ .max_int = s };
        },
        .min_float => blk: {
            const s = try aa.alloc(?f64, capacity);
            @memset(s, null);
            break :blk .{ .min_float = s };
        },
        .max_float => blk: {
            const s = try aa.alloc(?f64, capacity);
            @memset(s, null);
            break :blk .{ .max_float = s };
        },
        .other => blk: {
            const s = try aa.alloc(AccState, capacity);
            const init = initialState(func, in);
            @memset(s, init);
            break :blk .{ .other = s };
        },
    };
}

/// Grow one aggregate's column to `new_capacity`, initializing only the newly
/// added cells (`old_len .. new_capacity`) to the aggregate's initial value.
fn growAggCol(aa: Allocator, col: *AggCol, func: AggFunc, in: ?Type, new_capacity: usize) !void {
    switch (col.*) {
        .count => |*s| {
            const old = s.len;
            s.* = try aa.realloc(s.*, new_capacity);
            @memset(s.*[old..], 0);
        },
        .sum_int => |*s| {
            const old = s.len;
            s.* = try aa.realloc(s.*, new_capacity);
            @memset(s.*[old..], .{});
        },
        .sum_float => |*s| {
            const old = s.len;
            s.* = try aa.realloc(s.*, new_capacity);
            @memset(s.*[old..], .{});
        },
        .avg => |*s| {
            const old = s.len;
            s.* = try aa.realloc(s.*, new_capacity);
            @memset(s.*[old..], .{ .sum = 0.0, .count = 0, .scale_div = avgScaleDiv(in) });
        },
        .min_int, .max_int => |*s| {
            const old = s.len;
            s.* = try aa.realloc(s.*, new_capacity);
            @memset(s.*[old..], null);
        },
        .min_float, .max_float => |*s| {
            const old = s.len;
            s.* = try aa.realloc(s.*, new_capacity);
            @memset(s.*[old..], null);
        },
        .other => |*s| {
            const old = s.len;
            s.* = try aa.realloc(s.*, new_capacity);
            @memset(s.*[old..], initialState(func, in));
        },
    }
}

pub fn initialState(func: AggFunc, in: ?Type) AccState {
    return switch (func) {
        .count => .{ .count = 0 },
        .sum => if (in != null and in.?.isFloat())
            .{ .sum_float = .{} }
        else
            // i128 accumulator covers BIGINT, LARGEINT, DECIMAL64, DECIMAL128.
            .{ .sum_int = .{} },
        .min => if (in != null and in.?.isFloat())
            .{ .min_float = null }
        else if (in != null and in.?.isString())
            .{ .min_str = null }
        else if (in != null and (in.? == .largeint or in.? == .decimal128))
            .{ .min_large = .{} }
        else
            .{ .min_int = null },
        .max => if (in != null and in.?.isFloat())
            .{ .max_float = null }
        else if (in != null and in.?.isString())
            .{ .max_str = null }
        else if (in != null and (in.? == .largeint or in.? == .decimal128))
            .{ .max_large = .{} }
        else
            .{ .max_int = null },
        .avg => .{ .avg = .{ .sum = 0.0, .count = 0, .scale_div = avgScaleDiv(in) } },
        .count_if => .{ .count = 0 },
        .bool_and => .{ .bool_acc = .{ .value = true } },
        .bool_or => .{ .bool_acc = .{ .value = false } },
        .any_value, .first, .last => .{ .value_acc = .{} },
        .max_by => .{ .max_by = .{} },
        .bit_and, .bit_or, .bit_xor => .{ .bitwise = .{} },
        .sum_distinct, .avg_distinct => .{ .distinct_numeric = .empty },
        .stddev_pop, .stddev_samp, .var_pop, .var_samp => .{ .welford = .{} },
        .count_distinct => blk: {
            if (in) |t| if (intKeyBits(t)) |vb| break :blk if (vb <= 32)
                .{ .distinct_int32 = .empty }
            else if (vb <= 64)
                .{ .distinct_int64 = .empty }
            else
                .{ .distinct_int = .empty };
            break :blk .{ .distinct = .{} };
        },
        .percentile => .{ .percentile_values = .empty },
        .group_concat => .{ .concat = null },
        .udf => unreachable,
    };
}

/// Pure GROUP BY output schema: the group-key columns (carried from the input
/// schema) then one column per aggregate (name = `as`, type from the func +
/// input column type). Shared by `Aggregate.create` and the adaptive router so
/// a deferred GROUP BY can report its schema before it has picked a strategy.
/// Caller owns the returned slice.
pub fn outputSchemaFor(
    allocator: Allocator,
    up_schema: []const Column,
    group_cols: []const []const u8,
    aggs: []const AggSpec,
) ![]Column {
    const out = try allocator.alloc(Column, group_cols.len + aggs.len);
    errdefer allocator.free(out);
    for (group_cols, 0..) |name, i| {
        const idx = types.findColumn(up_schema, name) orelse return Error.ColumnNotFound;
        out[i] = up_schema[idx];
    }
    for (aggs, 0..) |a, i| {
        const in_t: ?Type = if (a.col) |name| blk: {
            const idx = types.findColumn(up_schema, name) orelse return Error.ColumnNotFound;
            break :blk up_schema[idx].type;
        } else null;
        out[group_cols.len + i] = .{
            .name = a.as,
            .type = try aggOutputTypeFor(a, in_t),
            .nullable = aggOutputNullable(a.func),
        };
    }
    return out;
}

/// Output type for an AggSpec, honoring an explicit override (used by the
/// affine-aggregate reduction to pin an int SUM base to i128). The override
/// is only ever a wider-or-equal type than the canonical one, so finalize /
/// append stay correct (`.largeint` appends the i128 accumulator directly).
pub fn aggOutputTypeFor(a: AggSpec, in: ?Type) !Type {
    if (a.out_type_override) |t| return t;
    return aggOutputType(a.func, in);
}

/// Whether an aggregate's output column can hold NULL: everything except the
/// COUNT family (which finalizes to 0 over zero qualifying inputs).
pub fn aggOutputNullable(func: AggFunc) bool {
    return switch (func) {
        .count, .count_if, .count_distinct => false,
        else => true,
    };
}

/// True for output types that carry a usable i128 min/max range (matches
/// `ColStat.min/max`'s int-family-only contract). Float/string/uuid → false.
fn intFamilyOutput(t: Type) bool {
    return t.isInteger() or t.isDecimal() or t == .boolean or t.isTemporal();
}

/// Provable min/max for one aggregate's output column, given the source
/// column's stat (`src`, null for COUNT(*)), the output type, and the upstream
/// row upper bound. Only int-family outputs carry a range; everything else
/// (float SUM/AVG, string GROUP_CONCAT, etc.) stays null. Every i128 step is
/// overflow-checked → null bound on overflow, never a wrong one. The `ndv`
/// field is left at its default and overwritten by the caller.
fn aggColStat(a: AggSpec, out_type: Type, src: ?exec.ColStat, upper_rows: u64) exec.ColStat {
    const n: i128 = @intCast(upper_rows);
    switch (a.func) {
        .count, .count_if, .count_distinct => return .{ .min = 0, .max = n },
        .min, .max => {
            if (!intFamilyOutput(out_type)) return .{};
            const s = src orelse return .{};
            // MIN/MAX of a column lie within the column's own range.
            return .{ .min = s.min, .max = s.max };
        },
        .sum => {
            if (!intFamilyOutput(out_type)) return .{}; // float SUM → double
            const s = src orelse return .{};
            const lo = s.min orelse return .{};
            const hi = s.max orelse return .{};
            // Σ over `upper_rows` rows of values in [lo, hi]: the low end is
            // n·lo, the high end n·hi (n ≥ 0). Sign handled naturally since
            // lo ≤ hi ⇒ n·lo ≤ n·hi for n ≥ 0.
            const min = std.math.mul(i128, n, lo) catch return .{};
            const max = std.math.mul(i128, n, hi) catch return .{};
            return .{ .min = min, .max = max };
        },
        .avg => {
            // AVG is DOUBLE today (→ null range). Guard on the output type so
            // that an int-family AVG output, if it ever exists, inherits the
            // column range (the mean lies within [lo, hi]).
            if (!intFamilyOutput(out_type)) return .{};
            const s = src orelse return .{};
            return .{ .min = s.min, .max = s.max };
        },
        else => return .{},
    }
}

fn aggOutputType(func: AggFunc, in: ?Type) !Type {
    return switch (func) {
        .count, .count_if, .count_distinct => .bigint,
        .sum => blk: {
            const t = in orelse return Error.AggregateColumnRequired;
            // DESIGN.md §3.4: SUM(DECIMAL(p, s)) -> DECIMAL(38, s).
            if (t.decimalSpec()) |spec| break :blk .{ .decimal128 = .{ .p = 38, .s = spec.s } };
            if (t.isFloat()) break :blk .double;
            // A 64-bit integer SUM widens its accumulator and result to i128 so a
            // large-magnitude sum (e.g. SUM(UserID)) can't overflow; 8/16/32-bit
            // inputs stay in i64 (a sum can't overflow i64 short of billions of
            // rows). See DESIGN.md §3.4 (accumulator promotion).
            if (t == .largeint or t == .bigint) break :blk .largeint;
            break :blk .bigint;
        },
        .min, .max => in orelse return Error.AggregateNoSpecs,
        .avg, .sum_distinct, .avg_distinct, .stddev_pop, .stddev_samp, .var_pop, .var_samp, .percentile => .double,
        .bool_and, .bool_or => .boolean,
        .any_value, .first, .last, .max_by => in orelse return Error.AggregateColumnRequired,
        .bit_and, .bit_or, .bit_xor => .bigint,
        .group_concat => .string,
        .udf => Error.AggregateUnsupportedType,
    };
}

pub fn validateAggFn(func: AggFunc, in: ?Type, params: AggParams, arg2_in: ?Type) !void {
    switch (func) {
        .count => return,
        .sum, .avg, .sum_distinct, .avg_distinct, .stddev_pop, .stddev_samp, .var_pop, .var_samp => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (!(t.isInteger() or t.isDecimal() or t == .boolean or t == .float or t == .double)) {
                return Error.AggregateUnsupportedType;
            }
        },
        .count_if, .bool_and, .bool_or => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (t != .boolean) return Error.AggregateUnsupportedType;
        },
        .any_value, .first, .last => {
            _ = in orelse return Error.AggregateColumnRequired;
        },
        .max_by => {
            _ = in orelse return Error.AggregateColumnRequired;
            _ = arg2_in orelse return Error.AggregateColumnRequired;
        },
        .bit_and, .bit_or, .bit_xor => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (!(t == .boolean or t == .tinyint or t == .smallint or t == .int or t == .bigint)) return Error.AggregateUnsupportedType;
        },
        .min, .max => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (!(t.isInteger() or t.isDecimal() or t == .boolean or t == .float or t == .double or t == .date or t == .datetime or t.isString())) {
                return Error.AggregateUnsupportedType;
            }
        },
        .count_distinct => {
            // Any column type works (we hash the encoded bytes).
            _ = in orelse return Error.AggregateColumnRequired;
        },
        .percentile => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (!(t.isInteger() or t.isDecimal() or t == .boolean or t == .float or t == .double or t == .date or t == .datetime)) {
                return Error.AggregateUnsupportedType;
            }
            switch (params) {
                .percentile => |p| if (p < 0.0 or p > 1.0) return Error.AggregateInvalidParam,
                else => return Error.AggregateInvalidParam,
            }
        },
        .group_concat => {
            const t = in orelse return Error.AggregateColumnRequired;
            if (!t.isString()) return Error.AggregateUnsupportedType;
            switch (params) {
                .separator => {},
                else => return Error.AggregateInvalidParam,
            }
        },
        .udf => return Error.AggregateUnsupportedType,
    }
}

/// Single-row accumulator update for the hash-aggregate inner loop, where every
/// call covers exactly one row. `updateState` is built around contiguous-range
/// SIMD reductions; routing a one-element range through it pays a kernel
/// call + setup per value (millions of times). COUNT and SUM — the common hot
/// aggregates — get a direct scalar update here; everything else (MIN/MAX, AVG,
/// stddev, distinct, percentile, group_concat) defers to `updateState` so there
/// is exactly one definition of their semantics.
fn updateStateRow(aa: Allocator, s: *AccState, spec: AggSpec, batch: Batch, col_idx: ?usize, row: u32) !void {
    switch (spec.func) {
        .count => {
            if (col_idx) |idx| {
                if (batch.values[idx].isValid(row)) s.count += 1;
            } else s.count += 1;
        },
        .count_if => {
            const view = batch.values[col_idx.?];
            if (view.isValid(row) and view.data.boolean[row] != 0) s.count += 1;
        },
        .sum => {
            const view = batch.values[col_idx.?];
            if (!view.isValid(row)) return;
            switch (view.data) {
                .int => |sl| s.sum_int.v += sl[row],
                .smallint => |sl| s.sum_int.v += sl[row],
                .tinyint => |sl| s.sum_int.v += sl[row],
                .boolean => |sl| s.sum_int.v += sl[row],
                .bigint, .decimal64 => |sl| s.sum_int.v += sl[row],
                .largeint, .decimal128 => |sl| s.sum_int.v += sl[row],
                .float => |sl| {
                    s.sum_float.v += sl[row];
                    s.sum_float.seen = true;
                    return;
                },
                .double => |sl| {
                    s.sum_float.v += sl[row];
                    s.sum_float.seen = true;
                    return;
                },
                else => unreachable,
            }
            s.sum_int.seen = true;
        },
        .udf => return Error.AggregateUnsupportedType,
        else => try updateState(aa, s, spec, batch, col_idx, row, row + 1),
    }
}

pub fn updateState(
    aa: Allocator,
    s: *AccState,
    spec: AggSpec,
    batch: Batch,
    col_idx: ?usize,
    row_start: u32,
    row_end: u32,
) !void {
    const func = spec.func;
    switch (func) {
        .count => {
            // COUNT(*) counts every row. COUNT(col) skips NULLs.
            if (col_idx) |idx| {
                const view = batch.values[idx];
                if (view.nulls == null) {
                    s.count += @as(u64, row_end - row_start);
                } else {
                    var r: u32 = row_start;
                    while (r < row_end) : (r += 1) {
                        if (view.isValid(r)) s.count += 1;
                    }
                }
            } else {
                s.count += @as(u64, row_end - row_start);
            }
        },
        .sum => {
            const idx = col_idx.?;
            const view = batch.values[idx];
            const lo = row_start;
            const hi = row_end;
            if (view.nulls == null) {
                // No nulls in this column: reduce the contiguous range with a
                // tight SIMD kernel (no per-row validity branch). Small ints
                // widen through i64 lanes; 64-bit-plus inputs stay scalar
                // (i64-lane overflow / i128 isn't a SIMD win) but lose the
                // per-row branch.
                switch (view.data) {
                    .int => |sl| s.sum_int.v += simd.sumWiden(i32, sl[lo..hi]),
                    .smallint => |sl| s.sum_int.v += simd.sumWiden(i16, sl[lo..hi]),
                    .tinyint => |sl| s.sum_int.v += simd.sumWiden(i8, sl[lo..hi]),
                    .boolean => |sl| s.sum_int.v += simd.sumWiden(u8, sl[lo..hi]),
                    .float => |sl| s.sum_float.v += simd.sumFloat(f32, sl[lo..hi]),
                    .double => |sl| s.sum_float.v += simd.sumFloat(f64, sl[lo..hi]),
                    .bigint, .decimal64 => |sl| for (sl[lo..hi]) |v| {
                        s.sum_int.v += v;
                    },
                    .largeint, .decimal128 => |sl| for (sl[lo..hi]) |v| {
                        s.sum_int.v += v;
                    },
                    else => unreachable,
                }
                if (hi > lo) switch (view.data) {
                    .float, .double => s.sum_float.seen = true,
                    else => s.sum_int.seen = true,
                };
                return;
            }
            switch (view.data) {
                .int => |s_int| for (s_int[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int.v += v;
                    s.sum_int.seen = true;
                },
                .bigint => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int.v += v;
                    s.sum_int.seen = true;
                },
                .boolean => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int.v += v;
                    s.sum_int.seen = true;
                },
                .tinyint => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int.v += v;
                    s.sum_int.seen = true;
                },
                .smallint => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int.v += v;
                    s.sum_int.seen = true;
                },
                .largeint => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int.v += v;
                    s.sum_int.seen = true;
                },
                .decimal64 => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int.v += v;
                    s.sum_int.seen = true;
                },
                .decimal128 => |s_b| for (s_b[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_int.v += v;
                    s.sum_int.seen = true;
                },
                .float => |s_f| for (s_f[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_float.v += v;
                    s.sum_float.seen = true;
                },
                .double => |s_d| for (s_d[lo..hi], lo..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.sum_float.v += v;
                    s.sum_float.seen = true;
                },
                else => unreachable,
            }
        },
        .min => {
            const idx = col_idx.?;
            const view = batch.values[idx];
            const lo = row_start;
            const hi = row_end;
            // No-null numeric fast path: SIMD-reduce the range, fold the one
            // extreme into the accumulator. Strings can't reduce numerically —
            // they fall through to the scalar loop below.
            if (view.nulls == null and hi > lo) switch (view.data) {
                .int, .date => |sl| return foldMinInt(s, simd.minOf(i32, sl[lo..hi])),
                .bigint, .datetime => |sl| return foldMinInt(s, simd.minOf(i64, sl[lo..hi])),
                .boolean => |sl| return foldMinInt(s, simd.minOf(u8, sl[lo..hi])),
                .tinyint => |sl| return foldMinInt(s, simd.minOf(i8, sl[lo..hi])),
                .smallint => |sl| return foldMinInt(s, simd.minOf(i16, sl[lo..hi])),
                .decimal64 => |sl| return foldMinInt(s, simd.minOf(i64, sl[lo..hi])),
                .largeint, .decimal128 => |sl| return foldMinLarge(s, simd.minOf(i128, sl[lo..hi])),
                .float => |sl| return foldMinFloat(s, simd.minOf(f32, sl[lo..hi])),
                .double => |sl| return foldMinFloat(s, simd.minOf(f64, sl[lo..hi])),
                .varchar, .string, .char, .json => {},
                else => unreachable,
            };
            switch (view.data) {
                .int, .date => |s_int| for (s_int[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .bigint, .datetime => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.min_int == null or v < s.min_int.?) s.min_int = v;
                },
                .boolean => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .tinyint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .smallint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.min_int == null or iv < s.min_int.?) s.min_int = iv;
                },
                .largeint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (!s.min_large.present or v < s.min_large.v) s.min_large = .{ .v = v, .present = true };
                },
                .decimal64 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.min_int == null or v < s.min_int.?) s.min_int = v;
                },
                .decimal128 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (!s.min_large.present or v < s.min_large.v) s.min_large = .{ .v = v, .present = true };
                },
                .float => |s_f| for (s_f[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const fv: f64 = v;
                    if (s.min_float == null or fv < s.min_float.?) s.min_float = fv;
                },
                .double => |s_d| for (s_d[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.min_float == null or v < s.min_float.?) s.min_float = v;
                },
                .varchar, .string, .char, .json => {
                    var r: u32 = row_start;
                    while (r < row_end) : (r += 1) {
                        if (!view.isValid(r)) continue;
                        const bytes = stringRowBytes(view, r);
                        if (s.min_str == null or std.mem.order(u8, bytes, s.min_str.?) == .lt) {
                            s.min_str = try aa.dupe(u8, bytes);
                        }
                    }
                },
                else => unreachable,
            }
        },
        .max => {
            const idx = col_idx.?;
            const view = batch.values[idx];
            const lo = row_start;
            const hi = row_end;
            if (view.nulls == null and hi > lo) switch (view.data) {
                .int, .date => |sl| return foldMaxInt(s, simd.maxOf(i32, sl[lo..hi])),
                .bigint, .datetime => |sl| return foldMaxInt(s, simd.maxOf(i64, sl[lo..hi])),
                .boolean => |sl| return foldMaxInt(s, simd.maxOf(u8, sl[lo..hi])),
                .tinyint => |sl| return foldMaxInt(s, simd.maxOf(i8, sl[lo..hi])),
                .smallint => |sl| return foldMaxInt(s, simd.maxOf(i16, sl[lo..hi])),
                .decimal64 => |sl| return foldMaxInt(s, simd.maxOf(i64, sl[lo..hi])),
                .largeint, .decimal128 => |sl| return foldMaxLarge(s, simd.maxOf(i128, sl[lo..hi])),
                .float => |sl| return foldMaxFloat(s, simd.maxOf(f32, sl[lo..hi])),
                .double => |sl| return foldMaxFloat(s, simd.maxOf(f64, sl[lo..hi])),
                .varchar, .string, .char, .json => {},
                else => unreachable,
            };
            switch (view.data) {
                .int, .date => |s_int| for (s_int[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .bigint, .datetime => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.max_int == null or v > s.max_int.?) s.max_int = v;
                },
                .boolean => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .tinyint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .smallint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const iv: i64 = v;
                    if (s.max_int == null or iv > s.max_int.?) s.max_int = iv;
                },
                .largeint => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (!s.max_large.present or v > s.max_large.v) s.max_large = .{ .v = v, .present = true };
                },
                .decimal64 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.max_int == null or v > s.max_int.?) s.max_int = v;
                },
                .decimal128 => |s_b| for (s_b[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (!s.max_large.present or v > s.max_large.v) s.max_large = .{ .v = v, .present = true };
                },
                .float => |s_f| for (s_f[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    const fv: f64 = v;
                    if (s.max_float == null or fv > s.max_float.?) s.max_float = fv;
                },
                .double => |s_d| for (s_d[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    if (s.max_float == null or v > s.max_float.?) s.max_float = v;
                },
                .varchar, .string, .char, .json => {
                    var r: u32 = row_start;
                    while (r < row_end) : (r += 1) {
                        if (!view.isValid(r)) continue;
                        const bytes = stringRowBytes(view, r);
                        if (s.max_str == null or std.mem.order(u8, bytes, s.max_str.?) == .gt) {
                            s.max_str = try aa.dupe(u8, bytes);
                        }
                    }
                },
                else => unreachable,
            }
        },
        .avg => {
            const idx = col_idx.?;
            const view = batch.values[idx];
            const lo = row_start;
            const hi = row_end;
            // No-null fast path: SIMD-sum the range, count is the row count.
            // Integers sum exactly in i128 then convert (more accurate than the
            // per-row f64 accumulation it replaces); floats use f64 lanes.
            if (view.nulls == null and hi > lo) {
                switch (view.data) {
                    .int => |sl| s.avg.sum += @floatFromInt(simd.sumWiden(i32, sl[lo..hi])),
                    .smallint => |sl| s.avg.sum += @floatFromInt(simd.sumWiden(i16, sl[lo..hi])),
                    .tinyint => |sl| s.avg.sum += @floatFromInt(simd.sumWiden(i8, sl[lo..hi])),
                    .boolean => |sl| s.avg.sum += @floatFromInt(simd.sumWiden(u8, sl[lo..hi])),
                    .bigint, .decimal64 => |sl| for (sl[lo..hi]) |v| {
                        s.avg.sum += @floatFromInt(v);
                    },
                    .largeint, .decimal128 => |sl| for (sl[lo..hi]) |v| {
                        s.avg.sum += @floatFromInt(v);
                    },
                    .float => |sl| s.avg.sum += simd.sumFloat(f32, sl[lo..hi]),
                    .double => |sl| s.avg.sum += simd.sumFloat(f64, sl[lo..hi]),
                    else => unreachable,
                }
                s.avg.count += hi - lo;
                return;
            }
            switch (view.data) {
                .int, .bigint, .boolean, .tinyint, .smallint, .decimal64 => {
                    avgUpdateInt(s, view, row_start, row_end);
                },
                .largeint => |slice| for (slice[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.avg.sum += @as(f64, @floatFromInt(v));
                    s.avg.count += 1;
                },
                .decimal128 => |slice| for (slice[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.avg.sum += @as(f64, @floatFromInt(v));
                    s.avg.count += 1;
                },
                .float => |slice| for (slice[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.avg.sum += v;
                    s.avg.count += 1;
                },
                .double => |slice| for (slice[row_start..row_end], row_start..) |v, r| {
                    if (!view.isValid(r)) continue;
                    s.avg.sum += v;
                    s.avg.count += 1;
                },
                else => unreachable,
            }
        },
        .count_if => {
            const view = batch.values[col_idx.?];
            const vals = view.data.boolean;
            var r: u32 = row_start;
            while (r < row_end) : (r += 1) {
                if (view.isValid(r) and vals[r] != 0) s.count += 1;
            }
        },
        .bool_and, .bool_or => {
            try boolUpdate(s, func, batch.values[col_idx.?], row_start, row_end);
        },
        .any_value, .first, .last => {
            try valueUpdate(aa, s, func, batch.values[col_idx.?], row_start, row_end);
        },
        .max_by => {
            const key_name = spec.arg2_col orelse return Error.AggregateColumnRequired;
            const key_idx = types.findColumn(batch.schema, key_name) orelse return Error.ColumnNotFound;
            try maxByUpdate(aa, s, batch.values[col_idx.?], batch.values[key_idx], row_start, row_end);
        },
        .bit_and, .bit_or, .bit_xor => {
            try bitwiseUpdate(s, func, batch.values[col_idx.?], row_start, row_end);
        },
        .sum_distinct, .avg_distinct => {
            try distinctNumericUpdate(aa, s, batch.values[col_idx.?], row_start, row_end);
        },
        .stddev_pop, .stddev_samp, .var_pop, .var_samp => {
            try welfordUpdate(s, batch.values[col_idx.?], row_start, row_end);
        },
        .count_distinct => {
            try distinctUpdate(aa, s, batch.values[col_idx.?], row_start, row_end);
        },
        .percentile => {
            try percentileUpdate(aa, s, batch.values[col_idx.?], row_start, row_end);
        },
        .group_concat => {
            const sep = switch (spec.params) {
                .separator => |sv| sv,
                else => return Error.AggregateInvalidParam,
            };
            try groupConcatUpdate(aa, s, batch.values[col_idx.?], row_start, row_end, sep);
        },
        .udf => return Error.AggregateUnsupportedType,
    }
}

/// Welford's online algorithm — numerically stable mean + M2 (sum of
/// squared deviations from the running mean). Variance = M2 / n or
/// M2 / (n-1) depending on population vs sample. Updates one row at a
/// time so the result is invariant to batch boundaries.
fn welfordUpdate(s: *AccState, view: ColumnView, row_start: u32, row_end: u32) !void {
    switch (view.data) {
        inline .int, .bigint, .boolean, .tinyint, .smallint, .largeint, .decimal64, .decimal128 => |slice| {
            for (slice[row_start..row_end], row_start..) |v, r| {
                if (!view.isValid(r)) continue;
                welfordStep(&s.welford, @as(f64, @floatFromInt(v)));
            }
        },
        inline .float, .double => |slice| {
            for (slice[row_start..row_end], row_start..) |v, r| {
                if (!view.isValid(r)) continue;
                welfordStep(&s.welford, @as(f64, v));
            }
        },
        else => unreachable,
    }
}

fn welfordStep(w: *WelfordAcc, x: f64) void {
    w.count += 1;
    const n: f64 = @floatFromInt(w.count);
    const delta = x - w.mean;
    w.mean += delta / n;
    const delta2 = x - w.mean;
    w.m2 += delta * delta2;
}

/// COUNT(DISTINCT col): hash the encoded value bytes; first sighting
/// arena-dups the key for storage. Validation rejects NULL — SQL
/// semantics say NULL is excluded from DISTINCT counts.
fn distinctUpdate(aa: Allocator, s: *AccState, view: ColumnView, row_start: u32, row_end: u32) !void {
    switch (s.*) {
        .distinct_int64 => |*set| {
            try set.ensureFor(aa, row_end - row_start);
            switch (view.data) {
                .boolean => |sl| insertDistinctRange(set, view, u8, sl, row_start, row_end),
                .tinyint => |sl| insertDistinctRange(set, view, i8, sl, row_start, row_end),
                .smallint => |sl| insertDistinctRange(set, view, i16, sl, row_start, row_end),
                .int, .date => |sl| insertDistinctRange(set, view, i32, sl, row_start, row_end),
                .bigint, .datetime, .decimal64 => |sl| insertDistinctRange(set, view, i64, sl, row_start, row_end),
                else => unreachable,
            }
        },
        .distinct_int32 => |*set| {
            try set.ensureFor(aa, row_end - row_start);
            switch (view.data) {
                .boolean => |sl| insertDistinctRange(set, view, u8, sl, row_start, row_end),
                .tinyint => |sl| insertDistinctRange(set, view, i8, sl, row_start, row_end),
                .smallint => |sl| insertDistinctRange(set, view, i16, sl, row_start, row_end),
                .int, .date => |sl| insertDistinctRange(set, view, i32, sl, row_start, row_end),
                else => unreachable,
            }
        },
        .distinct_int => |*set| {
            var r: u32 = row_start;
            while (r < row_end) : (r += 1) {
                if (!view.isValid(r)) continue;
                try set.put(aa, distinctIntKey(view, r), {});
            }
        },
        .distinct => |*d| {
            // The empty string is one distinct value (NULL is excluded above)
            // but dominates optional text columns; count it via `seen_empty`
            // rather than hashing + probing the map for every empty row. Only
            // string-family columns have an empty form — the other types
            // reaching this path are float/double, where `sv` stays null.
            const sv: ?storage.StringView = switch (view.data) {
                .string, .varchar, .char, .json => |strv| strv,
                else => null,
            };
            var scratch: std.ArrayList(u8) = .empty;
            defer scratch.deinit(aa);
            var r: u32 = row_start;
            while (r < row_end) : (r += 1) {
                if (!view.isValid(r)) continue;
                if (sv) |strv| if (strv.rowBytes(r).len == 0) {
                    d.seen_empty = true;
                    continue;
                };
                scratch.clearRetainingCapacity();
                try encodeOneValue(aa, &scratch, view, r);
                const gop = try d.set.getOrPut(aa, scratch.items);
                if (!gop.found_existing) {
                    gop.key_ptr.* = try aa.dupe(u8, scratch.items);
                }
            }
        },
        else => unreachable,
    }
}

/// Insert a contiguous row range's integer values into a `DistinctU64Set` via
/// the software-prefetch pipeline. The (column type → u64 key) decision is the
/// caller's one-time switch; the key is a zero-extension of the value's
/// two's-complement bits — injective within the column's ≤64-bit domain, so the
/// distinct count is exact. A look-ahead `@prefetch` hides the random slot miss;
/// NULLs are skipped (excluded from DISTINCT). `ensureFor` must have reserved
/// the range up front so no grow invalidates the look-ahead's slot addresses.
fn insertDistinctRange(
    set: anytype,
    view: ColumnView,
    comptime T: type,
    sl: []const T,
    row_start: u32,
    row_end: u32,
) void {
    const U = std.meta.Int(.unsigned, @bitSizeOf(T));
    const KeyT = std.meta.Elem(@TypeOf(set.slots));
    const D: u32 = 12;
    if (view.nulls == null) {
        var r: u32 = row_start;
        while (r < row_end) : (r += 1) {
            if (r + D < row_end) set.prefetch(@as(KeyT, @as(U, @bitCast(sl[r + D]))));
            set.insert(@as(KeyT, @as(U, @bitCast(sl[r]))));
        }
    } else {
        var r: u32 = row_start;
        while (r < row_end) : (r += 1) {
            if (r + D < row_end) set.prefetch(@as(KeyT, @as(U, @bitCast(sl[r + D]))));
            if (!view.isValid(r)) continue;
            set.insert(@as(KeyT, @as(U, @bitCast(sl[r]))));
        }
    }
}

/// Raw value bits of a fixed-width integer-family column at `row`, packed into a
/// u128 for direct hashing in the count_distinct int fast path. The per-type
/// `fieldBits` cast is bijective, so distinct stored values map to distinct keys.
fn distinctIntKey(view: ColumnView, row: u32) u128 {
    return switch (view.data) {
        .boolean => |s| fieldBits(u8, s[row], 8),
        .tinyint => |s| fieldBits(i8, s[row], 8),
        .smallint => |s| fieldBits(i16, s[row], 16),
        .int, .date => |s| fieldBits(i32, s[row], 32),
        .bigint, .datetime, .decimal64 => |s| fieldBits(i64, s[row], 64),
        .largeint, .decimal128 => |s| fieldBits(i128, s[row], 128),
        .uuid => |s| fieldBits(u128, s[row], 128),
        else => unreachable,
    };
}

/// PERCENTILE_CONT(p): collect every valid value as f64, sort at
/// finalize, linear-interpolate at p×(n-1). Memory O(N).
fn percentileUpdate(aa: Allocator, s: *AccState, view: ColumnView, row_start: u32, row_end: u32) !void {
    switch (view.data) {
        inline .int, .bigint, .boolean, .tinyint, .smallint, .largeint, .date, .datetime, .decimal64, .decimal128 => |slice| {
            for (slice[row_start..row_end], row_start..) |v, r| {
                if (!view.isValid(r)) continue;
                try s.percentile_values.append(aa, @as(f64, @floatFromInt(v)));
            }
        },
        inline .float, .double => |slice| {
            for (slice[row_start..row_end], row_start..) |v, r| {
                if (!view.isValid(r)) continue;
                try s.percentile_values.append(aa, @as(f64, v));
            }
        },
        else => unreachable,
    }
}

/// GROUP_CONCAT: append separator + value bytes for each non-null row.
/// `nonempty` distinguishes "no values yet" from "first value was empty".
fn groupConcatUpdate(aa: Allocator, s: *AccState, view: ColumnView, row_start: u32, row_end: u32, sep: []const u8) !void {
    var r: u32 = row_start;
    while (r < row_end) : (r += 1) {
        if (!view.isValid(r)) continue;
        const bytes = switch (view.data) {
            .string => |sv| sv.rowBytes(r),
            .varchar => |sv| sv.rowBytes(r),
            .char => |sv| sv.rowBytes(r),
            .json => |sv| sv.rowBytes(r),
            else => unreachable,
        };
        const c = s.concat orelse blk: {
            const box = try aa.create(ConcatAcc);
            box.* = .{};
            s.concat = box;
            break :blk box;
        };
        if (c.nonempty) try c.buf.appendSlice(aa, sep);
        try c.buf.appendSlice(aa, bytes);
        c.nonempty = true;
    }
}

/// Bytes of a string-family value at `row`. Caller must ensure the view
/// is varchar/string/char.
fn stringRowBytes(view: ColumnView, row: u32) []const u8 {
    return switch (view.data) {
        .varchar => |sv| sv.rowBytes(row),
        .string => |sv| sv.rowBytes(row),
        .char => |sv| sv.rowBytes(row),
        .json => |sv| sv.rowBytes(row),
        else => unreachable,
    };
}

/// Encode a single value as bytes for hashing (count_distinct). Mirrors
/// the layout in buildCompoundGroupKey but for one row, one column.
fn encodeOneValue(aa: Allocator, out: *std.ArrayList(u8), view: ColumnView, row: u32) !void {
    switch (view.data) {
        .int, .date => |s| try storage.format.appendI32(aa, out, s[row]),
        .bigint, .datetime => |s| try storage.format.appendI64(aa, out, s[row]),
        .boolean => |s| try out.append(aa, s[row]),
        .tinyint => |s| try out.append(aa, @bitCast(s[row])),
        .smallint => |s| {
            var b: [2]u8 = undefined;
            std.mem.writeInt(i16, &b, s[row], .little);
            try out.appendSlice(aa, &b);
        },
        .largeint => |s| {
            var b: [16]u8 = undefined;
            std.mem.writeInt(i128, &b, s[row], .little);
            try out.appendSlice(aa, &b);
        },
        .decimal64 => |s| try storage.format.appendI64(aa, out, s[row]),
        .decimal128 => |s| {
            var b: [16]u8 = undefined;
            std.mem.writeInt(i128, &b, s[row], .little);
            try out.appendSlice(aa, &b);
        },
        .uuid => |s| {
            var b: [16]u8 = undefined;
            std.mem.writeInt(u128, &b, s[row], .little);
            try out.appendSlice(aa, &b);
        },
        .float => |s| {
            var b: [4]u8 = undefined;
            storage.format.writeF32(&b, s[row]);
            try out.appendSlice(aa, &b);
        },
        .double => |s| {
            var b: [8]u8 = undefined;
            storage.format.writeF64(&b, s[row]);
            try out.appendSlice(aa, &b);
        },
        .string, .varchar, .char, .json => |sv| {
            const bytes = sv.rowBytes(row);
            try storage.format.appendU32(aa, out, @intCast(bytes.len));
            try out.appendSlice(aa, bytes);
        },
    }
}

fn avgUpdateInt(s: *AccState, view: ColumnView, row_start: u32, row_end: u32) void {
    switch (view.data) {
        inline .int, .bigint, .boolean, .tinyint, .smallint, .decimal64 => |slice| {
            for (slice[row_start..row_end], row_start..) |v, r| {
                if (!view.isValid(r)) continue;
                s.avg.sum += @as(f64, @floatFromInt(v));
                s.avg.count += 1;
            }
        },
        else => unreachable,
    }
}

pub fn appendAccToColumn(
    allocator: Allocator,
    spec: AggSpec,
    state: AccState,
    col: *ColumnStore,
    out_type: Type,
    /// For a combined COUNT(DISTINCT int) aggregate, the group's distinct count
    /// from the gid-indexed `CombinedDistinct.counts` — overrides the
    /// `AccState` set count (which the combined path never populates). `null`
    /// for every other aggregate / call site.
    cd_count: ?u64,
) !void {
    const func = spec.func;
    const row = col.data.rowCount();
    var is_null = false;
    switch (func) {
        .count => {
            try col.data.bigint.append(allocator, @intCast(state.count));
        },
        // SQL: every aggregate except COUNT over zero qualifying (non-NULL)
        // inputs finalizes to NULL. The placeholder + cleared validity bit
        // pattern matches the V2 lanes (`v2_global_aggregate.emitRow`).
        .sum => switch (state) {
            .sum_int => |total| if (!total.seen) {
                try col.data.appendNullPlaceholder(allocator);
                is_null = true;
            } else switch (out_type) {
                .largeint => try col.data.largeint.append(allocator, total.v),
                // DESIGN.md §3.4: SUM(DECIMAL) -> DECIMAL128(38, s). i128
                // overflow is impossible here because total is already i128;
                // any further widening would only occur in row-level arithmetic.
                .decimal128 => try col.data.decimal128.append(allocator, total.v),
                else => {
                    if (total.v > std.math.maxInt(i64) or total.v < std.math.minInt(i64)) {
                        return Error.ArithmeticOverflow;
                    }
                    try col.data.bigint.append(allocator, @intCast(total.v));
                },
            },
            .sum_float => |total| if (!total.seen) {
                try col.data.appendNullPlaceholder(allocator);
                is_null = true;
            } else try col.data.double.append(allocator, total.v),
            else => unreachable,
        },
        .min, .max => switch (state) {
            .min_int, .max_int => {
                const m: ?i64 = if (func == .min) state.min_int else state.max_int;
                if (m) |v| switch (out_type) {
                    .int => try col.data.int.append(allocator, @intCast(v)),
                    .bigint => try col.data.bigint.append(allocator, v),
                    .boolean => try col.data.boolean.append(allocator, @intCast(v)),
                    .date => try col.data.date.append(allocator, @intCast(v)),
                    .datetime => try col.data.datetime.append(allocator, v),
                    .tinyint => try col.data.tinyint.append(allocator, @intCast(v)),
                    .smallint => try col.data.smallint.append(allocator, @intCast(v)),
                    .decimal64 => try col.data.decimal64.append(allocator, v),
                    else => unreachable,
                } else {
                    try col.data.appendNullPlaceholder(allocator);
                    is_null = true;
                }
            },
            .min_large, .max_large => {
                const acc = if (func == .min) state.min_large else state.max_large;
                if (acc.present) switch (out_type) {
                    .largeint => try col.data.largeint.append(allocator, acc.v),
                    .decimal128 => try col.data.decimal128.append(allocator, acc.v),
                    else => unreachable,
                } else {
                    try col.data.appendNullPlaceholder(allocator);
                    is_null = true;
                }
            },
            .min_float, .max_float => {
                const m: ?f64 = if (func == .min) state.min_float else state.max_float;
                if (m) |v| switch (out_type) {
                    .float => try col.data.float.append(allocator, @floatCast(v)),
                    .double => try col.data.double.append(allocator, v),
                    else => unreachable,
                } else {
                    try col.data.appendNullPlaceholder(allocator);
                    is_null = true;
                }
            },
            .min_str, .max_str => {
                const m: ?[]const u8 = if (func == .min) state.min_str else state.max_str;
                if (m) |v| switch (out_type) {
                    .varchar => try col.data.varchar.appendValue(allocator, v),
                    .string => try col.data.string.appendValue(allocator, v),
                    .char => try col.data.char.appendValue(allocator, v),
                    .json => try col.data.json.appendValue(allocator, v),
                    else => unreachable,
                } else {
                    try col.data.appendNullPlaceholder(allocator);
                    is_null = true;
                }
            },
            else => unreachable,
        },
        .avg => {
            const a = state.avg;
            if (a.count == 0) {
                try col.data.appendNullPlaceholder(allocator);
                is_null = true;
            } else {
                try col.data.double.append(allocator, a.sum / @as(f64, @floatFromInt(a.count)) / a.scale_div);
            }
        },
        .count_if => {
            try col.data.bigint.append(allocator, @intCast(state.count));
        },
        .bool_and, .bool_or => {
            const b = state.bool_acc;
            if (!b.seen) {
                try col.data.appendNullPlaceholder(allocator);
                is_null = true;
            } else {
                try col.data.boolean.append(allocator, if (b.value) 1 else 0);
            }
        },
        .any_value, .first, .last => {
            const v = state.value_acc;
            if (!v.seen) {
                try col.data.appendNullPlaceholder(allocator);
                is_null = true;
            } else try appendValueToColumn(allocator, col, out_type, v.value);
        },
        .max_by => {
            const v = state.max_by;
            if (!v.seen) {
                try col.data.appendNullPlaceholder(allocator);
                is_null = true;
            } else try appendValueToColumn(allocator, col, out_type, v.value);
        },
        .bit_and, .bit_or, .bit_xor => {
            const b = state.bitwise;
            if (!b.seen) {
                try col.data.appendNullPlaceholder(allocator);
                is_null = true;
            } else {
                try col.data.bigint.append(allocator, b.value);
            }
        },
        .sum_distinct, .avg_distinct => {
            const vals = state.distinct_numeric.items;
            if (vals.len == 0) {
                try col.data.appendNullPlaceholder(allocator);
                is_null = true;
            } else {
                std.mem.sortUnstable(f64, @constCast(vals), {}, std.sort.asc(f64));
                var sum: f64 = 0.0;
                var count: u64 = 0;
                var prev: ?f64 = null;
                for (vals) |v| {
                    if (prev == null or v != prev.?) {
                        sum += v;
                        count += 1;
                        prev = v;
                    }
                }
                try col.data.double.append(allocator, if (func == .avg_distinct) sum / @as(f64, @floatFromInt(count)) else sum);
            }
        },
        .var_pop, .var_samp, .stddev_pop, .stddev_samp => {
            const w = state.welford;
            // Sample variants additionally need ≥2 inputs (n−1 divisor).
            const defined = switch (func) {
                .var_pop, .stddev_pop => w.count >= 1,
                else => w.count >= 2,
            };
            if (!defined) {
                try col.data.appendNullPlaceholder(allocator);
                is_null = true;
            } else {
                const variance: f64 = switch (func) {
                    .var_pop, .stddev_pop => w.m2 / @as(f64, @floatFromInt(w.count)),
                    else => w.m2 / @as(f64, @floatFromInt(w.count - 1)),
                };
                const out: f64 = if (func == .stddev_pop or func == .stddev_samp) @sqrt(variance) else variance;
                try col.data.double.append(allocator, out);
            }
        },
        .count_distinct => {
            const n: u64 = if (cd_count) |cnt| cnt else switch (state) {
                .distinct => |d| d.set.count() + @as(u32, @intFromBool(d.seen_empty)),
                .distinct_int => |set| set.count(),
                .distinct_int64 => |set| set.count(),
                .distinct_int32 => |set| set.count(),
                else => unreachable,
            };
            try col.data.bigint.append(allocator, @intCast(n));
        },
        .percentile => {
            const p: f64 = switch (spec.params) {
                .percentile => |pv| pv,
                else => 0.5,
            };
            const vals = state.percentile_values.items;
            if (vals.len == 0) {
                try col.data.appendNullPlaceholder(allocator);
                is_null = true;
            } else {
                // Sort in place — arena owns the backing slice; nothing
                // outside this aggregate observes the buffer.
                std.mem.sortUnstable(f64, @constCast(vals), {}, std.sort.asc(f64));
                const n: f64 = @floatFromInt(vals.len);
                // Linear interpolation (PostgreSQL percentile_cont rule):
                //   idx = p * (n - 1); blend floor and ceil.
                const idx = p * (n - 1);
                const lo: usize = @intFromFloat(@floor(idx));
                const hi: usize = @intFromFloat(@ceil(idx));
                const frac = idx - @floor(idx);
                const v = if (lo == hi) vals[lo] else vals[lo] + (vals[hi] - vals[lo]) * frac;
                try col.data.double.append(allocator, v);
            }
        },
        .group_concat => {
            if (state.concat) |c| {
                try col.data.string.appendValue(allocator, c.buf.items);
            } else {
                try col.data.appendNullPlaceholder(allocator);
                is_null = true;
            }
        },
        .udf => return Error.AggregateUnsupportedType,
    }
    try col.appendValidBit(allocator, row, !is_null);
}

/// Pack the group-by columns of the current batch row into `out`. Layout
/// per type matches `comparison.appendColumnValueBytes`. `out` is owned
/// by the caller and is cleared+reused across rows in the accumulate
/// loop — only new groups get arena-owned copies.
/// Bit-width of a fixed-width integer-family type when packed into the u128
/// compound key. `null` for any type that can't participate (floats, strings),
/// which keeps the key off the integer fast path.
fn intKeyBits(t: Type) ?u8 {
    return switch (t) {
        .boolean => 8,
        .tinyint => 8,
        .smallint => 16,
        .int, .date => 32,
        .bigint, .datetime, .decimal64 => 64,
        .largeint, .decimal128, .uuid => 128,
        else => null,
    };
}

/// One group column's slot within the packed u128 key: its bit offset (from
/// the low end) and width. Layout is column-order, column 0 in the lowest bits.
pub const IntKeyField = struct {
    offset: u8,
    bits: u8,
    type_tag: types.TypeTag,
    /// When true the field's value is a dict CODE read from `batch.coded[ci]`
    /// (a low-card string group column) rather than `batch.values[ci]`, packed
    /// as a 32-bit unsigned code. `IntKeyLayout.coded_dicts[i]` decodes it back
    /// to the string at emit. `type_tag` stays the string type for emit routing.
    coded: bool = false,
};

/// Slot-size tier for the packed integer key, routed by the summed group-column
/// bit width. A narrower slot fits more entries per cache line, so the
/// memory-bound probe moves fewer bytes. The 32/96/128 cutoffs match
/// `group_table.IntKeyTable`'s three slot layouts.
const IntKeyTier = enum { bits32, bits96, bits128 };

/// Decision + layout for the integer fast path. `fields` (column-order)
/// reconstructs each group column on emit; `tier` selects the slot size of the
/// `group_table.IntKeyTable` that holds the key→gid map. Returned by
/// `planIntKey`; `null` when any group column isn't a fixed-width integer family
/// or the widths sum past 128 bits — the operator then uses the byte-serialized
/// key path.
pub const IntKeyLayout = struct {
    fields: []IntKeyField,
    tier: IntKeyTier,
    /// Per-field GlobalDict for coded fields (null for non-coded), parallel to
    /// `fields`. Owned; empty when no field is coded.
    coded_dicts: []const ?*exec.GlobalDict = &.{},

    pub fn deinit(self: IntKeyLayout, allocator: Allocator) void {
        allocator.free(self.fields);
        if (self.coded_dicts.len > 0) allocator.free(@constCast(self.coded_dicts));
    }
};

/// Build an `IntKeyLayout` when every group column is a fixed-width integer
/// family type whose widths sum to ≤128 bits; otherwise `null`. The summed width
/// also picks the slot tier (≤32 / ≤96 / ≤128 bits). Caller owns the returned
/// `fields` (freed in `Aggregate.deinit`).
/// `coded_mask[i]` (when non-null) marks group column `i` as a low-card dict
/// string keyed on its 32-bit global code instead of its type's native width;
/// `dicts[i]` is that column's GlobalDict (decoded back at emit). Passing a
/// null mask reproduces the all-native behaviour. A coded field always fits a
/// 32-bit slot, so a coded string counts as 32 bits toward the 128-bit budget.
pub fn planIntKey(
    allocator: Allocator,
    group_col_indices: []const usize,
    up_schema: []const Column,
    coded_mask: ?[]const bool,
    dicts: []const ?*exec.GlobalDict,
) !?IntKeyLayout {
    if (group_col_indices.len == 0) return null;
    var total: u16 = 0;
    for (group_col_indices, 0..) |ci, i| {
        // A nullable key has no slot in the packed layout for its validity (a
        // NULL slot's decoded payload is an encoding artifact that collides
        // with a real value) — such keys take the tagged byte path.
        if (up_schema[ci].nullable) return null;
        const coded = coded_mask != null and coded_mask.?[i];
        const b: u16 = if (coded) 32 else (intKeyBits(up_schema[ci].type) orelse return null);
        total += b;
    }
    if (total > 128) return null;

    const fields = try allocator.alloc(IntKeyField, group_col_indices.len);
    errdefer allocator.free(fields);
    const cdicts = try allocator.alloc(?*exec.GlobalDict, group_col_indices.len);
    errdefer allocator.free(cdicts);
    var offset: u8 = 0;
    for (group_col_indices, fields, cdicts, 0..) |ci, *f, *cd, i| {
        const coded = coded_mask != null and coded_mask.?[i];
        const b: u8 = if (coded) 32 else intKeyBits(up_schema[ci].type).?;
        f.* = .{ .offset = offset, .bits = b, .type_tag = std.meta.activeTag(up_schema[ci].type), .coded = coded };
        cd.* = if (coded and i < dicts.len) dicts[i] else null;
        offset += b;
    }
    const tier: IntKeyTier = if (total <= 32) .bits32 else if (total <= 96) .bits96 else .bits128;
    return .{ .fields = fields, .tier = tier, .coded_dicts = cdicts };
}

/// Decide the FOR-narrow inline-state fast path: exactly one integer-family
/// group column, exactly one inline-able aggregate (`SUM`/`MIN`/`MAX` over an
/// integer-family column), a proven group-column min/max range that leaves the
/// `maxInt(KeyW)` EMPTY sentinel free, and a `{ KeyW, i64 }` slot ≤16 bytes.
/// `null` ⟺ ineligible (the caller then uses the existing int / byte path).
///
/// NULL handling: a NULL group value would make `value − base` meaningless and
/// SQL still groups NULLs together — so a *nullable* group column is a fallback
/// condition (the existing int path handles its NULLs). The gate also requires
/// the aggregate's input to be a non-128-bit int family so the i64 accumulator
/// is the canonical SUM/MIN/MAX state, and (for SUM) that the proven sum bound
/// fits i64 so the i64 accumulator never overflows where the canonical i128 one
/// wouldn't — keeping results bit-identical to the fallback.
fn planInlineFor(
    group_col_indices: []const usize,
    agg_col_indices: []const ?usize,
    aggs: []const AggSpec,
    up_schema: []const Column,
    st: exec.PipelineStats,
) ?InlineForPlan {
    if (group_col_indices.len != 1 or aggs.len != 1) return null;

    const gci = group_col_indices[0];
    const gcol = up_schema[gci];
    if (gcol.nullable) return null;
    const gbits = intKeyBits(gcol.type) orelse return null;
    if (gbits > 64) return null;

    const a = aggs[0];
    const kind: InlineAggKind = switch (a.func) {
        .sum => .sum,
        .min => .min,
        .max => .max,
        else => return null,
    };
    const aci = agg_col_indices[0] orelse return null;
    // Nullable value column ⇒ canonical path: the inline loop skips NULL rows
    // before group creation, which would drop all-NULL groups instead of
    // emitting them with a NULL aggregate.
    if (up_schema[aci].nullable) return null;
    const vt = up_schema[aci].type;
    // i64 accumulator only: integer family ≤64 bits. 128-bit families
    // (largeint/decimal128) and float/string take the canonical path.
    const vbits = intKeyBits(vt) orelse return null;
    if (vbits > 64) return null;

    // FOR range from the group column's proven stats.
    if (gci >= st.column_stats.len) return null;
    const gs = st.column_stats[gci];
    const min_i128 = gs.min orelse return null;
    const max_i128 = gs.max orelse return null;
    if (min_i128 > max_i128) return null;
    if (min_i128 < std.math.minInt(i64) or max_i128 > std.math.maxInt(i64)) return null;
    const base: i64 = @intCast(min_i128);
    // range = max − base, computed in i128 then range-checked into u64.
    const range_i128 = max_i128 - min_i128;
    if (range_i128 < 0 or range_i128 > std.math.maxInt(u64)) return null;
    const range: u64 = @intCast(range_i128);

    // Smallest unsigned width whose `maxInt` strictly exceeds `range` (reserving
    // the sentinel). A range needing the full u64 (`range == maxInt(u64)`) has no
    // headroom for the sentinel ⇒ ineligible.
    const tier: InlineKeyTier = if (range < std.math.maxInt(u8))
        .w8
    else if (range < std.math.maxInt(u16))
        .w16
    else if (range < std.math.maxInt(u32))
        .w32
    else if (range < std.math.maxInt(u64))
        .w64
    else
        return null;

    // SUM overflow guard: the proven sum bound (n·lo … n·hi) must fit i64 so the
    // i64 accumulator can't wrap where the canonical i128 path wouldn't. MIN/MAX
    // always fit i64 (a single value of a ≤64-bit family).
    if (kind == .sum) {
        const vs: ?exec.ColStat = if (aci < st.column_stats.len) st.column_stats[aci] else null;
        const s = vs orelse return null;
        const lo = s.min orelse return null;
        const hi = s.max orelse return null;
        const n: i128 = @intCast(@max(st.upper_rows, 1));
        const lo_sum = std.math.mul(i128, n, lo) catch return null;
        const hi_sum = std.math.mul(i128, n, hi) catch return null;
        if (lo_sum < std.math.minInt(i64) or hi_sum > std.math.maxInt(i64)) return null;
    }

    // Slot-size gate: `{ KeyW, i64 }` ≤ 16 bytes. i64 state at 8-byte alignment
    // keeps every tier at ≤16 bytes, but assert it so a future wider state can't
    // silently slip past this contract.
    const slot_size: usize = switch (tier) {
        .w8 => @sizeOf(InlineTable8.Slot),
        .w16 => @sizeOf(InlineTable16.Slot),
        .w32 => @sizeOf(InlineTable32.Slot),
        .w64 => @sizeOf(InlineTable64.Slot),
    };
    if (slot_size > 16) return null;

    return .{
        .base = base,
        .tier = tier,
        .kind = kind,
        .key_tag = std.meta.activeTag(gcol.type),
        .val_tag = std.meta.activeTag(vt),
    };
}

/// Reinterpret a signed/unsigned value of `bits` width as the low `bits` of a
/// u128, masked to the field width (two's-complement bit pattern, so packing is
/// bijective with `unpackIntField`).
inline fn fieldBits(comptime SignedT: type, v: SignedT, bits: u8) u128 {
    const UnsignedT = std.meta.Int(.unsigned, @bitSizeOf(SignedT));
    const u: UnsignedT = @bitCast(v);
    const mask: u128 = if (bits >= 128) std.math.maxInt(u128) else (@as(u128, 1) << @intCast(bits)) - 1;
    return @as(u128, u) & mask;
}

/// OR group column `ci`'s bit-field contribution into every row's u128 `key`
/// per field `f`. Column-major: the ValueView type switch runs once here, then a
/// tight row loop — hoisting the dispatch out of the caller's per-row key build
/// (and letting the row loop vectorize). The raw stored value is used (matching
/// the byte path, which also ignores group-column validity), so the two paths
/// group identically. Same-payload variants share a prong via `inline`.
pub fn orKeyColumn(keys: []u128, batch: Batch, ci: usize, f: IntKeyField) void {
    const off: u7 = @intCast(f.offset);
    const bits = f.bits;
    if (f.coded) {
        const codes = batch.coded.?[ci].?.codes;
        for (keys, codes) |*k, code| k.* |= fieldBits(u32, code, bits) << off;
        return;
    }
    switch (batch.values[ci].data) {
        .boolean => |s| for (keys, s) |*k, v| {
            k.* |= fieldBits(u8, v, bits) << off;
        },
        .tinyint => |s| for (keys, s) |*k, v| {
            k.* |= fieldBits(i8, v, bits) << off;
        },
        .smallint => |s| for (keys, s) |*k, v| {
            k.* |= fieldBits(i16, v, bits) << off;
        },
        inline .int, .date => |s| for (keys, s) |*k, v| {
            k.* |= fieldBits(i32, v, bits) << off;
        },
        inline .bigint, .datetime, .decimal64 => |s| for (keys, s) |*k, v| {
            k.* |= fieldBits(i64, v, bits) << off;
        },
        inline .largeint, .decimal128 => |s| for (keys, s) |*k, v| {
            k.* |= fieldBits(i128, v, bits) << off;
        },
        .uuid => |s| for (keys, s) |*k, v| {
            k.* |= fieldBits(u128, v, bits) << off;
        },
        else => unreachable,
    }
}

/// Pack a single integer-family group column's `value` into the u128 key,
/// matching `orKeyColumn` for the one-column case (`fieldBits` of the column's
/// stored type at the field's offset). The inline-FOR lower produces keys
/// `appendIntGroupKey` then decodes bit-identically to the canonical path.
fn packSingleIntField(f: IntKeyField, value: i64) u128 {
    const fb: u128 = switch (f.type_tag) {
        .boolean => fieldBits(u8, @intCast(value), f.bits),
        .tinyint => fieldBits(i8, @intCast(value), f.bits),
        .smallint => fieldBits(i16, @intCast(value), f.bits),
        .int, .date => fieldBits(i32, @intCast(value), f.bits),
        .bigint, .datetime, .decimal64 => fieldBits(i64, value, f.bits),
        else => unreachable,
    };
    return fb << @intCast(f.offset);
}

/// The unsigned key width type backing an `InlineKeyTier`.
fn inlineKeyWidth(comptime tier: InlineKeyTier) type {
    return switch (tier) {
        .w8 => u8,
        .w16 => u16,
        .w32 => u32,
        .w64 => u64,
    };
}

/// One key-width tier of the inline-FOR accumulate. Dispatches the group
/// column's stored slice type (the FOR-key read), then the (func × value stored
/// slice type) fold, before entering the comptime-fixed inner loop — so the
/// per-row probe carries no tier / func / type branch.
inline fn inlineForTier(
    aa: Allocator,
    plan: InlineForPlan,
    key_view: ColumnView,
    val_view: ColumnView,
    comptime KeyW: type,
    table: *group_table.InlineSlotTable(KeyW, InlineState),
) !void {
    switch (plan.kind) {
        inline .sum, .min, .max => |kind| switch (plan.key_tag) {
            inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |kt| switch (plan.val_tag) {
                inline .boolean, .tinyint, .smallint, .int, .date, .bigint, .datetime, .decimal64 => |vt| {
                    const key_sl = @field(key_view.data, @tagName(kt));
                    const val_sl = @field(val_view.data, @tagName(vt));
                    try inlineForLoop(aa, plan.base, val_view, KeyW, key_sl, val_sl, table, kind);
                },
                else => unreachable,
            },
            else => unreachable,
        },
    }
}

/// Comptime-fixed inline-FOR inner loop. FOR-normalizes each row's key
/// (`code = value − base`) into `KeyW`, then `getOrPut` either seeds the slot's
/// state to the fold identity on first sighting or folds the value in place.
/// Prefetch-pipelined: the table is grown for the whole batch up front so slot
/// addresses stay stable across the look-ahead window. The group column is
/// non-nullable (`planInlineFor` gate), so the key read needs no validity
/// branch; the value column is also gated non-nullable, but the validity check
/// stays as a cheap belt-and-suspenders skip.
inline fn inlineForLoop(
    aa: Allocator,
    base: i64,
    val_view: ColumnView,
    comptime KeyW: type,
    key_sl: anytype,
    val_sl: anytype,
    table: *group_table.InlineSlotTable(KeyW, InlineState),
    comptime kind: InlineAggKind,
) !void {
    const n = key_sl.len;
    try table.ensureFor(aa, n);
    const val_has_nulls = val_view.nulls != null;
    const base128: i128 = base;
    var r: usize = 0;
    while (r < n) : (r += 1) {
        if (r + Aggregate.PREFETCH_DIST < n) {
            // FOR code in i128 so a range exceeding i64 (a valid w64 tier) can't
            // overflow the subtraction; the gate guarantees it fits `KeyW`.
            const code_pf: KeyW = @intCast(@as(i128, key_sl[r + Aggregate.PREFETCH_DIST]) - base128);
            table.prefetch(code_pf);
        }
        if (val_has_nulls and !val_view.isValid(r)) continue;
        const code: KeyW = @intCast(@as(i128, key_sl[r]) - base128);
        const v: i64 = val_sl[r];
        const e = table.getOrPut(code);
        switch (kind) {
            .sum => {
                if (!e.found) e.state.* = 0;
                e.state.* += v;
            },
            .min => {
                if (!e.found) e.state.* = std.math.maxInt(i64);
                if (v < e.state.*) e.state.* = v;
            },
            .max => {
                if (!e.found) e.state.* = std.math.minInt(i64);
                if (v > e.state.*) e.state.* = v;
            },
        }
    }
}

/// Extract one field's signed/unsigned value from the packed key (inverse of
/// `fieldBits`): mask out the field, then sign-extend through the matching
/// signed integer type.
inline fn unpackField(comptime SignedT: type, key: u128, f: IntKeyField) SignedT {
    const UnsignedT = std.meta.Int(.unsigned, @bitSizeOf(SignedT));
    const mask: u128 = if (f.bits >= 128) std.math.maxInt(u128) else (@as(u128, 1) << @intCast(f.bits)) - 1;
    const raw: UnsignedT = @truncate((key >> @intCast(f.offset)) & mask);
    return @bitCast(raw);
}

/// Decode a packed u128 key back into the group output columns — the integer
/// path's mirror of `appendGroupKey`. Reconstructs values bit-identically to
/// what the byte path would have stored.
pub fn appendIntGroupKey(
    allocator: Allocator,
    key: u128,
    layout: IntKeyLayout,
    out_cols: []ColumnStore,
) !void {
    for (layout.fields, 0..) |f, i| {
        if (f.coded) {
            const code = unpackField(u32, key, f);
            const bytes = layout.coded_dicts[i].?.decode(code);
            switch (out_cols[i].data) {
                .varchar, .string, .char, .json => |*ss| try ss.appendValue(allocator, bytes),
                else => unreachable,
            }
            continue;
        }
        switch (f.type_tag) {
            .boolean => try out_cols[i].data.boolean.append(allocator, unpackField(u8, key, f)),
            .tinyint => try out_cols[i].data.tinyint.append(allocator, unpackField(i8, key, f)),
            .smallint => try out_cols[i].data.smallint.append(allocator, unpackField(i16, key, f)),
            .int => try out_cols[i].data.int.append(allocator, unpackField(i32, key, f)),
            .date => try out_cols[i].data.date.append(allocator, unpackField(i32, key, f)),
            .bigint => try out_cols[i].data.bigint.append(allocator, unpackField(i64, key, f)),
            .datetime => try out_cols[i].data.datetime.append(allocator, unpackField(i64, key, f)),
            .decimal64 => try out_cols[i].data.decimal64.append(allocator, unpackField(i64, key, f)),
            .largeint => try out_cols[i].data.largeint.append(allocator, unpackField(i128, key, f)),
            .decimal128 => try out_cols[i].data.decimal128.append(allocator, unpackField(i128, key, f)),
            .uuid => try out_cols[i].data.uuid.append(allocator, unpackField(u128, key, f)),
            else => unreachable,
        }
    }
}

fn buildCompoundGroupKey(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    batch: Batch,
    group_col_indices: []const usize,
    row: u32,
) !void {
    for (group_col_indices) |ci| {
        const view = batch.values[ci];
        // A nullable key column carries a leading validity tag: a NULL slot's
        // decoded payload bytes are encoding artifacts (FOR base, dict entry
        // 0), so without the tag NULL rows silently merge into a real value's
        // group. NULL writes the tag alone — all NULL keys form one group
        // (SQL standard). Keyed on the schema flag, not `view.nulls`, so the
        // layout is stable across batches that happen to have no NULLs.
        if (batch.schema[ci].nullable) {
            const valid = view.isValid(row);
            try out.append(allocator, @intFromBool(valid));
            if (!valid) continue;
        }
        switch (view.data) {
            .int => |s| try storage.format.appendI32(allocator, out, s[row]),
            .bigint => |s| try storage.format.appendI64(allocator, out, s[row]),
            .boolean => |s| try out.append(allocator, s[row]),
            .varchar => |sv| {
                const bytes = sv.rowBytes(row);
                try storage.format.appendU32(allocator, out, @intCast(bytes.len));
                try out.appendSlice(allocator, bytes);
            },
            .string => |sv| {
                const bytes = sv.rowBytes(row);
                try storage.format.appendU32(allocator, out, @intCast(bytes.len));
                try out.appendSlice(allocator, bytes);
            },
            .float => |s| {
                var b: [4]u8 = undefined;
                storage.format.writeF32(&b, s[row]);
                try out.appendSlice(allocator, &b);
            },
            .double => |s| {
                var b: [8]u8 = undefined;
                storage.format.writeF64(&b, s[row]);
                try out.appendSlice(allocator, &b);
            },
            .date => |s| try storage.format.appendI32(allocator, out, s[row]),
            .datetime => |s| try storage.format.appendI64(allocator, out, s[row]),
            .tinyint => |s| try out.append(allocator, @bitCast(s[row])),
            .smallint => |s| {
                var b: [2]u8 = undefined;
                std.mem.writeInt(i16, &b, s[row], .little);
                try out.appendSlice(allocator, &b);
            },
            .largeint => |s| {
                var b: [16]u8 = undefined;
                std.mem.writeInt(i128, &b, s[row], .little);
                try out.appendSlice(allocator, &b);
            },
            .char => |sv| {
                const bytes = sv.rowBytes(row);
                try storage.format.appendU32(allocator, out, @intCast(bytes.len));
                try out.appendSlice(allocator, bytes);
            },
            .json => |sv| {
                const bytes = sv.rowBytes(row);
                try storage.format.appendU32(allocator, out, @intCast(bytes.len));
                try out.appendSlice(allocator, bytes);
            },
            .decimal64 => |s| try storage.format.appendI64(allocator, out, s[row]),
            .decimal128 => |s| {
                var b: [16]u8 = undefined;
                std.mem.writeInt(i128, &b, s[row], .little);
                try out.appendSlice(allocator, &b);
            },
            .uuid => |s| {
                var b: [16]u8 = undefined;
                std.mem.writeInt(u128, &b, s[row], .little);
                try out.appendSlice(allocator, &b);
            },
        }
    }
}

/// Decode a packed group key back into the output columns (one value per
/// group column). Mirrors the encoding in `compoundGroupKey`.
fn appendGroupKey(
    allocator: Allocator,
    key_bytes: []const u8,
    group_col_indices: []const usize,
    up_schema: []const Column,
    out_cols: []ColumnStore,
) !void {
    var cursor: usize = 0;
    for (group_col_indices, 0..) |src_idx, i| {
        // Mirror of the validity tag in `buildCompoundGroupKey`. A nullable
        // output column's bitmap defaults to 0 (= NULL), so valid rows must
        // set their bit explicitly too.
        if (up_schema[src_idx].nullable) {
            const row = out_cols[i].data.rowCount();
            const valid = key_bytes[cursor] != 0;
            cursor += 1;
            try out_cols[i].appendValidBit(allocator, row, valid);
            if (!valid) {
                try out_cols[i].data.appendNullPlaceholder(allocator);
                continue;
            }
        }
        const t = up_schema[src_idx].type;
        switch (t) {
            .int => {
                const v = storage.format.readI32(key_bytes[cursor .. cursor + 4]);
                cursor += 4;
                try out_cols[i].data.int.append(allocator, v);
            },
            .bigint => {
                const v = storage.format.readI64(key_bytes[cursor .. cursor + 8]);
                cursor += 8;
                try out_cols[i].data.bigint.append(allocator, v);
            },
            .boolean => {
                try out_cols[i].data.boolean.append(allocator, key_bytes[cursor]);
                cursor += 1;
            },
            .varchar, .string, .char, .json => {
                const len = storage.format.readU32(key_bytes[cursor .. cursor + 4]);
                cursor += 4;
                const bytes = key_bytes[cursor .. cursor + len];
                cursor += len;
                const ss: *engine.StringStore = switch (out_cols[i].data) {
                    .varchar => |*x| x,
                    .string => |*x| x,
                    .char => |*x| x,
                    .json => |*x| x,
                    else => unreachable,
                };
                try ss.appendValue(allocator, bytes);
            },
            .float => {
                const v = storage.format.readF32(key_bytes[cursor .. cursor + 4]);
                cursor += 4;
                try out_cols[i].data.float.append(allocator, v);
            },
            .double => {
                const v = storage.format.readF64(key_bytes[cursor .. cursor + 8]);
                cursor += 8;
                try out_cols[i].data.double.append(allocator, v);
            },
            .date => {
                const v = storage.format.readI32(key_bytes[cursor .. cursor + 4]);
                cursor += 4;
                try out_cols[i].data.date.append(allocator, v);
            },
            .datetime => {
                const v = storage.format.readI64(key_bytes[cursor .. cursor + 8]);
                cursor += 8;
                try out_cols[i].data.datetime.append(allocator, v);
            },
            .tinyint => {
                const v: i8 = @bitCast(key_bytes[cursor]);
                cursor += 1;
                try out_cols[i].data.tinyint.append(allocator, v);
            },
            .smallint => {
                const v = std.mem.readInt(i16, key_bytes[cursor..][0..2], .little);
                cursor += 2;
                try out_cols[i].data.smallint.append(allocator, v);
            },
            .largeint => {
                const v = std.mem.readInt(i128, key_bytes[cursor..][0..16], .little);
                cursor += 16;
                try out_cols[i].data.largeint.append(allocator, v);
            },
            .decimal64 => {
                const v = storage.format.readI64(key_bytes[cursor .. cursor + 8]);
                cursor += 8;
                try out_cols[i].data.decimal64.append(allocator, v);
            },
            .decimal128 => {
                const v = std.mem.readInt(i128, key_bytes[cursor..][0..16], .little);
                cursor += 16;
                try out_cols[i].data.decimal128.append(allocator, v);
            },
            .uuid => {
                const v = std.mem.readInt(u128, key_bytes[cursor..][0..16], .little);
                cursor += 16;
                try out_cols[i].data.uuid.append(allocator, v);
            },
        }
    }
}
