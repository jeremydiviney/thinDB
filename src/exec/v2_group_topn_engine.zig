//! Engine V2 grouped Top-N shape executor.
//!
//! This is the execution boundary for the simple
//! scan -> optional fused filter -> group by -> order by aggregate desc -> top-N
//! query shape. The first implementation keeps the settled staged-final
//! scheduler/core from the isolated harness, while exposing a generic shape and
//! chunk model that the native SoA chunk implementation can replace in-place.

const std = @import("std");
const Allocator = std.mem.Allocator;
const build_options = @import("build_options");

const api = @import("../api/api.zig");
const types = @import("../types.zig");
const exec = @import("exec.zig");
const storage = @import("../storage/storage.zig");

const HarnessCore = exec.group_topn_harness_core;

const DEFAULT_DOP: usize = 12;
const DEFAULT_BUCKET_COUNT: usize = 256;
const DEFAULT_SCAN_TILE_RGS: usize = 16;
const DEFAULT_ROUTE_BLOCK_ROWS: usize = 2048;
const DEFAULT_GROUP_LEASE_BUCKETS: usize = 8;
const DEFAULT_GROUP_INIT_CAP: usize = 0;
const DEFAULT_RAW_CHUNK_ROWS: usize = 8192;
const DEFAULT_RAW_GROUP_CHUNK_ROWS: usize = 8192;
const DEFAULT_RAW_BATCH_CHUNKS: usize = 12;

pub const KeyWidth = enum {
    u32,
    u64,
    u96,
    u128,

    pub fn fromBits(bits: u16) KeyWidth {
        if (bits <= 32) return .u32;
        if (bits <= 64) return .u64;
        if (bits <= 96) return .u96;
        return .u128;
    }

    pub fn label(self: KeyWidth) []const u8 {
        return switch (self) {
            .u32 => "u32",
            .u64 => "u64",
            .u96 => "u96",
            .u128 => "u128",
        };
    }
};

pub const KeyBlock = union(KeyWidth) {
    u32: []u32,
    u64: []u64,
    u96: struct {
        lo: []u64,
        hi: []u32,
    },
    u128: []u128,
};

pub const BucketIds = union(enum) {
    none,
    u8: []u8,
    u16: []u16,
};

pub const PhysicalType = enum {
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f32,
    f64,
};

pub const ColumnRole = enum {
    group_key_input,
    aggregate_input,
    having_input,
    order_input,
    projection_input,
    late_materialization_ref,
};

pub const ColumnValues = union(PhysicalType) {
    i8: []i8,
    i16: []i16,
    i32: []i32,
    i64: []i64,
    u8: []u8,
    u16: []u16,
    u32: []u32,
    u64: []u64,
    f32: []f32,
    f64: []f64,
};

pub const ColumnSpec = struct {
    source_index: u16,
    role: ColumnRole,
    physical_type: PhysicalType,
    nullable: bool = false,
};

pub const ColumnVector = struct {
    spec: ColumnSpec,
    values: ColumnValues,
    nulls: ?[]u64 = null,
};

pub const RowRef = packed struct {
    segment: u32,
    row_group: u32,
    row: u32,
};

pub const GroupChunkLayout = struct {
    key_width: KeyWidth,
    capacity: usize,
    columns: []const ColumnSpec,
    keep_row_refs: bool = false,
    keep_hashes: bool = false,
    keep_bucket_ids: bool = false,
};

pub const GroupChunk = struct {
    slab: []align(16) u8 = &.{},
    row_count: usize = 0,
    capacity: usize = 0,
    key_width: KeyWidth = .u128,
    key_bytes: []u8 = &.{},
    columns: []ColumnVector = &.{},
    row_refs: ?[]RowRef = null,
    hashes: ?[]u64 = null,
    bucket_ids: ?[]u16 = null,

    pub fn init(allocator: Allocator, layout: GroupChunkLayout) !GroupChunk {
        try validateGroupChunkLayout(layout);

        var bytes_needed: usize = 0;
        bytes_needed = alignForward(bytes_needed, keyAlignment(layout.key_width));
        const key_offset = bytes_needed;
        bytes_needed += keyBytes(layout.key_width) * layout.capacity;

        const hash_offset: ?usize = if (layout.keep_hashes) blk: {
            bytes_needed = alignForward(bytes_needed, @alignOf(u64));
            const offset = bytes_needed;
            bytes_needed += @sizeOf(u64) * layout.capacity;
            break :blk offset;
        } else null;

        const bucket_offset: ?usize = if (layout.keep_bucket_ids) blk: {
            bytes_needed = alignForward(bytes_needed, @alignOf(u16));
            const offset = bytes_needed;
            bytes_needed += @sizeOf(u16) * layout.capacity;
            break :blk offset;
        } else null;

        const row_ref_offset: ?usize = if (layout.keep_row_refs) blk: {
            bytes_needed = alignForward(bytes_needed, @alignOf(RowRef));
            const offset = bytes_needed;
            bytes_needed += @sizeOf(RowRef) * layout.capacity;
            break :blk offset;
        } else null;

        var column_offsets = try allocator.alloc(usize, layout.columns.len);
        defer allocator.free(column_offsets);
        var column_null_offsets = try allocator.alloc(?usize, layout.columns.len);
        defer allocator.free(column_null_offsets);
        for (layout.columns, 0..) |col, i| {
            bytes_needed = alignForward(bytes_needed, physicalAlignment(col.physical_type));
            column_offsets[i] = bytes_needed;
            bytes_needed += physicalSize(col.physical_type) * layout.capacity;
            column_null_offsets[i] = null;
            if (col.nullable) {
                bytes_needed = alignForward(bytes_needed, @alignOf(u64));
                column_null_offsets[i] = bytes_needed;
                bytes_needed += nullBitmapWords(layout.capacity) * @sizeOf(u64);
            }
        }

        const slab = try allocator.alignedAlloc(u8, .@"16", bytes_needed);
        errdefer allocator.free(slab);

        const columns = try allocator.alloc(ColumnVector, layout.columns.len);
        errdefer allocator.free(columns);

        for (layout.columns, 0..) |col, i| {
            const values = columnValuesFromBytes(col.physical_type, slab[column_offsets[i]..], layout.capacity);
            const nulls: ?[]u64 = if (column_null_offsets[i]) |offset|
                bytesAsSliceMut(u64, slab[offset..], nullBitmapWords(layout.capacity))
            else
                null;
            columns[i] = .{ .spec = col, .values = values, .nulls = nulls };
        }

        return .{
            .slab = slab,
            .capacity = layout.capacity,
            .key_width = layout.key_width,
            .key_bytes = slab[key_offset .. key_offset + keyBytes(layout.key_width) * layout.capacity],
            .columns = columns,
            .row_refs = if (row_ref_offset) |offset| bytesAsSliceMut(RowRef, slab[offset..], layout.capacity) else null,
            .hashes = if (hash_offset) |offset| bytesAsSliceMut(u64, slab[offset..], layout.capacity) else null,
            .bucket_ids = if (bucket_offset) |offset| bytesAsSliceMut(u16, slab[offset..], layout.capacity) else null,
        };
    }

    pub fn clear(self: *GroupChunk) void {
        self.row_count = 0;
    }

    pub fn deinit(self: *GroupChunk, allocator: Allocator) void {
        allocator.free(self.slab);
        allocator.free(self.columns);
        self.* = .{};
    }

    pub fn keysU32(self: *const GroupChunk) []const u32 {
        std.debug.assert(self.key_width == .u32);
        return bytesAsSliceConst(u32, self.key_bytes, self.row_count);
    }

    pub fn keysU64(self: *const GroupChunk) []const u64 {
        std.debug.assert(self.key_width == .u64);
        return bytesAsSliceConst(u64, self.key_bytes, self.row_count);
    }

    pub fn keyU96Lo(self: *const GroupChunk) []const u64 {
        std.debug.assert(self.key_width == .u96);
        return bytesAsSliceConst(u64, self.key_bytes, self.row_count);
    }

    pub fn keyU96Hi(self: *const GroupChunk) []const u32 {
        std.debug.assert(self.key_width == .u96);
        const offset = @sizeOf(u64) * self.capacity;
        return bytesAsSliceConst(u32, self.key_bytes[offset..], self.row_count);
    }

    pub fn keysU128(self: *const GroupChunk) []const u128 {
        std.debug.assert(self.key_width == .u128);
        return bytesAsSliceConst(u128, self.key_bytes, self.row_count);
    }

    pub fn keysU32Mut(self: *GroupChunk) []u32 {
        std.debug.assert(self.key_width == .u32);
        return bytesAsSliceMut(u32, self.key_bytes, self.row_count);
    }

    pub fn keysU64Mut(self: *GroupChunk) []u64 {
        std.debug.assert(self.key_width == .u64);
        return bytesAsSliceMut(u64, self.key_bytes, self.row_count);
    }

    pub fn keyU96LoMut(self: *GroupChunk) []u64 {
        std.debug.assert(self.key_width == .u96);
        return bytesAsSliceMut(u64, self.key_bytes, self.row_count);
    }

    pub fn keyU96HiMut(self: *GroupChunk) []u32 {
        std.debug.assert(self.key_width == .u96);
        const offset = @sizeOf(u64) * self.capacity;
        return bytesAsSliceMut(u32, self.key_bytes[offset..], self.row_count);
    }

    pub fn keysU128Mut(self: *GroupChunk) []u128 {
        std.debug.assert(self.key_width == .u128);
        return bytesAsSliceMut(u128, self.key_bytes, self.row_count);
    }
};

fn validateGroupChunkLayout(layout: GroupChunkLayout) !void {
    if (layout.capacity == 0) return error.UnsupportedOperatorForType;
    for (layout.columns) |col| {
        if (col.role != .aggregate_input and
            col.role != .having_input and
            col.role != .order_input and
            col.role != .projection_input and
            col.role != .late_materialization_ref)
        {
            return error.UnsupportedOperatorForType;
        }
    }
}

inline fn alignForward(value: usize, alignment: usize) usize {
    std.debug.assert(alignment != 0);
    std.debug.assert((alignment & (alignment - 1)) == 0);
    return (value + alignment - 1) & ~(alignment - 1);
}

inline fn nullBitmapWords(capacity: usize) usize {
    return (capacity + 63) / 64;
}

inline fn keyBytes(width: KeyWidth) usize {
    return switch (width) {
        .u32 => @sizeOf(u32),
        .u64 => @sizeOf(u64),
        .u96 => @sizeOf(u64) + @sizeOf(u32),
        .u128 => @sizeOf(u128),
    };
}

inline fn keyAlignment(width: KeyWidth) usize {
    return switch (width) {
        .u32 => @alignOf(u32),
        .u64 => @alignOf(u64),
        .u96 => @alignOf(u64),
        .u128 => @alignOf(u128),
    };
}

inline fn physicalSize(typ: PhysicalType) usize {
    return switch (typ) {
        .i8, .u8 => 1,
        .i16, .u16 => 2,
        .i32, .u32, .f32 => 4,
        .i64, .u64, .f64 => 8,
    };
}

inline fn physicalAlignment(typ: PhysicalType) usize {
    return switch (typ) {
        .i8 => @alignOf(i8),
        .i16 => @alignOf(i16),
        .i32 => @alignOf(i32),
        .i64 => @alignOf(i64),
        .u8 => @alignOf(u8),
        .u16 => @alignOf(u16),
        .u32 => @alignOf(u32),
        .u64 => @alignOf(u64),
        .f32 => @alignOf(f32),
        .f64 => @alignOf(f64),
    };
}

fn bytesAsSliceMut(comptime T: type, bytes: []u8, len: usize) []T {
    return @as([*]T, @ptrCast(@alignCast(bytes.ptr)))[0..len];
}

fn bytesAsSliceConst(comptime T: type, bytes: []const u8, len: usize) []const T {
    return @as([*]const T, @ptrCast(@alignCast(bytes.ptr)))[0..len];
}

fn columnValuesFromBytes(typ: PhysicalType, bytes: []u8, capacity: usize) ColumnValues {
    return switch (typ) {
        .i8 => .{ .i8 = bytesAsSliceMut(i8, bytes, capacity) },
        .i16 => .{ .i16 = bytesAsSliceMut(i16, bytes, capacity) },
        .i32 => .{ .i32 = bytesAsSliceMut(i32, bytes, capacity) },
        .i64 => .{ .i64 = bytesAsSliceMut(i64, bytes, capacity) },
        .u8 => .{ .u8 = bytesAsSliceMut(u8, bytes, capacity) },
        .u16 => .{ .u16 = bytesAsSliceMut(u16, bytes, capacity) },
        .u32 => .{ .u32 = bytesAsSliceMut(u32, bytes, capacity) },
        .u64 => .{ .u64 = bytesAsSliceMut(u64, bytes, capacity) },
        .f32 => .{ .f32 = bytesAsSliceMut(f32, bytes, capacity) },
        .f64 => .{ .f64 = bytesAsSliceMut(f64, bytes, capacity) },
    };
}

pub const AggregateInput = struct {
    source_name: []const u8,
    source_type: types.Type,
    physical_type: PhysicalType,
    // Nullable inputs stage a per-row validity byte alongside the value
    // column (see GroupColumnSpec in the harness core).
    nullable: bool = false,
};

pub const GroupKeyInput = struct {
    name: []const u8,
    source_type: types.Type,
    offset_bits: u8,
    width_bits: u8,
    // Packed lane: width_bits includes a validity top bit. Hashed lane: the
    // validity tag joins the digest. See GroupKeyColumnSpec in the harness core.
    nullable: bool = false,
};

pub const AggregateOp = enum {
    count_star,
    count_col,
    sum,
    avg,
    min,
    max,
    count_distinct,
    // Variance/stddev family — Welford over three slots (see the harness
    // core's GroupAggregateOp).
    welford,
};

pub const AggregateSpec = struct {
    op: AggregateOp,
    input_column_index: ?u16,
    input_type: PhysicalType,
    state_index: u16,
    // The input column is nullable: NULL rows skip the fold; the companion
    // slot `valid_count_index` (0 = none) holds the group's non-null input
    // count for the AVG denominator and the all-NULL → NULL finalize.
    nullable: bool = false,
    valid_count_index: u16 = 0,
    // String MIN/MAX: reads `Shape.string_aggregate_inputs[str_input_index]` and
    // keeps its result in string state slot `str_state_index` (not the numeric
    // slots). False/0 for every numeric aggregate.
    is_string: bool = false,
    str_input_index: u16 = 0,
    str_state_index: u16 = 0,
    // COUNT(DISTINCT col): reads the carried integer `input_column_index` and
    // tracks membership in combined set `distinct_state_index`; `state_index` is
    // the running count slot. False/0 for every other aggregate.
    is_distinct: bool = false,
    distinct_state_index: u16 = 0,
    // SUM/AVG over a 64-bit integer input: i128 accumulation across two
    // consecutive slots (lo at state_index-1, hi at state_index). Generic
    // per-row program only; fused/weighted kernels decline.
    wide: bool = false,
};

pub const StringAggInput = struct {
    source_name: []const u8,
};

pub const AggregateProgram = struct {
    specs: []const AggregateSpec,
};

pub const Shape = struct {
    key_width: KeyWidth,
    key_bits: u16,
    group_key_count: usize,
    group_key_inputs: []const GroupKeyInput = &.{},
    aggregate_inputs: []const AggregateInput,
    // String agg-input columns (for string MIN/MAX), carried as variable-length
    // values. Empty for numeric-only shapes.
    string_aggregate_inputs: []const StringAggInput = &.{},
    aggregate_program: []const AggregateSpec = &.{},
    has_filter: bool,
    order_by_count_desc: bool,
    limit: usize,
    offset: usize,
    emit_all_groups: bool = false,
    // When `emit_all_groups` is set for an unordered `LIMIT N` (no ORDER BY),
    // only the first N+offset groups are ever needed — any N groups satisfy it.
    // Cap the emit at this many groups instead of materializing every group just
    // to slice the first N. With HAVING, `emit_filter` decides which groups
    // count toward this cap. 0 means no cap.
    emit_all_groups_cap: usize = 0,
    // Per-group HAVING predicate applied during the capped all-groups emit, so
    // only HAVING survivors count toward `emit_all_groups_cap`. Null when there
    // is no HAVING.
    emit_filter: ?HarnessCore.EmitFilter = null,
    // The group key is a hash of the key columns (string / >128-bit keys); each
    // staged row carries its __rowloc so the real key values are recovered at
    // emit via late materialization. Forces the u128 key lane + rowref region.
    hashed: bool = false,
};

pub const Params = struct {
    dop: usize = DEFAULT_DOP,
    bucket_count: usize = DEFAULT_BUCKET_COUNT,
    scan_tile_rgs: usize = DEFAULT_SCAN_TILE_RGS,
    route_block_rows: usize = DEFAULT_ROUTE_BLOCK_ROWS,
    group_lease_buckets: usize = DEFAULT_GROUP_LEASE_BUCKETS,
    group_lease_rows: u64 = 0,
    group_init_cap: usize = DEFAULT_GROUP_INIT_CAP,
    raw_chunk_rows: usize = DEFAULT_RAW_CHUNK_ROWS,
    raw_group_chunk_rows: usize = DEFAULT_RAW_GROUP_CHUNK_ROWS,
    raw_batch_chunks: usize = DEFAULT_RAW_BATCH_CHUNKS,
    shared_stage_builders: bool = true,
    worker_profile: bool = false,
    trace_timing: bool = false,
    arena_workspace: bool = false,
    sync_teardown: bool = false,
};

pub const RunRequest = struct {
    table: *api.Table,
    shape: Shape,
    params: Params,
    scan_columns: ?[]const []const u8 = null,
    // Derived columns the scan-layer Compute adds to each batch before keying
    // and aggregation (e.g. ClientIP - 1, length(URL)). Empty for plain shapes.
    derived: []const exec.Derived = &.{},
    filter_expr: ?exec.PredicateExpr = null,
};

pub const Times = struct {
    cpu_layout_ticks: i64 = 0,
    core_ticks: i64 = 0,
    result_copy_ticks: i64 = 0,
    workspace_teardown_ticks: i64 = 0,
};

pub const Result = struct {
    allocator: Allocator,
    rows: []HarnessCore.TopRow = &.{},
    params: Params,
    times: Times = .{},

    pub fn deinit(self: *Result) void {
        if (self.rows.len > 0) self.allocator.free(self.rows);
        self.rows = &.{};
    }
};

const AsyncWorkspaceTeardownTask = struct {
    allocator: Allocator,
    workspace: HarnessCore.SiloGridWorkspace,
    cpus: []usize,
    n_workers: usize,
    trace_timing: bool,
    query_label: []const u8,
    bucket_count: usize,
};

pub fn paramsFromEnv(default_dop: usize) Params {
    return .{
        .dop = @max(@as(usize, 1), default_dop),
        .bucket_count = @max(@as(usize, 1), envUsize("THINDB_V2_BUCKET_COUNT", DEFAULT_BUCKET_COUNT)),
        .scan_tile_rgs = @max(@as(usize, 1), envUsize("THINDB_V2_SCAN_TILE_RGS", DEFAULT_SCAN_TILE_RGS)),
        .route_block_rows = @max(@as(usize, 1), envUsize("THINDB_V2_ROUTE_BLOCK_ROWS", DEFAULT_ROUTE_BLOCK_ROWS)),
        .group_lease_buckets = @max(@as(usize, 1), envUsize("THINDB_V2_GROUP_LEASE_BUCKETS", DEFAULT_GROUP_LEASE_BUCKETS)),
        .group_lease_rows = @intCast(envUsize("THINDB_V2_GROUP_LEASE_ROWS", 0)),
        .group_init_cap = envUsize("THINDB_V2_GROUP_INIT_CAP", DEFAULT_GROUP_INIT_CAP),
        .raw_chunk_rows = @max(@as(usize, 1), envUsize("THINDB_V2_RAW_CHUNK_ROWS", DEFAULT_RAW_CHUNK_ROWS)),
        .raw_group_chunk_rows = @max(@as(usize, 1), envUsize("THINDB_V2_RAW_GROUP_CHUNK_ROWS", DEFAULT_RAW_GROUP_CHUNK_ROWS)),
        .raw_batch_chunks = @max(@as(usize, 1), envUsize("THINDB_V2_RAW_BATCH_CHUNKS", DEFAULT_RAW_BATCH_CHUNKS)),
        .shared_stage_builders = getenv("THINDB_V2_NO_SHARED_STAGE_BUILDERS") == null,
        .worker_profile = build_options.profiling and (getenv("THINDB_V2_WORKER_PROFILE") != null),
        .trace_timing = build_options.profiling and (getenv("THINDB_V2_PIPELINE_TRACE") != null),
        .arena_workspace = getenv("THINDB_V2_ARENA_WORKSPACE") != null,
        .sync_teardown = getenv("THINDB_V2_SYNC_TEARDOWN") != null,
    };
}

pub fn run(allocator: Allocator, request: RunRequest) !Result {
    try validateShape(request.shape);
    try validateGroupChunkShape(request.shape, request.params);

    const t_layout = exec.prof.nowTicks();
    var layout = try HarnessCore.cpuLayout(allocator);
    defer layout.deinit(allocator);
    var times = Times{ .cpu_layout_ticks = exec.prof.nowTicks() - t_layout };

    const n = @min(@max(@as(usize, 1), request.params.dop), layout.order.len);
    if (n == 0) return .{ .allocator = allocator, .params = request.params, .times = times };

    if (request.params.arena_workspace) {
        return runArenaWorkspace(allocator, request, layout.order[0..n], &times);
    }
    return runFreshWorkspace(allocator, request, layout.order[0..n], &times);
}

fn validateShape(shape: Shape) !void {
    if (shape.group_key_count == 0) return error.UnsupportedOperatorForType;
    if (shape.group_key_inputs.len != shape.group_key_count) return error.UnsupportedOperatorForType;
    if (shape.key_width != KeyWidth.fromBits(shape.key_bits)) return error.UnsupportedOperatorForType;
    for (shape.aggregate_program) |agg| {
        if (agg.input_column_index) |idx| {
            if (idx >= shape.aggregate_inputs.len) return error.UnsupportedOperatorForType;
        }
    }
}

fn validateGroupChunkShape(shape: Shape, params: Params) !void {
    if (shape.aggregate_inputs.len > 64) return error.UnsupportedOperatorForType;
    var columns_buf: [64]ColumnSpec = undefined;
    for (shape.aggregate_inputs, 0..) |input, i| {
        columns_buf[i] = .{
            .source_index = @intCast(i),
            .role = .aggregate_input,
            .physical_type = input.physical_type,
            .nullable = false,
        };
    }
    const layout = GroupChunkLayout{
        .key_width = shape.key_width,
        .capacity = params.raw_group_chunk_rows,
        .columns = columns_buf[0..shape.aggregate_inputs.len],
    };
    try validateGroupChunkLayout(layout);
}

fn runArenaWorkspace(allocator: Allocator, request: RunRequest, cpus: []const usize, times: *Times) !Result {
    var arena = std.heap.ArenaAllocator.init(allocator);
    const arena_allocator = arena.allocator();
    errdefer arena.deinit();

    var workspace: HarnessCore.SiloGridWorkspace = .{};
    var rows: std.ArrayListUnmanaged(HarnessCore.TopRow) = .empty;
    const core_t0 = exec.prof.nowTicks();
    try runHarness(arena_allocator, request.table, cpus, request.shape, request.params, request.scan_columns, request.derived, request.filter_expr, &rows, &workspace);
    times.core_ticks = exec.prof.nowTicks() - core_t0;

    const copy_t0 = exec.prof.nowTicks();
    const owned = try allocator.dupe(HarnessCore.TopRow, rows.items);
    times.result_copy_ticks = exec.prof.nowTicks() - copy_t0;

    const teardown_t0 = exec.prof.nowTicks();
    arena.deinit();
    times.workspace_teardown_ticks = exec.prof.nowTicks() - teardown_t0;
    return .{ .allocator = allocator, .rows = owned, .params = request.params, .times = times.* };
}

fn runFreshWorkspace(allocator: Allocator, request: RunRequest, cpus: []const usize, times: *Times) !Result {
    var workspace: HarnessCore.SiloGridWorkspace = .{};
    errdefer workspace.deinitParallel(allocator, cpus.len, cpus, null);

    var rows: std.ArrayListUnmanaged(HarnessCore.TopRow) = .empty;
    errdefer rows.deinit(allocator);

    const core_t0 = exec.prof.nowTicks();
    try runHarness(allocator, request.table, cpus, request.shape, request.params, request.scan_columns, request.derived, request.filter_expr, &rows, &workspace);
    times.core_ticks = exec.prof.nowTicks() - core_t0;

    const copy_t0 = exec.prof.nowTicks();
    const owned = try rows.toOwnedSlice(allocator);
    times.result_copy_ticks = exec.prof.nowTicks() - copy_t0;

    times.workspace_teardown_ticks = scheduleWorkspaceTeardown(
        allocator,
        &workspace,
        cpus.len,
        cpus,
        request.params,
        "generic",
    );
    return .{ .allocator = allocator, .rows = owned, .params = request.params, .times = times.* };
}

// Payload-weighted collapse only pays when the COMPOSITE key actually arrives
// in runs long enough to amortize the widened staging. The storage layer
// already measured per-column run structure: an RLE block's header carries its
// run count. Sample one mid-table block per key column — every column must be
// RLE (a composite run needs all parts running at once), and the composite
// run count (≈ the union of all columns' run boundaries, conservatively their
// sum) must leave an average run of ≥4 rows. Derived keys (absent from the
// schema) and segment-less tables decline — per-row staging is the default.
fn keyColumnsRunStructured(table: *api.Table, key_columns: []const HarnessCore.GroupKeyColumnSpec) bool {
    if (key_columns.len == 0) return false;
    const segs = table.manifest.segments.items;
    if (segs.len == 0) return false;
    const entry = table.acquireSegment(segs[segs.len / 2].segment_id) catch return false;
    defer table.releaseSegment(entry);
    const seg = &entry.seg;
    if (seg.info.row_groups.len == 0) return false;
    const rg = seg.info.row_groups.len / 2;
    const rg_count = seg.info.row_groups[rg].row_count;
    if (rg_count == 0) return false;
    var composite_runs: u64 = 0;
    for (key_columns) |part| {
        if (part.nullable) return false;
        const phys = blk: {
            for (table.schema.columns, 0..) |col, i| {
                if (types.columnNameEql(col.name, part.name)) break :blk i;
            }
            return false;
        };
        var block = seg.borrowColumnBlock(table.allocator, rg, phys, &table.cache) catch return false;
        defer block.release(table.allocator, &table.cache);
        if (block.encoding != .rle) return false;
        const flags = storage.format.ColumnBlockFlags{ .has_nulls = table.schema.columns[phys].nullable };
        composite_runs += storage.segment_reader.rleViewOf(block.bytes, rg_count, flags).block.n_runs;
    }
    return @as(u64, rg_count) >= 4 * @max(composite_runs, 1);
}

fn runHarness(
    allocator: Allocator,
    table: *api.Table,
    cpus: []const usize,
    shape: Shape,
    params: Params,
    scan_columns: ?[]const []const u8,
    derived: []const exec.Derived,
    filter_expr: ?exec.PredicateExpr,
    rows: *std.ArrayListUnmanaged(HarnessCore.TopRow),
    workspace: *HarnessCore.SiloGridWorkspace,
) !void {
    var group_key_columns_buf: [8]HarnessCore.GroupKeyColumnSpec = undefined;
    if (shape.group_key_inputs.len > group_key_columns_buf.len) return error.UnsupportedOperatorForType;
    for (shape.group_key_inputs, 0..) |input, i| {
        group_key_columns_buf[i] = .{
            .name = input.name,
            .typ = input.source_type,
            .offset_bits = input.offset_bits,
            .width_bits = input.width_bits,
            .nullable = input.nullable,
        };
    }
    var group_columns_buf: [16]HarnessCore.GroupColumnSpec = undefined;
    if (shape.aggregate_inputs.len > group_columns_buf.len) return error.UnsupportedOperatorForType;
    var n_valid_lanes: u16 = 0;
    for (shape.aggregate_inputs, 0..) |input, i| {
        group_columns_buf[i] = .{ .physical_type = switch (input.physical_type) {
            .i8 => .i8,
            .i16 => .i16,
            .i32 => .i32,
            .i64 => .i64,
            .f32 => .f32,
            .f64 => .f64,
            else => return error.UnsupportedOperatorForType,
        }, .source = try harnessColumnSource(input.source_name), .source_name = input.source_name, .nullable = input.nullable, .valid_index = n_valid_lanes };
        if (input.nullable) n_valid_lanes += 1;
    }
    var group_str_columns_buf: [4]HarnessCore.GroupStrColumnSpec = undefined;
    if (shape.string_aggregate_inputs.len > group_str_columns_buf.len) return error.UnsupportedOperatorForType;
    for (shape.string_aggregate_inputs, 0..) |sin, i| {
        group_str_columns_buf[i] = .{ .source_name = sin.source_name };
    }
    var group_aggregates_buf: [16]HarnessCore.GroupAggregateSpec = undefined;
    if (shape.aggregate_program.len > group_aggregates_buf.len) return error.UnsupportedOperatorForType;
    var distinct_slot_count: u16 = 0;
    for (shape.aggregate_program, 0..) |agg, i| {
        if (agg.is_distinct) distinct_slot_count = @max(distinct_slot_count, agg.distinct_state_index + 1);
        group_aggregates_buf[i] = .{
            .op = switch (agg.op) {
                .count_star => .count_star,
                .count_col => .count_col,
                .sum => .sum,
                .avg => .avg,
                .min => .min,
                .max => .max,
                .count_distinct => .count_distinct,
                .welford => .welford,
            },
            .input_column_index = agg.input_column_index,
            .state_index = agg.state_index,
            .is_string = agg.is_string,
            .str_input_index = agg.str_input_index,
            .str_state_index = agg.str_state_index,
            .is_distinct = agg.is_distinct,
            .distinct_state_index = agg.distinct_state_index,
            .wide = agg.wide,
            .nullable = agg.nullable,
            .valid_count_index = agg.valid_count_index,
        };
    }
    // shape.hashed comes from the shape gate (string / >128-bit keys). The env
    // toggle additionally forces an integer-key query down the same path for
    // cross-checking against the exact integer-packed result.
    const force_hash = shape.hashed or getenv("THINDB_V2_FORCE_HASH_KEY") != null;
    // Weighted run collapse: decomposable programs (COUNT(*) / SUM / AVG /
    // MIN / MAX) ship (key, weight, pre-folded inputs) staged rows — the scan
    // emitter folds a run of adjacent-equal keys (the table's physical order
    // clusters them) into ONE row carrying the run's partial aggregates.
    // Declined for COUNT(col)/DISTINCT/string aggs (need per-row data), for
    // an input column shared by two aggregates (one staged value can't be
    // both a run-sum and a run-min), and for a non-count aggregate at state
    // slot 0 (the lane's kernel pass can't express it — mirrors its
    // `kernelizable` check).
    const weight_mode = blk: {
        if (shape.aggregate_program.len == 0) break :blk false;
        if (shape.string_aggregate_inputs.len != 0 or distinct_slot_count != 0) break :blk false;
        var input_used = [_]bool{false} ** 16;
        for (shape.aggregate_program) |agg| {
            if (agg.is_distinct or agg.is_string) break :blk false;
            switch (agg.op) {
                .count_star => {},
                .sum, .avg, .min, .max => {
                    if (agg.state_index == 0) break :blk false;
                    // Wide (i128) state folds per row in the generic program;
                    // run partials are single i64 cells — and NULL-blind, so
                    // nullable inputs decline too.
                    if (agg.wide or agg.nullable) break :blk false;
                    const ic = agg.input_column_index orelse break :blk false;
                    if (ic >= shape.aggregate_inputs.len or input_used[ic]) break :blk false;
                    input_used[ic] = true;
                },
                .count_col, .count_distinct, .welford => break :blk false,
            }
        }
        // Every staged column must belong to exactly one folding aggregate.
        for (input_used[0..shape.aggregate_inputs.len]) |u| {
            if (!u) break :blk false;
        }
        // COUNT-only collapse is nearly free (a 4-byte weight) and ships
        // unconditionally. Payload-carrying collapse widens every staged
        // column to 8 bytes, so on keys that DON'T arrive in runs it is pure
        // staging inflation (measured: Q31/Q32-class +15-20%) — require
        // storage-proven run structure on every key column first.
        if (shape.aggregate_inputs.len != 0 and
            !keyColumnsRunStructured(table, group_key_columns_buf[0..shape.group_key_inputs.len])) break :blk false;
        break :blk true;
    };
    // Weighted staged columns carry RUN PARTIALS, not row values: widen to
    // i64/f64 so a pre-summed run can't overflow its column (and the emitter
    // folds into one uniform width per family).
    if (weight_mode) {
        for (group_columns_buf[0..shape.aggregate_inputs.len]) |*col| {
            col.physical_type = switch (col.physical_type) {
                .i8, .i16, .i32, .i64 => .i64,
                .f32, .f64 => .f64,
            };
        }
    }
    const group_rows_layout = HarnessCore.GroupRowsLayout{
        .key_width = if (force_hash) .u128 else harnessKeyWidth(shape.key_width),
        .key_columns = group_key_columns_buf[0..shape.group_key_inputs.len],
        .columns = group_columns_buf[0..shape.aggregate_inputs.len],
        .aggregates = group_aggregates_buf[0..shape.aggregate_program.len],
        .str_columns = group_str_columns_buf[0..shape.string_aggregate_inputs.len],
        .has_str_payload = shape.string_aggregate_inputs.len > 0,
        .distinct_slot_count = distinct_slot_count,
        .has_rowref = force_hash,
        .has_weight = weight_mode,
    };

    // Scan-side key digests: a string group key consumed only as hashed
    // identity (its bytes come back via rowref late-mat) never materializes —
    // the scan emits per-row `stringKeyDigest` values straight off the cached
    // block. Excluded when the column is also an aggregate input or read by a
    // derived expression (those need real bytes in the batch).
    var hash_key_cols_buf: [8][]const u8 = undefined;
    var n_hash_keys: usize = 0;
    if (force_hash and derived.len == 0) {
        outer: for (shape.group_key_inputs) |input| {
            switch (input.source_type) {
                .varchar, .string, .char => {},
                else => continue,
            }
            for (shape.aggregate_inputs) |ai| {
                if (types.columnNameEql(ai.source_name, input.name)) continue :outer;
            }
            for (shape.string_aggregate_inputs) |si| {
                if (types.columnNameEql(si.source_name, input.name)) continue :outer;
            }
            for (derived) |d| {
                if (derivedReadsColumn(allocator, d, input.name) catch true) continue :outer;
            }
            hash_key_cols_buf[n_hash_keys] = input.name;
            n_hash_keys += 1;
        }
    }

    try HarnessCore.runSiloGrid(allocator, table, cpus, .{
        .dop = params.dop,
        .bucket_count = params.bucket_count,
        .silo_grid = true,
        .scan_filter = true,
        .chunk_rows = params.raw_chunk_rows,
        .chunk_rows_set = true,
        .scan_tile_rgs = params.scan_tile_rgs,
        .scan_tile_rgs_set = true,
        .route_block_rows = params.route_block_rows,
        .route_block_rows_set = true,
        .group_lease_buckets = params.group_lease_buckets,
        .group_lease_rows = params.group_lease_rows,
        .group_init_cap = params.group_init_cap,
        .raw_group_mode = .staged_final,
        .raw_chunk_rows = params.raw_chunk_rows,
        .raw_group_chunk_rows = params.raw_group_chunk_rows,
        .raw_batch_chunks = params.raw_batch_chunks,
        .group_rows_layout = group_rows_layout,
        .scan_columns = scan_columns,
        .derived = derived,
        .filter_expr = filter_expr,
        .shared_stage_builders = params.shared_stage_builders,
        .no_profile = !params.worker_profile,
        .quiet = !params.worker_profile,
        .result_out = rows,
        .top_k = shape.limit + shape.offset,
        .result_all_groups = shape.emit_all_groups,
        .result_all_groups_cap = shape.emit_all_groups_cap,
        .result_emit_filter = shape.emit_filter,
        .trace_timing = params.trace_timing,
        .workspace = workspace,
        // The arena path frees the whole arena synchronously right after the
        // run — a detached free racing it would be use-after-free.
        .defer_heavy_teardown = !params.arena_workspace and !params.sync_teardown,
        .hash_key_columns = hash_key_cols_buf[0..n_hash_keys],
    });
}

fn derivedReadsColumn(allocator: Allocator, d: exec.Derived, name: []const u8) !bool {
    var refs: std.ArrayListUnmanaged([]const u8) = .empty;
    defer refs.deinit(allocator);
    try exec.compute_op.collectColumnRefs(allocator, &refs, d.expr);
    for (refs.items) |r| {
        if (types.columnNameEql(r, name)) return true;
    }
    return false;
}

fn harnessColumnSource(name: []const u8) !HarnessCore.GroupColumnSource {
    if (types.columnNameEql(name, "IsRefresh")) return .is_refresh;
    if (types.columnNameEql(name, "ResolutionWidth")) return .resolution_width;
    return .is_refresh;
}

inline fn harnessKeyWidth(width: KeyWidth) HarnessCore.GroupKeyWidth {
    return switch (width) {
        .u32 => .u32,
        .u64 => .u64,
        .u96 => .u96,
        .u128 => .u128,
    };
}

fn scheduleWorkspaceTeardown(
    allocator: Allocator,
    workspace: *HarnessCore.SiloGridWorkspace,
    n_workers: usize,
    cpus: []const usize,
    params: Params,
    query_label: []const u8,
) i64 {
    const t0 = exec.prof.nowTicks();
    if (params.sync_teardown) {
        var teardown_profile: HarnessCore.WorkspaceProfile = .{};
        const profile_ptr: ?*HarnessCore.WorkspaceProfile = if (params.trace_timing) &teardown_profile else null;
        workspace.deinitParallel(allocator, n_workers, cpus, profile_ptr);
        if (profile_ptr) |profile| profile.printTeardown(query_label, params.bucket_count);
        return exec.prof.nowTicks() - t0;
    }

    const task = allocator.create(AsyncWorkspaceTeardownTask) catch {
        workspace.deinitParallel(allocator, n_workers, cpus, null);
        return exec.prof.nowTicks() - t0;
    };
    const cpus_copy = allocator.dupe(usize, cpus) catch {
        allocator.destroy(task);
        workspace.deinitParallel(allocator, n_workers, cpus, null);
        return exec.prof.nowTicks() - t0;
    };
    task.* = .{
        .allocator = allocator,
        .workspace = workspace.*,
        .cpus = cpus_copy,
        .n_workers = n_workers,
        .trace_timing = params.trace_timing,
        .query_label = query_label,
        .bucket_count = params.bucket_count,
    };
    workspace.* = .{};
    const thread = std.Thread.spawn(.{}, asyncWorkspaceTeardown, .{task}) catch {
        var teardown_profile: HarnessCore.WorkspaceProfile = .{};
        const profile_ptr: ?*HarnessCore.WorkspaceProfile = if (params.trace_timing) &teardown_profile else null;
        task.workspace.deinitParallel(allocator, n_workers, cpus_copy, profile_ptr);
        if (profile_ptr) |profile| profile.printTeardown(query_label, params.bucket_count);
        allocator.free(cpus_copy);
        allocator.destroy(task);
        return exec.prof.nowTicks() - t0;
    };
    thread.detach();
    return exec.prof.nowTicks() - t0;
}

fn asyncWorkspaceTeardown(task: *AsyncWorkspaceTeardownTask) void {
    var teardown_profile: HarnessCore.WorkspaceProfile = .{};
    const profile_ptr: ?*HarnessCore.WorkspaceProfile = if (task.trace_timing) &teardown_profile else null;
    task.workspace.deinitParallel(task.allocator, task.n_workers, task.cpus, profile_ptr);
    if (profile_ptr) |profile| {
        profile.printTeardown(task.query_label, task.bucket_count);
        std.debug.print("[workspace-teardown-async] query={s} complete=true\n", .{task.query_label});
    }
    task.allocator.free(task.cpus);
    const allocator = task.allocator;
    allocator.destroy(task);
}

fn envUsize(comptime name: [:0]const u8, default: usize) usize {
    const raw = getenv(name.ptr) orelse return default;
    const text = std.mem.span(raw);
    if (text.len == 0) return default;
    return std.fmt.parseInt(usize, text, 10) catch default;
}

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;
