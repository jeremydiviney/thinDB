//! Build a segment file in memory from a set of ColumnViews, then flush to disk.
//!
//! Single entry point: `writeSegment`. Caller owns the returned SegmentInfo
//! (call `.deinit(allocator)`).

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const TableSchema = types.TableSchema;

const format = @import("format.zig");
const column = @import("column.zig");
const compression_mod = @import("compression.zig");
const fsst = @import("fsst.zig");
const hll = @import("../util/hll.zig");
const ColumnView = column.ColumnView;
const StringView = column.StringView;
const RowGroupMeta = format.RowGroupMeta;
const SegmentInfo = format.SegmentInfo;

/// Fixed hash for NULL cells, so all NULLs count as one distinct value
/// (matching GROUP BY treating NULL as a single group).
const null_hash: u64 = 0x9E3779B97F4A7C15;

fn hashCell(view: ColumnView, row: usize) u64 {
    switch (view.data) {
        .varchar, .string, .char => |sv| return std.hash.Wyhash.hash(0, sv.rowBytes(row)),
        // Full-value 64-bit hash (not a truncated prefix) so two distinct
        // values never collapse — under-counting would wrongly make a
        // high-cardinality field look hashable.
        inline else => |s| return std.hash.Wyhash.hash(0, std.mem.asBytes(&s[row])),
    }
}

/// Per-column HyperLogLog sketches over all rows, concatenated (column
/// `ci` at `[ci*hll.m .. (ci+1)*hll.m]`). Caller owns the returned slice.
fn computeSketches(allocator: Allocator, columns: []const ColumnView, row_count: usize) ![]u8 {
    const buf = try allocator.alloc(u8, columns.len * hll.m);
    errdefer allocator.free(buf);
    for (columns, 0..) |view, ci| {
        var sketch: hll.Hll = .{};
        var r: usize = 0;
        while (r < row_count) : (r += 1) {
            sketch.add(if (view.isValid(r)) hashCell(view, r) else null_hash);
        }
        @memcpy(buf[ci * hll.m .. ci * hll.m + hll.m], sketch.bytes());
    }
    return buf;
}

pub fn writeSegment(
    allocator: Allocator,
    io: Io,
    dir: Io.Dir,
    file_name: []const u8,
    schema: TableSchema,
    segment_id: u64,
    schema_fingerprint: u64,
    row_group_size: usize,
    columns: []const ColumnView,
    /// Per-column HLL bytes of every segment that will coexist with this one
    /// (same layout as `SegmentInfo.column_sketches`). Used only to compute the
    /// global dict-eligibility cardinality; pass `&.{}` when no global view is
    /// available (the gate then sees this segment's NDV alone).
    prior_sketches: []const []const u8,
    sync_on_close: bool,
    /// Worker threads for per-column block encode+compress (same grain as
    /// `MergedSegmentWriter`). 1 = serial; output is byte-identical either way.
    encode_threads: usize,
) !SegmentInfo {
    if (columns.len != schema.columns.len) return format.Error.SchemaMismatch;
    if (row_group_size == 0) return format.Error.InvalidRowGroupSize;

    const row_count: usize = if (columns.len == 0) 0 else columns[0].rowCount();
    for (columns, schema.columns) |view, schema_col| {
        if (view.rowCount() != row_count) return format.Error.UnevenColumns;
        if (std.meta.activeTag(view.data) != std.meta.activeTag(schema_col.type)) {
            return format.Error.SchemaMismatch;
        }
    }

    // Per-column HLL over this segment's rows, computed up front: the
    // dict-eligibility gate below needs the segment-local NDV, and the same
    // sketches are returned in SegmentInfo (so we compute them once).
    const column_sketches = try computeSketches(allocator, columns, row_count);
    errdefer allocator.free(column_sketches);

    // Dict eligibility (per column): a string column is dictionary-encoded only
    // when BOTH the segment-local NDV and the global NDV (this segment merged
    // with every coexisting segment's sketch) are ≤ the cap. Above it the column
    // stays raw — query-time coding would decline a high-card column anyway, and
    // per-block dicts would only add decode cost. See NARROW_ENCODING_PLAN §2/§3.
    const dict_eligible = try allocator.alloc(bool, columns.len);
    defer allocator.free(dict_eligible);
    for (dict_eligible, 0..) |*elig, ci| {
        const seg_h = hll.Hll.fromBytes(column_sketches[ci * hll.m .. (ci + 1) * hll.m]);
        var global = seg_h;
        for (prior_sketches) |ps| {
            if (ps.len >= (ci + 1) * hll.m) {
                const ph = hll.Hll.fromBytes(ps[ci * hll.m .. (ci + 1) * hll.m]);
                global.merge(&ph);
            }
        }
        elig.* = seg_h.estimate() <= dict_max_global_ndv and global.estimate() <= dict_max_global_ndv;
    }

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    // ---- Header ----
    try buf.appendSlice(allocator, &format.segment_magic);
    try appendU16(allocator, &buf, format.segment_version);
    try appendU16(allocator, &buf, 0); // flags
    try appendU64(allocator, &buf, schema_fingerprint);
    try appendU64(allocator, &buf, segment_id);
    try appendU64(allocator, &buf, @intCast(row_count));
    std.debug.assert(buf.items.len == format.header_size);

    // ---- Row groups ----
    var row_groups: std.ArrayList(RowGroupMeta) = .empty;
    errdefer {
        for (row_groups.items) |rg| {
            allocator.free(rg.stats);
            allocator.free(@constCast(rg.col_offsets));
        }
        row_groups.deinit(allocator);
    }

    var row_offset: usize = 0;
    while (row_offset < row_count) {
        const rows_in_group: usize = @min(row_group_size, row_count - row_offset);
        const rg_file_offset: u64 = @intCast(buf.items.len);

        try appendU32(allocator, &buf, @intCast(rows_in_group));
        try appendU32(allocator, &buf, 0); // padding

        // Encode every column block in parallel (each into its own buffer),
        // then concatenate in column order — byte-identical to the serial
        // path regardless of thread count. The job also computes each
        // column's row-group stats.
        const blocks = try allocator.alloc(std.ArrayList(u8), columns.len);
        defer {
            for (blocks) |*b| b.deinit(allocator);
            allocator.free(blocks);
        }
        for (blocks) |*b| b.* = .empty;

        const stats = try allocator.alloc(format.Stats, columns.len);
        errdefer allocator.free(stats);

        var job = BlockEncodeJob{
            .allocator = allocator,
            .columns = columns,
            .schema = schema,
            .dict_eligible = dict_eligible,
            .row_start = row_offset,
            .row_end = row_offset + rows_in_group,
            .blocks = blocks,
            .stats = stats,
        };
        try job.run(encode_threads);

        // Record each column block's absolute file offset so the reader can
        // pread individual columns without loading the whole segment.
        const col_offsets = try allocator.alloc(u64, columns.len);
        errdefer allocator.free(col_offsets);
        var blocks_total: usize = 0;
        for (blocks) |b| blocks_total += b.items.len;
        try buf.ensureUnusedCapacity(allocator, blocks_total);
        for (blocks, 0..) |b, ci| {
            col_offsets[ci] = @intCast(buf.items.len);
            buf.appendSliceAssumeCapacity(b.items);
        }

        const rg_length: u32 = @intCast(buf.items.len - rg_file_offset);

        try row_groups.append(allocator, .{
            .offset = rg_file_offset,
            .length = rg_length,
            .row_count = @intCast(rows_in_group),
            .stats = stats,
            .col_offsets = col_offsets,
        });

        row_offset += rows_in_group;
    }

    // ---- Footer ----
    const footer_start = buf.items.len;
    try appendU32(allocator, &buf, @intCast(row_groups.items.len));
    for (row_groups.items) |rg| {
        try appendU64(allocator, &buf, rg.offset);
        try appendU32(allocator, &buf, rg.length);
        try appendU32(allocator, &buf, rg.row_count);
        for (rg.stats) |s| {
            try appendI128(allocator, &buf, s.min);
            try appendI128(allocator, &buf, s.max);
            try appendI128(allocator, &buf, s.sum);
            try appendU64(allocator, &buf, s.null_count);
        }
        for (rg.col_offsets) |co| {
            try appendU64(allocator, &buf, co);
        }
    }
    const footer_size: u32 = @intCast(buf.items.len - footer_start + format.footer_trailer_size);
    try appendU32(allocator, &buf, footer_size);
    try buf.appendSlice(allocator, &format.segment_magic);

    // ---- Flush to disk ----
    const byte_size: u64 = @intCast(buf.items.len);
    try @import("storage.zig").writeFileSynced(io, dir, file_name, buf.items, sync_on_close);

    return SegmentInfo{
        .segment_id = segment_id,
        .row_count = row_count,
        .schema_fingerprint = schema_fingerprint,
        .byte_size = byte_size,
        .row_groups = try row_groups.toOwnedSlice(allocator),
        .column_sketches = column_sketches,
    };
}

/// Compute per-column dict-eligibility from a set of per-column HLL sketches
/// (same layout as `SegmentInfo.column_sketches`). A column is eligible when
/// the merged NDV across all `sketches` is at or below `dict_max_global_ndv`.
/// Caller owns the returned slice.
///
/// Used by the streaming merge path, which derives the output's sketch from the
/// union of the input segments' sketches rather than from a materialized column.
pub fn dictEligibleFromSketches(
    allocator: Allocator,
    column_count: usize,
    sketches: []const []const u8,
) ![]bool {
    const dict_eligible = try allocator.alloc(bool, column_count);
    errdefer allocator.free(dict_eligible);
    for (dict_eligible, 0..) |*elig, ci| {
        var global: hll.Hll = .{};
        for (sketches) |ps| {
            if (ps.len >= (ci + 1) * hll.m) {
                const ph = hll.Hll.fromBytes(ps[ci * hll.m .. (ci + 1) * hll.m]);
                global.merge(&ph);
            }
        }
        elig.* = global.estimate() <= dict_max_global_ndv;
    }
    return dict_eligible;
}

/// Streaming segment writer for compaction's k-way merge. Unlike `writeSegment`,
/// which needs every column fully materialized, this accepts sorted row-groups
/// one at a time so the merge holds at most ~(k input row-groups + one output
/// row-group + the in-flight `buf`) in memory rather than the whole output.
///
/// `dict_eligible` and `column_sketches` are precomputed by the caller (from the
/// union of the input segments' sketches — a correct upper bound for dict
/// eligibility and an acceptable sketch for the merged output) and must outlive
/// `begin`/`writeRowGroup`. Ownership of `column_sketches` transfers to the
/// returned `SegmentInfo` at `finish`.
///
/// Usage: `begin`, then `writeRowGroup` per sorted row-group (≤ `row_group_size`
/// rows), then `finish`. On any error before `finish`, call `deinit`.
pub const MergedSegmentWriter = struct {
    allocator: Allocator,
    schema: TableSchema,
    segment_id: u64,
    schema_fingerprint: u64,
    row_group_size: usize,
    dict_eligible: []const bool,
    column_sketches: []const u8,
    /// Worker threads for per-column block encode+compress inside
    /// `writeRowGroup`. 1 = serial (encode on the calling thread).
    encode_threads: usize,

    buf: std.ArrayList(u8),
    row_groups: std.ArrayList(RowGroupMeta),
    row_count: u64,

    pub fn begin(
        allocator: Allocator,
        schema: TableSchema,
        segment_id: u64,
        schema_fingerprint: u64,
        row_group_size: usize,
        dict_eligible: []const bool,
        /// Per-column HLL bytes for the merged output (same layout as
        /// `SegmentInfo.column_sketches`). Ownership transfers to `finish`'s
        /// `SegmentInfo`; freed by `deinit` if the writer is torn down first.
        column_sketches: []const u8,
        encode_threads: usize,
    ) !MergedSegmentWriter {
        if (row_group_size == 0) return format.Error.InvalidRowGroupSize;
        if (dict_eligible.len != schema.columns.len) return format.Error.SchemaMismatch;

        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);

        try buf.appendSlice(allocator, &format.segment_magic);
        try appendU16(allocator, &buf, format.segment_version);
        try appendU16(allocator, &buf, 0); // flags
        try appendU64(allocator, &buf, schema_fingerprint);
        try appendU64(allocator, &buf, segment_id);
        try appendU64(allocator, &buf, 0); // row_count placeholder, patched at finish
        std.debug.assert(buf.items.len == format.header_size);

        return .{
            .allocator = allocator,
            .schema = schema,
            .segment_id = segment_id,
            .schema_fingerprint = schema_fingerprint,
            .row_group_size = row_group_size,
            .dict_eligible = dict_eligible,
            .column_sketches = column_sketches,
            .encode_threads = @max(1, encode_threads),
            .buf = buf,
            .row_groups = .empty,
            .row_count = 0,
        };
    }

    /// Append one sorted row-group. `columns` is one view per schema column,
    /// each holding the same `rows` (≤ `row_group_size`) rows. Encodes every
    /// column block (raw / FOR / dict per `dict_eligible`) and records the
    /// row-group's stats + offsets — the same per-block path `writeSegment` runs.
    ///
    /// Column blocks encode in parallel across `encode_threads` workers (each
    /// block into its own buffer, concatenated in column order afterward — the
    /// output is byte-identical to the serial path regardless of thread count).
    /// Block compression (LZ4HC for large string blocks, zstd otherwise)
    /// dominates merge cost, and the per-column grain parallelizes it cleanly.
    pub fn writeRowGroup(self: *MergedSegmentWriter, columns: []const ColumnView) !void {
        if (columns.len != self.schema.columns.len) return format.Error.SchemaMismatch;
        const rows: usize = if (columns.len == 0) 0 else columns[0].rowCount();
        if (rows == 0) return;
        for (columns) |view| {
            if (view.rowCount() != rows) return format.Error.UnevenColumns;
        }
        if (rows > self.row_group_size) return format.Error.InvalidRowGroupSize;

        const blocks = try self.allocator.alloc(std.ArrayList(u8), columns.len);
        defer {
            for (blocks) |*b| b.deinit(self.allocator);
            self.allocator.free(blocks);
        }
        for (blocks) |*b| b.* = .empty;

        const stats = try self.allocator.alloc(format.Stats, columns.len);
        errdefer self.allocator.free(stats);

        var job = BlockEncodeJob{
            .allocator = self.allocator,
            .columns = columns,
            .schema = self.schema,
            .dict_eligible = self.dict_eligible,
            .row_start = 0,
            .row_end = rows,
            .blocks = blocks,
            .stats = stats,
        };
        try job.run(self.encode_threads);

        const rg_file_offset: u64 = @intCast(self.buf.items.len);

        try appendU32(self.allocator, &self.buf, @intCast(rows));
        try appendU32(self.allocator, &self.buf, 0); // padding

        const col_offsets = try self.allocator.alloc(u64, columns.len);
        errdefer self.allocator.free(col_offsets);
        var total: usize = 0;
        for (blocks) |b| total += b.items.len;
        try self.buf.ensureUnusedCapacity(self.allocator, total);
        for (blocks, 0..) |b, ci| {
            col_offsets[ci] = @intCast(self.buf.items.len);
            self.buf.appendSliceAssumeCapacity(b.items);
        }

        const rg_length: u32 = @intCast(self.buf.items.len - rg_file_offset);

        try self.row_groups.append(self.allocator, .{
            .offset = rg_file_offset,
            .length = rg_length,
            .row_count = @intCast(rows),
            .stats = stats,
            .col_offsets = col_offsets,
        });
        self.row_count += @intCast(rows);
    }

    /// Write the footer, patch the header's row_count, flush to disk, and return
    /// the `SegmentInfo` (which takes ownership of `column_sketches` and the
    /// row-group metadata). After a successful `finish` the writer's `buf` is
    /// released; do not call `deinit` (it becomes a no-op).
    pub fn finish(self: *MergedSegmentWriter, io: Io, dir: Io.Dir, file_name: []const u8, sync_on_close: bool) !SegmentInfo {
        format.writeU64(self.buf.items[24..32], self.row_count);

        const footer_start = self.buf.items.len;
        try appendU32(self.allocator, &self.buf, @intCast(self.row_groups.items.len));
        for (self.row_groups.items) |rg| {
            try appendU64(self.allocator, &self.buf, rg.offset);
            try appendU32(self.allocator, &self.buf, rg.length);
            try appendU32(self.allocator, &self.buf, rg.row_count);
            for (rg.stats) |s| {
                try appendI128(self.allocator, &self.buf, s.min);
                try appendI128(self.allocator, &self.buf, s.max);
                try appendI128(self.allocator, &self.buf, s.sum);
                try appendU64(self.allocator, &self.buf, s.null_count);
            }
            for (rg.col_offsets) |co| {
                try appendU64(self.allocator, &self.buf, co);
            }
        }
        const footer_size: u32 = @intCast(self.buf.items.len - footer_start + format.footer_trailer_size);
        try appendU32(self.allocator, &self.buf, footer_size);
        try self.buf.appendSlice(self.allocator, &format.segment_magic);

        const byte_size: u64 = @intCast(self.buf.items.len);
        try @import("storage.zig").writeFileSynced(io, dir, file_name, self.buf.items, sync_on_close);

        const info = SegmentInfo{
            .segment_id = self.segment_id,
            .row_count = self.row_count,
            .schema_fingerprint = self.schema_fingerprint,
            .byte_size = byte_size,
            .row_groups = try self.row_groups.toOwnedSlice(self.allocator),
            .column_sketches = self.column_sketches,
        };

        self.buf.deinit(self.allocator);
        self.* = undefined;
        return info;
    }

    /// Release everything the writer owns. Frees `column_sketches` too —
    /// ownership only leaves the writer on a successful `finish`. Safe to call
    /// on a fresh `begin`-ed writer that never reached `finish`.
    pub fn deinit(self: *MergedSegmentWriter) void {
        for (self.row_groups.items) |rg| {
            self.allocator.free(rg.stats);
            self.allocator.free(@constCast(rg.col_offsets));
        }
        self.row_groups.deinit(self.allocator);
        self.buf.deinit(self.allocator);
        if (self.column_sketches.len > 0) self.allocator.free(@constCast(self.column_sketches));
        self.* = undefined;
    }
};

/// One row-group's parallel block-encode: workers claim columns off an atomic
/// cursor and write column `ci`'s complete on-disk block (header + payload)
/// into `blocks[ci]`, plus its `stats[ci]`. Each worker owns a private zstd
/// context (the shared `Compressor` is not thread-safe); LZ4HC is stateless.
/// The expensive part per column is the compress call, so the column grain
/// load-balances well even when a few string columns dwarf the rest.
const BlockEncodeJob = struct {
    allocator: Allocator,
    columns: []const ColumnView,
    schema: TableSchema,
    dict_eligible: []const bool,
    row_start: usize,
    row_end: usize,
    blocks: []std.ArrayList(u8),
    stats: []format.Stats,

    next: std.atomic.Value(usize) = .init(0),
    failed: std.atomic.Value(bool) = .init(false),
    err_mutex: std.atomic.Mutex = .unlocked,
    err: ?anyerror = null,

    fn run(self: *BlockEncodeJob, threads: usize) !void {
        const n = @min(@max(1, threads), self.columns.len);
        if (n > 1) {
            const handles = self.allocator.alloc(std.Thread, n - 1) catch null;
            if (handles) |hs| {
                var spawned: usize = 0;
                for (hs) |*h| {
                    h.* = std.Thread.spawn(.{}, worker, .{self}) catch break;
                    spawned += 1;
                }
                worker(self);
                for (hs[0..spawned]) |h| h.join();
                self.allocator.free(hs);
            } else {
                worker(self);
            }
        } else {
            worker(self);
        }
        if (self.err) |e| return e;
    }

    fn worker(self: *BlockEncodeJob) void {
        var compressor = compression_mod.Compressor.init() catch |e| return self.fail(e);
        defer compressor.deinit();
        while (!self.failed.load(.acquire)) {
            const ci = self.next.fetchAdd(1, .monotonic);
            if (ci >= self.columns.len) return;
            const view = self.columns[ci];
            writeColumnBlock(
                self.allocator,
                &compressor,
                &self.blocks[ci],
                view,
                self.schema.columns[ci].nullable,
                self.dict_eligible[ci],
                self.schema.compression,
                self.row_start,
                self.row_end,
            ) catch |e| return self.fail(e);
            self.stats[ci] = computeStats(view, self.row_start, self.row_end);
        }
    }

    fn fail(self: *BlockEncodeJob, e: anyerror) void {
        while (!self.err_mutex.tryLock()) std.atomic.spinLoopHint();
        if (self.err == null) self.err = e;
        self.err_mutex.unlock();
        self.failed.store(true, .release);
    }
};

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

const appendU16 = format.appendU16;
const appendU32 = format.appendU32;
const appendI128 = format.appendI128;
const appendU64 = format.appendU64;

/// Build the encoded (uncompressed) column-block payload, then try zstd-compress
/// it via the shared `Compressor`. Keep whichever is smaller. Prepend the
/// on-disk header.
///
/// Encoding choice (raw vs Frame-of-Reference) is made once per block by
/// `tryEncodeFor`: an eligible integer-family fixed-width column whose value
/// range narrows is written FOR, everything else stays raw. The validity bitmap
/// (when the column is nullable) is laid out identically in both encodings — it
/// always prefixes the payload — so the reader's bitmap handling is shared.
fn writeColumnBlock(
    allocator: Allocator,
    compressor: *compression_mod.Compressor,
    buf: *std.ArrayList(u8),
    view: ColumnView,
    nullable: bool,
    dict_eligible: bool,
    table_compression: types.TableCompression,
    row_start: usize,
    row_end: usize,
) !void {
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);

    // Validity bitmap prefix. We emit it whenever the column is declared
    // nullable, even if no rows are actually null in this block — the reader
    // shape stays uniform per-column across row groups.
    const has_nulls = nullable;
    if (has_nulls) {
        try writeValidityBitmap(allocator, &scratch, view, row_start, row_end);
    }

    // Try a narrow encoding first; fall back to raw. Each `tryEncode*` appends
    // its body onto `scratch` (after the bitmap) and returns true iff it
    // committed; on false `scratch` is untouched past the bitmap. RLE commits
    // only when its run body beats BOTH raw and this block's FOR candidate, so
    // the chain order encodes "pick the smallest". Dict (strings) is mutually
    // exclusive with the int-family encoders by column type.
    const encoding: format.Encoding = if (try tryEncodeRle(allocator, &scratch, view, row_start, row_end))
        .rle
    else if (try tryEncodeFor(allocator, &scratch, view, row_start, row_end))
        .for_
    else if (dict_eligible and try tryEncodeDict(allocator, &scratch, view, row_start, row_end))
        .dict
    else if (table_compression == .lz4_fsst and
        try tryEncodeFsst(allocator, &scratch, view, row_start, row_end, lz4_string_min_block_bytes))
        .fsst
    else blk: {
        try writeRawColumnBlock(allocator, &scratch, view, row_start, row_end);
        break :blk .raw;
    };

    const raw_size: u32 = @intCast(scratch.items.len);

    // Block compression per the table's `compression` option (a kept block is
    // only compressed when that actually shrinks it). Under `.lz4`, large raw
    // string blocks are additionally flagged `at_rest`: the cache keeps them
    // compressed and pays a whole-block decompress per access (LZ4 decodes at
    // multi-GB/s), trading CPU for a much smaller resident set. Every other
    // block — any algorithm — decompresses once at cache fill.
    // Under `.lz4_fsst` an FSST block is the at-rest form itself: the cache
    // holds the FSST bytes (decompressed once at fill if the on-disk LZ4
    // wrapper below shrank them), never the raw expansion. Large raw string
    // blocks FSST declined (ratio gate) fall back to LZ4-at-rest as in `.lz4`.
    const want_at_rest = (table_compression == .lz4 or table_compression == .lz4_fsst) and
        encoding == .raw and
        isStringView(view) and scratch.items.len >= lz4_string_min_block_bytes;

    var compressed: ?[]u8 = null;
    defer if (compressed) |c| allocator.free(c);
    var kind: format.Compression = .none;
    switch (table_compression) {
        .none => {},
        .zstd => {
            compressed = try compressor.compress(allocator, scratch.items);
            kind = if (compressed.?.len < scratch.items.len) .zstd else .none;
        },
        .lz4, .lz4_fsst => {
            compressed = try compression_mod.lz4CompressHC(allocator, scratch.items);
            kind = if (compressed.?.len < scratch.items.len) .lz4 else .none;
        },
    }

    const use_compressed = kind != .none;
    const payload_size: u32 = if (use_compressed)
        @intCast(compressed.?.len)
    else
        raw_size;

    const flags = format.ColumnBlockFlags{
        .has_nulls = has_nulls,
        .at_rest = want_at_rest and kind == .lz4,
    };

    // Header: kind (u8) + flags (u8) + encoding (u8) + 1 reserved + uncompressed_size (u32) + compressed_size (u32)
    try buf.ensureUnusedCapacity(allocator, format.column_block_header_size + payload_size);
    buf.appendAssumeCapacity(@intFromEnum(kind));
    buf.appendAssumeCapacity(flags.toByte());
    buf.appendAssumeCapacity(@intFromEnum(encoding));
    buf.appendAssumeCapacity(0); // remaining reserved byte
    var b4: [4]u8 = undefined;
    format.writeU32(&b4, raw_size);
    buf.appendSliceAssumeCapacity(&b4);
    format.writeU32(&b4, payload_size);
    buf.appendSliceAssumeCapacity(&b4);

    if (use_compressed) {
        buf.appendSliceAssumeCapacity(compressed.?);
    } else {
        buf.appendSliceAssumeCapacity(scratch.items);
    }
}

/// Frame-of-Reference body, appended after the (already-written) validity
/// bitmap. Layout of the FOR portion of the decompressed payload:
///
///   [base: i128 LE (16 bytes)] [width: u8 (1|2|4|8)] [3 pad bytes]
///   [deltas: row_count × width, LE]
///
/// `base` is the minimum non-null value; each row stores the unsigned delta
/// `value - base` truncated to `width` bytes (a NULL row still occupies a delta
/// slot — its value is whatever placeholder the column array holds, masked off
/// by the validity bitmap, so the slot's contents are irrelevant). The 3 pad
/// bytes keep the deltas region from a width-misaligned start, mirroring the
/// raw-block alignment that the borrowed-view fast path relies on; FOR blocks
/// themselves bail out of the borrow path in 2A, but the padding keeps the
/// layout self-consistent and aligned for the 2B narrow accessor.
///
/// Returns false (leaving `scratch` unchanged past the bitmap) when the column
/// type is ineligible or the range can't narrow below the native width; the
/// caller then writes a raw body.
fn tryEncodeRle(
    allocator: Allocator,
    scratch: *std.ArrayList(u8),
    view: ColumnView,
    row_start: usize,
    row_end: usize,
) !bool {
    return switch (view.data) {
        .int, .date => |d| rleEncode(i32, allocator, scratch, view, d, row_start, row_end),
        .bigint, .datetime, .decimal64 => |d| rleEncode(i64, allocator, scratch, view, d, row_start, row_end),
        .smallint => |d| rleEncode(i16, allocator, scratch, view, d, row_start, row_end),
        .tinyint => |d| rleEncode(i8, allocator, scratch, view, d, row_start, row_end),
        .boolean => |d| rleEncode(u8, allocator, scratch, view, d, row_start, row_end),
        // Same eligibility set as FOR; variable-width and 16-byte types stay out.
        else => false,
    };
}

fn rleEncode(
    comptime T: type,
    allocator: Allocator,
    scratch: *std.ArrayList(u8),
    view: ColumnView,
    data: []const T,
    row_start: usize,
    row_end: usize,
) !bool {
    const native = @sizeOf(T);
    const n = row_end - row_start;
    if (n == 0) return false;

    // One pass: run count over the stored stream as-is (NULL placeholders
    // included — they must round-trip exactly), plus the non-null min/max the
    // FOR-candidate size needs. RLE only commits when it beats BOTH raw and
    // what FOR would produce for this block, so a non-runny block falls
    // through to `tryEncodeFor` unchanged.
    var n_runs: usize = 1;
    var prev = data[row_start];
    var lo: i128 = std.math.maxInt(i128);
    var hi: i128 = std.math.minInt(i128);
    var any = false;
    for (data[row_start..row_end], row_start..) |v, r| {
        if (v != prev) {
            n_runs += 1;
            prev = v;
        }
        if (view.isValid(r)) {
            const iv: i128 = @intCast(v);
            if (iv < lo) lo = iv;
            if (iv > hi) hi = iv;
            any = true;
        }
    }

    const rle_body_size = 4 + 1 + 3 + n_runs * (native + 4);
    if (rle_body_size >= n * native) return false;
    if (any) {
        if (forWidth(@intCast(hi - lo), native)) |w| {
            const for_body_size = 16 + 1 + 3 + n * @as(usize, w);
            if (rle_body_size >= for_body_size) return false;
        }
    }

    try scratch.ensureUnusedCapacity(allocator, rle_body_size);
    var b4: [4]u8 = undefined;
    std.mem.writeInt(u32, &b4, @intCast(n_runs), .little);
    scratch.appendSliceAssumeCapacity(&b4);
    scratch.appendSliceAssumeCapacity(&[_]u8{ native, 0, 0, 0 });

    // Pass 2: run values, native little-endian.
    var bv: [@sizeOf(T)]u8 = undefined;
    prev = data[row_start];
    std.mem.writeInt(T, &bv, prev, .little);
    scratch.appendSliceAssumeCapacity(&bv);
    for (data[row_start + 1 .. row_end]) |v| {
        if (v != prev) {
            prev = v;
            std.mem.writeInt(T, &bv, v, .little);
            scratch.appendSliceAssumeCapacity(&bv);
        }
    }

    // Pass 3: run lengths, u32 little-endian.
    prev = data[row_start];
    var run_len: u32 = 0;
    for (data[row_start..row_end]) |v| {
        if (v != prev) {
            std.mem.writeInt(u32, &b4, run_len, .little);
            scratch.appendSliceAssumeCapacity(&b4);
            prev = v;
            run_len = 1;
        } else {
            run_len += 1;
        }
    }
    std.mem.writeInt(u32, &b4, run_len, .little);
    scratch.appendSliceAssumeCapacity(&b4);

    return true;
}

fn tryEncodeFor(
    allocator: Allocator,
    scratch: *std.ArrayList(u8),
    view: ColumnView,
    row_start: usize,
    row_end: usize,
) !bool {
    return switch (view.data) {
        .int, .date => |d| forEncode(i32, allocator, scratch, view, d, row_start, row_end),
        .bigint, .datetime, .decimal64 => |d| forEncode(i64, allocator, scratch, view, d, row_start, row_end),
        .smallint => |d| forEncode(i16, allocator, scratch, view, d, row_start, row_end),
        .tinyint => |d| forEncode(i8, allocator, scratch, view, d, row_start, row_end),
        .boolean => |d| forEncode(u8, allocator, scratch, view, d, row_start, row_end),
        // largeint / decimal128 / uuid / float / double / strings are not FOR-eligible.
        else => false,
    };
}

/// Narrowest FOR delta width in bytes for a value span, or null when it can't
/// beat (be strictly smaller than) the native element width `native`.
fn forWidth(span: u128, native: usize) ?u8 {
    const width: u8 = if (span <= std.math.maxInt(u8))
        1
    else if (span <= std.math.maxInt(u16))
        2
    else if (span <= std.math.maxInt(u32))
        4
    else
        8;
    return if (width < native) width else null;
}

fn forEncode(
    comptime T: type,
    allocator: Allocator,
    scratch: *std.ArrayList(u8),
    view: ColumnView,
    data: []const T,
    row_start: usize,
    row_end: usize,
) !bool {
    const native = @sizeOf(T);
    // Native-1-byte types (tinyint, boolean) can never narrow further.
    if (native <= 1) return false;

    // Min/max over non-null values, in i128 to make `max - base` overflow-proof.
    var lo: i128 = std.math.maxInt(i128);
    var hi: i128 = std.math.minInt(i128);
    var any = false;
    for (data[row_start..row_end], row_start..) |v, r| {
        if (!view.isValid(r)) continue;
        const iv: i128 = @intCast(v);
        if (iv < lo) lo = iv;
        if (iv > hi) hi = iv;
        any = true;
    }
    // All-null block: no base to derive — stay raw (degenerate, rare).
    if (!any) return false;

    const span: u128 = @intCast(hi - lo);
    const width = forWidth(span, native) orelse return false;

    const n = row_end - row_start;
    const for_body_size = 16 + 1 + 3 + n * width;
    // Only commit to FOR when the encoded body is strictly smaller than the raw
    // body (n * native). Conservative: a marginal case stays raw.
    if (for_body_size >= n * native) return false;

    try scratch.ensureUnusedCapacity(allocator, for_body_size);
    var b16: [16]u8 = undefined;
    format.writeI128(&b16, lo);
    scratch.appendSliceAssumeCapacity(&b16);
    scratch.appendSliceAssumeCapacity(&[_]u8{ width, 0, 0, 0 });

    for (data[row_start..row_end], row_start..) |v, r| {
        // A NULL slot's placeholder value can sit outside [lo, hi]; clamp its
        // delta into [0, span] so it never under/overflows the narrow width.
        // The validity bitmap masks these rows on read, so the exact stored code
        // is irrelevant to correctness — clamping just keeps every code a real
        // in-range delta (what the 2B narrow accessor assumes).
        const iv: i128 = @intCast(v);
        const delta_i: i128 = if (!view.isValid(r) or iv < lo)
            0
        else if (iv > hi)
            @intCast(span)
        else
            iv - lo;
        const delta: u128 = @intCast(delta_i);
        var b8: [8]u8 = undefined;
        std.mem.writeInt(u64, &b8, @truncate(delta), .little);
        scratch.appendSliceAssumeCapacity(b8[0..width]);
    }
    return true;
}

/// Maximum distinct values a dict block may hold. The per-row code is then at
/// most `u16`-wide; a column whose block exceeds this stays raw (mid-build
/// abandon below avoids paying the full distinct scan for high-card columns).
const dict_max_ndv: usize = 65536;
/// Whole-segment / global cardinality cap for dict *eligibility* (distinct from
/// the per-block `dict_max_ndv` code-width cap). A string column is dict-encoded
/// only when BOTH this segment's NDV and the global NDV (merged HLL across all
/// coexisting segments) are at or below this. Above it the column stays raw.
const dict_max_global_ndv: u64 = 65536;
/// Skip dict encoding for blob-like columns: a high average value length means
/// the dict bytes dominate and per-row codes save little. Long-but-low-card
/// values (URLs) still qualify because the dict stores each long value once.
const dict_max_avg_len: usize = 256;

/// Segment-local string dictionary body, appended after the (already-written)
/// validity bitmap. Encodes a low-cardinality string block as the `k` distinct
/// values once (sorted) plus a narrow per-row code. Layout documented on
/// `format.Encoding.dict`. Returns false (leaving `scratch` untouched past the
/// bitmap) when the column is not a string, is blob-like, exceeds the NDV cap,
/// is too high-cardinality to pay off, or doesn't shrink the body — the caller
/// then writes a raw string block.
fn tryEncodeDict(
    allocator: Allocator,
    scratch: *std.ArrayList(u8),
    view: ColumnView,
    row_start: usize,
    row_end: usize,
) !bool {
    const sv: StringView = switch (view.data) {
        .varchar, .string, .char => |s| s,
        else => return false,
    };

    const n = row_end - row_start;
    if (n == 0) return false;

    // Avg-len gate (cheap, upfront): total value bytes / rows. Uses the raw
    // offset span, which includes any null-row bytes — a conservative bound.
    const total_bytes: usize = sv.offsets[row_end] - sv.offsets[row_start];
    if (total_bytes / n > dict_max_avg_len) return false;

    // Distinct map (insertion order) over non-null rows, with mid-build abandon.
    var map: std.StringHashMapUnmanaged(u32) = .empty;
    defer map.deinit(allocator);
    var distinct: std.ArrayList([]const u8) = .empty; // slices alias sv.bytes
    defer distinct.deinit(allocator);

    const row_codes = try allocator.alloc(u32, n); // insertion code per row
    defer allocator.free(row_codes);

    var r = row_start;
    while (r < row_end) : (r += 1) {
        const i = r - row_start;
        if (!view.isValid(r)) {
            row_codes[i] = 0; // placeholder; masked by validity on read
            continue;
        }
        const s = sv.rowBytes(r);
        const gop = try map.getOrPut(allocator, s);
        if (!gop.found_existing) {
            if (map.count() > dict_max_ndv) return false; // abandon: high-card
            gop.value_ptr.* = @intCast(distinct.items.len);
            try distinct.append(allocator, s);
        }
        row_codes[i] = gop.value_ptr.*;
    }

    const k = distinct.items.len;
    if (k == 0) return false; // all-null block: stay raw (degenerate)
    // NDV ≤ n/2: with more distinct values the codes + dict approach the raw
    // body, so the encoding can't pay for its dict header.
    if (k * 2 > n) return false;

    // Sort distinct indices by value bytes; build insertion→sorted remap.
    const order = try allocator.alloc(u32, k);
    defer allocator.free(order);
    for (order, 0..) |*o, idx| o.* = @intCast(idx);
    const SortCtx = struct {
        items: []const []const u8,
        fn lessThan(ctx: @This(), a: u32, b: u32) bool {
            return std.mem.lessThan(u8, ctx.items[a], ctx.items[b]);
        }
    };
    std.sort.pdq(u32, order, SortCtx{ .items = distinct.items }, SortCtx.lessThan);
    const ins_to_sorted = try allocator.alloc(u32, k);
    defer allocator.free(ins_to_sorted);
    for (order, 0..) |ins_idx, p| ins_to_sorted[ins_idx] = @intCast(p);

    var dict_byte_count: usize = 0;
    for (distinct.items) |s| dict_byte_count += s.len;

    const code_width: u8 = if (k <= 256) 1 else if (k <= 65536) 2 else 4;
    const dict_body = 4 + 4 + (k + 1) * 4 + dict_byte_count + n * code_width;
    // Raw string body, per `writeStringBlock`: [u32 byte_count][(n+1) u32][bytes].
    const raw_body = 4 + (n + 1) * 4 + total_bytes;
    if (dict_body >= raw_body) return false;

    try scratch.ensureUnusedCapacity(allocator, dict_body);
    var b4: [4]u8 = undefined;

    format.writeU32(&b4, @intCast(k));
    scratch.appendSliceAssumeCapacity(&b4);
    scratch.appendSliceAssumeCapacity(&[_]u8{ code_width, 0, 0, 0 });

    // dict_offsets in sorted order, rebased to 0 (k+1 entries).
    format.writeU32(&b4, 0);
    scratch.appendSliceAssumeCapacity(&b4);
    var acc: u32 = 0;
    for (order) |ins_idx| {
        acc += @intCast(distinct.items[ins_idx].len);
        format.writeU32(&b4, acc);
        scratch.appendSliceAssumeCapacity(&b4);
    }

    // dict_bytes in sorted order.
    for (order) |ins_idx| scratch.appendSliceAssumeCapacity(distinct.items[ins_idx]);

    // codes: per-row sorted-dict index.
    for (row_codes) |ic| {
        const code = ins_to_sorted[ic];
        var b8: [8]u8 = undefined;
        std.mem.writeInt(u64, &b8, code, .little);
        scratch.appendSliceAssumeCapacity(b8[0..code_width]);
    }
    return true;
}

/// Raw string blocks at least this large compress with LZ4HC and stay
/// compressed in the block cache. Below it, the per-access decompress isn't
/// worth the bookkeeping and zstd's ratio wins.
const lz4_string_min_block_bytes: usize = 64 * 1024;

/// True when the view is a string-family column (the LZ4 cache policy is
/// strings-only: that's where the resident bytes live).
fn isStringView(view: ColumnView) bool {
    return switch (view.data) {
        .varchar, .string, .char => true,
        else => false,
    };
}

/// Minimum average value length. Below this the per-row offset overhead
/// dominates and symbols rarely cover enough to pay.
const fsst_min_avg_len: usize = 4;
/// Sampling budget for the symbol-table build: spread across the block,
/// capped in both string count and total bytes.
const fsst_sample_max_strings: usize = 1024;
const fsst_sample_max_bytes: usize = 16 * 1024;
/// Commit only when the FSST body is at most 7/8 of the raw body.
const fsst_accept_num: usize = 7;
const fsst_accept_den: usize = 8;

/// FSST body for a high-NDV string block, appended after the (already-
/// written) validity bitmap. Layout documented on `format.Encoding.fsst`.
/// Returns false (leaving `scratch` untouched past the bitmap) when the
/// column is not a string, the block is too small/short-valued, or the
/// encoding doesn't clear the acceptance ratio — the caller then writes a
/// raw string block. Runs after the dict attempt, so it sees exactly the
/// blocks dict declined (high NDV / blob-like / poor savings).
fn tryEncodeFsst(
    allocator: Allocator,
    scratch: *std.ArrayList(u8),
    view: ColumnView,
    row_start: usize,
    row_end: usize,
    /// Minimum raw string bytes before FSST is attempted. Callers pass the
    /// same threshold that gates LZ4-at-rest — FSST replaces exactly that
    /// tier; smaller blocks can't amortize the symbol table anyway.
    min_total_bytes: usize,
) !bool {
    const sv: StringView = switch (view.data) {
        .varchar, .string, .char => |s| s,
        else => return false,
    };
    const n = row_end - row_start;
    if (n == 0) return false;

    const total_bytes: usize = sv.offsets[row_end] - sv.offsets[row_start];
    if (total_bytes < min_total_bytes) return false;
    if (total_bytes / n < fsst_min_avg_len) return false;

    // Sample rows spread across the block (stride keeps the sample
    // representative when values cluster).
    var sample: std.ArrayListUnmanaged([]const u8) = .empty;
    defer sample.deinit(allocator);
    const stride = @max(@as(usize, 1), n / fsst_sample_max_strings);
    var sampled_bytes: usize = 0;
    var r = row_start;
    while (r < row_end and sampled_bytes < fsst_sample_max_bytes) : (r += stride) {
        const s = sv.rowBytes(r);
        if (s.len == 0) continue;
        try sample.append(allocator, s);
        sampled_bytes += s.len;
    }
    if (sample.items.len == 0) return false;

    const table = try fsst.buildTable(allocator, sample.items);

    // Encode every row, building the per-row offsets as we go.
    var comp: std.ArrayListUnmanaged(u8) = .empty;
    defer comp.deinit(allocator);
    const offsets = try allocator.alloc(u32, n + 1);
    defer allocator.free(offsets);
    offsets[0] = 0;
    r = row_start;
    while (r < row_end) : (r += 1) {
        const s = sv.rowBytes(r);
        try comp.ensureUnusedCapacity(allocator, fsst.encodedSizeBound(s.len));
        table.encodeAppend(s, &comp);
        offsets[r - row_start + 1] = @intCast(comp.items.len);
    }

    var table_buf: [fsst.max_serialized_size]u8 = undefined;
    const table_len = table.serialize(&table_buf);

    const fsst_body = 4 + table_len + 4 + 4 + (n + 1) * 4 + comp.items.len;
    // Raw string body, per `writeStringBlock`: [u32 byte_count][(n+1) u32][bytes].
    const raw_body = 4 + (n + 1) * 4 + total_bytes;
    if (fsst_body * fsst_accept_den > raw_body * fsst_accept_num) return false;

    try scratch.ensureUnusedCapacity(allocator, fsst_body);
    var b4: [4]u8 = undefined;
    format.writeU32(&b4, @intCast(table_len));
    scratch.appendSliceAssumeCapacity(&b4);
    scratch.appendSliceAssumeCapacity(table_buf[0..table_len]);
    format.writeU32(&b4, @intCast(total_bytes));
    scratch.appendSliceAssumeCapacity(&b4);
    format.writeU32(&b4, @intCast(comp.items.len));
    scratch.appendSliceAssumeCapacity(&b4);
    for (offsets) |off| {
        format.writeU32(&b4, off);
        scratch.appendSliceAssumeCapacity(&b4);
    }
    scratch.appendSliceAssumeCapacity(comp.items);
    return true;
}

fn writeValidityBitmap(
    allocator: Allocator,
    buf: *std.ArrayList(u8),
    view: ColumnView,
    row_start: usize,
    row_end: usize,
) !void {
    const n = row_end - row_start;
    const byte_len = column.bitmapBytes(n);
    try buf.ensureUnusedCapacity(allocator, byte_len);

    // Emit zero-initialized bitmap, then flip valid bits on.
    const start = buf.items.len;
    buf.appendNTimesAssumeCapacity(0, byte_len);
    const slice = buf.items[start .. start + byte_len];
    for (0..n) |i| {
        const valid = column.isValidBit(view.nulls, row_start + i);
        if (valid) column.setValidBit(slice, i, true);
    }
}

fn writeRawColumnBlock(
    allocator: Allocator,
    buf: *std.ArrayList(u8),
    view: ColumnView,
    row_start: usize,
    row_end: usize,
) !void {
    switch (view.data) {
        .int => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 4);
            for (slice) |v| {
                var b: [4]u8 = undefined;
                format.writeI32(&b, v);
                buf.appendSliceAssumeCapacity(&b);
            }
        },
        .bigint => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 8);
            for (slice) |v| {
                var b: [8]u8 = undefined;
                format.writeI64(&b, v);
                buf.appendSliceAssumeCapacity(&b);
            }
        },
        .boolean => |data| {
            try buf.appendSlice(allocator, data[row_start..row_end]);
        },
        .varchar => |sv| try writeStringBlock(allocator, buf, sv, row_start, row_end),
        .string => |sv| try writeStringBlock(allocator, buf, sv, row_start, row_end),
        .float => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 4);
            for (slice) |v| {
                var b: [4]u8 = undefined;
                format.writeF32(&b, v);
                buf.appendSliceAssumeCapacity(&b);
            }
        },
        .double => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 8);
            for (slice) |v| {
                var b: [8]u8 = undefined;
                format.writeF64(&b, v);
                buf.appendSliceAssumeCapacity(&b);
            }
        },
        .date => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 4);
            for (slice) |v| {
                var b: [4]u8 = undefined;
                format.writeI32(&b, v);
                buf.appendSliceAssumeCapacity(&b);
            }
        },
        .datetime => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 8);
            for (slice) |v| {
                var b: [8]u8 = undefined;
                format.writeI64(&b, v);
                buf.appendSliceAssumeCapacity(&b);
            }
        },
        .tinyint => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len);
            for (slice) |v| buf.appendAssumeCapacity(@bitCast(v));
        },
        .smallint => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 2);
            for (slice) |v| {
                var b: [2]u8 = undefined;
                std.mem.writeInt(i16, &b, v, .little);
                buf.appendSliceAssumeCapacity(&b);
            }
        },
        .largeint => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 16);
            for (slice) |v| {
                var b: [16]u8 = undefined;
                std.mem.writeInt(i128, &b, v, .little);
                buf.appendSliceAssumeCapacity(&b);
            }
        },
        .char => |sv| try writeStringBlock(allocator, buf, sv, row_start, row_end),
        .decimal64 => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 8);
            for (slice) |v| {
                var b: [8]u8 = undefined;
                format.writeI64(&b, v);
                buf.appendSliceAssumeCapacity(&b);
            }
        },
        .decimal128 => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 16);
            for (slice) |v| {
                var b: [16]u8 = undefined;
                std.mem.writeInt(i128, &b, v, .little);
                buf.appendSliceAssumeCapacity(&b);
            }
        },
        .uuid => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 16);
            for (slice) |v| {
                var b: [16]u8 = undefined;
                std.mem.writeInt(u128, &b, v, .little);
                buf.appendSliceAssumeCapacity(&b);
            }
        },
    }
}

fn computeStats(view: ColumnView, row_start: usize, row_end: usize) format.Stats {
    return switch (view.data) {
        .int, .date => |d| extentInt(i32, view, d, row_start, row_end),
        .bigint, .datetime, .decimal64 => |d| extentInt(i64, view, d, row_start, row_end),
        .boolean => |d| extentInt(u8, view, d, row_start, row_end),
        .tinyint => |d| extentInt(i8, view, d, row_start, row_end),
        .smallint => |d| extentInt(i16, view, d, row_start, row_end),
        .largeint, .decimal128 => |d| extentInt(i128, view, d, row_start, row_end),
        .uuid => |d| blk: {
            // u128 → i128 via top-bit XOR so signed compare preserves unsigned
            // ordering. See `format.encodeUnsignedU128`.
            var lo: i128 = std.math.maxInt(i128);
            var hi: i128 = std.math.minInt(i128);
            var nulls: u64 = 0;
            for (d[row_start..row_end], row_start..) |v, r| {
                if (!view.isValid(r)) {
                    nulls += 1;
                    continue;
                }
                const enc = format.encodeUnsignedU128(v);
                if (enc < lo) lo = enc;
                if (enc > hi) hi = enc;
            }
            break :blk .{ .min = lo, .max = hi, .null_count = nulls };
        },
        // Strings store the prefix-encoded i128 of the first 16 bytes of each
        // row's value. See `format.encodeStringPrefix`.
        .varchar, .string, .char => |sv| computeStringStats(view, sv, row_start, row_end),
        // Floats: order-preserving bit transform, NaN skipped. See
        // `format.encodeFloatOrder`.
        .float => |d| extentFloat(f32, view, d, row_start, row_end),
        .double => |d| extentFloat(f64, view, d, row_start, row_end),
    };
}

/// Min/max over the non-null values of a float column, encoded to i128 via
/// `format.encodeFloatOrder` (NaN sorts last, so a NaN row raises `max` to the
/// NaN sentinel — never wrongly pruned under the NaN-last total order). NULL
/// rows are skipped; an all-null range leaves the inverted `{maxInt, minInt}`
/// sentinel that consumers detect via `min > max`.
fn extentFloat(comptime T: type, view: ColumnView, data: []const T, row_start: usize, row_end: usize) format.Stats {
    var lo: i128 = std.math.maxInt(i128);
    var hi: i128 = std.math.minInt(i128);
    var sum: f64 = 0;
    var nulls: u64 = 0;
    for (data[row_start..row_end], row_start..) |v, r| {
        if (!view.isValid(r)) {
            nulls += 1;
            continue;
        }
        const enc = format.encodeFloatOrder(@as(f64, v));
        if (enc < lo) lo = enc;
        if (enc > hi) hi = enc;
        sum += @as(f64, v);
    }
    return .{
        .min = lo,
        .max = hi,
        .sum = format.sumSlotFromF64(sum),
        .null_count = nulls,
    };
}

/// Min/max over the *non-null* values of an integer-family column, encoded to
/// `i128`. NULL rows are skipped: the column's per-type data array still holds
/// a placeholder at null slots, so including them would pollute the extreme
/// (e.g. a 0 at a null slot becoming a spurious MIN). An all-null range leaves
/// `min = maxInt(T), max = minInt(T)` — an inverted "no values" sentinel that
/// composes correctly under min/max folding and that consumers detect via
/// `min > max`. Non-nullable columns have `view.nulls == null`, so `isValid`
/// is always true and behaviour is unchanged.
fn extentInt(comptime T: type, view: ColumnView, data: []const T, row_start: usize, row_end: usize) format.Stats {
    var lo: T = std.math.maxInt(T);
    var hi: T = std.math.minInt(T);
    // i128 columns (largeint/decimal128) carry no sum — it could overflow the
    // i128 slot. Every narrower type sums exactly (≤2^32 rows × i64 < 2^96).
    const with_sum = @bitSizeOf(T) < 128;
    var sum: i128 = 0;
    var nulls: u64 = 0;
    for (data[row_start..row_end], row_start..) |v, r| {
        if (!view.isValid(r)) {
            nulls += 1;
            continue;
        }
        if (v < lo) lo = v;
        if (v > hi) hi = v;
        if (with_sum) sum += v;
    }
    return .{ .min = @intCast(lo), .max = @intCast(hi), .sum = sum, .null_count = nulls };
}

fn computeStringStats(view: ColumnView, sv: StringView, row_start: usize, row_end: usize) format.Stats {
    var lo: i128 = std.math.maxInt(i128);
    var hi: i128 = std.math.minInt(i128);
    // The string `sum` slot holds the blank-excluded min prefix: '' is the
    // global minimum of nearly every string column, so the plain min is
    // useless for ORDER BY pruning. `maxInt` sentinel when no non-empty value.
    var lo_nonblank: i128 = std.math.maxInt(i128);
    var nulls: u64 = 0;
    var i = row_start;
    while (i < row_end) : (i += 1) {
        if (!view.isValid(i)) {
            nulls += 1;
            continue;
        }
        const bytes = sv.rowBytes(i);
        const enc = format.encodeStringPrefix(bytes);
        if (enc < lo) lo = enc;
        if (enc > hi) hi = enc;
        if (bytes.len > 0 and enc < lo_nonblank) lo_nonblank = enc;
    }
    return .{ .min = lo, .max = hi, .sum = lo_nonblank, .null_count = nulls };
}

fn writeStringBlock(
    allocator: Allocator,
    buf: *std.ArrayList(u8),
    sv: StringView,
    row_start: usize,
    row_end: usize,
) !void {
    const n = row_end - row_start;
    const byte_start = sv.offsets[row_start];
    const byte_end = sv.offsets[row_end];
    const byte_count = byte_end - byte_start;

    const total = 4 + (n + 1) * 4 + byte_count;
    try buf.ensureUnusedCapacity(allocator, total);

    var b4: [4]u8 = undefined;

    // byte_count
    format.writeU32(&b4, byte_count);
    buf.appendSliceAssumeCapacity(&b4);

    // offsets (rebased to 0)
    for (sv.offsets[row_start .. row_end + 1]) |off| {
        format.writeU32(&b4, off - byte_start);
        buf.appendSliceAssumeCapacity(&b4);
    }

    // bytes
    buf.appendSliceAssumeCapacity(sv.bytes[byte_start..byte_end]);
}
