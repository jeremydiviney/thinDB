//! Writes a segment file from a set of ColumnViews, streaming its blocks to
//! disk as they're encoded.
//!
//! Single entry point: `writeSegment`. Caller owns the returned SegmentInfo
//! (call `.deinit(allocator)`).

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const TableSchema = types.TableSchema;

const format = @import("format.zig");
const column = @import("column.zig");
const compression_mod = @import("compression.zig");
const fsst = @import("fsst.zig");
const int_streams = @import("int_streams.zig");
const segment_strings = @import("segment_strings.zig");
const table_strings = @import("table_strings.zig");
const ngram_index = @import("ngram_index.zig");
const hll = @import("../util/hll.zig");
const ColumnView = column.ColumnView;
const StringView = column.StringView;
const Streams = std.ArrayList(int_streams.Stream);
const RowGroupMeta = format.RowGroupMeta;
const SegmentInfo = format.SegmentInfo;

/// Fixed hash for NULL cells, so all NULLs count as one distinct value
/// (matching GROUP BY treating NULL as a single group).
const null_hash: u64 = 0x9E3779B97F4A7C15;

/// A segment file written as its bytes are produced, so a writer holds the
/// pieces it's still building, never the whole segment. Small pieces gather
/// in `pending` into larger writes. A segment counts only once the manifest
/// names it, so a partial file is never read.
const SegmentFile = struct {
    io: Io,
    file: Io.File,
    /// Bytes already in the file.
    written: u64 = 0,
    /// Bytes whose writeback has started.
    written_back: u64 = 0,
    pending: std.ArrayList(u8) = .empty,

    const pending_limit: usize = 4 << 20;
    /// Bytes written between writeback starts.
    const writeback_step: u64 = 16 << 20;

    fn create(io: Io, dir: Io.Dir, file_name: []const u8) !SegmentFile {
        return .{ .io = io, .file = try dir.createFile(io, file_name, .{}) };
    }

    /// The file offset the next appended byte lands at.
    fn offset(self: *const SegmentFile) u64 {
        return self.written + self.pending.items.len;
    }

    fn append(self: *SegmentFile, allocator: Allocator, bytes: []const u8) !void {
        if (self.pending.items.len + bytes.len > pending_limit) try self.drain();
        if (bytes.len >= pending_limit) {
            try self.file.writePositionalAll(self.io, bytes, self.written);
            self.wrote(bytes.len);
            return;
        }
        try self.pending.appendSlice(allocator, bytes);
    }

    fn drain(self: *SegmentFile) !void {
        if (self.pending.items.len == 0) return;
        try self.file.writePositionalAll(self.io, self.pending.items, self.written);
        self.wrote(self.pending.items.len);
        self.pending.clearRetainingCapacity();
    }

    /// Counts `n` bytes written, starting the writeback of each
    /// `writeback_step` that lands. A segment is fsynced at close, and the
    /// kernel would otherwise keep its pages dirty until then, so the fsync
    /// would write the whole file while the writer waits; started as the
    /// file grows, the writeback leaves the fsync only the tail.
    fn wrote(self: *SegmentFile, n: usize) void {
        self.written += n;
        if (comptime builtin.os.tag != .linux or @sizeOf(usize) != 8 or !@hasField(std.os.linux.SYS, "sync_file_range")) return;
        if (self.written - self.written_back < writeback_step) return;
        const sync_file_range_write = 2;
        _ = std.os.linux.syscall4(.sync_file_range, @intCast(self.file.handle), self.written_back, self.written - self.written_back, sync_file_range_write);
        self.written_back = self.written;
    }

    fn close(self: *SegmentFile, allocator: Allocator) void {
        self.pending.deinit(allocator);
        self.file.close(self.io);
    }
};

fn headerBytes(schema_fingerprint: u64, segment_id: u64, row_count: u64) [format.header_size]u8 {
    var h: [format.header_size]u8 = undefined;
    @memcpy(h[0..4], &format.segment_magic);
    format.writeU16(h[4..6], format.segment_version);
    format.writeU16(h[6..8], 0); // flags
    format.writeU64(h[8..16], schema_fingerprint);
    format.writeU64(h[16..24], segment_id);
    format.writeU64(h[row_count_at..][0..8], row_count);
    return h;
}

/// The header's row count, which `MergedSegmentWriter` writes last.
const row_count_at: usize = 24;

fn hashCell(view: ColumnView, row: usize) u64 {
    switch (view.data) {
        .varchar, .string, .char, .json, .nested, .bitmap => |sv| return std.hash.Wyhash.hash(0, sv.rowBytes(row)),
        // Full-value 64-bit hash (not a truncated prefix) so two distinct
        // values never collapse — under-counting would wrongly make a
        // high-cardinality field look hashable.
        inline else => |s| return std.hash.Wyhash.hash(0, std.mem.asBytes(&s[row])),
    }
}

/// Per-column HyperLogLog sketches over all rows, concatenated (column
/// `ci` at `[ci*hll.m .. (ci+1)*hll.m]`). Caller owns the returned slice.
pub fn computeSketches(allocator: Allocator, columns: []const ColumnView, row_count: usize) ![]u8 {
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
    sync_on_close: bool,
    /// Worker threads for per-column block encode+compress (same grain as
    /// `MergedSegmentWriter`). 1 = serial; output is byte-identical either way.
    encode_threads: usize,
) !SegmentInfo {
    return writeSegmentWithStrings(allocator, io, dir, file_name, schema, segment_id, schema_fingerprint, row_group_size, columns, sync_on_close, encode_threads, .{});
}

/// How a segment's string columns take segment string tables.
pub const StringOptions = struct {
    /// Distinct values a column's table may hold (0: none, every string row
    /// group takes the per-row-group encodings).
    value_cap: u32 = segment_strings.segment_value_cap,
    /// The table's string ids, for each table to carry (DESIGN.md §4.6).
    table_ids: ?table_strings.Target = null,
};

pub fn writeSegmentWithStrings(
    allocator: Allocator,
    io: Io,
    dir: Io.Dir,
    file_name: []const u8,
    schema: TableSchema,
    segment_id: u64,
    schema_fingerprint: u64,
    row_group_size: usize,
    columns: []const ColumnView,
    sync_on_close: bool,
    encode_threads: usize,
    string_options: StringOptions,
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

    const column_sketches = try computeSketches(allocator, columns, row_count);
    errdefer allocator.free(column_sketches);

    var out = try SegmentFile.create(io, dir, file_name);
    errdefer dir.deleteFile(io, file_name) catch {};
    defer out.close(allocator);
    try out.append(allocator, &headerBytes(schema_fingerprint, segment_id, row_count));

    // ---- Row groups ----
    var row_groups: std.ArrayList(RowGroupMeta) = .empty;
    errdefer {
        for (row_groups.items) |rg| freeRowGroupMeta(allocator, rg);
        row_groups.deinit(allocator);
    }
    var strings = try segment_strings.SegmentStrings.init(allocator, schema, string_options.value_cap);
    defer strings.deinit();

    var row_offset: usize = 0;
    while (row_offset < row_count) {
        const rows_in_group: usize = @min(row_group_size, row_count - row_offset);
        try row_groups.ensureUnusedCapacity(allocator, 1);
        row_groups.appendAssumeCapacity(try encodeRowGroup(
            allocator,
            &out,
            columns,
            schema,
            row_offset,
            row_offset + rows_in_group,
            strings.columns,
            @intCast(row_groups.items.len),
            encode_threads,
        ));
        row_offset += rows_in_group;
    }

    var tables: std.ArrayList(format.StringTable) = .empty;
    errdefer {
        for (tables.items) |t| freeStringTable(allocator, t);
        tables.deinit(allocator);
    }
    try writeStringTables(allocator, &out, &strings, row_groups.items, schema.compression, encode_threads, string_options.table_ids, &tables);
    const string_tables = try tables.toOwnedSlice(allocator);
    errdefer format.freeStringTables(allocator, string_tables);
    try writeFooter(allocator, &out, row_groups.items, string_tables);
    try out.drain();
    if (sync_on_close) try out.file.sync(io);
    const byte_size = out.offset();

    return SegmentInfo{
        .segment_id = segment_id,
        .row_count = row_count,
        .schema_fingerprint = schema_fingerprint,
        .byte_size = byte_size,
        .row_groups = try row_groups.toOwnedSlice(allocator),
        .column_sketches = column_sketches,
        .string_tables = string_tables,
    };
}

fn freeStringTable(allocator: Allocator, t: format.StringTable) void {
    allocator.free(t.pages);
    allocator.free(t.blocks);
}

fn freeRowGroupMeta(allocator: Allocator, rg: RowGroupMeta) void {
    allocator.free(rg.stats);
    allocator.free(rg.col_offsets);
    allocator.free(rg.col_lengths);
}

/// Encodes rows `row_start..row_end` of `columns` as one row group appended
/// to `out`: every column block in parallel (each into its own buffer), then
/// appended in column order — byte-identical to the serial path regardless
/// of thread count.
fn encodeRowGroup(
    allocator: Allocator,
    out: *SegmentFile,
    columns: []const ColumnView,
    schema: TableSchema,
    row_start: usize,
    row_end: usize,
    strings: []?segment_strings.ColumnStrings,
    rg_idx: u32,
    encode_threads: usize,
) !RowGroupMeta {
    const blocks = try allocator.alloc(ColumnBlock, columns.len);
    defer {
        for (blocks) |*b| b.deinit(allocator);
        allocator.free(blocks);
    }
    @memset(blocks, .{});

    var job = BlockEncodeJob{
        .allocator = allocator,
        .columns = columns,
        .schema = schema,
        .row_start = row_start,
        .row_end = row_end,
        .blocks = blocks,
        .strings = strings,
        .rg_idx = rg_idx,
    };
    try job.run(encode_threads);
    return appendRowGroupBlocks(allocator, out, blocks, row_end - row_start);
}

/// One column of one row group, encoded apart from the others: its block,
/// or, for a column with a string table, its rows readied for the table.
pub const ColumnBlock = struct {
    bytes: std.ArrayList(u8) = .empty,
    stats: format.Stats = undefined,
    strings: ?segment_strings.Prepared = null,

    pub fn deinit(self: *ColumnBlock, allocator: Allocator) void {
        self.bytes.deinit(allocator);
        if (self.strings) |*p| p.deinit(allocator);
        self.* = undefined;
    }
};

/// Encodes rows `row_start..row_end` of column `ci` into `out`: its block,
/// or, when `to_table` and the rows hold a string, the rows readied for the
/// column's string table, which `commitBlock` hands them to.
fn encodeBlock(
    allocator: Allocator,
    compressor: *compression_mod.Compressor,
    schema: TableSchema,
    ci: usize,
    to_table: bool,
    view: ColumnView,
    row_start: usize,
    row_end: usize,
    fsst_threads: usize,
    out: *ColumnBlock,
) !void {
    const nullable = schema.columns[ci].nullable;
    out.stats = computeStats(view, row_start, row_end);
    if (to_table) {
        var bitmap: std.ArrayList(u8) = .empty;
        defer bitmap.deinit(allocator);
        if (nullable) try writeValidityBitmap(allocator, &bitmap, view, row_start, row_end);
        out.strings = try segment_strings.prepare(allocator, view, row_start, row_end, bitmap.items, !nullable);
        if (out.strings != null) return;
    }
    try writeColumnBlock(allocator, compressor, &out.bytes, view, nullable, schema.compression, row_start, row_end, fsst_threads);
}

/// Hands `block`'s readied rows to column `ci`'s string table `cs` as row
/// group `rg_idx`, or, once the table can take no more values, encodes them
/// as a block of their own. Per column, row groups commit in order.
fn commitBlock(
    allocator: Allocator,
    compressor: *compression_mod.Compressor,
    schema: TableSchema,
    ci: usize,
    cs: *segment_strings.ColumnStrings,
    view: ColumnView,
    row_start: usize,
    row_end: usize,
    rg_idx: u32,
    fsst_threads: usize,
    block: *ColumnBlock,
) !void {
    var p = block.strings orelse return;
    block.strings = null;
    const taken = cs.commit(&p, rg_idx) catch |e| {
        p.deinit(allocator);
        return e;
    };
    if (taken) return;
    p.deinit(allocator);
    try writeColumnBlock(allocator, compressor, &block.bytes, view, schema.columns[ci].nullable, schema.compression, row_start, row_end, fsst_threads);
}

/// Appends one row group of `rows` rows to `out`: its header, then each
/// column's block in column order. A column whose string table took its
/// rows has no block here; `writeStringTable` sets its offset and length.
fn appendRowGroupBlocks(allocator: Allocator, out: *SegmentFile, blocks: []const ColumnBlock, rows: usize) !RowGroupMeta {
    const stats = try allocator.alloc(format.Stats, blocks.len);
    errdefer allocator.free(stats);
    for (stats, blocks) |*s, b| s.* = b.stats;

    const rg_file_offset = out.offset();
    var rg_header: [8]u8 = undefined;
    format.writeU32(rg_header[0..4], @intCast(rows));
    format.writeU32(rg_header[4..8], 0); // padding
    try out.append(allocator, &rg_header);

    // Each column block's own offset and length let the reader pread one
    // column without loading the whole segment.
    const col_offsets = try allocator.alloc(u64, blocks.len);
    errdefer allocator.free(col_offsets);
    const col_lengths = try allocator.alloc(u32, blocks.len);
    errdefer allocator.free(col_lengths);
    for (blocks, 0..) |b, ci| {
        col_offsets[ci] = out.offset();
        col_lengths[ci] = @intCast(b.bytes.items.len);
        try out.append(allocator, b.bytes.items);
    }

    return .{
        .offset = rg_file_offset,
        .length = @intCast(out.offset() - rg_file_offset),
        .row_count = @intCast(rows),
        .stats = stats,
        .col_offsets = col_offsets,
        .col_lengths = col_lengths,
    };
}

/// Writes each string column's segment string table still in `strings` to
/// `out` — its shared-string pages, then the row-group blocks it took,
/// grouped by column — onto `tables`, and points those row groups' column
/// offsets and lengths at the blocks. Frees each column's builder once
/// written.
fn writeStringTables(
    allocator: Allocator,
    out: *SegmentFile,
    strings: *segment_strings.SegmentStrings,
    row_groups: []RowGroupMeta,
    table_compression: types.TableCompression,
    threads: usize,
    table_ids: ?table_strings.Target,
    tables: *std.ArrayList(format.StringTable),
) !void {
    var compressor = try compression_mod.Compressor.init();
    defer compressor.deinit();
    var page_index: std.ArrayList(u8) = .empty;
    defer page_index.deinit(allocator);
    var page_bytes: std.ArrayList(u8) = .empty;
    defer page_bytes.deinit(allocator);
    var page_block: std.ArrayList(u8) = .empty;
    defer page_block.deinit(allocator);

    for (strings.columns, 0..) |*slot, ci| {
        const cs = if (slot.*) |*c| c else continue;
        defer {
            cs.deinit();
            slot.* = null;
        }
        if (cs.blocks.items.len == 0) continue;
        try cs.assignIds();

        const pages = try allocator.alloc(format.SharedPage, segment_strings.pageCount(cs.sharedCount()));
        errdefer allocator.free(pages);
        for (pages, 0..) |*page, p| {
            page_index.clearRetainingCapacity();
            page_bytes.clearRetainingCapacity();
            page_block.clearRetainingCapacity();
            const lengths = try encodeSharedPage(allocator, &compressor, cs, @intCast(p), table_compression, &page_index, &page_bytes, &page_block);
            page.* = .{ .offset = out.offset(), .index_length = lengths.index, .bytes_length = lengths.bytes };
            try out.append(allocator, page_block.items);
        }
        const blocks = try allocator.alloc(format.StringBlockParts, row_groups.len);
        errdefer allocator.free(blocks);
        @memset(blocks, .{});

        // Encoded a few blocks per thread at a time, so a column's encoded
        // blocks never all sit in memory at once.
        const encoded = try allocator.alloc(std.ArrayList(u8), @min(@max(1, threads) * 4, cs.blocks.items.len));
        defer {
            for (encoded) |*e| e.deinit(allocator);
            allocator.free(encoded);
        }
        for (encoded) |*e| e.* = .empty;
        const parts = try allocator.alloc(format.StringBlockParts, encoded.len);
        defer allocator.free(parts);
        var first: usize = 0;
        while (first < cs.blocks.items.len) {
            const chunk = cs.blocks.items[first..@min(first + encoded.len, cs.blocks.items.len)];
            for (encoded) |*e| e.clearRetainingCapacity();
            var job = StringBlockJob{ .allocator = allocator, .strings = cs, .blocks = chunk, .table_compression = table_compression, .encoded = encoded, .parts = parts };
            try encodeParallel(allocator, threads, chunk.len, &job);
            for (chunk, encoded[0..chunk.len], parts[0..chunk.len]) |b, e, p| {
                const rg = row_groups[b.rg_idx];
                @constCast(rg.col_offsets)[ci] = out.offset();
                @constCast(rg.col_lengths)[ci] = @intCast(e.items.len);
                blocks[b.rg_idx] = p;
                try out.append(allocator, e.items);
            }
            first += chunk.len;
        }

        var ids_block: format.TableIdsBlock = .{};
        if (table_ids) |target| if (try resolveTableIds(allocator, target, cs, ci)) |ids| {
            defer allocator.free(ids);
            page_block.clearRetainingCapacity();
            try encodeTableIds(allocator, &compressor, ids, table_compression, &page_block);
            ids_block = .{ .offset = out.offset(), .length = @intCast(page_block.items.len) };
            try out.append(allocator, page_block.items);
        };
        var ngram_range: format.FileRange = .{ .offset = 0, .length = 0 };
        page_block.clearRetainingCapacity();
        if (try encodeNgramIndex(allocator, &compressor, cs, threads, table_compression, &page_block)) {
            ngram_range = .{ .offset = out.offset(), .length = @intCast(page_block.items.len) };
            try out.append(allocator, page_block.items);
        }

        try tables.ensureUnusedCapacity(allocator, 1);
        tables.appendAssumeCapacity(.{
            .column_idx = @intCast(ci),
            .shared_count = cs.sharedCount(),
            .value_count = cs.valueCount(),
            .value_bytes = cs.valueBytes(),
            .pages = pages,
            .blocks = blocks,
            .table_ids = ids_block,
            .ngram_index = ngram_range,
        });
    }
}

/// Encodes shared-string page `p` of `cs` onto `out` as its two blocks,
/// its count and offsets then its bytes, building their payloads in
/// `index` and `bytes`, and returns their lengths.
fn encodeSharedPage(
    allocator: Allocator,
    compressor: *compression_mod.Compressor,
    cs: *const segment_strings.ColumnStrings,
    p: u32,
    table_compression: types.TableCompression,
    index: *std.ArrayList(u8),
    bytes: *std.ArrayList(u8),
    out: *std.ArrayList(u8),
) !struct { index: u32, bytes: u32 } {
    const offsets = try cs.pagePayload(allocator, p, index, bytes);
    const start = out.items.len;
    try appendBlock(allocator, compressor, out, index.items, &.{offsets}, @backingInt(format.Encoding.raw), false, table_compression);
    const index_length: u32 = @intCast(out.items.len - start);
    try appendBlock(allocator, compressor, out, bytes.items, &.{}, @backingInt(format.Encoding.raw), false, table_compression);
    return .{ .index = index_length, .bytes = @intCast(out.items.len - start - index_length) };
}

/// Encodes string-table block `b` of `cs` onto `out`, after `assignIds`,
/// as its parts, and returns their lengths.
fn encodeStringBlock(
    allocator: Allocator,
    compressor: *compression_mod.Compressor,
    cs: *const segment_strings.ColumnStrings,
    b: segment_strings.ColumnStrings.Block,
    table_compression: types.TableCompression,
    out: *std.ArrayList(u8),
) !format.StringBlockParts {
    var payloads: segment_strings.PartPayloads = .initFill(.{});
    defer for (&payloads.values) |*p| p.deinit(allocator);
    try cs.blockPayload(allocator, b, &payloads);
    var parts: format.StringBlockParts = .{};
    for (std.enums.values(format.StringBlockPart)) |part| {
        const payload = payloads.getPtr(part);
        if (payload.bytes.items.len == 0) continue;
        const start = out.items.len;
        const has_nulls = part == .index and b.bitmap.len > 0;
        try appendBlock(allocator, compressor, out, payload.bytes.items, payload.streams.items, format.segment_dict_encoding, has_nulls, table_compression);
        parts.lengths.set(part, @intCast(out.items.len - start));
    }
    return parts;
}

/// A string table's values in segment-id order, for
/// `TableStrings.resolve`.
const SegmentValues = struct {
    strings: *const segment_strings.ColumnStrings,
    /// The value of each segment id.
    by_id: []const u32,

    pub fn len(self: SegmentValues) usize {
        return self.by_id.len;
    }

    pub fn get(self: SegmentValues, i: usize) []const u8 {
        return self.strings.value(self.by_id[i]);
    }
};

/// The table id of each of `cs`'s segment ids, after `assignIds`; null
/// when column `ci` takes none. Caller owns the result.
fn resolveTableIds(allocator: Allocator, target: table_strings.Target, cs: *const segment_strings.ColumnStrings, ci: usize) !?[]u32 {
    const by_id = try allocator.alloc(u32, cs.valueCount());
    defer allocator.free(by_id);
    for (cs.ids, 0..) |id, v| by_id[id] = @intCast(v);
    const ids = try allocator.alloc(u32, by_id.len);
    errdefer allocator.free(ids);
    const values: SegmentValues = .{ .strings = cs, .by_id = by_id };
    if (!try target.strings.resolve(target.io, target.table_dir, target.threads, ci, values, ids)) {
        allocator.free(ids);
        return null;
    }
    return ids;
}

/// Encodes `ids` as a table-ids block (`format.TableIdsBlock`) onto `out`.
fn encodeTableIds(
    allocator: Allocator,
    compressor: *compression_mod.Compressor,
    ids: []const u32,
    table_compression: types.TableCompression,
    out: *std.ArrayList(u8),
) !void {
    const chunk_ids: usize = @min(ids.len, format.table_ids_chunk);
    const payload = try allocator.alloc(u8, chunk_ids * 4);
    defer allocator.free(payload);
    var lo: usize = 0;
    while (true) {
        const chunk = ids[lo..@min(lo + format.table_ids_chunk, ids.len)];
        for (chunk, 0..) |id, i| format.writeU32(payload[i * 4 ..][0..4], id);
        try appendBlock(allocator, compressor, out, payload[0 .. chunk.len * 4], &.{.{ .offset = 0, .count = @intCast(chunk.len), .width = 4 }}, @backingInt(format.Encoding.raw), false, table_compression);
        lo += chunk.len;
        if (lo >= ids.len) break;
    }
}

/// Encodes column `cs`'s n-gram index (DESIGN.md §4.7) onto `out`, its row
/// groups' grams gathered on up to `threads` threads; false, adding
/// nothing, when the column takes none.
fn encodeNgramIndex(
    allocator: Allocator,
    compressor: *compression_mod.Compressor,
    cs: *const segment_strings.ColumnStrings,
    threads: usize,
    table_compression: types.TableCompression,
    out: *std.ArrayList(u8),
) !bool {
    if (!ngram_index.qualifies(cs)) return false;
    var build = try ngram_index.Build.init(allocator, cs);
    defer build.deinit();
    var job: NgramGatherJob = .{ .build = &build, .ranges = @min(@max(1, threads) * 4, cs.blocks.items.len) };
    try encodeParallel(allocator, threads, job.ranges, &job);
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    const streams = (try build.assemble(&payload)) orelse return false;
    try appendBlock(allocator, compressor, out, payload.items, &streams, @backingInt(format.Encoding.raw), false, table_compression);
    return true;
}

/// Gathers an n-gram index's grams, one range of its blocks per call.
const NgramGatherJob = struct {
    build: *ngram_index.Build,
    ranges: usize,

    fn encode(self: *NgramGatherJob, _: *compression_mod.Compressor, i: usize) !void {
        const n = self.build.grams.len;
        try self.build.gather(n * i / self.ranges, n * (i + 1) / self.ranges);
    }
};

/// One column's string table, laid out apart from its segment file in
/// pieces that encode in parallel: its shared-string pages, then its
/// row-group blocks, then its table ids if it carries them, then its
/// n-gram index if it qualifies for one.
/// `MergedSegmentWriter.appendStringTable` places it.
pub const StringTableBuild = struct {
    strings: segment_strings.ColumnStrings,
    column_idx: u32,
    page_count: usize,
    /// The table id of each segment id; null when the table carries none.
    table_ids: ?[]u32,
    /// Whether the last piece is the table's n-gram index, which stays
    /// empty when the index comes to hold nothing.
    ngram: bool,
    /// Each piece's encoded bytes, pages first.
    pieces: []std.ArrayList(u8),
    /// Each piece's part lengths; a page's are its index and bytes.
    parts: []format.StringBlockParts,

    pub fn deinit(self: *StringTableBuild, allocator: Allocator) void {
        for (self.pieces) |*p| p.deinit(allocator);
        allocator.free(self.pieces);
        allocator.free(self.parts);
        if (self.table_ids) |ids| allocator.free(ids);
        self.strings.deinit();
        self.* = undefined;
    }

    fn blockCount(self: *const StringTableBuild) usize {
        return self.strings.blocks.items.len;
    }
};

/// Encodes some of one string column's deferred row-group blocks, each into
/// its own buffer.
const StringBlockJob = struct {
    allocator: Allocator,
    strings: *const segment_strings.ColumnStrings,
    blocks: []const segment_strings.ColumnStrings.Block,
    table_compression: types.TableCompression,
    encoded: []std.ArrayList(u8),
    parts: []format.StringBlockParts,

    fn encode(self: *StringBlockJob, compressor: *compression_mod.Compressor, i: usize) !void {
        self.parts[i] = try encodeStringBlock(self.allocator, compressor, self.strings, self.blocks[i], self.table_compression, &self.encoded[i]);
    }
};

fn writeFooter(
    allocator: Allocator,
    out: *SegmentFile,
    row_groups: []const RowGroupMeta,
    string_tables: []const format.StringTable,
) !void {
    var footer: std.ArrayList(u8) = .empty;
    defer footer.deinit(allocator);
    try appendFooter(allocator, &footer, row_groups, string_tables);
    try out.append(allocator, footer.items);
}

fn appendFooter(
    allocator: Allocator,
    buf: *std.ArrayList(u8),
    row_groups: []const RowGroupMeta,
    string_tables: []const format.StringTable,
) !void {
    const footer_start = buf.items.len;
    try appendU32(allocator, buf, @intCast(row_groups.len));
    for (row_groups) |rg| {
        try appendU64(allocator, buf, rg.offset);
        try appendU32(allocator, buf, rg.length);
        try appendU32(allocator, buf, rg.row_count);
        for (rg.stats) |s| {
            try appendI128(allocator, buf, s.min);
            try appendI128(allocator, buf, s.max);
            try appendI128(allocator, buf, s.sum);
            try appendU64(allocator, buf, s.null_count);
        }
        for (rg.col_offsets) |co| try appendU64(allocator, buf, co);
        for (rg.col_lengths) |cl| try appendU32(allocator, buf, cl);
    }
    try appendU32(allocator, buf, @intCast(string_tables.len));
    for (string_tables) |t| {
        try appendU32(allocator, buf, t.column_idx);
        try appendU32(allocator, buf, t.shared_count);
        try appendU32(allocator, buf, t.value_count);
        try appendU32(allocator, buf, @intCast(t.pages.len));
        try appendU64(allocator, buf, t.value_bytes);
        for (t.pages) |p| {
            try appendU64(allocator, buf, p.offset);
            try appendU32(allocator, buf, p.index_length);
            try appendU32(allocator, buf, p.bytes_length);
        }
        for (t.blocks) |b| for (b.lengths.values) |len| try appendU32(allocator, buf, len);
        try appendU64(allocator, buf, t.table_ids.offset);
        try appendU32(allocator, buf, t.table_ids.length);
        try appendU64(allocator, buf, t.ngram_index.offset);
        try appendU32(allocator, buf, t.ngram_index.length);
    }
    const footer_size: u32 = @intCast(buf.items.len - footer_start + format.footer_trailer_size);
    try appendU32(allocator, buf, footer_size);
    try buf.appendSlice(allocator, &format.segment_magic);
}

/// Streaming segment writer for compaction's k-way merge. Unlike `writeSegment`,
/// which needs every column fully materialized, this accepts sorted row-groups
/// one at a time so the merge holds at most ~(k input row-groups + one output
/// row-group) in memory rather than the whole output, plus each string
/// column's distinct values and deferred row-group blocks for its segment
/// string table. Row groups stream to the output file as they encode.
///
/// `column_sketches` is precomputed by the caller (the union of the input
/// segments' sketches — an acceptable sketch for the merged output); ownership
/// transfers to the returned `SegmentInfo` at `finish`.
///
/// Usage: `begin`, then `writeRowGroup` per sorted row-group (≤ `row_group_size`
/// rows), then `finish`. On any error before `finish`, call `deinit`, which
/// also deletes the partial file.
///
/// A caller with its own workers can instead take `writeRowGroup` and the
/// string tables apart: `encodeColumn` per column of each row group, on any
/// thread and in any order; `commitColumn`, in row-group order per column;
/// `appendRowGroup`, in row-group order. Once a column's row groups have all
/// committed, `beginStringTable` takes its table, `encodeStringTablePiece`
/// encodes the table's pieces in parallel, and `appendStringTable` places it
/// after every row group, in column order, before `finish`. Or, column by
/// column: `beginRowGroups` starts every row group, `appendColumnBlock`
/// appends each column's blocks, and `appendStringTable` places a table
/// once its column's blocks are appended.
pub const MergedSegmentWriter = struct {
    allocator: Allocator,
    schema: TableSchema,
    segment_id: u64,
    schema_fingerprint: u64,
    row_group_size: usize,
    column_sketches: []const u8,
    /// Worker threads for per-column block encode+compress inside
    /// `writeRowGroup`. 1 = serial (encode on the calling thread).
    encode_threads: usize,

    dir: Io.Dir,
    file_name: []const u8,
    out: SegmentFile,
    row_groups: std.ArrayList(RowGroupMeta),
    row_count: u64,
    strings: segment_strings.SegmentStrings,
    /// String tables `appendStringTable` placed.
    string_tables: std.ArrayList(format.StringTable),
    table_ids: ?table_strings.Target = null,

    pub fn begin(
        allocator: Allocator,
        io: Io,
        dir: Io.Dir,
        file_name: []const u8,
        schema: TableSchema,
        segment_id: u64,
        schema_fingerprint: u64,
        row_group_size: usize,
        /// Per-column HLL bytes for the merged output (same layout as
        /// `SegmentInfo.column_sketches`). Ownership transfers to `finish`'s
        /// `SegmentInfo`; freed by `deinit` if the writer is torn down first.
        column_sketches: []const u8,
        encode_threads: usize,
    ) !MergedSegmentWriter {
        if (row_group_size == 0) return format.Error.InvalidRowGroupSize;

        const name = try allocator.dupe(u8, file_name);
        errdefer allocator.free(name);
        var strings = try segment_strings.SegmentStrings.init(allocator, schema, segment_strings.segment_value_cap);
        errdefer strings.deinit();
        var out = try SegmentFile.create(io, dir, name);
        errdefer {
            out.close(allocator);
            dir.deleteFile(io, name) catch {};
        }
        // The row count is unknown until `finish`, which writes it in place.
        try out.append(allocator, &headerBytes(schema_fingerprint, segment_id, 0));

        return .{
            .allocator = allocator,
            .schema = schema,
            .segment_id = segment_id,
            .schema_fingerprint = schema_fingerprint,
            .row_group_size = row_group_size,
            .column_sketches = column_sketches,
            .encode_threads = @max(1, encode_threads),
            .dir = dir,
            .file_name = name,
            .out = out,
            .row_groups = .empty,
            .row_count = 0,
            .strings = strings,
            .string_tables = .empty,
        };
    }

    /// Append one sorted row-group. `columns` is one view per schema column,
    /// each holding the same `rows` (≤ `row_group_size`) rows. Encodes every
    /// column block (raw / FOR / RLE / dict / FSST) and records the
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

        const rg = try encodeRowGroup(
            self.allocator,
            &self.out,
            columns,
            self.schema,
            0,
            rows,
            self.strings.columns,
            @intCast(self.row_groups.items.len),
            self.encode_threads,
        );
        self.row_groups.append(self.allocator, rg) catch |e| {
            freeRowGroupMeta(self.allocator, rg);
            return e;
        };
        self.row_count += @intCast(rows);
    }

    /// Has the writer's string tables carry the table's string ids
    /// (DESIGN.md §4.6).
    pub fn useTableIds(self: *MergedSegmentWriter, target: table_strings.Target) void {
        self.table_ids = target;
    }

    /// Encodes column `ci` of a row group into `out`, apart from the writer's
    /// other work. A column with a string table comes back readied for
    /// `commitColumn`, and `view` must outlive that commit.
    pub fn encodeColumn(self: *const MergedSegmentWriter, compressor: *compression_mod.Compressor, ci: usize, view: ColumnView, out: *ColumnBlock) !void {
        try encodeBlock(self.allocator, compressor, self.schema, ci, self.strings.columns[ci] != null, view, 0, view.rowCount(), 1, out);
    }

    /// Hands column `ci`'s readied rows to its string table as row group
    /// `rg_idx`, the index `appendRowGroup` will give it; a no-op for a block
    /// already encoded. Per column, row groups commit in order; columns
    /// commit in parallel.
    pub fn commitColumn(self: *MergedSegmentWriter, compressor: *compression_mod.Compressor, ci: usize, rg_idx: u32, view: ColumnView, block: *ColumnBlock) !void {
        const cs = if (self.strings.columns[ci]) |*c| c else return;
        try commitBlock(self.allocator, compressor, self.schema, ci, cs, view, 0, view.rowCount(), rg_idx, 1, block);
    }

    /// Appends the next row group of `rows` rows from its column blocks,
    /// each encoded and committed.
    pub fn appendRowGroup(self: *MergedSegmentWriter, blocks: []const ColumnBlock, rows: usize) !void {
        if (blocks.len != self.schema.columns.len) return format.Error.SchemaMismatch;
        if (rows == 0 or rows > self.row_group_size) return format.Error.InvalidRowGroupSize;
        try self.row_groups.ensureUnusedCapacity(self.allocator, 1);
        self.row_groups.appendAssumeCapacity(try appendRowGroupBlocks(self.allocator, &self.out, blocks, rows));
        self.row_count += @intCast(rows);
    }

    /// Starts every row group of a segment of `rows` rows at once, writing
    /// their headers, for `appendColumnBlock` to append their blocks in
    /// place of `appendRowGroup`.
    pub fn beginRowGroups(self: *MergedSegmentWriter, rows: usize) !void {
        if (rows == 0 or self.row_groups.items.len > 0) return format.Error.InvalidRowGroupSize;
        const columns = self.schema.columns.len;
        const count = (rows + self.row_group_size - 1) / self.row_group_size;
        try self.row_groups.ensureTotalCapacity(self.allocator, count);
        for (0..count) |r| {
            const rg_rows = @min(self.row_group_size, rows - r * self.row_group_size);
            const stats = try self.allocator.alloc(format.Stats, columns);
            errdefer self.allocator.free(stats);
            @memset(stats, std.mem.zeroes(format.Stats));
            const col_offsets = try self.allocator.alloc(u64, columns);
            errdefer self.allocator.free(col_offsets);
            @memset(col_offsets, 0);
            const col_lengths = try self.allocator.alloc(u32, columns);
            @memset(col_lengths, 0);
            var header: [8]u8 = undefined;
            format.writeU32(header[0..4], @intCast(rg_rows));
            format.writeU32(header[4..8], 0);
            self.row_groups.appendAssumeCapacity(.{
                .offset = self.out.offset(),
                .length = header.len,
                .row_count = @intCast(rg_rows),
                .stats = stats,
                .col_offsets = col_offsets,
                .col_lengths = col_lengths,
            });
            try self.out.append(self.allocator, &header);
        }
        self.row_count = rows;
    }

    /// Appends row group `rg`'s block of column `ci`, encoded and
    /// committed, after `beginRowGroups`. A block a string table took is
    /// empty until `appendStringTable` places the table.
    pub fn appendColumnBlock(self: *MergedSegmentWriter, rg: usize, ci: usize, block: *const ColumnBlock) !void {
        const meta = &self.row_groups.items[rg];
        @constCast(meta.stats)[ci] = block.stats;
        @constCast(meta.col_offsets)[ci] = self.out.offset();
        @constCast(meta.col_lengths)[ci] = @intCast(block.bytes.items.len);
        meta.length += @intCast(block.bytes.items.len);
        try self.out.append(self.allocator, block.bytes.items);
    }

    /// Starts column `ci`'s string table, once its row groups have all
    /// committed, taking over the column's builder; null when the column has
    /// none. Its pieces then encode in parallel with `encodeStringTablePiece`.
    pub fn beginStringTable(self: *MergedSegmentWriter, ci: usize) !?StringTableBuild {
        const slot = &self.strings.columns[ci];
        var cs = slot.* orelse return null;
        slot.* = null;
        errdefer cs.deinit();
        if (cs.blocks.items.len == 0) {
            cs.deinit();
            return null;
        }
        try cs.assignIds();
        const table_ids = if (self.table_ids) |target| try resolveTableIds(self.allocator, target, &cs, ci) else null;
        errdefer if (table_ids) |ids| self.allocator.free(ids);
        const page_count = segment_strings.pageCount(cs.sharedCount());
        const ngram = ngram_index.qualifies(&cs);
        const pieces = try self.allocator.alloc(std.ArrayList(u8), page_count + cs.blocks.items.len + @intFromBool(table_ids != null) + @intFromBool(ngram));
        errdefer self.allocator.free(pieces);
        @memset(pieces, .empty);
        const parts = try self.allocator.alloc(format.StringBlockParts, pieces.len);
        @memset(parts, .{});
        return .{ .strings = cs, .column_idx = @intCast(ci), .page_count = page_count, .table_ids = table_ids, .ngram = ngram, .pieces = pieces, .parts = parts };
    }

    /// Encodes piece `i` of `build` into its own buffer, apart from the
    /// writer's other work and the build's other pieces.
    pub fn encodeStringTablePiece(self: *const MergedSegmentWriter, compressor: *compression_mod.Compressor, build: *StringTableBuild, i: usize) !void {
        const cs = &build.strings;
        if (build.ngram and i == build.pieces.len - 1) {
            _ = try encodeNgramIndex(self.allocator, compressor, cs, 1, self.schema.compression, &build.pieces[i]);
            return;
        }
        if (i == build.page_count + build.blockCount()) {
            try encodeTableIds(self.allocator, compressor, build.table_ids.?, self.schema.compression, &build.pieces[i]);
            return;
        }
        if (i >= build.page_count) {
            build.parts[i] = try encodeStringBlock(self.allocator, compressor, cs, cs.blocks.items[i - build.page_count], self.schema.compression, &build.pieces[i]);
            return;
        }
        var index: std.ArrayList(u8) = .empty;
        defer index.deinit(self.allocator);
        var bytes: std.ArrayList(u8) = .empty;
        defer bytes.deinit(self.allocator);
        const lengths = try encodeSharedPage(self.allocator, compressor, cs, @intCast(i), self.schema.compression, &index, &bytes, &build.pieces[i]);
        build.parts[i].lengths.set(.index, lengths.index);
        build.parts[i].lengths.set(.bytes, lengths.bytes);
    }

    /// Places `build`'s string table at the file's end, pointing its row
    /// groups' column offsets and lengths at its blocks, and frees the build.
    /// Tables place in column order: after every row group, or after their
    /// own column's blocks once the row groups began together.
    pub fn appendStringTable(self: *MergedSegmentWriter, build: *StringTableBuild) !void {
        defer build.deinit(self.allocator);
        const cs = &build.strings;
        try self.string_tables.ensureUnusedCapacity(self.allocator, 1);
        const pages = try self.allocator.alloc(format.SharedPage, build.page_count);
        errdefer self.allocator.free(pages);
        const blocks = try self.allocator.alloc(format.StringBlockParts, self.row_groups.items.len);
        errdefer self.allocator.free(blocks);
        @memset(blocks, .{});
        for (pages, build.pieces[0..build.page_count], build.parts[0..build.page_count]) |*page, piece, parts| {
            page.* = .{ .offset = self.out.offset(), .index_length = parts.lengths.get(.index), .bytes_length = parts.lengths.get(.bytes) };
            try self.out.append(self.allocator, piece.items);
        }
        const blocks_end = build.page_count + build.blockCount();
        for (cs.blocks.items, build.pieces[build.page_count..blocks_end], build.parts[build.page_count..blocks_end]) |b, piece, parts| {
            const rg = self.row_groups.items[b.rg_idx];
            @constCast(rg.col_offsets)[build.column_idx] = self.out.offset();
            @constCast(rg.col_lengths)[build.column_idx] = @intCast(piece.items.len);
            blocks[b.rg_idx] = parts;
            try self.out.append(self.allocator, piece.items);
        }
        var ids_block: format.TableIdsBlock = .{};
        if (build.table_ids != null) {
            const piece = build.pieces[blocks_end].items;
            ids_block = .{ .offset = self.out.offset(), .length = @intCast(piece.len) };
            try self.out.append(self.allocator, piece);
        }
        var ngram_range: format.FileRange = .{ .offset = 0, .length = 0 };
        if (build.ngram) {
            const piece = build.pieces[build.pieces.len - 1].items;
            ngram_range = .{ .offset = self.out.offset(), .length = @intCast(piece.len) };
            try self.out.append(self.allocator, piece);
        }
        self.string_tables.appendAssumeCapacity(.{
            .column_idx = build.column_idx,
            .shared_count = cs.sharedCount(),
            .value_count = cs.valueCount(),
            .value_bytes = cs.valueBytes(),
            .pages = pages,
            .blocks = blocks,
            .table_ids = ids_block,
            .ngram_index = ngram_range,
        });
    }

    /// Write the string tables and footer, fill in the header's row count,
    /// close the file, and return the `SegmentInfo` (which takes ownership of
    /// `column_sketches` and the row-group metadata). After a successful
    /// `finish` the writer is spent; do not call `deinit`. After a failed one,
    /// call `deinit`.
    pub fn finish(self: *MergedSegmentWriter, sync_on_close: bool) !SegmentInfo {
        try writeStringTables(self.allocator, &self.out, &self.strings, self.row_groups.items, self.schema.compression, self.encode_threads, self.table_ids, &self.string_tables);
        const string_tables = try self.string_tables.toOwnedSlice(self.allocator);
        errdefer format.freeStringTables(self.allocator, string_tables);
        try writeFooter(self.allocator, &self.out, self.row_groups.items, string_tables);
        try self.out.drain();
        var row_count: [8]u8 = undefined;
        format.writeU64(&row_count, self.row_count);
        try self.out.file.writePositionalAll(self.out.io, &row_count, row_count_at);
        if (sync_on_close) try self.out.file.sync(self.out.io);

        const info = SegmentInfo{
            .segment_id = self.segment_id,
            .row_count = self.row_count,
            .schema_fingerprint = self.schema_fingerprint,
            .byte_size = self.out.offset(),
            .row_groups = try self.row_groups.toOwnedSlice(self.allocator),
            .column_sketches = self.column_sketches,
            .string_tables = string_tables,
        };

        self.out.close(self.allocator);
        self.allocator.free(self.file_name);
        self.strings.deinit();
        self.* = undefined;
        return info;
    }

    /// Release everything the writer owns and delete the partial file. Frees
    /// `column_sketches` too — ownership only leaves the writer on a
    /// successful `finish`.
    pub fn deinit(self: *MergedSegmentWriter) void {
        for (self.row_groups.items) |rg| freeRowGroupMeta(self.allocator, rg);
        self.row_groups.deinit(self.allocator);
        self.strings.deinit();
        for (self.string_tables.items) |t| freeStringTable(self.allocator, t);
        self.string_tables.deinit(self.allocator);
        if (self.column_sketches.len > 0) self.allocator.free(@constCast(self.column_sketches));
        const io = self.out.io;
        self.out.close(self.allocator);
        self.dir.deleteFile(io, self.file_name) catch {};
        self.allocator.free(self.file_name);
        self.* = undefined;
    }
};

/// One row-group's block encode: column `ci`'s complete on-disk block
/// (header + payload) and stats into `blocks[ci]` — or, for a string column
/// its segment string table takes, no block. The expensive part per column
/// is the compress call, so the column grain load-balances well even when a
/// few string columns dwarf the rest.
const BlockEncodeJob = struct {
    allocator: Allocator,
    columns: []const ColumnView,
    schema: TableSchema,
    row_start: usize,
    row_end: usize,
    blocks: []ColumnBlock,
    strings: []?segment_strings.ColumnStrings,
    rg_idx: u32,
    fsst_threads: usize = 1,

    fn run(self: *BlockEncodeJob, threads: usize) !void {
        // FSST row-encode lanes nest under the per-column workers: only a
        // handful of heavy string columns FSST-encode per row group, so the
        // column grain alone strands cores once the cheap columns finish.
        // threads/4 lanes per column keeps total concurrency ≈ threads while
        // those stragglers run.
        self.fsst_threads = @max(1, threads / 4);
        try encodeParallel(self.allocator, threads, self.columns.len, self);
    }

    fn encode(self: *BlockEncodeJob, compressor: *compression_mod.Compressor, ci: usize) !void {
        const view = self.columns[ci];
        const cs: ?*segment_strings.ColumnStrings = if (self.strings[ci]) |*c| c else null;
        const to_table = if (cs) |c| !c.full else false;
        try encodeBlock(self.allocator, compressor, self.schema, ci, to_table, view, self.row_start, self.row_end, self.fsst_threads, &self.blocks[ci]);
        if (cs) |c| try commitBlock(self.allocator, compressor, self.schema, ci, c, view, self.row_start, self.row_end, self.rg_idx, self.fsst_threads, &self.blocks[ci]);
    }
};

/// Runs `job.encode(compressor, i)` for every `i < count` across up to
/// `threads` workers (the calling thread is one), claiming indices off an
/// atomic cursor. Each worker owns a private zstd context (the shared
/// `Compressor` is not thread-safe); LZ4HC is stateless. The first error
/// stops further claims and is returned.
fn encodeParallel(allocator: Allocator, threads: usize, count: usize, job: anytype) !void {
    const State = struct {
        next: std.atomic.Value(usize) = .init(0),
        failed: std.atomic.Value(bool) = .init(false),
        err_mutex: std.atomic.Mutex = .unlocked,
        err: ?anyerror = null,

        fn worker(state: *@This(), j: @TypeOf(job), total: usize) void {
            var compressor = compression_mod.Compressor.init() catch |e| return state.fail(e);
            defer compressor.deinit();
            while (!state.failed.load(.acquire)) {
                const i = state.next.fetchAdd(1, .monotonic);
                if (i >= total) return;
                j.encode(&compressor, i) catch |e| return state.fail(e);
            }
        }

        fn fail(state: *@This(), e: anyerror) void {
            while (!state.err_mutex.tryLock()) std.atomic.spinLoopHint();
            if (state.err == null) state.err = e;
            state.err_mutex.unlock();
            state.failed.store(true, .release);
        }
    };
    var state: State = .{};
    const n = @min(@max(1, threads), count);
    const handles = if (n > 1) allocator.alloc(std.Thread, n - 1) catch null else null;
    if (handles) |hs| {
        var spawned: usize = 0;
        for (hs) |*h| {
            h.* = std.Thread.spawn(.{}, State.worker, .{ &state, job, count }) catch break;
            spawned += 1;
        }
        State.worker(&state, job, count);
        for (hs[0..spawned]) |h| h.join();
        allocator.free(hs);
    } else {
        State.worker(&state, job, count);
    }
    if (state.err) |e| return e;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

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
    table_compression: types.TableCompression,
    row_start: usize,
    row_end: usize,
    /// Sub-worker lanes for the FSST row encode (the one per-block encoder
    /// expensive enough to parallelize WITHIN a column). 1 = serial.
    fsst_threads: usize,
) !void {
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var streams: Streams = .empty;
    defer streams.deinit(allocator);

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
    const encoding: format.Encoding = if (try tryEncodeRle(allocator, &scratch, &streams, view, row_start, row_end))
        .rle
    else if (try tryEncodeFor(allocator, &scratch, &streams, view, row_start, row_end))
        .for_
    else if (try tryEncodeDict(allocator, &scratch, &streams, view, row_start, row_end, !nullable))
        .dict
    else if (table_compression == .lz4_fsst and
        try tryEncodeFsst(allocator, &scratch, &streams, view, row_start, row_end, fsst_min_block_bytes, fsst_threads))
        .fsst
    else blk: {
        try writeRawColumnBlock(allocator, &scratch, &streams, view, row_start, row_end);
        break :blk .raw;
    };

    try appendBlock(allocator, compressor, buf, scratch.items, streams.items, @backingInt(encoding), has_nulls, table_compression);
}

fn addStream(allocator: Allocator, streams: *Streams, offset: usize, count: usize, width: u8, signed: bool) !void {
    try streams.append(allocator, .{ .offset = @intCast(offset), .count = @intCast(count), .width = width, .signed = signed });
}

/// Appends one block — the 12-byte header, then `payload` compressed per the
/// table's `compression` option (kept only when that shrinks it) — to `buf`.
/// A compressed table first rewrites the payload's integer arrays, `streams`,
/// where that compresses smaller (`int_streams`).
/// Every block, any algorithm, decompresses once at cache fill. Under
/// `.lz4_fsst` the cache holds the FSST bytes (decompressed once at fill if
/// the on-disk LZ4 wrapper shrank them), never the raw expansion.
fn appendBlock(
    allocator: Allocator,
    compressor: *compression_mod.Compressor,
    buf: *std.ArrayList(u8),
    payload: []const u8,
    streams: []const int_streams.Stream,
    encoding_byte: u8,
    has_nulls: bool,
    table_compression: types.TableCompression,
) !void {
    const raw_size: u32 = @intCast(payload.len);
    var directory: std.ArrayList(u8) = .empty;
    defer directory.deinit(allocator);
    var rewritten: std.ArrayList(u8) = .empty;
    defer rewritten.deinit(allocator);
    const rewrite = table_compression != .none and streams.len > 0 and
        try int_streams.transform(allocator, compressor, payload, streams, &directory, &rewritten);
    const source = if (rewrite) rewritten.items else payload;
    var compressed: ?[]u8 = null;
    defer if (compressed) |c| allocator.free(c);
    var kind: format.Compression = .none;
    switch (table_compression) {
        .none => {},
        .zstd => {
            compressed = try compressor.compress(allocator, source, compression_mod.block_level);
            kind = if (compressed.?.len < source.len) .zstd else .none;
        },
        .lz4, .lz4_fsst => {
            compressed = try compressor.compressLz4HC(allocator, source);
            kind = if (compressed.?.len < source.len) .lz4 else .none;
        },
    }
    const stored = if (kind != .none) compressed.? else source;

    const flags = format.ColumnBlockFlags{ .has_nulls = has_nulls, .int_streams = rewrite };

    // Header: kind (u8) + flags (u8) + encoding (u8) + 1 reserved + uncompressed_size (u32) + compressed_size (u32)
    try buf.ensureUnusedCapacity(allocator, format.column_block_header_size + directory.items.len + stored.len);
    buf.appendAssumeCapacity(@backingInt(kind));
    buf.appendAssumeCapacity(flags.toByte());
    buf.appendAssumeCapacity(encoding_byte);
    buf.appendAssumeCapacity(0); // remaining reserved byte
    var b4: [4]u8 = undefined;
    format.writeU32(&b4, raw_size);
    buf.appendSliceAssumeCapacity(&b4);
    format.writeU32(&b4, @intCast(directory.items.len + stored.len));
    buf.appendSliceAssumeCapacity(&b4);
    buf.appendSliceAssumeCapacity(directory.items);
    buf.appendSliceAssumeCapacity(stored);
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
    streams: *Streams,
    view: ColumnView,
    row_start: usize,
    row_end: usize,
) !bool {
    return switch (view.data) {
        .int, .date => |d| rleEncode(i32, allocator, scratch, streams, view, d, row_start, row_end),
        .bigint, .datetime, .decimal64 => |d| rleEncode(i64, allocator, scratch, streams, view, d, row_start, row_end),
        .smallint => |d| rleEncode(i16, allocator, scratch, streams, view, d, row_start, row_end),
        .tinyint => |d| rleEncode(i8, allocator, scratch, streams, view, d, row_start, row_end),
        .boolean => |d| rleEncode(u8, allocator, scratch, streams, view, d, row_start, row_end),
        // Same eligibility set as FOR; variable-width and 16-byte types stay out.
        else => false,
    };
}

fn rleEncode(
    comptime T: type,
    allocator: Allocator,
    scratch: *std.ArrayList(u8),
    streams: *Streams,
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
    try addStream(allocator, streams, scratch.items.len, n_runs, native, @typeInfo(T).int.signedness == .signed);
    try addStream(allocator, streams, scratch.items.len + n_runs * native, n_runs, 4, false);

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
    streams: *Streams,
    view: ColumnView,
    row_start: usize,
    row_end: usize,
) !bool {
    return switch (view.data) {
        .int, .date => |d| forEncode(i32, allocator, scratch, streams, view, d, row_start, row_end),
        .bigint, .datetime, .decimal64 => |d| forEncode(i64, allocator, scratch, streams, view, d, row_start, row_end),
        .smallint => |d| forEncode(i16, allocator, scratch, streams, view, d, row_start, row_end),
        .tinyint => |d| forEncode(i8, allocator, scratch, streams, view, d, row_start, row_end),
        .boolean => |d| forEncode(u8, allocator, scratch, streams, view, d, row_start, row_end),
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
    streams: *Streams,
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
    try addStream(allocator, streams, scratch.items.len, n, width, false);

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
/// Mid-build abandon past this distinct fraction of the block's rows: there
/// evaluating once per value saves little, so the distinct scan stops early.
/// A row-group dictionary stays cache-resident at any NDV under the cap, so
/// decoding it never costs more than reading the raw layout, and the body
/// comparison in `tryEncodeDict` settles storage exactly.
const dict_max_distinct_num: usize = 9;
const dict_max_distinct_den: usize = 10;
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
/// then writes a raw string block. With `entry_stats` the body also carries
/// each value's row count and first row where it stays under the raw body;
/// they decide nothing about the encoding.
fn tryEncodeDict(
    allocator: Allocator,
    scratch: *std.ArrayList(u8),
    streams: *Streams,
    view: ColumnView,
    row_start: usize,
    row_end: usize,
    entry_stats: bool,
) !bool {
    const sv: StringView = switch (view.data) {
        .varchar, .string, .char, .json, .nested, .bitmap => |s| s,
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
            if (map.count() > dict_max_ndv or map.count() * dict_max_distinct_den > n * dict_max_distinct_num) return false;
            gop.value_ptr.* = @intCast(distinct.items.len);
            try distinct.append(allocator, s);
        }
        row_codes[i] = gop.value_ptr.*;
    }

    const k = distinct.items.len;
    if (k == 0) return false; // all-null block: stay raw (degenerate)

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
    const stat_width = format.dictEntryStatWidth(@intCast(n));
    const stats_body = 2 * k * stat_width;
    const with_stats = entry_stats and dict_body + stats_body < raw_body;

    try scratch.ensureUnusedCapacity(allocator, dict_body + if (with_stats) stats_body else 0);
    var b4: [4]u8 = undefined;

    format.writeU32(&b4, @intCast(k));
    scratch.appendSliceAssumeCapacity(&b4);
    const flags: u8 = if (with_stats) format.DICT_ENTRY_STATS else 0;
    scratch.appendSliceAssumeCapacity(&[_]u8{ code_width, flags, 0, 0 });

    // dict_offsets in sorted order, rebased to 0 (k+1 entries).
    try addStream(allocator, streams, scratch.items.len, k + 1, 4, false);
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
    try addStream(allocator, streams, scratch.items.len, n, code_width, false);
    for (row_codes) |ic| {
        const code = ins_to_sorted[ic];
        var b8: [8]u8 = undefined;
        std.mem.writeInt(u64, &b8, code, .little);
        scratch.appendSliceAssumeCapacity(b8[0..code_width]);
    }
    if (!with_stats) return true;

    // Every value occurs, so a count is stored less one.
    const counts = try allocator.alloc(u32, k);
    defer allocator.free(counts);
    const first_rows = try allocator.alloc(u32, k);
    defer allocator.free(first_rows);
    @memset(counts, 0);
    for (row_codes, 0..) |ic, i| {
        const code = ins_to_sorted[ic];
        if (counts[code] == 0) first_rows[code] = @intCast(i);
        counts[code] += 1;
    }
    for ([_][]const u32{ counts, first_rows }, [_]u32{ 1, 0 }) |values, less| {
        try addStream(allocator, streams, scratch.items.len, k, stat_width, false);
        for (values) |v| {
            var b8: [8]u8 = undefined;
            std.mem.writeInt(u64, &b8, v - less, .little);
            scratch.appendSliceAssumeCapacity(b8[0..stat_width]);
        }
    }
    return true;
}

/// Raw string blocks at least this large are offered to FSST under
/// `.lz4_fsst`; smaller blocks can't amortize the symbol table.
const fsst_min_block_bytes: usize = 64 * 1024;

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
    streams: *Streams,
    view: ColumnView,
    row_start: usize,
    row_end: usize,
    /// Minimum raw string bytes before FSST is attempted; smaller blocks
    /// can't amortize the symbol table.
    min_total_bytes: usize,
    /// Row-encode lanes (1 = serial). Lane splits are byte-balanced and
    /// contiguous, so the output is byte-identical at any lane count.
    threads: usize,
) !bool {
    const sv: StringView = switch (view.data) {
        .varchar, .string, .char, .json, .nested, .bitmap => |s| s,
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
    const lanes = @max(@as(usize, 1), @min(threads, @min(fsst_max_lanes, n / fsst_min_rows_per_lane)));
    if (lanes > 1) {
        try encodeFsstLanes(allocator, &table, sv, row_start, row_end, total_bytes, lanes, offsets, &comp);
    } else {
        r = row_start;
        while (r < row_end) : (r += 1) {
            const s = sv.rowBytes(r);
            try comp.ensureUnusedCapacity(allocator, fsst.encodedSizeBound(s.len));
            table.encodeAppend(s, &comp);
            offsets[r - row_start + 1] = @intCast(comp.items.len);
        }
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
    try addStream(allocator, streams, scratch.items.len, n + 1, 4, false);
    for (offsets) |off| {
        format.writeU32(&b4, off);
        scratch.appendSliceAssumeCapacity(&b4);
    }
    scratch.appendSliceAssumeCapacity(comp.items);
    return true;
}

/// Beyond ~8 lanes the per-lane stitch/spawn overhead outgrows the win on a
/// 64K-row block.
const fsst_max_lanes: usize = 8;
const fsst_min_rows_per_lane: usize = 1024;

const FsstLane = struct {
    allocator: Allocator,
    table: *const fsst.SymbolTable,
    sv: StringView,
    block_row_start: usize,
    lane_begin: usize,
    lane_end: usize,
    offsets: []u32,
    buf: std.ArrayListUnmanaged(u8) = .empty,
    err: ?anyerror = null,

    fn run(self: *FsstLane) void {
        var r = self.lane_begin;
        while (r < self.lane_end) : (r += 1) {
            const s = self.sv.rowBytes(r);
            self.buf.ensureUnusedCapacity(self.allocator, fsst.encodedSizeBound(s.len)) catch |e| {
                self.err = e;
                return;
            };
            self.table.encodeAppend(s, &self.buf);
            self.offsets[r - self.block_row_start + 1] = @intCast(self.buf.items.len);
        }
    }
};

/// Lane-parallel FSST row encode. Rows are split into `lanes` contiguous
/// ranges balanced by raw byte count (binary search on the source offsets);
/// each lane encodes into a private buffer writing lane-relative offsets,
/// then the buffers are stitched in lane order and the offsets rebased.
/// Output is byte-identical to the serial encode.
fn encodeFsstLanes(
    allocator: Allocator,
    table: *const fsst.SymbolTable,
    sv: StringView,
    row_start: usize,
    row_end: usize,
    total_bytes: usize,
    lanes: usize,
    offsets: []u32,
    comp: *std.ArrayListUnmanaged(u8),
) !void {
    var lane_jobs = try allocator.alloc(FsstLane, lanes);
    defer {
        for (lane_jobs) |*lj| lj.buf.deinit(allocator);
        allocator.free(lane_jobs);
    }

    var begin = row_start;
    for (lane_jobs, 1..) |*lj, k| {
        const end = if (k == lanes) row_end else blk: {
            const target = sv.offsets[row_start] + @as(u32, @intCast(total_bytes * k / lanes));
            var lo = begin;
            var hi = row_end;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (sv.offsets[mid] < target) lo = mid + 1 else hi = mid;
            }
            break :blk lo;
        };
        lj.* = .{
            .allocator = allocator,
            .table = table,
            .sv = sv,
            .block_row_start = row_start,
            .lane_begin = begin,
            .lane_end = end,
            .offsets = offsets,
        };
        begin = end;
    }

    const handles = allocator.alloc(std.Thread, lanes - 1) catch null;
    var spawned: usize = 0;
    if (handles) |hs| {
        for (hs, lane_jobs[1..]) |*h, *lj| {
            h.* = std.Thread.spawn(.{}, FsstLane.run, .{lj}) catch break;
            spawned += 1;
        }
    }
    FsstLane.run(&lane_jobs[0]);
    for (lane_jobs[1 + spawned ..]) |*lj| FsstLane.run(lj);
    if (handles) |hs| {
        for (hs[0..spawned]) |h| h.join();
        allocator.free(hs);
    }

    var stitched: usize = 0;
    for (lane_jobs) |*lj| {
        if (lj.err) |e| return e;
        stitched += lj.buf.items.len;
    }
    try comp.ensureUnusedCapacity(allocator, stitched);
    for (lane_jobs) |*lj| {
        const base: u32 = @intCast(comp.items.len);
        comp.appendSliceAssumeCapacity(lj.buf.items);
        for (offsets[lj.lane_begin - row_start + 1 .. lj.lane_end - row_start + 1]) |*off| {
            off.* += base;
        }
    }
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
    streams: *Streams,
    view: ColumnView,
    row_start: usize,
    row_end: usize,
) !void {
    const integer_width: ?u8 = switch (view.data) {
        .int, .date => 4,
        .bigint, .datetime, .decimal64 => 8,
        .smallint => 2,
        .tinyint => 1,
        else => null,
    };
    if (integer_width) |w| try addStream(allocator, streams, buf.items.len, row_end - row_start, w, true);
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
        .varchar => |sv| try writeStringBlock(allocator, buf, streams, sv, row_start, row_end),
        .string => |sv| try writeStringBlock(allocator, buf, streams, sv, row_start, row_end),
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
        .char => |sv| try writeStringBlock(allocator, buf, streams, sv, row_start, row_end),
        .json => |sv| try writeStringBlock(allocator, buf, streams, sv, row_start, row_end),
        .nested => |sv| try writeStringBlock(allocator, buf, streams, sv, row_start, row_end),
        .bitmap => |sv| try writeStringBlock(allocator, buf, streams, sv, row_start, row_end),
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
        .decimal256 => |data| {
            const slice = data[row_start..row_end];
            try buf.ensureUnusedCapacity(allocator, slice.len * 32);
            for (slice) |v| {
                var b: [32]u8 = undefined;
                std.mem.writeInt(i256, &b, v, .little);
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
        // No min/max for i256 values (`format.typeHasStats`): the full i128
        // range never excludes the block, and the null count stays exact.
        .decimal256 => .{ .min = std.math.minInt(i128), .max = std.math.maxInt(i128), .null_count = nullCount(view, row_start, row_end) },
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
        .varchar, .string, .char, .json, .nested, .bitmap => |sv| computeStringStats(view, sv, row_start, row_end),
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

fn nullCount(view: ColumnView, row_start: usize, row_end: usize) u64 {
    var nulls: u64 = 0;
    for (row_start..row_end) |r| {
        if (!view.isValid(r)) nulls += 1;
    }
    return nulls;
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
    streams: *Streams,
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
    try addStream(allocator, streams, buf.items.len, n + 1, 4, false);
    for (sv.offsets[row_start .. row_end + 1]) |off| {
        format.writeU32(&b4, off - byte_start);
        buf.appendSliceAssumeCapacity(&b4);
    }

    // bytes
    buf.appendSliceAssumeCapacity(sv.bytes[byte_start..byte_end]);
}
