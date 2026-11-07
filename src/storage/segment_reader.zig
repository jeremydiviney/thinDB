//! Read a segment file into memory, parse header/footer, and decode columns
//! on demand. Caller owns returned OwnedColumns.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const native_endian = builtin.cpu.arch.endian();

/// Decode a packed fixed-width column block into a typed array. The on-disk
/// layout is little-endian and contiguous, so on a little-endian host it is
/// bit-identical to the in-memory `[]T` — one bulk copy at memory-bandwidth
/// speed, replacing a per-element `readInt` parse that ran ~10-40x slower on
/// wide scans. Big-endian hosts byteswap each element.
fn decodeFixed(comptime T: type, allocator: Allocator, values: []const u8, row_count: u32) ![]T {
    const data = try allocator.alloc(T, row_count);
    errdefer allocator.free(data);
    const n_bytes = @as(usize, row_count) * @sizeOf(T);
    if (native_endian == .little) {
        @memcpy(std.mem.sliceAsBytes(data), values[0..n_bytes]);
    } else {
        const Bits = @Int(.unsigned, @bitSizeOf(T));
        for (data, 0..) |*slot, i| {
            const bits = std.mem.readInt(Bits, values[i * @sizeOf(T) ..][0..@sizeOf(T)], .little);
            slot.* = @bitCast(bits);
        }
    }
    return data;
}

const types = @import("../types.zig");
const Type = types.Type;
const TableSchema = types.TableSchema;

const format = @import("format.zig");
const column = @import("column.zig");
const compression_mod = @import("compression.zig");
const int_streams = @import("int_streams.zig");
const fsst = @import("fsst.zig");
const segment_strings = @import("segment_strings.zig");
const table_strings = @import("table_strings.zig");
const storage_cache = @import("cache.zig");
const core_scheduler = @import("../util/core_scheduler.zig");
const parallel = @import("../util/parallel.zig");
const simd_mod = @import("../util/simd.zig");
const prof = @import("../util/prof.zig");
const ColumnView = column.ColumnView;
const StringView = column.StringView;
const OwnedColumn = column.OwnedColumn;
const OwnedStringColumn = column.OwnedStringColumn;
const RowGroupMeta = format.RowGroupMeta;
const SegmentInfo = format.SegmentInfo;

pub const ReadSegment = struct {
    allocator: Allocator,
    file: Io.File,
    io: Io,
    info: SegmentInfo,
    /// Per shared page of the string tables, in `info.string_tables` order:
    /// the awake-clock millisecond a reader last hinted its bytes
    /// (`claimPageHint`), 0 for never. Shared by every copy of the segment.
    page_hints: []std.atomic.Value(u32) = &.{},
    /// The table's lists, whose values the blocks of `info.int_id_columns`
    /// name by table id (§4.7); null for a reader of no such blocks.
    lists: ?*table_strings.TableStrings = null,

    pub fn deinit(self: *ReadSegment) void {
        for (self.info.row_groups) |rg| {
            self.allocator.free(rg.stats);
            self.allocator.free(rg.col_offsets);
            self.allocator.free(rg.col_lengths);
        }
        self.allocator.free(self.info.row_groups);
        format.freeStringTables(self.allocator, self.info.string_tables);
        if (self.info.int_id_columns.len > 0) self.allocator.free(self.info.int_id_columns);
        self.allocator.free(self.page_hints);
        self.file.close(self.io);
        self.* = undefined;
    }

    /// How long one reader's hint of a shared page stands in for the
    /// others': the page is read in by then, or at least on its way.
    const PAGE_HINT_MS = 1000;

    /// Whether the caller is to hint page `p` of column `column_idx`'s
    /// string table, at `now_ms`: false when another reader did within
    /// `PAGE_HINT_MS`. The readers of one segment want its pages at once,
    /// and a hint blocks while the disk queue is full, so each that hinted
    /// every page waited out the whole read before decompressing one.
    fn claimPageHint(self: ReadSegment, column_idx: u32, p: usize, now_ms: u32) bool {
        var base: usize = 0;
        for (self.info.string_tables) |t| {
            if (t.column_idx == column_idx) break;
            base += t.pages.len;
        } else return true;
        if (base + p >= self.page_hints.len) return true;
        const stamp = &self.page_hints[base + p];
        const last = stamp.load(.monotonic);
        if (last != 0 and now_ms -% last < PAGE_HINT_MS) return false;
        return stamp.cmpxchgStrong(last, now_ms, .monotonic, .monotonic) == null;
    }

    /// Decode a single column out of a single row group. Caller frees via `OwnedColumn.deinit`.
    pub fn decodeColumn(
        self: ReadSegment,
        allocator: Allocator,
        schema: TableSchema,
        row_group_idx: usize,
        column_idx: usize,
    ) !OwnedColumn {
        return self.decodeColumnMaybeCached(allocator, schema, row_group_idx, column_idx, null);
    }

    /// Decode one column of one row group, reading only that column's block
    /// from disk (located via the footer's per-column offsets) instead of the
    /// whole segment file. `c` (if non-null) is the decompressed-block buffer
    /// pool: on a hit we touch no I/O at all (`has_nulls` is schema-derived);
    /// on a miss we pread the block, decompress, cache it, and decode.
    pub fn decodeColumnMaybeCached(
        self: ReadSegment,
        allocator: Allocator,
        schema: TableSchema,
        row_group_idx: usize,
        column_idx: usize,
        c: ?storage_cache.TableCache,
    ) !OwnedColumn {
        std.debug.assert(column_idx < schema.columns.len);
        const col_type = schema.columns[column_idx].type;
        const rg = self.info.row_groups[row_group_idx];
        // The writer emits a validity bitmap for every nullable column, so
        // `has_nulls` is fixed by the schema — no need to read the block header.
        const flags = format.ColumnBlockFlags{ .has_nulls = schema.columns[column_idx].nullable };

        if (c) |tc| {
            const cc = tc.cache;
            const key = storage_cache.Key{
                .table_uid = tc.table_uid,
                .segment_id = self.info.segment_id,
                .row_group_idx = @intCast(row_group_idx),
                .column_idx = @intCast(column_idx),
                .content = blockContent(tc, column_idx),
            };
            const entry = try self.acquireBlock(allocator, tc, key, column_idx);
            defer cc.release(entry);
            return decodeColumnPayload(allocator, col_type, entry.bytes, rg.row_count, flags, entry.encoding);
        }

        const block = try self.readColumnBlock(allocator, row_group_idx, column_idx);
        defer allocator.free(block);
        if (isTableIdsBlock(block)) {
            const values = try self.blockValues(allocator, allocator, block, column_idx, rg.row_count, false);
            defer allocator.free(values.bytes);
            return decodeColumnPayload(allocator, col_type, values.bytes, rg.row_count, flags, values.encoding);
        }
        if (!isSegmentDictBlock(block)) return decodeBlock(allocator, block, 0, col_type, rg.row_count, flags);
        const payload = try self.rebuildSegmentDict(allocator, allocator, block, 0, row_group_idx, column_idx, null, .full);
        defer allocator.free(payload);
        return decodeColumnPayload(allocator, col_type, payload, rg.row_count, flags, .dict);
    }

    /// The cached block for `key`, pinned; on a miss read in by this reader
    /// (`fillCacheEntry`), or by the one already reading it, waited for.
    fn acquireBlock(
        self: ReadSegment,
        allocator: Allocator,
        tc: storage_cache.TableCache,
        key: storage_cache.Key,
        column_idx: usize,
    ) !*storage_cache.Cache.Entry {
        const fill = switch (try tc.cache.claimAfterFill(self.io, key)) {
            .hit => |entry| return entry,
            .fill => |fill| fill,
        };
        defer tc.cache.endFill(self.io, fill);
        if (tc.misses) |log| log.note(column_idx, .block);
        return self.fillCacheEntry(allocator, tc.cache, key, column_idx);
    }

    /// Cache-miss fill: pread the block, decompress it and insert it pinned,
    /// so a hit costs only the decode. Every block is cached decompressed
    /// (still encoded — raw / FOR / dict / RLE / FSST); the resident set is the
    /// LRU's problem, not a per-access decompress. The payload lives in the
    /// cache's huge-page pool, so it's both allocated and (on eviction) freed
    /// there.
    fn fillCacheEntry(
        self: ReadSegment,
        allocator: Allocator,
        cc: *storage_cache.Cache,
        key: storage_cache.Key,
        column_idx: usize,
    ) !*storage_cache.Cache.Entry {
        const block = try self.readColumnBlock(allocator, key.row_group_idx, column_idx);
        defer allocator.free(block);
        const block_alloc = cc.blockAllocator();
        if (isTableIdsBlock(block)) {
            const values = try self.blockValues(block_alloc, allocator, block, column_idx, self.info.row_groups[key.row_group_idx].row_count, key.content == .int_ids);
            errdefer block_alloc.free(values.bytes);
            return cc.insertPinned(key, values.bytes, values.encoding);
        }
        // A reader asks for ids only of a column every block of which
        // stores them (`SegmentInfo.int_id_columns`).
        if (key.content == .int_ids) return format.Error.SchemaMismatch;
        const tc: storage_cache.TableCache = .{ .cache = cc, .table_uid = key.table_uid };
        const raw = if (isSegmentDictBlock(block))
            try self.rebuildSegmentDict(block_alloc, allocator, block, 0, key.row_group_idx, column_idx, tc, .full)
        else
            try getDecompressedBytes(block_alloc, allocator, block, 0);
        errdefer block_alloc.free(raw);
        return cc.insertPinned(key, raw, blockEncoding(block, 0));
    }

    /// `borrowColumnBlock` for a reader that needs only some of a string
    /// column's block: a row group the column's string table took reads
    /// only the parts `form` asks for (`.no_codes` only where the block
    /// stores entry stats, which stand in for the codes' counts), rebuilt
    /// with the `.dict` flags marking what it leaves out and cached apart
    /// from the whole block, which serves the read instead when cached. Any
    /// other block reads whole.
    pub fn borrowStringBlock(
        self: ReadSegment,
        allocator: Allocator,
        row_group_idx: usize,
        column_idx: usize,
        c: ?storage_cache.TableCache,
        form: segment_strings.Form,
    ) !BorrowedBlock {
        const parts = self.stringBlockParts(row_group_idx, column_idx) orelse
            return self.borrowColumnBlock(allocator, row_group_idx, column_idx, c);
        if (form == .full or (form == .no_codes and parts.lengths.get(.stats) == 0)) {
            return self.borrowColumnBlock(allocator, row_group_idx, column_idx, c);
        }
        if (c) |tc| {
            var key = storage_cache.Key{
                .table_uid = tc.table_uid,
                .segment_id = self.info.segment_id,
                .row_group_idx = @intCast(row_group_idx),
                .column_idx = @intCast(column_idx),
            };
            if (tc.cache.acquireAfterFill(self.io, key)) |entry| return .{ .bytes = entry.bytes, .encoding = entry.encoding, .entry = entry };
            key.content = partialContent(form);
            const entry = switch (try tc.cache.claimAfterFill(self.io, key)) {
                .hit => |entry| entry,
                .fill => |fill| filled: {
                    defer tc.cache.endFill(self.io, fill);
                    if (tc.misses) |log| log.note(column_idx, missRead(form));
                    const read = try self.readStringParts(allocator, row_group_idx, column_idx, parts, form);
                    defer allocator.free(read.bytes);
                    const raw = try self.rebuildSegmentDict(tc.cache.blockAllocator(), allocator, read.bytes, read.start, row_group_idx, column_idx, tc, form);
                    errdefer tc.cache.blockAllocator().free(raw);
                    break :filled try tc.cache.insertPinned(key, raw, .dict);
                },
            };
            return .{ .bytes = entry.bytes, .encoding = .dict, .entry = entry, .form = form };
        }
        const read = try self.readStringParts(allocator, row_group_idx, column_idx, parts, form);
        defer allocator.free(read.bytes);
        const raw = try self.rebuildSegmentDict(allocator, allocator, read.bytes, read.start, row_group_idx, column_idx, null, form);
        return .{ .bytes = raw, .encoding = .dict, .owned = raw, .form = form };
    }

    /// Row group `row_group_idx`'s values of non-nullable string column
    /// `column_idx` by segment id, with their row counts and first rows,
    /// when the column's string table took the row group and its block
    /// stores entry stats; else null. Reads only the block's index and stats parts, cached apart
    /// from the whole block; a cached whole block, or one cached without its
    /// codes, serves the read instead.
    pub fn borrowStringEntries(
        self: ReadSegment,
        allocator: Allocator,
        row_group_idx: usize,
        column_idx: usize,
        c: ?storage_cache.TableCache,
    ) !?BorrowedEntries {
        const parts = self.stringBlockParts(row_group_idx, column_idx) orelse return null;
        if (parts.lengths.get(.stats) == 0) return null;
        const rows = self.info.row_groups[row_group_idx].row_count;
        if (c) |tc| {
            var key: storage_cache.Key = .{
                .table_uid = tc.table_uid,
                .segment_id = self.info.segment_id,
                .row_group_idx = @intCast(row_group_idx),
                .column_idx = @intCast(column_idx),
            };
            const block = tc.cache.acquireAfterFill(self.io, key) orelse no_codes: {
                key.content = .dict_no_codes;
                break :no_codes tc.cache.acquireAfterFill(self.io, key);
            };
            if (block) |entry| {
                if (entry.encoding == .dict) {
                    const db = dictBlockOf(entry.bytes, rows);
                    if (db.hasSegmentIds() and db.hasEntryStats()) return .{ .entries = db.entryCounts(), .entry = entry };
                }
                tc.cache.release(entry);
            }
            key.content = .dict_counts;
            const fill = switch (try tc.cache.claimAfterFill(self.io, key)) {
                .hit => |entry| return .{ .entries = .of(entry.bytes), .entry = entry },
                .fill => |fill| fill,
            };
            defer tc.cache.endFill(self.io, fill);
            if (tc.misses) |log| log.note(column_idx, .entry_counts);
            const out_allocator = tc.cache.blockAllocator();
            const raw = (try self.readEntryCounts(out_allocator, allocator, row_group_idx, column_idx, parts)) orelse return null;
            errdefer out_allocator.free(raw);
            const entry = try tc.cache.insertPinned(key, raw, .raw);
            return .{ .entries = .of(entry.bytes), .entry = entry };
        }
        const raw = (try self.readEntryCounts(allocator, allocator, row_group_idx, column_idx, parts)) orelse return null;
        return .{ .entries = .of(raw), .owned = raw };
    }

    /// Reads a block's index and stats parts and rebuilds them into an
    /// `EntryCounts` payload from `out_allocator`.
    fn readEntryCounts(
        self: ReadSegment,
        out_allocator: Allocator,
        allocator: Allocator,
        row_group_idx: usize,
        column_idx: usize,
        parts: format.StringBlockParts,
    ) !?[]align(16) u8 {
        const table = self.info.stringTable(column_idx) orelse return format.Error.CorruptColumnBlockHeader;
        const block_offset = self.info.row_groups[row_group_idx].col_offsets[column_idx];
        var payloads: [2][]u8 = .{ &.{}, &.{} };
        defer for (payloads) |p| allocator.free(p);
        var has_nulls = false;
        for ([_]format.StringBlockPart{ .index, .stats }, &payloads) |part, *payload| {
            const frame = try allocator.alloc(u8, parts.lengths.get(part));
            defer allocator.free(frame);
            if (try self.file.readPositionalAll(self.io, frame, block_offset + parts.offset(part)) != frame.len) {
                return format.Error.UnexpectedEof;
            }
            try checkFrame(frame);
            if (!isSegmentDictBlock(frame)) return format.Error.CorruptColumnBlockHeader;
            if (part == .index) has_nulls = format.ColumnBlockFlags.fromByte(frame[1]).has_nulls;
            payload.* = try allocator.alloc(u8, format.readU32(frame[4..8]));
            try decompressBlockInto(allocator, frame, payload.*);
        }
        return segment_strings.rebuildEntryCounts(out_allocator, payloads[0], payloads[1], self.info.row_groups[row_group_idx].row_count, has_nulls, table);
    }

    /// The shared values of column `column_idx`'s string table, read page
    /// by page through the cache when `c` is set; release with `release`.
    pub fn sharedStrings(self: ReadSegment, allocator: Allocator, column_idx: usize, c: ?storage_cache.TableCache) ?SharedStrings {
        const table = self.info.stringTable(column_idx) orelse return null;
        return .{ .pages = .{ .seg = self, .allocator = allocator, .table = table, .c = c, .with_bytes = true } };
    }

    /// Row group `row_group_idx`'s values of column `column_idx` by segment
    /// id, each read apart (`EntryValues`), the pages through the cache when
    /// `c` is set; null when the column's string table didn't take the row
    /// group. Release with `EntryValues.release`.
    pub fn entryValues(self: ReadSegment, allocator: Allocator, row_group_idx: usize, column_idx: usize, c: ?storage_cache.TableCache) ?EntryValues {
        _ = self.stringBlockParts(row_group_idx, column_idx) orelse return null;
        const shared = self.sharedStrings(allocator, column_idx, c) orelse return null;
        return .{ .seg = self, .allocator = allocator, .row_group_idx = row_group_idx, .column_idx = column_idx, .shared = shared };
    }

    /// The table id (DESIGN.md §4.6) of each segment id of column
    /// `column_idx`'s string table, owned by the caller; null when the
    /// segment carries none for the column.
    pub fn tableIds(self: ReadSegment, allocator: Allocator, column_idx: usize) !?[]u32 {
        const table = self.info.stringTable(column_idx) orelse return null;
        if (table.table_ids.length == 0) return null;
        const payload = try self.readTableIds(allocator, allocator, table);
        defer allocator.free(payload);
        const ids = try allocator.alloc(u32, table.value_count);
        for (ids, 0..) |*id, i| id.* = format.readU32(payload[i * 4 ..][0..4]);
        return ids;
    }

    /// `tableIds` through the cache, held until `TableIds.release`; null
    /// when the segment carries none for the column.
    pub fn cachedTableIds(self: ReadSegment, column_idx: usize, c: storage_cache.TableCache) !?TableIds {
        const table = self.info.stringTable(column_idx) orelse return null;
        if (table.table_ids.length == 0) return null;
        const key = tableIdsKey(self, column_idx, c);
        return switch (try c.cache.claimAfterFill(self.io, key)) {
            .hit => |entry| heldTableIds(entry, c),
            .fill => |fill| try self.fillTableIds(table, key, fill, c),
        };
    }

    /// `cachedTableIds`, but `.busy`, reading nothing, while another reader
    /// fills them.
    pub fn claimTableIds(self: ReadSegment, column_idx: usize, c: storage_cache.TableCache) !TableIdsClaim {
        const table = self.info.stringTable(column_idx) orelse return .none;
        if (table.table_ids.length == 0) return .none;
        const key = tableIdsKey(self, column_idx, c);
        return switch (try c.cache.claim(key)) {
            .hit => |entry| .{ .held = heldTableIds(entry, c) },
            .busy => .busy,
            .fill => |fill| .{ .held = try self.fillTableIds(table, key, fill, c) },
        };
    }

    pub const TableIdsClaim = union(enum) {
        /// The segment carries none for the column.
        none,
        busy,
        held: TableIds,
    };

    fn tableIdsKey(self: ReadSegment, column_idx: usize, c: storage_cache.TableCache) storage_cache.Key {
        return .{
            .table_uid = c.table_uid,
            .segment_id = self.info.segment_id,
            .row_group_idx = 0,
            .column_idx = @intCast(column_idx),
            .content = .table_ids,
        };
    }

    fn fillTableIds(self: ReadSegment, table: format.StringTable, key: storage_cache.Key, fill: *storage_cache.Cache.Fill, c: storage_cache.TableCache) !TableIds {
        defer c.cache.endFill(self.io, fill);
        const payload = try self.readTableIds(c.cache.blockAllocator(), self.allocator, table);
        errdefer c.cache.blockAllocator().free(payload);
        return heldTableIds(try c.cache.insertPinned(key, payload, .raw), c);
    }

    fn heldTableIds(entry: *storage_cache.Cache.Entry, c: storage_cache.TableCache) TableIds {
        return .{ .ids = std.mem.bytesAsSlice(u32, entry.bytes), .entry = entry, .cache = c.cache };
    }

    /// A segment string table's table ids, pinned in the cache.
    pub const TableIds = struct {
        /// Little-endian, as stored.
        ids: []align(1) const u32,
        entry: *storage_cache.Cache.Entry,
        cache: *storage_cache.Cache,

        pub fn release(self: TableIds) void {
            self.cache.release(self.entry);
        }
    };

    /// The decompressed payload of `table`'s table-ids block, from
    /// `out_allocator`; `scratch` holds the stored run. Its column blocks
    /// decode side by side, with what idle threads the machine has.
    fn readTableIds(self: ReadSegment, out_allocator: Allocator, scratch: Allocator, table: format.StringTable) ![]align(16) u8 {
        const block = table.table_ids;
        const run = try scratch.alloc(u8, block.length);
        defer scratch.free(run);
        if (try self.file.readPositionalAll(self.io, run, block.offset) != run.len) return format.Error.UnexpectedEof;
        const payload = try out_allocator.alignedAlloc(u8, .@"16", @as(usize, table.value_count) * 4);
        errdefer out_allocator.free(payload);
        var chunks: std.ArrayList(IdsDecode.Chunk) = .empty;
        defer chunks.deinit(scratch);
        var at: usize = 0;
        var out_at: usize = 0;
        while (at < run.len) {
            if (run.len - at < format.column_block_header_size) return format.Error.CorruptColumnBlockHeader;
            const frame_len = format.column_block_header_size + @as(usize, format.readU32(run[at + 8 ..][0..4]));
            if (frame_len > run.len - at) return format.Error.CorruptColumnBlockHeader;
            const frame = run[at..][0..frame_len];
            try checkFrame(frame);
            const raw_len: usize = format.readU32(frame[4..8]);
            if (raw_len > payload.len - out_at) return format.Error.CorruptColumnBlockHeader;
            try chunks.append(scratch, .{ .frame = frame, .out = payload[out_at..][0..raw_len] });
            at += frame_len;
            out_at += raw_len;
        }
        if (out_at != payload.len) return format.Error.CorruptColumnBlockHeader;
        var grant = core_scheduler.reserveWorkers(chunks.items.len);
        defer grant.release();
        var decode: IdsDecode = .{ .allocator = self.allocator, .chunks = chunks.items };
        decode.run(grant.count);
        for (chunks.items) |c| {
            if (!c.done) try decompressBlockInto(scratch, c.frame, c.out);
        }
        return payload;
    }

    /// One table-ids block's column blocks as jobs its threads claim in
    /// order.
    const IdsDecode = struct {
        /// The segment's own allocator, which any thread may use.
        allocator: Allocator,
        chunks: []Chunk,
        next: std.atomic.Value(usize) = .init(0),

        const Chunk = struct { frame: []const u8, out: []u8, done: bool = false };

        fn run(self: *IdsDecode, threads: usize) void {
            var helpers: [parallel.MAX_THREADS]?parallel.Helper = @splat(null);
            const n = @min(@max(threads, 1), helpers.len);
            for (helpers[1..n]) |*h| h.* = parallel.spawn(work, .{self}) catch null;
            self.work();
            for (helpers[1..n]) |h| if (h) |helper| helper.join();
        }

        fn work(self: *IdsDecode) void {
            while (true) {
                const j = self.next.fetchAdd(1, .monotonic);
                if (j >= self.chunks.len) return;
                const c = &self.chunks[j];
                // A block that fails is decoded again by `readTableIds`,
                // which returns the error.
                decompressBlockInto(self.allocator, c.frame, c.out) catch continue;
                c.done = true;
            }
        }
    };

    /// Row group `row_group_idx`'s block parts of column `column_idx`, when
    /// the column's string table took the row group.
    fn stringBlockParts(self: ReadSegment, row_group_idx: usize, column_idx: usize) ?format.StringBlockParts {
        const table = self.info.stringTable(column_idx) orelse return null;
        const parts = table.blocks[row_group_idx];
        return if (parts.isSplit()) parts else null;
    }

    /// The block `borrowStringBlock` cached for row group `row_group_idx` of
    /// column `column_idx` in form `form` (not `.full`), when the cache
    /// holds it; else null, reading nothing.
    pub fn cachedStringBlock(
        self: ReadSegment,
        row_group_idx: usize,
        column_idx: usize,
        c: ?storage_cache.TableCache,
        form: segment_strings.Form,
    ) ?BorrowedBlock {
        const tc = c orelse return null;
        const entry = tc.cache.acquireDerived(.{
            .table_uid = tc.table_uid,
            .segment_id = self.info.segment_id,
            .row_group_idx = @intCast(row_group_idx),
            .column_idx = @intCast(column_idx),
            .content = partialContent(form),
        }) orelse return null;
        return .{ .bytes = entry.bytes, .encoding = .dict, .entry = entry, .form = form };
    }

    fn missRead(form: segment_strings.Form) storage_cache.MissLog.Read {
        return switch (form) {
            .full => .block,
            .no_codes => .no_codes,
            .no_bytes => .no_bytes,
        };
    }

    /// Gaps between two hinted ranges this short are hinted with them: one
    /// read of both costs the disk about what either does alone.
    const HINT_GAP = 8 << 10;

    /// Hints (`willNeed`) the reads `misses` name in row groups
    /// `row_groups` (ascending), of each block, or block part, the cache
    /// holds nothing for, as few ranges as they make: a column's blocks of
    /// consecutive row groups lie end to end in a column-major segment.
    pub fn hintMisses(self: ReadSegment, row_groups: []const u32, misses: []const storage_cache.MissLog.Miss, c: storage_cache.TableCache) void {
        for (misses) |m| {
            var run: ?FileRange = null;
            for (row_groups) |rg_idx| {
                var buf: [2]FileRange = undefined;
                for (self.missRanges(rg_idx, m, c, &buf)) |next| {
                    if (run) |*r| if (next.start >= r.end and next.start - r.end <= HINT_GAP) {
                        r.end = next.end;
                        continue;
                    };
                    if (run) |r| self.willNeed(r.start, r.end - r.start);
                    run = next;
                }
            }
            if (run) |r| self.willNeed(r.start, r.end - r.start);
        }
    }

    const FileRange = struct { start: u64, end: u64 };

    /// The file ranges miss `m` reads in row group `rg_idx`, in `buf`;
    /// none when the cache holds or fills them.
    fn missRanges(self: ReadSegment, rg_idx: u32, m: storage_cache.MissLog.Miss, c: storage_cache.TableCache, buf: *[2]FileRange) []const FileRange {
        const rg = self.info.row_groups[rg_idx];
        const col: usize = m.column_idx;
        var key: storage_cache.Key = .{
            .table_uid = c.table_uid,
            .segment_id = self.info.segment_id,
            .row_group_idx = rg_idx,
            .column_idx = m.column_idx,
            .content = blockContent(c, col),
        };
        if (c.cache.holdsOrFills(key)) return &.{};
        const block_offset = rg.col_offsets[col];
        if (m.read == .block) {
            buf[0] = .{ .start = block_offset, .end = block_offset + rg.col_lengths[col] };
            return buf[0..1];
        }
        const parts = self.stringBlockParts(rg_idx, col) orelse return &.{};
        key.content = switch (m.read) {
            .block => unreachable,
            .no_codes => .dict_no_codes,
            .no_bytes => .dict_no_bytes,
            .entry_counts => .dict_counts,
        };
        if (c.cache.holdsOrFills(key)) return &.{};
        switch (m.read) {
            .block => unreachable,
            .no_codes, .no_bytes => {
                const range = partsRange(parts, if (m.read == .no_codes) .no_codes else .no_bytes) orelse return &.{};
                buf[0] = .{ .start = block_offset + range.start, .end = block_offset + range.end };
                return buf[0..1];
            },
            .entry_counts => {
                for ([_]format.StringBlockPart{ .index, .stats }, buf) |part, *r| {
                    const start = block_offset + parts.offset(part);
                    r.* = .{ .start = start, .end = start + parts.lengths.get(part) };
                }
                return buf;
            },
        }
    }

    /// The byte range, from the block's start, that the parts `form` reads
    /// lie in.
    fn partsRange(parts: format.StringBlockParts, form: segment_strings.Form) ?struct { start: u32, end: u32 } {
        var start: ?u32 = null;
        var end: u32 = 0;
        for (std.enums.values(format.StringBlockPart)) |part| {
            if (!form.reads(part) or parts.lengths.get(part) == 0) continue;
            if (start == null) start = parts.offset(part);
            end = parts.offset(part) + parts.lengths.get(part);
        }
        return .{ .start = start orelse return null, .end = end };
    }

    fn partialContent(form: segment_strings.Form) storage_cache.Content {
        return switch (form) {
            .full => .column,
            .no_codes => .dict_no_codes,
            .no_bytes => .dict_no_bytes,
        };
    }

    /// Rebuilds the parts `form` reads of a segment-dictionary block
    /// (`block`: those parts as read, from `start` bytes into the block) into
    /// the `.dict` payload with segment ids, allocated from `out_allocator`.
    /// Shared-string pages come through the cache when `c` is set.
    fn rebuildSegmentDict(
        self: ReadSegment,
        out_allocator: Allocator,
        allocator: Allocator,
        block: []const u8,
        start: u32,
        row_group_idx: usize,
        column_idx: usize,
        c: ?storage_cache.TableCache,
        form: segment_strings.Form,
    ) ![]align(16) u8 {
        const table = self.info.stringTable(column_idx) orelse return format.Error.CorruptColumnBlockHeader;
        const parts = self.stringBlockParts(row_group_idx, column_idx) orelse return format.Error.CorruptColumnBlockHeader;
        var payload_len: usize = 0;
        for (std.enums.values(format.StringBlockPart)) |part| {
            if (form.reads(part) and parts.lengths.get(part) > 0) payload_len += format.readU32(block[parts.offset(part) - start + 4 ..][0..4]);
        }
        const payload = try allocator.alloc(u8, payload_len);
        defer allocator.free(payload);
        var payloads: segment_strings.BlockParts = .initFill(&.{});
        var at: usize = 0;
        for (std.enums.values(format.StringBlockPart)) |part| {
            const len = parts.lengths.get(part);
            if (!form.reads(part) or len == 0) continue;
            const frame = block[parts.offset(part) - start ..][0..len];
            const out = payload[at..][0..format.readU32(frame[4..8])];
            try decompressBlockInto(allocator, frame, out);
            payloads.set(part, out);
            at += out.len;
        }
        var pages: SharedPages = .{ .seg = self, .allocator = allocator, .table = table, .c = c, .with_bytes = form.reads(.bytes) };
        defer pages.release();
        const has_nulls = format.ColumnBlockFlags.fromByte(block[parts.offset(.index) - start + 1]).has_nulls;
        return segment_strings.rebuildDict(out_allocator, payloads, self.info.row_groups[row_group_idx].row_count, has_nulls, table, &pages, form);
    }

    /// The shared-string pages one rebuild reads, each read (and through the
    /// cache, pinned) at first use, or together by `loadWanted`, and held
    /// until `release`.
    const SharedPages = struct {
        seg: ReadSegment,
        allocator: Allocator,
        table: format.StringTable,
        c: ?storage_cache.TableCache,
        /// Whether the pages' bytes are read, or only their offsets.
        with_bytes: bool,
        slots: []Slot = &.{},
        /// How many slots `want` marked.
        wanted: usize = 0,

        const Slot = struct {
            held: ?Held = null,
            wanted: bool = false,
        };

        const Held = struct {
            page: segment_strings.Page,
            entry: ?*storage_cache.Cache.Entry,
            owned: ?[]align(16) u8,
        };

        pub fn page(self: *SharedPages, p: u32) !segment_strings.Page {
            const slot = try self.slotOf(p);
            if (slot.held) |h| return h.page;
            const h = try self.load(p);
            slot.held = h;
            return h.page;
        }

        /// Marks page `p` as one the caller reads, for `loadWanted`; true
        /// once every page is marked.
        pub fn want(self: *SharedPages, p: u32) !bool {
            const slot = try self.slotOf(p);
            if (!slot.wanted) {
                slot.wanted = true;
                self.wanted += 1;
            }
            return self.wanted == self.slots.len;
        }

        /// Takes every page `want` marked. The ones nobody holds or reads
        /// are hinted to the OS first, so the disk reads them side by side
        /// rather than one pread at a time. Then this reader fills the
        /// pages no other reader is filling, in order, with what idle
        /// threads the machine has, and waits for the rest last: a page
        /// takes milliseconds to decompress, and the readers of one segment
        /// want the same pages at once, so few readers would otherwise
        /// decompress them all one after another.
        pub fn loadWanted(self: *SharedPages) !void {
            const now_ms: u32 = @as(u32, @truncate(@as(u64, @intCast(@divTrunc(@max(Io.Clock.awake.now(self.seg.io).nanoseconds, 0), std.time.ns_per_ms))))) | 1;
            const tc = self.c orelse {
                for (self.slots, 0..) |s, p| {
                    if (s.wanted and s.held == null) self.hint(p, now_ms);
                }
                return;
            };
            var reads: std.ArrayListUnmanaged(u32) = .empty;
            defer reads.deinit(self.allocator);
            for (self.slots, 0..) |s, p| {
                if (!s.wanted or s.held != null) continue;
                if (tc.cache.holdsOrFills(self.pageKey(tc, p, .shared_strings))) continue;
                if (!self.with_bytes and tc.cache.holdsOrFills(self.pageKey(tc, p, .shared_offsets))) continue;
                try reads.append(self.allocator, @intCast(p));
            }
            if (reads.items.len > 0) {
                // One job hints, the rest fill a page each: a lone page
                // gains nothing from a helper.
                var grant = core_scheduler.reserveWorkers(if (reads.items.len > 1) reads.items.len + 1 else 1);
                defer grant.release();
                var fetch: Fetch = .{ .pages = self, .tc = tc, .reads = reads.items, .now_ms = now_ms };
                fetch.run(grant.count);
            }
            for (self.slots, 0..) |*s, p| {
                if (s.wanted and s.held == null) s.held = try self.load(@intCast(p));
            }
        }

        /// One `loadWanted`'s reads as jobs its threads claim in order:
        /// hinting them all, then filling one page each.
        const Fetch = struct {
            pages: *SharedPages,
            tc: storage_cache.TableCache,
            reads: []const u32,
            now_ms: u32,
            next: std.atomic.Value(usize) = .init(0),

            fn run(self: *Fetch, threads: usize) void {
                var helpers: [parallel.MAX_THREADS]?parallel.Helper = @splat(null);
                const n = @min(@max(threads, 1), helpers.len);
                for (helpers[1..n]) |*h| h.* = parallel.spawn(work, .{self}) catch null;
                self.work();
                for (helpers[1..n]) |h| if (h) |helper| helper.join();
            }

            fn work(self: *Fetch) void {
                while (true) {
                    const j = self.next.fetchAdd(1, .monotonic);
                    if (j > self.reads.len) return;
                    if (j == 0) {
                        for (self.reads) |p| self.pages.hint(p, self.now_ms);
                        continue;
                    }
                    const p = self.reads[j - 1];
                    // A fill that fails leaves the page to `loadWanted`'s
                    // waiting pass, which reads it again and returns the
                    // error.
                    self.pages.slots[p].held = self.pages.tryLoad(self.tc, p) catch null;
                }
            }
        };

        /// Hints page `p` unless another reader just hinted its bytes. A
        /// reader of the offsets alone hints them regardless: the reader of
        /// the whole page it would stand in for hints the bytes too.
        fn hint(self: *SharedPages, p: usize, now_ms: u32) void {
            if (self.with_bytes and !self.seg.claimPageHint(self.table.column_idx, p, now_ms)) return;
            const range = self.table.pages[p];
            self.seg.willNeed(range.offset, readLength(range, self.with_bytes));
        }

        fn slotOf(self: *SharedPages, p: u32) !*Slot {
            if (p >= self.table.pages.len) return format.Error.CorruptColumnBlockHeader;
            if (self.slots.len == 0) {
                self.slots = try self.allocator.alloc(Slot, self.table.pages.len);
                @memset(self.slots, .{});
            }
            return &self.slots[p];
        }

        /// Page `p`, through the cache when set: the whole page when cached,
        /// else, reading only its offsets unless `with_bytes`, cached apart
        /// from the whole page. A page another reader is reading in is
        /// waited for.
        fn load(self: *SharedPages, p: u32) !Held {
            if (self.c) |tc| {
                const whole = self.pageKey(tc, p, .shared_strings);
                const entry = if (self.with_bytes)
                    try self.fillPage(tc, whole)
                else
                    tc.cache.acquireAfterFill(self.seg.io, whole) orelse try self.fillPage(tc, self.pageKey(tc, p, .shared_offsets));
                return self.heldOf(tc, entry, p);
            }
            const raw = try self.readPage(self.allocator, self.allocator, self.table.pages[p], self.with_bytes);
            errdefer self.allocator.free(raw);
            return .{ .page = try segment_strings.Page.of(raw, self.expectedCount(p), self.with_bytes), .entry = null, .owned = raw };
        }

        /// `load` through the cache, but null instead of waiting while
        /// another reader fills the page.
        fn tryLoad(self: *SharedPages, tc: storage_cache.TableCache, p: usize) !?Held {
            const whole = self.pageKey(tc, p, .shared_strings);
            const key = if (self.with_bytes) whole else offsets: {
                if (tc.cache.acquire(whole)) |entry| return try self.heldOf(tc, entry, p);
                break :offsets self.pageKey(tc, p, .shared_offsets);
            };
            const fill = switch (try tc.cache.claim(key)) {
                .hit => |entry| return try self.heldOf(tc, entry, p),
                .busy => return null,
                .fill => |fill| fill,
            };
            defer tc.cache.endFill(self.seg.io, fill);
            return try self.heldOf(tc, try self.insertPage(tc, key), p);
        }

        fn fillPage(self: *SharedPages, tc: storage_cache.TableCache, key: storage_cache.Key) !*storage_cache.Cache.Entry {
            const fill = switch (try tc.cache.claimAfterFill(self.seg.io, key)) {
                .hit => |entry| return entry,
                .fill => |fill| fill,
            };
            defer tc.cache.endFill(self.seg.io, fill);
            return self.insertPage(tc, key);
        }

        fn insertPage(self: *SharedPages, tc: storage_cache.TableCache, key: storage_cache.Key) !*storage_cache.Cache.Entry {
            // Helper threads fill pages too (`Fetch`): the scratch comes from
            // the segment's own allocator, which any thread may use.
            const raw = try self.readPage(tc.cache.blockAllocator(), self.seg.allocator, self.table.pages[key.row_group_idx], key.content == .shared_strings);
            errdefer tc.cache.blockAllocator().free(raw);
            return tc.cache.insertPinned(key, raw, .raw);
        }

        fn heldOf(self: *SharedPages, tc: storage_cache.TableCache, entry: *storage_cache.Cache.Entry, p: usize) !Held {
            errdefer tc.cache.release(entry);
            const with_bytes = entry.key.content == .shared_strings;
            return .{ .page = try segment_strings.Page.of(entry.bytes, self.expectedCount(p), with_bytes), .entry = entry, .owned = null };
        }

        fn pageKey(self: *const SharedPages, tc: storage_cache.TableCache, p: usize, content: storage_cache.Content) storage_cache.Key {
            return .{
                .table_uid = tc.table_uid,
                .segment_id = self.seg.info.segment_id,
                .row_group_idx = @intCast(p),
                .column_idx = self.table.column_idx,
                .content = content,
            };
        }

        fn expectedCount(self: *const SharedPages, p: usize) u32 {
            return segment_strings.pageValueCount(self.table.shared_count, @intCast(p));
        }

        fn readLength(range: format.SharedPage, with_bytes: bool) usize {
            return @as(usize, range.index_length) + if (with_bytes) range.bytes_length else 0;
        }

        /// Reads a page's offsets block, and its bytes block when
        /// `with_bytes`, into one buffer, as `segment_strings.Page.of` takes
        /// it.
        fn readPage(self: *SharedPages, out_allocator: Allocator, scratch: Allocator, range: format.SharedPage, with_bytes: bool) ![]align(16) u8 {
            const block = try scratch.alloc(u8, readLength(range, with_bytes));
            defer scratch.free(block);
            if (try self.seg.file.readPositionalAll(self.seg.io, block, range.offset) != block.len) {
                return format.Error.UnexpectedEof;
            }
            const index = block[0..range.index_length];
            const bytes = block[range.index_length..];
            try checkFrame(index);
            if (with_bytes) try checkFrame(bytes);
            const index_len = format.readU32(index[4..8]);
            const bytes_len: usize = if (with_bytes) format.readU32(bytes[4..8]) else 0;
            const out = try out_allocator.alignedAlloc(u8, .@"16", @as(usize, index_len) + bytes_len);
            errdefer out_allocator.free(out);
            try decompressBlockInto(scratch, index, out[0..index_len]);
            if (with_bytes) try decompressBlockInto(scratch, bytes, out[index_len..]);
            return out;
        }

        fn release(self: *SharedPages) void {
            for (self.slots) |s| if (s.held) |h| {
                if (h.entry) |e| self.c.?.cache.release(e);
                if (h.owned) |o| self.allocator.free(o);
            };
            self.allocator.free(self.slots);
        }
    };

    /// A pinned, decompressed column block borrowed for the duration of one
    /// scan `next()` call. `bytes` are the decompressed block bytes. Exactly
    /// one of `entry` / `owned` is set: `entry` when the bytes live in the
    /// shared cache (release the pin via `release`), `owned` when they were
    /// decompressed without a cache (free via `release`). Callers MUST call
    /// `release` to drop the pin / free the buffer.
    pub const BorrowedBlock = struct {
        bytes: []const u8,
        /// Value encoding of `bytes`. A `.for_` block's `bytes` are narrow
        /// deltas, not native-width values, so `viewRawColumn` declines a direct
        /// in-place view. The scan's borrow path instead expands the FOR codes
        /// ONCE into `expanded` (native width) and views over that buffer —
        /// keeping the rest of the projection zero-copy. Raw blocks view in place.
        encoding: format.Encoding = .raw,
        entry: ?*storage_cache.Cache.Entry = null,
        owned: ?[]align(16) u8 = null,
        /// `owned` came from the cache's scratch pool (FSST expansion) rather
        /// than `allocator` — `release` returns it to the pool. Only set
        /// alongside a non-null cache.
        pooled: bool = false,
        /// Native-width expansion of a FOR-encoded block, allocated by the
        /// borrow path so a single FOR column doesn't force the whole row group
        /// onto the owned-decode path. Freed on `release`. Null for raw blocks.
        expanded: ?OwnedColumn = null,
        /// The parts of a segment-dictionary block `bytes` holds
        /// (`borrowStringBlock`).
        form: segment_strings.Form = .full,

        pub fn release(self: *BorrowedBlock, allocator: Allocator, c: ?storage_cache.TableCache) void {
            if (self.entry) |e| {
                if (c) |tc| tc.cache.release(e);
            }
            if (self.owned) |b| {
                if (self.pooled) c.?.cache.releaseScratch(b) else allocator.free(b);
            }
            if (self.expanded) |*col| col.deinit(allocator);
            self.* = undefined;
        }
    };

    /// One row group's values by segment id (`borrowStringEntries`), pinned
    /// in the cache or owned.
    pub const BorrowedEntries = struct {
        entries: segment_strings.EntryCounts,
        entry: ?*storage_cache.Cache.Entry = null,
        owned: ?[]align(16) u8 = null,

        pub fn release(self: *BorrowedEntries, allocator: Allocator, c: ?storage_cache.TableCache) void {
            if (self.entry) |e| {
                if (c) |tc| tc.cache.release(e);
            }
            if (self.owned) |b| allocator.free(b);
            self.* = undefined;
        }
    };

    /// A string table's shared values by rank (`sharedStrings`). A value
    /// stays valid until `release`.
    pub const SharedStrings = struct {
        pages: SharedPages,

        pub fn count(self: *const SharedStrings) u32 {
            return self.pages.table.shared_count;
        }

        pub fn value(self: *SharedStrings, rank: u32) ![]const u8 {
            if (rank >= self.pages.table.shared_count) return format.Error.CorruptColumnBlockHeader;
            return (try self.pages.page(rank / segment_strings.page_values)).value(rank % segment_strings.page_values);
        }

        /// The rank of `bytes`, which the table holds ascending; null when
        /// it holds no such value.
        pub fn rankOf(self: *SharedStrings, bytes: []const u8) !?u32 {
            var lo: u32 = 0;
            var hi: u32 = self.pages.table.shared_count;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                switch (std.mem.order(u8, try self.value(mid), bytes)) {
                    .lt => lo = mid + 1,
                    .gt => hi = mid,
                    .eq => return mid,
                }
            }
            return null;
        }

        pub fn release(self: *SharedStrings) void {
            self.pages.release();
        }
    };

    /// A row group's values by segment id, read apart from its block, whose
    /// shared values span pages a few values don't need: a shared value from
    /// its page alone, an own value from the row group's index and bytes
    /// parts alone. A value stays valid until `release`.
    pub const EntryValues = struct {
        seg: ReadSegment,
        allocator: Allocator,
        row_group_idx: usize,
        column_idx: usize,
        shared: SharedStrings,
        /// The decompressed index and bytes parts, read at the first own
        /// value: cached when the pages are, else owned.
        locals: ?struct { entry: ?*storage_cache.Cache.Entry, owned: ?[]align(16) u8, values: segment_strings.LocalValues } = null,

        /// A `.local_strings` entry's layout: its index part's length (u32),
        /// the block's flags byte, padding to 8, then the index and bytes
        /// parts, decompressed.
        const LOCALS_HEADER = 8;

        pub fn value(self: *EntryValues, segment_id: u32) ![]const u8 {
            if (segment_id < self.shared.count()) return self.shared.value(segment_id);
            return (try self.heldLocals()).value(segment_id);
        }

        /// Marks the page of `segment_id`, when it's a shared value's, for
        /// `loadWanted`.
        pub fn want(self: *EntryValues, segment_id: u32) !void {
            if (segment_id < self.shared.count()) _ = try self.shared.pages.want(segment_id / segment_strings.page_values);
        }

        /// Reads the pages `want` marked side by side (`SharedPages.loadWanted`).
        pub fn loadWanted(self: *EntryValues) !void {
            try self.shared.pages.loadWanted();
        }

        fn heldLocals(self: *EntryValues) !segment_strings.LocalValues {
            if (self.locals) |l| return l.values;
            const tc = self.shared.pages.c orelse {
                const owned = try self.readLocals(self.allocator);
                errdefer self.allocator.free(owned);
                const values = try self.localsOf(owned);
                self.locals = .{ .entry = null, .owned = owned, .values = values };
                return values;
            };
            const key: storage_cache.Key = .{
                .table_uid = tc.table_uid,
                .segment_id = self.seg.info.segment_id,
                .row_group_idx = @intCast(self.row_group_idx),
                .column_idx = @intCast(self.column_idx),
                .content = .local_strings,
            };
            const entry = switch (try tc.cache.claimAfterFill(self.seg.io, key)) {
                .hit => |entry| entry,
                .fill => |fill| fill: {
                    defer tc.cache.endFill(self.seg.io, fill);
                    const bytes = try self.readLocals(tc.cache.blockAllocator());
                    errdefer tc.cache.blockAllocator().free(bytes);
                    break :fill try tc.cache.insertPinned(key, bytes, .raw);
                },
            };
            errdefer tc.cache.release(entry);
            const values = try self.localsOf(entry.bytes);
            self.locals = .{ .entry = entry, .owned = null, .values = values };
            return values;
        }

        /// The row group's index and bytes parts, decompressed, in the
        /// `.local_strings` layout.
        fn readLocals(self: *EntryValues, out_allocator: Allocator) ![]align(16) u8 {
            const seg = self.seg;
            const parts = seg.stringBlockParts(self.row_group_idx, self.column_idx) orelse return format.Error.CorruptColumnBlockHeader;
            const start = parts.offset(.index);
            const read = try self.allocator.alloc(u8, parts.offset(.bytes) + parts.lengths.get(.bytes) - start);
            defer self.allocator.free(read);
            const block_offset = seg.info.row_groups[self.row_group_idx].col_offsets[self.column_idx];
            if (try seg.file.readPositionalAll(seg.io, read, block_offset + start) != read.len) return format.Error.UnexpectedEof;
            const frames: [2][]const u8 = .{ read[0..parts.lengths.get(.index)], read[parts.offset(.bytes) - start ..] };
            if (frames[0].len == 0) return format.Error.CorruptColumnBlockHeader;
            var payload_len: usize = LOCALS_HEADER;
            for (frames) |frame| if (frame.len > 0) {
                try checkFrame(frame);
                if (!isSegmentDictBlock(frame)) return format.Error.CorruptColumnBlockHeader;
                payload_len += format.readU32(frame[4..8]);
            };
            const payload = try out_allocator.alignedAlloc(u8, .@"16", payload_len);
            errdefer out_allocator.free(payload);
            @memset(payload[0..LOCALS_HEADER], 0);
            format.writeU32(payload[0..4], format.readU32(frames[0][4..8]));
            payload[4] = frames[0][1];
            var at: usize = LOCALS_HEADER;
            for (frames) |frame| if (frame.len > 0) {
                const out = payload[at..][0..format.readU32(frame[4..8])];
                try decompressBlockInto(self.allocator, frame, out);
                at += out.len;
            };
            return payload;
        }

        fn localsOf(self: *const EntryValues, bytes: []const u8) !segment_strings.LocalValues {
            const table = self.seg.info.stringTable(self.column_idx) orelse return format.Error.CorruptColumnBlockHeader;
            if (bytes.len < LOCALS_HEADER) return format.Error.CorruptColumnBlockHeader;
            const index_len = format.readU32(bytes[0..4]);
            if (bytes.len - LOCALS_HEADER < index_len) return format.Error.CorruptColumnBlockHeader;
            const index = bytes[LOCALS_HEADER..][0..index_len];
            const has_nulls = format.ColumnBlockFlags.fromByte(bytes[4]).has_nulls;
            return segment_strings.LocalValues.init(index, bytes[LOCALS_HEADER + index_len ..], self.seg.info.row_groups[self.row_group_idx].row_count, has_nulls, table);
        }

        pub fn release(self: *EntryValues) void {
            self.shared.release();
            if (self.locals) |l| {
                if (l.entry) |e| self.shared.pages.c.?.cache.release(e);
                if (l.owned) |o| self.allocator.free(o);
            }
            self.locals = null;
        }
    };

    /// Pin (and, on a miss, decompress + cache) one column's decompressed block
    /// so the caller can build a BORROWED typed view over it. The returned
    /// `BorrowedBlock` keeps the cache pin held until `release` — used by the
    /// scan-side in-place filter, which evaluates the predicate and compacts
    /// survivors out of the borrowed view, then releases, all within one
    /// `next()`. When `c` is null the block is decompressed into an owned
    /// buffer carried by the returned struct (still skips the decode-copy).
    pub fn borrowColumnBlock(
        self: ReadSegment,
        allocator: Allocator,
        row_group_idx: usize,
        column_idx: usize,
        c: ?storage_cache.TableCache,
    ) !BorrowedBlock {
        if (c) |tc| {
            const key = storage_cache.Key{
                .table_uid = tc.table_uid,
                .segment_id = self.info.segment_id,
                .row_group_idx = @intCast(row_group_idx),
                .column_idx = @intCast(column_idx),
                .content = blockContent(tc, column_idx),
            };
            const entry = try self.acquireBlock(allocator, tc, key, column_idx);
            return .{ .bytes = entry.bytes, .encoding = entry.encoding, .entry = entry };
        }
        const block = try self.readColumnBlock(allocator, row_group_idx, column_idx);
        defer allocator.free(block);
        if (isTableIdsBlock(block)) {
            const values = try self.blockValues(allocator, allocator, block, column_idx, self.info.row_groups[row_group_idx].row_count, false);
            return .{ .bytes = values.bytes, .encoding = values.encoding, .owned = values.bytes };
        }
        const raw = if (isSegmentDictBlock(block))
            try self.rebuildSegmentDict(allocator, allocator, block, 0, row_group_idx, column_idx, null, .full)
        else
            try getDecompressedBytes(allocator, allocator, block, 0);
        return .{ .bytes = raw, .encoding = blockEncoding(block, 0), .owned = raw };
    }

    /// Column `column_idx`'s block of row group `row_group_idx` as the table
    /// ids it stores (§4.7), pinned as `borrowColumnBlock` pins it: 8-byte
    /// ids, raw or in runs. The segment lists the column
    /// (`SegmentInfo.intIdColumn`).
    pub fn borrowIdBlock(self: ReadSegment, allocator: Allocator, row_group_idx: usize, column_idx: usize, tc: storage_cache.TableCache) !BorrowedBlock {
        const key = storage_cache.Key{
            .table_uid = tc.table_uid,
            .segment_id = self.info.segment_id,
            .row_group_idx = @intCast(row_group_idx),
            .column_idx = @intCast(column_idx),
            .content = .int_ids,
        };
        const entry = try self.acquireBlock(allocator, tc, key, column_idx);
        return .{ .bytes = entry.bytes, .encoding = entry.encoding, .entry = entry };
    }

    /// Whether a reader of some rows of column `column_idx`'s row group
    /// `row_group_idx` reads their values by `idRows`: the column stores
    /// table ids here, and no reader holds or reads in the values' block.
    /// That block's ids lie on every page of a wide list; a few rows' lie
    /// on a few.
    pub fn readsIdRows(self: ReadSegment, row_group_idx: usize, column_idx: usize, tc: storage_cache.TableCache) bool {
        if (self.lists == null or !self.info.intIdColumn(column_idx)) return false;
        return !tc.cache.holdsOrFills(.{
            .table_uid = tc.table_uid,
            .segment_id = self.info.segment_id,
            .row_group_idx = @intCast(row_group_idx),
            .column_idx = @intCast(column_idx),
        });
    }

    /// The values of `rows` of column `column_idx`'s row group
    /// `row_group_idx`, which `readsIdRows`, as a raw block payload of
    /// `rows.len` rows in `allocator`: their validity bitmap when `nullable`,
    /// then each value in its list's width. Reads the ids' block and the
    /// list pages the rows' ids lie on.
    pub fn idRows(self: ReadSegment, allocator: Allocator, row_group_idx: usize, column_idx: usize, tc: storage_cache.TableCache, nullable: bool, rows: []const u32) ![]align(16) u8 {
        const corrupt = format.Error.CorruptColumnBlockHeader;
        const lists = self.lists orelse return corrupt;
        const width: usize = lists.fixedListWidth(column_idx) orelse return corrupt;
        var block = try self.borrowIdBlock(allocator, row_group_idx, column_idx, tc);
        defer block.release(allocator, tc);
        const rc = self.info.row_groups[row_group_idx].row_count;
        const flags = format.ColumnBlockFlags{ .has_nulls = nullable };
        var expanded: ?OwnedColumn = null;
        defer if (expanded) |*col| col.deinit(allocator);
        const view = viewRawColumn(.bigint, block.bytes, rc, flags, block.encoding) orelse view: {
            expanded = try decodeColumnPayload(allocator, .bigint, block.bytes, rc, flags, block.encoding);
            break :view expanded.?.view();
        };
        const block_ids = switch (view.data) {
            .bigint => |s| s,
            else => return corrupt,
        };
        const bm_len: usize = if (nullable) column.bitmapBytes(rows.len) else 0;
        const out = try allocator.alignedAlloc(u8, .@"16", bm_len + rows.len * width);
        errdefer allocator.free(out);
        @memset(out, 0);
        const ids = try allocator.alloc(u32, rows.len);
        defer allocator.free(ids);
        const valid = try allocator.alloc(u32, rows.len);
        defer allocator.free(valid);
        var n: usize = 0;
        for (rows, 0..) |row, i| {
            if (row >= rc) return corrupt;
            if (!view.isValid(row)) continue;
            if (nullable) column.setValidBit(out[0..bm_len], i, true);
            ids[n] = try tableIdOf(block_ids[row]);
            valid[n] = @intCast(i);
            n += 1;
        }
        const values = out[bm_len..];
        if (n == rows.len) {
            try lists.fixedValues(self.io, column_idx, ids, values);
            return out;
        }
        const named = try allocator.alloc(u8, n * width);
        defer allocator.free(named);
        try lists.fixedValues(self.io, column_idx, ids[0..n], named);
        for (valid[0..n], 0..) |i, k| @memcpy(values[i * width ..][0..width], named[k * width ..][0..width]);
        return out;
    }

    /// The payload of column `column_idx`'s block `block`, coded in table
    /// ids, over the values they name (or, with `ids`, over the ids
    /// themselves), in `allocator`, with the encoding it reads as.
    /// `scratch` holds the ids meanwhile.
    fn blockValues(self: ReadSegment, allocator: Allocator, scratch: Allocator, block: []const u8, column_idx: usize, row_count: u32, ids: bool) !IdBlockValues {
        const lists: ?*table_strings.TableStrings = if (ids) null else self.lists orelse return format.Error.CorruptColumnBlockHeader;
        const payload = try getDecompressedBytes(scratch, scratch, block, 0);
        defer scratch.free(payload);
        const has_nulls = format.ColumnBlockFlags.fromByte(block[1]).has_nulls;
        return idBlockValues(allocator, scratch, self.io, lists, column_idx, payload, blockEncoding(block, 0), row_count, has_nulls);
    }

    /// The string bytes one row group's block of a string column decodes to,
    /// read without expanding a row. A cached block answers from its layout.
    /// Otherwise the block's header is read first: a raw block's size gives
    /// the count. A dict or FSST block is read into the cache (the scan that
    /// follows reuses it) and its codes or recorded byte count give it.
    pub fn stringBlockBytes(
        self: ReadSegment,
        allocator: Allocator,
        row_group_idx: usize,
        column_idx: usize,
        nullable: bool,
        c: storage_cache.TableCache,
    ) !u64 {
        const rg = self.info.row_groups[row_group_idx];
        const key = storage_cache.Key{
            .table_uid = c.table_uid,
            .segment_id = self.info.segment_id,
            .row_group_idx = @intCast(row_group_idx),
            .column_idx = @intCast(column_idx),
        };
        const entry = switch (try c.cache.claimAfterFill(self.io, key)) {
            .hit => |entry| entry,
            .fill => |fill| miss: {
                defer c.cache.endFill(self.io, fill);
                var header: [format.column_block_header_size]u8 = undefined;
                if (try self.file.readPositionalAll(self.io, &header, rg.col_offsets[column_idx]) != header.len) {
                    return format.Error.UnexpectedEof;
                }
                if (blockEncoding(&header, 0) == .raw) {
                    const flags = format.ColumnBlockFlags.fromByte(header[1]);
                    const framing = if (flags.has_nulls) column.bitmapBytes(rg.row_count) else 0;
                    const offsets = 4 + (@as(u64, rg.row_count) + 1) * 4;
                    const payload = format.readU32(header[4..8]);
                    if (payload < framing + offsets) return format.Error.CorruptColumnBlockHeader;
                    return payload - framing - offsets;
                }
                break :miss try self.fillCacheEntry(allocator, c.cache, key, column_idx);
            },
        };
        defer c.cache.release(entry);
        const values = if (nullable) entry.bytes[column.bitmapBytes(rg.row_count)..] else entry.bytes;
        return switch (entry.encoding) {
            .raw => format.readU32(values[0..4]),
            .dict => dictStringBytes(
                allocator,
                dictBlockOf(values, rg.row_count),
                if (nullable) entry.bytes[0..column.bitmapBytes(rg.row_count)] else null,
                rg.row_count,
            ),
            .fsst => (try fsstBlockOf(values, rg.row_count)).raw_byte_count,
            .for_, .rle => format.Error.CorruptColumnBlockHeader,
        };
    }

    /// Feeds every sampled non-NULL value of one row group's block of a
    /// string column to `sampler` (`DistinctWidthSampler.ROWS_PER_BLOCK`
    /// rows at an even stride), touching only those rows: a raw block by its
    /// offsets, read unaligned as a block past a validity bitmap lies, a dict
    /// block by code, an FSST block decoded straight into the hash. The block
    /// is read through the cache, where the scan that follows finds it.
    pub fn sampleDistinctStrings(
        self: ReadSegment,
        allocator: Allocator,
        row_group_idx: usize,
        column_idx: usize,
        nullable: bool,
        c: storage_cache.TableCache,
        sampler: *column.DistinctWidthSampler,
    ) !void {
        const rows = self.info.row_groups[row_group_idx].row_count;
        var block = try self.borrowColumnBlock(allocator, row_group_idx, column_idx, c);
        defer block.release(allocator, c);
        const step = column.DistinctWidthSampler.stride(rows, column.DistinctWidthSampler.ROWS_PER_BLOCK);
        const nulls: ?[]const u8 = if (nullable) block.bytes[0..column.bitmapBytes(rows)] else null;
        const values = if (nulls) |n| block.bytes[n.len..] else block.bytes;
        switch (block.encoding) {
            .raw => {
                const offsets_len = (@as(usize, rows) + 1) * 4;
                if (values.len < 4 + offsets_len) return format.Error.CorruptColumnBlockHeader;
                const offsets = values[4..][0..offsets_len];
                const payload = values[4 + offsets_len ..];
                var row: usize = 0;
                while (row < rows) : (row += step) {
                    if (!column.isValidBit(nulls, row)) continue;
                    const start = format.readU32(offsets[row * 4 ..][0..4]);
                    const end = format.readU32(offsets[(row + 1) * 4 ..][0..4]);
                    if (end < start or end > payload.len) return format.Error.CorruptColumnBlockHeader;
                    try sampler.add(allocator, payload[start..end]);
                }
            },
            .dict => {
                const db = dictBlockOf(values, rows);
                var row: usize = 0;
                while (row < rows) : (row += step) {
                    if (column.isValidBit(nulls, row)) try sampler.add(allocator, db.dictValue(db.rowCode(row)));
                }
            },
            .fsst => {
                const fb = try fsstBlockOf(values, rows);
                var row: usize = 0;
                while (row < rows) : (row += step) {
                    if (!column.isValidBit(nulls, row)) continue;
                    var digest: ValueDigest = .{};
                    fb.table.decodeStream(fb.rowComp(row), &digest);
                    try sampler.addHashed(allocator, digest.hasher.final(), digest.len);
                }
            },
            .for_, .rle => return format.Error.CorruptColumnBlockHeader,
        }
    }

    /// The hash and length `DistinctWidthSampler.add` would take from a
    /// value, fed in decoded pieces.
    const ValueDigest = struct {
        hasher: std.hash.Wyhash = .init(0),
        len: usize = 0,

        pub fn update(self: *ValueDigest, bytes: []const u8) void {
            self.hasher.update(bytes);
            self.len += bytes.len;
        }
    };

    /// Counts each code's rows, then weighs each by its value's length. A NULL
    /// row carries placeholder code 0 but no bytes, as in a raw block.
    fn dictStringBytes(allocator: Allocator, db: DictBlock, nulls: ?[]const u8, row_count: u32) !u64 {
        const ndv: usize = db.ndv;
        const lanes: usize = if (db.code_width == 4) 1 else CODE_COUNT_LANES;
        const counts = try allocator.alloc(u32, lanes * ndv);
        defer allocator.free(counts);
        @memset(counts, 0);
        switch (db.code_width) {
            1 => countCodes(u8, CODE_COUNT_LANES, db.codes, row_count, ndv, counts),
            2 => countCodes(u16, CODE_COUNT_LANES, db.codes, row_count, ndv, counts),
            else => countCodes(u32, 1, db.codes, row_count, ndv, counts),
        }
        for (1..lanes) |lane| {
            for (counts[0..ndv], counts[lane * ndv ..][0..ndv]) |*total, count| total.* += count;
        }
        if (nulls) |bm| for (bm, 0..) |valid_bits, byte| {
            if (valid_bits == 0xFF) continue;
            for (byte * 8..@min(byte * 8 + 8, row_count)) |row| {
                if (!column.isValidBit(bm, row)) counts[db.rowCode(row)] -= 1;
            }
        };
        var bytes: u64 = 0;
        for (counts[0..ndv], 0..) |count, code| bytes += @as(u64, count) * db.dictValue(@intCast(code)).len;
        return bytes;
    }

    /// The most of a range one WILLNEED hint asks for. Linux reads at most
    /// the larger of a device's readahead window and its largest request
    /// per hint (128 and 256 KiB on EBS) and drops the rest, so a longer
    /// range is hinted in pieces; 128 KiB pieces also beat 256 KiB ones on a
    /// c6a gp2 volume (40 MiB cold: 19 against 31 ms).
    const HINT_PIECE = 128 << 10;

    /// Tells the OS the `len` bytes at `offset` will be read soon, so it
    /// reads them in while the caller works and a later pread of them waits
    /// on what is still in flight, not on a whole disk read. Only for bytes
    /// the caller will read: a hint is a real disk read. Linux only; a hint
    /// that fails changes nothing.
    pub fn willNeed(self: ReadSegment, offset: u64, len: usize) void {
        if (builtin.os.tag != .linux) return;
        var at: usize = 0;
        while (at < len) : (at += HINT_PIECE) {
            _ = std.os.linux.fadvise(self.file.handle, @intCast(offset + at), @intCast(@min(HINT_PIECE, len - at)), std.os.linux.POSIX_FADV.WILLNEED);
        }
    }

    /// Read the raw on-disk bytes (header + payload, or a segment-dictionary
    /// block's parts) of one column's block in a row group via a single
    /// positioned read.
    fn readColumnBlock(self: ReadSegment, allocator: Allocator, row_group_idx: usize, column_idx: usize) ![]u8 {
        const rg = self.info.row_groups[row_group_idx];
        const buf = try allocator.alloc(u8, rg.col_lengths[column_idx]);
        errdefer allocator.free(buf);
        if (try self.file.readPositionalAll(self.io, buf, rg.col_offsets[column_idx]) != buf.len) {
            return format.Error.UnexpectedEof;
        }
        if (self.stringBlockParts(row_group_idx, column_idx)) |parts| {
            try checkStringParts(buf, 0, parts, .full);
        } else {
            try checkFrame(buf);
            if (isSegmentDictBlock(buf)) return format.Error.CorruptColumnBlockHeader;
        }
        return buf;
    }

    /// The parts `form` reads of row group `row_group_idx`'s
    /// segment-dictionary block of column `column_idx`, by one positioned
    /// read of the range they lie in, from `start` bytes into the block.
    fn readStringParts(
        self: ReadSegment,
        allocator: Allocator,
        row_group_idx: usize,
        column_idx: usize,
        parts: format.StringBlockParts,
        form: segment_strings.Form,
    ) !struct { bytes: []u8, start: u32 } {
        const range = partsRange(parts, form) orelse return format.Error.CorruptColumnBlockHeader;
        const first = range.start;
        const buf = try allocator.alloc(u8, range.end - first);
        errdefer allocator.free(buf);
        const block_offset = self.info.row_groups[row_group_idx].col_offsets[column_idx];
        if (try self.file.readPositionalAll(self.io, buf, block_offset + first) != buf.len) {
            return format.Error.UnexpectedEof;
        }
        try checkStringParts(buf, first, parts, form);
        return .{ .bytes = buf, .start = first };
    }
};

pub fn readSegment(
    allocator: Allocator,
    io: Io,
    dir: Io.Dir,
    file_name: []const u8,
    schema: TableSchema,
) !ReadSegment {
    // A segment open can transiently race the compactor deleting an old
    // segment (Windows returns a sharing/delete-pending error, surfaced as
    // error.FileBusy). It clears in milliseconds, so retry with backoff a
    // bounded number of times before surfacing it — bounded ⇒ never deadlocks,
    // and with the Bloom prune these opens are already rare (#137).
    var file = blk: {
        var attempt: u8 = 0;
        while (true) : (attempt += 1) {
            if (dir.openFile(io, file_name, .{})) |f| {
                break :blk f;
            } else |err| {
                if (err == error.FileBusy and attempt < 50) {
                    Io.sleep(io, Io.Duration.fromMilliseconds(2), .awake) catch {};
                    continue;
                }
                return err;
            }
        }
    };
    errdefer file.close(io);

    const file_size = (try file.stat(io)).size;
    if (file_size < format.header_size + format.footer_trailer_size) {
        return format.Error.SegmentTooSmall;
    }

    // Header (first 32 bytes) — magic, version, fingerprint, ids.
    var header: [format.header_size]u8 = undefined;
    if (try file.readPositionalAll(io, &header, 0) != format.header_size) return format.Error.UnexpectedEof;
    if (!std.mem.eql(u8, header[0..4], &format.segment_magic)) return format.Error.BadMagic;
    const version = format.readU16(header[4..6]);
    if (version != format.segment_version) return format.Error.UnsupportedVersion;
    const schema_fingerprint = format.readU64(header[8..16]);
    const segment_id = format.readU64(header[16..24]);
    const row_count = format.readU64(header[24..32]);

    // Trailer (last 8 bytes) — footer size + magic.
    var trailer: [format.footer_trailer_size]u8 = undefined;
    if (try file.readPositionalAll(io, &trailer, file_size - format.footer_trailer_size) != format.footer_trailer_size) {
        return format.Error.UnexpectedEof;
    }
    if (!std.mem.eql(u8, trailer[4..8], &format.segment_magic)) return format.Error.BadFooterMagic;
    const footer_size = format.readU32(trailer[0..4]);
    if (footer_size < format.footer_trailer_size or footer_size > file_size - format.header_size) {
        return format.Error.CorruptFooter;
    }

    // The footer is the only data region we read in full; column blocks are
    // pread on demand. It carries per-row-group offset/length/row_count, then
    // per-column min/max stats, then per-column block offsets.
    const footer = try allocator.alloc(u8, footer_size);
    defer allocator.free(footer);
    if (try file.readPositionalAll(io, footer, file_size - footer_size) != footer_size) {
        return format.Error.UnexpectedEof;
    }

    const row_group_count = format.readU32(footer[0..4]);
    const ncols = schema.columns.len;
    const per_rg = 16 + ncols * 56 + ncols * 8 + ncols * 4; // offset/len/rows + stats + col offsets + col lengths
    const tables_at = 4 + @as(usize, row_group_count) * per_rg;
    if (tables_at + 4 + format.footer_trailer_size > footer_size) return format.Error.CorruptFooter;

    const row_groups = try allocator.alloc(RowGroupMeta, row_group_count);
    errdefer allocator.free(row_groups);
    var inited_rg: usize = 0;
    errdefer for (row_groups[0..inited_rg]) |rg| {
        allocator.free(rg.stats);
        allocator.free(rg.col_offsets);
        allocator.free(rg.col_lengths);
    };

    var off: usize = 4;
    for (row_groups) |*rg| {
        rg.offset = format.readU64(footer[off .. off + 8]);
        off += 8;
        rg.length = format.readU32(footer[off .. off + 4]);
        off += 4;
        rg.row_count = format.readU32(footer[off .. off + 4]);
        off += 4;

        const stats = try allocator.alloc(format.Stats, ncols);
        errdefer allocator.free(stats);
        for (stats) |*s| {
            s.min = format.readI128(footer[off .. off + 16]);
            off += 16;
            s.max = format.readI128(footer[off .. off + 16]);
            off += 16;
            s.sum = format.readI128(footer[off .. off + 16]);
            off += 16;
            s.null_count = format.readU64(footer[off .. off + 8]);
            off += 8;
        }
        rg.stats = stats;

        const col_offsets = try allocator.alloc(u64, ncols);
        errdefer allocator.free(col_offsets);
        for (col_offsets) |*co| {
            co.* = format.readU64(footer[off .. off + 8]);
            off += 8;
        }
        const col_lengths = try allocator.alloc(u32, ncols);
        for (col_lengths) |*cl| {
            cl.* = format.readU32(footer[off .. off + 4]);
            off += 4;
        }
        rg.col_offsets = col_offsets;
        rg.col_lengths = col_lengths;
        inited_rg += 1;
    }
    const string_tables = try readStringTables(allocator, footer[off .. footer.len - format.footer_trailer_size], ncols, row_groups, &off);
    errdefer format.freeStringTables(allocator, string_tables);
    const int_id_columns = try readIntIdColumns(allocator, footer[off .. footer.len - format.footer_trailer_size], schema);
    errdefer if (int_id_columns.len > 0) allocator.free(int_id_columns);
    var page_count: usize = 0;
    for (string_tables) |t| page_count += t.pages.len;
    const page_hints = try allocator.alloc(std.atomic.Value(u32), page_count);
    @memset(page_hints, .init(0));

    return ReadSegment{
        .allocator = allocator,
        .file = file,
        .io = io,
        .info = .{
            .segment_id = segment_id,
            .row_count = row_count,
            .schema_fingerprint = schema_fingerprint,
            .byte_size = file_size,
            .row_groups = row_groups,
            .string_tables = string_tables,
            .int_id_columns = int_id_columns,
        },
        .page_hints = page_hints,
    };
}

/// Parse the footer's string-table directory, which `bytes` starts with,
/// advancing `end` past it.
fn readStringTables(allocator: Allocator, bytes: []const u8, ncols: usize, row_groups: []const RowGroupMeta, end: *usize) ![]const format.StringTable {
    if (bytes.len < 4) return format.Error.CorruptFooter;
    const count = format.readU32(bytes[0..4]);
    if (count > ncols) return format.Error.CorruptFooter;
    const tables = try allocator.alloc(format.StringTable, count);
    var inited: usize = 0;
    errdefer {
        for (tables[0..inited]) |t| {
            allocator.free(t.pages);
            allocator.free(t.blocks);
        }
        allocator.free(tables);
    }
    var off: usize = 4;
    for (tables) |*t| {
        if (bytes.len < off + 24) return format.Error.CorruptFooter;
        const column_idx = format.readU32(bytes[off..][0..4]);
        const shared_count = format.readU32(bytes[off + 4 ..][0..4]);
        const value_count = format.readU32(bytes[off + 8 ..][0..4]);
        const page_count = format.readU32(bytes[off + 12 ..][0..4]);
        const value_bytes = format.readU64(bytes[off + 16 ..][0..8]);
        off += 24;
        if (column_idx >= ncols or shared_count > value_count or page_count != segment_strings.pageCount(shared_count)) {
            return format.Error.CorruptFooter;
        }
        const parts_len = std.enums.values(format.StringBlockPart).len * 4;
        if (bytes.len < off + @as(usize, page_count) * 16 + row_groups.len * parts_len + 12) return format.Error.CorruptFooter;
        const pages = try allocator.alloc(format.SharedPage, page_count);
        errdefer allocator.free(pages);
        for (pages) |*p| {
            p.* = .{
                .offset = format.readU64(bytes[off..][0..8]),
                .index_length = format.readU32(bytes[off + 8 ..][0..4]),
                .bytes_length = format.readU32(bytes[off + 12 ..][0..4]),
            };
            off += 16;
        }
        const blocks = try allocator.alloc(format.StringBlockParts, row_groups.len);
        errdefer allocator.free(blocks);
        for (blocks, row_groups) |*b, rg| {
            for (&b.lengths.values) |*len| {
                len.* = format.readU32(bytes[off..][0..4]);
                off += 4;
            }
            if (!b.isSplit() and b.total() != 0) return format.Error.CorruptFooter;
            if (b.isSplit() and b.total() != rg.col_lengths[column_idx]) return format.Error.CorruptFooter;
        }
        const table_ids: format.TableIdsBlock = .{ .offset = format.readU64(bytes[off..][0..8]), .length = format.readU32(bytes[off + 8 ..][0..4]) };
        off += 12;
        t.* = .{ .column_idx = column_idx, .shared_count = shared_count, .value_count = value_count, .value_bytes = value_bytes, .pages = pages, .blocks = blocks, .table_ids = table_ids };
        inited += 1;
    }
    end.* += off;
    return tables;
}

/// Parse the footer's list of columns coded in table ids (`bytes` runs
/// from its count to the footer trailer, exactly).
fn readIntIdColumns(allocator: Allocator, bytes: []const u8, schema: TableSchema) ![]const u32 {
    if (bytes.len < 4) return format.Error.CorruptFooter;
    const count = format.readU32(bytes[0..4]);
    if (count > schema.columns.len or bytes.len != 4 + @as(usize, count) * 4) return format.Error.CorruptFooter;
    if (count == 0) return &.{};
    const columns = try allocator.alloc(u32, count);
    errdefer allocator.free(columns);
    for (columns, 0..) |*c, i| {
        c.* = format.readU32(bytes[4 + i * 4 ..][0..4]);
        if (c.* >= schema.columns.len or table_strings.fixedWidth(schema.columns[c.*].type) == null) return format.Error.CorruptFooter;
        if (i > 0 and c.* <= columns[i - 1]) return format.Error.CorruptFooter;
    }
    return columns;
}

// ---------------------------------------------------------------------------
// Block decoding
// ---------------------------------------------------------------------------

fn isSegmentDictBlock(block: []const u8) bool {
    return block[format.column_block_encoding_offset] == format.segment_dict_encoding;
}

fn isTableIdsBlock(block: []const u8) bool {
    return format.ColumnBlockFlags.fromByte(block[1]).table_ids;
}

fn blockContent(tc: storage_cache.TableCache, column_idx: usize) storage_cache.Content {
    return if (tc.readsIds(column_idx)) .int_ids else .column;
}

const IdBlockValues = struct { bytes: []align(16) u8, encoding: format.Encoding };

/// A fixed-width column's block payload `payload`, an `int` block of
/// table ids (§4.7) of `encoding`, rebuilt over the values its ids name in
/// `lists`, or over the ids themselves as 8-byte values without: a raw or
/// FOR block as raw values, a run block as runs of them.
fn idBlockValues(
    allocator: Allocator,
    scratch: Allocator,
    io: Io,
    lists: ?*table_strings.TableStrings,
    column_idx: usize,
    payload: []const u8,
    encoding: format.Encoding,
    row_count: u32,
    has_nulls: bool,
) !IdBlockValues {
    const corrupt = format.Error.CorruptColumnBlockHeader;
    const width: usize = if (lists) |l| l.fixedListWidth(column_idx) orelse return corrupt else @sizeOf(i64);
    const bm_len: usize = if (has_nulls) column.bitmapBytes(row_count) else 0;
    if (payload.len < bm_len) return corrupt;
    const body = payload[bm_len..];
    switch (encoding) {
        .raw, .for_ => {
            const ids = try scratch.alloc(u32, row_count);
            defer scratch.free(ids);
            if (encoding == .raw) {
                if (body.len != ids.len * 4) return corrupt;
                for (ids, 0..) |*id, i| id.* = try tableIdOf(std.mem.readInt(i32, body[i * 4 ..][0..4], .little));
            } else {
                if (body.len < 20) return corrupt;
                const code_width = body[16];
                if (code_width == 0 or code_width > 8 or body.len != 20 + ids.len * code_width) return corrupt;
                const fb = forBlockOf(body, row_count);
                for (ids, 0..) |*id, i| id.* = try tableIdOf(fb.base + readForCode(fb.codes, fb.width, i));
            }
            const out = try allocator.alignedAlloc(u8, .@"16", bm_len + ids.len * width);
            errdefer allocator.free(out);
            @memcpy(out[0..bm_len], payload[0..bm_len]);
            try namedValues(io, lists, column_idx, ids, out[bm_len..]);
            return .{ .bytes = out, .encoding = .raw };
        },
        .rle => {
            if (body.len < 8 or body[4] != 4) return corrupt;
            const runs = rleBlockOf(body);
            if (body.len != 8 + @as(usize, runs.n_runs) * 8) return corrupt;
            const ids = try scratch.alloc(u32, runs.n_runs);
            defer scratch.free(ids);
            for (ids, 0..) |*id, i| id.* = try tableIdOf(std.mem.readInt(i32, runs.values[i * 4 ..][0..4], .little));
            const out = try allocator.alignedAlloc(u8, .@"16", bm_len + 8 + ids.len * (width + 4));
            errdefer allocator.free(out);
            @memcpy(out[0..bm_len], payload[0..bm_len]);
            const head = out[bm_len..][0..8];
            format.writeU32(head[0..4], runs.n_runs);
            head[4..8].* = .{ @intCast(width), 0, 0, 0 };
            const values = out[bm_len + 8 ..][0 .. ids.len * width];
            try namedValues(io, lists, column_idx, ids, values);
            @memcpy(out[bm_len + 8 + values.len ..], runs.lengths);
            return .{ .bytes = out, .encoding = .rle };
        },
        .dict, .fsst => return corrupt,
    }
}

/// The value each of `ids` names in `lists` into `out`, or without
/// lists the ids themselves as little-endian i64s.
fn namedValues(io: Io, lists: ?*table_strings.TableStrings, column_idx: usize, ids: []const u32, out: []u8) !void {
    const l = lists orelse {
        for (ids, 0..) |id, i| std.mem.writeInt(i64, out[i * 8 ..][0..8], id, .little);
        return;
    };
    try l.fixedValues(io, column_idx, ids, out);
}

fn tableIdOf(v: i128) !u32 {
    if (v < 0 or v > table_strings.id_cap) return format.Error.CorruptColumnBlockHeader;
    return @intCast(v);
}

/// Checks that `frame` is exactly one column block: its header, then the
/// stored bytes the header sizes.
fn checkFrame(frame: []const u8) !void {
    if (frame.len < format.column_block_header_size or
        frame.len != format.column_block_header_size + format.readU32(frame[8..12]))
    {
        return format.Error.CorruptColumnBlockHeader;
    }
}

/// Checks each part `form` reads of a segment-dictionary block, `buf`
/// holding the block from `start` bytes in, is one column block of that
/// encoding.
fn checkStringParts(buf: []const u8, start: u32, parts: format.StringBlockParts, form: segment_strings.Form) !void {
    for (std.enums.values(format.StringBlockPart)) |part| {
        const len = parts.lengths.get(part);
        if (!form.reads(part) or len == 0) continue;
        const frame = buf[parts.offset(part) - start ..][0..len];
        try checkFrame(frame);
        if (!isSegmentDictBlock(frame)) return format.Error.CorruptColumnBlockHeader;
    }
}

/// Decompresses the column block `frame` (header, then its stored bytes)
/// into `out`, its payload's size. `scratch` holds an `int_streams`
/// block's stored form while it is restored.
fn decompressBlockInto(scratch: Allocator, frame: []const u8, out: []u8) !void {
    const kind_byte = frame[0];
    if (kind_byte > @backingInt(format.Compression.lz4)) return format.Error.UnknownCompression;
    const kind: format.Compression = @fromBackingInt(@intCast(kind_byte));
    if (format.readU32(frame[4..8]) != out.len) return format.Error.CorruptColumnBlockHeader;
    const body = frame[format.column_block_header_size..];
    if (format.ColumnBlockFlags.fromByte(frame[1]).int_streams) return restoreIntStreams(scratch, kind, body, out);
    switch (kind) {
        .none => {
            if (body.len != out.len) return format.Error.CorruptColumnBlockHeader;
            @memcpy(out, body);
        },
        .zstd => try compression_mod.decompressInto(body, out),
        .lz4 => try compression_mod.lz4DecompressInto(body, out),
    }
}

/// The encoding of the payload a column block reads as: its header's
/// `encoding` byte, with a segment-dictionary block reading as the `.dict`
/// payload the reader rebuilds it into.
fn blockEncoding(bytes: []const u8, offset: usize) format.Encoding {
    const b = bytes[offset + format.column_block_encoding_offset];
    if (b == format.segment_dict_encoding) return .dict;
    // Unknown encodings degrade to raw; the writer fully controls this byte and
    // an unknown value would be caught by a downstream decode mismatch, but we
    // keep the reader total here rather than introducing a new error path for a
    // byte the writer fully controls.
    return if (b <= @backingInt(format.Encoding.fsst)) @fromBackingInt(@intCast(b)) else .raw;
}

fn decodeBlock(
    allocator: Allocator,
    bytes: []const u8,
    offset: usize,
    col_type: Type,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !OwnedColumn {
    var owned_raw: ?[]u8 = null;
    defer if (owned_raw) |r| allocator.free(r);

    const encoding = blockEncoding(bytes, offset);
    const raw = try readBlockRaw(allocator, bytes, offset, &owned_raw);
    return decodeColumnPayload(allocator, col_type, raw, row_count, flags, encoding);
}

/// Reads the block at `offset`, returns its decompressed bytes. If the block
/// is stored uncompressed, returns a slice into `bytes` (zero allocation) and
/// leaves `*owned_raw` as null. Otherwise allocates the decompressed buffer,
/// assigns it to `*owned_raw`, and returns it.
fn readBlockRaw(
    allocator: Allocator,
    bytes: []const u8,
    offset: usize,
    owned_raw: *?[]u8,
) ![]const u8 {
    const kind_byte = bytes[offset];
    if (kind_byte > @backingInt(format.Compression.lz4)) return format.Error.UnknownCompression;
    const kind: format.Compression = @fromBackingInt(@intCast(kind_byte));
    const uncompressed_size = format.readU32(bytes[offset + 4 .. offset + 8]);
    const compressed_size = format.readU32(bytes[offset + 8 .. offset + 12]);
    const payload_start = offset + format.column_block_header_size;
    const payload = bytes[payload_start .. payload_start + compressed_size];

    if (format.ColumnBlockFlags.fromByte(bytes[offset + 1]).int_streams) {
        const r = try allocator.alloc(u8, uncompressed_size);
        errdefer allocator.free(r);
        try restoreIntStreams(allocator, kind, payload, r);
        owned_raw.* = r;
        return r;
    }
    switch (kind) {
        .none => return payload,
        .zstd => {
            const r = try compression_mod.decompress(allocator, payload, uncompressed_size);
            owned_raw.* = r;
            return r;
        },
        .lz4 => {
            const r = try allocator.alloc(u8, uncompressed_size);
            errdefer allocator.free(r);
            try compression_mod.lz4DecompressInto(payload, r);
            owned_raw.* = r;
            return r;
        },
    }
}

/// Like `readBlockRaw` but ALWAYS returns owned, 16-byte-aligned bytes (caller
/// frees), so it can be inserted into the cache and viewed zero-copy. For
/// uncompressed blocks, allocates an aligned copy. `scratch` holds an
/// `int_streams` block's stored form while it is restored.
fn getDecompressedBytes(
    allocator: Allocator,
    scratch: Allocator,
    bytes: []const u8,
    offset: usize,
) ![]align(16) u8 {
    const kind_byte = bytes[offset];
    if (kind_byte > @backingInt(format.Compression.lz4)) return format.Error.UnknownCompression;
    const kind: format.Compression = @fromBackingInt(@intCast(kind_byte));
    const uncompressed_size = format.readU32(bytes[offset + 4 .. offset + 8]);
    const compressed_size = format.readU32(bytes[offset + 8 .. offset + 12]);
    const payload_start = offset + format.column_block_header_size;
    const payload = bytes[payload_start .. payload_start + compressed_size];

    if (format.ColumnBlockFlags.fromByte(bytes[offset + 1]).int_streams) {
        const dst = try allocator.alignedAlloc(u8, .@"16", uncompressed_size);
        errdefer allocator.free(dst);
        try restoreIntStreams(scratch, kind, payload, dst);
        return dst;
    }
    switch (kind) {
        .none => {
            const dst = try allocator.alignedAlloc(u8, .@"16", payload.len);
            @memcpy(dst, payload);
            return dst;
        },
        .zstd => return compression_mod.decompressAligned(allocator, payload, uncompressed_size),
        .lz4 => return compression_mod.lz4DecompressAligned(allocator, payload, uncompressed_size),
    }
}

/// Decompresses the stored payload of an `int_streams` block, whose stored
/// bytes are `body`, into the tail of `out` and restores its payload there
/// in place.
fn restoreIntStreams(allocator: Allocator, kind: format.Compression, body: []const u8, out: []u8) !void {
    const directory = try int_streams.readDirectory(body, out.len);
    const frame = body[directory.bytes.len..];
    const stored = out[out.len - directory.stored_len ..];
    switch (kind) {
        .none => {
            if (frame.len != stored.len) return format.Error.CorruptColumnBlockHeader;
            @memcpy(stored, frame);
        },
        .zstd => try compression_mod.decompressInto(frame, stored),
        .lz4 => try compression_mod.lz4DecompressInto(frame, stored),
    }
    try int_streams.restore(allocator, directory, out);
}

/// Decode a decompressed block payload into an OwnedColumn, dispatching on the
/// block's value encoding. Both encodings share the validity-bitmap prefix
/// convention (bitmap first when nullable); a `.for_` block's value region is
/// expanded back to the native-width values so the produced column is the SAME
/// native shape every consumer already expects.
pub fn decodeColumnPayload(
    allocator: Allocator,
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
    encoding: format.Encoding,
) !OwnedColumn {
    return switch (encoding) {
        .raw => decodeRawColumn(allocator, col_type, raw, row_count, flags),
        .for_ => decodeForColumn(allocator, col_type, raw, row_count, flags),
        .dict => decodeDictColumn(allocator, col_type, raw, row_count, flags),
        .rle => decodeRleColumn(allocator, col_type, raw, row_count, flags),
        .fsst => decodeFsstColumn(allocator, col_type, raw, row_count, flags),
    };
}

fn decodeRawColumn(
    allocator: Allocator,
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !OwnedColumn {
    // If has_nulls, the first `bitmapBytes(row_count)` bytes of `raw` are the
    // validity bitmap. Copy them out so OwnedColumn owns its bitmap, then
    // decode the rest as values.
    var nulls: ?[]u8 = null;
    errdefer if (nulls) |n| allocator.free(n);
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        const bm_copy = try allocator.alloc(u8, bm_len);
        @memcpy(bm_copy, raw[0..bm_len]);
        nulls = bm_copy;
        values = raw[bm_len..];
    }

    switch (col_type) {
        .int => return .{ .data = .{ .int = try decodeFixed(i32, allocator, values, row_count) }, .nulls = nulls },
        .bigint => return .{ .data = .{ .bigint = try decodeFixed(i64, allocator, values, row_count) }, .nulls = nulls },
        .boolean => {
            const data = try allocator.alloc(u8, row_count);
            errdefer allocator.free(data);
            @memcpy(data, values[0..row_count]);
            return .{ .data = .{ .boolean = data }, .nulls = nulls };
        },
        .varchar => return .{ .data = .{ .varchar = try decodeStringRaw(allocator, values, row_count) }, .nulls = nulls },
        .string => return .{ .data = .{ .string = try decodeStringRaw(allocator, values, row_count) }, .nulls = nulls },
        .float => return .{ .data = .{ .float = try decodeFixed(f32, allocator, values, row_count) }, .nulls = nulls },
        .double => return .{ .data = .{ .double = try decodeFixed(f64, allocator, values, row_count) }, .nulls = nulls },
        .date => return .{ .data = .{ .date = try decodeFixed(i32, allocator, values, row_count) }, .nulls = nulls },
        .datetime => return .{ .data = .{ .datetime = try decodeFixed(i64, allocator, values, row_count) }, .nulls = nulls },
        .tinyint => return .{ .data = .{ .tinyint = try decodeFixed(i8, allocator, values, row_count) }, .nulls = nulls },
        .smallint => return .{ .data = .{ .smallint = try decodeFixed(i16, allocator, values, row_count) }, .nulls = nulls },
        .largeint => return .{ .data = .{ .largeint = try decodeFixed(i128, allocator, values, row_count) }, .nulls = nulls },
        .char => return .{ .data = .{ .char = try decodeStringRaw(allocator, values, row_count) }, .nulls = nulls },
        .json => return .{ .data = .{ .json = try decodeStringRaw(allocator, values, row_count) }, .nulls = nulls },
        .nested => return .{ .data = .{ .nested = try decodeStringRaw(allocator, values, row_count) }, .nulls = nulls },
        .bitmap => return .{ .data = .{ .bitmap = try decodeStringRaw(allocator, values, row_count) }, .nulls = nulls },
        .decimal64 => return .{ .data = .{ .decimal64 = try decodeFixed(i64, allocator, values, row_count) }, .nulls = nulls },
        .decimal128 => return .{ .data = .{ .decimal128 = try decodeFixed(i128, allocator, values, row_count) }, .nulls = nulls },
        .decimal256 => return .{ .data = .{ .decimal256 = try decodeFixed(i256, allocator, values, row_count) }, .nulls = nulls },
        .uuid => return .{ .data = .{ .uuid = try decodeFixed(u128, allocator, values, row_count) }, .nulls = nulls },
    }
}

/// A FOR (Frame-of-Reference) block's value region, parsed in place over the
/// decompressed payload (past any validity bitmap): the `base`, the delta
/// `width` in bytes, and the `codes` (row_count × width narrow deltas). This is
/// the hook Phase 2B's FOR-aware filter consumes — it lets a predicate compare
/// against `const - base` over the narrow codes without expanding to native
/// width. In 2A it is exercised only by a unit test.
pub const ForBlock = struct {
    base: i128,
    width: u8,
    codes: []const u8,
};

/// Parse the FOR header + codes out of a decompressed block payload. `values`
/// is the payload past the validity bitmap (the caller strips the bitmap the
/// same way the raw path does). Layout: `[base i128 LE][width u8][3 pad][codes]`.
pub fn forBlockOf(values: []const u8, row_count: u32) ForBlock {
    const base = format.readI128(values[0..16]);
    const width = values[16];
    const codes_start = 16 + 1 + 3;
    const codes = values[codes_start .. codes_start + @as(usize, row_count) * width];
    return .{ .base = base, .width = width, .codes = codes };
}

/// Split a decompressed FOR block payload into (validity bitmap, ForBlock).
/// Mirrors the bitmap-stripping the raw/expand paths do, but keeps the codes
/// narrow — the Phase 2B FOR-aware filter compares against these directly.
pub const ForView = struct {
    nulls: ?[]const u8,
    block: ForBlock,
};

pub fn forViewOf(raw: []const u8, row_count: u32, flags: format.ColumnBlockFlags) ForView {
    var nulls: ?[]const u8 = null;
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        nulls = raw[0..bm_len];
        values = raw[bm_len..];
    }
    return .{ .nulls = nulls, .block = forBlockOf(values, row_count) };
}

/// Compare the narrow FOR codes against `want` under `op`, writing the result
/// into `mask` — never expanding to native width. The width tiers a v9 FOR
/// block can carry are u8/u16/u32 (8-byte deltas never beat the 8-byte native
/// width, so the writer never emits width 8). `want` is the constant already
/// translated into the code domain (`const - base`) by `translateForLeaf`,
/// which guarantees it is in `[0, max_code]` for this width — so the unsigned
/// comparison over codes is bit-for-bit identical to comparing the native
/// values. NULLs are not handled here; the caller ANDs the validity bitmap.
///
/// SIMD over an aligned codes slice; an unaligned codes start (the FOR payload
/// begins past a width-misaligned validity bitmap) falls back to a per-row
/// narrow load + scalar compare — still narrow-width, no native expansion.
/// The `@ptrCast`/`@alignCast` stays at this storage boundary.
pub fn forCompareInto(fb: ForBlock, op: simd_mod.CmpOp, want: u64, row_count: u32, mask: []bool) void {
    switch (fb.width) {
        1 => forCompareWidth(u8, fb.codes, @truncate(want), row_count, op, mask),
        2 => forCompareWidth(u16, fb.codes, @truncate(want), row_count, op, mask),
        4 => forCompareWidth(u32, fb.codes, @truncate(want), row_count, op, mask),
        // width 8 is never emitted by the writer; degrade safely by reading
        // each code wide rather than reaching for an unreachable.
        else => for (mask[0..row_count], 0..) |*m, i| {
            const code = readForCode(fb.codes, fb.width, i);
            m.* = cmpScalar(u64, code, want, op);
        },
    }
}

fn forCompareWidth(comptime U: type, codes: []const u8, want: U, row_count: u32, op: simd_mod.CmpOp, mask: []bool) void {
    const n: usize = row_count;
    if (@sizeOf(U) == 1) {
        const typed: []const U = codes[0..n];
        switch (op) {
            inline else => |o| simd_mod.compareInto(U, o, typed, want, mask[0..n]),
        }
        return;
    }
    if (@intFromPtr(codes.ptr) % @alignOf(U) == 0) {
        const typed: []const U = @as([*]const U, @ptrCast(@alignCast(codes.ptr)))[0..n];
        switch (op) {
            inline else => |o| simd_mod.compareInto(U, o, typed, want, mask[0..n]),
        }
        return;
    }
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const code = std.mem.readInt(U, codes[i * @sizeOf(U) ..][0..@sizeOf(U)], .little);
        mask[i] = cmpScalar(U, code, want, op);
    }
}

/// Largest code an IN-list's FOR membership bitmap covers (8 KiB of bits).
const FOR_IN_BITMAP_CODES: u64 = 1 << 16;

/// `mask[i]` = whether row i's code is one of `set` (ascending, non-empty,
/// already in the code domain) — one pass over the narrow codes however many
/// literals the IN-list carries. Sets whose codes all sit below 2^16 test a
/// bitmap; wider ones binary-search. NULLs are the caller's, as in
/// `forCompareInto`.
pub fn forInSetInto(fb: ForBlock, set: []const u64, row_count: u32, mask: []bool) void {
    const top = set[set.len - 1];
    var bitmap: [FOR_IN_BITMAP_CODES / 8]u8 = undefined;
    const bits: ?[]const u8 = if (top < FOR_IN_BITMAP_CODES) blk: {
        const used = bitmap[0 .. @as(usize, @intCast(top / 8)) + 1];
        @memset(used, 0);
        for (set) |c| used[@intCast(c / 8)] |= @as(u8, 1) << @intCast(c % 8);
        break :blk used;
    } else null;
    switch (fb.width) {
        1 => forInSetWidth(u8, fb.codes, set, bits, row_count, mask),
        2 => forInSetWidth(u16, fb.codes, set, bits, row_count, mask),
        4 => forInSetWidth(u32, fb.codes, set, bits, row_count, mask),
        else => for (mask[0..row_count], 0..) |*m, i| {
            m.* = codeInSet(readForCode(fb.codes, fb.width, i), set, bits);
        },
    }
}

fn forInSetWidth(comptime U: type, codes: []const u8, set: []const u64, bits: ?[]const u8, row_count: u32, mask: []bool) void {
    const n: usize = row_count;
    if (@sizeOf(U) == 1 or @intFromPtr(codes.ptr) % @alignOf(U) == 0) {
        const typed: []const U = @as([*]const U, @ptrCast(@alignCast(codes.ptr)))[0..n];
        if (bits) |b| {
            for (typed, mask[0..n]) |c, *m| m.* = bitmapHas(b, c);
        } else {
            for (typed, mask[0..n]) |c, *m| m.* = sortedHas(set, c);
        }
        return;
    }
    for (mask[0..n], 0..) |*m, i| {
        m.* = codeInSet(std.mem.readInt(U, codes[i * @sizeOf(U) ..][0..@sizeOf(U)], .little), set, bits);
    }
}

fn codeInSet(code: u64, set: []const u64, bits: ?[]const u8) bool {
    return if (bits) |b| bitmapHas(b, code) else sortedHas(set, code);
}

/// Branch-free: a code past the bitmap reads its last byte and masks the
/// answer off.
inline fn bitmapHas(bits: []const u8, code: u64) bool {
    const limit: u64 = bits.len * 8;
    const in_range = code < limit;
    const at: usize = @intCast(@min(code, limit - 1));
    return in_range and (bits[at / 8] >> @intCast(at % 8)) & 1 != 0;
}

fn sortedHas(sorted: []const u64, code: u64) bool {
    var base: usize = 0;
    var len = sorted.len;
    while (len > 1) {
        const half = len / 2;
        if (sorted[base + half] <= code) base += half;
        len -= half;
    }
    return sorted[base] == code;
}

fn cmpScalar(comptime U: type, a: U, b: U, op: simd_mod.CmpOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .lte => a <= b,
        .gt => a > b,
        .gte => a >= b,
    };
}

/// `out[k] = base + codes[rows[k]]` — the survivors' native values gathered
/// straight off the narrow deltas (no i128, no per-row temp copy).
pub fn forGatherRows(comptime T: type, fb: ForBlock, rows: []const u32, out: []T) void {
    const base: T = @intCast(fb.base);
    switch (fb.width) {
        1 => gatherRowsW(T, u8, base, fb.codes, rows, out),
        2 => gatherRowsW(T, u16, base, fb.codes, rows, out),
        4 => gatherRowsW(T, u32, base, fb.codes, rows, out),
        8 => gatherRowsW(T, u64, base, fb.codes, rows, out),
        else => unreachable,
    }
}

inline fn gatherRowsW(comptime T: type, comptime W: type, base: T, codes: []const u8, rows: []const u32, out: []T) void {
    const wsz = @sizeOf(W);
    for (rows, out) |r, *o| {
        const d = std.mem.readInt(W, codes[@as(usize, r) * wsz ..][0..wsz], .little);
        o.* = base +% @as(T, @intCast(d));
    }
}

/// Expand a FOR-encoded block back to its native-width values so the produced
/// OwnedColumn has the identical shape a raw block would. Reconstructs each row
/// as `base + delta`. NULL rows (masked by the validity bitmap) still get a
/// delta slot expanded into a placeholder value, which consumers ignore.
pub fn decodeForColumn(
    allocator: Allocator,
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !OwnedColumn {
    const _pt = if (prof.enabled) prof.nowTicks() else 0;
    defer if (prof.enabled) prof.add("for-decode (expand→native)", @intCast(@max(0, prof.nowTicks() - _pt)));

    var nulls: ?[]u8 = null;
    errdefer if (nulls) |n| allocator.free(n);
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        const bm_copy = try allocator.alloc(u8, bm_len);
        @memcpy(bm_copy, raw[0..bm_len]);
        nulls = bm_copy;
        values = raw[bm_len..];
    }

    const fb = forBlockOf(values, row_count);

    // The writer only ever FOR-encodes these integer-family fixed-width types.
    return switch (col_type) {
        .int => .{ .data = .{ .int = try expandFor(i32, allocator, fb, row_count) }, .nulls = nulls },
        .date => .{ .data = .{ .date = try expandFor(i32, allocator, fb, row_count) }, .nulls = nulls },
        .bigint => .{ .data = .{ .bigint = try expandFor(i64, allocator, fb, row_count) }, .nulls = nulls },
        .datetime => .{ .data = .{ .datetime = try expandFor(i64, allocator, fb, row_count) }, .nulls = nulls },
        .decimal64 => .{ .data = .{ .decimal64 = try expandFor(i64, allocator, fb, row_count) }, .nulls = nulls },
        .smallint => .{ .data = .{ .smallint = try expandFor(i16, allocator, fb, row_count) }, .nulls = nulls },
        .tinyint => .{ .data = .{ .tinyint = try expandFor(i8, allocator, fb, row_count) }, .nulls = nulls },
        .boolean => .{ .data = .{ .boolean = try expandFor(u8, allocator, fb, row_count) }, .nulls = nulls },
        else => unreachable,
    };
}

/// One narrow delta from the codes region (little-endian, `width` bytes).
fn readForCode(codes: []const u8, width: u8, row: usize) u64 {
    var b8: [8]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0 };
    const off = row * width;
    @memcpy(b8[0..width], codes[off .. off + width]);
    return std.mem.readInt(u64, &b8, .little);
}

fn expandFor(comptime T: type, allocator: Allocator, fb: ForBlock, row_count: u32) ![]T {
    const data = try allocator.alloc(T, row_count);
    fillFor(T, fb, data);
    return data;
}

/// Write `fb`'s values over `data`, a value per row.
fn fillFor(comptime T: type, fb: ForBlock, data: []T) void {
    const base: T = @intCast(fb.base);
    switch (fb.width) {
        1 => expandWidth(T, u8, base, fb.codes, data),
        2 => expandWidth(T, u16, base, fb.codes, data),
        4 => expandWidth(T, u32, base, fb.codes, data),
        8 => expandWidth(T, u64, base, fb.codes, data),
        else => unreachable,
    }
}

/// Reconstruct native values from the narrow unsigned deltas: `out[i] = base +
/// codes[i]`. FOR is only applied when `value = base + delta` provably fits `T`
/// (the writer narrows only when the span does), so the add runs in `T` — no
/// per-row i128. Vectorized in chunks; the deltas are read unaligned (the codes
/// region is not aligned in the decompressed buffer), then widened + added with
/// `base` broadcast.
inline fn expandWidth(comptime T: type, comptime W: type, base: T, codes: []const u8, out: []T) void {
    const wsz = @sizeOf(W);
    const N = std.simd.suggestVectorLength(T) orelse @max(1, 16 / @sizeOf(T));
    var i: usize = 0;
    if (N > 1) {
        const base_vec: @Vector(N, T) = @splat(base);
        const VecW = @Vector(N, W);
        while (i + N <= out.len) : (i += N) {
            const dv: VecW = @as(*align(1) const VecW, @ptrCast(codes.ptr + i * wsz)).*;
            const wide: @Vector(N, T) = @intCast(dv);
            out[i..][0..N].* = wide +% base_vec;
        }
    }
    while (i < out.len) : (i += 1) {
        const delta = std.mem.readInt(W, codes[i * wsz ..][0..wsz], .little);
        out[i] = base +% @as(T, @intCast(delta));
    }
}

/// An RLE block's value region, parsed in place over the decompressed payload
/// (past any validity bitmap): `n_runs` runs as two parallel arrays — each
/// run's value (native element width) and its row count. This is the hook
/// run-aware kernels consume: a predicate evaluates once per run and a
/// SUM/COUNT folds `value × length` without expanding to row width.
pub const RleBlock = struct {
    n_runs: u32,
    value_width: u8,
    /// n_runs × value_width, native little-endian.
    values: []const u8,
    /// n_runs × 4, u32 little-endian.
    lengths: []const u8,

    pub fn runLength(self: RleBlock, run: usize) u32 {
        return std.mem.readInt(u32, self.lengths[run * 4 ..][0..4], .little);
    }
};

/// Parse the RLE header + run arrays out of a decompressed block payload.
/// `values` is the payload past the validity bitmap. Layout:
/// `[n_runs u32][value_width u8][3 pad][run values][run lengths u32]`.
pub fn rleBlockOf(values: []const u8) RleBlock {
    const n_runs = format.readU32(values[0..4]);
    const width = values[4];
    const vals_start = 4 + 1 + 3;
    const vals_len = @as(usize, n_runs) * width;
    return .{
        .n_runs = n_runs,
        .value_width = width,
        .values = values[vals_start .. vals_start + vals_len],
        .lengths = values[vals_start + vals_len .. vals_start + vals_len + @as(usize, n_runs) * 4],
    };
}

/// Split a decompressed RLE block payload into (validity bitmap, RleBlock),
/// mirroring `forViewOf`.
pub const RleView = struct {
    nulls: ?[]const u8,
    block: RleBlock,
};

pub fn rleViewOf(raw: []const u8, row_count: u32, flags: format.ColumnBlockFlags) RleView {
    var nulls: ?[]const u8 = null;
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        nulls = raw[0..bm_len];
        values = raw[bm_len..];
    }
    return .{ .nulls = nulls, .block = rleBlockOf(values) };
}

/// Expand an RLE-encoded block back to its native-width values so the produced
/// OwnedColumn has the identical shape a raw block would. The writer encodes
/// the stored value stream as-is (NULL placeholders included), so expansion
/// reproduces it exactly; the validity bitmap stays orthogonal.
pub fn decodeRleColumn(
    allocator: Allocator,
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !OwnedColumn {
    const _pt = if (prof.enabled) prof.nowTicks() else 0;
    defer if (prof.enabled) prof.add("rle-decode (expand→native)", @intCast(@max(0, prof.nowTicks() - _pt)));

    var nulls: ?[]u8 = null;
    errdefer if (nulls) |n| allocator.free(n);
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        const bm_copy = try allocator.alloc(u8, bm_len);
        @memcpy(bm_copy, raw[0..bm_len]);
        nulls = bm_copy;
        values = raw[bm_len..];
    }

    const rb = rleBlockOf(values);

    // The writer only ever RLE-encodes these integer-family fixed-width types.
    return switch (col_type) {
        .int => .{ .data = .{ .int = try expandRle(i32, allocator, rb, row_count) }, .nulls = nulls },
        .date => .{ .data = .{ .date = try expandRle(i32, allocator, rb, row_count) }, .nulls = nulls },
        .bigint => .{ .data = .{ .bigint = try expandRle(i64, allocator, rb, row_count) }, .nulls = nulls },
        .datetime => .{ .data = .{ .datetime = try expandRle(i64, allocator, rb, row_count) }, .nulls = nulls },
        .decimal64 => .{ .data = .{ .decimal64 = try expandRle(i64, allocator, rb, row_count) }, .nulls = nulls },
        .smallint => .{ .data = .{ .smallint = try expandRle(i16, allocator, rb, row_count) }, .nulls = nulls },
        .tinyint => .{ .data = .{ .tinyint = try expandRle(i8, allocator, rb, row_count) }, .nulls = nulls },
        .boolean => .{ .data = .{ .boolean = try expandRle(u8, allocator, rb, row_count) }, .nulls = nulls },
        else => unreachable,
    };
}

fn expandRle(comptime T: type, allocator: Allocator, rb: RleBlock, row_count: u32) ![]T {
    const data = try allocator.alloc(T, row_count);
    fillRuns(T, rb, data);
    return data;
}

/// Expand an RLE or FOR block of an integer-family column into `buf`, grown
/// to fit and reused across calls, and view the values there; the view's
/// nulls alias `raw`. Null for any other encoding or type.
pub fn expandBlockInto(
    allocator: Allocator,
    buf: *std.ArrayListUnmanaged(u64),
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
    encoding: format.Encoding,
) !?ColumnView {
    var nulls: ?[]const u8 = null;
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        nulls = raw[0..bm_len];
        values = raw[bm_len..];
    }
    const packed_values: PackedValues = switch (encoding) {
        .rle => .{ .rle = rleBlockOf(values) },
        .for_ => .{ .for_ = forBlockOf(values, row_count) },
        else => return null,
    };
    return switch (col_type) {
        .int => .{ .data = .{ .int = try expandInto(i32, allocator, buf, packed_values, row_count) }, .nulls = nulls },
        .date => .{ .data = .{ .date = try expandInto(i32, allocator, buf, packed_values, row_count) }, .nulls = nulls },
        .bigint => .{ .data = .{ .bigint = try expandInto(i64, allocator, buf, packed_values, row_count) }, .nulls = nulls },
        .datetime => .{ .data = .{ .datetime = try expandInto(i64, allocator, buf, packed_values, row_count) }, .nulls = nulls },
        .decimal64 => .{ .data = .{ .decimal64 = try expandInto(i64, allocator, buf, packed_values, row_count) }, .nulls = nulls },
        .smallint => .{ .data = .{ .smallint = try expandInto(i16, allocator, buf, packed_values, row_count) }, .nulls = nulls },
        .tinyint => .{ .data = .{ .tinyint = try expandInto(i8, allocator, buf, packed_values, row_count) }, .nulls = nulls },
        .boolean => .{ .data = .{ .boolean = try expandInto(u8, allocator, buf, packed_values, row_count) }, .nulls = nulls },
        else => null,
    };
}

const PackedValues = union(enum) { rle: RleBlock, for_: ForBlock };

fn expandInto(comptime T: type, allocator: Allocator, buf: *std.ArrayListUnmanaged(u64), packed_values: PackedValues, row_count: u32) ![]T {
    const len = @as(usize, row_count) * @sizeOf(T);
    try buf.resize(allocator, (len + @sizeOf(u64) - 1) / @sizeOf(u64));
    const data: []T = std.mem.bytesAsSlice(T, std.mem.sliceAsBytes(buf.items)[0..len]);
    switch (packed_values) {
        .rle => |rb| fillRuns(T, rb, data),
        .for_ => |fb| fillFor(T, fb, data),
    }
    return data;
}

/// Write `rb`'s runs over `data`, a value per row.
fn fillRuns(comptime T: type, rb: RleBlock, data: []T) void {
    // A run this short is written as one full-width store whose overhang the
    // next run overwrites: runs of a few rows otherwise pay a fill loop each.
    const short = 32 / @sizeOf(T);
    const row_count = data.len;
    var pos: usize = 0;
    var run: usize = 0;
    while (run < rb.n_runs) : (run += 1) {
        const v = std.mem.readInt(T, rb.values[run * @sizeOf(T) ..][0..@sizeOf(T)], .little);
        // Clamp against a corrupt length so expansion can never write past the
        // block's row count; a short fill surfaces as a value mismatch, not UB.
        const len = @min(@as(usize, rb.runLength(run)), row_count - pos);
        if (len <= short and pos + short <= row_count) {
            data[pos..][0..short].* = @splat(v);
        } else {
            @memset(data[pos .. pos + len], v);
        }
        pos += len;
        if (pos == row_count) break;
    }
}

/// Build a BORROWED `ColumnView` over the raw decompressed block bytes —
/// zero allocation, the view aliases `raw` directly. Used by the scan-side
/// in-place filter fast path: the view is created over PINNED cache bytes,
/// used for predicate evaluation + survivor compaction, and dropped within a
/// single `next()` call. It must never be handed to a downstream operator
/// unless its caller keeps the block pinned for as long as the view lives.
///
/// Returns `null` when a typed view can't be formed safely:
///   - a `.for_` block: its bytes are narrow deltas, not native-width values,
///     so it has no in-place native view — the caller falls back to the
///     owned-decode (expand) path,
///   - a fixed-width element slice would be misaligned (`@alignCast` of
///     misaligned bytes is UB, so we decline and the caller falls back to the
///     owned-copy decode path),
///   - on a big-endian host (the borrowed view assumes the on-disk
///     little-endian layout is bit-identical to the in-memory `[]T`).
///
/// Only this `storage/` boundary performs the `@ptrCast`/`@alignCast`.
pub fn viewRawColumn(
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
    encoding: format.Encoding,
) ?ColumnView {
    if (native_endian != .little) return null;
    if (encoding != .raw) return null;

    var nulls: ?[]const u8 = null;
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        nulls = raw[0..bm_len];
        values = raw[bm_len..];
    }

    switch (col_type) {
        .int => return fixedView(i32, values, row_count, nulls, .int),
        .bigint => return fixedView(i64, values, row_count, nulls, .bigint),
        .boolean => return .{ .data = .{ .boolean = values[0..row_count] }, .nulls = nulls },
        .float => return fixedView(f32, values, row_count, nulls, .float),
        .double => return fixedView(f64, values, row_count, nulls, .double),
        .date => return fixedView(i32, values, row_count, nulls, .date),
        .datetime => return fixedView(i64, values, row_count, nulls, .datetime),
        .tinyint => return .{ .data = .{ .tinyint = @ptrCast(values[0..row_count]) }, .nulls = nulls },
        .smallint => return fixedView(i16, values, row_count, nulls, .smallint),
        .largeint => return fixedView(i128, values, row_count, nulls, .largeint),
        .decimal64 => return fixedView(i64, values, row_count, nulls, .decimal64),
        .decimal128 => return fixedView(i128, values, row_count, nulls, .decimal128),
        .decimal256 => return fixedView(i256, values, row_count, nulls, .decimal256),
        .uuid => return fixedView(u128, values, row_count, nulls, .uuid),
        .varchar => return stringView(values, row_count, nulls, .varchar),
        .string => return stringView(values, row_count, nulls, .string),
        .char => return stringView(values, row_count, nulls, .char),
        .json => return stringView(values, row_count, nulls, .json),
        .nested => return stringView(values, row_count, nulls, .nested),
        .bitmap => return stringView(values, row_count, nulls, .bitmap),
    }
}

/// Reinterpret `values` as `[]const T` with a runtime alignment guard. Returns
/// null when the byte slice isn't aligned for `T` (caller falls back to copy).
fn fixedView(
    comptime T: type,
    values: []const u8,
    row_count: u32,
    nulls: ?[]const u8,
    comptime tag: std.meta.Tag(column.ValueView),
) ?ColumnView {
    if (@intFromPtr(values.ptr) % @alignOf(T) != 0) return null;
    const typed: []const T = @as([*]const T, @ptrCast(@alignCast(values.ptr)))[0..row_count];
    return .{ .data = @unionInit(column.ValueView, @tagName(tag), typed), .nulls = nulls };
}

/// Build a borrowed `StringView` over `values` laid out as
/// `[u32 byte_count][(n+1) u32 offsets][bytes]`. Returns null when the offset
/// array isn't u32-aligned in place.
fn stringView(
    values: []const u8,
    row_count: u32,
    nulls: ?[]const u8,
    comptime tag: std.meta.Tag(column.ValueView),
) ?ColumnView {
    const off_start = 4;
    const off_count = @as(usize, row_count) + 1;
    const off_bytes = off_count * 4;
    const offsets_ptr = values.ptr + off_start;
    if (@intFromPtr(offsets_ptr) % @alignOf(u32) != 0) return null;
    const offsets: []const u32 = @as([*]const u32, @ptrCast(@alignCast(offsets_ptr)))[0..off_count];
    const byte_count = offsets[row_count];
    const data_start = off_start + off_bytes;
    const bytes = values[data_start .. data_start + byte_count];
    const sv = StringView{ .offsets = offsets, .bytes = bytes };
    return .{ .data = @unionInit(column.ValueView, @tagName(tag), sv), .nulls = nulls };
}

fn decodeStringRaw(allocator: Allocator, raw: []const u8, row_count: u32) !OwnedStringColumn {
    var cursor: usize = 0;
    const byte_count = format.readU32(raw[cursor .. cursor + 4]);
    cursor += 4;

    const offsets = try allocator.alloc(u32, @as(usize, row_count) + 1);
    errdefer allocator.free(offsets);
    const off_bytes = offsets.len * 4;
    if (native_endian == .little) {
        @memcpy(std.mem.sliceAsBytes(offsets), raw[cursor .. cursor + off_bytes]);
    } else {
        for (offsets, 0..) |*slot, i| {
            slot.* = format.readU32(raw[cursor + i * 4 .. cursor + i * 4 + 4]);
        }
    }
    cursor += off_bytes;

    const data = try allocator.alloc(u8, byte_count);
    errdefer allocator.free(data);
    @memcpy(data, raw[cursor .. cursor + byte_count]);

    return .{ .offsets = offsets, .bytes = data };
}

/// Separate counters for adjacent rows: a clustered column repeats one code
/// for long runs, and a single counter array would serialize every
/// increment on the one before it.
const CODE_COUNT_LANES: usize = 4;

/// Adds each row's code to `counts`, which holds `lanes` arrays of `ndv`
/// counters; a code's rows are the sum over the arrays.
fn countCodes(comptime Code: type, comptime lanes: usize, codes: []const u8, row_count: usize, ndv: usize, counts: []u32) void {
    const width = @sizeOf(Code);
    var row: usize = 0;
    while (row + lanes <= row_count) : (row += lanes) {
        inline for (0..lanes) |lane| {
            counts[lane * ndv + std.mem.readInt(Code, codes[(row + lane) * width ..][0..width], .little)] += 1;
        }
    }
    while (row < row_count) : (row += 1) {
        counts[std.mem.readInt(Code, codes[row * width ..][0..width], .little)] += 1;
    }
}

/// `countCodes`, also leaving in `rows[c]` the last row holding code c.
fn countEntriesAs(comptime Code: type, comptime lanes: usize, codes: []const u8, row_count: usize, ndv: usize, counts: []u32, rows: []u32) void {
    const width = @sizeOf(Code);
    var row: usize = 0;
    while (row + lanes <= row_count) : (row += lanes) {
        inline for (0..lanes) |lane| {
            const c = std.mem.readInt(Code, codes[(row + lane) * width ..][0..width], .little);
            counts[lane * ndv + c] += 1;
            rows[c] = @intCast(row + lane);
        }
    }
    while (row < row_count) : (row += 1) {
        const c = std.mem.readInt(Code, codes[row * width ..][0..width], .little);
        counts[c] += 1;
        rows[c] = @intCast(row);
    }
}

/// A segment-local string dictionary block's regions, parsed in place over the
/// decompressed payload (past any validity bitmap). `dict_bytes` holds the `ndv`
/// distinct values concatenated in sorted order; `offsets_region` is the
/// `(ndv+1) × u32` rebased offset array (read via `format.readU32` so it tolerates
/// the payload's arbitrary alignment); `codes` is `row_count × code_width`. This
/// is the seam a later dictionary-aware execution layer consumes to operate on
/// codes directly (global-code stitch, predicate-per-distinct); the Phase 3
/// reader only uses it to rebuild the raw string shape.
pub const DictBlock = struct {
    ndv: u32,
    code_width: u8,
    offsets_region: []const u8,
    dict_bytes: []const u8,
    codes: []const u8,
    /// Per value, its row count less one and its first row, `stat_width`
    /// bytes each, when the block stores them (`format.DICT_ENTRY_STATS`).
    entry_counts: []const u8 = &.{},
    entry_rows: []const u8 = &.{},
    stat_width: u8 = 0,
    /// Each value's segment id (`ndv` × u32), when the block was rebuilt from
    /// a segment string table (`format.DICT_SEGMENT_IDS`).
    segment_ids: []const u8 = &.{},

    /// Byte length of distinct value at sorted index `code`, which a block
    /// rebuilt without its values' bytes (`format.DICT_NO_BYTES`) still has.
    pub fn valueLen(self: DictBlock, code: u32) u32 {
        const a = format.readU32(self.offsets_region[@as(usize, code) * 4 ..][0..4]);
        const b = format.readU32(self.offsets_region[(@as(usize, code) + 1) * 4 ..][0..4]);
        return b - a;
    }

    /// Byte slice of distinct value at sorted index `code` (aliases `dict_bytes`).
    pub fn dictValue(self: DictBlock, code: u32) []const u8 {
        const a = format.readU32(self.offsets_region[@as(usize, code) * 4 ..][0..4]);
        const b = format.readU32(self.offsets_region[(@as(usize, code) + 1) * 4 ..][0..4]);
        return self.dict_bytes[a..b];
    }

    /// `offsets_region` as native u32s in place, when the payload happens to
    /// be u32-aligned on a little-endian host; else null (copy it through
    /// `format.readU32`).
    pub fn offsetsInPlace(self: DictBlock) ?[]const u32 {
        if (comptime builtin.cpu.arch.endian() != .little) return null;
        if (!std.mem.isAligned(@intFromPtr(self.offsets_region.ptr), @alignOf(u32))) return null;
        const aligned: []align(@alignOf(u32)) const u8 = @alignCast(self.offsets_region);
        return std.mem.bytesAsSlice(u32, aligned);
    }

    /// Per-row code (little-endian, `code_width` bytes). Fixed-width reads per
    /// writer width: a runtime-length copy compiles to a memcpy call per row.
    pub fn rowCode(self: DictBlock, row: usize) u32 {
        switch (self.code_width) {
            1 => return self.codes[row],
            2 => return std.mem.readInt(u16, self.codes[row * 2 ..][0..2], .little),
            4 => return std.mem.readInt(u32, self.codes[row * 4 ..][0..4], .little),
            else => {
                var b4: [4]u8 = .{ 0, 0, 0, 0 };
                const off = row * self.code_width;
                @memcpy(b4[0..self.code_width], self.codes[off .. off + self.code_width]);
                return std.mem.readInt(u32, &b4, .little);
            },
        }
    }

    /// `rowCode` for a block known to carry `width`-byte codes, so a per-row
    /// loop switches on the width once instead of on every row.
    pub inline fn rowCodeAs(self: DictBlock, comptime width: u8, row: usize) u32 {
        return std.mem.readInt(@Int(.unsigned, @as(u16, width) * 8), self.codes[row * width ..][0..width], .little);
    }

    /// Every row's code, into `out`, one per row.
    pub fn codesInto(self: DictBlock, out: []u32) void {
        switch (self.code_width) {
            inline 1, 2, 4 => |w| widenCodes(@Int(.unsigned, @as(u16, w) * 8), self.codes, out),
            else => for (out, 0..) |*c, row| {
                c.* = self.rowCode(row);
            },
        }
    }

    /// The codes of `rows`, into `out`.
    pub fn codesAt(self: DictBlock, rows: []const u32, out: []u32) void {
        switch (self.code_width) {
            inline 1, 2, 4 => |w| for (rows, out) |row, *c| {
                c.* = self.rowCodeAs(w, row);
            },
            else => for (rows, out) |row, *c| {
                c.* = self.rowCode(row);
            },
        }
    }

    pub fn hasEntryStats(self: DictBlock) bool {
        return self.stat_width != 0;
    }

    pub fn hasSegmentIds(self: DictBlock) bool {
        return self.segment_ids.len != 0;
    }

    /// The segment id of the value at sorted index `code`, for a block with
    /// `hasSegmentIds`.
    pub fn segmentId(self: DictBlock, code: u32) u32 {
        return format.readU32(self.segment_ids[@as(usize, code) * 4 ..][0..4]);
    }

    /// The block's ids and stats as `EntryCounts`, for a block with
    /// `hasSegmentIds` and `hasEntryStats`.
    pub fn entryCounts(self: DictBlock) segment_strings.EntryCounts {
        return .{ .ndv = self.ndv, .segment_ids = self.segment_ids, .entry_counts = self.entry_counts, .entry_rows = self.entry_rows, .stat_width = self.stat_width };
    }

    /// The stored row count of each value into `counts` and its first row
    /// into `rows`, as `countEntries` counts them, for a block with
    /// `hasEntryStats`.
    pub fn entryStatsInto(self: DictBlock, counts: []u32, rows: []u32) void {
        switch (self.stat_width) {
            inline 2, 4 => |w| {
                const T = @Int(.unsigned, @as(u16, w) * 8);
                for (counts, 0..) |*n, c| n.* = @as(u32, std.mem.readInt(T, self.entry_counts[c * w ..][0..w], .little)) + 1;
                for (rows, 0..) |*r, c| r.* = std.mem.readInt(T, self.entry_rows[c * w ..][0..w], .little);
            },
            else => unreachable,
        }
    }

    /// Into `entries`, the value each of `rows` (ascending) is the first row
    /// of, from the stored first rows alone, for a block with
    /// `hasEntryStats`; false when a row is no value's first row.
    pub fn firstRowEntries(self: DictBlock, rows: []const u32, entries: []u32) bool {
        const order = struct {
            fn f(target: u32, item: u32) std.math.Order {
                return std.math.order(target, item);
            }
        }.f;
        var matched: usize = 0;
        switch (self.stat_width) {
            inline 2, 4 => |w| {
                const T = @Int(.unsigned, @as(u16, w) * 8);
                for (0..self.ndv) |c| {
                    if (matched == rows.len) break;
                    const first: u32 = std.mem.readInt(T, self.entry_rows[c * w ..][0..w], .little);
                    var j = std.sort.lowerBound(u32, rows, first, order);
                    while (j < rows.len and rows[j] == first) : (j += 1) {
                        entries[j] = @intCast(c);
                        matched += 1;
                    }
                }
            },
            else => unreachable,
        }
        return matched == rows.len;
    }

    /// The rows holding the values `pass` flags, from the stored counts, for
    /// a block with `hasEntryStats`.
    pub fn rowsPassing(self: DictBlock, pass: []const bool) usize {
        var rows: usize = 0;
        switch (self.stat_width) {
            inline 2, 4 => |w| {
                const T = @Int(.unsigned, @as(u16, w) * 8);
                for (pass, 0..) |p, c| {
                    if (p) rows += @as(usize, std.mem.readInt(T, self.entry_counts[c * w ..][0..w], .little)) + 1;
                }
            },
            else => unreachable,
        }
        return rows;
    }

    /// The counter arrays `countEntries` takes for `row_count` rows: separate
    /// ones break the chain of increments a repeated code makes, which pays
    /// where codes repeat often enough to cover the extra arrays.
    pub fn entryCountLanes(self: DictBlock, row_count: u32) usize {
        return if (self.code_width <= 2 and 2 * @as(usize, self.ndv) <= row_count) CODE_COUNT_LANES else 1;
    }

    /// Each code's row count into `counts[0..ndv]`, and into `rows[c]` a row
    /// holding code c where any does. `counts` holds `entryCountLanes`
    /// arrays of `ndv` counters, zeroed.
    pub fn countEntries(self: DictBlock, row_count: u32, counts: []u32, rows: []u32) void {
        const ndv: usize = self.ndv;
        const lanes = self.entryCountLanes(row_count);
        switch (self.code_width) {
            inline 1, 2 => |w| {
                const Code = @Int(.unsigned, @as(u16, w) * 8);
                if (lanes == CODE_COUNT_LANES)
                    countEntriesAs(Code, CODE_COUNT_LANES, self.codes, row_count, ndv, counts, rows)
                else
                    countEntriesAs(Code, 1, self.codes, row_count, ndv, counts, rows);
            },
            4 => countEntriesAs(u32, 1, self.codes, row_count, ndv, counts, rows),
            else => for (0..row_count) |row| {
                const c = self.rowCode(row);
                counts[c] += 1;
                rows[c] = @intCast(row);
            },
        }
        for (1..lanes) |lane| {
            for (counts[0..ndv], counts[lane * ndv ..][0..ndv]) |*total, count| total.* += count;
        }
    }

    /// `mask[i]` = whether row i's code lies in `[lo, lo + span)`, or outside
    /// it with `outside`.
    pub fn codeRangeInto(self: DictBlock, lo: u32, span: u32, outside: bool, row_count: u32, mask: []bool) void {
        const out = mask[0..row_count];
        switch (self.code_width) {
            inline 1, 2, 4 => |w| {
                const T = @Int(.unsigned, @as(u16, w) * 8);
                // A span of every code a width holds doesn't fit that width.
                if (span == 0 or span > std.math.maxInt(T)) return simd_mod.fillMaskRun(out, 0, out.len, (span != 0) != outside);
                switch (outside) {
                    inline else => |o| codeRangeWidth(T, o, self.codes, @intCast(lo), @intCast(span), out),
                }
            },
            else => for (out, 0..) |*m, i| {
                m.* = (self.rowCode(i) -% lo < span) != outside;
            },
        }
    }
};

/// One wrapping subtract and one unsigned compare per code, a vector of codes
/// at a time: the per-row loop stays scalar. Codes sit at any byte offset, so
/// a vector's worth is read as bytes.
fn codeRangeWidth(comptime T: type, comptime outside: bool, codes: []const u8, lo: T, span: T, mask: []bool) void {
    const W = @sizeOf(T);
    const N = comptime std.simd.suggestVectorLength(T) orelse 16 / W;
    var i: usize = 0;
    if (native_endian == .little and N > 1) {
        const lo_v: @Vector(N, T) = @splat(lo);
        const span_v: @Vector(N, T) = @splat(span);
        while (i + N <= mask.len) : (i += N) {
            const bytes: [N * W]u8 = codes[i * W ..][0 .. N * W].*;
            const v: @Vector(N, T) = @bitCast(bytes);
            const inside = v -% lo_v < span_v;
            mask[i..][0..N].* = if (outside) !inside else inside;
        }
    }
    while (i < mask.len) : (i += 1) {
        mask[i] = (std.mem.readInt(T, codes[i * W ..][0..W], .little) -% lo < span) != outside;
    }
}

/// Widens `T`-wide codes into `out`, a vector at a time; codes sit at any
/// byte offset, so a vector's worth is read as bytes.
fn widenCodes(comptime T: type, codes: []const u8, out: []u32) void {
    const W = @sizeOf(T);
    const N = comptime std.simd.suggestVectorLength(u32) orelse 4;
    var i: usize = 0;
    if (native_endian == .little and N > 1) {
        while (i + N <= out.len) : (i += N) {
            const bytes: [N * W]u8 = codes[i * W ..][0 .. N * W].*;
            const v: @Vector(N, T) = @bitCast(bytes);
            out[i..][0..N].* = @as(@Vector(N, u32), v);
        }
    }
    while (i < out.len) : (i += 1) out[i] = std.mem.readInt(T, codes[i * W ..][0..W], .little);
}

/// Parse the dict header + regions out of a decompressed block payload. `values`
/// is the payload past the validity bitmap (the caller strips the bitmap the
/// same way the raw/FOR paths do). Layout documented on `format.Encoding.dict`.
pub fn dictBlockOf(values: []const u8, row_count: u32) DictBlock {
    const ndv = format.readU32(values[0..4]);
    const code_width = values[4];
    const off_start = 8;
    const off_bytes = (@as(usize, ndv) + 1) * 4;
    const offsets_region = values[off_start .. off_start + off_bytes];
    const dict_byte_count: usize = if (values[5] & format.DICT_NO_BYTES != 0)
        0
    else
        format.readU32(values[off_start + @as(usize, ndv) * 4 ..][0..4]);
    const dict_start = off_start + off_bytes;
    const dict_bytes = values[dict_start .. dict_start + dict_byte_count];
    const codes_start = dict_start + dict_byte_count;
    const codes_len: usize = if (values[5] & format.DICT_NO_CODES != 0) 0 else @as(usize, row_count) * code_width;
    const codes = values[codes_start .. codes_start + codes_len];
    var db: DictBlock = .{
        .ndv = ndv,
        .code_width = code_width,
        .offsets_region = offsets_region,
        .dict_bytes = dict_bytes,
        .codes = codes,
    };
    var stats_end = codes_start + codes.len;
    if (values[5] & format.DICT_ENTRY_STATS != 0) {
        const w = format.dictEntryStatWidth(row_count);
        const len = @as(usize, ndv) * w;
        db.entry_counts = values[stats_end..][0..len];
        db.entry_rows = values[stats_end + len ..][0..len];
        db.stat_width = w;
        stats_end += 2 * len;
    }
    if (values[5] & format.DICT_SEGMENT_IDS != 0) db.segment_ids = values[stats_end..][0 .. @as(usize, ndv) * 4];
    return db;
}

/// Decode a dict-encoded string block into an OwnedColumn whose shape is
/// IDENTICAL to what a raw string block produces — every consumer is unchanged.
/// Reconstructs each row by indexing the sorted dict with the row's code. NULL
/// rows (masked by the validity bitmap) reconstruct their placeholder code's
/// value, which consumers ignore (same convention as the FOR path).
pub fn decodeDictColumn(
    allocator: Allocator,
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !OwnedColumn {
    const _pt = if (prof.enabled) prof.nowTicks() else 0;
    defer if (prof.enabled) prof.add("dict-decode (expand→strings)", @intCast(@max(0, prof.nowTicks() - _pt)));

    var nulls: ?[]u8 = null;
    errdefer if (nulls) |n| allocator.free(n);
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        const bm_copy = try allocator.alloc(u8, bm_len);
        @memcpy(bm_copy, raw[0..bm_len]);
        nulls = bm_copy;
        values = raw[bm_len..];
    }

    const db = dictBlockOf(values, row_count);
    const sc = try expandDict(allocator, db, row_count);

    // The writer only ever dict-encodes string-family columns.
    return switch (col_type) {
        .varchar => .{ .data = .{ .varchar = sc }, .nulls = nulls },
        .string => .{ .data = .{ .string = sc }, .nulls = nulls },
        .char => .{ .data = .{ .char = sc }, .nulls = nulls },
        .json => .{ .data = .{ .json = sc }, .nulls = nulls },
        .nested => .{ .data = .{ .nested = sc }, .nulls = nulls },
        .bitmap => .{ .data = .{ .bitmap = sc }, .nulls = nulls },
        else => unreachable,
    };
}

/// Rebuild the `[offsets][bytes]` owned string layout from dict codes. Two
/// passes: size the per-row offsets, then copy each row's dict value in.
fn expandDict(allocator: Allocator, db: DictBlock, row_count: u32) !OwnedStringColumn {
    const offsets = try allocator.alloc(u32, @as(usize, row_count) + 1);
    errdefer allocator.free(offsets);

    var total: usize = 0;
    offsets[0] = 0;
    var row: usize = 0;
    while (row < row_count) : (row += 1) {
        total += db.dictValue(db.rowCode(row)).len;
        offsets[row + 1] = @intCast(total);
    }

    const bytes = try allocator.alloc(u8, total);
    errdefer allocator.free(bytes);
    row = 0;
    while (row < row_count) : (row += 1) {
        const v = db.dictValue(db.rowCode(row));
        @memcpy(bytes[offsets[row]..offsets[row + 1]], v);
    }

    return .{ .offsets = offsets, .bytes = bytes };
}

/// An FSST block's regions, parsed in place over the decompressed payload
/// (past any validity bitmap). The symbol table is rebuilt by value (it is a
/// few KB of plain arrays); the per-row compressed slices alias the payload.
/// This is the seam compressed-domain consumers use: digest-while-decode via
/// `table.decodeStream(rowComp(r), sink)`, per-segment equality via
/// memcmp over `rowComp`, and one-row decode for late materialization.
pub const FsstBlock = struct {
    table: fsst.SymbolTable,
    raw_byte_count: u32,
    /// (row_count+1) × u32 little-endian, rebased to 0.
    offsets_region: []const u8,
    comp_bytes: []const u8,

    pub fn rowComp(self: *const FsstBlock, row: usize) []const u8 {
        const a = format.readU32(self.offsets_region[row * 4 ..][0..4]);
        const b = format.readU32(self.offsets_region[(row + 1) * 4 ..][0..4]);
        return self.comp_bytes[a..b];
    }
};

/// Parse the FSST regions out of a decompressed block payload. `values` is the
/// payload past the validity bitmap. Layout documented on
/// `format.Encoding.fsst`. Errors on a corrupt symbol table rather than
/// risking out-of-bounds symbol lengths.
pub fn fsstBlockOf(values: []const u8, row_count: u32) !FsstBlock {
    const table_len = format.readU32(values[0..4]);
    const table = try fsst.SymbolTable.deserialize(values[4 .. 4 + table_len]);
    var cur: usize = 4 + table_len;
    const raw_byte_count = format.readU32(values[cur..][0..4]);
    cur += 4;
    const comp_byte_count = format.readU32(values[cur..][0..4]);
    cur += 4;
    const off_bytes = (@as(usize, row_count) + 1) * 4;
    const offsets_region = values[cur .. cur + off_bytes];
    cur += off_bytes;
    return .{
        .table = table,
        .raw_byte_count = raw_byte_count,
        .offsets_region = offsets_region,
        .comp_bytes = values[cur .. cur + comp_byte_count],
    };
}

/// Split a decompressed FSST block payload into (validity bitmap, FsstBlock),
/// mirroring `rleViewOf` / `forViewOf`.
pub const FsstView = struct {
    nulls: ?[]const u8,
    block: FsstBlock,
};

pub fn fsstViewOf(raw: []const u8, row_count: u32, flags: format.ColumnBlockFlags) !FsstView {
    var nulls: ?[]const u8 = null;
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        nulls = raw[0..bm_len];
        values = raw[bm_len..];
    }
    return .{ .nulls = nulls, .block = try fsstBlockOf(values, row_count) };
}

/// Expand a borrowed FSST block into the cache's recycled scratch pool —
/// `[offsets (row_count+1 × u32)][bytes]` in one buffer — and return a
/// borrowed string view over it. A fresh allocation per borrow re-faults
/// freshly-zeroed pages on every scan `next()`; the recycled pool pays the
/// page commit once. The scratch rides `block.owned` / `block.pooled` and
/// returns to the pool on `block.release`; the cache pin stays held (the
/// nulls bitmap aliases the entry's bytes).
pub fn expandFsstPooled(
    block: *ReadSegment.BorrowedBlock,
    tc: storage_cache.TableCache,
    col_type: Type,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !ColumnView {
    const cc = tc.cache;
    const fv = try fsstViewOf(block.bytes, row_count, flags);
    const n: usize = row_count;
    const offsets_bytes = (n + 1) * @sizeOf(u32);
    const scratch = try cc.acquireScratch(offsets_bytes + fv.block.raw_byte_count);
    errdefer cc.releaseScratch(scratch);

    const offsets: []u32 = @alignCast(std.mem.bytesAsSlice(u32, scratch[0..offsets_bytes]));
    const bytes = scratch[offsets_bytes..][0..fv.block.raw_byte_count];
    var o: usize = 0;
    offsets[0] = 0;
    for (0..n) |r| {
        // Prefetch a few rows ahead: row starts hop unpredictably enough that
        // the hardware prefetcher misses them (+~10% measured in the micro).
        if (r + 8 < n) @prefetch(fv.block.rowComp(r + 8).ptr, .{ .rw = .read, .locality = 2 });
        o += fv.block.table.decodeInto(fv.block.rowComp(r), bytes[o..]);
        offsets[r + 1] = @intCast(o);
    }
    if (o != fv.block.raw_byte_count) return format.Error.CorruptColumnBlockHeader;

    block.owned = scratch;
    block.pooled = true;
    const sc = column.StringView{ .offsets = offsets, .bytes = bytes };
    return switch (col_type) {
        .varchar => .{ .data = .{ .varchar = sc }, .nulls = fv.nulls },
        .string => .{ .data = .{ .string = sc }, .nulls = fv.nulls },
        .char => .{ .data = .{ .char = sc }, .nulls = fv.nulls },
        .json => .{ .data = .{ .json = sc }, .nulls = fv.nulls },
        .nested => .{ .data = .{ .nested = sc }, .nulls = fv.nulls },
        .bitmap => .{ .data = .{ .bitmap = sc }, .nulls = fv.nulls },
        else => format.Error.CorruptColumnBlockHeader,
    };
}

/// Expand an FSST block into the owned `[offsets][bytes]` string shape every
/// consumer expects — identical output to a raw string block.
pub fn decodeFsstColumn(
    allocator: Allocator,
    col_type: Type,
    raw: []const u8,
    row_count: u32,
    flags: format.ColumnBlockFlags,
) !OwnedColumn {
    const _pt = if (prof.enabled) prof.nowTicks() else 0;
    defer if (prof.enabled) prof.add("fsst-decode (expand→strings)", @intCast(@max(0, prof.nowTicks() - _pt)));

    var nulls: ?[]u8 = null;
    errdefer if (nulls) |n| allocator.free(n);
    var values = raw;
    if (flags.has_nulls) {
        const bm_len = column.bitmapBytes(row_count);
        const bm_copy = try allocator.alloc(u8, bm_len);
        @memcpy(bm_copy, raw[0..bm_len]);
        nulls = bm_copy;
        values = raw[bm_len..];
    }

    const fb = try fsstBlockOf(values, row_count);

    const offsets = try allocator.alloc(u32, @as(usize, row_count) + 1);
    errdefer allocator.free(offsets);
    const bytes = try allocator.alloc(u8, fb.raw_byte_count);
    errdefer allocator.free(bytes);

    var o: usize = 0;
    offsets[0] = 0;
    var row: usize = 0;
    while (row < row_count) : (row += 1) {
        if (row + 8 < row_count) @prefetch(fb.rowComp(row + 8).ptr, .{ .rw = .read, .locality = 2 });
        o += fb.table.decodeInto(fb.rowComp(row), bytes[o..]);
        offsets[row + 1] = @intCast(o);
    }
    if (o != fb.raw_byte_count) return format.Error.CorruptColumnBlockHeader;

    const sc = OwnedStringColumn{ .offsets = offsets, .bytes = bytes };
    // The writer only ever FSST-encodes string-family columns.
    return switch (col_type) {
        .varchar => .{ .data = .{ .varchar = sc }, .nulls = nulls },
        .string => .{ .data = .{ .string = sc }, .nulls = nulls },
        .char => .{ .data = .{ .char = sc }, .nulls = nulls },
        .json => .{ .data = .{ .json = sc }, .nulls = nulls },
        .nested => .{ .data = .{ .nested = sc }, .nulls = nulls },
        .bitmap => .{ .data = .{ .bitmap = sc }, .nulls = nulls },
        else => unreachable,
    };
}
