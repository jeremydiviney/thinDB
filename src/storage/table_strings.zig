//! Table string ids (DESIGN.md §4.6): one id per distinct value of a string
//! column across the whole table, so a value takes the same id in every
//! segment. Each column keeps a list of its distinct values in id order,
//! append-only and touched only by segment writers; a segment stores the
//! table id of each of its string-table values (`format.StringTable.table_ids`),
//! and queries read those, never the list.
//!
//! The lists live in `strings/` under the table directory. Each column's
//! sealed pages go to `<ci>.list`, appended to and never rewritten; the
//! values past them, its tail, go to the `tails` file, one page per column,
//! replaced whole each time a manifest is published. A page is a header and
//! its payload, compressed with the table's codec (a tail with plain LZ4):
//!
//!   header:  [magic "tDBL"][first_id: u32][count: u32][raw_len: u32]
//!            [stored_len: u32][codec: u8][pad: 3][checksum: u64]
//!   payload: [offsets: (count+1) × u32][bytes]
//!
//! The checksum is Wyhash over the header's first 24 bytes and the stored
//! payload. The `tails` file is [magic "tDBA"][count: u32], count ×
//! [column_idx: u32][length: u32], then those pages back to back.
//!
//! A writer finds a value's id through an in-memory index of {38-bit hash
//! tag, id} slots, then checks the candidate's bytes: in the tail directly,
//! or in its sealed page, reading each page a batch of candidates touches
//! once. The index holds no bytes, so it grows by reading the pages again.
//!
//! Fixed-width columns (`fixedWidth`) keep lists too (§4.7), each value its
//! little-endian bytes, and a segment that takes their ids stores the ids in
//! place of the values, so readers do read these lists: they turn a block's
//! ids back into values a page at a time, the pages cached in the block
//! cache. Such a list differs in four ways. Its page payload is the values
//! back to back, no offsets, rewritten (`int_streams`) before compression,
//! which the header's flags record. Its pages are small, `fixed_page_bytes`,
//! for a reader after a few values. Each publish seals its whole tail, so
//! the `tails` file never holds it, and `<ci>.dir` lists its sealed pages, a
//! record each, for readers to find them without reading every header:
//!
//!   record: [first_id: u32][count: u32][offset: u64][stored_len: u32][pad: 4]
//!
//! And a closed fixed-width column keeps its list: the segments that stored
//! its ids still read through it.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const format = @import("format.zig");
const compression = @import("compression.zig");
const int_streams = @import("int_streams.zig");
const manifest = @import("manifest.zig");
const segment_strings = @import("segment_strings.zig");
const storage_cache = @import("cache.zig");
const parallel = @import("../util/parallel.zig");

pub const dir_name = "strings";
/// Ids a column may hold. A write whose new values would pass it closes the
/// column for good: later segments store no ids for it, and readers ignore
/// the ids earlier ones stored.
pub const id_cap: u32 = (1 << 26) - 1;
/// Payload bytes past which the tail seals into a page.
pub const page_bytes: usize = 1 << 20;
/// `page_bytes` of a fixed-width column.
pub const fixed_page_bytes: usize = 64 << 10;
/// The block-cache segment id a list page is cached under, past every
/// segment's.
pub const list_segment_id: u64 = std.math.maxInt(u64);

pub const State = manifest.StringIdState;

const page_magic = [4]u8{ 't', 'D', 'B', 'L' };
const tails_magic = [4]u8{ 't', 'D', 'B', 'A' };
const tails_name = "tails";
const tails_tmp_name = "tails.tmp";
const page_header_size: usize = 32;
/// The header bytes the checksum covers: all but the checksum.
const checksummed_header: usize = 24;
/// Below this many values a resolve hashes and probes on its own thread.
const parallel_min_values: usize = 1 << 14;
const none: u32 = std.math.maxInt(u32);

const JobError = Io.File.ReadPositionalError || Allocator.Error;

/// Whether column type `t` of `schema` takes table ids: the columns that
/// take segment string tables.
pub fn eligible(schema: types.TableSchema, t: types.Type) bool {
    return segment_strings.eligible(t) and schema.compression != .lz4_fsst;
}

/// The bytes per value of a fixed-width column type that takes table ids:
/// the 8-byte integers, whose values a 26-bit id can stand in for.
pub fn fixedWidth(t: types.Type) ?u8 {
    return switch (t) {
        .bigint, .datetime, .decimal64 => 8,
        else => null,
    };
}

/// Where a segment writer takes its table ids from.
pub const Target = struct {
    strings: *TableStrings,
    io: Io,
    table_dir: Io.Dir,
    threads: usize,
};

/// What `TableStrings.resolve` asks before a write takes ids: `admits` with
/// how many new ids the write's values would take and the ids the column
/// holds.
pub const admit_all: AdmitAll = .{};

pub const AdmitAll = struct {
    pub fn admits(_: AdmitAll, _: usize, _: u32) bool {
        return true;
    }
};

/// Where a table's readers find its fixed-width lists (`TableStrings.attach`).
pub const Reader = struct {
    io: Io,
    /// The table's directory handle, which a DDL swap reopens in place.
    table_dir: *const Io.Dir,
    cache: *storage_cache.Cache,
    /// The cache namespace of these lists' pages.
    uid: u64,
};

/// A table's string-id lists, one per eligible column. Loaded from disk on
/// the first resolve, then held for the table's life.
pub const TableStrings = struct {
    allocator: Allocator,
    /// One per schema column; null where the column takes no ids.
    columns: []?ColumnList,
    /// Ids a column may hold; tests lower it.
    cap: u32 = id_cap,
    loaded: std.atomic.Value(bool) = .init(false),
    load_mutex: Io.Mutex = .init,
    persist_mutex: Io.Mutex = .init,
    reader: ?Reader = null,

    /// `states` is what the table's manifest recorded.
    pub fn init(allocator: Allocator, schema: types.TableSchema, states: []const State) !TableStrings {
        const columns = try allocator.alloc(?ColumnList, schema.columns.len);
        for (columns, schema.columns, 0..) |*slot, col, ci| {
            const width: ?u8 = if (eligible(schema, col.type)) 0 else fixedWidth(col.type);
            slot.* = if (width) |w| ColumnList.init(allocator, @intCast(ci), schema.compression, w, stateOf(states, ci)) else null;
        }
        return .{ .allocator = allocator, .columns = columns };
    }

    pub fn deinit(self: *TableStrings) void {
        if (self.reader) |r| self.closeReaders(r.io);
        for (self.columns) |*slot| if (slot.*) |*c| c.deinit();
        self.allocator.free(self.columns);
        self.* = undefined;
    }

    /// Lets queries read the fixed-width lists, under `table_dir` and through
    /// `cache`.
    pub fn attach(self: *TableStrings, io: Io, table_dir: *const Io.Dir, cache: *storage_cache.Cache) void {
        self.reader = .{ .io = io, .table_dir = table_dir, .cache = cache, .uid = storage_cache.newTableUid() };
    }

    /// Closes the files readers hold open, ahead of a DDL swap moving the
    /// table's directory; the next read opens them again.
    pub fn closeReaders(self: *TableStrings, io: Io) void {
        for (self.columns) |*slot| if (slot.*) |*c| {
            c.read_mutex.lockUncancelable(io);
            defer c.read_mutex.unlock(io);
            if (c.read_file) |f| f.close(io);
            c.read_file = null;
        };
    }

    /// Bytes per value of column `ci`'s list when it is fixed-width; null
    /// for a string list or a column without one.
    pub fn fixedListWidth(self: *const TableStrings, ci: usize) ?u8 {
        if (ci >= self.columns.len) return null;
        const c = if (self.columns[ci]) |*col| col else return null;
        return if (c.width == 0) null else c.width;
    }

    /// Writes the value of each of `ids` of fixed-width column `ci` into
    /// `out`, `width` little-endian bytes each, reading the pages they lie
    /// in through the block cache.
    pub fn fixedValues(self: *TableStrings, io: Io, ci: usize, ids: []const u32, out: []u8) !void {
        const r = self.reader orelse return format.Error.CorruptColumnBlockHeader;
        const c = if (ci < self.columns.len) (if (self.columns[ci]) |*col| col else null) else null;
        const list = c orelse return format.Error.CorruptColumnBlockHeader;
        if (list.width == 0) return format.Error.CorruptColumnBlockHeader;
        if (ids.len == 0) return;
        var top: u32 = 0;
        for (ids) |id| top = @max(top, id);
        const count = try list.coverReads(io, r.table_dir.*, top + 1);
        const pinned = try self.allocator.alloc(?*storage_cache.Cache.Entry, count);
        defer {
            for (pinned) |p| if (p) |e| r.cache.release(e);
            self.allocator.free(pinned);
        }
        @memset(pinned, null);
        const w: usize = list.width;
        var at: usize = 0;
        var page = list.readPage(0);
        for (ids, 0..) |id, i| {
            if (id < page.first_id or id - page.first_id >= page.count) {
                at = list.readPageOf(id, count);
                page = list.readPage(at);
            }
            const entry = pinned[at] orelse pin: {
                const e = try list.pinPage(io, r, at, page);
                pinned[at] = e;
                break :pin e;
            };
            @memcpy(out[i * w ..][0..w], entry.bytes[(id - page.first_id) * w ..][0..w]);
        }
    }

    /// Column `ci`'s state as the next publish would record it; null when
    /// the column takes no ids.
    pub fn state(self: *TableStrings, io: Io, ci: usize) ?State {
        if (ci >= self.columns.len) return null;
        const c = if (self.columns[ci]) |*col| col else return null;
        c.mutex.lockUncancelable(io);
        defer c.mutex.unlock(io);
        return c.currentState();
    }

    /// `state` as last persisted or closed, without waiting on a writer: a
    /// query's view. Every id a published segment stores is below its
    /// `id_count`.
    pub fn readerState(self: *const TableStrings, ci: usize) ?State {
        if (ci >= self.columns.len) return null;
        const c = if (self.columns[ci]) |*col| col else return null;
        return unpackState(c.column_idx, c.reader_state.load(.acquire));
    }

    /// Gives each of `values` its table id of column `ci` in `ids`, taking
    /// new ids for values the list lacks. `values` has `len()` and
    /// `get(i)`, and holds distinct values. False, with `ids` unset, when
    /// the column takes no ids: it is closed, or the new values would pass
    /// `cap`, which closes it, or `admit` turns them away (`admit_all`).
    pub fn resolve(self: *TableStrings, io: Io, table_dir: Io.Dir, threads: usize, ci: usize, values: anytype, ids: []u32, admit: anytype) !bool {
        const c = if (self.columns[ci]) |*col| col else return false;
        try self.ensureLoaded(io, table_dir, threads);
        c.mutex.lockUncancelable(io);
        defer c.mutex.unlock(io);
        if (c.closed) return false;
        var dir = try table_dir.createDirPathOpen(io, dir_name, .{});
        defer dir.close(io);
        return c.resolve(io, dir, @max(1, threads), self.cap, values, ids, admit);
    }

    /// Makes every id given so far durable, ahead of the manifest about to
    /// be published, and returns each column's state for that manifest.
    /// Caller owns the slice.
    pub fn persist(self: *TableStrings, allocator: Allocator, io: Io, table_dir: Io.Dir, durable: bool) ![]State {
        self.persist_mutex.lockUncancelable(io);
        defer self.persist_mutex.unlock(io);
        var count: usize = 0;
        for (self.columns) |slot| count += @intFromBool(slot != null);
        const states = try allocator.alloc(State, count);
        errdefer allocator.free(states);
        if (!self.loaded.load(.acquire)) {
            var i: usize = 0;
            for (self.columns) |*slot| if (slot.*) |*c| {
                states[i] = c.currentState();
                i += 1;
            };
            return states;
        }

        var dir = try table_dir.createDirPathOpen(io, dir_name, .{});
        defer dir.close(io);
        const saved = try allocator.alloc(u32, count);
        defer allocator.free(saved);
        var directory: std.ArrayList(u8) = .empty;
        defer directory.deinit(allocator);
        var pages: std.ArrayList(u8) = .empty;
        defer pages.deinit(allocator);
        var page_count: u32 = 0;
        var changed = false;
        var i: usize = 0;
        for (self.columns) |*slot| if (slot.*) |*c| {
            c.mutex.lockUncancelable(io);
            defer c.mutex.unlock(io);
            states[i] = try c.persist(io, dir, durable);
            saved[i] = c.page_version;
            changed = changed or c.saved_version != c.page_version;
            if (!c.closed and c.tail_page.len > 0) {
                try format.appendU32(allocator, &directory, c.column_idx);
                try format.appendU32(allocator, &directory, @intCast(c.tail_page.len));
                try pages.appendSlice(allocator, c.tail_page);
                page_count += 1;
            }
            i += 1;
        };
        if (!changed) return states;

        var file: std.ArrayList(u8) = .empty;
        defer file.deinit(allocator);
        try file.ensureTotalCapacity(allocator, 8 + directory.items.len + pages.items.len);
        file.appendSliceAssumeCapacity(&tails_magic);
        var b4: [4]u8 = undefined;
        format.writeU32(&b4, page_count);
        file.appendSliceAssumeCapacity(&b4);
        file.appendSliceAssumeCapacity(directory.items);
        file.appendSliceAssumeCapacity(pages.items);
        try @import("storage.zig").writeFileAtomic(io, dir, tails_tmp_name, tails_name, file.items, durable);
        i = 0;
        for (self.columns) |*slot| if (slot.*) |*c| {
            c.mutex.lockUncancelable(io);
            defer c.mutex.unlock(io);
            c.saved_version = saved[i];
            i += 1;
        };
        return states;
    }

    fn ensureLoaded(self: *TableStrings, io: Io, table_dir: Io.Dir, threads: usize) !void {
        if (self.loaded.load(.acquire)) return;
        self.load_mutex.lockUncancelable(io);
        defer self.load_mutex.unlock(io);
        if (self.loaded.load(.acquire)) return;
        var dir = try table_dir.createDirPathOpen(io, dir_name, .{});
        defer dir.close(io);
        const tails = dir.readFileAlloc(io, tails_name, self.allocator, .unlimited) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        defer if (tails) |t| self.allocator.free(t);
        for (self.columns) |*slot| if (slot.*) |*c| {
            c.mutex.lockUncancelable(io);
            defer c.mutex.unlock(io);
            if (!c.closed) try c.load(io, dir, tailPageOf(tails, c.column_idx), @max(1, threads));
        };
        self.loaded.store(true, .release);
    }
};

fn stateOf(states: []const State, ci: usize) State {
    for (states) |s| if (s.column_idx == ci) return s;
    return .{ .column_idx = @intCast(ci), .id_count = 0, .closed = false };
}

/// Column `column_idx`'s page in the `tails` file `bytes`; null when it
/// has none or the file is malformed.
fn tailPageOf(bytes: ?[]const u8, column_idx: u32) ?[]const u8 {
    const b = bytes orelse return null;
    if (b.len < 8 or !std.mem.eql(u8, b[0..4], &tails_magic)) return null;
    const count: u64 = format.readU32(b[4..8]);
    if (8 + count * 8 > b.len) return null;
    var offset: u64 = 8 + count * 8;
    for (0..@intCast(count)) |k| {
        const entry = b[8 + k * 8 ..][0..8];
        const len = format.readU32(entry[4..8]);
        if (offset + len > b.len) return null;
        if (format.readU32(entry[0..4]) == column_idx) return b[@intCast(offset)..][0..len];
        offset += len;
    }
    return null;
}

fn listName(buf: []u8, column_idx: u32) ![]u8 {
    return std.fmt.bufPrint(buf, "{d}.list", .{column_idx});
}

fn dirFileName(buf: []u8, column_idx: u32) ![]u8 {
    return std.fmt.bufPrint(buf, "{d}.dir", .{column_idx});
}

fn dirTmpName(buf: []u8, column_idx: u32) ![]u8 {
    return std.fmt.bufPrint(buf, "{d}.dir.tmp", .{column_idx});
}

/// A sealed page of `.list`.
const Page = struct {
    first_id: u32,
    count: u32,
    offset: u64,
    stored_len: u32,

    const record_size: usize = 24;

    fn record(self: Page) [record_size]u8 {
        var r: [record_size]u8 = @splat(0);
        format.writeU32(r[0..4], self.first_id);
        format.writeU32(r[4..8], self.count);
        format.writeU64(r[8..16], self.offset);
        format.writeU32(r[16..20], self.stored_len);
        return r;
    }

    fn ofRecord(r: *const [record_size]u8) Page {
        return .{
            .first_id = format.readU32(r[0..4]),
            .count = format.readU32(r[4..8]),
            .offset = format.readU64(r[8..16]),
            .stored_len = format.readU32(r[16..20]),
        };
    }
};

/// Pages per chunk of a reader's page directory. Chunks never move, so a
/// reader looks pages up while another reads more of `.dir` in.
const read_chunk_pages: usize = 4096;
const ReadChunk = [read_chunk_pages]Page;

/// One column's list: its sealed pages on disk, its tail in memory, and
/// the index over both.
pub const ColumnList = struct {
    allocator: Allocator,
    mutex: Io.Mutex = .init,
    column_idx: u32,
    codec: format.Compression,
    /// Bytes per value of a fixed-width list; 0 for a string list.
    width: u8,
    /// `.dir` bytes the sealed pages' records take, of a fixed-width list.
    dir_len: u64 = 0,
    dir_synced: bool = true,
    /// Readers' view of a fixed-width list, apart from the writers': the
    /// pages `.dir` listed when last read (`coverReads`), `read_count` of
    /// them covering ids `0 .. read_ids`, and `.list` open for reading.
    read_mutex: Io.Mutex = .init,
    read_chunks: []?*ReadChunk = &.{},
    read_count: std.atomic.Value(usize) = .init(0),
    read_ids: std.atomic.Value(u32) = .init(0),
    read_file: ?Io.File = null,
    /// Ids the table's last published manifest counted; the files hold at
    /// least these.
    committed: u32,
    closed: bool,
    pages: std.ArrayList(Page) = .empty,
    /// `.list` bytes the sealed pages take; bytes past them are a torn or
    /// unfinished page.
    list_len: u64 = 0,
    /// Ids the sealed pages hold, so the tail's first id.
    sealed: u32 = 0,
    /// Whether `.list` is synced through `list_len`.
    list_synced: bool = true,
    /// The tail's values, back to back, and each one's end in them.
    tail_bytes: std.ArrayList(u8) = .empty,
    tail_ends: std.ArrayList(u32) = .empty,
    /// Bumped when the tail changes. `tail_page` encodes the tail as of
    /// `page_version`, and the `tails` file holds `saved_version`.
    tail_version: u32 = 0,
    page_version: u32 = 0,
    saved_version: u32 = 0,
    tail_page: []u8 = &.{},
    index: Index = .{},
    /// `currentState` packed for `TableStrings.readerState`, stored
    /// whenever it changes.
    reader_state: std.atomic.Value(u64),

    fn init(allocator: Allocator, column_idx: u32, table_compression: types.TableCompression, width: u8, s: State) ColumnList {
        const codec: format.Compression = switch (table_compression) {
            .none => .none,
            .zstd => .zstd,
            .lz4, .lz4_fsst => .lz4,
        };
        return .{ .allocator = allocator, .column_idx = column_idx, .codec = codec, .width = width, .committed = s.id_count, .closed = s.closed, .reader_state = .init(packState(s)) };
    }

    fn deinit(self: *ColumnList) void {
        self.reset();
        for (self.read_chunks) |chunk| if (chunk) |ch| self.allocator.destroy(ch);
        self.allocator.free(self.read_chunks);
        self.* = undefined;
    }

    fn pageBytes(self: *const ColumnList) usize {
        return if (self.width == 0) page_bytes else fixed_page_bytes;
    }

    // ---- Reads (fixed-width lists) ----

    fn readPage(self: *const ColumnList, i: usize) Page {
        return self.read_chunks[i / read_chunk_pages].?[i % read_chunk_pages];
    }

    /// The page of the first `count` read in that holds `id`.
    fn readPageOf(self: *const ColumnList, id: u32, count: usize) usize {
        var lo: usize = 0;
        var hi: usize = count;
        while (hi - lo > 1) {
            const mid = lo + (hi - lo) / 2;
            if (self.readPage(mid).first_id <= id) lo = mid else hi = mid;
        }
        return lo;
    }

    /// Reads `.dir` on until its pages cover ids `0 .. ids` and returns how
    /// many pages are read in. Takes only the pages the last published
    /// manifest counts: a page past them can still be cut and written anew.
    fn coverReads(self: *ColumnList, io: Io, table_dir: Io.Dir, ids: u32) !usize {
        if (self.read_ids.load(.acquire) >= ids) return self.read_count.load(.acquire);
        self.read_mutex.lockUncancelable(io);
        defer self.read_mutex.unlock(io);
        if (self.read_ids.load(.acquire) >= ids) return self.read_count.load(.acquire);
        const limit = unpackState(self.column_idx, self.reader_state.load(.acquire)).id_count;
        var dir = try table_dir.openDir(io, dir_name, .{});
        defer dir.close(io);
        var name_buf: [24]u8 = undefined;
        const bytes = try dir.readFileAlloc(io, try dirFileName(&name_buf, self.column_idx), self.allocator, .unlimited);
        defer self.allocator.free(bytes);
        if (self.read_chunks.len == 0) {
            const chunks = try self.allocator.alloc(?*ReadChunk, id_cap / read_chunk_pages + 1);
            @memset(chunks, null);
            self.read_chunks = chunks;
        }
        var count = self.read_count.load(.monotonic);
        var covered = self.read_ids.load(.monotonic);
        while ((count + 1) * Page.record_size <= bytes.len and count / read_chunk_pages < self.read_chunks.len) {
            const page = Page.ofRecord(bytes[count * Page.record_size ..][0..Page.record_size]);
            if (page.first_id != covered or page.count == 0 or @as(u64, covered) + page.count > limit) break;
            const slot = &self.read_chunks[count / read_chunk_pages];
            if (slot.* == null) slot.* = try self.allocator.create(ReadChunk);
            slot.*.?[count % read_chunk_pages] = page;
            count += 1;
            covered += page.count;
            self.read_count.store(count, .release);
            self.read_ids.store(covered, .release);
        }
        if (covered < ids) return format.Error.CorruptColumnBlockHeader;
        if (self.read_file == null) {
            var list_buf: [16]u8 = undefined;
            self.read_file = try dir.openFile(io, try listName(&list_buf, self.column_idx), .{});
        }
        return count;
    }

    /// Page `p`, `page`, decoded, pinned in the block cache; read in on a
    /// miss.
    fn pinPage(self: *ColumnList, io: Io, r: Reader, p: usize, page: Page) !*storage_cache.Cache.Entry {
        const key: storage_cache.Key = .{
            .table_uid = r.uid,
            .segment_id = list_segment_id,
            .row_group_idx = @intCast(p),
            .column_idx = self.column_idx,
            .content = .list_page,
        };
        const fill = switch (try r.cache.claimAfterFill(io, key)) {
            .hit => |entry| return entry,
            .fill => |fill| fill,
        };
        defer r.cache.endFill(io, fill);
        const file = file: {
            self.read_mutex.lockUncancelable(io);
            defer self.read_mutex.unlock(io);
            if (self.read_file == null) {
                var dir = try r.table_dir.openDir(io, dir_name, .{});
                defer dir.close(io);
                var list_buf: [16]u8 = undefined;
                self.read_file = try dir.openFile(io, try listName(&list_buf, self.column_idx), .{});
            }
            break :file self.read_file.?;
        };
        const stored = try self.allocator.alloc(u8, page_header_size + @as(usize, page.stored_len));
        defer self.allocator.free(stored);
        if (try file.readPositionalAll(io, stored, page.offset) != stored.len) return format.Error.UnexpectedEof;
        var raw: std.ArrayList(u8) = .empty;
        defer raw.deinit(self.allocator);
        const decoded = try decodePage(self.allocator, stored, page.first_id, &raw) orelse return format.Error.CorruptColumnBlockHeader;
        if (decoded.view.count != page.count or decoded.view.width != self.width) return format.Error.CorruptColumnBlockHeader;
        const block_alloc = r.cache.blockAllocator();
        const bytes = try block_alloc.alignedAlloc(u8, .@"16", raw.items.len);
        errdefer block_alloc.free(bytes);
        @memcpy(bytes, raw.items);
        return r.cache.insertPinned(key, bytes, .raw);
    }

    /// Drops everything held in memory, keeping the column's identity and
    /// state.
    fn reset(self: *ColumnList) void {
        self.pages.clearAndFree(self.allocator);
        self.tail_bytes.clearAndFree(self.allocator);
        self.tail_ends.clearAndFree(self.allocator);
        self.allocator.free(self.tail_page);
        self.tail_page = &.{};
        self.allocator.free(self.index.slots);
        self.index = .{};
        self.list_len = 0;
        self.sealed = 0;
        self.list_synced = true;
    }

    /// Stops the column taking ids, for good once the next manifest is
    /// published. A fixed-width list keeps what it holds, its tail included:
    /// segments that stored its ids, some maybe not yet published, read
    /// through it.
    fn close(self: *ColumnList) void {
        if (self.width == 0) self.reset();
        self.closed = true;
        self.reader_state.store(packState(self.currentState()), .release);
    }

    fn closeDamaged(self: *ColumnList) void {
        std.log.warn("table string ids: column {d}'s list is damaged; it takes no more ids", .{self.column_idx});
        self.close();
    }

    fn currentState(self: *const ColumnList) State {
        if (self.closed and self.width == 0) return .{ .column_idx = self.column_idx, .id_count = 0, .closed = true };
        return .{ .column_idx = self.column_idx, .id_count = self.committed, .closed = self.closed };
    }

    fn idCount(self: *const ColumnList) u32 {
        return self.sealed + @as(u32, @intCast(self.tail_ends.items.len));
    }

    fn tailStart(self: *const ColumnList, k: usize) u32 {
        return if (k == 0) 0 else self.tail_ends.items[k - 1];
    }

    fn tailValue(self: *const ColumnList, id: u32) []const u8 {
        const k = id - self.sealed;
        return self.tail_bytes.items[self.tailStart(k)..self.tail_ends.items[k]];
    }

    fn tailPayloadLen(self: *const ColumnList, lo: usize, hi: usize) usize {
        const bytes = self.tailStart(hi) - self.tailStart(lo);
        return if (self.width > 0) bytes else (hi - lo + 1) * 4 + bytes;
    }

    // ---- Load ----

    /// Reads the column's sealed pages and its page of the `tails` file,
    /// cutting a torn page off `.list`, and indexes every value. Closes the
    /// column when the files hold fewer ids than the manifest counted.
    fn load(self: *ColumnList, io: Io, dir: Io.Dir, tail_page: ?[]const u8, threads: usize) !void {
        self.reset();
        var name_buf: [16]u8 = undefined;
        const name = try listName(&name_buf, self.column_idx);
        if (dir.openFile(io, name, .{ .mode = .read_write })) |file| {
            defer file.close(io);
            try self.scanPages(io, file);
            while (try self.rebuildIndex(io, file, threads, self.sealed + tailCount(tail_page))) |damaged| {
                const cut = self.pages.items[damaged];
                self.pages.shrinkRetainingCapacity(damaged);
                self.sealed = cut.first_id;
                self.list_len = cut.offset;
                try file.setLength(io, self.list_len);
            }
            if (self.width > 0) try self.matchDir(io, dir);
        } else |err| switch (err) {
            error.FileNotFound => {
                try self.allocateIndex(tailCount(tail_page));
                if (self.width > 0) try self.matchDir(io, dir);
            },
            else => return err,
        }

        if (tail_page) |bytes| {
            // A tail written before the list's last pages sealed is stale;
            // one past the list's end follows ids lost with a torn page.
            var raw: std.ArrayList(u8) = .empty;
            defer raw.deinit(self.allocator);
            const page = try decodePage(self.allocator, bytes, null, &raw);
            if (page) |p| if (p.first_id <= self.sealed and @as(u64, p.first_id) + p.view.count > self.sealed) {
                const skip = self.sealed - p.first_id;
                try self.tail_bytes.ensureUnusedCapacity(self.allocator, p.view.valueStart(p.view.count) - p.view.valueStart(skip));
                try self.tail_ends.ensureUnusedCapacity(self.allocator, p.view.count - skip);
                for (skip..p.view.count) |k| {
                    const value = p.view.value(k);
                    self.tail_bytes.appendSliceAssumeCapacity(value);
                    self.tail_ends.appendAssumeCapacity(@intCast(self.tail_bytes.items.len));
                    self.index.insert(std.hash.Wyhash.hash(0, value), self.idCount() - 1);
                }
            };
            self.tail_version = 1;
        }
        if (self.idCount() < self.committed) self.closeDamaged();
    }

    /// Makes `.dir` list exactly the sealed pages, replacing it when it
    /// lists others: a crash can leave it short of `.list` or past it.
    fn matchDir(self: *ColumnList, io: Io, dir: Io.Dir) !void {
        const records = try self.allocator.alloc(u8, self.pages.items.len * Page.record_size);
        defer self.allocator.free(records);
        for (self.pages.items, 0..) |p, k| records[k * Page.record_size ..][0..Page.record_size].* = p.record();
        self.dir_len = records.len;
        var name_buf: [24]u8 = undefined;
        const name = try dirFileName(&name_buf, self.column_idx);
        const current = dir.readFileAlloc(io, name, self.allocator, .unlimited) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        defer if (current) |c| self.allocator.free(c);
        if (std.mem.eql(u8, current orelse &.{}, records)) return;
        var tmp_buf: [24]u8 = undefined;
        try @import("storage.zig").writeFileAtomic(io, dir, try dirTmpName(&tmp_buf, self.column_idx), name, records, true);
    }

    fn tailCount(tail_page: ?[]const u8) u32 {
        const bytes = tail_page orelse return 0;
        if (bytes.len < page_header_size) return 0;
        const h = parseHeader(bytes[0..page_header_size]) orelse return 0;
        return h.count;
    }

    /// Reads `.list`'s page headers, keeping the run of well-formed pages
    /// from its start and cutting off what follows.
    fn scanPages(self: *ColumnList, io: Io, file: Io.File) !void {
        const size = (try file.stat(io)).size;
        var header: [page_header_size]u8 = undefined;
        while (self.list_len + page_header_size <= size) {
            if (try file.readPositionalAll(io, &header, self.list_len) != header.len) break;
            const h = parseHeader(&header) orelse break;
            if (h.first_id != self.sealed or h.count > id_cap - self.sealed) break;
            if (self.list_len + page_header_size + h.stored_len > size) break;
            try self.pages.append(self.allocator, .{ .first_id = h.first_id, .count = h.count, .offset = self.list_len, .stored_len = h.stored_len });
            self.list_len += page_header_size + h.stored_len;
            self.sealed += h.count;
        }
        if (self.list_len < size) try file.setLength(io, self.list_len);
    }

    // ---- Index ----

    fn allocateIndex(self: *ColumnList, ids: usize) !void {
        const slots = try self.allocator.alloc(u64, Index.slotsFor(ids));
        @memset(slots, Index.empty_slot);
        self.allocator.free(self.index.slots);
        self.index = Index.over(slots);
    }

    /// Indexes every value anew, in slots for `ids` ids: the sealed pages'
    /// values, read from `file` (`.list`, open while there are any), then
    /// the tail's. Returns the first damaged page, leaving the index as it
    /// was, or null.
    fn rebuildIndex(self: *ColumnList, io: Io, file: ?Io.File, threads: usize, ids: usize) !?usize {
        const slots = try self.allocator.alloc(u64, Index.slotsFor(ids));
        errdefer self.allocator.free(slots);
        @memset(slots, Index.empty_slot);
        var index = Index.over(slots);
        if (self.pages.items.len > 0) {
            var job: RebuildJob = .{ .allocator = self.allocator, .io = io, .file = file.?, .pages = self.pages.items, .index = &index };
            defer for (&job.scratch) |*s| s.deinit(self.allocator);
            parallel.forJobs(threads, self.pages.items.len, &job, RebuildJob.run);
            if (job.failure.err) |e| return e;
            const damaged = job.failure.damaged.load(.monotonic);
            if (damaged != std.math.maxInt(usize)) {
                self.allocator.free(slots);
                return damaged;
            }
        }
        for (0..self.tail_ends.items.len) |k| {
            const id = self.sealed + @as(u32, @intCast(k));
            index.insert(std.hash.Wyhash.hash(0, self.tailValue(id)), id);
        }
        self.allocator.free(self.index.slots);
        self.index = index;
        return null;
    }

    // ---- Resolve ----

    const Check = struct {
        id: u32,
        value: u32,
        /// The candidate's slot, to probe on from when it isn't the value.
        at: u32,

        fn lessThan(_: void, a: Check, b: Check) bool {
            return a.id < b.id;
        }
    };

    fn resolve(self: *ColumnList, io: Io, dir: Io.Dir, threads: usize, cap: u32, values: anytype, ids: []u32, admit: anytype) !bool {
        const allocator = self.allocator;
        const n = values.len();
        @memset(ids, none);
        const hashes = try allocator.alloc(u64, n);
        defer allocator.free(hashes);
        const at = try allocator.alloc(u32, n);
        defer allocator.free(at);
        var probe: ProbeJob(@TypeOf(values)) = .{ .index = &self.index, .values = values, .hashes = hashes, .at = at };
        parallel.forRanges(if (n >= parallel_min_values) threads else 1, n, &probe, @TypeOf(probe).run);

        var open: std.ArrayList(u32) = .empty;
        defer open.deinit(allocator);
        for (at, 0..) |a, i| if (a != none) try open.append(allocator, @intCast(i));
        var checks: std.ArrayList(Check) = .empty;
        defer checks.deinit(allocator);
        while (open.items.len > 0) {
            checks.clearRetainingCapacity();
            for (open.items) |i| {
                var slot: ?u32 = at[i];
                while (slot) |s| {
                    const id = Index.idOf(self.index.slots[s]);
                    if (id < self.sealed) {
                        try checks.append(allocator, .{ .id = id, .value = i, .at = s });
                        break;
                    }
                    if (std.mem.eql(u8, self.tailValue(id), values.get(i))) {
                        ids[i] = id;
                        break;
                    }
                    slot = self.index.next(hashes[i], s + 1);
                }
            }
            open.clearRetainingCapacity();
            if (checks.items.len == 0) break;
            if (!try self.verify(io, dir, threads, values, checks.items, ids)) {
                self.closeDamaged();
                return false;
            }
            for (checks.items) |c| if (ids[c.value] == none) {
                if (self.index.next(hashes[c.value], c.at + 1)) |s| {
                    at[c.value] = s;
                    try open.append(allocator, c.value);
                }
            };
        }

        var new_count: usize = 0;
        for (ids) |id| new_count += @intFromBool(id == none);
        if (!admit.admits(new_count, self.idCount())) {
            @memset(ids, none);
            return false;
        }
        if (new_count == 0) return true;
        if (@as(u64, self.idCount()) + new_count > cap) {
            self.close();
            return false;
        }
        if (!self.index.fits(self.idCount() + new_count)) {
            var name_buf: [16]u8 = undefined;
            const file = if (self.pages.items.len > 0) try dir.openFile(io, try listName(&name_buf, self.column_idx), .{}) else null;
            defer if (file) |f| f.close(io);
            if (try self.rebuildIndex(io, file, threads, self.idCount() + new_count) != null) {
                self.closeDamaged();
                return false;
            }
        }

        // Pages seal as the tail grows, a batch at a time, so the tail never
        // holds all of a large write's new values.
        const seal_at = 2 * threads * self.pageBytes();
        defer self.tail_version +%= 1;
        for (ids, 0..) |*id, i| if (id.* == none) {
            const bytes = values.get(i);
            try self.tail_bytes.ensureUnusedCapacity(allocator, bytes.len);
            try self.tail_ends.ensureUnusedCapacity(allocator, 1);
            const new_id = self.idCount();
            self.tail_bytes.appendSliceAssumeCapacity(bytes);
            self.tail_ends.appendAssumeCapacity(@intCast(self.tail_bytes.items.len));
            self.index.insert(hashes[i], new_id);
            id.* = new_id;
            if (self.tail_bytes.items.len >= seal_at) try self.sealPages(io, dir, threads, false);
        };
        try self.sealPages(io, dir, threads, false);
        return true;
    }

    /// Checks each candidate of `checks` against its sealed page, reading
    /// each page they touch once, and sets `ids` for those that match.
    /// False when a page is damaged.
    fn verify(self: *ColumnList, io: Io, dir: Io.Dir, threads: usize, values: anytype, checks: []Check, ids: []u32) !bool {
        const allocator = self.allocator;
        std.sort.pdq(Check, checks, {}, Check.lessThan);
        var groups: std.ArrayList(CheckGroup) = .empty;
        defer groups.deinit(allocator);
        var p: usize = 0;
        var k: usize = 0;
        while (k < checks.len) {
            while (self.pages.items[p].first_id + self.pages.items[p].count <= checks[k].id) p += 1;
            const end_id = self.pages.items[p].first_id + self.pages.items[p].count;
            const lo = k;
            while (k < checks.len and checks[k].id < end_id) k += 1;
            try groups.append(allocator, .{ .page = @intCast(p), .lo = @intCast(lo), .hi = @intCast(k) });
        }
        var name_buf: [16]u8 = undefined;
        const file = try dir.openFile(io, try listName(&name_buf, self.column_idx), .{});
        defer file.close(io);
        var job: VerifyJob(@TypeOf(values)) = .{
            .allocator = allocator,
            .io = io,
            .file = file,
            .pages = self.pages.items,
            .groups = groups.items,
            .checks = checks,
            .values = values,
            .ids = ids,
        };
        defer for (&job.scratch) |*s| s.deinit(allocator);
        parallel.forJobs(threads, groups.items.len, &job, @TypeOf(job).run);
        if (job.failure.err) |e| return e;
        return job.failure.damaged.load(.monotonic) == std.math.maxInt(usize);
    }

    // ---- Sealing ----

    const Cut = struct { lo: u32, hi: u32 };

    /// Seals the tail's front into full pages, or with `all` the whole tail,
    /// compressed in parallel and appended to `.list`, and lists them in
    /// `.dir` when fixed-width. On failure the column is as it was: the
    /// pages count only once all are written.
    fn sealPages(self: *ColumnList, io: Io, dir: Io.Dir, threads: usize, all: bool) !void {
        const allocator = self.allocator;
        var cuts: std.ArrayList(Cut) = .empty;
        defer cuts.deinit(allocator);
        var lo: usize = 0;
        var hi: usize = 0;
        while (hi < self.tail_ends.items.len) {
            hi += 1;
            if (self.tailPayloadLen(lo, hi) < self.pageBytes() and !(all and hi == self.tail_ends.items.len)) continue;
            try cuts.append(allocator, .{ .lo = @intCast(lo), .hi = @intCast(hi) });
            lo = hi;
        }
        if (cuts.items.len == 0) return;

        const out = try allocator.alloc([]u8, cuts.items.len);
        @memset(out, &.{});
        defer {
            for (out) |o| allocator.free(o);
            allocator.free(out);
        }
        var job: SealJob = .{ .allocator = allocator, .column = self, .cuts = cuts.items, .out = out };
        defer for (&job.compressors) |*c| if (c.*) |*comp| comp.deinit();
        parallel.forJobs(threads, cuts.items.len, &job, SealJob.run);
        if (job.failure.err) |e| return e;

        try self.pages.ensureUnusedCapacity(allocator, cuts.items.len);
        var name_buf: [16]u8 = undefined;
        const file = try dir.createFile(io, try listName(&name_buf, self.column_idx), .{ .read = true, .truncate = false });
        defer file.close(io);
        var offset = self.list_len;
        for (out) |page| {
            try file.writePositionalAll(io, page, offset);
            offset += page.len;
        }
        // The list is fsynced only at the next persist, the end of a bulk
        // load, which would otherwise wait on the whole load's pages.
        @import("storage.zig").startWriteback(file, self.list_len, offset - self.list_len);
        offset = self.list_len;
        const first_new = self.pages.items.len;
        for (cuts.items, out) |c, page| {
            self.pages.appendAssumeCapacity(.{ .first_id = self.sealed + c.lo, .count = c.hi - c.lo, .offset = offset, .stored_len = @intCast(page.len - page_header_size) });
            offset += page.len;
        }
        if (self.width > 0) {
            const records = try allocator.alloc(u8, cuts.items.len * Page.record_size);
            defer allocator.free(records);
            for (self.pages.items[first_new..], 0..) |p, k| records[k * Page.record_size ..][0..Page.record_size].* = p.record();
            var dir_buf: [24]u8 = undefined;
            const dir_file = dir.createFile(io, try dirFileName(&dir_buf, self.column_idx), .{ .truncate = false }) catch |e| {
                self.pages.shrinkRetainingCapacity(first_new);
                return e;
            };
            defer dir_file.close(io);
            dir_file.writePositionalAll(io, records, self.dir_len) catch |e| {
                self.pages.shrinkRetainingCapacity(first_new);
                return e;
            };
            self.dir_len += records.len;
            self.dir_synced = false;
        }
        self.list_len = offset;
        self.list_synced = false;

        const sealed_values = cuts.items[cuts.items.len - 1].hi;
        const sealed_bytes = self.tailStart(sealed_values);
        const rest = self.tail_bytes.items.len - sealed_bytes;
        std.mem.copyForwards(u8, self.tail_bytes.items[0..rest], self.tail_bytes.items[sealed_bytes..]);
        self.tail_bytes.shrinkRetainingCapacity(rest);
        const rest_values = self.tail_ends.items.len - sealed_values;
        for (self.tail_ends.items[0..rest_values], self.tail_ends.items[sealed_values..]) |*dst, src| dst.* = src - sealed_bytes;
        self.tail_ends.shrinkRetainingCapacity(rest_values);
        self.sealed += sealed_values;
    }

    /// The tail values `lo..hi` as a page payload.
    fn tailPayload(self: *const ColumnList, allocator: Allocator, lo: usize, hi: usize) ![]u8 {
        const raw = try allocator.alloc(u8, self.tailPayloadLen(lo, hi));
        const base = self.tailStart(lo);
        if (self.width > 0) {
            @memcpy(raw, self.tail_bytes.items[base..self.tailStart(hi)]);
            return raw;
        }
        format.writeU32(raw[0..4], 0);
        for (self.tail_ends.items[lo..hi], 1..) |end, k| format.writeU32(raw[k * 4 ..][0..4], end - base);
        @memcpy(raw[(hi - lo + 1) * 4 ..], self.tail_bytes.items[base..self.tailStart(hi)]);
        return raw;
    }

    // ---- Persist ----

    /// Syncs the sealed pages when `durable` and encodes the tail's page,
    /// for `TableStrings.persist`, and returns the column's state.
    fn persist(self: *ColumnList, io: Io, dir: Io.Dir, durable: bool) !State {
        if (self.closed and self.width == 0) {
            var name_buf: [16]u8 = undefined;
            dir.deleteFile(io, try listName(&name_buf, self.column_idx)) catch {};
            return self.currentState();
        }
        if (self.width > 0) try self.sealPages(io, dir, 1, true);
        if (durable and !self.list_synced) {
            var name_buf: [16]u8 = undefined;
            const file = try dir.openFile(io, try listName(&name_buf, self.column_idx), .{ .mode = .read_write });
            defer file.close(io);
            try file.sync(io);
            self.list_synced = true;
        }
        if (durable and !self.dir_synced) {
            var name_buf: [24]u8 = undefined;
            const file = try dir.openFile(io, try dirFileName(&name_buf, self.column_idx), .{ .mode = .read_write });
            defer file.close(io);
            try file.sync(io);
            self.dir_synced = true;
        }
        if (self.width > 0) {
            self.page_version = self.tail_version;
        } else if (self.page_version != self.tail_version) {
            const page: []u8 = if (self.tail_ends.items.len == 0) &.{} else page: {
                const raw = try self.tailPayload(self.allocator, 0, self.tail_ends.items.len);
                defer self.allocator.free(raw);
                break :page try encodePage(self.allocator, .fast, self.sealed, @intCast(self.tail_ends.items.len), raw, 0);
            };
            self.allocator.free(self.tail_page);
            self.tail_page = page;
            self.page_version = self.tail_version;
        }
        self.committed = self.idCount();
        self.reader_state.store(packState(self.currentState()), .release);
        return self.currentState();
    }
};

fn packState(s: State) u64 {
    return @as(u64, @intFromBool(s.closed)) << 32 | s.id_count;
}

fn unpackState(column_idx: u32, packed_state: u64) State {
    return .{ .column_idx = column_idx, .id_count = @truncate(packed_state), .closed = packed_state >> 32 != 0 };
}

// ---------------------------------------------------------------------------
// Index
// ---------------------------------------------------------------------------

/// Open-addressed slots of {38-bit hash tag, 26-bit id}, linear probing.
/// The tag is the hash's low bits and the home slot its high bits, apart
/// until the table passes 2^26 slots, so two values share a tag by chance
/// about once in 2^38 comparisons.
const Index = struct {
    slots: []u64 = &.{},
    count: usize = 0,
    shift: u6 = 63,

    /// No slot holds it: its id would be 2^26 - 1, past `id_cap`'s last id.
    const empty_slot: u64 = std.math.maxInt(u64);
    const id_bits = 26;
    const id_mask: u64 = (1 << id_bits) - 1;
    const tag_mask: u64 = (1 << (64 - id_bits)) - 1;
    const min_slots: usize = 1 << 12;

    fn over(slots: []u64) Index {
        return .{ .slots = slots, .shift = @intCast(@as(u32, 64) - std.math.log2_int(usize, slots.len)) };
    }

    fn slotsFor(ids: usize) usize {
        return @max(min_slots, std.math.ceilPowerOfTwoAssert(usize, ids + ids / 3 + 1));
    }

    fn slotOf(h: u64, id: u32) u64 {
        return ((h & tag_mask) << id_bits) | id;
    }

    fn idOf(slot: u64) u32 {
        return @intCast(slot & id_mask);
    }

    fn tagMatches(slot: u64, h: u64) bool {
        return slot >> id_bits == h & tag_mask;
    }

    fn mask(self: Index) usize {
        return self.slots.len - 1;
    }

    fn home(self: Index, h: u64) usize {
        return @intCast(h >> self.shift);
    }

    /// Whether `ids` ids fit with the slots at most 3/4 full.
    fn fits(self: Index, ids: usize) bool {
        return ids * 4 <= self.slots.len * 3;
    }

    /// The first slot from `from` on whose tag is `h`'s, or null at an
    /// empty slot.
    fn next(self: Index, h: u64, from: usize) ?u32 {
        var s = from & self.mask();
        while (true) : (s = (s + 1) & self.mask()) {
            const slot = self.slots[s];
            if (slot == empty_slot) return null;
            if (tagMatches(slot, h)) return @intCast(s);
        }
    }

    fn insert(self: *Index, h: u64, id: u32) void {
        var s = self.home(h);
        while (self.slots[s] != empty_slot) s = (s + 1) & self.mask();
        self.slots[s] = slotOf(h, id);
        self.count += 1;
    }

    /// `insert`, for workers filling one index together; `count` is the
    /// caller's to set.
    fn insertShared(self: *Index, h: u64, id: u32) void {
        const slot = slotOf(h, id);
        var s = self.home(h);
        while (@cmpxchgStrong(u64, &self.slots[s], empty_slot, slot, .monotonic, .monotonic) != null) s = (s + 1) & self.mask();
    }
};

// ---------------------------------------------------------------------------
// Pages
// ---------------------------------------------------------------------------

/// A page header's byte 21 is its values' width (0: a string page) and byte
/// 22 its flags.
const page_streams_flag: u8 = 1;

const PageHeader = struct {
    first_id: u32,
    count: u32,
    raw_len: u32,
    stored_len: u32,
    codec: format.Compression,
    width: u8,
    /// The stored payload is its `int_streams` directory, then the codec
    /// frame of the rewritten payload.
    streams: bool,
};

fn parseHeader(h: *const [page_header_size]u8) ?PageHeader {
    if (!std.mem.eql(u8, h[0..4], &page_magic)) return null;
    if (h[20] > @backingInt(format.Compression.lz4)) return null;
    const count = format.readU32(h[8..12]);
    const raw_len = format.readU32(h[12..16]);
    const width = h[21];
    const streams = h[22] & page_streams_flag != 0;
    if (count == 0) return null;
    if (width == 0 and (streams or raw_len < (@as(u64, count) + 1) * 4)) return null;
    if (width > 0 and raw_len != @as(u64, count) * width) return null;
    return .{
        .first_id = format.readU32(h[4..8]),
        .count = count,
        .raw_len = raw_len,
        .stored_len = format.readU32(h[16..20]),
        .codec = @fromBackingInt(@intCast(h[20])),
        .width = width,
        .streams = streams,
    };
}

fn checksum(header: []const u8, stored: []const u8) u64 {
    var h = std.hash.Wyhash.init(0);
    h.update(header[0..checksummed_header]);
    h.update(stored);
    return h.final();
}

/// A decoded page payload.
const PageView = struct {
    raw: []const u8,
    count: u32,
    /// Bytes per value; 0 for a string page.
    width: u8 = 0,

    /// Null when the offsets don't lie within `raw` in order, or a
    /// fixed-width page's values don't fill it.
    fn of(raw: []const u8, count: u32, width: u8) ?PageView {
        if (width > 0) return if (raw.len == @as(usize, count) * width) .{ .raw = raw, .count = count, .width = width } else null;
        const base = (@as(usize, count) + 1) * 4;
        if (raw.len < base or format.readU32(raw[0..4]) != 0) return null;
        var prev: u32 = 0;
        for (1..@as(usize, count) + 1) |k| {
            const end = format.readU32(raw[k * 4 ..][0..4]);
            if (end < prev) return null;
            prev = end;
        }
        if (prev != raw.len - base) return null;
        return .{ .raw = raw, .count = count };
    }

    fn valueStart(self: PageView, k: usize) u32 {
        return format.readU32(self.raw[k * 4 ..][0..4]);
    }

    fn value(self: PageView, k: usize) []const u8 {
        if (self.width > 0) return self.raw[k * self.width ..][0..self.width];
        const base = (@as(usize, self.count) + 1) * 4;
        return self.raw[base + self.valueStart(k) .. base + self.valueStart(k + 1)];
    }
};

/// How a page's payload is stored.
const Packing = union(enum) {
    /// The table's codec, as sealed pages take.
    table: struct { compressor: *compression.Compressor, codec: format.Compression },
    /// Plain LZ4, for the tail rewritten at each publish.
    fast,
};

/// Header and stored payload of the page of `count` values from `first_id`
/// whose payload is `raw`, `width` bytes a value (0: strings). Caller owns
/// the result.
fn encodePage(allocator: Allocator, packing: Packing, first_id: u32, count: u32, raw: []const u8, width: u8) compression.Error![]u8 {
    var directory: std.ArrayList(u8) = .empty;
    defer directory.deinit(allocator);
    var rewritten: std.ArrayList(u8) = .empty;
    defer rewritten.deinit(allocator);
    var source = raw;
    if (width > 0) switch (packing) {
        .table => |t| if (t.codec != .none) {
            const stream: int_streams.Stream = .{ .offset = 0, .count = count, .width = width, .signed = true };
            if (try int_streams.transform(allocator, t.compressor, raw, &.{stream}, &directory, &rewritten)) source = rewritten.items;
        },
        .fast => {},
    };
    var codec: format.Compression = .none;
    var compressed: ?[]u8 = null;
    defer if (compressed) |c| allocator.free(c);
    switch (packing) {
        .table => |t| switch (t.codec) {
            .none => {},
            .zstd => {
                // A string list is never read by a query, so its level trades
                // only the load's CPU for bytes on disk; a fixed-width list's
                // reads are rarer than its blocks'.
                compressed = try t.compressor.compress(allocator, source, compression.default_level);
                codec = .zstd;
            },
            .lz4 => {
                compressed = try t.compressor.compressLz4HC(allocator, source);
                codec = .lz4;
            },
        },
        .fast => {
            compressed = try compression.lz4CompressFast(allocator, source);
            codec = .lz4;
        },
    }
    var body = source;
    if (compressed) |c| {
        if (c.len < source.len) body = c else codec = .none;
    }
    const stored_len = directory.items.len + body.len;
    const page = try allocator.alloc(u8, page_header_size + stored_len);
    @memcpy(page[0..4], &page_magic);
    format.writeU32(page[4..8], first_id);
    format.writeU32(page[8..12], count);
    format.writeU32(page[12..16], @intCast(raw.len));
    format.writeU32(page[16..20], @intCast(stored_len));
    page[20] = @backingInt(codec);
    page[21] = width;
    page[22] = if (directory.items.len > 0) page_streams_flag else 0;
    page[23] = 0;
    @memcpy(page[page_header_size..][0..directory.items.len], directory.items);
    @memcpy(page[page_header_size + directory.items.len ..], body);
    format.writeU64(page[24..32], checksum(page[0..page_header_size], page[page_header_size..]));
    return page;
}

const DecodedPage = struct { first_id: u32, view: PageView };

/// Checks `bytes`, one page, and decodes its payload into `raw`. Null when
/// it is damaged, or doesn't start at `first_id` when one is given.
fn decodePage(allocator: Allocator, bytes: []const u8, first_id: ?u32, raw: *std.ArrayList(u8)) Allocator.Error!?DecodedPage {
    if (bytes.len < page_header_size) return null;
    const h = parseHeader(bytes[0..page_header_size]) orelse return null;
    if (first_id) |f| if (h.first_id != f) return null;
    if (bytes.len != page_header_size + @as(usize, h.stored_len)) return null;
    const stored = bytes[page_header_size..];
    if (format.readU64(bytes[24..32]) != checksum(bytes[0..page_header_size], stored)) return null;
    try raw.resize(allocator, h.raw_len);
    var body = stored;
    var into = raw.items;
    var directory: ?int_streams.Directory = null;
    if (h.streams) {
        const d = int_streams.readDirectory(stored, h.raw_len) catch return null;
        directory = d;
        body = stored[d.bytes.len..];
        into = raw.items[raw.items.len - d.stored_len ..];
    }
    switch (h.codec) {
        .none => {
            if (body.len != into.len) return null;
            @memcpy(into, body);
        },
        .zstd => compression.decompressInto(body, into) catch return null,
        .lz4 => compression.lz4DecompressInto(body, into) catch return null,
    }
    if (directory) |d| int_streams.restore(allocator, d, raw.items) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return null,
    };
    const view = PageView.of(raw.items, h.count, h.width) orelse return null;
    return .{ .first_id = h.first_id, .view = view };
}

/// One worker's buffers for reading pages.
const Scratch = struct {
    page: std.ArrayList(u8) = .empty,
    raw: std.ArrayList(u8) = .empty,

    fn deinit(self: *Scratch, allocator: Allocator) void {
        self.page.deinit(allocator);
        self.raw.deinit(allocator);
    }

    /// Reads and decodes `page` of `file`; null when it is damaged.
    fn read(self: *Scratch, allocator: Allocator, io: Io, file: Io.File, page: Page) JobError!?PageView {
        try self.page.resize(allocator, page_header_size + @as(usize, page.stored_len));
        if (try file.readPositionalAll(io, self.page.items, page.offset) != self.page.items.len) return null;
        const decoded = try decodePage(allocator, self.page.items, page.first_id, &self.raw) orelse return null;
        if (decoded.view.count != page.count) return null;
        return decoded.view;
    }
};

// ---------------------------------------------------------------------------
// Parallel jobs
// ---------------------------------------------------------------------------

/// The first error and the lowest damaged job a pass's workers met.
fn Failure(comptime E: type) type {
    return struct {
        lock: std.atomic.Mutex = .unlocked,
        err: ?E = null,
        damaged: std.atomic.Value(usize) = .init(std.math.maxInt(usize)),
        stop: std.atomic.Value(bool) = .init(false),

        fn fail(self: *@This(), e: E) void {
            while (!self.lock.tryLock()) std.atomic.spinLoopHint();
            if (self.err == null) self.err = e;
            self.lock.unlock();
            self.stop.store(true, .release);
        }

        fn damage(self: *@This(), job: usize) void {
            _ = self.damaged.fetchMin(job, .monotonic);
        }

        fn stopped(self: *@This()) bool {
            return self.stop.load(.acquire);
        }
    };
}

fn ProbeJob(comptime Values: type) type {
    return struct {
        index: *const Index,
        values: Values,
        hashes: []u64,
        /// Each value's first candidate slot, or `none`.
        at: []u32,

        fn run(self: *@This(), _: usize, lo: usize, hi: usize) void {
            for (lo..hi) |i| {
                const h = std.hash.Wyhash.hash(0, self.values.get(i));
                self.hashes[i] = h;
                self.at[i] = self.index.next(h, self.index.home(h)) orelse none;
            }
        }
    };
}

const RebuildJob = struct {
    allocator: Allocator,
    io: Io,
    file: Io.File,
    pages: []const Page,
    index: *Index,
    scratch: [parallel.MAX_THREADS]Scratch = @splat(.{}),
    failure: Failure(JobError) = .{},

    fn run(self: *RebuildJob, t: usize, j: usize) void {
        if (self.failure.stopped()) return;
        const page = self.pages[j];
        const view = self.scratch[t].read(self.allocator, self.io, self.file, page) catch |e| return self.failure.fail(e);
        const v = view orelse return self.failure.damage(j);
        for (0..v.count) |k| self.index.insertShared(std.hash.Wyhash.hash(0, v.value(k)), page.first_id + @as(u32, @intCast(k)));
        _ = @atomicRmw(usize, &self.index.count, .Add, @as(usize, v.count), .monotonic);
    }
};

const CheckGroup = struct { page: u32, lo: u32, hi: u32 };

fn VerifyJob(comptime Values: type) type {
    return struct {
        allocator: Allocator,
        io: Io,
        file: Io.File,
        pages: []const Page,
        groups: []const CheckGroup,
        checks: []const ColumnList.Check,
        values: Values,
        ids: []u32,
        scratch: [parallel.MAX_THREADS]Scratch = @splat(.{}),
        failure: Failure(JobError) = .{},

        fn run(self: *@This(), t: usize, j: usize) void {
            if (self.failure.stopped()) return;
            const group = self.groups[j];
            const page = self.pages[group.page];
            const view = self.scratch[t].read(self.allocator, self.io, self.file, page) catch |e| return self.failure.fail(e);
            const v = view orelse return self.failure.damage(j);
            for (self.checks[group.lo..group.hi]) |c| {
                if (std.mem.eql(u8, v.value(c.id - page.first_id), self.values.get(c.value))) self.ids[c.value] = c.id;
            }
        }
    };
}

const SealJob = struct {
    allocator: Allocator,
    column: *const ColumnList,
    cuts: []const ColumnList.Cut,
    out: [][]u8,
    compressors: [parallel.MAX_THREADS]?compression.Compressor = @splat(null),
    failure: Failure(compression.Error) = .{},

    fn run(self: *SealJob, t: usize, j: usize) void {
        if (self.failure.stopped()) return;
        if (self.compressors[t] == null) self.compressors[t] = compression.Compressor.init() catch |e| return self.failure.fail(e);
        const cut = self.cuts[j];
        const raw = self.column.tailPayload(self.allocator, cut.lo, cut.hi) catch |e| return self.failure.fail(e);
        defer self.allocator.free(raw);
        const packing: Packing = .{ .table = .{ .compressor = &self.compressors[t].?, .codec = self.column.codec } };
        self.out[j] = encodePage(self.allocator, packing, self.column.sealed + cut.lo, cut.hi - cut.lo, raw, self.column.width) catch |e| return self.failure.fail(e);
    }
};

test {
    _ = @import("table_strings_test.zig");
}
