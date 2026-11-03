//! Segment string tables (DESIGN.md §4.4): one id per distinct value of a
//! string column within a segment. The writer gathers a column's row groups
//! as sorted per-row-group dictionaries over the segment's distinct values,
//! then lays out the shared-string pages and the row-group blocks at segment
//! end. The reader rebuilds each row-group block into an ordinary `.dict`
//! payload that also carries every value's segment id.
//!
//! Row-group block payload, past the validity bitmap (on-disk encoding byte
//! `format.segment_dict_encoding`):
//!
//!   [ndv: u32]
//!   [code_width: u8][flags: u8][rank_width: u8][pad]
//!   [local_count: u32][local_first_id: u32]
//!   [shared: ceil(ndv/8)]                bit e set when value e is shared
//!   [ranks: (ndv-local_count) × rank_width]  shared values' table ranks
//!   [local_offsets: (local_count+1) × u32]
//!   [local_bytes]                        the block's own values, in order
//!   [codes: row_count × code_width]      as `.dict`
//!   [entry stats]                        as `.dict`, per `flags`
//!
//! Shared-string page payload: [count: u32][(count+1) × u32 offsets][bytes].

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const format = @import("format.zig");
const compression = @import("compression.zig");
const column = @import("column.zig");
const ColumnView = column.ColumnView;
const StringView = column.StringView;

/// Distinct values one segment's column may hold; past it the column's later
/// row groups fall back to the per-row-group encodings.
pub const segment_value_cap: u32 = 1 << 26;
/// Shared values per page, the unit a reader reads and caches.
pub const page_values: u32 = 1 << 16;
/// Shared tables of up to this many values store a rank in 3 bytes.
const narrow_rank_limit: u32 = 1 << 24;
const block_header_bytes: usize = 16;

pub fn eligible(t: types.Type) bool {
    return switch (t) {
        .varchar, .string, .char => true,
        else => false,
    };
}

pub fn rankWidth(shared_count: u32) u8 {
    return if (shared_count <= narrow_rank_limit) 3 else 4;
}

pub fn pageCount(shared_count: u32) u32 {
    return @intCast((@as(u64, shared_count) + page_values - 1) / page_values);
}

const SortCtx = struct {
    items: []const []const u8,
    fn lessThan(ctx: SortCtx, a: u32, b: u32) bool {
        return std.mem.lessThan(u8, ctx.items[a], ctx.items[b]);
    }
};

/// A column's distinct values, their bytes packed into chunks: a segment holds
/// every distinct value until its end, and a growing arena's newest block can
/// leave a third of what it holds unused. Chunks grow 4x up to
/// `max_chunk_size`, past glibc's largest mmap threshold (32 MiB), so a big
/// column's chunks are mapped straight from the OS and go back to it when the
/// segment is written; smaller ones stay in malloc's heap once freed, which a
/// merge of tens of millions of rows leaves holding gigabytes it no longer
/// uses.
const ValueStore = struct {
    chunks: std.ArrayList([]u8) = .empty,
    refs: std.ArrayList(Ref) = .empty,
    /// The chunk taking new values, and its bytes used.
    open: ?u32 = null,
    open_used: usize = 0,
    next_chunk_size: usize = first_chunk_size,

    const first_chunk_size: usize = 64 << 10;
    const max_chunk_size: usize = 64 << 20;
    /// Larger values each take a chunk of their own size, so any other value
    /// fits a new chunk, and a chunk's unused tail stays under this.
    const inline_limit: usize = first_chunk_size;

    const Ref = struct { chunk: u32, offset: u32, len: u32 };

    fn deinit(self: *ValueStore, allocator: Allocator) void {
        for (self.chunks.items) |c| allocator.free(c);
        self.chunks.deinit(allocator);
        self.refs.deinit(allocator);
    }

    fn len(self: *const ValueStore) usize {
        return self.refs.items.len;
    }

    fn value(self: *const ValueStore, v: u32) []const u8 {
        const r = self.refs.items[v];
        return self.chunks.items[r.chunk][r.offset..][0..r.len];
    }

    /// Adds a copy of `bytes` as the next value. The caller has reserved its
    /// ref with `refs.ensureUnusedCapacity`.
    fn appendAssumeRefCapacity(self: *ValueStore, allocator: Allocator, bytes: []const u8) !void {
        if (bytes.len > inline_limit) {
            try self.chunks.ensureUnusedCapacity(allocator, 1);
            const own = try allocator.dupe(u8, bytes);
            self.chunks.appendAssumeCapacity(own);
            self.refs.appendAssumeCapacity(.{ .chunk = @intCast(self.chunks.items.len - 1), .offset = 0, .len = @intCast(bytes.len) });
            return;
        }
        if (self.open == null or self.open_used + bytes.len > self.chunks.items[self.open.?].len) {
            try self.chunks.ensureUnusedCapacity(allocator, 1);
            self.chunks.appendAssumeCapacity(try allocator.alloc(u8, self.next_chunk_size));
            self.next_chunk_size = @min(self.next_chunk_size * 4, max_chunk_size);
            self.open = @intCast(self.chunks.items.len - 1);
            self.open_used = 0;
        }
        const open = self.open.?;
        @memcpy(self.chunks.items[open][self.open_used..][0..bytes.len], bytes);
        self.refs.appendAssumeCapacity(.{ .chunk = open, .offset = @intCast(self.open_used), .len = @intCast(bytes.len) });
        self.open_used += bytes.len;
    }
};

const StoreSortCtx = struct {
    store: *const ValueStore,
    fn lessThan(ctx: StoreSortCtx, a: u32, b: u32) bool {
        return std.mem.lessThan(u8, ctx.store.value(a), ctx.store.value(b));
    }
};

/// One segment's builders, one per schema column (null where ineligible).
pub const SegmentStrings = struct {
    allocator: Allocator,
    columns: []?ColumnStrings,

    /// A `.lz4_fsst` table chose FSST for its large string blocks, so its
    /// strings keep the per-row-group encodings.
    pub fn init(allocator: Allocator, schema: types.TableSchema, cap: u32) !SegmentStrings {
        const columns = try allocator.alloc(?ColumnStrings, schema.columns.len);
        for (columns, schema.columns) |*c, col| {
            c.* = if (eligible(col.type) and schema.compression != .lz4_fsst) ColumnStrings.init(allocator, cap) else null;
        }
        return .{ .allocator = allocator, .columns = columns };
    }

    pub fn deinit(self: *SegmentStrings) void {
        for (self.columns) |*c| if (c.*) |*cs| cs.deinit();
        self.allocator.free(self.columns);
        self.* = undefined;
    }
};

/// One string column's distinct values across a segment, and the row-group
/// blocks deferred until the segment's end.
pub const ColumnStrings = struct {
    allocator: Allocator,
    cap: u32,
    /// Set once a row group didn't fit under `cap`; the column takes no more.
    full: bool = false,
    values: ValueStore = .{},
    /// Per value, the one row group holding it, or `shared_owner`.
    owners: std.ArrayList(u32) = .empty,
    index: Index = .empty,
    blocks: std.ArrayList(Block) = .empty,
    /// Per value, its segment id; set by `assignIds`.
    ids: []u32 = &.{},
    /// The shared values by rank; set by `assignIds`.
    shared: []u32 = &.{},

    const shared_owner = std.math.maxInt(u32);

    const Index = std.HashMapUnmanaged(u32, void, IndexContext, std.hash_map.default_max_load_percentage);

    const IndexContext = struct {
        values: *const ValueStore,
        pub fn hash(ctx: IndexContext, v: u32) u64 {
            return std.hash.Wyhash.hash(0, ctx.values.value(v));
        }
        pub fn eql(_: IndexContext, a: u32, b: u32) bool {
            return a == b;
        }
    };

    const BytesAdapter = struct {
        values: *const ValueStore,
        pub fn hash(_: BytesAdapter, bytes: []const u8) u64 {
            return std.hash.Wyhash.hash(0, bytes);
        }
        pub fn eql(ctx: BytesAdapter, bytes: []const u8, v: u32) bool {
            return std.mem.eql(u8, bytes, ctx.values.value(v));
        }
    };

    pub const Block = struct {
        rg_idx: u32,
        row_count: u32,
        /// The segment value of each entry of the row group's sorted dictionary.
        values: []u32,
        bitmap: []u8,
        code_width: u8,
        codes_len: u32,
        /// The `.dict` entry stats' length; 0 when the block carries none.
        stats_len: u32,
        /// The codes then the entry stats, LZ4-compressed: a segment holds
        /// every block until its end, and uncompressed codes alone cost a
        /// byte or two per row per string column.
        held: []u8,
    };

    pub fn init(allocator: Allocator, cap: u32) ColumnStrings {
        return .{ .allocator = allocator, .cap = cap, .full = cap == 0 };
    }

    pub fn deinit(self: *ColumnStrings) void {
        for (self.blocks.items) |b| self.freeBlock(b);
        self.blocks.deinit(self.allocator);
        self.index.deinit(self.allocator);
        self.owners.deinit(self.allocator);
        self.values.deinit(self.allocator);
        self.allocator.free(self.ids);
        self.allocator.free(self.shared);
        self.* = undefined;
    }

    pub fn value(self: *const ColumnStrings, v: u32) []const u8 {
        return self.values.value(v);
    }

    fn freeBlock(self: *ColumnStrings, b: Block) void {
        self.allocator.free(b.values);
        self.allocator.free(b.bitmap);
        self.allocator.free(b.held);
    }

    /// Takes rows `row_start..row_end` as row group `rg_idx`'s block,
    /// deferred to the segment's end. False when the column can't take them
    /// (every row NULL, or the segment's values past `cap`); the caller then
    /// encodes the block in place. `bitmap` is the block's validity prefix,
    /// empty for a non-nullable column.
    pub fn absorb(
        self: *ColumnStrings,
        view: ColumnView,
        row_start: usize,
        row_end: usize,
        rg_idx: u32,
        bitmap: []const u8,
        entry_stats: bool,
    ) !bool {
        if (self.full) return false;
        const sv: StringView = switch (view.data) {
            .varchar, .string, .char => |s| s,
            else => return false,
        };
        const allocator = self.allocator;
        const n = row_end - row_start;

        var local: std.StringHashMapUnmanaged(u32) = .empty;
        defer local.deinit(allocator);
        var distinct: std.ArrayList([]const u8) = .empty;
        defer distinct.deinit(allocator);
        const row_codes = try allocator.alloc(u32, n);
        defer allocator.free(row_codes);
        for (row_codes, row_start..) |*code, r| {
            if (!view.isValid(r)) {
                code.* = 0;
                continue;
            }
            const s = sv.rowBytes(r);
            const gop = try local.getOrPut(allocator, s);
            if (!gop.found_existing) {
                gop.value_ptr.* = @intCast(distinct.items.len);
                try distinct.append(allocator, s);
            }
            code.* = gop.value_ptr.*;
        }
        const k = distinct.items.len;
        if (k == 0) return false;

        const order = try allocator.alloc(u32, k);
        defer allocator.free(order);
        for (order, 0..) |*o, i| o.* = @intCast(i);
        std.sort.pdq(u32, order, SortCtx{ .items = distinct.items }, SortCtx.lessThan);

        const entries = try allocator.alloc(u32, k);
        errdefer allocator.free(entries);
        if (!try self.intern(distinct.items, order, rg_idx, entries)) {
            allocator.free(entries);
            return false;
        }

        const ins_to_sorted = try allocator.alloc(u32, k);
        defer allocator.free(ins_to_sorted);
        for (order, 0..) |ins, p| ins_to_sorted[ins] = @intCast(p);

        const code_width: u8 = if (k <= 256) 1 else if (k <= 65536) 2 else 4;
        const codes_len = n * code_width;
        var dict_bytes: usize = 0;
        for (distinct.items) |s| dict_bytes += s.len;
        const stat_width = format.dictEntryStatWidth(@intCast(n));
        // The same rule as a `.dict` block: stats only while the block with
        // them stays under the raw layout, so a near-unique block skips them.
        const dict_body = 8 + (k + 1) * 4 + dict_bytes + codes_len;
        const raw_body = 4 + (n + 1) * 4 + (sv.offsets[row_end] - sv.offsets[row_start]);
        const with_stats = entry_stats and dict_body + 2 * k * stat_width < raw_body;
        const stats_len = if (with_stats) 2 * k * stat_width else 0;
        const raw = try allocator.alloc(u8, codes_len + stats_len);
        defer allocator.free(raw);
        const codes = raw[0..codes_len];
        const stats = raw[codes_len..];
        for (row_codes, 0..) |ic, i| {
            var b4: [4]u8 = undefined;
            std.mem.writeInt(u32, &b4, ins_to_sorted[ic], .little);
            @memcpy(codes[i * code_width ..][0..code_width], b4[0..code_width]);
        }

        if (with_stats) {
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
            // Every value occurs, so a count is stored less one.
            for (counts, first_rows, 0..) |count, first, e| {
                var b4: [4]u8 = undefined;
                std.mem.writeInt(u32, &b4, count - 1, .little);
                @memcpy(stats[e * stat_width ..][0..stat_width], b4[0..stat_width]);
                std.mem.writeInt(u32, &b4, first, .little);
                @memcpy(stats[(k + e) * stat_width ..][0..stat_width], b4[0..stat_width]);
            }
        }

        const held = try compression.lz4CompressFast(allocator, raw);
        errdefer allocator.free(held);
        const bitmap_copy = try allocator.dupe(u8, bitmap);
        errdefer allocator.free(bitmap_copy);
        try self.blocks.append(allocator, .{
            .rg_idx = rg_idx,
            .row_count = @intCast(n),
            .values = entries,
            .bitmap = bitmap_copy,
            .code_width = code_width,
            .codes_len = @intCast(codes_len),
            .stats_len = @intCast(stats_len),
            .held = held,
        });
        return true;
    }

    /// Looks up or adds each of a row group's distinct values (`distinct` in
    /// `order`), filling `entries` with their segment values. False, with
    /// every change undone, when the new values would pass `cap`.
    fn intern(self: *ColumnStrings, distinct: []const []const u8, order: []const u32, rg_idx: u32, entries: []u32) !bool {
        const allocator = self.allocator;
        const start = self.values.len();
        const Reowned = struct { value: u32, owner: u32 };
        var reowned: std.ArrayList(Reowned) = .empty;
        defer reowned.deinit(allocator);
        try self.values.refs.ensureUnusedCapacity(allocator, order.len);
        try self.owners.ensureUnusedCapacity(allocator, order.len);
        const adapter = BytesAdapter{ .values = &self.values };
        const ctx = IndexContext{ .values = &self.values };

        for (order, entries) |ins, *entry| {
            const bytes = distinct[ins];
            const existing = if (self.values.len() == self.cap) self.index.getKeyAdapted(bytes, adapter) else null;
            const v: u32 = existing orelse found: {
                if (self.values.len() == self.cap) {
                    for (reowned.items) |r| self.owners.items[r.value] = r.owner;
                    for (start..self.values.len()) |added| _ = self.index.removeContext(@intCast(added), ctx);
                    self.values.refs.shrinkRetainingCapacity(start);
                    self.owners.shrinkRetainingCapacity(start);
                    self.full = true;
                    return false;
                }
                const gop = try self.index.getOrPutContextAdapted(allocator, bytes, adapter, ctx);
                if (gop.found_existing) break :found gop.key_ptr.*;
                const added: u32 = @intCast(self.values.len());
                self.values.appendAssumeRefCapacity(allocator, bytes) catch |e| {
                    self.index.removeByPtr(gop.key_ptr);
                    return e;
                };
                self.owners.appendAssumeCapacity(rg_idx);
                gop.key_ptr.* = added;
                entry.* = added;
                continue;
            };
            // A row group's values are distinct, so an existing value belongs
            // to an earlier row group (or is already shared).
            if (self.owners.items[v] != shared_owner) {
                try reowned.append(allocator, .{ .value = v, .owner = self.owners.items[v] });
                self.owners.items[v] = shared_owner;
            }
            entry.* = v;
        }
        return true;
    }

    /// Ranks the shared values and numbers the local ones after them, row
    /// group by row group in dictionary order (DESIGN.md §4.4).
    pub fn assignIds(self: *ColumnStrings) !void {
        const allocator = self.allocator;
        var shared_count: usize = 0;
        for (self.owners.items) |o| shared_count += @intFromBool(o == shared_owner);
        const shared = try allocator.alloc(u32, shared_count);
        errdefer allocator.free(shared);
        var i: usize = 0;
        for (self.owners.items, 0..) |o, v| if (o == shared_owner) {
            shared[i] = @intCast(v);
            i += 1;
        };
        std.sort.pdq(u32, shared, StoreSortCtx{ .store = &self.values }, StoreSortCtx.lessThan);

        const ids = try allocator.alloc(u32, self.values.len());
        errdefer allocator.free(ids);
        for (shared, 0..) |v, rank| ids[v] = @intCast(rank);
        var next: u32 = @intCast(shared_count);
        for (self.blocks.items) |b| for (b.values) |v| if (self.owners.items[v] != shared_owner) {
            ids[v] = next;
            next += 1;
        };
        self.shared = shared;
        self.ids = ids;
    }

    pub fn sharedCount(self: ColumnStrings) u32 {
        return @intCast(self.shared.len);
    }

    pub fn valueCount(self: ColumnStrings) u32 {
        return @intCast(self.values.len());
    }

    pub fn valueBytes(self: ColumnStrings) u64 {
        var bytes: u64 = 0;
        for (self.values.refs.items) |r| bytes += r.len;
        return bytes;
    }

    /// Appends shared-string page `p`'s payload to `out`.
    pub fn pagePayload(self: ColumnStrings, allocator: Allocator, p: u32, out: *std.ArrayList(u8)) !void {
        const lo = @as(usize, p) * page_values;
        const hi = @min(self.shared.len, lo + page_values);
        var bytes: usize = 0;
        for (self.shared[lo..hi]) |v| bytes += self.value(v).len;
        try out.ensureUnusedCapacity(allocator, 4 + (hi - lo + 1) * 4 + bytes);
        appendU32(out, @intCast(hi - lo));
        appendU32(out, 0);
        var acc: u32 = 0;
        for (self.shared[lo..hi]) |v| {
            acc += @intCast(self.value(v).len);
            appendU32(out, acc);
        }
        for (self.shared[lo..hi]) |v| out.appendSliceAssumeCapacity(self.value(v));
    }

    /// Appends block `b`'s payload to `out`, after `assignIds`.
    pub fn blockPayload(self: ColumnStrings, allocator: Allocator, b: Block, out: *std.ArrayList(u8)) !void {
        const k = b.values.len;
        var local_count: usize = 0;
        var local_bytes: usize = 0;
        var local_first_id: u32 = self.sharedCount();
        for (b.values) |v| if (self.owners.items[v] != shared_owner) {
            if (local_count == 0) local_first_id = self.ids[v];
            local_count += 1;
            local_bytes += self.value(v).len;
        };
        const rank_width = rankWidth(self.sharedCount());
        const shared_bits = (k + 7) / 8;
        try out.ensureUnusedCapacity(allocator, b.bitmap.len + block_header_bytes + shared_bits +
            (k - local_count) * rank_width + (local_count + 1) * 4 + local_bytes + b.codes_len + b.stats_len);

        out.appendSliceAssumeCapacity(b.bitmap);
        appendU32(out, @intCast(k));
        const flags: u8 = if (b.stats_len > 0) format.DICT_ENTRY_STATS else 0;
        out.appendSliceAssumeCapacity(&[_]u8{ b.code_width, flags, rank_width, 0 });
        appendU32(out, @intCast(local_count));
        appendU32(out, local_first_id);

        const bits_at = out.items.len;
        out.appendNTimesAssumeCapacity(0, shared_bits);
        for (b.values, 0..) |v, e| if (self.owners.items[v] == shared_owner) {
            out.items[bits_at + e / 8] |= @as(u8, 1) << @intCast(e % 8);
            var b4: [4]u8 = undefined;
            std.mem.writeInt(u32, &b4, self.ids[v], .little);
            out.appendSliceAssumeCapacity(b4[0..rank_width]);
        };
        appendU32(out, 0);
        var acc: u32 = 0;
        for (b.values) |v| if (self.owners.items[v] != shared_owner) {
            acc += @intCast(self.value(v).len);
            appendU32(out, acc);
        };
        for (b.values) |v| if (self.owners.items[v] != shared_owner) out.appendSliceAssumeCapacity(self.value(v));
        const tail = out.unusedCapacitySlice()[0 .. b.codes_len + b.stats_len];
        try compression.lz4DecompressInto(b.held, tail);
        out.items.len += tail.len;
    }
};

fn appendU32(out: *std.ArrayList(u8), v: u32) void {
    var b4: [4]u8 = undefined;
    format.writeU32(&b4, v);
    out.appendSliceAssumeCapacity(&b4);
}

/// A shared-string page's values, checked once when the page is read.
pub const Page = struct {
    count: u32,
    offsets: []const u8,
    bytes: []const u8,

    pub fn of(payload: []const u8, expected_count: u32) !Page {
        if (payload.len < 8) return format.Error.CorruptColumnBlockHeader;
        const count = format.readU32(payload[0..4]);
        if (count != expected_count) return format.Error.CorruptColumnBlockHeader;
        const offsets_len = (@as(usize, count) + 1) * 4;
        if (payload.len < 4 + offsets_len) return format.Error.CorruptColumnBlockHeader;
        const page: Page = .{
            .count = count,
            .offsets = payload[4..][0..offsets_len],
            .bytes = payload[4 + offsets_len ..],
        };
        var prev: u32 = 0;
        for (0..@as(usize, count) + 1) |i| {
            const o = format.readU32(page.offsets[i * 4 ..][0..4]);
            if (o < prev) return format.Error.CorruptColumnBlockHeader;
            prev = o;
        }
        if (prev != page.bytes.len) return format.Error.CorruptColumnBlockHeader;
        return page;
    }

    pub fn value(self: Page, i: u32) []const u8 {
        const a = format.readU32(self.offsets[@as(usize, i) * 4 ..][0..4]);
        const b = format.readU32(self.offsets[(@as(usize, i) + 1) * 4 ..][0..4]);
        return self.bytes[a..b];
    }
};

/// The shared-string page count of page `p` of a table of `shared_count`.
pub fn pageValueCount(shared_count: u32, p: u32) u32 {
    return @min(page_values, shared_count - p * page_values);
}

/// Rebuilds a row-group block's decompressed payload (validity bitmap first
/// when `has_nulls`) into a `.dict` payload with `format.DICT_SEGMENT_IDS`,
/// allocated 16-aligned from `allocator`. `pages.page(p)` yields shared page
/// `p` of the column's table.
pub fn rebuildDict(
    allocator: Allocator,
    payload: []const u8,
    row_count: u32,
    has_nulls: bool,
    table: format.StringTable,
    pages: anytype,
) ![]align(16) u8 {
    const corrupt = format.Error.CorruptColumnBlockHeader;
    const bitmap_len: usize = if (has_nulls) column.bitmapBytes(row_count) else 0;
    if (payload.len < bitmap_len + block_header_bytes) return corrupt;
    const values = payload[bitmap_len..];
    const k = format.readU32(values[0..4]);
    const code_width = values[4];
    const flags = values[5];
    const rank_width = values[6];
    const local_count = format.readU32(values[8..12]);
    const local_first_id = format.readU32(values[12..16]);
    if (local_count > k or (code_width != 1 and code_width != 2 and code_width != 4)) return corrupt;
    if (rank_width != 3 and rank_width != 4) return corrupt;
    if (local_count > 0 and (local_first_id < table.shared_count or @as(u64, local_first_id) + local_count > table.value_count)) return corrupt;

    const shared_bits_len = (@as(usize, k) + 7) / 8;
    const ranks_len = @as(usize, k - local_count) * rank_width;
    const local_offsets_len = (@as(usize, local_count) + 1) * 4;
    var at: usize = block_header_bytes;
    if (values.len < at + shared_bits_len + ranks_len + local_offsets_len) return corrupt;
    const shared_bits = values[at..][0..shared_bits_len];
    at += shared_bits_len;
    const ranks = values[at..][0..ranks_len];
    at += ranks_len;
    const local_offsets = values[at..][0..local_offsets_len];
    at += local_offsets_len;
    const local_byte_count = format.readU32(local_offsets[local_count * 4 ..][0..4]);
    if (values.len < at + local_byte_count) return corrupt;
    const local_bytes = values[at..][0..local_byte_count];
    at += local_byte_count;
    const stats_len: usize = if (flags & format.DICT_ENTRY_STATS != 0) 2 * @as(usize, k) * format.dictEntryStatWidth(row_count) else 0;
    const tail_len = @as(usize, row_count) * code_width + stats_len;
    if (values.len != at + tail_len) return corrupt;
    const tail = values[at..];

    const Entries = struct {
        shared_bits: []const u8,
        ranks: []const u8,
        rank_width: u8,
        local_offsets: []const u8,
        local_bytes: []const u8,
        table: format.StringTable,

        fn isShared(e: @This(), i: usize) bool {
            return e.shared_bits[i / 8] & (@as(u8, 1) << @intCast(i % 8)) != 0;
        }
        fn rank(e: @This(), r: usize) !u32 {
            var b4: [4]u8 = .{ 0, 0, 0, 0 };
            @memcpy(b4[0..e.rank_width], e.ranks[r * e.rank_width ..][0..e.rank_width]);
            const v = std.mem.readInt(u32, &b4, .little);
            if (v >= e.table.shared_count) return format.Error.CorruptColumnBlockHeader;
            return v;
        }
        fn local(e: @This(), l: usize) ![]const u8 {
            const a = format.readU32(e.local_offsets[l * 4 ..][0..4]);
            const b = format.readU32(e.local_offsets[(l + 1) * 4 ..][0..4]);
            if (a > b or b > e.local_bytes.len) return format.Error.CorruptColumnBlockHeader;
            return e.local_bytes[a..b];
        }
    };
    const entries: Entries = .{
        .shared_bits = shared_bits,
        .ranks = ranks,
        .rank_width = rank_width,
        .local_offsets = local_offsets,
        .local_bytes = local_bytes,
        .table = table,
    };

    var shared_seen: usize = 0;
    for (0..k) |e| shared_seen += @intFromBool(entries.isShared(e));
    if (shared_seen != k - local_count) return corrupt;

    var dict_bytes: usize = local_byte_count;
    {
        var r: usize = 0;
        for (0..k) |e| if (entries.isShared(e)) {
            const rank = try entries.rank(r);
            dict_bytes += (try pages.page(rank / page_values)).value(rank % page_values).len;
            r += 1;
        };
    }
    if (dict_bytes > std.math.maxInt(u32)) return corrupt;

    const out_len = bitmap_len + 8 + (@as(usize, k) + 1) * 4 + dict_bytes + tail_len + @as(usize, k) * 4;
    const out = try allocator.alignedAlloc(u8, .@"16", out_len);
    errdefer allocator.free(out);
    @memcpy(out[0..bitmap_len], payload[0..bitmap_len]);
    var o: usize = bitmap_len;
    format.writeU32(out[o..][0..4], k);
    out[o + 4] = code_width;
    out[o + 5] = flags | format.DICT_SEGMENT_IDS;
    out[o + 6] = 0;
    out[o + 7] = 0;
    o += 8;
    const offsets_at = o;
    const bytes_at = offsets_at + (@as(usize, k) + 1) * 4;
    const tail_at = bytes_at + dict_bytes;
    const ids_at = tail_at + tail_len;
    format.writeU32(out[offsets_at..][0..4], 0);
    var acc: usize = 0;
    var r: usize = 0;
    var l: usize = 0;
    for (0..k) |e| {
        const bytes, const id = if (entries.isShared(e)) shared: {
            const rank = try entries.rank(r);
            r += 1;
            break :shared .{ (try pages.page(rank / page_values)).value(rank % page_values), rank };
        } else local: {
            const id: u32 = local_first_id + @as(u32, @intCast(l));
            const bytes = try entries.local(l);
            l += 1;
            break :local .{ bytes, id };
        };
        @memcpy(out[bytes_at + acc ..][0..bytes.len], bytes);
        acc += bytes.len;
        format.writeU32(out[offsets_at + (e + 1) * 4 ..][0..4], @intCast(acc));
        format.writeU32(out[ids_at + e * 4 ..][0..4], id);
    }
    if (acc != dict_bytes) return corrupt;
    @memcpy(out[tail_at..][0..tail_len], tail);
    return out;
}

test "ColumnStrings shares values held by two row groups and numbers the rest after them" {
    const allocator = std.testing.allocator;
    var cs = ColumnStrings.init(allocator, segment_value_cap);
    defer cs.deinit();

    const rows = [_][]const u8{ "pear", "fig", "pear", "kiwi", "fig", "date", "apple", "kiwi" };
    var offsets: [rows.len + 1]u32 = undefined;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(allocator);
    offsets[0] = 0;
    for (rows, 1..) |r, i| {
        try bytes.appendSlice(allocator, r);
        offsets[i] = @intCast(bytes.items.len);
    }
    const view: ColumnView = .{ .data = .{ .string = .{ .offsets = &offsets, .bytes = bytes.items } } };
    try std.testing.expect(try cs.absorb(view, 0, 4, 0, &.{}, true));
    try std.testing.expect(try cs.absorb(view, 4, 8, 1, &.{}, true));
    try cs.assignIds();

    // fig and kiwi are in both row groups: ranks 0 and 1, sorted.
    try std.testing.expectEqual(@as(u32, 2), cs.sharedCount());
    try std.testing.expectEqual(@as(u64, "pearfigkiwidateapple".len), cs.valueBytes());
    try std.testing.expectEqualStrings("fig", cs.value(cs.shared[0]));
    try std.testing.expectEqualStrings("kiwi", cs.value(cs.shared[1]));
    // Local values follow in row-group then dictionary order: pear; apple, date.
    const want = [_]struct { []const u8, u32 }{ .{ "fig", 0 }, .{ "kiwi", 1 }, .{ "pear", 2 }, .{ "apple", 3 }, .{ "date", 4 } };
    for (want) |w| {
        const v = cs.index.getKeyAdapted(w[0], ColumnStrings.BytesAdapter{ .values = &cs.values }).?;
        try std.testing.expectEqual(w[1], cs.ids[v]);
    }
}

test "ValueStore packs values into chunks and gives large ones their own" {
    const allocator = std.testing.allocator;
    var store: ValueStore = .{};
    defer store.deinit(allocator);

    const small = 20 << 10;
    const large = ValueStore.inline_limit + 1;
    const buf = try allocator.alloc(u8, large + 8);
    defer allocator.free(buf);
    for (buf, 0..) |*b, i| b.* = @truncate(i *% 31);
    const smalls: [300]usize = @splat(small);
    const lens = [_]usize{ 0, large } ++ smalls;
    try store.refs.ensureUnusedCapacity(allocator, lens.len);
    for (lens, 0..) |n, i| try store.appendAssumeRefCapacity(allocator, buf[i % 7 ..][0..@min(n, buf.len - i % 7)]);

    // 300 values of 20 KiB fill chunks of 64 KiB, 256 KiB, 1 MiB and 4 MiB,
    // then start one of 16 MiB; the large value has its own.
    try std.testing.expectEqual(@as(usize, 6), store.chunks.items.len);
    var sizes: [6]usize = undefined;
    for (store.chunks.items, &sizes) |c, *s| s.* = c.len;
    try std.testing.expectEqualSlices(usize, &.{ 64 << 10, large, 256 << 10, 1 << 20, 4 << 20, 16 << 20 }, &sizes);
    try std.testing.expectEqual(ValueStore.max_chunk_size, store.next_chunk_size);
    for (lens, 0..) |n, i| try std.testing.expectEqualSlices(u8, buf[i % 7 ..][0..@min(n, buf.len - i % 7)], store.value(@intCast(i)));
}

test "ColumnStrings stops at the cap and undoes the row group that passed it" {
    const allocator = std.testing.allocator;
    var cs = ColumnStrings.init(allocator, 3);
    defer cs.deinit();

    const rows = [_][]const u8{ "a", "b", "a", "c", "b", "d" };
    var offsets: [rows.len + 1]u32 = undefined;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(allocator);
    offsets[0] = 0;
    for (rows, 1..) |r, i| {
        try bytes.appendSlice(allocator, r);
        offsets[i] = @intCast(bytes.items.len);
    }
    const view: ColumnView = .{ .data = .{ .string = .{ .offsets = &offsets, .bytes = bytes.items } } };
    try std.testing.expect(try cs.absorb(view, 0, 3, 0, &.{}, false));
    // b is already held, but c and d would make four values.
    try std.testing.expect(!try cs.absorb(view, 3, 6, 1, &.{}, false));
    try std.testing.expect(cs.full);
    try std.testing.expectEqual(@as(u32, 2), cs.valueCount());
    try cs.assignIds();
    // b reverts to row group 0 alone, so nothing is shared.
    try std.testing.expectEqual(@as(u32, 0), cs.sharedCount());
}
