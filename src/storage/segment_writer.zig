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
    sync_on_close: bool,
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

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    // One zstd context, reused for every column block in this segment. Avoids
    // ~200 µs of CCtx setup/teardown per call (typically 64+ calls per flush).
    var compressor = try compression_mod.Compressor.init();
    defer compressor.deinit();

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

        // Record each column block's absolute file offset so the reader can
        // pread individual columns without loading the whole segment.
        const col_offsets = try allocator.alloc(u64, columns.len);
        errdefer allocator.free(col_offsets);
        for (columns, schema.columns, 0..) |view, schema_col, ci| {
            col_offsets[ci] = @intCast(buf.items.len);
            try writeColumnBlock(allocator, &compressor, &buf, view, schema_col.nullable, row_offset, row_offset + rows_in_group);
        }

        const rg_length: u32 = @intCast(buf.items.len - rg_file_offset);

        const stats = try allocator.alloc(format.Stats, columns.len);
        errdefer allocator.free(stats);
        for (columns, 0..) |view, ci| {
            stats[ci] = computeStats(view, row_offset, row_offset + rows_in_group);
        }

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
        }
        for (rg.col_offsets) |co| {
            try appendU64(allocator, &buf, co);
        }
    }
    const footer_size: u32 = @intCast(buf.items.len - footer_start + format.footer_trailer_size);
    try appendU32(allocator, &buf, footer_size);
    try buf.appendSlice(allocator, &format.segment_magic);

    // ---- Per-column HyperLogLog sketches (whole-segment distinct est) ----
    const column_sketches = try computeSketches(allocator, columns, row_count);
    errdefer allocator.free(column_sketches);

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

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

const appendU16 = format.appendU16;
const appendU32 = format.appendU32;
const appendI128 = format.appendI128;
const appendU64 = format.appendU64;
const appendI32 = format.appendI32;
const appendI64 = format.appendI64;

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

    // Try a narrow encoding first; fall back to raw. `tryEncodeFor` (int-family)
    // and `tryEncodeDict` (low-card strings) each append their body onto
    // `scratch` (after the bitmap) and return true iff they committed; on false
    // `scratch` is untouched past the bitmap. The two are mutually exclusive by
    // column type, so the order between them doesn't matter.
    const encoding: format.Encoding = if (try tryEncodeFor(allocator, &scratch, view, row_start, row_end))
        .for_
    else if (try tryEncodeDict(allocator, &scratch, view, row_start, row_end))
        .dict
    else blk: {
        try writeRawColumnBlock(allocator, &scratch, view, row_start, row_end);
        break :blk .raw;
    };

    const raw_size: u32 = @intCast(scratch.items.len);

    // Try compressing. If the result is smaller, use it; otherwise keep raw.
    const compressed = try compressor.compress(allocator, scratch.items);
    defer allocator.free(compressed);

    const use_compressed = compressed.len < scratch.items.len;
    const kind: format.Compression = if (use_compressed) .zstd else .none;
    const payload_size: u32 = if (use_compressed)
        @intCast(compressed.len)
    else
        raw_size;

    const flags = format.ColumnBlockFlags{ .has_nulls = has_nulls };

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
        buf.appendSliceAssumeCapacity(compressed);
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
            for (d[row_start..row_end], row_start..) |v, r| {
                if (!view.isValid(r)) continue;
                const enc = format.encodeUnsignedU128(v);
                if (enc < lo) lo = enc;
                if (enc > hi) hi = enc;
            }
            break :blk .{ .min = lo, .max = hi };
        },
        // Strings store the prefix-encoded i128 of the first 16 bytes of each
        // row's value. See `format.encodeStringPrefix`.
        .varchar, .string, .char => |sv| computeStringStats(view, sv, row_start, row_end),
        // Floats carry no stats today (NaN/sign handling deferred).
        .float, .double => .{ .min = 0, .max = 0 },
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
    for (data[row_start..row_end], row_start..) |v, r| {
        if (!view.isValid(r)) continue;
        if (v < lo) lo = v;
        if (v > hi) hi = v;
    }
    return .{ .min = @intCast(lo), .max = @intCast(hi) };
}

fn computeStringStats(view: ColumnView, sv: StringView, row_start: usize, row_end: usize) format.Stats {
    var lo: i128 = std.math.maxInt(i128);
    var hi: i128 = std.math.minInt(i128);
    var i = row_start;
    while (i < row_end) : (i += 1) {
        if (!view.isValid(i)) continue;
        const enc = format.encodeStringPrefix(sv.rowBytes(i));
        if (enc < lo) lo = enc;
        if (enc > hi) hi = enc;
    }
    return .{ .min = lo, .max = hi };
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
