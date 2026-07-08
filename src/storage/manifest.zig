//! Per-table manifest. Lists active segments. Updated atomically via
//! write-tmp-then-rename so readers either see the old or new state, never a
//! partial state.
//!
//! Format (v7, binary, little-endian):
//!
//!   [Header — 32 bytes]
//!     magic "tDBM"            (4)
//!     version u16             (2)
//!     flags u16               (2 — reserved, written 0)
//!     schema_fingerprint u64  (8)
//!     segment_count u32       (4)
//!     column_count u32        (4)
//!     auto_inc_next u64       (8 — MySQL-style table counter for the
//!                              AUTO_INCREMENT column. 0 if no AI column.)
//!
//!   [Entries — entry_prefix_size + column_count*(stats_slot_size +
//!    sketch_slot_size) bytes each]
//!     For each segment:
//!       segment_id u64        (8)
//!       row_count u64         (8)
//!       byte_size u64         (8) — .dat file size
//!       row_group_count u32   (4)
//!       flags u32             (4) — bit 0 = leading_key_stats valid
//!       leading_key_min i128  (16)
//!       leading_key_max i128  (16)
//!       per_column_stats      (stats_slot_size * column_count — i128 min +
//!                              i128 max + i128 sum + u64 null_count each,
//!                              encoded per column type (see `format.Stats`);
//!                              all-zero for columns whose type has no stats)
//!       per_column_sketches   (sketch_slot_size * column_count — one
//!                              HyperLogLog sketch per column, mergeable
//!                              across segments for cardinality-based GROUP BY
//!                              routing; all-zero ⟺ no sketch)
//!
//!   [Trailer]
//!     magic "tDBM"            (4)
//!
//! No backward compatibility with older formats — version mismatches
//! at read time are a hard error.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const format = @import("format.zig");
const hll = @import("../util/hll.zig");
const types = @import("../types.zig");

pub const manifest_magic: [4]u8 = .{ 't', 'D', 'B', 'M' };
pub const manifest_version: u16 = 9;
pub const manifest_filename = "manifest";
pub const manifest_tmp_filename = "manifest.tmp";
pub const header_size: usize = 32;
/// Per-entry fixed prefix (excluding the per-column stats tail).
pub const entry_prefix_size: usize = 64;
pub const stats_slot_size: usize = 56; // i128 min + i128 max + i128 sum + u64 null_count
pub const sketch_slot_size: usize = hll.m; // one HyperLogLog sketch per column
pub const trailer_size: usize = 4;

/// Per-entry flag bits.
const flag_leading_key_stats: u32 = 1 << 0;

pub const Error = error{
    ManifestTooSmall,
    ManifestBadMagic,
    ManifestBadTrailerMagic,
    ManifestUnsupportedVersion,
    ManifestCorrupt,
    SchemaFingerprintMismatch,
};

pub const ManifestEntry = struct {
    segment_id: u64,
    row_count: u64,
    /// Total `.dat` file size in bytes. 0 when reading a v1 manifest.
    byte_size: u64 = 0,
    /// Row group count in the segment. 0 when reading a v1 manifest.
    row_group_count: u32 = 0,
    /// Min/max of the leading order-key column across the whole segment.
    /// `null` when the leading-key column's type doesn't carry stats or
    /// when reading a v1 manifest. Used by `Scan.stats()` for cross-
    /// segment non-overlap detection.
    leading_key_stats: ?format.Stats = null,
    /// Per-column min/max stats — one slot per schema column, in
    /// column-index order. Lifetime is tied to the owning `Manifest`
    /// (freed in `Manifest.deinit`). Empty when reading a v1/v2/v3
    /// manifest (no per-column stats stored). For columns whose type
    /// has no stats (float/double), the slot is `{0, 0}`.
    column_stats: []format.Stats = &.{},
    /// Per-column HyperLogLog sketches, concatenated (`column_count * hll.m`
    /// bytes; column `ci` at `[ci*hll.m ..]`). Mergeable across segments for
    /// a distinct-value estimate. Lifetime tied to the owning `Manifest`.
    /// Empty when not populated.
    column_sketches: []u8 = &.{},
    /// Serialized Bloom filter over the compound primary key (util/bloom.zig
    /// format). Kept in-memory so the upsert existence probe can skip a whole
    /// segment with zero I/O when its keys definitely aren't present. Variable
    /// length (sized to the segment's row count). Empty on non-unique tables.
    /// Lifetime tied to the owning `Manifest`.
    key_bloom: []u8 = &.{},
};

pub const Manifest = struct {
    allocator: Allocator,
    schema_fingerprint: u64,
    /// Column count from the schema fingerprint. v4 entries carry
    /// exactly `column_count` `Stats` slots in their `column_stats`
    /// field. Set at `empty()` time; immutable thereafter.
    column_count: u32,
    /// Next AUTO_INCREMENT value the table will assign. Bumped on
    /// insert (after both omitted-fill and explicit-supply paths).
    /// Zero when the table has no AUTO_INCREMENT column. Persists
    /// across reopens via the v5 manifest header.
    auto_inc_next: u64 = 0,
    segments: std.ArrayList(ManifestEntry),

    pub fn empty(allocator: Allocator, schema_fingerprint: u64, column_count: u32) Manifest {
        return .{
            .allocator = allocator,
            .schema_fingerprint = schema_fingerprint,
            .column_count = column_count,
            .segments = .empty,
        };
    }

    pub fn deinit(self: *Manifest) void {
        for (self.segments.items) |e| {
            if (e.column_stats.len > 0) self.allocator.free(e.column_stats);
            if (e.column_sketches.len > 0) self.allocator.free(e.column_sketches);
            if (e.key_bloom.len > 0) self.allocator.free(e.key_bloom);
        }
        self.segments.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn appendSegment(self: *Manifest, entry: ManifestEntry) !void {
        try self.segments.append(self.allocator, entry);
    }

    pub fn nextSegmentId(self: Manifest) u64 {
        var max_id: u64 = 0;
        for (self.segments.items) |e| {
            if (e.segment_id > max_id) max_id = e.segment_id;
        }
        return max_id + 1;
    }
};

pub fn writeManifest(io: Io, dir: Io.Dir, m: Manifest, sync: bool) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(m.allocator);

    try buf.appendSlice(m.allocator, &manifest_magic);
    try appendU16(m.allocator, &buf, manifest_version);
    try appendU16(m.allocator, &buf, 0); // flags
    try appendU64(m.allocator, &buf, m.schema_fingerprint);
    try appendU32(m.allocator, &buf, @intCast(m.segments.items.len));
    try appendU32(m.allocator, &buf, m.column_count);
    try appendU64(m.allocator, &buf, m.auto_inc_next);

    for (m.segments.items) |e| {
        try appendU64(m.allocator, &buf, e.segment_id);
        try appendU64(m.allocator, &buf, e.row_count);
        try appendU64(m.allocator, &buf, e.byte_size);
        try appendU32(m.allocator, &buf, e.row_group_count);

        var flags: u32 = 0;
        var lk_min: i128 = 0;
        var lk_max: i128 = 0;
        if (e.leading_key_stats) |s| {
            flags |= flag_leading_key_stats;
            lk_min = s.min;
            lk_max = s.max;
        }
        try appendU32(m.allocator, &buf, flags);
        try appendI128(m.allocator, &buf, lk_min);
        try appendI128(m.allocator, &buf, lk_max);

        // Per-column stats: write exactly `column_count` slots.
        // If the entry's column_stats is empty (legacy or not yet
        // populated), emit `{0, 0}` for each — pruning treats these
        // as "no stats" and stays conservative.
        var ci: u32 = 0;
        while (ci < m.column_count) : (ci += 1) {
            const cs: format.Stats = if (ci < e.column_stats.len)
                e.column_stats[ci]
            else
                .{ .min = 0, .max = 0 };
            try appendI128(m.allocator, &buf, cs.min);
            try appendI128(m.allocator, &buf, cs.max);
            try appendI128(m.allocator, &buf, cs.sum);
            try appendU64(m.allocator, &buf, cs.null_count);
        }

        // Per-column HyperLogLog sketches: `column_count * hll.m` bytes.
        // A missing/short entry is zero-filled — an all-zero sketch
        // estimates 0, which the merge treats as "no info" (conservative).
        const want = @as(usize, m.column_count) * hll.m;
        if (e.column_sketches.len >= want) {
            try buf.appendSlice(m.allocator, e.column_sketches[0..want]);
        } else {
            try buf.appendSlice(m.allocator, e.column_sketches);
            try buf.appendNTimes(m.allocator, 0, want - e.column_sketches.len);
        }

        // Compound-key Bloom filter: length-prefixed (variable size). 0 = none.
        try appendU32(m.allocator, &buf, @intCast(e.key_bloom.len));
        try buf.appendSlice(m.allocator, e.key_bloom);
    }

    try buf.appendSlice(m.allocator, &manifest_magic);

    // write-tmp + (optional fsync) + atomic rename. Parent-directory fsync
    // is skipped — NTFS rename is durable via the journal; ext4 has a small
    // known hole closed by future WAL work.
    try @import("storage.zig").writeFileSynced(io, dir, manifest_tmp_filename, buf.items, sync);
    try Io.Dir.rename(dir, manifest_tmp_filename, dir, manifest_filename, io);
}

pub fn readManifest(
    allocator: Allocator,
    io: Io,
    dir: Io.Dir,
    schema_fingerprint: u64,
) !Manifest {
    const bytes = dir.readFileAlloc(io, manifest_filename, allocator, .unlimited) catch |err| switch (err) {
        error.FileNotFound => return Manifest.empty(allocator, schema_fingerprint, 0),
        else => return err,
    };
    defer allocator.free(bytes);

    if (bytes.len < header_size + trailer_size) return Error.ManifestTooSmall;
    if (!std.mem.eql(u8, bytes[0..4], &manifest_magic)) return Error.ManifestBadMagic;

    const version = format.readU16(bytes[4..6]);
    if (version != manifest_version) return Error.ManifestUnsupportedVersion;

    const fp = format.readU64(bytes[8..16]);
    if (fp != schema_fingerprint) return Error.SchemaFingerprintMismatch;

    const count = format.readU32(bytes[16..20]);
    const column_count = format.readU32(bytes[20..24]);
    const auto_inc_next = format.readU64(bytes[24..32]);

    // Per-entry minimum: prefix + fixed stats/sketch slots + the 4-byte
    // key-Bloom length prefix. The Bloom itself is variable-length, so the total
    // is a lower bound, not an exact size — the trailing magic validates the end.
    const entry_min: usize = entry_prefix_size + @as(usize, column_count) * (stats_slot_size + sketch_slot_size) + 4;
    const min_size: usize = header_size + @as(usize, count) * entry_min + trailer_size;
    if (bytes.len < min_size) return Error.ManifestCorrupt;

    if (!std.mem.eql(u8, bytes[bytes.len - 4 .. bytes.len], &manifest_magic)) {
        return Error.ManifestBadTrailerMagic;
    }

    var segments: std.ArrayList(ManifestEntry) = .empty;
    errdefer {
        for (segments.items) |e| {
            if (e.column_stats.len > 0) allocator.free(e.column_stats);
            if (e.column_sketches.len > 0) allocator.free(e.column_sketches);
            if (e.key_bloom.len > 0) allocator.free(e.key_bloom);
        }
        segments.deinit(allocator);
    }
    try segments.ensureTotalCapacityPrecise(allocator, count);

    var off: usize = header_size;
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const segment_id = format.readU64(bytes[off .. off + 8]);
        off += 8;
        const row_count = format.readU64(bytes[off .. off + 8]);
        off += 8;
        const byte_size = format.readU64(bytes[off .. off + 8]);
        off += 8;
        const row_group_count = format.readU32(bytes[off .. off + 4]);
        off += 4;
        const flags = format.readU32(bytes[off .. off + 4]);
        off += 4;
        const lk_min = format.readI128(bytes[off .. off + 16]);
        off += 16;
        const lk_max = format.readI128(bytes[off .. off + 16]);
        off += 16;

        var entry: ManifestEntry = .{
            .segment_id = segment_id,
            .row_count = row_count,
            .byte_size = byte_size,
            .row_group_count = row_group_count,
        };
        if ((flags & flag_leading_key_stats) != 0) {
            entry.leading_key_stats = .{ .min = lk_min, .max = lk_max };
        }

        if (column_count > 0) {
            const stats = try allocator.alloc(format.Stats, column_count);
            errdefer allocator.free(stats);
            for (stats) |*s| {
                s.min = format.readI128(bytes[off .. off + 16]);
                off += 16;
                s.max = format.readI128(bytes[off .. off + 16]);
                off += 16;
                s.sum = format.readI128(bytes[off .. off + 16]);
                off += 16;
                s.null_count = format.readU64(bytes[off .. off + 8]);
                off += 8;
            }
            entry.column_stats = stats;

            const sketch_bytes = @as(usize, column_count) * hll.m;
            const sketches = try allocator.alloc(u8, sketch_bytes);
            errdefer allocator.free(sketches);
            @memcpy(sketches, bytes[off .. off + sketch_bytes]);
            off += sketch_bytes;
            entry.column_sketches = sketches;
        }

        // Compound-key Bloom filter (length-prefixed; written unconditionally).
        const bloom_len = format.readU32(bytes[off .. off + 4]);
        off += 4;
        if (bloom_len > 0) {
            const bloom = try allocator.alloc(u8, bloom_len);
            errdefer allocator.free(bloom);
            @memcpy(bloom, bytes[off .. off + bloom_len]);
            off += bloom_len;
            entry.key_bloom = bloom;
        }

        segments.appendAssumeCapacity(entry);
    }

    return Manifest{
        .allocator = allocator,
        .schema_fingerprint = schema_fingerprint,
        .column_count = column_count,
        .auto_inc_next = auto_inc_next,
        .segments = segments,
    };
}

/// Build a `ManifestEntry` for a freshly-written segment, aggregating
/// row-group stats into segment-level stats:
///   - `leading_key_stats`: min/max of the leading order-key column
///     (or null when not applicable).
///   - `column_stats`: per-column min/max across the whole segment.
///     Allocated in `allocator`; Manifest takes ownership and frees
///     in `Manifest.deinit`.
///
/// `columns` is the segment's schema columns — the per-type fold of the
/// `Stats.sum` slot (integer add / f64 add / string min, see
/// `format.foldStats`) needs each column's type. Columns whose type carries
/// no stats (per `format.typeHasStats`) get an all-zero slot.
pub fn entryFromSegmentInfo(
    allocator: Allocator,
    info: format.SegmentInfo,
    leading_key_idx: ?usize,
    columns: []const types.Column,
) !ManifestEntry {
    var entry: ManifestEntry = .{
        .segment_id = info.segment_id,
        .row_count = info.row_count,
        .byte_size = info.byte_size,
        .row_group_count = @intCast(info.row_groups.len),
    };
    errdefer {
        if (entry.column_stats.len > 0) allocator.free(entry.column_stats);
        if (entry.column_sketches.len > 0) allocator.free(entry.column_sketches);
    }

    if (columns.len > 0 and info.row_groups.len > 0) {
        const stats = try allocator.alloc(format.Stats, columns.len);
        for (columns, 0..) |col, ci| {
            if (!format.typeHasStats(col.type)) {
                stats[ci] = .{ .min = 0, .max = 0 };
                continue;
            }
            const kind = format.sumKindOf(col.type);
            var acc: format.Stats = .{
                .min = std.math.maxInt(i128),
                .max = std.math.minInt(i128),
                .sum = format.sumIdentity(kind),
                .null_count = 0,
            };
            for (info.row_groups) |rg| {
                format.foldStats(&acc, rg.stats[ci], kind);
            }
            stats[ci] = acc;
        }
        entry.column_stats = stats;
    }

    // Carry the writer's per-column HLL sketches into the manifest entry.
    if (info.column_sketches.len > 0) {
        const sk = try allocator.alloc(u8, info.column_sketches.len);
        @memcpy(sk, info.column_sketches);
        entry.column_sketches = sk;
    }

    // Carry the compound-key Bloom (built by the API layer) into the entry, so
    // the in-memory manifest can prune the upsert probe with zero I/O.
    if (info.key_bloom.len > 0) {
        const bf = try allocator.alloc(u8, info.key_bloom.len);
        @memcpy(bf, info.key_bloom);
        entry.key_bloom = bf;
    }

    if (leading_key_idx) |idx| {
        if (idx < columns.len and format.typeHasStats(columns[idx].type)) {
            entry.leading_key_stats = entry.column_stats[idx];
        }
    }

    return entry;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

const appendU16 = format.appendU16;
const appendU32 = format.appendU32;
const appendU64 = format.appendU64;
const appendI128 = format.appendI128;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "manifest round-trips with entries" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var written = Manifest.empty(allocator, 0xABCDEF, 0);
    defer written.deinit();

    try written.appendSegment(.{
        .segment_id = 1,
        .row_count = 100,
        .byte_size = 4096,
        .row_group_count = 2,
        .leading_key_stats = .{ .min = 1, .max = 99 },
    });
    try written.appendSegment(.{
        .segment_id = 2,
        .row_count = 250,
        .byte_size = 8192,
        .row_group_count = 4,
        .leading_key_stats = .{ .min = 100, .max = 349 },
    });
    try written.appendSegment(.{
        .segment_id = 5,
        .row_count = 73,
        .byte_size = 2048,
        .row_group_count = 1,
        // No stats for this one — simulates a string-keyed segment.
        .leading_key_stats = null,
    });

    try writeManifest(io, tmp.dir, written, false);

    var read = try readManifest(allocator, io, tmp.dir, 0xABCDEF);
    defer read.deinit();

    try std.testing.expectEqual(@as(u64, 0xABCDEF), read.schema_fingerprint);
    try std.testing.expectEqual(@as(usize, 3), read.segments.items.len);

    try std.testing.expectEqual(@as(u64, 1), read.segments.items[0].segment_id);
    try std.testing.expectEqual(@as(u64, 100), read.segments.items[0].row_count);
    try std.testing.expectEqual(@as(u64, 4096), read.segments.items[0].byte_size);
    try std.testing.expectEqual(@as(u32, 2), read.segments.items[0].row_group_count);
    try std.testing.expectEqual(@as(i128, 1), read.segments.items[0].leading_key_stats.?.min);
    try std.testing.expectEqual(@as(i128, 99), read.segments.items[0].leading_key_stats.?.max);

    try std.testing.expectEqual(@as(u64, 5), read.segments.items[2].segment_id);
    try std.testing.expectEqual(@as(u64, 73), read.segments.items[2].row_count);
    try std.testing.expect(read.segments.items[2].leading_key_stats == null);

    try std.testing.expectEqual(@as(u64, 6), read.nextSegmentId());
}

test "manifest read of missing file returns empty" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var m = try readManifest(allocator, io, tmp.dir, 42);
    defer m.deinit();

    try std.testing.expectEqual(@as(usize, 0), m.segments.items.len);
    try std.testing.expectEqual(@as(u64, 42), m.schema_fingerprint);
    try std.testing.expectEqual(@as(u64, 1), m.nextSegmentId());
}

test "manifest fingerprint mismatch errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var written = Manifest.empty(allocator, 0xAAAA, 0);
    defer written.deinit();
    try writeManifest(io, tmp.dir, written, false);

    try std.testing.expectError(Error.SchemaFingerprintMismatch, readManifest(allocator, io, tmp.dir, 0xBBBB));
}

test "manifest atomic rename replaces existing manifest" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // First write
    var v1 = Manifest.empty(allocator, 1, 0);
    defer v1.deinit();
    try v1.appendSegment(.{ .segment_id = 1, .row_count = 10 });
    try writeManifest(io, tmp.dir, v1, false);

    // Second write replaces it
    var v2 = Manifest.empty(allocator, 1, 0);
    defer v2.deinit();
    try v2.appendSegment(.{ .segment_id = 1, .row_count = 10 });
    try v2.appendSegment(.{ .segment_id = 2, .row_count = 20 });
    try writeManifest(io, tmp.dir, v2, false);

    var read = try readManifest(allocator, io, tmp.dir, 1);
    defer read.deinit();
    try std.testing.expectEqual(@as(usize, 2), read.segments.items.len);
}

