//! Per-table manifest. Lists active segments. Updated atomically via
//! write-tmp-then-rename so readers either see the old or new state, never a
//! partial state.
//!
//! Format (binary, little-endian):
//!
//!   [Header — 24 bytes]
//!     magic "tDBM"            (4)
//!     version u16             (2)
//!     flags u16               (2 — reserved, written 0)
//!     schema_fingerprint u64  (8)
//!     segment_count u32       (4)
//!     column_count u32        (4)
//!
//!   [Entries — 64 + 32*column_count bytes each]
//!     For each segment:
//!       segment_id u64        (8)
//!       row_count u64         (8)
//!       byte_size u64         (8) — .dat file size
//!       row_group_count u32   (4)
//!       flags u32             (4) — bit 0 = leading_key_stats valid
//!       leading_key_min i128  (16)
//!       leading_key_max i128  (16)
//!       per_column_stats      (32 * column_count — i128 min + i128 max
//!                              each, encoded per column type; {0,0}
//!                              for columns whose type has no stats)
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

pub const manifest_magic: [4]u8 = .{ 't', 'D', 'B', 'M' };
pub const manifest_version: u16 = 4;
pub const manifest_filename = "manifest";
pub const manifest_tmp_filename = "manifest.tmp";
pub const header_size: usize = 24;
/// Per-entry fixed prefix (excluding the per-column stats tail).
pub const entry_prefix_size: usize = 64;
pub const stats_slot_size: usize = 32; // i128 min + i128 max
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
};

pub const Manifest = struct {
    allocator: Allocator,
    schema_fingerprint: u64,
    /// Column count from the schema fingerprint. v4 entries carry
    /// exactly `column_count` `Stats` slots in their `column_stats`
    /// field. Set at `empty()` time; immutable thereafter.
    column_count: u32,
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
        }
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

    const entry_size: usize = entry_prefix_size + @as(usize, column_count) * stats_slot_size;
    const expected_size: usize = header_size + @as(usize, count) * entry_size + trailer_size;
    if (bytes.len != expected_size) return Error.ManifestCorrupt;

    if (!std.mem.eql(u8, bytes[bytes.len - 4 .. bytes.len], &manifest_magic)) {
        return Error.ManifestBadTrailerMagic;
    }

    var segments: std.ArrayList(ManifestEntry) = .empty;
    errdefer {
        for (segments.items) |e| {
            if (e.column_stats.len > 0) allocator.free(e.column_stats);
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
            }
            entry.column_stats = stats;
        }

        segments.appendAssumeCapacity(entry);
    }

    return Manifest{
        .allocator = allocator,
        .schema_fingerprint = schema_fingerprint,
        .column_count = column_count,
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
/// `column_has_stats[i]` indicates whether column `i`'s `Stats` slot
/// in the row-group footer carries meaningful values (per
/// `format.typeHasStats`). Columns flagged `false` get `{0, 0}` in
/// the manifest entry.
pub fn entryFromSegmentInfo(
    allocator: Allocator,
    info: format.SegmentInfo,
    leading_key_idx: ?usize,
    column_has_stats: []const bool,
) !ManifestEntry {
    var entry: ManifestEntry = .{
        .segment_id = info.segment_id,
        .row_count = info.row_count,
        .byte_size = info.byte_size,
        .row_group_count = @intCast(info.row_groups.len),
    };

    if (column_has_stats.len > 0 and info.row_groups.len > 0) {
        const stats = try allocator.alloc(format.Stats, column_has_stats.len);
        errdefer allocator.free(stats);
        for (column_has_stats, 0..) |has, ci| {
            if (!has) {
                stats[ci] = .{ .min = 0, .max = 0 };
                continue;
            }
            var lo: i128 = std.math.maxInt(i128);
            var hi: i128 = std.math.minInt(i128);
            for (info.row_groups) |rg| {
                const s = rg.stats[ci];
                if (s.min < lo) lo = s.min;
                if (s.max > hi) hi = s.max;
            }
            stats[ci] = .{ .min = lo, .max = hi };
        }
        entry.column_stats = stats;
    }

    if (leading_key_idx) |idx| {
        if (idx < column_has_stats.len and column_has_stats[idx]) {
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

