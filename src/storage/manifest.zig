//! Per-table manifest. Lists active segments. Updated atomically via
//! write-tmp-then-rename so readers either see the old or new state, never a
//! partial state.
//!
//! Format (v0.1, binary, little-endian):
//!
//!   [Header — 20 bytes]
//!     magic "tDBM"            (4)
//!     version u16             (2)
//!     flags u16               (2 — reserved, written 0)
//!     schema_fingerprint u64  (8)
//!     segment_count u32       (4)
//!
//!   [Entries]
//!     For each segment:
//!       segment_id u64        (8)
//!       row_count u64         (8)
//!
//!   [Trailer]
//!     magic "tDBM"            (4)

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const format = @import("format.zig");

pub const manifest_magic: [4]u8 = .{ 't', 'D', 'B', 'M' };
pub const manifest_version: u16 = 1;
pub const manifest_filename = "manifest";
pub const manifest_tmp_filename = "manifest.tmp";
pub const header_size: usize = 20;
pub const entry_size: usize = 16;
pub const trailer_size: usize = 4;

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
};

pub const Manifest = struct {
    allocator: Allocator,
    schema_fingerprint: u64,
    segments: std.ArrayList(ManifestEntry),

    pub fn empty(allocator: Allocator, schema_fingerprint: u64) Manifest {
        return .{
            .allocator = allocator,
            .schema_fingerprint = schema_fingerprint,
            .segments = .empty,
        };
    }

    pub fn deinit(self: *Manifest) void {
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

pub fn writeManifest(io: Io, dir: Io.Dir, m: Manifest) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(m.allocator);

    try buf.appendSlice(m.allocator, &manifest_magic);
    try appendU16(m.allocator, &buf, manifest_version);
    try appendU16(m.allocator, &buf, 0); // flags
    try appendU64(m.allocator, &buf, m.schema_fingerprint);
    try appendU32(m.allocator, &buf, @intCast(m.segments.items.len));

    for (m.segments.items) |e| {
        try appendU64(m.allocator, &buf, e.segment_id);
        try appendU64(m.allocator, &buf, e.row_count);
    }

    try buf.appendSlice(m.allocator, &manifest_magic);

    try dir.writeFile(io, .{ .sub_path = manifest_tmp_filename, .data = buf.items });
    try Io.Dir.rename(dir, manifest_tmp_filename, dir, manifest_filename, io);
}

pub fn readManifest(
    allocator: Allocator,
    io: Io,
    dir: Io.Dir,
    schema_fingerprint: u64,
) !Manifest {
    const bytes = dir.readFileAlloc(io, manifest_filename, allocator, .unlimited) catch |err| switch (err) {
        error.FileNotFound => return Manifest.empty(allocator, schema_fingerprint),
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

    const expected_size: usize = header_size + @as(usize, count) * entry_size + trailer_size;
    if (bytes.len != expected_size) return Error.ManifestCorrupt;

    if (!std.mem.eql(u8, bytes[bytes.len - 4 .. bytes.len], &manifest_magic)) {
        return Error.ManifestBadTrailerMagic;
    }

    var segments: std.ArrayList(ManifestEntry) = .empty;
    errdefer segments.deinit(allocator);
    try segments.ensureTotalCapacityPrecise(allocator, count);

    var off: usize = header_size;
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const segment_id = format.readU64(bytes[off .. off + 8]);
        off += 8;
        const row_count = format.readU64(bytes[off .. off + 8]);
        off += 8;
        segments.appendAssumeCapacity(.{ .segment_id = segment_id, .row_count = row_count });
    }

    return Manifest{
        .allocator = allocator,
        .schema_fingerprint = schema_fingerprint,
        .segments = segments,
    };
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

const appendU16 = format.appendU16;
const appendU32 = format.appendU32;
const appendU64 = format.appendU64;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "manifest round-trips with entries" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var written = Manifest.empty(allocator, 0xABCDEF);
    defer written.deinit();

    try written.appendSegment(.{ .segment_id = 1, .row_count = 100 });
    try written.appendSegment(.{ .segment_id = 2, .row_count = 250 });
    try written.appendSegment(.{ .segment_id = 5, .row_count = 73 });

    try writeManifest(io, tmp.dir, written);

    var read = try readManifest(allocator, io, tmp.dir, 0xABCDEF);
    defer read.deinit();

    try std.testing.expectEqual(@as(u64, 0xABCDEF), read.schema_fingerprint);
    try std.testing.expectEqual(@as(usize, 3), read.segments.items.len);
    try std.testing.expectEqual(@as(u64, 1), read.segments.items[0].segment_id);
    try std.testing.expectEqual(@as(u64, 100), read.segments.items[0].row_count);
    try std.testing.expectEqual(@as(u64, 5), read.segments.items[2].segment_id);
    try std.testing.expectEqual(@as(u64, 73), read.segments.items[2].row_count);

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

    var written = Manifest.empty(allocator, 0xAAAA);
    defer written.deinit();
    try writeManifest(io, tmp.dir, written);

    try std.testing.expectError(Error.SchemaFingerprintMismatch, readManifest(allocator, io, tmp.dir, 0xBBBB));
}

test "manifest atomic rename replaces existing manifest" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // First write
    var v1 = Manifest.empty(allocator, 1);
    defer v1.deinit();
    try v1.appendSegment(.{ .segment_id = 1, .row_count = 10 });
    try writeManifest(io, tmp.dir, v1);

    // Second write replaces it
    var v2 = Manifest.empty(allocator, 1);
    defer v2.deinit();
    try v2.appendSegment(.{ .segment_id = 1, .row_count = 10 });
    try v2.appendSegment(.{ .segment_id = 2, .row_count = 20 });
    try writeManifest(io, tmp.dir, v2);

    var read = try readManifest(allocator, io, tmp.dir, 1);
    defer read.deinit();
    try std.testing.expectEqual(@as(usize, 2), read.segments.items.len);
}
