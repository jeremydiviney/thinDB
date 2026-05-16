//! Per-segment tombstone sidecar (<seg_id>.tomb).
//!
//! Format (v1, binary, little-endian):
//!
//!   magic "tDBT"  (4)
//!   version u16   (2)
//!   flags u16     (2)
//!   count u32     (4)
//!   [count × u32] row_offsets
//!   magic "tDBT"  (4)
//!
//! v0.2 keeps things dead simple: every write is a full file rewrite
//! (read-modify-write). Counts in real workloads are small relative to
//! segment row counts, and this avoids edge cases with partial-append
//! semantics across filesystems.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const format = @import("format.zig");

pub const tombstone_magic: [4]u8 = .{ 't', 'D', 'B', 'T' };
pub const tombstone_version: u16 = 1;
pub const trailer_size: usize = 4;

pub const Error = error{
    TombstoneTooSmall,
    TombstoneBadMagic,
    TombstoneBadTrailerMagic,
    TombstoneUnsupportedVersion,
    TombstoneCorrupt,
};

pub fn fileNameFor(buf: []u8, seg_id: u64) ![]u8 {
    return std.fmt.bufPrint(buf, "{d:0>20}.tomb", .{seg_id});
}

/// Read the tombstone file for `seg_id`. Returns `null` if it doesn't exist.
/// On success the caller owns the returned slice and must `allocator.free` it.
pub fn read(
    allocator: Allocator,
    io: Io,
    segments_dir: Io.Dir,
    seg_id: u64,
) !?[]u32 {
    var name_buf: [32]u8 = undefined;
    const file_name = try fileNameFor(&name_buf, seg_id);

    const bytes = segments_dir.readFileAlloc(io, file_name, allocator, .unlimited) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer allocator.free(bytes);

    if (bytes.len < 12 + trailer_size) return Error.TombstoneTooSmall;
    if (!std.mem.eql(u8, bytes[0..4], &tombstone_magic)) return Error.TombstoneBadMagic;

    const version = format.readU16(bytes[4..6]);
    if (version != tombstone_version) return Error.TombstoneUnsupportedVersion;

    const count = format.readU32(bytes[8..12]);
    const expected = 12 + @as(usize, count) * 4 + trailer_size;
    if (bytes.len != expected) return Error.TombstoneCorrupt;
    if (!std.mem.eql(u8, bytes[bytes.len - 4 .. bytes.len], &tombstone_magic)) {
        return Error.TombstoneBadTrailerMagic;
    }

    const out = try allocator.alloc(u32, count);
    var i: usize = 0;
    while (i < count) : (i += 1) {
        out[i] = format.readU32(bytes[12 + i * 4 .. 12 + i * 4 + 4]);
    }
    return out;
}

/// Atomically replace the tombstone file with the given sorted/deduped offsets.
pub fn write(
    io: Io,
    segments_dir: Io.Dir,
    seg_id: u64,
    offsets: []const u32,
    scratch: Allocator,
) !void {
    var name_buf: [32]u8 = undefined;
    const file_name = try fileNameFor(&name_buf, seg_id);

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(scratch);

    try buf.appendSlice(scratch, &tombstone_magic);
    try appendU16(scratch, &buf, tombstone_version);
    try appendU16(scratch, &buf, 0);
    try appendU32(scratch, &buf, @intCast(offsets.len));
    for (offsets) |off| try appendU32(scratch, &buf, off);
    try buf.appendSlice(scratch, &tombstone_magic);

    try segments_dir.writeFile(io, .{ .sub_path = file_name, .data = buf.items });
}

/// Read existing tombstones (if any), merge in `new_offsets`, dedupe, sort,
/// and write the result back. If no existing file, writes one with just the
/// new offsets.
pub fn merge(
    allocator: Allocator,
    io: Io,
    segments_dir: Io.Dir,
    seg_id: u64,
    new_offsets: []const u32,
) !void {
    var combined: std.ArrayList(u32) = .empty;
    defer combined.deinit(allocator);

    if (try read(allocator, io, segments_dir, seg_id)) |existing| {
        defer allocator.free(existing);
        try combined.appendSlice(allocator, existing);
    }
    try combined.appendSlice(allocator, new_offsets);

    std.sort.pdq(u32, combined.items, {}, std.sort.asc(u32));

    // dedupe in place
    var write_idx: usize = 0;
    for (combined.items, 0..) |v, i| {
        if (i == 0 or combined.items[i - 1] != v) {
            combined.items[write_idx] = v;
            write_idx += 1;
        }
    }
    combined.items.len = write_idx;

    try write(io, segments_dir, seg_id, combined.items, allocator);
}

const appendU16 = format.appendU16;
const appendU32 = format.appendU32;

test "round-trip tombstone offsets" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const offsets = [_]u32{ 3, 17, 42, 1024 };
    try write(io, tmp.dir, 7, &offsets, allocator);

    const got = (try read(allocator, io, tmp.dir, 7)).?;
    defer allocator.free(got);

    try std.testing.expectEqualSlices(u32, &offsets, got);
}

test "read of missing tomb returns null" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try std.testing.expect((try read(allocator, io, tmp.dir, 99)) == null);
}

test "merge sorts and dedupes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try merge(allocator, io, tmp.dir, 1, &[_]u32{ 5, 10, 3 });
    try merge(allocator, io, tmp.dir, 1, &[_]u32{ 10, 7, 5 });

    const got = (try read(allocator, io, tmp.dir, 1)).?;
    defer allocator.free(got);

    try std.testing.expectEqualSlices(u32, &[_]u32{ 3, 5, 7, 10 }, got);
}
