//! Parquet reader for bulk loads: the file footer, and each column chunk
//! decoded into a table column's `ColumnStore`.
//!
//! Scope: flat schemas (REQUIRED or OPTIONAL leaves, no repetition); data
//! pages v1 and v2; PLAIN and dictionary (PLAIN_DICTIONARY / RLE_DICTIONARY)
//! values, plus RLE booleans; UNCOMPRESSED, SNAPPY, ZSTD, GZIP and LZ4_RAW
//! pages. Anything else is `error.UnsupportedParquet`, never a guess.
//!
//! Integers fill integer, date and datetime columns. An integer annotated as
//! DATE is days and one annotated as TIMESTAMP carries its unit; a bare
//! integer into a DATE column is days since 1970-01-01 and into a DATETIME
//! column seconds since 1970-01-01 (the form ClickBench's `hits.parquet`
//! stores). A value outside its column's range is an error, not a wrap.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const store = @import("store.zig");
const compression = @import("../storage/compression.zig");
const snappy = @import("../storage/snappy.zig");

const ColumnStore = store.ColumnStore;
const StringStore = store.StringStore;

pub const Error = error{
    NotParquet,
    CorruptParquet,
    UnsupportedParquet,
    /// A value doesn't fit its column's type, or a NULL meets a NOT NULL
    /// column.
    ParquetValueOutOfRange,
};

const magic = "PAR1";

pub const PhysicalType = enum(i32) {
    boolean = 0,
    int32 = 1,
    int64 = 2,
    int96 = 3,
    float = 4,
    double = 5,
    byte_array = 6,
    fixed_len_byte_array = 7,
    _,
};

pub const Codec = enum(i32) {
    uncompressed = 0,
    snappy = 1,
    gzip = 2,
    lzo = 3,
    brotli = 4,
    lz4 = 5,
    zstd = 6,
    lz4_raw = 7,
    _,
};

const Encoding = enum(i32) {
    plain = 0,
    plain_dictionary = 2,
    rle = 3,
    bit_packed = 4,
    delta_binary_packed = 5,
    delta_length_byte_array = 6,
    delta_byte_array = 7,
    rle_dictionary = 8,
    byte_stream_split = 9,
    _,
};

const PageType = enum(i32) {
    data_page = 0,
    index_page = 1,
    dictionary_page = 2,
    data_page_v2 = 3,
    _,
};

/// What a leaf's annotation says its values mean, as far as loading goes.
pub const Annotation = enum {
    none,
    /// Days since 1970-01-01.
    days,
    /// A TIMESTAMP in this unit since 1970-01-01.
    millis,
    micros,
    nanos,
    /// DECIMAL, TIME, INTERVAL: values this reader doesn't convert.
    unsupported,
};

/// A leaf column of the file's schema.
pub const Column = struct {
    name: []const u8,
    physical: PhysicalType,
    optional: bool,
    annotation: Annotation,
    /// The integers are unsigned (UINT_* or INTEGER(isSigned = false)).
    unsigned: bool,
};

pub const ColumnChunk = struct {
    codec: Codec,
    /// File offset of the chunk's first page (its dictionary page if any).
    start: u64,
    len: u64,
    values: u64,
};

pub const RowGroup = struct {
    rows: u64,
    /// One per leaf column, in `Metadata.columns` order.
    chunks: []ColumnChunk,
    /// The file range covering every chunk.
    start: u64,
    end: u64,
};

pub const Metadata = struct {
    arena: std.heap.ArenaAllocator,
    columns: []Column,
    row_groups: []RowGroup,
    rows: u64,

    pub fn deinit(self: *Metadata) void {
        self.arena.deinit();
    }

    /// The leaf named `name`, matched case-insensitively.
    pub fn columnIndex(self: Metadata, name: []const u8) ?usize {
        for (self.columns, 0..) |c, i| {
            if (std.ascii.eqlIgnoreCase(c.name, name)) return i;
        }
        return null;
    }
};

/// Reads and parses the footer of `file`.
pub fn readMetadata(allocator: Allocator, io: Io, file: Io.File) !Metadata {
    const size = try file.length(io);
    if (size < 12) return error.NotParquet;
    var tail: [8]u8 = undefined;
    if (try file.readPositionalAll(io, &tail, size - 8) != 8) return error.NotParquet;
    if (!std.mem.eql(u8, tail[4..8], magic)) return error.NotParquet;
    const footer_len = std.mem.readInt(u32, tail[0..4], .little);
    if (footer_len > size - 12) return error.CorruptParquet;

    const footer = try allocator.alloc(u8, footer_len);
    defer allocator.free(footer);
    if (try file.readPositionalAll(io, footer, size - 8 - footer_len) != footer_len) return error.CorruptParquet;
    return parseMetadata(allocator, footer, size);
}

/// Parses a serialized FileMetaData. `file_size` bounds every chunk range.
pub fn parseMetadata(allocator: Allocator, footer: []const u8, file_size: u64) !Metadata {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();

    var r: Thrift = .{ .bytes = footer };
    var schema: []SchemaElement = &.{};
    var row_groups: []RowGroup = &.{};
    var rows: u64 = 0;
    var last: i16 = 0;
    while (try r.field(&last)) |f| switch (f.id) {
        2 => {
            schema = try a.alloc(SchemaElement, try r.listHeader(f.kind, .@"struct"));
            for (schema) |*e| e.* = try parseSchemaElement(&r);
        },
        3 => rows = try r.unsigned(f.kind, .i64),
        4 => {
            row_groups = try a.alloc(RowGroup, try r.listHeader(f.kind, .@"struct"));
            for (row_groups) |*rg| rg.* = try parseRowGroup(a, &r, file_size);
        },
        else => try r.skip(f.kind),
    };

    const columns = try leafColumns(a, schema);
    for (row_groups) |rg| {
        if (rg.chunks.len != columns.len) return error.CorruptParquet;
    }
    return .{ .arena = arena, .columns = columns, .row_groups = row_groups, .rows = rows };
}

const SchemaElement = struct {
    physical: ?PhysicalType = null,
    repetition: i32 = 0,
    name: []const u8 = "",
    children: u32 = 0,
    converted: ?i32 = null,
    logical: Annotation = .none,
    logical_unsigned: bool = false,
};

fn parseSchemaElement(r: *Thrift) !SchemaElement {
    var e: SchemaElement = .{};
    var last: i16 = 0;
    while (try r.field(&last)) |f| switch (f.id) {
        1 => e.physical = @fromBackingInt(@intCast(try r.int(f.kind, .i32))),
        3 => e.repetition = @intCast(try r.int(f.kind, .i32)),
        4 => e.name = try r.binary(f.kind),
        5 => e.children = @intCast(try r.unsigned(f.kind, .i32)),
        6 => e.converted = @intCast(try r.int(f.kind, .i32)),
        10 => try parseLogicalType(r, f.kind, &e),
        else => try r.skip(f.kind),
    };
    return e;
}

/// The LogicalType union: DECIMAL (5), DATE (6), TIME (7), TIMESTAMP (8)
/// and INTEGER (10) matter for loading.
fn parseLogicalType(r: *Thrift, kind: Kind, e: *SchemaElement) !void {
    if (kind != .@"struct") return error.CorruptParquet;
    var last: i16 = 0;
    while (try r.field(&last)) |f| switch (f.id) {
        5, 7 => {
            e.logical = .unsupported;
            try r.skip(f.kind);
        },
        6 => {
            e.logical = .days;
            try r.skip(f.kind);
        },
        8 => e.logical = try parseTimestampType(r, f.kind),
        10 => {
            if (f.kind != .@"struct") return error.CorruptParquet;
            var int_last: i16 = 0;
            while (try r.field(&int_last)) |inf| switch (inf.id) {
                2 => e.logical_unsigned = !try r.boolean(inf.kind),
                else => try r.skip(inf.kind),
            };
        },
        else => try r.skip(f.kind),
    };
}

fn parseTimestampType(r: *Thrift, kind: Kind) !Annotation {
    if (kind != .@"struct") return error.CorruptParquet;
    var unit: Annotation = .unsupported;
    var last: i16 = 0;
    while (try r.field(&last)) |f| switch (f.id) {
        2 => {
            if (f.kind != .@"struct") return error.CorruptParquet;
            var unit_last: i16 = 0;
            while (try r.field(&unit_last)) |uf| {
                unit = switch (uf.id) {
                    1 => .millis,
                    2 => .micros,
                    3 => .nanos,
                    else => .unsupported,
                };
                try r.skip(uf.kind);
            }
        },
        else => try r.skip(f.kind),
    };
    return unit;
}

/// The schema's leaves: a root with only primitive children. Nested
/// schemas (groups below the root, REPEATED fields) are unsupported.
fn leafColumns(a: Allocator, schema: []const SchemaElement) ![]Column {
    if (schema.len == 0) return error.CorruptParquet;
    if (schema[0].children != schema.len - 1) return error.UnsupportedParquet;
    const out = try a.alloc(Column, schema.len - 1);
    for (schema[1..], out) |e, *c| {
        if (e.children != 0 or e.physical == null) return error.UnsupportedParquet;
        // REQUIRED = 0, OPTIONAL = 1, REPEATED = 2.
        if (e.repetition == 2) return error.UnsupportedParquet;
        const converted = convertedAnnotation(e.converted);
        c.* = .{
            .name = try a.dupe(u8, e.name),
            .physical = e.physical.?,
            .optional = e.repetition == 1,
            .annotation = if (e.logical != .none) e.logical else converted.annotation,
            .unsigned = e.logical_unsigned or converted.unsigned,
        };
    }
    return out;
}

/// A legacy ConvertedType: DECIMAL (5), DATE (6), TIME_* (7, 8),
/// TIMESTAMP_* (9, 10), UINT_* (11-14), INTERVAL (21).
fn convertedAnnotation(converted: ?i32) struct { annotation: Annotation = .none, unsigned: bool = false } {
    return switch (converted orelse return .{}) {
        5, 7, 8, 21 => .{ .annotation = .unsupported },
        6 => .{ .annotation = .days },
        9 => .{ .annotation = .millis },
        10 => .{ .annotation = .micros },
        11...14 => .{ .unsigned = true },
        else => .{},
    };
}

fn parseRowGroup(a: Allocator, r: *Thrift, file_size: u64) !RowGroup {
    var rg: RowGroup = .{ .rows = 0, .chunks = &.{}, .start = 0, .end = 0 };
    var last: i16 = 0;
    while (try r.field(&last)) |f| switch (f.id) {
        1 => {
            rg.chunks = try a.alloc(ColumnChunk, try r.listHeader(f.kind, .@"struct"));
            rg.start = std.math.maxInt(u64);
            for (rg.chunks) |*c| {
                c.* = try parseColumnChunk(r);
                if (c.len > file_size or c.start > file_size - c.len) return error.CorruptParquet;
                rg.start = @min(rg.start, c.start);
                rg.end = @max(rg.end, c.start + c.len);
            }
            if (rg.chunks.len == 0) rg.start = 0;
        },
        3 => rg.rows = try r.unsigned(f.kind, .i64),
        else => try r.skip(f.kind),
    };
    return rg;
}

fn parseColumnChunk(r: *Thrift) !ColumnChunk {
    var chunk: ?ColumnChunk = null;
    var last: i16 = 0;
    while (try r.field(&last)) |f| switch (f.id) {
        // file_path: the chunk lives in another file.
        1 => return error.UnsupportedParquet,
        3 => chunk = try parseColumnMetaData(r, f.kind),
        else => try r.skip(f.kind),
    };
    return chunk orelse error.CorruptParquet;
}

fn parseColumnMetaData(r: *Thrift, kind: Kind) !ColumnChunk {
    if (kind != .@"struct") return error.CorruptParquet;
    var codec: Codec = .uncompressed;
    var values: u64 = 0;
    var len: u64 = 0;
    var data_page: ?u64 = null;
    var dictionary_page: ?u64 = null;
    var last: i16 = 0;
    while (try r.field(&last)) |f| switch (f.id) {
        4 => codec = @fromBackingInt(@intCast(try r.int(f.kind, .i32))),
        5 => values = try r.unsigned(f.kind, .i64),
        7 => len = try r.unsigned(f.kind, .i64),
        9 => data_page = try r.unsigned(f.kind, .i64),
        11 => dictionary_page = try r.unsigned(f.kind, .i64),
        else => try r.skip(f.kind),
    };
    const first = data_page orelse return error.CorruptParquet;
    // Some writers record a dictionary offset of 0 for "none".
    const start = if (dictionary_page) |d| (if (d != 0 and d < first) d else first) else first;
    return .{ .codec = codec, .start = start, .len = len, .values = values };
}

/// Per-thread buffers `decodeChunk` reuses across chunks.
pub const Scratch = struct {
    page: std.ArrayList(u8) = .empty,
    dictionary: std.ArrayList(u8) = .empty,
    /// Byte offsets of a BYTE_ARRAY dictionary's values (count + 1).
    dictionary_offsets: std.ArrayList(u32) = .empty,
    indices: std.ArrayList(u32) = .empty,
    defined: std.ArrayList(u8) = .empty,

    pub fn deinit(self: *Scratch, allocator: Allocator) void {
        self.page.deinit(allocator);
        self.dictionary.deinit(allocator);
        self.dictionary_offsets.deinit(allocator);
        self.indices.deinit(allocator);
        self.defined.deinit(allocator);
    }
};

/// Decodes the column chunk whose bytes are `bytes` (the file range
/// `chunk.start .. chunk.start + chunk.len`), appending its values to
/// `out`. `out` is nullable iff it has a validity bitmap: an OPTIONAL
/// leaf's NULLs become NULLs there, and into a NOT NULL column are an error.
pub fn decodeChunk(
    allocator: Allocator,
    scratch: *Scratch,
    column: Column,
    chunk: ColumnChunk,
    bytes: []const u8,
    out: *ColumnStore,
) !void {
    var dictionary: Dictionary = .none;
    var pos: usize = 0;
    var values_left = chunk.values;
    while (values_left > 0) {
        if (pos >= bytes.len) return error.CorruptParquet;
        var r: Thrift = .{ .bytes = bytes[pos..] };
        const page = try parsePageHeader(&r);
        pos += r.pos;
        if (page.compressed > bytes.len - pos) return error.CorruptParquet;
        const body = bytes[pos..][0..page.compressed];
        pos += page.compressed;

        switch (page.kind) {
            .dictionary_page => {
                const plain = try pageBytes(allocator, &scratch.page, chunk.codec, body, page.uncompressed);
                dictionary = try loadDictionary(allocator, scratch, column.physical, plain, page.values);
            },
            .data_page => {
                const plain = try pageBytes(allocator, &scratch.page, chunk.codec, body, page.uncompressed);
                var values = plain;
                var defined: ?[]const u8 = null;
                if (column.optional) {
                    if (page.def_levels_encoding != .rle) return error.UnsupportedParquet;
                    if (4 > plain.len) return error.CorruptParquet;
                    const len = std.mem.readInt(u32, plain[0..4], .little);
                    if (len > plain.len - 4) return error.CorruptParquet;
                    defined = try definitionLevels(allocator, scratch, plain[4..][0..len], page.values);
                    values = plain[4 + len ..];
                }
                try decodeValues(allocator, scratch, column, page.encoding, dictionary, values, page.values, defined, out);
                values_left -|= page.values;
            },
            .data_page_v2 => {
                if (page.rep_levels_len != 0) return error.UnsupportedParquet;
                const levels: usize = page.def_levels_len;
                if (levels > body.len or levels > page.uncompressed) return error.CorruptParquet;
                const defined = if (column.optional) try definitionLevels(allocator, scratch, body[0..levels], page.values) else null;
                const values = if (page.v2_compressed)
                    try pageBytes(allocator, &scratch.page, chunk.codec, body[levels..], page.uncompressed - levels)
                else
                    body[levels..];
                try decodeValues(allocator, scratch, column, page.encoding, dictionary, values, page.values, defined, out);
                values_left -|= page.values;
            },
            .index_page => {},
            _ => return error.UnsupportedParquet,
        }
    }
}

const PageHeader = struct {
    kind: PageType = .data_page,
    uncompressed: usize = 0,
    compressed: usize = 0,
    values: usize = 0,
    encoding: Encoding = .plain,
    def_levels_len: u32 = 0,
    def_levels_encoding: Encoding = .rle,
    rep_levels_len: u32 = 0,
    v2_compressed: bool = true,
};

/// A PageHeader and the one of its DataPageHeader (5), DictionaryPageHeader
/// (7) or DataPageHeaderV2 (8) that its type uses.
fn parsePageHeader(r: *Thrift) !PageHeader {
    var h: PageHeader = .{};
    var last: i16 = 0;
    while (try r.field(&last)) |f| switch (f.id) {
        1 => h.kind = @fromBackingInt(@intCast(try r.int(f.kind, .i32))),
        2 => h.uncompressed = @intCast(try r.unsigned(f.kind, .i32)),
        3 => h.compressed = @intCast(try r.unsigned(f.kind, .i32)),
        5, 7 => {
            if (f.kind != .@"struct") return error.CorruptParquet;
            var sub: i16 = 0;
            while (try r.field(&sub)) |sf| switch (sf.id) {
                1 => h.values = @intCast(try r.unsigned(sf.kind, .i32)),
                2 => h.encoding = @fromBackingInt(@intCast(try r.int(sf.kind, .i32))),
                3 => if (f.id == 5) {
                    h.def_levels_encoding = @fromBackingInt(@intCast(try r.int(sf.kind, .i32)));
                } else try r.skip(sf.kind),
                else => try r.skip(sf.kind),
            };
        },
        8 => {
            if (f.kind != .@"struct") return error.CorruptParquet;
            var sub: i16 = 0;
            while (try r.field(&sub)) |sf| switch (sf.id) {
                1 => h.values = @intCast(try r.unsigned(sf.kind, .i32)),
                4 => h.encoding = @fromBackingInt(@intCast(try r.int(sf.kind, .i32))),
                5 => h.def_levels_len = @intCast(try r.unsigned(sf.kind, .i32)),
                6 => h.rep_levels_len = @intCast(try r.unsigned(sf.kind, .i32)),
                7 => h.v2_compressed = try r.boolean(sf.kind),
                else => try r.skip(sf.kind),
            };
        },
        else => try r.skip(f.kind),
    };
    return h;
}

/// The page's uncompressed bytes: `body` itself when stored plain, else
/// decompressed into `buf`.
fn pageBytes(allocator: Allocator, buf: *std.ArrayList(u8), codec: Codec, body: []const u8, uncompressed: usize) ![]const u8 {
    if (codec == .uncompressed) {
        if (body.len != uncompressed) return error.CorruptParquet;
        return body;
    }
    try buf.resize(allocator, uncompressed);
    const dst = buf.items;
    switch (codec) {
        .snappy => snappy.decompress(body, dst) catch return error.CorruptParquet,
        .lz4_raw => compression.lz4DecompressInto(body, dst) catch return error.CorruptParquet,
        .zstd => compression.decompressInto(body, dst) catch return error.CorruptParquet,
        .gzip => {
            var in: Io.Reader = .fixed(body);
            var out: Io.Writer = .fixed(dst);
            var gz: std.compress.flate.Decompress = .init(&in, .gzip, &.{});
            const n = gz.reader.streamRemaining(&out) catch return error.CorruptParquet;
            if (n != dst.len) return error.CorruptParquet;
        },
        else => return error.UnsupportedParquet,
    }
    return dst;
}

const Dictionary = union(enum) {
    none,
    /// Fixed-width values, `width` bytes each.
    fixed: struct { bytes: []const u8, width: usize, count: usize },
    /// BYTE_ARRAY values back to back, value `i` at `offsets[i]..offsets[i + 1]`.
    strings: struct { bytes: []const u8, offsets: []const u32 },
};

/// Copies a dictionary page's values into `scratch`, where they outlive the
/// page buffer the chunk's data pages reuse.
fn loadDictionary(allocator: Allocator, scratch: *Scratch, physical: PhysicalType, plain: []const u8, count: usize) !Dictionary {
    const width: usize = switch (physical) {
        .byte_array => {
            try scratch.dictionary_offsets.resize(allocator, count + 1);
            try scratch.dictionary.resize(allocator, plain.len);
            const offsets = scratch.dictionary_offsets.items;
            const bytes = scratch.dictionary.items;
            var src: usize = 0;
            var dst: usize = 0;
            for (offsets[0..count]) |*o| {
                if (4 > plain.len - src) return error.CorruptParquet;
                const len = std.mem.readInt(u32, plain[src..][0..4], .little);
                src += 4;
                if (len > plain.len - src) return error.CorruptParquet;
                o.* = @intCast(dst);
                @memcpy(bytes[dst..][0..len], plain[src..][0..len]);
                src += len;
                dst += len;
            }
            offsets[count] = @intCast(dst);
            return .{ .strings = .{ .bytes = bytes[0..dst], .offsets = offsets } };
        },
        .int32, .float => 4,
        .int64, .double => 8,
        else => return error.UnsupportedParquet,
    };
    if (count > plain.len / width) return error.CorruptParquet;
    try scratch.dictionary.resize(allocator, count * width);
    @memcpy(scratch.dictionary.items, plain[0 .. count * width]);
    return .{ .fixed = .{ .bytes = scratch.dictionary.items, .width = width, .count = count } };
}

/// One byte per value, 1 where it is defined (not NULL), from definition
/// levels at bit width 1 (flat OPTIONAL columns).
/// The page's definition levels as one byte per value, or null when every
/// value is defined.
fn definitionLevels(allocator: Allocator, scratch: *Scratch, levels: []const u8, n: usize) !?[]const u8 {
    if (try allDefined(levels, n)) return null;
    try scratch.indices.resize(allocator, n);
    try decodeHybrid(levels, 1, scratch.indices.items);
    try scratch.defined.resize(allocator, n);
    for (scratch.defined.items, scratch.indices.items) |*d, l| d.* = @intFromBool(l != 0);
    return scratch.defined.items;
}

/// Whether the first `n` definition levels are RLE runs of 1, as a page
/// without NULLs writes them.
fn allDefined(levels: []const u8, n: usize) !bool {
    var pos: usize = 0;
    var covered: usize = 0;
    while (covered < n) {
        const run_header = try varint(levels, &pos);
        if (run_header & 1 != 0 or pos >= levels.len or levels[pos] != 1) return false;
        pos += 1;
        covered +|= std.math.cast(usize, run_header >> 1) orelse return error.CorruptParquet;
    }
    return true;
}

/// Decodes `out.len` values of the RLE / bit-packed hybrid encoding at
/// `bit_width`.
fn decodeHybrid(bytes: []const u8, bit_width: u6, out: []u32) !void {
    if (bit_width > 32) return error.CorruptParquet;
    const value_bytes: usize = (@as(usize, bit_width) + 7) / 8;
    var pos: usize = 0;
    var i: usize = 0;
    while (i < out.len) {
        const run_header = try varint(bytes, &pos);
        if (run_header & 1 == 0) {
            const run = std.math.cast(usize, run_header >> 1) orelse return error.CorruptParquet;
            if (value_bytes > bytes.len - pos) return error.CorruptParquet;
            var v: u32 = 0;
            for (bytes[pos..][0..value_bytes], 0..) |b, k| v |= @as(u32, b) << @intCast(8 * k);
            pos += value_bytes;
            const n = @min(run, out.len - i);
            @memset(out[i..][0..n], v);
            i += n;
        } else {
            const groups = std.math.cast(usize, run_header >> 1) orelse return error.CorruptParquet;
            if (groups > (bytes.len - pos) / @max(bit_width, 1)) return error.CorruptParquet;
            const packed_len = groups * bit_width;
            const n = @min(groups * 8, out.len - i);
            unpackBits(bytes[pos..][0..packed_len], bit_width, out[i..][0..n]);
            pos += packed_len;
            i += n;
        }
    }
}

/// Unpacks `out.len` little-endian bit-packed values of `bit_width` bits.
fn unpackBits(bytes: []const u8, bit_width: u6, out: []u32) void {
    if (bit_width == 0) return @memset(out, 0);
    const mask: u64 = (@as(u64, 1) << bit_width) - 1;
    var bit: usize = 0;
    for (out) |*o| {
        const byte = bit >> 3;
        const word: u64 = if (bytes.len - byte >= 8)
            std.mem.readInt(u64, bytes[byte..][0..8], .little)
        else blk: {
            var w: u64 = 0;
            for (bytes[byte..], 0..) |b, k| w |= @as(u64, b) << @intCast(8 * k);
            break :blk w;
        };
        o.* = @intCast((word >> @intCast(bit & 7)) & mask);
        bit += bit_width;
    }
}

fn varint(bytes: []const u8, pos: *usize) !u64 {
    var v: u64 = 0;
    var shift: u7 = 0;
    while (pos.* < bytes.len) {
        const b = bytes[pos.*];
        pos.* += 1;
        if (shift >= 64) return error.CorruptParquet;
        v |= @as(u64, b & 0x7f) << @intCast(shift);
        if (b & 0x80 == 0) return v;
        shift += 7;
    }
    return error.CorruptParquet;
}

/// A page's present values: PLAIN bytes, or dictionary indices (or RLE
/// booleans).
const Values = union(enum) {
    plain: []const u8,
    indices: []const u32,
};

fn decodeValues(
    allocator: Allocator,
    scratch: *Scratch,
    column: Column,
    encoding: Encoding,
    dictionary: Dictionary,
    bytes: []const u8,
    n: usize,
    defined: ?[]const u8,
    out: *ColumnStore,
) !void {
    const present = if (defined) |d| countDefined(d) else n;
    if (present != n and out.nulls == null) return error.ParquetValueOutOfRange;

    const values: Values = switch (encoding) {
        .plain => .{ .plain = bytes },
        .plain_dictionary, .rle_dictionary => blk: {
            if (dictionary == .none) return error.CorruptParquet;
            if (bytes.len == 0) {
                if (present != 0) return error.CorruptParquet;
                break :blk .{ .indices = &.{} };
            }
            if (bytes[0] > 32) return error.CorruptParquet;
            try scratch.indices.resize(allocator, present);
            try decodeHybrid(bytes[1..], @intCast(bytes[0]), scratch.indices.items);
            break :blk .{ .indices = scratch.indices.items };
        },
        .rle => blk: {
            if (column.physical != .boolean or 4 > bytes.len) return error.UnsupportedParquet;
            const len = std.mem.readInt(u32, bytes[0..4], .little);
            if (len > bytes.len - 4) return error.CorruptParquet;
            try scratch.indices.resize(allocator, present);
            try decodeHybrid(bytes[4..][0..len], 1, scratch.indices.items);
            break :blk .{ .indices = scratch.indices.items };
        },
        else => return error.UnsupportedParquet,
    };

    const start = out.rowCount();
    switch (out.data) {
        .varchar, .string, .char, .json => |*s| {
            if (column.physical != .byte_array or column.annotation != .none) return error.UnsupportedParquet;
            try appendStrings(allocator, s, values, dictionary, n, present, defined);
        },
        .boolean => |*list| {
            if (column.physical != .boolean) return error.UnsupportedParquet;
            try appendBooleans(allocator, list, values, n, present, defined);
        },
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .date, .datetime => |*list, tag| {
            const T = @typeInfo(@TypeOf(list.items)).pointer.child;
            const scale = try timeScale(tag, column.annotation);
            switch (column.physical) {
                inline .int32, .int64, .float, .double => |physical| {
                    const P = switch (physical) {
                        .int32 => i32,
                        .int64 => i64,
                        .float => f32,
                        .double => f64,
                        else => comptime unreachable,
                    };
                    try appendNumbers(T, P, allocator, list, values, dictionary, n, present, defined, scale, column.unsigned);
                },
                else => return error.UnsupportedParquet,
            }
        },
        else => return error.UnsupportedParquet,
    }
    if (out.nulls != null) {
        for (0..n) |i| try out.appendValidBit(allocator, start + i, defined == null or defined.?[i] != 0);
    }
}

fn countDefined(defined: []const u8) usize {
    var c: usize = 0;
    for (defined) |d| c += d;
    return c;
}

fn appendStrings(allocator: Allocator, s: *StringStore, values: Values, dictionary: Dictionary, n: usize, present: usize, defined: ?[]const u8) !void {
    try s.offsets.ensureUnusedCapacity(allocator, n);
    switch (values) {
        .plain => |bytes| {
            // Length prefixes make the values total less than the page.
            try s.bytes.ensureUnusedCapacity(allocator, bytes.len);
            var pos: usize = 0;
            for (0..n) |i| {
                if (defined) |d| if (d[i] == 0) {
                    s.appendValueAssumeCapacity("");
                    continue;
                };
                if (4 > bytes.len - pos) return error.CorruptParquet;
                const len = std.mem.readInt(u32, bytes[pos..][0..4], .little);
                pos += 4;
                if (len > bytes.len - pos) return error.CorruptParquet;
                s.appendValueAssumeCapacity(bytes[pos..][0..len]);
                pos += len;
            }
        },
        .indices => |ix| {
            const d = switch (dictionary) {
                .strings => |d| d,
                else => return error.CorruptParquet,
            };
            if (ix.len != present) return error.CorruptParquet;
            const count = d.offsets.len - 1;
            var total: usize = 0;
            for (ix) |k| {
                if (k >= count) return error.CorruptParquet;
                total += d.offsets[k + 1] - d.offsets[k];
            }
            try s.bytes.ensureUnusedCapacity(allocator, total);
            var next: usize = 0;
            for (0..n) |i| {
                if (defined) |def| if (def[i] == 0) {
                    s.appendValueAssumeCapacity("");
                    continue;
                };
                const k = ix[next];
                next += 1;
                s.appendValueAssumeCapacity(d.bytes[d.offsets[k]..d.offsets[k + 1]]);
            }
        },
    }
}

fn appendBooleans(allocator: Allocator, list: *std.ArrayList(u8), values: Values, n: usize, present: usize, defined: ?[]const u8) !void {
    switch (values) {
        .plain => |bytes| if (present > bytes.len * 8) return error.CorruptParquet,
        .indices => |ix| if (ix.len != present) return error.CorruptParquet,
    }
    try list.ensureUnusedCapacity(allocator, n);
    var next: usize = 0;
    for (0..n) |i| {
        if (defined) |d| if (d[i] == 0) {
            list.appendAssumeCapacity(0);
            continue;
        };
        list.appendAssumeCapacity(switch (values) {
            .plain => |bytes| (bytes[next >> 3] >> @intCast(next & 7)) & 1,
            .indices => |ix| @intFromBool(ix[next] != 0),
        });
        next += 1;
    }
}

fn appendNumbers(
    comptime T: type,
    comptime P: type,
    allocator: Allocator,
    list: *std.ArrayList(T),
    values: Values,
    dictionary: Dictionary,
    n: usize,
    present: usize,
    defined: ?[]const u8,
    scale: Scale,
    unsigned: bool,
) !void {
    const width = @sizeOf(P);
    const source: []const u8 = switch (values) {
        .plain => |bytes| blk: {
            if (present > bytes.len / width) return error.CorruptParquet;
            break :blk bytes;
        },
        .indices => |ix| blk: {
            const d = switch (dictionary) {
                .fixed => |d| d,
                else => return error.CorruptParquet,
            };
            if (d.width != width or ix.len != present) return error.CorruptParquet;
            for (ix) |k| if (k >= d.count) return error.CorruptParquet;
            break :blk d.bytes;
        },
    };
    try list.ensureUnusedCapacity(allocator, n);
    if (defined == null and scale == .mul and scale.mul == 1) {
        const out = list.addManyAsSliceAssumeCapacity(n);
        errdefer list.shrinkRetainingCapacity(list.items.len - n);
        switch (unsigned) {
            inline else => |u| switch (values) {
                .plain => for (out, 0..) |*v, slot| {
                    v.* = try convertUnscaled(T, P, u, readValue(P, source, slot));
                },
                .indices => |ix| for (out, ix) |*v, slot| {
                    v.* = try convertUnscaled(T, P, u, readValue(P, source, slot));
                },
            },
        }
        return;
    }
    var next: usize = 0;
    for (0..n) |i| {
        if (defined) |d| if (d[i] == 0) {
            list.appendAssumeCapacity(0);
            continue;
        };
        const slot = switch (values) {
            .plain => next,
            .indices => |ix| ix[next],
        };
        next += 1;
        const Bits = @Int(.unsigned, @bitSizeOf(P));
        const raw: P = @bitCast(std.mem.readInt(Bits, source[slot * width ..][0..width], .little));
        list.appendAssumeCapacity(try convert(T, P, raw, scale, unsigned));
    }
}

/// The factor taking a leaf's integers to column type `tag`'s unit: days
/// for DATE, microseconds for DATETIME (see the module doc), 1 otherwise.
fn timeScale(comptime tag: anytype, annotation: Annotation) !Scale {
    return switch (tag) {
        .date => switch (annotation) {
            .none, .days => .{ .mul = 1 },
            else => error.UnsupportedParquet,
        },
        .datetime => switch (annotation) {
            .none => .{ .mul = 1_000_000 },
            .days => .{ .mul = 86_400_000_000 },
            .millis => .{ .mul = 1_000 },
            .micros => .{ .mul = 1 },
            .nanos => .{ .div = 1_000 },
            .unsupported => error.UnsupportedParquet,
        },
        else => if (annotation != .none) error.UnsupportedParquet else .{ .mul = 1 },
    };
}

const Scale = union(enum) { mul: i64, div: i64 };

fn readValue(comptime P: type, source: []const u8, slot: usize) P {
    const width = @sizeOf(P);
    return @bitCast(std.mem.readInt(@Int(.unsigned, @bitSizeOf(P)), source[slot * width ..][0..width], .little));
}

/// `convert` at scale 1, in `P`'s own width rather than through i128, so a
/// page of values converts in a tight loop.
fn convertUnscaled(comptime T: type, comptime P: type, comptime unsigned: bool, raw: P) !T {
    if (@typeInfo(T) == .float) {
        return if (@typeInfo(P) == .float) @floatCast(raw) else @floatFromInt(raw);
    }
    if (@typeInfo(P) == .float) return error.UnsupportedParquet;
    const value = if (unsigned) @as(@Int(.unsigned, @bitSizeOf(P)), @bitCast(raw)) else raw;
    return std.math.cast(T, value) orelse error.ParquetValueOutOfRange;
}

fn convert(comptime T: type, comptime P: type, raw: P, scale: Scale, unsigned: bool) !T {
    if (@typeInfo(T) == .float) {
        return if (@typeInfo(P) == .float) @floatCast(raw) else @floatFromInt(raw);
    }
    if (@typeInfo(P) == .float) return error.UnsupportedParquet;
    const value: i128 = if (unsigned) @as(@Int(.unsigned, @bitSizeOf(P)), @bitCast(raw)) else raw;
    const scaled: i128 = switch (scale) {
        .mul => |m| value * m,
        .div => |d| @divFloor(value, d),
    };
    return std.math.cast(T, scaled) orelse error.ParquetValueOutOfRange;
}

// ---- Thrift compact protocol ----

const Kind = enum(u4) {
    stop = 0,
    true = 1,
    false = 2,
    byte = 3,
    i16 = 4,
    i32 = 5,
    i64 = 6,
    double = 7,
    binary = 8,
    list = 9,
    set = 10,
    map = 11,
    @"struct" = 12,
    _,
};

const Thrift = struct {
    bytes: []const u8,
    pos: usize = 0,
    depth: u8 = 0,

    const max_depth = 32;

    fn byte(self: *Thrift) !u8 {
        if (self.pos >= self.bytes.len) return error.CorruptParquet;
        defer self.pos += 1;
        return self.bytes[self.pos];
    }

    fn zigzag(self: *Thrift) !i64 {
        const v = try varint(self.bytes, &self.pos);
        return @bitCast((v >> 1) ^ (0 -% (v & 1)));
    }

    /// The next field's id and kind, or null at the struct's end. `last`
    /// carries the previous id: ids are deltas from it.
    fn field(self: *Thrift, last: *i16) !?struct { id: i16, kind: Kind } {
        const b = try self.byte();
        if (b == 0) return null;
        const delta: i16 = b >> 4;
        const id: i16 = if (delta != 0) last.* +% delta else std.math.cast(i16, try self.zigzag()) orelse return error.CorruptParquet;
        last.* = id;
        return .{ .id = id, .kind = @fromBackingInt(@intCast(@as(u4, @truncate(b)))) };
    }

    /// An integer field declared `want` wide; narrower encodings are accepted.
    fn int(self: *Thrift, kind: Kind, comptime want: Kind) !i64 {
        const ok = switch (kind) {
            .i16 => true,
            .i32 => want != .i16,
            .i64 => want == .i64,
            else => false,
        };
        if (!ok) return error.CorruptParquet;
        return self.zigzag();
    }

    fn unsigned(self: *Thrift, kind: Kind, comptime want: Kind) !u64 {
        return std.math.cast(u64, try self.int(kind, want)) orelse error.CorruptParquet;
    }

    fn boolean(self: *Thrift, kind: Kind) !bool {
        _ = self;
        return switch (kind) {
            .true => true,
            .false => false,
            else => error.CorruptParquet,
        };
    }

    fn binary(self: *Thrift, kind: Kind) ![]const u8 {
        if (kind != .binary) return error.CorruptParquet;
        const len = try varint(self.bytes, &self.pos);
        if (len > self.bytes.len - self.pos) return error.CorruptParquet;
        defer self.pos += @intCast(len);
        return self.bytes[self.pos..][0..@intCast(len)];
    }

    fn listHeader(self: *Thrift, kind: Kind, comptime elem: Kind) !usize {
        if (kind != .list) return error.CorruptParquet;
        const h = try self.listOrSet();
        if (h.elem != elem) return error.CorruptParquet;
        return h.len;
    }

    fn listOrSet(self: *Thrift) !struct { len: usize, elem: Kind } {
        const b = try self.byte();
        const short: usize = b >> 4;
        const len: usize = if (short == 15) std.math.cast(usize, try varint(self.bytes, &self.pos)) orelse return error.CorruptParquet else short;
        // Every element takes at least one byte.
        if (len > self.bytes.len - self.pos) return error.CorruptParquet;
        return .{ .len = len, .elem = @fromBackingInt(@intCast(@as(u4, @truncate(b)))) };
    }

    fn skip(self: *Thrift, kind: Kind) Error!void {
        if (self.depth >= max_depth) return error.CorruptParquet;
        self.depth += 1;
        defer self.depth -= 1;
        switch (kind) {
            .true, .false => {},
            .byte => _ = try self.byte(),
            .i16, .i32, .i64 => _ = try varint(self.bytes, &self.pos),
            .double => {
                if (8 > self.bytes.len - self.pos) return error.CorruptParquet;
                self.pos += 8;
            },
            .binary => _ = try self.binary(.binary),
            .list, .set => {
                const h = try self.listOrSet();
                for (0..h.len) |_| try self.skipElement(h.elem);
            },
            .map => {
                const len = try varint(self.bytes, &self.pos);
                if (len == 0) return;
                if (len > self.bytes.len - self.pos) return error.CorruptParquet;
                const kinds = try self.byte();
                for (0..@intCast(len)) |_| {
                    try self.skipElement(@fromBackingInt(@intCast(@as(u4, @truncate(kinds >> 4)))));
                    try self.skipElement(@fromBackingInt(@intCast(@as(u4, @truncate(kinds)))));
                }
            },
            .@"struct" => {
                var last: i16 = 0;
                while (try self.field(&last)) |f| try self.skip(f.kind);
            },
            .stop, _ => return error.CorruptParquet,
        }
    }

    /// A list, set or map element: a boolean takes a whole byte there.
    fn skipElement(self: *Thrift, kind: Kind) Error!void {
        if (kind == .true or kind == .false) {
            _ = try self.byte();
            return;
        }
        try self.skip(kind);
    }
};

// ---- tests ----

/// Thrift compact-protocol bytes for hand-built footers and page headers.
const TestThrift = struct {
    bytes: std.ArrayList(u8) = .empty,

    fn deinit(self: *TestThrift) void {
        self.bytes.deinit(std.testing.allocator);
    }

    fn raw(self: *TestThrift, b: []const u8) !void {
        try self.bytes.appendSlice(std.testing.allocator, b);
    }

    fn uvarint(self: *TestThrift, value: u64) !void {
        var v = value;
        while (v >= 0x80) : (v >>= 7) try self.raw(&.{@as(u8, @truncate(v)) | 0x80});
        try self.raw(&.{@intCast(v)});
    }

    /// A field `delta` ids after the previous one, of type `kind`.
    fn field(self: *TestThrift, delta: u4, kind: Kind) !void {
        try self.raw(&.{(@as(u8, delta) << 4) | @backingInt(kind)});
    }

    fn int(self: *TestThrift, delta: u4, value: i64) !void {
        try self.field(delta, .i32);
        try self.uvarint(@bitCast((value << 1) ^ (value >> 63)));
    }

    fn stop(self: *TestThrift) !void {
        try self.raw(&.{0});
    }

    /// A V1 data page's or a dictionary page's header, then its body.
    fn page(self: *TestThrift, kind: PageType, values: i64, encoding: Encoding, body: []const u8) !void {
        try self.int(1, @backingInt(kind));
        try self.int(1, @intCast(body.len));
        try self.int(1, @intCast(body.len));
        try self.field(if (kind == .dictionary_page) 4 else 2, .@"struct");
        try self.int(1, values);
        try self.int(1, @backingInt(encoding));
        try self.stop();
        try self.stop();
        try self.raw(body);
    }
};

fn testDecode(column: Column, chunk_bytes: []const u8, values: u64, out: *ColumnStore) !void {
    var scratch: Scratch = .{};
    defer scratch.deinit(std.testing.allocator);
    const chunk: ColumnChunk = .{ .codec = .uncompressed, .start = 0, .len = chunk_bytes.len, .values = values };
    try decodeChunk(std.testing.allocator, &scratch, column, chunk, chunk_bytes, out);
}

test "parquet hybrid decoder reads RLE and bit-packed runs" {
    // Five 3s at width 2, then one bit-packed group of 0, 1, 2, 3, 0, 1, 2, 3.
    const bytes = [_]u8{ 5 << 1, 3, (1 << 1) | 1, 0b11100100, 0b11100100 };
    var out: [13]u32 = undefined;
    try decodeHybrid(&bytes, 2, &out);
    try std.testing.expectEqualSlices(u32, &.{ 3, 3, 3, 3, 3, 0, 1, 2, 3, 0, 1, 2, 3 }, &out);
    // A run past the end of the bytes.
    try std.testing.expectError(error.CorruptParquet, decodeHybrid(bytes[0..4], 2, &out));
    try std.testing.expectError(error.CorruptParquet, decodeHybrid(bytes[0..1], 2, &out));
}

test "parquet unpackBits reads every width" {
    var prng: std.Random.DefaultPrng = .init(7);
    const random = prng.random();
    var width: u6 = 1;
    while (width <= 32) : (width += 1) {
        var want: [40]u32 = undefined;
        var packed_bytes: [40 * 4]u8 = @splat(0);
        var bit: usize = 0;
        for (&want) |*w| {
            w.* = @truncate(random.int(u64) & ((@as(u64, 1) << width) - 1));
            for (0..width) |b| {
                if ((w.* >> @intCast(b)) & 1 != 0) packed_bytes[(bit + b) / 8] |= @as(u8, 1) << @intCast((bit + b) % 8);
            }
            bit += width;
        }
        var got: [40]u32 = undefined;
        unpackBits(packed_bytes[0 .. (bit + 7) / 8], width, &got);
        try std.testing.expectEqualSlices(u32, &want, &got);
    }
}

test "parquet REQUIRED chunk decodes dictionary and PLAIN pages in order" {
    const allocator = std.testing.allocator;
    var w: TestThrift = .{};
    defer w.deinit();
    var dict: [12]u8 = undefined;
    for ([_]i32{ 10, -20, 30 }, 0..) |v, i| std.mem.writeInt(i32, dict[i * 4 ..][0..4], v, .little);
    try w.page(.dictionary_page, 3, .plain, &dict);
    // Width 2, one bit-packed group of indices 0, 1, 2, 2, 1, 0 (and two
    // pad values past the page's six).
    try w.page(.data_page, 6, .rle_dictionary, &.{ 2, (1 << 1) | 1, 0b10100100, 0b00000001 });
    var plain: [8]u8 = undefined;
    std.mem.writeInt(i32, plain[0..4], 7, .little);
    std.mem.writeInt(i32, plain[4..8], -1, .little);
    try w.page(.data_page, 2, .plain, &plain);

    const column: Column = .{ .name = "x", .physical = .int32, .optional = false, .annotation = .none, .unsigned = false };
    var ints = try ColumnStore.initCapacity(allocator, .int, false, 8, 0);
    defer ints.deinit(allocator);
    try testDecode(column, w.bytes.items, 8, &ints);
    try std.testing.expectEqualSlices(i32, &.{ 10, -20, 30, 30, -20, 10, 7, -1 }, ints.data.int.items);

    // UINT_32: the raw -1 is 4294967295, which a BIGINT holds and an INT
    // doesn't.
    var unsigned = column;
    unsigned.unsigned = true;
    var bigints = try ColumnStore.initCapacity(allocator, .bigint, false, 8, 0);
    defer bigints.deinit(allocator);
    try testDecode(unsigned, w.bytes.items, 8, &bigints);
    try std.testing.expectEqual(@as(i64, 4294967295), bigints.data.bigint.items[7]);
    var narrow = try ColumnStore.initCapacity(allocator, .int, false, 8, 0);
    defer narrow.deinit(allocator);
    try std.testing.expectError(error.ParquetValueOutOfRange, testDecode(unsigned, w.bytes.items, 8, &narrow));

    // A data page before any dictionary page.
    var bad: TestThrift = .{};
    defer bad.deinit();
    try bad.page(.data_page, 1, .rle_dictionary, &.{ 2, 1 << 1, 0 });
    var none = try ColumnStore.initCapacity(allocator, .int, false, 1, 0);
    defer none.deinit(allocator);
    try std.testing.expectError(error.CorruptParquet, testDecode(column, bad.bytes.items, 1, &none));
}

test "parquet thrift skips every field kind" {
    var w: TestThrift = .{};
    defer w.deinit();
    try w.field(1, .binary);
    try w.uvarint(3);
    try w.raw("abc");
    try w.field(1, .list);
    try w.raw(&.{@as(u8, 2 << 4) | @backingInt(Kind.i32)});
    try w.uvarint(2);
    try w.uvarint(4);
    try w.field(1, .@"struct");
    try w.field(1, .double);
    try w.raw(&@as([8]u8, @splat(0)));
    try w.stop();
    try w.field(1, .true);
    try w.field(1, .map);
    try w.uvarint(1);
    try w.raw(&.{(@as(u8, @backingInt(Kind.binary)) << 4) | @backingInt(Kind.i64)});
    try w.uvarint(1);
    try w.raw("k");
    try w.uvarint(10);
    try w.int(1, -5);
    try w.stop();

    var r: Thrift = .{ .bytes = w.bytes.items };
    var last: i16 = 0;
    var fields: usize = 0;
    while (try r.field(&last)) |f| {
        if (f.id == 6) try std.testing.expectEqual(@as(i64, -5), try r.int(f.kind, .i32)) else try r.skip(f.kind);
        fields += 1;
    }
    try std.testing.expectEqual(@as(usize, 6), fields);
    try std.testing.expectEqual(w.bytes.items.len, r.pos);

    // A nested struct past the depth bound, and a truncated binary.
    var deep: TestThrift = .{};
    defer deep.deinit();
    for (0..40) |_| try deep.field(1, .@"struct");
    var rd: Thrift = .{ .bytes = deep.bytes.items };
    var dl: i16 = 0;
    const f = (try rd.field(&dl)).?;
    try std.testing.expectError(error.CorruptParquet, rd.skip(f.kind));
    var rt: Thrift = .{ .bytes = &.{ 5, 'a' } };
    try std.testing.expectError(error.CorruptParquet, rt.binary(.binary));
}

test "parquet footer parse rejects files that aren't Parquet" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const io = std.testing.io;
    for ([_][]const u8{ "", "PAR1", "PAR1\x00\x00\x00\x00PAR0", "PAR1\xff\xff\x00\x00PAR1" }, 0..) |bytes, i| {
        var name_buf: [16]u8 = undefined;
        const name = try std.fmt.bufPrint(&name_buf, "f{d}", .{i});
        try tmp.dir.writeFile(io, .{ .sub_path = name, .data = bytes });
        var file = try tmp.dir.openFile(io, name, .{});
        defer file.close(io);
        const result = readMetadata(allocator, io, file);
        if (result) |m| {
            var meta = m;
            meta.deinit();
            return error.TestExpectedError;
        } else |err| try std.testing.expect(err == error.NotParquet or err == error.CorruptParquet);
    }
}
