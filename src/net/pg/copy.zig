//! PostgreSQL `COPY ... FROM STDIN` / `COPY ... TO STDOUT` wire handler.
//!
//! Text format only (FORMAT TEXT in PG's options grammar). Fields are
//! TAB-separated, rows NEWLINE-terminated, NULL is the literal `\N`,
//! and backslash escapes the standard set (`\t`, `\n`, `\r`, `\b`,
//! `\f`, `\v`, `\\`). Any other `\<x>` passes through as `<x>`.
//!
//! COPY interleaves with the wire (the server reads `CopyData`/`CopyDone`/
//! `CopyFail` frames between the initial `CopyInResponse` and the
//! terminating `CommandComplete`), so it can't ride the generic
//! `compileWithSession` path — this module is the dispatcher the PG
//! server hands `.copy` ops off to.

const std = @import("std");
const Allocator = std.mem.Allocator;

const thindb_api = @import("../../api/api.zig");
const Catalog = thindb_api.Catalog;
const Session = thindb_api.Session;
const Table = thindb_api.Table;
const ApiError = thindb_api.Error;

const types = @import("../../types.zig");
const Column = types.Column;
const Value = types.Value;

const storage = @import("../../storage/storage.zig");
const ColumnView = storage.ColumnView;

const ir = @import("../../ir/ir.zig");
const local = @import("../local.zig");
const exec = @import("../../exec/exec.zig");

const packet = @import("packet.zig");
const result = @import("result.zig");
const wire_format = @import("../wire_format.zig");

/// COPY-specific failure surface. Errors here propagate up to the
/// generic mapInternal path; the dispatcher catches them, emits an
/// ErrorResponse, drains the input stream, then sends ReadyForQuery.
pub const Error = error{
    CopyFileNotSupported,
    CopyFromClientAborted,
    CopyUnexpectedFrame,
    CopyMalformedRow,
    CopyMustBeSoleStatement,
};

/// Entry point for a top-level `.copy` op coming out of the parser.
/// Owns the entire wire round-trip: CopyInResponse / CopyOutResponse,
/// CopyData frames, CopyDone / CopyFail, and the terminating
/// CommandComplete. Does NOT send ReadyForQuery — the caller does that
/// after this returns (success or error).
pub fn handleCopy(
    allocator: Allocator,
    w: *std.Io.Writer,
    r: *std.Io.Reader,
    catalog: *Catalog,
    session: Session,
    op: ir.CopyOp,
) !void {
    const t = try local.resolveTable(catalog, session, op.table);
    switch (op.direction) {
        .from_stdin => try handleCopyFrom(allocator, w, r, t, op.columns),
        .to_stdout => try handleCopyTo(allocator, w, t, op.columns),
    }
}

// ---------------------------------------------------------------------------
// COPY FROM STDIN
// ---------------------------------------------------------------------------

fn handleCopyFrom(
    allocator: Allocator,
    w: *std.Io.Writer,
    r: *std.Io.Reader,
    table: *Table,
    user_columns: ?[]const []const u8,
) !void {
    const tbl_schema = table.schema;

    // schema_to_source[i] = which source column feeds table column i,
    // or null when the user's column list omits it (must be nullable).
    const schema_to_source = try allocator.alloc(?usize, tbl_schema.columns.len);
    defer allocator.free(schema_to_source);
    for (schema_to_source) |*s| s.* = null;

    const source_width = if (user_columns) |cols| cols.len else tbl_schema.columns.len;
    if (user_columns) |cols| {
        for (cols, 0..) |cname, src_idx| {
            const si = tbl_schema.columnIndex(cname) orelse return ApiError.ColumnNotFound;
            schema_to_source[si] = src_idx;
        }
    } else {
        for (schema_to_source, 0..) |*s, i| s.* = i;
    }
    for (schema_to_source, 0..) |maybe_src, si| {
        if (maybe_src == null and !tbl_schema.columns[si].nullable) return ApiError.ColumnNotFound;
    }

    try sendCopyInResponse(allocator, w, source_width);
    try w.flush();

    // Drain all input first, THEN parse / insert. Errors mid-parse
    // can't leave the wire half-consumed because we've already read
    // through CopyDone/CopyFail before we touch row content.
    var raw_buf = std.ArrayList(u8).empty;
    defer raw_buf.deinit(allocator);
    var client_failed: bool = false;
    while (true) {
        const f = try packet.readFrame(allocator, r);
        defer allocator.free(f.payload);
        switch (f.type_byte) {
            'd' => try raw_buf.appendSlice(allocator, f.payload),
            'c' => break,
            'f' => {
                client_failed = true;
                break;
            },
            // Sync, Flush, and the like aren't expected mid-COPY in
            // simple-query mode; treat them as protocol errors.
            else => return Error.CopyUnexpectedFrame,
        }
    }
    if (client_failed) return Error.CopyFromClientAborted;

    var rows = std.ArrayList([]const ?[]const u8).empty;
    defer {
        for (rows.items) |row| {
            for (row) |c| if (c) |s| allocator.free(s);
            allocator.free(row);
        }
        rows.deinit(allocator);
    }

    try splitLines(allocator, raw_buf.items, &rows, source_width);

    if (rows.items.len == 0) {
        try sendCommandComplete(allocator, w, 0);
        return;
    }

    var builder = try local.InsertColumnBuilder.init(allocator, tbl_schema, rows.items.len);
    defer builder.deinit();

    for (rows.items) |row| {
        for (tbl_schema.columns, 0..) |col, si| {
            const maybe_src = schema_to_source[si];
            const cell_text: ?[]const u8 = if (maybe_src) |src| row[src] else null;
            const v = try cellToValue(col, cell_text);
            builder.appendCell(si, col, v) catch return Error.CopyMalformedRow;
        }
    }

    try table.insertBatch(builder.schemaSlice(), builder.views(), rows.items.len);
    try sendCommandComplete(allocator, w, rows.items.len);
}

/// Split `data` on '\n' into rows, honouring `\.` as PG's end-of-data
/// sentinel and tolerating a missing trailing newline.
fn splitLines(
    allocator: Allocator,
    data: []const u8,
    rows: *std.ArrayList([]const ?[]const u8),
    source_width: usize,
) !void {
    var i: usize = 0;
    while (i < data.len) {
        const nl = std.mem.indexOfScalarPos(u8, data, i, '\n') orelse data.len;
        const line = data[i..nl];
        if (line.len == 2 and line[0] == '\\' and line[1] == '.') return;
        const row = try parseTextRow(allocator, line, source_width);
        try rows.append(allocator, row);
        i = if (nl == data.len) data.len else nl + 1;
    }
}

/// Parse one text-format row into N nullable cells. Cells are owned by
/// `allocator`; freed when the parent row slice is freed.
fn parseTextRow(
    allocator: Allocator,
    line: []const u8,
    source_width: usize,
) ![]const ?[]const u8 {
    var cells = std.ArrayList(?[]const u8).empty;
    errdefer {
        for (cells.items) |c| if (c) |s| allocator.free(s);
        cells.deinit(allocator);
    }

    var i: usize = 0;
    var field = std.ArrayList(u8).empty;
    defer field.deinit(allocator);

    while (true) {
        field.clearRetainingCapacity();
        while (i < line.len and line[i] != '\t') {
            if (line[i] == '\\' and i + 1 < line.len) {
                try field.append(allocator, line[i]);
                try field.append(allocator, line[i + 1]);
                i += 2;
            } else {
                try field.append(allocator, line[i]);
                i += 1;
            }
        }
        const raw = field.items;
        // `\N` (exactly two bytes, no other content) → NULL.
        const cell: ?[]const u8 = if (raw.len == 2 and raw[0] == '\\' and raw[1] == 'N')
            null
        else
            try decodeTextField(allocator, raw);
        try cells.append(allocator, cell);

        if (i >= line.len) break;
        if (line[i] == '\t') {
            i += 1;
            continue;
        }
    }

    if (cells.items.len != source_width) return Error.CopyMalformedRow;
    return try cells.toOwnedSlice(allocator);
}

/// Decode the text-format escape sequences in `raw` into a fresh
/// allocator-owned slice.
fn decodeTextField(allocator: Allocator, raw: []const u8) ![]u8 {
    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    try out.ensureTotalCapacity(allocator, raw.len);

    var i: usize = 0;
    while (i < raw.len) : (i += 1) {
        const c = raw[i];
        if (c != '\\' or i + 1 >= raw.len) {
            try out.append(allocator, c);
            continue;
        }
        i += 1;
        const next = raw[i];
        const decoded: u8 = switch (next) {
            'b' => 0x08,
            'f' => 0x0C,
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            'v' => 0x0B,
            '\\' => '\\',
            else => next,
        };
        try out.append(allocator, decoded);
    }
    return try out.toOwnedSlice(allocator);
}

fn cellToValue(col: Column, text: ?[]const u8) !?Value {
    const s = text orelse return null;
    return switch (col.type) {
        .int => Value{ .int = parseSignedInt(i32, s) catch return Error.CopyMalformedRow },
        .bigint => Value{ .bigint = parseSignedInt(i64, s) catch return Error.CopyMalformedRow },
        .smallint => Value{ .smallint = parseSignedInt(i16, s) catch return Error.CopyMalformedRow },
        .tinyint => Value{ .tinyint = parseSignedInt(i8, s) catch return Error.CopyMalformedRow },
        .largeint => Value{ .largeint = parseSignedInt(i128, s) catch return Error.CopyMalformedRow },
        .boolean => Value{ .boolean = try parseBoolText(s) },
        .float => Value{ .float = std.fmt.parseFloat(f32, s) catch return Error.CopyMalformedRow },
        .double => Value{ .double = std.fmt.parseFloat(f64, s) catch return Error.CopyMalformedRow },
        .date, .datetime, .uuid, .decimal64, .decimal128, .varchar, .string, .char, .json => Value{ .text = s },
    };
}

fn parseSignedInt(comptime T: type, s: []const u8) !T {
    return std.fmt.parseInt(T, s, 10);
}

fn parseBoolText(s: []const u8) !bool {
    if (s.len == 0) return Error.CopyMalformedRow;
    // PG accepts t/true/y/yes/on/1 and the negative counterparts.
    if (eqIgnoreCase(s, "t") or eqIgnoreCase(s, "true") or
        eqIgnoreCase(s, "y") or eqIgnoreCase(s, "yes") or
        eqIgnoreCase(s, "on") or std.mem.eql(u8, s, "1")) return true;
    if (eqIgnoreCase(s, "f") or eqIgnoreCase(s, "false") or
        eqIgnoreCase(s, "n") or eqIgnoreCase(s, "no") or
        eqIgnoreCase(s, "off") or std.mem.eql(u8, s, "0")) return false;
    return Error.CopyMalformedRow;
}

fn eqIgnoreCase(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

// ---------------------------------------------------------------------------
// COPY TO STDOUT
// ---------------------------------------------------------------------------

fn handleCopyTo(
    allocator: Allocator,
    w: *std.Io.Writer,
    table: *Table,
    user_columns: ?[]const []const u8,
) !void {
    const tbl_schema = table.schema;

    // Compute the projection: which output column maps to which scan
    // batch column. When `user_columns == null` we just stream every
    // column in schema-declared order.
    const col_count = if (user_columns) |cols| cols.len else tbl_schema.columns.len;
    const out_indices = try allocator.alloc(usize, col_count);
    defer allocator.free(out_indices);
    if (user_columns) |cols| {
        for (cols, 0..) |cname, out_i| {
            out_indices[out_i] = tbl_schema.columnIndex(cname) orelse return ApiError.ColumnNotFound;
        }
    } else {
        for (out_indices, 0..) |*idx, i| idx.* = i;
    }

    try sendCopyOutResponse(allocator, w, col_count);

    var scan_query = try exec.scan(allocator, table);
    defer scan_query.deinit();

    var scratch = std.ArrayList(u8).empty;
    defer scratch.deinit(allocator);
    var line = std.ArrayList(u8).empty;
    defer line.deinit(allocator);

    var row_count: u64 = 0;
    while (try scan_query.next()) |batch| {
        var r_idx: usize = 0;
        while (r_idx < batch.row_count) : (r_idx += 1) {
            line.clearRetainingCapacity();
            for (out_indices, 0..) |col_idx, out_i| {
                if (out_i > 0) try line.append(allocator, '\t');
                const col = batch.schema[col_idx];
                const view = batch.values[col_idx];
                if (!view.isValid(r_idx)) {
                    try line.appendSlice(allocator, "\\N");
                    continue;
                }
                scratch.clearRetainingCapacity();
                try formatCellText(&scratch, allocator, col, view, r_idx);
                try appendEscaped(allocator, &line, scratch.items);
            }
            try line.append(allocator, '\n');
            try packet.writeFrame(w, 'd', line.items);
            row_count += 1;
        }
    }

    // CopyDone (empty 'c' frame) marks end-of-stream.
    try packet.writeFrame(w, 'c', "");
    try sendCommandComplete(allocator, w, row_count);
}

/// Append `raw` to `out`, escaping the bytes COPY text format reserves
/// (`\\`, `\b`, `\f`, `\n`, `\r`, `\t`, `\v`). Anything else passes
/// through unchanged.
fn appendEscaped(allocator: Allocator, out: *std.ArrayList(u8), raw: []const u8) !void {
    for (raw) |c| switch (c) {
        '\\' => try out.appendSlice(allocator, "\\\\"),
        '\n' => try out.appendSlice(allocator, "\\n"),
        '\r' => try out.appendSlice(allocator, "\\r"),
        '\t' => try out.appendSlice(allocator, "\\t"),
        0x08 => try out.appendSlice(allocator, "\\b"),
        0x0C => try out.appendSlice(allocator, "\\f"),
        0x0B => try out.appendSlice(allocator, "\\v"),
        else => try out.append(allocator, c),
    };
}

fn formatCellText(
    out: *std.ArrayList(u8),
    allocator: Allocator,
    col: Column,
    view: ColumnView,
    row: usize,
) !void {
    var num_buf: [64]u8 = undefined;
    switch (view.data) {
        .int => |s| try out.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .bigint => |s| try out.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .smallint => |s| try out.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .tinyint => |s| try out.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .largeint => |s| try out.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .boolean => |s| try out.appendSlice(allocator, if (s[row] != 0) "t" else "f"),
        .float => |s| try out.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{@as(f64, s[row])})),
        .double => |s| try out.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .date => |s| {
            var buf: [16]u8 = undefined;
            try out.appendSlice(allocator, try wire_format.formatDate(&buf, s[row]));
        },
        .datetime => |s| {
            var buf: [40]u8 = undefined;
            try out.appendSlice(allocator, try wire_format.formatDateTime(&buf, s[row]));
        },
        .decimal64 => |s| try wire_format.formatDecimal(allocator, out, @as(i128, s[row]), col.type),
        .decimal128 => |s| try wire_format.formatDecimal(allocator, out, s[row], col.type),
        .uuid => |s| {
            var buf: [40]u8 = undefined;
            try out.appendSlice(allocator, try wire_format.formatUuid(&buf, s[row]));
        },
        .string, .varchar, .char, .json => |sv| try out.appendSlice(allocator, sv.rowBytes(row)),
    }
}

// ---------------------------------------------------------------------------
// Frame senders
// ---------------------------------------------------------------------------

/// CopyInResponse 'G' / CopyOutResponse 'H' share a payload shape:
///   int8  format (0 = text, 1 = binary)
///   int16 column count
///   int16 * N column formats (0 for text rows)
fn writeCopyHeader(
    allocator: Allocator,
    w: *std.Io.Writer,
    type_byte: u8,
    column_count: usize,
) !void {
    var payload = std.ArrayList(u8).empty;
    defer payload.deinit(allocator);
    try payload.append(allocator, 0); // overall format: text
    try packet.appendI16(allocator, &payload, @intCast(column_count));
    var i: usize = 0;
    while (i < column_count) : (i += 1) {
        try packet.appendI16(allocator, &payload, 0);
    }
    try packet.writeFrame(w, type_byte, payload.items);
}

pub fn sendCopyInResponse(
    allocator: Allocator,
    w: *std.Io.Writer,
    column_count: usize,
) !void {
    try writeCopyHeader(allocator, w, 'G', column_count);
}

pub fn sendCopyOutResponse(
    allocator: Allocator,
    w: *std.Io.Writer,
    column_count: usize,
) !void {
    try writeCopyHeader(allocator, w, 'H', column_count);
}

fn sendCommandComplete(allocator: Allocator, w: *std.Io.Writer, rows: u64) !void {
    var tag_buf: [40]u8 = undefined;
    const tag = try std.fmt.bufPrint(&tag_buf, "COPY {d}", .{rows});
    try result.sendCommandComplete(allocator, w, tag);
}


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "decodeTextField unescapes the documented sequences" {
    const allocator = std.testing.allocator;
    const cases = .{
        .{ .raw = "hello", .expected = "hello" },
        .{ .raw = "a\\tb", .expected = "a\tb" },
        .{ .raw = "x\\\\y", .expected = "x\\y" },
        .{ .raw = "line1\\nline2", .expected = "line1\nline2" },
        .{ .raw = "carry\\r", .expected = "carry\r" },
        .{ .raw = "raw\\x", .expected = "rawx" },
    };
    inline for (cases) |c| {
        const out = try decodeTextField(allocator, c.raw);
        defer allocator.free(out);
        try std.testing.expectEqualStrings(c.expected, out);
    }
}

test "appendEscaped escapes tab newline backslash" {
    const allocator = std.testing.allocator;
    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    try appendEscaped(allocator, &out, "a\tb\nc\\d");
    try std.testing.expectEqualStrings("a\\tb\\nc\\\\d", out.items);
}

test "parseTextRow splits on tabs and honours \\N" {
    const allocator = std.testing.allocator;
    const row = try parseTextRow(allocator, "1\thello\t\\N", 3);
    defer {
        for (row) |c| if (c) |s| allocator.free(s);
        allocator.free(row);
    }
    try std.testing.expect(row[0] != null);
    try std.testing.expectEqualStrings("1", row[0].?);
    try std.testing.expectEqualStrings("hello", row[1].?);
    try std.testing.expect(row[2] == null);
}

test "parseTextRow round-trips embedded escapes" {
    const allocator = std.testing.allocator;
    const row = try parseTextRow(allocator, "with\\ttab\twith\\nnewline\twith\\\\back", 3);
    defer {
        for (row) |c| if (c) |s| allocator.free(s);
        allocator.free(row);
    }
    try std.testing.expectEqualStrings("with\ttab", row[0].?);
    try std.testing.expectEqualStrings("with\nnewline", row[1].?);
    try std.testing.expectEqualStrings("with\\back", row[2].?);
}

test "parseTextRow rejects wrong column count" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(Error.CopyMalformedRow, parseTextRow(allocator, "1\t2", 3));
}

test "splitLines tolerates a missing trailing newline" {
    const allocator = std.testing.allocator;
    var rows = std.ArrayList([]const ?[]const u8).empty;
    defer {
        for (rows.items) |row| {
            for (row) |c| if (c) |s| allocator.free(s);
            allocator.free(row);
        }
        rows.deinit(allocator);
    }
    try splitLines(allocator, "1\tfoo\n2\tbar", &rows, 2);
    try std.testing.expectEqual(@as(usize, 2), rows.items.len);
    try std.testing.expectEqualStrings("1", rows.items[0][0].?);
    try std.testing.expectEqualStrings("bar", rows.items[1][1].?);
}

test "splitLines honours the \\. end-of-data sentinel" {
    const allocator = std.testing.allocator;
    var rows = std.ArrayList([]const ?[]const u8).empty;
    defer {
        for (rows.items) |row| {
            for (row) |c| if (c) |s| allocator.free(s);
            allocator.free(row);
        }
        rows.deinit(allocator);
    }
    try splitLines(allocator, "1\tfoo\n\\.\n2\tbar\n", &rows, 2);
    try std.testing.expectEqual(@as(usize, 1), rows.items.len);
}
