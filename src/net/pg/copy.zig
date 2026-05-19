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
const Type = types.Type;
const Value = types.Value;
const TableSchema = types.TableSchema;

const storage = @import("../../storage/storage.zig");
const ColumnView = storage.ColumnView;

const ir = @import("../../ir/ir.zig");
const local = @import("../local.zig");
const exec = @import("../../exec/exec.zig");

const packet = @import("packet.zig");
const startup = @import("startup.zig");
const result = @import("result.zig");
const errors = @import("errors.zig");
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

    var builder = try InsertColumnBuilder.init(allocator, tbl_schema, rows.items.len);
    defer builder.deinit();

    for (rows.items) |row| {
        for (tbl_schema.columns, 0..) |col, si| {
            const maybe_src = schema_to_source[si];
            const cell_text: ?[]const u8 = if (maybe_src) |src| row[src] else null;
            const v = try cellToValue(col, cell_text);
            try builder.appendCell(si, col, v);
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
        .date, .datetime, .uuid, .decimal64, .decimal128, .varchar, .string, .char => Value{ .text = s },
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
        .string, .varchar, .char => |sv| try out.appendSlice(allocator, sv.rowBytes(row)),
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
// InsertColumnBuilder — a slimmed-down copy of local.zig's builder so
// the COPY path doesn't have to round-trip its rows through the IR
// Insert encoder. Only handles the subset of coercions that the COPY
// text format produces (numeric → typed, text → date/datetime/decimal/
// uuid/string-family).
// ---------------------------------------------------------------------------

const InsertColumnBuilder = struct {
    allocator: Allocator,
    schema_copy: []Column,
    fixed_slabs: [][]align(16) u8,
    fixed_cursor: []usize,
    string_offsets: []std.ArrayListUnmanaged(u32),
    string_bytes: []std.ArrayListUnmanaged(u8),
    nulls: []std.ArrayListUnmanaged(u8),
    view_slice: []ColumnView,
    row_count: usize,

    fn init(allocator: Allocator, table_schema: TableSchema, row_count: usize) !InsertColumnBuilder {
        const n_cols = table_schema.columns.len;
        const schema_copy = try allocator.alloc(Column, n_cols);
        errdefer allocator.free(schema_copy);
        for (table_schema.columns, 0..) |c, i| schema_copy[i] = c;

        const fixed_slabs = try allocator.alloc([]align(16) u8, n_cols);
        errdefer allocator.free(fixed_slabs);
        var slabs_inited: usize = 0;
        errdefer for (fixed_slabs[0..slabs_inited]) |s| if (s.len > 0) allocator.free(s);
        for (table_schema.columns, 0..) |c, i| {
            const per_row: usize = switch (c.type) {
                .int, .date, .float => 4,
                .bigint, .double, .datetime, .decimal64 => 8,
                .smallint => 2,
                .tinyint, .boolean => 1,
                .largeint, .decimal128, .uuid => 16,
                .varchar, .string, .char => 0,
            };
            fixed_slabs[i] = if (per_row == 0)
                &[_]u8{}
            else
                try allocator.alignedAlloc(u8, .@"16", per_row * row_count);
            slabs_inited = i + 1;
        }

        const fixed_cursor = try allocator.alloc(usize, n_cols);
        errdefer allocator.free(fixed_cursor);
        for (fixed_cursor) |*c| c.* = 0;

        const string_offsets = try allocator.alloc(std.ArrayListUnmanaged(u32), n_cols);
        errdefer allocator.free(string_offsets);
        for (string_offsets) |*b| b.* = .empty;

        const string_bytes = try allocator.alloc(std.ArrayListUnmanaged(u8), n_cols);
        errdefer allocator.free(string_bytes);
        for (string_bytes) |*b| b.* = .empty;

        const nulls = try allocator.alloc(std.ArrayListUnmanaged(u8), n_cols);
        errdefer allocator.free(nulls);
        for (nulls) |*b| b.* = .empty;

        const view_slice = try allocator.alloc(ColumnView, n_cols);
        errdefer allocator.free(view_slice);

        const bitmap_len = (row_count + 7) / 8;
        for (table_schema.columns, 0..) |c, i| {
            if (c.nullable) try nulls[i].appendNTimes(allocator, 0, bitmap_len);
            if (c.type.isString()) try string_offsets[i].append(allocator, 0);
        }
        return .{
            .allocator = allocator,
            .schema_copy = schema_copy,
            .fixed_slabs = fixed_slabs,
            .fixed_cursor = fixed_cursor,
            .string_offsets = string_offsets,
            .string_bytes = string_bytes,
            .nulls = nulls,
            .view_slice = view_slice,
            .row_count = row_count,
        };
    }

    fn deinit(self: *InsertColumnBuilder) void {
        for (self.fixed_slabs) |s| if (s.len > 0) self.allocator.free(s);
        for (self.string_offsets) |*b| b.deinit(self.allocator);
        for (self.string_bytes) |*b| b.deinit(self.allocator);
        for (self.nulls) |*b| b.deinit(self.allocator);
        self.allocator.free(self.fixed_slabs);
        self.allocator.free(self.fixed_cursor);
        self.allocator.free(self.string_offsets);
        self.allocator.free(self.string_bytes);
        self.allocator.free(self.nulls);
        self.allocator.free(self.schema_copy);
        self.allocator.free(self.view_slice);
    }

    fn schemaSlice(self: *InsertColumnBuilder) []const Column {
        return self.schema_copy;
    }

    fn appendCell(self: *InsertColumnBuilder, col_idx: usize, col: Column, maybe_val: ?Value) !void {
        const row_in_col = self.currentRow(col_idx, col);
        if (maybe_val == null) {
            try self.appendPlaceholder(col_idx, col);
            return;
        }
        if (col.nullable) {
            self.nulls[col_idx].items[row_in_col >> 3] |= @as(u8, 1) << @intCast(row_in_col & 7);
        }
        try self.appendCoerced(col_idx, col, maybe_val.?);
    }

    fn currentRow(self: *InsertColumnBuilder, col_idx: usize, col: Column) usize {
        return switch (col.type) {
            .int, .date, .float => self.fixed_cursor[col_idx] / 4,
            .bigint, .double, .datetime, .decimal64 => self.fixed_cursor[col_idx] / 8,
            .smallint => self.fixed_cursor[col_idx] / 2,
            .tinyint, .boolean => self.fixed_cursor[col_idx],
            .largeint, .decimal128, .uuid => self.fixed_cursor[col_idx] / 16,
            .varchar, .string, .char => self.string_offsets[col_idx].items.len - 1,
        };
    }

    fn appendPlaceholder(self: *InsertColumnBuilder, col_idx: usize, col: Column) !void {
        switch (col.type) {
            .int, .date, .float => self.writeFixedZero(col_idx, 4),
            .bigint, .double, .datetime, .decimal64 => self.writeFixedZero(col_idx, 8),
            .smallint => self.writeFixedZero(col_idx, 2),
            .tinyint, .boolean => self.writeFixedZero(col_idx, 1),
            .largeint, .decimal128, .uuid => self.writeFixedZero(col_idx, 16),
            .varchar, .string, .char => {
                const cur = self.string_offsets[col_idx].items[self.string_offsets[col_idx].items.len - 1];
                try self.string_offsets[col_idx].append(self.allocator, cur);
            },
        }
    }

    fn writeFixedZero(self: *InsertColumnBuilder, col_idx: usize, width: usize) void {
        const cursor = self.fixed_cursor[col_idx];
        @memset(self.fixed_slabs[col_idx][cursor .. cursor + width], 0);
        self.fixed_cursor[col_idx] = cursor + width;
    }

    fn writeFixedBytes(self: *InsertColumnBuilder, col_idx: usize, bytes: []const u8) void {
        const cursor = self.fixed_cursor[col_idx];
        @memcpy(self.fixed_slabs[col_idx][cursor .. cursor + bytes.len], bytes);
        self.fixed_cursor[col_idx] = cursor + bytes.len;
    }

    fn appendCoerced(self: *InsertColumnBuilder, col_idx: usize, col: Column, v: Value) !void {
        switch (col.type) {
            .int => self.writeFixedInt(col_idx, i32, switch (v) {
                .int => |x| x,
                else => return Error.CopyMalformedRow,
            }),
            .bigint => self.writeFixedInt(col_idx, i64, switch (v) {
                .bigint => |x| x,
                else => return Error.CopyMalformedRow,
            }),
            .smallint => self.writeFixedInt(col_idx, i16, switch (v) {
                .smallint => |x| x,
                else => return Error.CopyMalformedRow,
            }),
            .tinyint => self.writeFixedBytes(col_idx, &[_]u8{@as(u8, @bitCast(switch (v) {
                .tinyint => |x| x,
                else => return Error.CopyMalformedRow,
            }))}),
            .largeint => self.writeFixedInt(col_idx, i128, switch (v) {
                .largeint => |x| x,
                else => return Error.CopyMalformedRow,
            }),
            .boolean => self.writeFixedBytes(col_idx, &[_]u8{@intFromBool(switch (v) {
                .boolean => |x| x,
                else => return Error.CopyMalformedRow,
            })}),
            .float => self.writeFixedFloat(col_idx, f32, switch (v) {
                .float => |x| x,
                else => return Error.CopyMalformedRow,
            }),
            .double => self.writeFixedFloat(col_idx, f64, switch (v) {
                .double => |x| x,
                else => return Error.CopyMalformedRow,
            }),
            .date => {
                const s = switch (v) {
                    .text => |t| t,
                    else => return Error.CopyMalformedRow,
                };
                const d = parseDateLiteral(s) catch return Error.CopyMalformedRow;
                self.writeFixedInt(col_idx, i32, d);
            },
            .datetime => {
                const s = switch (v) {
                    .text => |t| t,
                    else => return Error.CopyMalformedRow,
                };
                const dt = parseDateTimeLiteral(s) catch return Error.CopyMalformedRow;
                self.writeFixedInt(col_idx, i64, dt);
            },
            .uuid => {
                const s = switch (v) {
                    .text => |t| t,
                    else => return Error.CopyMalformedRow,
                };
                const u = parseUuidLiteral(s) catch return Error.CopyMalformedRow;
                self.writeFixedInt(col_idx, u128, u);
            },
            .decimal64 => |spec| {
                const s = switch (v) {
                    .text => |t| t,
                    else => return Error.CopyMalformedRow,
                };
                const d = parseDecimalLiteral(i64, s, spec) catch return Error.CopyMalformedRow;
                self.writeFixedInt(col_idx, i64, d);
            },
            .decimal128 => |spec| {
                const s = switch (v) {
                    .text => |t| t,
                    else => return Error.CopyMalformedRow,
                };
                const d = parseDecimalLiteral(i128, s, spec) catch return Error.CopyMalformedRow;
                self.writeFixedInt(col_idx, i128, d);
            },
            .varchar, .string, .char => {
                const s = switch (v) {
                    .text => |t| t,
                    else => return Error.CopyMalformedRow,
                };
                const sb = &self.string_bytes[col_idx];
                try sb.appendSlice(self.allocator, s);
                try self.string_offsets[col_idx].append(self.allocator, @intCast(sb.items.len));
            },
        }
    }

    fn writeFixedInt(self: *InsertColumnBuilder, col_idx: usize, comptime T: type, v: T) void {
        var buf: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &buf, v, .little);
        self.writeFixedBytes(col_idx, &buf);
    }

    fn writeFixedFloat(self: *InsertColumnBuilder, col_idx: usize, comptime T: type, v: T) void {
        const Bits = if (T == f32) u32 else u64;
        var buf: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(Bits, &buf, @bitCast(v), .little);
        self.writeFixedBytes(col_idx, &buf);
    }

    fn views(self: *InsertColumnBuilder) []const ColumnView {
        for (self.schema_copy, 0..) |col, i| {
            const data: @import("../../storage/column.zig").ValueView = switch (col.type) {
                .int => .{ .int = std.mem.bytesAsSlice(i32, self.fixed_slabs[i])[0..self.row_count] },
                .bigint => .{ .bigint = std.mem.bytesAsSlice(i64, self.fixed_slabs[i])[0..self.row_count] },
                .smallint => .{ .smallint = std.mem.bytesAsSlice(i16, self.fixed_slabs[i])[0..self.row_count] },
                .tinyint => .{ .tinyint = std.mem.bytesAsSlice(i8, self.fixed_slabs[i])[0..self.row_count] },
                .largeint => .{ .largeint = std.mem.bytesAsSlice(i128, self.fixed_slabs[i])[0..self.row_count] },
                .boolean => .{ .boolean = self.fixed_slabs[i][0..self.row_count] },
                .float => .{ .float = std.mem.bytesAsSlice(f32, self.fixed_slabs[i])[0..self.row_count] },
                .double => .{ .double = std.mem.bytesAsSlice(f64, self.fixed_slabs[i])[0..self.row_count] },
                .date => .{ .date = std.mem.bytesAsSlice(i32, self.fixed_slabs[i])[0..self.row_count] },
                .datetime => .{ .datetime = std.mem.bytesAsSlice(i64, self.fixed_slabs[i])[0..self.row_count] },
                .decimal64 => .{ .decimal64 = std.mem.bytesAsSlice(i64, self.fixed_slabs[i])[0..self.row_count] },
                .decimal128 => .{ .decimal128 = std.mem.bytesAsSlice(i128, self.fixed_slabs[i])[0..self.row_count] },
                .uuid => .{ .uuid = std.mem.bytesAsSlice(u128, self.fixed_slabs[i])[0..self.row_count] },
                .varchar => .{ .varchar = .{
                    .offsets = self.string_offsets[i].items,
                    .bytes = self.string_bytes[i].items,
                } },
                .string => .{ .string = .{
                    .offsets = self.string_offsets[i].items,
                    .bytes = self.string_bytes[i].items,
                } },
                .char => .{ .char = .{
                    .offsets = self.string_offsets[i].items,
                    .bytes = self.string_bytes[i].items,
                } },
            };
            self.view_slice[i] = .{ .data = data, .nulls = if (col.nullable) self.nulls[i].items else null };
        }
        return self.view_slice;
    }
};

// ---------------------------------------------------------------------------
// Literal parsers (date / datetime / uuid / decimal)
//
// Inlined rather than re-exported from local.zig because that path is
// private. Behaviour matches the local.zig versions byte-for-byte.
// ---------------------------------------------------------------------------

fn parseDateLiteral(s: []const u8) !i32 {
    if (s.len < 10) return Error.CopyMalformedRow;
    if (s[4] != '-' or s[7] != '-') return Error.CopyMalformedRow;
    const year = parseIntField(i32, s[0..4]) catch return Error.CopyMalformedRow;
    const month = parseIntField(u32, s[5..7]) catch return Error.CopyMalformedRow;
    const day = parseIntField(u32, s[8..10]) catch return Error.CopyMalformedRow;
    if (month < 1 or month > 12 or day < 1 or day > 31) return Error.CopyMalformedRow;
    return civilToDays(year, month, day);
}

fn parseDateTimeLiteral(s: []const u8) !i64 {
    if (s.len < 19) return Error.CopyMalformedRow;
    if (s[4] != '-' or s[7] != '-') return Error.CopyMalformedRow;
    const sep = s[10];
    if (sep != ' ' and sep != 'T') return Error.CopyMalformedRow;
    if (s[13] != ':' or s[16] != ':') return Error.CopyMalformedRow;
    const year = parseIntField(i32, s[0..4]) catch return Error.CopyMalformedRow;
    const month = parseIntField(u32, s[5..7]) catch return Error.CopyMalformedRow;
    const day = parseIntField(u32, s[8..10]) catch return Error.CopyMalformedRow;
    const hour = parseIntField(u32, s[11..13]) catch return Error.CopyMalformedRow;
    const minute = parseIntField(u32, s[14..16]) catch return Error.CopyMalformedRow;
    const second = parseIntField(u32, s[17..19]) catch return Error.CopyMalformedRow;
    if (hour > 23 or minute > 59 or second > 59) return Error.CopyMalformedRow;
    var micros: u64 = 0;
    if (s.len > 19) {
        if (s[19] != '.') return Error.CopyMalformedRow;
        var idx: usize = 20;
        var digits: usize = 0;
        while (idx < s.len and digits < 6 and s[idx] >= '0' and s[idx] <= '9') : (idx += 1) {
            micros = micros * 10 + (s[idx] - '0');
            digits += 1;
        }
        while (digits < 6) : (digits += 1) micros *= 10;
        if (idx != s.len) return Error.CopyMalformedRow;
    }
    const days = try civilToDays(year, month, day);
    const day_secs: i64 = @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second);
    return @as(i64, days) * 86_400 * 1_000_000 + day_secs * 1_000_000 + @as(i64, @intCast(micros));
}

fn parseUuidLiteral(s: []const u8) !u128 {
    if (s.len != 36) return Error.CopyMalformedRow;
    if (s[8] != '-' or s[13] != '-' or s[18] != '-' or s[23] != '-') return Error.CopyMalformedRow;
    var out: u128 = 0;
    var nibbles: usize = 0;
    for (s) |ch| {
        if (ch == '-') continue;
        const nib: u128 = switch (ch) {
            '0'...'9' => ch - '0',
            'a'...'f' => ch - 'a' + 10,
            'A'...'F' => ch - 'A' + 10,
            else => return Error.CopyMalformedRow,
        };
        out = (out << 4) | nib;
        nibbles += 1;
    }
    if (nibbles != 32) return Error.CopyMalformedRow;
    return out;
}

fn parseDecimalLiteral(comptime T: type, s: []const u8, spec: types.DecimalSpec) !T {
    var idx: usize = 0;
    var negate = false;
    if (idx < s.len and (s[idx] == '-' or s[idx] == '+')) {
        negate = s[idx] == '-';
        idx += 1;
    }
    var int_part: T = 0;
    while (idx < s.len and s[idx] >= '0' and s[idx] <= '9') : (idx += 1) {
        int_part = std.math.mul(T, int_part, 10) catch return Error.CopyMalformedRow;
        int_part = std.math.add(T, int_part, @as(T, s[idx] - '0')) catch return Error.CopyMalformedRow;
    }
    var frac_digits: u8 = 0;
    var frac_part: T = 0;
    if (idx < s.len and s[idx] == '.') {
        idx += 1;
        while (idx < s.len and s[idx] >= '0' and s[idx] <= '9' and frac_digits < spec.s) : (idx += 1) {
            frac_part = std.math.mul(T, frac_part, 10) catch return Error.CopyMalformedRow;
            frac_part = std.math.add(T, frac_part, @as(T, s[idx] - '0')) catch return Error.CopyMalformedRow;
            frac_digits += 1;
        }
        while (idx < s.len and s[idx] >= '0' and s[idx] <= '9') : (idx += 1) {}
    }
    if (idx != s.len) return Error.CopyMalformedRow;
    while (frac_digits < spec.s) : (frac_digits += 1) {
        frac_part = std.math.mul(T, frac_part, 10) catch return Error.CopyMalformedRow;
    }
    var scaled = std.math.mul(T, int_part, std.math.powi(T, 10, spec.s) catch return Error.CopyMalformedRow) catch return Error.CopyMalformedRow;
    scaled = std.math.add(T, scaled, frac_part) catch return Error.CopyMalformedRow;
    if (negate) scaled = -scaled;
    return scaled;
}

fn parseIntField(comptime T: type, s: []const u8) !T {
    return std.fmt.parseInt(T, s, 10) catch return Error.CopyMalformedRow;
}

fn civilToDays(year: i32, month: u32, day: u32) !i32 {
    if (month == 0 or day == 0) return Error.CopyMalformedRow;
    const y = if (month <= 2) year - 1 else year;
    const era = if (y >= 0) @divFloor(y, 400) else @divFloor(y - 399, 400);
    const yoe: u32 = @intCast(y - era * 400);
    const m: u32 = if (month > 2) month - 3 else month + 9;
    const doy: u32 = (153 * m + 2) / 5 + day - 1;
    const doe: u32 = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return @as(i32, era) * 146_097 + @as(i32, @intCast(doe)) - 719_468;
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
