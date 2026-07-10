//! MySQL prepared-statement (COM_STMT_*) wire-format support.
//!
//! v1 strategy: rewrite SQL text. PREPARE counts `?` placeholders by
//! lexing the original statement; EXECUTE substitutes each bound
//! parameter as a SQL literal in placeholder order and re-parses the
//! result. The IR / executor stay placeholder-unaware — only the wire
//! layer touches placeholders. This keeps the change surface tiny and
//! avoids spreading a placeholder concept through `Value`, predicate
//! validation, and every operator's switch table.
//!
//! Trade-off vs. an IR-level placeholder node: the SQL is re-parsed on
//! every EXECUTE rather than once at PREPARE. A real workload that
//! cares can layer a parse cache later; for the v1 use case (driver
//! reuse, single-digit-`?` queries) the overhead is in the noise.

const std = @import("std");
const Allocator = std.mem.Allocator;

const packet = @import("packet.zig");
const result_mod = @import("result.zig");

const lexer_mod = @import("../../sql/lexer.zig");
const types = @import("../../types.zig");
const Column = types.Column;
const wire_format = @import("../wire_format.zig");
const sql_text = @import("../sql_text.zig");

/// Single per-connection prepared-statement entry. Strings stored
/// directly on the stmt outlive the SQL frame's payload buffer (which
/// the server frees at the end of COM_STMT_PREPARE handling).
pub const PreparedStmt = struct {
    id: u32,
    sql: []u8,
    num_params: u16,
    num_columns: u16,
    /// Output schema captured at PREPARE time by compiling against a
    /// dummy parameter binding. Null when prepare-time schema inference
    /// failed (e.g., placeholder in a string-column predicate); the
    /// EXECUTE path still emits real schema from the actual run, so the
    /// degradation is purely cosmetic.
    column_schema: ?[]Column,
    /// Owned copies of column-name bytes for `column_schema`. Tracked
    /// separately so deinit can free them without keeping a parallel
    /// `[]u8` for each column.
    column_names: ?[][]u8,
    /// Param-slot type bytes captured on the first EXECUTE with
    /// `new_params_bound_flag = 1`. Reused on subsequent EXECUTEs that
    /// pass `new_params_bound_flag = 0`. Null until the first bind.
    param_types: ?[]u8 = null,
    /// Per-param "unsigned" flags captured alongside `param_types`.
    /// Matches `MYSQL_TYPE_*` flag byte (high bit 0x80 = unsigned).
    param_flags: ?[]u8 = null,
    /// COM_STMT_SEND_LONG_DATA buffers. Slot `i` holds accumulated bytes
    /// for parameter `i`; null = no long data buffered. We only support
    /// VAR_STRING / STRING / BLOB-like long data in v1.
    long_data: []?std.ArrayList(u8),
    allocator: Allocator,

    pub fn deinit(self: *PreparedStmt) void {
        self.allocator.free(self.sql);
        if (self.column_schema) |s| self.allocator.free(s);
        if (self.column_names) |names| {
            for (names) |n| self.allocator.free(n);
            self.allocator.free(names);
        }
        if (self.param_types) |pt| self.allocator.free(pt);
        if (self.param_flags) |pf| self.allocator.free(pf);
        for (self.long_data) |*ld| {
            if (ld.*) |*buf| buf.deinit(self.allocator);
        }
        self.allocator.free(self.long_data);
        self.allocator.destroy(self);
    }
};

/// Count `?` placeholders in `sql` while skipping single-quoted strings,
/// backtick-quoted identifiers, and -- / /* */ comments. Uses the
/// existing lexer so escape rules stay consistent with the parser.
pub fn countPlaceholders(arena: Allocator, sql: []const u8) !u16 {
    var lex = lexer_mod.Lexer.init(arena, sql);
    lex.dialect = .mysql;
    var count: u16 = 0;
    while (true) {
        const tok = lex.next() catch return count;
        if (tok.tag == .eof) break;
        if (tok.tag == .question) count += 1;
    }
    return count;
}

/// Allocate + initialize a PreparedStmt. Copies the SQL bytes into the
/// stmt's own allocator (caller-owned input frame is freed immediately
/// after this returns).
pub fn createPreparedStmt(
    allocator: Allocator,
    id: u32,
    sql: []const u8,
    num_params: u16,
) !*PreparedStmt {
    const sql_copy = try allocator.dupe(u8, sql);
    errdefer allocator.free(sql_copy);

    const long_data = try allocator.alloc(?std.ArrayList(u8), num_params);
    errdefer allocator.free(long_data);
    for (long_data) |*ld| ld.* = null;

    const stmt = try allocator.create(PreparedStmt);
    stmt.* = .{
        .id = id,
        .sql = sql_copy,
        .num_params = num_params,
        .num_columns = 0,
        .column_schema = null,
        .column_names = null,
        .long_data = long_data,
        .allocator = allocator,
    };
    return stmt;
}

/// Walk the original SQL substituting `?` positions with rendered
/// literal text. `params` is a list of optional rendered-literal strings
/// (null = SQL NULL). Returns an allocator-owned slice the caller frees.
pub fn substituteSql(
    allocator: Allocator,
    sql: []const u8,
    params: []const ?[]const u8,
) ![]u8 {
    return sql_text.substituteQuestionPlaceholders(allocator, sql, params);
}

/// Convenience: render a substituted SQL with all placeholders bound to
/// `0` (numeric default). Used by PREPARE to derive output column
/// metadata. The result is freed by the caller.
pub fn renderDummySubstitution(allocator: Allocator, sql: []const u8, n: u16) ![]u8 {
    const buf = try allocator.alloc(?[]const u8, n);
    defer allocator.free(buf);
    for (buf) |*slot| slot.* = "0";
    return try substituteSql(allocator, sql, buf);
}

// ---------------------------------------------------------------------------
// Wire layer — request decode + response emit
// ---------------------------------------------------------------------------

/// Bound parameter value rendered as SQL-literal text. Allocated into
/// the EXECUTE-scoped arena.
pub const BoundParam = struct {
    /// Null = SQL NULL.
    literal: ?[]const u8,
};

/// Decoded type byte + flag byte (high bit = unsigned).
const ParamMeta = struct {
    type_byte: u8,
    unsigned: bool,
};

/// COM_STMT_EXECUTE request layout (after the command byte). All offsets
/// are into the COM_STMT_EXECUTE payload AFTER stripping the 0x17 byte.
const ExecuteHeader = struct {
    stmt_id: u32,
    flags: u8,
    iteration_count: u32,
};

fn readExecuteHeader(body: []const u8, cursor: *usize) !ExecuteHeader {
    if (body.len < 9) return error.MalformedExecute;
    const stmt_id = std.mem.readInt(u32, body[0..4], .little);
    const flags = body[4];
    const iteration_count = std.mem.readInt(u32, body[5..9], .little);
    cursor.* = 9;
    return .{ .stmt_id = stmt_id, .flags = flags, .iteration_count = iteration_count };
}

/// Decode bound parameter values out of an EXECUTE payload. Allocates
/// each rendered literal into `arena`. The returned slice has length
/// equal to `stmt.num_params`; each entry is either a literal string
/// or null (SQL NULL). On the first execute with new_params_bound_flag
/// set, the stmt's param_types/param_flags are updated from the wire.
pub fn decodeExecuteParams(
    arena: Allocator,
    stmt: *PreparedStmt,
    body: []const u8,
    after_header: usize,
) ![]const ?[]const u8 {
    if (stmt.num_params == 0) return &[_]?[]const u8{};

    var cursor = after_header;
    const nullmap_bytes = (@as(usize, stmt.num_params) + 7) / 8;
    if (cursor + nullmap_bytes + 1 > body.len) return error.MalformedExecute;
    const nullmap = body[cursor .. cursor + nullmap_bytes];
    cursor += nullmap_bytes;

    const new_params_bound_flag = body[cursor];
    cursor += 1;

    if (new_params_bound_flag == 1) {
        const param_bytes = @as(usize, stmt.num_params) * 2;
        if (cursor + param_bytes > body.len) return error.MalformedExecute;
        if (stmt.param_types) |pt| stmt.allocator.free(pt);
        if (stmt.param_flags) |pf| stmt.allocator.free(pf);
        const pt = try stmt.allocator.alloc(u8, stmt.num_params);
        const pf = try stmt.allocator.alloc(u8, stmt.num_params);
        var pi: usize = 0;
        while (pi < stmt.num_params) : (pi += 1) {
            pt[pi] = body[cursor + 2 * pi];
            pf[pi] = body[cursor + 2 * pi + 1];
        }
        stmt.param_types = pt;
        stmt.param_flags = pf;
        cursor += param_bytes;
    } else {
        if (stmt.param_types == null) return error.NoBoundParamTypes;
    }

    const types_slice = stmt.param_types orelse return error.NoBoundParamTypes;
    const flags_slice = stmt.param_flags.?;

    const out = try arena.alloc(?[]const u8, stmt.num_params);

    var i: usize = 0;
    while (i < stmt.num_params) : (i += 1) {
        const is_null = (nullmap[i / 8] >> @as(u3, @intCast(i % 8))) & 1 != 0;
        if (is_null) {
            // If the client sent long-data for this slot AND marked
            // NULL, that's the legal "use the long-data buffer" signal.
            if (i < stmt.long_data.len) {
                if (stmt.long_data[i]) |ld| {
                    out[i] = try renderLongData(arena, ld.items);
                    continue;
                }
            }
            out[i] = null;
            continue;
        }
        if (i < stmt.long_data.len) {
            if (stmt.long_data[i]) |_| return error.LongDataAndValueBoth;
        }
        const meta: ParamMeta = .{
            .type_byte = types_slice[i],
            .unsigned = (flags_slice[i] & 0x80) != 0,
        };
        out[i] = try decodeBinaryValue(arena, body, &cursor, meta);
    }
    return out;
}

/// Render `bytes` (accumulated COM_STMT_SEND_LONG_DATA buffer) as a
/// SQL string literal. We treat long data as text/varchar — binary
/// blobs are out of scope for v1.
fn renderLongData(arena: Allocator, bytes: []const u8) ![]const u8 {
    return try sql_text.renderStringLiteral(arena, bytes);
}

/// Decode one binary parameter value at `body[cursor.*]`. Advances
/// the cursor. Returns the SQL-literal text for substitution.
fn decodeBinaryValue(
    arena: Allocator,
    body: []const u8,
    cursor: *usize,
    meta: ParamMeta,
) ![]const u8 {
    return switch (meta.type_byte) {
        result_mod.MYSQL_TYPE_TINY => blk: {
            if (cursor.* + 1 > body.len) return error.MalformedExecute;
            const raw: u8 = body[cursor.*];
            cursor.* += 1;
            if (meta.unsigned) break :blk try std.fmt.allocPrint(arena, "{d}", .{raw});
            const signed: i8 = @bitCast(raw);
            break :blk try std.fmt.allocPrint(arena, "{d}", .{signed});
        },
        result_mod.MYSQL_TYPE_SHORT => blk: {
            if (cursor.* + 2 > body.len) return error.MalformedExecute;
            const raw = std.mem.readInt(u16, body[cursor.*..][0..2], .little);
            cursor.* += 2;
            if (meta.unsigned) break :blk try std.fmt.allocPrint(arena, "{d}", .{raw});
            const signed: i16 = @bitCast(raw);
            break :blk try std.fmt.allocPrint(arena, "{d}", .{signed});
        },
        result_mod.MYSQL_TYPE_LONG => blk: {
            if (cursor.* + 4 > body.len) return error.MalformedExecute;
            const raw = std.mem.readInt(u32, body[cursor.*..][0..4], .little);
            cursor.* += 4;
            if (meta.unsigned) break :blk try std.fmt.allocPrint(arena, "{d}", .{raw});
            const signed: i32 = @bitCast(raw);
            break :blk try std.fmt.allocPrint(arena, "{d}", .{signed});
        },
        result_mod.MYSQL_TYPE_LONGLONG => blk: {
            if (cursor.* + 8 > body.len) return error.MalformedExecute;
            const raw = std.mem.readInt(u64, body[cursor.*..][0..8], .little);
            cursor.* += 8;
            if (meta.unsigned) break :blk try std.fmt.allocPrint(arena, "{d}", .{raw});
            const signed: i64 = @bitCast(raw);
            break :blk try std.fmt.allocPrint(arena, "{d}", .{signed});
        },
        result_mod.MYSQL_TYPE_FLOAT => blk: {
            if (cursor.* + 4 > body.len) return error.MalformedExecute;
            const raw = std.mem.readInt(u32, body[cursor.*..][0..4], .little);
            cursor.* += 4;
            const v: f32 = @bitCast(raw);
            break :blk try std.fmt.allocPrint(arena, "{d}", .{v});
        },
        result_mod.MYSQL_TYPE_DOUBLE => blk: {
            if (cursor.* + 8 > body.len) return error.MalformedExecute;
            const raw = std.mem.readInt(u64, body[cursor.*..][0..8], .little);
            cursor.* += 8;
            const v: f64 = @bitCast(raw);
            break :blk try std.fmt.allocPrint(arena, "{d}", .{v});
        },
        result_mod.MYSQL_TYPE_VAR_STRING,
        result_mod.MYSQL_TYPE_STRING,
        result_mod.MYSQL_TYPE_NEWDECIMAL,
        => blk: {
            const s = try packet.readLenEncString(body, cursor);
            break :blk try sql_text.renderStringLiteral(arena, s);
        },
        result_mod.MYSQL_TYPE_DATE,
        result_mod.MYSQL_TYPE_DATETIME,
        result_mod.MYSQL_TYPE_TIMESTAMP,
        => try decodeBinaryTemporal(arena, body, cursor),
        result_mod.MYSQL_TYPE_NULL => "NULL",
        else => return error.UnsupportedParamType,
    };
}

/// Decode a MySQL binary DATE / DATETIME / TIMESTAMP payload (length-
/// prefixed) and render as a quoted temporal literal ('YYYY-MM-DD' /
/// 'YYYY-MM-DD HH:MM:SS[.ffffff]') — exactly what a text-protocol client
/// would send, so the value coerces into date/datetime AND string columns
/// alike. The old raw-integer form (µs / days) fails type-checking against
/// native datetime columns.
fn decodeBinaryTemporal(arena: Allocator, body: []const u8, cursor: *usize) ![]const u8 {
    if (cursor.* + 1 > body.len) return error.MalformedExecute;
    const len = body[cursor.*];
    cursor.* += 1;
    if (cursor.* + len > body.len) return error.MalformedExecute;
    if (len == 0) return "NULL";
    if (len < 4) return error.MalformedExecute;
    const year = std.mem.readInt(u16, body[cursor.*..][0..2], .little);
    const month = body[cursor.* + 2];
    const day = body[cursor.* + 3];
    var hour: u8 = 0;
    var minute: u8 = 0;
    var second: u8 = 0;
    var micros: u32 = 0;
    if (len >= 7) {
        hour = body[cursor.* + 4];
        minute = body[cursor.* + 5];
        second = body[cursor.* + 6];
    }
    if (len >= 11) {
        micros = std.mem.readInt(u32, body[cursor.*..][7..11], .little);
    }
    cursor.* += len;

    const days_since_epoch: i64 = wire_format.daysFromCivil(@intCast(year), month, day);
    if (len == 4) {
        var buf: [16]u8 = undefined;
        const txt = try wire_format.formatDate(&buf, @intCast(days_since_epoch));
        return try std.fmt.allocPrint(arena, "'{s}'", .{txt});
    }
    const seconds_of_day: i64 = (@as(i64, hour) * 3600) + (@as(i64, minute) * 60) + @as(i64, second);
    const total_micros: i64 = days_since_epoch * 86_400_000_000 + seconds_of_day * 1_000_000 + @as(i64, micros);
    var buf: [40]u8 = undefined;
    const txt = try wire_format.formatDateTime(&buf, total_micros);
    return try std.fmt.allocPrint(arena, "'{s}'", .{txt});
}

// ---------------------------------------------------------------------------
// Response builders
// ---------------------------------------------------------------------------

/// Build the COM_STMT_PREPARE_OK header packet.
pub fn sendPrepareOkHeader(
    allocator: Allocator,
    w: *std.Io.Writer,
    seq_id: *u8,
    stmt_id: u32,
    num_columns: u16,
    num_params: u16,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);

    try payload.append(allocator, 0x00);
    var buf4: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf4, stmt_id, .little);
    try payload.appendSlice(allocator, &buf4);
    var buf2: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf2, num_columns, .little);
    try payload.appendSlice(allocator, &buf2);
    std.mem.writeInt(u16, &buf2, num_params, .little);
    try payload.appendSlice(allocator, &buf2);
    try payload.append(allocator, 0x00);
    std.mem.writeInt(u16, &buf2, 0, .little);
    try payload.appendSlice(allocator, &buf2);

    try packet.writePacket(w, seq_id.*, payload.items);
    seq_id.* +%= 1;
}

/// Emit a placeholder ColumnDef41 (used for the param-meta section of
/// COM_STMT_PREPARE_OK). Empty name + MYSQL_TYPE_VAR_STRING matches the
/// upstream server's behavior — clients ignore the metadata and rebind
/// types at EXECUTE.
pub fn sendParamColumnDef(allocator: Allocator, w: *std.Io.Writer, seq_id: *u8) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);

    try packet.appendLenEncString(allocator, &payload, "def");
    try packet.appendLenEncString(allocator, &payload, "");
    try packet.appendLenEncString(allocator, &payload, "");
    try packet.appendLenEncString(allocator, &payload, "");
    try packet.appendLenEncString(allocator, &payload, "?");
    try packet.appendLenEncString(allocator, &payload, "");
    try packet.appendLenEncInt(allocator, &payload, 0x0c);

    var buf2: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf2, 0x3f, .little); // CHARSET_BINARY
    try payload.appendSlice(allocator, &buf2);

    var buf4: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf4, 0, .little);
    try payload.appendSlice(allocator, &buf4);

    try payload.append(allocator, result_mod.MYSQL_TYPE_VAR_STRING);

    std.mem.writeInt(u16, &buf2, 0x80, .little); // BINARY_FLAG | (NUM_FLAG cleared)
    try payload.appendSlice(allocator, &buf2);

    try payload.append(allocator, 0);
    try payload.append(allocator, 0);
    try payload.append(allocator, 0);

    try packet.writePacket(w, seq_id.*, payload.items);
    seq_id.* +%= 1;
}

// ---------------------------------------------------------------------------
// Binary row encoding
// ---------------------------------------------------------------------------

/// Emit a single binary-protocol result row. `schema` and `view_for_col(i)`
/// give the per-column type + accessor. The NULL bitmap reserves bits
/// 0 and 1 (offset = 2), per the MySQL binary protocol.
pub fn appendBinaryRow(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    schema: []const Column,
    values: anytype,
    row: usize,
) !void {
    try out.append(allocator, 0x00);

    const nullmap_bytes = (schema.len + 7 + 2) / 8;
    const nullmap_start = out.items.len;
    var nm_i: usize = 0;
    while (nm_i < nullmap_bytes) : (nm_i += 1) try out.append(allocator, 0);

    for (schema, 0..) |col, ci| {
        const view = values[ci];
        if (!view.isValid(row)) {
            const bit: usize = ci + 2;
            out.items[nullmap_start + bit / 8] |= @as(u8, 1) << @as(u3, @intCast(bit % 8));
            continue;
        }
        try appendBinaryCell(allocator, out, col, view, row);
    }
}

fn appendBinaryCell(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    col: Column,
    view: anytype,
    row: usize,
) !void {
    var b: [16]u8 = undefined;
    const data = view.data;
    switch (data) {
        .tinyint => |s| try out.append(allocator, @bitCast(s[row])),
        .smallint => |s| {
            std.mem.writeInt(i16, b[0..2], s[row], .little);
            try out.appendSlice(allocator, b[0..2]);
        },
        .int => |s| {
            std.mem.writeInt(i32, b[0..4], s[row], .little);
            try out.appendSlice(allocator, b[0..4]);
        },
        .bigint => |s| {
            std.mem.writeInt(i64, b[0..8], s[row], .little);
            try out.appendSlice(allocator, b[0..8]);
        },
        .largeint => |s| {
            // i128 doesn't have a stable MySQL binary type. Render as
            // ASCII via NEWDECIMAL-style lenenc string so the client
            // sees a meaningful value.
            var buf: [48]u8 = undefined;
            const text = try std.fmt.bufPrint(&buf, "{d}", .{s[row]});
            try packet.appendLenEncString(allocator, out, text);
        },
        .boolean => |s| try out.append(allocator, s[row]),
        .float => |s| {
            const bits: u32 = @bitCast(s[row]);
            std.mem.writeInt(u32, b[0..4], bits, .little);
            try out.appendSlice(allocator, b[0..4]);
        },
        .double => |s| {
            const bits: u64 = @bitCast(s[row]);
            std.mem.writeInt(u64, b[0..8], bits, .little);
            try out.appendSlice(allocator, b[0..8]);
        },
        .date => |s| {
            const ymd = wire_format.civilFromDays(@intCast(s[row]));
            try out.append(allocator, 4);
            std.mem.writeInt(u16, b[0..2], @intCast(ymd.y), .little);
            try out.appendSlice(allocator, b[0..2]);
            try out.append(allocator, @intCast(ymd.m));
            try out.append(allocator, @intCast(ymd.d));
        },
        .datetime => |s| try appendBinaryDateTime(allocator, out, s[row]),
        .decimal64 => |s| {
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(allocator);
            try wire_format.formatDecimal(allocator, &buf, @as(i128, s[row]), col.type);
            try packet.appendLenEncString(allocator, out, buf.items);
        },
        .decimal128 => |s| {
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(allocator);
            try wire_format.formatDecimal(allocator, &buf, s[row], col.type);
            try packet.appendLenEncString(allocator, out, buf.items);
        },
        .uuid => |s| {
            var buf: [40]u8 = undefined;
            const text = try wire_format.formatUuid(&buf, s[row]);
            try packet.appendLenEncString(allocator, out, text);
        },
        .varchar, .char, .string, .json => |sv| try packet.appendLenEncString(allocator, out, sv.rowBytes(row)),
    }
}

fn appendBinaryDateTime(allocator: Allocator, out: *std.ArrayList(u8), micros: i64) !void {
    const sec = @divFloor(micros, 1_000_000);
    var us = @rem(micros, 1_000_000);
    var s = sec;
    if (us < 0) {
        us += 1_000_000;
        s -= 1;
    }
    const day = @divFloor(s, 86_400);
    var tod = @rem(s, 86_400);
    if (tod < 0) tod += 86_400;
    const ymd = wire_format.civilFromDays(@intCast(day));
    const hours: u8 = @intCast(@divFloor(tod, 3600));
    const minutes: u8 = @intCast(@divFloor(@rem(tod, 3600), 60));
    const seconds: u8 = @intCast(@rem(tod, 60));
    const has_micros = us != 0;
    try out.append(allocator, if (has_micros) 11 else 7);
    var b2: [2]u8 = undefined;
    std.mem.writeInt(u16, &b2, @intCast(ymd.y), .little);
    try out.appendSlice(allocator, &b2);
    try out.append(allocator, @intCast(ymd.m));
    try out.append(allocator, @intCast(ymd.d));
    try out.append(allocator, hours);
    try out.append(allocator, minutes);
    try out.append(allocator, seconds);
    if (has_micros) {
        var b4: [4]u8 = undefined;
        std.mem.writeInt(u32, &b4, @intCast(us), .little);
        try out.appendSlice(allocator, &b4);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "countPlaceholders skips strings + comments" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const sql = "SELECT * FROM t WHERE a = ? AND b = '?' AND c = ? -- ?\nAND d = ?";
    const n = try countPlaceholders(arena.allocator(), sql);
    try std.testing.expectEqual(@as(u16, 3), n);
}

test "countPlaceholders zero on no placeholders" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const n = try countPlaceholders(arena.allocator(), "SELECT 1");
    try std.testing.expectEqual(@as(u16, 0), n);
}

test "substituteSql replaces in order, preserves strings + comments" {
    const allocator = std.testing.allocator;
    const params = [_]?[]const u8{ "42", "'hello''world'", null };
    const out = try substituteSql(
        allocator,
        "SELECT * FROM t WHERE a = ? AND b = ? AND c = '?' AND d = ?",
        params[0..],
    );
    defer allocator.free(out);
    try std.testing.expectEqualStrings(
        "SELECT * FROM t WHERE a = 42 AND b = 'hello''world' AND c = '?' AND d = NULL",
        out,
    );
}
