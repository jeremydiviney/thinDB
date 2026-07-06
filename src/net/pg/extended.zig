//! PostgreSQL Extended Query protocol — Parse/Bind/Describe/Execute/
//! Close/Sync/Flush, plus portal lifecycle.
//!
//! v1 strategy mirrors the MySQL prepared-statement work: rewrite SQL
//! text. Parse stores the SQL with `$N` placeholders intact; Bind
//! decodes each parameter (binary or text) into a SQL literal, splices
//! the result into a portal's `bound_sql`; Execute re-parses + compiles
//! that string and streams it through the existing exec pipeline. The
//! IR / executor stay placeholder-unaware.
//!
//! Trade-off vs. an IR-level placeholder node: the SQL is re-parsed on
//! every Execute. A real workload can layer a parse cache later; for
//! the v1 use case (driver default Extended Query for `WHERE id = $1`)
//! the overhead is in the noise.

const std = @import("std");
const Allocator = std.mem.Allocator;

const packet = @import("packet.zig");
const wire_format = @import("../wire_format.zig");
const sql_text = @import("../sql_text.zig");

const lexer_mod = @import("../../sql/lexer.zig");
const sql_mod = @import("../../sql/sql.zig");
const ir = @import("../../ir/ir.zig");
const local = @import("../local.zig");
const types = @import("../../types.zig");
const Column = types.Column;

const thindb_api = @import("../../api/api.zig");
const Catalog = thindb_api.Catalog;
const Session = thindb_api.Session;

pub const Error = error{
    UnknownStatement,
    UnknownPortal,
    BindParamCountMismatch,
    UnsupportedParamFormat,
    UnsupportedParamOid,
    MalformedBindParam,
    InvalidDescribeTarget,
    InvalidCloseTarget,
};

/// PostgreSQL type OIDs we recognize on the wire. Mirrors the constants
/// in `result.zig` but exposed here for binary-decode dispatch.
pub const OID_BOOL: u32 = 16;
pub const OID_INT8: u32 = 20;
pub const OID_INT2: u32 = 21;
pub const OID_INT4: u32 = 23;
pub const OID_TEXT: u32 = 25;
pub const OID_FLOAT4: u32 = 700;
pub const OID_FLOAT8: u32 = 701;
pub const OID_NUMERIC: u32 = 1700;
pub const OID_DATE: u32 = 1082;
pub const OID_TIMESTAMP: u32 = 1114;
pub const OID_UUID: u32 = 2950;

/// PG date epoch is 2000-01-01 (unix days 10957). Binary date OID 1082
/// transmits days relative to this epoch; thinDB internally uses unix
/// days, so we shift on decode.
const PG_DATE_EPOCH_UNIX_DAYS: i32 = 10957;
const PG_TIMESTAMP_EPOCH_UNIX_MICROS: i64 = 10957 * @as(i64, 86_400) * 1_000_000;

/// One server-side prepared statement. Lifetime: until explicit
/// `Close('S', name)` or end-of-session. Statements own their SQL
/// bytes; portals reference back via pointer.
pub const PreparedStmt = struct {
    name: []u8,
    sql: []u8,
    num_params: u32,
    /// Type-OID hints from the client's Parse. Length == num_params.
    /// Each entry is the wire OID or `0` ("server, you decide").
    param_oids: []u32,
    /// Cached SELECT output schema captured by a dry-compile with
    /// each `$N` substituted as a string literal. `null` when the SQL
    /// is a side-effect statement (INSERT/CREATE/etc.) or when the dry
    /// compile failed; Describe-statement reports NoData in that case.
    column_schema: ?[]Column = null,
    /// Owned copies of column-name bytes for `column_schema`.
    column_names: ?[][]u8 = null,
    arena: std.heap.ArenaAllocator,

    pub fn deinit(self: *PreparedStmt, gpa: Allocator) void {
        self.arena.deinit();
        gpa.free(self.name);
        gpa.free(self.sql);
        gpa.free(self.param_oids);
        if (self.column_schema) |s| gpa.free(s);
        if (self.column_names) |names| {
            for (names) |n| gpa.free(n);
            gpa.free(names);
        }
    }
};

/// One Bind product: a portal naming a statement + the rendered SQL
/// with placeholders replaced + the result-column format codes.
pub const Portal = struct {
    name: []u8,
    stmt: *PreparedStmt,
    bound_sql: []u8,
    /// Format code per result column (0 = text, 1 = binary). If the
    /// client sent 0 codes, this is empty and we default to text. If
    /// it sent 1 code, this is length-1 and applies to every column.
    /// Otherwise length must equal the statement's column count, but
    /// we don't enforce that until Execute (the column count may be
    /// unknown until the bound SQL is compiled).
    result_formats: []u16,

    pub fn deinit(self: *Portal, gpa: Allocator) void {
        gpa.free(self.name);
        gpa.free(self.bound_sql);
        gpa.free(self.result_formats);
    }
};

/// Server-side state container the message loop hangs off of.
pub const ExtendedState = struct {
    statements: std.StringHashMapUnmanaged(*PreparedStmt) = .empty,
    portals: std.StringHashMapUnmanaged(*Portal) = .empty,
    /// Set to true when a frame in an Extended sequence fails; cleared
    /// by Sync. While set, the dispatcher drops every frame except
    /// Sync / Terminate.
    in_error_until_sync: bool = false,

    pub fn deinit(self: *ExtendedState, gpa: Allocator) void {
        var sit = self.statements.iterator();
        while (sit.next()) |entry| {
            gpa.free(entry.key_ptr.*);
            entry.value_ptr.*.deinit(gpa);
            gpa.destroy(entry.value_ptr.*);
        }
        self.statements.deinit(gpa);

        var pit = self.portals.iterator();
        while (pit.next()) |entry| {
            gpa.free(entry.key_ptr.*);
            entry.value_ptr.*.deinit(gpa);
            gpa.destroy(entry.value_ptr.*);
        }
        self.portals.deinit(gpa);
    }

    pub fn dropStatement(self: *ExtendedState, gpa: Allocator, name: []const u8) void {
        const kv = self.statements.fetchRemove(name) orelse return;
        gpa.free(kv.key);
        kv.value.deinit(gpa);
        gpa.destroy(kv.value);
    }

    pub fn dropPortal(self: *ExtendedState, gpa: Allocator, name: []const u8) void {
        const kv = self.portals.fetchRemove(name) orelse return;
        gpa.free(kv.key);
        kv.value.deinit(gpa);
        gpa.destroy(kv.value);
    }
};

// ---------------------------------------------------------------------------
// Frame senders
// ---------------------------------------------------------------------------

pub fn sendParseComplete(w: *std.Io.Writer) !void {
    try packet.writeFrame(w, '1', "");
}

pub fn sendBindComplete(w: *std.Io.Writer) !void {
    try packet.writeFrame(w, '2', "");
}

pub fn sendCloseComplete(w: *std.Io.Writer) !void {
    try packet.writeFrame(w, '3', "");
}

pub fn sendNoData(w: *std.Io.Writer) !void {
    try packet.writeFrame(w, 'n', "");
}

pub fn sendPortalSuspended(w: *std.Io.Writer) !void {
    try packet.writeFrame(w, 's', "");
}

pub fn sendParameterDescription(
    allocator: Allocator,
    w: *std.Io.Writer,
    oids: []const u32,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    try packet.appendI16(allocator, &payload, @intCast(oids.len));
    for (oids) |oid| try packet.appendU32(allocator, &payload, oid);
    try packet.writeFrame(w, 't', payload.items);
}

// ---------------------------------------------------------------------------
// Placeholder substitution
// ---------------------------------------------------------------------------

/// Walk `sql` substituting each `$N` outside string/identifier/comment
/// context with `params[N-1]` (or `NULL` when the entry is null).
/// Returns an allocator-owned slice the caller frees.
pub fn substituteDollarSql(
    allocator: Allocator,
    sql: []const u8,
    params: []const ?[]const u8,
) ![]u8 {
    return sql_text.substituteDollarPlaceholders(allocator, sql, params) catch |err| switch (err) {
        sql_text.DollarError.MalformedBindParam => Error.MalformedBindParam,
        sql_text.DollarError.BindParamCountMismatch => Error.BindParamCountMismatch,
        else => err,
    };
}

/// Count the highest `$N` referenced in `sql` (skipping string/comment
/// context). Returns 0 when there are no placeholders. Used as a
/// double-check against the Parse frame's `num_params` count — PG's
/// spec lets the client lie, but most drivers send accurate hints.
pub fn maxDollarIndex(arena: Allocator, sql: []const u8) !u32 {
    var lex = lexer_mod.Lexer.init(arena, sql);
    lex.dialect = .postgres;
    var max_idx: u32 = 0;
    while (true) {
        const tok = lex.next() catch return max_idx;
        if (tok.tag == .eof) break;
        if (tok.tag == .dollar_param) {
            if (tok.value.dollar_param > max_idx) max_idx = tok.value.dollar_param;
        }
    }
    return max_idx;
}

// ---------------------------------------------------------------------------
// Frame parsers
// ---------------------------------------------------------------------------

pub const ParsePayload = struct {
    statement_name: []const u8,
    sql: []const u8,
    type_oids: []const u32,
};

/// Decode a Parse ('P') payload into named slices borrowing from
/// `payload`. `oids_buf` is allocator-owned (must be freed by caller).
pub fn parseParseFrame(
    allocator: Allocator,
    payload: []const u8,
) !ParsePayload {
    var cursor: usize = 0;
    const stmt_name = try packet.readCString(payload, &cursor);
    const sql_bytes = try packet.readCString(payload, &cursor);
    const n_oids = try packet.readU16(payload, &cursor);
    const oids = try allocator.alloc(u32, n_oids);
    errdefer allocator.free(oids);
    var i: usize = 0;
    while (i < n_oids) : (i += 1) oids[i] = try packet.readU32(payload, &cursor);
    return .{
        .statement_name = stmt_name,
        .sql = sql_bytes,
        .type_oids = oids,
    };
}

/// Returned by parseBindFrame. All slices borrow into the input
/// payload bytes; the parameter literals (rendered into SQL literal
/// text) are arena-allocated by the caller.
pub const BindPayload = struct {
    portal_name: []const u8,
    statement_name: []const u8,
    /// Format codes for the inbound parameters. Empty = all text.
    /// Length 1 = same code for every param. Otherwise one per param.
    param_formats: []const u16,
    /// Each entry: null = SQL NULL on the wire (length -1), else the
    /// raw param bytes (length comes from the wire).
    param_values: []const ?[]const u8,
    /// Format codes for result columns (same special rules).
    result_formats: []const u16,
};

/// Decode a Bind ('B') payload. All allocations happen in `arena`;
/// the returned slices share that lifetime.
pub fn parseBindFrame(arena: Allocator, payload: []const u8) !BindPayload {
    var cursor: usize = 0;
    const portal_name = try packet.readCString(payload, &cursor);
    const stmt_name = try packet.readCString(payload, &cursor);

    const n_param_formats = try packet.readU16(payload, &cursor);
    const param_formats = try arena.alloc(u16, n_param_formats);
    var i: usize = 0;
    while (i < n_param_formats) : (i += 1) param_formats[i] = try packet.readU16(payload, &cursor);

    const n_params = try packet.readU16(payload, &cursor);
    const param_values = try arena.alloc(?[]const u8, n_params);
    i = 0;
    while (i < n_params) : (i += 1) {
        const len_i32 = try packet.readI32(payload, &cursor);
        if (len_i32 == -1) {
            param_values[i] = null;
        } else {
            const len: usize = @intCast(len_i32);
            if (cursor + len > payload.len) return packet.Error.FrameTruncated;
            param_values[i] = payload[cursor .. cursor + len];
            cursor += len;
        }
    }

    const n_result_formats = try packet.readU16(payload, &cursor);
    const result_formats = try arena.alloc(u16, n_result_formats);
    i = 0;
    while (i < n_result_formats) : (i += 1) result_formats[i] = try packet.readU16(payload, &cursor);

    return .{
        .portal_name = portal_name,
        .statement_name = stmt_name,
        .param_formats = param_formats,
        .param_values = param_values,
        .result_formats = result_formats,
    };
}

pub const DescribePayload = struct {
    /// 'S' or 'P'.
    kind: u8,
    name: []const u8,
};

pub fn parseDescribeFrame(payload: []const u8) !DescribePayload {
    if (payload.len < 1) return packet.Error.FrameTruncated;
    var cursor: usize = 1;
    const name = try packet.readCString(payload, &cursor);
    return .{ .kind = payload[0], .name = name };
}

pub const ExecutePayload = struct {
    portal_name: []const u8,
    max_rows: u32,
};

pub fn parseExecuteFrame(payload: []const u8) !ExecutePayload {
    var cursor: usize = 0;
    const portal_name = try packet.readCString(payload, &cursor);
    const max_rows = try packet.readU32(payload, &cursor);
    return .{ .portal_name = portal_name, .max_rows = max_rows };
}

pub const ClosePayload = struct {
    kind: u8,
    name: []const u8,
};

pub fn parseCloseFrame(payload: []const u8) !ClosePayload {
    if (payload.len < 1) return packet.Error.FrameTruncated;
    var cursor: usize = 1;
    const name = try packet.readCString(payload, &cursor);
    return .{ .kind = payload[0], .name = name };
}

// ---------------------------------------------------------------------------
// Binary parameter decoding → SQL-literal text
// ---------------------------------------------------------------------------

/// Decode `n` parameter values into an array of optional SQL-literal
/// strings allocated in `arena`. Honours per-param format codes per
/// PG's special rules (empty list = all text; length-1 = applies to
/// all). Uses `type_oids[i]` (or 0 = unspecified text) for binary
/// decoding; text-format params are wrapped as SQL string literals.
pub fn renderBindParams(
    arena: Allocator,
    bind: BindPayload,
    type_oids: []const u32,
) ![]?[]const u8 {
    const n = bind.param_values.len;
    const out = try arena.alloc(?[]const u8, n);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const raw = bind.param_values[i] orelse {
            out[i] = null;
            continue;
        };
        const fmt: u16 = blk: {
            if (bind.param_formats.len == 0) break :blk 0;
            if (bind.param_formats.len == 1) break :blk bind.param_formats[0];
            if (i < bind.param_formats.len) break :blk bind.param_formats[i];
            break :blk 0;
        };
        const oid: u32 = if (i < type_oids.len) type_oids[i] else 0;
        out[i] = try renderOneParam(arena, raw, fmt, oid);
    }
    return out;
}

fn renderOneParam(arena: Allocator, raw: []const u8, fmt: u16, oid: u32) ![]const u8 {
    if (fmt == 0) {
        // Text format: parser-friendly literal. Numeric / temporal /
        // uuid / bool render bare; everything else (including OID 0
        // unspecified) becomes a SQL string literal.
        return try renderTextParam(arena, raw, oid);
    }
    if (fmt != 1) return Error.UnsupportedParamFormat;
    return try renderBinaryParam(arena, raw, oid);
}

fn renderTextParam(arena: Allocator, raw: []const u8, oid: u32) ![]const u8 {
    return switch (oid) {
        OID_BOOL => blk: {
            // PG accepts t/true/y/yes/on/1 and the negatives.
            const lower_buf = try arena.alloc(u8, raw.len);
            for (raw, 0..) |b, idx| lower_buf[idx] = std.ascii.toLower(b);
            const s = lower_buf;
            if (std.mem.eql(u8, s, "t") or std.mem.eql(u8, s, "true") or
                std.mem.eql(u8, s, "y") or std.mem.eql(u8, s, "yes") or
                std.mem.eql(u8, s, "on") or std.mem.eql(u8, s, "1"))
            {
                break :blk try arena.dupe(u8, "TRUE");
            }
            break :blk try arena.dupe(u8, "FALSE");
        },
        OID_INT2, OID_INT4, OID_INT8, OID_FLOAT4, OID_FLOAT8, OID_NUMERIC => try arena.dupe(u8, raw),
        OID_DATE, OID_TIMESTAMP, OID_UUID => try renderStringLiteral(arena, raw),
        OID_TEXT => try renderStringLiteral(arena, raw),
        // OID 0 = client didn't tell us. Auto-detect numeric / boolean
        // shapes (bare), otherwise treat as text. This is necessary
        // because thinDB's parser has no implicit string→number
        // coercion in predicates: `WHERE qty = '20'` errors with
        // PredicateTypeMismatch, while `WHERE qty = 20` works.
        0 => guessLiteral(arena, raw),
        else => try renderStringLiteral(arena, raw),
    };
}

fn guessLiteral(arena: Allocator, raw: []const u8) ![]const u8 {
    if (raw.len == 0) return try renderStringLiteral(arena, raw);
    // Boolean literals.
    if (std.ascii.eqlIgnoreCase(raw, "true") or std.ascii.eqlIgnoreCase(raw, "false"))
        return try arena.dupe(u8, raw);
    // Numeric: optional sign + digits + optional `.digits` + optional
    // exponent. Anything else => SQL string literal.
    var i: usize = 0;
    if (raw[i] == '+' or raw[i] == '-') i += 1;
    var saw_digit = false;
    while (i < raw.len and raw[i] >= '0' and raw[i] <= '9') : (i += 1) saw_digit = true;
    if (i < raw.len and raw[i] == '.') {
        i += 1;
        while (i < raw.len and raw[i] >= '0' and raw[i] <= '9') : (i += 1) saw_digit = true;
    }
    if (i < raw.len and (raw[i] == 'e' or raw[i] == 'E')) {
        i += 1;
        if (i < raw.len and (raw[i] == '+' or raw[i] == '-')) i += 1;
        var exp_digit = false;
        while (i < raw.len and raw[i] >= '0' and raw[i] <= '9') : (i += 1) exp_digit = true;
        if (!exp_digit) return try renderStringLiteral(arena, raw);
    }
    if (saw_digit and i == raw.len) return try arena.dupe(u8, raw);
    return try renderStringLiteral(arena, raw);
}

fn renderBinaryParam(arena: Allocator, raw: []const u8, oid: u32) ![]const u8 {
    return switch (oid) {
        OID_BOOL => blk: {
            if (raw.len < 1) return Error.MalformedBindParam;
            break :blk try arena.dupe(u8, if (raw[0] != 0) "TRUE" else "FALSE");
        },
        OID_INT2 => blk: {
            if (raw.len != 2) return Error.MalformedBindParam;
            const v = std.mem.readInt(i16, raw[0..2], .big);
            break :blk try std.fmt.allocPrint(arena, "{d}", .{v});
        },
        OID_INT4 => blk: {
            if (raw.len != 4) return Error.MalformedBindParam;
            const v = std.mem.readInt(i32, raw[0..4], .big);
            break :blk try std.fmt.allocPrint(arena, "{d}", .{v});
        },
        OID_INT8 => blk: {
            if (raw.len != 8) return Error.MalformedBindParam;
            const v = std.mem.readInt(i64, raw[0..8], .big);
            break :blk try std.fmt.allocPrint(arena, "{d}", .{v});
        },
        OID_FLOAT4 => blk: {
            if (raw.len != 4) return Error.MalformedBindParam;
            const bits = std.mem.readInt(u32, raw[0..4], .big);
            const v: f32 = @bitCast(bits);
            break :blk try std.fmt.allocPrint(arena, "{d}", .{v});
        },
        OID_FLOAT8 => blk: {
            if (raw.len != 8) return Error.MalformedBindParam;
            const bits = std.mem.readInt(u64, raw[0..8], .big);
            const v: f64 = @bitCast(bits);
            break :blk try std.fmt.allocPrint(arena, "{d}", .{v});
        },
        OID_TEXT, 0 => try renderStringLiteral(arena, raw),
        OID_DATE => blk: {
            if (raw.len != 4) return Error.MalformedBindParam;
            const pg_days = std.mem.readInt(i32, raw[0..4], .big);
            const unix_days = pg_days + PG_DATE_EPOCH_UNIX_DAYS;
            // thinDB's parser doesn't accept ISO date literals; emit
            // the underlying integer the same way MySQL prepared does
            // for temporal types.
            break :blk try std.fmt.allocPrint(arena, "{d}", .{unix_days});
        },
        OID_TIMESTAMP => blk: {
            if (raw.len != 8) return Error.MalformedBindParam;
            const pg_micros = std.mem.readInt(i64, raw[0..8], .big);
            const unix_micros = pg_micros + PG_TIMESTAMP_EPOCH_UNIX_MICROS;
            break :blk try std.fmt.allocPrint(arena, "{d}", .{unix_micros});
        },
        OID_UUID => blk: {
            if (raw.len != 16) return Error.MalformedBindParam;
            var buf: [40]u8 = undefined;
            const text = try wire_format.formatUuid(&buf, std.mem.readInt(u128, raw[0..16], .big));
            break :blk try renderStringLiteral(arena, text);
        },
        // OID_NUMERIC binary is a complex variable-precision encoding;
        // we accept text-format numerics in v1 and reject binary.
        OID_NUMERIC => Error.UnsupportedParamOid,
        else => Error.UnsupportedParamOid,
    };
}

fn renderStringLiteral(arena: Allocator, s: []const u8) ![]const u8 {
    return sql_text.renderStringLiteral(arena, s);
}

// ---------------------------------------------------------------------------
// OID inference for ParameterDescription
// ---------------------------------------------------------------------------

/// Return the parameter OIDs to advertise on Describe-statement. If the
/// client supplied non-zero hints in Parse, echo them back. Otherwise
/// fill with OID_TEXT (25) — real drivers usually bind with their own
/// explicit format codes and don't rely on this.
pub fn paramOidsForDescribe(arena: Allocator, hints: []const u32, num_params: u32) ![]u32 {
    const out = try arena.alloc(u32, num_params);
    var i: usize = 0;
    while (i < num_params) : (i += 1) {
        const hint = if (i < hints.len) hints[i] else 0;
        out[i] = if (hint != 0) hint else OID_TEXT;
    }
    return out;
}

// ---------------------------------------------------------------------------
// Dry-compile for Describe-statement schema inference
// ---------------------------------------------------------------------------

pub const DryCompileResult = struct {
    columns: []Column,
    names: [][]u8,
};

/// Try to compile the statement with every `$N` replaced by a dummy
/// literal so the parser sees a complete statement. Returns the
/// resulting output schema (or null when the SQL is a side-effect
/// statement or compile fails). The returned columns + their name
/// bytes are allocated through `gpa` so they outlive the dry-compile
/// arena.
///
/// Dummy strategy: we try the integer `0` first (succeeds on numeric
/// columns, the common case for `WHERE id = $1`), then fall back to
/// `'placeholder'` for string columns. Failure of both leaves
/// schema = null and Describe-statement reports NoData. EXECUTE still
/// emits the real schema from the actual run.
pub fn dryCompileSchema(
    gpa: Allocator,
    catalog: *Catalog,
    session: Session,
    sql: []const u8,
    num_params: u32,
) !?DryCompileResult {
    // Pre-flight parse to short-circuit side-effect statements without
    // building any dummy substitution.
    {
        var probe_arena = std.heap.ArenaAllocator.init(gpa);
        defer probe_arena.deinit();
        const pa = probe_arena.allocator();
        const dummies = try pa.alloc(?[]const u8, num_params);
        for (dummies) |*slot| slot.* = "0";
        const substituted = substituteDollarSql(pa, sql, dummies) catch return null;
        const op = sql_mod.parseDialect(pa, substituted, .postgres) catch return null;
        if (isSideEffect(op.*)) return null;
    }

    const dummies_to_try = [_][]const u8{ "0", "'placeholder'" };
    inline for (dummies_to_try) |dummy| {
        if (try tryDryCompile(gpa, catalog, session, sql, num_params, dummy)) |result| {
            return result;
        }
    }
    return null;
}

fn tryDryCompile(
    gpa: Allocator,
    catalog: *Catalog,
    session: Session,
    sql: []const u8,
    num_params: u32,
    dummy: []const u8,
) !?DryCompileResult {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const aa = arena.allocator();

    const dummies = try aa.alloc(?[]const u8, num_params);
    for (dummies) |*slot| slot.* = dummy;

    const substituted = substituteDollarSql(aa, sql, dummies) catch return null;
    const op = sql_mod.parseDialect(aa, substituted, .postgres) catch return null;
    if (isSideEffect(op.*)) return null;

    const db = catalog.database(session.current_db) orelse return null;
    var compiled = local.compileWithSession(aa, db, session, op) catch return null;
    defer compiled.deinit();

    const schema = compiled.outputSchema();
    const cols = try gpa.alloc(Column, schema.len);
    errdefer gpa.free(cols);
    const names = try gpa.alloc([]u8, schema.len);
    errdefer {
        for (names) |n| gpa.free(n);
        gpa.free(names);
    }
    var inited: usize = 0;
    errdefer {
        var k: usize = 0;
        while (k < inited) : (k += 1) gpa.free(names[k]);
    }
    for (schema, 0..) |col, idx| {
        const name_dup = try gpa.dupe(u8, col.name);
        names[idx] = name_dup;
        inited = idx + 1;
        cols[idx] = .{
            .name = name_dup,
            .type = col.type,
            .nullable = col.nullable,
        };
    }
    return .{ .columns = cols, .names = names };
}

fn isSideEffect(op: ir.Op) bool {
    return switch (op) {
        .ddl, .insert, .copy, .batch => true,
        else => false,
    };
}

// ---------------------------------------------------------------------------
// Binary DataRow encoding
// ---------------------------------------------------------------------------

/// Encode one cell as binary per its column type + PG type rules.
/// Appends raw bytes to `out` (caller writes the length prefix).
pub fn appendBinaryCell(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    col: Column,
    view: anytype,
    row: usize,
) !void {
    var b: [16]u8 = undefined;
    switch (view.data) {
        .boolean => |s| try out.append(allocator, if (s[row] != 0) 1 else 0),
        .tinyint => |s| {
            // No PG OID for int1 — widen to int2.
            std.mem.writeInt(i16, b[0..2], @intCast(s[row]), .big);
            try out.appendSlice(allocator, b[0..2]);
        },
        .smallint => |s| {
            std.mem.writeInt(i16, b[0..2], s[row], .big);
            try out.appendSlice(allocator, b[0..2]);
        },
        .int => |s| {
            std.mem.writeInt(i32, b[0..4], s[row], .big);
            try out.appendSlice(allocator, b[0..4]);
        },
        .bigint => |s| {
            std.mem.writeInt(i64, b[0..8], s[row], .big);
            try out.appendSlice(allocator, b[0..8]);
        },
        .largeint => |s| {
            // No PG OID — render as text (matches the text-format
            // fallback). Caller still treats this as variable-length.
            var buf: [48]u8 = undefined;
            const text = try std.fmt.bufPrint(&buf, "{d}", .{s[row]});
            try out.appendSlice(allocator, text);
        },
        .float => |s| {
            const bits: u32 = @bitCast(s[row]);
            std.mem.writeInt(u32, b[0..4], bits, .big);
            try out.appendSlice(allocator, b[0..4]);
        },
        .double => |s| {
            const bits: u64 = @bitCast(s[row]);
            std.mem.writeInt(u64, b[0..8], bits, .big);
            try out.appendSlice(allocator, b[0..8]);
        },
        .date => |s| {
            const pg_days = s[row] - PG_DATE_EPOCH_UNIX_DAYS;
            std.mem.writeInt(i32, b[0..4], pg_days, .big);
            try out.appendSlice(allocator, b[0..4]);
        },
        .datetime => |s| {
            const pg_micros = s[row] - PG_TIMESTAMP_EPOCH_UNIX_MICROS;
            std.mem.writeInt(i64, b[0..8], pg_micros, .big);
            try out.appendSlice(allocator, b[0..8]);
        },
        .uuid => |s| {
            std.mem.writeInt(u128, b[0..16], s[row], .big);
            try out.appendSlice(allocator, b[0..16]);
        },
        .decimal64 => |s| {
            // PG NUMERIC binary is complex; fall back to text bytes
            // (clients receive the digits and parse identically).
            try wire_format.formatDecimal(allocator, out, @as(i128, s[row]), col.type);
        },
        .decimal128 => |s| {
            try wire_format.formatDecimal(allocator, out, s[row], col.type);
        },
        .varchar, .char, .string, .json => |sv| try out.appendSlice(allocator, sv.rowBytes(row)),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "substituteDollarSql replaces $N in order, preserves strings" {
    const allocator = std.testing.allocator;
    const params = [_]?[]const u8{ "42", "'hello''world'", null };
    const out = try substituteDollarSql(
        allocator,
        "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = '$1' AND d = $3",
        params[0..],
    );
    defer allocator.free(out);
    try std.testing.expectEqualStrings(
        "SELECT * FROM t WHERE a = 42 AND b = 'hello''world' AND c = '$1' AND d = NULL",
        out,
    );
}

test "substituteDollarSql honours -- and /* */ comments" {
    const allocator = std.testing.allocator;
    const params = [_]?[]const u8{"42"};
    const out = try substituteDollarSql(
        allocator,
        "SELECT $1 -- $1 in comment\n /* also $1 */ FROM t",
        params[0..],
    );
    defer allocator.free(out);
    try std.testing.expectEqualStrings(
        "SELECT 42 -- $1 in comment\n /* also $1 */ FROM t",
        out,
    );
}

test "substituteDollarSql errors on out-of-range param index" {
    const allocator = std.testing.allocator;
    const params = [_]?[]const u8{"1"};
    try std.testing.expectError(
        Error.BindParamCountMismatch,
        substituteDollarSql(allocator, "SELECT $1 + $5", params[0..]),
    );
}

test "maxDollarIndex finds highest reference" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const n = try maxDollarIndex(arena.allocator(), "SELECT $1, $3 FROM t WHERE x = $2");
    try std.testing.expectEqual(@as(u32, 3), n);
}

test "maxDollarIndex ignores $ inside strings" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const n = try maxDollarIndex(arena.allocator(), "SELECT '$99' FROM t");
    try std.testing.expectEqual(@as(u32, 0), n);
}

test "renderOneParam decodes text int4 unchanged" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const out = try renderOneParam(arena.allocator(), "42", 0, OID_INT4);
    try std.testing.expectEqualStrings("42", out);
}

test "renderOneParam decodes binary int4 BE" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var raw: [4]u8 = undefined;
    std.mem.writeInt(i32, &raw, 123_456, .big);
    const out = try renderOneParam(arena.allocator(), &raw, 1, OID_INT4);
    try std.testing.expectEqualStrings("123456", out);
}

test "renderOneParam decodes binary int8 BE" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var raw: [8]u8 = undefined;
    std.mem.writeInt(i64, &raw, 9_000_000_000, .big);
    const out = try renderOneParam(arena.allocator(), &raw, 1, OID_INT8);
    try std.testing.expectEqualStrings("9000000000", out);
}

test "renderOneParam text-format text wraps as SQL string literal" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const out = try renderOneParam(arena.allocator(), "O'Brien", 0, OID_TEXT);
    try std.testing.expectEqualStrings("'O''Brien'", out);
}

test "paramOidsForDescribe echoes hints, fills unspecified with TEXT" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const hints = [_]u32{ OID_INT4, 0, OID_BOOL };
    const out = try paramOidsForDescribe(arena.allocator(), hints[0..], 3);
    try std.testing.expectEqual(@as(u32, OID_INT4), out[0]);
    try std.testing.expectEqual(@as(u32, OID_TEXT), out[1]);
    try std.testing.expectEqual(@as(u32, OID_BOOL), out[2]);
}
