//! DDL + DML + COPY + SHOW parsing — extracted from parser.zig.
//!
//! All free functions take the Parser via `anytype` (the only caller
//! ever passes `*parser.Parser`); avoiding a circular import keeps the
//! type plumbing simple. Helpers borrow Parser methods via duck typing
//! (`p.expect`, `p.parseTableRef`, etc.).

const std = @import("std");

const ir = @import("../ir/ir.zig");
const types = @import("../types.zig");
const Value = types.Value;

pub const ColDefResult = struct { def: ir.ColumnDef, is_pk: bool };

pub fn parseDdl(p: anytype) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    const head = p.cur.tag;
    try p.advance();
    switch (head) {
        .kw_create => {
            if (p.cur.tag == .kw_database) {
                try p.advance();
                const name = try p.dupedIdent();
                return try p.allocOp(.{ .ddl = .{ .create_database = name } });
            }
            if (p.cur.tag == .kw_schema) {
                try p.advance();
                const name = try p.dupedIdent();
                return try p.allocOp(.{ .ddl = .{ .create_schema = name } });
            }
            var is_temp = false;
            if (p.cur.tag == .kw_temp or p.cur.tag == .kw_temporary) {
                is_temp = true;
                try p.advance();
            }
            if (p.cur.tag == .kw_table) {
                try p.advance();
                return try parseCreateTableBody(p, is_temp);
            }
            return PE.SqlExpectedKeyword;
        },
        .kw_drop => {
            if (p.cur.tag == .kw_database) {
                try p.advance();
                const name = try p.dupedIdent();
                return try p.allocOp(.{ .ddl = .{ .drop_database = name } });
            }
            if (p.cur.tag == .kw_schema) {
                try p.advance();
                const name = try p.dupedIdent();
                return try p.allocOp(.{ .ddl = .{ .drop_schema = name } });
            }
            if (p.cur.tag == .kw_temp or p.cur.tag == .kw_temporary) {
                try p.advance();
            }
            if (p.cur.tag == .kw_table) {
                try p.advance();
                return try parseDropTableBody(p);
            }
            return PE.SqlExpectedKeyword;
        },
        .kw_use => {
            const first = try p.dupedIdent();
            if (p.cur.tag == .dot) {
                try p.advance();
                const second = try p.dupedIdent();
                return try p.allocOp(.{ .ddl = .{ .use_database_schema = .{
                    .database = first,
                    .schema = second,
                } } });
            }
            return try p.allocOp(.{ .ddl = .{ .use_schema = first } });
        },
        else => unreachable,
    }
}

/// CREATE [TEMP|TEMPORARY] TABLE [IF NOT EXISTS] [db.][schema.]name
///   ( column_def, ... [, PRIMARY KEY (..)] )
///
/// CTAS form: `CREATE TABLE name AS SELECT ...` — column list omitted;
/// schema inferred from the source query at compile time.
pub fn parseCreateTableBody(p: anytype, is_temp: bool) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    var if_not_exists = false;
    if (p.cur.tag == .kw_if) {
        try p.advance();
        if (p.cur.tag != .kw_not) return PE.SqlExpectedKeyword;
        try p.advance();
        if (p.cur.tag != .kw_exists) return PE.SqlExpectedKeyword;
        try p.advance();
        if_not_exists = true;
    }
    const ref = try p.parseTableRef();

    // CTAS path: `CREATE TABLE name AS SELECT ...`. No column list.
    if (p.cur.tag == .kw_as) {
        try p.advance();
        if (p.cur.tag != .kw_select) return PE.SqlExpectedSelect;
        const source = try p.parseStatement();
        return try p.allocOp(.{ .create_table_as = .{
            .table = ref,
            .if_not_exists = if_not_exists,
            .is_temp = is_temp,
            .source = source,
        } });
    }

    try p.expect(.lparen);

    var cols: std.ArrayList(ir.ColumnDef) = .empty;
    defer cols.deinit(p.arena);
    var inline_pk: ?[]const u8 = null;
    var table_pk: ?[]const []const u8 = null;

    while (true) {
        if (p.cur.tag == .kw_primary) {
            try p.advance();
            if (p.cur.tag != .kw_key) return PE.SqlExpectedKeyword;
            try p.advance();
            try p.expect(.lparen);
            table_pk = try p.parseIdentList();
            try p.expect(.rparen);
        } else {
            const col = try parseColumnDef(p);
            try cols.append(p.arena, col.def);
            if (col.is_pk) {
                if (inline_pk != null) return PE.SqlInvalidProjection;
                inline_pk = col.def.name;
            }
        }
        if (p.cur.tag != .comma) break;
        try p.advance();
    }
    try p.expect(.rparen);

    // Tolerate trailing engine-options noise like `ENGINE=...` / `CHARSET=...`
    // by eating any tokens up to EOF or semicolon. Keeps MySQL clients happy.
    while (p.cur.tag != .eof and p.cur.tag != .semicolon) {
        try p.advance();
    }

    if (inline_pk != null and table_pk != null) {
        return PE.SqlInvalidProjection;
    }
    const order_key: []const []const u8 = if (table_pk) |tpk|
        tpk
    else if (inline_pk) |ipk| blk: {
        const one = try p.arena.alloc([]const u8, 1);
        one[0] = ipk;
        break :blk one;
    } else return PE.SqlInvalidProjection;

    const owned_cols = try p.arena.alloc(ir.ColumnDef, cols.items.len);
    for (cols.items, 0..) |c, i| owned_cols[i] = c;

    return try p.allocOp(.{ .ddl = .{ .create_table = .{
        .table = ref,
        .if_not_exists = if_not_exists,
        .is_temp = is_temp,
        .columns = owned_cols,
        .order_key = order_key,
    } } });
}

pub fn parseDropTableBody(p: anytype) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    var if_exists = false;
    if (p.cur.tag == .kw_if) {
        try p.advance();
        if (p.cur.tag != .kw_exists) return PE.SqlExpectedKeyword;
        try p.advance();
        if_exists = true;
    }
    const ref = try p.parseTableRef();
    return try p.allocOp(.{ .ddl = .{ .drop_table = .{
        .table = ref,
        .if_exists = if_exists,
    } } });
}

pub fn parseInsert(p: anytype) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    try p.advance(); // consume INSERT
    if (p.cur.tag != .kw_into) return PE.SqlExpectedKeyword;
    try p.advance();
    const ref = try p.parseTableRef();

    var cols_opt: ?[]const []const u8 = null;
    if (p.cur.tag == .lparen) {
        try p.advance();
        cols_opt = try p.parseIdentList();
        try p.expect(.rparen);
    }

    // INSERT INTO t (cols) SELECT ... — source rows from a query
    // rather than a VALUES list. Parsed before the VALUES branch so
    // SELECT/WITH show up in the same dispatch position.
    if (p.cur.tag == .kw_select or p.cur.tag == .kw_with) {
        const source = try p.parseStatement();
        return try p.allocOp(.{ .insert_select = .{
            .table = ref,
            .columns = cols_opt,
            .source = source,
        } });
    }

    if (p.cur.tag != .kw_values) return PE.SqlExpectedKeyword;
    try p.advance();

    var rows: std.ArrayList([]const ?Value) = .empty;
    defer rows.deinit(p.arena);
    while (true) {
        try p.expect(.lparen);
        var row_vals: std.ArrayList(?Value) = .empty;
        defer row_vals.deinit(p.arena);
        while (true) {
            const v = try parseInsertValue(p);
            try row_vals.append(p.arena, v);
            if (p.cur.tag != .comma) break;
            try p.advance();
        }
        try p.expect(.rparen);
        const row_owned = try p.arena.alloc(?Value, row_vals.items.len);
        for (row_vals.items, 0..) |v, i| row_owned[i] = v;
        try rows.append(p.arena, row_owned);
        if (p.cur.tag != .comma) break;
        try p.advance();
    }
    const rows_owned = try p.arena.alloc([]const ?Value, rows.items.len);
    for (rows.items, 0..) |r, i| rows_owned[i] = r;

    return try p.allocOp(.{ .insert = .{
        .table = ref,
        .columns = cols_opt,
        .rows = rows_owned,
    } });
}

/// COPY [db.][schema.]table [(col, ...)] FROM STDIN [WITH (...)]
/// COPY [db.][schema.]table [(col, ...)] TO STDOUT [WITH (...)]
/// File-path forms (`FROM 'path'` / `TO 'path'`) are rejected.
///
/// `TO`, `STDIN`, `STDOUT`, `FORMAT`, `TEXT` are NOT lexer keywords
/// (they collide with column-type names like `TEXT`). We accept
/// them as identifiers and match case-insensitively here.
pub fn parseCopy(p: anytype) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    try p.advance(); // consume COPY
    const ref = try p.parseTableRef();

    var cols_opt: ?[]const []const u8 = null;
    if (p.cur.tag == .lparen) {
        try p.advance();
        cols_opt = try p.parseIdentList();
        try p.expect(.rparen);
    }

    const direction: ir.CopyOp.Direction = switch (p.cur.tag) {
        .kw_from => .from_stdin,
        .identifier => blk: {
            if (std.ascii.eqlIgnoreCase(p.cur.text, "to")) break :blk .to_stdout;
            return PE.SqlExpectedKeyword;
        },
        else => return PE.SqlExpectedKeyword,
    };
    try p.advance();

    switch (p.cur.tag) {
        .identifier => {
            const text = p.cur.text;
            if (std.ascii.eqlIgnoreCase(text, "stdin")) {
                if (direction != .from_stdin) return PE.SqlExpectedKeyword;
            } else if (std.ascii.eqlIgnoreCase(text, "stdout")) {
                if (direction != .to_stdout) return PE.SqlExpectedKeyword;
            } else return PE.SqlExpectedKeyword;
            try p.advance();
        },
        .string => return PE.SqlCopyFileNotSupported,
        else => return PE.SqlExpectedKeyword,
    }

    if (p.cur.tag == .kw_with) {
        try p.advance();
        try p.expect(.lparen);
        while (true) {
            try parseCopyOption(p);
            if (p.cur.tag != .comma) break;
            try p.advance();
        }
        try p.expect(.rparen);
    }

    return try p.allocOp(.{ .copy = .{
        .direction = direction,
        .table = ref,
        .columns = cols_opt,
    } });
}

fn parseCopyOption(p: anytype) !void {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag != .identifier or
        !std.ascii.eqlIgnoreCase(p.cur.text, "format"))
        return PE.SqlCopyUnsupportedFormat;
    try p.advance();
    if (p.cur.tag != .identifier or
        !std.ascii.eqlIgnoreCase(p.cur.text, "text"))
        return PE.SqlCopyUnsupportedFormat;
    try p.advance();
}

pub fn parseColumnDef(p: anytype) !ColDefResult {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
    const name = try p.arena.dupe(u8, p.cur.text);
    try p.advance();

    const ty = try parseColumnType(p);

    var nullable = true; // SQL standard default; flipped to false by NOT NULL or PRIMARY KEY
    var is_pk = false;
    var saw_not_null = false;
    var default_value: ?types.Value = null;
    var auto_increment = false;
    while (true) {
        switch (p.cur.tag) {
            .kw_not => {
                try p.advance();
                if (p.cur.tag != .kw_null) return PE.SqlExpectedNull;
                try p.advance();
                nullable = false;
                saw_not_null = true;
            },
            .kw_primary => {
                try p.advance();
                if (p.cur.tag != .kw_key) return PE.SqlExpectedKeyword;
                try p.advance();
                is_pk = true;
                nullable = false;
            },
            .kw_null => {
                if (saw_not_null) return PE.SqlExpectedKeyword;
                try p.advance();
                nullable = true;
            },
            // DEFAULT <literal> — column-level default for omitted-column
            // INSERTs. v1 accepts only literal values (no expressions /
            // function calls). The Value tag's type must match the
            // column type; we don't enforce that here at the parse layer
            // (the compile path validates it once the schema is known).
            .kw_default => {
                try p.advance();
                default_value = try p.parseValue();
            },
            .kw_auto_increment => {
                try p.advance();
                auto_increment = true;
            },
            else => break,
        }
    }
    return .{
        .def = .{
            .name = name,
            .column_type = ty,
            .nullable = nullable,
            .default_value = default_value,
            .auto_increment = auto_increment,
        },
        .is_pk = is_pk,
    };
}

pub fn parseColumnType(p: anytype) !types.Type {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
    const name = p.cur.text;
    try p.advance();

    if (asciiEqlAny(name, &.{"bigint"})) return .bigint;
    if (asciiEqlAny(name, &.{ "int", "integer" })) return .int;
    if (asciiEqlAny(name, &.{"smallint"})) return .smallint;
    if (asciiEqlAny(name, &.{"tinyint"})) return .smallint;
    if (asciiEqlAny(name, &.{ "float", "real" })) return .float;
    if (asciiEqlAny(name, &.{"double"})) {
        if (p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "precision")) {
            try p.advance();
        }
        return .double;
    }
    if (asciiEqlAny(name, &.{ "decimal", "numeric" })) {
        try p.expect(.lparen);
        if (p.cur.tag != .integer) return PE.SqlExpectedValue;
        const p_raw = p.cur.value.integer;
        try p.advance();
        try p.expect(.comma);
        if (p.cur.tag != .integer) return PE.SqlExpectedValue;
        const s_raw = p.cur.value.integer;
        try p.advance();
        try p.expect(.rparen);
        if (p_raw < 1 or p_raw > 38 or s_raw < 0 or s_raw > p_raw) return PE.SqlExpectedValue;
        const p_u: u8 = @intCast(p_raw);
        const s_u: u8 = @intCast(s_raw);
        return if (p_u <= 18)
            types.Type{ .decimal64 = .{ .p = p_u, .s = s_u } }
        else
            types.Type{ .decimal128 = .{ .p = p_u, .s = s_u } };
    }
    if (asciiEqlAny(name, &.{"varchar"})) {
        try p.expect(.lparen);
        if (p.cur.tag != .integer) return PE.SqlExpectedValue;
        const n_raw = p.cur.value.integer;
        try p.advance();
        try p.expect(.rparen);
        if (n_raw < 1) return PE.SqlExpectedValue;
        return types.Type{ .varchar = @intCast(n_raw) };
    }
    if (asciiEqlAny(name, &.{ "text", "string" })) return .string;
    if (asciiEqlAny(name, &.{ "boolean", "bool" })) return .boolean;
    if (asciiEqlAny(name, &.{"date"})) return .date;
    if (asciiEqlAny(name, &.{ "datetime", "timestamp" })) return .datetime;
    if (asciiEqlAny(name, &.{"uuid"})) return .uuid;
    return PE.SqlExpectedKeyword;
}

fn parseInsertValue(p: anytype) !?Value {
    if (p.cur.tag == .kw_null) {
        try p.advance();
        return null;
    }
    return try p.parseValue();
}

pub fn parseShow(p: anytype) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    try p.advance(); // consume SHOW
    switch (p.cur.tag) {
        .kw_databases => {
            try p.advance();
            return try p.allocOp(.{ .show = .databases });
        },
        .kw_schemas => {
            try p.advance();
            var db: ?[]const u8 = null;
            if (p.cur.tag == .kw_from) {
                try p.advance();
                db = try p.dupedIdent();
            }
            return try p.allocOp(.{ .show = .{ .schemas = db } });
        },
        .kw_tables => {
            try p.advance();
            var ref: ir.TableRef = .{ .name = "" };
            if (p.cur.tag == .kw_from) {
                try p.advance();
                const first = try p.dupedIdent();
                if (p.cur.tag == .dot) {
                    try p.advance();
                    const second = try p.dupedIdent();
                    ref = .{ .database = first, .schema = second, .name = "" };
                } else {
                    ref = .{ .schema = first, .name = "" };
                }
            }
            return try p.allocOp(.{ .show = .{ .tables = ref } });
        },
        else => return PE.SqlExpectedKeyword,
    }
}

fn asciiEqlAny(s: []const u8, candidates: []const []const u8) bool {
    for (candidates) |c| {
        if (std.ascii.eqlIgnoreCase(s, c)) return true;
    }
    return false;
}
