//! Virtual `pg_catalog` tables, materialized on demand from the live
//! catalog so PostgreSQL clients (psql, ORMs) can introspect via real,
//! queryable / JOIN-able relations rather than text-pattern probes.
//!
//! A FROM target naming a known `pg_catalog` relation (qualified
//! `pg_catalog.pg_class` or bare `pg_class`) compiles to a
//! `PgCatalogSource` that yields one in-memory batch with PG-shaped
//! columns. OIDs are assigned deterministically from object names so a
//! JOIN across two independently-built tables (e.g. `pg_class.relnamespace
//! = pg_namespace.oid`) agrees on values. Object kinds live in disjoint
//! OID ranges so a table OID never collides with a schema OID.
//!
//! Scope: the relations + their common columns, queryable and joinable.
//! Functions psql `\d` also needs (format_type, ::regclass, the ~ regex
//! operator, pg_table_is_visible) are out of scope here.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const exec = @import("../exec/exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const ir = @import("../ir/ir.zig");
const api = @import("../api/api.zig");
const Catalog = api.Catalog;
const Session = api.Session;
const conn_registry = @import("conn_registry.zig");

pub const Table = enum {
    pg_namespace,
    pg_class,
    pg_attribute,
    pg_type,
    pg_database,
    pg_proc,
    pg_tables,
    pg_views,
    pg_indexes,
    info_schemata,
    info_tables,
    info_columns,
    info_processlist,
    perf_processlist,
    pg_stat_activity,
};

/// Recognize a FROM target as a `pg_catalog`, `information_schema` or
/// process-list relation. pg_catalog matches a bare name (`pg_class`) or one
/// explicitly qualified — the `pg_` prefix is reserved, so a bare match never
/// shadows a user table. information_schema relations match ONLY when
/// qualified: a bare `tables` must stay a user table. The MySQL dialect sees
/// only the process lists: the MySQL wire answers its other metadata probes
/// itself, and a bare `pg_class` there is a user table.
pub fn match(ref: ir.TableRef, dialect: api.Dialect) ?Table {
    if (ref.database != null) return null;
    if (ref.schema) |s| {
        if (std.ascii.eqlIgnoreCase(s, "information_schema")) {
            if (std.ascii.eqlIgnoreCase(ref.name, "processlist")) return .info_processlist;
            if (dialect == .mysql) return null;
            if (std.ascii.eqlIgnoreCase(ref.name, "schemata")) return .info_schemata;
            if (std.ascii.eqlIgnoreCase(ref.name, "tables")) return .info_tables;
            if (std.ascii.eqlIgnoreCase(ref.name, "columns")) return .info_columns;
            return null;
        }
        if (std.ascii.eqlIgnoreCase(s, "performance_schema")) {
            return if (std.ascii.eqlIgnoreCase(ref.name, "processlist")) .perf_processlist else null;
        }
        if (!std.ascii.eqlIgnoreCase(s, "pg_catalog")) return null;
    }
    if (dialect == .mysql) return null;
    const n = ref.name;
    if (std.ascii.eqlIgnoreCase(n, "pg_stat_activity")) return .pg_stat_activity;
    if (std.ascii.eqlIgnoreCase(n, "pg_namespace")) return .pg_namespace;
    if (std.ascii.eqlIgnoreCase(n, "pg_class")) return .pg_class;
    if (std.ascii.eqlIgnoreCase(n, "pg_attribute")) return .pg_attribute;
    if (std.ascii.eqlIgnoreCase(n, "pg_type")) return .pg_type;
    if (std.ascii.eqlIgnoreCase(n, "pg_database")) return .pg_database;
    if (std.ascii.eqlIgnoreCase(n, "pg_proc")) return .pg_proc;
    if (std.ascii.eqlIgnoreCase(n, "pg_tables")) return .pg_tables;
    if (std.ascii.eqlIgnoreCase(n, "pg_views")) return .pg_views;
    if (std.ascii.eqlIgnoreCase(n, "pg_indexes")) return .pg_indexes;
    return null;
}

const SCHEMA_OID_BASE: u32 = 16_384;
const TABLE_OID_BASE: u32 = 2_000_000;
const DB_OID_BASE: u32 = 12_000_000;
const PG_CATALOG_OID: i32 = 11; // PG's own well-known pg_catalog namespace OID

fn oidIn(base: u32, parts: []const []const u8) i32 {
    var h = std.hash.Wyhash.init(0);
    for (parts) |p| {
        h.update(p);
        h.update("\x00");
    }
    return @intCast(base + @as(u32, @truncate(h.final())) % 1_000_000);
}

fn schemaOid(name: []const u8) i32 {
    if (std.ascii.eqlIgnoreCase(name, "pg_catalog")) return PG_CATALOG_OID;
    return oidIn(SCHEMA_OID_BASE, &.{name});
}
fn tableOid(schema: []const u8, name: []const u8) i32 {
    return oidIn(TABLE_OID_BASE, &.{ schema, name });
}
fn dbOid(name: []const u8) i32 {
    return oidIn(DB_OID_BASE, &.{name});
}

fn typeOid(t: types.Type) i32 {
    return switch (t) {
        .tinyint, .smallint => 21,
        .int => 23,
        .bigint, .largeint => 20,
        .boolean => 16,
        .float => 700,
        .double => 701,
        .date => 1082,
        .datetime => 1114,
        .decimal64, .decimal128 => 1700,
        .uuid => 2950,
        .varchar, .string, .char, .json => 25,
    };
}
fn typeLen(t: types.Type) i16 {
    return if (t.fixedSize()) |sz| @intCast(sz) else -1;
}

// --- column builders (allocate into the source arena) ----------------------

fn colInt(a: Allocator, vals: []const i32) !ColumnView {
    return .{ .data = .{ .int = try a.dupe(i32, vals) } };
}
fn colSmallint(a: Allocator, vals: []const i16) !ColumnView {
    return .{ .data = .{ .smallint = try a.dupe(i16, vals) } };
}
fn colBool(a: Allocator, vals: []const u8) !ColumnView {
    return .{ .data = .{ .boolean = try a.dupe(u8, vals) } };
}
fn colString(a: Allocator, vals: []const []const u8) !ColumnView {
    const offsets = try a.alloc(u32, vals.len + 1);
    var total: u32 = 0;
    offsets[0] = 0;
    for (vals, 0..) |v, i| {
        total += @intCast(v.len);
        offsets[i + 1] = total;
    }
    const bytes = try a.alloc(u8, total);
    var pos: usize = 0;
    for (vals) |v| {
        @memcpy(bytes[pos .. pos + v.len], v);
        pos += v.len;
    }
    return .{ .data = .{ .string = .{ .offsets = offsets, .bytes = bytes } } };
}
fn colBigint(a: Allocator, vals: []const i64) !ColumnView {
    return .{ .data = .{ .bigint = try a.dupe(i64, vals) } };
}

/// Validity bitmap for optional values; null when every value is present.
fn validity(a: Allocator, vals: anytype) !?[]const u8 {
    for (vals) |v| {
        if (v == null) break;
    } else return null;
    const bitmap = try a.alloc(u8, storage.column.bitmapBytes(vals.len));
    @memset(bitmap, 0);
    for (vals, 0..) |v, i| storage.column.setValidBit(bitmap, i, v != null);
    return bitmap;
}
fn colOptString(a: Allocator, vals: []const ?[]const u8) !ColumnView {
    const texts = try a.alloc([]const u8, vals.len);
    for (vals, texts) |v, *text| text.* = v orelse "";
    var view = try colString(a, texts);
    view.nulls = try validity(a, vals);
    return view;
}
fn colOptInt(a: Allocator, vals: []const ?i32) !ColumnView {
    const ints = try a.alloc(i32, vals.len);
    for (vals, ints) |v, *int| int.* = v orelse 0;
    return .{ .data = .{ .int = ints }, .nulls = try validity(a, vals) };
}
fn colOptDatetime(a: Allocator, vals: []const ?i64) !ColumnView {
    const micros = try a.alloc(i64, vals.len);
    for (vals, micros) |v, *us| us.* = v orelse 0;
    return .{ .data = .{ .datetime = micros }, .nulls = try validity(a, vals) };
}

pub const PgCatalogSource = struct {
    gpa: Allocator,
    arena: std.heap.ArenaAllocator,
    tag: Table,
    schema: []const Column,
    views: []const ColumnView,
    row_count: usize,
    emitted: bool = false,

    pub fn next(self: *PgCatalogSource) !?Batch {
        if (self.emitted) return null;
        self.emitted = true;
        return Batch{ .schema = self.schema, .values = self.views, .row_count = self.row_count };
    }

    pub fn deinit(self: *PgCatalogSource) void {
        const gpa = self.gpa;
        self.arena.deinit();
        gpa.destroy(self);
    }

    pub fn outputSchema(self: *PgCatalogSource) []const Column {
        return self.schema;
    }

    pub fn addPrune(_: *PgCatalogSource, _: exec.Predicate) !void {}

    pub fn stats(self: *PgCatalogSource) exec.PipelineStats {
        return .{ .upper_rows = self.row_count };
    }

    pub fn accountant(_: *PgCatalogSource) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(self: *PgCatalogSource, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainIndent(out, allocator, depth);
        try out.appendSlice(allocator, "PgCatalogScan ");
        try out.appendSlice(allocator, @tagName(self.tag));
        try out.append(allocator, '\n');
    }
};

pub fn build(gpa: Allocator, catalog: *Catalog, session: Session, table: Table) !Query {
    const self = try gpa.create(PgCatalogSource);
    self.* = .{
        .gpa = gpa,
        .arena = std.heap.ArenaAllocator.init(gpa),
        .tag = table,
        .schema = &.{},
        .views = &.{},
        .row_count = 0,
    };
    errdefer {
        self.arena.deinit();
        gpa.destroy(self);
    }
    const a = self.arena.allocator();
    switch (table) {
        .pg_namespace => try buildNamespace(a, catalog, session, self),
        .pg_class => try buildClass(a, catalog, session, self),
        .pg_attribute => try buildAttribute(a, catalog, session, self),
        .pg_type => try buildType(a, self),
        .pg_database => try buildDatabase(a, catalog, self),
        .pg_proc => try buildProc(a, catalog, session, self),
        .pg_tables => try buildPgTables(a, catalog, session, self),
        .pg_views => try buildPgViews(a, catalog, session, self),
        .pg_indexes => try buildPgIndexes(a, catalog, session, self),
        .info_schemata => try buildInfoSchemata(a, catalog, session, self),
        .info_tables => try buildInfoTables(a, catalog, session, self),
        .info_columns => try buildInfoColumns(a, catalog, session, self),
        .info_processlist => try buildProcessList(a, catalog, session, self, false),
        .perf_processlist => try buildProcessList(a, catalog, session, self, true),
        .pg_stat_activity => try buildStatActivity(a, catalog, session, self),
    }
    return exec.makeQuery(gpa, self);
}

fn currentDb(catalog: *Catalog, session: Session) ?*api.Database {
    return catalog.database(session.current_db orelse return null);
}

fn buildNamespace(a: Allocator, catalog: *Catalog, session: Session, self: *PgCatalogSource) !void {
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    if (currentDb(catalog, session)) |db| {
        const schema_names = try db.listSchemas(a);
        for (schema_names) |n| try names.append(a, n);
    }
    // Synthesize the system namespaces clients filter on/against.
    for ([_][]const u8{ "pg_catalog", "information_schema" }) |sys| {
        var present = false;
        for (names.items) |n| {
            if (std.ascii.eqlIgnoreCase(n, sys)) present = true;
        }
        if (!present) try names.append(a, sys);
    }

    const n = names.items.len;
    const oids = try a.alloc(i32, n);
    const owners = try a.alloc(i32, n);
    for (names.items, 0..) |name, i| {
        oids[i] = schemaOid(name);
        owners[i] = 10;
    }

    const schema = try a.alloc(Column, 3);
    schema[0] = .{ .name = "oid", .type = .int };
    schema[1] = .{ .name = "nspname", .type = .string };
    schema[2] = .{ .name = "nspowner", .type = .int };
    const views = try a.alloc(ColumnView, 3);
    views[0] = try colInt(a, oids);
    views[1] = try colString(a, names.items);
    views[2] = try colInt(a, owners);
    self.schema = schema;
    self.views = views;
    self.row_count = n;
}

/// The `pg_tables` system view — the shape psql documents and countless
/// tools query directly instead of joining pg_class/pg_namespace themselves.
fn buildPgTables(a: Allocator, catalog: *Catalog, session: Session, self: *PgCatalogSource) !void {
    var schemanames: std.ArrayListUnmanaged([]const u8) = .empty;
    var tablenames: std.ArrayListUnmanaged([]const u8) = .empty;

    if (currentDb(catalog, session)) |db| {
        const schema_names = try db.listSchemas(a);
        for (schema_names) |sname| {
            const sc = db.schema(sname) orelse continue;
            const tnames = try sc.listTables(a);
            for (tnames) |tname| {
                try schemanames.append(a, sname);
                try tablenames.append(a, tname);
            }
        }
    }

    const n = tablenames.items.len;
    const owners = try a.alloc([]const u8, n);
    const flags = try a.alloc(u8, n);
    for (0..n) |i| {
        owners[i] = "thindb";
        flags[i] = 0;
    }

    const schema = try a.alloc(Column, 7);
    schema[0] = .{ .name = "schemaname", .type = .string };
    schema[1] = .{ .name = "tablename", .type = .string };
    schema[2] = .{ .name = "tableowner", .type = .string };
    schema[3] = .{ .name = "hasindexes", .type = .boolean };
    schema[4] = .{ .name = "hasrules", .type = .boolean };
    schema[5] = .{ .name = "hastriggers", .type = .boolean };
    schema[6] = .{ .name = "rowsecurity", .type = .boolean };
    const views = try a.alloc(ColumnView, 7);
    views[0] = try colString(a, schemanames.items);
    views[1] = try colString(a, tablenames.items);
    views[2] = try colString(a, owners);
    views[3] = try colBool(a, flags);
    views[4] = try colBool(a, flags);
    views[5] = try colBool(a, flags);
    views[6] = try colBool(a, flags);
    self.schema = schema;
    self.views = views;
    self.row_count = n;
}

/// PostgreSQL-style data_type names for information_schema.columns.
fn pgDataTypeName(t: types.Type) []const u8 {
    return switch (t) {
        .tinyint, .smallint => "smallint",
        .int => "integer",
        .bigint => "bigint",
        .largeint, .decimal64, .decimal128 => "numeric",
        .boolean => "boolean",
        .float => "real",
        .double => "double precision",
        .date => "date",
        .datetime => "timestamp without time zone",
        .uuid => "uuid",
        .varchar, .char => "character varying",
        .string => "text",
        .json => "json",
    };
}

fn buildInfoSchemata(a: Allocator, catalog: *Catalog, session: Session, self: *PgCatalogSource) !void {
    var catalogs: std.ArrayListUnmanaged([]const u8) = .empty;
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    if (currentDb(catalog, session)) |db| {
        const schema_names = try db.listSchemas(a);
        for (schema_names) |n| {
            try catalogs.append(a, db.name);
            try names.append(a, n);
        }
    }
    const schema = try a.alloc(Column, 2);
    schema[0] = .{ .name = "catalog_name", .type = .string };
    schema[1] = .{ .name = "schema_name", .type = .string };
    const views = try a.alloc(ColumnView, 2);
    views[0] = try colString(a, catalogs.items);
    views[1] = try colString(a, names.items);
    self.schema = schema;
    self.views = views;
    self.row_count = names.items.len;
}

fn buildInfoTables(a: Allocator, catalog: *Catalog, session: Session, self: *PgCatalogSource) !void {
    var schemanames: std.ArrayListUnmanaged([]const u8) = .empty;
    var tablenames: std.ArrayListUnmanaged([]const u8) = .empty;
    if (currentDb(catalog, session)) |db| {
        const schema_names = try db.listSchemas(a);
        for (schema_names) |sname| {
            const sc = db.schema(sname) orelse continue;
            const tnames = try sc.listTables(a);
            for (tnames) |tname| {
                try schemanames.append(a, sname);
                try tablenames.append(a, tname);
            }
        }
    }
    const n = tablenames.items.len;
    const cats = try a.alloc([]const u8, n);
    const ttypes = try a.alloc([]const u8, n);
    for (0..n) |i| {
        cats[i] = session.current_db orelse "";
        ttypes[i] = "BASE TABLE";
    }
    const schema = try a.alloc(Column, 4);
    schema[0] = .{ .name = "table_catalog", .type = .string };
    schema[1] = .{ .name = "table_schema", .type = .string };
    schema[2] = .{ .name = "table_name", .type = .string };
    schema[3] = .{ .name = "table_type", .type = .string };
    const views = try a.alloc(ColumnView, 4);
    views[0] = try colString(a, cats);
    views[1] = try colString(a, schemanames.items);
    views[2] = try colString(a, tablenames.items);
    views[3] = try colString(a, ttypes);
    self.schema = schema;
    self.views = views;
    self.row_count = n;
}

fn buildInfoColumns(a: Allocator, catalog: *Catalog, session: Session, self: *PgCatalogSource) !void {
    var schemanames: std.ArrayListUnmanaged([]const u8) = .empty;
    var tablenames: std.ArrayListUnmanaged([]const u8) = .empty;
    var colnames: std.ArrayListUnmanaged([]const u8) = .empty;
    var ordinals: std.ArrayListUnmanaged(i32) = .empty;
    var nullables: std.ArrayListUnmanaged([]const u8) = .empty;
    var dtypes: std.ArrayListUnmanaged([]const u8) = .empty;
    if (currentDb(catalog, session)) |db| {
        const schema_names = try db.listSchemas(a);
        for (schema_names) |sname| {
            const sc = db.schema(sname) orelse continue;
            const tnames = try sc.listTables(a);
            for (tnames) |tname| {
                const t = sc.openTable(tname, .{}) catch continue;
                for (t.schema.columns, 0..) |col, i| {
                    try schemanames.append(a, sname);
                    try tablenames.append(a, tname);
                    try colnames.append(a, try a.dupe(u8, col.name));
                    try ordinals.append(a, @intCast(i + 1));
                    try nullables.append(a, if (col.nullable) "YES" else "NO");
                    try dtypes.append(a, pgDataTypeName(col.type));
                }
            }
        }
    }
    const n = colnames.items.len;
    const cats = try a.alloc([]const u8, n);
    for (0..n) |i| cats[i] = session.current_db orelse "";
    const schema = try a.alloc(Column, 7);
    schema[0] = .{ .name = "table_catalog", .type = .string };
    schema[1] = .{ .name = "table_schema", .type = .string };
    schema[2] = .{ .name = "table_name", .type = .string };
    schema[3] = .{ .name = "column_name", .type = .string };
    schema[4] = .{ .name = "ordinal_position", .type = .int };
    schema[5] = .{ .name = "is_nullable", .type = .string };
    schema[6] = .{ .name = "data_type", .type = .string };
    const views = try a.alloc(ColumnView, 7);
    views[0] = try colString(a, cats);
    views[1] = try colString(a, schemanames.items);
    views[2] = try colString(a, tablenames.items);
    views[3] = try colString(a, colnames.items);
    views[4] = try colInt(a, ordinals.items);
    views[5] = try colString(a, nullables.items);
    views[6] = try colString(a, dtypes.items);
    self.schema = schema;
    self.views = views;
    self.row_count = n;
}

fn listProcesses(a: Allocator, session: Session) ![]conn_registry.Process {
    const registry = session.connections orelse return &.{};
    return registry.processList(a);
}

/// MySQL's `information_schema.PROCESSLIST`, the rows SHOW FULL PROCESSLIST
/// prints; `performance_schema.processlist` adds EXECUTION_ENGINE.
fn buildProcessList(a: Allocator, catalog: *Catalog, session: Session, self: *PgCatalogSource, with_engine: bool) !void {
    const processes = try listProcesses(a, session);
    const now_ms = conn_registry.nowMs(catalog.io);
    const n = processes.len;
    const ids = try a.alloc(i64, n);
    const users = try a.alloc([]const u8, n);
    const hosts = try a.alloc([]const u8, n);
    const dbs = try a.alloc(?[]const u8, n);
    const commands = try a.alloc([]const u8, n);
    const times = try a.alloc(i32, n);
    const states = try a.alloc(?[]const u8, n);
    const infos = try a.alloc(?[]const u8, n);
    const engines = try a.alloc([]const u8, n);
    for (processes, 0..) |*process, i| {
        const activity = &process.activity;
        ids[i] = process.backend_id;
        users[i] = if (activity.user.len > 0) activity.user.slice() else "unauthenticated user";
        hosts[i] = activity.host.slice();
        dbs[i] = if (activity.db.len > 0) activity.db.slice() else null;
        commands[i] = activity.command.label();
        times[i] = std.math.cast(i32, (now_ms -| activity.since_ms) / std.time.ms_per_s) orelse std.math.maxInt(i32);
        states[i] = activity.command.state();
        infos[i] = if (activity.command.running()) activity.info.slice() else null;
        engines[i] = "PRIMARY";
    }

    const width: usize = if (with_engine) 9 else 8;
    const schema = try a.alloc(Column, width);
    schema[0] = .{ .name = "ID", .type = .bigint };
    schema[1] = .{ .name = "USER", .type = .string };
    schema[2] = .{ .name = "HOST", .type = .string };
    schema[3] = .{ .name = "DB", .type = .string, .nullable = true };
    schema[4] = .{ .name = "COMMAND", .type = .string };
    schema[5] = .{ .name = "TIME", .type = .int };
    schema[6] = .{ .name = "STATE", .type = .string, .nullable = true };
    schema[7] = .{ .name = "INFO", .type = .string, .nullable = true };
    const views = try a.alloc(ColumnView, width);
    views[0] = try colBigint(a, ids);
    views[1] = try colString(a, users);
    views[2] = try colString(a, hosts);
    views[3] = try colOptString(a, dbs);
    views[4] = try colString(a, commands);
    views[5] = try colInt(a, times);
    views[6] = try colOptString(a, states);
    views[7] = try colOptString(a, infos);
    if (with_engine) {
        schema[8] = .{ .name = "EXECUTION_ENGINE", .type = .string };
        views[8] = try colString(a, engines);
    }
    self.schema = schema;
    self.views = views;
    self.row_count = n;
}

/// PostgreSQL's `pg_stat_activity`, the columns tools commonly read. A
/// connection's awake-clock marks become wall-clock timestamps by their age.
/// `query` is the running statement's text, empty while idle.
fn buildStatActivity(a: Allocator, catalog: *Catalog, session: Session, self: *PgCatalogSource) !void {
    const processes = try listProcesses(a, session);
    const now_ms = conn_registry.nowMs(catalog.io);
    const now_us = std.Io.Timestamp.now(catalog.io, .real).toMicroseconds();
    const n = processes.len;
    const datnames = try a.alloc(?[]const u8, n);
    const pids = try a.alloc(i32, n);
    const usenames = try a.alloc(?[]const u8, n);
    const applications = try a.alloc([]const u8, n);
    const addrs = try a.alloc(?[]const u8, n);
    const ports = try a.alloc(?i32, n);
    const backend_starts = try a.alloc(?i64, n);
    const query_starts = try a.alloc(?i64, n);
    const state_changes = try a.alloc(?i64, n);
    const states = try a.alloc(?[]const u8, n);
    const queries = try a.alloc([]const u8, n);
    const backend_types = try a.alloc([]const u8, n);
    for (processes, 0..) |*process, i| {
        const activity = &process.activity;
        const db = activity.db.slice();
        datnames[i] = if (db.len == 0) null else if (std.mem.indexOf(u8, db, "__")) |sep| db[0..sep] else db;
        pids[i] = @bitCast(process.backend_id);
        usenames[i] = if (activity.user.len > 0) activity.user.slice() else null;
        applications[i] = activity.application.slice();
        const peer = splitPeer(activity.host.slice());
        addrs[i] = peer.addr;
        ports[i] = peer.port;
        backend_starts[i] = now_us - @as(i64, @intCast((now_ms -| activity.connected_ms) * std.time.us_per_ms));
        state_changes[i] = now_us - @as(i64, @intCast((now_ms -| activity.since_ms) * std.time.us_per_ms));
        query_starts[i] = if (activity.command.running()) state_changes[i] else null;
        states[i] = switch (activity.command) {
            .connect => null,
            .sleep => "idle",
            .query, .prepare, .execute => "active",
        };
        queries[i] = activity.info.slice();
        backend_types[i] = "client backend";
    }

    const schema = try a.alloc(Column, 12);
    schema[0] = .{ .name = "datname", .type = .string, .nullable = true };
    schema[1] = .{ .name = "pid", .type = .int };
    schema[2] = .{ .name = "usename", .type = .string, .nullable = true };
    schema[3] = .{ .name = "application_name", .type = .string };
    schema[4] = .{ .name = "client_addr", .type = .string, .nullable = true };
    schema[5] = .{ .name = "client_port", .type = .int, .nullable = true };
    schema[6] = .{ .name = "backend_start", .type = .datetime };
    schema[7] = .{ .name = "query_start", .type = .datetime, .nullable = true };
    schema[8] = .{ .name = "state_change", .type = .datetime };
    schema[9] = .{ .name = "state", .type = .string, .nullable = true };
    schema[10] = .{ .name = "query", .type = .string };
    schema[11] = .{ .name = "backend_type", .type = .string };
    const views = try a.alloc(ColumnView, 12);
    views[0] = try colOptString(a, datnames);
    views[1] = try colInt(a, pids);
    views[2] = try colOptString(a, usenames);
    views[3] = try colString(a, applications);
    views[4] = try colOptString(a, addrs);
    views[5] = try colOptInt(a, ports);
    views[6] = try colOptDatetime(a, backend_starts);
    views[7] = try colOptDatetime(a, query_starts);
    views[8] = try colOptDatetime(a, state_changes);
    views[9] = try colOptString(a, states);
    views[10] = try colString(a, queries);
    views[11] = try colString(a, backend_types);
    self.schema = schema;
    self.views = views;
    self.row_count = n;
}

const Peer = struct { addr: ?[]const u8, port: ?i32 };

/// Split a peer written `ip:port` (IPv6 as `[ip]:port`).
fn splitPeer(host: []const u8) Peer {
    const colon = std.mem.lastIndexOfScalar(u8, host, ':') orelse return .{ .addr = null, .port = null };
    const port = std.fmt.parseInt(i32, host[colon + 1 ..], 10) catch null;
    return .{ .addr = std.mem.trim(u8, host[0..colon], "[]"), .port = port };
}

/// The `pg_views` system view over the catalog's registered views. thinDB
/// views are database-scoped, so they present under the session's current
/// schema name.
fn buildPgViews(a: Allocator, catalog: *Catalog, session: Session, self: *PgCatalogSource) !void {
    var schemanames: std.ArrayListUnmanaged([]const u8) = .empty;
    var viewnames: std.ArrayListUnmanaged([]const u8) = .empty;
    var defs: std.ArrayListUnmanaged([]const u8) = .empty;

    {
        while (!catalog.views.mutex.tryLock()) std.atomic.spinLoopHint();
        defer catalog.views.mutex.unlock();
        var it = catalog.views.map.iterator();
        while (it.next()) |e| {
            const key = e.key_ptr.*;
            const sep = std.mem.indexOfScalar(u8, key, 0) orelse continue;
            if (!std.mem.eql(u8, key[0..sep], session.current_db orelse continue)) continue;
            try schemanames.append(a, try a.dupe(u8, session.current_schema));
            try viewnames.append(a, try a.dupe(u8, e.value_ptr.name));
            try defs.append(a, try a.dupe(u8, e.value_ptr.body));
        }
    }

    const n = viewnames.items.len;
    const owners = try a.alloc([]const u8, n);
    for (0..n) |i| owners[i] = "thindb";

    const schema = try a.alloc(Column, 4);
    schema[0] = .{ .name = "schemaname", .type = .string };
    schema[1] = .{ .name = "viewname", .type = .string };
    schema[2] = .{ .name = "viewowner", .type = .string };
    schema[3] = .{ .name = "definition", .type = .string };
    const views = try a.alloc(ColumnView, 4);
    views[0] = try colString(a, schemanames.items);
    views[1] = try colString(a, viewnames.items);
    views[2] = try colString(a, owners);
    views[3] = try colString(a, defs.items);
    self.schema = schema;
    self.views = views;
    self.row_count = n;
}

/// The `pg_indexes` system view: one synthetic row per table describing its
/// clustering key (PRIMARY for unique tables, order_key otherwise) — the
/// only index-like structure thinDB has.
fn buildPgIndexes(a: Allocator, catalog: *Catalog, session: Session, self: *PgCatalogSource) !void {
    var schemanames: std.ArrayListUnmanaged([]const u8) = .empty;
    var tablenames: std.ArrayListUnmanaged([]const u8) = .empty;
    var indexnames: std.ArrayListUnmanaged([]const u8) = .empty;
    var defs: std.ArrayListUnmanaged([]const u8) = .empty;

    if (currentDb(catalog, session)) |db| {
        const schema_names = try db.listSchemas(a);
        for (schema_names) |sname| {
            const sc = db.schema(sname) orelse continue;
            const tnames = try sc.listTables(a);
            for (tnames) |tname| {
                const t = sc.openTable(tname, .{}) catch continue;
                var def: std.ArrayListUnmanaged(u8) = .empty;
                try def.print(a, "CREATE {s}INDEX ON {s}.{s} (", .{
                    if (t.schema.unique) "UNIQUE " else "", sname, tname,
                });
                for (t.schema.order_key, 0..) |k, i| {
                    try def.print(a, "{s}{s}", .{ if (i == 0) "" else ", ", k });
                }
                try def.append(a, ')');
                try schemanames.append(a, sname);
                try tablenames.append(a, tname);
                try indexnames.append(a, if (t.schema.unique) "PRIMARY" else "order_key");
                try defs.append(a, def.items);
            }
        }
    }

    const n = tablenames.items.len;
    const schema = try a.alloc(Column, 4);
    schema[0] = .{ .name = "schemaname", .type = .string };
    schema[1] = .{ .name = "tablename", .type = .string };
    schema[2] = .{ .name = "indexname", .type = .string };
    schema[3] = .{ .name = "indexdef", .type = .string };
    const views = try a.alloc(ColumnView, 4);
    views[0] = try colString(a, schemanames.items);
    views[1] = try colString(a, tablenames.items);
    views[2] = try colString(a, indexnames.items);
    views[3] = try colString(a, defs.items);
    self.schema = schema;
    self.views = views;
    self.row_count = n;
}

fn buildClass(a: Allocator, catalog: *Catalog, session: Session, self: *PgCatalogSource) !void {
    var relname: std.ArrayListUnmanaged([]const u8) = .empty;
    var oids: std.ArrayListUnmanaged(i32) = .empty;
    var relns: std.ArrayListUnmanaged(i32) = .empty;
    var relnatts: std.ArrayListUnmanaged(i16) = .empty;

    if (currentDb(catalog, session)) |db| {
        const schema_names = try db.listSchemas(a);
        for (schema_names) |sname| {
            const sc = db.schema(sname) orelse continue;
            const tnames = try sc.listTables(a);
            for (tnames) |tname| {
                const t = sc.openTable(tname, .{}) catch continue;
                try relname.append(a, tname);
                try oids.append(a, tableOid(sname, tname));
                try relns.append(a, schemaOid(sname));
                try relnatts.append(a, @intCast(t.schema.columns.len));
            }
        }
    }

    const n = relname.items.len;
    const relkind = try a.alloc([]const u8, n);
    const relpersist = try a.alloc([]const u8, n);
    const relhasindex = try a.alloc(u8, n);
    const relowner = try a.alloc(i32, n);
    const relam = try a.alloc(i32, n);
    for (0..n) |i| {
        relkind[i] = "r";
        relpersist[i] = "p";
        relhasindex[i] = 0;
        relowner[i] = 10;
        relam[i] = 0;
    }

    const schema = try a.alloc(Column, 9);
    schema[0] = .{ .name = "oid", .type = .int };
    schema[1] = .{ .name = "relname", .type = .string };
    schema[2] = .{ .name = "relnamespace", .type = .int };
    schema[3] = .{ .name = "relkind", .type = .string };
    schema[4] = .{ .name = "relnatts", .type = .smallint };
    schema[5] = .{ .name = "relhasindex", .type = .boolean };
    schema[6] = .{ .name = "relpersistence", .type = .string };
    schema[7] = .{ .name = "relowner", .type = .int };
    schema[8] = .{ .name = "relam", .type = .int };
    const views = try a.alloc(ColumnView, 9);
    views[0] = try colInt(a, oids.items);
    views[1] = try colString(a, relname.items);
    views[2] = try colInt(a, relns.items);
    views[3] = try colString(a, relkind);
    views[4] = try colSmallint(a, relnatts.items);
    views[5] = try colBool(a, relhasindex);
    views[6] = try colString(a, relpersist);
    views[7] = try colInt(a, relowner);
    views[8] = try colInt(a, relam);
    self.schema = schema;
    self.views = views;
    self.row_count = n;
}

fn buildAttribute(a: Allocator, catalog: *Catalog, session: Session, self: *PgCatalogSource) !void {
    var attrelid: std.ArrayListUnmanaged(i32) = .empty;
    var attname: std.ArrayListUnmanaged([]const u8) = .empty;
    var atttypid: std.ArrayListUnmanaged(i32) = .empty;
    var attnum: std.ArrayListUnmanaged(i16) = .empty;
    var attnotnull: std.ArrayListUnmanaged(u8) = .empty;
    var attlen: std.ArrayListUnmanaged(i16) = .empty;
    var atthasdef: std.ArrayListUnmanaged(u8) = .empty;

    if (currentDb(catalog, session)) |db| {
        const schema_names = try db.listSchemas(a);
        for (schema_names) |sname| {
            const sc = db.schema(sname) orelse continue;
            const tnames = try sc.listTables(a);
            for (tnames) |tname| {
                const t = sc.openTable(tname, .{}) catch continue;
                const oid = tableOid(sname, tname);
                for (t.schema.columns, 0..) |col, ci| {
                    try attrelid.append(a, oid);
                    try attname.append(a, col.name);
                    try atttypid.append(a, typeOid(col.type));
                    try attnum.append(a, @intCast(ci + 1));
                    try attnotnull.append(a, if (col.nullable) 0 else 1);
                    try attlen.append(a, typeLen(col.type));
                    try atthasdef.append(a, if (col.default_value != null or col.default_now) 1 else 0);
                }
            }
        }
    }

    const n = attname.items.len;
    const atttypmod = try a.alloc(i32, n);
    const attisdropped = try a.alloc(u8, n);
    for (0..n) |i| {
        atttypmod[i] = -1;
        attisdropped[i] = 0;
    }

    const schema = try a.alloc(Column, 9);
    schema[0] = .{ .name = "attrelid", .type = .int };
    schema[1] = .{ .name = "attname", .type = .string };
    schema[2] = .{ .name = "atttypid", .type = .int };
    schema[3] = .{ .name = "attnum", .type = .smallint };
    schema[4] = .{ .name = "attnotnull", .type = .boolean };
    schema[5] = .{ .name = "atttypmod", .type = .int };
    schema[6] = .{ .name = "attisdropped", .type = .boolean };
    schema[7] = .{ .name = "attlen", .type = .smallint };
    schema[8] = .{ .name = "atthasdef", .type = .boolean };
    const views = try a.alloc(ColumnView, 9);
    views[0] = try colInt(a, attrelid.items);
    views[1] = try colString(a, attname.items);
    views[2] = try colInt(a, atttypid.items);
    views[3] = try colSmallint(a, attnum.items);
    views[4] = try colBool(a, attnotnull.items);
    views[5] = try colInt(a, atttypmod);
    views[6] = try colBool(a, attisdropped);
    views[7] = try colSmallint(a, attlen.items);
    views[8] = try colBool(a, atthasdef.items);
    self.schema = schema;
    self.views = views;
    self.row_count = n;
}

const TypeRow = struct { oid: i32, name: []const u8, len: i16 };
const pg_types = [_]TypeRow{
    .{ .oid = 16, .name = "bool", .len = 1 },
    .{ .oid = 21, .name = "int2", .len = 2 },
    .{ .oid = 23, .name = "int4", .len = 4 },
    .{ .oid = 20, .name = "int8", .len = 8 },
    .{ .oid = 700, .name = "float4", .len = 4 },
    .{ .oid = 701, .name = "float8", .len = 8 },
    .{ .oid = 1700, .name = "numeric", .len = -1 },
    .{ .oid = 1082, .name = "date", .len = 4 },
    .{ .oid = 1114, .name = "timestamp", .len = 8 },
    .{ .oid = 2950, .name = "uuid", .len = 16 },
    .{ .oid = 25, .name = "text", .len = -1 },
    .{ .oid = 1043, .name = "varchar", .len = -1 },
};

fn buildType(a: Allocator, self: *PgCatalogSource) !void {
    const n = pg_types.len;
    const oids = try a.alloc(i32, n);
    const names = try a.alloc([]const u8, n);
    const nsp = try a.alloc(i32, n);
    const typtype = try a.alloc([]const u8, n);
    const lens = try a.alloc(i16, n);
    for (pg_types, 0..) |tr, i| {
        oids[i] = tr.oid;
        names[i] = tr.name;
        nsp[i] = PG_CATALOG_OID;
        typtype[i] = "b";
        lens[i] = tr.len;
    }

    const schema = try a.alloc(Column, 5);
    schema[0] = .{ .name = "oid", .type = .int };
    schema[1] = .{ .name = "typname", .type = .string };
    schema[2] = .{ .name = "typnamespace", .type = .int };
    schema[3] = .{ .name = "typtype", .type = .string };
    schema[4] = .{ .name = "typlen", .type = .smallint };
    const views = try a.alloc(ColumnView, 5);
    views[0] = try colInt(a, oids);
    views[1] = try colString(a, names);
    views[2] = try colInt(a, nsp);
    views[3] = try colString(a, typtype);
    views[4] = try colSmallint(a, lens);
    self.schema = schema;
    self.views = views;
    self.row_count = n;
}

/// Registered table functions: SQL inline (session database) + Zig/DLL/
/// embedded table UDFs (process-wide). Enough shape for programmatic
/// discovery (`SELECT * FROM pg_proc`); psql's full `\df` additionally
/// calls pg_get_function_* helpers we don't serve yet.
fn buildProc(a: Allocator, catalog: *Catalog, session: Session, self: *PgCatalogSource) !void {
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    var langs: std.ArrayListUnmanaged([]const u8) = .empty;

    const sql_names = if (session.current_db) |db_name| try catalog.sql_fns.listNames(a, db_name) else try a.alloc([]u8, 0);
    for (sql_names) |n| {
        try names.append(a, n);
        try langs.append(a, "sql");
    }
    for (catalog.udfs.tables.items) |t| {
        try names.append(a, try a.dupe(u8, t.name));
        try langs.append(a, "zig");
    }

    const n = names.items.len;
    const oids = try a.alloc(i32, n);
    const nsp = try a.alloc(i32, n);
    const owner = try a.alloc(i32, n);
    const kind = try a.alloc([]const u8, n);
    const rettype = try a.alloc(i32, n);
    for (0..n) |i| {
        oids[i] = oidIn(TABLE_OID_BASE, &.{ "fn", names.items[i] });
        nsp[i] = schemaOid("public");
        owner[i] = 10;
        kind[i] = "f";
        rettype[i] = 2249; // record
    }

    const schema = try a.alloc(Column, 7);
    schema[0] = .{ .name = "oid", .type = .int };
    schema[1] = .{ .name = "proname", .type = .string };
    schema[2] = .{ .name = "pronamespace", .type = .int };
    schema[3] = .{ .name = "proowner", .type = .int };
    schema[4] = .{ .name = "prokind", .type = .string };
    schema[5] = .{ .name = "prorettype", .type = .int };
    schema[6] = .{ .name = "prolang_name", .type = .string };
    const views = try a.alloc(ColumnView, 7);
    views[0] = try colInt(a, oids);
    views[1] = try colString(a, names.items);
    views[2] = try colInt(a, nsp);
    views[3] = try colInt(a, owner);
    views[4] = try colString(a, kind);
    views[5] = try colInt(a, rettype);
    views[6] = try colString(a, langs.items);
    self.schema = schema;
    self.views = views;
    self.row_count = n;
}

fn buildDatabase(a: Allocator, catalog: *Catalog, self: *PgCatalogSource) !void {
    const db_names = try catalog.listDatabases(a);
    const n = db_names.len;
    const oids = try a.alloc(i32, n);
    const dba = try a.alloc(i32, n);
    const enc = try a.alloc(i32, n);
    const istemplate = try a.alloc(u8, n);
    const allowconn = try a.alloc(u8, n);
    const connlimit = try a.alloc(i32, n);
    for (db_names, 0..) |name, i| {
        oids[i] = dbOid(name);
        dba[i] = 10;
        enc[i] = 6; // UTF8
        istemplate[i] = 0;
        allowconn[i] = 1;
        connlimit[i] = -1;
    }

    const schema = try a.alloc(Column, 7);
    schema[0] = .{ .name = "oid", .type = .int };
    schema[1] = .{ .name = "datname", .type = .string };
    schema[2] = .{ .name = "datdba", .type = .int };
    schema[3] = .{ .name = "encoding", .type = .int };
    schema[4] = .{ .name = "datistemplate", .type = .boolean };
    schema[5] = .{ .name = "datallowconn", .type = .boolean };
    schema[6] = .{ .name = "datconnlimit", .type = .int };
    const views = try a.alloc(ColumnView, 7);
    views[0] = try colInt(a, oids);
    views[1] = try colString(a, db_names);
    views[2] = try colInt(a, dba);
    views[3] = try colInt(a, enc);
    views[4] = try colBool(a, istemplate);
    views[5] = try colBool(a, allowconn);
    views[6] = try colInt(a, connlimit);
    self.schema = schema;
    self.views = views;
    self.row_count = n;
}
