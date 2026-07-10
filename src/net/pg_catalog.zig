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

pub const Table = enum { pg_namespace, pg_class, pg_attribute, pg_type, pg_database, pg_proc, pg_tables, pg_views, pg_indexes };

/// Recognize a FROM target as a `pg_catalog` relation. Matches a bare name
/// (`pg_class`) or one explicitly qualified by the `pg_catalog` schema;
/// the `pg_` prefix is reserved, so a bare match never shadows a user table.
pub fn match(ref: ir.TableRef) ?Table {
    if (ref.database != null) return null;
    if (ref.schema) |s| {
        if (!std.ascii.eqlIgnoreCase(s, "pg_catalog")) return null;
    }
    const n = ref.name;
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
    }
    return exec.makeQuery(gpa, self);
}

fn currentDb(catalog: *Catalog, session: Session) ?*api.Database {
    return catalog.database(session.current_db);
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
            if (!std.mem.eql(u8, key[0..sep], session.current_db)) continue;
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
                    try atthasdef.append(a, if (col.default_value != null) 1 else 0);
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

    const sql_names = try catalog.sql_fns.listNames(a, session.current_db);
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
