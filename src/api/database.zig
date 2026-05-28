//! Database — second level of the Catalog → Database → Schema → Table
//! hierarchy. Owns a directory containing zero or more named Schemas.
//!
//! The back-compat constructor `Database.open(allocator, io, data_dir,
//! config)` boots an internal Catalog rooted at `data_dir`, creates a
//! "main" database with a "public" schema, and returns that Database so
//! existing call sites (`db.table(...)`, etc.) keep working.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const api = @import("api.zig");
const Config = api.Config;
const Error = api.Error;
const TableOptions = api.TableOptions;
const OpenOptions = api.OpenOptions;
const AlterOp = api.AlterOp;
const Table = api.Table;

const SchemaMod = @import("schema.zig");
const Schema = SchemaMod.Schema;
const CatalogMod = @import("catalog.zig");
const Catalog = CatalogMod.Catalog;

const snapshot = @import("../util/snapshot.zig");

pub const default_schema_name: []const u8 = "public";
pub const back_compat_database_name: []const u8 = "main";

pub const Database = struct {
    allocator: Allocator,
    io: Io,
    name: []u8,
    db_dir: Io.Dir,
    config: Config,
    schemas: std.StringHashMap(*Schema),
    /// Back-reference; not owning.
    catalog: ?*Catalog = null,
    /// Set by `Database.open` (the back-compat path). When non-null,
    /// `close()` tears down the implicit Catalog as well.
    owned_catalog: ?*Catalog = null,
    /// Back-compat alias for `schema("public").tables`. Pre-v2 callers
    /// that reached into `db.tables.get(...)` keep working. Wired in
    /// `create` once the public schema is open.
    tables: *std.StringHashMap(*Table) = undefined,

    schemas_mutex: Io.Mutex = .init,

    /// Open a database rooted at `data_dir`. Back-compat entry point —
    /// internally spins up a Catalog + "main" database + "public" schema
    /// and returns the Database. Reopen-safe: if the on-disk layout
    /// already contains a "main" database, it's adopted rather than
    /// reconstructed (the in-memory schema map is empty either way —
    /// per-table reopen lands lazily through `db.table(name, ...)`).
    pub fn open(
        allocator: Allocator,
        io: Io,
        data_dir: Io.Dir,
        config: Config,
    ) !*Database {
        const catalog = try Catalog.openInPlace(allocator, io, data_dir, config);
        errdefer catalog.close();

        const db = try catalog.createOrOpenDatabase(back_compat_database_name);
        db.owned_catalog = catalog;
        return db;
    }

    /// Internal: construct a fresh Database under `parent_dir/name/`. Used
    /// by `Catalog.createDatabase`.
    pub fn create(
        allocator: Allocator,
        io: Io,
        parent_dir: Io.Dir,
        name: []const u8,
        config: Config,
        catalog: *Catalog,
    ) !*Database {
        const db_dir = try parent_dir.createDirPathOpen(io, name, .{});
        errdefer {
            var d = db_dir;
            d.close(io);
        }

        const name_copy = try allocator.dupe(u8, name);
        errdefer allocator.free(name_copy);

        const self = try allocator.create(Database);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .name = name_copy,
            .db_dir = db_dir,
            .config = config,
            .schemas = .init(allocator),
            .catalog = catalog,
        };

        const public = try Schema.open(allocator, io, db_dir, default_schema_name, config);
        errdefer public.close();
        public.database = self;
        try self.schemas.put(public.name, public);
        self.tables = &public.tables;

        try discoverSchemasOnDisk(allocator, io, db_dir, config, self);

        return self;
    }

    /// Adopt any non-`public` schema subdirs on disk into this Database.
    /// Non-fatal on iteration error — the database still has `public` and
    /// works for the back-compat case.
    fn discoverSchemasOnDisk(
        allocator: Allocator,
        io: Io,
        db_dir: Io.Dir,
        config: Config,
        self: *Database,
    ) !void {
        var iter_dir = db_dir.openDir(io, ".", .{ .iterate = true }) catch return;
        defer iter_dir.close(io);
        var dir_it = iter_dir.iterate();
        while (try dir_it.next(io)) |entry| {
            if (entry.kind != .directory) continue;
            if (std.mem.eql(u8, entry.name, default_schema_name)) continue;
            if (self.schemas.get(entry.name) != null) continue;
            const s = Schema.open(allocator, io, db_dir, entry.name, config) catch continue;
            s.database = self;
            try self.schemas.put(s.name, s);
        }
    }

    pub fn close(self: *Database) void {
        if (self.owned_catalog) |c| {
            c.close();
            return;
        }
        self.closeInPlace();
    }

    /// Tear down schemas + this Database without touching any owning
    /// Catalog. Called by `Catalog.close` and by `close()` when this
    /// Database has no implicit Catalog of its own.
    pub fn closeInPlace(self: *Database) void {
        var it = self.schemas.iterator();
        while (it.next()) |entry| entry.value_ptr.*.close();
        self.schemas.deinit();
        self.db_dir.close(self.io);
        const allocator = self.allocator;
        allocator.free(self.name);
        allocator.destroy(self);
    }

    pub fn schema(self: *Database, name: []const u8) ?*Schema {
        self.schemas_mutex.lockUncancelable(self.io);
        defer self.schemas_mutex.unlock(self.io);
        return self.schemas.get(name);
    }

    pub fn createSchema(self: *Database, name: []const u8) !*Schema {
        self.schemas_mutex.lockUncancelable(self.io);
        defer self.schemas_mutex.unlock(self.io);

        if (self.schemas.get(name) != null) return Error.SchemaAlreadyExists;
        if (self.db_dir.openDir(self.io, name, .{})) |probe_| {
            var probe = probe_;
            probe.close(self.io);
            return Error.SchemaAlreadyExists;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }

        const s = try Schema.open(self.allocator, self.io, self.db_dir, name, self.config);
        errdefer s.close();
        s.database = self;
        try self.schemas.put(s.name, s);
        return s;
    }

    pub fn dropSchema(self: *Database, name: []const u8) !void {
        self.schemas_mutex.lockUncancelable(self.io);
        const maybe = self.schemas.fetchRemove(name);
        self.schemas_mutex.unlock(self.io);

        if (maybe) |entry| {
            entry.value.close();
        } else {
            var probe = self.db_dir.openDir(self.io, name, .{}) catch |err| switch (err) {
                error.FileNotFound => return Error.SchemaNotFound,
                else => return err,
            };
            probe.close(self.io);
        }

        try self.db_dir.deleteTree(self.io, name);
    }

    pub fn listSchemas(self: *Database, allocator: Allocator) ![][]u8 {
        self.schemas_mutex.lockUncancelable(self.io);
        defer self.schemas_mutex.unlock(self.io);

        const out = try allocator.alloc([]u8, self.schemas.count());
        errdefer allocator.free(out);
        var i: usize = 0;
        errdefer for (out[0..i]) |s| allocator.free(s);
        var it = self.schemas.keyIterator();
        while (it.next()) |k| : (i += 1) {
            out[i] = try allocator.dupe(u8, k.*);
        }
        return out;
    }

    /// Back-compat: delegates to the "public" schema. Pre-v2 call sites
    /// that did `db.table(...)` keep working unchanged.
    pub fn table(
        self: *Database,
        name: []const u8,
        table_schema: @import("../types.zig").TableSchema,
        options: TableOptions,
    ) !*Table {
        const s = self.schema(default_schema_name) orelse return Error.SchemaNotFound;
        return s.table(name, table_schema, options);
    }

    pub fn openTable(self: *Database, name: []const u8, options: OpenOptions) !*Table {
        const s = self.schema(default_schema_name) orelse return Error.SchemaNotFound;
        return s.openTable(name, options);
    }

    pub fn dropTable(self: *Database, name: []const u8) !void {
        const s = self.schema(default_schema_name) orelse return Error.SchemaNotFound;
        return s.dropTable(name);
    }

    pub fn alterTable(self: *Database, name: []const u8, ops: []const AlterOp) !void {
        const s = self.schema(default_schema_name) orelse return Error.SchemaNotFound;
        return s.alterTable(name, ops);
    }

    pub fn renameTable(self: *Database, old_name: []const u8, new_name: []const u8) !void {
        const s = self.schema(default_schema_name) orelse return Error.SchemaNotFound;
        return s.renameTable(old_name, new_name);
    }

    pub fn registerScalarUdf(self: *Database, desc: api.ScalarUdf) !void {
        const catalog = self.catalog orelse self.owned_catalog orelse return Error.DatabaseNotFound;
        return catalog.registerScalarUdf(desc);
    }

    pub fn registerAggregateUdf(self: *Database, desc: api.AggregateUdf) !void {
        const catalog = self.catalog orelse self.owned_catalog orelse return Error.DatabaseNotFound;
        return catalog.registerAggregateUdf(desc);
    }

    /// Look up a table across schemas using the public schema first.
    /// Used by transports (`net/local.zig`, `net/tcp_server.zig`) that
    /// pre-v2 reached straight into `db.tables.get(name)`.
    pub fn findTable(self: *Database, name: []const u8) ?*Table {
        const s = self.schema(default_schema_name) orelse return null;
        s.tables_mutex.lockUncancelable(s.io);
        defer s.tables_mutex.unlock(s.io);
        return s.tables.get(name);
    }

    /// Walk every schema in this database and run one flush sweep on each.
    pub fn backgroundFlushSweep(self: *Database) !void {
        const names = try snapshot.snapshotMapKeys(self.allocator, self.io, &self.schemas_mutex, self.schemas);
        defer snapshot.freeNames(self.allocator, names);
        for (names) |name| {
            const s = self.schema(name) orelse continue;
            try s.backgroundFlushSweep();
        }
    }

    pub fn runBackgroundFlusher(
        self: *Database,
        sleeper_io: Io,
        poll_ms: u32,
        should_stop: *std.atomic.Value(bool),
    ) void {
        const duration: Io.Duration = .fromMilliseconds(@intCast(poll_ms));
        while (!should_stop.load(.acquire)) {
            Io.sleep(sleeper_io, duration, .awake) catch return;
            if (should_stop.load(.acquire)) return;
            self.backgroundFlushSweep() catch {};
        }
    }

    /// Returns true if any schema merged a group this sweep.
    pub fn backgroundCompactSweep(self: *Database) !bool {
        const names = try snapshot.snapshotMapKeys(self.allocator, self.io, &self.schemas_mutex, self.schemas);
        defer snapshot.freeNames(self.allocator, names);
        var worked = false;
        for (names) |name| {
            const s = self.schema(name) orelse continue;
            if (s.backgroundCompactSweep() catch false) worked = true;
        }
        return worked;
    }

    pub fn runBackgroundCompactor(
        self: *Database,
        sleeper_io: Io,
        poll_ms: u32,
        should_stop: *std.atomic.Value(bool),
    ) void {
        const duration: Io.Duration = .fromMilliseconds(@intCast(poll_ms));
        while (!should_stop.load(.acquire)) {
            const worked = self.backgroundCompactSweep() catch false;
            if (should_stop.load(.acquire)) return;
            if (!worked) Io.sleep(sleeper_io, duration, .awake) catch return;
        }
    }

};
