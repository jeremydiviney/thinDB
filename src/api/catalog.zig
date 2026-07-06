//! Catalog — top of the Catalog → Database → Schema → Table hierarchy.
//! Owns a root directory containing zero or more named Databases. Created
//! either explicitly via `Catalog.open` or implicitly by the back-compat
//! `Database.open` constructor.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const api = @import("api.zig");
const Config = api.Config;
const Error = api.Error;

const DatabaseMod = @import("database.zig");
const Database = DatabaseMod.Database;

const snapshot = @import("../util/snapshot.zig");
const udf_mod = @import("../udf.zig");
const memory = @import("../memory.zig");

pub const Catalog = struct {
    allocator: Allocator,
    io: Io,
    root_dir: Io.Dir,
    config: Config,
    udfs: udf_mod.UdfRegistry,
    /// SQL inline table functions (`CREATE FUNCTION ... RETURNS TABLE`),
    /// keyed per database. Loaded from `<db>/_functions/*.sql` on open;
    /// function DDL keeps the registry and the files in sync.
    sql_fns: udf_mod.SqlFnRegistry,
    databases: std.StringHashMap(*Database),
    /// The shared cross-query memory pool, when this Catalog minted it from
    /// `Config.memory_budget` (vs. adopting a caller-provided one, which the
    /// caller owns). Destroyed last in `close` — every query's accountant
    /// holds a pointer into it.
    owned_pool: ?*memory.MemoryPool = null,
    /// True when the Catalog allocator-owns its struct (the user called
    /// `Catalog.open` and gets a `*Catalog`). False when the Catalog is
    /// nested inside another owner (currently unused; reserved).
    heap_owned: bool = true,

    databases_mutex: Io.Mutex = .init,

    pub fn open(
        allocator: Allocator,
        io: Io,
        root_dir: Io.Dir,
        config: Config,
    ) !*Catalog {
        return Catalog.openInPlace(allocator, io, root_dir, config);
    }

    pub fn openInPlace(
        allocator: Allocator,
        io: Io,
        root_dir: Io.Dir,
        config: Config,
    ) !*Catalog {
        // Best-effort sweep of any `_temp/` left behind by an ungraceful
        // exit. Per-session dirs only ever belong to a process that's
        // currently alive; if we're booting fresh, every previous tenant
        // is gone.
        @import("temp_namespace.zig").sweepStaleTempDirs(io, root_dir);

        // The shared cross-query memory pool. A caller-provided pool (multi-
        // Catalog processes sharing one budget) is adopted as-is; otherwise
        // one is minted from the resolved budget and owned here. The pointer
        // rides in the Config copy handed to every Database and Table below.
        var cfg = config;
        var owned_pool: ?*memory.MemoryPool = null;
        if (cfg.memory_pool == null) {
            const pool_budget = api.autoMemoryBudgetBytes(cfg.memory_budget);
            if (pool_budget > 0) {
                const pool = try allocator.create(memory.MemoryPool);
                pool.* = memory.MemoryPool.init(pool_budget);
                owned_pool = pool;
                cfg.memory_pool = pool;
            }
        }
        errdefer if (owned_pool) |p| allocator.destroy(p);

        const self = try allocator.create(Catalog);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .root_dir = root_dir,
            .config = cfg,
            .owned_pool = owned_pool,
            .udfs = udf_mod.UdfRegistry.init(allocator),
            .sql_fns = udf_mod.SqlFnRegistry.init(allocator),
            .databases = .init(allocator),
        };
        errdefer {
            var it = self.databases.iterator();
            while (it.next()) |entry| entry.value_ptr.*.closeInPlace();
            self.udfs.deinit();
            self.sql_fns.deinit();
            self.databases.deinit();
        }
        try discoverDatabasesOnDisk(allocator, io, root_dir, cfg, &self.databases);
        var it = self.databases.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.catalog = self;
            self.loadSqlFunctions(entry.value_ptr.*) catch {};
        }
        return self;
    }

    /// Load persisted SQL inline table functions from `<db>/_functions/`.
    /// Each file holds one verbatim CREATE FUNCTION statement; re-parsing
    /// it is the single source of truth. Unreadable or unparsable files
    /// are skipped (best-effort — a bad file must not block the open).
    pub fn loadSqlFunctions(self: *Catalog, db: *Database) !void {
        const sqlp = @import("../sql/sql.zig");
        var dir = db.db_dir.openDir(self.io, "_functions", .{ .iterate = true }) catch return;
        defer dir.close(self.io);
        var it = dir.iterate();
        while (try it.next(self.io)) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.name, ".sql")) continue;
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();
            const text = dir.readFileAlloc(self.io, entry.name, arena.allocator(), .limited(1 << 20)) catch continue;
            const op = sqlp.parse(arena.allocator(), text) catch continue;
            if (op.* != .ddl or op.ddl != .create_sql_function) continue;
            const cf = op.ddl.create_sql_function;
            self.registerSqlFunction(db.name, cf, true) catch continue;
        }
    }

    /// Register a SQL function from its parsed CREATE statement and
    /// persist the canonical statement text to `<db>/_functions/<name>.sql`.
    /// `persist=false` on the load-from-disk path.
    pub fn registerSqlFunction(
        self: *Catalog,
        db_name: []const u8,
        cf: @import("../ir/ir.zig").CreateSqlFunction,
        skip_persist: bool,
    ) !void {
        var text: std.ArrayList(u8) = .empty;
        defer text.deinit(self.allocator);
        try text.appendSlice(self.allocator, "CREATE FUNCTION ");
        try text.appendSlice(self.allocator, cf.name);
        try text.append(self.allocator, '(');
        for (cf.param_names, cf.param_types, 0..) |pn, pt, i| {
            if (i > 0) try text.appendSlice(self.allocator, ", ");
            try text.appendSlice(self.allocator, pn);
            try text.append(self.allocator, ' ');
            try appendSqlTypeName(&text, self.allocator, pt);
        }
        try text.appendSlice(self.allocator, ") RETURNS TABLE AS (\n");
        try text.appendSlice(self.allocator, cf.body);
        try text.appendSlice(self.allocator, "\n)");

        try self.sql_fns.register(db_name, .{
            .name = cf.name,
            .param_names = cf.param_names,
            .param_types = cf.param_types,
            .body = cf.body,
            .create_text = text.items,
        }, cf.or_replace or skip_persist);

        if (skip_persist) return;
        const db = self.database(db_name) orelse return Error.DatabaseNotFound;
        var dir = db.db_dir.openDir(self.io, "_functions", .{}) catch
            try db.db_dir.createDirPathOpen(self.io, "_functions", .{});
        defer dir.close(self.io);
        var namebuf: [300]u8 = undefined;
        const fname = try fnFileName(&namebuf, cf.name);
        try dir.writeFile(self.io, .{ .sub_path = fname, .data = text.items });
    }

    pub fn dropSqlFunction(self: *Catalog, db_name: []const u8, name: []const u8) !bool {
        const existed = try self.sql_fns.drop(db_name, name);
        if (!existed) return false;
        if (self.database(db_name)) |db| {
            var dir = db.db_dir.openDir(self.io, "_functions", .{}) catch return true;
            defer dir.close(self.io);
            var namebuf: [300]u8 = undefined;
            const fname = fnFileName(&namebuf, name) catch return true;
            dir.deleteFile(self.io, fname) catch {};
        }
        return true;
    }

    fn fnFileName(buf: []u8, name: []const u8) ![]const u8 {
        if (name.len + 4 > buf.len) return Error.FunctionInvalidDefinition;
        for (name, buf[0..name.len]) |c, *o| o.* = std.ascii.toLower(c);
        @memcpy(buf[name.len..][0..4], ".sql");
        return buf[0 .. name.len + 4];
    }

    fn appendSqlTypeName(list: *std.ArrayList(u8), allocator: Allocator, t: @import("../types.zig").Type) !void {
        switch (t) {
            .int => try list.appendSlice(allocator, "INT"),
            .bigint => try list.appendSlice(allocator, "BIGINT"),
            .boolean => try list.appendSlice(allocator, "BOOLEAN"),
            .varchar => |n| try list.print(allocator, "VARCHAR({d})", .{n}),
            .string => try list.appendSlice(allocator, "STRING"),
            .float => try list.appendSlice(allocator, "FLOAT"),
            .double => try list.appendSlice(allocator, "DOUBLE"),
            .date => try list.appendSlice(allocator, "DATE"),
            .datetime => try list.appendSlice(allocator, "DATETIME"),
            .tinyint => try list.appendSlice(allocator, "TINYINT"),
            .smallint => try list.appendSlice(allocator, "SMALLINT"),
            .largeint => try list.appendSlice(allocator, "LARGEINT"),
            .char => |n| try list.print(allocator, "CHAR({d})", .{n}),
            .decimal64, .decimal128 => |d| try list.print(allocator, "DECIMAL({d},{d})", .{ d.p, d.s }),
            .uuid => try list.appendSlice(allocator, "UUID"),
        }
    }

    pub fn registerScalarUdf(self: *Catalog, desc: udf_mod.ScalarUdf) !void {
        return self.udfs.registerScalar(desc) catch |err| switch (err) {
            udf_mod.Error.FunctionAlreadyExists => Error.FunctionAlreadyExists,
            udf_mod.Error.FunctionInvalidDefinition => Error.FunctionInvalidDefinition,
            else => err,
        };
    }

    pub fn registerAggregateUdf(self: *Catalog, desc: udf_mod.AggregateUdf) !void {
        return self.udfs.registerAggregate(desc) catch |err| switch (err) {
            udf_mod.Error.FunctionAlreadyExists => Error.FunctionAlreadyExists,
            udf_mod.Error.FunctionInvalidDefinition => Error.FunctionInvalidDefinition,
            else => err,
        };
    }

    /// Scan `root_dir` for subdirectories and adopt each one as a Database
    /// in `out_map`. Skips reserved names (currently just `_temp/`). Used
    /// at Catalog open to surface previously-persisted databases.
    fn discoverDatabasesOnDisk(
        allocator: Allocator,
        io: Io,
        root_dir: Io.Dir,
        config: Config,
        out_map: *std.StringHashMap(*Database),
    ) !void {
        const temp_name = @import("temp_namespace.zig").temp_root_dir_name;
        // The root_dir handle may not have iterate-access; try opening a
        // sibling handle with iterate. If that fails (e.g. permissions),
        // skip discovery — non-fatal, callers can still create databases
        // by name.
        var iter_dir = root_dir.openDir(io, ".", .{ .iterate = true }) catch return;
        defer iter_dir.close(io);
        var dir_it = iter_dir.iterate();
        while (try dir_it.next(io)) |entry| {
            if (entry.kind != .directory) continue;
            if (std.mem.eql(u8, entry.name, temp_name)) continue;
            if (out_map.get(entry.name) != null) continue;
            // Database.create is idempotent on existing on-disk state: it
            // re-opens the dir, re-opens the `public` schema, and the
            // catalog pointer is fixed up by the caller.
            const db = Database.create(allocator, io, root_dir, entry.name, config, undefined) catch continue;
            try out_map.put(db.name, db);
        }
    }

    pub fn close(self: *Catalog) void {
        var it = self.databases.iterator();
        while (it.next()) |entry| entry.value_ptr.*.closeInPlace();
        self.udfs.deinit();
        self.sql_fns.deinit();
        self.databases.deinit();
        const allocator = self.allocator;
        if (self.owned_pool) |p| allocator.destroy(p);
        allocator.destroy(self);
    }

    pub fn database(self: *Catalog, name: []const u8) ?*Database {
        self.databases_mutex.lockUncancelable(self.io);
        defer self.databases_mutex.unlock(self.io);
        return self.databases.get(name);
    }

    pub fn createDatabase(self: *Catalog, name: []const u8) !*Database {
        self.databases_mutex.lockUncancelable(self.io);
        defer self.databases_mutex.unlock(self.io);

        if (self.databases.get(name) != null) return Error.DatabaseAlreadyExists;
        if (self.root_dir.openDir(self.io, name, .{})) |probe_| {
            var probe = probe_;
            probe.close(self.io);
            return Error.DatabaseAlreadyExists;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }

        const db = try Database.create(self.allocator, self.io, self.root_dir, name, self.config, self);
        errdefer db.closeInPlace();
        try self.databases.put(db.name, db);
        return db;
    }

    /// Get-or-create. Used by the back-compat `Database.open` shim to
    /// adopt an existing on-disk database directory without erroring.
    pub fn createOrOpenDatabase(self: *Catalog, name: []const u8) !*Database {
        self.databases_mutex.lockUncancelable(self.io);
        defer self.databases_mutex.unlock(self.io);

        if (self.databases.get(name)) |existing| return existing;
        const db = try Database.create(self.allocator, self.io, self.root_dir, name, self.config, self);
        errdefer db.closeInPlace();
        try self.databases.put(db.name, db);
        return db;
    }

    pub fn dropDatabase(self: *Catalog, name: []const u8) !void {
        self.databases_mutex.lockUncancelable(self.io);
        const maybe = self.databases.fetchRemove(name);
        self.databases_mutex.unlock(self.io);

        if (maybe) |entry| {
            entry.value.closeInPlace();
        } else {
            var probe = self.root_dir.openDir(self.io, name, .{}) catch |err| switch (err) {
                error.FileNotFound => return Error.DatabaseNotFound,
                else => return err,
            };
            probe.close(self.io);
        }

        try self.root_dir.deleteTree(self.io, name);
    }

    pub fn listDatabases(self: *Catalog, allocator: Allocator) ![][]u8 {
        self.databases_mutex.lockUncancelable(self.io);
        defer self.databases_mutex.unlock(self.io);

        const out = try allocator.alloc([]u8, self.databases.count());
        errdefer allocator.free(out);
        var i: usize = 0;
        errdefer for (out[0..i]) |s| allocator.free(s);
        var it = self.databases.keyIterator();
        while (it.next()) |k| : (i += 1) {
            out[i] = try allocator.dupe(u8, k.*);
        }
        return out;
    }

    /// Walk every database (and every schema beneath each) and run one
    /// flush sweep. Errors from individual sweeps are swallowed — the
    /// background loop keeps running.
    pub fn backgroundFlushSweep(self: *Catalog) !void {
        const names = try snapshot.snapshotMapKeys(self.allocator, self.io, &self.databases_mutex, self.databases);
        defer snapshot.freeNames(self.allocator, names);
        for (names) |name| {
            const db = self.database(name) orelse continue;
            db.backgroundFlushSweep() catch {};
        }
    }

    pub fn runBackgroundFlusher(
        self: *Catalog,
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

    /// Returns true if any database merged a group this sweep.
    pub fn backgroundCompactSweep(self: *Catalog) !bool {
        const names = try snapshot.snapshotMapKeys(self.allocator, self.io, &self.databases_mutex, self.databases);
        defer snapshot.freeNames(self.allocator, names);
        var worked = false;
        for (names) |name| {
            const db = self.database(name) orelse continue;
            if (db.backgroundCompactSweep() catch false) worked = true;
        }
        return worked;
    }

    pub fn runBackgroundCompactor(
        self: *Catalog,
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
