//! Schema — a namespace inside a Database that owns a set of Tables.
//! v2: third level of the Catalog → Database → Schema → Table hierarchy.
//! All per-table coordination (the tables map, the DDL mutex, background
//! sweeps) lives here.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const types = @import("../types.zig");
const TableSchema = types.TableSchema;

const api = @import("api.zig");
const Config = api.Config;
const Error = api.Error;
const TableOptions = api.TableOptions;
const OpenOptions = api.OpenOptions;
const AlterOp = api.AlterOp;
const Table = api.Table;

const schemaFingerprint = api.schemaFingerprint;

const snapshot = @import("../util/snapshot.zig");

pub const Schema = struct {
    allocator: Allocator,
    io: Io,
    name: []u8,
    schema_dir: Io.Dir,
    config: Config,
    tables: std.StringHashMap(*Table),
    /// Back-reference; not owning. Set by Database.createSchema.
    database: ?*@import("database.zig").Database = null,

    /// Guards `tables` map iteration from a background flusher caller.
    tables_mutex: Io.Mutex = .init,

    pub fn open(
        allocator: Allocator,
        io: Io,
        parent_dir: Io.Dir,
        name: []const u8,
        config: Config,
    ) !*Schema {
        const schema_dir = try parent_dir.createDirPathOpen(io, name, .{});
        errdefer {
            var d = schema_dir;
            d.close(io);
        }

        const name_copy = try allocator.dupe(u8, name);
        errdefer allocator.free(name_copy);

        const self = try allocator.create(Schema);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .name = name_copy,
            .schema_dir = schema_dir,
            .config = config,
            .tables = .init(allocator),
        };
        return self;
    }

    pub fn close(self: *Schema) void {
        var it = self.tables.iterator();
        while (it.next()) |entry| entry.value_ptr.*.close();
        self.tables.deinit();
        self.schema_dir.close(self.io);
        const allocator = self.allocator;
        allocator.free(self.name);
        allocator.destroy(self);
    }

    /// One sweep of the background flush check. Looks up each known table
    /// by name under `tables_mutex` and atomically grabs its `ddl_lock`
    /// shared before releasing the map lock — this prevents a concurrent
    /// `dropTable` from freeing the Table while we still hold a pointer
    /// to it. Then calls `tryBackgroundFlush` (non-blocking on the per-
    /// table write mutex).
    pub fn backgroundFlushSweep(self: *Schema) !void {
        const names = try snapshot.snapshotMapKeys(self.allocator, self.io, &self.tables_mutex, self.tables);
        defer snapshot.freeNames(self.allocator, names);

        for (names) |name| {
            if (self.acquireTableShared(name)) |t| {
                defer t.ddl_lock.unlockShared(t.io);
                t.tryBackgroundFlush() catch {};
            }
        }
    }

    /// Re-resolve `name` under `tables_mutex` AND grab its `ddl_lock`
    /// shared atomically. Returns `null` if the table has been dropped
    /// since the snapshot. Caller MUST `t.ddl_lock.unlockShared` when done.
    fn acquireTableShared(self: *Schema, name: []const u8) ?*Table {
        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);
        const t = self.tables.get(name) orelse return null;
        t.ddl_lock.lockSharedUncancelable(t.io);
        return t;
    }

    pub fn runBackgroundFlusher(
        self: *Schema,
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

    /// One sweep of the background compaction check. Same coordination
    /// pattern as `backgroundFlushSweep`: name-snapshot + atomic shared
    /// lock acquisition so a concurrent drop can't free the Table out
    /// from under us.
    pub fn backgroundCompactSweep(self: *Schema) !void {
        const names = try snapshot.snapshotMapKeys(self.allocator, self.io, &self.tables_mutex, self.tables);
        defer snapshot.freeNames(self.allocator, names);

        const min_segs = self.config.compact_min_segments;
        const tomb_thresh = self.config.compact_tombstone_threshold;
        for (names) |name| {
            if (self.acquireTableShared(name)) |t| {
                defer t.ddl_lock.unlockShared(t.io);
                t.tryBackgroundCompact(min_segs, tomb_thresh) catch {};
            }
        }
    }

    pub fn runBackgroundCompactor(
        self: *Schema,
        sleeper_io: Io,
        poll_ms: u32,
        should_stop: *std.atomic.Value(bool),
    ) void {
        const duration: Io.Duration = .fromMilliseconds(@intCast(poll_ms));
        while (!should_stop.load(.acquire)) {
            Io.sleep(sleeper_io, duration, .awake) catch return;
            if (should_stop.load(.acquire)) return;
            self.backgroundCompactSweep() catch {};
        }
    }

    /// Create-or-open a table with the given schema. If the table already
    /// exists on disk, its persisted schema must match the one passed here.
    pub fn table(
        self: *Schema,
        name: []const u8,
        table_schema: TableSchema,
        options: TableOptions,
    ) !*Table {
        try table_schema.validate();

        {
            self.tables_mutex.lockUncancelable(self.io);
            defer self.tables_mutex.unlock(self.io);
            if (self.tables.get(name)) |existing| {
                if (schemaFingerprint(table_schema) != existing.schema_fingerprint) {
                    return Error.SchemaMismatch;
                }
                return existing;
            }
        }

        const t = try Table.open(
            self.allocator,
            self.io,
            self.schema_dir,
            name,
            table_schema,
            self.config,
            options.row_group_size orelse self.config.row_group_size,
        );
        errdefer t.close();

        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);
        try self.tables.put(t.name, t);
        return t;
    }

    /// Open an existing table by name. The schema is loaded from the
    /// persisted `schema.bin`. Errors if the table doesn't exist.
    pub fn openTable(
        self: *Schema,
        name: []const u8,
        options: OpenOptions,
    ) !*Table {
        {
            self.tables_mutex.lockUncancelable(self.io);
            defer self.tables_mutex.unlock(self.io);
            if (self.tables.get(name)) |existing| return existing;
        }

        const t = try Table.open(
            self.allocator,
            self.io,
            self.schema_dir,
            name,
            null,
            self.config,
            options.row_group_size orelse self.config.row_group_size,
        );
        errdefer t.close();

        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);
        try self.tables.put(t.name, t);
        return t;
    }

    /// Drop a table by name. Removes it from the in-memory map, waits for
    /// any in-flight scans to finish (via the table's exclusive ddl_lock),
    /// then closes and deletes the directory tree from disk.
    pub fn dropTable(self: *Schema, name: []const u8) !void {
        self.tables_mutex.lockUncancelable(self.io);
        const maybe_existing = self.tables.fetchRemove(name);
        self.tables_mutex.unlock(self.io);

        if (maybe_existing) |entry| {
            const t = entry.value;
            t.ddl_lock.lockUncancelable(t.io);
            t.close();
        } else {
            var probe = self.schema_dir.openDir(self.io, name, .{}) catch |err| switch (err) {
                error.FileNotFound => return Error.TableNotFound,
                else => return err,
            };
            probe.close(self.io);
        }

        try self.schema_dir.deleteTree(self.io, name);
    }

    /// Apply schema operations (`.add`, `.drop`, `.rename` columns) to a
    /// table. See `alter.execAlter` for orchestration details.
    pub fn alterTable(self: *Schema, name: []const u8, ops: []const AlterOp) !void {
        self.tables_mutex.lockUncancelable(self.io);
        const t = self.tables.get(name) orelse {
            self.tables_mutex.unlock(self.io);
            return Error.TableNotFound;
        };
        self.tables_mutex.unlock(self.io);

        try @import("alter.zig").execAlter(self, t, ops);
    }

    /// Rename a table. Renames the on-disk directory, updates the in-memory
    /// map key, and updates the Table's internal name string.
    pub fn renameTable(self: *Schema, old_name: []const u8, new_name: []const u8) !void {
        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);

        if (self.tables.get(new_name) != null) return Error.TableAlreadyExists;
        if (self.schema_dir.openDir(self.io, new_name, .{})) |probe_| {
            var probe = probe_;
            probe.close(self.io);
            return Error.TableAlreadyExists;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }

        const t = self.tables.get(old_name) orelse return Error.TableNotFound;

        t.ddl_lock.lockUncancelable(t.io);
        defer t.ddl_lock.unlock(t.io);
        t.mutex.lockUncancelable(t.io);
        defer t.mutex.unlock(t.io);

        t.segments_dir.close(t.io);
        t.table_dir.close(t.io);

        try self.schema_dir.rename(old_name, self.schema_dir, new_name, self.io);

        t.table_dir = try self.schema_dir.openDir(self.io, new_name, .{});
        t.segments_dir = try t.table_dir.openDir(t.io, "segments", .{});

        const new_owned = try self.allocator.dupe(u8, new_name);
        const old_owned = t.name;
        t.name = new_owned;

        _ = self.tables.remove(old_name);
        try self.tables.put(t.name, t);

        self.allocator.free(old_owned);
    }

    /// List the names of every table in this schema. Caller frees the
    /// returned slice and each name with `allocator`.
    pub fn listTables(self: *Schema, allocator: Allocator) ![][]u8 {
        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);

        const out = try allocator.alloc([]u8, self.tables.count());
        errdefer allocator.free(out);
        var i: usize = 0;
        errdefer for (out[0..i]) |s| allocator.free(s);
        var it = self.tables.keyIterator();
        while (it.next()) |k| : (i += 1) {
            out[i] = try allocator.dupe(u8, k.*);
        }
        return out;
    }
};
