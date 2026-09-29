//! Schema — a namespace inside a Database that owns a set of Tables.
//! v2: third level of the Catalog → Database → Schema → Table hierarchy.
//! All per-table coordination (the tables map, the DDL mutex, background
//! sweeps) lives here.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const types = @import("../types.zig");
const TableSchema = types.TableSchema;
const storage = @import("../storage/storage.zig");

const api = @import("api.zig");
const Config = api.Config;
const Error = api.Error;
const TableOptions = api.TableOptions;
const OpenOptions = api.OpenOptions;
const AlterOp = api.AlterOp;
const Table = api.Table;
const Database = api.Database;
const Catalog = api.Catalog;

const schemaFingerprint = api.schemaFingerprint;

const snapshot = @import("../util/snapshot.zig");
const StatementGate = @import("../util/statement_gate.zig").StatementGate;
const compact = @import("compact.zig");
const alter = @import("alter.zig");

/// How a background sweep finds a schema again. Between tables a sweep holds
/// no statement lease, so a DROP SCHEMA or DROP DATABASE may free the schema
/// meanwhile; every table step re-resolves it under a fresh lease.
pub const SweepRoute = union(enum) {
    /// The caller keeps the schema alive for the whole sweep.
    schema: *Schema,
    /// The caller keeps the database alive for the whole sweep.
    database: struct { database: *Database, schema: []const u8 },
    catalog: struct { catalog: *Catalog, database: []const u8, schema: []const u8 },

    fn resolve(route: SweepRoute) ?*Schema {
        return switch (route) {
            .schema => |s| s,
            .database => |r| r.database.schema(r.schema),
            .catalog => |r| (r.catalog.database(r.database) orelse return null).schema(r.schema),
        };
    }

    fn gate(route: SweepRoute) ?*StatementGate {
        return switch (route) {
            .schema => |s| s.config.statement_gate,
            .database => |r| r.database.config.statement_gate,
            .catalog => |r| &r.catalog.statement_gate,
        };
    }
};

const SweepNames = struct { allocator: Allocator, names: [][]u8 };

fn sweepTableNames(route: SweepRoute, comptime which: enum { open, on_disk }) !?SweepNames {
    const lease = try acquireSweepLease(route.gate());
    defer if (lease) |l| l.release();
    const s = route.resolve() orelse return null;
    const names = switch (which) {
        .open => try snapshot.snapshotMapKeys(s.allocator, s.io, &s.tables_mutex, &s.tables),
        .on_disk => try s.listTables(s.allocator),
    };
    return .{ .allocator = s.allocator, .names = names };
}

fn acquireSweepLease(gate: ?*StatementGate) !?StatementGate.Lease {
    return if (gate) |g| try g.acquire(false) else null;
}

/// Held for a whole sweep so the catalog outlives it: a sweep holds no
/// statement lease while it merges, so shutdown would not otherwise wait for
/// it. Shutdown still waits at most one table step, because the next lease
/// fails once the catalog is closing.
pub fn retainSweepLifetime(gate: ?*StatementGate) !?StatementGate.LifetimeLease {
    return if (gate) |g| try g.retainAllocator() else null;
}

/// Directory names under this prefix hold a table that CREATE TABLE ... AS
/// SELECT is still filling: no table is created, renamed, listed or opened
/// under one, and a schema deletes any a crash left as it opens.
const table_build_prefix = "__ctas_";
/// The prefix and the decimal digits of any u64.
const table_build_dir_name_max = table_build_prefix.len + 20;

fn isReservedTableName(name: []const u8) bool {
    return alter.isReservedTableName(name) or std.mem.startsWith(u8, name, table_build_prefix);
}

/// A crash ends a build's statement with it, before anything was published,
/// so every build directory a schema finds as it opens is garbage.
fn deleteTableBuildLeftovers(allocator: Allocator, io: Io, schema_dir: Io.Dir) !void {
    var names: std.ArrayList([]u8) = .empty;
    defer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }
    // Collected first: the deletes below remove entries of the directory
    // being walked.
    var it = schema_dir.iterate();
    while (try it.next(io)) |entry| {
        if (entry.kind != .directory or !std.mem.startsWith(u8, entry.name, table_build_prefix)) continue;
        const name = try allocator.dupe(u8, entry.name);
        errdefer allocator.free(name);
        try names.append(allocator, name);
    }
    // No name resolves to a build directory, so one a refusal keeps costs
    // only disk until the next open.
    for (names.items) |name| {
        storage.retryTransientWindowsRefusal(io, Io.Dir.deleteTree, .{ schema_dir, io, name }) catch {};
    }
}

/// A table no other statement can resolve: its directory has a reserved name
/// and the schema's map does not hold it. CREATE TABLE ... AS SELECT fills
/// one and publishes it only once its query has succeeded, so a failed query
/// frees a table nobody else can hold (see `Schema.dropTable`). The build
/// holds a statement lease until `deinit`, so its schema outlives it.
pub const TableBuild = struct {
    schema: *Schema,
    table: *Table,
    /// The name it publishes under; the table takes it on publish.
    name: []u8,
    published: bool = false,
    lease: ?StatementGate.Lease,
    dir_name_buf: [table_build_dir_name_max]u8,
    dir_name_len: usize,

    fn dirName(build: *const TableBuild) []const u8 {
        return build.dir_name_buf[0..build.dir_name_len];
    }

    /// Publish the table under its name, with every row in place. A table or
    /// directory that took the name meanwhile fails it with
    /// TableAlreadyExists, and the build stays unpublished.
    pub fn publish(build: *TableBuild) !void {
        if (build.published) return Error.TableAlreadyExists;
        const s = build.schema;
        const t = build.table;
        // The log keeps rows until a flush and lives inside the directory the
        // rename moves; flushed, it holds nothing live and starts afresh in
        // the published directory.
        const had_wal = t.wal != null;
        if (had_wal) try t.flush();

        s.tables_mutex.lockUncancelable(s.io);
        defer s.tables_mutex.unlock(s.io);
        if (s.dropping.contains(build.name)) return Error.TableBusy;
        if (s.tables.contains(build.name) or try s.nameTakenOnDisk(build.name)) return Error.TableAlreadyExists;
        try s.tables.ensureUnusedCapacity(1);

        // A refused rename moved nothing, so the build is still whole to
        // discard.
        closeTableHandles(t);
        try storage.retryTransientWindowsRefusal(s.io, Io.Dir.rename, .{ s.schema_dir, build.dirName(), s.schema_dir, build.name, s.io });
        s.allocator.free(t.name);
        t.name = build.name;
        s.tables.putAssumeCapacity(t.name, t);
        build.published = true;
        // The table stands under its name on disk now. As in renameTable, a
        // failure below leaves it without directory handles, so fence it
        // until reopen.
        errdefer t.requireRecovery();
        try s.reopenTableDirs(t, t.name, had_wal);
        if (t.syncEnabled()) storage.syncDirectory(s.io, s.schema_dir) catch return Error.DurabilityUncertain;
    }

    /// Release the build. An unpublished table goes with it, directory and
    /// all.
    pub fn deinit(build: *TableBuild) void {
        const s = build.schema;
        if (!build.published) {
            build.table.close();
            // No name resolves to the directory, so one a refusal keeps
            // costs only disk until the schema next opens.
            storage.retryTransientWindowsRefusal(s.io, Io.Dir.deleteTree, .{ s.schema_dir, s.io, build.dirName() }) catch {};
            s.allocator.free(build.name);
        }
        if (build.lease) |lease| lease.release();
        build.* = undefined;
    }
};

/// Windows refuses to rename a directory while a handle inside it is open:
/// the log's, the directories' own, and cached segment files'. Close them
/// all; `Schema.reopenTableDirs` restores them.
fn closeTableHandles(t: *Table) void {
    if (t.wal) |*w| w.deinit();
    t.wal = null;
    t.segments_dir.close(t.io);
    t.table_dir.close(t.io);
    t.dirs_open = false;
    // Scans reopen them from the directory's new name.
    t.seg_handles.clear(t.allocator);
}

pub const Schema = struct {
    allocator: Allocator,
    io: Io,
    name: []u8,
    schema_dir: Io.Dir,
    config: Config,
    tables: std.StringHashMap(*Table),
    dropping: std.StringHashMapUnmanaged(void) = .empty,
    /// Back-reference; not owning. Set by Database.createSchema.
    database: ?*@import("database.zig").Database = null,

    /// Guards `tables` map iteration from a background flusher caller.
    tables_mutex: Io.Mutex = .init,
    /// Numbers `TableBuild` directories.
    build_seq: std.atomic.Value(u64) = .init(0),

    pub fn open(
        allocator: Allocator,
        io: Io,
        parent_dir: Io.Dir,
        name: []const u8,
        config: Config,
    ) !*Schema {
        const schema_dir = try parent_dir.createDirPathOpen(io, name, .{
            .open_options = .{ .iterate = true },
        });
        errdefer {
            var d = schema_dir;
            d.close(io);
        }
        try alter.recoverInterruptedAlters(allocator, io, schema_dir);
        try deleteTableBuildLeftovers(allocator, io, schema_dir);

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
        while (it.next()) |entry| {
            const t = entry.value_ptr.*;
            // A background merge holds no statement lease, so the gate a
            // drop or shutdown holds does not exclude it. Stop it, as
            // dropTable does, before freeing the table under it.
            t.lockCompactPreempting();
            // Persist any memtable residue before teardown, WAL or not. A
            // WAL-backed table could lean on replay instead, but that makes
            // restart durability hinge on the log being found where the next
            // process looks (2026-08-29: it wasn't — see execAlter), while a
            // flushed table has nothing left to lose. Best-effort — close
            // must proceed regardless, and on failure the log still carries
            // the rows. The drop paths delete the table dir right after
            // close, so a wasted flush there is harmless.
            t.flush() catch |err| std.debug.print(
                "thindb: shutdown flush failed on table '{s}': {s}\n",
                .{ t.name, @errorName(err) },
            );
            t.close();
        }
        self.tables.deinit();
        self.dropping.deinit(self.allocator);
        self.schema_dir.close(self.io);
        const allocator = self.allocator;
        allocator.free(self.name);
        allocator.destroy(self);
    }

    /// One sweep of the background flush check over this schema's open
    /// tables. The caller keeps the schema alive for the call.
    pub fn backgroundFlushSweep(self: *Schema) !void {
        const lifetime = try retainSweepLifetime(self.config.statement_gate);
        defer if (lifetime) |l| l.release();
        try flushSweep(.{ .schema = self });
    }

    /// Flush every open table of the routed schema that is due. Each table
    /// holds its own statement lease, taken only while the table is resolved
    /// and flushed: DDL and XA COMMIT wait for one table's flush, not the
    /// sweep. The flush must stay under the lease, because an XA COMMIT's
    /// rollback restores its tables' manifests from the journal and would
    /// drop a segment a concurrent flush had published.
    pub fn flushSweep(route: SweepRoute) !void {
        const names = (try sweepTableNames(route, .open)) orelse return;
        defer snapshot.freeNames(names.allocator, names.names);
        for (names.names) |name| {
            const lease = try acquireSweepLease(route.gate());
            defer if (lease) |l| l.release();
            const s = route.resolve() orelse return;
            if (s.acquireTableShared(name)) |t| {
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

    /// Re-resolve `name` under `tables_mutex` AND grab its `compact_lock`
    /// (non-blocking) atomically. Returns `null` if the table was dropped
    /// since the snapshot or a compaction/DDL already holds the lock.
    /// `compact_lock` (not `ddl_lock` shared) is the compaction-liveness
    /// guard: held for the whole compaction, it lets the commit phase take
    /// `ddl_lock` exclusive without self-deadlocking, while still blocking
    /// a concurrent drop/alter/rename from running mid-merge. Caller MUST
    /// `t.compact_lock.unlock` when done.
    fn acquireTableForCompact(self: *Schema, name: []const u8) ?*Table {
        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);
        const t = self.tables.get(name) orelse return null;
        if (!t.compact_lock.tryLock()) return null;
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

    /// One sweep of the background compaction check over this schema. The
    /// caller keeps the schema alive for the call. Returns true if any table
    /// merged a group (so the background loop can keep draining without
    /// sleeping).
    pub fn backgroundCompactSweep(self: *Schema) !bool {
        const lifetime = try retainSweepLifetime(self.config.statement_gate);
        defer if (lifetime) |l| l.release();
        return compactSweep(.{ .schema = self });
    }

    /// Run one background compaction step on every table of the routed
    /// schema. Returns true if any table merged a group.
    pub fn compactSweep(route: SweepRoute) !bool {
        // Discover every table on disk, not just those a query has already
        // opened: the background compactor must monitor freshly-loaded tables
        // (e.g. a bulk import done by another process, or any table on a
        // just-started server) without needing client activity first.
        // `listTables` returns opened + on-disk names; opening each adopts it
        // into `tables` so it stays monitored from here on.
        const names = (try sweepTableNames(route, .on_disk)) orelse return false;
        defer snapshot.freeNames(names.allocator, names.names);
        var worked = false;
        for (names.names) |name| {
            if (try compactTableStep(route, name)) worked = true;
        }
        return worked;
    }

    /// Pick, merge and commit one background compaction of `name`. Only the
    /// pick holds a statement lease; a merge can run for minutes, and a lease
    /// held across it made every DDL and XA COMMIT (and, the gate preferring
    /// writers, every statement queued behind them) wait for it. The merge
    /// and its commit run under the table's `compact_lock` alone, which every
    /// path that frees or rewrites the table takes first: DROP TABLE, ALTER,
    /// RENAME, TRUNCATE, XA COMMIT and `Schema.close` (DROP SCHEMA, DROP
    /// DATABASE, shutdown). Returns whether a merge landed.
    fn compactTableStep(route: SweepRoute, name: []const u8) !bool {
        const t, const group = pick: {
            const lease = try acquireSweepLease(route.gate());
            defer if (lease) |l| l.release();
            const s = route.resolve() orelse return false;
            _ = s.openTable(name, .{}) catch return false;
            const t = s.acquireTableForCompact(name) orelse return false;
            const min_segs = s.config.compact_min_segments;
            const tomb_thresh = s.config.compact_tombstone_threshold;
            const group = (t.pickBackgroundCompaction(min_segs, tomb_thresh) catch null) orelse {
                t.compact_lock.unlock(t.io);
                return false;
            };
            break :pick .{ t, group };
        };
        // Runs last: once `compact_lock` is free a waiting drop may free `t`.
        defer t.compact_lock.unlock(t.io);
        defer t.allocator.free(group);
        return compact.mergeInBackground(t, group, compact.background_commit_wait) catch false;
    }

    pub fn runBackgroundCompactor(
        self: *Schema,
        sleeper_io: Io,
        poll_ms: u32,
        should_stop: *std.atomic.Value(bool),
    ) void {
        const duration: Io.Duration = .fromMilliseconds(@intCast(poll_ms));
        while (!should_stop.load(.acquire)) {
            // Keep sweeping back-to-back while there's a backlog to drain; only
            // sleep the poll interval once a sweep finds nothing to merge.
            const worked = self.backgroundCompactSweep() catch false;
            if (should_stop.load(.acquire)) return;
            if (!worked) Io.sleep(sleeper_io, duration, .awake) catch return;
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
        const statement_lease = if (self.config.statement_gate) |gate| try gate.acquire(false) else null;
        defer if (statement_lease) |lease| lease.release();
        try table_schema.validate();
        if (isReservedTableName(name)) return Error.ReservedTableName;

        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);
        if (self.dropping.contains(name)) return Error.TableBusy;
        if (self.tables.get(name)) |existing| {
            if (schemaFingerprint(table_schema) != existing.schema_fingerprint) {
                return Error.SchemaMismatch;
            }
            return existing;
        }

        // A table this call creates goes again if opening it fails part way,
        // so a failed CREATE TABLE leaves no table behind.
        const creating = !try self.tableOnDisk(name);
        errdefer if (creating) self.schema_dir.deleteTree(self.io, name) catch {};
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

        try self.tables.put(t.name, t);
        return t;
    }

    /// Whether `name` has a table directory holding its schema.
    fn tableOnDisk(self: *Schema, name: []const u8) !bool {
        var dir = self.schema_dir.openDir(self.io, name, .{}) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        defer dir.close(self.io);
        dir.access(self.io, "schema.bin", .{ .read = true }) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        return true;
    }

    /// Whether any directory holds `name`: a rename onto it fails, or on
    /// POSIX replaces an empty one.
    fn nameTakenOnDisk(self: *Schema, name: []const u8) !bool {
        var dir = self.schema_dir.openDir(self.io, name, .{}) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        dir.close(self.io);
        return true;
    }

    /// Begin a table to publish as `name` once it is filled (see
    /// `TableBuild`). Fails at once when a table holds the name, opened or
    /// only on disk.
    pub fn beginTableBuild(
        self: *Schema,
        name: []const u8,
        table_schema: TableSchema,
        options: TableOptions,
    ) !TableBuild {
        if (isReservedTableName(name)) return Error.ReservedTableName;
        try table_schema.validate();
        const lease = if (self.config.statement_gate) |gate| try gate.acquire(false) else null;
        errdefer if (lease) |l| l.release();
        {
            self.tables_mutex.lockUncancelable(self.io);
            defer self.tables_mutex.unlock(self.io);
            if (self.dropping.contains(name)) return Error.TableBusy;
            if (self.tables.contains(name)) return Error.TableAlreadyExists;
        }
        if (try self.tableOnDisk(name)) return Error.TableAlreadyExists;

        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);
        var build: TableBuild = .{
            .schema = self,
            .table = undefined,
            .name = owned_name,
            .lease = lease,
            .dir_name_buf = undefined,
            .dir_name_len = 0,
        };
        // A directory under a fresh number is one a delete left behind.
        while (true) {
            const seq = self.build_seq.fetchAdd(1, .monotonic);
            build.dir_name_len = (try std.fmt.bufPrint(&build.dir_name_buf, table_build_prefix ++ "{d}", .{seq})).len;
            if (!try self.nameTakenOnDisk(build.dirName())) break;
        }
        errdefer self.schema_dir.deleteTree(self.io, build.dirName()) catch {};
        build.table = try Table.open(
            self.allocator,
            self.io,
            self.schema_dir,
            build.dirName(),
            table_schema,
            self.config,
            options.row_group_size orelse self.config.row_group_size,
        );
        return build;
    }

    /// Open an existing table by name. The schema is loaded from the
    /// persisted `schema.bin`. Errors if the table doesn't exist.
    pub fn openTable(
        self: *Schema,
        name: []const u8,
        options: OpenOptions,
    ) !*Table {
        if (isReservedTableName(name)) return Error.TableNotFound;
        const statement_lease = if (self.config.statement_gate) |gate| try gate.acquire(false) else null;
        defer if (statement_lease) |lease| lease.release();
        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);
        if (self.dropping.contains(name)) return Error.TableBusy;
        if (self.tables.get(name)) |existing| return existing;

        var probe = self.schema_dir.openDir(self.io, name, .{}) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => return Error.TableNotFound,
            else => return err,
        };
        probe.access(self.io, "schema.bin", .{ .read = true }) catch |err| {
            probe.close(self.io);
            return switch (err) {
                error.FileNotFound => Error.TableNotFound,
                else => err,
            };
        };
        probe.close(self.io);

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

        try self.tables.put(t.name, t);
        return t;
    }

    /// Drop a table by name. Removes it from the in-memory map, waits for
    /// any in-flight scans to finish (via the table's exclusive ddl_lock),
    /// then closes and deletes the directory tree from disk.
    ///
    /// A Table is freed only when no other statement can hold a reference
    /// to it: under the exclusive statement lease, which waits out every
    /// statement that could have resolved it, or because it was never
    /// published where another statement could resolve it (a create that
    /// failed, a `TableBuild`). `ddl_lock` alone is not enough: a reader
    /// resolves the table first and locks it later, and would then lock a
    /// freed one. Called under a shared lease, the drop fails with TableBusy.
    pub fn dropTable(self: *Schema, name: []const u8) !void {
        if (isReservedTableName(name)) return Error.TableNotFound;
        const statement_lease = if (self.config.statement_gate) |gate| try gate.acquire(true) else null;
        defer if (statement_lease) |lease| lease.release();
        const owned_name = try self.allocator.dupe(u8, name);
        defer self.allocator.free(owned_name);
        self.tables_mutex.lockUncancelable(self.io);
        if (self.dropping.contains(owned_name)) {
            self.tables_mutex.unlock(self.io);
            return Error.TableBusy;
        }
        self.dropping.put(self.allocator, owned_name, {}) catch |err| {
            self.tables_mutex.unlock(self.io);
            return err;
        };
        defer {
            self.tables_mutex.lockUncancelable(self.io);
            _ = self.dropping.remove(owned_name);
            self.tables_mutex.unlock(self.io);
        }
        const maybe_existing = self.tables.fetchRemove(name);
        self.tables_mutex.unlock(self.io);

        if (maybe_existing) |entry| {
            const t = entry.value;
            // compact_lock before ddl_lock (global order): an in-flight
            // compaction holds no ddl_lock during its merge, so stop it.
            t.lockCompactPreempting();
            t.ddl_lock.lockUncancelable(t.io);
            t.close();
        } else {
            var probe = self.schema_dir.openDir(self.io, name, .{}) catch |err| switch (err) {
                error.FileNotFound => return Error.TableNotFound,
                else => return err,
            };
            probe.close(self.io);
        }

        // An original an ALTER set aside but failed to delete goes first:
        // the next open would otherwise put it back under the dropped name.
        try alter.deleteAlterLeftovers(self.io, self.schema_dir, owned_name);
        try self.schema_dir.deleteTree(self.io, owned_name);
    }

    /// Apply schema operations (`.add`, `.drop`, `.rename` columns) to a
    /// table. See `alter.execAlter` for orchestration details.
    ///
    /// Exclusive, as `dropTable`: the rewrite frees the schema a statement
    /// that resolved the table may still read.
    pub fn alterTable(self: *Schema, name: []const u8, ops: []const AlterOp) !void {
        const statement_lease = if (self.config.statement_gate) |gate| try gate.acquire(true) else null;
        defer if (statement_lease) |lease| lease.release();
        self.tables_mutex.lockUncancelable(self.io);
        const t = self.tables.get(name) orelse {
            self.tables_mutex.unlock(self.io);
            return Error.TableNotFound;
        };
        self.tables_mutex.unlock(self.io);

        try alter.execAlter(self, t, ops);
    }

    /// Rename a table. Renames the on-disk directory, updates the in-memory
    /// map key, and updates the Table's internal name string.
    ///
    /// Exclusive, as `dropTable`: the rename frees the name a statement that
    /// resolved the table may still read.
    pub fn renameTable(self: *Schema, old_name: []const u8, new_name: []const u8) !void {
        if (isReservedTableName(old_name)) return Error.TableNotFound;
        if (isReservedTableName(new_name)) return Error.ReservedTableName;
        const statement_lease = if (self.config.statement_gate) |gate| try gate.acquire(true) else null;
        defer if (statement_lease) |lease| lease.release();
        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);

        if (self.dropping.contains(old_name) or self.dropping.contains(new_name)) return Error.TableBusy;

        if (self.tables.get(new_name) != null or try self.nameTakenOnDisk(new_name)) return Error.TableAlreadyExists;

        const t = self.tables.get(old_name) orelse return Error.TableNotFound;

        t.lockCompactPreempting();
        defer t.compact_lock.unlock(t.io);
        t.ddl_lock.lockUncancelable(t.io);
        defer t.ddl_lock.unlock(t.io);
        t.mutex.lockUncancelable(t.io);
        defer t.mutex.unlock(t.io);
        // A fenced table may own no directory handles, or have no directory
        // under its name.
        try t.ensureUsable();
        // As in dropTable: an original left set aside would come back under
        // the old name at the next open.
        try alter.deleteAlterLeftovers(self.io, self.schema_dir, old_name);

        // Flush residue so the log carries nothing live across the rename;
        // it restarts fresh in the renamed directory.
        const had_wal = t.wal != null;
        if (had_wal) try t.flushLocked();
        closeTableHandles(t);
        // A refused rename moved nothing, so the table reopens where it stands.
        storage.retryTransientWindowsRefusal(self.io, Io.Dir.rename, .{ self.schema_dir, old_name, self.schema_dir, new_name, self.io }) catch |err| {
            self.reopenTableDirs(t, old_name, had_wal) catch t.requireRecovery();
            return err;
        };
        // Same contract as execAlter's swap: a failure here leaves the table
        // without directory handles, so fence it until reopen.
        errdefer t.requireRecovery();
        try self.reopenTableDirs(t, new_name, had_wal);

        const new_owned = try self.allocator.dupe(u8, new_name);
        const old_owned = t.name;
        t.name = new_owned;

        _ = self.tables.remove(old_name);
        try self.tables.put(t.name, t);

        self.allocator.free(old_owned);
    }

    fn reopenTableDirs(self: *Schema, t: *Table, name: []const u8, recreate_wal: bool) !void {
        t.table_dir = try self.schema_dir.openDir(self.io, name, .{});
        t.segments_dir = t.table_dir.openDir(t.io, "segments", .{}) catch |err| {
            t.table_dir.close(t.io);
            return err;
        };
        t.dirs_open = true;
        if (recreate_wal) {
            t.wal = try @import("../engine/engine.zig").wal.WalWriter.create(
                t.allocator,
                t.io,
                t.table_dir,
                t.schema_fingerprint,
            );
        }
    }

    /// List the names of every table in this schema. Caller frees the
    /// returned slice and each name with `allocator`.
    pub fn listTables(self: *Schema, allocator: Allocator) ![][]u8 {
        var out_list: std.ArrayList([]u8) = .empty;
        errdefer {
            for (out_list.items) |s| allocator.free(s);
            out_list.deinit(allocator);
        }

        self.tables_mutex.lockUncancelable(self.io);
        {
            defer self.tables_mutex.unlock(self.io);

            var it = self.tables.keyIterator();
            while (it.next()) |k| {
                try out_list.append(allocator, try allocator.dupe(u8, k.*));
            }
        }

        var dir_it = self.schema_dir.iterate();
        while (try dir_it.next(self.io)) |entry| {
            if (entry.kind != .directory or isReservedTableName(entry.name)) continue;
            if (ownedNameListContains(out_list.items, entry.name)) continue;
            if (!self.diskTableExists(entry.name)) continue;
            try out_list.append(allocator, try allocator.dupe(u8, entry.name));
        }

        return try out_list.toOwnedSlice(allocator);
    }

    fn diskTableExists(self: *Schema, name: []const u8) bool {
        var table_dir = self.schema_dir.openDir(self.io, name, .{}) catch return false;
        defer table_dir.close(self.io);
        table_dir.access(self.io, "schema.bin", .{ .read = true }) catch return false;
        return true;
    }

    fn ownedNameListContains(names: []const []u8, needle: []const u8) bool {
        for (names) |name| {
            if (std.mem.eql(u8, name, needle)) return true;
        }
        return false;
    }
};
