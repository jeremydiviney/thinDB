//! Public thinDB API: Database, Table, Config, TableOptions.
//!
//! v0.1 scope:
//!   - open a Database backed by a directory
//!   - createOrOpen a Table with a strict schema
//!   - row-oriented insert (with comptime field validation)
//!   - manual flush (memtable → on-disk segment + manifest update)
//!   - segment scanning is provided by `src/exec/` (operators)

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const types = @import("../types.zig");
const Schema = types.Schema;
const TypeTag = types.TypeTag;

const storage = @import("../storage/storage.zig");
const engine = @import("../engine/engine.zig");
const exec = @import("../exec/exec.zig");

pub const Error = error{
    SchemaMismatch,
    UnsupportedUniqueKeyType,
    UpsertRequiresUniqueKey,
    TableNotFound,
    TableAlreadyExists,
    ColumnNotFound,
    ColumnAlreadyExists,
    UnsupportedAlterOp,
};

pub const SyncMode = enum { none, per_flush };

/// One schema-change operation. `alterTable` takes a slice of these and
/// applies them in order to derive the new schema, then rewrites every
/// segment under that schema.
///
/// Not supported in v1: `change_type` (per-value conversion is a separate
/// piece of work). Use drop+add as a workaround if you need it.
pub const AlterOp = union(enum) {
    /// Append a new column. Existing rows get `default` as their value.
    /// `default`'s active tag must match `type` (e.g., type=.int requires
    /// `.int = N`); nullable columns may pass anything (the default is
    /// only used as the placeholder bytes — the validity bit is set true
    /// either way for existing rows).
    add: AddColumn,
    /// Remove a column by name. Errors if the column is part of the order
    /// key (would change row identity).
    drop: []const u8,
    /// Rename a column. Existing data unchanged; only the schema name
    /// changes. Errors if `to` is already a column name.
    rename: RenameColumn,

    pub const AddColumn = struct {
        name: []const u8,
        type: types.Type,
        nullable: bool = false,
        default: types.Value,
    };

    pub const RenameColumn = struct {
        from: []const u8,
        to: []const u8,
    };
};

pub const Config = struct {
    /// Default rows per row-group in flushed segments.
    row_group_size: usize = 65_536,

    /// Auto-flush triggers. A flush fires inline (from `insert` or `delete`)
    /// when ANY of these conditions hold against the current memtable.
    auto_flush_bytes: usize = 64 * 1024 * 1024,
    auto_flush_rows: u64 = 1_000_000,
    /// Seconds since the memtable's first write. 0 disables the time trigger.
    auto_flush_secs: u32 = 5,
    /// Time-based trigger only fires once both these are met (avoids tiny
    /// segments on low-volume tables).
    auto_flush_min_rows: u64 = 1_000,
    auto_flush_min_bytes: usize = 1 * 1024 * 1024,

    /// LRU cache budget for decompressed column blocks. 0 disables caching.
    cache_size_bytes: usize = 256 * 1024 * 1024,

    /// Background-compactor threshold. When a table has at least this many
    /// live segments and the background compactor sweep runs, it triggers
    /// a compaction. 0 disables the count-based trigger. Default 8 — a
    /// conservative tier-1 cutoff per DESIGN.md §7.1.
    compact_min_segments: u32 = 8,

    /// Tombstone-pressure trigger. When any segment's tombstone fraction
    /// (tombs / row_count) crosses this threshold, the next compaction
    /// sweep picks it (regardless of tier count) so the dead rows get
    /// reclaimed. Default 0.30. Set to a value > 1.0 to disable.
    compact_tombstone_threshold: f32 = 0.30,

    /// Durability mode. `.none` (default) returns from flush/delete as
    /// soon as bytes are in the OS page cache — fast but lossy on power
    /// loss. `.per_flush` fsyncs each segment + tombstone file after
    /// writing, and routes the manifest update through an atomic
    /// write-tmp-then-rename with an fsync on the temp. Adds ~3-15 ms
    /// per flush on consumer NVMe; reads are unaffected.
    ///
    /// Note: v0.7 fsync does NOT fsync the parent directory. NTFS handles
    /// rename durability via its journal; on Linux ext4 there is a small
    /// theoretical hole closed by future WAL support.
    sync_mode: SyncMode = .none,

    /// Enable the write-ahead log. When true, every insert and delete is
    /// appended to a single per-table WAL file and fsynced BEFORE the
    /// operation returns success. On reopen, the WAL is replayed to
    /// reconstruct any memtable contents that were lost on shutdown.
    ///
    /// This gives "INSERT returned OK = durable" semantics without
    /// per-insert segment flushes. Pairs naturally with `sync_mode =
    /// .per_flush` (segments + manifest also durable).
    ///
    /// Cost: one fsync per insert() / delete() call. Many small calls →
    /// many fsyncs. Application code should batch inserts where possible
    /// — a single insert(big_batch) is one fsync regardless of row count.
    wal_enabled: bool = false,

};

pub const TableOptions = struct {
    /// Required for create. On reopen via `db.table(...)`, must match the
    /// persisted schema's order key. Not used by `db.openTable(...)`.
    order_key: []const []const u8,
    unique: bool = false,
    /// Overrides Database.config.row_group_size for this table when set.
    row_group_size: ?usize = null,
};

/// Used by `openTable` — no schema or order_key, just runtime tunables.
pub const OpenOptions = struct {
    row_group_size: ?usize = null,
};

pub const Database = struct {
    allocator: Allocator,
    io: Io,
    data_dir: Io.Dir,
    config: Config,
    tables: std.StringHashMap(*Table),

    /// Guards `tables` map iteration from a background flusher caller.
    /// Held briefly to snapshot table pointers.
    tables_mutex: Io.Mutex = .init,

    /// Open a database rooted at `data_dir`. Caller retains ownership of
    /// `data_dir` and must keep it open for the lifetime of the returned
    /// `Database`.
    pub fn open(
        allocator: Allocator,
        io: Io,
        data_dir: Io.Dir,
        config: Config,
    ) !*Database {
        const self = try allocator.create(Database);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .data_dir = data_dir,
            .config = config,
            .tables = .init(allocator),
        };
        return self;
    }

    pub fn close(self: *Database) void {
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.close();
        }
        self.tables.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    /// One sweep of the background flush check. Looks up each known table
    /// by name under `tables_mutex` and atomically grabs its `ddl_lock`
    /// shared before releasing the map lock — this prevents a concurrent
    /// `dropTable` from freeing the Table while we still hold a pointer
    /// to it. Then calls `tryBackgroundFlush` (non-blocking on the per-
    /// table write mutex).
    pub fn backgroundFlushSweep(self: *Database) !void {
        const names = try self.snapshotTableNames();
        defer self.freeTableNames(names);

        for (names) |name| {
            if (self.acquireTableShared(name)) |t| {
                defer t.ddl_lock.unlockShared(t.io);
                t.tryBackgroundFlush() catch {};
            }
        }
    }

    /// Snapshot the current set of table names. Names are duplicated so
    /// they survive a concurrent drop. Caller frees with `freeTableNames`.
    fn snapshotTableNames(self: *Database) ![][]u8 {
        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);

        const out = try self.allocator.alloc([]u8, self.tables.count());
        errdefer self.allocator.free(out);
        var i: usize = 0;
        errdefer for (out[0..i]) |s| self.allocator.free(s);
        var it = self.tables.keyIterator();
        while (it.next()) |k| : (i += 1) {
            out[i] = try self.allocator.dupe(u8, k.*);
        }
        return out;
    }

    fn freeTableNames(self: *Database, names: [][]u8) void {
        for (names) |s| self.allocator.free(s);
        self.allocator.free(names);
    }

    /// Re-resolve `name` under `tables_mutex` AND grab its `ddl_lock`
    /// shared atomically. Returns `null` if the table has been dropped
    /// since the snapshot. Caller MUST `t.ddl_lock.unlockShared` when done.
    fn acquireTableShared(self: *Database, name: []const u8) ?*Table {
        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);
        const t = self.tables.get(name) orelse return null;
        t.ddl_lock.lockSharedUncancelable(t.io);
        return t;
    }

    /// Blocking loop that drives `backgroundFlushSweep` at `poll_ms`
    /// intervals until `should_stop` flips to true. Designed to be
    /// `std.Thread.spawn`-ed from the application:
    ///
    /// ```zig
    /// var stop: std.atomic.Value(bool) = .init(false);
    /// const thr = try std.Thread.spawn(.{}, thindb.Database.runBackgroundFlusher,
    ///                                  .{ db, sleeper_io, 500, &stop });
    /// defer { stop.store(true, .release); thr.join(); }
    /// ```
    ///
    /// `sleeper_io` is the `Io` used for the per-iteration sleep. It must
    /// support being called from this thread; in practice that means a
    /// multi-threaded `Io` (e.g. `std.Io.Threaded.init(...)`), separate
    /// from the `io` used for storage operations if those are
    /// single-threaded.
    pub fn runBackgroundFlusher(
        self: *Database,
        sleeper_io: Io,
        poll_ms: u32,
        should_stop: *std.atomic.Value(bool),
    ) void {
        const duration: Io.Duration = .fromMilliseconds(@intCast(poll_ms));
        while (!should_stop.load(.acquire)) {
            // Swallow sleep errors (cancellation/clock-unavailable): the loop
            // will re-check the stop flag and exit cleanly.
            Io.sleep(sleeper_io, duration, .awake) catch return;
            if (should_stop.load(.acquire)) return;
            self.backgroundFlushSweep() catch {};
        }
    }

    /// One sweep of the background compaction check. Same coordination
    /// pattern as `backgroundFlushSweep`: name-snapshot + atomic shared
    /// lock acquisition so a concurrent drop can't free the Table out
    /// from under us.
    pub fn backgroundCompactSweep(self: *Database) !void {
        const names = try self.snapshotTableNames();
        defer self.freeTableNames(names);

        const min_segs = self.config.compact_min_segments;
        const tomb_thresh = self.config.compact_tombstone_threshold;
        for (names) |name| {
            if (self.acquireTableShared(name)) |t| {
                defer t.ddl_lock.unlockShared(t.io);
                t.tryBackgroundCompact(min_segs, tomb_thresh) catch {};
            }
        }
    }

    /// Blocking loop that drives `backgroundCompactSweep` at `poll_ms`
    /// intervals until `should_stop` flips to true. Spawn from the
    /// application alongside (or instead of) `runBackgroundFlusher`.
    /// Same `sleeper_io` semantics as the flusher.
    pub fn runBackgroundCompactor(
        self: *Database,
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
        self: *Database,
        name: []const u8,
        schema: Schema,
        options: TableOptions,
    ) !*Table {
        try schema.validate();

        {
            self.tables_mutex.lockUncancelable(self.io);
            defer self.tables_mutex.unlock(self.io);
            if (self.tables.get(name)) |existing| {
                if (schemaFingerprint(schema) != existing.schema_fingerprint) {
                    return Error.SchemaMismatch;
                }
                return existing;
            }
        }

        const t = try Table.open(
            self.allocator,
            self.io,
            self.data_dir,
            name,
            schema,
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
        self: *Database,
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
            self.data_dir,
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
    /// then closes and deletes the directory tree from disk. Errors with
    /// `TableNotFound` if no table by that name exists in memory or on disk.
    ///
    /// After this returns, any caller-held `*Table` pointer is dangling.
    pub fn dropTable(self: *Database, name: []const u8) !void {
        self.tables_mutex.lockUncancelable(self.io);
        const maybe_existing = self.tables.fetchRemove(name);
        self.tables_mutex.unlock(self.io);

        if (maybe_existing) |entry| {
            const t = entry.value;
            // Wait for any active scans on this table to finish. Once we
            // hold the exclusive lock, no further shared lock acquisitions
            // can succeed — and the table is no longer in the map, so new
            // scans wouldn't even find it.
            t.ddl_lock.lockUncancelable(t.io);
            // Don't unlock — we're destroying the holder of the lock.
            t.close();
        } else {
            // Not in memory — verify it exists on disk before claiming success.
            var probe = self.data_dir.openDir(self.io, name, .{}) catch |err| switch (err) {
                error.FileNotFound => return Error.TableNotFound,
                else => return err,
            };
            probe.close(self.io);
        }

        // deleteTree silently treats a missing path as success — no need to
        // special-case FileNotFound here.
        try self.data_dir.deleteTree(self.io, name);
    }

    /// Apply schema operations (`.add`, `.drop`, `.rename` columns) to a
    /// table. Implemented as orchestrated copy-and-swap (DESIGN.md §9.2):
    /// flush memtable → write a shadow directory with rewritten segments
    /// → atomic directory rename → re-init Table in place.
    ///
    /// CALLER RESPONSIBILITY: no active `Query` references during alter.
    /// Active scans hold a memtable snapshot of the pre-alter schema and
    /// will see inconsistent state if they try to advance past it.
    pub fn alterTable(self: *Database, name: []const u8, ops: []const AlterOp) !void {
        self.tables_mutex.lockUncancelable(self.io);
        const t = self.tables.get(name) orelse {
            self.tables_mutex.unlock(self.io);
            return Error.TableNotFound;
        };
        self.tables_mutex.unlock(self.io);

        try @import("alter.zig").execAlter(self, t, ops);
    }

    /// Rename a table. Renames the on-disk directory, updates the in-memory
    /// map key, and updates the Table's internal name string. Errors with
    /// `TableNotFound` if `old_name` isn't present, or `TableAlreadyExists`
    /// if `new_name` is already taken. The existing `*Table` pointer
    /// remains valid — any caller holding it continues to work.
    ///
    /// Acquires the table's ddl_lock exclusive — blocks until any in-flight
    /// scans finish, and blocks new scans for the duration of the rename.
    pub fn renameTable(self: *Database, old_name: []const u8, new_name: []const u8) !void {
        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);

        if (self.tables.get(new_name) != null) return Error.TableAlreadyExists;
        // Detect collision with an on-disk-only directory under the new name.
        if (self.data_dir.openDir(self.io, new_name, .{})) |probe_| {
            var probe = probe_;
            probe.close(self.io);
            return Error.TableAlreadyExists;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }

        const t = self.tables.get(old_name) orelse return Error.TableNotFound;

        // Block readers (waits for in-flight scans) and writers.
        t.ddl_lock.lockUncancelable(t.io);
        defer t.ddl_lock.unlock(t.io);
        t.mutex.lockUncancelable(t.io);
        defer t.mutex.unlock(t.io);

        // Close the dir handles so Windows will let us rename. The handles
        // point to the same inode after rename, but we have to reopen via
        // the new path.
        t.segments_dir.close(t.io);
        t.table_dir.close(t.io);

        try self.data_dir.rename(old_name, self.data_dir, new_name, self.io);

        t.table_dir = try self.data_dir.openDir(self.io, new_name, .{});
        t.segments_dir = try t.table_dir.openDir(t.io, "segments", .{});

        // Update the Table's name string. The hashmap stores the new name
        // as its key (which is t.name).
        const new_owned = try self.allocator.dupe(u8, new_name);
        const old_owned = t.name;
        t.name = new_owned;

        _ = self.tables.remove(old_name);
        try self.tables.put(t.name, t);

        self.allocator.free(old_owned);
    }
};

// Table struct lives in table.zig — re-exported here so the historic
// `api.Table` and `api.schemaFingerprint` references in peer files keep
// working without churn.
pub const Table = @import("table.zig").Table;
pub const schemaFingerprint = @import("table.zig").schemaFingerprint;
