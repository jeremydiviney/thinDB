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
};

pub const SyncMode = enum { none, per_flush };

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

    /// One sweep of the background flush check: takes a brief snapshot of
    /// open tables and calls `tryBackgroundFlush` on each. Designed to be
    /// driven by an external loop (an OS thread, a task on an `Io`, or a
    /// test harness). Safe to call concurrently with `insert` / `flush`
    /// because each `tryBackgroundFlush` non-blockingly `tryLock`s the
    /// table.
    pub fn backgroundFlushSweep(self: *Database) !void {
        self.tables_mutex.lockUncancelable(self.io);
        const count = self.tables.count();
        const ptrs = self.allocator.alloc(*Table, count) catch |err| {
            self.tables_mutex.unlock(self.io);
            return err;
        };
        defer self.allocator.free(ptrs);
        var i: usize = 0;
        var it = self.tables.valueIterator();
        while (it.next()) |p| : (i += 1) ptrs[i] = p.*;
        self.tables_mutex.unlock(self.io);

        for (ptrs) |t| t.tryBackgroundFlush() catch {};
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

    /// One sweep of the background compaction check. Snapshots open tables
    /// under `tables_mutex`, then calls `tryBackgroundCompact` on each.
    /// Compaction runs only when a table's segment count meets the
    /// configured threshold AND the table mutex is uncontended.
    pub fn backgroundCompactSweep(self: *Database) !void {
        self.tables_mutex.lockUncancelable(self.io);
        const count = self.tables.count();
        const ptrs = self.allocator.alloc(*Table, count) catch |err| {
            self.tables_mutex.unlock(self.io);
            return err;
        };
        defer self.allocator.free(ptrs);
        var i: usize = 0;
        var it = self.tables.valueIterator();
        while (it.next()) |p| : (i += 1) ptrs[i] = p.*;
        self.tables_mutex.unlock(self.io);

        const min_segs = self.config.compact_min_segments;
        const tomb_thresh = self.config.compact_tombstone_threshold;
        for (ptrs) |t| t.tryBackgroundCompact(min_segs, tomb_thresh) catch {};
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
};

pub const Table = struct {
    allocator: Allocator,
    io: Io,
    name: []u8,
    schema_owner: storage.schema_file.SchemaOwner,
    schema: Schema,
    schema_fingerprint: u64,
    row_group_size: usize,
    order_key_indices: []usize,

    /// LRU cache of decompressed column-block bytes. Lifetime = Table.
    cache: storage.cache.Cache,

    // Auto-flush configuration (copied from Database.Config at open time).
    auto_flush_bytes: usize,
    auto_flush_rows: u64,
    auto_flush_secs: u32,
    auto_flush_min_rows: u64,
    auto_flush_min_bytes: usize,

    /// Durability mode (copied from Database.Config at open time).
    sync_mode: SyncMode,
    /// Timestamp at which the current (post-flush) memtable received its
    /// first row. Reset to `null` after every flush. Drives the time trigger.
    first_write_ts: ?Io.Timestamp = null,

    table_dir: Io.Dir,
    segments_dir: Io.Dir,

    manifest: storage.Manifest,
    /// Active memtable. Heap-allocated + refcounted so concurrent scans
    /// can pin a snapshot of it: flush/delete swap this pointer to a fresh
    /// memtable and retire the old, but the old stays alive while readers
    /// hold a reference (see `Memtable.acquire` / `release`).
    memtable: *engine.Memtable,

    /// WAL writer when `Config.wal_enabled = true`. `null` otherwise.
    wal: ?engine.wal.WalWriter,

    /// Serializes writers vs. the background flusher. Public mutating
    /// entry points (`insert`, `delete`, `flush`, `compact`) lock this
    /// before touching the memtable / manifest. Internal `*Locked` helpers
    /// assume it's already held.
    mutex: Io.Mutex = .init,

    fn open(
        allocator: Allocator,
        io: Io,
        data_dir: Io.Dir,
        name: []const u8,
        maybe_schema: ?Schema,
        cfg: Config,
        row_group_size: usize,
    ) !*Table {
        var table_dir = try data_dir.createDirPathOpen(io, name, .{});
        errdefer table_dir.close(io);

        var schema_owner = try acquireSchema(allocator, io, table_dir, maybe_schema);
        errdefer schema_owner.deinit();
        const schema = schema_owner.view();

        // Enforce v0.2 limitation: unique-key upsert is implemented only for
        // single-column bigint keys. Reject anything else at open time so
        // users don't get silent no-op upserts.
        if (schema.unique) try validateUniqueKey(schema);

        const fp = schemaFingerprint(schema);

        var segments_dir = try table_dir.createDirPathOpen(io, "segments", .{});
        errdefer segments_dir.close(io);

        var manifest = try storage.readManifest(allocator, io, table_dir, fp);
        errdefer manifest.deinit();

        const memtable = try engine.Memtable.create(allocator, schema);
        errdefer memtable.release();

        // If a WAL exists on disk, replay it into the memtable BEFORE
        // we open the WAL writer for new appends. If wal_enabled is
        // false but a WAL file is present from a previous run, we still
        // replay it so no acked writes are silently dropped.
        _ = engine.wal.replay(allocator, io, table_dir, fp, memtable) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };

        const name_copy = try allocator.dupe(u8, name);
        errdefer allocator.free(name_copy);

        const order_key_indices = try allocator.alloc(usize, schema.order_key.len);
        errdefer allocator.free(order_key_indices);
        for (schema.order_key, 0..) |k, i| {
            order_key_indices[i] = schema.columnIndex(k) orelse return Error.SchemaMismatch;
        }

        const self = try allocator.create(Table);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .io = io,
            .name = name_copy,
            .schema_owner = schema_owner,
            .schema = schema,
            .schema_fingerprint = fp,
            .row_group_size = row_group_size,
            .order_key_indices = order_key_indices,
            .cache = storage.cache.Cache.init(allocator, cfg.cache_size_bytes),
            .auto_flush_bytes = cfg.auto_flush_bytes,
            .auto_flush_rows = cfg.auto_flush_rows,
            .auto_flush_secs = cfg.auto_flush_secs,
            .auto_flush_min_rows = cfg.auto_flush_min_rows,
            .auto_flush_min_bytes = cfg.auto_flush_min_bytes,
            .sync_mode = cfg.sync_mode,
            .table_dir = table_dir,
            .segments_dir = segments_dir,
            .manifest = manifest,
            .memtable = memtable,
            .wal = null,
        };

        // If we replayed WAL records into a unique-key table's memtable,
        // re-run upsert resolution so any same-key dupes get tombstoned
        // (intra-memtable + against existing segments). Without this,
        // unique-key tables wouldn't behave the same after a crash as
        // they did before.
        if (schema.unique and self.memtable.row_count > 0) {
            try @import("upsert.zig").applyUpsertResolution(self);
        }

        if (cfg.wal_enabled) {
            // Replay already happened above (from the on-disk file). Now
            // truncate-and-recreate so subsequent writes go through a fresh log.
            self.wal = try engine.wal.WalWriter.create(allocator, io, table_dir, fp);
        }
        return self;
    }

    fn close(self: *Table) void {
        const allocator = self.allocator;
        const io = self.io;
        if (self.wal) |*w| w.deinit();
        self.cache.deinit();
        // Drop the Table's reference. If scans pinned a snapshot, the
        // memtable stays alive until the last reader releases it.
        self.memtable.release();
        self.manifest.deinit();
        self.segments_dir.close(io);
        self.table_dir.close(io);
        self.schema_owner.deinit();
        allocator.free(self.order_key_indices);
        allocator.free(self.name);
        allocator.destroy(self);
    }

    /// Append rows to the in-memory memtable. Rows are validated against the
    /// schema at compile time (column names and Zig types must match) and at
    /// runtime (every non-nullable schema column must appear).
    ///
    /// If the table was created with `unique = true`, any inserted row whose
    /// order key matches an existing row (in the memtable or any segment)
    /// causes the old row to be tombstoned. The new row becomes the visible
    /// value. (StarRocks "last writer wins" semantics.)
    pub fn insert(self: *Table, rows: anytype) !void {
        self.mutex.lockUncancelable(self.io);
        var wal_target: ?u64 = null;
        {
            defer self.mutex.unlock(self.io);
            wal_target = try self.insertLocked(rows);
        }
        try self.awaitWalDurable(wal_target);
    }

    /// Explicit upsert. Identical to `insert` on a unique-key table —
    /// new rows overwrite any existing row sharing the same order key
    /// (older copies are tombstoned). On a non-unique table this is
    /// nonsensical and errors with `UpsertRequiresUniqueKey`. Use this
    /// when you want the call site to be explicit that overwrite is
    /// the intent.
    pub fn upsert(self: *Table, rows: anytype) !void {
        if (!self.schema.unique) return Error.UpsertRequiresUniqueKey;
        self.mutex.lockUncancelable(self.io);
        var wal_target: ?u64 = null;
        {
            defer self.mutex.unlock(self.io);
            wal_target = try self.insertLocked(rows);
        }
        try self.awaitWalDurable(wal_target);
    }

    /// Mutates the memtable + appends bytes to the WAL (no fsync). The
    /// returned offset is the cumulative WAL write_offset after our append,
    /// passed to `awaitDurable` once the Table mutex is released so a
    /// concurrent batch of writers can amortize a single fsync syscall.
    /// Returns null when no WAL is configured.
    fn insertLocked(self: *Table, rows: anytype) !?u64 {
        const was_empty = self.memtable.isEmpty();
        const before_count: usize = @intCast(self.memtable.row_count);
        try self.memtable.insertRows(rows);
        const after_count: usize = @intCast(self.memtable.row_count);

        var wal_target: ?u64 = null;
        if (self.wal) |*w| {
            wal_target = try w.appendInsert(self.memtable, before_count, after_count);
        }

        if (was_empty and !self.memtable.isEmpty()) {
            self.first_write_ts = Io.Clock.awake.now(self.io);
        }
        if (self.schema.unique) {
            try @import("upsert.zig").applyUpsertResolution(self);
        }
        try self.maybeAutoFlushLocked();
        return wal_target;
    }

    /// Called outside the Table mutex (after releasing it) to wait for the
    /// WAL through `target` to be durably fsynced. No-op when WAL disabled,
    /// sync_mode is `.none`, or when an in-call flush already truncated past
    /// the target (in which case `WalWriter.truncate` has bumped synced past
    /// our offset and `awaitDurable` returns immediately).
    fn awaitWalDurable(self: *Table, target: ?u64) !void {
        if (!self.syncEnabled()) return;
        const o = target orelse return;
        if (self.wal) |*w| try w.awaitDurable(self.io, o);
    }

    /// Flush the memtable to a new segment on disk and update the manifest
    /// atomically. Rows are written sorted by the table's order key. No-op
    /// if the memtable is empty.
    pub fn flush(self: *Table) !void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        try self.flushLocked();
    }

    fn flushLocked(self: *Table) !void {
        if (self.memtable.isEmpty()) {
            self.first_write_ts = null;
            return;
        }

        const row_count = self.memtable.row_count;
        const seg_id = self.manifest.nextSegmentId();

        var name_buf: [32]u8 = undefined;
        const file_name = try std.fmt.bufPrint(&name_buf, "{d:0>20}.dat", .{seg_id});

        var snapshot = try self.memtable.buildSortedSnapshot(self.allocator, self.order_key_indices);
        defer snapshot.deinit();

        // Allocate the replacement memtable up front. If any of the segment /
        // manifest / WAL work below fails, errdefer frees the new one and we
        // leave the current memtable intact.
        const new_mt = try engine.Memtable.create(self.allocator, self.schema);
        errdefer new_mt.release();

        const sync = self.syncEnabled();
        var info = try storage.writeSegment(
            self.allocator,
            self.io,
            self.segments_dir,
            file_name,
            self.schema,
            seg_id,
            self.schema_fingerprint,
            self.row_group_size,
            snapshot.views,
            sync,
        );
        defer info.deinit(self.allocator);

        try self.manifest.appendSegment(.{ .segment_id = seg_id, .row_count = row_count });
        try storage.writeManifest(self.io, self.table_dir, self.manifest, sync);

        // WAL: the records preceding this flush are now redundant. Append a
        // flush marker so a crash mid-truncate is still recoverable, then
        // truncate. Order is intentional — marker first, truncate second.
        // Truncate is self-syncing AND bumps `synced_offset` past every
        // append before the truncate, so any pending `awaitDurable` from
        // the same call chain becomes a no-op.
        if (self.wal) |*w| {
            _ = try w.appendFlushMarker(seg_id);
            try w.truncate(self.schema_fingerprint);
        }

        // Retire-replace: swap the active memtable for the fresh one. The
        // old memtable's columns are not mutated again; any scan that pinned
        // it via `acquire` continues to read it safely until its `release`
        // drops the refcount to zero and frees it.
        const old_mt = self.memtable;
        self.memtable = new_mt;
        old_mt.retire();
        old_mt.release();

        self.first_write_ts = null;
    }

    /// Called from `insert`/`delete` after mutation. Flushes the memtable if
    /// any auto-flush trigger is satisfied. Caller holds `self.mutex`.
    fn maybeAutoFlushLocked(self: *Table) !void {
        if (self.memtable.isEmpty()) return;
        const rows = self.memtable.row_count;
        const bytes = self.memtable.byteSize();

        if (rows >= self.auto_flush_rows or bytes >= self.auto_flush_bytes) {
            try self.flushLocked();
            return;
        }

        if (self.auto_flush_secs > 0) {
            if (self.first_write_ts) |start| {
                const now_ts = Io.Clock.awake.now(self.io);
                const elapsed_ns: u64 = @intCast(start.durationTo(now_ts).toNanoseconds());
                const threshold_ns: u64 = @as(u64, self.auto_flush_secs) * std.time.ns_per_s;
                if (elapsed_ns >= threshold_ns and
                    rows >= self.auto_flush_min_rows and
                    bytes >= self.auto_flush_min_bytes)
                {
                    try self.flushLocked();
                }
            }
        }
    }

    /// Used by the Database-level background flusher thread. Tries to
    /// acquire the table's mutex non-blockingly; on success, runs the
    /// time-based auto-flush check. No-op on lock contention.
    pub fn tryBackgroundFlush(self: *Table) !void {
        if (!self.mutex.tryLock()) return;
        defer self.mutex.unlock(self.io);
        try self.maybeAutoFlushLocked();
    }

    /// Background-compactor entry point. Tries to acquire the mutex
    /// non-blockingly; on success, runs one compaction step. Considers
    /// both the tombstone-pressure trigger and (when at least
    /// `min_segments` are live) the count-based tier trigger. No-op on
    /// lock contention, no qualifying segment, or both gates disabled.
    pub fn tryBackgroundCompact(self: *Table, min_segments: u32, tomb_threshold: f32) !void {
        if (!self.mutex.tryLock()) return;
        defer self.mutex.unlock(self.io);
        // Cheap optimization: skip the work if neither trigger can fire.
        const enough_for_tier = (min_segments != 0 and self.manifest.segments.items.len >= min_segments);
        const tomb_enabled = (tomb_threshold <= 1.0);
        if (!enough_for_tier and !tomb_enabled) return;
        try @import("compact.zig").execTieredCompact(self, tomb_threshold);
    }

    pub fn segmentCount(self: Table) usize {
        return self.manifest.segments.items.len;
    }

    /// True iff this table is configured for durable writes (Config.sync_mode).
    /// Storage primitives that fsync take this as a parameter.
    pub fn syncEnabled(self: Table) bool {
        return self.sync_mode != .none;
    }

    pub fn segmentFileName(buf: []u8, seg_id: u64) ![]u8 {
        return std.fmt.bufPrint(buf, "{d:0>20}.dat", .{seg_id});
    }

    /// Delete every row matching `pred`. Scans all segments + the memtable,
    /// emits tombstones for matches in segments, and rebuilds the memtable
    /// without them. Returns the number of rows deleted.
    pub fn delete(self: *Table, pred: exec.Predicate) !usize {
        self.mutex.lockUncancelable(self.io);
        var wal_target: ?u64 = null;
        var deleted: usize = 0;
        {
            defer self.mutex.unlock(self.io);
            // Log first; the delete primitive is idempotent on replay.
            if (self.wal) |*w| {
                wal_target = try w.appendDelete(pred);
            }
            deleted = try @import("delete.zig").execDelete(self, pred);
        }
        try self.awaitWalDurable(wal_target);
        return deleted;
    }

    /// Merge all segments into a single new segment. Drops tombstoned rows.
    /// No-op if there's at most one segment.
    pub fn compact(self: *Table) !void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.manifest.segments.items.len <= 1) return;
        try @import("compact.zig").execCompact(self);
    }

    pub fn deleteSegmentFiles(self: *Table, seg_id: u64) !void {
        var dat_buf: [32]u8 = undefined;
        const dat_name = try Table.segmentFileName(&dat_buf, seg_id);
        self.segments_dir.deleteFile(self.io, dat_name) catch {};

        var tomb_buf: [32]u8 = undefined;
        const tomb_name = try storage.tombstone.fileNameFor(&tomb_buf, seg_id);
        self.segments_dir.deleteFile(self.io, tomb_name) catch {};
    }
};


/// Either loads the persisted schema, validates against the caller-provided
/// one, or — when no schema.bin exists — clones the caller schema and
/// persists it.
fn acquireSchema(
    allocator: Allocator,
    io: Io,
    table_dir: Io.Dir,
    maybe_schema: ?Schema,
) !storage.schema_file.SchemaOwner {
    if (storage.schema_file.readSchema(allocator, io, table_dir)) |loaded| {
        if (maybe_schema) |s| {
            if (!storage.schema_file.schemasEqual(s, loaded.view())) {
                var owner = loaded;
                owner.deinit();
                return storage.schema_file.Error.SchemaMismatch;
            }
        }
        return loaded;
    } else |err| switch (err) {
        error.FileNotFound => {
            const s = maybe_schema orelse return storage.schema_file.Error.SchemaRequired;
            try s.validate();
            var owner = try storage.schema_file.SchemaOwner.clone(allocator, s);
            errdefer owner.deinit();
            try storage.schema_file.writeSchema(io, table_dir, s, allocator);
            return owner;
        },
        else => return err,
    }
}

/// Validate that the schema's order key is supported for unique upsert.
/// v0.3: any compound key built from scalar types (`INT`, `BIGINT`, `BOOLEAN`,
/// `VARCHAR(N)`, `STRING`) is allowed. No-op currently — all supported types
/// pass.
fn validateUniqueKey(schema: Schema) !void {
    if (schema.order_key.len == 0) return Error.UnsupportedUniqueKeyType;
    for (schema.order_key) |k| {
        const idx = schema.columnIndex(k) orelse return Error.SchemaMismatch;
        _ = idx;
        // All v0.3 column types are supported in compound keys.
    }
}

/// Stable hash of a schema's column names, types, order key, and unique flag.
/// Used to detect schema drift when reopening a table.
pub fn schemaFingerprint(schema: Schema) u64 {
    var hasher = std.hash.Wyhash.init(0);
    for (schema.columns) |c| {
        hasher.update(c.name);
        const tag: u8 = @intFromEnum(@as(TypeTag, c.type));
        hasher.update(&[_]u8{tag});
        hasher.update(&[_]u8{@intFromBool(c.nullable)});
        switch (c.type) {
            .varchar, .char => |n| {
                var b: [4]u8 = undefined;
                std.mem.writeInt(u32, &b, n, .little);
                hasher.update(&b);
            },
            .decimal64, .decimal128 => |spec| {
                hasher.update(&[_]u8{ spec.p, spec.s });
            },
            else => {},
        }
    }
    for (schema.order_key) |k| hasher.update(k);
    hasher.update(&[_]u8{@intFromBool(schema.unique)});
    return hasher.final();
}

test {
    _ = @import("api_test.zig");
}
