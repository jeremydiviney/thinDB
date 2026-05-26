//! Table — per-table state: schema, manifest, segments, memtable, WAL.
//! Database (in api.zig) owns the lifecycle and the in-memory map keyed
//! by table name. Public methods on Table are the per-table mutation
//! entry points (insert, upsert, delete, flush, compact). Internal
//! `*Locked` helpers assume `Table.mutex` is held.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const types = @import("../types.zig");
const TableSchema = types.TableSchema;
const TypeTag = types.TypeTag;

const storage = @import("../storage/storage.zig");
const engine = @import("../engine/engine.zig");
const exec = @import("../exec/exec.zig");

const api = @import("api.zig");
const Config = api.Config;
const SyncMode = api.SyncMode;
const Error = api.Error;

pub const Table = struct {
    allocator: Allocator,
    io: Io,
    name: []u8,
    schema_owner: storage.schema_file.SchemaOwner,
    schema: TableSchema,
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

    /// Per-query memory ceiling for blocking operators. 0 = unlimited
    /// (no tracking). Copied from Config.query_memory_budget at open.
    query_memory_budget: usize,

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

    /// Reader/DDL coordination. Scans hold this SHARED for their entire
    /// lifetime (acquire on create, release on deinit). DDL operations
    /// (drop / alter / rename) hold it EXCLUSIVE. Background flushers and
    /// compactors briefly hold it shared while they have a `*Table` pointer.
    ///
    /// Semantic: DDL waits for in-flight scans to finish, then runs while
    /// no new scans can start. Standard SQL-DB behavior (cf. PostgreSQL
    /// AccessExclusiveLock, MySQL MDL).
    ddl_lock: Io.RwLock = .init,

    /// Serializes compaction against itself (background sweep vs. explicit
    /// COMPACT). Held for a whole compaction; does NOT block scans/inserts.
    compact_lock: Io.Mutex = .init,

    /// Monotonic source of new segment IDs. Initialized to `max(id)+1` at
    /// open and bumped atomically on every segment creation (flush AND
    /// compaction). A counter rather than `manifest.nextSegmentId()` so a
    /// build-aside compaction can write its new segment file before the
    /// manifest swap without colliding with a concurrent flush's ID.
    next_segment_id: std.atomic.Value(u64) = .init(0),

    pub fn open(
        allocator: Allocator,
        io: Io,
        parent_dir: Io.Dir,
        name: []const u8,
        maybe_schema: ?TableSchema,
        cfg: Config,
        row_group_size: usize,
    ) !*Table {
        var table_dir = try parent_dir.createDirPathOpen(io, name, .{});
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
        // Fresh manifests (no file on disk) carry column_count=0; set
        // from the schema so future writes emit the right per-entry size.
        if (manifest.column_count == 0) manifest.column_count = @intCast(schema.columns.len);
        // AUTO_INCREMENT reopen reconciliation: defensively bump the
        // persisted counter past whatever max value already lives in
        // segment stats. Cheap insurance against crash-between-insert-
        // and-flush; without this we could re-issue an id already on
        // disk. v1 counter starts at 1.
        if (autoIncrementColumnIndex(schema)) |ai_idx| {
            const observed = manifestMaxAutoIncrement(manifest, ai_idx);
            const candidate: u64 = if (observed) |v| v +| 1 else 1;
            if (candidate > manifest.auto_inc_next) manifest.auto_inc_next = candidate;
            if (manifest.auto_inc_next == 0) manifest.auto_inc_next = 1;
        }

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
            .cache = storage.cache.Cache.init(allocator, api.autoCacheSizeBytes(cfg.cache_size_bytes)),
            .auto_flush_bytes = cfg.auto_flush_bytes,
            .auto_flush_rows = cfg.auto_flush_rows,
            .auto_flush_secs = cfg.auto_flush_secs,
            .auto_flush_min_rows = cfg.auto_flush_min_rows,
            .auto_flush_min_bytes = cfg.auto_flush_min_bytes,
            .query_memory_budget = cfg.query_memory_budget,
            .sync_mode = cfg.sync_mode,
            .table_dir = table_dir,
            .segments_dir = segments_dir,
            .manifest = manifest,
            .memtable = memtable,
            .wal = null,
            .next_segment_id = .init(manifest.nextSegmentId()),
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

    pub fn close(self: *Table) void {
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

    /// Bulk-insert a wire-decoded columnar batch. Used by the TCP write
    /// path: client encodes rows into the wire batch format, server
    /// decodes and lands here. Avoids the row-major detour `insert`
    /// takes for `anytype` rows.
    ///
    /// Same lock + WAL + unique + auto-flush semantics as `insert`.
    pub fn insertBatch(
        self: *Table,
        batch_schema: []const types.Column,
        views: []const storage.ColumnView,
        row_count: usize,
    ) !void {
        self.mutex.lockUncancelable(self.io);
        var wal_target: ?u64 = null;
        {
            defer self.mutex.unlock(self.io);
            wal_target = try self.insertBatchInner(batch_schema, views, row_count);
        }
        try self.awaitWalDurable(wal_target);
    }

    /// Same as `insertBatch` but assumes the caller already holds
    /// `self.mutex`. Used by the SQL INSERT compile path when it needs
    /// to reserve AUTO_INCREMENT ids under the same lock as the batch
    /// insert. WAL durability await also happens here so the caller
    /// can drop the lock immediately.
    pub fn insertBatchLocked(
        self: *Table,
        batch_schema: []const types.Column,
        views: []const storage.ColumnView,
        row_count: usize,
    ) !void {
        const wal_target = try self.insertBatchInner(batch_schema, views, row_count);
        try self.awaitWalDurable(wal_target);
    }

    pub fn insertBatchInner(
        self: *Table,
        batch_schema: []const types.Column,
        views: []const storage.ColumnView,
        row_count: usize,
    ) !?u64 {
        var wal_target: ?u64 = null;
        try self.cloneMemtableIfPinnedLocked();
        const was_empty = self.memtable.isEmpty();
        const before_count: usize = @intCast(self.memtable.row_count);
        try self.memtable.insertColumnarBatch(batch_schema, views, row_count);
        const after_count: usize = @intCast(self.memtable.row_count);

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

    /// Mutates the memtable + appends bytes to the WAL (no fsync). The
    /// returned offset is the cumulative WAL write_offset after our append,
    /// passed to `awaitDurable` once the Table mutex is released so a
    /// concurrent batch of writers can amortize a single fsync syscall.
    /// Returns null when no WAL is configured.
    fn insertLocked(self: *Table, rows: anytype) !?u64 {
        try self.cloneMemtableIfPinnedLocked();
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

    /// Snapshot-isolation guard for in-place memtable mutation. If any scan
    /// has pinned the current memtable (`refcount > 1`), build a fresh
    /// memtable carrying the same rows, retire-replace the table's pointer,
    /// and leave the old one alive for the pinned reader. Caller holds the
    /// table mutex. No-op when no reader is pinned, so the steady-state
    /// (write-only) cost is one atomic load.
    fn cloneMemtableIfPinnedLocked(self: *Table) !void {
        if (!self.memtable.hasSnapshotReaders()) return;
        const cloned = try self.memtable.cloneAll(self.allocator);
        const old_mt = self.memtable;
        self.memtable = cloned;
        old_mt.retire();
        old_mt.release();
    }

    /// Called outside the Table mutex (after releasing it) to wait for the
    /// WAL through `target` to be durably fsynced. No-op when WAL disabled,
    /// sync_mode is `.none`, or when an in-call flush already truncated past
    /// the target (in which case `WalWriter.truncate` has bumped synced past
    /// our offset and `awaitDurable` returns immediately).
    pub fn awaitWalDurable(self: *Table, target: ?u64) !void {
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

    pub fn flushLocked(self: *Table) !void {
        if (self.memtable.isEmpty()) {
            self.first_write_ts = null;
            return;
        }

        const seg_id = self.allocSegmentId();

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

        // Coexisting segments' sketches feed the global dict-eligibility gate.
        // Safe to reference directly: we hold the table mutex and writeSegment
        // does not mutate the manifest.
        const prior = try self.allocator.alloc([]const u8, self.manifest.segments.items.len);
        defer self.allocator.free(prior);
        for (self.manifest.segments.items, 0..) |e, i| prior[i] = e.column_sketches;

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
            prior,
            sync,
        );
        defer info.deinit(self.allocator);

        try self.manifest.appendSegment(try self.entryFor(info));
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

    /// Background-compactor entry point. Caller (the background compact
    /// sweep) already holds `compact_lock`. Runs one compaction step,
    /// considering both the tombstone-pressure trigger and (when at least
    /// `min_segments` are live) the count-based tier trigger. No-op when
    /// no segment qualifies or both gates are disabled.
    pub fn tryBackgroundCompact(self: *Table, min_segments: u32, tomb_threshold: f32) !void {
        // Cheap optimization: skip the work if neither trigger can fire.
        self.mutex.lockUncancelable(self.io);
        const seg_count = self.manifest.segments.items.len;
        self.mutex.unlock(self.io);
        const enough_for_tier = (min_segments != 0 and seg_count >= min_segments);
        const tomb_enabled = (tomb_threshold <= 1.0);
        if (!enough_for_tier and !tomb_enabled) return;
        try @import("compact.zig").execTieredCompact(self, tomb_threshold);
    }

    pub fn segmentCount(self: Table) usize {
        return self.manifest.segments.items.len;
    }

    /// Reserve `count` consecutive AUTO_INCREMENT values starting at the
    /// table's current counter, advance the counter past them, and return
    /// the starting value. Caller must hold `self.mutex` (the INSERT
    /// compile path runs under it). When the table has no AI column the
    /// counter is zero — caller is expected to gate on
    /// `autoIncrementColumnIndex` first.
    pub fn reserveAutoIncrement(self: *Table, count: u64) u64 {
        if (self.manifest.auto_inc_next == 0) self.manifest.auto_inc_next = 1;
        const start = self.manifest.auto_inc_next;
        self.manifest.auto_inc_next = start +| count;
        return start;
    }

    /// Advance the counter so a future `reserveAutoIncrement` returns at
    /// least `v + 1`. Used when the caller supplied an explicit value for
    /// the AI column — MySQL bumps the counter past the largest such
    /// value so future omitted-column inserts don't collide.
    pub fn observeAutoIncrement(self: *Table, v: i128) void {
        if (v < 0) return;
        const u: u64 = if (v > std.math.maxInt(u64)) std.math.maxInt(u64) else @intCast(v);
        const next = u +| 1;
        if (next > self.manifest.auto_inc_next) self.manifest.auto_inc_next = next;
    }

    /// Build a `ManifestEntry` for a freshly-written segment under this
    /// table's schema. Populates byte_size, row_group_count,
    /// leading_key_stats, and the new v4 per-column_stats. Caller
    /// (Manifest) owns the allocated `column_stats` slice via
    /// `Manifest.deinit`.
    pub fn entryFor(self: Table, info: storage.format.SegmentInfo) !storage.manifest.ManifestEntry {
        const lk_idx: ?usize = if (self.order_key_indices.len > 0) self.order_key_indices[0] else null;
        const has_stats = try buildColumnHasStats(self.allocator, self.schema);
        defer self.allocator.free(has_stats);
        return storage.manifest.entryFromSegmentInfo(self.allocator, info, lk_idx, has_stats);
    }

    /// True iff this table is configured for durable writes (Config.sync_mode).
    /// Storage primitives that fsync take this as a parameter.
    pub fn syncEnabled(self: Table) bool {
        return self.sync_mode != .none;
    }

    pub fn segmentFileName(buf: []u8, seg_id: u64) ![]u8 {
        return std.fmt.bufPrint(buf, "{d:0>20}.dat", .{seg_id});
    }

    /// Allocate a fresh, never-reused (within this run) segment ID.
    pub fn allocSegmentId(self: *Table) u64 {
        return self.next_segment_id.fetchAdd(1, .monotonic);
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

    /// SQL `DELETE FROM t [WHERE expr]` — generalized delete with the
    /// rich PredicateExpr. Subqueries and `@vars` must already be
    /// resolved by the pre-compile pass. `pred == null` deletes every
    /// row. Returns the deleted row count. Streams per segment so
    /// memory stays bounded by segment size.
    ///
    /// Note: WAL semantics for the rich predicate aren't implemented
    /// yet (the existing appendDelete signature only carries the
    /// simple `exec.Predicate`). For v1, durability comes from the
    /// per-segment tombstone-file atomicity (tmp + rename) and the
    /// memtable clone-and-swap. Crash recovery rebuilds segment
    /// state from the persisted tombstone files.
    pub fn deleteByExpr(self: *Table, pred: ?exec.PredicateExpr) !usize {
        // Widen literals in the predicate up front so both the WAL-
        // logged form and the executor see the same shape (BIGINT
        // column + INT literal etc.). The mutation is local to this
        // function but propagates because both calls take the
        // widened value.
        var pred_local: ?exec.PredicateExpr = pred;
        if (pred_local) |*p| try exec.predicate.validateExpr(p, self.schema.columns);

        self.mutex.lockUncancelable(self.io);
        var wal_target: ?u64 = null;
        var deleted: usize = 0;
        {
            defer self.mutex.unlock(self.io);
            wal_target = try self.logDeleteExprLocked(pred_local);
            deleted = try @import("delete.zig").execDeleteByExpr(self, pred_local);
        }
        try self.awaitWalDurable(wal_target);
        return deleted;
    }

    /// WAL-log a rich `DELETE FROM t WHERE ...` predicate. Mutex must
    /// be held. Returns the WAL write_offset to await for durability,
    /// or null when there's no WAL writer or the predicate shape
    /// isn't loggable (caller should still proceed with the delete —
    /// segment tombstones are durable independently).
    pub fn logDeleteExprLocked(self: *Table, pred_opt: ?exec.PredicateExpr) !?u64 {
        if (self.wal == null) return null;
        if (pred_opt) |pred| {
            return self.wal.?.appendDeleteExpr(pred) catch |err| switch (err) {
                // Predicate variant the WAL can't encode (subquery /
                // var_ref / correlated). Skip logging — delete still
                // succeeds; documented gap.
                error.WalPredicateUnsupported => null,
                else => err,
            };
        }
        // DELETE without WHERE — log as `.always = true` so replay
        // wipes the memtable too.
        const wal_pred: exec.PredicateExpr = .{ .always = true };
        return try self.wal.?.appendDeleteExpr(wal_pred);
    }

    /// SQL `UPDATE t SET ... [WHERE expr]` — atomic DELETE-old +
    /// INSERT-new under the table mutex. Caller has pre-computed the
    /// replacement rows in `sink` (a transient Memtable owned by the
    /// caller). This method:
    ///   1. Tombstones segment rows matching `pred` + filters the
    ///      memtable for non-matching rows (same path as `deleteByExpr`).
    ///   2. Appends every row in `sink` to the now-filtered memtable
    ///      as a single columnar batch.
    /// Both steps run while holding the table mutex so concurrent
    /// SELECT readers either see the all-old or the all-new state.
    /// Streaming UPDATE — replaces the older buffer-then-apply path
    /// with per-segment delete+insert pairs so memory stays bounded
    /// regardless of how many rows the UPDATE touches. Each segment's
    /// tombstone-merge + new-row inserts happen together under the
    /// table mutex.
    pub fn updateStreaming(
        self: *Table,
        pred: ?exec.PredicateExpr,
        assignments: []const @import("update.zig").Assignment,
    ) !usize {
        return try @import("update.zig").execUpdateStreaming(self, pred, assignments);
    }

    pub fn applyUpdate(self: *Table, pred: ?exec.PredicateExpr, sink: *@import("../engine/engine.zig").Memtable) !void {
        // Widen the predicate up front so the WAL-logged form matches
        // what execDeleteByExpr will run.
        var pred_local: ?exec.PredicateExpr = pred;
        if (pred_local) |*p| try exec.predicate.validateExpr(p, self.schema.columns);

        self.mutex.lockUncancelable(self.io);
        var wal_target: ?u64 = null;
        {
            defer self.mutex.unlock(self.io);

            // DELETE leg — WAL-log the predicate, then tombstone +
            // memtable-filter rows that match it.
            const del_target = try self.logDeleteExprLocked(pred_local);
            _ = del_target; // The insert WAL append below supersedes this.
            _ = try @import("delete.zig").execDeleteByExpr(self, pred_local);

            // INSERT leg — append sink rows into the table memtable.
            // Goes through insertBatchInner which also WAL-logs and
            // returns the offset we await on for durability.
            const n = @as(usize, @intCast(sink.row_count));
            if (n > 0) {
                const views_buf = try self.allocator.alloc(@import("../storage/storage.zig").ColumnView, sink.columns.len);
                defer self.allocator.free(views_buf);
                for (sink.columns, views_buf) |*c, *v| v.* = c.view();
                wal_target = try self.insertBatchInner(sink.schema.columns, views_buf, n);
            }
        }
        // Await the later of the two WAL writes — insert's offset is
        // higher than delete's so waiting on insert covers both.
        try self.awaitWalDurable(wal_target);
    }

    /// Merge all segments into a single new segment. Drops tombstoned rows.
    /// No-op if there's at most one segment.
    pub fn compact(self: *Table) !void {
        self.compact_lock.lockUncancelable(self.io);
        defer self.compact_lock.unlock(self.io);
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
    maybe_schema: ?TableSchema,
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
fn validateUniqueKey(schema: TableSchema) !void {
    if (schema.order_key.len == 0) return Error.UnsupportedUniqueKeyType;
    for (schema.order_key) |k| {
        const idx = schema.columnIndex(k) orelse return Error.SchemaMismatch;
        _ = idx;
        // All v0.3 column types are supported in compound keys.
    }
}

/// Build a per-column "has_stats" bitvec under the given schema.
/// Caller owns the returned slice.
pub fn buildColumnHasStats(allocator: Allocator, schema: TableSchema) ![]bool {
    const out = try allocator.alloc(bool, schema.columns.len);
    for (schema.columns, 0..) |c, i| out[i] = storage.format.typeHasStats(c.type);
    return out;
}

/// Return the column index of the (at most one) AUTO_INCREMENT column,
/// or null when none is declared.
pub fn autoIncrementColumnIndex(schema: TableSchema) ?usize {
    for (schema.columns, 0..) |c, i| {
        if (c.auto_increment) return i;
    }
    return null;
}

/// Largest non-negative AI value observed across the manifest's
/// per-column segment stats, or null when no segment carries usable
/// stats for the AI column. Used at reopen to ensure the persisted
/// counter never re-issues an id already on disk.
fn manifestMaxAutoIncrement(m: storage.Manifest, ai_idx: usize) ?u64 {
    var best: ?i128 = null;
    for (m.segments.items) |e| {
        if (ai_idx >= e.column_stats.len) continue;
        const s = e.column_stats[ai_idx];
        if (s.min == 0 and s.max == 0) continue;
        if (best == null or s.max > best.?) best = s.max;
    }
    if (best) |v| {
        if (v < 0) return 0;
        if (v > std.math.maxInt(u64)) return std.math.maxInt(u64);
        return @intCast(v);
    }
    return null;
}

/// Stable hash of a schema's column names, types, order key, and unique flag.
/// Used to detect schema drift when reopening a table.
pub fn schemaFingerprint(schema: TableSchema) u64 {
    var hasher = std.hash.Wyhash.init(0);
    for (schema.columns) |c| {
        hasher.update(c.name);
        const tag: u8 = @intFromEnum(@as(TypeTag, c.type));
        hasher.update(&[_]u8{tag});
        hasher.update(&[_]u8{@intFromBool(c.nullable)});
        hasher.update(&[_]u8{@intFromBool(c.auto_increment)});
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
