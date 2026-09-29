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
const bloom_util = @import("../util/bloom.zig");
const FairMutex = @import("../util/fair_mutex.zig").FairMutex;
const ReaderPreferringRwLock = @import("../util/reader_preferring_rwlock.zig").ReaderPreferringRwLock;
const StatementGate = @import("../util/statement_gate.zig").StatementGate;

const api = @import("api.zig");
const upsert_mod = @import("upsert.zig");
const Config = api.Config;
const SyncMode = api.SyncMode;
const Error = api.Error;

pub const Table = struct {
    allocator: Allocator,
    statement_gate: ?*StatementGate = null,
    recovery_required: std.atomic.Value(bool) = .init(false),
    io: Io,
    name: []u8,
    schema_owner: storage.schema_file.SchemaOwner,
    schema: TableSchema,
    schema_fingerprint: u64,
    row_group_size: usize,
    order_key_indices: []usize,

    /// The shared (Catalog-wide) LRU cache of decompressed column-block
    /// bytes. Entries this table inserts are scoped by `cache_uid`; the byte
    /// budget and LRU order are global across every table sharing the cache.
    cache: *storage.cache.Cache,
    /// This table generation's namespace in `cache`. Re-minted (invalidating
    /// every cached block in O(1)) on TRUNCATE and ALTER.
    cache_uid: u64,
    /// Set when no shared cache was in the Config (direct `Table.open`, no
    /// Catalog — tests) and this table minted a private one to free at close.
    owned_cache: bool = false,

    /// Opened-segment cache: parsed footers + tombstone state, shared across
    /// queries and scan workers (footer parse is ~ms; segments are immutable).
    /// Lifetime = Table; entries retire when their files are deleted.
    seg_handles: storage.cache.SegmentHandles = .{},

    // Auto-flush configuration (copied from Database.Config at open time).
    auto_flush_bytes: usize,
    auto_flush_rows: u64,
    auto_flush_secs: u32,
    auto_flush_min_rows: u64,
    auto_flush_min_bytes: usize,

    /// Per-query memory ceiling for blocking operators. 0 = unlimited
    /// (no tracking). Copied from Config.query_memory_budget at open.
    query_memory_budget: usize,

    /// The Catalog's shared cross-query memory pool (null = none). Self-
    /// minted scan accountants draw from it so raw-builder pipelines are
    /// pool-constrained like SQL-compiled ones. Copied from Config at open.
    memory_pool: ?*exec.memory.MemoryPool,

    /// Encoder threads for compaction merges (resolved from
    /// Config.compact_threads at open; ≥1).
    compact_threads: usize,

    /// Durability mode (copied from Database.Config at open time).
    sync_mode: SyncMode,
    /// Timestamp at which the current (post-flush) memtable received its
    /// first row. Reset to `null` after every flush. Drives the time trigger.
    first_write_ts: ?Io.Timestamp = null,

    /// Consecutive background-flush failures, for throttled logging only.
    flush_fail_streak: u32 = 0,

    /// Segment ids whose files couldn't be deleted because a reader still
    /// held them open past the bounded retry (#137). Retried by the
    /// background flush sweep until reclaimed, so nothing is orphaned.
    /// Compactor appends, flusher drains — hence the dedicated lock.
    pending_deletes: std.ArrayListUnmanaged(u64) = .empty,
    pending_deletes_lock: std.atomic.Mutex = .unlocked,

    table_dir: Io.Dir,
    segments_dir: Io.Dir,
    /// False while a DDL swap (ALTER rewrite, RENAME) has closed the two
    /// directory handles and not yet reopened them. A swap that fails in
    /// between leaves them closed for good; `close` must not close them again
    /// (Windows reports INVALID_HANDLE, POSIX would hit a reused descriptor).
    dirs_open: bool = true,

    manifest: storage.Manifest,
    /// Active memtable. Heap-allocated + refcounted so concurrent scans
    /// can pin a snapshot of it: flush/delete swap this pointer to a fresh
    /// memtable and retire the old, but the old stays alive while readers
    /// hold a reference (see `Memtable.acquire` / `release`).
    memtable: *engine.Memtable,

    /// Incremental upsert key index (unique tables): compound-order-key bytes
    /// → current live memtable row index. Persists across insert batches so
    /// upsert resolution processes only the NEW rows (O(batch)) instead of
    /// rescanning the whole memtable (O(n) per batch → O(n²) total). Bound to
    /// one memtable generation via `memtable_gen`. A swap to a memtable that
    /// keeps the old one's rows in order, minus some (delete / update /
    /// dedup-clone / copy for a pinned reader), carries it over
    /// (`upsert.carryIndex`); any other swap (flush, truncate, ALTER) forces
    /// a rebuild. Key bytes live in `upsert_idx_arena`.
    ///
    /// The binding was previously the memtable POINTER — a proven data-loss
    /// ABA: the allocator can hand a later clone the freed memtable's
    /// address, revalidating a stale index whose row mappings then tombstone
    /// UNRELATED rows on the next upsert (rows acked to the client silently
    /// vanish). The counter can't be reused.
    /// An array map: a carry rewrites every row id, and its values are one
    /// dense array however large the map has been.
    upsert_idx: std.StringArrayHashMapUnmanaged(u32) = .empty,
    upsert_idx_arena: ?std.heap.ArenaAllocator = null,
    upsert_idx_gen: ?u64 = null,
    upsert_idx_rows: u32 = 0,
    /// Keys carried swaps have removed since the last rebuild.
    upsert_idx_removed: u32 = 0,

    /// Bumped by `installMemtableLocked` on every memtable retire-replace.
    memtable_gen: u64 = 0,

    /// WAL writer when `Config.wal_enabled = true`. `null` otherwise.
    wal: ?engine.wal.WalWriter,
    /// The UPDATE or DELETE staging steps on this table, between
    /// `beginStatementLocked` and its commit or `endStatementLocked`.
    statement: ?*Statement = null,
    /// `STATEMENT_STAGE_BYTES`, which tests lower to cross it with a few
    /// rows.
    stage_bytes: usize = STATEMENT_STAGE_BYTES,

    /// Serializes writers vs. the background flusher. Public mutating
    /// entry points (`insert`, `delete`, `flush`, `compact`) lock this
    /// before touching the memtable / manifest. Internal `*Locked` helpers
    /// assume it's already held.
    ///
    /// FIFO-fair (not `Io.Mutex`): critical sections here are long — a
    /// unique-key batch holds it across the segment probe and any inline
    /// auto-flush — and under sustained sink traffic a barging lock starved
    /// one parked writer for 559 s while fresh acquirers kept stealing the
    /// word (#164, 2026-07-11 incident).
    mutex: FairMutex = .init,

    /// Reader/DDL coordination. Scans hold this SHARED for their entire
    /// lifetime (acquire on create, release on deinit). DDL operations
    /// (drop / alter / rename) and compaction commits hold it EXCLUSIVE. The
    /// background flusher holds it shared while it flushes.
    ///
    /// Semantic: DDL waits for in-flight scans to finish, then runs while
    /// no new scans can start. Standard SQL-DB behavior (cf. PostgreSQL
    /// AccessExclusiveLock, MySQL MDL). Reader-preferring: one statement's
    /// Scans take it shared one after another, so a queued writer must not
    /// hold back the next one (see util/reader_preferring_rwlock.zig).
    ddl_lock: ReaderPreferringRwLock = .init,

    /// Serializes compaction against itself (background sweep vs. explicit
    /// COMPACT). Held for a whole compaction; does NOT block scans/inserts.
    /// A background merge holds no statement lease, so this lock alone keeps
    /// its table alive: every path that frees or rewrites the table takes it
    /// first, through `lockCompactPreempting`.
    compact_lock: Io.Mutex = .init,

    /// How many callers of `lockCompactPreempting` are waiting. A background
    /// merge polls it and abandons itself when it reads nonzero.
    merge_preempt: std.atomic.Value(u32) = .init(0),

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

        var schema_owner = try acquireSchema(allocator, io, table_dir, maybe_schema, cfg.sync_mode != .none);
        errdefer schema_owner.deinit();
        const schema = schema_owner.view();

        // Enforce v0.2 limitation: unique-key upsert is implemented only for
        // single-column bigint keys. Reject anything else at open time so
        // users don't get silent no-op upserts.
        if (schema.unique) try validateUniqueKey(schema);

        const fp = schemaFingerprint(schema);

        var segments_dir = try table_dir.createDirPathOpen(io, "segments", .{});
        errdefer segments_dir.close(io);

        // The log belongs beside the manifest. One inside segments/ is the
        // footprint of a writer that outlived a directory swap and holds
        // acked rows the replay below would never see; opening anyway would
        // drop them silently.
        if (segments_dir.access(io, engine.wal.wal_filename, .{})) |_| {
            return Error.WalOrphaned;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }

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
        var replayed_tombstones: engine.wal.SegmentTombstones = .empty;
        defer engine.wal.deinitSegmentTombstones(allocator, &replayed_tombstones);
        const replayed = try engine.wal.replayFromCheckpoint(allocator, io, table_dir, fp, memtable, manifest.wal_checkpoint, &replayed_tombstones);
        if (replayed.did_replay) manifest.wal_checkpoint = replayed.checkpoint;

        const name_copy = try allocator.dupe(u8, name);
        errdefer allocator.free(name_copy);

        const order_key_indices = try allocator.alloc(usize, schema.order_key.len);
        errdefer allocator.free(order_key_indices);
        for (schema.order_key, 0..) |k, i| {
            order_key_indices[i] = schema.columnIndex(k) orelse return Error.SchemaMismatch;
        }

        var owned_cache = false;
        const cache_ptr = cfg.block_cache orelse blk: {
            const c = try allocator.create(storage.cache.Cache);
            c.* = storage.cache.Cache.init(allocator, api.autoCacheSizeBytes(cfg.cache_size_bytes));
            owned_cache = true;
            break :blk c;
        };
        errdefer if (owned_cache) {
            cache_ptr.deinit();
            allocator.destroy(cache_ptr);
        };

        const self = try allocator.create(Table);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .statement_gate = cfg.statement_gate,
            .io = io,
            .name = name_copy,
            .schema_owner = schema_owner,
            .schema = schema,
            .schema_fingerprint = fp,
            .row_group_size = row_group_size,
            .order_key_indices = order_key_indices,
            .cache = cache_ptr,
            .cache_uid = storage.cache.newTableUid(),
            .owned_cache = owned_cache,
            .auto_flush_bytes = cfg.auto_flush_bytes,
            .auto_flush_rows = cfg.auto_flush_rows,
            .auto_flush_secs = cfg.auto_flush_secs,
            .auto_flush_min_rows = cfg.auto_flush_min_rows,
            .auto_flush_min_bytes = cfg.auto_flush_min_bytes,
            .query_memory_budget = api.autoQueryBudgetBytes(cfg.query_memory_budget),
            .memory_pool = cfg.memory_pool,
            .compact_threads = api.resolveCompactThreads(allocator, cfg.compact_threads),
            .sync_mode = cfg.sync_mode,
            .table_dir = table_dir,
            .segments_dir = segments_dir,
            .manifest = manifest,
            .memtable = memtable,
            .wal = null,
            .next_segment_id = .init(manifest.nextSegmentId()),
        };

        // Reclaim orphaned segment files: a crash mid-compaction, or a #137
        // pending delete that never drained before shutdown, leaves .dat/
        // .tomb files no manifest entry references. Nothing is writing yet,
        // so any unreferenced file here is garbage. Best-effort.
        self.sweepOrphanedSegmentFiles() catch {};
        self.loadKeyBloomSidecars();
        // The fresh WAL created below drops the replayed log, so the offsets
        // its records carry must be in the segments first.
        try self.mergeReplayedTombstonesLocked(&replayed_tombstones);

        // If we replayed WAL records into a unique-key table's memtable,
        // re-run upsert resolution so any same-key dupes get tombstoned
        // (intra-memtable + against existing segments). Without this,
        // unique-key tables wouldn't behave the same after a crash as
        // they did before.
        if (schema.unique and self.memtable.row_count > 0) {
            try @import("upsert.zig").applyUpsertResolution(self);
        }

        if (cfg.wal_enabled) {
            // Replay already happened above (from the on-disk file).
            // Persist any replayed rows BEFORE truncating the log: a
            // second crash before the next flush would otherwise drop
            // them (the fresh WAL no longer carries their records).
            if (self.memtable.row_count > 0) try self.flushLocked();
            // Now truncate-and-recreate so subsequent writes go through
            // a fresh log.
            self.wal = try engine.wal.WalWriter.create(allocator, io, table_dir, fp);
        }
        return self;
    }

    pub fn close(self: *Table) void {
        const allocator = self.allocator;
        const io = self.io;
        self.pending_deletes.deinit(allocator);
        if (self.wal) |*w| w.deinit();
        self.upsert_idx.deinit(allocator);
        if (self.upsert_idx_arena) |*a| a.deinit();
        self.seg_handles.deinit(allocator);
        if (self.owned_cache) {
            self.cache.deinit();
            allocator.destroy(self.cache);
        } else {
            self.cache.purgeTable(self.cache_uid);
        }
        // Drop the Table's reference. If scans pinned a snapshot, the
        // memtable stays alive until the last reader releases it.
        self.memtable.release();
        self.manifest.deinit();
        if (self.dirs_open) {
            self.segments_dir.close(io);
            self.table_dir.close(io);
        }
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
        const statement_lease = try self.acquireStatement();
        defer if (statement_lease) |lease| lease.release();
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
        const statement_lease = try self.acquireStatement();
        defer if (statement_lease) |lease| lease.release();
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
        const statement_lease = try self.acquireStatement();
        defer if (statement_lease) |lease| lease.release();
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

    /// INSERT IGNORE / INSERT ... ON DUPLICATE KEY UPDATE on a unique table:
    /// a row whose key the table holds follows `action` instead of replacing
    /// the stored row (see upsert.zig).
    pub fn insertBatchOnDuplicate(
        self: *Table,
        batch_schema: []const types.Column,
        views: []const storage.ColumnView,
        row_count: usize,
        action: upsert_mod.OnDuplicate,
    ) !upsert_mod.DuplicateCounts {
        const statement_lease = try self.acquireStatement();
        defer if (statement_lease) |lease| lease.release();
        self.mutex.lockUncancelable(self.io);
        var wal_target: ?u64 = null;
        const counts = blk: {
            defer self.mutex.unlock(self.io);
            break :blk try upsert_mod.insertOnDuplicateLocked(self, batch_schema, views, row_count, action, &wal_target);
        };
        try self.awaitWalDurable(wal_target);
        return counts;
    }

    /// `insertBatchOnDuplicate` for a caller already holding `self.mutex`.
    pub fn insertBatchOnDuplicateLocked(
        self: *Table,
        batch_schema: []const types.Column,
        views: []const storage.ColumnView,
        row_count: usize,
        action: upsert_mod.OnDuplicate,
    ) !upsert_mod.DuplicateCounts {
        var wal_target: ?u64 = null;
        const counts = try upsert_mod.insertOnDuplicateLocked(self, batch_schema, views, row_count, action, &wal_target);
        try self.awaitWalDurable(wal_target);
        return counts;
    }

    pub fn insertBatchInner(
        self: *Table,
        batch_schema: []const types.Column,
        views: []const storage.ColumnView,
        row_count: usize,
    ) !?u64 {
        try self.ensureUsable();
        const before_count: usize = @intCast(self.memtable.row_count);
        try self.appendBatchLocked(batch_schema, views, row_count);
        return self.commitInsertLocked(before_count);
    }

    /// The memtable half of a columnar insert; the caller logs it.
    fn appendBatchLocked(
        self: *Table,
        batch_schema: []const types.Column,
        views: []const storage.ColumnView,
        row_count: usize,
    ) !void {
        try self.cloneMemtableIfPinnedLocked();
        const was_empty = self.memtable.isEmpty();
        try self.memtable.insertColumnarBatch(batch_schema, views, row_count);
        if (was_empty and !self.memtable.isEmpty()) {
            self.first_write_ts = Io.Clock.awake.now(self.io);
        }
    }

    /// Log the memtable rows from `before_count` on and resolve their keys
    /// (last writer wins on a unique table). Up to the log record a failure
    /// takes the rows back out, leaving the table as it was. After it, only
    /// a tombstone write can fail, and that fences the table so a reopen
    /// replays the log.
    fn commitInsertLocked(self: *Table, before_count: usize) !?u64 {
        var resolution: upsert_mod.Resolution = .{};
        defer resolution.deinit(self.allocator);
        var wal_target: ?u64 = null;
        {
            errdefer {
                self.memtable.truncate(before_count);
                if (self.schema.unique) upsert_mod.resetIndex(self);
            }
            if (self.schema.unique) resolution = try upsert_mod.prepareResolution(self, self.memtable, self.memtable_gen, null);
            if (self.wal) |*w| wal_target = try w.appendInsert(self.memtable, before_count, @intCast(self.memtable.row_count));
        }
        upsert_mod.commitResolution(self, &resolution) catch |err| {
            self.requireRecovery();
            return err;
        };
        try self.maybeAutoFlushLocked();
        return wal_target;
    }

    /// How much change a statement stages before it commits any: an UPDATE
    /// or DELETE (`Statement`), or the rows an INSERT ... SELECT gathers into
    /// one batch, which the table validates whole before taking any of it.
    /// Within the bound a failed statement leaves nothing behind, as MySQL
    /// rolls it back. Past it, an UPDATE or DELETE commits in chunks of about
    /// this size and an INSERT ... SELECT streams batch by batch, so memory
    /// stays bounded.
    pub const STATEMENT_STAGE_BYTES: usize = 64 << 20;

    /// One UPDATE or DELETE, staged step by step and committed whole (#335).
    ///
    /// A step changes the live memtable at once. Its segment offsets and its
    /// log record wait here for `commitStatementLocked`, so a statement that
    /// fails first leaves nothing on disk, and `endStatementLocked` puts the
    /// memtable back. No reader sees a staged step: readers capture the
    /// memtable under the table mutex, which the statement holds throughout.
    ///
    /// Past `STATEMENT_STAGE_BYTES` of staged change a statement commits what
    /// it has, flushes if due, and goes on as a new chunk. It is then atomic
    /// per chunk, not whole.
    pub const Statement = struct {
        /// The memtable the chunk started from, and its row count.
        base: *engine.Memtable = undefined,
        base_rows: usize = 0,
        base_first_write_ts: ?Io.Timestamp = null,
        /// Set once a step swapped `base` out. The table's reference to it
        /// passes to the statement, so a rollback can put it back.
        holds_base: bool = false,
        /// The segment rows the chunk deletes, per segment: its own, and the
        /// older versions of the keys it writes.
        tombstones: engine.wal.SegmentTombstones = .empty,
        steps: engine.wal.StatementSteps = .{},
        /// Whether a step removed or added memtable rows.
        touches_memtable: bool = false,
        staged_bytes: usize = 0,
        wal_target: ?u64 = null,
    };

    /// Rows one step of an UPDATE or DELETE removes: memtable rows (`keep[i]`
    /// false for each, `rows` holding their values) or offsets in one
    /// segment.
    pub const Replaced = union(enum) {
        memtable: struct { keep: []const bool, rows: []const engine.ColumnStore, row_count: usize },
        segment: struct { id: u64, offsets: []const u32 },
    };

    /// Start `s` on this table. Pair with `endStatementLocked`.
    pub fn beginStatementLocked(self: *Table, s: *Statement) void {
        std.debug.assert(self.statement == null);
        s.base = self.memtable;
        s.base_rows = @intCast(self.memtable.row_count);
        s.base_first_write_ts = self.first_write_ts;
        s.holds_base = false;
        s.touches_memtable = false;
        s.staged_bytes = 0;
        self.statement = s;
    }

    /// Stage one step of `s`: remove `replaced` and add `rows` (none for a
    /// DELETE) in its place. Both halves go into the statement's log record
    /// together, so recovery can't keep the delete and lose the rows (#48).
    pub fn replaceRowsLocked(
        self: *Table,
        s: *Statement,
        replaced: Replaced,
        rows: []const engine.ColumnStore,
        row_count: usize,
    ) !void {
        std.debug.assert(self.statement == s);
        try self.ensureUsable();
        var staged: ?*engine.Memtable = null;
        defer if (staged) |mt| mt.release();
        var removed_bytes: usize = 0;
        switch (replaced) {
            .memtable => |m| {
                staged = try self.memtable.cloneWithRetainedRows(self.allocator, m.keep);
                removed_bytes = engine.memtable.columnsByteSize(m.rows);
            },
            .segment => |seg| {
                try engine.wal.addSegmentTombstones(self.allocator, &s.tombstones, seg.id, seg.offsets);
                removed_bytes = seg.offsets.len * @sizeOf(u32);
            },
        }
        if (staged) |mt| upsert_mod.carryIndexKept(self, replaced.memtable.keep, mt, self.memtable_gen + 1);
        var resolution: upsert_mod.Resolution = .{};
        defer resolution.deinit(self.allocator);
        if (row_count > 0) {
            const views = try self.allocator.alloc(storage.ColumnView, rows.len);
            defer self.allocator.free(views);
            for (rows, views) |*store, *view| view.* = store.view();
            if (staged) |mt| {
                try mt.insertColumnarBatch(self.schema.columns, views, row_count);
            } else {
                try self.appendBatchLocked(self.schema.columns, views, row_count);
            }
            if (self.schema.unique) resolution = if (staged) |mt|
                try upsert_mod.prepareResolution(self, mt, self.memtable_gen + 1, &s.tombstones)
            else
                try upsert_mod.prepareResolution(self, self.memtable, self.memtable_gen, &s.tombstones);
        }
        if (self.wal != null) switch (replaced) {
            .memtable => |m| try s.steps.append(self.allocator, self.schema.columns, m.rows, m.row_count, 0, &.{}, rows, row_count),
            .segment => |seg| try s.steps.append(self.allocator, self.schema.columns, &.{}, 0, seg.id, seg.offsets, rows, row_count),
        };
        if (staged) |mt| {
            staged = null;
            self.installMemtableLocked(mt);
        }
        try upsert_mod.commitResolution(self, &resolution);
        if (replaced == .memtable or row_count > 0) s.touches_memtable = true;
        s.staged_bytes += removed_bytes + engine.memtable.columnsByteSize(rows);
        if (s.staged_bytes > self.stage_bytes) {
            try self.commitStagedLocked(s);
            try self.maybeAutoFlushLocked();
            self.beginStatementLocked(s);
        }
    }

    /// Stage the removal of the memtable rows `keep` marks false and return
    /// how many. The log names these rows, so replay removes exactly them
    /// instead of re-running the DELETE's predicate (#61).
    pub fn deleteMemtableRowsLocked(self: *Table, s: *Statement, keep: []const bool) !usize {
        const removed_count = std.mem.countScalar(bool, keep, false);
        if (removed_count == 0) return 0;
        const removed_mask = try self.allocator.alloc(bool, keep.len);
        defer self.allocator.free(removed_mask);
        for (keep, removed_mask) |k, *r| r.* = !k;
        const removed = try self.memtable.cloneWithRetainedRows(self.allocator, removed_mask);
        defer if (removed) |mt| mt.release();
        const rows = if (removed) |mt| mt.columns else self.memtable.columns;
        const replaced: Replaced = .{ .memtable = .{ .keep = keep, .rows = rows, .row_count = removed_count } };
        try self.replaceRowsLocked(s, replaced, &.{}, 0);
        return removed_count;
    }

    /// Commit what `s` has staged and end it. Returns the WAL offset to
    /// await once the table mutex is released.
    pub fn commitStatementLocked(self: *Table, s: *Statement) !?u64 {
        try self.commitStagedLocked(s);
        // The statement has committed. Failing it now for a flush would
        // invite a retry that applies it twice; the memtable is intact, and
        // the next write or the background flusher tries the flush again.
        self.maybeAutoFlushLocked() catch {};
        return s.wal_target;
    }

    /// Take back what `s` staged since it last committed, if anything, and
    /// free it.
    pub fn endStatementLocked(self: *Table, s: *Statement) void {
        if (self.statement == s) self.rollbackStatementLocked(s);
        engine.wal.deinitSegmentTombstones(self.allocator, &s.tombstones);
        s.steps.deinit(self.allocator);
    }

    /// Log the chunk's steps as one record, then write its tombstone files,
    /// and end the chunk. A failure up to the record leaves the chunk to
    /// `endStatementLocked`. After it the chunk stands, and a failed
    /// tombstone write fences the table so a reopen replays the log. A chunk
    /// whose only change is offsets in one segment needs no record: its one
    /// tombstone file write commits it.
    fn commitStagedLocked(self: *Table, s: *Statement) !void {
        try self.ensureUsable();
        const TombstoneFile = struct { segment_id: u64, bytes: []u8 };
        var files: std.ArrayList(TombstoneFile) = .empty;
        defer {
            for (files.items) |f| self.allocator.free(f.bytes);
            files.deinit(self.allocator);
        }
        try files.ensureTotalCapacity(self.allocator, s.tombstones.count());
        for (s.tombstones.keys(), s.tombstones.values()) |segment_id, offsets| {
            const bytes = try storage.tombstone.encodeMerged(self.allocator, self.io, self.segments_dir, segment_id, offsets.items);
            files.appendAssumeCapacity(.{ .segment_id = segment_id, .bytes = bytes });
        }
        const logged = self.wal != null and (s.touches_memtable or files.items.len > 1);
        if (logged) s.wal_target = try self.wal.?.appendStatement(&s.steps);
        for (files.items, 0..) |f, i| self.writeTombstoneFile(f.segment_id, f.bytes) catch |err| {
            if (!logged and i == 0) return err;
            self.requireRecovery();
            self.endChunkLocked(s);
            return err;
        };
        self.endChunkLocked(s);
    }

    fn endChunkLocked(self: *Table, s: *Statement) void {
        self.statement = null;
        if (s.holds_base) s.base.release();
        s.holds_base = false;
        engine.wal.deinitSegmentTombstones(self.allocator, &s.tombstones);
        s.steps.clear();
    }

    fn rollbackStatementLocked(self: *Table, s: *Statement) void {
        self.statement = null;
        if (s.holds_base) {
            s.holds_base = false;
            const staged = self.memtable;
            self.memtable = s.base;
            self.memtable_gen += 1;
            s.base.retired.store(false, .release);
            staged.retire();
            staged.release();
        }
        std.debug.assert(self.memtable == s.base);
        self.memtable.truncate(s.base_rows);
        self.first_write_ts = s.base_first_write_ts;
        if (self.schema.unique) upsert_mod.resetIndex(self);
    }

    /// Merge the offsets replayed `replace` records carry into the segments'
    /// tombstone files. Offsets for a segment the manifest no longer lists
    /// are dropped: it was compacted or truncated away after they had been
    /// merged.
    fn mergeReplayedTombstonesLocked(self: *Table, replayed: *const engine.wal.SegmentTombstones) !void {
        const sync = self.syncEnabled();
        for (replayed.keys(), replayed.values()) |segment_id, offsets| {
            if (offsets.items.len == 0) continue;
            const listed = for (self.manifest.segments.items) |entry| {
                if (entry.segment_id == segment_id) break true;
            } else false;
            if (!listed) continue;
            try self.mergeTombstones(self.allocator, segment_id, offsets.items, sync);
            self.seg_handles.invalidateTombstones(self.allocator, segment_id);
        }
    }

    /// Mutates the memtable + appends bytes to the WAL (no fsync). The
    /// returned offset is the cumulative WAL write_offset after our append,
    /// passed to `awaitDurable` once the Table mutex is released so a
    /// concurrent batch of writers can amortize a single fsync syscall.
    /// Returns null when no WAL is configured.
    fn insertLocked(self: *Table, rows: anytype) !?u64 {
        try self.ensureUsable();
        try self.cloneMemtableIfPinnedLocked();
        const was_empty = self.memtable.isEmpty();
        const before_count: usize = @intCast(self.memtable.row_count);
        try self.memtable.insertRows(rows);
        if (was_empty and !self.memtable.isEmpty()) {
            self.first_write_ts = Io.Clock.awake.now(self.io);
        }
        return self.commitInsertLocked(before_count);
    }

    /// Retire-replace the active memtable. EVERY swap must route through
    /// here: the generation bump is what invalidates the incremental upsert
    /// index, unless the swapper carried it to the new generation first
    /// (`upsert.carryIndex`). Binding that index by pointer identity is an
    /// ABA data-loss bug — the allocator can hand a later clone the freed
    /// memtable's address, and the stale index's row mappings then
    /// tombstone unrelated rows on the next upsert. A running statement
    /// takes over the table's reference to the memtable it started from, so
    /// a rollback can put it back. Caller holds the table mutex.
    pub fn installMemtableLocked(self: *Table, new_mt: *engine.Memtable) void {
        const old_mt = self.memtable;
        self.memtable = new_mt;
        self.memtable_gen += 1;
        old_mt.retire();
        if (self.statement) |s| if (old_mt == s.base) {
            s.holds_base = true;
            return;
        };
        old_mt.release();
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
        upsert_mod.carryIndex(self, &.{}, cloned, self.memtable_gen + 1);
        self.installMemtableLocked(cloned);
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
        const statement_lease = try self.acquireStatement();
        defer if (statement_lease) |lease| lease.release();
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        try self.flushLocked();
    }

    pub fn flushLocked(self: *Table) !void {
        try self.ensureUsable();
        std.debug.assert(self.statement == null);
        if (self.memtable.isEmpty()) {
            self.first_write_ts = null;
            // What the log's records wrote is in the tombstone files or was
            // taken back by a later record, so it can go. This way a clean
            // stop always leaves an empty log, which the previous release can
            // open, while a `statement` record would stop it (#335).
            if (self.wal) |*w| if (w.physical_offset > engine.wal.header_size) try self.replaceWal();
            return;
        }

        const seg_id = self.allocSegmentId();

        var name_buf: [32]u8 = undefined;
        const file_name = try std.fmt.bufPrint(&name_buf, "{d:0>20}.dat", .{seg_id});

        var snapshot = try self.memtable.buildSortedSnapshot(self.allocator, self.order_key_indices, self.compact_threads);
        defer snapshot.deinit();

        // Allocate the replacement memtable up front. If any of the segment /
        // manifest / WAL work below fails, errdefer frees the new one and we
        // leave the current memtable intact.
        const new_mt = try engine.Memtable.create(self.allocator, self.schema);
        var owns_new_mt = true;
        errdefer if (owns_new_mt) new_mt.release();

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
            self.compact_threads,
        );
        defer info.deinit(self.allocator);

        // Compound-key Bloom over the flushed rows — carried into the in-memory
        // manifest entry so the upsert probe can skip this segment with zero
        // I/O when a key definitely isn't present (#138), and persisted as a
        // `<id>.bloom` sidecar written ONCE here — not into manifest.bin,
        // which is rewritten wholly on every flush under the table mutex and
        // would grow O(total rows) (#140). Unique tables only. A crash after
        // the segment but before the sidecar just costs pruning (probe treats
        // a missing sidecar as "no filter").
        if (self.schema.unique and self.order_key_indices.len > 0 and info.row_count > 0) {
            var hashes: std.ArrayList(u64) = .empty;
            defer hashes.deinit(self.allocator);
            try upsert_mod.appendKeyHashes(self.allocator, &hashes, snapshot.views, self.order_key_indices, @intCast(info.row_count));
            info.key_bloom = try upsert_mod.serializeKeyBloom(self.allocator, hashes.items);
            var bbuf: [32]u8 = undefined;
            const bloom_name = try Table.segmentBloomFileName(&bbuf, seg_id);
            try storage.writeFileSynced(self.io, self.segments_dir, bloom_name, info.key_bloom, sync);
        }

        var entries: std.ArrayList(storage.ManifestEntry) = .empty;
        defer entries.deinit(self.allocator);
        try entries.ensureTotalCapacity(self.allocator, self.manifest.segments.items.len + 1);
        entries.appendSliceAssumeCapacity(self.manifest.segments.items);
        var entry = try self.entryFor(info);
        var owns_entry = true;
        errdefer if (owns_entry) entry.deinit(self.allocator);
        entries.appendAssumeCapacity(entry);
        var candidate = self.manifest;
        candidate.segments = entries;
        if (self.wal) |*w| candidate.wal_checkpoint = w.checkpoint();
        if (sync) try storage.syncDirectory(self.io, self.segments_dir);
        try self.persistManifest(candidate, sync);

        self.manifest.segments.deinit(self.allocator);
        self.manifest = candidate;
        entries = .empty;
        owns_entry = false;

        // Retire-replace: swap the active memtable for the fresh one. The
        // old memtable's columns are not mutated again; any scan that pinned
        // it via `acquire` continues to read it safely until its `release`
        // drops the refcount to zero and frees it.
        self.installMemtableLocked(new_mt);
        owns_new_mt = false;
        self.first_write_ts = null;

        // The manifest owns the WAL checkpoint now. A failed cleanup leaves
        // both the live view and restart recovery at the same committed state.
        try self.replaceWal();
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
    ///
    /// The sweep caller swallows errors (one bad table must not stop the
    /// sweep), so a deterministically-failing flush would otherwise retry
    /// silently forever while the memtable stays volatile — log it here,
    /// throttled, so the failure is visible in the server log.
    pub fn tryBackgroundFlush(self: *Table) !void {
        try self.ensureUsable();
        if (!self.mutex.tryLock()) return;
        defer self.mutex.unlock(self.io);
        self.drainPendingDeletesLocked();
        self.maybeAutoFlushLocked() catch |err| {
            self.recordIoFailure(err);
            self.flush_fail_streak +|= 1;
            if (self.flush_fail_streak == 1 or self.flush_fail_streak % 60 == 0) {
                std.debug.print(
                    "thindb: background flush failed on table '{s}' ({d} consecutive): {s}\n",
                    .{ self.name, self.flush_fail_streak, @errorName(err) },
                );
            }
            return err;
        };
        self.flush_fail_streak = 0;
    }

    /// Background-compactor pick. Caller (the background compact sweep)
    /// already holds `compact_lock`. Returns the segment ids of the next
    /// merge, considering both the tombstone-pressure trigger and (when at
    /// least `min_segments` are live) the count-based tier trigger; null when
    /// no segment qualifies or both gates are disabled. Caller owns the ids.
    pub fn pickBackgroundCompaction(self: *Table, min_segments: u32, tomb_threshold: f32) !?[]u64 {
        try self.ensureUsable();
        // Cheap optimization: skip the work if neither trigger can fire.
        self.mutex.lockUncancelable(self.io);
        const seg_count = self.manifest.segments.items.len;
        self.mutex.unlock(self.io);
        const enough_for_tier = (min_segments != 0 and seg_count >= min_segments);
        const tomb_enabled = (tomb_threshold <= 1.0);
        if (!enough_for_tier and !tomb_enabled) return null;
        return @import("compact.zig").pickTieredGroup(self, tomb_threshold);
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
        return storage.manifest.entryFromSegmentInfo(self.allocator, info, lk_idx, self.schema.columns);
    }

    /// True iff this table is configured for durable writes (Config.sync_mode).
    /// Storage primitives that fsync take this as a parameter.
    pub fn ensureUsable(self: *const Table) !void {
        if (self.recovery_required.load(.acquire)) return error.RecoveryRequired;
        if (self.statement_gate) |g| if (g.recovery_required.load(.acquire)) return error.RecoveryRequired;
    }

    pub fn acquireStatement(self: *Table) !?StatementGate.Lease {
        try self.ensureUsable();
        return if (self.statement_gate) |gate| try gate.acquire(false) else null;
    }

    pub fn persistManifest(self: *Table, manifest: storage.Manifest, sync: bool) !void {
        try self.ensureUsable();
        storage.writeManifest(self.io, self.table_dir, manifest, sync) catch |err| {
            self.recordIoFailure(err);
            return err;
        };
    }

    pub fn mergeTombstones(self: *Table, scratch: Allocator, id: u64, offsets: []const u32, sync: bool) !void {
        try self.ensureUsable();
        // A tombstone can hide a row whose replacement so far lives only in
        // the WAL (UPDATE, upsert). Syncing the log first means power loss
        // can't keep the tombstone and lose the replacement.
        if (sync) if (self.wal) |*w| try w.awaitAllDurable(self.io);
        storage.tombstone.merge(scratch, self.io, self.segments_dir, id, offsets, sync) catch |err| {
            self.recordIoFailure(err);
            return err;
        };
    }

    /// Replace segment `id`'s tombstone file with `bytes` from
    /// `storage.tombstone.encodeMerged`, as `mergeTombstones` would.
    pub fn writeTombstoneFile(self: *Table, id: u64, bytes: []const u8) !void {
        try self.ensureUsable();
        const sync = self.syncEnabled();
        if (sync) if (self.wal) |*w| try w.awaitAllDurable(self.io);
        storage.tombstone.writeEncoded(self.io, self.segments_dir, id, bytes, sync) catch |err| {
            self.recordIoFailure(err);
            return err;
        };
        self.seg_handles.invalidateTombstones(self.allocator, id);
    }

    fn replaceWal(self: *Table) !void {
        if (self.wal) |*w| w.truncate(self.schema_fingerprint) catch |err| {
            self.recordIoFailure(err);
            return err;
        };
    }

    fn recordIoFailure(self: *Table, err: anyerror) void {
        if (err != error.DurabilityUncertain) return;
        self.requireRecovery();
    }

    /// Fence every later operation with `RecoveryRequired` until the database
    /// is reopened. Raised when the on-disk state can no longer be trusted:
    /// an uncertain durability write, or a DDL swap that failed after the
    /// table's directory handles were closed.
    pub fn requireRecovery(self: *Table) void {
        self.recovery_required.store(true, .release);
        if (self.statement_gate) |gate| gate.recovery_required.store(true, .release);
    }

    pub fn syncEnabled(self: Table) bool {
        return self.sync_mode != .none;
    }

    pub fn segmentFileName(buf: []u8, seg_id: u64) ![]u8 {
        return std.fmt.bufPrint(buf, "{d:0>20}.dat", .{seg_id});
    }

    /// Compound-key Bloom sidecar (#140). Written once when the segment is
    /// (flush or compaction), deleted with it.
    pub fn segmentBloomFileName(buf: []u8, seg_id: u64) ![]u8 {
        return std.fmt.bufPrint(buf, "{d:0>20}.bloom", .{seg_id});
    }

    /// Allocate a fresh, never-reused (within this run) segment ID.
    pub fn allocSegmentId(self: *Table) u64 {
        return self.next_segment_id.fetchAdd(1, .monotonic);
    }

    /// Delete every row matching `pred`. Scans all segments + the memtable,
    /// emits tombstones for matches in segments, and rebuilds the memtable
    /// without them. Returns the number of rows deleted.
    pub fn delete(self: *Table, pred: exec.Predicate) !usize {
        const statement_lease = try self.acquireStatement();
        defer if (statement_lease) |lease| lease.release();
        self.mutex.lockUncancelable(self.io);
        var wal_target: ?u64 = null;
        var deleted: usize = 0;
        {
            defer self.mutex.unlock(self.io);
            try self.ensureUsable();
            var stmt: Statement = .{};
            self.beginStatementLocked(&stmt);
            defer self.endStatementLocked(&stmt);
            deleted = try @import("delete.zig").execDelete(self, &stmt, pred);
            wal_target = try self.commitStatementLocked(&stmt);
        }
        try self.awaitWalDurable(wal_target);
        return deleted;
    }

    /// SQL `DELETE FROM t [WHERE expr]` — generalized delete with the
    /// rich PredicateExpr. Subqueries and `@vars` must already be
    /// resolved by the pre-compile pass. `pred == null` deletes every
    /// row; `derived` are the computed operands it compares by name.
    /// Returns the deleted row count. Runs as one `Statement`, streaming
    /// row group by row group.
    pub fn deleteByExpr(self: *Table, pred: ?exec.PredicateExpr, derived: []const exec.Derived) !usize {
        const statement_lease = try self.acquireStatement();
        defer if (statement_lease) |lease| lease.release();

        self.mutex.lockUncancelable(self.io);
        var wal_target: ?u64 = null;
        var deleted: usize = 0;
        {
            defer self.mutex.unlock(self.io);
            try self.ensureUsable();
            var stmt: Statement = .{};
            self.beginStatementLocked(&stmt);
            defer self.endStatementLocked(&stmt);
            deleted = try @import("delete.zig").execDeleteByExpr(self, &stmt, pred, derived);
            wal_target = try self.commitStatementLocked(&stmt);
        }
        try self.awaitWalDurable(wal_target);
        return deleted;
    }

    /// Batched keyed DELETE (#146): N `DELETE ... WHERE <full key>`
    /// statements executed as one segment sweep under a single mutex
    /// acquisition — one Bloom/zonemap-pruned probe pass, one tombstone
    /// write per touched segment. `counts[j]` gets statement j's
    /// affected-row count. Returns null (before any mutation) when a
    /// statement isn't a strict full-key equality conjunction on a
    /// unique table; the caller must then execute them individually.
    pub fn deleteKeyedBatch(
        self: *Table,
        preds: []const ?exec.PredicateExpr,
        counts: []usize,
    ) !?usize {
        const statement_lease = try self.acquireStatement();
        defer if (statement_lease) |lease| lease.release();
        const del = @import("delete.zig");

        // Widen literals up front (same as deleteByExpr) so key encoding and
        // zonemap checks see the widened shape.
        const local_preds = try self.allocator.alloc(?exec.PredicateExpr, preds.len);
        defer self.allocator.free(local_preds);
        for (preds, local_preds) |p, *lp| {
            lp.* = p;
            if (lp.*) |*x| try exec.predicate.validateExpr(x, self.schema.columns);
        }

        self.mutex.lockUncancelable(self.io);
        var wal_target: ?u64 = null;
        var deleted: ?usize = null;
        {
            defer self.mutex.unlock(self.io);
            try self.ensureUsable();
            if (!try del.keyedBatchEligible(self, local_preds)) return null;
            var stmt: Statement = .{};
            self.beginStatementLocked(&stmt);
            defer self.endStatementLocked(&stmt);
            deleted = try del.execDeleteKeyedBatch(self, &stmt, local_preds, counts);
            wal_target = try self.commitStatementLocked(&stmt);
        }
        try self.awaitWalDurable(wal_target);
        return deleted;
    }

    /// SQL `UPDATE t SET ... [WHERE expr]`: per-batch delete+insert pairs
    /// under the table mutex, committed as one statement (see update.zig).
    pub fn updateStreaming(
        self: *Table,
        pred: ?exec.PredicateExpr,
        derived: []const exec.Derived,
        assignments: []const @import("update.zig").Assignment,
    ) !usize {
        const statement_lease = try self.acquireStatement();
        defer if (statement_lease) |lease| lease.release();
        return try @import("update.zig").execUpdateStreaming(self, pred, derived, assignments);
    }

    /// Remove every row while preserving schema and table identity. This is a
    /// DDL-style physical reset: segments, tombstones, memtable, WAL contents,
    /// row-group cache, and AUTO_INCREMENT state are cleared together.
    pub fn truncate(self: *Table) !void {
        const statement_lease = try self.acquireStatement();
        defer if (statement_lease) |lease| lease.release();
        self.lockCompactPreempting();
        defer self.compact_lock.unlock(self.io);
        self.ddl_lock.lockUncancelable(self.io);
        defer self.ddl_lock.unlock(self.io);
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        try self.ensureUsable();

        var candidate = storage.Manifest.empty(self.allocator, self.schema_fingerprint, @intCast(self.schema.columns.len));
        var owns_candidate = true;
        errdefer if (owns_candidate) candidate.deinit();
        if (autoIncrementColumnIndex(self.schema) != null) candidate.auto_inc_next = 1;
        if (self.wal) |*w| candidate.wal_checkpoint = w.checkpoint();
        const replacement = try engine.Memtable.create(self.allocator, self.schema);
        var owns_replacement = true;
        errdefer if (owns_replacement) replacement.release();
        try self.persistManifest(candidate, self.syncEnabled());
        var previous = self.manifest;
        defer previous.deinit();
        self.manifest = candidate;
        owns_candidate = false;
        self.installMemtableLocked(replacement);
        owns_replacement = false;
        self.first_write_ts = null;
        self.cache.purgeTable(self.cache_uid);
        self.cache_uid = storage.cache.newTableUid();
        self.seg_handles.clear(self.allocator);
        // Keep IDs monotonic while old files may remain queued for deletion.
        try self.replaceWal();
        for (previous.segments.items) |entry| try self.deleteSegmentFiles(entry.segment_id);
    }

    /// Take `compact_lock` for a path that frees or rewrites the table: DROP,
    /// ALTER, RENAME, TRUNCATE, XA COMMIT and schema teardown. Those usually
    /// hold the catalog gate exclusively, which queues every new statement, so
    /// they must not wait out a background merge that can run for minutes
    /// (#87). The merge sees the request between steps, and also while it
    /// waits to commit, deletes what it wrote and releases the lock; its
    /// segments stay for a later sweep.
    pub fn lockCompactPreempting(self: *Table) void {
        _ = self.merge_preempt.fetchAdd(1, .acq_rel);
        defer _ = self.merge_preempt.fetchSub(1, .acq_rel);
        self.ddl_lock.wakeTimedWriters(self.io);
        self.compact_lock.lockUncancelable(self.io);
    }

    /// Merge all segments into a single new segment. Drops tombstoned rows.
    /// No-op if there's at most one segment.
    pub fn compact(self: *Table) !void {
        const statement_lease = try self.acquireStatement();
        defer if (statement_lease) |lease| lease.release();
        self.compact_lock.lockUncancelable(self.io);
        defer self.compact_lock.unlock(self.io);
        try @import("compact.zig").execCompact(self);
    }

    /// This table generation's handle into the shared block cache — what the
    /// segment-read path takes to build uid-scoped cache keys.
    pub fn cacheRef(self: *const Table) storage.cache.TableCache {
        return .{ .cache = self.cache, .table_uid = self.cache_uid };
    }

    /// Get-or-open the cached parsed segment (pinned; pair with
    /// `releaseSegment`). Shared across queries and scan workers.
    pub fn acquireSegment(self: *Table, seg_id: u64) !*storage.cache.SegmentHandles.Entry {
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, seg_id);
        return self.seg_handles.acquire(self.allocator, self.io, self.segments_dir, file_name, self.schema, seg_id);
    }

    pub fn releaseSegment(self: *Table, entry: *storage.cache.SegmentHandles.Entry) void {
        self.seg_handles.release(self.allocator, entry);
    }

    /// The segment's tombstone list as a dupe owned by `allocator` (null =
    /// none). Cached file read; caller frees, same contract as
    /// `storage.tombstone.read`.
    pub fn segmentTombstones(self: *Table, allocator: Allocator, entry: *storage.cache.SegmentHandles.Entry) !?[]u32 {
        return self.seg_handles.tombstones(self.allocator, allocator, self.io, self.segments_dir, entry);
    }

    pub fn deleteSegmentFiles(self: *Table, seg_id: u64) !void {
        // Close any cached handle first — Windows refuses to delete open files.
        self.seg_handles.retire(self.allocator, seg_id);
        if (!self.tryDeleteSegmentFiles(seg_id)) {
            // A pinned reader outlasted the bounded retry (long scan). Queue
            // the id so the background flush sweep reclaims the files once
            // the reader drains, instead of orphaning them on disk (#137).
            while (!self.pending_deletes_lock.tryLock()) std.atomic.spinLoopHint();
            defer self.pending_deletes_lock.unlock();
            self.pending_deletes.append(self.allocator, seg_id) catch {};
        }
    }

    fn tryDeleteSegmentFiles(self: *Table, seg_id: u64) bool {
        var dat_buf: [32]u8 = undefined;
        const dat_name = Table.segmentFileName(&dat_buf, seg_id) catch return true;
        const dat_ok = self.deleteFileTolerant(dat_name);

        var tomb_buf: [32]u8 = undefined;
        const tomb_name = storage.tombstone.fileNameFor(&tomb_buf, seg_id) catch return true;
        const tomb_ok = self.deleteFileTolerant(tomb_name);

        var bloom_buf: [32]u8 = undefined;
        const bloom_name = Table.segmentBloomFileName(&bloom_buf, seg_id) catch return true;
        const bloom_ok = self.deleteFileTolerant(bloom_name);
        return dat_ok and tomb_ok and bloom_ok;
    }

    /// Attach persisted key-Bloom sidecars (#140) to the in-memory manifest
    /// entries. Called at open and after ALTER reloads the manifest;
    /// flush/compaction attach the blooms of segments they create directly. Best-effort: a missing or torn sidecar
    /// (crash between segment write and sidecar write) just means no probe
    /// pruning for that segment.
    pub fn loadKeyBloomSidecars(self: *Table) void {
        if (!self.schema.unique) return;
        for (self.manifest.segments.items) |*entry| {
            var buf: [32]u8 = undefined;
            const name = Table.segmentBloomFileName(&buf, entry.segment_id) catch continue;
            const bytes = self.segments_dir.readFileAlloc(self.io, name, self.allocator, .limited(1 << 30)) catch continue;
            if (!bloom_util.validSerialized(bytes)) {
                self.allocator.free(bytes);
                continue;
            }
            entry.key_bloom = bytes;
        }
    }

    /// Delete any segment/tombstone file whose id has no manifest entry.
    /// Runs once at open (before any writes), so unreferenced files can only
    /// be leftovers of a crash or an undrained pending delete (#137).
    fn sweepOrphanedSegmentFiles(self: *Table) !void {
        var live: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer live.deinit(self.allocator);
        for (self.manifest.segments.items) |entry| {
            try live.put(self.allocator, entry.segment_id, {});
        }

        var to_delete: std.ArrayListUnmanaged(u64) = .empty;
        defer to_delete.deinit(self.allocator);
        // segments_dir wasn't opened with .iterate; take a scoped handle.
        var iter_dir = try self.table_dir.openDir(self.io, "segments", .{ .iterate = true });
        defer iter_dir.close(self.io);
        var it = iter_dir.iterate();
        while (try it.next(self.io)) |entry| {
            if (entry.kind != .file) continue;
            const dot = std.mem.lastIndexOfScalar(u8, entry.name, '.') orelse continue;
            const ext = entry.name[dot..];
            if (!std.mem.eql(u8, ext, ".dat") and !std.mem.eql(u8, ext, ".tomb") and !std.mem.eql(u8, ext, ".bloom")) continue;
            const id = std.fmt.parseInt(u64, entry.name[0..dot], 10) catch continue;
            if (!live.contains(id)) try to_delete.append(self.allocator, id);
        }
        // Deleting while iterating the directory is undefined on some
        // platforms — collect first, delete after.
        for (to_delete.items) |id| _ = self.tryDeleteSegmentFiles(id);
    }

    /// Retry the deletes a prior compaction couldn't finish. Called from the
    /// background flush sweep (under the table mutex).
    fn drainPendingDeletesLocked(self: *Table) void {
        while (!self.pending_deletes_lock.tryLock()) std.atomic.spinLoopHint();
        defer self.pending_deletes_lock.unlock();
        var i: usize = 0;
        while (i < self.pending_deletes.items.len) {
            if (self.tryDeleteSegmentFiles(self.pending_deletes.items[i])) {
                _ = self.pending_deletes.swapRemove(i);
            } else {
                i += 1;
            }
        }
    }

    /// Delete a segment file, retrying briefly if a reader still has it open
    /// (Windows error.FileBusy). Bounded, and never surfaces an error — a
    /// still-contended file goes on the pending-delete queue rather than
    /// crash the writer (#137). Returns false when the file is still busy.
    fn deleteFileTolerant(self: *Table, name: []const u8) bool {
        var attempt: u8 = 0;
        while (attempt < 50) : (attempt += 1) {
            self.segments_dir.deleteFile(self.io, name) catch |err| {
                if (err == error.FileBusy) {
                    Io.sleep(self.io, Io.Duration.fromMilliseconds(2), .awake) catch {};
                    continue;
                }
                return true; // FileNotFound or anything else: nothing to do
            };
            return true; // deleted
        }
        return false;
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
    durable: bool,
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
            try storage.schema_file.writeSchema(io, table_dir, s, allocator, durable);
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
