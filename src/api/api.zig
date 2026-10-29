//! Public thinDB API surface.
//!
//! v2 introduces a three-level namespace: Catalog → Database → Schema →
//! Table. Existing v1 call sites (`Database.open(...)` + `db.table(...)`)
//! remain valid via back-compat shims that route through an implicit
//! "main" database and "public" schema.

const std = @import("std");

pub const Error = error{
    SchemaMismatch,
    UnsupportedUniqueKeyType,
    UpsertRequiresUniqueKey,
    TableNotFound,
    TableAlreadyExists,
    ColumnNotFound,
    ColumnAlreadyExists,
    UnsupportedAlterOp,
    /// ALTER dropped a column of a unique key: rows unique on the whole key
    /// need not be unique on the rest of it.
    UniqueKeyColumnDrop,
    DatabaseNotFound,
    DatabaseAlreadyExists,
    /// A database name that can't name a database directory: one of the
    /// engine's own root directories (`Catalog.reserved_database_names`),
    /// or not a single path component.
    InvalidDatabaseName,
    /// An unqualified name needs the session's current database, and the
    /// session has none: it dropped it, or another session did.
    NoDatabaseSelected,
    SchemaNotFound,
    SchemaAlreadyExists,
    /// A schema name starting with an underscore: those name a database's
    /// own directories (functions, views, dropped schemas awaiting delete).
    InvalidSchemaName,
    FunctionAlreadyExists,
    FunctionInvalidDefinition,
    FunctionNotFound,
    /// A WAL file was found inside the table's segments directory. It holds
    /// acknowledged rows that replay would never see; the table refuses to
    /// open until an operator moves the log into place or aside.
    WalOrphaned,
    /// A segment the manifest lists is missing, or isn't the size it was
    /// written at: an OS crash lost it before it reached the disk. The table
    /// refuses to open, rather than answer without those rows, until it is
    /// dropped and reloaded from its source.
    SegmentDamaged,
    XaBranchTooLarge,
    XaInvalidXid,
    DatabaseInUse,
    TableBusy,
    /// Table names starting `__alter_` belong to ALTER's on-disk swap.
    ReservedTableName,
    RecoveryRequired,
    DurabilityUncertain,
    DatabaseClosed,
};

pub const SyncMode = enum { none, per_flush };

pub const FileScanAccess = union(enum) {
    /// Embedded/default behavior: paths resolve exactly as the process sees
    /// them, matching DuckDB's local-file ergonomics.
    process,
    /// Server default: SQL file scans are disabled unless an operator provides
    /// a root directory explicitly.
    disabled,
    /// Server allow-list root. SQL paths must be relative and stay under this
    /// directory.
    root: []const u8,
};

/// One schema-change operation. `alterTable` takes a slice of these and
/// applies them in order to derive the new schema, then rewrites every
/// segment under that schema.
///
/// Not supported in v1: `change_type` (per-value conversion is a separate
/// piece of work). Use drop+add as a workaround if you need it.
pub const AlterOp = union(enum) {
    /// Append a new column. Existing rows get `default` as their value
    /// when provided; nullable columns may omit it to backfill SQL NULL.
    /// NOT NULL columns must provide a default whose active tag matches
    /// `type` (e.g., type=.int requires `.int = N`).
    add: AddColumn,
    /// Remove a column by name. Errors if the column is part of the order
    /// key (would change row identity).
    drop: []const u8,
    /// Rename a column. Existing data unchanged; only the schema name
    /// changes. Errors if `to` is already a column name.
    rename: RenameColumn,
    /// Replace a column's DEFAULT and ON UPDATE CURRENT_TIMESTAMP clauses.
    /// Existing rows keep their values.
    set_clauses: SetClauses,

    pub const AddColumn = struct {
        name: []const u8,
        type: @import("../types.zig").Type,
        nullable: bool = false,
        default: ?@import("../types.zig").Value = null,
        /// The column's DEFAULT is CURRENT_TIMESTAMP; `default` holds the
        /// time existing rows take.
        default_now: bool = false,
        on_update_now: bool = false,
        position: @import("../types.zig").ColumnPosition = .last,
    };

    pub const SetClauses = struct {
        name: []const u8,
        default: ?@import("../types.zig").Value = null,
        default_now: bool = false,
        on_update_now: bool = false,
    };

    pub const RenameColumn = struct {
        from: []const u8,
        to: []const u8,
    };
};

pub const Config = struct {
    statement_gate: ?*@import("../util/statement_gate.zig").StatementGate = null,
    /// Default rows per row-group in flushed segments.
    row_group_size: usize = 65_536,

    /// Auto-flush triggers. A flush fires inline (from `insert` or `delete`)
    /// when ANY of these conditions hold against the current memtable, and
    /// also fires from `backgroundFlushSweep` when the time trigger condition
    /// is met. `thindb-server` always spawns the background flusher; embedded
    /// callers must spawn `Database.runBackgroundFlusher` (or call
    /// `backgroundFlushSweep` manually) to drive the time-based path.
    auto_flush_bytes: usize = 64 * 1024 * 1024,
    auto_flush_rows: u64 = 1_000_000,
    /// Seconds since the memtable's first write. 0 disables the time trigger.
    /// 30s is the default: a memtable that's seen any write but hasn't crossed
    /// the row/byte triggers will flush within ~30s + poll interval. The
    /// background flusher uses a 1s poll, so worst-case visible latency is
    /// ~31s from first write.
    auto_flush_secs: u32 = 30,
    /// Lower bounds the time trigger respects so an empty memtable doesn't
    /// thrash the flush path. 1 row / 0 bytes means: anything in the memtable
    /// eventually flushes. Compaction handles the tiny-segment cleanup later.
    auto_flush_min_rows: u64 = 1,
    auto_flush_min_bytes: usize = 0,

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

    /// Worker threads for the per-column block encode+compress inside a
    /// compaction merge (block compression — LZ4HC for large string blocks —
    /// dominates merge cost). `1` (default) = serial, matching the embedded
    /// no-surprise-threads contract. `0` = auto: ~25% of the physical cores.
    /// `thindb-server` passes auto unless `--compact-threads` says otherwise.
    compact_threads: usize = 1,

    /// Durability mode. `.per_flush` makes every acknowledged write survive
    /// an OS crash or power loss: each statement waits for its WAL fsync
    /// (group commit), and segments, tombstone files, the manifest and the
    /// directories that name them are synced before anything refers to
    /// them. A flush costs a few ms more on NVMe; reads are unaffected.
    /// `thindb-server` runs `.per_flush` unless started with `--no-fsync`.
    ///
    /// `.none` (the embedded default) leaves writes in the OS page cache. A
    /// process crash loses nothing, but an OS crash can lose acked rows and
    /// newly written segments a durable manifest already names (the table
    /// then fails to open with `SegmentDamaged`). Only for data that can be
    /// reloaded.
    sync_mode: SyncMode = .none,

    /// Enable the write-ahead log. When true, every insert and delete is
    /// appended to a single per-table WAL file; on reopen the WAL is
    /// replayed to reconstruct memtable contents lost on shutdown.
    ///
    /// Durability tier depends on `sync_mode`:
    ///   - `.none` (default): appends land in the OS page cache, no
    ///     fsync. A PROCESS crash or kill loses nothing (the OS still
    ///     writes the pages); only an OS crash / power loss can drop
    ///     the tail. Near-zero write-path cost.
    ///   - `.per_flush`: the WAL append is group-commit fsynced BEFORE
    ///     the operation returns — "INSERT returned OK = durable" even
    ///     across power loss. One amortized fsync per commit group;
    ///     batch inserts where possible.
    ///
    /// Default ON since 2026-07-12: a crash between ack and flush
    /// otherwise silently sheds acknowledged rows — acceptable for a
    /// disposable bench replica, not for a server another system
    /// trusts (the CDC sink lost acked rows to exactly this, twice).
    /// Bulk import tools loading reimportable data opt out explicitly.
    wal_enabled: bool = true,

    /// The process's one memory budget (`MemoryPool`): every query's
    /// reservations, the block cache, its predicate memo and idle scratch
    /// pools share it, so concurrent queries and the cache can't sum past the
    /// box. Statements may take all of it but `min_cache_bytes` and the memo,
    /// each failing with `error.MemoryBudgetExceeded` rather than spilling to
    /// disk (spilling is future work); the cache grows into whatever they
    /// leave. `null` → ~65% of usable RAM, floored at 256 MiB
    /// (`memorySizes`).
    memory_budget: ?usize = null,

    /// The least of `memory_budget` the block cache keeps: no statement can
    /// take it, and the cache never evicts under it. `null` → a tenth of the
    /// budget.
    min_cache_bytes: ?usize = null,

    /// The predicate memo's size: per row group, the scan predicates that
    /// matched none of its stored rows, so a later scan with the same
    /// predicate (or with one AND arm of it) skips that row group unread.
    /// Fixed, and charged to `memory_budget` whole. `null` → 1/1024 of the
    /// budget; under 4 KiB, 0 included, there is no memo.
    predicate_memo_bytes: ?usize = null,

    /// The memory controller itself. Normally null in user-built Configs —
    /// the Catalog creates one from the sizes above at open, with the block
    /// cache under it, and threads the pointer through this field to every
    /// Database and Table it owns. An embedding application that opens
    /// MULTIPLE Catalogs in one process can pass one pool here, with a cache
    /// already shared into it (`MemoryPool.shareCache`), to make them share
    /// one budget and one cache; a Catalog uses both as-is and owns neither.
    /// Given a pool without a cache, the Catalog creates the cache and owns
    /// it.
    memory_pool: ?*@import("../memory.zig").MemoryPool = null,

    /// Maximum intra-query degree of parallelism (worker threads per query for
    /// the parallel scan/filter leaf). `1` (default) = fully serial, byte-
    /// identical to the single-threaded engine — the canonical result and the
    /// test baseline. `>1` lets a single query's leaf scan hand out contiguous
    /// row-group ranges across up to this many workers; the per-query count is
    /// further bounded by a global worker-slot budget so concurrent queries
    /// share cores instead of oversubscribing. `0` is treated as `1`.
    max_dop: usize = 1,

    /// Filesystem path of the data root directory, when the embedder knows
    /// it (the server passes its --data-dir). Required for LANGUAGE zig
    /// functions: the compile pipeline spawns the toolchain in a scratch
    /// directory under this path and dlopens the artifact by path. Null =
    /// LANGUAGE zig unavailable (clear error at CREATE).
    data_root_path: ?[]const u8 = null,

    /// Max concurrent client connections across all wire protocols.
    /// Server-wide cap; reject (close immediately) when exceeded.
    /// Default 256 — generous for embedded use; bump for high-
    /// concurrency deployments.
    max_connections: u32 = 256,

    /// Per-connection idle timeout in seconds. If a client sends no
    /// command for this duration, the server closes the connection.
    /// Default 0 = disabled (matches Postgres's default; pool drivers
    /// won't have their idle pool members killed). Common production
    /// values: 1800 (30 min), 3600 (1 hour).
    idle_timeout_secs: u32 = 0,

    /// SQL external file scans (`FROM 'x.csv'`, `read_csv(...)`, etc.).
    /// Embedded callers get process-local paths by default. The standalone
    /// server overrides this to `.disabled` unless started with `--file-root`.
    file_scan_access: FileScanAccess = .process,
};

/// Resolve `Config.compact_threads`: non-zero is honored verbatim; `0` means
/// "auto" — ~25% of the physical cores (cross-OS, SMT-aware: see
/// `affinity.physicalCoreCount`), so a background merge speeds up
/// meaningfully without competing with foreground queries for the box.
pub fn resolveCompactThreads(allocator: std.mem.Allocator, configured: usize) usize {
    if (configured != 0) return configured;
    const physical = physicalCoreCount(allocator);
    return @max(1, physical / 4);
}

/// Physical (non-SMT) core count, cross-OS. Re-exported for tools that size
/// their own worker pools (the bulk loader uses every core, unlike the
/// background compactor's 25% default).
pub fn physicalCoreCount(allocator: std.mem.Allocator) usize {
    return @import("../util/affinity.zig").physicalCoreCount(allocator);
}

/// Hardware threads the process may use (its CPU affinity set, capped by a
/// cgroup CPU quota): the standalone server's default `max_dop`. Embedded
/// callers keep `Config.max_dop`'s serial default.
pub fn hardwareThreadCount() usize {
    return @import("../util/affinity.zig").allowedHardwareThreadCount();
}

/// Memory the process may actually use: physical RAM, or the container's
/// cgroup limit when that is smaller. What every "auto" budget is sized from.
pub fn totalMemoryBytes() ?u64 {
    return @import("../util/affinity.zig").totalMemoryBytes();
}

/// The memory sizes a Catalog opens with: `Config.memory_budget`,
/// `min_cache_bytes` and `predicate_memo_bytes`, resolved.
pub const MemorySizes = struct {
    budget: usize,
    min_cache: usize,
    predicate_memo: usize,
};

/// Resolves the memory settings. An unset budget is ~65% of usable memory
/// (physical RAM, or the container's cgroup limit when that is smaller),
/// floored at 256 MiB, or the floor when the OS can't report it. The rest of
/// the box is left for what no budget accounts (memtables, segment metadata,
/// allocator overhead) and the OS page cache.
pub fn memorySizes(config: Config) MemorySizes {
    const budget = config.memory_budget orelse auto: {
        const floor: u64 = 256 * 1024 * 1024;
        const total = totalMemoryBytes() orelse break :auto @as(usize, @intCast(floor));
        break :auto @as(usize, @intCast(@max(total * 65 / 100, floor)));
    };
    return .{
        .budget = budget,
        .min_cache = config.min_cache_bytes orelse budget / 10,
        .predicate_memo = config.predicate_memo_bytes orelse budget / 1024,
    };
}

pub const TableOptions = struct {
    /// Required for create. On reopen via `db.table(...)`, must match the
    /// persisted schema's order key. Not used by `db.openTable(...)`.
    order_key: []const []const u8,
    unique: bool = false,
    /// Overrides the parent Config.row_group_size for this table when set.
    row_group_size: ?usize = null,
};

/// Used by `openTable` — no schema or order_key, just runtime tunables.
pub const OpenOptions = struct {
    row_group_size: ?usize = null,
};

pub const Table = @import("table.zig").Table;
pub const schemaFingerprint = @import("table.zig").schemaFingerprint;
pub const Schema = @import("schema.zig").Schema;
pub const TableBuild = @import("schema.zig").TableBuild;
pub const Database = @import("database.zig").Database;
pub const default_schema_name = @import("database.zig").default_schema_name;
pub const Catalog = @import("catalog.zig").Catalog;
pub const TempNamespace = @import("temp_namespace.zig").TempNamespace;
pub const sweepStaleTempDirs = @import("temp_namespace.zig").sweepStaleTempDirs;
pub const udf = @import("../udf.zig");
pub const UdfRegistry = udf.UdfRegistry;
pub const ScalarUdf = udf.ScalarUdf;
pub const AggregateUdf = udf.AggregateUdf;
pub const TableUdf = udf.TableUdf;
pub const UdfVolatility = udf.Volatility;
pub const UdfNullStrategy = udf.NullStrategy;

/// Per-connection resolution context. `compile()` consults this to fill
/// in any null database/schema fields on a `TableRef`, and DDL `USE`
/// statements mutate it. Callers own the value — pass by mutable
/// pointer so DDL can update it.
///
/// `temp_namespace` is borrowed; the wire layer (mysql/pg server) owns
/// the per-session TempNamespace and threads a pointer through here so
/// CREATE TEMP TABLE / unqualified resolution / DROP TABLE can consult
/// it ahead of the persistent catalog.
/// Which client wire a session is served over. Lets wire-neutral
/// compilation tailor a few dialect-specific surfaces (e.g. the EXPLAIN
/// result column name) without forking the parser or engine.
pub const Dialect = @import("../types.zig").Dialect;

pub const Session = struct {
    /// Null when the session has no current database, as MySQL leaves a
    /// session whose default database was dropped. Statements that name
    /// every table fully keep working; an unqualified name fails with
    /// `NoDatabaseSelected`.
    current_db: ?[]const u8 = "main",
    current_schema: []const u8 = "public",
    dialect: Dialect = .neutral,
    temp_namespace: ?*TempNamespace = null,
    /// MySQL-style user-defined variables (`@name`). Lazily heap-
    /// allocated on first `SET @name = ...`. Owned by the Connection;
    /// Session copies share the same pointer so mutations from one
    /// statement are visible to the next.
    vars: ?*SessionVars = null,
    /// LAST_INSERT_ID(): the first AUTO_INCREMENT id an INSERT generated on
    /// this connection, 0 before any. The wire layer carries it between
    /// statements.
    last_insert_id: u64 = 0,
    /// ROW_COUNT(): what the previous statement changed — its affected rows
    /// for DML, -1 after a result set, 0 otherwise.
    row_count: i64 = -1,
    /// What CONNECTION_ID(), USER() and VERSION() answer on this connection.
    /// A caller with no such notion (embedded use, the PG wire) leaves them
    /// null, and the calls stay unknown functions there.
    connection_id: ?u32 = null,
    user: ?[]const u8 = null,
    server_version: ?[]const u8 = null,
    /// The server's connections, which the process-list relations
    /// (information_schema.PROCESSLIST, pg_stat_activity) list. Null in
    /// embedded use, where those relations are empty.
    connections: ?*@import("../net/conn_registry.zig").Registry = null,
    /// `SET thindb_max_dop = N`: the parallelism this connection's statements
    /// run at, at most the server's max_dop. 0 = the server's max_dop.
    max_dop: u32 = 0,
    /// A statement's `/*+ SET_VAR(thindb_max_dop = N) */` hint, which takes
    /// the place of `max_dop` for that statement alone.
    statement_max_dop: ?u32 = null,
    /// `SET group_concat_max_len = N`: the bytes a GROUP_CONCAT's text keeps,
    /// at least 4, as in MySQL. Null keeps all of it.
    group_concat_max_len: ?u64 = null,

    /// The cap a compile puts on parallelism, below the server's max_dop.
    pub fn dopCap(self: Session) ?usize {
        const requested = self.statement_max_dop orelse self.max_dop;
        return if (requested == 0) null else requested;
    }
};

/// A user variable's value and its type. An array, map or struct holds
/// its encoded bytes in `.text`, which only its `type` reads.
pub const SessionVar = struct {
    value: @import("../types.zig").Value,
    type: @import("../types.zig").Type,
};

/// Per-connection variable storage. Names and string values are
/// duped into the backing arena so they outlive the SQL statement
/// that defined them.
pub const SessionVars = struct {
    arena: std.heap.ArenaAllocator,
    /// `null` map value = a variable explicitly `SET @x = NULL` (SQL NULL),
    /// distinct from an absent key (undefined variable).
    map: std.StringHashMapUnmanaged(?SessionVar) = .empty,

    pub fn init(allocator: std.mem.Allocator) SessionVars {
        return .{ .arena = std.heap.ArenaAllocator.init(allocator) };
    }

    pub fn deinit(self: *SessionVars) void {
        const backing = self.arena.child_allocator;
        self.map.deinit(backing);
        self.arena.deinit();
    }

    /// Outer optional: present iff the variable was set. Inner optional: the
    /// value, or `null` for a variable set to SQL NULL.
    pub fn get(self: *const SessionVars, name: []const u8) ??SessionVar {
        return self.map.get(name);
    }

    /// Set / overwrite a variable. `null` stores SQL NULL. String values are
    /// dup'd into the arena so the caller's backing storage can go away.
    pub fn set(self: *SessionVars, name: []const u8, value: ?SessionVar) !void {
        const arena_alloc = self.arena.allocator();
        const owned_name = try arena_alloc.dupe(u8, name);
        const owned_value: ?SessionVar = if (value) |v| .{
            .value = switch (v.value) {
                .text => |s| .{ .text = try arena_alloc.dupe(u8, s) },
                else => v.value,
            },
            .type = v.type,
        } else null;
        try self.map.put(self.arena.child_allocator, owned_name, owned_value);
    }

    /// Put back what `get` gave before a `set` of `name`. It doesn't
    /// allocate, so a statement that fails can always undo its assignments.
    pub fn restore(self: *SessionVars, name: []const u8, prior: ??SessionVar) void {
        const value = prior orelse {
            _ = self.map.remove(name);
            return;
        };
        if (self.map.getPtr(name)) |slot| slot.* = value;
    }
};

/// Translate any `api.Error` into the equivalently-named variant of
/// `DstError`. IO / allocator / unknown errors pass through unchanged.
/// Used by transports that expose their own `Error` set so internal
/// catalog errors surface with stable names on the wire.
pub fn remapError(comptime DstError: type, e: anyerror) anyerror {
    return switch (e) {
        Error.DatabaseNotFound => DstError.DatabaseNotFound,
        Error.DatabaseAlreadyExists => DstError.DatabaseAlreadyExists,
        Error.SchemaNotFound => DstError.SchemaNotFound,
        Error.SchemaAlreadyExists => DstError.SchemaAlreadyExists,
        Error.TableNotFound => DstError.TableNotFound,
        Error.TableAlreadyExists => DstError.TableAlreadyExists,
        Error.FunctionAlreadyExists => DstError.FunctionAlreadyExists,
        Error.FunctionInvalidDefinition => DstError.FunctionInvalidDefinition,
        Error.FunctionNotFound => DstError.FunctionNotFound,
        else => e,
    };
}

test {
    _ = @import("table.zig");
    _ = @import("api_test.zig");
    _ = @import("compact.zig");
    _ = @import("delete.zig");
    _ = @import("upsert.zig");
}
