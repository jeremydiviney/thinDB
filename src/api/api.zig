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
    DatabaseNotFound,
    DatabaseAlreadyExists,
    SchemaNotFound,
    SchemaAlreadyExists,
    FunctionAlreadyExists,
    FunctionInvalidDefinition,
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

    pub const AddColumn = struct {
        name: []const u8,
        type: @import("../types.zig").Type,
        nullable: bool = false,
        default: ?@import("../types.zig").Value = null,
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

    /// LRU cache budget for decompressed column blocks. The special value
    /// `0` means "auto" — resolved at open time to ~60% of physical RAM
    /// (floored at 256 MiB) by `autoCacheSizeBytes`. Set an explicit non-zero
    /// value to pin the budget; there is no separate "disable cache" — a tiny
    /// explicit value (e.g. 1) is the way to effectively turn it off.
    cache_size_bytes: usize = 0,

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

    /// Per-query memory ceiling for blocking operators (Sort, hash
    /// GroupBy, hash Join build, SMJ sort buffers, materialize-with-
    /// stats). When the estimated or actual usage of any blocking
    /// operator would exceed this, the query is refused (pre-flight)
    /// or aborted (runtime) with `error.MemoryBudgetExceeded` rather
    /// than spilling to disk.
    ///
    /// Spilling support is a future enhancement. v1's contract is:
    /// every query either runs entirely within `query_memory_budget`
    /// or fails fast with a clear error — never silently degrades.
    ///
    /// Default `auto_query_budget` → resolved at open to ~25% of physical RAM
    /// by `autoQueryBudgetBytes`. High-cardinality GROUP BY / large sorts on
    /// hundred-million-row tables hold their whole hash table or buffer at once
    /// (eviction can't shrink the single peak), so the budget has to clear that
    /// peak; a RAM fraction scales it to the box. Independent of the decoded-
    /// block cache (a separate bucket — see `cache_size_bytes`). An explicit
    /// non-zero value pins the budget; `0` disables tracking. A real fix for the
    /// GROUP BY ceiling (spill-to-disk) is future work.
    query_memory_budget: usize = auto_query_budget,

    /// Maximum intra-query degree of parallelism (worker threads per query for
    /// the parallel scan/filter leaf). `1` (default) = fully serial, byte-
    /// identical to the single-threaded engine — the canonical result and the
    /// test baseline. `>1` lets a single query's leaf scan hand out contiguous
    /// row-group ranges across up to this many workers; the per-query count is
    /// further bounded by a global worker-slot budget so concurrent queries
    /// share cores instead of oversubscribing. `0` is treated as `1`.
    max_dop: usize = 1,

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

/// Resolve the decompressed-block cache budget. A non-zero `configured` is
/// honored verbatim; `0` means "auto" — ~50% of physical RAM, floored at
/// 256 MiB so small machines still get a usable pool. The LRU fills lazily and
/// evicts to this bound, so a large auto budget on a big box costs nothing
/// until the workload's hot set actually grows into it. Falls back to the
/// floor if the OS can't report total memory.
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

pub fn autoCacheSizeBytes(configured: usize) usize {
    if (configured != 0) return configured;
    const floor: u64 = 256 * 1024 * 1024;
    const total = std.process.totalSystemMemory() catch return @intCast(floor);
    // 35% of system memory (not 50%): a larger decompressed-block cache stops
    // paying off once the OS starts trimming the working set — re-faulting those
    // pages on access costs more than the decompression the cache avoids (the
    // resident set grows past the point where address translation stays cheap).
    return @intCast(@max(total * 35 / 100, floor));
}

/// Auto-resolve sentinel for `Config.query_memory_budget`: the default leaves
/// the budget unset so it resolves to a RAM fraction at open. An explicit value
/// (including `0` = no tracking) is honored verbatim. Distinct from `0` so the
/// "disable tracking" escape hatch survives.
pub const auto_query_budget: usize = std.math.maxInt(usize);

/// Resolve the per-query memory budget. The `auto_query_budget` sentinel (the
/// `Config` default) means "auto" — ~25% of physical RAM, floored at 256 MiB.
/// Any other value (including `0` = disable tracking) is honored verbatim.
/// Falls back to the floor if the OS can't report total memory.
pub fn autoQueryBudgetBytes(configured: usize) usize {
    if (configured != auto_query_budget) return configured;
    const floor: u64 = 256 * 1024 * 1024;
    const total = std.process.totalSystemMemory() catch return @intCast(floor);
    return @intCast(@max(total / 4, floor));
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
pub const Database = @import("database.zig").Database;
pub const Catalog = @import("catalog.zig").Catalog;
pub const TempNamespace = @import("temp_namespace.zig").TempNamespace;
pub const sweepStaleTempDirs = @import("temp_namespace.zig").sweepStaleTempDirs;
pub const udf = @import("../udf.zig");
pub const UdfRegistry = udf.UdfRegistry;
pub const ScalarUdf = udf.ScalarUdf;
pub const AggregateUdf = udf.AggregateUdf;
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
    current_db: []const u8 = "main",
    current_schema: []const u8 = "public",
    dialect: Dialect = .neutral,
    temp_namespace: ?*TempNamespace = null,
    /// MySQL-style user-defined variables (`@name`). Lazily heap-
    /// allocated on first `SET @name = ...`. Owned by the Connection;
    /// Session copies share the same pointer so mutations from one
    /// statement are visible to the next.
    vars: ?*SessionVars = null,
};

/// Per-connection variable storage. Names and string values are
/// duped into the backing arena so they outlive the SQL statement
/// that defined them.
pub const SessionVars = struct {
    arena: std.heap.ArenaAllocator,
    map: std.StringHashMapUnmanaged(@import("../types.zig").Value) = .empty,

    pub fn init(allocator: std.mem.Allocator) SessionVars {
        return .{ .arena = std.heap.ArenaAllocator.init(allocator) };
    }

    pub fn deinit(self: *SessionVars) void {
        const backing = self.arena.child_allocator;
        self.map.deinit(backing);
        self.arena.deinit();
    }

    pub fn get(self: *const SessionVars, name: []const u8) ?@import("../types.zig").Value {
        return self.map.get(name);
    }

    /// Set / overwrite a variable. String values are dup'd into the
    /// arena so the caller's backing storage can go away. Numeric
    /// values are value-typed and stored inline.
    pub fn set(self: *SessionVars, name: []const u8, value: @import("../types.zig").Value) !void {
        const arena_alloc = self.arena.allocator();
        const owned_name = try arena_alloc.dupe(u8, name);
        const owned_value: @import("../types.zig").Value = switch (value) {
            .text => |s| .{ .text = try arena_alloc.dupe(u8, s) },
            else => value,
        };
        try self.map.put(self.arena.child_allocator, owned_name, owned_value);
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
        else => e,
    };
}

test {
    _ = @import("table.zig");
    _ = @import("api_test.zig");
}
