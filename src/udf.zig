//! Trusted in-process Zig UDF descriptors and registry.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("types.zig");
const Type = types.Type;
const TypeTag = types.TypeTag;
const storage_column = @import("storage/column.zig");
const ColumnView = storage_column.ColumnView;
const store = @import("engine/store.zig");
const ColumnStore = store.ColumnStore;

pub const Error = error{
    FunctionAlreadyExists,
    FunctionInvalidDefinition,
    ViewAlreadyExists,
    ViewNotFound,
};

pub const NullStrategy = enum {
    propagates,
    absorbs,
    kernel_managed,
    /// `propagates`, and a zero in the last argument (the divisor) also
    /// yields NULL: division by zero, as MySQL and StarRocks answer it.
    zero_divisor,
};

pub const Volatility = enum {
    immutable,
    stable,
    @"volatile",
};

pub const ScalarContext = struct {
    allocator: Allocator,
    user_data: ?*anyopaque = null,
};

pub const ScalarKernel = *const fn (
    ctx: *const ScalarContext,
    args: []const ColumnView,
    out: *ColumnStore,
    row_count: usize,
) anyerror!void;

pub const ScalarUdf = struct {
    name: []const u8,
    arg_types: []const Type,
    return_type: Type,
    null_strategy: NullStrategy = .propagates,
    volatility: Volatility = .@"volatile",
    kernel: ScalarKernel,
    user_data: ?*anyopaque = null,
};

pub const AggregateContext = struct {
    allocator: Allocator,
    user_data: ?*anyopaque = null,
};

pub const AggregateInit = *const fn (ctx: *const AggregateContext, state: *anyopaque) anyerror!void;
pub const AggregateUpdateOne = *const fn (
    ctx: *const AggregateContext,
    state: *anyopaque,
    args: []const ColumnView,
    row: usize,
) anyerror!void;
pub const AggregateUpdateBatch = *const fn (
    ctx: *const AggregateContext,
    state: *anyopaque,
    args: []const ColumnView,
    row_count: usize,
) anyerror!void;
pub const AggregateCombine = *const fn (
    ctx: *const AggregateContext,
    dst_state: *anyopaque,
    src_state: *const anyopaque,
) anyerror!void;
pub const AggregateFinalize = *const fn (
    ctx: *const AggregateContext,
    state: *anyopaque,
    out: *ColumnStore,
) anyerror!void;
pub const AggregateDestroy = *const fn (ctx: *const AggregateContext, state: *anyopaque) void;

pub const AggregateUdf = struct {
    name: []const u8,
    arg_types: []const Type,
    return_type: Type,
    state_size: usize,
    state_align: usize = 1,
    init: AggregateInit,
    update_one: AggregateUpdateOne,
    update_batch: ?AggregateUpdateBatch = null,
    combine: ?AggregateCombine = null,
    finalize: AggregateFinalize,
    destroy: ?AggregateDestroy = null,
    volatility: Volatility = .@"volatile",
    user_data: ?*anyopaque = null,
};

pub const ScalarEntry = struct {
    name: []const u8,
    arg_types: []const Type,
    return_type: Type,
    null_strategy: NullStrategy,
    volatility: Volatility,
    kernel: ScalarKernel,
    user_data: ?*anyopaque,
};

pub const AggregateEntry = struct {
    name: []const u8,
    arg_types: []const Type,
    return_type: Type,
    state_size: usize,
    state_align: usize,
    init: AggregateInit,
    update_one: AggregateUpdateOne,
    update_batch: ?AggregateUpdateBatch,
    combine: ?AggregateCombine,
    finalize: AggregateFinalize,
    destroy: ?AggregateDestroy,
    volatility: Volatility,
    user_data: ?*anyopaque,
};

/// Execution modes for table-valued UDFs. Part of the declared signature
/// and compile-time enforced against the call site: a `.partitioned`
/// function requires PARTITION BY (per-key state would silently bleed
/// across keys without it); a `.global` function forbids it (global
/// visibility silently lost); `.either` accepts both.
pub const TvfExecution = enum {
    partitioned,
    global,
    either,
};

/// One input partition handed to a table UDF's process callback: the
/// declared input columns (in declared order), the row count, and — for
/// partitioned calls — the partition key values (one per PARTITION BY
/// column, in clause order; empty for global calls). Column views are
/// borrowed for the duration of the call; rows within the partition are
/// ordered per the call site's ORDER BY.
pub const TvfPartition = struct {
    columns: []const ColumnView,
    row_count: usize,
    keys: []const ?types.Value,
};

/// Output sink for a table UDF's process callback: append values
/// column-by-column for each emitted row via the engine ColumnStores
/// (one per declared output column, in declared order). The RAW layer —
/// the ergonomic comptime SDK wraps this later. The callback must leave
/// every output column at the same row count (rectangular output); the
/// operator validates after each partition.
pub const TvfOutput = struct {
    columns: []*ColumnStore,
    allocator: Allocator,
};

pub const TvfContext = struct {
    /// Per-partition arena: freed after each process() call returns, so
    /// scratch allocations cannot leak.
    arena: Allocator,
    user_data: ?*anyopaque = null,
    /// Scalar call arguments (literals at the call site), in declared
    /// order. Identical for every partition of one call.
    args: []const ?types.Value = &.{},
    /// Worker-lifetime arena + state slot: one worker executes many
    /// partitions, and a kernel may lazily build shared lookup state on
    /// its first call (allocate in worker_arena, stash the pointer in
    /// worker_state.*) and reuse it for every later partition the same
    /// worker claims. Valid contents derive only from broadcast inputs
    /// and args — per-partition data changes call to call. Null when the
    /// execution path has no worker lifetime (validation); kernels fall
    /// back to the per-partition arena via the SDK.
    worker_arena: ?Allocator = null,
    worker_state: ?*?*anyopaque = null,
};

/// One co-grouped partition per input table, in declared input order. A
/// single-input function receives `parts.len == 1`. An input with no rows
/// for the group's key still gets an entry (row_count 0, columns empty)
/// with `keys` populated — empty tables are ALIGNED, never skipped.
pub const TvfProcess = *const fn (
    ctx: *const TvfContext,
    parts: []const TvfPartition,
    out: *TvfOutput,
) anyerror!void;

/// A table-valued UDF: the raw engine-facing descriptor (the comptime
/// user SDK generates one of these). Declared input/output shapes are
/// the type contract — the compiler matches the call's input subquery
/// against `input_schema` name-for-name/type-for-type and reports
/// `output_schema` downstream, and the operator hands the callback
/// exactly the declared columns.
/// One operator-filled output column: `out_idx` (into output_schema) is
/// materialized by the operator as a permuted copy of input column
/// `in_idx` (into input_schemas[0] — always the FIRST input) — the kernel
/// never sees or emits it. Requires `row_aligned`.
pub const PassPair = struct { out_idx: u32, in_idx: u32 };

pub const TableUdf = struct {
    name: []const u8,
    /// Declared input tables, each a column list in the order the callback
    /// receives it. One entry per input subquery at the call site.
    input_schemas: []const []const types.Column,
    /// Declared output columns — the operator's output schema.
    output_schema: []const types.Column,
    execution: TvfExecution = .either,
    /// Declared scalar argument types, in call order (empty = none).
    arg_types: []const types.Type = &.{},
    /// The callback emits exactly one output row per row of input 0, in
    /// partition order (validated per partition/group). Unlocks
    /// pass-through. Multi-input functions align to input 0: a group where
    /// input 0 arrives empty must emit zero rows.
    row_aligned: bool = false,
    /// The callback asserts its emitted rows are, within each partition,
    /// in nondecreasing (call-site ORDER BY) order, and that output
    /// columns named like the call's PARTITION BY / ORDER BY keys carry
    /// the partition's key values / that order. Lets the staged compiler
    /// advertise the operator's output order so downstream same-key
    /// windows / TVFs skip their sorts. `row_aligned` functions whose key
    /// columns are all pass-through get this for free — the flag exists
    /// for row-GENERATING kernels (e.g. gap fill) where the engine cannot
    /// prove it.
    ordered_output: bool = false,
    /// Inputs (indices into input_schemas; never 0) delivered WHOLE to
    /// every partition instead of co-grouped: lookup tables the kernel
    /// probes rather than streams. Broadcast inputs need no PARTITION
    /// BY / ORDER BY columns in their schema and are never sorted.
    broadcast_inputs: []const u32 = &.{},
    /// Operator-filled pass-through columns (requires row_aligned; sources
    /// always come from input 0). The callback receives views for only the
    /// first `kernel_input_cols` input-0 columns and output stores for
    /// only the non-pass-through output columns (dense, in declared
    /// order).
    passthrough: []const PassPair = &.{},
    /// How many leading input-0 columns the callback reads. Columns past
    /// this exist purely as pass-through sources ("carry" columns).
    /// 0 = all of input_schemas[0] (no carry split).
    kernel_input_cols: u32 = 0,
    process: TvfProcess,
    user_data: ?*anyopaque = null,
};

pub const TableEntry = struct {
    name: []const u8,
    input_schemas: []const []const types.Column,
    output_schema: []const types.Column,
    execution: TvfExecution,
    arg_types: []const types.Type,
    row_aligned: bool,
    ordered_output: bool,
    broadcast_inputs: []const u32,
    passthrough: []const PassPair,
    kernel_input_cols: u32,
    process: TvfProcess,
    user_data: ?*anyopaque,
    /// Unique per registration within its registry, from 1; 0 for entries
    /// built directly rather than registered. A replaced function's new
    /// library can load where the old one was, so its `process` pointer may
    /// repeat — caches that retain kernel state key on this instead.
    registration: u64 = 0,
};

/// A SQL inline table function: `CREATE FUNCTION f(a INT, ...) RETURNS
/// TABLE AS (SELECT ...)`. A parameterized view — the parser expands a
/// `FROM f(1, 'x')` reference by re-parsing `body` with each parameter
/// identifier bound to the call's literal argument, splicing the result
/// inline (no materialize boundary, full pushdown/fusion apply).
pub const SqlTableFn = struct {
    /// Lowercased function name (single identifier, database-scoped).
    name: []const u8,
    /// Parameter names in declaration order (original case; matched
    /// case-insensitively at expansion).
    param_names: []const []const u8,
    /// Declared parameter types (arity/documentation; argument literals
    /// are substituted as-is in v1).
    param_types: []const Type,
    /// Raw SQL text of the body SELECT (inside the `AS ( ... )` parens).
    body: []const u8,
    /// The verbatim CREATE FUNCTION statement — the persistence format:
    /// stored as `<db>/_functions/<name>.sql` and re-parsed on open.
    create_text: []const u8,

    pub fn clone(self: SqlTableFn, allocator: Allocator) Allocator.Error!SqlTableFn {
        const name = try allocator.dupe(u8, self.name);
        errdefer allocator.free(name);
        const param_names = try allocator.alloc([]const u8, self.param_names.len);
        var copied: usize = 0;
        errdefer {
            for (param_names[0..copied]) |p| allocator.free(p);
            allocator.free(param_names);
        }
        for (self.param_names) |p| {
            param_names[copied] = try allocator.dupe(u8, p);
            copied += 1;
        }
        const param_types = try allocator.dupe(Type, self.param_types);
        errdefer allocator.free(param_types);
        const body = try allocator.dupe(u8, self.body);
        errdefer allocator.free(body);
        const create_text = try allocator.dupe(u8, self.create_text);
        return .{
            .name = name,
            .param_names = param_names,
            .param_types = param_types,
            .body = body,
            .create_text = create_text,
        };
    }

    pub fn deinit(self: SqlTableFn, allocator: Allocator) void {
        allocator.free(self.name);
        for (self.param_names) |p| allocator.free(p);
        allocator.free(self.param_names);
        allocator.free(self.param_types);
        allocator.free(self.body);
        allocator.free(self.create_text);
    }
};

/// The registry key `<db>\x00<lowercased name>` in `buf`, or null when it
/// does not fit (no registered name can be that long).
fn lookupKey(buf: *[512]u8, db: []const u8, name: []const u8) ?[]const u8 {
    const len = db.len + 1 + name.len;
    if (len > buf.len) return null;
    @memcpy(buf[0..db.len], db);
    buf[db.len] = 0;
    for (name, buf[db.len + 1 ..][0..name.len]) |c, *o| o.* = std.ascii.toLower(c);
    return buf[0..len];
}

fn keyInDatabase(map_key: []const u8, db: []const u8) bool {
    return map_key.len > db.len + 1 and std.mem.eql(u8, map_key[0..db.len], db) and map_key[db.len] == 0;
}

/// Free and remove every entry of `db`. The caller holds the registry mutex.
fn removeDatabaseEntries(comptime V: type, allocator: Allocator, map: *std.StringHashMapUnmanaged(V), db: []const u8) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        if (!keyInDatabase(entry.key_ptr.*, db)) continue;
        const map_key = entry.key_ptr.*;
        const value = entry.value_ptr.*;
        // removeByPtr only tombstones the slot, so the iterator stays valid.
        map.removeByPtr(entry.key_ptr);
        allocator.free(map_key);
        value.deinit(allocator);
    }
}

/// Catalog-owned registry of SQL inline table functions, keyed by
/// `<database>\x00<name>` (functions are database-scoped like tables).
/// All strings are owned copies. Thread-safe: every read and DDL mutation
/// takes the mutex, and no pointer into the map is handed out — the parser
/// reads definitions before its statement holds a lease.
pub const SqlFnRegistry = struct {
    allocator: Allocator,
    map: std.StringHashMapUnmanaged(SqlTableFn) = .empty,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn init(allocator: Allocator) SqlFnRegistry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *SqlFnRegistry) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            e.value_ptr.deinit(self.allocator);
        }
        self.map.deinit(self.allocator);
        self.* = undefined;
    }

    fn key(allocator: Allocator, db: []const u8, name: []const u8) ![]u8 {
        const k = try allocator.alloc(u8, db.len + 1 + name.len);
        @memcpy(k[0..db.len], db);
        k[db.len] = 0;
        for (name, k[db.len + 1 ..]) |c, *o| o.* = std.ascii.toLower(c);
        return k;
    }

    /// Register (copies everything). `replace=false` errors on collision.
    pub fn register(
        self: *SqlFnRegistry,
        db: []const u8,
        f: SqlTableFn,
        replace: bool,
    ) !void {
        try validateName(f.name);
        if (f.param_names.len != f.param_types.len or f.param_names.len > 32) {
            return Error.FunctionInvalidDefinition;
        }
        const k = try key(self.allocator, db, f.name);
        errdefer self.allocator.free(k);
        const name = try lowerName(self.allocator, f.name);
        defer self.allocator.free(name);
        var lowered = f;
        lowered.name = name;
        const owned = try lowered.clone(self.allocator);
        errdefer owned.deinit(self.allocator);

        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const gop = try self.map.getOrPut(self.allocator, k);
        if (gop.found_existing) {
            // The error return frees k and `owned` via the errdefers above —
            // no manual frees here or they double-free.
            if (!replace) return Error.FunctionAlreadyExists;
            self.allocator.free(k);
            gop.value_ptr.deinit(self.allocator);
            gop.value_ptr.* = owned;
        } else {
            gop.value_ptr.* = owned;
        }
    }

    /// Remove. Returns false when absent.
    pub fn drop(self: *SqlFnRegistry, db: []const u8, name: []const u8) !bool {
        const k = try key(self.allocator, db, name);
        defer self.allocator.free(k);
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.map.fetchRemove(k)) |kv| {
            self.allocator.free(kv.key);
            kv.value.deinit(self.allocator);
            return true;
        }
        return false;
    }

    pub fn dropDatabase(self: *SqlFnRegistry, db: []const u8) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        removeDatabaseEntries(SqlTableFn, self.allocator, &self.map, db);
    }

    /// Names of every function registered for `db`, allocated copies.
    pub fn listNames(self: *SqlFnRegistry, allocator: Allocator, db: []const u8) ![][]u8 {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        var out: std.ArrayList([]u8) = .empty;
        errdefer {
            for (out.items) |n| allocator.free(n);
            out.deinit(allocator);
        }
        var it = self.map.iterator();
        while (it.next()) |entry| {
            const map_key = entry.key_ptr.*;
            if (!keyInDatabase(map_key, db)) continue;
            try out.append(allocator, try allocator.dupe(u8, map_key[db.len + 1 ..]));
        }
        return out.toOwnedSlice(allocator);
    }

    /// A copy of the definition in `allocator`, or null. The copy is taken
    /// under the mutex: a concurrent replace or drop frees the entry, and
    /// any register can move it.
    pub fn get(self: *SqlFnRegistry, allocator: Allocator, db: []const u8, name: []const u8) Allocator.Error!?SqlTableFn {
        var kbuf: [512]u8 = undefined;
        const k = lookupKey(&kbuf, db, name) orelse return null;
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const def = self.map.get(k) orelse return null;
        return try def.clone(allocator);
    }
};

/// A view: `CREATE [MATERIALIZED] VIEW name AS <select>`. A plain view is a
/// named query expanded inline at each `FROM name` reference (like a
/// zero-parameter inline function). A materialized view additionally owns a
/// backing table of the same name; `body` is the defining query re-run by
/// REFRESH.
pub const ViewDef = struct {
    name: []const u8,
    materialized: bool,
    /// Raw defining-query text (everything after `AS`).
    body: []const u8,
    /// Verbatim CREATE statement — the `<db>/_views/<name>.sql` persistence
    /// format, re-parsed on catalog open.
    create_text: []const u8,

    pub fn clone(self: ViewDef, allocator: Allocator) Allocator.Error!ViewDef {
        const name = try allocator.dupe(u8, self.name);
        errdefer allocator.free(name);
        const body = try allocator.dupe(u8, self.body);
        errdefer allocator.free(body);
        const create_text = try allocator.dupe(u8, self.create_text);
        return .{
            .name = name,
            .materialized = self.materialized,
            .body = body,
            .create_text = create_text,
        };
    }

    pub fn deinit(self: ViewDef, allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.body);
        allocator.free(self.create_text);
    }
};

/// Catalog-owned registry of views, keyed `<database>\x00<name>` like the
/// function registry. All strings are owned copies; thread-safe, and like
/// the function registry it hands out copies, never pointers into the map.
pub const ViewRegistry = struct {
    allocator: Allocator,
    map: std.StringHashMapUnmanaged(ViewDef) = .empty,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn init(allocator: Allocator) ViewRegistry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *ViewRegistry) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            e.value_ptr.deinit(self.allocator);
        }
        self.map.deinit(self.allocator);
        self.* = undefined;
    }

    fn viewKey(allocator: Allocator, db: []const u8, name: []const u8) ![]u8 {
        const k = try allocator.alloc(u8, db.len + 1 + name.len);
        @memcpy(k[0..db.len], db);
        k[db.len] = 0;
        for (name, k[db.len + 1 ..]) |c, *o| o.* = std.ascii.toLower(c);
        return k;
    }

    /// Register (copies everything). `replace=false` errors on collision.
    pub fn register(self: *ViewRegistry, db: []const u8, v: ViewDef, replace: bool) !void {
        if (try self.put(db, v, replace)) |previous| previous.deinit(self.allocator);
    }

    /// `register`, handing back the definition it replaced (null when it
    /// added one), which the caller owns: `restore` takes it to undo the
    /// register.
    pub fn put(self: *ViewRegistry, db: []const u8, v: ViewDef, replace: bool) !?ViewDef {
        try validateName(v.name);
        // `get`, `contains` and `restore` find only keys that fit the lookup
        // buffer, so a longer one would register a view nothing reaches.
        var kbuf: [512]u8 = undefined;
        if (lookupKey(&kbuf, db, v.name) == null) return Error.FunctionInvalidDefinition;
        const k = try viewKey(self.allocator, db, v.name);
        errdefer self.allocator.free(k);
        const name = try lowerName(self.allocator, v.name);
        defer self.allocator.free(name);
        var lowered = v;
        lowered.name = name;
        const owned = try lowered.clone(self.allocator);
        errdefer owned.deinit(self.allocator);

        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const gop = try self.map.getOrPut(self.allocator, k);
        if (gop.found_existing) {
            if (!replace) return Error.ViewAlreadyExists;
            self.allocator.free(k);
            const previous = gop.value_ptr.*;
            gop.value_ptr.* = owned;
            return previous;
        }
        gop.value_ptr.* = owned;
        return null;
    }

    /// Undo a `put` of `name`: reinstate the definition it replaced, taking
    /// ownership of it, or remove the one it added.
    pub fn restore(self: *ViewRegistry, db: []const u8, name: []const u8, previous: ?ViewDef) void {
        var kbuf: [512]u8 = undefined;
        // `put` refused any name whose key does not fit, so this finds what
        // it registered.
        const k = lookupKey(&kbuf, db, name) orelse {
            if (previous) |p| p.deinit(self.allocator);
            return;
        };
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (previous) |p| {
            // A drop since the `put` took what it registered; the view stays
            // dropped.
            const current = self.map.getPtr(k) orelse return p.deinit(self.allocator);
            current.deinit(self.allocator);
            current.* = p;
        } else if (self.map.fetchRemove(k)) |kv| {
            self.allocator.free(kv.key);
            kv.value.deinit(self.allocator);
        }
    }

    /// Remove. Returns false when absent.
    pub fn drop(self: *ViewRegistry, db: []const u8, name: []const u8) !bool {
        const k = try viewKey(self.allocator, db, name);
        defer self.allocator.free(k);
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.map.fetchRemove(k)) |kv| {
            self.allocator.free(kv.key);
            kv.value.deinit(self.allocator);
            return true;
        }
        return false;
    }

    pub fn dropDatabase(self: *ViewRegistry, db: []const u8) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        removeDatabaseEntries(ViewDef, self.allocator, &self.map, db);
    }

    /// A copy of the definition in `allocator`, or null. The copy is taken
    /// under the mutex: a concurrent replace or drop frees the entry, and
    /// any register can move it.
    pub fn get(self: *ViewRegistry, allocator: Allocator, db: []const u8, name: []const u8) Allocator.Error!?ViewDef {
        var kbuf: [512]u8 = undefined;
        const k = lookupKey(&kbuf, db, name) orelse return null;
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const def = self.map.get(k) orelse return null;
        return try def.clone(allocator);
    }

    pub fn contains(self: *ViewRegistry, db: []const u8, name: []const u8) bool {
        var kbuf: [512]u8 = undefined;
        const k = lookupKey(&kbuf, db, name) orelse return false;
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        return self.map.contains(k);
    }

    /// Names of every view registered for `db`, allocated copies.
    pub fn listNames(self: *ViewRegistry, allocator: Allocator, db: []const u8) ![][]u8 {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        var out: std.ArrayList([]u8) = .empty;
        errdefer {
            for (out.items) |n| allocator.free(n);
            out.deinit(allocator);
        }
        var it = self.map.iterator();
        while (it.next()) |entry| {
            const map_key = entry.key_ptr.*;
            if (!keyInDatabase(map_key, db)) continue;
            try out.append(allocator, try allocator.dupe(u8, map_key[db.len + 1 ..]));
        }
        return out.toOwnedSlice(allocator);
    }
};

/// Parse-time context for SQL table-function expansion: which registry
/// to consult and which database scopes unqualified names. Also carries the
/// view registry so a bare `FROM viewname` can be expanded inline.
pub const SqlFnCtx = struct {
    registry: *SqlFnRegistry,
    /// The database whose functions and views are in scope; null when the
    /// session has none, so none are.
    db: ?[]const u8,
    views: ?*ViewRegistry = null,
    /// Absent = an unqualified `JOIN ... ON` column can't be attributed to
    /// a base-table input.
    tables: ?TableColumns = null,
};

/// Parse-time lookup of a base table's column names, resolved the way the
/// statement's compile will resolve the reference. Null = no such table.
/// The reference arrives as its parts because this file ships inside the
/// Zig-function SDK, which must not import the IR.
pub const TableColumns = struct {
    context: *const anyopaque,
    lookup: *const fn (
        context: *const anyopaque,
        arena: Allocator,
        database: ?[]const u8,
        schema: ?[]const u8,
        name: []const u8,
    ) Allocator.Error!?[]const []const u8,
};

pub const UdfRegistry = struct {
    allocator: Allocator,
    scalars: std.ArrayList(ScalarEntry) = .empty,
    aggregates: std.ArrayList(AggregateEntry) = .empty,
    tables: std.ArrayList(TableEntry) = .empty,
    last_registration: u64 = 0,

    pub fn init(allocator: Allocator) UdfRegistry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *UdfRegistry) void {
        for (self.scalars.items) |entry| {
            self.allocator.free(entry.name);
            self.allocator.free(entry.arg_types);
        }
        self.scalars.deinit(self.allocator);
        for (self.aggregates.items) |entry| {
            self.allocator.free(entry.name);
            self.allocator.free(entry.arg_types);
        }
        self.aggregates.deinit(self.allocator);
        for (self.tables.items) |entry| {
            self.allocator.free(entry.name);
            for (entry.input_schemas) |cols| freeColumns(self.allocator, cols);
            self.allocator.free(entry.input_schemas);
            freeColumns(self.allocator, entry.output_schema);
            self.allocator.free(entry.arg_types);
            self.allocator.free(entry.passthrough);
            self.allocator.free(entry.broadcast_inputs);
        }
        self.tables.deinit(self.allocator);
        self.* = undefined;
    }

    fn freeColumns(allocator: Allocator, cols: []const types.Column) void {
        for (cols) |c| allocator.free(c.name);
        allocator.free(cols);
    }

    fn dupeColumns(allocator: Allocator, cols: []const types.Column) ![]const types.Column {
        const out = try allocator.alloc(types.Column, cols.len);
        var n: usize = 0;
        errdefer {
            for (out[0..n]) |c| allocator.free(c.name);
            allocator.free(out);
        }
        for (cols, out) |src, *dst| {
            dst.* = src;
            dst.name = try allocator.dupe(u8, src.name);
            n += 1;
        }
        return out;
    }

    pub fn registerTable(self: *UdfRegistry, udf: TableUdf) !void {
        try validateName(udf.name);
        if (udf.input_schemas.len == 0 or udf.output_schema.len == 0) {
            return Error.FunctionInvalidDefinition;
        }
        for (udf.input_schemas) |cols| {
            if (cols.len == 0) return Error.FunctionInvalidDefinition;
        }
        if (udf.passthrough.len > 0) {
            // Pass-through requires row alignment (to input 0), at least
            // one kernel-computed output column, and every pair must
            // reference real input-0 / output columns with matching types.
            if (!udf.row_aligned) {
                return Error.FunctionInvalidDefinition;
            }
            if (udf.passthrough.len >= udf.output_schema.len) {
                return Error.FunctionInvalidDefinition;
            }
            const in_cols = udf.input_schemas[0];
            for (udf.passthrough) |pp| {
                if (pp.out_idx >= udf.output_schema.len or pp.in_idx >= in_cols.len) {
                    return Error.FunctionInvalidDefinition;
                }
                const oc = udf.output_schema[pp.out_idx];
                const ic = in_cols[pp.in_idx];
                if (!std.meta.eql(oc.type, ic.type) or oc.nullable != ic.nullable) {
                    return Error.FunctionInvalidDefinition;
                }
            }
        }
        if (udf.kernel_input_cols > udf.input_schemas[0].len) {
            return Error.FunctionInvalidDefinition;
        }
        // Broadcast inputs: valid indices only, never input 0 (it defines
        // partitioning and row alignment).
        for (udf.broadcast_inputs) |b| {
            if (b == 0 or b >= udf.input_schemas.len) {
                return Error.FunctionInvalidDefinition;
            }
        }
        if (self.tableByName(udf.name) != null) return Error.FunctionAlreadyExists;

        const name = try lowerName(self.allocator, udf.name);
        errdefer self.allocator.free(name);
        const input_schemas = try self.allocator.alloc([]const types.Column, udf.input_schemas.len);
        var n_in: usize = 0;
        errdefer {
            for (input_schemas[0..n_in]) |cols| freeColumns(self.allocator, cols);
            self.allocator.free(input_schemas);
        }
        for (udf.input_schemas, input_schemas) |src, *dst| {
            dst.* = try dupeColumns(self.allocator, src);
            n_in += 1;
        }
        const output_schema = try dupeColumns(self.allocator, udf.output_schema);
        errdefer freeColumns(self.allocator, output_schema);
        const arg_types = try self.allocator.dupe(types.Type, udf.arg_types);
        errdefer self.allocator.free(arg_types);
        const passthrough = try self.allocator.dupe(PassPair, udf.passthrough);
        errdefer self.allocator.free(passthrough);
        const broadcast_inputs = try self.allocator.dupe(u32, udf.broadcast_inputs);
        errdefer self.allocator.free(broadcast_inputs);
        try self.tables.append(self.allocator, .{
            .name = name,
            .input_schemas = input_schemas,
            .output_schema = output_schema,
            .execution = udf.execution,
            .arg_types = arg_types,
            .row_aligned = udf.row_aligned,
            .ordered_output = udf.ordered_output,
            .broadcast_inputs = broadcast_inputs,
            .passthrough = passthrough,
            .kernel_input_cols = if (udf.kernel_input_cols == 0)
                @intCast(udf.input_schemas[0].len)
            else
                udf.kernel_input_cols,
            .process = udf.process,
            .user_data = udf.user_data,
            .registration = self.last_registration + 1,
        });
        self.last_registration += 1;
    }

    pub fn dropTable(self: *UdfRegistry, name: []const u8) bool {
        for (self.tables.items, 0..) |entry, i| {
            if (std.ascii.eqlIgnoreCase(entry.name, name)) {
                self.allocator.free(entry.name);
                for (entry.input_schemas) |cols| freeColumns(self.allocator, cols);
                self.allocator.free(entry.input_schemas);
                freeColumns(self.allocator, entry.output_schema);
                self.allocator.free(entry.arg_types);
                self.allocator.free(entry.passthrough);
                self.allocator.free(entry.broadcast_inputs);
                _ = self.tables.swapRemove(i);
                return true;
            }
        }
        return false;
    }

    pub fn tableByName(self: *const UdfRegistry, name: []const u8) ?*const TableEntry {
        for (self.tables.items) |*entry| {
            if (std.ascii.eqlIgnoreCase(entry.name, name)) return entry;
        }
        return null;
    }

    pub fn registerScalar(self: *UdfRegistry, udf: ScalarUdf) !void {
        try validateName(udf.name);
        if (udf.arg_types.len > 16) return Error.FunctionInvalidDefinition;
        if (self.scalarOverloadExists(udf.name, udf.arg_types)) return Error.FunctionAlreadyExists;

        const name = try lowerName(self.allocator, udf.name);
        errdefer self.allocator.free(name);
        const arg_types = try self.allocator.dupe(Type, udf.arg_types);
        errdefer self.allocator.free(arg_types);
        try self.scalars.append(self.allocator, .{
            .name = name,
            .arg_types = arg_types,
            .return_type = udf.return_type,
            .null_strategy = udf.null_strategy,
            .volatility = udf.volatility,
            .kernel = udf.kernel,
            .user_data = udf.user_data,
        });
    }

    pub fn registerAggregate(self: *UdfRegistry, udf: AggregateUdf) !void {
        try validateName(udf.name);
        if (udf.arg_types.len > 16 or udf.state_size == 0) return Error.FunctionInvalidDefinition;
        if (udf.state_align == 0 or udf.state_align > 16 or !std.math.isPowerOfTwo(udf.state_align)) {
            return Error.FunctionInvalidDefinition;
        }
        if (isReservedAggregateName(udf.name)) return Error.FunctionAlreadyExists;
        if (self.aggregateOverloadExists(udf.name, udf.arg_types)) return Error.FunctionAlreadyExists;

        const name = try lowerName(self.allocator, udf.name);
        errdefer self.allocator.free(name);
        const arg_types = try self.allocator.dupe(Type, udf.arg_types);
        errdefer self.allocator.free(arg_types);
        try self.aggregates.append(self.allocator, .{
            .name = name,
            .arg_types = arg_types,
            .return_type = udf.return_type,
            .state_size = udf.state_size,
            .state_align = udf.state_align,
            .init = udf.init,
            .update_one = udf.update_one,
            .update_batch = udf.update_batch,
            .combine = udf.combine,
            .finalize = udf.finalize,
            .destroy = udf.destroy,
            .volatility = udf.volatility,
            .user_data = udf.user_data,
        });
    }

    pub fn scalarEntries(self: *const UdfRegistry) []const ScalarEntry {
        return self.scalars.items;
    }

    pub fn aggregateEntries(self: *const UdfRegistry) []const AggregateEntry {
        return self.aggregates.items;
    }

    pub fn hasAggregateName(self: *const UdfRegistry, name: []const u8) bool {
        for (self.aggregates.items) |entry| {
            if (std.ascii.eqlIgnoreCase(entry.name, name)) return true;
        }
        return false;
    }

    pub fn resolveAggregateExact(
        self: *const UdfRegistry,
        name: []const u8,
        arg_types: []const Type,
    ) ?AggregateEntry {
        for (self.aggregates.items) |entry| {
            if (!std.ascii.eqlIgnoreCase(entry.name, name)) continue;
            if (sameTypeTags(entry.arg_types, arg_types)) return entry;
        }
        return null;
    }

    fn scalarOverloadExists(self: *const UdfRegistry, name: []const u8, arg_types: []const Type) bool {
        for (self.scalars.items) |entry| {
            if (!std.ascii.eqlIgnoreCase(entry.name, name)) continue;
            if (sameTypeTags(entry.arg_types, arg_types)) return true;
        }
        return false;
    }

    fn aggregateOverloadExists(self: *const UdfRegistry, name: []const u8, arg_types: []const Type) bool {
        for (self.aggregates.items) |entry| {
            if (!std.ascii.eqlIgnoreCase(entry.name, name)) continue;
            if (sameTypeTags(entry.arg_types, arg_types)) return true;
        }
        return false;
    }
};

pub fn sameTypeTags(a: []const Type, b: []const Type) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (@as(TypeTag, x) != @as(TypeTag, y)) return false;
    }
    return true;
}

fn validateName(name: []const u8) !void {
    if (name.len == 0) return Error.FunctionInvalidDefinition;
    for (name) |c| {
        if (!(std.ascii.isAlphanumeric(c) or c == '_')) return Error.FunctionInvalidDefinition;
    }
}

fn lowerName(allocator: Allocator, name: []const u8) ![]u8 {
    const out = try allocator.alloc(u8, name.len);
    for (name, out) |c, *dst| dst.* = std.ascii.toLower(c);
    return out;
}

fn isReservedAggregateName(name: []const u8) bool {
    const names = [_][]const u8{
        "count",        "sum",         "min",        "max",      "avg",
        "stddev_pop",   "stddev_samp", "var_pop",    "var_samp", "count_distinct",
        "group_concat", "string_agg",  "percentile", "max_by",
    };
    for (names) |n| if (std.ascii.eqlIgnoreCase(name, n)) return true;
    return false;
}

// Creating a function reserves builtin names (`Catalog.registerScalarUdf`);
// the registry itself takes any name.
test "udf registry rejects a duplicate overload and takes a builtin's name" {
    const testing = std.testing;
    var reg = UdfRegistry.init(testing.allocator);
    defer reg.deinit();

    const noop = struct {
        fn kernel(ctx: *const ScalarContext, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            _ = args;
            var i: usize = 0;
            while (i < row_count) : (i += 1) try out.data.int.append(ctx.allocator, 0);
        }
    }.kernel;

    try reg.registerScalar(.{
        .name = "upper",
        .arg_types = &.{.string},
        .return_type = .string,
        .kernel = noop,
    });
    try reg.registerScalar(.{
        .name = "score_bucket",
        .arg_types = &.{.double},
        .return_type = .int,
        .kernel = noop,
    });
    try testing.expectError(Error.FunctionAlreadyExists, reg.registerScalar(.{
        .name = "SCORE_BUCKET",
        .arg_types = &.{.double},
        .return_type = .int,
        .kernel = noop,
    }));
}

// The parser reads view and function definitions before its statement holds
// a lease (#368), so a lookup's result must survive a concurrent replace,
// drop, or map growth.
test "registry lookups return copies that outlive replace, growth and drop" {
    const testing = std.testing;
    var views = ViewRegistry.init(testing.allocator);
    defer views.deinit();
    try views.register("db", .{ .name = "V", .materialized = false, .body = "SELECT 1", .create_text = "CREATE VIEW V AS SELECT 1" }, false);
    const view = (try views.get(testing.allocator, "db", "v")).?;
    defer view.deinit(testing.allocator);

    try views.register("db", .{ .name = "v", .materialized = true, .body = "SELECT 2", .create_text = "CREATE VIEW v AS SELECT 2" }, true);
    var name_buf: [16]u8 = undefined;
    for (0..64) |i| {
        const filler = try std.fmt.bufPrint(&name_buf, "filler_{d}", .{i});
        try views.register("db", .{ .name = filler, .materialized = false, .body = "SELECT 3", .create_text = "CREATE VIEW f AS SELECT 3" }, false);
    }
    try testing.expect(try views.drop("db", "v"));

    try testing.expectEqualStrings("v", view.name);
    try testing.expect(!view.materialized);
    try testing.expectEqualStrings("SELECT 1", view.body);
    try testing.expectEqualStrings("CREATE VIEW V AS SELECT 1", view.create_text);
    try testing.expect((try views.get(testing.allocator, "db", "v")) == null);
    try testing.expect(!views.contains("db", "v"));
    try testing.expect(views.contains("db", "FILLER_7"));

    var fns = SqlFnRegistry.init(testing.allocator);
    defer fns.deinit();
    try fns.register("db", .{ .name = "F", .param_names = &.{"a"}, .param_types = &.{.bigint}, .body = "SELECT a", .create_text = "CREATE FUNCTION F(a BIGINT)" }, false);
    const f = (try fns.get(testing.allocator, "db", "f")).?;
    defer f.deinit(testing.allocator);

    try fns.register("db", .{ .name = "f", .param_names = &.{ "b", "c" }, .param_types = &.{ .int, .string }, .body = "SELECT b, c", .create_text = "CREATE FUNCTION f(b INT, c STRING)" }, true);
    try testing.expect(try fns.drop("db", "f"));

    try testing.expectEqualStrings("f", f.name);
    try testing.expectEqual(@as(usize, 1), f.param_names.len);
    try testing.expectEqualStrings("a", f.param_names[0]);
    try testing.expectEqualSlices(Type, &.{.bigint}, f.param_types);
    try testing.expectEqualStrings("SELECT a", f.body);
    try testing.expectEqualStrings("CREATE FUNCTION F(a BIGINT)", f.create_text);
    try testing.expect((try fns.get(testing.allocator, "db", "f")) == null);
}

test "dropping a database removes only that database's views and functions" {
    const testing = std.testing;
    var views = ViewRegistry.init(testing.allocator);
    defer views.deinit();
    var fns = SqlFnRegistry.init(testing.allocator);
    defer fns.deinit();

    const dbs = [_][]const u8{ "probe", "probe_v", "probe_vx" };
    var name_buf: [16]u8 = undefined;
    for (dbs) |db| for (0..40) |i| {
        const name = try std.fmt.bufPrint(&name_buf, "item_{d}", .{i});
        try views.register(db, .{ .name = name, .materialized = false, .body = "SELECT 1", .create_text = "CREATE VIEW v AS SELECT 1" }, false);
        try fns.register(db, .{ .name = name, .param_names = &.{"x"}, .param_types = &.{.bigint}, .body = "SELECT x", .create_text = "CREATE FUNCTION f(x BIGINT)" }, false);
    };

    views.dropDatabase("probe_v");
    fns.dropDatabase("probe_v");

    for (dbs) |db| {
        const kept = !std.mem.eql(u8, db, "probe_v");
        const view_names = try views.listNames(testing.allocator, db);
        defer {
            for (view_names) |n| testing.allocator.free(n);
            testing.allocator.free(view_names);
        }
        try testing.expectEqual(@as(usize, if (kept) 40 else 0), view_names.len);
        const fn_names = try fns.listNames(testing.allocator, db);
        defer {
            for (fn_names) |n| testing.allocator.free(n);
            testing.allocator.free(fn_names);
        }
        try testing.expectEqual(@as(usize, if (kept) 40 else 0), fn_names.len);
    }
    try testing.expect(views.contains("probe", "item_0"));
    try testing.expect((try fns.get(testing.allocator, "probe_v", "item_0")) == null);

    try views.register("probe_v", .{ .name = "item_0", .materialized = false, .body = "SELECT 2", .create_text = "CREATE VIEW item_0 AS SELECT 2" }, false);
    try fns.register("probe_v", .{ .name = "item_0", .param_names = &.{}, .param_types = &.{}, .body = "SELECT 2", .create_text = "CREATE FUNCTION item_0()" }, false);
}

test "a registry copy that runs out of memory frees what it copied" {
    const testing = std.testing;
    var fns = SqlFnRegistry.init(testing.allocator);
    defer fns.deinit();
    try fns.register("db", .{ .name = "g", .param_names = &.{ "a", "b" }, .param_types = &.{ .int, .int }, .body = "SELECT a + b", .create_text = "CREATE FUNCTION g(a INT, b INT)" }, false);
    var views = ViewRegistry.init(testing.allocator);
    defer views.deinit();
    try views.register("db", .{ .name = "w", .materialized = false, .body = "SELECT 1", .create_text = "CREATE VIEW w AS SELECT 1" }, false);

    const copy_both = struct {
        fn run(allocator: Allocator, fn_registry: *SqlFnRegistry, view_registry: *ViewRegistry) !void {
            const f = (try fn_registry.get(allocator, "db", "g")).?;
            defer f.deinit(allocator);
            const view = (try view_registry.get(allocator, "db", "w")).?;
            view.deinit(allocator);
        }
    }.run;
    try testing.checkAllAllocationFailures(testing.allocator, copy_both, .{ &fns, &views });
}
