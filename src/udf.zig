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
    process: TvfProcess,
    user_data: ?*anyopaque = null,
};

pub const TableEntry = struct {
    name: []const u8,
    input_schemas: []const []const types.Column,
    output_schema: []const types.Column,
    execution: TvfExecution,
    arg_types: []const types.Type,
    process: TvfProcess,
    user_data: ?*anyopaque,
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
};

/// Catalog-owned registry of SQL inline table functions, keyed by
/// `<database>\x00<name>` (functions are database-scoped like tables).
/// All strings are owned copies. Thread-safe: parse-time lookups and
/// DDL mutations take the mutex.
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
            self.freeFn(e.value_ptr.*);
        }
        self.map.deinit(self.allocator);
        self.* = undefined;
    }

    fn freeFn(self: *SqlFnRegistry, f: SqlTableFn) void {
        self.allocator.free(f.name);
        for (f.param_names) |p| self.allocator.free(p);
        self.allocator.free(f.param_names);
        self.allocator.free(f.param_types);
        self.allocator.free(f.body);
        self.allocator.free(f.create_text);
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
        errdefer self.allocator.free(name);
        const param_names = try self.allocator.alloc([]const u8, f.param_names.len);
        var pn: usize = 0;
        errdefer {
            for (param_names[0..pn]) |p| self.allocator.free(p);
            self.allocator.free(param_names);
        }
        for (f.param_names) |p| {
            param_names[pn] = try self.allocator.dupe(u8, p);
            pn += 1;
        }
        const param_types = try self.allocator.dupe(Type, f.param_types);
        errdefer self.allocator.free(param_types);
        const body = try self.allocator.dupe(u8, f.body);
        errdefer self.allocator.free(body);
        const create_text = try self.allocator.dupe(u8, f.create_text);
        errdefer self.allocator.free(create_text);
        const owned: SqlTableFn = .{
            .name = name,
            .param_names = param_names,
            .param_types = param_types,
            .body = body,
            .create_text = create_text,
        };

        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const gop = try self.map.getOrPut(self.allocator, k);
        if (gop.found_existing) {
            // The error return frees k + every `owned` component via the
            // errdefers above — no manual frees here or they double-free.
            if (!replace) return Error.FunctionAlreadyExists;
            self.allocator.free(k);
            self.freeFn(gop.value_ptr.*);
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
            self.freeFn(kv.value);
            return true;
        }
        return false;
    }

    /// Parse-time lookup. The returned pointer stays valid only while no
    /// concurrent register/drop mutates this entry — callers copy what
    /// they need into their arena before releasing implied ownership
    /// (queries parse fast; DDL on a function mid-parse of a query using
    /// it is a documented race we accept like table DDL).
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
            if (map_key.len <= db.len + 1) continue;
            if (!std.mem.eql(u8, map_key[0..db.len], db) or map_key[db.len] != 0) continue;
            try out.append(allocator, try allocator.dupe(u8, map_key[db.len + 1 ..]));
        }
        return out.toOwnedSlice(allocator);
    }

    pub fn get(self: *SqlFnRegistry, db: []const u8, name: []const u8) ?*const SqlTableFn {
        var kbuf: [512]u8 = undefined;
        if (db.len + 1 + name.len > kbuf.len) return null;
        @memcpy(kbuf[0..db.len], db);
        kbuf[db.len] = 0;
        for (name, kbuf[db.len + 1 ..][0..name.len]) |c, *o| o.* = std.ascii.toLower(c);
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        return self.map.getPtr(kbuf[0 .. db.len + 1 + name.len]);
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
};

/// Catalog-owned registry of views, keyed `<database>\x00<name>` like the
/// function registry. All strings are owned copies; thread-safe.
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
            self.freeView(e.value_ptr.*);
        }
        self.map.deinit(self.allocator);
        self.* = undefined;
    }

    fn freeView(self: *ViewRegistry, v: ViewDef) void {
        self.allocator.free(v.name);
        self.allocator.free(v.body);
        self.allocator.free(v.create_text);
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
        try validateName(v.name);
        const k = try viewKey(self.allocator, db, v.name);
        errdefer self.allocator.free(k);
        const name = try lowerName(self.allocator, v.name);
        errdefer self.allocator.free(name);
        const body = try self.allocator.dupe(u8, v.body);
        errdefer self.allocator.free(body);
        const create_text = try self.allocator.dupe(u8, v.create_text);
        errdefer self.allocator.free(create_text);
        const owned: ViewDef = .{
            .name = name,
            .materialized = v.materialized,
            .body = body,
            .create_text = create_text,
        };

        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const gop = try self.map.getOrPut(self.allocator, k);
        if (gop.found_existing) {
            if (!replace) return Error.ViewAlreadyExists;
            self.allocator.free(k);
            self.freeView(gop.value_ptr.*);
            gop.value_ptr.* = owned;
        } else {
            gop.value_ptr.* = owned;
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
            self.freeView(kv.value);
            return true;
        }
        return false;
    }

    pub fn get(self: *ViewRegistry, db: []const u8, name: []const u8) ?*const ViewDef {
        var kbuf: [512]u8 = undefined;
        if (db.len + 1 + name.len > kbuf.len) return null;
        @memcpy(kbuf[0..db.len], db);
        kbuf[db.len] = 0;
        for (name, kbuf[db.len + 1 ..][0..name.len]) |c, *o| o.* = std.ascii.toLower(c);
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        return self.map.getPtr(kbuf[0 .. db.len + 1 + name.len]);
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
            if (map_key.len <= db.len + 1) continue;
            if (!std.mem.eql(u8, map_key[0..db.len], db) or map_key[db.len] != 0) continue;
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
    db: []const u8,
    views: ?*ViewRegistry = null,
};

pub const UdfRegistry = struct {
    allocator: Allocator,
    scalars: std.ArrayList(ScalarEntry) = .empty,
    aggregates: std.ArrayList(AggregateEntry) = .empty,
    tables: std.ArrayList(TableEntry) = .empty,

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
        try self.tables.append(self.allocator, .{
            .name = name,
            .input_schemas = input_schemas,
            .output_schema = output_schema,
            .execution = udf.execution,
            .arg_types = arg_types,
            .process = udf.process,
            .user_data = udf.user_data,
        });
    }

    pub fn dropTable(self: *UdfRegistry, name: []const u8) bool {
        for (self.tables.items, 0..) |entry, i| {
            if (std.ascii.eqlIgnoreCase(entry.name, name)) {
                self.allocator.free(entry.name);
                for (entry.input_schemas) |cols| freeColumns(self.allocator, cols);
                self.allocator.free(entry.input_schemas);
                freeColumns(self.allocator, entry.output_schema);
                self.allocator.free(entry.arg_types);
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
        if (isReservedScalarName(udf.name)) return Error.FunctionAlreadyExists;
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

fn isReservedScalarName(name: []const u8) bool {
    const names = [_][]const u8{
        "upper",          "lower",             "ltrim",           "rtrim",           "trim",            "reverse",
        "length",         "octet_length",      "char_length",     "concat",          "substring",       "replace",
        "regexp_replace", "coalesce",          "ifnull",          "nullif",          "abs",             "ceil",
        "floor",          "round",             "sign",            "mod",             "add",             "sub",
        "mul",            "div",               "__narrow_bigint", "pow",             "sqrt",            "exp",
        "ln",             "log10",             "log2",            "greatest",        "least",           "truncate",
        "degrees",        "radians",           "atan2",           "dayofweek",       "dayofyear",       "quarter",
        "last_day",       "date_add",          "date_sub",        "date_add_months", "date_add_years",  "extract",
        "date_trunc",     "year",              "month",           "day",             "makedate",        "hour",
        "minute",         "second",            "datediff",        "unix_timestamp",  "from_unixtime",   "date_format",
        "now",            "current_timestamp", "current_date",    "to_bigint",       "to_double",       "to_int",
        "to_smallint",    "to_tinyint",        "to_largeint",     "to_boolean",      "to_date",         "to_datetime",
        "to_string",      "md5",               "sha1",            "sha256",          "crc32",           "hex",
        "unhex",          "to_base64",         "from_base64",     "lpad",            "rpad",            "repeat",
        "space",          "ascii",             "position",        "instr",           "substring_index", "strcmp",
        "lcase",          "ucase",             "power",           "ceiling",         "chr",
    };
    for (names) |n| if (std.ascii.eqlIgnoreCase(name, n)) return true;
    return false;
}

test "udf registry rejects duplicates and reserved builtins" {
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

    try testing.expectError(Error.FunctionAlreadyExists, reg.registerScalar(.{
        .name = "upper",
        .arg_types = &.{.string},
        .return_type = .string,
        .kernel = noop,
    }));

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
