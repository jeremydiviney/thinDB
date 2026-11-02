//! The part of a scan's filter that reads computed columns: `WHERE
//! LOWER(s) = 'x'`, `REGEXP_LIKE(url, '...')`, a UDF's verdict. The parser
//! lowers each expression operand to a computed column (`__pred_expr_N`)
//! that a Compute above the scan would fill, with the conjuncts that read
//! it filtering the Compute's output. Taken into the scan instead
//! (`Scan.tryFuseDerivedFilter`), the expressions are computed over only
//! the rows the rest of the filter leaves, and the condition cache and the
//! predicate memo key the whole filter by what it computes, so a later
//! statement repeating it reads its verdicts back instead of computing
//! them again.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;
const transform = @import("../engine/transform.zig");

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const PredicateExpr = predicate.PredicateExpr;
const expr_mod = @import("expr.zig");
const compute_mod = @import("compute.zig");
const Compute = compute_mod.Compute;
const Derived = compute_mod.Derived;
const udf_mod = @import("../udf.zig");

/// Schema-only upstream for the filter's Compute, which only ever
/// evaluates the batches the scan gathers.
const InputSchema = struct {
    schema: []const Column,
    pub fn next(_: *InputSchema) !?Batch {
        return null;
    }
    pub fn deinit(_: *InputSchema) void {}
    pub fn outputSchema(self: *InputSchema) []const Column {
        return self.schema;
    }
    pub fn addPrune(_: *InputSchema, _: predicate.Predicate) !void {}
    pub fn stats(_: *InputSchema) exec.PipelineStats {
        return .{ .upper_rows = std.math.maxInt(u64) };
    }
    pub fn accountant(_: *InputSchema) ?*exec.memory.MemoryAccountant {
        return null;
    }
    pub fn explain(_: *InputSchema, _: *std.ArrayList(u8), _: Allocator, _: usize) !void {}
};

/// A WHERE's conjuncts over computed columns and the columns' definitions,
/// for every scan of a parallel plan to filter on itself
/// (`Scan.tryFuseDerivedFilter`).
pub const ComputedWhere = struct {
    predicate: PredicateExpr,
    derived: []const Derived,
};

pub const DerivedFilter = struct {
    /// Most table columns the filter may read: a scan holds a row group's
    /// input columns on the stack.
    pub const MAX_INPUTS = 16;

    allocator: Allocator,
    /// The conjuncts that read computed columns, validated against
    /// `eval_schema`.
    predicate: PredicateExpr,
    /// Computes the columns `predicate` reads from batches of the
    /// `input_schema` columns.
    compute: Query,
    stub: *InputSchema,
    /// `input_schema` followed by the computed columns.
    eval_schema: []const Column,
    /// The table columns the computed ones and `predicate` itself read, as
    /// table-schema indices, in `input_schema`'s order.
    input_phys: []const usize,
    input_schema: []const Column,
    /// The computed columns' names and their expressions' digests, which
    /// stand in for the names wherever the filter is digested.
    columns: predicate.ColumnDigests,
    /// Per input column: the view a row group's rows are read from, filled
    /// by the scan before each `apply`.
    input_views: []ColumnView,
    /// The rows `apply` evaluates, gathered when they are not the whole batch.
    gathered: []ColumnStore,
    gathered_views: []ColumnView,
    rows: std.ArrayListUnmanaged(u32) = .empty,
    verdicts: std.ArrayListUnmanaged(bool) = .empty,

    /// A filter over `conjuncts`, computing the entries of `derived` they
    /// read, directly or through other entries. Null when it can't stand in
    /// for the filter above the Compute exactly: an entry reads a column
    /// `table_columns` lacks, or is named like a table column. Null, too,
    /// when what it matches could differ between statements (a volatile
    /// function, a subquery, ...) or has no digest, since then no statement
    /// could reuse it, and when it reads more than `MAX_INPUTS` columns.
    pub fn create(allocator: Allocator, table_columns: []const Column, conjuncts: PredicateExpr, derived: []const Derived, registry: ?*const udf_mod.UdfRegistry) !?*DerivedFilter {
        const needed = try allocator.alloc(bool, derived.len);
        defer allocator.free(needed);
        for (derived, needed) |d, *n| n.* = predicate.touchesColumn(conjuncts, d.name);
        var refs: std.ArrayListUnmanaged([]const u8) = .empty;
        defer refs.deinit(allocator);
        var i = derived.len;
        while (i > 0) {
            i -= 1;
            if (!needed[i]) continue;
            refs.clearRetainingCapacity();
            try compute_mod.collectColumnRefs(allocator, &refs, derived[i].expr);
            for (derived[0..i], needed[0..i]) |d, *n| n.* = n.* or nameListed(refs.items, d.name);
        }

        var defs: std.ArrayListUnmanaged(Derived) = .empty;
        defer defs.deinit(allocator);
        var input_phys: std.ArrayListUnmanaged(usize) = .empty;
        defer input_phys.deinit(allocator);
        for (derived, needed) |d, n| {
            if (!n) continue;
            if (types.findColumn(table_columns, d.name) != null) return null;
            if (!compute_mod.immutable(d.expr, registry)) return null;
            refs.clearRetainingCapacity();
            try compute_mod.collectColumnRefs(allocator, &refs, d.expr);
            for (refs.items) |r| {
                if (definedIn(defs.items, r)) continue;
                const phys = types.findColumn(table_columns, r) orelse return null;
                if (std.mem.indexOfScalar(usize, input_phys.items, phys) == null) try input_phys.append(allocator, phys);
            }
            try defs.append(allocator, d);
        }
        if (defs.items.len == 0) return null;
        refs.clearRetainingCapacity();
        try predicate.collectColumnNames(allocator, &refs, conjuncts);
        for (refs.items) |r| {
            if (definedIn(defs.items, r)) continue;
            const phys = types.findColumn(table_columns, r) orelse return null;
            if (std.mem.indexOfScalar(usize, input_phys.items, phys) == null) try input_phys.append(allocator, phys);
        }
        if (input_phys.items.len > MAX_INPUTS) return null;

        const names = try allocator.alloc([]const u8, defs.items.len);
        errdefer allocator.free(names);
        const digests = try allocator.alloc(u128, defs.items.len);
        errdefer allocator.free(digests);
        for (defs.items, names, digests, 0..) |d, *n, *dg, k| {
            n.* = d.name;
            const earlier: predicate.ColumnDigests = .{ .names = names[0..k], .digests = digests[0..k] };
            dg.* = expr_mod.digest(d.expr, &earlier) orelse {
                allocator.free(digests);
                allocator.free(names);
                return null;
            };
        }
        const columns: predicate.ColumnDigests = .{ .names = names, .digests = digests };
        if (predicate.digestWith(conjuncts, &columns) == null) {
            allocator.free(digests);
            allocator.free(names);
            return null;
        }

        const phys = try input_phys.toOwnedSlice(allocator);
        errdefer allocator.free(phys);
        const input_schema = try allocator.alloc(Column, phys.len);
        errdefer allocator.free(input_schema);
        for (phys, input_schema) |p, *c| c.* = table_columns[p];
        const stub = try allocator.create(InputSchema);
        errdefer allocator.destroy(stub);
        stub.* = .{ .schema = input_schema };
        var compute = try Compute.createWithRegistry(allocator, makeQuery(allocator, stub), defs.items, registry);
        errdefer compute.deinit();
        // The computed columns live in the Compute's own copy from here on.
        for (names, compute.outputSchema()[phys.len..]) |*n, c| n.* = c.name;
        const eval_schema = compute.outputSchema();
        var validated = conjuncts;
        try predicate.validateExpr(&validated, eval_schema);

        const gathered = try allocator.alloc(ColumnStore, phys.len);
        errdefer allocator.free(gathered);
        var inited: usize = 0;
        errdefer for (gathered[0..inited]) |*g| g.deinit(allocator);
        for (gathered, input_schema) |*g, c| {
            g.* = try ColumnStore.init(allocator, c.type, c.nullable);
            inited += 1;
        }
        const gathered_views = try allocator.alloc(ColumnView, phys.len);
        errdefer allocator.free(gathered_views);
        const input_views = try allocator.alloc(ColumnView, phys.len);
        errdefer allocator.free(input_views);

        const self = try allocator.create(DerivedFilter);
        self.* = .{
            .allocator = allocator,
            .predicate = validated,
            .compute = compute,
            .stub = stub,
            .eval_schema = eval_schema,
            .input_phys = phys,
            .input_schema = input_schema,
            .columns = columns,
            .input_views = input_views,
            .gathered = gathered,
            .gathered_views = gathered_views,
        };
        return self;
    }

    /// Whether `create` takes these conjuncts, for a planner choosing
    /// between a scan that filters on them and a filter over the Compute.
    pub fn accepts(allocator: Allocator, table_columns: []const Column, conjuncts: PredicateExpr, derived: []const Derived, registry: ?*const udf_mod.UdfRegistry) !bool {
        const df = (try create(allocator, table_columns, conjuncts, derived, registry)) orelse return false;
        df.destroy();
        return true;
    }

    pub fn destroy(self: *DerivedFilter) void {
        const allocator = self.allocator;
        self.compute.deinit();
        allocator.destroy(self.stub);
        allocator.free(self.input_schema);
        allocator.free(self.input_phys);
        allocator.free(self.columns.names);
        allocator.free(self.columns.digests);
        allocator.free(self.input_views);
        for (self.gathered) |*g| g.deinit(allocator);
        allocator.free(self.gathered);
        allocator.free(self.gathered_views);
        self.rows.deinit(allocator);
        self.verdicts.deinit(allocator);
        allocator.destroy(self);
    }

    /// Clears the rows of `mask` the filter fails, computing the expressions
    /// over only the rows it holds set. `input_views` hold the batch's input
    /// columns, `mask.len` rows each.
    pub fn apply(self: *DerivedFilter, mask: []bool) !void {
        const n = mask.len;
        self.rows.clearRetainingCapacity();
        try self.rows.ensureTotalCapacity(self.allocator, n);
        for (mask, 0..) |m, r| if (m) self.rows.appendAssumeCapacity(@intCast(r));
        const k = self.rows.items.len;
        if (k == 0) return;
        const batch: Batch = if (k == n) .{ .schema = self.input_schema, .values = self.input_views, .row_count = n } else blk: {
            for (self.gathered, self.gathered_views, self.input_views) |*g, *gv, v| {
                g.clear();
                try transform.appendByIndices(self.allocator, v, self.rows.items, g);
                gv.* = g.view();
            }
            break :blk .{ .schema = self.input_schema, .values = self.gathered_views, .row_count = k };
        };
        const evaluated = try exec.queryAs(Compute, self.compute).?.evalBatch(batch);
        try self.verdicts.resize(self.allocator, k);
        try predicate.evaluateExprGuided(self.allocator, self.predicate, self.eval_schema, evaluated, self.verdicts.items, null);
        for (self.rows.items, self.verdicts.items) |r, v| mask[r] = v;
    }
};

fn nameListed(names: []const []const u8, name: []const u8) bool {
    for (names) |n| if (types.columnNameEql(n, name)) return true;
    return false;
}

fn definedIn(defs: []const Derived, name: []const u8) bool {
    for (defs) |d| if (types.columnNameEql(d.name, name)) return true;
    return false;
}
