//! Operator IR — the binary wire format clients send to the server.
//!
//! Each query is a single operator tree, encoded as a tagged tree: each
//! operator carries its upstream encoded immediately after the operator's
//! own payload. Decoding is recursive — read the tag, decode the op-
//! specific payload, then (for non-source operators) recursively decode
//! the upstream.
//!
//! Scope (walking skeleton): Scan + Limit only. More operators land as
//! the client API grows. The format is versioned so additions are safe.
//!
//! Wire format:
//!
//!   [header — 8 bytes]
//!     magic  "tDBQ"          4
//!     version u16            2
//!     flags u16              2 (reserved)
//!
//!   [op tree — recursive]
//!     tag u8                 1
//!     op-specific payload    N
//!     (for non-source ops) upstream op tree (recursive)

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Value = types.Value;
const ValueTag = types.ValueTag;

const exec_predicate = @import("../exec/predicate.zig");
const Predicate = exec_predicate.Predicate;
const PredicateExpr = exec_predicate.PredicateExpr;
const PredicateOp = exec_predicate.PredicateOp;

const exec_sort = @import("../exec/sort.zig");
pub const SortSpec = exec_sort.SortSpec;

const exec_aggregate = @import("../exec/aggregate.zig");
pub const AggFunc = exec_aggregate.AggFunc;
pub const AggSpec = exec_aggregate.AggSpec;

const exec_compute = @import("../exec/compute.zig");
pub const Derived = exec_compute.Derived;

const exec_join = @import("../exec/join.zig");
pub const JoinSpec = exec_join.Spec;
pub const JoinKeyPair = exec_join.KeyPair;
pub const JoinRangePredicate = exec_join.RangePredicate;
pub const JoinAlgorithm = exec_join.Algorithm;
pub const JoinType = exec_join.JoinType;

const exec_expr = @import("../exec/expr.zig");
pub const Expr = exec_expr.Expr;

pub const magic: [4]u8 = .{ 't', 'D', 'B', 'Q' };
pub const version: u16 = 1;
pub const header_size: usize = 8;

/// Qualified table reference. Either segment may be null when the
/// user left it implicit — resolution against the active Session
/// happens at compile time, not parse time.
pub const TableRef = struct {
    database: ?[]const u8 = null,
    schema: ?[]const u8 = null,
    name: []const u8,
};

/// Side-effect statement payload. DDL ops don't produce rows; their
/// `Query` returns null on the first `next()` call after invoking the
/// side effect against the active Session.
pub const DdlOp = union(enum) {
    create_database: []const u8,
    drop_database: []const u8,
    create_schema: []const u8,
    drop_schema: []const u8,
    /// `USE name` — set current schema (one identifier).
    use_schema: []const u8,
    /// `USE db.schema` — set current database AND schema.
    use_database_schema: struct { database: []const u8, schema: []const u8 },
    create_table: CreateTable,
    drop_table: DropTable,
};

pub const ColumnDef = struct {
    name: []const u8,
    column_type: @import("../types.zig").Type,
    nullable: bool,
    /// Optional DEFAULT clause from CREATE TABLE. Today only literal
    /// values are accepted. Threads into `types.Column.default_value`
    /// at compile time; INSERT fills omitted columns from this.
    default_value: ?@import("../types.zig").Value = null,
    /// MySQL-style AUTO_INCREMENT attribute. When set, the column is
    /// integer-typed and the table maintains a per-table monotonic
    /// counter that fills NULL/omitted inserts. Counter advances past
    /// any explicit value the caller supplies (MySQL semantics).
    auto_increment: bool = false,
};

pub const CreateTable = struct {
    table: TableRef,
    if_not_exists: bool,
    /// Session-local temp table. Lives in the connection's per-session
    /// temp namespace, not the persistent catalog. Dropped on disconnect
    /// / RESET CONNECTION / DISCARD ALL.
    is_temp: bool = false,
    columns: []const ColumnDef,
    order_key: []const []const u8,
};

pub const DropTable = struct {
    table: TableRef,
    if_exists: bool,
};

/// Side-effect: bulk insert literal rows. Lives outside DdlOp because
/// INSERT isn't DDL (it mutates data, not metadata) and its execution
/// path produces an affected-row count.
pub const InsertOp = struct {
    table: TableRef,
    /// `null` means positional against the resolved table's schema; a
    /// non-null slice names the target columns in declared order.
    columns: ?[]const []const u8,
    /// `rows[i][j]` is the literal supplied for column j of row i. A
    /// null inner cell means SQL NULL. Inner-slice length must match
    /// `columns.?.len` (when named) or the table schema width
    /// (positional).
    rows: []const []const ?Value,
};

/// Introspection statement payload. SHOW ops materialize one column
/// of row names into a single Batch.
pub const ShowOp = union(enum) {
    databases,
    /// `null` schema-target → list schemas in current_db.
    schemas: ?[]const u8,
    /// All three fields fall back to the Session as needed.
    tables: TableRef,
};

/// PostgreSQL `COPY ... FROM STDIN` / `COPY ... TO STDOUT` bulk
/// transfer. Execution is wire-driven (the operator interleaves
/// CopyData frames with the client), so this op never appears
/// inside `compileWithSession` — the PG dispatcher handles it
/// directly and only sees a top-level Copy.
pub const CopyOp = struct {
    pub const Direction = enum(u8) { from_stdin = 0, to_stdout = 1 };

    direction: Direction,
    table: TableRef,
    /// Optional column list. `null` means "use every table column in
    /// schema-declared order". A non-null list reorders the source
    /// columns to the named slots; missing columns must be nullable.
    columns: ?[]const []const u8,
};

/// Window function payload. A single Window operator carries zero-or-more
/// `WindowSpec`s (one per unique PARTITION/ORDER/frame combination found
/// in the SELECT) and N `WindowCall`s, each call referencing a spec by
/// index. Spec-deduplication happens at parse / IR-build time so the
/// downstream executor can sort once per spec and run all of that spec's
/// calls in a single sweep.
pub const WindowOp = struct {
    specs: []const WindowSpec,
    calls: []const WindowCall,
    upstream: *Op,
};

pub const WindowSpec = struct {
    partition_by: []const []const u8,
    order_by: []const SortSpec,
    frame: Frame,
};

pub const Frame = struct {
    kind: FrameKind,
    start: FrameBound,
    end: FrameBound,

    pub const default_no_order: Frame = .{
        .kind = .rows,
        .start = .unbounded_preceding,
        .end = .unbounded_following,
    };

    pub const default_with_order: Frame = .{
        .kind = .range,
        .start = .unbounded_preceding,
        .end = .current_row,
    };
};

pub const FrameKind = enum(u8) { rows = 0, range = 1, groups = 2 };

pub const FrameBound = union(enum) {
    unbounded_preceding,
    /// N PRECEDING. Non-negative offset (0 is treated as CURRENT ROW
    /// per SQL standard; the parser folds that case).
    preceding: u64,
    current_row,
    following: u64,
    unbounded_following,
};

pub const WindowCall = struct {
    spec_idx: u32,
    func: WindowFunc,
    /// Function arguments. ROW_NUMBER/RANK/DENSE_RANK take none.
    /// LAG/LEAD take (expr [, offset [, default]]).
    /// FIRST_VALUE/LAST_VALUE/NTH_VALUE take (expr [, n]).
    /// SUM/AVG/COUNT/MIN/MAX take (expr).
    args: []const Expr,
    /// Honor IGNORE NULLS for null-skipping functions. False = RESPECT
    /// NULLS (the default). Operator decides whether the flag is
    /// meaningful for the function and rejects it where it isn't.
    ignore_nulls: bool,
    /// Output column name — user's AS alias, or a parser-derived label
    /// from the call form (e.g. "rank()").
    output_name: []const u8,
};

pub const WindowFunc = enum(u8) {
    // Ranking.
    row_number = 0,
    rank = 1,
    dense_rank = 2,
    // Value access.
    lag = 3,
    lead = 4,
    first_value = 5,
    last_value = 6,
    nth_value = 7,
    // Aggregates (window-context flavors).
    sum = 8,
    avg = 9,
    count = 10,
    min = 11,
    max = 12,
    // Tier 2 — reserved, parser will reject until implemented.
    ntile = 13,
    cume_dist = 14,
    percent_rank = 15,
};

/// Lookup table: window-function name → enum, or null when the name
/// isn't a window function. The set is fixed; ordering matches the
/// `WindowFunc` enum for readability.
pub fn windowFuncForName(name: []const u8) ?WindowFunc {
    const table = [_]struct { name: []const u8, func: WindowFunc }{
        .{ .name = "row_number", .func = .row_number },
        .{ .name = "rank", .func = .rank },
        .{ .name = "dense_rank", .func = .dense_rank },
        .{ .name = "lag", .func = .lag },
        .{ .name = "lead", .func = .lead },
        .{ .name = "first_value", .func = .first_value },
        .{ .name = "last_value", .func = .last_value },
        .{ .name = "nth_value", .func = .nth_value },
        .{ .name = "sum", .func = .sum },
        .{ .name = "avg", .func = .avg },
        .{ .name = "count", .func = .count },
        .{ .name = "min", .func = .min },
        .{ .name = "max", .func = .max },
        .{ .name = "ntile", .func = .ntile },
        .{ .name = "cume_dist", .func = .cume_dist },
        .{ .name = "percent_rank", .func = .percent_rank },
    };
    for (table) |e| {
        if (std.ascii.eqlIgnoreCase(name, e.name)) return e.func;
    }
    return null;
}

pub fn windowFuncName(f: WindowFunc) []const u8 {
    return switch (f) {
        .row_number => "row_number",
        .rank => "rank",
        .dense_rank => "dense_rank",
        .lag => "lag",
        .lead => "lead",
        .first_value => "first_value",
        .last_value => "last_value",
        .nth_value => "nth_value",
        .sum => "sum",
        .avg => "avg",
        .count => "count",
        .min => "min",
        .max => "max",
        .ntile => "ntile",
        .cume_dist => "cume_dist",
        .percent_rank => "percent_rank",
    };
}

pub const Error = error{
    IrBadMagic,
    IrUnsupportedVersion,
    IrTooSmall,
    IrUnknownOp,
    IrCorrupt,
};

pub const OpTag = enum(u8) {
    scan = 0,
    limit = 1,
    /// Whitelist projection: keep only these columns, in the listed order.
    select = 2,
    /// Anti-projection: drop these columns. Strict pipeline semantics —
    /// after Exclude, downstream operators cannot reference the dropped
    /// columns. (Server enforces this via the existing per-operator
    /// schema-lookup error path.)
    exclude = 3,
    /// Predicate filter. `.where` and `.filter` on the client both encode
    /// to this tag — `.where` is the SQL-flavored canonical spelling.
    filter = 4,
    /// Multi-column ORDER BY with per-key ASC/DESC.
    order_by = 5,
    /// GROUP BY (with empty group_cols, acts as a global aggregate).
    group_by = 6,
    /// Derived columns via scalar functions.
    compute = 7,
    /// Inner / outer / range / opaque join. Two upstreams.
    join = 8,
    /// Materialization barrier: drain the upstream once into a
    /// shared buffer; multiple references to this node share the
    /// buffer (one drain, many readers). Parser inserts this for
    /// CTEs marked MATERIALIZED, or auto-inserts when a CTE has
    /// refcount >= 2 and lacks NOT MATERIALIZED.
    materialize = 9,
    /// Side-effect DDL statement (CREATE/DROP DATABASE|SCHEMA, USE).
    /// Produces no rows.
    ddl = 10,
    /// Introspection (SHOW DATABASES/SCHEMAS/TABLES). Produces one
    /// string column.
    show = 11,
    /// INSERT INTO ... VALUES (...). Side-effect; produces no rows, but
    /// the execution path returns the affected-row count via the wire
    /// layer's command-complete machinery.
    insert = 12,
    /// Multi-statement bundle: an ordered list of independent statements
    /// parsed from a single `;`-separated SQL frame. Only ever appears
    /// as the root of a parse — wire layers iterate the sub-statements
    /// and compile each one independently. `compileWithSession` rejects
    /// this tag (it isn't a unified pipeline).
    batch = 13,
    /// PostgreSQL bulk COPY (FROM STDIN / TO STDOUT). Wire-driven —
    /// the PG dispatcher handles it directly; `compileWithSession`
    /// rejects it because the operator interleaves with the client.
    copy = 14,
    /// Window functions: ranking, value-access, and aggregate variants
    /// over PARTITION BY / ORDER BY / frame. One Window op carries
    /// multiple WindowSpecs (deduped) and N calls referencing those
    /// specs; the operator does one sort per spec and runs all of that
    /// spec's calls in a single sweep.
    window = 15,
    /// UNION ALL — concatenate two upstreams with compatible schemas.
    /// (UNION distinct deferred — needs a dedup pass on top of this.)
    set_union = 16,
    /// CREATE TABLE name AS SELECT ... — schema inferred from the
    /// source query's output schema; rows materialized into the new
    /// table on execute.
    create_table_as = 17,
    /// INSERT INTO name [(cols)] SELECT ... — inserts the source's
    /// rows into an existing table. Compile-time validation of the
    /// schema match against the target.
    insert_select = 18,
};

pub const BatchOp = struct {
    statements: []const *Op,
};

/// UNION ALL — concatenate the row streams from `left` and `right`.
/// Schemas must be compatible (same column count + per-position type
/// tags must match); output schema borrows the left side's names.
/// UNION (distinct) lands on top of this as a dedup pass when needed.
pub const SetUnion = struct {
    left: *Op,
    right: *Op,
    /// `true` = UNION ALL (no dedup); `false` is reserved for the
    /// future distinct variant. v1 only emits `all = true`.
    all: bool,
};

/// CREATE TABLE name AS SELECT ... — schema inferred from the
/// source query's output schema. `if_not_exists` mirrors the v1
/// DDL semantics: skip silently if the target already exists.
pub const CreateTableAs = struct {
    table: TableRef,
    if_not_exists: bool,
    is_temp: bool,
    source: *Op,
};

/// INSERT INTO name [(cols)] SELECT ... — source query feeds rows
/// into an existing table. `columns` is `null` for positional
/// (every table column must be populated by the source in declared
/// order); a non-null list narrows it to a subset.
pub const InsertSelect = struct {
    table: TableRef,
    columns: ?[]const []const u8,
    source: *Op,
};

/// In-memory operator tree, built by the client query-builder and decoded
/// by the server dispatcher. Tagged union: each variant carries an
/// optional `upstream` (null for sources like Scan).
pub const Op = union(OpTag) {
    scan: Scan,
    limit: Limit,
    select: Project,
    exclude: Project,
    filter: Filter,
    order_by: OrderBy,
    group_by: GroupBy,
    compute: Compute,
    join: Join,
    materialize: Materialize,
    ddl: DdlOp,
    show: ShowOp,
    insert: InsertOp,
    batch: BatchOp,
    copy: CopyOp,
    window: WindowOp,
    set_union: SetUnion,
    create_table_as: CreateTableAs,
    insert_select: InsertSelect,

    pub const Scan = struct {
        /// Qualified table reference. Each segment is null when the user
        /// left it implicit (resolved against the Session at compile time).
        /// Strings borrowed from the encoded buffer on decode; owned by
        /// the client on encode.
        table: TableRef,
        /// Optional FROM-clause alias (`FROM lineitem AS l1`). When
        /// set, the compile path wraps this scan in a column-renaming
        /// projection so the scan's output columns are exposed as
        /// `alias.colname` — enabling self-joins and disambiguating
        /// multi-table FROM clauses.
        alias: ?[]const u8 = null,
    };

    pub const Limit = struct {
        n: u64,
        upstream: *Op,
    };

    pub const Project = struct {
        /// Column names. Shared variant for both .select (keep) and
        /// .exclude (drop) — disambiguated by the outer Op tag.
        columns: []const []const u8,
        upstream: *Op,
    };

    pub const Filter = struct {
        /// Decoded predicate tree. Strings (column names + text values) +
        /// children arrays + `not` child pointer are all allocated into
        /// the decoder's allocator; freed via `freeDecodedPredicate`.
        predicate: PredicateExpr,
        upstream: *Op,
    };

    pub const OrderBy = struct {
        /// Sort keys + per-key ASC/DESC.
        specs: []const SortSpec,
        upstream: *Op,
    };

    pub const GroupBy = struct {
        /// Group-by column names. Empty slice = global aggregate (one
        /// output row over the whole input).
        group_cols: []const []const u8,
        /// Aggregate specs (func + col + output name).
        aggs: []const AggSpec,
        upstream: *Op,
    };

    pub const Compute = struct {
        /// Derived columns to append. Each carries an output name +
        /// an Expr tree (col_ref / lit / call).
        derived: []const Derived,
        upstream: *Op,
    };

    pub const Materialize = struct {
        /// The subtree to drain once into a shared buffer. Multiple
        /// parent pointers can reference the SAME *Op.materialize
        /// node — compile() detects that and routes them to one
        /// buffer with multiple reader cursors.
        upstream: *Op,
    };

    pub const Join = struct {
        /// Join algorithm choice. `.auto` lets compile() route via
        /// stats; explicit values pin the operator.
        algorithm: JoinAlgorithm,
        join_type: JoinType,
        /// Equi-join key pairs (left_col, right_col). Empty when the
        /// shape is pure-range or pure-opaque-predicate.
        on: []const JoinKeyPair,
        /// Inequality predicates AND'd onto the equi join.
        ranges: []const JoinRangePredicate,
        /// Optional post-join filter (Predicate over the joined schema).
        extra_predicate: ?PredicateExpr,
        skew_ratio_threshold: f32,
        skew_absolute_threshold: u32,
        skew_sample_interval: u32,
        /// Two upstreams. Both compile to executable Queries before the
        /// join operator is built.
        left: *Op,
        right: *Op,
    };

    /// Free any allocations made by `decode`. No-op for client-built trees
    /// whose strings come from caller storage.
    pub fn deinitDecoded(self: *Op, allocator: Allocator) void {
        switch (self.*) {
            .scan => {},
            .limit => |l| {
                l.upstream.deinitDecoded(allocator);
                allocator.destroy(l.upstream);
            },
            .select => |p| freeProject(p, allocator),
            .exclude => |p| freeProject(p, allocator),
            .filter => |f| {
                freeDecodedPredicate(f.predicate, allocator);
                f.upstream.deinitDecoded(allocator);
                allocator.destroy(f.upstream);
            },
            .order_by => |o| {
                allocator.free(o.specs);
                o.upstream.deinitDecoded(allocator);
                allocator.destroy(o.upstream);
            },
            .group_by => |g| {
                allocator.free(g.group_cols);
                allocator.free(g.aggs);
                g.upstream.deinitDecoded(allocator);
                allocator.destroy(g.upstream);
            },
            .compute => |c| {
                for (c.derived) |d| freeDecodedExpr(d.expr, allocator);
                allocator.free(c.derived);
                c.upstream.deinitDecoded(allocator);
                allocator.destroy(c.upstream);
            },
            .join => |j| {
                if (j.on.len > 0) allocator.free(j.on);
                if (j.ranges.len > 0) allocator.free(j.ranges);
                if (j.extra_predicate) |pred| freeDecodedPredicate(pred, allocator);
                j.left.deinitDecoded(allocator);
                allocator.destroy(j.left);
                j.right.deinitDecoded(allocator);
                allocator.destroy(j.right);
            },
            .materialize => |m| {
                m.upstream.deinitDecoded(allocator);
                allocator.destroy(m.upstream);
            },
            .ddl => |d| freeDecodedDdl(d, allocator),
            .show => {},
            .insert => |i| {
                if (i.columns) |cols| allocator.free(cols);
                for (i.rows) |row| allocator.free(row);
                allocator.free(i.rows);
            },
            .batch => |b| {
                for (b.statements) |sub| {
                    sub.deinitDecoded(allocator);
                    allocator.destroy(sub);
                }
                allocator.free(b.statements);
            },
            .copy => |c| {
                if (c.columns) |cols| allocator.free(cols);
            },
            .window => |w| {
                for (w.specs) |sp| {
                    allocator.free(sp.partition_by);
                    allocator.free(sp.order_by);
                }
                allocator.free(w.specs);
                for (w.calls) |c| {
                    for (c.args) |a| freeDecodedExpr(a, allocator);
                    allocator.free(c.args);
                }
                allocator.free(w.calls);
                w.upstream.deinitDecoded(allocator);
                allocator.destroy(w.upstream);
            },
            .set_union => |u| {
                u.left.deinitDecoded(allocator);
                allocator.destroy(u.left);
                u.right.deinitDecoded(allocator);
                allocator.destroy(u.right);
            },
            .create_table_as => |c| {
                c.source.deinitDecoded(allocator);
                allocator.destroy(c.source);
            },
            .insert_select => |i| {
                if (i.columns) |cols| allocator.free(cols);
                i.source.deinitDecoded(allocator);
                allocator.destroy(i.source);
            },
        }
    }
};

fn freeDecodedDdl(d: DdlOp, allocator: Allocator) void {
    switch (d) {
        .create_table => |ct| {
            allocator.free(ct.columns);
            allocator.free(ct.order_key);
        },
        else => {},
    }
}

fn freeProject(p: Op.Project, allocator: Allocator) void {
    // p.columns is a freshly-allocated slice of slices; the individual
    // string slices are borrowed from the encoded buffer (not owned).
    // Only free the outer slice.
    allocator.free(p.columns);
    p.upstream.deinitDecoded(allocator);
    allocator.destroy(p.upstream);
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

/// Serialize `root` into `out` (header + tree). Caller owns `out`.
pub fn encode(allocator: Allocator, out: *std.ArrayList(u8), root: Op) !void {
    try out.appendSlice(allocator, &magic);
    try appendU16(allocator, out, version);
    try appendU16(allocator, out, 0); // flags
    try encodeOp(allocator, out, root);
}

const EncodeError = Allocator.Error;

fn encodeOp(allocator: Allocator, out: *std.ArrayList(u8), op: Op) EncodeError!void {
    try out.append(allocator, @intFromEnum(@as(OpTag, op)));
    switch (op) {
        .scan => |s| {
            try encodeTableRef(allocator, out, s.table);
            try encodeOptString(allocator, out, s.alias);
        },
        .limit => |l| {
            try appendU64(allocator, out, l.n);
            try encodeOp(allocator, out, l.upstream.*);
        },
        .select => |p| try encodeProject(allocator, out, p),
        .exclude => |p| try encodeProject(allocator, out, p),
        .filter => |f| try encodeFilter(allocator, out, f),
        .order_by => |o| try encodeOrderBy(allocator, out, o),
        .group_by => |g| try encodeGroupBy(allocator, out, g),
        .compute => |c| try encodeCompute(allocator, out, c),
        .join => |j| try encodeJoin(allocator, out, j),
        .materialize => |m| try encodeOp(allocator, out, m.upstream.*),
        .ddl => |d| try encodeDdl(allocator, out, d),
        .show => |s| try encodeShow(allocator, out, s),
        .insert => |i| try encodeInsert(allocator, out, i),
        .batch => |b| {
            try appendU32(allocator, out, @intCast(b.statements.len));
            for (b.statements) |sub| try encodeOp(allocator, out, sub.*);
        },
        .copy => |c| try encodeCopy(allocator, out, c),
        .window => |w| try encodeWindow(allocator, out, w),
        .set_union => |u| {
            try out.append(allocator, if (u.all) @as(u8, 1) else 0);
            try encodeOp(allocator, out, u.left.*);
            try encodeOp(allocator, out, u.right.*);
        },
        .create_table_as => |c| {
            try encodeTableRef(allocator, out, c.table);
            try out.append(allocator, if (c.if_not_exists) @as(u8, 1) else 0);
            try out.append(allocator, if (c.is_temp) @as(u8, 1) else 0);
            try encodeOp(allocator, out, c.source.*);
        },
        .insert_select => |i| {
            try encodeTableRef(allocator, out, i.table);
            if (i.columns) |cols| {
                try out.append(allocator, 1);
                try appendU32(allocator, out, @intCast(cols.len));
                for (cols) |c| {
                    try appendU32(allocator, out, @intCast(c.len));
                    try out.appendSlice(allocator, c);
                }
            } else {
                try out.append(allocator, 0);
            }
            try encodeOp(allocator, out, i.source.*);
        },
    }
}

fn encodeWindow(allocator: Allocator, out: *std.ArrayList(u8), w: WindowOp) EncodeError!void {
    try appendU32(allocator, out, @intCast(w.specs.len));
    for (w.specs) |sp| {
        try appendU32(allocator, out, @intCast(sp.partition_by.len));
        for (sp.partition_by) |c| {
            try appendU32(allocator, out, @intCast(c.len));
            try out.appendSlice(allocator, c);
        }
        try appendU32(allocator, out, @intCast(sp.order_by.len));
        for (sp.order_by) |s| {
            try appendU32(allocator, out, @intCast(s.col.len));
            try out.appendSlice(allocator, s.col);
            try out.append(allocator, @intFromBool(s.desc));
        }
        try out.append(allocator, @intFromEnum(sp.frame.kind));
        try encodeFrameBound(allocator, out, sp.frame.start);
        try encodeFrameBound(allocator, out, sp.frame.end);
    }
    try appendU32(allocator, out, @intCast(w.calls.len));
    for (w.calls) |c| {
        try appendU32(allocator, out, c.spec_idx);
        try out.append(allocator, @intFromEnum(c.func));
        try out.append(allocator, @intFromBool(c.ignore_nulls));
        try appendU32(allocator, out, @intCast(c.output_name.len));
        try out.appendSlice(allocator, c.output_name);
        try appendU32(allocator, out, @intCast(c.args.len));
        for (c.args) |a| try encodeExpr(allocator, out, a);
    }
    try encodeOp(allocator, out, w.upstream.*);
}

fn encodeFrameBound(allocator: Allocator, out: *std.ArrayList(u8), b: FrameBound) EncodeError!void {
    switch (b) {
        .unbounded_preceding => try out.append(allocator, 0),
        .preceding => |n| {
            try out.append(allocator, 1);
            try appendU64(allocator, out, n);
        },
        .current_row => try out.append(allocator, 2),
        .following => |n| {
            try out.append(allocator, 3);
            try appendU64(allocator, out, n);
        },
        .unbounded_following => try out.append(allocator, 4),
    }
}

fn encodeCopy(allocator: Allocator, out: *std.ArrayList(u8), c: CopyOp) EncodeError!void {
    try out.append(allocator, @intFromEnum(c.direction));
    try encodeTableRef(allocator, out, c.table);
    if (c.columns) |cols| {
        try out.append(allocator, 1);
        try appendU32(allocator, out, @intCast(cols.len));
        for (cols) |name| {
            try appendU32(allocator, out, @intCast(name.len));
            try out.appendSlice(allocator, name);
        }
    } else {
        try out.append(allocator, 0);
    }
}

fn encodeTableRef(allocator: Allocator, out: *std.ArrayList(u8), ref: TableRef) EncodeError!void {
    try encodeOptString(allocator, out, ref.database);
    try encodeOptString(allocator, out, ref.schema);
    try appendU32(allocator, out, @intCast(ref.name.len));
    try out.appendSlice(allocator, ref.name);
}

fn encodeOptString(allocator: Allocator, out: *std.ArrayList(u8), s: ?[]const u8) EncodeError!void {
    if (s) |v| {
        try out.append(allocator, 1);
        try appendU32(allocator, out, @intCast(v.len));
        try out.appendSlice(allocator, v);
    } else {
        try out.append(allocator, 0);
    }
}

const DdlTag = enum(u8) {
    create_database = 0,
    drop_database = 1,
    create_schema = 2,
    drop_schema = 3,
    use_schema = 4,
    use_database_schema = 5,
    create_table = 6,
    drop_table = 7,
};

const TypeWireTag = enum(u8) {
    int = 0,
    bigint = 1,
    boolean = 2,
    varchar = 3,
    string = 4,
    float = 5,
    double = 6,
    date = 7,
    datetime = 8,
    tinyint = 9,
    smallint = 10,
    largeint = 11,
    char = 12,
    decimal64 = 13,
    decimal128 = 14,
    uuid = 15,
};

fn encodeType(allocator: Allocator, out: *std.ArrayList(u8), t: types.Type) EncodeError!void {
    switch (t) {
        .int => try out.append(allocator, @intFromEnum(TypeWireTag.int)),
        .bigint => try out.append(allocator, @intFromEnum(TypeWireTag.bigint)),
        .boolean => try out.append(allocator, @intFromEnum(TypeWireTag.boolean)),
        .varchar => |n| {
            try out.append(allocator, @intFromEnum(TypeWireTag.varchar));
            try appendU32(allocator, out, n);
        },
        .string => try out.append(allocator, @intFromEnum(TypeWireTag.string)),
        .float => try out.append(allocator, @intFromEnum(TypeWireTag.float)),
        .double => try out.append(allocator, @intFromEnum(TypeWireTag.double)),
        .date => try out.append(allocator, @intFromEnum(TypeWireTag.date)),
        .datetime => try out.append(allocator, @intFromEnum(TypeWireTag.datetime)),
        .tinyint => try out.append(allocator, @intFromEnum(TypeWireTag.tinyint)),
        .smallint => try out.append(allocator, @intFromEnum(TypeWireTag.smallint)),
        .largeint => try out.append(allocator, @intFromEnum(TypeWireTag.largeint)),
        .char => |n| {
            try out.append(allocator, @intFromEnum(TypeWireTag.char));
            try appendU32(allocator, out, n);
        },
        .decimal64 => |spec| {
            try out.append(allocator, @intFromEnum(TypeWireTag.decimal64));
            try out.append(allocator, spec.p);
            try out.append(allocator, spec.s);
        },
        .decimal128 => |spec| {
            try out.append(allocator, @intFromEnum(TypeWireTag.decimal128));
            try out.append(allocator, spec.p);
            try out.append(allocator, spec.s);
        },
        .uuid => try out.append(allocator, @intFromEnum(TypeWireTag.uuid)),
    }
}

fn decodeType(bytes: []const u8, cursor: *usize) DecodeError!types.Type {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const t = bytes[cursor.*];
    cursor.* += 1;
    if (t > @intFromEnum(TypeWireTag.uuid)) return Error.IrCorrupt;
    const tag: TypeWireTag = @enumFromInt(t);
    return switch (tag) {
        .int => .int,
        .bigint => .bigint,
        .boolean => .boolean,
        .varchar => blk: {
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            break :blk types.Type{ .varchar = n };
        },
        .string => .string,
        .float => .float,
        .double => .double,
        .date => .date,
        .datetime => .datetime,
        .tinyint => .tinyint,
        .smallint => .smallint,
        .largeint => .largeint,
        .char => blk: {
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            break :blk types.Type{ .char = n };
        },
        .decimal64 => blk: {
            if (cursor.* + 2 > bytes.len) return Error.IrCorrupt;
            const p = bytes[cursor.*];
            const s = bytes[cursor.* + 1];
            cursor.* += 2;
            break :blk types.Type{ .decimal64 = .{ .p = p, .s = s } };
        },
        .decimal128 => blk: {
            if (cursor.* + 2 > bytes.len) return Error.IrCorrupt;
            const p = bytes[cursor.*];
            const s = bytes[cursor.* + 1];
            cursor.* += 2;
            break :blk types.Type{ .decimal128 = .{ .p = p, .s = s } };
        },
        .uuid => .uuid,
    };
}

fn encodeDdl(allocator: Allocator, out: *std.ArrayList(u8), d: DdlOp) EncodeError!void {
    switch (d) {
        .create_database => |n| {
            try out.append(allocator, @intFromEnum(DdlTag.create_database));
            try appendU32(allocator, out, @intCast(n.len));
            try out.appendSlice(allocator, n);
        },
        .drop_database => |n| {
            try out.append(allocator, @intFromEnum(DdlTag.drop_database));
            try appendU32(allocator, out, @intCast(n.len));
            try out.appendSlice(allocator, n);
        },
        .create_schema => |n| {
            try out.append(allocator, @intFromEnum(DdlTag.create_schema));
            try appendU32(allocator, out, @intCast(n.len));
            try out.appendSlice(allocator, n);
        },
        .drop_schema => |n| {
            try out.append(allocator, @intFromEnum(DdlTag.drop_schema));
            try appendU32(allocator, out, @intCast(n.len));
            try out.appendSlice(allocator, n);
        },
        .use_schema => |n| {
            try out.append(allocator, @intFromEnum(DdlTag.use_schema));
            try appendU32(allocator, out, @intCast(n.len));
            try out.appendSlice(allocator, n);
        },
        .use_database_schema => |p| {
            try out.append(allocator, @intFromEnum(DdlTag.use_database_schema));
            try appendU32(allocator, out, @intCast(p.database.len));
            try out.appendSlice(allocator, p.database);
            try appendU32(allocator, out, @intCast(p.schema.len));
            try out.appendSlice(allocator, p.schema);
        },
        .create_table => |ct| {
            try out.append(allocator, @intFromEnum(DdlTag.create_table));
            try encodeTableRef(allocator, out, ct.table);
            try out.append(allocator, @intFromBool(ct.if_not_exists));
            try out.append(allocator, @intFromBool(ct.is_temp));
            try appendU32(allocator, out, @intCast(ct.columns.len));
            for (ct.columns) |c| {
                try appendU32(allocator, out, @intCast(c.name.len));
                try out.appendSlice(allocator, c.name);
                try encodeType(allocator, out, c.column_type);
                try out.append(allocator, @intFromBool(c.nullable));
            }
            try appendU32(allocator, out, @intCast(ct.order_key.len));
            for (ct.order_key) |k| {
                try appendU32(allocator, out, @intCast(k.len));
                try out.appendSlice(allocator, k);
            }
        },
        .drop_table => |dt| {
            try out.append(allocator, @intFromEnum(DdlTag.drop_table));
            try encodeTableRef(allocator, out, dt.table);
            try out.append(allocator, @intFromBool(dt.if_exists));
        },
    }
}

fn encodeInsert(allocator: Allocator, out: *std.ArrayList(u8), i: InsertOp) EncodeError!void {
    try encodeTableRef(allocator, out, i.table);
    if (i.columns) |cols| {
        try out.append(allocator, 1);
        try appendU32(allocator, out, @intCast(cols.len));
        for (cols) |c| {
            try appendU32(allocator, out, @intCast(c.len));
            try out.appendSlice(allocator, c);
        }
    } else {
        try out.append(allocator, 0);
    }
    try appendU32(allocator, out, @intCast(i.rows.len));
    for (i.rows) |row| {
        try appendU32(allocator, out, @intCast(row.len));
        for (row) |maybe_v| {
            if (maybe_v) |v| {
                try out.append(allocator, 1);
                try encodeValue(allocator, out, v);
            } else {
                try out.append(allocator, 0);
            }
        }
    }
}

const ShowTag = enum(u8) {
    databases = 0,
    schemas = 1,
    tables = 2,
};

fn encodeShow(allocator: Allocator, out: *std.ArrayList(u8), s: ShowOp) EncodeError!void {
    switch (s) {
        .databases => try out.append(allocator, @intFromEnum(ShowTag.databases)),
        .schemas => |db| {
            try out.append(allocator, @intFromEnum(ShowTag.schemas));
            try encodeOptString(allocator, out, db);
        },
        .tables => |ref| {
            try out.append(allocator, @intFromEnum(ShowTag.tables));
            try encodeTableRef(allocator, out, ref);
        },
    }
}

fn encodeOrderBy(allocator: Allocator, out: *std.ArrayList(u8), o: Op.OrderBy) EncodeError!void {
    try appendU32(allocator, out, @intCast(o.specs.len));
    for (o.specs) |s| {
        try appendU32(allocator, out, @intCast(s.col.len));
        try out.appendSlice(allocator, s.col);
        try out.append(allocator, @intFromBool(s.desc));
    }
    try encodeOp(allocator, out, o.upstream.*);
}

fn encodeGroupBy(allocator: Allocator, out: *std.ArrayList(u8), g: Op.GroupBy) EncodeError!void {
    try appendU32(allocator, out, @intCast(g.group_cols.len));
    for (g.group_cols) |c| {
        try appendU32(allocator, out, @intCast(c.len));
        try out.appendSlice(allocator, c);
    }
    try appendU32(allocator, out, @intCast(g.aggs.len));
    for (g.aggs) |a| {
        // func (u8)
        try out.append(allocator, @intFromEnum(a.func));
        // optional column: 0 = null (COUNT(*)), 1 = present
        if (a.col) |c| {
            try out.append(allocator, 1);
            try appendU32(allocator, out, @intCast(c.len));
            try out.appendSlice(allocator, c);
        } else {
            try out.append(allocator, 0);
        }
        // alias (always present — server enforces this on the existing API)
        try appendU32(allocator, out, @intCast(a.as.len));
        try out.appendSlice(allocator, a.as);
        // params tag (u8) + payload (matches decoder)
        switch (a.params) {
            .none => try out.append(allocator, 0),
            .percentile => |p| {
                try out.append(allocator, 1);
                const bits: u64 = @bitCast(p);
                var b: [8]u8 = undefined;
                std.mem.writeInt(u64, &b, bits, .little);
                try out.appendSlice(allocator, &b);
            },
            .separator => |sep| {
                try out.append(allocator, 2);
                try appendU32(allocator, out, @intCast(sep.len));
                try out.appendSlice(allocator, sep);
            },
        }
    }
    try encodeOp(allocator, out, g.upstream.*);
}

fn encodeProject(allocator: Allocator, out: *std.ArrayList(u8), p: Op.Project) EncodeError!void {
    try appendU32(allocator, out, @intCast(p.columns.len));
    for (p.columns) |c| {
        try appendU32(allocator, out, @intCast(c.len));
        try out.appendSlice(allocator, c);
    }
    try encodeOp(allocator, out, p.upstream.*);
}

fn encodeFilter(allocator: Allocator, out: *std.ArrayList(u8), f: Op.Filter) EncodeError!void {
    try encodePredicate(allocator, out, f.predicate);
    try encodeOp(allocator, out, f.upstream.*);
}

fn encodeCompute(allocator: Allocator, out: *std.ArrayList(u8), c: Op.Compute) EncodeError!void {
    try appendU32(allocator, out, @intCast(c.derived.len));
    for (c.derived) |d| {
        try appendU32(allocator, out, @intCast(d.name.len));
        try out.appendSlice(allocator, d.name);
        try encodeExpr(allocator, out, d.expr);
    }
    try encodeOp(allocator, out, c.upstream.*);
}

fn encodeJoin(allocator: Allocator, out: *std.ArrayList(u8), j: Op.Join) EncodeError!void {
    try out.append(allocator, @intFromEnum(j.algorithm));
    try out.append(allocator, @intFromEnum(j.join_type));
    // ON pairs
    try appendU32(allocator, out, @intCast(j.on.len));
    for (j.on) |kp| {
        try appendU32(allocator, out, @intCast(kp.left.len));
        try out.appendSlice(allocator, kp.left);
        try appendU32(allocator, out, @intCast(kp.right.len));
        try out.appendSlice(allocator, kp.right);
    }
    // Ranges
    try appendU32(allocator, out, @intCast(j.ranges.len));
    for (j.ranges) |rg| {
        try appendU32(allocator, out, @intCast(rg.left.len));
        try out.appendSlice(allocator, rg.left);
        try appendU32(allocator, out, @intCast(rg.right.len));
        try out.appendSlice(allocator, rg.right);
        try out.append(allocator, @intFromEnum(rg.op));
    }
    // Optional extra predicate (post-join filter)
    if (j.extra_predicate) |pred| {
        try out.append(allocator, 1);
        try encodePredicate(allocator, out, pred);
    } else {
        try out.append(allocator, 0);
    }
    // Skew detection knobs (f32 ratio + u32 absolute + u32 interval)
    {
        const bits: u32 = @bitCast(j.skew_ratio_threshold);
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, bits, .little);
        try out.appendSlice(allocator, &b);
    }
    try appendU32(allocator, out, j.skew_absolute_threshold);
    try appendU32(allocator, out, j.skew_sample_interval);
    // Two upstreams
    try encodeOp(allocator, out, j.left.*);
    try encodeOp(allocator, out, j.right.*);
}

const ExprTag = enum(u8) { col_ref = 0, lit = 1, call = 2, case = 3 };

/// Wire-encode an Expr tree (col_ref / lit / call / case). Recursive
/// — mirrors the recursive Predicate encoding above.
pub fn encodeExpr(allocator: Allocator, out: *std.ArrayList(u8), e: Expr) EncodeError!void {
    switch (e) {
        .col_ref => |name| {
            try out.append(allocator, @intFromEnum(ExprTag.col_ref));
            try appendU32(allocator, out, @intCast(name.len));
            try out.appendSlice(allocator, name);
        },
        .lit => |v| {
            try out.append(allocator, @intFromEnum(ExprTag.lit));
            try encodeValue(allocator, out, v);
        },
        .call => |c| {
            try out.append(allocator, @intFromEnum(ExprTag.call));
            try appendU32(allocator, out, @intCast(c.fn_name.len));
            try out.appendSlice(allocator, c.fn_name);
            try appendU32(allocator, out, @intCast(c.args.len));
            for (c.args) |child| try encodeExpr(allocator, out, child);
        },
        // Subqueries are parse-time only — they get resolved into
        // literals before any wire round-trip would happen. Surface
        // loudly if someone tries to encode an unresolved tree.
        .scalar_subquery, .exists_subquery => return EncodeError.OutOfMemory,
        .case => |cs| {
            try out.append(allocator, @intFromEnum(ExprTag.case));
            try appendU32(allocator, out, @intCast(cs.branches.len));
            for (cs.branches) |br| {
                try encodePredicate(allocator, out, br.cond);
                try encodeExpr(allocator, out, br.then);
            }
            try out.append(allocator, if (cs.else_branch != null) @as(u8, 1) else 0);
            if (cs.else_branch) |eb| try encodeExpr(allocator, out, eb.*);
        },
    }
}

// ---------------------------------------------------------------------------
// Predicate encoding — mirrors exec.predicate.PredicateExpr shape.
//
//   Tag (u8):
//     0 = leaf            [op u8][col_len u32][col bytes][value]
//     1 = is_null         [col_len u32][col bytes]
//     2 = is_not_null     [col_len u32][col bytes]
//     3 = and             [n u32][child0][child1]...
//     4 = or              [n u32][child0][child1]...
//     5 = not             [child]
//
//   Value (after value_tag u8): per-type bytes.
//     int       i32 LE       (4)
//     bigint    i64 LE       (8)
//     boolean   u8           (1)
//     text      u32 len + bytes
//     float     f32 LE       (4)
//     double    f64 LE       (8)
//     date      i32 LE       (4)
//     datetime  i64 LE       (8)
//     tinyint   i8           (1)
//     smallint  i16 LE       (2)
//     largeint  i128 LE      (16)
//     decimal64 i64 LE       (8)
//     decimal128 i128 LE     (16)
// ---------------------------------------------------------------------------

const PredTag = enum(u8) {
    leaf = 0,
    is_null = 1,
    is_not_null = 2,
    p_and = 3,
    p_or = 4,
    p_not = 5,
    like = 6,
    leaf_col_col = 7,
};

pub fn encodePredicate(allocator: Allocator, out: *std.ArrayList(u8), expr: PredicateExpr) EncodeError!void {
    switch (expr) {
        .leaf => |p| {
            try out.append(allocator, @intFromEnum(PredTag.leaf));
            try out.append(allocator, @intFromEnum(p.op));
            try appendU32(allocator, out, @intCast(p.col.len));
            try out.appendSlice(allocator, p.col);
            try encodeValue(allocator, out, p.val);
        },
        .is_null => |col| {
            try out.append(allocator, @intFromEnum(PredTag.is_null));
            try appendU32(allocator, out, @intCast(col.len));
            try out.appendSlice(allocator, col);
        },
        .is_not_null => |col| {
            try out.append(allocator, @intFromEnum(PredTag.is_not_null));
            try appendU32(allocator, out, @intCast(col.len));
            try out.appendSlice(allocator, col);
        },
        .like => |lp| {
            try out.append(allocator, @intFromEnum(PredTag.like));
            try appendU32(allocator, out, @intCast(lp.col.len));
            try out.appendSlice(allocator, lp.col);
            try appendU32(allocator, out, @intCast(lp.pattern.len));
            try out.appendSlice(allocator, lp.pattern);
        },
        .leaf_col_col => |lc| {
            try out.append(allocator, @intFromEnum(PredTag.leaf_col_col));
            try out.append(allocator, @intFromEnum(lc.op));
            try appendU32(allocator, out, @intCast(lc.left.len));
            try out.appendSlice(allocator, lc.left);
            try appendU32(allocator, out, @intCast(lc.right.len));
            try out.appendSlice(allocator, lc.right);
        },
        // Subqueries are parse-time only — resolved before any wire
        // round-trip. Surface loudly if we hit an unresolved one.
        // `.always` / `.in_set` / `.correlated_set` are post-resolution
        // forms that also shouldn't appear in wire IR (callers re-emit
        // via SQL).
        .scalar_subquery, .exists_subquery, .in_subquery, .always, .in_set, .correlated_set, .correlated_scalar, .correlated_range => return EncodeError.OutOfMemory,
        .@"and" => |children| {
            try out.append(allocator, @intFromEnum(PredTag.p_and));
            try appendU32(allocator, out, @intCast(children.len));
            for (children) |c| try encodePredicate(allocator, out, c);
        },
        .@"or" => |children| {
            try out.append(allocator, @intFromEnum(PredTag.p_or));
            try appendU32(allocator, out, @intCast(children.len));
            for (children) |c| try encodePredicate(allocator, out, c);
        },
        .not => |child| {
            try out.append(allocator, @intFromEnum(PredTag.p_not));
            try encodePredicate(allocator, out, child.*);
        },
    }
}

pub fn encodeValue(allocator: Allocator, out: *std.ArrayList(u8), v: Value) EncodeError!void {
    try out.append(allocator, @intFromEnum(@as(ValueTag, v)));
    var b: [16]u8 = undefined;
    switch (v) {
        .int => |x| {
            std.mem.writeInt(i32, b[0..4], x, .little);
            try out.appendSlice(allocator, b[0..4]);
        },
        .bigint => |x| {
            std.mem.writeInt(i64, b[0..8], x, .little);
            try out.appendSlice(allocator, b[0..8]);
        },
        .boolean => |x| try out.append(allocator, @intFromBool(x)),
        .text => |s| {
            try appendU32(allocator, out, @intCast(s.len));
            try out.appendSlice(allocator, s);
        },
        .float => |x| {
            std.mem.writeInt(u32, b[0..4], @bitCast(x), .little);
            try out.appendSlice(allocator, b[0..4]);
        },
        .double => |x| {
            std.mem.writeInt(u64, b[0..8], @bitCast(x), .little);
            try out.appendSlice(allocator, b[0..8]);
        },
        .date => |x| {
            std.mem.writeInt(i32, b[0..4], x, .little);
            try out.appendSlice(allocator, b[0..4]);
        },
        .datetime => |x| {
            std.mem.writeInt(i64, b[0..8], x, .little);
            try out.appendSlice(allocator, b[0..8]);
        },
        .tinyint => |x| try out.append(allocator, @bitCast(x)),
        .smallint => |x| {
            std.mem.writeInt(i16, b[0..2], x, .little);
            try out.appendSlice(allocator, b[0..2]);
        },
        .largeint => |x| {
            std.mem.writeInt(i128, b[0..16], x, .little);
            try out.appendSlice(allocator, &b);
        },
        .decimal64 => |x| {
            std.mem.writeInt(i64, b[0..8], x, .little);
            try out.appendSlice(allocator, b[0..8]);
        },
        .decimal128 => |x| {
            std.mem.writeInt(i128, b[0..16], x, .little);
            try out.appendSlice(allocator, &b);
        },
        .uuid => |x| {
            std.mem.writeInt(u128, b[0..16], x, .little);
            try out.appendSlice(allocator, &b);
        },
    }
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// Parse `bytes` into an Op tree. Returns the root op; caller calls
/// `root.deinitDecoded(allocator)` when done. String fields (table_name)
/// are borrowed slices into `bytes` — keep it alive until the tree is
/// no longer needed.
pub fn decode(allocator: Allocator, bytes: []const u8) !Op {
    if (bytes.len < header_size) return Error.IrTooSmall;
    if (!std.mem.eql(u8, bytes[0..4], &magic)) return Error.IrBadMagic;
    const ver = readU16(bytes[4..6]);
    if (ver != version) return Error.IrUnsupportedVersion;
    var cursor: usize = header_size;
    return try decodeOp(allocator, bytes, &cursor);
}

const DecodeError = Error || Allocator.Error;

fn decodeOp(allocator: Allocator, bytes: []const u8, cursor: *usize) DecodeError!Op {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const tag_byte = bytes[cursor.*];
    cursor.* += 1;
    if (tag_byte > @intFromEnum(OpTag.insert_select)) return Error.IrUnknownOp;
    const tag: OpTag = @enumFromInt(tag_byte);

    return switch (tag) {
        .scan => blk: {
            const ref = try decodeTableRef(bytes, cursor);
            const alias = try decodeOptString(bytes, cursor);
            break :blk Op{ .scan = .{ .table = ref, .alias = alias } };
        },
        .limit => blk: {
            if (cursor.* + 8 > bytes.len) return Error.IrCorrupt;
            const n = readU64(bytes[cursor.* .. cursor.* + 8]);
            cursor.* += 8;
            const upstream = try allocator.create(Op);
            errdefer allocator.destroy(upstream);
            upstream.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .limit = .{ .n = n, .upstream = upstream } };
        },
        .select, .exclude => blk: {
            const project = try decodeProject(allocator, bytes, cursor);
            break :blk if (tag == .select) Op{ .select = project } else Op{ .exclude = project };
        },
        .filter => blk: {
            const pred = try decodePredicate(allocator, bytes, cursor);
            errdefer freeDecodedPredicate(pred, allocator);
            const upstream = try allocator.create(Op);
            errdefer allocator.destroy(upstream);
            upstream.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .filter = .{ .predicate = pred, .upstream = upstream } };
        },
        .order_by => blk: {
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const specs = try allocator.alloc(SortSpec, n);
            errdefer allocator.free(specs);
            for (specs) |*s| {
                const col = try readString(bytes, cursor);
                if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
                const desc = bytes[cursor.*] != 0;
                cursor.* += 1;
                s.* = .{ .col = col, .desc = desc };
            }
            const upstream = try allocator.create(Op);
            errdefer allocator.destroy(upstream);
            upstream.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .order_by = .{ .specs = specs, .upstream = upstream } };
        },
        .group_by => blk: {
            // group_cols
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n_groups = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const group_cols = try allocator.alloc([]const u8, n_groups);
            errdefer allocator.free(group_cols);
            for (group_cols) |*c| c.* = try readString(bytes, cursor);

            // aggs
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n_aggs = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const aggs = try allocator.alloc(AggSpec, n_aggs);
            errdefer allocator.free(aggs);
            for (aggs) |*a| {
                if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
                const func_byte = bytes[cursor.*];
                cursor.* += 1;
                if (func_byte > @intFromEnum(AggFunc.group_concat)) return Error.IrCorrupt;
                const func: AggFunc = @enumFromInt(func_byte);
                if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
                const has_col = bytes[cursor.*];
                cursor.* += 1;
                const col: ?[]const u8 = if (has_col != 0) try readString(bytes, cursor) else null;
                const as = try readString(bytes, cursor);
                // AggParams tag: 0=none, 1=percentile (f64 payload),
                // 2=separator (string payload). Older encoders omit
                // it; we treat absence as .none for back-compat with
                // pre-params IR.
                if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
                const params_tag = bytes[cursor.*];
                cursor.* += 1;
                const params: exec_aggregate.AggParams = switch (params_tag) {
                    0 => .none,
                    1 => blk2: {
                        if (cursor.* + 8 > bytes.len) return Error.IrCorrupt;
                        const bits = std.mem.readInt(u64, bytes[cursor.*..][0..8], .little);
                        cursor.* += 8;
                        break :blk2 .{ .percentile = @as(f64, @bitCast(bits)) };
                    },
                    2 => .{ .separator = try readString(bytes, cursor) },
                    else => return Error.IrCorrupt,
                };
                a.* = .{ .func = func, .col = col, .as = as, .params = params };
            }

            const upstream = try allocator.create(Op);
            errdefer allocator.destroy(upstream);
            upstream.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .group_by = .{
                .group_cols = group_cols,
                .aggs = aggs,
                .upstream = upstream,
            } };
        },
        .compute => blk: {
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const derived = try allocator.alloc(Derived, n);
            errdefer allocator.free(derived);
            for (derived) |*d| {
                const name = try readString(bytes, cursor);
                const expr = try decodeExpr(allocator, bytes, cursor);
                d.* = .{ .name = name, .expr = expr };
            }
            const upstream = try allocator.create(Op);
            errdefer allocator.destroy(upstream);
            upstream.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .compute = .{ .derived = derived, .upstream = upstream } };
        },
        .join => blk: {
            if (cursor.* + 2 > bytes.len) return Error.IrCorrupt;
            const algo_byte = bytes[cursor.*];
            const jtype_byte = bytes[cursor.* + 1];
            cursor.* += 2;
            if (algo_byte > @intFromEnum(JoinAlgorithm.range_sweep)) return Error.IrCorrupt;
            if (jtype_byte > @intFromEnum(JoinType.full)) return Error.IrCorrupt;
            const algo: JoinAlgorithm = @enumFromInt(algo_byte);
            const jtype: JoinType = @enumFromInt(jtype_byte);

            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n_on = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const on = if (n_on == 0)
                @as([]JoinKeyPair, &.{})
            else
                try allocator.alloc(JoinKeyPair, n_on);
            errdefer if (n_on > 0) allocator.free(on);
            for (on) |*kp| {
                const l = try readString(bytes, cursor);
                const r = try readString(bytes, cursor);
                kp.* = .{ .left = l, .right = r };
            }

            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n_ranges = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const ranges = if (n_ranges == 0)
                @as([]JoinRangePredicate, &.{})
            else
                try allocator.alloc(JoinRangePredicate, n_ranges);
            errdefer if (n_ranges > 0) allocator.free(ranges);
            for (ranges) |*rg| {
                const l = try readString(bytes, cursor);
                const r = try readString(bytes, cursor);
                if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
                const op_byte = bytes[cursor.*];
                cursor.* += 1;
                if (op_byte > @intFromEnum(PredicateOp.gte)) return Error.IrCorrupt;
                rg.* = .{ .left = l, .right = r, .op = @enumFromInt(op_byte) };
            }

            if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
            const has_pred = bytes[cursor.*];
            cursor.* += 1;
            const extra_predicate: ?PredicateExpr = if (has_pred != 0)
                try decodePredicate(allocator, bytes, cursor)
            else
                null;
            errdefer if (extra_predicate) |p| freeDecodedPredicate(p, allocator);

            if (cursor.* + 12 > bytes.len) return Error.IrCorrupt;
            const ratio_bits = std.mem.readInt(u32, bytes[cursor.*..][0..4], .little);
            const skew_ratio: f32 = @bitCast(ratio_bits);
            cursor.* += 4;
            const skew_abs = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const skew_interval = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;

            const left_up = try allocator.create(Op);
            errdefer allocator.destroy(left_up);
            left_up.* = try decodeOp(allocator, bytes, cursor);
            errdefer left_up.deinitDecoded(allocator);
            const right_up = try allocator.create(Op);
            errdefer allocator.destroy(right_up);
            right_up.* = try decodeOp(allocator, bytes, cursor);

            break :blk Op{ .join = .{
                .algorithm = algo,
                .join_type = jtype,
                .on = on,
                .ranges = ranges,
                .extra_predicate = extra_predicate,
                .skew_ratio_threshold = skew_ratio,
                .skew_absolute_threshold = skew_abs,
                .skew_sample_interval = skew_interval,
                .left = left_up,
                .right = right_up,
            } };
        },
        .materialize => blk: {
            const upstream = try allocator.create(Op);
            errdefer allocator.destroy(upstream);
            upstream.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .materialize = .{ .upstream = upstream } };
        },
        .ddl => Op{ .ddl = try decodeDdl(allocator, bytes, cursor) },
        .show => Op{ .show = try decodeShow(bytes, cursor) },
        .insert => Op{ .insert = try decodeInsert(allocator, bytes, cursor) },
        .batch => blk: {
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const subs = try allocator.alloc(*Op, n);
            errdefer allocator.free(subs);
            var inited: usize = 0;
            errdefer for (subs[0..inited]) |s| {
                s.deinitDecoded(allocator);
                allocator.destroy(s);
            };
            for (subs) |*slot| {
                const sub = try allocator.create(Op);
                errdefer allocator.destroy(sub);
                sub.* = try decodeOp(allocator, bytes, cursor);
                slot.* = sub;
                inited += 1;
            }
            break :blk Op{ .batch = .{ .statements = subs } };
        },
        .copy => Op{ .copy = try decodeCopy(allocator, bytes, cursor) },
        .window => try decodeWindow(allocator, bytes, cursor),
        .set_union => blk: {
            if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
            const all = bytes[cursor.*] != 0;
            cursor.* += 1;
            const left = try allocator.create(Op);
            errdefer allocator.destroy(left);
            left.* = try decodeOp(allocator, bytes, cursor);
            errdefer left.deinitDecoded(allocator);
            const right = try allocator.create(Op);
            errdefer allocator.destroy(right);
            right.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .set_union = .{ .left = left, .right = right, .all = all } };
        },
        .create_table_as => blk: {
            const ref = try decodeTableRef(bytes, cursor);
            if (cursor.* + 2 > bytes.len) return Error.IrCorrupt;
            const if_not_exists = bytes[cursor.*] != 0;
            cursor.* += 1;
            const is_temp = bytes[cursor.*] != 0;
            cursor.* += 1;
            const source = try allocator.create(Op);
            errdefer allocator.destroy(source);
            source.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .create_table_as = .{
                .table = ref,
                .if_not_exists = if_not_exists,
                .is_temp = is_temp,
                .source = source,
            } };
        },
        .insert_select => blk: {
            const ref = try decodeTableRef(bytes, cursor);
            if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
            const has_cols = bytes[cursor.*] != 0;
            cursor.* += 1;
            var cols_opt: ?[]const []const u8 = null;
            if (has_cols) {
                if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
                const n = readU32(bytes[cursor.* .. cursor.* + 4]);
                cursor.* += 4;
                const cols = try allocator.alloc([]const u8, n);
                errdefer allocator.free(cols);
                for (cols) |*c| c.* = try readString(bytes, cursor);
                cols_opt = cols;
            }
            const source = try allocator.create(Op);
            errdefer allocator.destroy(source);
            source.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .insert_select = .{
                .table = ref,
                .columns = cols_opt,
                .source = source,
            } };
        },
    };
}

fn decodeWindow(allocator: Allocator, bytes: []const u8, cursor: *usize) DecodeError!Op {
    if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
    const n_specs = readU32(bytes[cursor.* .. cursor.* + 4]);
    cursor.* += 4;
    const specs = try allocator.alloc(WindowSpec, n_specs);
    errdefer allocator.free(specs);
    var initialized_specs: usize = 0;
    errdefer for (specs[0..initialized_specs]) |sp| {
        allocator.free(sp.partition_by);
        allocator.free(sp.order_by);
    };
    var i: u32 = 0;
    while (i < n_specs) : (i += 1) {
        if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
        const n_pb = readU32(bytes[cursor.* .. cursor.* + 4]);
        cursor.* += 4;
        const pb = try allocator.alloc([]const u8, n_pb);
        errdefer allocator.free(pb);
        for (pb) |*c| c.* = try readString(bytes, cursor);

        if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
        const n_ob = readU32(bytes[cursor.* .. cursor.* + 4]);
        cursor.* += 4;
        const ob = try allocator.alloc(SortSpec, n_ob);
        errdefer allocator.free(ob);
        for (ob) |*s| {
            const col = try readString(bytes, cursor);
            if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
            const desc = bytes[cursor.*] != 0;
            cursor.* += 1;
            s.* = .{ .col = col, .desc = desc };
        }

        if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
        const kind_byte = bytes[cursor.*];
        cursor.* += 1;
        if (kind_byte > @intFromEnum(FrameKind.groups)) return Error.IrCorrupt;
        const kind: FrameKind = @enumFromInt(kind_byte);
        const start = try decodeFrameBound(bytes, cursor);
        const end = try decodeFrameBound(bytes, cursor);

        specs[i] = .{
            .partition_by = pb,
            .order_by = ob,
            .frame = .{ .kind = kind, .start = start, .end = end },
        };
        initialized_specs = i + 1;
    }

    if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
    const n_calls = readU32(bytes[cursor.* .. cursor.* + 4]);
    cursor.* += 4;
    const calls = try allocator.alloc(WindowCall, n_calls);
    errdefer allocator.free(calls);
    var initialized_calls: usize = 0;
    errdefer for (calls[0..initialized_calls]) |c| {
        for (c.args) |a| freeDecodedExpr(a, allocator);
        allocator.free(c.args);
    };
    var j: u32 = 0;
    while (j < n_calls) : (j += 1) {
        if (cursor.* + 4 + 1 + 1 > bytes.len) return Error.IrCorrupt;
        const spec_idx = readU32(bytes[cursor.* .. cursor.* + 4]);
        cursor.* += 4;
        const func_byte = bytes[cursor.*];
        cursor.* += 1;
        if (func_byte > @intFromEnum(WindowFunc.percent_rank)) return Error.IrCorrupt;
        const func: WindowFunc = @enumFromInt(func_byte);
        const ignore_nulls = bytes[cursor.*] != 0;
        cursor.* += 1;
        const output_name = try readString(bytes, cursor);
        if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
        const n_args = readU32(bytes[cursor.* .. cursor.* + 4]);
        cursor.* += 4;
        const args = try allocator.alloc(Expr, n_args);
        errdefer allocator.free(args);
        var inited_args: usize = 0;
        errdefer for (args[0..inited_args]) |a| freeDecodedExpr(a, allocator);
        for (args) |*a| {
            a.* = try decodeExpr(allocator, bytes, cursor);
            inited_args += 1;
        }
        calls[j] = .{
            .spec_idx = spec_idx,
            .func = func,
            .args = args,
            .ignore_nulls = ignore_nulls,
            .output_name = output_name,
        };
        initialized_calls = j + 1;
    }

    const upstream = try allocator.create(Op);
    errdefer allocator.destroy(upstream);
    upstream.* = try decodeOp(allocator, bytes, cursor);
    return Op{ .window = .{ .specs = specs, .calls = calls, .upstream = upstream } };
}

fn decodeFrameBound(bytes: []const u8, cursor: *usize) DecodeError!FrameBound {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const tag = bytes[cursor.*];
    cursor.* += 1;
    return switch (tag) {
        0 => FrameBound{ .unbounded_preceding = {} },
        1 => blk: {
            if (cursor.* + 8 > bytes.len) return Error.IrCorrupt;
            const n = readU64(bytes[cursor.* .. cursor.* + 8]);
            cursor.* += 8;
            break :blk FrameBound{ .preceding = n };
        },
        2 => FrameBound{ .current_row = {} },
        3 => blk: {
            if (cursor.* + 8 > bytes.len) return Error.IrCorrupt;
            const n = readU64(bytes[cursor.* .. cursor.* + 8]);
            cursor.* += 8;
            break :blk FrameBound{ .following = n };
        },
        4 => FrameBound{ .unbounded_following = {} },
        else => Error.IrCorrupt,
    };
}

fn decodeCopy(allocator: Allocator, bytes: []const u8, cursor: *usize) DecodeError!CopyOp {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const dir_byte = bytes[cursor.*];
    cursor.* += 1;
    if (dir_byte > @intFromEnum(CopyOp.Direction.to_stdout)) return Error.IrCorrupt;
    const direction: CopyOp.Direction = @enumFromInt(dir_byte);
    const ref = try decodeTableRef(bytes, cursor);
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const has_cols = bytes[cursor.*] != 0;
    cursor.* += 1;
    const cols_opt: ?[]const []const u8 = if (has_cols) blk: {
        if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
        const n = readU32(bytes[cursor.* .. cursor.* + 4]);
        cursor.* += 4;
        const out = try allocator.alloc([]const u8, n);
        errdefer allocator.free(out);
        for (out) |*c| c.* = try readString(bytes, cursor);
        break :blk out;
    } else null;
    return .{ .direction = direction, .table = ref, .columns = cols_opt };
}

fn decodeTableRef(bytes: []const u8, cursor: *usize) DecodeError!TableRef {
    const database = try decodeOptString(bytes, cursor);
    const schema = try decodeOptString(bytes, cursor);
    const name = try readString(bytes, cursor);
    return .{ .database = database, .schema = schema, .name = name };
}

fn decodeOptString(bytes: []const u8, cursor: *usize) DecodeError!?[]const u8 {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const present = bytes[cursor.*];
    cursor.* += 1;
    if (present == 0) return null;
    if (present != 1) return Error.IrCorrupt;
    return try readString(bytes, cursor);
}

fn decodeDdl(allocator: Allocator, bytes: []const u8, cursor: *usize) DecodeError!DdlOp {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const t = bytes[cursor.*];
    cursor.* += 1;
    if (t > @intFromEnum(DdlTag.drop_table)) return Error.IrCorrupt;
    const tag: DdlTag = @enumFromInt(t);
    return switch (tag) {
        .create_database => DdlOp{ .create_database = try readString(bytes, cursor) },
        .drop_database => DdlOp{ .drop_database = try readString(bytes, cursor) },
        .create_schema => DdlOp{ .create_schema = try readString(bytes, cursor) },
        .drop_schema => DdlOp{ .drop_schema = try readString(bytes, cursor) },
        .use_schema => DdlOp{ .use_schema = try readString(bytes, cursor) },
        .use_database_schema => blk: {
            const db = try readString(bytes, cursor);
            const sc = try readString(bytes, cursor);
            break :blk DdlOp{ .use_database_schema = .{ .database = db, .schema = sc } };
        },
        .create_table => blk: {
            const ref = try decodeTableRef(bytes, cursor);
            if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
            const ine = bytes[cursor.*] != 0;
            cursor.* += 1;
            if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
            const is_temp = bytes[cursor.*] != 0;
            cursor.* += 1;
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const ncols = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const cols = try allocator.alloc(ColumnDef, ncols);
            errdefer allocator.free(cols);
            for (cols) |*c| {
                const name = try readString(bytes, cursor);
                const ty = try decodeType(bytes, cursor);
                if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
                const nullable = bytes[cursor.*] != 0;
                cursor.* += 1;
                c.* = .{ .name = name, .column_type = ty, .nullable = nullable };
            }
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const nkeys = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const keys = try allocator.alloc([]const u8, nkeys);
            errdefer allocator.free(keys);
            for (keys) |*k| k.* = try readString(bytes, cursor);
            break :blk DdlOp{ .create_table = .{
                .table = ref,
                .if_not_exists = ine,
                .is_temp = is_temp,
                .columns = cols,
                .order_key = keys,
            } };
        },
        .drop_table => blk: {
            const ref = try decodeTableRef(bytes, cursor);
            if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
            const ie = bytes[cursor.*] != 0;
            cursor.* += 1;
            break :blk DdlOp{ .drop_table = .{ .table = ref, .if_exists = ie } };
        },
    };
}

fn decodeInsert(allocator: Allocator, bytes: []const u8, cursor: *usize) DecodeError!InsertOp {
    const ref = try decodeTableRef(bytes, cursor);
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const has_cols = bytes[cursor.*] != 0;
    cursor.* += 1;
    const cols_opt: ?[]const []const u8 = if (has_cols) blk: {
        if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
        const n = readU32(bytes[cursor.* .. cursor.* + 4]);
        cursor.* += 4;
        const out = try allocator.alloc([]const u8, n);
        errdefer allocator.free(out);
        for (out) |*c| c.* = try readString(bytes, cursor);
        break :blk out;
    } else null;
    errdefer if (cols_opt) |c| allocator.free(c);

    if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
    const nrows = readU32(bytes[cursor.* .. cursor.* + 4]);
    cursor.* += 4;
    const rows = try allocator.alloc([]const ?Value, nrows);
    errdefer allocator.free(rows);
    var inited: usize = 0;
    errdefer for (rows[0..inited]) |r| allocator.free(r);
    for (rows) |*r| {
        if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
        const ncells = readU32(bytes[cursor.* .. cursor.* + 4]);
        cursor.* += 4;
        const cells = try allocator.alloc(?Value, ncells);
        errdefer allocator.free(cells);
        for (cells) |*v| {
            if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
            const present = bytes[cursor.*] != 0;
            cursor.* += 1;
            v.* = if (present) try decodeValue(bytes, cursor) else null;
        }
        r.* = cells;
        inited += 1;
    }
    return .{ .table = ref, .columns = cols_opt, .rows = rows };
}

fn decodeShow(bytes: []const u8, cursor: *usize) DecodeError!ShowOp {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const t = bytes[cursor.*];
    cursor.* += 1;
    if (t > @intFromEnum(ShowTag.tables)) return Error.IrCorrupt;
    const tag: ShowTag = @enumFromInt(t);
    return switch (tag) {
        .databases => ShowOp.databases,
        .schemas => ShowOp{ .schemas = try decodeOptString(bytes, cursor) },
        .tables => ShowOp{ .tables = try decodeTableRef(bytes, cursor) },
    };
}

/// Decode an Expr tree (mirror of encodeExpr). All strings + sub-slices
/// are allocated into `allocator` and must be released via
/// `freeDecodedExpr`.
pub fn decodeExpr(allocator: Allocator, bytes: []const u8, cursor: *usize) DecodeError!Expr {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const tag_byte = bytes[cursor.*];
    cursor.* += 1;
    if (tag_byte > @intFromEnum(ExprTag.case)) return Error.IrCorrupt;
    const tag: ExprTag = @enumFromInt(tag_byte);
    return switch (tag) {
        .col_ref => Expr{ .col_ref = try readString(bytes, cursor) },
        .lit => Expr{ .lit = try decodeValue(bytes, cursor) },
        .call => blk: {
            const name = try readString(bytes, cursor);
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const args = try allocator.alloc(Expr, n);
            errdefer allocator.free(args);
            for (args) |*a| a.* = try decodeExpr(allocator, bytes, cursor);
            break :blk Expr{ .call = .{ .fn_name = name, .args = args } };
        },
        .case => blk: {
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const branches = try allocator.alloc(Expr.Branch, n);
            errdefer allocator.free(branches);
            for (branches) |*br| {
                br.cond = try decodePredicate(allocator, bytes, cursor);
                br.then = try decodeExpr(allocator, bytes, cursor);
            }
            if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
            const has_else = bytes[cursor.*] != 0;
            cursor.* += 1;
            var else_ptr: ?*const Expr = null;
            if (has_else) {
                const eb = try allocator.create(Expr);
                eb.* = try decodeExpr(allocator, bytes, cursor);
                else_ptr = eb;
            }
            break :blk Expr{ .case = .{ .branches = branches, .else_branch = else_ptr } };
        },
    };
}

pub fn freeDecodedExpr(e: Expr, allocator: Allocator) void {
    switch (e) {
        .col_ref, .lit, .scalar_subquery, .exists_subquery => {},
        .call => |c| {
            for (c.args) |child| freeDecodedExpr(child, allocator);
            allocator.free(c.args);
        },
        .case => |cs| {
            for (cs.branches) |br| {
                freeDecodedPredicate(br.cond, allocator);
                freeDecodedExpr(br.then, allocator);
            }
            allocator.free(cs.branches);
            if (cs.else_branch) |eb| {
                freeDecodedExpr(eb.*, allocator);
                allocator.destroy(@constCast(eb));
            }
        },
    }
}

pub fn decodePredicate(allocator: Allocator, bytes: []const u8, cursor: *usize) DecodeError!PredicateExpr {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const tag_byte = bytes[cursor.*];
    cursor.* += 1;
    if (tag_byte > @intFromEnum(PredTag.leaf_col_col)) return Error.IrCorrupt;
    const tag: PredTag = @enumFromInt(tag_byte);

    return switch (tag) {
        .leaf => blk: {
            if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
            const op_byte = bytes[cursor.*];
            cursor.* += 1;
            if (op_byte > @intFromEnum(PredicateOp.gte)) return Error.IrCorrupt;
            const op: PredicateOp = @enumFromInt(op_byte);
            const col = try readString(bytes, cursor);
            const val = try decodeValue(bytes, cursor);
            break :blk PredicateExpr{ .leaf = .{ .col = col, .op = op, .val = val } };
        },
        .is_null => blk: {
            const col = try readString(bytes, cursor);
            break :blk PredicateExpr{ .is_null = col };
        },
        .is_not_null => blk: {
            const col = try readString(bytes, cursor);
            break :blk PredicateExpr{ .is_not_null = col };
        },
        .p_and, .p_or => blk: {
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const children = try allocator.alloc(PredicateExpr, n);
            errdefer {
                // On failure mid-decode, free what we built.
                var freed: u32 = 0;
                while (freed < n) : (freed += 1) {
                    // Only free initialized entries; rest are uninitialized.
                }
                allocator.free(children);
            }
            var i: u32 = 0;
            while (i < n) : (i += 1) {
                children[i] = try decodePredicate(allocator, bytes, cursor);
            }
            break :blk if (tag == .p_and) PredicateExpr{ .@"and" = children } else PredicateExpr{ .@"or" = children };
        },
        .p_not => blk: {
            const child = try allocator.create(PredicateExpr);
            errdefer allocator.destroy(child);
            child.* = try decodePredicate(allocator, bytes, cursor);
            break :blk PredicateExpr{ .not = child };
        },
        .like => blk: {
            const col = try readString(bytes, cursor);
            const pat = try readString(bytes, cursor);
            break :blk PredicateExpr{ .like = .{ .col = col, .pattern = pat } };
        },
        .leaf_col_col => blk: {
            if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
            const op_byte = bytes[cursor.*];
            cursor.* += 1;
            if (op_byte > @intFromEnum(PredicateOp.gte)) return Error.IrCorrupt;
            const op: PredicateOp = @enumFromInt(op_byte);
            const left = try readString(bytes, cursor);
            const right = try readString(bytes, cursor);
            break :blk PredicateExpr{ .leaf_col_col = .{ .left = left, .op = op, .right = right } };
        },
    };
}

fn readString(bytes: []const u8, cursor: *usize) DecodeError![]const u8 {
    if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
    const len = readU32(bytes[cursor.* .. cursor.* + 4]);
    cursor.* += 4;
    if (cursor.* + len > bytes.len) return Error.IrCorrupt;
    const s = bytes[cursor.* .. cursor.* + len];
    cursor.* += len;
    return s;
}

pub fn decodeValue(bytes: []const u8, cursor: *usize) DecodeError!Value {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const tag_byte = bytes[cursor.*];
    cursor.* += 1;
    const tag: ValueTag = @enumFromInt(tag_byte);
    const c = cursor.*;
    return switch (tag) {
        .int => blk: {
            if (c + 4 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 4;
            break :blk Value{ .int = std.mem.readInt(i32, bytes[c..][0..4], .little) };
        },
        .bigint => blk: {
            if (c + 8 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 8;
            break :blk Value{ .bigint = std.mem.readInt(i64, bytes[c..][0..8], .little) };
        },
        .boolean => blk: {
            if (c + 1 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 1;
            break :blk Value{ .boolean = bytes[c] != 0 };
        },
        .text => blk: {
            if (c + 4 > bytes.len) return Error.IrCorrupt;
            const len = readU32(bytes[c..][0..4]);
            if (c + 4 + len > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 4 + len;
            break :blk Value{ .text = bytes[c + 4 .. c + 4 + len] };
        },
        .float => blk: {
            if (c + 4 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 4;
            const raw = std.mem.readInt(u32, bytes[c..][0..4], .little);
            break :blk Value{ .float = @bitCast(raw) };
        },
        .double => blk: {
            if (c + 8 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 8;
            const raw = std.mem.readInt(u64, bytes[c..][0..8], .little);
            break :blk Value{ .double = @bitCast(raw) };
        },
        .date => blk: {
            if (c + 4 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 4;
            break :blk Value{ .date = std.mem.readInt(i32, bytes[c..][0..4], .little) };
        },
        .datetime => blk: {
            if (c + 8 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 8;
            break :blk Value{ .datetime = std.mem.readInt(i64, bytes[c..][0..8], .little) };
        },
        .tinyint => blk: {
            if (c + 1 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 1;
            break :blk Value{ .tinyint = @bitCast(bytes[c]) };
        },
        .smallint => blk: {
            if (c + 2 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 2;
            break :blk Value{ .smallint = std.mem.readInt(i16, bytes[c..][0..2], .little) };
        },
        .largeint => blk: {
            if (c + 16 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 16;
            break :blk Value{ .largeint = std.mem.readInt(i128, bytes[c..][0..16], .little) };
        },
        .decimal64 => blk: {
            if (c + 8 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 8;
            break :blk Value{ .decimal64 = std.mem.readInt(i64, bytes[c..][0..8], .little) };
        },
        .decimal128 => blk: {
            if (c + 16 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 16;
            break :blk Value{ .decimal128 = std.mem.readInt(i128, bytes[c..][0..16], .little) };
        },
        .uuid => blk: {
            if (c + 16 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 16;
            break :blk Value{ .uuid = std.mem.readInt(u128, bytes[c..][0..16], .little) };
        },
    };
}

pub fn freeDecodedPredicate(expr: PredicateExpr, allocator: Allocator) void {
    switch (expr) {
        .leaf, .leaf_col_col, .is_null, .is_not_null, .like, .scalar_subquery, .exists_subquery, .in_subquery, .always, .in_set, .correlated_set, .correlated_scalar, .correlated_range => {},
        .@"and", .@"or" => |children| {
            for (children) |c| freeDecodedPredicate(c, allocator);
            allocator.free(children);
        },
        .not => |child| {
            freeDecodedPredicate(child.*, allocator);
            allocator.destroy(child);
        },
    }
}

// EXPLAIN rendering lives in `ir_explain.zig`; re-exported here so
// existing callers (`api/plan.zig` etc.) keep working unchanged.
pub const explain = @import("ir_explain.zig").explain;


fn decodeProject(allocator: Allocator, bytes: []const u8, cursor: *usize) DecodeError!Op.Project {
    if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
    const n_cols = readU32(bytes[cursor.* .. cursor.* + 4]);
    cursor.* += 4;

    const cols = try allocator.alloc([]const u8, n_cols);
    errdefer allocator.free(cols);
    for (cols) |*c| {
        if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
        const col_len = readU32(bytes[cursor.* .. cursor.* + 4]);
        cursor.* += 4;
        if (cursor.* + col_len > bytes.len) return Error.IrCorrupt;
        c.* = bytes[cursor.* .. cursor.* + col_len];
        cursor.* += col_len;
    }

    const upstream = try allocator.create(Op);
    errdefer allocator.destroy(upstream);
    upstream.* = try decodeOp(allocator, bytes, cursor);
    return .{ .columns = cols, .upstream = upstream };
}

// ---------------------------------------------------------------------------
// Little-endian helpers (lifted from storage/format.zig style)
// ---------------------------------------------------------------------------

fn appendU16(allocator: Allocator, out: *std.ArrayList(u8), v: u16) !void {
    var b: [2]u8 = undefined;
    std.mem.writeInt(u16, &b, v, .little);
    try out.appendSlice(allocator, &b);
}

fn appendU32(allocator: Allocator, out: *std.ArrayList(u8), v: u32) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(u32, &b, v, .little);
    try out.appendSlice(allocator, &b);
}

fn appendU64(allocator: Allocator, out: *std.ArrayList(u8), v: u64) !void {
    var b: [8]u8 = undefined;
    std.mem.writeInt(u64, &b, v, .little);
    try out.appendSlice(allocator, &b);
}

fn readU16(b: []const u8) u16 {
    return std.mem.readInt(u16, b[0..2], .little);
}

fn readU32(b: []const u8) u32 {
    return std.mem.readInt(u32, b[0..4], .little);
}

fn readU64(b: []const u8) u64 {
    return std.mem.readInt(u64, b[0..8], .little);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "ir: scan + limit round-trips through encode/decode" {
    const allocator = std.testing.allocator;

    var limit_upstream_storage: Op = .{ .scan = .{ .table = .{ .name = "orders" } } };
    const root: Op = .{ .limit = .{ .n = 42, .upstream = &limit_upstream_storage } };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encode(allocator, &buf, root);

    var decoded = try decode(allocator, buf.items);
    defer decoded.deinitDecoded(allocator);

    try std.testing.expect(decoded == .limit);
    try std.testing.expectEqual(@as(u64, 42), decoded.limit.n);
    try std.testing.expect(decoded.limit.upstream.* == .scan);
    try std.testing.expectEqualStrings("orders", decoded.limit.upstream.scan.table.name);
}

test "ir: decode rejects bad magic" {
    const allocator = std.testing.allocator;
    const bad = [_]u8{ 'X', 'X', 'X', 'X', 1, 0, 0, 0 };
    try std.testing.expectError(Error.IrBadMagic, decode(allocator, &bad));
}

test "ir: decode rejects unsupported version" {
    const allocator = std.testing.allocator;
    const bad = [_]u8{ 't', 'D', 'B', 'Q', 99, 0, 0, 0 };
    try std.testing.expectError(Error.IrUnsupportedVersion, decode(allocator, &bad));
}

test "ir: decode rejects truncated input" {
    const allocator = std.testing.allocator;
    const short = [_]u8{ 't', 'D', 'B' };
    try std.testing.expectError(Error.IrTooSmall, decode(allocator, &short));
}

test "ir: select round-trips with multiple columns" {
    const allocator = std.testing.allocator;

    var scan_storage: Op = .{ .scan = .{ .table = .{ .name = "orders" } } };
    const cols = [_][]const u8{ "id", "qty", "tag" };
    const root: Op = .{ .select = .{ .columns = &cols, .upstream = &scan_storage } };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encode(allocator, &buf, root);

    var decoded = try decode(allocator, buf.items);
    defer decoded.deinitDecoded(allocator);

    try std.testing.expect(decoded == .select);
    try std.testing.expectEqual(@as(usize, 3), decoded.select.columns.len);
    try std.testing.expectEqualStrings("id", decoded.select.columns[0]);
    try std.testing.expectEqualStrings("qty", decoded.select.columns[1]);
    try std.testing.expectEqualStrings("tag", decoded.select.columns[2]);
    try std.testing.expect(decoded.select.upstream.* == .scan);
    try std.testing.expectEqualStrings("orders", decoded.select.upstream.scan.table.name);
}

test "ir: exclude round-trips and is distinguishable from select" {
    const allocator = std.testing.allocator;

    var scan_storage: Op = .{ .scan = .{ .table = .{ .name = "t" } } };
    const cols = [_][]const u8{"secret"};
    const root: Op = .{ .exclude = .{ .columns = &cols, .upstream = &scan_storage } };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encode(allocator, &buf, root);

    var decoded = try decode(allocator, buf.items);
    defer decoded.deinitDecoded(allocator);

    try std.testing.expect(decoded == .exclude);
    try std.testing.expectEqualStrings("secret", decoded.exclude.columns[0]);
}

test "ir: compute round-trips with a call expr over a col_ref" {
    const allocator = std.testing.allocator;

    var scan_storage: Op = .{ .scan = .{ .table = .{ .name = "users" } } };
    const arg = Expr{ .col_ref = "name" };
    const args = [_]Expr{arg};
    const expr_call = Expr{ .call = .{ .fn_name = "upper", .args = &args } };
    const derived = [_]Derived{.{ .name = "name_upper", .expr = expr_call }};
    const root: Op = .{ .compute = .{ .derived = &derived, .upstream = &scan_storage } };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encode(allocator, &buf, root);

    var decoded = try decode(allocator, buf.items);
    defer decoded.deinitDecoded(allocator);

    try std.testing.expect(decoded == .compute);
    try std.testing.expectEqual(@as(usize, 1), decoded.compute.derived.len);
    try std.testing.expectEqualStrings("name_upper", decoded.compute.derived[0].name);
    try std.testing.expect(decoded.compute.derived[0].expr == .call);
    try std.testing.expectEqualStrings("upper", decoded.compute.derived[0].expr.call.fn_name);
    try std.testing.expectEqual(@as(usize, 1), decoded.compute.derived[0].expr.call.args.len);
    try std.testing.expect(decoded.compute.derived[0].expr.call.args[0] == .col_ref);
    try std.testing.expectEqualStrings("name", decoded.compute.derived[0].expr.call.args[0].col_ref);
}

test "ir: join round-trips with on + range + extra_predicate + skew" {
    const allocator = std.testing.allocator;

    var left_scan: Op = .{ .scan = .{ .table = .{ .name = "orders" } } };
    var right_scan: Op = .{ .scan = .{ .table = .{ .name = "items" } } };
    const on_pairs = [_]JoinKeyPair{.{ .left = "item_id", .right = "id" }};
    const ranges = [_]JoinRangePredicate{.{ .left = "qty", .right = "min_qty", .op = .gte }};
    // Post-join filter on a column that exists in the joined output.
    const extra = PredicateExpr{ .leaf = .{ .col = "price", .op = .lt, .val = .{ .double = 100.0 } } };
    const root: Op = .{ .join = .{
        .algorithm = .auto,
        .join_type = .inner,
        .on = &on_pairs,
        .ranges = &ranges,
        .extra_predicate = extra,
        .skew_ratio_threshold = 0.5,
        .skew_absolute_threshold = 50_000,
        .skew_sample_interval = 10,
        .left = &left_scan,
        .right = &right_scan,
    } };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encode(allocator, &buf, root);

    var decoded = try decode(allocator, buf.items);
    defer decoded.deinitDecoded(allocator);

    try std.testing.expect(decoded == .join);
    try std.testing.expectEqual(JoinAlgorithm.auto, decoded.join.algorithm);
    try std.testing.expectEqual(JoinType.inner, decoded.join.join_type);
    try std.testing.expectEqual(@as(usize, 1), decoded.join.on.len);
    try std.testing.expectEqualStrings("item_id", decoded.join.on[0].left);
    try std.testing.expectEqualStrings("id", decoded.join.on[0].right);
    try std.testing.expectEqual(@as(usize, 1), decoded.join.ranges.len);
    try std.testing.expectEqual(PredicateOp.gte, decoded.join.ranges[0].op);
    try std.testing.expect(decoded.join.extra_predicate != null);
    try std.testing.expectEqual(@as(f32, 0.5), decoded.join.skew_ratio_threshold);
    try std.testing.expectEqual(@as(u32, 50_000), decoded.join.skew_absolute_threshold);
    try std.testing.expect(decoded.join.left.* == .scan);
    try std.testing.expectEqualStrings("orders", decoded.join.left.scan.table.name);
    try std.testing.expect(decoded.join.right.* == .scan);
    try std.testing.expectEqualStrings("items", decoded.join.right.scan.table.name);
}

test "ir: join with no on/ranges and no extra_predicate (pure-NLJ shape)" {
    const allocator = std.testing.allocator;

    var left_scan: Op = .{ .scan = .{ .table = .{ .name = "a" } } };
    var right_scan: Op = .{ .scan = .{ .table = .{ .name = "b" } } };
    const root: Op = .{ .join = .{
        .algorithm = .nested_loop,
        .join_type = .inner,
        .on = &.{},
        .ranges = &.{},
        .extra_predicate = null,
        .skew_ratio_threshold = 0.0,
        .skew_absolute_threshold = 0,
        .skew_sample_interval = 10,
        .left = &left_scan,
        .right = &right_scan,
    } };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encode(allocator, &buf, root);

    var decoded = try decode(allocator, buf.items);
    defer decoded.deinitDecoded(allocator);

    try std.testing.expect(decoded == .join);
    try std.testing.expectEqual(JoinAlgorithm.nested_loop, decoded.join.algorithm);
    try std.testing.expectEqual(@as(usize, 0), decoded.join.on.len);
    try std.testing.expectEqual(@as(usize, 0), decoded.join.ranges.len);
    try std.testing.expect(decoded.join.extra_predicate == null);
}
