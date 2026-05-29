//! PlanBuilder — multi-source plan-tree construction.
//!
//! The existing Query builder (`thindb.scan(...).filter(...).join(...)`)
//! works for linear pipelines but each step instantiates the executable
//! operator immediately. PlanBuilder instead produces an `ir.Op` tree,
//! letting callers:
//!   - express multi-branched shapes (joins of subtrees built independently)
//!   - inspect / render the plan before execution (EXPLAIN — task #151)
//!   - hold an inert plan and compile + run it multiple times
//!
//! Usage:
//!   var pb = PlanBuilder.init(allocator);
//!   defer pb.deinit();
//!
//!   const orders = try pb.scan("orders");
//!   const items  = try pb.scan("items");
//!   const joined = try pb.join(orders, items, .{ .on = ..., .algorithm = .auto });
//!   const filt   = try pb.filter(joined, predicate);
//!
//!   var q = try pb.compile(db, filt);
//!   defer q.deinit();
//!
//! All `*ir.Op` returned by builder methods live in the PlanBuilder's
//! arena. The arena (and every node + string referenced from it) is
//! released by `pb.deinit()`.

const std = @import("std");
const Allocator = std.mem.Allocator;

const api = @import("api.zig");
const Database = api.Database;

const ir = @import("../ir/ir.zig");

const exec = @import("../exec/exec.zig");
const Query = exec.Query;
const Predicate = exec.Predicate;
const PredicateExpr = exec.PredicateExpr;

const local = @import("../net/local.zig");

pub const PlanBuilder = struct {
    arena: std.heap.ArenaAllocator,

    pub fn init(parent_allocator: Allocator) PlanBuilder {
        return .{ .arena = std.heap.ArenaAllocator.init(parent_allocator) };
    }

    pub fn deinit(self: *PlanBuilder) void {
        self.arena.deinit();
    }

    /// Access the arena's allocator. Useful when callers need to build
    /// auxiliary structures (Expr trees, predicates) that the plan
    /// references — keeping them in the same arena ensures consistent
    /// lifetime.
    pub fn arenaAllocator(self: *PlanBuilder) Allocator {
        return self.arena.allocator();
    }

    // -----------------------------------------------------------------------
    // Source.
    // -----------------------------------------------------------------------

    pub fn scan(self: *PlanBuilder, table_name: []const u8) !*ir.Op {
        return self.scanQualified(null, null, table_name);
    }

    /// Build a Scan node with explicit database/schema qualifiers.
    /// Either qualifier may be null — null falls back to the active
    /// Session at compile time. Symmetric with the 1-/2-/3-part SQL
    /// surface.
    pub fn scanQualified(
        self: *PlanBuilder,
        database: ?[]const u8,
        schema: ?[]const u8,
        table_name: []const u8,
    ) !*ir.Op {
        const aa = self.arena.allocator();
        const op = try aa.create(ir.Op);
        const db_dup: ?[]const u8 = if (database) |d| try aa.dupe(u8, d) else null;
        const sc_dup: ?[]const u8 = if (schema) |s| try aa.dupe(u8, s) else null;
        op.* = .{ .scan = .{ .table = .{
            .database = db_dup,
            .schema = sc_dup,
            .name = try aa.dupe(u8, table_name),
        } } };
        return op;
    }

    // -----------------------------------------------------------------------
    // Unary linear operators.
    // -----------------------------------------------------------------------

    pub fn limit(self: *PlanBuilder, upstream: *ir.Op, n: u64) !*ir.Op {
        const aa = self.arena.allocator();
        const op = try aa.create(ir.Op);
        op.* = .{ .limit = .{ .n = n, .upstream = upstream } };
        return op;
    }

    /// Whitelist projection — keep only the named columns, in order.
    pub fn select(self: *PlanBuilder, upstream: *ir.Op, columns: []const []const u8) !*ir.Op {
        const aa = self.arena.allocator();
        const dup_cols = try aa.alloc([]const u8, columns.len);
        for (columns, dup_cols) |c, *out| out.* = try aa.dupe(u8, c);
        const op = try aa.create(ir.Op);
        op.* = .{ .select = .{ .columns = dup_cols, .upstream = upstream } };
        return op;
    }

    /// Anti-projection — drop the named columns.
    pub fn exclude(self: *PlanBuilder, upstream: *ir.Op, columns: []const []const u8) !*ir.Op {
        const aa = self.arena.allocator();
        const dup_cols = try aa.alloc([]const u8, columns.len);
        for (columns, dup_cols) |c, *out| out.* = try aa.dupe(u8, c);
        const op = try aa.create(ir.Op);
        op.* = .{ .exclude = .{ .columns = dup_cols, .upstream = upstream } };
        return op;
    }

    pub fn filter(self: *PlanBuilder, upstream: *ir.Op, predicate: PredicateExpr) !*ir.Op {
        const aa = self.arena.allocator();
        const op = try aa.create(ir.Op);
        op.* = .{ .filter = .{ .predicate = predicate, .upstream = upstream } };
        return op;
    }

    pub fn orderBy(self: *PlanBuilder, upstream: *ir.Op, specs: []const exec.SortSpec) !*ir.Op {
        const aa = self.arena.allocator();
        const dup_specs = try aa.alloc(exec.SortSpec, specs.len);
        for (specs, dup_specs) |s, *out| out.* = .{ .col = try aa.dupe(u8, s.col), .desc = s.desc };
        const op = try aa.create(ir.Op);
        op.* = .{ .order_by = .{ .specs = dup_specs, .upstream = upstream } };
        return op;
    }

    pub fn groupBy(
        self: *PlanBuilder,
        upstream: *ir.Op,
        group_cols: []const []const u8,
        aggs: []const ir.AggSpec,
    ) !*ir.Op {
        const aa = self.arena.allocator();
        const dup_groups = try aa.alloc([]const u8, group_cols.len);
        for (group_cols, dup_groups) |c, *out| out.* = try aa.dupe(u8, c);
        const dup_aggs = try aa.alloc(ir.AggSpec, aggs.len);
        for (aggs, dup_aggs) |a, *out| {
            const udf_arg_cols = try aa.alloc([]const u8, a.udf_arg_cols.len);
            for (a.udf_arg_cols, udf_arg_cols) |c, *dst| dst.* = try aa.dupe(u8, c);
            out.* = a;
            out.udf_arg_cols = udf_arg_cols;
            if (a.udf_name) |n| out.udf_name = try aa.dupe(u8, n);
            if (a.col) |c| out.col = try aa.dupe(u8, c);
            out.as = try aa.dupe(u8, a.as);
        }
        const op = try aa.create(ir.Op);
        op.* = .{ .group_by = .{
            .group_cols = dup_groups,
            .aggs = dup_aggs,
            .upstream = upstream,
        } };
        return op;
    }

    pub fn compute(self: *PlanBuilder, upstream: *ir.Op, derived: []const ir.Derived) !*ir.Op {
        const aa = self.arena.allocator();
        const dup_derived = try aa.alloc(ir.Derived, derived.len);
        @memcpy(dup_derived, derived);
        const op = try aa.create(ir.Op);
        op.* = .{ .compute = .{ .derived = dup_derived, .upstream = upstream } };
        return op;
    }

    // -----------------------------------------------------------------------
    // Binary (multi-branched) operator.
    // -----------------------------------------------------------------------

    pub fn join(
        self: *PlanBuilder,
        left: *ir.Op,
        right: *ir.Op,
        spec: JoinSpec,
    ) !*ir.Op {
        const aa = self.arena.allocator();
        const dup_on = try aa.alloc(ir.JoinKeyPair, spec.on.len);
        @memcpy(dup_on, spec.on);
        const dup_ranges = try aa.alloc(ir.JoinRangePredicate, spec.ranges.len);
        @memcpy(dup_ranges, spec.ranges);
        const op = try aa.create(ir.Op);
        op.* = .{ .join = .{
            .algorithm = spec.algorithm,
            .join_type = spec.join_type,
            .on = dup_on,
            .ranges = dup_ranges,
            .extra_predicate = spec.extra_predicate,
            .skew_ratio_threshold = spec.skew_ratio_threshold,
            .skew_absolute_threshold = spec.skew_absolute_threshold,
            .skew_sample_interval = spec.skew_sample_interval,
            .left = left,
            .right = right,
        } };
        return op;
    }

    // -----------------------------------------------------------------------
    // Compile — turn a built plan into an executable Query.
    // -----------------------------------------------------------------------

    /// Compile `root` into an executable Query against `db`. The
    /// returned Query owns its own state; callers `q.deinit()` it
    /// independently of `pb.deinit()`. Safe to call multiple times on
    /// the same plan to produce parallel Queries.
    pub fn compile(self: *PlanBuilder, query_allocator: Allocator, db: *Database, root: *ir.Op) !Query {
        _ = self;
        return local.buildServerQuery(query_allocator, db, root.*);
    }

    /// Render `root` as indented text. The returned slice lives in
    /// the PlanBuilder's arena; copy it out before pb.deinit() if you
    /// need it longer.
    pub fn explain(self: *PlanBuilder, root: *ir.Op) ![]const u8 {
        const aa = self.arena.allocator();
        var buf: std.ArrayList(u8) = .empty;
        try ir.explain(aa, &buf, root.*);
        return buf.toOwnedSlice(aa);
    }
};

/// Mirror of `exec.join.Spec` minus the opaque_predicate (not
/// representable on the plan tree — it's a function pointer).
/// All defaults mirror exec.join.Spec so the same defaults apply.
pub const JoinSpec = struct {
    on: []const ir.JoinKeyPair = &.{},
    ranges: []const ir.JoinRangePredicate = &.{},
    algorithm: ir.JoinAlgorithm = .auto,
    join_type: ir.JoinType = .inner,
    extra_predicate: ?PredicateExpr = null,
    skew_ratio_threshold: f32 = 0.3,
    skew_absolute_threshold: u32 = 20_000,
    skew_sample_interval: u32 = 10,
};
