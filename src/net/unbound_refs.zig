//! Pre-rewrite detection of references that can never bind.
//!
//! Operators bind names only when they compile, which is AFTER the removal
//! rewrites (`const_fold.foldDeadBranches`, `prune_columns.pruneDeadColumns`)
//! have deleted dead UNION arms, select items, compute derivations, window
//! calls and aggregates. So SQL whose only invalid part is unused —
//! `WITH b AS (SELECT id, nonexistent AS x FROM t) SELECT count(*) FROM b` —
//! would run, while the same body read in full fails. `find` reports the first
//! column or function reference no operator could ever resolve; the caller
//! then skips the removal rewrites so the operator binder raises the error.
//!
//! Soundness: each name set is a SUPERSET of what the operator exposes, and
//! names compare on their last dotted segment — the part `types.findColumn`
//! matches on in every one of its rules — so a reported reference really is
//! unbindable. A shape whose names can't be enumerated (table functions, file
//! scans, catalog views, unresolvable tables) is unknown and never reports. A
//! miss keeps the old behaviour; a false report costs only the pruning.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ir = @import("../ir/ir.zig");
const types = @import("../types.zig");
const api = @import("../api/api.zig");
const local = @import("local.zig");
const pgcat = @import("pg_catalog.zig");
const scalar_fn = @import("../exec/scalar_fn.zig");
const compute = @import("../exec/compute.zig");
const Expr = @import("../exec/expr.zig").Expr;
const PredicateExpr = @import("../exec/predicate.zig").PredicateExpr;
const UdfRegistry = @import("../udf.zig").UdfRegistry;

pub const Unbound = union(enum) {
    column: []const u8,
    function: []const u8,

    /// A copy whose name lives in `allocator`, so it outlives the tree it
    /// was found in.
    pub fn dupe(self: Unbound, allocator: Allocator) Allocator.Error!Unbound {
        return switch (self) {
            .column => |c| .{ .column = try allocator.dupe(u8, c) },
            .function => |f| .{ .function = try allocator.dupe(u8, f) },
        };
    }

    pub fn name(self: Unbound) []const u8 {
        return switch (self) {
            inline else => |n| n,
        };
    }
};

pub fn find(
    arena: Allocator,
    catalog: ?*api.Catalog,
    session: api.Session,
    udfs: ?*const UdfRegistry,
    root: *const ir.Op,
) Allocator.Error!?Unbound {
    var finder: Finder = .{ .arena = arena, .catalog = catalog, .session = session, .udfs = udfs };
    return finder.walk(root);
}

/// Lower-cased bare names of the columns an operator can expose.
const NameSet = std.StringHashMapUnmanaged(void);

const lastSegment = types.unqualifiedName;

fn isStar(name: []const u8) bool {
    return std.mem.eql(u8, name, "*") or std.mem.endsWith(u8, name, ".*");
}

const Finder = struct {
    arena: Allocator,
    catalog: ?*api.Catalog,
    session: api.Session,
    udfs: ?*const UdfRegistry,
    /// Memoized per operator: CTE bodies are shared subtrees.
    exposed: std.AutoHashMapUnmanaged(*const ir.Op, ?*const NameSet) = .empty,
    visited: std.AutoHashMapUnmanaged(*const ir.Op, void) = .empty,

    fn walk(self: *Finder, op: *const ir.Op) Allocator.Error!?Unbound {
        if ((try self.visited.getOrPut(self.arena, op)).found_existing) return null;
        if (try self.check(op)) |u| return u;
        return switch (op.*) {
            .limit => |l| self.walk(l.upstream),
            .select, .exclude => |p| self.walk(p.upstream),
            .filter => |f| self.walk(f.upstream),
            .order_by => |o| self.walk(o.upstream),
            .group_by => |g| self.walk(g.upstream),
            .compute => |c| self.walk(c.upstream),
            .materialize => |m| self.walk(m.upstream),
            .window => |w| self.walk(w.upstream),
            .alias => |a| self.walk(a.upstream),
            .join => |j| (try self.walk(j.left)) orelse self.walk(j.right),
            .set_union => |u| (try self.walk(u.left)) orelse self.walk(u.right),
            .create_table_as => |c| self.walk(c.source),
            .insert_select => |i| self.walk(i.source),
            .explain => |e| self.walk(e.inner),
            .batch => |b| self.walkAll(b.statements),
            .table_fn => |t| self.walkAll(t.inputs),
            else => null,
        };
    }

    fn walkAll(self: *Finder, ops: []const *ir.Op) Allocator.Error!?Unbound {
        for (ops) |op| if (try self.walk(op)) |u| return u;
        return null;
    }

    fn check(self: *Finder, op: *const ir.Op) Allocator.Error!?Unbound {
        switch (op.*) {
            .select => |p| {
                const scope = try self.names(p.upstream) orelse return null;
                for (p.columns) |c| {
                    if (!isStar(c) and !has(scope, c)) return .{ .column = c };
                }
            },
            .filter => |f| {
                const scope = try self.names(f.upstream) orelse return null;
                return self.checkPredicate(f.predicate, scope);
            },
            .order_by => |o| {
                const scope = try self.names(o.upstream) orelse return null;
                for (o.specs) |sp| if (!has(scope, sp.col)) return .{ .column = sp.col };
            },
            .group_by => |g| {
                const scope = try self.names(g.upstream) orelse return null;
                for (g.group_cols) |c| if (!has(scope, c)) return .{ .column = c };
                for (g.aggs) |a| {
                    for ([_]?[]const u8{ a.col, a.arg2_col }) |maybe| {
                        const c = maybe orelse continue;
                        if (!isStar(c) and !has(scope, c)) return .{ .column = c };
                    }
                    for (a.udf_arg_cols) |c| if (!has(scope, c)) return .{ .column = c };
                }
            },
            // A derivation may read its siblings, so the scope is the
            // compute's own output.
            .compute => |c| {
                const scope = try self.names(op) orelse return null;
                for (c.derived) |d| if (try self.checkExpr(d.expr, scope)) |u| return u;
            },
            .window => |w| {
                const scope = try self.names(op) orelse return null;
                for (w.specs) |spec| {
                    for (spec.partition_by) |c| if (!has(scope, c)) return .{ .column = c };
                    for (spec.order_by) |sp| if (!has(scope, sp.col)) return .{ .column = sp.col };
                }
                for (w.calls) |call| {
                    for (call.args) |arg| if (try self.checkExpr(arg, scope)) |u| return u;
                }
            },
            else => {},
        }
        return null;
    }

    fn checkExpr(self: *Finder, e: Expr, scope: *const NameSet) Allocator.Error!?Unbound {
        switch (e) {
            .col_ref => |c| if (!isStar(c) and !has(scope, c)) return .{ .column = c },
            .call => |c| {
                if (!scalar_fn.nameResolvable(self.udfs, c.fn_name)) return .{ .function = c.fn_name };
                for (c.args) |arg| if (try self.checkExpr(arg, scope)) |u| return u;
            },
            .case => |cs| {
                for (cs.branches) |b| {
                    if (try self.checkPredicate(b.cond, scope)) |u| return u;
                    if (try self.checkExpr(b.then, scope)) |u| return u;
                }
                if (cs.else_branch) |eb| return self.checkExpr(eb.*, scope);
            },
            .lit, .null_lit, .scalar_subquery, .exists_subquery, .var_ref => {},
        }
        return null;
    }

    fn checkPredicate(self: *Finder, p: PredicateExpr, scope: *const NameSet) Allocator.Error!?Unbound {
        var refs: std.ArrayListUnmanaged([]const u8) = .empty;
        try compute.collectPredicateColumnRefs(self.arena, &refs, p);
        for (refs.items) |c| if (!has(scope, c)) return .{ .column = c };
        return null;
    }

    /// Names `op` can expose, or null when they can't be enumerated.
    fn names(self: *Finder, op: *const ir.Op) Allocator.Error!?*const NameSet {
        if (self.exposed.get(op)) |memo| return memo;
        const result: ?*const NameSet = switch (op.*) {
            .scan => |s| try self.scanNames(s),
            .single_row => try self.newSet(),
            .limit => |l| try self.names(l.upstream),
            .filter => |f| try self.names(f.upstream),
            .order_by => |o| try self.names(o.upstream),
            .materialize => |m| try self.names(m.upstream),
            .alias => |a| try self.names(a.upstream),
            // Excluded names are kept: a superset never misreports.
            .exclude => |p| try self.names(p.upstream),
            .set_union => |u| try self.names(u.left),
            .select => |p| blk: {
                const set = try self.newSet();
                for (p.columns, 0..) |c, i| {
                    if (isStar(c)) {
                        const up = try self.names(p.upstream) orelse break :blk null;
                        try self.addAll(set, up);
                        continue;
                    }
                    // The source name too: shapes above a renaming select
                    // may still reach it.
                    try self.add(set, c);
                    if (p.outputs) |outs| if (i < outs.len) if (outs[i]) |o| try self.add(set, o);
                }
                break :blk set;
            },
            .compute => |c| blk: {
                const up = try self.names(c.upstream) orelse break :blk null;
                const set = try self.cloneSet(up);
                for (c.derived) |d| try self.add(set, d.name);
                break :blk set;
            },
            .window => |w| blk: {
                const up = try self.names(w.upstream) orelse break :blk null;
                const set = try self.cloneSet(up);
                for (w.calls) |call| try self.add(set, call.output_name);
                break :blk set;
            },
            .group_by => |g| blk: {
                const set = try self.newSet();
                for (g.group_cols) |c| try self.add(set, c);
                for (g.aggs) |a| try self.add(set, a.as);
                break :blk set;
            },
            .join => |j| blk: {
                const left = try self.names(j.left) orelse break :blk null;
                const right = try self.names(j.right) orelse break :blk null;
                const set = try self.cloneSet(left);
                try self.addAll(set, right);
                break :blk set;
            },
            else => null,
        };
        try self.exposed.put(self.arena, op, result);
        return result;
    }

    fn scanNames(self: *Finder, s: ir.Op.Scan) Allocator.Error!?*const NameSet {
        if (pgcat.match(s.table) != null) return null;
        const cat = self.catalog orelse return null;
        const table = local.resolveTable(cat, self.session, s.table) catch return null;
        const set = try self.newSet();
        for (table.schema.columns) |col| try self.add(set, col.name);
        return set;
    }

    fn newSet(self: *Finder) Allocator.Error!*NameSet {
        const set = try self.arena.create(NameSet);
        set.* = .empty;
        return set;
    }

    fn cloneSet(self: *Finder, src: *const NameSet) Allocator.Error!*NameSet {
        const set = try self.arena.create(NameSet);
        set.* = try src.clone(self.arena);
        return set;
    }

    fn add(self: *Finder, set: *NameSet, name: []const u8) Allocator.Error!void {
        const folded = try std.ascii.allocLowerString(self.arena, lastSegment(name));
        try set.put(self.arena, folded, {});
    }

    fn addAll(self: *Finder, set: *NameSet, src: *const NameSet) Allocator.Error!void {
        var it = src.keyIterator();
        while (it.next()) |k| try set.put(self.arena, k.*, {});
    }
};

fn has(set: *const NameSet, name: []const u8) bool {
    const seg = lastSegment(name);
    var buf: [256]u8 = undefined;
    // Too long to fold on the stack: compare case-insensitively instead.
    if (seg.len > buf.len) {
        var it = set.keyIterator();
        while (it.next()) |k| if (std.ascii.eqlIgnoreCase(k.*, seg)) return true;
        return false;
    }
    return set.contains(std.ascii.lowerString(buf[0..seg.len], seg));
}
