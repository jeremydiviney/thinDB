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
//!
//! `findAmbiguous` serves a compile that failed anyway: an operator refuses
//! a reference two of its input's columns answer to with the same bare
//! ColumnNotFound, and the report names it. Its column lists follow how
//! compile qualifies names, so a miss leaves the error unnamed and a false
//! report only misnames an error the statement raised regardless.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ir = @import("../ir/ir.zig");
const types = @import("../types.zig");
const api = @import("../api/api.zig");
const local = @import("local.zig");
const pgcat = @import("pg_catalog.zig");
const scalar_fn = @import("../exec/scalar_fn.zig");
const compute = @import("../exec/compute.zig");
const expr_mod = @import("../exec/expr.zig");
const Expr = expr_mod.Expr;
const PredicateExpr = @import("../exec/predicate.zig").PredicateExpr;
const UdfRegistry = @import("../udf.zig").UdfRegistry;

pub const Unbound = union(enum) {
    column: []const u8,
    function: []const u8,
    /// A column reference more than one input column answers to.
    ambiguous_column: []const u8,

    /// A copy whose name lives in `allocator`, so it outlives the tree it
    /// was found in.
    pub fn dupe(self: Unbound, allocator: Allocator) Allocator.Error!Unbound {
        return switch (self) {
            .column => |c| .{ .column = try allocator.dupe(u8, c) },
            .function => |f| .{ .function = try allocator.dupe(u8, f) },
            .ambiguous_column => |c| .{ .ambiguous_column = try allocator.dupe(u8, c) },
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
    var finder: Finder = .{ .arena = arena, .catalog = catalog, .session = session, .udfs = udfs, .mode = .unbound };
    return finder.walk(root);
}

/// The first column reference `types.findColumn` refuses for matching more
/// than one column of its input. For naming a ColumnNotFound after the fact.
pub fn findAmbiguous(
    arena: Allocator,
    catalog: ?*api.Catalog,
    session: api.Session,
    root: *const ir.Op,
) Allocator.Error!?Unbound {
    var finder: Finder = .{ .arena = arena, .catalog = catalog, .session = session, .udfs = null, .mode = .ambiguous };
    return finder.walk(root);
}

/// Lower-cased bare names of the columns an operator can expose.
const NameSet = std.StringHashMapUnmanaged(void);

const lastSegment = types.unqualifiedName;

fn isStar(name: []const u8) bool {
    return std.mem.eql(u8, name, "*") or std.mem.endsWith(u8, name, ".*");
}

/// A column an operator exposes, as `types.findColumn` matches it: the
/// qualifier compile gives it (null for a bare name) and its bare name.
const Exposed = struct { qualifier: ?[]const u8, bare: []const u8 };

fn bareColumn(name: []const u8) Exposed {
    return .{ .qualifier = null, .bare = name };
}

fn splitColumn(name: []const u8) Exposed {
    const split = types.splitQualifiedName(name) orelse return bareColumn(name);
    return .{ .qualifier = split.qualifier, .bare = split.bare };
}

const Mode = enum { unbound, ambiguous };

/// What a reference is checked against: the bare names an operator can
/// expose, or its qualified columns.
const Scope = union(Mode) {
    unbound: *const NameSet,
    ambiguous: []const Exposed,

    fn rejects(self: Scope, name: []const u8) ?Unbound {
        if (isStar(name)) return null;
        return switch (self) {
            .unbound => |set| if (has(set, name)) null else .{ .column = name },
            .ambiguous => |columns| if (isAmbiguous(columns, name)) .{ .ambiguous_column = name } else null,
        };
    }
};

const Finder = struct {
    arena: Allocator,
    catalog: ?*api.Catalog,
    session: api.Session,
    udfs: ?*const UdfRegistry,
    mode: Mode,
    /// Memoized per operator: CTE bodies are shared subtrees.
    exposed: std.AutoHashMapUnmanaged(*const ir.Op, ?*const NameSet) = .empty,
    exposed_columns: std.AutoHashMapUnmanaged(*const ir.Op, ?[]const Exposed) = .empty,
    visited: std.AutoHashMapUnmanaged(*const ir.Op, void) = .empty,
    /// The lambdas whose bodies are being checked: their parameters are no
    /// columns.
    lambdas: ?*const expr_mod.LambdaScope = null,

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
            .series => |sr| self.walk(sr.upstream),
            .join => |j| (try self.walk(j.left)) orelse self.walk(j.right),
            .set_union => |u| (try self.walk(u.left)) orelse self.walk(u.right),
            .create_table_as => |c| self.walk(c.source),
            .insert_select => |i| self.walk(i.source),
            .delete_op => |d| if (d.source) |s| self.walk(s) else null,
            .update_op => |u| if (u.source) |s| self.walk(s) else null,
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
                const scope = try self.scopeOf(p.upstream) orelse return null;
                for (p.columns) |c| if (scope.rejects(c)) |u| return u;
            },
            .filter => |f| {
                const scope = try self.scopeOf(f.upstream) orelse return null;
                return self.checkPredicate(f.predicate, scope);
            },
            .order_by => |o| {
                const scope = try self.scopeOf(o.upstream) orelse return null;
                for (o.specs) |sp| if (scope.rejects(sp.col)) |u| return u;
            },
            .group_by => |g| {
                const scope = try self.scopeOf(g.upstream) orelse return null;
                for (g.group_cols) |c| if (scope.rejects(c)) |u| return u;
                for (g.aggs) |a| {
                    for ([_]?[]const u8{ a.col, a.arg2_col }) |maybe| {
                        const c = maybe orelse continue;
                        if (scope.rejects(c)) |u| return u;
                    }
                    for (a.udf_arg_cols) |c| if (scope.rejects(c)) |u| return u;
                }
            },
            // A derivation may read its siblings, so the scope is the
            // compute's own output.
            .compute => |c| {
                const scope = try self.scopeOf(op) orelse return null;
                for (c.derived) |d| if (try self.checkExpr(d.expr, scope)) |u| return u;
            },
            .window => |w| {
                const scope = try self.scopeOf(op) orelse return null;
                for (w.specs) |spec| {
                    for (spec.partition_by) |c| if (scope.rejects(c)) |u| return u;
                    for (spec.order_by) |sp| if (scope.rejects(sp.col)) |u| return u;
                }
                for (w.calls) |call| {
                    for (call.args) |arg| if (try self.checkExpr(arg, scope)) |u| return u;
                }
            },
            else => {},
        }
        return null;
    }

    fn checkExpr(self: *Finder, e: Expr, scope: Scope) Allocator.Error!?Unbound {
        switch (e) {
            .col_ref => |c| if (!expr_mod.LambdaScope.binds(self.lambdas, c)) if (scope.rejects(c)) |u| return u,
            .call => |c| {
                if (self.mode == .unbound and !scalar_fn.nameResolvable(self.udfs, c.fn_name)) return .{ .function = c.fn_name };
                for (c.args) |arg| if (try self.checkExpr(arg, scope)) |u| return u;
            },
            .case => |cs| {
                for (cs.operands) |o| if (try self.checkExpr(o.expr, scope)) |u| return u;
                var refs: std.ArrayListUnmanaged([]const u8) = .empty;
                for (cs.branches) |b| {
                    refs.clearRetainingCapacity();
                    try expr_mod.collectCaseConditionRefs(self.arena, &refs, cs, b.cond);
                    for (refs.items) |c| if (!expr_mod.LambdaScope.binds(self.lambdas, c)) if (scope.rejects(c)) |u| return u;
                    if (try self.checkExpr(b.then, scope)) |u| return u;
                }
                if (cs.else_branch) |eb| return self.checkExpr(eb.*, scope);
            },
            .lambda => |l| {
                const enclosing = self.lambdas;
                const inner: expr_mod.LambdaScope = .{ .lambda = l, .outer = enclosing };
                self.lambdas = &inner;
                defer self.lambdas = enclosing;
                return self.checkExpr(l.body.*, scope);
            },
            .lit, .null_lit, .scalar_subquery, .exists_subquery, .var_ref => {},
        }
        return null;
    }

    fn checkPredicate(self: *Finder, p: PredicateExpr, scope: Scope) Allocator.Error!?Unbound {
        var refs: std.ArrayListUnmanaged([]const u8) = .empty;
        try compute.collectPredicateColumnRefs(self.arena, &refs, p);
        for (refs.items) |c| if (scope.rejects(c)) |u| return u;
        return null;
    }

    fn scopeOf(self: *Finder, op: *const ir.Op) Allocator.Error!?Scope {
        return switch (self.mode) {
            .unbound => .{ .unbound = try self.names(op) orelse return null },
            .ambiguous => .{ .ambiguous = try self.columns(op) orelse return null },
        };
    }

    /// The columns `op` exposes, qualified as compile qualifies them, or
    /// null when they can't be enumerated.
    fn columns(self: *Finder, op: *const ir.Op) Allocator.Error!?[]const Exposed {
        if (self.exposed_columns.get(op)) |memo| return memo;
        const result: ?[]const Exposed = switch (op.*) {
            .scan => |s| try self.scanColumns(s),
            .single_row => &.{},
            .series => |sr| try self.arena.dupe(Exposed, &.{.{ .qualifier = sr.alias, .bare = sr.column }}),
            .limit => |l| try self.columns(l.upstream),
            .filter => |f| try self.columns(f.upstream),
            .order_by => |o| try self.columns(o.upstream),
            .materialize => |m| try self.columns(m.upstream),
            .exclude => |p| try self.columns(p.upstream),
            .set_union => |u| try self.columns(u.left),
            // A subquery, CTE or view read through an alias: its columns
            // answer to the alias alone.
            .alias => |a| blk: {
                const up = try self.columns(a.upstream) orelse break :blk null;
                const out = try self.arena.alloc(Exposed, up.len);
                for (up, out) |c, *o| o.* = .{ .qualifier = a.alias, .bare = c.bare };
                break :blk out;
            },
            .select => |p| blk: {
                var out: std.ArrayListUnmanaged(Exposed) = .empty;
                for (p.columns, 0..) |c, i| {
                    if (isStar(c)) {
                        const up = try self.columns(p.upstream) orelse break :blk null;
                        const qualifier: ?[]const u8 = if (c.len > 2) c[0 .. c.len - 2] else null;
                        for (up) |u| {
                            if (qualifier) |q| {
                                const uq = u.qualifier orelse continue;
                                if (!std.ascii.eqlIgnoreCase(uq, q)) continue;
                            }
                            try out.append(self.arena, u);
                        }
                        continue;
                    }
                    // A projected column outputs its bare source name.
                    const explicit: ?[]const u8 = if (p.outputs) |outs| (if (i < outs.len) outs[i] else null) else null;
                    try out.append(self.arena, bareColumn(explicit orelse lastSegment(c)));
                }
                break :blk out.items;
            },
            .compute => |c| blk: {
                const up = try self.columns(c.upstream) orelse break :blk null;
                var out = try std.ArrayListUnmanaged(Exposed).initCapacity(self.arena, up.len + c.derived.len);
                out.appendSliceAssumeCapacity(up);
                for (c.derived) |d| out.appendAssumeCapacity(bareColumn(d.name));
                break :blk out.items;
            },
            .window => |w| blk: {
                const up = try self.columns(w.upstream) orelse break :blk null;
                var out = try std.ArrayListUnmanaged(Exposed).initCapacity(self.arena, up.len + w.calls.len);
                out.appendSliceAssumeCapacity(up);
                for (w.calls) |call| out.appendAssumeCapacity(bareColumn(call.output_name));
                break :blk out.items;
            },
            .group_by => |g| blk: {
                var out = try std.ArrayListUnmanaged(Exposed).initCapacity(self.arena, g.group_cols.len + g.aggs.len);
                for (g.group_cols) |c| out.appendAssumeCapacity(splitColumn(c));
                for (g.aggs) |a| out.appendAssumeCapacity(bareColumn(a.as));
                break :blk out.items;
            },
            .join => |j| blk: {
                const left = try self.columns(j.left) orelse break :blk null;
                const right = try self.columns(j.right) orelse break :blk null;
                break :blk try std.mem.concat(self.arena, Exposed, &.{ left, right });
            },
            else => null,
        };
        try self.exposed_columns.put(self.arena, op, result);
        return result;
    }

    fn scanColumns(self: *Finder, s: ir.Op.Scan) Allocator.Error!?[]const Exposed {
        if (pgcat.match(s.table, self.session.dialect) != null) return null;
        const cat = self.catalog orelse return null;
        const table = local.resolveTable(cat, self.session, s.table) catch return null;
        const qualifier = s.alias orelse s.table.name;
        const out = try self.arena.alloc(Exposed, table.schema.columns.len);
        for (table.schema.columns, out) |col, *o| o.* = .{ .qualifier = qualifier, .bare = col.name };
        return out;
    }

    /// Names `op` can expose, or null when they can't be enumerated.
    fn names(self: *Finder, op: *const ir.Op) Allocator.Error!?*const NameSet {
        if (self.exposed.get(op)) |memo| return memo;
        const result: ?*const NameSet = switch (op.*) {
            .scan => |s| try self.scanNames(s),
            .single_row => try self.newSet(),
            .series => |sr| blk: {
                const set = try self.newSet();
                try self.add(set, sr.column);
                break :blk set;
            },
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
        if (pgcat.match(s.table, self.session.dialect) != null) return null;
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

/// Whether `types.findColumn` refuses `name` over `columns` for matching
/// more than one: no column carries the name exactly, and two answer to it.
fn isAmbiguous(columns: []const Exposed, name: []const u8) bool {
    var matches: usize = 0;
    if (types.splitQualifiedName(name)) |split| {
        for (columns) |c| {
            const q = c.qualifier orelse {
                if (types.columnNameEql(c.bare, name) or types.columnNameEql(c.bare, split.bare)) return false;
                continue;
            };
            if (std.ascii.eqlIgnoreCase(q, split.qualifier) and types.columnNameEql(c.bare, split.bare)) matches += 1;
        }
        return matches > 1;
    }
    for (columns) |c| {
        if (!types.columnNameEql(c.bare, name)) continue;
        if (c.qualifier == null) return false;
        matches += 1;
    }
    return matches > 1;
}

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
