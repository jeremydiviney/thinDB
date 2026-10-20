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
//!
//! `findUnknownQualifier` serves the binder: `types.findColumn` reads a
//! qualified name its input has no column for as the bare name, so a block
//! over one table would answer `other.c` with its own `c`. A qualifier that
//! names nothing anywhere in the statement can only be a mistake, and the
//! caller refuses it. Its name set is a superset too, so a reported
//! qualifier really is unknown.

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

/// The first column reference `q.c` whose `q` nothing in the statement is
/// called: no table, schema, database or alias, no CTE, table function or
/// UNNEST, and no column or output a struct path could start from. Null
/// when there is none, or when some relation's names can't be listed.
pub fn findUnknownQualifier(
    arena: Allocator,
    catalog: ?*api.Catalog,
    session: api.Session,
    root: *const ir.Op,
) Allocator.Error!?Unbound {
    var finder: Finder = .{ .arena = arena, .catalog = catalog, .session = session, .udfs = null, .mode = .qualifier };
    if (!try finder.survey(root)) return null;
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

const Mode = enum { unbound, ambiguous, qualifier };

/// What a reference is checked against: the bare names an operator can
/// expose, its qualified columns, or every name in the statement.
const Scope = union(Mode) {
    unbound: *const NameSet,
    ambiguous: []const Exposed,
    qualifier: *const Known,

    fn rejects(self: Scope, name: []const u8) ?Unbound {
        if (isStar(name)) return null;
        return switch (self) {
            .unbound => |set| if (has(set, name)) null else .{ .column = name },
            .ambiguous => |columns| if (isAmbiguous(columns, name)) .{ .ambiguous_column = name } else null,
            .qualifier => |known| if (known.refuses(name)) .{ .column = name } else null,
        };
    }
};

/// The names a statement gives its relations, columns and outputs.
const Known = struct {
    /// Lower-cased: every relation's names and every column's and
    /// output's last part.
    names: NameSet = .empty,
    /// Lower-cased whole output names, which may read like `q.c`.
    outputs: NameSet = .empty,

    /// Whether `name` is `q.c` with a `q` nothing is called. A longer
    /// name is a struct path `struct_paths` judged, and a generated column
    /// (`a.__d0`) or an output named by its text is no reference.
    fn refuses(self: *const Known, name: []const u8) bool {
        const split = types.splitQualifiedName(name) orelse return false;
        if (!types.isPlainIdentifier(split.qualifier) or !types.isPlainIdentifier(split.bare)) return false;
        if (std.mem.startsWith(u8, split.bare, "__")) return false;
        if (hasFolded(&self.outputs, name)) return false;
        return !hasFolded(&self.names, split.qualifier);
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
    known: Known = .{},
    surveyed: std.AutoHashMapUnmanaged(*const ir.Op, void) = .empty,

    /// Gathers `known` from `op` and everything under it; false when a
    /// relation's columns can't be listed.
    fn survey(self: *Finder, op: *const ir.Op) Allocator.Error!bool {
        if ((try self.surveyed.getOrPut(self.arena, op)).found_existing) return true;
        const known = &self.known.names;
        switch (op.*) {
            .scan => |s| {
                try self.addTableNames(s.table);
                if (s.alias) |a| try self.add(known, a);
                if (pgcat.match(s.table, self.session.dialect) == null) if (self.catalog) |cat| {
                    if (local.resolveTable(cat, self.session, s.table)) |table| {
                        for (table.schema.columns) |col| try self.add(known, col.name);
                    } else |_| {}
                };
            },
            .file_scan => return false,
            .alias => |a| try self.add(known, a.alias),
            .materialize => |m| if (m.name) |n| try self.add(known, n),
            .table_fn => |t| {
                try self.add(known, t.name);
                if (t.alias) |a| try self.add(known, a);
            },
            .expand => |e| {
                try self.add(known, e.name());
                for (e.columns) |c| try self.add(known, c);
            },
            .select => |p| if (p.outputs) |outs| for (outs) |o| if (o) |n| try self.addOutput(n),
            .compute => |c| for (c.derived) |d| try self.addOutput(d.name),
            .window => |w| for (w.calls) |call| try self.addOutput(call.output_name),
            .group_by => |g| for (g.aggs) |a| try self.addOutput(a.as),
            .delete_op => |d| try self.addTableNames(d.table),
            .update_op => |u| try self.addTableNames(u.table),
            else => {},
        }
        return switch (op.*) {
            .limit => |l| self.survey(l.upstream),
            .select, .exclude => |p| self.survey(p.upstream),
            .filter => |f| self.survey(f.upstream),
            .order_by => |o| self.survey(o.upstream),
            .group_by => |g| self.survey(g.upstream),
            .compute => |c| self.survey(c.upstream),
            .materialize => |m| self.survey(m.upstream),
            .window => |w| self.survey(w.upstream),
            .alias => |a| self.survey(a.upstream),
            .expand => |e| self.survey(e.upstream),
            .join => |j| try self.survey(j.left) and try self.survey(j.right),
            .set_union => |u| try self.survey(u.left) and try self.survey(u.right),
            .create_table_as => |c| self.survey(c.source),
            .insert_select => |i| self.survey(i.source),
            .delete_op => |d| if (d.source) |s| self.survey(s) else true,
            .update_op => |u| if (u.source) |s| self.survey(s) else true,
            .explain => |e| self.survey(e.inner),
            .batch => |b| self.surveyAll(b.statements),
            .table_fn => |t| self.surveyAll(t.inputs),
            else => true,
        };
    }

    fn surveyAll(self: *Finder, ops: []const *ir.Op) Allocator.Error!bool {
        for (ops) |op| if (!try self.survey(op)) return false;
        return true;
    }

    fn addTableNames(self: *Finder, table: ir.TableRef) Allocator.Error!void {
        for ([_]?[]const u8{ table.name, table.schema, table.database }) |name| if (name) |n| try self.add(&self.known.names, n);
    }

    fn addOutput(self: *Finder, name: []const u8) Allocator.Error!void {
        try self.add(&self.known.names, name);
        try self.known.outputs.put(self.arena, try std.ascii.allocLowerString(self.arena, name), {});
    }

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
            .expand => |sr| self.walk(sr.upstream),
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
            .qualifier => .{ .qualifier = &self.known },
        };
    }

    /// The columns `op` exposes, qualified as compile qualifies them, or
    /// null when they can't be enumerated.
    fn columns(self: *Finder, op: *const ir.Op) Allocator.Error!?[]const Exposed {
        if (self.exposed_columns.get(op)) |memo| return memo;
        const result: ?[]const Exposed = switch (op.*) {
            .scan => |s| try self.scanColumns(s),
            .single_row => &.{},
            .expand => |e| blk: {
                var out: std.ArrayListUnmanaged(Exposed) = .empty;
                if (e.lateral) {
                    const up = try self.columns(e.upstream) orelse break :blk null;
                    for (up) |c| {
                        for (e.args) |arg| {
                            if (std.mem.eql(u8, arg, c.bare)) break;
                        } else try out.append(self.arena, c);
                    }
                }
                for (e.columns) |c| try out.append(self.arena, .{ .qualifier = e.alias, .bare = c });
                break :blk out.items;
            },
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
            .expand => |e| blk: {
                const set = try self.newSet();
                if (e.lateral) try self.addAll(set, try self.names(e.upstream) orelse break :blk null);
                for (e.columns) |c| try self.add(set, c);
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
    return hasFolded(set, lastSegment(name));
}

fn hasFolded(set: *const NameSet, name: []const u8) bool {
    var buf: [256]u8 = undefined;
    // Too long to fold on the stack: compare case-insensitively instead.
    if (name.len > buf.len) {
        var it = set.keyIterator();
        while (it.next()) |k| if (std.ascii.eqlIgnoreCase(k.*, name)) return true;
        return false;
    }
    return set.contains(std.ascii.lowerString(buf[0..name.len], name));
}
