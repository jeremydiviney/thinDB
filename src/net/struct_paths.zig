//! Struct field paths: `s.a`, `t.s.a`, `s.a.b`. The parser reads a dotted
//! name as a qualified column, since only the schemas tell `table.col` from
//! `struct.field`. This pass derives each struct path a SELECT block reads
//! as a column named by the path's text, computed right above the block's
//! FROM source, so every operator above resolves it like any other column.
//!
//! The parser drops the first of three names (`schema.table.col`), so
//! `s.a.b` reaches here as `a.b`; a path whose first name no column of the
//! source has is read as the tail of a path from the one STRUCT column that
//! has it. Past three names the parser keeps them all.
//!
//! It engages only when the statement scans a table with a STRUCT column or
//! reads a name of four or more parts, which is a field path or no column
//! at all. It runs after the pushdown passes, which move operators by column
//! name and would otherwise read `s.a` as column `a` of table `s`.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ir = @import("../ir/ir.zig");
const types = @import("../types.zig");
const Type = types.Type;
const exec = @import("../exec/exec.zig");
const scalar_fn = @import("../exec/scalar_fn.zig");
const api = @import("../api/api.zig");
const local = @import("local.zig");

/// Rewrite the tree under `root` in place; `arena` owns the new nodes. A
/// block whose paths already have their columns is left as it is, so the
/// pass may run again over a subtree it rewrote.
pub fn derivePaths(arena: Allocator, catalog: *api.Catalog, session: api.Session, root: *ir.Op) !void {
    var pass: Pass = .{ .arena = arena, .catalog = catalog, .session = session };
    try pass.survey(root);
    if (!pass.engaged) pass.engaged = pass.namesUnqualifiedPath();
    if (!pass.engaged) return;
    pass.visited.clearRetainingCapacity();
    var slot = root;
    try pass.block(&slot);
}

const Pass = struct {
    arena: Allocator,
    catalog: *api.Catalog,
    session: api.Session,
    /// Every table name and alias the statement names, which a dotted
    /// name's first segment may be instead of a column.
    qualifiers: std.ArrayList([]const u8) = .empty,
    /// The database and schema of every table the statement scans, which
    /// qualify a dotted name only ahead of a table name (`db.t.s.a`).
    namespaces: std.ArrayList([]const u8) = .empty,
    /// Every `a.b` name the statement reads.
    dotted: std.ArrayList([]const u8) = .empty,
    visited: std.AutoHashMapUnmanaged(*const ir.Op, void) = .empty,
    engaged: bool = false,
    hidden_count: usize = 0,

    fn survey(self: *Pass, op: *ir.Op) !void {
        if (op.* == .materialize) {
            if ((try self.visited.getOrPut(self.arena, op)).found_existing) return;
            if (op.materialize.name) |n| try self.qualifiers.append(self.arena, n);
        }
        switch (op.*) {
            .scan => |s| {
                try self.qualifiers.append(self.arena, s.table.name);
                if (s.table.schema) |n| try self.qualifiers.append(self.arena, n);
                if (s.table.database) |n| try self.qualifiers.append(self.arena, n);
                if (s.alias) |a| try self.qualifiers.append(self.arena, a);
                if (self.catalog.tableNamespace(self.session, s.table.database, s.table.schema)) |ns| {
                    try self.namespaces.append(self.arena, ns.db);
                    try self.namespaces.append(self.arena, ns.schema);
                } else |_| {}
                if (self.tableColumns(s)) |columns| {
                    for (columns) |c| if (c.type.structFields() != null) {
                        self.engaged = true;
                    };
                }
            },
            .alias => |a| try self.qualifiers.append(self.arena, a.alias),
            .table_fn => |t| if (t.alias) |a| try self.qualifiers.append(self.arena, a),
            .expand => |e| {
                if (e.alias) |a| try self.qualifiers.append(self.arena, a);
                // Only the compiled schema shows whether an array's elements
                // are structs.
                if (e.generator == .unnest) self.engaged = true;
            },
            else => {},
        }
        if (!self.engaged) {
            var names: std.ArrayListUnmanaged([]const u8) = .empty;
            var derived: std.ArrayListUnmanaged([]const u8) = .empty;
            try self.collectNames(op, &names, &derived);
            for (names.items) |name| if (isDeepName(name)) {
                self.engaged = true;
            } else if (std.mem.indexOfScalar(u8, name, '.') != null and !isStar(name)) {
                try self.dotted.append(self.arena, name);
            };
        }
        var children: [2]**ir.Op = undefined;
        for (childSlots(op, &children)) |child| try self.survey(child.*);
        if (op.* == .table_fn) for (op.table_fn.inputs) |input| try self.survey(input);
    }

    /// Whether some `a.b` name's `a` names no table of the statement, so
    /// may be a CTE's or derived table's struct column and `b` its field.
    fn namesUnqualifiedPath(self: *const Pass) bool {
        for (self.dotted.items) |name| {
            const dot = std.mem.indexOfScalar(u8, name, '.').?;
            if (!contains(self.qualifiers.items, name[0..dot])) return true;
        }
        return false;
    }

    fn tableColumns(self: *Pass, s: ir.Op.Scan) ?[]const types.Column {
        const t = local.resolveTable(self.catalog, self.session, s.table) catch return null;
        return t.schema.columns;
    }

    /// The block whose top `slot` points at: its pipeline down to its FROM
    /// source, then the blocks under that source, then this block's paths.
    fn block(self: *Pass, slot: **ir.Op) anyerror!void {
        var chain: std.ArrayList(*ir.Op) = .empty;
        var at = slot;
        while (isPipeline(at.*)) {
            try chain.append(self.arena, at.*);
            at = upstreamSlot(at.*);
        }
        try self.source(at.*);

        // Ops above the block's lowest SELECT read its output, not the
        // source's columns.
        var first: usize = 0;
        for (chain.items, 0..) |op, i| if (op.* == .select) {
            first = i;
        };
        const reading = chain.items[first..];
        var names: std.ArrayListUnmanaged([]const u8) = .empty;
        var derived: std.ArrayListUnmanaged([]const u8) = .empty;
        for (reading) |op| try self.collectNames(op, &names, &derived);

        var renames: std.ArrayList(exec.predicate.ColRename) = .empty;
        var paths: std.ArrayList(ir.Derived) = .empty;
        for (names.items) |name| {
            if (contains(derived.items, name)) continue;
            const expr = try self.pathExpr(at.*, name) orelse {
                // `findColumn` would read the name as its last part.
                if (isDeepName(name) and self.columnOf(at.*, name) == null) return error.ColumnNotFound;
                continue;
            };
            // The column's name has no dot, which operators would read as a
            // table qualifier.
            const hidden = try std.fmt.allocPrint(self.arena, "__struct_path_{d}", .{self.hidden_count});
            self.hidden_count += 1;
            try derived.append(self.arena, name);
            try renames.append(self.arena, .{ .from = name, .to = hidden });
            try paths.append(self.arena, .{ .name = hidden, .expr = expr });
        }
        if (paths.items.len == 0) return;
        const compute = try self.arena.create(ir.Op);
        compute.* = .{ .compute = .{ .derived = paths.items, .upstream = at.* } };
        at.* = compute;

        var visible = renames.items;
        var i = reading.len;
        while (i > 0) {
            i -= 1;
            const op = reading[i];
            try self.renameOp(op, visible);
            // Above a GROUP BY only its keys are left.
            if (op.* == .group_by) {
                var kept: std.ArrayList(exec.predicate.ColRename) = .empty;
                for (visible) |r| if (contains(op.group_by.group_cols, r.to)) try kept.append(self.arena, r);
                visible = kept.items;
            }
        }
        // With no SELECT list, or a `*` in it, the paths' columns would
        // reach the block's output: drop them at its top.
        const top = chain.items[first];
        if (top.* == .select and !hasStar(top.select.columns)) return;
        const hidden = try self.arena.alloc([]const u8, paths.items.len);
        for (hidden, paths.items) |*h, p| h.* = p.name;
        const below = try self.arena.create(ir.Op);
        below.* = slot.*.*;
        slot.*.* = .{ .exclude = .{ .columns = hidden, .upstream = below } };
    }

    fn collectNames(self: *Pass, op: *const ir.Op, names: *std.ArrayListUnmanaged([]const u8), derived: *std.ArrayListUnmanaged([]const u8)) !void {
        const a = self.arena;
        switch (op.*) {
            .select, .exclude => |p| try names.appendSlice(a, p.columns),
            .filter => |f| try exec.predicate.collectColumnNames(a, names, f.predicate),
            .order_by => |o| for (o.specs) |s| try names.append(a, s.col),
            .group_by => |g| {
                try names.appendSlice(a, g.group_cols);
                for (g.aggs) |agg| {
                    if (agg.col) |c| try names.append(a, c);
                    if (agg.arg2_col) |c| try names.append(a, c);
                    try names.appendSlice(a, agg.udf_arg_cols);
                }
            },
            .compute => |c| for (c.derived) |d| {
                try derived.append(a, d.name);
                try exec.compute_op.collectColumnRefs(a, names, d.expr);
            },
            .window => |w| {
                for (w.specs) |s| {
                    try names.appendSlice(a, s.partition_by);
                    for (s.order_by) |o| try names.append(a, o.col);
                }
                for (w.calls) |call| {
                    try derived.append(a, call.output_name);
                    for (call.args) |arg| try exec.compute_op.collectColumnRefs(a, names, arg);
                }
            },
            else => {},
        }
    }

    /// `op` reading each path by its column's name. A SELECT still names
    /// its output by the path.
    fn renameOp(self: *Pass, op: *ir.Op, renames: []const exec.predicate.ColRename) !void {
        if (renames.len == 0) return;
        const a = self.arena;
        switch (op.*) {
            .select => |*p| {
                const columns = try renamedNames(a, p.columns, renames);
                const outputs = try a.alloc(?[]const u8, p.columns.len);
                for (outputs, p.columns, columns, 0..) |*o, before, after, i| {
                    const given = if (p.outputs) |outs| outs[i] else null;
                    o.* = given orelse if (before.ptr != after.ptr) before else null;
                }
                p.columns = columns;
                p.outputs = outputs;
            },
            .exclude => |*p| p.columns = try renamedNames(a, p.columns, renames),
            .filter => |*f| f.predicate = try exec.predicate.deepClonePredicateRenamed(a, f.predicate, renames),
            .order_by => |*o| {
                const specs = try a.dupe(ir.SortSpec, o.specs);
                for (specs) |*s| s.col = exec.predicate.renameOf(renames, s.col);
                o.specs = specs;
            },
            .group_by => |*g| {
                g.group_cols = try renamedNames(a, g.group_cols, renames);
                const aggs = try a.dupe(ir.AggSpec, g.aggs);
                for (aggs) |*agg| {
                    if (agg.col) |c| agg.col = exec.predicate.renameOf(renames, c);
                    if (agg.arg2_col) |c| agg.arg2_col = exec.predicate.renameOf(renames, c);
                    agg.udf_arg_cols = try renamedNames(a, agg.udf_arg_cols, renames);
                }
                g.aggs = aggs;
            },
            .compute => |*c| {
                const derived = try a.dupe(ir.Derived, c.derived);
                for (derived) |*d| d.expr = try exec.expr_mod.deepCloneRenamed(a, d.expr, renames);
                c.derived = derived;
            },
            .window => |*w| {
                const specs = try a.dupe(ir.WindowSpec, w.specs);
                for (specs) |*s| {
                    s.partition_by = try renamedNames(a, s.partition_by, renames);
                    const order = try a.dupe(ir.SortSpec, s.order_by);
                    for (order) |*o| o.col = exec.predicate.renameOf(renames, o.col);
                    s.order_by = order;
                }
                const calls = try a.dupe(ir.WindowCall, w.calls);
                for (calls) |*call| {
                    const args = try a.dupe(ir.Expr, call.args);
                    for (args) |*arg| arg.* = try exec.expr_mod.deepCloneRenamed(a, arg.*, renames);
                    call.args = args;
                }
                w.specs = specs;
                w.calls = calls;
            },
            else => {},
        }
    }

    fn source(self: *Pass, op: *ir.Op) anyerror!void {
        if (op.* == .materialize) {
            if ((try self.visited.getOrPut(self.arena, op)).found_existing) return;
        }
        var children: [2]**ir.Op = undefined;
        for (childSlots(op, &children)) |child| try self.block(child);
        if (op.* == .table_fn) for (@constCast(op.table_fn.inputs)) |*input| try self.block(input);
    }

    /// The expression `name` reads when it is a struct path over `src`'s
    /// columns; null when it is a plain (qualified) column.
    fn pathExpr(self: *Pass, src: *const ir.Op, name: []const u8) !?ir.Expr {
        if (std.mem.indexOfScalar(u8, name, '.') == null) return null;
        if (self.columnOf(src, name) != null) return null;
        var segments: std.ArrayList([]const u8) = .empty;
        var it = std.mem.splitScalar(u8, name, '.');
        while (it.next()) |s| {
            if (!types.isPlainIdentifier(s)) return null;
            try segments.append(self.arena, s);
        }
        // Leading table names qualify the struct column, which the column
        // reference keeps the last of.
        var path = segments.items;
        var root_start: usize = 0;
        var root = path[0];
        while (path.len > 1 and self.qualifies(path)) {
            if (path.len == 2) return null;
            root = name[root_start .. root_start + path[0].len + 1 + path[1].len];
            root_start += path[0].len + 1;
            path = path[1..];
        }
        if (self.columnOf(src, path[0])) |found| {
            if (found.type) |t| if (fieldPathType(t, path[1..]) == null) return null;
        } else {
            const owner = try self.pathOwner(src, path) orelse return null;
            const full = try self.arena.alloc([]const u8, path.len + 1);
            full[0] = owner;
            @memcpy(full[1..], path);
            path = full;
            root = owner;
        }
        var expr: ir.Expr = .{ .col_ref = root };
        for (path[1..]) |field| {
            const args = try self.arena.alloc(ir.Expr, 2);
            args[0] = expr;
            args[1] = .{ .lit = .{ .text = field } };
            const fn_name = try std.mem.concat(self.arena, u8, &.{ scalar_fn.STRUCT_FIELD_PREFIX, field });
            expr = .{ .call = .{ .fn_name = fn_name, .args = args } };
        }
        return expr;
    }

    fn qualifies(self: *const Pass, path: []const []const u8) bool {
        if (contains(self.qualifiers.items, path[0])) return true;
        var i: usize = 0;
        while (i + 2 < path.len and contains(self.namespaces.items, path[i])) : (i += 1) {
            if (contains(self.qualifiers.items, path[i + 1])) return true;
        }
        return false;
    }

    /// The one STRUCT column of `src` whose fields hold `path`, or null when
    /// none or several do.
    fn pathOwner(self: *Pass, src: *const ir.Op, path: []const []const u8) !?[]const u8 {
        var structs: std.ArrayList(types.Column) = .empty;
        try self.scannedStructs(src, &structs);
        var owner: ?[]const u8 = null;
        for (structs.items) |c| {
            if (fieldPathType(c.type, path) == null) continue;
            if (self.columnOf(src, c.name) == null) continue;
            if (owner) |o| if (!types.columnNameEql(o, c.name)) return null;
            owner = c.name;
        }
        return owner;
    }

    fn scannedStructs(self: *Pass, op: *const ir.Op, out: *std.ArrayList(types.Column)) !void {
        if (op.* == .scan) if (self.tableColumns(op.scan)) |columns| {
            for (columns) |c| if (c.type.structFields() != null) try out.append(self.arena, c);
        };
        var children: [2]**ir.Op = undefined;
        for (childSlots(@constCast(op), &children)) |child| try self.scannedStructs(child.*, out);
    }

    const Found = struct { type: ?Type };

    /// The column `name` of `op`'s output, its type when a scan's schema
    /// gives it; null when `op` has no such column.
    fn columnOf(self: *Pass, op: *const ir.Op, name: []const u8) ?Found {
        switch (op.*) {
            .scan => |s| {
                const columns = self.tableColumns(s) orelse return null;
                for (columns) |c| if (nameMatches(c.name, name)) return .{ .type = c.type };
                return null;
            },
            .alias => |a| return self.columnOf(a.upstream, name),
            .materialize => |m| return self.columnOf(m.upstream, name),
            .limit => |l| return self.columnOf(l.upstream, name),
            .order_by => |o| return self.columnOf(o.upstream, name),
            .filter => |f| return self.columnOf(f.upstream, name),
            .compute => |c| {
                for (c.derived) |d| if (nameMatches(d.name, name)) {
                    if (d.expr == .col_ref) return self.columnOf(c.upstream, d.expr.col_ref) orelse .{ .type = null };
                    return .{ .type = null };
                };
                return self.columnOf(c.upstream, name);
            },
            .window => |w| {
                for (w.calls) |call| if (nameMatches(call.output_name, name)) return .{ .type = null };
                return self.columnOf(w.upstream, name);
            },
            .select => |p| {
                for (p.columns, 0..) |c, i| {
                    if (isStar(c)) {
                        if (self.columnOf(p.upstream, name)) |found| return found;
                        continue;
                    }
                    const output = if (p.outputs) |outs| outs[i] orelse c else c;
                    if (nameMatches(output, name)) return self.columnOf(p.upstream, c) orelse .{ .type = null };
                }
                return null;
            },
            .exclude => |p| {
                if (contains(p.columns, name)) return null;
                return self.columnOf(p.upstream, name);
            },
            .group_by => |g| {
                for (g.group_cols) |c| if (nameMatches(c, name)) return self.columnOf(g.upstream, c) orelse .{ .type = null };
                for (g.aggs) |a| if (nameMatches(a.as, name)) return .{ .type = null };
                return null;
            },
            .join => |j| return self.columnOf(j.left, name) orelse self.columnOf(j.right, name),
            .set_union => |u| return self.columnOf(u.left, name),
            .expand => |e| {
                for (e.columns) |c| {
                    if (nameMatches(c, name) or qualifiedNameMatches(e.alias, c, name)) return .{ .type = null };
                }
                return if (e.lateral) self.columnOf(e.upstream, name) else null;
            },
            else => return null,
        }
    }
};

/// The type at `fields` under the struct `t`, matched case-insensitively.
fn fieldPathType(t: Type, fields: []const []const u8) ?Type {
    var at = t;
    for (fields) |f| {
        const struct_fields = at.structFields() orelse return null;
        at = struct_fields[types.structFieldIndex(struct_fields, f) orelse return null].type;
    }
    return at;
}

/// Ops that belong to the SELECT block above them; any other op is a FROM
/// source. A materialize over a window is the staged compiler's seam inside
/// the block.
fn isPipeline(op: *const ir.Op) bool {
    return switch (op.*) {
        .select, .exclude, .filter, .order_by, .group_by, .compute, .window, .limit => true,
        .materialize => |m| m.upstream.* == .window,
        else => false,
    };
}

fn upstreamSlot(op: *ir.Op) **ir.Op {
    return switch (op.*) {
        .select, .exclude => |*p| &p.upstream,
        .filter => |*f| &f.upstream,
        .order_by => |*o| &o.upstream,
        .group_by => |*g| &g.upstream,
        .compute => |*c| &c.upstream,
        .window => |*w| &w.upstream,
        .limit => |*l| &l.upstream,
        .materialize => |*m| &m.upstream,
        else => unreachable,
    };
}

/// The slots of `op`'s child ops (a join's two sides, a union's two arms,
/// a pipeline op's, an alias's or a statement's one source), in `buf`.
fn childSlots(op: *ir.Op, buf: *[2]**ir.Op) []const **ir.Op {
    switch (op.*) {
        .join => |*j| buf.* = .{ &j.left, &j.right },
        .set_union => |*u| buf.* = .{ &u.left, &u.right },
        else => {
            buf[0] = switch (op.*) {
                .alias => |*a| &a.upstream,
                .expand => |*s| &s.upstream,
                .create_table_as => |*c| &c.source,
                .insert_select => |*i| &i.source,
                .explain => |*e| &e.inner,
                .select, .exclude, .filter, .order_by, .group_by, .compute, .window, .limit, .materialize => upstreamSlot(op),
                else => return buf[0..0],
            };
            return buf[0..1];
        },
    }
    return buf[0..2];
}

fn renamedNames(arena: Allocator, names: []const []const u8, renames: []const exec.predicate.ColRename) ![]const []const u8 {
    const out = try arena.alloc([]const u8, names.len);
    for (out, names) |*o, n| o.* = exec.predicate.renameOf(renames, n);
    return out;
}

/// A name of four or more parts, which only a field path has: the parser
/// drops the first of three (`schema.table.col`).
fn isDeepName(name: []const u8) bool {
    return std.mem.count(u8, name, ".") >= 3;
}

fn hasStar(names: []const []const u8) bool {
    for (names) |n| if (isStar(n)) return true;
    return false;
}

fn isStar(name: []const u8) bool {
    return std.mem.eql(u8, name, "*") or std.mem.endsWith(u8, name, ".*");
}

fn nameMatches(column: []const u8, name: []const u8) bool {
    return types.columnNameEql(column, name) or types.columnNameEql(types.unqualifiedName(column), name);
}

/// Whether `name` is `qualifier.column`.
fn qualifiedNameMatches(qualifier: ?[]const u8, column: []const u8, name: []const u8) bool {
    const q = qualifier orelse return false;
    const dot = std.mem.lastIndexOfScalar(u8, name, '.') orelse return false;
    return types.columnNameEql(name[0..dot], q) and types.columnNameEql(name[dot + 1 ..], column);
}

fn contains(names: []const []const u8, name: []const u8) bool {
    for (names) |n| if (types.columnNameEql(n, name)) return true;
    return false;
}
