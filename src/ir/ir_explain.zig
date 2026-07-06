//! EXPLAIN — render an IR Op tree as indented text. Binary ops (Join,
//! SetUnion) recurse into both branches so the output reflects the
//! full plan shape.
//!
//! Output style: one line per operator, two-space indent per depth
//! level, each line "OpName <key=value …>" with the most useful spec
//! fields. The goal is debuggability, not a stable machine-readable
//! schema — call formats are intentionally lightweight.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ir = @import("ir.zig");
const Op = ir.Op;
const Expr = ir.Expr;
const FrameBound = ir.FrameBound;
const TableRef = ir.TableRef;
const DdlOp = ir.DdlOp;
const ShowOp = ir.ShowOp;
const CopyOp = ir.CopyOp;
const InsertOp = ir.InsertOp;
const windowFuncName = ir.windowFuncName;

const types = @import("../types.zig");
const Value = types.Value;

const exec_predicate = @import("../exec/predicate.zig");
const PredicateExpr = exec_predicate.PredicateExpr;
const PredicateOp = exec_predicate.PredicateOp;

pub fn explain(allocator: Allocator, out: *std.ArrayList(u8), root: Op) !void {
    try explainOp(allocator, out, root, 0);
}

fn explainOp(allocator: Allocator, out: *std.ArrayList(u8), op: Op, depth: usize) !void {
    try writeIndent(allocator, out, depth);
    switch (op) {
        .single_row => try out.appendSlice(allocator, "SingleRow\n"),
        .explain => |e| {
            try out.appendSlice(allocator, "Explain\n");
            try explainOp(allocator, out, e.inner.*, depth + 1);
        },
        .scan => |s| {
            try out.appendSlice(allocator, "Scan ");
            try writeTableRef(allocator, out, s.table);
            if (s.alias) |a| {
                try out.appendSlice(allocator, " AS ");
                try out.appendSlice(allocator, a);
            }
            try out.append(allocator, '\n');
        },
        .file_scan => |f| {
            try out.appendSlice(allocator, "FileScan ");
            try out.appendSlice(allocator, @tagName(f.format));
            try out.appendSlice(allocator, " '");
            try out.appendSlice(allocator, f.path);
            try out.append(allocator, '\'');
            if (f.alias) |a| {
                try out.appendSlice(allocator, " AS ");
                try out.appendSlice(allocator, a);
            }
            try out.append(allocator, '\n');
        },
        .alias => |a| {
            try out.appendSlice(allocator, "Alias ");
            try out.appendSlice(allocator, a.alias);
            try out.append(allocator, '\n');
            try explainOp(allocator, out, a.upstream.*, depth + 1);
        },
        .table_fn => |t| {
            try out.appendSlice(allocator, "TableFn ");
            try out.appendSlice(allocator, t.name);
            try out.appendSlice(allocator, if (t.partition_by.len == 0) " [global]" else " [partitioned]");
            try out.append(allocator, '\n');
            try explainOp(allocator, out, t.input.*, depth + 1);
        },
        .limit => |l| {
            var buf: [48]u8 = undefined;
            const s = try std.fmt.bufPrint(&buf, "Limit n={d}\n", .{l.n});
            try out.appendSlice(allocator, s);
            try explainOp(allocator, out, l.upstream.*, depth + 1);
        },
        .select => |p| {
            try out.appendSlice(allocator, "Select [");
            try writeProjectNames(allocator, out, p);
            try out.appendSlice(allocator, "]\n");
            try explainOp(allocator, out, p.upstream.*, depth + 1);
        },
        .exclude => |p| {
            try out.appendSlice(allocator, "Exclude [");
            try writeJoinedNames(allocator, out, p.columns);
            try out.appendSlice(allocator, "]\n");
            try explainOp(allocator, out, p.upstream.*, depth + 1);
        },
        .filter => |f| {
            try out.appendSlice(allocator, "Filter (");
            try explainPredicate(allocator, out, f.predicate);
            try out.appendSlice(allocator, ")\n");
            try explainOp(allocator, out, f.upstream.*, depth + 1);
        },
        .order_by => |o| {
            try out.appendSlice(allocator, "OrderBy [");
            for (o.specs, 0..) |s, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try out.appendSlice(allocator, s.col);
                try out.appendSlice(allocator, if (s.desc) " DESC" else " ASC");
            }
            try out.appendSlice(allocator, "]\n");
            try explainOp(allocator, out, o.upstream.*, depth + 1);
        },
        .group_by => |g| {
            try out.appendSlice(allocator, "GroupBy keys=[");
            try writeJoinedNames(allocator, out, g.group_cols);
            try out.appendSlice(allocator, "] aggs=[");
            for (g.aggs, 0..) |a, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                if (a.func == .udf and a.udf_name != null) {
                    try out.appendSlice(allocator, a.udf_name.?);
                } else {
                    try out.appendSlice(allocator, @tagName(a.func));
                }
                try out.append(allocator, '(');
                if (a.udf_arg_cols.len > 0) {
                    try writeJoinedNames(allocator, out, a.udf_arg_cols);
                } else if (a.col) |c| {
                    try out.appendSlice(allocator, c);
                } else {
                    try out.append(allocator, '*');
                }
                try out.appendSlice(allocator, ") AS ");
                try out.appendSlice(allocator, a.as);
            }
            try out.appendSlice(allocator, "]\n");
            try explainOp(allocator, out, g.upstream.*, depth + 1);
        },
        .compute => |c| {
            try out.appendSlice(allocator, "Compute [");
            for (c.derived, 0..) |d, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try out.appendSlice(allocator, d.name);
                try out.appendSlice(allocator, " := ");
                try explainExpr(allocator, out, d.expr);
            }
            try out.appendSlice(allocator, "]\n");
            try explainOp(allocator, out, c.upstream.*, depth + 1);
        },
        .join => |j| {
            var buf: [128]u8 = undefined;
            const s = try std.fmt.bufPrint(
                &buf,
                "Join algorithm={s} type={s} on=[",
                .{ @tagName(j.algorithm), @tagName(j.join_type) },
            );
            try out.appendSlice(allocator, s);
            for (j.on, 0..) |kp, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try out.appendSlice(allocator, kp.left);
                try out.append(allocator, '=');
                try out.appendSlice(allocator, kp.right);
            }
            try out.append(allocator, ']');
            if (j.ranges.len > 0) {
                try out.appendSlice(allocator, " ranges=[");
                for (j.ranges, 0..) |rg, i| {
                    if (i > 0) try out.appendSlice(allocator, ", ");
                    try out.appendSlice(allocator, rg.left);
                    try out.append(allocator, ' ');
                    try out.appendSlice(allocator, opSymbol(rg.op));
                    try out.append(allocator, ' ');
                    try out.appendSlice(allocator, rg.right);
                }
                try out.append(allocator, ']');
            }
            if (j.extra_predicate) |pred| {
                try out.appendSlice(allocator, " extra=(");
                try explainPredicate(allocator, out, pred);
                try out.append(allocator, ')');
            }
            try out.append(allocator, '\n');
            try explainOp(allocator, out, j.left.*, depth + 1);
            try explainOp(allocator, out, j.right.*, depth + 1);
        },
        .materialize => |m| {
            try out.appendSlice(allocator, "Materialize\n");
            try explainOp(allocator, out, m.upstream.*, depth + 1);
        },
        .ddl => |d| try explainDdl(allocator, out, d),
        .show => |s| try explainShow(allocator, out, s),
        .insert => |i| try explainInsert(allocator, out, i),
        .batch => |b| {
            var buf: [48]u8 = undefined;
            const s = try std.fmt.bufPrint(&buf, "Batch n={d}\n", .{b.statements.len});
            try out.appendSlice(allocator, s);
            for (b.statements) |sub| try explainOp(allocator, out, sub.*, depth + 1);
        },
        .copy => |c| try explainCopy(allocator, out, c),
        .window => |w| {
            try out.appendSlice(allocator, "Window");
            for (w.calls, 0..) |call, i| {
                try out.appendSlice(allocator, if (i == 0) " [" else ", ");
                try out.appendSlice(allocator, call.output_name);
                try out.appendSlice(allocator, " = ");
                try out.appendSlice(allocator, windowFuncName(call.func));
                try out.append(allocator, '(');
                for (call.args, 0..) |a, ai| {
                    if (ai > 0) try out.appendSlice(allocator, ", ");
                    try explainExpr(allocator, out, a);
                }
                try out.append(allocator, ')');
                if (call.ignore_nulls) try out.appendSlice(allocator, " IGNORE NULLS");
                try out.appendSlice(allocator, " OVER #");
                var buf: [12]u8 = undefined;
                const s = try std.fmt.bufPrint(&buf, "{d}", .{call.spec_idx});
                try out.appendSlice(allocator, s);
            }
            if (w.calls.len > 0) try out.append(allocator, ']');
            try out.append(allocator, '\n');
            for (w.specs, 0..) |sp, si| {
                try writeIndent(allocator, out, depth);
                var indent_buf: [64]u8 = undefined;
                const indent = try std.fmt.bufPrint(&indent_buf, "  spec #{d}: ", .{si});
                try out.appendSlice(allocator, indent);
                try out.appendSlice(allocator, "PARTITION BY ");
                if (sp.partition_by.len == 0)
                    try out.appendSlice(allocator, "()")
                else
                    try writeJoinedNames(allocator, out, sp.partition_by);
                try out.appendSlice(allocator, " ORDER BY ");
                if (sp.order_by.len == 0) {
                    try out.appendSlice(allocator, "()");
                } else {
                    for (sp.order_by, 0..) |ob, oi| {
                        if (oi > 0) try out.appendSlice(allocator, ", ");
                        try out.appendSlice(allocator, ob.col);
                        try out.appendSlice(allocator, if (ob.desc) " DESC" else " ASC");
                    }
                }
                try out.appendSlice(allocator, " FRAME ");
                try out.appendSlice(allocator, switch (sp.frame.kind) {
                    .rows => "ROWS",
                    .range => "RANGE",
                    .groups => "GROUPS",
                });
                try out.appendSlice(allocator, " BETWEEN ");
                try explainFrameBound(allocator, out, sp.frame.start);
                try out.appendSlice(allocator, " AND ");
                try explainFrameBound(allocator, out, sp.frame.end);
                try out.append(allocator, '\n');
            }
            try explainOp(allocator, out, w.upstream.*, depth + 1);
        },
        .set_union => |u| {
            try out.appendSlice(allocator, if (u.all) "UnionAll\n" else "Union\n");
            try explainOp(allocator, out, u.left.*, depth + 1);
            try explainOp(allocator, out, u.right.*, depth + 1);
        },
        .create_table_as => |c| {
            try out.appendSlice(allocator, "CreateTableAs ");
            try writeTableRef(allocator, out, c.table);
            try out.append(allocator, '\n');
            try explainOp(allocator, out, c.source.*, depth + 1);
        },
        .insert_select => |i| {
            try out.appendSlice(allocator, if (i.mode == .replace) "ReplaceSelect " else "InsertSelect ");
            try writeTableRef(allocator, out, i.table);
            try out.append(allocator, '\n');
            try explainOp(allocator, out, i.source.*, depth + 1);
        },
        .set_var => |sv| {
            try out.appendSlice(allocator, "SetVar @");
            try out.appendSlice(allocator, sv.name);
            try out.append(allocator, '\n');
        },
        .delete_op => |d| {
            try out.appendSlice(allocator, "Delete ");
            try writeTableRef(allocator, out, d.table);
            if (d.predicate) |p| {
                try out.appendSlice(allocator, " WHERE ");
                try explainPredicate(allocator, out, p);
            }
            try out.append(allocator, '\n');
        },
        .update_op => |u| {
            try out.appendSlice(allocator, "Update ");
            try writeTableRef(allocator, out, u.table);
            try out.appendSlice(allocator, " SET ");
            for (u.assignments, 0..) |asn, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try out.appendSlice(allocator, asn.col);
                try out.appendSlice(allocator, " = ");
                try explainExpr(allocator, out, asn.value);
            }
            if (u.predicate) |p| {
                try out.appendSlice(allocator, " WHERE ");
                try explainPredicate(allocator, out, p);
            }
            try out.append(allocator, '\n');
        },
    }
}

fn explainFrameBound(allocator: Allocator, out: *std.ArrayList(u8), b: FrameBound) !void {
    switch (b) {
        .unbounded_preceding => try out.appendSlice(allocator, "UNBOUNDED PRECEDING"),
        .preceding => |n| {
            var buf: [32]u8 = undefined;
            const s = try std.fmt.bufPrint(&buf, "{d} PRECEDING", .{n});
            try out.appendSlice(allocator, s);
        },
        .current_row => try out.appendSlice(allocator, "CURRENT ROW"),
        .following => |n| {
            var buf: [32]u8 = undefined;
            const s = try std.fmt.bufPrint(&buf, "{d} FOLLOWING", .{n});
            try out.appendSlice(allocator, s);
        },
        .unbounded_following => try out.appendSlice(allocator, "UNBOUNDED FOLLOWING"),
    }
}

fn explainCopy(allocator: Allocator, out: *std.ArrayList(u8), c: CopyOp) !void {
    try out.appendSlice(allocator, switch (c.direction) {
        .from_stdin => "CopyFromStdin ",
        .to_stdout => "CopyToStdout ",
    });
    try writeTableRef(allocator, out, c.table);
    if (c.columns) |cols| {
        try out.appendSlice(allocator, " cols=[");
        try writeJoinedNames(allocator, out, cols);
        try out.append(allocator, ']');
    }
    try out.append(allocator, '\n');
}

fn explainInsert(allocator: Allocator, out: *std.ArrayList(u8), i: InsertOp) !void {
    try out.appendSlice(allocator, if (i.mode == .replace) "Replace " else "Insert ");
    try writeTableRef(allocator, out, i.table);
    if (i.columns) |cols| {
        try out.appendSlice(allocator, " cols=[");
        try writeJoinedNames(allocator, out, cols);
        try out.append(allocator, ']');
    }
    var buf: [32]u8 = undefined;
    const s = try std.fmt.bufPrint(&buf, " rows={d}\n", .{i.rows.len});
    try out.appendSlice(allocator, s);
}

fn writeTableRef(allocator: Allocator, out: *std.ArrayList(u8), ref: TableRef) !void {
    if (ref.database) |d| {
        try out.appendSlice(allocator, d);
        try out.append(allocator, '.');
    }
    if (ref.schema) |s| {
        try out.appendSlice(allocator, s);
        try out.append(allocator, '.');
    }
    try out.appendSlice(allocator, ref.name);
}

fn explainDdl(allocator: Allocator, out: *std.ArrayList(u8), d: DdlOp) !void {
    switch (d) {
        .create_database => |n| try writeAll(allocator, out, "CreateDatabase ", n, "\n"),
        .drop_database => |n| try writeAll(allocator, out, "DropDatabase ", n, "\n"),
        .create_schema => |n| try writeAll(allocator, out, "CreateSchema ", n, "\n"),
        .drop_schema => |n| try writeAll(allocator, out, "DropSchema ", n, "\n"),
        .use_schema => |n| try writeAll(allocator, out, "Use ", n, "\n"),
        .use_database_schema => |p| {
            try out.appendSlice(allocator, "Use ");
            try out.appendSlice(allocator, p.database);
            try out.append(allocator, '.');
            try out.appendSlice(allocator, p.schema);
            try out.append(allocator, '\n');
        },
        .create_table => |ct| {
            if (ct.is_temp) {
                try out.appendSlice(allocator, "CreateTempTable ");
            } else {
                try out.appendSlice(allocator, "CreateTable ");
            }
            try writeTableRef(allocator, out, ct.table);
            if (ct.if_not_exists) try out.appendSlice(allocator, " if_not_exists");
            try out.appendSlice(allocator, " cols=[");
            for (ct.columns, 0..) |c, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try out.appendSlice(allocator, c.name);
                try out.append(allocator, ' ');
                try out.appendSlice(allocator, @tagName(c.column_type));
                if (c.nullable) try out.appendSlice(allocator, " NULL");
            }
            try out.appendSlice(allocator, "] key=[");
            try writeJoinedNames(allocator, out, ct.order_key);
            try out.appendSlice(allocator, "]\n");
        },
        .drop_table => |dt| {
            try out.appendSlice(allocator, "DropTable ");
            try writeTableRef(allocator, out, dt.table);
            if (dt.if_exists) try out.appendSlice(allocator, " if_exists");
            try out.append(allocator, '\n');
        },
        .rename_table => |rt| {
            try out.appendSlice(allocator, "RenameTable ");
            try writeTableRef(allocator, out, rt.from);
            try out.appendSlice(allocator, " to ");
            try writeTableRef(allocator, out, rt.to);
            try out.append(allocator, '\n');
        },
        .alter_table_add_column => |at| {
            try out.appendSlice(allocator, "AlterTableAddColumn ");
            try writeTableRef(allocator, out, at.table);
            try out.append(allocator, ' ');
            try out.appendSlice(allocator, at.column.name);
            try out.append(allocator, ' ');
            try out.appendSlice(allocator, @tagName(at.column.column_type));
            if (at.column.nullable) try out.appendSlice(allocator, " NULL");
            try out.append(allocator, '\n');
        },
        .truncate_table => |ref| {
            try out.appendSlice(allocator, "TruncateTable ");
            try writeTableRef(allocator, out, ref);
            try out.append(allocator, '\n');
        },
        .create_sql_function => |cf| {
            try out.appendSlice(allocator, "CreateSqlFunction ");
            try out.appendSlice(allocator, cf.name);
            try out.print(allocator, " params={d}\n", .{cf.param_names.len});
        },
        .drop_sql_function => |df| {
            try writeAll(allocator, out, "DropSqlFunction ", df.name, "\n");
        },
        .create_zig_function => |zf| {
            try writeAll(allocator, out, "CreateZigFunction ", zf.name, "\n");
        },
    }
}

fn explainShow(allocator: Allocator, out: *std.ArrayList(u8), s: ShowOp) !void {
    switch (s) {
        .databases => try out.appendSlice(allocator, "ShowDatabases\n"),
        .schemas => |db| {
            try out.appendSlice(allocator, "ShowSchemas");
            if (db) |name| {
                try out.appendSlice(allocator, " from=");
                try out.appendSlice(allocator, name);
            }
            try out.append(allocator, '\n');
        },
        .tables => |ref| {
            try out.appendSlice(allocator, "ShowTables ");
            try writeTableRef(allocator, out, ref);
            try out.append(allocator, '\n');
        },
    }
}

fn writeIndent(allocator: Allocator, out: *std.ArrayList(u8), depth: usize) !void {
    var i: usize = 0;
    while (i < depth) : (i += 1) try out.appendSlice(allocator, "  ");
}

fn writeAll(allocator: Allocator, out: *std.ArrayList(u8), a: []const u8, b: []const u8, c: []const u8) !void {
    try out.appendSlice(allocator, a);
    try out.appendSlice(allocator, b);
    try out.appendSlice(allocator, c);
}

fn writeJoinedNames(allocator: Allocator, out: *std.ArrayList(u8), names: []const []const u8) !void {
    for (names, 0..) |n, i| {
        if (i > 0) try out.appendSlice(allocator, ", ");
        try out.appendSlice(allocator, n);
    }
}

fn writeProjectNames(allocator: Allocator, out: *std.ArrayList(u8), p: Op.Project) !void {
    for (p.columns, 0..) |n, i| {
        if (i > 0) try out.appendSlice(allocator, ", ");
        try out.appendSlice(allocator, n);
        if (p.outputs) |outs| {
            if (outs[i]) |alias| {
                try out.appendSlice(allocator, " AS ");
                try out.appendSlice(allocator, alias);
            }
        }
    }
}

fn opSymbol(op: PredicateOp) []const u8 {
    return switch (op) {
        .eq => "=",
        .neq => "!=",
        .lt => "<",
        .lte => "<=",
        .gt => ">",
        .gte => ">=",
    };
}

fn explainExpr(allocator: Allocator, out: *std.ArrayList(u8), e: Expr) anyerror!void {
    switch (e) {
        .col_ref => |name| try out.appendSlice(allocator, name),
        .lit => |v| try writeValue(allocator, out, v),
        .null_lit => try out.appendSlice(allocator, "NULL"),
        .call => |c| {
            try out.appendSlice(allocator, c.fn_name);
            try out.append(allocator, '(');
            for (c.args, 0..) |arg, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try explainExpr(allocator, out, arg);
            }
            try out.append(allocator, ')');
        },
        .case => |cs| {
            try out.appendSlice(allocator, "CASE");
            for (cs.branches) |br| {
                try out.appendSlice(allocator, " WHEN ");
                try explainPredicate(allocator, out, br.cond);
                try out.appendSlice(allocator, " THEN ");
                try explainExpr(allocator, out, br.then);
            }
            if (cs.else_branch) |eb| {
                try out.appendSlice(allocator, " ELSE ");
                try explainExpr(allocator, out, eb.*);
            }
            try out.appendSlice(allocator, " END");
        },
        .scalar_subquery => try out.appendSlice(allocator, "(SELECT …)"),
        .exists_subquery => try out.appendSlice(allocator, "EXISTS(SELECT …)"),
        .var_ref => |name| {
            try out.append(allocator, '@');
            try out.appendSlice(allocator, name);
        },
    }
}

fn explainPredicate(allocator: Allocator, out: *std.ArrayList(u8), p: PredicateExpr) anyerror!void {
    switch (p) {
        .leaf => |l| {
            try out.appendSlice(allocator, l.col);
            try out.append(allocator, ' ');
            try out.appendSlice(allocator, opSymbol(l.op));
            try out.append(allocator, ' ');
            try writeValue(allocator, out, l.val);
        },
        .day_leaf => |l| {
            try out.appendSlice(allocator, "DAY(");
            try out.appendSlice(allocator, l.col);
            try out.appendSlice(allocator, ") ");
            try out.appendSlice(allocator, opSymbol(l.op));
            try out.append(allocator, ' ');
            try writeValue(allocator, out, l.val);
        },
        .leaf_col_col => |lc| {
            try out.appendSlice(allocator, lc.left);
            try out.append(allocator, ' ');
            try out.appendSlice(allocator, opSymbol(lc.op));
            try out.append(allocator, ' ');
            try out.appendSlice(allocator, lc.right);
        },
        .is_null => |col| try writeAll(allocator, out, col, " IS NULL", ""),
        .is_not_null => |col| try writeAll(allocator, out, col, " IS NOT NULL", ""),
        .like => |lp| {
            try out.appendSlice(allocator, lp.col);
            try out.appendSlice(allocator, " LIKE '");
            try out.appendSlice(allocator, lp.pattern);
            try out.append(allocator, '\'');
        },
        .@"and" => |children| try joinPredicates(allocator, out, children, " AND "),
        .@"or" => |children| try joinPredicates(allocator, out, children, " OR "),
        .not => |child| {
            try out.appendSlice(allocator, "NOT (");
            try explainPredicate(allocator, out, child.*);
            try out.append(allocator, ')');
        },
        .scalar_subquery => |sq| {
            try out.appendSlice(allocator, sq.col);
            try out.append(allocator, ' ');
            try out.appendSlice(allocator, opSymbol(sq.op));
            try out.appendSlice(allocator, " (SELECT …)");
        },
        .exists_subquery => try out.appendSlice(allocator, "EXISTS (SELECT …)"),
        .always => |b| try out.appendSlice(allocator, if (b) "TRUE" else "FALSE"),
        .unknown => try out.appendSlice(allocator, "UNKNOWN"),
        .in_subquery => |s| {
            try out.appendSlice(allocator, s.col);
            try out.appendSlice(allocator, if (s.negate) " NOT IN (SELECT …)" else " IN (SELECT …)");
        },
        .in_set => |s| {
            try out.appendSlice(allocator, s.col);
            try out.appendSlice(allocator, if (s.negate) " NOT IN [" else " IN [");
            for (s.values, 0..) |v, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try writeValue(allocator, out, v);
            }
            try out.append(allocator, ']');
        },
        .correlated_set => |s| {
            try out.append(allocator, '(');
            for (s.outer_cols, 0..) |c, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try out.appendSlice(allocator, c);
            }
            try out.appendSlice(allocator, if (s.negate) ") NOT IN [" else ") IN [");
            var buf: [16]u8 = undefined;
            const s_count = try std.fmt.bufPrint(&buf, "{d} rows", .{s.rows.len});
            try out.appendSlice(allocator, s_count);
            try out.append(allocator, ']');
        },
        .correlated_scalar => |s| {
            try out.appendSlice(allocator, s.outer_compared);
            try out.append(allocator, ' ');
            try out.appendSlice(allocator, opSymbol(s.op));
            try out.appendSlice(allocator, " agg(");
            for (s.outer_keys, 0..) |c, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try out.appendSlice(allocator, c);
            }
            try out.appendSlice(allocator, ")");
            var buf: [16]u8 = undefined;
            const s_count = try std.fmt.bufPrint(&buf, " [{d} keys]", .{s.rows.len});
            try out.appendSlice(allocator, s_count);
        },
        .correlated_range => |s| {
            try out.appendSlice(allocator, if (s.negate) "NOT EXISTS range(" else "EXISTS range(");
            try out.appendSlice(allocator, s.outer_range_col);
            try out.append(allocator, ' ');
            try out.appendSlice(allocator, opSymbol(s.op));
            try out.appendSlice(allocator, " inner");
            if (s.outer_keys.len > 0) {
                try out.appendSlice(allocator, " by [");
                for (s.outer_keys, 0..) |c, i| {
                    if (i > 0) try out.appendSlice(allocator, ", ");
                    try out.appendSlice(allocator, c);
                }
                try out.append(allocator, ']');
            }
            try out.append(allocator, ')');
            var buf: [24]u8 = undefined;
            const s_count = try std.fmt.bufPrint(&buf, " [{d} groups]", .{s.groups.len});
            try out.appendSlice(allocator, s_count);
        },
        .leaf_var => |v| {
            try out.appendSlice(allocator, v.col);
            try out.append(allocator, ' ');
            try out.appendSlice(allocator, opSymbol(v.op));
            try out.appendSlice(allocator, " @");
            try out.appendSlice(allocator, v.var_name);
        },
    }
}

fn joinPredicates(allocator: Allocator, out: *std.ArrayList(u8), children: []const PredicateExpr, sep: []const u8) anyerror!void {
    for (children, 0..) |c, i| {
        if (i > 0) try out.appendSlice(allocator, sep);
        try explainPredicate(allocator, out, c);
    }
}

fn writeValue(allocator: Allocator, out: *std.ArrayList(u8), v: Value) anyerror!void {
    var buf: [64]u8 = undefined;
    switch (v) {
        .int => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .bigint => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .smallint => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .tinyint => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .largeint => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .float => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .double => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .boolean => |x| try out.appendSlice(allocator, if (x) "true" else "false"),
        .text => |s| {
            try out.append(allocator, '\'');
            try out.appendSlice(allocator, s);
            try out.append(allocator, '\'');
        },
        .date => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "date({d})", .{x})),
        .datetime => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "datetime({d})", .{x})),
        .decimal64 => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "decimal64({d})", .{x})),
        .decimal128 => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "decimal128({d})", .{x})),
        .uuid => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "uuid({d})", .{x})),
    }
}
