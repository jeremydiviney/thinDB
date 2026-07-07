//! Shared SQL test helpers used by every integration test that goes
//! through `thindb.sql.parse → thindb.net.compile → iterate batches`.
//! Previously copy-pasted into every file; one source-of-truth here.

const std = @import("std");
const thindb = @import("thindb");

pub const RunResult = struct {
    arena: std.heap.ArenaAllocator,
    cq: thindb.net.CompiledQuery,
    /// SessionVars created by an earlier SET in the same batch (or
    /// during the final statement's compile). Owned by RunResult so
    /// it survives across substatements and gets cleaned up here.
    owned_vars: ?*thindb.SessionVars,
    backing_allocator: std.mem.Allocator,

    pub fn deinit(self: *RunResult) void {
        self.cq.deinit();
        thindb.net.CompiledQuery.freeSessionVars(self.backing_allocator, self.owned_vars);
        self.arena.deinit();
    }

    pub fn next(self: *RunResult) !?thindb.Batch {
        return self.cq.next();
    }

    pub fn outputSchema(self: *RunResult) []const thindb.Column {
        return self.cq.outputSchema();
    }

    pub fn affectedRows(self: *const RunResult) u64 {
        return self.cq.affectedRows();
    }
};

/// Parse + compile a SQL statement against `db`. If the input is a
/// multi-statement batch (`SET @x = 1; SELECT ...`), the non-final
/// statements are compiled + drained eagerly with the session
/// threaded through, so user-defined variables persist into the
/// final statement that the caller drains. Caller owns the returned
/// `RunResult` and must `deinit` it.
pub fn runSql(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), sql);

    if (root.* != .batch) {
        const cq = try thindb.net.compile(allocator, db, root);
        return .{
            .arena = arena,
            .cq = cq,
            .owned_vars = cq.sessionValue().vars,
            .backing_allocator = allocator,
        };
    }

    // Batch: drain everything except the last statement eagerly,
    // threading the Session (with any SET-created vars) forward.
    var session: thindb.Session = .{};
    const stmts = root.batch.statements;
    if (stmts.len == 0) return error.EmptyBatch;
    for (stmts[0 .. stmts.len - 1]) |stmt| {
        var cq = try thindb.net.compileWithSession(allocator, db, session, stmt);
        while (try cq.next()) |_| {}
        session = cq.sessionValue();
        cq.deinit();
    }
    const final = try thindb.net.compileWithSession(allocator, db, session, stmts[stmts.len - 1]);
    return .{
        .arena = arena,
        .cq = final,
        .owned_vars = final.sessionValue().vars,
        .backing_allocator = allocator,
    };
}

/// Like `runSql` but parses with the database's SQL-function and view
/// registries in scope, so a bare `FROM viewname` expands. Single-statement
/// only. Used by view/function tests; plain `runSql` (no context) suffices
/// for everything else.
pub fn runSqlCtx(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const cat = db.catalog.?;
    const root = try thindb.sql.parseWithContext(
        arena.allocator(),
        sql,
        .neutral,
        &cat.udfs,
        .{ .registry = &cat.sql_fns, .db = db.name, .views = &cat.views },
    );
    const cq = try thindb.net.compile(allocator, db, root);
    return .{
        .arena = arena,
        .cq = cq,
        .owned_vars = cq.sessionValue().vars,
        .backing_allocator = allocator,
    };
}

pub fn execCtx(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !void {
    var q = try runSqlCtx(allocator, db, sql);
    defer q.deinit();
    while (try q.next()) |_| {}
}

/// Convenience wrapper for fire-and-forget statements (DDL, INSERT
/// VALUES, etc.) — drains the batches and discards.
pub fn exec(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !void {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    while (try q.next()) |_| {}
}

/// Collect every value from a single-column BIGINT result into a slice
/// owned by `allocator`. Caller must `allocator.free` the returned slice.
pub fn collectBigints(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]i64 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        for (batch.values[0].data.bigint[0..batch.row_count]) |v| try out.append(allocator, v);
    }
    return out.toOwnedSlice(allocator);
}

/// Asserts that compiling `sql` against `db` returns `expected`. Useful
/// for tests that exercise parse-time and compile-time error paths.
pub fn expectRunError(
    allocator: std.mem.Allocator,
    db: anytype,
    sql: []const u8,
    expected: anyerror,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const parsed = thindb.sql.parse(arena.allocator(), sql);
    if (parsed) |root| {
        const cq_result = thindb.net.compile(allocator, db, root);
        if (cq_result) |cq_ok| {
            var cq = cq_ok;
            cq.deinit();
            return error.TestUnexpectedSuccess;
        } else |err| {
            try std.testing.expectEqual(expected, err);
        }
    } else |err| {
        try std.testing.expectEqual(expected, err);
    }
}
