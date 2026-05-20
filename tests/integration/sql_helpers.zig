//! Shared SQL test helpers used by every integration test that goes
//! through `thindb.sql.parse → thindb.net.compile → iterate batches`.
//! Previously copy-pasted into every file; one source-of-truth here.

const std = @import("std");
const thindb = @import("thindb");

pub const RunResult = struct {
    arena: std.heap.ArenaAllocator,
    cq: thindb.net.CompiledQuery,

    pub fn deinit(self: *RunResult) void {
        self.cq.deinit();
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

/// Parse + compile a SQL statement against `db`. Caller owns the
/// returned `RunResult` and must `deinit` it.
pub fn runSql(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), sql);
    const cq = try thindb.net.compile(allocator, db, root);
    return .{ .arena = arena, .cq = cq };
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
