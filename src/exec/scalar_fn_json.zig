//! JSON scalar functions. JSON documents are stored as JSONB (see
//! `json_binary.zig`) — a value's first byte is a type tag, so a stored
//! value is self-identifying and the kernels transparently accept either the
//! binary form (the norm) or raw JSON text (legacy rows / literals) via
//! `jb.normalize`. Path navigation is then a pointer-walk over the binary
//! tree rather than a re-parse of the text.
//!
//! Path grammar: `$` root, `.member`, `."quoted member"`, `[index]`,
//! `["member"]`. Wildcards are rejected. Paths are parsed once from the
//! first row (constant in every practical call) and reused across the batch.

const std = @import("std");
const Allocator = std.mem.Allocator;

const common = @import("scalar_fn_common.zig");
const ColumnView = common.ColumnView;
const ColumnStore = common.ColumnStore;
const stringViewOf = common.stringViewOf;
const stringStoreOf = common.stringStoreOf;

const jb = @import("json_binary.zig");
const Step = jb.Step;

// ---------------------------------------------------------------------------
// Path expression
// ---------------------------------------------------------------------------

const PathError = error{JsonPathInvalid} || Allocator.Error;

/// Parse a `$`-rooted path into steps. Member names are decoded (quotes
/// stripped for the `."..."` / `["..."]` forms). Wildcards are rejected.
fn parsePath(aa: Allocator, path: []const u8) PathError![]const Step {
    var steps: std.ArrayList(Step) = .empty;
    errdefer steps.deinit(aa);
    var i: usize = 0;
    while (i < path.len and (path[i] == ' ' or path[i] == '\t')) i += 1;
    if (i >= path.len or path[i] != '$') return PathError.JsonPathInvalid;
    i += 1;
    while (i < path.len) {
        switch (path[i]) {
            ' ', '\t' => i += 1,
            '.' => {
                i += 1;
                if (i >= path.len) return PathError.JsonPathInvalid;
                if (path[i] == '*') return PathError.JsonPathInvalid;
                if (path[i] == '"') {
                    const close = std.mem.indexOfScalarPos(u8, path, i + 1, '"') orelse
                        return PathError.JsonPathInvalid;
                    try steps.append(aa, .{ .member = path[i + 1 .. close] });
                    i = close + 1;
                } else {
                    const key_start = i;
                    while (i < path.len and path[i] != '.' and path[i] != '[') i += 1;
                    if (i == key_start) return PathError.JsonPathInvalid;
                    try steps.append(aa, .{ .member = path[key_start..i] });
                }
            },
            '[' => {
                i += 1;
                if (i < path.len and path[i] == '*') return PathError.JsonPathInvalid;
                if (i < path.len and path[i] == '"') {
                    const close = std.mem.indexOfScalarPos(u8, path, i + 1, '"') orelse
                        return PathError.JsonPathInvalid;
                    try steps.append(aa, .{ .member = path[i + 1 .. close] });
                    i = close + 1;
                    if (i >= path.len or path[i] != ']') return PathError.JsonPathInvalid;
                    i += 1;
                } else {
                    const num_start = i;
                    while (i < path.len and path[i] >= '0' and path[i] <= '9') i += 1;
                    if (i == num_start) return PathError.JsonPathInvalid;
                    const idx = std.fmt.parseInt(usize, path[num_start..i], 10) catch
                        return PathError.JsonPathInvalid;
                    if (i >= path.len or path[i] != ']') return PathError.JsonPathInvalid;
                    i += 1;
                    try steps.append(aa, .{ .index = idx });
                }
            },
            else => return PathError.JsonPathInvalid,
        }
    }
    return steps.toOwnedSlice(aa);
}

fn pathStepsFromArg(aa: Allocator, path_arg: ColumnView, row: usize) !?[]const Step {
    if (!path_arg.isValid(row)) return null;
    return try parsePath(aa, stringViewOf(path_arg).rowBytes(row));
}

// ---------------------------------------------------------------------------
// Kernels
// ---------------------------------------------------------------------------

/// JSON_EXTRACT(doc, path) → JSON (JSONB). SQL NULL when the input is NULL,
/// the document is invalid, or the path does not resolve.
pub fn jsonExtractKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    if (row_count == 0) return;
    const doc_sv = stringViewOf(args[0]);
    const steps = try pathStepsFromArg(allocator, args[1], 0);
    defer if (steps) |s| allocator.free(s);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var wrote = false;
        if (steps != null and args[0].isValid(i)) {
            const norm = jb.normalize(allocator, doc_sv.rowBytes(i)) catch {
                try emitNull(allocator, ss, out, base + i);
                continue;
            };
            defer if (norm.owned) allocator.free(norm.bytes);
            if (jb.navigate(norm.bytes, steps.?)) |v| {
                try ss.appendValue(allocator, v);
                try out.appendValidBit(allocator, base + i, true);
                wrote = true;
            }
        }
        if (!wrote) try emitNull(allocator, ss, out, base + i);
    }
}

/// JSON_VALUE(doc, path) / `->>` → text (unquoted). SQL NULL on missing path
/// / invalid doc / NULL input / a JSON null at the path.
pub fn jsonValueKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    if (row_count == 0) return;
    const doc_sv = stringViewOf(args[0]);
    const steps = try pathStepsFromArg(allocator, args[1], 0);
    defer if (steps) |s| allocator.free(s);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var wrote = false;
        if (steps != null and args[0].isValid(i)) {
            const norm = jb.normalize(allocator, doc_sv.rowBytes(i)) catch {
                try emitNull(allocator, ss, out, base + i);
                continue;
            };
            defer if (norm.owned) allocator.free(norm.bytes);
            if (jb.navigate(norm.bytes, steps.?)) |v| {
                if (jb.tagOf(v) != .null) {
                    scratch.clearRetainingCapacity();
                    try jb.appendUnquoted(allocator, &scratch, v);
                    try ss.appendValue(allocator, scratch.items);
                    try out.appendValidBit(allocator, base + i, true);
                    wrote = true;
                }
            }
        }
        if (!wrote) try emitNull(allocator, ss, out, base + i);
    }
}

/// JSON_UNQUOTE(v) → text. `v` may be JSONB (from JSON_EXTRACT) or raw JSON
/// text. Input NULL propagates.
pub fn jsonUnquoteKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    const sv = stringViewOf(args[0]);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        scratch.clearRetainingCapacity();
        if (args[0].isValid(i)) {
            const norm = jb.normalize(allocator, sv.rowBytes(i)) catch {
                try ss.appendValue(allocator, sv.rowBytes(i));
                continue;
            };
            defer if (norm.owned) allocator.free(norm.bytes);
            try jb.appendUnquoted(allocator, &scratch, norm.bytes);
        }
        try ss.appendValue(allocator, scratch.items);
    }
}

/// JSON_VALID(doc) → boolean. NULL input → SQL NULL.
pub fn jsonValidKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    if (row_count == 0) return;
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        if (!args[0].isValid(i)) {
            try out.data.boolean.append(allocator, 0);
            try out.appendValidBit(allocator, base + i, false);
            continue;
        }
        try out.data.boolean.append(allocator, @intFromBool(jb.isValid(allocator, sv.rowBytes(i))));
        try out.appendValidBit(allocator, base + i, true);
    }
}

/// JSON_TYPE(doc) → text. Invalid doc → SQL NULL.
pub fn jsonTypeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    if (row_count == 0) return;
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var wrote = false;
        if (args[0].isValid(i)) {
            const norm = jb.normalize(allocator, sv.rowBytes(i)) catch {
                try emitNull(allocator, ss, out, base + i);
                continue;
            };
            defer if (norm.owned) allocator.free(norm.bytes);
            try ss.appendValue(allocator, jb.typeName(norm.bytes));
            try out.appendValidBit(allocator, base + i, true);
            wrote = true;
        }
        if (!wrote) try emitNull(allocator, ss, out, base + i);
    }
}

/// JSON_LENGTH(doc) → int (element count; scalars = 1). Invalid → SQL NULL.
pub fn jsonLengthKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    if (row_count == 0) return;
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var wrote = false;
        if (args[0].isValid(i)) {
            const norm = jb.normalize(allocator, sv.rowBytes(i)) catch {
                try out.data.int.append(allocator, 0);
                try out.appendValidBit(allocator, base + i, false);
                continue;
            };
            defer if (norm.owned) allocator.free(norm.bytes);
            const len = jb.arrayLen(norm.bytes) orelse 1;
            try out.data.int.append(allocator, @intCast(len));
            try out.appendValidBit(allocator, base + i, true);
            wrote = true;
        }
        if (!wrote) {
            try out.data.int.append(allocator, 0);
            try out.appendValidBit(allocator, base + i, false);
        }
    }
}

/// JSON_CONTAINS(target, candidate) → boolean. NULL/invalid → SQL NULL.
pub fn jsonContainsKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    if (row_count == 0) return;
    const tgt = stringViewOf(args[0]);
    const cnd = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var wrote = false;
        if (args[0].isValid(i) and args[1].isValid(i)) {
            if (jb.normalize(allocator, tgt.rowBytes(i))) |tn| {
                defer if (tn.owned) allocator.free(tn.bytes);
                if (jb.normalize(allocator, cnd.rowBytes(i))) |cn| {
                    defer if (cn.owned) allocator.free(cn.bytes);
                    try out.data.boolean.append(allocator, @intFromBool(jb.contains(tn.bytes, cn.bytes)));
                    try out.appendValidBit(allocator, base + i, true);
                    wrote = true;
                } else |_| {}
            } else |_| {}
        }
        if (!wrote) {
            try out.data.boolean.append(allocator, 0);
            try out.appendValidBit(allocator, base + i, false);
        }
    }
}

/// JSON_KEYS(doc) → JSON array of the top-level object's keys. SQL NULL if
/// NULL / invalid / not an object.
pub fn jsonKeysKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    if (row_count == 0) return;
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var wrote = false;
        if (args[0].isValid(i)) {
            const norm = jb.normalize(allocator, sv.rowBytes(i)) catch {
                try emitNull(allocator, ss, out, base + i);
                continue;
            };
            defer if (norm.owned) allocator.free(norm.bytes);
            if (try jb.keysArray(allocator, norm.bytes)) |arr| {
                defer allocator.free(arr);
                try ss.appendValue(allocator, arr);
                try out.appendValidBit(allocator, base + i, true);
                wrote = true;
            }
        }
        if (!wrote) try emitNull(allocator, ss, out, base + i);
    }
}

/// CAST(x AS JSON) / to_json: parse + normalize to JSONB. Invalid → SQL NULL.
pub fn toJsonKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    if (row_count == 0) return;
    const sv = stringViewOf(args[0]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var wrote = false;
        if (args[0].isValid(i)) {
            const norm = jb.normalize(allocator, sv.rowBytes(i)) catch {
                try emitNull(allocator, ss, out, base + i);
                continue;
            };
            defer if (norm.owned) allocator.free(norm.bytes);
            try ss.appendValue(allocator, norm.bytes);
            try out.appendValidBit(allocator, base + i, true);
            wrote = true;
        }
        if (!wrote) try emitNull(allocator, ss, out, base + i);
    }
}

fn emitNull(allocator: Allocator, ss: anytype, out: *ColumnStore, row: usize) !void {
    try ss.appendValue(allocator, "");
    try out.appendValidBit(allocator, row, false);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "parsePath rejects wildcards and malformed" {
    const aa = std.testing.allocator;
    try std.testing.expectError(PathError.JsonPathInvalid, parsePath(aa, "a.b"));
    try std.testing.expectError(PathError.JsonPathInvalid, parsePath(aa, "$.*"));
    try std.testing.expectError(PathError.JsonPathInvalid, parsePath(aa, "$[*]"));
    try std.testing.expectError(PathError.JsonPathInvalid, parsePath(aa, "$."));
}

test "parsePath: members, indices, quoted names" {
    const aa = std.testing.allocator;
    const p = try parsePath(aa, "$.a.\"b c\"[2][\"d.e\"]");
    defer aa.free(p);
    try std.testing.expectEqual(@as(usize, 4), p.len);
    try std.testing.expectEqualStrings("a", p[0].member);
    try std.testing.expectEqualStrings("b c", p[1].member);
    try std.testing.expectEqual(@as(usize, 2), p[2].index);
    try std.testing.expectEqualStrings("d.e", p[3].member);
}
