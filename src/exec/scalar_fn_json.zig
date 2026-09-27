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
const dec = @import("scalar_fn_decimal.zig");
const Type = @import("../types.zig").Type;
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
// Building JSON from SQL values
// ---------------------------------------------------------------------------

/// Row `row` of an argument of type `t` as the JSONB value MySQL makes of it:
/// text is a JSON string (never parsed), a JSON argument embeds as is, a
/// DECIMAL keeps its digits, and a date or datetime becomes its text.
fn appendSqlValue(aa: Allocator, out: *std.ArrayList(u8), t: Type, arg: ColumnView, row: usize) !void {
    if (!arg.isValid(row)) return jb.appendNull(aa, out);
    if (t == .json) {
        const norm = try jb.normalize(aa, stringViewOf(arg).rowBytes(row));
        defer if (norm.owned) aa.free(norm.bytes);
        return out.appendSlice(aa, norm.bytes);
    }
    var buf: [64]u8 = undefined;
    switch (arg.data) {
        .varchar, .string, .char, .json => |sv| try jb.appendString(aa, out, sv.rowBytes(row)),
        .boolean => |s| try jb.appendBool(aa, out, s[row] != 0),
        inline .tinyint, .smallint, .int, .bigint => |s| try jb.appendInt(aa, out, s[row]),
        .largeint => |s| {
            if (std.math.cast(i64, s[row])) |v| return jb.appendInt(aa, out, v);
            const digits = std.fmt.bufPrint(&buf, "{d}", .{s[row]}) catch unreachable;
            try jb.appendNumber(aa, out, if (std.math.cast(u64, s[row]) != null) .unsigned else .decimal, digits);
        },
        inline .float, .double => |s| {
            const x: f64 = s[row];
            if (std.math.isFinite(x)) try jb.appendDouble(aa, out, x) else try jb.appendNull(aa, out);
        },
        inline .decimal64, .decimal128 => |s| {
            const scale: u8 = if (t.decimalSpec()) |spec| spec.s else 0;
            try jb.appendNumber(aa, out, .decimal, dec.formatDecimal(&buf, s[row], scale));
        },
        .date => |s| try jb.appendString(aa, out, try common.formatDate(&buf, s[row])),
        .datetime => |s| {
            const text = try common.formatDateTime(&buf, s[row]);
            if (std.mem.indexOfScalar(u8, text, '.') != null) return jb.appendString(aa, out, text);
            const len = text.len;
            @memcpy(buf[len..][0..7], ".000000");
            try jb.appendString(aa, out, buf[0 .. len + 7]);
        },
        .uuid => |s| {
            var bytes: [16]u8 = undefined;
            std.mem.writeInt(u128, &bytes, s[row], .big);
            const hex = std.fmt.bytesToHex(bytes, .lower);
            try jb.appendString(aa, out, hex[0..8] ++ "-" ++ hex[8..12] ++ "-" ++ hex[12..16] ++ "-" ++ hex[16..20] ++ "-" ++ hex[20..32]);
        },
    }
}

/// An object member name from row `row` of an argument of type `t`: text as
/// is, anything else as it prints (`1` → "1"). MySQL rejects a NULL name.
fn appendMemberName(aa: Allocator, name: *std.ArrayList(u8), scratch: *std.ArrayList(u8), t: Type, arg: ColumnView, row: usize) !void {
    if (!arg.isValid(row)) return error.JsonNullMemberName;
    if (t.isString() and t != .json) return name.appendSlice(aa, stringViewOf(arg).rowBytes(row));
    scratch.clearRetainingCapacity();
    try appendSqlValue(aa, scratch, t, arg, row);
    try jb.appendUnquoted(aa, name, scratch.items);
}

/// JSON_ARRAY(v, ...) → JSON. Never NULL: a NULL argument is a JSON null.
pub fn jsonArrayKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
    _ = out_type;
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    const offsets = try allocator.alloc(u32, args.len);
    defer allocator.free(offsets);
    var elems: std.ArrayList(u8) = .empty;
    defer elems.deinit(allocator);
    var doc: std.ArrayList(u8) = .empty;
    defer doc.deinit(allocator);
    for (0..row_count) |row| {
        elems.clearRetainingCapacity();
        doc.clearRetainingCapacity();
        for (args, arg_types, offsets) |arg, t, *off| {
            off.* = @intCast(elems.items.len);
            try appendSqlValue(allocator, &elems, t, arg, row);
        }
        try jb.appendArray(allocator, &doc, elems.items, offsets);
        try ss.appendValue(allocator, doc.items);
        try out.appendValidBit(allocator, base + row, true);
    }
}

const Span = struct { start: u32, end: u32 };

/// JSON_OBJECT(k, v, ...) → JSON. Never NULL; a repeated name keeps its last
/// value.
pub fn jsonObjectKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
    _ = out_type;
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    const pairs = args.len / 2;
    const members = try allocator.alloc(jb.Member, pairs);
    defer allocator.free(members);
    const name_spans = try allocator.alloc(Span, pairs);
    defer allocator.free(name_spans);
    const value_spans = try allocator.alloc(Span, pairs);
    defer allocator.free(value_spans);
    var names: std.ArrayList(u8) = .empty;
    defer names.deinit(allocator);
    var values: std.ArrayList(u8) = .empty;
    defer values.deinit(allocator);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var doc: std.ArrayList(u8) = .empty;
    defer doc.deinit(allocator);
    for (0..row_count) |row| {
        names.clearRetainingCapacity();
        values.clearRetainingCapacity();
        doc.clearRetainingCapacity();
        for (name_spans, value_spans, 0..) |*name_span, *value_span, p| {
            name_span.start = @intCast(names.items.len);
            try appendMemberName(allocator, &names, &scratch, arg_types[2 * p], args[2 * p], row);
            name_span.end = @intCast(names.items.len);
            value_span.start = @intCast(values.items.len);
            try appendSqlValue(allocator, &values, arg_types[2 * p + 1], args[2 * p + 1], row);
            value_span.end = @intCast(values.items.len);
        }
        for (members, name_spans, value_spans) |*m, name_span, value_span| m.* = .{
            .key = names.items[name_span.start..name_span.end],
            .val = values.items[value_span.start..value_span.end],
        };
        try jb.appendObject(allocator, &doc, members);
        try ss.appendValue(allocator, doc.items);
        try out.appendValidBit(allocator, base + row, true);
    }
}

// JSON_ARRAYAGG(v) is GROUP_CONCAT(__json_agg_element(v) SEPARATOR ''), which
// packs each row's JSONB value back to back, wrapped in __json_agg_array;
// JSON_OBJECTAGG packs a JSONB name string before each value the same way.
// JSONB values are self-delimiting, so the wrapper splits them without a
// separator, and a DECIMAL keeps its digits (a text round trip would read
// `1.50` back as a double).

/// __json_agg_element(v) → the JSONB bytes of `v` (JSON null for NULL).
pub fn jsonAggElementKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
    _ = out_type;
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var value: std.ArrayList(u8) = .empty;
    defer value.deinit(allocator);
    for (0..row_count) |row| {
        value.clearRetainingCapacity();
        try appendSqlValue(allocator, &value, arg_types[0], args[0], row);
        try ss.appendValue(allocator, value.items);
        try out.appendValidBit(allocator, base + row, true);
    }
}

/// __json_agg_member(k, v) → a JSONB string of the name, then `v`'s JSONB.
pub fn jsonAggMemberKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
    _ = out_type;
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var name: std.ArrayList(u8) = .empty;
    defer name.deinit(allocator);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var packed_member: std.ArrayList(u8) = .empty;
    defer packed_member.deinit(allocator);
    for (0..row_count) |row| {
        name.clearRetainingCapacity();
        packed_member.clearRetainingCapacity();
        try appendMemberName(allocator, &name, &scratch, arg_types[0], args[0], row);
        try jb.appendString(allocator, &packed_member, name.items);
        try appendSqlValue(allocator, &packed_member, arg_types[1], args[1], row);
        try ss.appendValue(allocator, packed_member.items);
        try out.appendValidBit(allocator, base + row, true);
    }
}

/// The next packed JSONB value at `bytes[pos..]`, checked whole: the packed
/// text is an ordinary string anyone can pass in.
fn nextPackedValue(bytes: []const u8, pos: usize) ![]const u8 {
    const len = jb.checkedValueLen(bytes[pos..]) orelse return error.JsonInvalid;
    const v = bytes[pos..][0..len];
    if (!jb.wellFormed(v)) return error.JsonInvalid;
    return v;
}

/// __json_agg_array(packed) → the JSON array of the packed values. NULL (no
/// rows) stays NULL.
pub fn jsonAggArrayKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    const sv = stringViewOf(args[0]);
    var offsets: std.ArrayList(u32) = .empty;
    defer offsets.deinit(allocator);
    var doc: std.ArrayList(u8) = .empty;
    defer doc.deinit(allocator);
    for (0..row_count) |row| {
        if (!args[0].isValid(row)) {
            try emitNull(allocator, ss, out, base + row);
            continue;
        }
        const elems = sv.rowBytes(row);
        offsets.clearRetainingCapacity();
        doc.clearRetainingCapacity();
        var pos: usize = 0;
        while (pos < elems.len) {
            try offsets.append(allocator, @intCast(pos));
            pos += (try nextPackedValue(elems, pos)).len;
        }
        try jb.appendArray(allocator, &doc, elems, offsets.items);
        try ss.appendValue(allocator, doc.items);
        try out.appendValidBit(allocator, base + row, true);
    }
}

/// __json_agg_object(packed) → the JSON object of the packed name/value
/// pairs, a repeated name keeping its last value. NULL (no rows) stays NULL.
pub fn jsonAggObjectKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    const sv = stringViewOf(args[0]);
    var members: std.ArrayList(jb.Member) = .empty;
    defer members.deinit(allocator);
    var doc: std.ArrayList(u8) = .empty;
    defer doc.deinit(allocator);
    for (0..row_count) |row| {
        if (!args[0].isValid(row)) {
            try emitNull(allocator, ss, out, base + row);
            continue;
        }
        const packed_members = sv.rowBytes(row);
        members.clearRetainingCapacity();
        doc.clearRetainingCapacity();
        var pos: usize = 0;
        while (pos < packed_members.len) {
            const name = try nextPackedValue(packed_members, pos);
            if (jb.tagOf(name) != .string) return error.JsonInvalid;
            pos += name.len;
            if (pos >= packed_members.len) return error.JsonInvalid;
            const value = try nextPackedValue(packed_members, pos);
            pos += value.len;
            try members.append(allocator, .{ .key = name[5..], .val = value });
        }
        try jb.appendObject(allocator, &doc, members.items);
        try ss.appendValue(allocator, doc.items);
        try out.appendValidBit(allocator, base + row, true);
    }
}

/// CAST(json AS CHAR) → the document's text as MySQL prints it.
pub fn jsonToTextKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    const sv = stringViewOf(args[0]);
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(allocator);
    for (0..row_count) |row| {
        text.clearRetainingCapacity();
        if (args[0].isValid(row)) {
            const bytes = sv.rowBytes(row);
            if (jb.normalize(allocator, bytes)) |norm| {
                defer if (norm.owned) allocator.free(norm.bytes);
                try jb.toText(allocator, &text, norm.bytes);
            } else |_| try text.appendSlice(allocator, bytes);
        }
        try ss.appendValue(allocator, text.items);
    }
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
