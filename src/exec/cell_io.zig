//! Per-cell append helpers shared by the join operators. Each
//! function takes a destination ColumnStore + one source row and
//! appends it (preserving validity for nullable destinations).
//!
//! Type switches over the column tag are isolated here so each
//! operator's inner loop reads as a single function call. Until
//! this module landed, four copies of these switches lived in
//! join.zig / smj.zig / nlj.zig / range_sweep.zig — extraction was
//! flagged in a comment inside smj.zig.

const std = @import("std");
const Allocator = std.mem.Allocator;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

pub fn appendNullTo(allocator: Allocator, dst: *ColumnStore) !void {
    switch (dst.data) {
        .int => |*l| try l.append(allocator, 0),
        .bigint => |*l| try l.append(allocator, 0),
        .boolean => |*l| try l.append(allocator, 0),
        .float => |*l| try l.append(allocator, 0),
        .double => |*l| try l.append(allocator, 0),
        .date => |*l| try l.append(allocator, 0),
        .datetime => |*l| try l.append(allocator, 0),
        .tinyint => |*l| try l.append(allocator, 0),
        .smallint => |*l| try l.append(allocator, 0),
        .largeint => |*l| try l.append(allocator, 0),
        .decimal64 => |*l| try l.append(allocator, 0),
        .decimal128 => |*l| try l.append(allocator, 0),
        .uuid => |*l| try l.append(allocator, 0),
        .varchar => |*s| try s.appendValue(allocator, ""),
        .string => |*s| try s.appendValue(allocator, ""),
        .char => |*s| try s.appendValue(allocator, ""),
    }
    try dst.appendValidBit(allocator, dst.data.rowCount() - 1, false);
}

pub inline fn appendOneFromView(
    allocator: Allocator,
    dst: *ColumnStore,
    src: ColumnView,
    row: u32,
) !void {
    const valid = src.isValid(row);
    switch (src.data) {
        .int => |s| try dst.data.int.append(allocator, s[row]),
        .bigint => |s| try dst.data.bigint.append(allocator, s[row]),
        .boolean => |s| try dst.data.boolean.append(allocator, s[row]),
        .float => |s| try dst.data.float.append(allocator, s[row]),
        .double => |s| try dst.data.double.append(allocator, s[row]),
        .date => |s| try dst.data.date.append(allocator, s[row]),
        .datetime => |s| try dst.data.datetime.append(allocator, s[row]),
        .tinyint => |s| try dst.data.tinyint.append(allocator, s[row]),
        .smallint => |s| try dst.data.smallint.append(allocator, s[row]),
        .largeint => |s| try dst.data.largeint.append(allocator, s[row]),
        .decimal64 => |s| try dst.data.decimal64.append(allocator, s[row]),
        .decimal128 => |s| try dst.data.decimal128.append(allocator, s[row]),
        .uuid => |s| try dst.data.uuid.append(allocator, s[row]),
        .varchar => |sv| try dst.data.varchar.appendValue(allocator, sv.rowBytes(row)),
        .string => |sv| try dst.data.string.appendValue(allocator, sv.rowBytes(row)),
        .char => |sv| try dst.data.char.appendValue(allocator, sv.rowBytes(row)),
    }
    if (dst.nulls != null) {
        try dst.appendValidBit(allocator, dst.data.rowCount() - 1, valid);
    }
}

pub inline fn appendOneFromBuild(
    allocator: Allocator,
    dst: *ColumnStore,
    src: *const ColumnStore,
    row: u32,
) !void {
    try appendOneFromView(allocator, dst, src.view(), row);
}

// ---------------------------------------------------------------------------
// Row-emit helpers shared by SMJ / NLJ / range-sweep. Each appends ONE
// output row from materialized left + right sides into `output_columns`.
// `right_kept_mask = null` means "keep every right column" (used by
// range-sweep which has no USING-key elision).
// ---------------------------------------------------------------------------

pub inline fn emitMatchedRow(
    allocator: Allocator,
    output_columns: []ColumnStore,
    left_columns: []const ColumnStore,
    left_row: u32,
    right_columns: []const ColumnStore,
    right_row: u32,
    right_kept_mask: ?[]const bool,
) !void {
    var out_idx: usize = 0;
    for (left_columns) |*col| {
        try appendOneFromBuild(allocator, &output_columns[out_idx], col, left_row);
        out_idx += 1;
    }
    // Hot path: hoist the mask check out of the inner loop. For
    // range-sweep / pure-Cartesian shapes (mask = null) this lets the
    // compiler unroll a tight no-branch emit; the mask case keeps the
    // per-column skip. Before this split, the per-iteration `?[]const bool`
    // optional-unwrap was a measurable 20-30% drag on high-output-rate
    // pure-range joins (90 → 70 M out/s).
    if (right_kept_mask) |m| {
        for (right_columns, 0..) |*col, i| {
            if (!m[i]) continue;
            try appendOneFromBuild(allocator, &output_columns[out_idx], col, right_row);
            out_idx += 1;
        }
    } else {
        for (right_columns) |*col| {
            try appendOneFromBuild(allocator, &output_columns[out_idx], col, right_row);
            out_idx += 1;
        }
    }
}

pub fn emitLeftOnlyRow(
    allocator: Allocator,
    output_columns: []ColumnStore,
    left_columns: []const ColumnStore,
    left_row: u32,
    right_kept_mask: []const bool,
) !void {
    var out_idx: usize = 0;
    for (left_columns) |*col| {
        try appendOneFromBuild(allocator, &output_columns[out_idx], col, left_row);
        out_idx += 1;
    }
    for (right_kept_mask) |kept| {
        if (!kept) continue;
        try appendNullTo(allocator, &output_columns[out_idx]);
        out_idx += 1;
    }
}

pub fn emitRightOnlyRow(
    allocator: Allocator,
    output_columns: []ColumnStore,
    right_columns: []const ColumnStore,
    right_row: u32,
    right_kept_mask: []const bool,
    left_col_count: usize,
) !void {
    var out_idx: usize = 0;
    var i: usize = 0;
    while (i < left_col_count) : (i += 1) {
        try appendNullTo(allocator, &output_columns[out_idx]);
        out_idx += 1;
    }
    for (right_columns, 0..) |*col, idx| {
        if (!right_kept_mask[idx]) continue;
        try appendOneFromBuild(allocator, &output_columns[out_idx], col, right_row);
        out_idx += 1;
    }
}
