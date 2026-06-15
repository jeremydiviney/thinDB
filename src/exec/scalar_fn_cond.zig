//! Conditional / null-handling scalar kernels: coalesce, ifnull, nullif, if.
//! Conditional functions deal with row-level NULL propagation, so they're tagged
//! either `.absorbs` (coalesce / ifnull) or `.kernel_managed` (nullif / if)
//! in their registry entries.

const std = @import("std");
const Allocator = std.mem.Allocator;

const common = @import("scalar_fn_common.zig");
const ColumnView = common.ColumnView;
const ColumnStore = common.ColumnStore;
const stringViewOf = common.stringViewOf;
const stringStoreOf = common.stringStoreOf;

// ---------------------------------------------------------------------------
// COALESCE ? first non-null wins. Compute writes the bitmap (absorbs);
// kernels emit a placeholder value when all args are null.
// ---------------------------------------------------------------------------

pub fn coalesceStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var chosen: ?[]const u8 = null;
        for (args) |arg| {
            if (!arg.isValid(i)) continue;
            chosen = stringViewOf(arg).rowBytes(i);
            break;
        }
        try ss.appendValue(allocator, chosen orelse "");
    }
}

pub fn coalesceIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var v: i32 = 0;
        for (args) |arg| {
            if (arg.isValid(i)) {
                v = arg.data.int[i];
                break;
            }
        }
        try out.data.int.append(allocator, v);
    }
}

pub fn coalesceBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var v: i64 = 0;
        for (args) |arg| {
            if (arg.isValid(i)) {
                v = arg.data.bigint[i];
                break;
            }
        }
        try out.data.bigint.append(allocator, v);
    }
}

pub fn coalesceDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var v: f64 = 0.0;
        for (args) |arg| {
            if (arg.isValid(i)) {
                v = arg.data.double[i];
                break;
            }
        }
        try out.data.double.append(allocator, v);
    }
}

pub fn coalesceBooleanKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var v: u8 = 0;
        for (args) |arg| {
            if (arg.isValid(i)) {
                v = arg.data.boolean[i];
                break;
            }
        }
        try out.data.boolean.append(allocator, v);
    }
}

pub fn coalesceDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var v: i32 = 0;
        for (args) |arg| {
            if (arg.isValid(i)) {
                v = arg.data.date[i];
                break;
            }
        }
        try out.data.date.append(allocator, v);
    }
}

pub fn coalesceDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        var v: i64 = 0;
        for (args) |arg| {
            if (arg.isValid(i)) {
                v = arg.data.datetime[i];
                break;
            }
        }
        try out.data.datetime.append(allocator, v);
    }
}

// ---------------------------------------------------------------------------
// IFNULL ? structurally identical to a 2+ arg coalesce; thin aliases so
// callers reading the registry see the user-facing name they expect.
// ---------------------------------------------------------------------------

pub fn ifnullStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    return coalesceStringKernel(allocator, args, out, row_count);
}

pub fn ifnullIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    return coalesceIntKernel(allocator, args, out, row_count);
}

pub fn ifnullBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    return coalesceBigintKernel(allocator, args, out, row_count);
}

pub fn ifnullDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    return coalesceDoubleKernel(allocator, args, out, row_count);
}

pub fn ifnullBooleanKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    return coalesceBooleanKernel(allocator, args, out, row_count);
}

pub fn ifnullDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    return coalesceDateKernel(allocator, args, out, row_count);
}

pub fn ifnullDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    return coalesceDatetimeKernel(allocator, args, out, row_count);
}

// ---------------------------------------------------------------------------
// IF(cond, then, else). NULL conditions are treated as false, matching MySQL.
// The selected branch's validity becomes the output validity.
// ---------------------------------------------------------------------------

inline fn conditionTrue(cond: ColumnView, row: usize) bool {
    return cond.isValid(row) and cond.data.boolean[row] != 0;
}

pub fn ifStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const then_sv = stringViewOf(args[1]);
    const else_sv = stringViewOf(args[2]);
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const chosen_idx: usize = if (conditionTrue(args[0], i)) 1 else 2;
        const valid = args[chosen_idx].isValid(i);
        if (valid) {
            try ss.appendValue(allocator, if (chosen_idx == 1) then_sv.rowBytes(i) else else_sv.rowBytes(i));
        } else {
            try ss.appendValue(allocator, "");
        }
        try out.appendValidBit(allocator, base + i, valid);
    }
}

pub fn ifIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const chosen_idx: usize = if (conditionTrue(args[0], i)) 1 else 2;
        const valid = args[chosen_idx].isValid(i);
        try out.data.int.append(allocator, if (valid) args[chosen_idx].data.int[i] else 0);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

pub fn ifBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const chosen_idx: usize = if (conditionTrue(args[0], i)) 1 else 2;
        const valid = args[chosen_idx].isValid(i);
        try out.data.bigint.append(allocator, if (valid) args[chosen_idx].data.bigint[i] else 0);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

pub fn ifDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const chosen_idx: usize = if (conditionTrue(args[0], i)) 1 else 2;
        const valid = args[chosen_idx].isValid(i);
        try out.data.double.append(allocator, if (valid) args[chosen_idx].data.double[i] else 0.0);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

pub fn ifBooleanKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const chosen_idx: usize = if (conditionTrue(args[0], i)) 1 else 2;
        const valid = args[chosen_idx].isValid(i);
        try out.data.boolean.append(allocator, if (valid) args[chosen_idx].data.boolean[i] else 0);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

pub fn ifDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const chosen_idx: usize = if (conditionTrue(args[0], i)) 1 else 2;
        const valid = args[chosen_idx].isValid(i);
        try out.data.date.append(allocator, if (valid) args[chosen_idx].data.date[i] else 0);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

pub fn ifDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const chosen_idx: usize = if (conditionTrue(args[0], i)) 1 else 2;
        const valid = args[chosen_idx].isValid(i);
        try out.data.datetime.append(allocator, if (valid) args[chosen_idx].data.datetime[i] else 0);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

// ---------------------------------------------------------------------------
// NULLIF(a, b) returns NULL when a == b, otherwise a. Can produce nulls
// from non-null inputs, so the kernel manages the bitmap directly
// (registered with `null_strategy = .kernel_managed`).
// ---------------------------------------------------------------------------

pub fn nullifIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0];
    const b = args[1];
    const base = out.data.rowCount();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const a_valid = a.isValid(i);
        const av = a.data.int[i];
        const bv = b.data.int[i];
        const should_null = a_valid and b.isValid(i) and av == bv;
        try out.data.int.append(allocator, if (should_null) 0 else av);
        try out.appendValidBit(allocator, base + i, a_valid and !should_null);
    }
}

pub fn nullifBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0];
    const b = args[1];
    const base = out.data.rowCount();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const a_valid = a.isValid(i);
        const av = a.data.bigint[i];
        const bv = b.data.bigint[i];
        const should_null = a_valid and b.isValid(i) and av == bv;
        try out.data.bigint.append(allocator, if (should_null) 0 else av);
        try out.appendValidBit(allocator, base + i, a_valid and !should_null);
    }
}

pub fn nullifStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0];
    const b = args[1];
    const a_sv = stringViewOf(a);
    const b_sv = stringViewOf(b);
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const a_valid = a.isValid(i);
        const a_bytes = a_sv.rowBytes(i);
        const matches = a_valid and b.isValid(i) and std.mem.eql(u8, a_bytes, b_sv.rowBytes(i));
        if (matches or !a_valid) {
            try ss.appendValue(allocator, "");
        } else {
            try ss.appendValue(allocator, a_bytes);
        }
        try out.appendValidBit(allocator, base + i, a_valid and !matches);
    }
}
