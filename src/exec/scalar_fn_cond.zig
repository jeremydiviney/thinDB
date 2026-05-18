//! Conditional / null-handling scalar kernels: coalesce, ifnull, nullif.
//! All three deal with row-level NULL propagation, so they're tagged
//! either `.absorbs` (coalesce / ifnull) or `.kernel_managed` (nullif)
//! in their registry entries — the kernels here mirror that behavior.

const std = @import("std");
const Allocator = std.mem.Allocator;

const common = @import("scalar_fn_common.zig");
const ColumnView = common.ColumnView;
const ColumnStore = common.ColumnStore;
const stringViewOf = common.stringViewOf;
const stringStoreOf = common.stringStoreOf;

// ---------------------------------------------------------------------------
// COALESCE — first non-null wins. Compute writes the bitmap (absorbs);
// kernels emit a placeholder value when all args are null.
// ---------------------------------------------------------------------------

pub fn coalesceStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0];
    const b = args[1];
    const a_sv = stringViewOf(a);
    const b_sv = stringViewOf(b);
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        if (a.isValid(i)) {
            try ss.appendValue(allocator, a_sv.rowBytes(i));
        } else if (b.isValid(i)) {
            try ss.appendValue(allocator, b_sv.rowBytes(i));
        } else {
            // Both null → output null. Compute pre-allocates the bitmap
            // if the column is nullable; we write a placeholder + leave
            // the validity bit cleared (Compute writes the bitmap).
            try ss.appendValue(allocator, "");
        }
    }
}

pub fn coalesceIntKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0];
    const b = args[1];
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        if (a.isValid(i)) {
            try out.data.int.append(allocator, a.data.int[i]);
        } else if (b.isValid(i)) {
            try out.data.int.append(allocator, b.data.int[i]);
        } else {
            try out.data.int.append(allocator, 0);
        }
    }
}

pub fn coalesceBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0];
    const b = args[1];
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        if (a.isValid(i)) {
            try out.data.bigint.append(allocator, a.data.bigint[i]);
        } else if (b.isValid(i)) {
            try out.data.bigint.append(allocator, b.data.bigint[i]);
        } else {
            try out.data.bigint.append(allocator, 0);
        }
    }
}

pub fn coalesceDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v: f64 = if (args[0].isValid(i)) args[0].data.double[i] else if (args[1].isValid(i)) args[1].data.double[i] else 0.0;
        try out.data.double.append(allocator, v);
    }
}

// ---------------------------------------------------------------------------
// IFNULL — structurally identical to a 2-arg coalesce; thin aliases so
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
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const v: f64 = if (args[0].isValid(i)) args[0].data.double[i] else args[1].data.double[i];
        try out.data.double.append(allocator, v);
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
        // Equality compares only when both sides are non-null. If a is
        // null, output stays null (mirrors a). If b is null, can't
        // match a, so output is a.
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
