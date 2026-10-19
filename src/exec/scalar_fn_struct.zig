//! STRUCT functions: the constructor `row(...)`, `struct(...)` and
//! `named_struct(...)` lower to, field access, and CAST to structs. A
//! struct's fields are flat columns while a kernel runs: the constructor
//! encodes one value per field (`nested.encodeStructColumn`) and field
//! access reads one back out (`nested.appendStructField`).

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;

const scalar_fn = @import("scalar_fn.zig");
const ResolvedOverload = scalar_fn.ResolvedOverload;
const array = @import("scalar_fn_array.zig");
const overload = array.overload;
const castable = array.castable;
const AsType = array.AsType;
const common = @import("scalar_fn_common.zig");
const nested = @import("nested.zig");
const Error = @import("exec.zig").Error;

pub fn isStructFnName(name: []const u8) bool {
    return std.mem.startsWith(u8, name, scalar_fn.STRUCT_VALUE_PREFIX) or std.mem.startsWith(u8, name, scalar_fn.STRUCT_FIELD_PREFIX);
}

/// The overload of a struct function, or null when `name` is none or its
/// arguments don't fit it.
pub fn resolveStruct(aa: Allocator, name: []const u8, arg_types: []const Type) Allocator.Error!?ResolvedOverload {
    if (std.mem.startsWith(u8, name, scalar_fn.NESTED_CAST_PREFIX)) return resolveCastToStruct(aa, name, arg_types);
    if (std.mem.startsWith(u8, name, scalar_fn.STRUCT_VALUE_PREFIX)) {
        const names = types.parseFieldNamesText(aa, name[scalar_fn.STRUCT_VALUE_PREFIX.len..]) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.InvalidTypeText => return null,
        };
        if (names.len != arg_types.len) return null;
        const fields = try aa.alloc(types.StructField, names.len);
        for (fields, names, arg_types) |*f, n, t| f.* = .{ .name = n, .type = t };
        return overload(aa, name, arg_types, try types.structOf(fields), valueKernel);
    }
    if (std.mem.startsWith(u8, name, scalar_fn.STRUCT_FIELD_PREFIX)) {
        // The name rides in the function's name for the result type, and in
        // its second argument for the kernel, which sees no function name.
        if (arg_types.len != 2 or !array.isText(arg_types[1])) return null;
        const fields = arg_types[0].structFields() orelse return null;
        const index = types.structFieldIndex(fields, name[scalar_fn.STRUCT_FIELD_PREFIX.len..]) orelse return null;
        return overload(aa, name, arg_types, fields[index].type, fieldKernel);
    }
    return null;
}

/// CAST to a struct: of another struct of as many fields, field by field in
/// order as each field's CAST converts it; of text or JSON, read as the
/// struct it spells (`nested.parseText`).
fn resolveCastToStruct(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (arg_types.len != 1) return null;
    const target = types.parseTypeText(name[scalar_fn.NESTED_CAST_PREFIX.len..]) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidTypeText => return null,
    };
    const to = target.structFields() orelse return null;
    const from = arg_types[0];
    if (from.structFields()) |fields| {
        if (fields.len != to.len) return null;
        for (fields, to) |f, t| if (!try castable(aa, f.type, t.type)) return null;
        return overload(aa, name, arg_types, target, castStructKernel);
    }
    if (from.isString()) return overload(aa, name, arg_types, target, array.parseNestedKernel);
    return null;
}

fn valueKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    try nested.encodeStructColumn(allocator, out, out_type.structFields().?, args, null, rows);
}

fn fieldKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    if (rows == 0) return;
    const name = common.stringViewOf(args[1]).rowBytes(0);
    const index = types.structFieldIndex(arg_types[0].structFields().?, name) orelse return Error.ComputeNoSuchOverload;
    try nested.appendStructField(allocator, out, args[0], index, rows);
}

fn castStructKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    const from = arg_types[0].structFields().?;
    const to = out_type.structFields().?;
    const raw = try allocator.alloc(ColumnStore, from.len);
    defer allocator.free(raw);
    var raw_ready: usize = 0;
    defer for (raw[0..raw_ready]) |*c| c.deinit(allocator);
    const converted = try allocator.alloc(AsType, from.len);
    defer allocator.free(converted);
    var converted_ready: usize = 0;
    defer for (converted[0..converted_ready]) |*c| c.deinit(allocator);
    const views = try allocator.alloc(ColumnView, from.len);
    defer allocator.free(views);
    for (from, to, 0..) |f, t, i| {
        raw[i] = try ColumnStore.init(allocator, f.type, true);
        raw_ready += 1;
        try nested.appendStructField(allocator, &raw[i], args[0], i, rows);
        converted[i] = try AsType.init(allocator, raw[i].view(), f.type, t.type, rows);
        converted_ready += 1;
        views[i] = converted[i].view;
    }
    try nested.encodeStructColumn(allocator, out, to, views, args[0].nulls, rows);
}
