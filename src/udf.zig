//! Trusted in-process Zig UDF descriptors and registry.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("types.zig");
const Type = types.Type;
const TypeTag = types.TypeTag;
const storage = @import("storage/storage.zig");
const ColumnView = storage.ColumnView;
const store = @import("engine/store.zig");
const ColumnStore = store.ColumnStore;

pub const Error = error{
    FunctionAlreadyExists,
    FunctionInvalidDefinition,
};

pub const NullStrategy = enum {
    propagates,
    absorbs,
    kernel_managed,
};

pub const Volatility = enum {
    immutable,
    stable,
    @"volatile",
};

pub const ScalarContext = struct {
    allocator: Allocator,
    user_data: ?*anyopaque = null,
};

pub const ScalarKernel = *const fn (
    ctx: *const ScalarContext,
    args: []const ColumnView,
    out: *ColumnStore,
    row_count: usize,
) anyerror!void;

pub const ScalarUdf = struct {
    name: []const u8,
    arg_types: []const Type,
    return_type: Type,
    null_strategy: NullStrategy = .propagates,
    volatility: Volatility = .@"volatile",
    kernel: ScalarKernel,
    user_data: ?*anyopaque = null,
};

pub const AggregateContext = struct {
    allocator: Allocator,
    user_data: ?*anyopaque = null,
};

pub const AggregateInit = *const fn (ctx: *const AggregateContext, state: *anyopaque) anyerror!void;
pub const AggregateUpdateOne = *const fn (
    ctx: *const AggregateContext,
    state: *anyopaque,
    args: []const ColumnView,
    row: usize,
) anyerror!void;
pub const AggregateUpdateBatch = *const fn (
    ctx: *const AggregateContext,
    state: *anyopaque,
    args: []const ColumnView,
    row_count: usize,
) anyerror!void;
pub const AggregateCombine = *const fn (
    ctx: *const AggregateContext,
    dst_state: *anyopaque,
    src_state: *const anyopaque,
) anyerror!void;
pub const AggregateFinalize = *const fn (
    ctx: *const AggregateContext,
    state: *anyopaque,
    out: *ColumnStore,
) anyerror!void;
pub const AggregateDestroy = *const fn (ctx: *const AggregateContext, state: *anyopaque) void;

pub const AggregateUdf = struct {
    name: []const u8,
    arg_types: []const Type,
    return_type: Type,
    state_size: usize,
    state_align: usize = 1,
    init: AggregateInit,
    update_one: AggregateUpdateOne,
    update_batch: ?AggregateUpdateBatch = null,
    combine: ?AggregateCombine = null,
    finalize: AggregateFinalize,
    destroy: ?AggregateDestroy = null,
    volatility: Volatility = .@"volatile",
    user_data: ?*anyopaque = null,
};

pub const ScalarEntry = struct {
    name: []const u8,
    arg_types: []const Type,
    return_type: Type,
    null_strategy: NullStrategy,
    volatility: Volatility,
    kernel: ScalarKernel,
    user_data: ?*anyopaque,
};

pub const AggregateEntry = struct {
    name: []const u8,
    arg_types: []const Type,
    return_type: Type,
    state_size: usize,
    state_align: usize,
    init: AggregateInit,
    update_one: AggregateUpdateOne,
    update_batch: ?AggregateUpdateBatch,
    combine: ?AggregateCombine,
    finalize: AggregateFinalize,
    destroy: ?AggregateDestroy,
    volatility: Volatility,
    user_data: ?*anyopaque,
};

pub const UdfRegistry = struct {
    allocator: Allocator,
    scalars: std.ArrayList(ScalarEntry) = .empty,
    aggregates: std.ArrayList(AggregateEntry) = .empty,

    pub fn init(allocator: Allocator) UdfRegistry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *UdfRegistry) void {
        for (self.scalars.items) |entry| {
            self.allocator.free(entry.name);
            self.allocator.free(entry.arg_types);
        }
        self.scalars.deinit(self.allocator);
        for (self.aggregates.items) |entry| {
            self.allocator.free(entry.name);
            self.allocator.free(entry.arg_types);
        }
        self.aggregates.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn registerScalar(self: *UdfRegistry, udf: ScalarUdf) !void {
        try validateName(udf.name);
        if (udf.arg_types.len > 16) return Error.FunctionInvalidDefinition;
        if (isReservedScalarName(udf.name)) return Error.FunctionAlreadyExists;
        if (self.scalarOverloadExists(udf.name, udf.arg_types)) return Error.FunctionAlreadyExists;

        const name = try lowerName(self.allocator, udf.name);
        errdefer self.allocator.free(name);
        const arg_types = try self.allocator.dupe(Type, udf.arg_types);
        errdefer self.allocator.free(arg_types);
        try self.scalars.append(self.allocator, .{
            .name = name,
            .arg_types = arg_types,
            .return_type = udf.return_type,
            .null_strategy = udf.null_strategy,
            .volatility = udf.volatility,
            .kernel = udf.kernel,
            .user_data = udf.user_data,
        });
    }

    pub fn registerAggregate(self: *UdfRegistry, udf: AggregateUdf) !void {
        try validateName(udf.name);
        if (udf.arg_types.len > 16 or udf.state_size == 0) return Error.FunctionInvalidDefinition;
        if (udf.state_align == 0 or udf.state_align > 16 or !std.math.isPowerOfTwo(udf.state_align)) {
            return Error.FunctionInvalidDefinition;
        }
        if (isReservedAggregateName(udf.name)) return Error.FunctionAlreadyExists;
        if (self.aggregateOverloadExists(udf.name, udf.arg_types)) return Error.FunctionAlreadyExists;

        const name = try lowerName(self.allocator, udf.name);
        errdefer self.allocator.free(name);
        const arg_types = try self.allocator.dupe(Type, udf.arg_types);
        errdefer self.allocator.free(arg_types);
        try self.aggregates.append(self.allocator, .{
            .name = name,
            .arg_types = arg_types,
            .return_type = udf.return_type,
            .state_size = udf.state_size,
            .state_align = udf.state_align,
            .init = udf.init,
            .update_one = udf.update_one,
            .update_batch = udf.update_batch,
            .combine = udf.combine,
            .finalize = udf.finalize,
            .destroy = udf.destroy,
            .volatility = udf.volatility,
            .user_data = udf.user_data,
        });
    }

    pub fn scalarEntries(self: *const UdfRegistry) []const ScalarEntry {
        return self.scalars.items;
    }

    pub fn aggregateEntries(self: *const UdfRegistry) []const AggregateEntry {
        return self.aggregates.items;
    }

    pub fn hasAggregateName(self: *const UdfRegistry, name: []const u8) bool {
        for (self.aggregates.items) |entry| {
            if (std.ascii.eqlIgnoreCase(entry.name, name)) return true;
        }
        return false;
    }

    pub fn resolveAggregateExact(
        self: *const UdfRegistry,
        name: []const u8,
        arg_types: []const Type,
    ) ?AggregateEntry {
        for (self.aggregates.items) |entry| {
            if (!std.ascii.eqlIgnoreCase(entry.name, name)) continue;
            if (sameTypeTags(entry.arg_types, arg_types)) return entry;
        }
        return null;
    }

    fn scalarOverloadExists(self: *const UdfRegistry, name: []const u8, arg_types: []const Type) bool {
        for (self.scalars.items) |entry| {
            if (!std.ascii.eqlIgnoreCase(entry.name, name)) continue;
            if (sameTypeTags(entry.arg_types, arg_types)) return true;
        }
        return false;
    }

    fn aggregateOverloadExists(self: *const UdfRegistry, name: []const u8, arg_types: []const Type) bool {
        for (self.aggregates.items) |entry| {
            if (!std.ascii.eqlIgnoreCase(entry.name, name)) continue;
            if (sameTypeTags(entry.arg_types, arg_types)) return true;
        }
        return false;
    }
};

pub fn sameTypeTags(a: []const Type, b: []const Type) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (@as(TypeTag, x) != @as(TypeTag, y)) return false;
    }
    return true;
}

fn validateName(name: []const u8) !void {
    if (name.len == 0) return Error.FunctionInvalidDefinition;
    for (name) |c| {
        if (!(std.ascii.isAlphanumeric(c) or c == '_')) return Error.FunctionInvalidDefinition;
    }
}

fn lowerName(allocator: Allocator, name: []const u8) ![]u8 {
    const out = try allocator.alloc(u8, name.len);
    for (name, out) |c, *dst| dst.* = std.ascii.toLower(c);
    return out;
}

fn isReservedAggregateName(name: []const u8) bool {
    const names = [_][]const u8{
        "count",        "sum",         "min",        "max",      "avg",
        "stddev_pop",   "stddev_samp", "var_pop",    "var_samp", "count_distinct",
        "group_concat", "string_agg",  "percentile", "max_by",
    };
    for (names) |n| if (std.ascii.eqlIgnoreCase(name, n)) return true;
    return false;
}

fn isReservedScalarName(name: []const u8) bool {
    const names = [_][]const u8{
        "upper",          "lower",             "ltrim",           "rtrim",           "trim",            "reverse",
        "length",         "octet_length",      "char_length",     "concat",          "substring",       "replace",
        "regexp_replace", "coalesce",          "ifnull",          "nullif",          "abs",             "ceil",
        "floor",          "round",             "sign",            "mod",             "add",             "sub",
        "mul",            "div",               "__narrow_bigint", "pow",             "sqrt",            "exp",
        "ln",             "log10",             "log2",            "greatest",        "least",           "truncate",
        "degrees",        "radians",           "atan2",           "dayofweek",       "dayofyear",       "quarter",
        "last_day",       "date_add",          "date_sub",        "date_add_months", "date_add_years",  "extract",
        "date_trunc",     "year",              "month",           "day",             "makedate",        "hour",
        "minute",         "second",            "datediff",        "unix_timestamp",  "from_unixtime",   "date_format",
        "now",            "current_timestamp", "current_date",    "to_bigint",       "to_double",       "to_int",
        "to_smallint",    "to_tinyint",        "to_largeint",     "to_boolean",      "to_date",         "to_datetime",
        "to_string",      "md5",               "sha1",            "sha256",          "crc32",           "hex",
        "unhex",          "to_base64",         "from_base64",     "lpad",            "rpad",            "repeat",
        "space",          "ascii",             "position",        "instr",           "substring_index", "strcmp",
        "lcase",          "ucase",             "power",           "ceiling",         "chr",
    };
    for (names) |n| if (std.ascii.eqlIgnoreCase(name, n)) return true;
    return false;
}

test "udf registry rejects duplicates and reserved builtins" {
    const testing = std.testing;
    var reg = UdfRegistry.init(testing.allocator);
    defer reg.deinit();

    const noop = struct {
        fn kernel(ctx: *const ScalarContext, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            _ = args;
            var i: usize = 0;
            while (i < row_count) : (i += 1) try out.data.int.append(ctx.allocator, 0);
        }
    }.kernel;

    try testing.expectError(Error.FunctionAlreadyExists, reg.registerScalar(.{
        .name = "upper",
        .arg_types = &.{.string},
        .return_type = .string,
        .kernel = noop,
    }));

    try reg.registerScalar(.{
        .name = "score_bucket",
        .arg_types = &.{.double},
        .return_type = .int,
        .kernel = noop,
    });
    try testing.expectError(Error.FunctionAlreadyExists, reg.registerScalar(.{
        .name = "SCORE_BUCKET",
        .arg_types = &.{.double},
        .return_type = .int,
        .kernel = noop,
    }));
}
