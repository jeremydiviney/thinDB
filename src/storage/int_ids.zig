//! Fixed-width table ids (DESIGN.md §4.7): whether a segment's fixed-width
//! column stores the table id of each row's value in place of the value,
//! and each row's id when it does.
//!
//! A segment's column takes ids when its values are wide past the ids by
//! `min_gain_bits` and each value it adds to the table's list serves enough
//! of its rows: `wide_reuse` rows when the values are wide past the ids by
//! `wide_gain_bits`, else `narrow_reuse`. Its ids are then as small as the
//! list allows, and repeat as often as its values, so its blocks shrink by
//! about the bits saved per row while the list costs each new value once.
//! A column of values new to every row (an event id) takes none. The rule
//! looks at one segment at a time, so a column may take ids in some
//! segments and not others; readers see values either way.

const std = @import("std");
const Allocator = std.mem.Allocator;

const column = @import("column.zig");
const format = @import("format.zig");
const table_strings = @import("table_strings.zig");
const ColumnView = column.ColumnView;

const min_gain_bits = 8;
const wide_gain_bits = 16;
const wide_reuse = 2;
const narrow_reuse = 8;

/// Bits that hold every value below `n`.
fn bitsBelow(n: u64) u64 {
    return if (n <= 1) 0 else 64 - @clz(n - 1);
}

/// The admission `TableStrings.resolve` asks of a segment's column: of
/// `rows` non-NULL rows whose values span `value_bits` bits.
const Admit = struct {
    rows: u64,
    value_bits: u64,

    pub fn admits(self: Admit, new_count: usize, id_count: u32) bool {
        const id_bits = bitsBelow(@as(u64, id_count) + new_count);
        if (self.value_bits < id_bits + min_gain_bits) return false;
        const reuse: u64 = if (self.value_bits - id_bits >= wide_gain_bits) wide_reuse else narrow_reuse;
        return self.rows >= reuse * new_count;
    }
};

/// Whether a column of `rows` non-NULL rows of about `distinct` values
/// could take ids at all, given the `id_count` its list holds: its new
/// values number at least `distinct - id_count`.
pub fn worthPlanning(rows: u64, distinct: u64, id_count: u32) bool {
    const fewest_new = distinct -| id_count;
    return rows > 0 and rows >= wide_reuse * fewest_new;
}

/// An open-addressed map of a column's values, to `slot_present` while a
/// plan gathers them and to each one's table id plus one after.
const ValueMap = struct {
    keys: []i64 = &.{},
    vals: []u32 = &.{},
    count: usize = 0,
    shift: u6 = 63,

    const slot_present: u32 = 1;

    fn deinit(self: *ValueMap, allocator: Allocator) void {
        allocator.free(self.keys);
        allocator.free(self.vals);
        self.* = undefined;
    }

    fn slotOf(self: *const ValueMap, key: i64) usize {
        return @intCast((@as(u64, @bitCast(key)) *% 0x9E3779B97F4A7C15) >> self.shift);
    }

    fn find(self: *const ValueMap, key: i64) usize {
        const mask = self.keys.len - 1;
        var s = self.slotOf(key);
        while (self.vals[s] != 0 and self.keys[s] != key) s = (s + 1) & mask;
        return s;
    }

    fn put(self: *ValueMap, allocator: Allocator, key: i64) !void {
        if ((self.count + 1) * 10 > self.keys.len * 7) try self.grow(allocator);
        const s = self.find(key);
        if (self.vals[s] != 0) return;
        self.keys[s] = key;
        self.vals[s] = slot_present;
        self.count += 1;
    }

    fn grow(self: *ValueMap, allocator: Allocator) !void {
        const capacity = @max(1024, self.keys.len * 2);
        const keys = try allocator.alloc(i64, capacity);
        errdefer allocator.free(keys);
        const vals = try allocator.alloc(u32, capacity);
        @memset(vals, 0);
        var next: ValueMap = .{ .keys = keys, .vals = vals, .count = self.count, .shift = @intCast(64 - @as(u7, std.math.log2_int(usize, capacity))) };
        for (self.keys, self.vals) |k, v| if (v != 0) {
            const s = next.find(k);
            next.keys[s] = k;
            next.vals[s] = v;
        };
        self.deinit(allocator);
        self.* = next;
    }

    fn idOf(self: *const ValueMap, key: i64) ?u32 {
        if (self.keys.len == 0) return null;
        const v = self.vals[self.find(key)];
        return if (v == 0) null else v - 1;
    }
};

/// Gathers a segment column's values, then asks the table's list for
/// their ids (`finish`).
pub const Builder = struct {
    allocator: Allocator,
    map: ValueMap = .{},
    rows: u64 = 0,
    min: i64 = std.math.maxInt(i64),
    max: i64 = std.math.minInt(i64),

    pub fn init(allocator: Allocator) Builder {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Builder) void {
        self.map.deinit(self.allocator);
        self.* = undefined;
    }

    /// Adds `view`'s non-NULL values, of a fixed-width column.
    pub fn add(self: *Builder, view: ColumnView) !void {
        const values = valuesOf(view) orelse return format.Error.SchemaMismatch;
        for (values, 0..) |v, r| if (view.isValid(r)) {
            try self.map.put(self.allocator, v);
            self.rows += 1;
            self.min = @min(self.min, v);
            self.max = @max(self.max, v);
        };
    }

    /// The column's plan when `target`'s list of column `ci` admits its
    /// values, else null. Spends the builder either way.
    pub fn finish(self: *Builder, target: table_strings.Target, ci: usize) !?Plan {
        defer self.deinit();
        if (self.rows == 0) return null;
        const id_count = if (target.strings.state(target.io, ci)) |s| s.id_count else return null;
        const range: u64 = @intCast(@as(i128, self.max) - self.min);
        const value_bits = if (range == std.math.maxInt(u64)) 64 else bitsBelow(range + 1);
        if (value_bits < bitsBelow(self.map.count) + min_gain_bits) return null;
        if (!worthPlanning(self.rows, self.map.count, id_count)) return null;

        const sorted = try self.allocator.alloc(i64, self.map.count);
        defer self.allocator.free(sorted);
        var n: usize = 0;
        for (self.map.keys, self.map.vals) |k, v| if (v != 0) {
            sorted[n] = k;
            n += 1;
        };
        std.sort.pdq(i64, sorted, {}, std.sort.asc(i64));
        for (sorted) |*v| v.* = std.mem.nativeToLittle(i64, v.*);
        const ids = try self.allocator.alloc(u32, sorted.len);
        defer self.allocator.free(ids);
        const admit: Admit = .{ .rows = self.rows, .value_bits = value_bits };
        if (!try target.strings.resolve(target.io, target.table_dir, target.threads, ci, SortedValues{ .items = sorted }, ids, admit)) return null;
        for (sorted, ids) |v, id| self.map.vals[self.map.find(std.mem.littleToNative(i64, v))] = id + 1;
        const plan: Plan = .{ .map = self.map, .any_id = ids[0] };
        self.map = .{};
        return plan;
    }
};

const SortedValues = struct {
    items: []const i64,

    pub fn len(self: SortedValues) usize {
        return self.items.len;
    }

    pub fn get(self: SortedValues, i: usize) []const u8 {
        return std.mem.sliceAsBytes(self.items[i..][0..1]);
    }
};

/// A segment column's table id of each of its values.
pub const Plan = struct {
    map: ValueMap,
    /// An id of the plan, which a NULL row before any value stores.
    any_id: u32,

    pub fn deinit(self: *Plan, allocator: Allocator) void {
        self.map.deinit(allocator);
        self.* = undefined;
    }

    /// The id of each of rows `row_start..row_end` of `view` into `out`; a
    /// NULL row repeats the row before it, so runs stay whole. Fails on a
    /// value the plan lacks, which only a view other than the one planned
    /// holds.
    pub fn idsOf(self: *const Plan, view: ColumnView, row_start: usize, row_end: usize, out: []i32) !void {
        const values = valuesOf(view) orelse return format.Error.SchemaMismatch;
        var prev = self.any_id;
        for (out[0 .. row_end - row_start], row_start..) |*id, r| {
            if (view.isValid(r)) prev = self.map.idOf(values[r]) orelse return format.Error.SchemaMismatch;
            id.* = @intCast(prev);
        }
    }
};

/// The table id of each of `view`'s rows of fixed-width column `ci` into
/// `out`, from `target`'s list, which takes any value it lacks; false, with
/// `out` unset, when the list takes no ids or every row is NULL. A NULL row
/// repeats the row before it.
pub fn resolveRows(allocator: Allocator, target: table_strings.Target, ci: usize, view: ColumnView, out: []i32) !bool {
    const values = valuesOf(view) orelse return format.Error.SchemaMismatch;
    const sorted = try allocator.alloc(i64, values.len);
    defer allocator.free(sorted);
    var n: usize = 0;
    for (values, 0..) |v, r| if (view.isValid(r)) {
        sorted[n] = v;
        n += 1;
    };
    if (n == 0) return false;
    std.sort.pdq(i64, sorted[0..n], {}, std.sort.asc(i64));
    var d: usize = 1;
    for (sorted[1..n]) |v| if (v != sorted[d - 1]) {
        sorted[d] = v;
        d += 1;
    };
    const distinct = sorted[0..d];
    const little = try allocator.alloc(i64, d);
    defer allocator.free(little);
    for (little, distinct) |*l, v| l.* = std.mem.nativeToLittle(i64, v);
    const ids = try allocator.alloc(u32, d);
    defer allocator.free(ids);
    if (!try target.strings.resolve(target.io, target.table_dir, target.threads, ci, SortedValues{ .items = little }, ids, table_strings.admit_all)) return false;
    var prev = ids[0];
    for (out[0..values.len], values, 0..) |*id, v, r| {
        if (view.isValid(r)) prev = ids[std.sort.binarySearch(i64, distinct, v, orderI64).?];
        id.* = @intCast(prev);
    }
    return true;
}

fn orderI64(a: i64, b: i64) std.math.Order {
    return std.math.order(a, b);
}

fn valuesOf(view: ColumnView) ?[]const i64 {
    return switch (view.data) {
        .bigint, .datetime, .decimal64 => |s| s,
        else => null,
    };
}

test "a column takes ids when its values outgrow them and repeat" {
    const cases = .{
        // A user id: 64-bit values, six rows a new value.
        .{ .rows = 8_000_000, .value_bits = 60, .new = 1_280_000, .ids = 0, .admits = true },
        // An event id: new on every row.
        .{ .rows = 8_000_000, .value_bits = 62, .new = 8_000_000, .ids = 0, .admits = false },
        // A timestamp a few bits past its ids, repeated often.
        .{ .rows = 8_000_000, .value_bits = 30, .new = 250_000, .ids = 0, .admits = true },
        // The same, repeated too little for the narrow gain.
        .{ .rows = 1_000_000, .value_bits = 30, .new = 250_000, .ids = 0, .admits = false },
        // A narrow code no id beats.
        .{ .rows = 8_000_000, .value_bits = 8, .new = 10, .ids = 0, .admits = false },
        // Values the list already holds.
        .{ .rows = 100, .value_bits = 64, .new = 0, .ids = 5_000_000, .admits = true },
    };
    inline for (cases) |c| {
        const admit: Admit = .{ .rows = c.rows, .value_bits = c.value_bits };
        try std.testing.expectEqual(c.admits, admit.admits(c.new, c.ids));
    }
}
