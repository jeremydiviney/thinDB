// Adapted from Zig 0.17 std/sort/pdq.zig to propagate cancellation without
// changing comparator ordering. Upstream license follows.
// The MIT License (Expat)
//
// Copyright (c) Zig contributors
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

const std = @import("std");
const sort = std.sort;
const mem = std.mem;
const math = std.math;

pub fn pdq(
    comptime T: type,
    items: []T,
    context: anytype,
    flag: ?*const std.atomic.Value(bool),
    comptime lessThanFn: fn (context: @TypeOf(context), lhs: T, rhs: T) bool,
) error{QueryCancelled}!void {
    if (flag == null) return std.sort.pdq(T, items, context, lessThanFn);
    return pdqPrefix(T, items, items.len, context, flag, lessThanFn);
}

/// Puts the `prefix` least items, in order, at the front of `items`; the rest
/// end up in unspecified order. Ranges that start at or past `prefix` are
/// never partitioned further, so the cost is about a partition pass over the
/// rest plus a sort of the prefix.
pub fn pdqPrefix(
    comptime T: type,
    items: []T,
    prefix: usize,
    context: anytype,
    flag: ?*const std.atomic.Value(bool),
    comptime lessThanFn: fn (context: @TypeOf(context), lhs: T, rhs: T) bool,
) error{QueryCancelled}!void {
    var check = Check{ .flag = flag };
    try check.poll();
    const Context = struct {
        items: []T,
        sub_ctx: @TypeOf(context),
        check: *Check,

        pub fn lessThan(ctx: @This(), a: usize, b: usize) bool {
            return lessThanFn(ctx.sub_ctx, ctx.items[a], ctx.items[b]);
        }

        pub fn swap(ctx: @This(), a: usize, b: usize) void {
            return mem.swap(T, &ctx.items[a], &ctx.items[b]);
        }
    };
    try pdqContext(0, items.len, prefix, Context{ .items = items, .sub_ctx = context, .check = &check });
    try check.poll();
}

const Hint = enum {
    increasing,
    decreasing,
    unknown,
};

const Range = struct {
    a: usize,
    b: usize,
    limit: usize,
    leftmost: bool,
    balanced: bool,
    partitioned: bool,
};

pub fn pdqContext(a: usize, b: usize, need: usize, context: anytype) error{QueryCancelled}!void {
    const max_insertion = 24;
    const max_limit = if (b > a) math.log2_int(usize, b - a) else 0;

    var stack: [@bitSizeOf(usize)]Range = undefined;
    var range = Range{
        .a = a,
        .b = b,
        .limit = max_limit,
        .leftmost = true,
        .balanced = true,
        .partitioned = true,
    };
    var top: usize = 0;

    while (true) {
        while (true) {
            try context.check.step();
            if (range.a >= need) break;
            const len = range.b - range.a;

            if (len <= max_insertion) {
                if (range.leftmost) {
                    break sort.insertionContext(range.a, range.b, context);
                } else {
                    break unguardedInsertionContext(range.a, range.b, context);
                }
            }

            if (range.limit == 0) {
                break try heapContext(range.a, range.b, context);
            }

            if (!range.balanced) {
                breakPatterns(range.a, range.b, context);
                range.limit -= 1;
            }

            var pivot: usize = 0;
            var hint = chosePivot(range.a, range.b, &pivot, context);

            if (hint == .decreasing) {
                try reverseRange(range.a, range.b, context);
                pivot = (range.b - 1) - (pivot - range.a);
                hint = .increasing;
            }

            if (range.balanced and range.partitioned and hint == .increasing) {
                if (try partialInsertionSort(range.a, range.b, context)) break;
            }

            if (range.a > a and !context.lessThan(range.a - 1, pivot)) {
                range.a = try partitionEqual(range.a, range.b, pivot, context);
                continue;
            }

            var mid = pivot;
            const was_partitioned = try partition(range.a, range.b, &mid, context);

            const left_len = mid - range.a;
            const right_len = range.b - (mid + 1);
            const balanced_threshold = len / 8;

            const left_is_smaller = left_len < right_len;

            const smaller_len = if (left_is_smaller) left_len else right_len;
            const was_balanced = smaller_len >= balanced_threshold;

            const smaller_start_offset = if (left_is_smaller) range.a else mid + 1;
            const larger_start_offset = if (left_is_smaller) mid + 1 else range.a;
            const smaller_end_exclusive_offset = if (left_is_smaller) mid else range.b;
            const larger_end_exclusive_offset = if (left_is_smaller) range.b else mid;

            const smaller_is_leftmost = if (left_is_smaller) range.leftmost else false;
            const larger_is_leftmost = if (left_is_smaller) false else range.leftmost;

            stack[top] = .{
                .a = larger_start_offset,
                .b = larger_end_exclusive_offset,
                .limit = range.limit,
                .leftmost = larger_is_leftmost,
                .balanced = was_balanced,
                .partitioned = was_partitioned,
            };
            top += 1;

            range.a = smaller_start_offset;
            range.b = smaller_end_exclusive_offset;
            range.leftmost = smaller_is_leftmost;
            range.balanced = true;
            range.partitioned = was_partitioned;
        }

        top = math.sub(usize, top, 1) catch break;
        range = stack[top];
    }
}

/// Insertion sort that assumes `items[a-1]` exists and is <= all elements in `[a, b)`.
fn unguardedInsertionContext(a: usize, b: usize, context: anytype) void {
    var i = a + 1;
    while (i < b) : (i += 1) {
        var j = i;
        while (context.lessThan(j, j - 1)) : (j -= 1) {
            context.swap(j, j - 1);
        }
    }
}

fn partition(a: usize, b: usize, pivot: *usize, context: anytype) error{QueryCancelled}!bool {
    context.swap(a, pivot.*);

    var i = a + 1;
    var j = b - 1;

    while (i <= j and context.lessThan(i, a)) {
        try context.check.step();
        i += 1;
    }
    while (i <= j and !context.lessThan(j, a)) {
        try context.check.step();
        j -= 1;
    }

    if (i > j) {
        context.swap(j, a);
        pivot.* = j;
        return true;
    }

    context.swap(i, j);
    i += 1;
    j -= 1;

    const block_size = 64;
    var offsets_l: [block_size]u8 align(std.atomic.cache_line) = undefined;
    var offsets_r: [block_size]u8 align(std.atomic.cache_line) = undefined;

    var offsets_l_base = i;
    var offsets_r_base = j;
    var num_l: usize = 0;
    var num_r: usize = 0;
    var start_l: usize = 0;
    var start_r: usize = 0;

    while (i <= j) {
        const num_unknown = j + 1 - i;
        const left_split = if (num_l == 0)
            @min(block_size, if (num_r == 0) num_unknown / 2 else num_unknown)
        else
            0;
        const right_split = if (num_r == 0)
            @min(block_size, num_unknown - left_split)
        else
            0;
        // `@min` against the comptime block size narrows both splits to u7.
        try context.check.stepBy(@as(usize, left_split) + right_split);

        for (0..left_split) |k| {
            offsets_l[num_l] = @intCast(k);
            num_l += @intFromBool(!context.lessThan(i + k, a));
        }
        i += left_split;

        for (0..right_split) |k| {
            offsets_r[num_r] = @intCast(k);
            num_r += @intFromBool(context.lessThan(j - k, a));
        }
        j -= right_split;

        const num = @min(num_l, num_r);
        for (0..num) |m| {
            context.swap(
                offsets_l_base + offsets_l[start_l + m],
                offsets_r_base - offsets_r[start_r + m],
            );
        }
        num_l -= num;
        num_r -= num;
        start_l += num;
        start_r += num;

        if (num_l == 0) {
            start_l = 0;
            offsets_l_base = i;
        }
        if (num_r == 0) {
            start_r = 0;
            offsets_r_base = j;
        }
    }

    if (num_l > 0) {
        while (num_l > 0) {
            num_l -= 1;
            context.swap(offsets_l_base + offsets_l[start_l + num_l], j);
            j -= 1;
        }
        i = j + 1;
    }
    if (num_r > 0) {
        while (num_r > 0) {
            num_r -= 1;
            context.swap(offsets_r_base - offsets_r[start_r + num_r], i);
            i += 1;
        }
        j = i - 1;
    }

    context.swap(j, a);
    pivot.* = j;
    return false;
}

fn partitionEqual(a: usize, b: usize, pivot: usize, context: anytype) error{QueryCancelled}!usize {
    context.swap(a, pivot);

    var i = a + 1;
    var j = b - 1;

    while (true) {
        try context.check.step();
        while (i <= j and !context.lessThan(a, i)) {
            try context.check.step();
            i += 1;
        }
        while (i <= j and context.lessThan(a, j)) {
            try context.check.step();
            j -= 1;
        }
        if (i > j) break;

        context.swap(i, j);
        i += 1;
        j -= 1;
    }

    return i;
}

fn partialInsertionSort(a: usize, b: usize, context: anytype) error{QueryCancelled}!bool {
    @branchHint(.cold);

    const max_steps = 5;

    const shortest_shifting = 50;

    var i = a + 1;
    for (0..max_steps) |_| {
        while (i < b and !context.lessThan(i, i - 1)) {
            try context.check.step();
            i += 1;
        }

        if (i == b) return true;

        if (b - a < shortest_shifting) return false;

        context.swap(i, i - 1);

        if (i - a >= 2) {
            var j = i - 1;
            while (j > a) : (j -= 1) {
                try context.check.step();
                if (!context.lessThan(j, j - 1)) break;
                context.swap(j, j - 1);
            }
        }

        if (b - i >= 2) {
            var j = i + 1;
            while (j < b) : (j += 1) {
                try context.check.step();
                if (!context.lessThan(j, j - 1)) break;
                context.swap(j, j - 1);
            }
        }
    }

    return false;
}

fn breakPatterns(a: usize, b: usize, context: anytype) void {
    @branchHint(.cold);

    const len = b - a;
    if (len < 8) return;

    var rand = @as(u64, @intCast(len));
    const modulus = math.ceilPowerOfTwoAssert(u64, len);

    var i = a + (len / 4) * 2 - 1;
    while (i <= a + (len / 4) * 2 + 1) : (i += 1) {
        rand ^= rand << 13;
        rand ^= rand >> 7;
        rand ^= rand << 17;

        var other = @as(usize, @intCast(rand & (modulus - 1)));
        if (other >= len) other -= len;
        context.swap(i, a + other);
    }
}

fn chosePivot(a: usize, b: usize, pivot: *usize, context: anytype) Hint {
    const shortest_ninther = 50;
    const max_swaps = 4 * 3;

    const len = b - a;
    const i = a + len / 4 * 1;
    const j = a + len / 4 * 2;
    const k = a + len / 4 * 3;
    var swaps: usize = 0;

    if (len >= 8) {
        if (len >= shortest_ninther) {
            sort3(i - 1, i, i + 1, &swaps, context);
            sort3(j - 1, j, j + 1, &swaps, context);
            sort3(k - 1, k, k + 1, &swaps, context);
        }

        sort3(i, j, k, &swaps, context);
    }

    pivot.* = j;
    return switch (swaps) {
        0 => .increasing,
        max_swaps => .decreasing,
        else => .unknown,
    };
}

fn sort3(a: usize, b: usize, c: usize, swaps: *usize, context: anytype) void {
    if (context.lessThan(b, a)) {
        swaps.* += 1;
        context.swap(b, a);
    }

    if (context.lessThan(c, b)) {
        swaps.* += 1;
        context.swap(c, b);
    }

    if (context.lessThan(b, a)) {
        swaps.* += 1;
        context.swap(b, a);
    }
}

fn reverseRange(a: usize, b: usize, context: anytype) error{QueryCancelled}!void {
    var i = a;
    var j = b - 1;
    while (i < j) {
        try context.check.step();
        context.swap(i, j);
        i += 1;
        j -= 1;
    }
}

const Check = struct {
    flag: ?*const std.atomic.Value(bool),
    remaining: usize = poll_interval,

    /// Comparisons between polls of the flag.
    const poll_interval = 1024;

    fn poll(self: *const Check) error{QueryCancelled}!void {
        if (self.flag) |flag| if (flag.load(.acquire)) return error.QueryCancelled;
    }

    fn step(self: *Check) error{QueryCancelled}!void {
        return self.stepBy(1);
    }

    /// Counts `n` comparisons toward the next poll.
    fn stepBy(self: *Check, n: usize) error{QueryCancelled}!void {
        if (n < self.remaining) {
            self.remaining -= n;
            return;
        }
        self.remaining = poll_interval;
        try self.poll();
    }
};

fn heapContext(a: usize, b: usize, context: anytype) error{QueryCancelled}!void {
    const n = b - a;
    var root = n / 2;
    while (root > 0) {
        root -= 1;
        try sift(a, root, n, context);
    }
    var end = n;
    while (end > 1) {
        try context.check.step();
        end -= 1;
        context.swap(a, a + end);
        try sift(a, 0, end, context);
    }
}
fn sift(a: usize, initial: usize, end: usize, context: anytype) error{QueryCancelled}!void {
    var root = initial;
    while (root < end / 2) {
        try context.check.step();
        var child = root * 2 + 1;
        if (child + 1 < end and context.lessThan(a + child, a + child + 1)) child += 1;
        if (!context.lessThan(a + root, a + child)) return;
        context.swap(a + root, a + child);
        root = child;
    }
}

test "cancellation sort matches standard ordering across patterns" {
    const alloc = std.testing.allocator;
    var flag = std.atomic.Value(bool).init(false);
    var random = std.Random.DefaultPrng.init(1875);
    for ([_]usize{ 0, 1, 2, 24, 25, 127, 1024, 4097 }) |n| {
        const values = try alloc.alloc(u32, n);
        defer alloc.free(values);
        const expected = try alloc.alloc(u32, n);
        defer alloc.free(expected);
        for (0..5) |pattern| {
            for (values, 0..) |*v, i| v.* = switch (pattern) {
                0 => random.random().int(u32),
                1 => @intCast(i),
                2 => @intCast(n - i),
                3 => @intCast(i % 3),
                else => @intCast(@min(i, n - i)),
            };
            @memcpy(expected, values);
            std.sort.pdq(u32, expected, {}, std.sort.asc(u32));
            try pdq(u32, values, {}, &flag, std.sort.asc(u32));
            try std.testing.expectEqualSlices(u32, expected, values);
        }
    }
}

test "prefix sort orders the least items and keeps every item" {
    const alloc = std.testing.allocator;
    var random = std.Random.DefaultPrng.init(4421);
    for ([_]usize{ 0, 1, 25, 1024, 4097 }) |n| {
        const values = try alloc.alloc(u32, n);
        defer alloc.free(values);
        const expected = try alloc.alloc(u32, n);
        defer alloc.free(expected);
        for ([_]usize{ 0, @min(1, n), n / 3, n -| 1, n }) |prefix| for (0..4) |pattern| {
            for (values, 0..) |*v, i| v.* = switch (pattern) {
                0 => random.random().int(u32),
                1 => @intCast(n - i),
                2 => @intCast(i % 7),
                else => @intCast(@min(i, n - i)),
            };
            @memcpy(expected, values);
            std.sort.pdq(u32, expected, {}, std.sort.asc(u32));
            try pdqPrefix(u32, values, prefix, {}, null, std.sort.asc(u32));
            try std.testing.expectEqualSlices(u32, expected[0..prefix], values[0..prefix]);
            std.sort.pdq(u32, values, {}, std.sort.asc(u32));
            try std.testing.expectEqualSlices(u32, expected, values);
        };
    }
}

test "cancellation sort interrupts an in-progress partition without changing comparison" {
    const Ctx = struct {
        flag: std.atomic.Value(bool) = .init(false),
        comparisons: usize = 0,
        fn less(self: *@This(), a: u32, b: u32) bool {
            self.comparisons += 1;
            if (self.comparisons == 2048) self.flag.store(true, .release);
            return a < b;
        }
    };
    var ctx = Ctx{};
    const values = try std.testing.allocator.alloc(u32, 32768);
    defer std.testing.allocator.free(values);
    for (values, 0..) |*v, i| v.* = @intCast((i * 7919) % values.len);
    try std.testing.expectError(error.QueryCancelled, pdq(u32, values, &ctx, &ctx.flag, Ctx.less));
    try std.testing.expect(ctx.comparisons < 8192);
    // Cancellation may reorder items, but must neither lose nor duplicate one.
    std.sort.pdq(u32, values, {}, std.sort.asc(u32));
    for (values, 0..) |v, i| try std.testing.expectEqual(@as(u32, @intCast(i)), v);
}
