// Adapted from Zig 0.16 std/sort/pdq.zig to propagate cancellation without
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
    var check = Check{ .flag = flag.? };
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
    try pdqContext(0, items.len, Context{ .items = items, .sub_ctx = context, .check = &check });
    try check.poll();
}

const Hint = enum {
    increasing,
    decreasing,
    unknown,
};

pub fn pdqContext(a: usize, b: usize, context: anytype) error{QueryCancelled}!void {
    const max_insertion = 24;

    const max_limit = std.math.floorPowerOfTwo(usize, b - a) + 1;

    const Range = struct { a: usize, b: usize, limit: usize };
    const stack_size = math.log2(math.maxInt(usize) + 1);
    var stack: [stack_size]Range = undefined;
    var range = Range{ .a = a, .b = b, .limit = max_limit };
    var top: usize = 0;

    while (true) {
        try context.check.step();
        var was_balanced = true;
        var was_partitioned = true;

        while (true) {
            try context.check.step();
            const len = range.b - range.a;

            if (len <= max_insertion) {
                break sort.insertionContext(range.a, range.b, context);
            }

            if (range.limit == 0) {
                break try heapContext(range.a, range.b, context);
            }

            if (!was_balanced) {
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

            if (was_balanced and was_partitioned and hint == .increasing) {
                if (try partialInsertionSort(range.a, range.b, context)) break;
            }

            if (range.a > a and !context.lessThan(range.a - 1, pivot)) {
                range.a = try partitionEqual(range.a, range.b, pivot, context);
                continue;
            }

            var mid = pivot;
            was_partitioned = try partition(range.a, range.b, &mid, context);

            const left_len = mid - range.a;
            const right_len = range.b - mid;
            const balanced_threshold = len / 8;
            if (left_len < right_len) {
                was_balanced = left_len >= balanced_threshold;
                stack[top] = .{ .a = range.a, .b = mid, .limit = range.limit };
                top += 1;
                range.a = mid + 1;
            } else {
                was_balanced = right_len >= balanced_threshold;
                stack[top] = .{ .a = mid + 1, .b = range.b, .limit = range.limit };
                top += 1;
                range.b = mid;
            }
        }

        top = math.sub(usize, top, 1) catch break;
        range = stack[top];
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

    while (true) {
        try context.check.step();
        while (i <= j and context.lessThan(i, a)) {
            try context.check.step();
            i += 1;
        }
        while (i <= j and !context.lessThan(j, a)) {
            try context.check.step();
            j -= 1;
        }
        if (i > j) break;

        context.swap(i, j);
        i += 1;
        j -= 1;
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
    flag: *const std.atomic.Value(bool),
    remaining: usize = 1024,
    fn poll(self: *Check) error{QueryCancelled}!void {
        if (self.flag.load(.acquire)) return error.QueryCancelled;
    }
    fn step(self: *Check) error{QueryCancelled}!void {
        self.remaining -= 1;
        if (self.remaining == 0) {
            self.remaining = 1024;
            try self.poll();
        }
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
