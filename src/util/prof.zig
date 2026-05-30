//! Tiny per-operator execution profiler. When `enabled`, `makeQuery`'s next()
//! wrapper accumulates wall time per operator *type* (keyed by the comptime
//! `@typeName`, whose pointer is stable/interned). Time is INCLUSIVE — an
//! operator's total includes the upstream `next()` calls nested inside it — so
//! for a linear pipeline the self time of an operator is its inclusive minus
//! its direct upstream's inclusive. Thread-local: each connection thread keeps
//! its own counters, reset + dumped around a single query's drain.
//!
//! This Zig routes all clocks through the async `Io` abstraction; `std.time`
//! exposes only constants. The operator next() wrapper has no `Io` handle, so
//! we read the raw Windows performance counter directly (diagnostic builds are
//! Windows-only here; elsewhere ticks are 0 and the dump is empty).

const std = @import("std");
const builtin = @import("builtin");
const win = std.os.windows;

pub var enabled: bool = false;

const Slot = struct { name: []const u8, ticks: u64, calls: u64 };
threadlocal var slots: [48]Slot = undefined;
threadlocal var count: usize = 0;

pub fn nowTicks() i64 {
    if (builtin.os.tag != .windows) return 0;
    var c: win.LARGE_INTEGER = 0;
    _ = win.ntdll.RtlQueryPerformanceCounter(&c);
    return c;
}

fn freq() i64 {
    if (builtin.os.tag != .windows) return 1;
    var f: win.LARGE_INTEGER = 0;
    _ = win.ntdll.RtlQueryPerformanceFrequency(&f);
    return if (f == 0) 1 else f;
}

/// Convert a raw performance-counter tick delta to milliseconds. Used by
/// operators that time their own internal phases (e.g. ParallelScan's worker
/// drain) and print directly, outside the per-type `add`/`dump` path.
pub fn ticksToMs(ticks: i64) f64 {
    const hz: f64 = @floatFromInt(freq());
    return @as(f64, @floatFromInt(ticks)) * 1000.0 / hz;
}

pub inline fn add(name: []const u8, ticks: u64) void {
    if (!enabled) return;
    var i: usize = 0;
    while (i < count) : (i += 1) {
        if (slots[i].name.ptr == name.ptr) {
            slots[i].ticks += ticks;
            slots[i].calls += 1;
            return;
        }
    }
    if (count < slots.len) {
        slots[count] = .{ .name = name, .ticks = ticks, .calls = 1 };
        count += 1;
    }
}

pub fn reset() void {
    count = 0;
}

pub fn dump(label: []const u8) void {
    if (!enabled or count == 0) return;
    const hz: f64 = @floatFromInt(freq());
    var order: [48]usize = undefined;
    for (0..count) |i| order[i] = i;
    var a: usize = 0;
    while (a < count) : (a += 1) {
        var b = a + 1;
        while (b < count) : (b += 1) {
            if (slots[order[b]].ticks > slots[order[a]].ticks) {
                const t = order[a];
                order[a] = order[b];
                order[b] = t;
            }
        }
    }
    std.debug.print("[oprof] {s} — per-operator INCLUSIVE time:\n", .{label});
    for (order[0..count]) |idx| {
        const s = slots[idx];
        const ms = @as(f64, @floatFromInt(s.ticks)) * 1000.0 / hz;
        std.debug.print("[oprof]   {s: <44} {d: >10.2} ms  ({d} next calls)\n", .{ s.name, ms, s.calls });
    }
}
