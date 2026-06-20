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
    cte_child_ticks = 0;
}

// Per-CTE-stage profiling. Each `Stage.ensureRun` drains one CTE block serially
// on the calling thread; we snapshot the per-operator slots around that drain to
// attribute layer time (scan / filter / aggregate / sort / window) to that one
// stage, and net out any nested upstream stage it triggered lazily so each line
// reports SELF cost. `cte_child_ticks` is the running sum a child contributes to
// its parent's subtraction.
threadlocal var cte_child_ticks: u64 = 0;

pub fn cteChildTicks() u64 {
    return cte_child_ticks;
}

pub fn addCteChildTicks(ticks: u64) void {
    cte_child_ticks +%= ticks;
}

const SlotSnap = struct {
    names: [48][]const u8 = undefined,
    ticks: [48]u64 = undefined,
    n: usize = 0,
};

pub fn snapSlots() SlotSnap {
    var s: SlotSnap = .{ .n = count };
    for (0..count) |i| {
        s.names[i] = slots[i].name;
        s.ticks[i] = slots[i].ticks;
    }
    return s;
}

/// Print one CTE stage's drain: SELF wall (net of nested stages), full wall
/// (incl. nested), realized rows, and the per-operator-type time that accrued
/// during this stage only (current slots minus the pre-drain snapshot).
pub fn dumpStageDelta(stage_id: usize, rows: u64, self_ticks: i64, wall_ticks: i64, before: SlotSnap) void {
    if (!enabled) return;
    const hz: f64 = @floatFromInt(freq());
    const self_ms = @as(f64, @floatFromInt(@max(self_ticks, 0))) * 1000.0 / hz;
    const wall_ms = @as(f64, @floatFromInt(@max(wall_ticks, 0))) * 1000.0 / hz;
    std.debug.print("[cte] stage#{d: <3} rows={d: <9} self={d: >8.2}ms wall={d: >8.2}ms\n", .{ stage_id, rows, self_ms, wall_ms });
    for (slots[0..count]) |s| {
        var prev: u64 = 0;
        for (0..before.n) |j| {
            if (before.names[j].ptr == s.name.ptr) {
                prev = before.ticks[j];
                break;
            }
        }
        if (s.ticks <= prev) continue;
        const ms = @as(f64, @floatFromInt(s.ticks - prev)) * 1000.0 / hz;
        if (ms < 0.01) continue;
        // The per-type counters are INCLUSIVE and global; a delta exceeding this
        // stage's own wall is overlap leaked from a nested stage's drain, not
        // real work in this one. A single layer can't out-cost its stage's wall.
        if (ms > wall_ms * 1.05) continue;
        std.debug.print("[cte]      {s: <46} {d: >8.2} ms\n", .{ shortOpName(s.name), ms });
    }
}

/// Trim the long `exec.foo.Bar` operator type name to its leaf for the per-CTE
/// breakdown (the full names already appear in the global `[oprof]` dump).
fn shortOpName(name: []const u8) []const u8 {
    var i = name.len;
    while (i > 0) : (i -= 1) {
        if (name[i - 1] == '.') return name[i..];
    }
    return name;
}

// Separate "phase" namespace for handler sub-step breakdowns (operator
// construction / teardown), kept apart from the per-operator `slots` above so
// the execute-time `reset()` doesn't clobber construction timings. Accumulate
// with `addPhase`, clear with `resetPhases`, print with `dumpPhases`.
threadlocal var phase_slots: [32]Slot = undefined;
threadlocal var phase_count: usize = 0;

pub inline fn addPhase(name: []const u8, ticks: u64) void {
    if (!enabled) return;
    var i: usize = 0;
    while (i < phase_count) : (i += 1) {
        if (phase_slots[i].name.ptr == name.ptr) {
            phase_slots[i].ticks += ticks;
            phase_slots[i].calls += 1;
            return;
        }
    }
    if (phase_count < phase_slots.len) {
        phase_slots[phase_count] = .{ .name = name, .ticks = ticks, .calls = 1 };
        phase_count += 1;
    }
}

pub fn resetPhases() void {
    phase_count = 0;
}

pub fn dumpPhases(label: []const u8) void {
    if (!enabled or phase_count == 0) return;
    const hz: f64 = @floatFromInt(freq());
    std.debug.print("[hprof] {s} — handler sub-phase time:\n", .{label});
    for (phase_slots[0..phase_count]) |s| {
        const ms = @as(f64, @floatFromInt(s.ticks)) * 1000.0 / hz;
        std.debug.print("[hprof]   {s: <34} {d: >9.3} ms  ({d}×)\n", .{ s.name, ms, s.calls });
    }
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
