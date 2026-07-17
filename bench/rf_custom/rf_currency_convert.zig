// Rollforward currency conversion: replaces the 10-block SQL cluster
// (with_normalized_currency .. with_month_diff) with one row-aligned pass
// over invoice_base_with_estimates (input 0), plus two lookup inputs:
// rf_usd_rates() (input 1, rate at exact (currencyTo, date)) and
// rf_external_plans(projectId) (input 2, internalPlanId by (projectId,
// integrationConfigId, externalPlanId)). Only the INVOICE_DATE exchange-rate
// path and the non-expanded partition shapes are implemented; the emission
// gate falls through to the inline blocks otherwise.
//
// Call shape: PARTITION BY customerNumberLC with rf_usd_rates() and
// rf_external_plans() as BROADCAST inputs (delivered whole to every
// partition, no call keys in their schemas) — the kernel runs one
// partition per customer across all workers, building its rate/plan
// lookup maps once per worker (ctx.worker_state / worker_arena). No
// ORDER BY: the LAST_VALUE winner below is computed by explicit
// comparison, so within-partition row order is value-irrelevant, and
// row-aligned output follows the presented order either way.
//
// Must stay value-identical to the inline SQL emission. Engine semantics
// mirrored exactly (all live-probed on the bench):
//   - decimal(16,6) division: result scale s0+4 = 10, mantissa =
//     roundDiv(a_m * 10^10, b_m), ties away from zero; b_m == 0 → 0 (no
//     error). CAST(dec AS DOUBLE) = f64(mantissa) / 10^scale.
//   - int * decimal(16,6) then ROUND then CAST INT = satCast(i32,
//     roundDiv(amount * rate_micros, 10^6)).
//   - ROUND(double) = @round (half away from zero); CAST(double AS INT)
//     truncates toward zero with i32 clamping (NaN → maxInt).
//   - datetime = date join keys compare at datetime granularity (date is
//     cast to midnight); rate rows keyed by exact datetime micros.
//   - LAST_VALUE(originalCurrency) over (partition ORDER BY invoiceDate,
//     originalCurrency ASC, full frame): the winner is the max
//     (invoiceDate, originalCurrency) pair, NULLs first — ties share the
//     currency value, so the pick is unambiguous.
//
// Row-drop finding: the inline handler/pass-through split is 3VL — a row
// with NULL originalCurrency (or NULL normalizedCurrency) falls out of BOTH
// union arms and is dropped. Probed: zero such rows exist in the dataset
// (invoice_import_amortized has no NULL originalCurrency with deleted = 0),
// so row alignment holds; if one ever appears the kernel emits it with
// pass-through values (originalAmountNormalized = originalAmount) instead
// of dropping, which would diverge from the inline SQL by that row.
const std = @import("std");
const tdb = @import("thindb").tdb;

pub const spec = tdb.TableFnSpec{
    .name = "rf_currency_convert",
    .execution = .partitioned,
    .row_aligned = true,
    .broadcast_inputs = &.{ 1, 2 },
};

pub const Args = struct {
    targetCurrency: []const u8,
    // 1 = normalizedCurrency groups on (projectId, divisionId,
    // customerNumberLC); 0 = cross-division, no divisionId in the key.
    useDivision: i64,
};

pub const Input = struct {
    projectId: ?i32,
    divisionId: ?i32,
    customerNumberLC: ?[]const u8,
    invoiceDate: ?tdb.Date,
    originalCurrency: ?[]const u8,
    amount: ?i32,
    originalAmount: ?i32,
    lineItemType: ?[]const u8,
    planId: ?[]const u8,
    integrationConfigId: ?i32,
    date: ?tdb.Date,
    startDate: ?tdb.Date,
};

pub const Carry = struct {
    customerNumber: ?[]const u8,
    customerName: ?[]const u8,
    parentCustomerNumber: ?[]const u8,
    parentCustomerName: ?[]const u8,
    customerNumberHash: ?[]const u8,
    invoiceId: ?[]const u8,
    originalCustomerNumber: ?[]const u8,
    originalCustomerName: ?[]const u8,
    customerEmail: ?[]const u8,
};

// rf_usd_rates() with the rate shipped as an exact micros mantissa
// (CAST(rate * 1000000 AS BIGINT), decimal(16,6) → lossless). Broadcast.
pub const Input2 = struct {
    currencyTo: ?[]const u8,
    date: ?tdb.DateTime,
    rateMicros: ?i64,
};

// rf_external_plans(projectId). Broadcast.
pub const Input3 = struct {
    projectId: ?i32,
    integrationConfigId: ?i32,
    externalPlanId: ?[]const u8,
    id: ?i32,
};

// with_month_diff column order; the wrapper CTE renames
// normalizedCurrency AS originalCurrency after the TABLE() call.
pub const Output = struct {
    projectId: ?i32,
    divisionId: ?i32,
    customerNumber: ?[]const u8,
    customerName: ?[]const u8,
    customerNumberLC: ?[]const u8,
    parentCustomerNumber: ?[]const u8,
    parentCustomerName: ?[]const u8,
    customerNumberHash: ?[]const u8,
    date: ?tdb.Date,
    invoiceDate: ?tdb.Date,
    invoiceId: ?[]const u8,
    originalCustomerNumber: ?[]const u8,
    originalCustomerName: ?[]const u8,
    normalizedCurrency: ?[]const u8,
    planId: ?[]const u8,
    lineItemType: ?[]const u8,
    integrationConfigId: ?i32,
    startDate: ?tdb.Date,
    customerEmail: ?[]const u8,
    nonRecurringAmount: ?i32,
    originalNonRecurringAmount: ?i32,
    amount: ?i32,
    originalAmount: ?i32,
    internalPlanId: ?i32,
    monthDiff: ?i32,
};

pub const passthrough = .{
    "projectId",              "divisionId",           "customerNumber",     "customerName",
    "customerNumberLC",       "parentCustomerNumber", "parentCustomerName", "customerNumberHash",
    "date",                   "invoiceDate",          "invoiceId",          "originalCustomerNumber",
    "originalCustomerName",   "planId",               "lineItemType",       "integrationConfigId",
    "startDate",              "customerEmail",
};

pub const Computed = struct {
    normalizedCurrency: ?[]const u8,
    nonRecurringAmount: ?i32,
    originalNonRecurringAmount: ?i32,
    amount: ?i32,
    originalAmount: ?i32,
    internalPlanId: ?i32,
    monthDiff: ?i32,
};

const RATE_ONE: i64 = 1_000_000; // COALESCE(rate, 1) at scale 6
const POW10_10: i128 = 10_000_000_000;

/// Engine roundDiv: nearest integer, ties away from zero.
fn roundDiv(num: i128, den: i128) i128 {
    const q = @divTrunc(num, den);
    const r = @rem(num, den);
    if (r == 0) return q;
    const abs_r = @abs(r);
    const abs_d = @abs(den);
    if (abs_r >= abs_d - abs_r) {
        return if ((num < 0) != (den < 0)) q - 1 else q + 1;
    }
    return q;
}

/// Engine satCast(i32, ...) — decimal → INT cast clamping.
fn satI32(v: i128) i32 {
    if (v > std.math.maxInt(i32)) return std.math.maxInt(i32);
    if (v < std.math.minInt(i32)) return std.math.minInt(i32);
    return @intCast(v);
}

/// Engine CAST(double AS INT): truncate toward zero, clamp, NaN → maxInt.
fn dblToI32(v: f64) i32 {
    const t = @trunc(v);
    if (std.math.isNan(v) or t > @as(f64, @floatFromInt(std.math.maxInt(i32)))) return std.math.maxInt(i32);
    if (t < @as(f64, @floatFromInt(std.math.minInt(i32)))) return std.math.minInt(i32);
    return @intFromFloat(t);
}

// (currency bytes, datetime micros) → rate micros; borrowed key bytes.
const RateKey = struct { cur: []const u8, us: i64 };
const RateKeyCtx = struct {
    pub fn hash(_: @This(), k: RateKey) u64 {
        var h = std.hash.Wyhash.init(@as(u64, @bitCast(k.us)));
        h.update(k.cur);
        return h.final();
    }
    pub fn eql(_: @This(), x: RateKey, y: RateKey) bool {
        return x.us == y.us and std.mem.eql(u8, x.cur, y.cur);
    }
};
const RateMap = std.HashMapUnmanaged(RateKey, i64, RateKeyCtx, std.hash_map.default_max_load_percentage);

const PlanKey = struct { pid: i32, icid: i32, ep: []const u8 };
const PlanKeyCtx = struct {
    pub fn hash(_: @This(), k: PlanKey) u64 {
        var h = std.hash.Wyhash.init((@as(u64, @as(u32, @bitCast(k.pid))) << 32) | @as(u64, @as(u32, @bitCast(k.icid))));
        h.update(k.ep);
        return h.final();
    }
    pub fn eql(_: @This(), x: PlanKey, y: PlanKey) bool {
        return x.pid == y.pid and x.icid == y.icid and std.mem.eql(u8, x.ep, y.ep);
    }
};
const PlanMap = std.HashMapUnmanaged(PlanKey, i32, PlanKeyCtx, std.hash_map.default_max_load_percentage);

fn optStrEq(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.mem.eql(u8, a.?, b.?);
}

fn optI32Eq(a: ?i32, b: ?i32) bool {
    if (a == null or b == null) return a == null and b == null;
    return a.? == b.?;
}

/// (invoiceDate, originalCurrency) ASC with NULLs first — the LAST_VALUE
/// window's sort key order.
fn ordInvCur(inv_a: ?tdb.Date, cur_a: ?[]const u8, inv_b: ?tdb.Date, cur_b: ?[]const u8) std.math.Order {
    if (inv_a == null or inv_b == null) {
        if (inv_a != null) return .gt;
        if (inv_b != null) return .lt;
    } else {
        const o = std.math.order(inv_a.?.days(), inv_b.?.days());
        if (o != .eq) return o;
    }
    if (cur_a == null or cur_b == null) {
        if (cur_a != null) return .gt;
        if (cur_b != null) return .lt;
        return .eq;
    }
    return std.mem.order(u8, cur_a.?, cur_b.?);
}

// One normalizedCurrency group inside a customerNumberLC run: the window
// PARTITION BY includes projectId (and divisionId unless cross-division)
// while the input is only sorted by customerNumberLC, so subgroups are
// tracked with a small per-run list (nearly always exactly one entry).
const Group = struct {
    pid: ?i32,
    did: ?i32,
    win_inv: ?tdb.Date,
    win_cur: ?[]const u8,
};

/// Dense (currency × day) rate grid. Rate lookups always come in day-aligned
/// (`invoiceDate.days() * us_per_day`), so a rate row with a non-midnight
/// datetime could never match the hash map either — skipping those preserves
/// exact join semantics. ~150 currencies × ~10K days ≈ 12MB of direct array
/// writes builds ~10× faster than 1M random hash inserts into a cache-cold
/// 1M-slot table, and every lookup becomes an interned-id + array read.
const RateTable = struct {
    ids: std.StringHashMapUnmanaged(u32),
    min_day: i64,
    n_days: usize,
    /// rates[id * n_days + (day - min_day)]; MISSING = no rate row.
    grid: []i64,

    const MISSING: i64 = std.math.minInt(i64);
    /// Fallback bounds: a degenerate date span or currency explosion makes
    /// the grid worse than the hash map.
    const MAX_DAYS: i64 = 200_000;
    const MAX_CURRENCIES: usize = 4096;
    const MAX_CELLS: usize = 64 << 20;

    fn lookup(self: *const RateTable, cur: []const u8, us: i64) ?i64 {
        const id = self.ids.get(cur) orelse return null;
        const off = @divTrunc(us, std.time.us_per_day) - self.min_day;
        if (off < 0 or off >= @as(i64, @intCast(self.n_days))) return null;
        const v = self.grid[@as(usize, id) * self.n_days + @as(usize, @intCast(off))];
        return if (v == MISSING) null else v;
    }
};

const State = struct { rate_table: ?RateTable, rate_map: RateMap, plan_map: PlanMap };

/// Two passes over the broadcast rates: day span + currency interning (a
/// last-key memo skips the tiny-map probe on sorted runs), then direct grid
/// fills. Returns null when the shape busts the grid bounds — the caller
/// falls back to the hash map.
fn buildRateTable(wa: std.mem.Allocator, rates: tdb.Partition(Input2)) !?RateTable {
    const cur_col = rates.col(.currencyTo);
    const date_col = rates.col(.date);
    const rm_col = rates.col(.rateMicros);

    var t: RateTable = .{ .ids = .empty, .min_day = std.math.maxInt(i64), .n_days = 0, .grid = &.{} };
    var max_day: i64 = std.math.minInt(i64);
    var last_cur: ?[]const u8 = null;
    var next_id: u32 = 0;
    for (0..rates.len) |i| {
        const cu = cur_col.get(i) orelse continue;
        const dt = date_col.get(i) orelse continue;
        if (rm_col.get(i) == null) continue;
        const us = dt.micros();
        if (@mod(us, std.time.us_per_day) != 0) continue; // unreachable by any lookup
        const day = @divTrunc(us, std.time.us_per_day);
        t.min_day = @min(t.min_day, day);
        max_day = @max(max_day, day);
        if (last_cur == null or !std.mem.eql(u8, last_cur.?, cu)) {
            last_cur = cu;
            const gop = try t.ids.getOrPut(wa, cu);
            if (!gop.found_existing) {
                gop.value_ptr.* = next_id;
                next_id += 1;
                if (next_id > RateTable.MAX_CURRENCIES) return null;
            }
        }
    }
    if (next_id == 0) return null;
    const span = max_day - t.min_day + 1;
    if (span <= 0 or span > RateTable.MAX_DAYS) return null;
    t.n_days = @intCast(span);
    if (next_id * t.n_days > RateTable.MAX_CELLS) return null;

    t.grid = try wa.alloc(i64, next_id * t.n_days);
    @memset(t.grid, RateTable.MISSING);
    var memo_cur: ?[]const u8 = null;
    var memo_id: u32 = 0;
    for (0..rates.len) |i| {
        const cu = cur_col.get(i) orelse continue;
        const dt = date_col.get(i) orelse continue;
        const rm = rm_col.get(i) orelse continue;
        const us = dt.micros();
        if (@mod(us, std.time.us_per_day) != 0) continue;
        if (memo_cur == null or !std.mem.eql(u8, memo_cur.?, cu)) {
            memo_cur = cu;
            memo_id = t.ids.get(cu).?;
        }
        const off: usize = @intCast(@divTrunc(us, std.time.us_per_day) - t.min_day);
        t.grid[@as(usize, memo_id) * t.n_days + off] = rm;
    }
    return t;
}

/// Lookup maps built ONCE per worker from the broadcast inputs; key bytes
/// borrow from the operator's input stores, which outlive every process()
/// call of the operator.
fn workerState(ctx: *tdb.Ctx, rates: tdb.Partition(Input2), plans: tdb.Partition(Input3)) !*State {
    if (ctx.worker_state.*) |ptr| return @ptrCast(@alignCast(ptr));
    const wa = ctx.worker_arena;
    const st = try wa.create(State);
    st.* = .{ .rate_table = null, .rate_map = .empty, .plan_map = .empty };
    st.rate_table = try buildRateTable(wa, rates);
    if (st.rate_table == null) {
        const cur_col = rates.col(.currencyTo);
        const date_col = rates.col(.date);
        const rm_col = rates.col(.rateMicros);
        try st.rate_map.ensureTotalCapacity(wa, @intCast(rates.len));
        for (0..rates.len) |i| {
            const cu = cur_col.get(i) orelse continue;
            const dt = date_col.get(i) orelse continue;
            // NULL rate ≡ missing row: both COALESCE to 1.
            const rm = rm_col.get(i) orelse continue;
            st.rate_map.putAssumeCapacity(.{ .cur = cu, .us = dt.micros() }, rm);
        }
    }
    {
        const pid_col = plans.col(.projectId);
        const icid_col = plans.col(.integrationConfigId);
        const ep_col = plans.col(.externalPlanId);
        const id_col = plans.col(.id);
        try st.plan_map.ensureTotalCapacity(wa, @intCast(plans.len));
        for (0..plans.len) |i| {
            // NULL join keys never match; NULL id ≡ join miss (both NULL).
            const pid = pid_col.get(i) orelse continue;
            const icid = icid_col.get(i) orelse continue;
            const ep = ep_col.get(i) orelse continue;
            const id = id_col.get(i) orelse continue;
            st.plan_map.putAssumeCapacity(.{ .pid = pid, .icid = icid, .ep = ep }, id);
        }
    }
    ctx.worker_state.* = st;
    return st;
}

pub fn process(ctx: *tdb.Ctx, args: Args, p: tdb.Partition(Input), rates: tdb.Partition(Input2), plans: tdb.Partition(Input3), out: *tdb.Writer(Computed)) !void {
    if (p.len == 0) return;
    const a = ctx.arena;
    const use_division = args.useDivision != 0;
    const target_is_usd = std.mem.eql(u8, args.targetCurrency, "USD");

    const st = try workerState(ctx, rates, plans);
    _ = &st.rate_map;
    const plan_map = &st.plan_map;

    const lc_col = p.col(.customerNumberLC);
    const pid_col = p.col(.projectId);
    const did_col = p.col(.divisionId);
    const inv_col = p.col(.invoiceDate);
    const cur_col = p.col(.originalCurrency);
    const amount_col = p.col(.amount);
    const orig_col = p.col(.originalAmount);
    const ltype_col = p.col(.lineItemType);
    const plan_col = p.col(.planId);
    const icid_col = p.col(.integrationConfigId);
    const date_col = p.col(.date);
    const start_col = p.col(.startDate);

    var groups = std.ArrayListUnmanaged(Group).empty;

    var run_start: usize = 0;
    while (run_start < p.len) {
        const run_lc = lc_col.get(run_start);
        var run_end = run_start + 1;
        while (run_end < p.len and optStrEq(lc_col.get(run_end), run_lc)) run_end += 1;

        // Pass 1: LAST_VALUE winner per (projectId[, divisionId]) subgroup —
        // the max (invoiceDate, originalCurrency), later row wins ties.
        groups.clearRetainingCapacity();
        for (run_start..run_end) |i| {
            const pid = pid_col.get(i);
            const did: ?i32 = if (use_division) did_col.get(i) else null;
            const inv = inv_col.get(i);
            const cur = cur_col.get(i);
            var found: ?*Group = null;
            for (groups.items) |*g| {
                if (optI32Eq(g.pid, pid) and optI32Eq(g.did, did)) {
                    found = g;
                    break;
                }
            }
            if (found) |g| {
                if (ordInvCur(inv, cur, g.win_inv, g.win_cur) != .lt) {
                    g.win_inv = inv;
                    g.win_cur = cur;
                }
            } else {
                try groups.append(a, .{ .pid = pid, .did = did, .win_inv = inv, .win_cur = cur });
            }
        }

        // Pass 2: compute + emit in input order (row-aligned).
        for (run_start..run_end) |i| {
            const pid = pid_col.get(i);
            const did: ?i32 = if (use_division) did_col.get(i) else null;
            var norm: ?[]const u8 = null;
            for (groups.items) |*g| {
                if (optI32Eq(g.pid, pid) and optI32Eq(g.did, did)) {
                    norm = g.win_cur;
                    break;
                }
            }

            const orig_cur = cur_col.get(i);
            const amount = amount_col.get(i);
            const orig_amt = orig_col.get(i);
            const inv_us: ?i64 = if (inv_col.get(i)) |d| @as(i64, d.days()) * std.time.us_per_day else null;

            // converted_invoice_normalized: handler arm only when both
            // currencies are non-NULL and differ; otherwise the pass-through
            // arm (the 3VL dropped-row case also lands here — see header).
            const oan: ?i32 = blk: {
                if (orig_cur == null or norm == null or std.mem.eql(u8, orig_cur.?, norm.?)) {
                    break :blk orig_amt; // originalAmount AS originalAmountNormalized
                }
                if (std.mem.eql(u8, norm.?, "USD")) break :blk amount;
                const r_in: i64 = lookupRate(st, orig_cur, inv_us) orelse RATE_ONE;
                const r_out: i64 = lookupRate(st, norm, inv_us) orelse RATE_ONE;
                // CAST(COALESCE(c_out.rate,1)/COALESCE(c_in.rate,1) AS DOUBLE)
                const x: f64 = if (r_in == 0)
                    0.0
                else
                    @as(f64, @floatFromInt(roundDiv(@as(i128, r_out) * POW10_10, r_in))) / 1e10;
                const oa = orig_amt orelse break :blk null;
                // CAST(ROUND(originalAmount * x) AS INT)
                break :blk dblToI32(@round(@as(f64, @floatFromInt(oa)) * x));
            };

            // converted_invoice_target_currency.
            const conv: ?i32 = blk: {
                if (target_is_usd) break :blk amount; // amount AS converted_amount
                if (orig_cur != null and std.mem.eql(u8, orig_cur.?, args.targetCurrency)) {
                    break :blk orig_amt;
                }
                const r_t: i64 = lookupRate(st, args.targetCurrency, inv_us) orelse RATE_ONE;
                const am = amount orelse break :blk null;
                // CAST(ROUND(amount * COALESCE(c_target.rate,1)) AS INT),
                // exact decimal(16,6) arithmetic.
                break :blk satI32(roundDiv(@as(i128, am) * r_t, RATE_ONE));
            };

            // post_converted_invoice IF ladder; NULL lineItemType fails IN.
            const nonrec = if (ltype_col.get(i)) |v|
                std.mem.eql(u8, v, "revenue-nonrecurring") or std.mem.eql(u8, v, "adjustment-nonrecurring")
            else
                false;

            // with_plan_data: LEFT JOIN external_plan; NULL keys never match.
            const ipid: ?i32 = blk: {
                const pj = pid orelse break :blk null;
                const ic = icid_col.get(i) orelse break :blk null;
                const ep = plan_col.get(i) orelse break :blk null;
                break :blk plan_map.get(.{ .pid = pj, .icid = ic, .ep = ep });
            };

            // with_month_diff, NULL-propagating.
            const md: ?i32 = blk: {
                const d = (date_col.get(i) orelse break :blk null).ymd();
                const s = (start_col.get(i) orelse break :blk null).ymd();
                break :blk (d.y - s.y) * 12 + (@as(i32, d.m) - @as(i32, s.m));
            };

            try out.row(.{
                .normalizedCurrency = norm,
                .nonRecurringAmount = if (nonrec) conv else @as(?i32, 0),
                .originalNonRecurringAmount = if (nonrec) oan else @as(?i32, 0),
                .amount = if (nonrec) @as(?i32, 0) else conv,
                .originalAmount = if (nonrec) @as(?i32, 0) else oan,
                .internalPlanId = ipid,
                .monthDiff = md,
            });
        }

        run_start = run_end;
    }
}

/// Rate at (currency, invoiceDate): join misses on a NULL date and on any
/// absent (currency, datetime) pair — caller COALESCEs to 1.
fn lookupRate(st: *const State, cur: ?[]const u8, inv_us: ?i64) ?i64 {
    const cu = cur orelse return null;
    const us = inv_us orelse return null;
    if (st.rate_table) |*t| return t.lookup(cu, us);
    return st.rate_map.get(.{ .cur = cu, .us = us });
}
