// Rollforward gap fill: replaces the 5-block SQL cluster
// (rollforward_customer_records_with_gap_meta .. the gaps-filled UNION ALL)
// with one pass per (projectId, divisionId, customerNumberLC) partition
// ordered by month. Input = rollforward_customer_records (21 cols, one row
// per customer-month); output = the same rows plus a zero-amount synthetic
// row for every skipped month between consecutive real months (and a
// comparisonMonths-long trailing buffer after the last month). Months are
// unique within a partition (grouped upstream), so emission order carries
// no tie ambiguity. Row-GENERATING — not row-aligned.
//
// Must stay value-identical to the inline SQL emission; the SQL being
// reproduced is annotated per step. Engine-probed semantics mirrored here:
// MONTHS_ADD clamps the day to the target month's end (same as
// Date.addMonths), and MONTHS_DIFF counts complete months (day-aware).
const std = @import("std");
const tdb = @import("thindb").tdb;

// ordered_output: the loop below emits each partition's rows in
// nondecreasing month order by construction (originals interleaved with
// their gap rows), so the engine advertises the output order and the
// downstream rf_updown_chain call skips its input sort (TVF-to-TVF ride).
pub const spec = tdb.TableFnSpec{ .name = "rf_gap_fill", .execution = .partitioned, .ordered_output = true };

pub const Args = struct { comparisonMonths: i64 };

pub const Input = struct {
    projectId: ?i32,
    divisionId: ?i32,
    customerNumber: ?[]const u8,
    customerNumberLC: ?[]const u8,
    customerName: ?[]const u8,
    customerEmail: ?[]const u8,
    customerNumberHash: ?[]const u8,
    parentCustomerNumber: ?[]const u8,
    parentCustomerName: ?[]const u8,
    date: ?tdb.Date,
    minDate: ?tdb.Date,
    month: ?tdb.Date,
    amount: ?i64,
    originalAmount: ?i64,
    nonRecurringAmount: ?i64,
    originalNonRecurringAmount: ?i64,
    currency: ?[]const u8,
    integrationConfigId: ?i32,
    exchangeRate: ?f64,
    planId: ?i32,
    hasAdjustment: ?i32,
};

pub const Output = Input;

fn daysInMonth(y: i32, m: u8) u8 {
    const leap = (@mod(y, 4) == 0 and @mod(y, 100) != 0) or @mod(y, 400) == 0;
    return switch (m) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (leap) 29 else 28,
        else => 30,
    };
}

// MONTHS_DIFF(a, b): complete months from b to a (engine-probed:
// MONTHS_DIFF('2026-04-15','2026-01-20') = 2 — the raw month distance minus
// one when a's day hasn't reached b's day yet).
fn monthsDiff(a: tdb.Date, b: tdb.Date) i32 {
    const va = a.ymd();
    const vb = b.ymd();
    var months: i32 = (va.y - vb.y) * 12 + (@as(i32, va.m) - @as(i32, vb.m));
    if (months > 0 and va.d < vb.d) months -= 1;
    if (months < 0 and va.d > vb.d) months += 1;
    return months;
}

pub fn process(ctx: *tdb.Ctx, args: Args, p: tdb.Partition(Input), out: *tdb.Writer(Output)) !void {
    _ = ctx;
    if (p.len == 0) return;
    const cm: i32 = @intCast(args.comparisonMonths);

    const month_col = p.col(.month);
    const date_col = p.col(.date);
    const amount_col = p.col(.amount);

    for (0..p.len) |i| {
        const row = p.at(i);
        try out.row(row); // rollforward_customer_records_for_union arm, verbatim

        const m = month_col.get(i) orelse continue; // NULL month: MONTHS_DIFF(_, NULL) is NULL -> no gaps

        // nextMonthGap = MONTHS_DIFF(COALESCE(LEAD(month), month + (cm+1) months), month);
        // isEndBuffer = LEAD(month) IS NULL. LEAD is NULL past the partition
        // end AND when the next row's month is NULL.
        const next: ?tdb.Date = if (i + 1 < p.len) month_col.get(i + 1) else null;
        const gap: i32 = if (next) |nm| monthsDiff(nm, m) else cm + 1;
        if (gap <= 1) continue;
        const is_end_buffer = next == null;
        // WHERE ... AND (isEndBuffer = 0 OR @comparisonMonths > 1 OR amount <> 0)
        const amt_nonzero = if (amount_col.get(i)) |v| v != 0 else false;
        if (is_end_buffer and cm <= 1 and !amt_nonzero) continue;

        // lastDayOfMonthLast2Months = MAX(DAY(date)) OVER (... ROWS BETWEEN
        // 1 PRECEDING AND CURRENT ROW) — MAX skips NULL days.
        var last_day2: ?i32 = null;
        if (date_col.get(i)) |d| last_day2 = d.ymd().d;
        if (i > 0) {
            if (date_col.get(i - 1)) |d| {
                const dd: i32 = d.ymd().d;
                last_day2 = if (last_day2) |x| @max(x, dd) else dd;
            }
        }

        const date_i = date_col.get(i);
        // CASE WHEN DAY(r.date) = DAY(LAST_DAY(r.date)) — NULL date fails the
        // condition (NULL comparison) and falls to the MONTHS_ADD else-branch.
        const date_is_month_end = if (date_i) |d| blk: {
            const v = d.ymd();
            break :blk v.d == daysInMonth(v.y, v.m);
        } else false;

        var gi: i32 = 1;
        while (gi <= gap - 1) : (gi += 1) {
            const gm = m.addMonths(gi); // CAST(MONTHS_ADD(r.month, n.id) AS DATE)
            var inj: ?tdb.Date = null; // candidateInjectionDate
            if (date_is_month_end) {
                // first-of-gm + (LEAST(lastDay2, DAY(LAST_DAY(gm))) - 1) days;
                // NULL lastDay2 makes the whole expression NULL.
                if (last_day2) |ld| {
                    const gv = gm.ymd();
                    const gm_dim: i32 = daysInMonth(gv.y, gv.m);
                    const first = tdb.Date.fromYmd(.{ .y = gv.y, .m = gv.m, .d = 1 });
                    inj = tdb.Date.fromDays(first.days() + @min(ld, gm_dim) - 1);
                }
            } else if (date_i) |d| {
                inj = d.addMonths(gi); // MONTHS_ADD(r.date, n.id), day clamped
            }

            var g = row; // gap_fill_candidates_for_union arm
            g.date = inj;
            g.minDate = inj;
            g.month = gm;
            g.amount = 0;
            g.originalAmount = 0;
            g.nonRecurringAmount = 0;
            g.originalNonRecurringAmount = 0;
            g.hasAdjustment = 0;
            try out.row(g);
        }
    }
}
