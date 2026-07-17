// Rollforward up/down chain: one pass over each (projectId, divisionId,
// customerNumberLC) partition ordered by month, replacing 6 CTE blocks
// (rollforward_with_last_amounts .. rollforward_with_active_status).
// Full-width via operator pass-through: the kernel reads Input's 9 columns
// and emits the 10 Computed columns; the 37 pass-through columns (Input's
// keys + every Carry column) are materialized by the engine as permuted
// copies and never touch kernel code — no join-back needed, and the
// call-site input subquery supplies Input fields then Carry fields.
//
// Must stay value-identical to the inline SQL emission — the SQL semantics
// being reproduced are annotated on each step. Engine-probed semantics this
// kernel mirrors: thinDB ROUND() rounds half away from zero (@round) and
// CAST(x AS INT) truncates toward zero (@intFromFloat).
const std = @import("std");
const tdb = @import("thindb").tdb;

pub const spec = tdb.TableFnSpec{
    .name = "rf_updown_chain",
    .execution = .partitioned,
    .row_aligned = true,
};

pub const Args = struct { comparisonMonths: i64 };

pub const Input = struct {
    projectId: ?i32,
    divisionId: ?i32,
    customerNumberLC: ?[]const u8,
    month: ?tdb.Date,
    minDate: ?tdb.Date,
    amount: ?i64,
    originalAmount: ?i64,
    exchangeRate: ?f64,
    planId: ?i32,
};

pub const Carry = struct {
    customerNumber: ?[]const u8,
    customerName: ?[]const u8,
    customerEmail: ?[]const u8,
    customerNumberHash: ?[]const u8,
    parentCustomerNumber: ?[]const u8,
    parentCustomerName: ?[]const u8,
    date: ?tdb.Date,
    nonRecurringAmount: ?i64,
    originalNonRecurringAmount: ?i64,
    currency: ?[]const u8,
    integrationConfigId: ?i32,
    hasAdjustment: ?i32,
    childAddedToParentCount: ?i64,
    childRemovedFromParentCount: ?i64,
    childAddedPlanCount: ?i64,
    childRemovedPlanCount: ?i64,
    childUpCount: ?i64,
    childDownCount: ?i64,
    childAddedToParentAmount: ?i64,
    childRemovedFromParentAmount: ?i64,
    childAddedPlanAmount: ?i64,
    childRemovedPlanAmount: ?i64,
    childUpAmount: ?i64,
    childDownAmount: ?i64,
    crossSellCount: ?i64,
    crossChurnCount: ?i64,
    crossSellAmount: ?i64,
    crossChurnAmount: ?i64,
};

// Column order mirrors the inline rollforward_with_active_status block.
pub const Output = struct {
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
    lastAmount: ?i64,
    lastOriginalAmount: ?i64,
    lastPlanId: ?i32,
    lastExchangeRate: ?f64,
    childAddedToParentCount: ?i64,
    childRemovedFromParentCount: ?i64,
    childAddedPlanCount: ?i64,
    childRemovedPlanCount: ?i64,
    childUpCount: ?i64,
    childDownCount: ?i64,
    childAddedToParentAmount: ?i64,
    childRemovedFromParentAmount: ?i64,
    childAddedPlanAmount: ?i64,
    childRemovedPlanAmount: ?i64,
    childUpAmount: ?i64,
    childDownAmount: ?i64,
    crossSellCount: ?i64,
    crossChurnCount: ?i64,
    crossSellAmount: ?i64,
    crossChurnAmount: ?i64,
    diffAmount: ?i32,
    fxChange: ?i32,
    customerStartDate: ?tdb.Date,
    upDown: []const u8,
    activeChange: i32,
    isActive: i64,
};

pub const passthrough = .{
    "projectId",                   "divisionId",                 "customerNumber",     "customerNumberLC",
    "customerName",                "customerEmail",              "customerNumberHash", "parentCustomerNumber",
    "parentCustomerName",          "date",                       "minDate",            "month",
    "amount",                      "originalAmount",             "nonRecurringAmount", "originalNonRecurringAmount",
    "currency",                    "integrationConfigId",        "exchangeRate",       "planId",
    "hasAdjustment",               "childAddedToParentCount",    "childRemovedFromParentCount",
    "childAddedPlanCount",         "childRemovedPlanCount",      "childUpCount",       "childDownCount",
    "childAddedToParentAmount",    "childRemovedFromParentAmount",
    "childAddedPlanAmount",        "childRemovedPlanAmount",     "childUpAmount",      "childDownAmount",
    "crossSellCount",              "crossChurnCount",            "crossSellAmount",    "crossChurnAmount",
};

pub const Computed = struct {
    lastAmount: ?i64,
    lastOriginalAmount: ?i64,
    lastPlanId: ?i32,
    lastExchangeRate: ?f64,
    diffAmount: ?i32,
    fxChange: ?i32,
    customerStartDate: ?tdb.Date,
    upDown: []const u8,
    activeChange: i32,
    isActive: i64,
};

pub fn process(ctx: *tdb.Ctx, args: Args, p: tdb.Partition(Input), out: *tdb.Writer(Computed)) !void {
    if (p.len == 0) return;
    const cm: i32 = @intCast(args.comparisonMonths);
    const cm_usize: usize = @intCast(args.comparisonMonths);

    const month_col = p.col(.month);
    const min_date_col = p.col(.minDate);
    const amount_col = p.col(.amount);
    const orig_col = p.col(.originalAmount);
    const rate_col = p.col(.exchangeRate);
    const plan_col = p.col(.planId);

    // customerStartDate = MIN(CASE WHEN originalAmount > 0 THEN minDate END)
    // OVER (PARTITION BY ...) — whole-partition scan, NULLs ignored by MIN.
    var customer_start_date: ?tdb.Date = null;
    for (0..p.len) |i| {
        const orig = orig_col.get(i) orelse continue;
        if (orig <= 0) continue;
        const md = min_date_col.get(i) orelse continue;
        if (customer_start_date == null or md.lt(customer_start_date.?)) customer_start_date = md;
    }

    // isActive = SUM(activeChange) OVER (PARTITION BY ..., MOD(rn, cm)
    // ORDER BY month ROWS UNBOUNDED PRECEDING..CURRENT): one running sum
    // per residue class of the 1-based row number.
    const class_sums = try ctx.arena.alloc(i64, cm_usize);
    @memset(class_sums, 0);

    for (0..p.len) |i| {
        // LAG(x, cm, default) — row-based offset within the partition.
        const has_lag = i >= cm_usize;
        const j = if (has_lag) i - cm_usize else 0;
        const last_amount: ?i64 = if (has_lag) amount_col.get(j) else 0;
        const last_orig: ?i64 = if (has_lag) orig_col.get(j) else 0;
        const last_plan: ?i32 = if (has_lag) plan_col.get(j) else null;
        const last_rate: ?f64 = if (has_lag) rate_col.get(j) else 0;

        const amount = amount_col.get(i);
        const orig = orig_col.get(i);
        const rate_cur = rate_col.get(i);
        const month = month_col.get(i);

        // IF(exchangeRate <> 0, exchangeRate, lastExchangeRate): a NULL
        // condition takes the false branch, same as exchangeRate = 0.
        const rate: ?f64 = blk: {
            if (rate_cur) |er| {
                if (er != 0) break :blk er;
            }
            break :blk last_rate;
        };

        // diffAmount = CAST(ROUND((orig - lastOrig) * rate) AS INT);
        // fxChange = CAST(amount - lastAmount - ROUND(...) AS INT).
        // ROUND yields an integral double so the INT truncation is exact.
        var rounded_diff: ?f64 = null;
        if (orig != null and last_orig != null and rate != null) {
            const d: f64 = @floatFromInt(orig.? - last_orig.?);
            rounded_diff = @round(d * rate.?);
        }
        const diff_amount: ?i32 = if (rounded_diff) |rd| @intFromFloat(rd) else null;
        const fx_change: ?i32 = if (amount != null and last_amount != null and rounded_diff != null)
            @intFromFloat(@as(f64, @floatFromInt(amount.? - last_amount.?)) - rounded_diff.?)
        else
            null;

        // upDown CASE ladder, 3VL: a NULL comparison never satisfies a WHEN.
        const last_c: i64 = last_orig orelse 0; // COALESCE(lastOriginalAmount, 0)
        const up_down: []const u8 = blk: {
            if (last_c <= 0 and orig != null and orig.? > 0) {
                // reactivation: customerStartDate IS NOT NULL AND
                // customerStartDate < DATE_ADD(month, INTERVAL -(cm-1) MONTH)
                if (customer_start_date != null and month != null and
                    customer_start_date.?.lt(month.?.addMonths(-(cm - 1))))
                    break :blk "reactivation";
                break :blk "new";
            }
            if (last_c > 0 and orig != null and orig.? <= 0) break :blk "churn";
            if (orig != null and last_orig != null and @abs(orig.? - last_orig.?) > 1) {
                if (orig.? > last_orig.?) break :blk "up";
                if (orig.? < last_orig.?) break :blk "down";
            }
            break :blk "same";
        };

        const active_change: i32 = if (std.mem.eql(u8, up_down, "churn"))
            -1
        else if (std.mem.eql(u8, up_down, "new") or std.mem.eql(u8, up_down, "reactivation"))
            1
        else
            0;

        const rn = i + 1; // ROW_NUMBER() ORDER BY month, months unique per partition
        const cls = rn % cm_usize;
        class_sums[cls] += active_change;

        try out.row(.{
            .lastAmount = last_amount,
            .lastOriginalAmount = last_orig,
            .lastPlanId = last_plan,
            .lastExchangeRate = last_rate,
            .diffAmount = diff_amount,
            .fxChange = fx_change,
            .customerStartDate = customer_start_date,
            .upDown = up_down,
            .activeChange = active_change,
            .isActive = class_sums[cls],
        });
    }
}
