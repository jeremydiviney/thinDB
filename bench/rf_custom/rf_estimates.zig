// Rollforward estimates generator: replaces the 7-block SQL cluster
// (last_month_invoices .. estimates_transformed) with one pass per
// (projectId, divisionId, customerNumberLC) partition. Input = invoice_base
// rows with date in [curMonth-2M, LAST_DAY(curMonth)]; output = estimate
// line items in invoice_base column order (the caller UNION ALLs them).
// Row-GENERATING (0..k rows per partition) — not row-aligned.
//
// SQL semantics reproduced (annotated per step). estimate_date_map is
// exactly targetDay = min(sourceDay, daysInMonth(target, leap)) — verified
// over all 662 rows — so the kernel computes it instead of joining.
// Pick-dependent aggregates (MAX_BY on tied DAY(), ANY_VALUE per invoice)
// use first-wins in input order (ORDER BY invoiceId, date at the call
// site); the byte-exact harness validates this matches the inline engine.
const std = @import("std");
const tdb = @import("thindb").tdb;

pub const spec = tdb.TableFnSpec{ .name = "rf_estimates", .execution = .partitioned };

pub const Args = struct {
    curMonth: []const u8, // 'YYYY-MM-DD' (first of month)
    currentDate: []const u8, // 'YYYY-MM-DD'
};

pub const Input = struct {
    projectId: ?i32,
    divisionId: ?i32,
    customerNumberHash: ?[]const u8,
    date: ?tdb.Date,
    originalAmount: ?i32,
    invoiceId: ?[]const u8,
    invoiceDate: ?tdb.Date,
    startDate: ?tdb.Date,
    originalCustomerNumber: ?[]const u8,
    originalCustomerName: ?[]const u8,
    originalCurrency: ?[]const u8,
    planId: ?[]const u8,
    lineItemType: ?[]const u8,
    integrationConfigId: ?i32,
    parentCustomerNumber: ?[]const u8,
    parentCustomerName: ?[]const u8,
    customerNumber: ?[]const u8,
    customerNumberLC: ?[]const u8,
    customerName: ?[]const u8,
    customerEmail: ?[]const u8,
    amount: ?i32,
};

// invoice_base column order — the UNION arm must line up.
pub const Output = Input;

// Tolerant 'YYYY-MM-DD' parse: the engine's validation subprocess calls the
// kernel with synthetic (non-date) string args, so malformed input must not
// panic — fall back to the epoch, which only ever surfaces in validation.
fn parseDate(s: []const u8) tdb.Date {
    if (s.len < 10) return tdb.Date.fromDays(0);
    const y = std.fmt.parseInt(i32, s[0..4], 10) catch return tdb.Date.fromDays(0);
    const m = std.fmt.parseInt(u8, s[5..7], 10) catch return tdb.Date.fromDays(0);
    const d = std.fmt.parseInt(u8, s[8..10], 10) catch return tdb.Date.fromDays(0);
    if (m < 1 or m > 12 or d < 1 or d > 31) return tdb.Date.fromDays(0);
    return tdb.Date.fromYmd(.{ .y = y, .m = m, .d = d });
}

fn daysInMonth(y: i32, m: u8) u8 {
    const leap = (@mod(y, 4) == 0 and @mod(y, 100) != 0) or @mod(y, 400) == 0;
    return switch (m) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (leap) 29 else 28,
        else => unreachable,
    };
}

fn inRange(d: tdb.Date, lo: tdb.Date, hi: tdb.Date) bool {
    return !d.lt(lo) and !hi.lt(d);
}

const InvoiceGroup = struct {
    id: []const u8,
    est_inv: ?tdb.Date, // ANY_VALUE(estimatedInvoiceDate) — first line in input order
    rank: usize = 0,
    qualifies: bool = false,
};

pub fn process(ctx: *tdb.Ctx, args: Args, p: tdb.Partition(Input), out: *tdb.Writer(Output)) !void {
    if (p.len == 0) return;
    const a = ctx.arena;

    const cm = parseDate(args.curMonth); // first of current month
    const cur_date = parseDate(args.currentDate);
    const cm_ymd = cm.ymd();
    const cm_dim = daysInMonth(cm_ymd.y, cm_ymd.m);
    const cm_end = tdb.Date.fromYmd(.{ .y = cm_ymd.y, .m = cm_ymd.m, .d = cm_dim }); // LAST_DAY(@curMonth)
    const lm1_start = cm.addMonths(-1); // DATE_ADD(@curMonth, INTERVAL -1 MONTH)
    const lm1_end = tdb.Date.fromDays(cm.days() - 1); // @curMonth - 1 DAY
    const lm2_start = cm.addMonths(-2);
    const lm2_end = tdb.Date.fromDays(lm1_start.days() - 1);

    const date_col = p.col(.date);
    const start_col = p.col(.startDate);
    const inv_date_col = p.col(.invoiceDate);
    const inv_id_col = p.col(.invoiceId);
    const line_type_col = p.col(.lineItemType);

    // ── customer_3month_aggregates (this partition IS the group) ──────────
    // COUNT(DISTINCT invoiceId) per month bucket + MAX_BY(startDate/
    // invoiceDate, DAY() in [cm-2M, cm-1D] else 0), excluding
    // 'revenue-nonrecurring'. Ties/no-key picks: first row wins.
    var this_ids = std.StringHashMapUnmanaged(void).empty;
    var last_ids = std.StringHashMapUnmanaged(void).empty;
    var two_ids = std.StringHashMapUnmanaged(void).empty;
    var max_start: ?tdb.Date = null;
    var max_start_key: i32 = std.math.minInt(i32);
    var max_inv: ?tdb.Date = null;
    var max_inv_key: i32 = std.math.minInt(i32);
    var has_agg_rows = false;

    for (0..p.len) |i| {
        const d = date_col.get(i) orelse continue;
        if (!inRange(d, lm2_start, cm_end)) continue;
        if (line_type_col.get(i)) |lt| {
            if (std.mem.eql(u8, lt, "revenue-nonrecurring")) continue;
        }
        has_agg_rows = true;
        const inv_id = inv_id_col.get(i);
        if (inv_id) |id| {
            if (inRange(d, cm, cm_end)) try this_ids.put(a, id, {});
            if (inRange(d, lm1_start, lm1_end)) try last_ids.put(a, id, {});
            if (inRange(d, lm2_start, lm2_end)) try two_ids.put(a, id, {});
        }
        const in_maxby_window = inRange(d, lm2_start, lm1_end);
        if (start_col.get(i)) |sd| {
            const key: i32 = if (in_maxby_window) sd.ymd().d else 0;
            if (key > max_start_key) {
                max_start_key = key;
                max_start = sd;
            }
        }
        if (inv_date_col.get(i)) |vd| {
            const key: i32 = if (in_maxby_window) vd.ymd().d else 0;
            if (key > max_inv_key) {
                max_inv_key = key;
                max_inv = vd;
            }
        }
    }
    // LEFT JOIN semantics: no agg row → all counts NULL → the sequence
    // filter (thisMonthCount + rank <= lastMonthCount) is never true.
    if (!has_agg_rows) return;
    const this_count = this_ids.count();
    const last_count = last_ids.count();
    const two_count = two_ids.count();

    // ── last_month_invoices + adjusted dates + estimate_candidates ────────
    const Line = struct {
        row: usize,
        est_bill: ?tdb.Date,
        est_inv: ?tdb.Date,
    };
    var lines = std.ArrayListUnmanaged(Line).empty;
    var groups = std.ArrayListUnmanaged(InvoiceGroup).empty;
    var group_of = std.StringHashMapUnmanaged(usize).empty;

    const single_pair = last_count == 1 and two_count == 1;
    for (0..p.len) |i| {
        const d = date_col.get(i) orelse continue;
        if (!inRange(d, lm1_start, lm1_end)) continue; // last_month_invoices

        // finalStartDate / finalInvoiceDate: month-end drift correction for
        // single-invoice customers (day >= 28 pulls the 2-month max day).
        const final_start: ?tdb.Date = blk: {
            const sd = start_col.get(i) orelse break :blk null;
            if (single_pair and sd.ymd().d >= 28) break :blk max_start orelse sd;
            break :blk sd;
        };
        const final_inv: ?tdb.Date = blk: {
            const vd = inv_date_col.get(i) orelse break :blk null;
            if (single_pair and vd.ymd().d >= 28) break :blk max_inv orelse vd;
            break :blk vd;
        };

        // estimate_date_map ≡ clamp the source day into the target month.
        const est_bill: ?tdb.Date = if (final_start) |fs|
            tdb.Date.fromYmd(.{ .y = cm_ymd.y, .m = cm_ymd.m, .d = @min(fs.ymd().d, cm_dim) })
        else
            null;
        const est_inv: ?tdb.Date = if (final_inv) |fi|
            tdb.Date.fromYmd(.{ .y = cm_ymd.y, .m = cm_ymd.m, .d = @min(fi.ymd().d, cm_dim) })
        else
            null;

        try lines.append(a, .{ .row = i, .est_bill = est_bill, .est_inv = est_inv });

        if (inv_id_col.get(i)) |id| {
            const gop = try group_of.getOrPut(a, id);
            if (!gop.found_existing) {
                gop.value_ptr.* = groups.items.len;
                try groups.append(a, .{ .id = id, .est_inv = est_inv });
            }
        }
    }
    if (groups.items.len == 0) return;

    // ── invoice_estimate_sequence: rank invoices by ANY_VALUE(est_inv)
    // DESC (NULLs last), invoiceId ASC. ─────────────────────────────────
    const order = try a.alloc(usize, groups.items.len);
    for (order, 0..) |*o, k| o.* = k;
    const Ctx2 = struct {
        groups: []const InvoiceGroup,
        fn less(c: @This(), x: usize, y: usize) bool {
            const gx = c.groups[x];
            const gy = c.groups[y];
            if (gx.est_inv == null and gy.est_inv == null) return std.mem.order(u8, gx.id, gy.id) == .lt;
            if (gx.est_inv == null) return false; // NULLs last in DESC
            if (gy.est_inv == null) return true;
            if (!gx.est_inv.?.eq(gy.est_inv.?)) return gy.est_inv.?.lt(gx.est_inv.?); // DESC
            return std.mem.order(u8, gx.id, gy.id) == .lt;
        }
    };
    std.mem.sortUnstable(usize, order, Ctx2{ .groups = groups.items }, Ctx2.less);
    for (order, 1..) |gi, rank| {
        groups.items[gi].rank = rank;
        // estimates_final count filter: (thisMonthCount + seq) <= lastMonthCount
        groups.items[gi].qualifies = this_count + rank <= last_count;
    }

    // ── estimates_final line filter + estimates_transformed emission ──────
    for (lines.items) |ln| {
        const id = inv_id_col.get(ln.row) orelse continue;
        const g = groups.items[group_of.get(id).?];
        if (!g.qualifies) continue;
        const bill_ok = if (ln.est_bill) |b| !b.lt(cur_date) else false;
        const inv_ok = if (ln.est_inv) |v| !v.lt(cur_date) else false;
        if (!bill_ok and !inv_ok) continue;

        const r = p.at(ln.row);
        var o: Output = r;
        o.date = if (ln.est_bill) |b| blk: { // LAST_DAY(estimatedBillDate)
            const by = b.ymd();
            break :blk tdb.Date.fromYmd(.{ .y = by.y, .m = by.m, .d = daysInMonth(by.y, by.m) });
        } else null;
        o.invoiceId = try std.fmt.allocPrint(a, "{s}-estimate", .{id});
        o.invoiceDate = ln.est_inv;
        o.startDate = ln.est_bill;
        try out.row(o);
    }
}
