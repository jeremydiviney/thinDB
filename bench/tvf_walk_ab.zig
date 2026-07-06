//! TVF flagship A/B: the rollforward gap-fill walk as ONE table UDF vs the
//! 6-CTE SQL chain it replaces (gap_meta → gap_fill_candidates → union →
//! gaps_filled → last_amounts → diffs). Runs both against the real wayroll
//! database EMBEDDED (registerTableFn is embedded-only until P2), compares
//! a per-month summary value-for-value, and times alternating passes.
//!
//!   zig build tvf-walk-ab            (ReleaseFast forced)
//!   ./zig-out/bin/tvf_walk_ab [db-dir] [passes]

const std = @import("std");
const thindb = @import("thindb");
const tdb = thindb.tdb;

// ---------------------------------------------------------------------------
// The kernel — written exactly as a user would write it. Input = the shape
// of rollforward_customer_records; output = the diffs CTE shape minus its
// 16 constant-zero columns.
// ---------------------------------------------------------------------------

const rf_walk = struct {
    pub const spec = tdb.TableFnSpec{ .name = "rf_walk", .execution = .partitioned };

    pub const Input = struct {
        projectId: i32,
        divisionId: i32,
        customerNumber: ?[]const u8,
        customerNumberLC: []const u8,
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

    pub const Output = struct {
        projectId: i32,
        divisionId: i32,
        customerNumberLC: []const u8,
        date: ?tdb.Date,
        minDate: ?tdb.Date,
        month: ?tdb.Date,
        amount: ?i64,
        originalAmount: ?i64,
        currency: ?[]const u8,
        exchangeRate: ?f64,
        planId: ?i32,
        hasAdjustment: ?i32,
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

    /// One synthesizable row: either a real input row (index) or a filler.
    const Pending = struct {
        month: ?tdb.Date,
        date: ?tdb.Date,
        min_date: ?tdb.Date,
        amount: ?i64,
        original: ?i64,
        currency: ?[]const u8,
        er: ?f64,
        plan: ?i32,
        has_adj: ?i32,
    };

    pub fn process(ctx: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Output)) !void {
        const project = p.key(.projectId);
        const division = p.key(.divisionId);
        const customer = p.key(.customerNumberLC);
        const months = p.col(.month);
        const dates = p.col(.date);
        const min_dates = p.col(.minDate);
        const amounts = p.col(.amount);
        const originals = p.col(.originalAmount);
        const currencies = p.col(.currency);
        const ers = p.col(.exchangeRate);
        const plans = p.col(.planId);
        const has_adjs = p.col(.hasAdjustment);

        // Pass 1: real rows + gap fillers, in month order (input is already
        // month-ordered; fillers land inside their gap, so a simple merge
        // emits everything ordered — same stream LAG saw after the UNION).
        var rows: std.ArrayList(Pending) = .empty;
        defer rows.deinit(ctx.arena);

        var prev_day_of_date: i32 = 0;
        for (0..p.len) |i| {
            const m = months.get(i);
            try rows.append(ctx.arena, .{
                .month = m,
                .date = dates.get(i),
                .min_date = min_dates.get(i),
                .amount = amounts.get(i),
                .original = originals.get(i),
                .currency = currencies.get(i),
                .er = ers.get(i),
                .plan = plans.get(i),
                .has_adj = has_adjs.get(i),
            });

            // Gap meta for THIS row: months to the next row's month (or
            // month+2 at the end), max day-of-date over this + prev row.
            const d = dates.get(i);
            const day_of_date: i32 = if (d) |dd| dd.ymd().d else 0;
            const last2 = @max(day_of_date, prev_day_of_date);
            prev_day_of_date = day_of_date;

            const this_month = m orelse continue;
            const is_end = i + 1 >= p.len;
            const next_month: tdb.Date = if (!is_end)
                months.get(i + 1) orelse continue
            else
                this_month.addMonths(2);
            const gap = monthSpan(this_month, next_month);
            if (gap <= 1) continue;
            // End-buffer rows only spawn fillers when the amount is nonzero
            // (the churn buffer month); mid-stream gaps always fill.
            if (is_end and (amounts.get(i) orelse 0) == 0) continue;

            var n: i32 = 1;
            while (n < gap) : (n += 1) {
                const fill_month = this_month.addMonths(n);
                const inj = injectionDate(d, fill_month, last2);
                try rows.append(ctx.arena, .{
                    .month = fill_month,
                    .date = inj,
                    .min_date = inj,
                    .amount = 0,
                    .original = 0,
                    .currency = currencies.get(i),
                    .er = ers.get(i),
                    .plan = plans.get(i),
                    .has_adj = 0,
                });
            }
        }

        std.mem.sortUnstable(Pending, rows.items, {}, monthLess);

        // rollforward_with_customer_start_date: whole-partition
        // MIN(minDate) over rows with originalAmount > 0 (fillers carry
        // originalAmount = 0, so they never contribute — same as SQL,
        // which computes this after gap filling).
        var customer_start: ?tdb.Date = null;
        for (rows.items) |r| {
            if ((r.original orelse 0) > 0) {
                if (r.min_date) |md| {
                    if (customer_start == null or md.lt(customer_start.?)) customer_start = md;
                }
            }
        }

        // Pass 2: LAG carries + diff arithmetic over the merged stream.
        var is_active: i64 = 0;
        var last_amount: ?i64 = 0;
        var last_original: ?i64 = 0;
        var last_plan: ?i32 = null;
        var last_er: ?f64 = 0;
        for (rows.items) |r| {
            const er_eff: ?f64 = if (r.er) |e|
                (if (e != 0) e else last_er)
            else
                last_er;
            var diff: ?i32 = null;
            if (r.original != null and last_original != null and er_eff != null) {
                const delta: f64 = @floatFromInt(r.original.? - last_original.?);
                diff = @intFromFloat(std.math.round(delta * er_eff.?));
            }
            var fx: ?i32 = null;
            if (r.amount != null and last_amount != null and diff != null) {
                fx = @intCast(r.amount.? - last_amount.? - diff.?);
            }

            // rollforward_with_updown: SQL CASE with 3VL — a NULL operand
            // fails its WHEN and falls through.
            const orig = r.original;
            const last_orig_c: i64 = last_original orelse 0;
            const up_down: []const u8 = blk: {
                if (orig != null and last_orig_c <= 0 and orig.? > 0) {
                    if (customer_start) |csd| {
                        if (r.month != null and csd.lt(r.month.?)) break :blk "reactivation";
                    }
                    break :blk "new";
                }
                if (orig != null and last_orig_c > 0 and orig.? <= 0) break :blk "churn";
                if (orig != null and last_original != null) {
                    const d = orig.? - last_original.?;
                    if (@abs(d) > 1 and d > 0) break :blk "up";
                    if (@abs(d) > 1 and d < 0) break :blk "down";
                }
                break :blk "same";
            };
            // rollforward_with_active_change + active_status: running sum
            // of the +1/-1 transitions, in month order.
            const active_change: i32 = if (std.mem.eql(u8, up_down, "churn"))
                -1
            else if (std.mem.eql(u8, up_down, "new") or std.mem.eql(u8, up_down, "reactivation"))
                1
            else
                0;
            is_active += active_change;

            try out.row(.{
                .projectId = project,
                .divisionId = division,
                .customerNumberLC = customer,
                .date = r.date,
                .minDate = r.min_date,
                .month = r.month,
                .amount = r.amount,
                .originalAmount = r.original,
                .currency = r.currency,
                .exchangeRate = r.er,
                .planId = r.plan,
                .hasAdjustment = r.has_adj,
                .lastAmount = last_amount,
                .lastOriginalAmount = last_original,
                .lastPlanId = last_plan,
                .lastExchangeRate = last_er,
                .diffAmount = diff,
                .fxChange = fx,
                .customerStartDate = customer_start,
                .upDown = up_down,
                .activeChange = active_change,
                .isActive = is_active,
            });
            last_amount = r.amount;
            last_original = r.original;
            last_plan = r.plan;
            last_er = r.er;
        }
    }

    fn monthLess(_: void, a: Pending, b: Pending) bool {
        const av = if (a.month) |m| m.days() else std.math.minInt(i32);
        const bv = if (b.month) |m| m.days() else std.math.minInt(i32);
        return av < bv;
    }

    /// Whole months between two first-of-month dates.
    fn monthSpan(a: tdb.Date, b: tdb.Date) i32 {
        const av = a.ymd();
        const bv = b.ymd();
        return (bv.y - av.y) * 12 + (@as(i32, bv.m) - @as(i32, av.m));
    }

    /// candidateInjectionDate: end-of-month dates anchor to the (clamped)
    /// max recent billing day inside the filler month; otherwise plain
    /// calendar month-add of the source date.
    fn injectionDate(src_date: ?tdb.Date, fill_month: tdb.Date, last2_day: i32) ?tdb.Date {
        const d = src_date orelse return null;
        const v = d.ymd();
        const last_day_src = lastDayOfMonth(v.y, v.m);
        if (v.d == last_day_src) {
            const fv = fill_month.ymd();
            const cap = lastDayOfMonth(fv.y, fv.m);
            const day: i32 = @min(last2_day, @as(i32, cap));
            return tdb.Date.fromYmd(.{ .y = fv.y, .m = fv.m, .d = @intCast(@max(1, day)) });
        }
        return d.addMonths(monthSpanFromDate(d, fill_month));
    }

    /// n such that MONTHS_ADD(date, n) lands in fill_month.
    fn monthSpanFromDate(d: tdb.Date, fill_month: tdb.Date) i32 {
        const dv = d.ymd();
        const fv = fill_month.ymd();
        return (fv.y - dv.y) * 12 + (@as(i32, fv.m) - @as(i32, dv.m));
    }

    fn lastDayOfMonth(y: i32, m: u8) u8 {
        const leap = (@mod(y, 4) == 0 and @mod(y, 100) != 0) or @mod(y, 400) == 0;
        return switch (m) {
            1, 3, 5, 7, 8, 10, 12 => 31,
            4, 6, 9, 11 => 30,
            2 => if (leap) 29 else 28,
            else => unreachable,
        };
    }
};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const CUT_B = ", rollforward_customer_records_for_union AS (";
const CUT_A = ", rollforward_with_plan_and_division_f7c96fc2 AS (";

const SUMMARY_COLS =
    "month, COUNT(*) AS n, SUM(amount) AS s_amount, SUM(lastAmount) AS s_last," ++
    " SUM(diffAmount) AS s_diff, SUM(fxChange) AS s_fx," ++
    " SUM(activeChange) AS s_ac, SUM(isActive) AS s_ia," ++
    " SUM(LENGTH(upDown)) AS s_ud, MIN(customerStartDate) AS s_csd";

fn buildSqls(allocator: std.mem.Allocator, io: std.Io, project: []const u8, division: []const u8) !struct { a: []u8, b: []u8 } {
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, "testSQL/rollforward_template.sql", allocator, .limited(4 << 20));
    defer allocator.free(raw);
    const p1 = try std.mem.replaceOwned(u8, allocator, raw, "{{PROJECT}}", project);
    defer allocator.free(p1);
    const tpl = try std.mem.replaceOwned(u8, allocator, p1, "{{DIVISION}}", division);
    defer allocator.free(tpl);
    const trimmed = std.mem.trimEnd(u8, tpl, "; \r\n\t");

    const cut_a = std.mem.indexOf(u8, trimmed, CUT_A) orelse return error.TemplateMarkerMissing;
    const sql_a = try std.fmt.allocPrint(allocator,
        \\{s}
        \\SELECT * FROM rollforward_with_active_status_f7c96fc2
        \\)
        \\SELECT {s} FROM primary_pipeline_mat1 GROUP BY month ORDER BY month
    , .{ trimmed[0..cut_a], SUMMARY_COLS });
    errdefer allocator.free(sql_a);

    const cut_b = std.mem.indexOf(u8, trimmed, CUT_B) orelse return error.TemplateMarkerMissing;
    const sql_b = try std.fmt.allocPrint(allocator,
        \\SELECT {s} FROM TABLE(rf_walk((
        \\{s}
        \\SELECT * FROM rollforward_customer_records
        \\)
        \\SELECT * FROM primary_pipeline_mat1
        \\)) PARTITION BY projectId, divisionId, customerNumberLC ORDER BY month)
        \\GROUP BY month ORDER BY month
    , .{ SUMMARY_COLS, trimmed[0..cut_b] });

    return .{ .a = sql_a, .b = sql_b };
}

fn registryFor(db: *thindb.Database) *const thindb.UdfRegistry {
    if (db.catalog) |catalog| return &catalog.udfs;
    return &db.owned_catalog.?.udfs;
}

/// Drain a query into canonical sorted lines (one per row, all columns
/// rendered; validity-aware).
fn runToLines(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, ms_out: *u64) ![][]u8 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const t0 = thindb.exec.prof.nowTicks();
    const root = try thindb.sql.parseDialectWithUdfs(arena.allocator(), sql, .neutral, registryFor(db));
    var cq = try thindb.net.compileWithSession(allocator, db, .{ .current_db = "wayroll" }, root);
    defer cq.deinit();
    defer thindb.net.CompiledQuery.freeSessionVars(allocator, cq.sessionValue().vars);

    var lines: std.ArrayList([]u8) = .empty;
    errdefer {
        for (lines.items) |l| allocator.free(l);
        lines.deinit(allocator);
    }
    while (try cq.next()) |batch| {
        for (0..batch.row_count) |i| {
            var line: std.ArrayList(u8) = .empty;
            errdefer line.deinit(allocator);
            for (batch.values) |v| {
                if (!v.isValid(i)) {
                    try line.appendSlice(allocator, "NULL|");
                    continue;
                }
                switch (v.data) {
                    .int, .date => |s| try line.print(allocator, "{d}|", .{@as([]const i32, s)[i]}),
                    .bigint, .datetime => |s| try line.print(allocator, "{d}|", .{@as([]const i64, s)[i]}),
                    .double => |s| try line.print(allocator, "{d:.4}|", .{s[i]}),
                    .float => |s| try line.print(allocator, "{d:.4}|", .{s[i]}),
                    .largeint, .decimal128 => |s| try line.print(allocator, "{d}|", .{@as([]const i128, s)[i]}),
                    .decimal64 => |s| try line.print(allocator, "{d}|", .{s[i]}),
                    .tinyint => |s| try line.print(allocator, "{d}|", .{s[i]}),
                    .smallint => |s| try line.print(allocator, "{d}|", .{s[i]}),
                    .boolean => |s| try line.print(allocator, "{d}|", .{s[i]}),
                    .varchar, .string, .char => |s| try line.print(allocator, "{s}|", .{s.rowBytes(i)}),
                    else => try line.appendSlice(allocator, "?|"),
                }
            }
            try lines.append(allocator, try line.toOwnedSlice(allocator));
        }
    }
    ms_out.* = @intFromFloat(thindb.exec.prof.ticksToMs(thindb.exec.prof.nowTicks() - t0));
    std.mem.sortUnstable([]u8, lines.items, {}, strLess);
    return lines.toOwnedSlice(allocator);
}

fn strLess(_: void, a: []u8, b: []u8) bool {
    return std.mem.lessThan(u8, a, b);
}

fn freeLines(allocator: std.mem.Allocator, lines: [][]u8) void {
    for (lines) |l| allocator.free(l);
    allocator.free(lines);
}

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{ .thread_safe = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path: []const u8 = if (getenv("THINDB_AB_DB")) |v| std.mem.span(v) else ".wayroll-db";
    const passes: usize = if (getenv("THINDB_AB_PASSES")) |p|
        try std.fmt.parseInt(usize, std.mem.span(p), 10)
    else
        3;

    var data_root = try std.Io.Dir.cwd().openDir(io, db_path, .{ .iterate = true });
    defer data_root.close(io);
    const catalog = try thindb.Catalog.open(allocator, io, data_root, .{ .max_dop = 12 });
    defer catalog.close();
    const db = catalog.database("wayroll") orelse return error.DatabaseNotFound;
    try db.registerTableFn(rf_walk);

    {
        var smoke_ms: u64 = 0;
        const smoke = runToLines(allocator, db, "SELECT COUNT(*) AS n FROM invoice_import_amortized", &smoke_ms) catch |err| {
            std.debug.print("smoke query failed: {t}\n", .{err});
            return err;
        };
        std.debug.print("smoke: {s} ({d}ms)\n", .{ smoke[0], smoke_ms });
        freeLines(allocator, smoke);
    }

    const sqls = try buildSqls(allocator, io, "1000073", "1000339");
    defer allocator.free(sqls.a);
    defer allocator.free(sqls.b);


    // Correctness gate first, then alternating timed passes (pass 0 warms).
    var ms_a: u64 = 0;
    var ms_b: u64 = 0;
    var best_a: u64 = std.math.maxInt(u64);
    var best_b: u64 = std.math.maxInt(u64);
    for (0..passes) |pass| {
        const la = runToLines(allocator, db, sqls.a, &ms_a) catch |err| {
            std.debug.print("CTE variant failed: {t}\n", .{err});
            return err;
        };
        defer freeLines(allocator, la);
        const lb = runToLines(allocator, db, sqls.b, &ms_b) catch |err| {
            std.debug.print("TVF variant failed: {t}\n", .{err});
            return err;
        };
        defer freeLines(allocator, lb);

        if (pass == 0) {
            var mismatches: usize = 0;
            if (la.len != lb.len) {
                std.debug.print("ROWS DIFFER: cte={d} tvf={d}\n", .{ la.len, lb.len });
                mismatches = 1;
            } else for (la, lb, 0..) |x, y, i| {
                if (!std.mem.eql(u8, x, y)) {
                    mismatches += 1;
                    if (mismatches <= 5) std.debug.print("row {d}:\n  cte {s}\n  tvf {s}\n", .{ i, x, y });
                }
            }
            std.debug.print("values: {s} ({d} summary rows)\n", .{
                if (mismatches == 0) "MATCH" else "MISMATCH",
                la.len,
            });
            if (mismatches != 0) {
                return error.ValueMismatch;
            }
        }
        best_a = @min(best_a, ms_a);
        best_b = @min(best_b, ms_b);
        std.debug.print("pass {d}: cte={d}ms tvf={d}ms\n", .{ pass, ms_a, ms_b });
    }
    std.debug.print("best: cte={d}ms tvf={d}ms ratio={d:.2}\n", .{
        best_a, best_b, @as(f64, @floatFromInt(best_b)) / @as(f64, @floatFromInt(@max(1, best_a))),
    });
}

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;
