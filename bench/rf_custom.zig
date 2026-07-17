//! rf_custom: purpose-built pipeline for the wayroll rollforward query
//! (task #183 speed-of-light probe). Uses the storage scan primitive
//! (Scan + fused filter + zonemap prunes) and the four rollforward Zig
//! kernels, with hand-written connectors replacing all the SQL between
//! them. Structure: shard by customerNumberLC once at scan time, then run
//! the entire chain shard-parallel with no barriers — every downstream
//! stage partitions on the same key. Two tiny broadcast joins (division,
//! external_plan); customer_monthly_totals is dead (WHERE 1=0).
//!
//!   zig build rf-custom -Doptimize=ReleaseFast
//!   ./zig-out/bin/rf_custom            (env RF_DB / RF_DBNAME / RF_THREADS / RF_SHARDS)
//!
//! The bench server must be STOPPED (this opens .wayroll-bench-db).
//! Reference values: the engine ladder caps in the session scratchpad
//! (ladder_on_p3..p15.txt) — milestone B validates the p3 cap (union of
//! invoice_base + rf_estimates output).

const std = @import("std");
const thindb = @import("thindb");
const tdb = thindb.tdb;
const rf_estimates = @import("rf_custom/rf_estimates.zig");
const rf_currency = @import("rf_custom/rf_currency_convert.zig");
const rf_gap_fill = @import("rf_custom/rf_gap_fill.zig");
const rf_updown = @import("rf_custom/rf_updown_chain.zig");

const PROJECT_ID: i32 = 1000073;
const MODEL_TYPE = "mrr";
const CUR_MONTH = "2026-07-01";
const CURRENT_DATE = "2026-07-14";
// Engine CURDATE() frozen to the reference-capture day (2026-07-16 + 3y).
const DATE_CEIL_Y = 2029;
const DATE_CEIL_M = 7;
const DATE_CEIL_D = 16;

const LINE_ITEM_TYPES = [_][]const u8{
    "credit",               "credit-prorated",        "discount-nonrecurring",
    "discount-recurring",   "revenue-recurring",      "adjustment-recurring",
    "revenue-nonrecurring", "adjustment-nonrecurring",
};

/// invoice_base scan projection, in batch order.
const SCAN_COLS = [_][]const u8{
    "projectId",              "divisionId",           "customerNumberHash", "date",
    "originalAmount",         "invoiceId",            "invoiceDate",        "startDate",
    "originalCustomerNumber", "originalCustomerName", "originalCurrency",   "planId",
    "lineItemType",           "integrationConfigId",  "customerNumber",     "customerName",
    "customerEmail",          "amount",
};

/// Shard column layout == rf_estimates.Input field order (21 cols), so a
/// kernel partition is just views over these stores. parentCustomerNumber /
/// parentCustomerName are the invoice_base NULL literals; customerNumberLC
/// is computed at scatter time.
const SH = struct {
    const projectId = 0;
    const divisionId = 1;
    const customerNumberHash = 2;
    const date = 3;
    const originalAmount = 4;
    const invoiceId = 5;
    const invoiceDate = 6;
    const startDate = 7;
    const originalCustomerNumber = 8;
    const originalCustomerName = 9;
    const originalCurrency = 10;
    const planId = 11;
    const lineItemType = 12;
    const integrationConfigId = 13;
    const parentCustomerNumber = 14;
    const parentCustomerName = 15;
    const customerNumber = 16;
    const customerNumberLC = 17;
    const customerName = 18;
    const customerEmail = 19;
    const amount = 20;
    const N = 21;
};
/// Shard column names in SH order (for the runtime scan-batch mapping —
/// the scan emits columns in TABLE-PHYSICAL order, not request order).
const SHARD_NAMES = [SH.N][]const u8{
    "projectId",              "divisionId",           "customerNumberHash", "date",
    "originalAmount",         "invoiceId",            "invoiceDate",        "startDate",
    "originalCustomerNumber", "originalCustomerName", "originalCurrency",   "planId",
    "lineItemType",           "integrationConfigId",  "parentCustomerNumber",
    "parentCustomerName",     "customerNumber",       "customerNumberLC",   "customerName",
    "customerEmail",          "amount",
};

const ColumnStore = tdb.ColumnStore;
const ColumnType = tdb.ColumnType;
const ColumnView = tdb.ColumnView;

const SHARD_META = [SH.N]struct { t: ColumnType, n: bool }{
    .{ .t = .int, .n = false },   .{ .t = .int, .n = true },    .{ .t = .string, .n = true },  .{ .t = .date, .n = false },
    .{ .t = .int, .n = true },    .{ .t = .string, .n = false }, .{ .t = .date, .n = true },    .{ .t = .date, .n = true },
    .{ .t = .string, .n = true }, .{ .t = .string, .n = true }, .{ .t = .string, .n = true },  .{ .t = .string, .n = true },
    .{ .t = .string, .n = true }, .{ .t = .int, .n = false },   .{ .t = .string, .n = true },  .{ .t = .string, .n = true },
    .{ .t = .string, .n = true }, .{ .t = .string, .n = true }, .{ .t = .string, .n = true },  .{ .t = .string, .n = true },
    .{ .t = .int, .n = true },
};

const ShardBuf = struct {
    cols: [SH.N]ColumnStore,
    rows: usize = 0,

    fn init(alloc: std.mem.Allocator) !ShardBuf {
        var self: ShardBuf = .{ .cols = undefined };
        var inited: usize = 0;
        errdefer for (self.cols[0..inited]) |*c| c.deinit(alloc);
        for (&self.cols, SHARD_META) |*c, m| {
            c.* = try ColumnStore.init(alloc, m.t, m.n);
            inited += 1;
        }
        return self;
    }

    fn deinit(self: *ShardBuf, alloc: std.mem.Allocator) void {
        for (&self.cols) |*c| c.deinit(alloc);
    }
};

// ---------------------------------------------------------------------------
// Broadcasts
// ---------------------------------------------------------------------------

const Broadcasts = struct {
    arena: std.heap.ArenaAllocator,
    division_names: std.AutoHashMapUnmanaged(i32, []const u8) = .empty,
    plan_project_id: std.ArrayListUnmanaged(i32) = .empty,
    plan_icid: std.ArrayListUnmanaged(i32) = .empty,
    plan_external_id: std.ArrayListUnmanaged([]const u8) = .empty,
    plan_id: std.ArrayListUnmanaged(i32) = .empty,
    plan_name: std.ArrayListUnmanaged(?[]const u8) = .empty,
    plan_by_id: std.AutoHashMapUnmanaged(i32, usize) = .empty,
    rate_currency: std.ArrayListUnmanaged([]const u8) = .empty,
    rate_us: std.ArrayListUnmanaged(i64) = .empty,
    rate_micros: std.ArrayListUnmanaged(i64) = .empty,
    /// The same broadcasts as kernel partitions (rf_currency Input2/Input3).
    rate_stores: [3]ColumnStore = undefined,
    plan_stores: [4]ColumnStore = undefined,
    rate_views: [3]ColumnView = undefined,
    plan_views: [4]ColumnView = undefined,
    rate_part: tdb.TvfPartition = undefined,
    plan_part: tdb.TvfPartition = undefined,

    fn buildKernelInputs(b: *Broadcasts) !void {
        const a = b.arena.allocator();
        b.rate_stores[0] = try ColumnStore.init(a, .string, true);
        b.rate_stores[1] = try ColumnStore.init(a, .datetime, true);
        b.rate_stores[2] = try ColumnStore.init(a, .bigint, true);
        for (b.rate_currency.items, b.rate_us.items, b.rate_micros.items) |cu, us, rm| {
            try appendStr(a, &b.rate_stores[0], cu);
            try b.rate_stores[1].data.datetime.append(a, us);
            try b.rate_stores[1].appendValidBit(a, b.rate_stores[1].rowCount() - 1, true);
            try b.rate_stores[2].data.bigint.append(a, rm);
            try b.rate_stores[2].appendValidBit(a, b.rate_stores[2].rowCount() - 1, true);
        }
        b.plan_stores[0] = try ColumnStore.init(a, .int, true);
        b.plan_stores[1] = try ColumnStore.init(a, .int, true);
        b.plan_stores[2] = try ColumnStore.init(a, .string, true);
        b.plan_stores[3] = try ColumnStore.init(a, .int, true);
        for (0..b.plan_id.items.len) |i| {
            try b.plan_stores[0].data.int.append(a, b.plan_project_id.items[i]);
            try b.plan_stores[0].appendValidBit(a, i, true);
            try b.plan_stores[1].data.int.append(a, b.plan_icid.items[i]);
            try b.plan_stores[1].appendValidBit(a, i, true);
            try appendStr(a, &b.plan_stores[2], b.plan_external_id.items[i]);
            try b.plan_stores[3].data.int.append(a, b.plan_id.items[i]);
            try b.plan_stores[3].appendValidBit(a, i, true);
        }
        for (&b.rate_views, &b.rate_stores) |*v, *s| v.* = s.view();
        for (&b.plan_views, &b.plan_stores) |*v, *s| v.* = s.view();
        b.rate_part = .{ .columns = &b.rate_views, .row_count = b.rate_us.items.len, .keys = &.{} };
        b.plan_part = .{ .columns = &b.plan_views, .row_count = b.plan_id.items.len, .keys = &.{} };
    }
};

var db_name_for_session: []const u8 = "wayroll_prod";

fn registryFor(db: *thindb.Database) *const thindb.UdfRegistry {
    if (db.catalog) |catalog| return &catalog.udfs;
    return &db.owned_catalog.?.udfs;
}

fn runSql(
    allocator: std.mem.Allocator,
    db: *thindb.Database,
    sql: []const u8,
    ctx: anytype,
    comptime sink: fn (@TypeOf(ctx), thindb.exec.Batch) anyerror!void,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parseDialectWithUdfs(arena.allocator(), sql, .neutral, registryFor(db));
    var cq = try thindb.net.compileWithSession(allocator, db, .{ .current_db = db_name_for_session }, root);
    defer cq.deinit();
    defer thindb.net.CompiledQuery.freeSessionVars(allocator, cq.sessionValue().vars);
    while (try cq.next()) |batch| try sink(ctx, batch);
}

fn strOrNull(v: ColumnView, i: usize) ?[]const u8 {
    if (!v.isValid(i)) return null;
    return switch (v.data) {
        .varchar, .string, .char, .json => |s| s.rowBytes(i),
        else => unreachable,
    };
}

fn loadBroadcasts(allocator: std.mem.Allocator, db: *thindb.Database, b: *Broadcasts) !void {
    const a = b.arena.allocator();

    const DivCtx = struct { a: std.mem.Allocator, b: *Broadcasts };
    try runSql(allocator, db, "SELECT id, name FROM division", DivCtx{ .a = a, .b = b }, struct {
        fn sink(c: DivCtx, batch: thindb.exec.Batch) anyerror!void {
            for (0..batch.row_count) |i| {
                const id = batch.values[0].data.int[i];
                const name = strOrNull(batch.values[1], i) orelse "";
                try c.b.division_names.put(c.a, id, try c.a.dupe(u8, name));
            }
        }
    }.sink);

    const PlanCtx = struct { a: std.mem.Allocator, b: *Broadcasts };
    try runSql(allocator, db,
        "SELECT projectId, integrationConfigId, externalPlanId, id, planName FROM external_plan WHERE projectId = 1000073",
        PlanCtx{ .a = a, .b = b }, struct {
        fn sink(c: PlanCtx, batch: thindb.exec.Batch) anyerror!void {
            for (0..batch.row_count) |i| {
                const row = c.b.plan_id.items.len;
                try c.b.plan_project_id.append(c.a, if (batch.values[0].isValid(i)) batch.values[0].data.int[i] else -1);
                try c.b.plan_icid.append(c.a, if (batch.values[1].isValid(i)) batch.values[1].data.int[i] else -1);
                const ep = strOrNull(batch.values[2], i) orelse "";
                try c.b.plan_external_id.append(c.a, try c.a.dupe(u8, ep));
                const id: i32 = if (batch.values[3].isValid(i)) batch.values[3].data.int[i] else -1;
                try c.b.plan_id.append(c.a, id);
                const pn = strOrNull(batch.values[4], i);
                try c.b.plan_name.append(c.a, if (pn) |s| try c.a.dupe(u8, s) else null);
                if (id != -1) try c.b.plan_by_id.put(c.a, id, row);
            }
        }
    }.sink);

    const RateCtx = struct { a: std.mem.Allocator, b: *Broadcasts };
    try runSql(allocator, db,
        "SELECT currencyTo, date, CAST(rate * 1000000 AS BIGINT) AS rateMicros FROM currency_exchange_rate WHERE currencyBase = 'USD'",
        RateCtx{ .a = a, .b = b }, struct {
        fn sink(c: RateCtx, batch: thindb.exec.Batch) anyerror!void {
            for (0..batch.row_count) |i| {
                if (!batch.values[1].isValid(i) or !batch.values[2].isValid(i)) continue;
                const cu = strOrNull(batch.values[0], i) orelse continue;
                try c.b.rate_currency.append(c.a, try c.a.dupe(u8, cu));
                try c.b.rate_us.append(c.a, batch.values[1].data.datetime[i]);
                try c.b.rate_micros.append(c.a, batch.values[2].data.bigint[i]);
            }
        }
    }.sink);
}

// ---------------------------------------------------------------------------
// Phase 1: parallel fused-filter scan + shard scatter
// ---------------------------------------------------------------------------

fn asciiLower(dst: []u8, src: []const u8) []const u8 {
    const n = @min(dst.len, src.len);
    for (src[0..n], dst[0..n]) |ch, *o| o.* = if (ch >= 'A' and ch <= 'Z') ch + 32 else ch;
    return dst[0..n];
}

fn appendStr(alloc: std.mem.Allocator, store: *ColumnStore, s: ?[]const u8) !void {
    if (s == null) {
        try store.appendNulls(alloc, 1);
        return;
    }
    switch (store.data) {
        .varchar, .string, .char, .json => |*d| try d.appendValue(alloc, s.?),
        else => unreachable,
    }
    if (store.nulls != null) try store.appendValidBit(alloc, store.rowCount() - 1, true);
}

fn appendFromView(alloc: std.mem.Allocator, store: *ColumnStore, v: ColumnView, i: usize) !void {
    if (store.nulls != null and !v.isValid(i)) {
        try store.appendNulls(alloc, 1);
        return;
    }
    switch (v.data) {
        .int => |s| try store.data.int.append(alloc, s[i]),
        .date => |s| try store.data.date.append(alloc, s[i]),
        .bigint => |s| try store.data.bigint.append(alloc, s[i]),
        .double => |s| try store.data.double.append(alloc, s[i]),
        .datetime => |s| try store.data.datetime.append(alloc, s[i]),
        .varchar, .string, .char => |s| switch (store.data) {
            .varchar, .string, .char, .json => |*d| try d.appendValue(alloc, s.rowBytes(i)),
            else => unreachable,
        },
        else => unreachable,
    }
    if (store.nulls != null) try store.appendValidBit(alloc, store.rowCount() - 1, true);
}

const ScanShared = struct {
    scans: []*thindb.exec.Scan,
    next: std.atomic.Value(usize) = .init(0),
    n_shards: usize,
    alloc: std.mem.Allocator,
    worker_shards: []ShardBuf,
    errs: []?anyerror,
    /// scan batch column index -> shard column index (physical order map)
    scan_to_shard: [SCAN_COLS.len]usize,
    /// batch index of customerNumber (for the LC compute + shard hash)
    cust_ci: usize,
};

fn scanWorker(sh: *ScanShared, w: usize) void {
    scanWorkerInner(sh, w) catch |e| {
        sh.errs[w] = e;
    };
}

fn scanWorkerInner(sh: *ScanShared, w: usize) !void {
    const my = sh.worker_shards[w * sh.n_shards .. (w + 1) * sh.n_shards];
    var lc_buf: [512]u8 = undefined;
    while (true) {
        const i = sh.next.fetchAdd(1, .monotonic);
        if (i >= sh.scans.len) break;
        const scan = sh.scans[i];
        while (try scan.next()) |batch| {
            for (0..batch.row_count) |r| {
                const cust = strOrNull(batch.values[sh.cust_ci], r);
                const lc: ?[]const u8 = if (cust) |cn| asciiLower(&lc_buf, cn) else null;
                const shard = std.hash.Wyhash.hash(0x9e3779b9, lc orelse "") % sh.n_shards;
                const sb = &my[shard];
                for (0..SCAN_COLS.len) |ci| {
                    try appendFromView(sh.alloc, &sb.cols[sh.scan_to_shard[ci]], batch.values[ci], r);
                }
                try sb.cols[SH.parentCustomerNumber].appendNulls(sh.alloc, 1);
                try sb.cols[SH.parentCustomerName].appendNulls(sh.alloc, 1);
                try appendStr(sh.alloc, &sb.cols[SH.customerNumberLC], lc);
                sb.rows += 1;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 2 (milestone B): consolidate -> sort -> rf_estimates -> p3 cap
// ---------------------------------------------------------------------------

/// Bulk-append a whole column view onto a store (concat).
fn appendViewAll(alloc: std.mem.Allocator, store: *ColumnStore, v: ColumnView, rows: usize) !void {
    switch (v.data) {
        .int => |s| try store.data.int.appendSlice(alloc, s[0..rows]),
        .date => |s| try store.data.date.appendSlice(alloc, s[0..rows]),
        .varchar, .string, .char, .json => |s| switch (store.data) {
            .varchar, .string, .char, .json => |*d| {
                for (0..rows) |i| try d.appendValue(alloc, s.rowBytes(i));
            },
            else => unreachable,
        },
        else => unreachable,
    }
    if (store.nulls != null) {
        const base = store.rowCount() - rows;
        for (0..rows) |i| try store.appendValidBit(alloc, base + i, v.isValid(i));
    }
}

/// rf_currency Computed column stores (row-aligned with the shard buf).
const CU = struct {
    const norm = 0;
    const nonrec = 1;
    const orignonrec = 2;
    const amount = 3;
    const orig = 4;
    const ipid = 5;
    const mdiff = 6;
    const N = 7;
};
const CUR_META = [CU.N]struct { t: ColumnType, n: bool }{
    .{ .t = .string, .n = true }, .{ .t = .int, .n = true }, .{ .t = .int, .n = true }, .{ .t = .int, .n = true },
    .{ .t = .int, .n = true },    .{ .t = .int, .n = true }, .{ .t = .int, .n = true },
};

/// customer_agg_by_month output = rf_gap_fill.Input column order (21 cols).
const AG = struct {
    const projectId = 0;
    const divisionId = 1;
    const customerNumber = 2;
    const customerNumberLC = 3;
    const customerName = 4;
    const customerEmail = 5;
    const customerNumberHash = 6;
    const parentCustomerNumber = 7;
    const parentCustomerName = 8;
    const date = 9;
    const minDate = 10;
    const month = 11;
    const amount = 12;
    const originalAmount = 13;
    const nonRecurringAmount = 14;
    const originalNonRecurringAmount = 15;
    const currency = 16;
    const integrationConfigId = 17;
    const exchangeRate = 18;
    const planId = 19;
    const hasAdjustment = 20;
    const N = 21;
};
const AGG_META = [AG.N]struct { t: ColumnType, n: bool }{
    .{ .t = .int, .n = true },    .{ .t = .int, .n = true },    .{ .t = .string, .n = true }, .{ .t = .string, .n = true },
    .{ .t = .string, .n = true }, .{ .t = .string, .n = true }, .{ .t = .string, .n = true }, .{ .t = .string, .n = true },
    .{ .t = .string, .n = true }, .{ .t = .date, .n = true },   .{ .t = .date, .n = true },   .{ .t = .date, .n = true },
    .{ .t = .bigint, .n = true }, .{ .t = .bigint, .n = true }, .{ .t = .bigint, .n = true }, .{ .t = .bigint, .n = true },
    .{ .t = .string, .n = true }, .{ .t = .int, .n = true },    .{ .t = .double, .n = true }, .{ .t = .int, .n = true },
    .{ .t = .int, .n = true },
};

/// Per-shard state carried through the pipeline stages.
const Shard = struct {
    buf: ShardBuf,
    /// (customerNumberLC, divisionId) groups; row indices in estimates
    /// input order (invoiceId, date) with generated estimate rows appended.
    groups: std.ArrayListUnmanaged(Group) = .empty,
    est_rows: usize = 0,
    /// After the gather-reorder: contiguous [start,end) per group.
    ranges: std.ArrayListUnmanaged([2]u32) = .empty,
    /// rf_currency computed columns, row-aligned with buf.
    cur: [CU.N]ColumnStore = undefined,
    /// customer_agg_by_month output (month-sorted rows per group).
    agg: [AG.N]ColumnStore = undefined,
    agg_ranges: std.ArrayListUnmanaged([2]u32) = .empty,
    agg_rows: usize = 0,

    const Group = struct {
        rows: std.ArrayListUnmanaged(u32) = .empty,
    };
};

fn optI32LessNf(a: ?i32, b: ?i32) ?bool {
    if (a == null or b == null) {
        if (a == null and b == null) return null;
        return a == null; // NULLs first
    }
    if (a.? == b.?) return null;
    return a.? < b.?;
}

fn optStrLessNf(a: ?[]const u8, b: ?[]const u8) ?bool {
    if (a == null or b == null) {
        if (a == null and b == null) return null;
        return a == null;
    }
    return switch (std.mem.order(u8, a.?, b.?)) {
        .lt => true,
        .gt => false,
        .eq => null,
    };
}

const SortKey = struct {
    lc: []const u8,
    lc_null: bool,
    div: i32,
    div_null: bool,
    inv: []const u8,
    date: i32,
};

fn keyLess(keys: []const SortKey, x: u32, y: u32) bool {
    const a = keys[x];
    const b = keys[y];
    if (a.lc_null != b.lc_null) return a.lc_null; // NULLs first
    if (!a.lc_null) {
        const o = std.mem.order(u8, a.lc, b.lc);
        if (o != .eq) return o == .lt;
    }
    if (a.div_null != b.div_null) return a.div_null;
    if (!a.div_null and a.div != b.div) return a.div < b.div;
    const oi = std.mem.order(u8, a.inv, b.inv);
    if (oi != .eq) return oi == .lt;
    return a.date < b.date;
}

fn sameGroup(a: SortKey, b: SortKey) bool {
    if (a.lc_null != b.lc_null or a.div_null != b.div_null) return false;
    if (!a.lc_null and !std.mem.eql(u8, a.lc, b.lc)) return false;
    if (!a.div_null and a.div != b.div) return false;
    return true;
}

/// p3 cap accumulator: global MAX/SUM over the union rows, matching
/// _p3_flat.sql column-for-column.
const P3Cap = struct {
    rows: usize = 0,
    max_str: [13]?[]const u8 = @splat(null), // owned copies
    max_date: [4]?i32 = @splat(null), // date, invoiceDate, startDate + spare
    max_int: [2]?i32 = @splat(null), // projectId, divisionId
    sum_orig: i64 = 0,
    sum_icid: i64 = 0,
    sum_amount: i64 = 0,

    // max_str slots: 0 custLC, 1 custHash, 2 invoiceId, 3 origCustNum,
    // 4 origCustName, 5 origCurrency, 6 planId, 7 lineItemType,
    // 8 parentCustNum, 9 parentCustName, 10 custNum, 11 custName, 12 custEmail
    fn takeStr(self: *P3Cap, alloc: std.mem.Allocator, slot: usize, s: ?[]const u8) !void {
        const v = s orelse return;
        if (self.max_str[slot] == null or std.mem.order(u8, v, self.max_str[slot].?) == .gt) {
            const copy = try alloc.dupe(u8, v);
            if (self.max_str[slot]) |old| alloc.free(old);
            self.max_str[slot] = copy;
        }
    }
    fn takeDate(self: *P3Cap, slot: usize, d: ?i32) void {
        const v = d orelse return;
        if (self.max_date[slot] == null or v > self.max_date[slot].?) self.max_date[slot] = v;
    }
    fn takeInt(self: *P3Cap, slot: usize, x: ?i32) void {
        const v = x orelse return;
        if (self.max_int[slot] == null or v > self.max_int[slot].?) self.max_int[slot] = v;
    }
};

fn strMax(alloc: std.mem.Allocator, slot: *?[]const u8, s: ?[]const u8) !void {
    const v = s orelse return;
    if (slot.* == null or std.mem.order(u8, v, slot.*.?) == .gt) {
        const copy = try alloc.dupe(u8, v);
        if (slot.*) |old| alloc.free(old);
        slot.* = copy;
    }
}

fn i32MaxInto(slot: *?i32, v: ?i32) void {
    const x = v orelse return;
    if (slot.* == null or x > slot.*.?) slot.* = x;
}

/// _p6_flat.sql cap: global MAX/SUMs over customer_agg_by_month output.
/// Note a1/a2 are the PRE-LAST_VALUE customerName/Email (agg CTE output).
const P6Cap = struct {
    rows: usize = 0,
    max_proj: ?i32 = null,
    max_div: ?i32 = null,
    max_month: ?i32 = null,
    // 0 custLC, 1 custNum, 2 custName, 3 custEmail, 4 hash, 5 parentNum,
    // 6 parentName, 7 currency
    max_str: [8]?[]const u8 = @splat(null),
    max_bill: ?i32 = null,
    max_min: ?i32 = null,
    max_invd: ?i32 = null,
    sum_amount: i64 = 0,
    sum_orig: i64 = 0,
    sum_nonrec: i64 = 0,
    sum_orignonrec: i64 = 0,
    sum_icid: i64 = 0,
    sum_er: f64 = 0,
    sum_plan: i64 = 0,
    sum_hadj: i64 = 0,
};

/// _p15_flat.sql cap: global MAX/SUMs over cross_division_fields.
const P15Cap = struct {
    rows: usize = 0,
    max_proj: ?i32 = null,
    max_div: ?i32 = null,
    max_month: ?i32 = null,
    max_date: ?i32 = null,
    max_min: ?i32 = null,
    // 0 custLC, 1 custNum, 2 hash, 3 custName, 4 custEmail, 5 parentNum,
    // 6 parentName, 7 currency
    max_str: [8]?[]const u8 = @splat(null),
    sum_amount: i64 = 0,
    sum_orig: i64 = 0,
    sum_nonrec: i64 = 0,
    sum_orignonrec: i64 = 0,
    sum_er: f64 = 0,
    sum_hadj: i64 = 0,
    sum_icid: i64 = 0,
    sum_plan: i64 = 0,
    sum_rn: i64 = 0,
};

/// Tie-independent p5 probes: per-row SUM(planId) and SUM(rn1)/SUM(rn2)
/// over rollforward_pre_records_temp — proves the MAX_BY inputs match the
/// engine even where tie-arbitrary picks differ.
const P5Sums = struct {
    ipid: i64 = 0,
    rn1: i64 = 0,
    rn2: i64 = 0,
    hadj: i64 = 0,
};

fn satCastI32(v: i64) i32 {
    if (v > std.math.maxInt(i32)) return std.math.maxInt(i32);
    if (v < std.math.minInt(i32)) return std.math.minInt(i32);
    return @intCast(v);
}

const StageShared = struct {
    next: std.atomic.Value(usize) = .init(0),
    shards: []Shard,
    n_threads: usize,
    n_shards: usize,
    worker_shards: []ShardBuf,
    alloc: std.mem.Allocator,
    errs: []?anyerror,
    caps: []P3Cap,
    p5sums: []P5Sums,
    p6caps: []P6Cap,
    p15caps: []P15Cap,
    rate_part: *const tdb.TvfPartition,
    plan_part: *const tdb.TvfPartition,
    bc: *const Broadcasts,
};

fn stageWorker(sh: *StageShared, w: usize) void {
    stageWorkerInner(sh, w) catch |e| {
        sh.errs[w] = e;
    };
}

fn stageWorkerInner(sh: *StageShared, w: usize) !void {
    const alloc = sh.alloc;
    var scratch_arena = std.heap.ArenaAllocator.init(alloc);
    defer scratch_arena.deinit();
    // Worker-lifetime arena: rf_currency builds its RateTable/plan maps once
    // per thread here (borrows broadcast bytes, which outlive the phase).
    var state_arena = std.heap.ArenaAllocator.init(alloc);
    defer state_arena.deinit();
    var worker_state: ?*anyopaque = null;

    while (true) {
        const s = sh.next.fetchAdd(1, .monotonic);
        if (s >= sh.shards.len) break;
        const shard = &sh.shards[s];

        // ---- consolidate this shard from every scan worker's bucket ----
        var total: usize = 0;
        for (0..sh.n_threads) |sw| total += sh.worker_shards[sw * sh.n_shards + s].rows;
        if (total == 0) continue;
        shard.buf = try ShardBuf.init(alloc);
        for (0..sh.n_threads) |sw| {
            const src = &sh.worker_shards[sw * sh.n_shards + s];
            if (src.rows == 0) continue;
            for (&shard.buf.cols, &src.cols) |*dst, *from| {
                try appendViewAll(alloc, dst, from.view(), src.rows);
            }
        }
        shard.buf.rows = total;

        // ---- sort keys + argsort (custLC, div, invoiceId, date) ----
        _ = scratch_arena.reset(.retain_capacity);
        const sa = scratch_arena.allocator();
        const keys = try sa.alloc(SortKey, total);
        {
            const lc_v = shard.buf.cols[SH.customerNumberLC].view();
            const div_v = shard.buf.cols[SH.divisionId].view();
            const inv_v = shard.buf.cols[SH.invoiceId].view();
            const date_v = shard.buf.cols[SH.date].view();
            for (0..total) |i| {
                keys[i] = .{
                    .lc = strOrNull(lc_v, i) orelse "",
                    .lc_null = !lc_v.isValid(i),
                    .div = if (div_v.isValid(i)) div_v.data.int[i] else 0,
                    .div_null = !div_v.isValid(i),
                    .inv = strOrNull(inv_v, i) orelse "",
                    .date = date_v.data.date[i],
                };
            }
        }
        const order = try sa.alloc(u32, total);
        for (order, 0..) |*o, i| o.* = @intCast(i);
        std.mem.sortUnstable(u32, order, keys, struct {
            fn less(k: []const SortKey, x: u32, y: u32) bool {
                return keyLess(k, x, y);
            }
        }.less);

        // ---- walk groups; run rf_estimates on the date-windowed subset ----
        const win_lo = tdb.Date.fromYmd(.{ .y = 2026, .m = 5, .d = 1 }).days();
        const win_hi = tdb.Date.fromYmd(.{ .y = 2026, .m = 7, .d = 31 }).days();

        var est_arena = std.heap.ArenaAllocator.init(alloc);
        defer est_arena.deinit();

        var g_start: usize = 0;
        while (g_start < total) {
            var g_end = g_start + 1;
            while (g_end < total and sameGroup(keys[order[g_start]], keys[order[g_end]])) g_end += 1;

            var grp = Shard.Group{};
            try grp.rows.ensureTotalCapacity(alloc, g_end - g_start);
            var any_window = false;
            for (order[g_start..g_end]) |ri| {
                grp.rows.appendAssumeCapacity(ri);
                const d = keys[ri].date;
                if (d >= win_lo and d <= win_hi) any_window = true;
            }

            if (any_window) {
                _ = est_arena.reset(.retain_capacity);
                const ea = est_arena.allocator();
                // Gather the windowed rows (already in invoiceId,date order)
                // into a compact 21-col partition.
                var part_stores: [SH.N]ColumnStore = undefined;
                for (&part_stores, SHARD_META) |*c, m| c.* = try ColumnStore.init(ea, m.t, m.n);
                var views: [SH.N]ColumnView = undefined;
                for (&views, &shard.buf.cols) |*v, *c| v.* = c.view();
                var k: usize = 0;
                for (order[g_start..g_end]) |ri| {
                    const d = keys[ri].date;
                    if (d < win_lo or d > win_hi) continue;
                    for (&part_stores, views) |*dst, srcv| {
                        try appendFromView(ea, dst, srcv, ri);
                    }
                    k += 1;
                }
                if (k > 0) {
                    var pviews: [SH.N]ColumnView = undefined;
                    for (&pviews, &part_stores) |*v, *c| v.* = c.view();
                    const raw_part = tdb.TvfPartition{ .columns = &pviews, .row_count = k, .keys = &.{} };
                    const p = tdb.Partition(rf_estimates.Input){ .raw = &raw_part, .len = k };

                    var out_ptrs: [SH.N]*ColumnStore = undefined;
                    for (&out_ptrs, &shard.buf.cols) |*ptr, *c| ptr.* = c;
                    var raw_out = tdb.TvfOutput{ .columns = &out_ptrs, .allocator = alloc };
                    var writer = tdb.Writer(rf_estimates.Output){ .raw = &raw_out };

                    var ctx = tdb.Ctx{
                        .arena = ea,
                        .worker_arena = scratch_arena.allocator(),
                        .worker_state = &worker_state,
                    };
                    const before = shard.buf.cols[0].rowCount();
                    try rf_estimates.process(&ctx, .{ .curMonth = CUR_MONTH, .currentDate = CURRENT_DATE }, p, &writer);
                    const after = shard.buf.cols[0].rowCount();
                    for (before..after) |nri| try grp.rows.append(alloc, @intCast(nri));
                    shard.est_rows += after - before;
                }
            }
            try shard.groups.append(alloc, grp);
            g_start = g_end;
        }
        shard.buf.rows = shard.buf.cols[0].rowCount();

        // ---- p3 cap partial over the union rows ----
        const cap = &sh.caps[w];
        var views: [SH.N]ColumnView = undefined;
        for (&views, &shard.buf.cols) |*v, *c| v.* = c.view();
        cap.rows += shard.buf.rows;
        for (0..shard.buf.rows) |i| {
            cap.takeInt(0, views[SH.projectId].data.int[i]);
            cap.takeInt(1, if (views[SH.divisionId].isValid(i)) views[SH.divisionId].data.int[i] else null);
            try cap.takeStr(alloc, 0, strOrNull(views[SH.customerNumberLC], i));
            cap.takeDate(0, views[SH.date].data.date[i]);
            try cap.takeStr(alloc, 1, strOrNull(views[SH.customerNumberHash], i));
            if (views[SH.originalAmount].isValid(i)) cap.sum_orig += views[SH.originalAmount].data.int[i];
            try cap.takeStr(alloc, 2, strOrNull(views[SH.invoiceId], i));
            cap.takeDate(1, if (views[SH.invoiceDate].isValid(i)) views[SH.invoiceDate].data.date[i] else null);
            cap.takeDate(2, if (views[SH.startDate].isValid(i)) views[SH.startDate].data.date[i] else null);
            try cap.takeStr(alloc, 3, strOrNull(views[SH.originalCustomerNumber], i));
            try cap.takeStr(alloc, 4, strOrNull(views[SH.originalCustomerName], i));
            try cap.takeStr(alloc, 5, strOrNull(views[SH.originalCurrency], i));
            try cap.takeStr(alloc, 6, strOrNull(views[SH.planId], i));
            try cap.takeStr(alloc, 7, strOrNull(views[SH.lineItemType], i));
            cap.sum_icid += views[SH.integrationConfigId].data.int[i];
            try cap.takeStr(alloc, 8, strOrNull(views[SH.parentCustomerNumber], i));
            try cap.takeStr(alloc, 9, strOrNull(views[SH.parentCustomerName], i));
            try cap.takeStr(alloc, 10, strOrNull(views[SH.customerNumber], i));
            try cap.takeStr(alloc, 11, strOrNull(views[SH.customerName], i));
            try cap.takeStr(alloc, 12, strOrNull(views[SH.customerEmail], i));
            if (views[SH.amount].isValid(i)) cap.sum_amount += views[SH.amount].data.int[i];
        }

        // ---- gather-reorder: apply group order physically -----------------
        {
            var obuf = try ShardBuf.init(alloc);
            var oviews: [SH.N]ColumnView = undefined;
            for (&oviews, &shard.buf.cols) |*v, *c| v.* = c.view();
            for (shard.groups.items) |*grp| {
                const start: u32 = @intCast(obuf.cols[0].rowCount());
                for (grp.rows.items) |ri| {
                    for (&obuf.cols, oviews) |*dst, srcv| try appendFromView(alloc, dst, srcv, ri);
                }
                try shard.ranges.append(alloc, .{ start, @intCast(obuf.cols[0].rowCount()) });
            }
            obuf.rows = obuf.cols[0].rowCount();
            shard.buf.deinit(alloc);
            shard.buf = obuf;
            for (shard.groups.items) |*g| g.rows.deinit(alloc);
            shard.groups.deinit(alloc);
            shard.groups = .empty;
        }

        // ---- rf_currency_convert: one row-aligned call over the shard -----
        const nrows = shard.buf.rows;
        for (&shard.cur, CUR_META) |*c, m| c.* = try ColumnStore.init(alloc, m.t, m.n);
        if (nrows > 0) {
            const cin = [_]usize{
                SH.projectId,    SH.divisionId,   SH.customerNumberLC, SH.invoiceDate,
                SH.originalCurrency, SH.amount,   SH.originalAmount,   SH.lineItemType,
                SH.planId,       SH.integrationConfigId, SH.date,      SH.startDate,
            };
            var cviews: [12]ColumnView = undefined;
            for (&cviews, cin) |*v, si| v.* = shard.buf.cols[si].view();
            const craw = tdb.TvfPartition{ .columns = &cviews, .row_count = nrows, .keys = &.{} };
            const cp = tdb.Partition(rf_currency.Input){ .raw = &craw, .len = nrows };
            const rp = tdb.Partition(rf_currency.Input2){ .raw = sh.rate_part, .len = sh.rate_part.row_count };
            const pp = tdb.Partition(rf_currency.Input3){ .raw = sh.plan_part, .len = sh.plan_part.row_count };
            var cur_ptrs: [CU.N]*ColumnStore = undefined;
            for (&cur_ptrs, &shard.cur) |*p, *c| p.* = c;
            var craw_out = tdb.TvfOutput{ .columns = &cur_ptrs, .allocator = alloc };
            var cwriter = tdb.Writer(rf_currency.Computed){ .raw = &craw_out };
            _ = est_arena.reset(.retain_capacity);
            var cctx = tdb.Ctx{
                .arena = est_arena.allocator(),
                .worker_arena = state_arena.allocator(),
                .worker_state = &worker_state,
            };
            try rf_currency.process(&cctx, .{ .targetCurrency = "USD", .useDivision = 0 }, cp, rp, pp, &cwriter);
        }

        // ---- rollforward_pre_records_temp + customer_agg_by_month ---------
        for (&shard.agg, AGG_META) |*c, m| c.* = try ColumnStore.init(alloc, m.t, m.n);
        var bviews: [SH.N]ColumnView = undefined;
        for (&bviews, &shard.buf.cols) |*v, *c| v.* = c.view();
        var cuviews: [CU.N]ColumnView = undefined;
        for (&cuviews, &shard.cur) |*v, *c| v.* = c.view();

        for (shard.ranges.items) |rng| {
            const s0: usize = rng[0];
            const s1: usize = rng[1];
            const n = s1 - s0;
            if (n == 0) continue;
            _ = est_arena.reset(.retain_capacity);
            const ra = est_arena.allocator();

            // Per-row pre_records computes.
            const bill = try ra.alloc(?i32, n); // billDate days
            const er = try ra.alloc(?f64, n);
            const hadj = try ra.alloc(i32, n);
            for (0..n) |li| {
                const i = s0 + li;
                bill[li] = blk: {
                    if (!bviews[SH.startDate].isValid(i) or !cuviews[CU.mdiff].isValid(i)) break :blk null;
                    const sd = tdb.Date.fromDays(bviews[SH.startDate].data.date[i]);
                    break :blk sd.addMonths(cuviews[CU.mdiff].data.int[i]).days();
                };
                const amt: ?i32 = if (cuviews[CU.amount].isValid(i)) cuviews[CU.amount].data.int[i] else null;
                const org: ?i32 = if (cuviews[CU.orig].isValid(i)) cuviews[CU.orig].data.int[i] else null;
                er[li] = blk: {
                    if (org != null and org.? == 0) break :blk 0;
                    if (org == null or amt == null) break :blk null;
                    const of32: f32 = @floatFromInt(org.?);
                    break :blk @as(f64, @floatFromInt(amt.?)) / @as(f64, of32);
                };
                hadj[li] = blk: {
                    const lt = strOrNull(bviews[SH.lineItemType], i) orelse break :blk 0;
                    break :blk if (std.mem.eql(u8, lt, "adjustment-recurring") or std.mem.eql(u8, lt, "adjustment-nonrecurring")) @as(i32, 1) else 0;
                };
            }

            // Two ROW_NUMBERs over the group (rank = position in argsort).
            const rn1 = try ra.alloc(u32, n);
            const rn2 = try ra.alloc(u32, n);
            {
                const ord = try ra.alloc(u32, n);
                const Ctx1 = struct {
                    bv: []const ColumnView,
                    cv: []const ColumnView,
                    s0: usize,
                    fn invDate(c: @This(), li: u32) ?i32 {
                        const i = c.s0 + li;
                        return if (c.bv[SH.invoiceDate].isValid(i)) c.bv[SH.invoiceDate].data.date[i] else null;
                    }
                    fn amountOf(c: @This(), li: u32) ?i32 {
                        const i = c.s0 + li;
                        return if (c.cv[CU.amount].isValid(i)) c.cv[CU.amount].data.int[i] else null;
                    }
                    fn ipidOf(c: @This(), li: u32) ?i32 {
                        const i = c.s0 + li;
                        return if (c.cv[CU.ipid].isValid(i)) c.cv[CU.ipid].data.int[i] else null;
                    }
                    fn invId(c: @This(), li: u32) ?[]const u8 {
                        return strOrNull(c.bv[SH.invoiceId], c.s0 + li);
                    }
                    fn less1(c: @This(), x: u32, y: u32) bool {
                        if (optI32LessNf(c.invDate(x), c.invDate(y))) |r| return r;
                        if (optStrLessNf(c.invId(x), c.invId(y))) |r| return r;
                        if (optI32LessNf(c.amountOf(x), c.amountOf(y))) |r| return r;
                        return x < y; // stable tiebreak on input order
                    }
                    fn less2(c: @This(), x: u32, y: u32) bool {
                        if (optI32LessNf(c.invDate(x), c.invDate(y))) |r| return r;
                        if (optI32LessNf(c.amountOf(x), c.amountOf(y))) |r| return r;
                        if (optI32LessNf(c.ipidOf(x), c.ipidOf(y))) |r| return r;
                        return x < y;
                    }
                };
                const c1 = Ctx1{ .bv = &bviews, .cv = &cuviews, .s0 = s0 };
                for (ord, 0..) |*o, k| o.* = @intCast(k);
                std.mem.sortUnstable(u32, ord, c1, Ctx1.less1);
                for (ord, 1..) |li, rk| rn1[li] = @intCast(rk);
                for (ord, 0..) |*o, k| o.* = @intCast(k);
                std.mem.sortUnstable(u32, ord, c1, Ctx1.less2);
                for (ord, 1..) |li, rk| rn2[li] = @intCast(rk);
            }
            {
                const p5 = &sh.p5sums[w];
                for (0..n) |li| {
                    const i = s0 + li;
                    if (cuviews[CU.ipid].isValid(i)) p5.ipid += cuviews[CU.ipid].data.int[i];
                    p5.rn1 += rn1[li];
                    p5.rn2 += rn2[li];
                    p5.hadj += hadj[li];
                }
            }

            // Month buckets.
            const Acc = struct {
                month: ?i32,
                rn1_best: u32 = 0,
                cust_num: ?[]const u8 = null,
                cust_name: ?[]const u8 = null,
                cust_email: ?[]const u8 = null,
                parent_num: ?[]const u8 = null,
                parent_name: ?[]const u8 = null,
                icid: ?i32 = null,
                any_hash: ?[]const u8 = null,
                any_currency: ?[]const u8 = null,
                first_set: bool = false,
                max_invd: ?i32 = null,
                max_bill: ?i32 = null,
                min_bill: ?i32 = null,
                sum_amount: ?i64 = null,
                sum_orig: ?i64 = null,
                sum_nonrec: ?i64 = null,
                sum_orignonrec: ?i64 = null,
                sum_absamt: ?i64 = null,
                sum_er_w: f64 = 0,
                plan_key: i64 = -1,
                plan_val: ?i32 = null,
                has_adj: i32 = 0,
            };
            var accs = std.ArrayListUnmanaged(Acc).empty;
            var acc_of = std.AutoHashMapUnmanaged(i64, usize).empty;
            const NULL_MONTH: i64 = std.math.minInt(i64);

            for (0..n) |li| {
                const i = s0 + li;
                const month: ?i32 = if (bill[li]) |bd| blk: {
                    const v = tdb.Date.fromDays(bd).ymd();
                    break :blk tdb.Date.fromYmd(.{ .y = v.y, .m = v.m, .d = 1 }).days();
                } else null;
                const mkey: i64 = if (month) |m| m else NULL_MONTH;
                const gop = try acc_of.getOrPut(ra, mkey);
                if (!gop.found_existing) {
                    gop.value_ptr.* = accs.items.len;
                    try accs.append(ra, .{ .month = month });
                }
                const acc = &accs.items[gop.value_ptr.*];

                if (rn1[li] > acc.rn1_best) {
                    acc.rn1_best = rn1[li];
                    acc.cust_num = strOrNull(bviews[SH.customerNumber], i);
                    acc.cust_name = strOrNull(bviews[SH.customerName], i);
                    acc.cust_email = strOrNull(bviews[SH.customerEmail], i);
                    acc.parent_num = strOrNull(bviews[SH.parentCustomerNumber], i);
                    acc.parent_name = strOrNull(bviews[SH.parentCustomerName], i);
                    acc.icid = if (bviews[SH.integrationConfigId].isValid(i)) bviews[SH.integrationConfigId].data.int[i] else null;
                }
                if (!acc.first_set) {
                    acc.first_set = true;
                    acc.any_hash = strOrNull(bviews[SH.customerNumberHash], i);
                    acc.any_currency = strOrNull(cuviews[CU.norm], i);
                }
                if (bill[li]) |bd| {
                    if (acc.max_bill == null or bd > acc.max_bill.?) acc.max_bill = bd;
                    if (acc.min_bill == null or bd < acc.min_bill.?) acc.min_bill = bd;
                }
                if (bviews[SH.invoiceDate].isValid(i)) {
                    const vd = bviews[SH.invoiceDate].data.date[i];
                    if (acc.max_invd == null or vd > acc.max_invd.?) acc.max_invd = vd;
                }
                const amt: ?i32 = if (cuviews[CU.amount].isValid(i)) cuviews[CU.amount].data.int[i] else null;
                if (amt) |v| {
                    acc.sum_amount = (acc.sum_amount orelse 0) + v;
                    acc.sum_absamt = (acc.sum_absamt orelse 0) + @as(i64, @intCast(@abs(v)));
                    if (er[li]) |e| acc.sum_er_w += e * @as(f64, @floatFromInt(@abs(v)));
                }
                if (cuviews[CU.orig].isValid(i)) acc.sum_orig = (acc.sum_orig orelse 0) + cuviews[CU.orig].data.int[i];
                if (cuviews[CU.nonrec].isValid(i)) acc.sum_nonrec = (acc.sum_nonrec orelse 0) + cuviews[CU.nonrec].data.int[i];
                if (cuviews[CU.orignonrec].isValid(i)) acc.sum_orignonrec = (acc.sum_orignonrec orelse 0) + cuviews[CU.orignonrec].data.int[i];
                const pkey: i64 = if (amt != null and amt.? > 0) @as(i64, rn2[li]) + 1_000_000_000 else @as(i64, rn2[li]);
                if (pkey > acc.plan_key) {
                    acc.plan_key = pkey;
                    acc.plan_val = if (cuviews[CU.ipid].isValid(i)) cuviews[CU.ipid].data.int[i] else null;
                }
                if (hadj[li] > acc.has_adj) acc.has_adj = hadj[li];
            }
            if (accs.items.len == 0) continue;

            // Month order (NULLs first) + LAST_VALUE(customerName/Email).
            const aord = try ra.alloc(u32, accs.items.len);
            for (aord, 0..) |*o, k| o.* = @intCast(k);
            std.mem.sortUnstable(u32, aord, accs.items, struct {
                fn less(as: []const Acc, x: u32, y: u32) bool {
                    const r = optI32LessNf(as[x].month, as[y].month) orelse return false;
                    return r;
                }
            }.less);
            const last = &accs.items[aord[accs.items.len - 1]];
            const lv_name = last.cust_name;
            const lv_email = last.cust_email;

            // Group-constant columns.
            const g_pid: ?i32 = if (bviews[SH.projectId].isValid(s0)) bviews[SH.projectId].data.int[s0] else null;
            const g_div: ?i32 = if (bviews[SH.divisionId].isValid(s0)) bviews[SH.divisionId].data.int[s0] else null;
            const g_lc = strOrNull(bviews[SH.customerNumberLC], s0);

            const astart: u32 = @intCast(shard.agg[0].rowCount());
            for (aord) |ai| {
                const acc = &accs.items[ai];
                const cols = &shard.agg;
                try appendOptInt(alloc, &cols[AG.projectId], g_pid);
                try appendOptInt(alloc, &cols[AG.divisionId], g_div);
                try appendStr(alloc, &cols[AG.customerNumber], acc.cust_num);
                try appendStr(alloc, &cols[AG.customerNumberLC], g_lc);
                try appendStr(alloc, &cols[AG.customerName], lv_name);
                try appendStr(alloc, &cols[AG.customerEmail], lv_email);
                try appendStr(alloc, &cols[AG.customerNumberHash], acc.any_hash);
                try appendStr(alloc, &cols[AG.parentCustomerNumber], acc.parent_num);
                try appendStr(alloc, &cols[AG.parentCustomerName], acc.parent_name);
                try appendOptDate(alloc, &cols[AG.date], acc.max_bill);
                try appendOptDate(alloc, &cols[AG.minDate], acc.min_bill);
                try appendOptDate(alloc, &cols[AG.month], acc.month);
                try appendOptI64(alloc, &cols[AG.amount], acc.sum_amount);
                try appendOptI64(alloc, &cols[AG.originalAmount], acc.sum_orig);
                try appendOptI64(alloc, &cols[AG.nonRecurringAmount], acc.sum_nonrec);
                try appendOptI64(alloc, &cols[AG.originalNonRecurringAmount], acc.sum_orignonrec);
                try appendStr(alloc, &cols[AG.currency], acc.any_currency);
                try appendOptInt(alloc, &cols[AG.integrationConfigId], acc.icid);
                const er_out: ?f64 = blk: {
                    const sabs = acc.sum_absamt orelse break :blk null;
                    if (sabs == 0) break :blk 0;
                    break :blk acc.sum_er_w / @as(f64, @floatFromInt(sabs));
                };
                try appendOptF64(alloc, &cols[AG.exchangeRate], er_out);
                try appendOptInt(alloc, &cols[AG.planId], acc.plan_val);
                try appendOptInt(alloc, &cols[AG.hasAdjustment], acc.has_adj);

                const p6c = &sh.p6caps[w];
                p6c.rows += 1;
                i32MaxInto(&p6c.max_proj, g_pid);
                i32MaxInto(&p6c.max_div, g_div);
                i32MaxInto(&p6c.max_month, acc.month);
                try strMax(alloc, &p6c.max_str[0], g_lc);
                try strMax(alloc, &p6c.max_str[1], acc.cust_num);
                try strMax(alloc, &p6c.max_str[2], acc.cust_name);
                try strMax(alloc, &p6c.max_str[3], acc.cust_email);
                try strMax(alloc, &p6c.max_str[4], acc.any_hash);
                try strMax(alloc, &p6c.max_str[5], acc.parent_num);
                try strMax(alloc, &p6c.max_str[6], acc.parent_name);
                try strMax(alloc, &p6c.max_str[7], acc.any_currency);
                i32MaxInto(&p6c.max_bill, acc.max_bill);
                i32MaxInto(&p6c.max_min, acc.min_bill);
                i32MaxInto(&p6c.max_invd, acc.max_invd);
                if (acc.sum_amount) |v| p6c.sum_amount += v;
                if (acc.sum_orig) |v| p6c.sum_orig += v;
                if (acc.sum_nonrec) |v| p6c.sum_nonrec += v;
                if (acc.sum_orignonrec) |v| p6c.sum_orignonrec += v;
                if (acc.icid) |v| p6c.sum_icid += v;
                if (er_out) |e| p6c.sum_er += e;
                if (acc.plan_val) |v| p6c.sum_plan += v;
                p6c.sum_hadj += acc.has_adj;
            }
            try shard.agg_ranges.append(alloc, .{ astart, @intCast(shard.agg[0].rowCount()) });
        }
        shard.agg_rows = shard.agg[0].rowCount();

        // ---- D: rf_gap_fill -> rf_updown_chain -> tail -> p15 cap ---------
        var aviews: [AG.N]ColumnView = undefined;
        for (&aviews, &shard.agg) |*v, *c| v.* = c.view();
        const p15c = &sh.p15caps[w];
        var lc_count: i64 = 0;
        var prev_lc: ?[]const u8 = null;
        var prev_lc_set = false;

        const UD_META = [10]struct { t: ColumnType, n: bool }{
            .{ .t = .bigint, .n = true }, .{ .t = .bigint, .n = true }, .{ .t = .int, .n = true },  .{ .t = .double, .n = true },
            .{ .t = .int, .n = true },    .{ .t = .int, .n = true },    .{ .t = .date, .n = true }, .{ .t = .string, .n = false },
            .{ .t = .int, .n = false },   .{ .t = .bigint, .n = false },
        };

        for (shard.agg_ranges.items) |arng| {
            const a0: usize = arng[0];
            const a1: usize = arng[1];
            if (a1 == a0) continue;
            _ = est_arena.reset(.retain_capacity);
            const da = est_arena.allocator();

            // custLC run bookkeeping for SUM(groupOrderNumber): the final
            // ROW_NUMBER partitions by customerNumberLC only (not division),
            // and same-LC ranges are consecutive in shard order.
            const lc_shardlife = strOrNull(aviews[AG.customerNumberLC], a0);
            const same_run = prev_lc_set and ((prev_lc == null and lc_shardlife == null) or
                (prev_lc != null and lc_shardlife != null and std.mem.eql(u8, prev_lc.?, lc_shardlife.?)));
            if (!same_run) {
                p15c.sum_rn += @divExact(lc_count * (lc_count + 1), 2);
                lc_count = 0;
                prev_lc = lc_shardlife;
                prev_lc_set = true;
            }

            // Gather the group (AG layout) into a compact partition.
            var ga: [AG.N]ColumnStore = undefined;
            for (&ga, AGG_META) |*c, m| c.* = try ColumnStore.init(da, m.t, m.n);
            for (a0..a1) |ri| {
                for (&ga, aviews) |*dst, srcv| try appendFromView(da, dst, srcv, ri);
            }
            var gviews: [AG.N]ColumnView = undefined;
            for (&gviews, &ga) |*v, *c| v.* = c.view();
            const graw = tdb.TvfPartition{ .columns = &gviews, .row_count = a1 - a0, .keys = &.{} };
            const gp = tdb.Partition(rf_gap_fill.Input){ .raw = &graw, .len = a1 - a0 };

            var gb: [AG.N]ColumnStore = undefined;
            for (&gb, AGG_META) |*c, m| c.* = try ColumnStore.init(da, m.t, m.n);
            var gb_ptrs: [AG.N]*ColumnStore = undefined;
            for (&gb_ptrs, &gb) |*p, *c| p.* = c;
            var graw_out = tdb.TvfOutput{ .columns = &gb_ptrs, .allocator = da };
            var gwriter = tdb.Writer(rf_gap_fill.Output){ .raw = &graw_out };
            var gctx = tdb.Ctx{ .arena = da, .worker_arena = state_arena.allocator(), .worker_state = &worker_state };
            try rf_gap_fill.process(&gctx, .{ .comparisonMonths = 1 }, gp, &gwriter);
            const gn = gb[0].rowCount();
            if (gn == 0) continue;
            var gbv: [AG.N]ColumnView = undefined;
            for (&gbv, &gb) |*v, *c| v.* = c.view();

            // rf_updown_chain over the gap-filled group (computed cols are
            // not observed by the p15 cap — ctc/ctl are empty — but the
            // full query computes them, so we pay the kernel here).
            {
                const uin = [_]usize{
                    AG.projectId, AG.divisionId, AG.customerNumberLC, AG.month, AG.minDate,
                    AG.amount,    AG.originalAmount, AG.exchangeRate, AG.planId,
                };
                var uviews: [9]ColumnView = undefined;
                for (&uviews, uin) |*v, si| v.* = gbv[si];
                const uraw = tdb.TvfPartition{ .columns = &uviews, .row_count = gn, .keys = &.{} };
                const up = tdb.Partition(rf_updown.Input){ .raw = &uraw, .len = gn };
                var uc: [10]ColumnStore = undefined;
                for (&uc, UD_META) |*c, m| c.* = try ColumnStore.init(da, m.t, m.n);
                var uc_ptrs: [10]*ColumnStore = undefined;
                for (&uc_ptrs, &uc) |*p, *c| p.* = c;
                var uraw_out = tdb.TvfOutput{ .columns = &uc_ptrs, .allocator = da };
                var uwriter = tdb.Writer(rf_updown.Computed){ .raw = &uraw_out };
                var uctx = tdb.Ctx{ .arena = da, .worker_arena = state_arena.allocator(), .worker_state = &worker_state };
                try rf_updown.process(&uctx, .{ .comparisonMonths = 1 }, up, &uwriter);
            }

            // Tail: division INNER join (drop) + plan LEFT join + LAG(1) +
            // final projection + cross_division cap accumulation.
            var last_ext: ?[]const u8 = null;
            var last_pname: ?[]const u8 = null;
            var lag_touch: usize = 0;
            for (0..gn) |i| {
                const div: ?i32 = if (gbv[AG.divisionId].isValid(i)) gbv[AG.divisionId].data.int[i] else null;
                const d = div orelse continue; // INNER JOIN: NULL never matches
                if (sh.bc.division_names.get(d) == null) continue;

                const pid_plan: ?i32 = if (gbv[AG.planId].isValid(i)) gbv[AG.planId].data.int[i] else null;
                var ext: ?[]const u8 = null;
                var pname: ?[]const u8 = null;
                if (pid_plan) |p| {
                    if (sh.bc.plan_by_id.get(p)) |row| {
                        ext = sh.bc.plan_external_id.items[row];
                        pname = sh.bc.plan_name.items[row];
                    }
                }
                // LAG(externalPlanId/planName, 1) over post-join rows; the
                // values feed only the empty-ctc CASE branches downstream —
                // consumed here so the work isn't optimized away.
                lag_touch +%= (if (last_ext) |e| e.len else 1) +% (if (last_pname) |p2| p2.len else 1);
                last_ext = ext;
                last_pname = pname;

                const date_v: ?i32 = if (gbv[AG.date].isValid(i)) gbv[AG.date].data.date[i] else null;
                const month_v: ?i32 = if (date_v) |dv| blk: {
                    const y = tdb.Date.fromDays(dv).ymd();
                    break :blk tdb.Date.fromYmd(.{ .y = y.y, .m = y.m, .d = 1 }).days();
                } else null;
                const min_v: ?i32 = if (gbv[AG.minDate].isValid(i)) gbv[AG.minDate].data.date[i] else null;

                p15c.rows += 1;
                lc_count += 1;
                i32MaxInto(&p15c.max_proj, if (gbv[AG.projectId].isValid(i)) gbv[AG.projectId].data.int[i] else null);
                p15c.max_div = -2;
                i32MaxInto(&p15c.max_month, month_v);
                i32MaxInto(&p15c.max_date, date_v);
                i32MaxInto(&p15c.max_min, min_v);
                try strMax(alloc, &p15c.max_str[0], strOrNull(gbv[AG.customerNumberLC], i));
                try strMax(alloc, &p15c.max_str[1], strOrNull(gbv[AG.customerNumber], i));
                try strMax(alloc, &p15c.max_str[2], strOrNull(gbv[AG.customerNumberHash], i));
                try strMax(alloc, &p15c.max_str[3], strOrNull(gbv[AG.customerName], i));
                try strMax(alloc, &p15c.max_str[4], strOrNull(gbv[AG.customerEmail], i));
                try strMax(alloc, &p15c.max_str[5], strOrNull(gbv[AG.parentCustomerNumber], i));
                try strMax(alloc, &p15c.max_str[6], strOrNull(gbv[AG.parentCustomerName], i));
                try strMax(alloc, &p15c.max_str[7], strOrNull(gbv[AG.currency], i));
                if (gbv[AG.amount].isValid(i)) p15c.sum_amount += satCastI32(gbv[AG.amount].data.bigint[i]);
                if (gbv[AG.originalAmount].isValid(i)) p15c.sum_orig += satCastI32(gbv[AG.originalAmount].data.bigint[i]);
                if (gbv[AG.nonRecurringAmount].isValid(i)) p15c.sum_nonrec += satCastI32(gbv[AG.nonRecurringAmount].data.bigint[i]);
                if (gbv[AG.originalNonRecurringAmount].isValid(i)) p15c.sum_orignonrec += satCastI32(gbv[AG.originalNonRecurringAmount].data.bigint[i]);
                if (gbv[AG.exchangeRate].isValid(i)) {
                    // final_rollforward: CAST(r.exchangeRate AS FLOAT).
                    const f: f32 = @floatCast(gbv[AG.exchangeRate].data.double[i]);
                    p15c.sum_er += @as(f64, f);
                }
                if (gbv[AG.hasAdjustment].isValid(i)) p15c.sum_hadj += gbv[AG.hasAdjustment].data.int[i];
                if (gbv[AG.integrationConfigId].isValid(i)) p15c.sum_icid += gbv[AG.integrationConfigId].data.int[i];
                if (pid_plan) |p| p15c.sum_plan += p;
            }
            std.mem.doNotOptimizeAway(lag_touch);
        }
        p15c.sum_rn += @divExact(lc_count * (lc_count + 1), 2);
    }
}

fn appendOptInt(alloc: std.mem.Allocator, store: *ColumnStore, v: ?i32) !void {
    if (v == null) return store.appendNulls(alloc, 1);
    try store.data.int.append(alloc, v.?);
    try store.appendValidBit(alloc, store.rowCount() - 1, true);
}

fn appendOptDate(alloc: std.mem.Allocator, store: *ColumnStore, v: ?i32) !void {
    if (v == null) return store.appendNulls(alloc, 1);
    try store.data.date.append(alloc, v.?);
    try store.appendValidBit(alloc, store.rowCount() - 1, true);
}

fn appendOptI64(alloc: std.mem.Allocator, store: *ColumnStore, v: ?i64) !void {
    if (v == null) return store.appendNulls(alloc, 1);
    try store.data.bigint.append(alloc, v.?);
    try store.appendValidBit(alloc, store.rowCount() - 1, true);
}

fn appendOptF64(alloc: std.mem.Allocator, store: *ColumnStore, v: ?f64) !void {
    if (v == null) return store.appendNulls(alloc, 1);
    try store.data.double.append(alloc, v.?);
    try store.appendValidBit(alloc, store.rowCount() - 1, true);
}

fn dateStr(buf: []u8, days: ?i32) []const u8 {
    const d = days orelse return "null";
    const v = tdb.Date.fromDays(d).ymd();
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{ @as(u32, @intCast(v.y)), v.m, v.d }) catch "?";
}

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

pub fn main() !void {
    const allocator = std.heap.smp_allocator;
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path: []const u8 = if (getenv("RF_DB")) |v| std.mem.span(v) else ".wayroll-bench-db";
    const db_name: []const u8 = if (getenv("RF_DBNAME")) |v| std.mem.span(v) else "wayroll_prod";
    const n_threads: usize = if (getenv("RF_THREADS")) |v| try std.fmt.parseInt(usize, std.mem.span(v), 10) else 12;
    const n_shards: usize = if (getenv("RF_SHARDS")) |v| try std.fmt.parseInt(usize, std.mem.span(v), 10) else 64;
    db_name_for_session = db_name;

    const prof = thindb.exec.prof;
    const t_all = prof.nowTicks();

    var data_root = try std.Io.Dir.cwd().openDir(io, db_path, .{ .iterate = true });
    defer data_root.close(io);
    const catalog = try thindb.Catalog.open(allocator, io, data_root, .{ .max_dop = 12 });
    defer catalog.close();
    const db = catalog.database(db_name) orelse return error.DatabaseNotFound;
    const open_ms = prof.ticksToMs(prof.nowTicks() - t_all);

    // ---- broadcasts ------------------------------------------------------
    var t = prof.nowTicks();
    var bc = Broadcasts{ .arena = std.heap.ArenaAllocator.init(allocator) };
    defer bc.arena.deinit();
    try loadBroadcasts(allocator, db, &bc);
    try bc.buildKernelInputs();
    const bc_ms = prof.ticksToMs(prof.nowTicks() - t);

    // ---- phase 1: scan + scatter ----------------------------------------
    t = prof.nowTicks();
    const table = try db.openTable("invoice_import_amortized", .{});

    table.ddl_lock.lockSharedUncancelable(table.io);
    defer table.ddl_lock.unlockShared(table.io);
    const Scan = thindb.exec.Scan;
    const snap = try Scan.captureSnapshotAlloc(table, allocator);
    defer allocator.free(snap.segments);
    var orch_pin_held = true;
    defer if (orch_pin_held) snap.memtable_snap.release();

    var total_rgs: usize = 0;
    const seg_start = try allocator.alloc(usize, snap.segment_count + 1);
    defer allocator.free(seg_start);
    for (snap.segments, 0..) |e, i| {
        seg_start[i] = total_rgs;
        total_rgs += e.row_group_count;
    }
    seg_start[snap.segment_count] = total_rgs;

    const n_chunks = @max(n_threads, @min(n_threads * 4, @max(total_rgs, 1)));

    const P = thindb.exec.PredicateExpr;
    var in_arms: [LINE_ITEM_TYPES.len]P = undefined;
    for (LINE_ITEM_TYPES, 0..) |lt, i| {
        in_arms[i] = .{ .leaf = .{ .col = "lineItemType", .op = .eq, .val = .{ .text = lt } } };
    }
    const date_ceil = tdb.Date.fromYmd(.{ .y = DATE_CEIL_Y, .m = DATE_CEIL_M, .d = DATE_CEIL_D }).days();
    const conj = [_]P{
        .{ .leaf = .{ .col = "projectId", .op = .eq, .val = .{ .int = PROJECT_ID } } },
        .{ .leaf = .{ .col = "modelType", .op = .eq, .val = .{ .text = MODEL_TYPE } } },
        .{ .leaf = .{ .col = "deleted", .op = .eq, .val = .{ .smallint = 0 } } },
        .{ .leaf = .{ .col = "date", .op = .lte, .val = .{ .date = date_ceil } } },
        .{ .@"or" = &in_arms },
    };
    const filter_expr = P{ .@"and" = &conj };

    const scans = try allocator.alloc(*Scan, n_chunks);
    defer allocator.free(scans);
    var scans_built: usize = 0;
    defer for (scans[0..scans_built]) |s| s.deinit();
    for (0..n_chunks) |i| {
        const lo = i * total_rgs / n_chunks;
        const hi = if (i == n_chunks - 1) total_rgs else (i + 1) * total_rgs / n_chunks;
        const s = try Scan.allocWithProjectionLoc(allocator, table, null, &SCAN_COLS, false, snap);
        scans[i] = s;
        scans_built += 1;
        const start = flatToCoord(lo, seg_start, snap.segment_count);
        const end = flatToCoord(hi, seg_start, snap.segment_count);
        s.setRange(start.seg, start.rg, end.seg, end.rg, i == n_chunks - 1);
        try s.addPrune(.{ .col = "projectId", .op = .eq, .val = .{ .int = PROJECT_ID } });
        try s.addPrune(.{ .col = "date", .op = .lte, .val = .{ .date = date_ceil } });
        const fused = try s.tryFuseFilter(filter_expr);
        if (!fused) return error.FilterNotFused;
    }
    snap.memtable_snap.release();
    orch_pin_held = false;

    var scan_to_shard: [SCAN_COLS.len]usize = undefined;
    var cust_ci: usize = 0;
    {
        const os = scans[0].outputSchema();
        if (os.len != SCAN_COLS.len) return error.UnexpectedScanSchema;
        for (os, 0..) |col, ci| {
            var found = false;
            for (SHARD_NAMES, 0..) |sn, si| {
                if (std.mem.eql(u8, col.name, sn)) {
                    scan_to_shard[ci] = si;
                    found = true;
                    break;
                }
            }
            if (!found) return error.UnknownScanColumn;
            if (std.mem.eql(u8, col.name, "customerNumber")) cust_ci = ci;
        }
    }
    const build_ms = prof.ticksToMs(prof.nowTicks() - t);

    t = prof.nowTicks();
    const worker_shards = try allocator.alloc(ShardBuf, n_threads * n_shards);
    defer {
        for (worker_shards) |*sb| sb.deinit(allocator);
        allocator.free(worker_shards);
    }
    for (worker_shards) |*sb| sb.* = try ShardBuf.init(allocator);

    const errs = try allocator.alloc(?anyerror, n_threads);
    defer allocator.free(errs);
    @memset(errs, null);

    var shared = ScanShared{
        .scans = scans,
        .n_shards = n_shards,
        .alloc = allocator,
        .worker_shards = worker_shards,
        .errs = errs,
        .scan_to_shard = scan_to_shard,
        .cust_ci = cust_ci,
    };

    const threads = try allocator.alloc(std.Thread, n_threads);
    defer allocator.free(threads);
    for (0..n_threads - 1) |w| {
        threads[w] = try std.Thread.spawn(.{}, scanWorker, .{ &shared, w });
    }
    scanWorker(&shared, n_threads - 1);
    for (0..n_threads - 1) |w| threads[w].join();
    for (errs) |e| if (e) |err| return err;
    const scan_ms = prof.ticksToMs(prof.nowTicks() - t);

    // ---- phase 2 (milestone B) -------------------------------------------
    t = prof.nowTicks();
    const shards = try allocator.alloc(Shard, n_shards);
    for (shards) |*s| s.* = .{ .buf = undefined };
    const caps = try allocator.alloc(P3Cap, n_threads);
    for (caps) |*c| c.* = .{};
    const p5sums = try allocator.alloc(P5Sums, n_threads);
    for (p5sums) |*c| c.* = .{};
    const p6caps = try allocator.alloc(P6Cap, n_threads);
    for (p6caps) |*c| c.* = .{};
    const p15caps = try allocator.alloc(P15Cap, n_threads);
    for (p15caps) |*c| c.* = .{};
    @memset(errs, null);

    var stage = StageShared{
        .shards = shards,
        .n_threads = n_threads,
        .n_shards = n_shards,
        .worker_shards = worker_shards,
        .alloc = allocator,
        .errs = errs,
        .caps = caps,
        .p5sums = p5sums,
        .p6caps = p6caps,
        .p15caps = p15caps,
        .rate_part = &bc.rate_part,
        .plan_part = &bc.plan_part,
        .bc = &bc,
    };
    for (0..n_threads - 1) |w| {
        threads[w] = try std.Thread.spawn(.{}, stageWorker, .{ &stage, w });
    }
    stageWorker(&stage, n_threads - 1);
    for (0..n_threads - 1) |w| threads[w].join();
    for (errs) |e| if (e) |err| return err;
    const stage_ms = prof.ticksToMs(prof.nowTicks() - t);

    // ---- combine + report -------------------------------------------------
    var cap = P3Cap{};
    for (caps) |*c| {
        cap.rows += c.rows;
        cap.sum_orig += c.sum_orig;
        cap.sum_icid += c.sum_icid;
        cap.sum_amount += c.sum_amount;
        for (0..13) |slot| try cap.takeStr(allocator, slot, c.max_str[slot]);
        for (0..4) |slot| cap.takeDate(slot, c.max_date[slot]);
        cap.takeInt(0, c.max_int[0]);
        cap.takeInt(1, c.max_int[1]);
    }
    var est_total: usize = 0;
    var agg_total: usize = 0;
    for (shards) |*s| {
        est_total += s.est_rows;
        agg_total += s.agg_rows;
    }

    var p5 = P5Sums{};
    for (p5sums) |*c| {
        p5.ipid += c.ipid;
        p5.rn1 += c.rn1;
        p5.rn2 += c.rn2;
        p5.hadj += c.hadj;
    }

    // p6 / p15 cap combines.
    var p6 = P6Cap{};
    for (p6caps) |*c| {
        p6.rows += c.rows;
        i32MaxInto(&p6.max_proj, c.max_proj);
        i32MaxInto(&p6.max_div, c.max_div);
        i32MaxInto(&p6.max_month, c.max_month);
        for (0..8) |s| try strMax(allocator, &p6.max_str[s], c.max_str[s]);
        i32MaxInto(&p6.max_bill, c.max_bill);
        i32MaxInto(&p6.max_min, c.max_min);
        i32MaxInto(&p6.max_invd, c.max_invd);
        p6.sum_amount += c.sum_amount;
        p6.sum_orig += c.sum_orig;
        p6.sum_nonrec += c.sum_nonrec;
        p6.sum_orignonrec += c.sum_orignonrec;
        p6.sum_icid += c.sum_icid;
        p6.sum_er += c.sum_er;
        p6.sum_plan += c.sum_plan;
        p6.sum_hadj += c.sum_hadj;
    }
    var p15 = P15Cap{};
    for (p15caps) |*c| {
        p15.rows += c.rows;
        i32MaxInto(&p15.max_proj, c.max_proj);
        i32MaxInto(&p15.max_div, c.max_div);
        i32MaxInto(&p15.max_month, c.max_month);
        i32MaxInto(&p15.max_date, c.max_date);
        i32MaxInto(&p15.max_min, c.max_min);
        for (0..8) |s| try strMax(allocator, &p15.max_str[s], c.max_str[s]);
        p15.sum_amount += c.sum_amount;
        p15.sum_orig += c.sum_orig;
        p15.sum_nonrec += c.sum_nonrec;
        p15.sum_orignonrec += c.sum_orignonrec;
        p15.sum_er += c.sum_er;
        p15.sum_hadj += c.sum_hadj;
        p15.sum_icid += c.sum_icid;
        p15.sum_plan += c.sum_plan;
        p15.sum_rn += c.sum_rn;
    }

    var d0: [12]u8 = undefined;
    var d1: [12]u8 = undefined;
    var d2: [12]u8 = undefined;
    std.debug.print(
        "rf_custom D: open={d:.0}ms bc={d:.0}ms build={d:.0}ms scan={d:.0}ms stages={d:.0}ms total={d:.0}ms union_rows={d} est_rows={d} agg_rows={d} final_rows={d}\n",
        .{ open_ms, bc_ms, build_ms, scan_ms, stage_ms, prof.ticksToMs(prof.nowTicks() - t_all), cap.rows, est_total, agg_total, p15.rows },
    );
    std.debug.print(
        "p5sums: sum_planId={d} sum_rn1={d} sum_rn2={d} sum_hasAdjustment={d}\n",
        .{ p5.ipid, p5.rn1, p5.rn2, p5.hadj },
    );
    {
        var b0: [12]u8 = undefined;
        var b1: [12]u8 = undefined;
        var b2: [12]u8 = undefined;
        var b3: [12]u8 = undefined;
        std.debug.print(
            "p6cap: g0={?d} g1={?d} g2={s} g3={s} s0={s} s1={s} s2={s} s3={s} s4={s} s5={s} s6={s} s7={s} s8={s} s9={d} s10={d} s11={d} s12={d} s13={s} s14={d} s15={d:.6} s16={d} s17={d}\n",
            .{
                p6.max_proj,                   p6.max_div,
                p6.max_str[0] orelse "null",   dateStr(&b0, p6.max_month),
                p6.max_str[1] orelse "null",   p6.max_str[2] orelse "null",
                p6.max_str[3] orelse "null",   p6.max_str[4] orelse "null",
                p6.max_str[5] orelse "null",   p6.max_str[6] orelse "null",
                dateStr(&b1, p6.max_bill),     dateStr(&b2, p6.max_min),
                dateStr(&b3, p6.max_invd),     p6.sum_amount,
                p6.sum_orig,                   p6.sum_nonrec,
                p6.sum_orignonrec,             p6.max_str[7] orelse "null",
                p6.sum_icid,                   p6.sum_er,
                p6.sum_plan,                   p6.sum_hadj,
            },
        );
        var c0: [12]u8 = undefined;
        var c1: [12]u8 = undefined;
        var c2: [12]u8 = undefined;
        std.debug.print(
            "p15cap: rows_out=1 g0={?d} g1={?d} g2={s} g3={s} s0={s} s1={s} s2={s} s3={s} s4={s} s5={s} s6={s} s7={s} s8={d} s9={d} s10={d} s11={d} s12={d:.6} s13={d} s14={s} s15={d} s16={d} s17..32=0 s33={d} (cap_groups~{d})\n",
            .{
                p15.max_proj,                  p15.max_div,
                p15.max_str[0] orelse "null",  dateStr(&c0, p15.max_month),
                p15.max_str[1] orelse "null",  p15.max_str[2] orelse "null",
                p15.max_str[3] orelse "null",  p15.max_str[4] orelse "null",
                p15.max_str[5] orelse "null",  p15.max_str[6] orelse "null",
                dateStr(&c1, p15.max_date),    dateStr(&c2, p15.max_min),
                p15.sum_amount,                p15.sum_orig,
                p15.sum_nonrec,                p15.sum_orignonrec,
                p15.sum_er,                    p15.sum_hadj,
                p15.max_str[7] orelse "null",  p15.sum_icid,
                p15.sum_plan,                  p15.sum_rn,
                p15.rows,
            },
        );
    }
    std.debug.print(
        "p3cap: rows_out=1 g0={?d} g1={?d} g2={s} g3={s} s0={s} s1={d} s2={s} s3={s} s4={s} s5={s} s6={s} s7={s} s8={s} s9={s} s10={d} s11={s} s12={s} s13={s} s14={s} s15={s} s16={d}\n",
        .{
            cap.max_int[0],                 cap.max_int[1],
            cap.max_str[0] orelse "null",   dateStr(&d0, cap.max_date[0]),
            cap.max_str[1] orelse "null",   cap.sum_orig,
            cap.max_str[2] orelse "null",   dateStr(&d1, cap.max_date[1]),
            dateStr(&d2, cap.max_date[2]),  cap.max_str[3] orelse "null",
            cap.max_str[4] orelse "null",   cap.max_str[5] orelse "null",
            cap.max_str[6] orelse "null",   cap.max_str[7] orelse "null",
            cap.sum_icid,                   cap.max_str[8] orelse "null",
            cap.max_str[9] orelse "null",   cap.max_str[10] orelse "null",
            cap.max_str[11] orelse "null",  cap.max_str[12] orelse "null",
            cap.sum_amount,
        },
    );
}

const Coord = struct { seg: usize, rg: usize };
fn flatToCoord(flat: usize, seg_start: []const usize, n_segs: usize) Coord {
    var s: usize = 0;
    while (s < n_segs and seg_start[s + 1] <= flat) s += 1;
    return .{ .seg = s, .rg = flat - seg_start[@min(s, n_segs)] };
}
