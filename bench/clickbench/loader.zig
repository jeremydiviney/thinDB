//! ClickBench TSV loader.
//!
//! Parses tab-separated rows into per-column `ColumnStore`
//! accumulators, flushes to the table via `Table.insertBatch`
//! every `batch_rows` rows (default 65 536 — one row group).
//!
//! Type coercion at parse time:
//!   * empty field → either NULL (if column nullable) or the zero
//!     value of the type (current schema declares everything NOT
//!     NULL so we use zero).
//!   * INT / BIGINT / SMALLINT → `std.fmt.parseInt`
//!   * DATE  → "YYYY-MM-DD" → days since epoch
//!   * DATETIME → "YYYY-MM-DD HH:MM:SS" → microseconds since epoch
//!   * VARCHAR / STRING / CHAR → bytes copied through
//!
//! The TSV format follows ClickHouse's TabSeparated dialect:
//! tab between fields, newline between rows, no quoting, `\t`
//! and `\n` escape sequences in strings. This loader implements
//! a subset — backslash escapes in strings are passed through
//! literally for now. If the dataset has escaped tabs in URL/
//! Referer fields, those rows will look like extra columns
//! and the row will be rejected.

const std = @import("std");
const thindb = @import("thindb");

const schema_mod = @import("schema.zig");

const Allocator = std.mem.Allocator;
const ColumnStore = thindb.engine.ColumnStore;
const Column = thindb.Column;

/// One in-flight batch builder: a ColumnStore per column, plus a
/// row counter. Rows accumulate here until we hit `batch_rows`,
/// at which point we flush via `Table.insertBatch` and reset.
pub const Batch = struct {
    allocator: Allocator,
    schema: []const Column,
    stores: []ColumnStore,
    row_count: usize = 0,

    pub fn init(allocator: Allocator, schema: []const Column) !Batch {
        const stores = try allocator.alloc(ColumnStore, schema.len);
        errdefer allocator.free(stores);
        var n_inited: usize = 0;
        errdefer for (stores[0..n_inited]) |*s| s.deinit(allocator);
        for (schema) |c| {
            stores[n_inited] = try ColumnStore.init(allocator, c.type, c.nullable);
            n_inited += 1;
        }
        return .{ .allocator = allocator, .schema = schema, .stores = stores };
    }

    pub fn deinit(self: *Batch) void {
        for (self.stores) |*s| s.deinit(self.allocator);
        self.allocator.free(self.stores);
    }

    pub fn reset(self: *Batch) !void {
        for (self.stores, self.schema) |*s, c| {
            s.deinit(self.allocator);
            s.* = try ColumnStore.init(self.allocator, c.type, c.nullable);
        }
        self.row_count = 0;
    }

    /// Flush via `Table.insertBatch`. Caller drops the batch's
    /// row count back to zero with `reset`.
    pub fn flush(self: *Batch, t: *thindb.Table) !void {
        if (self.row_count == 0) return;
        const views = try self.allocator.alloc(thindb.storage.ColumnView, self.stores.len);
        defer self.allocator.free(views);
        for (self.stores, views) |*s, *v| v.* = s.view();
        try t.insertBatch(self.schema, views, self.row_count);
    }
};

pub const LoadOptions = struct {
    /// Stop loading after this many rows (0 = no limit).
    max_rows: usize = 0,
    /// Flush to the table every this many rows.
    batch_rows: usize = 65_536,
    /// Print a progress line every this many rows (0 = silent).
    progress_every: usize = 500_000,
    /// Parse worker threads. 0 = ~25% of physical cores (the compactor's
    /// auto default).
    threads: usize = 0,
    /// Bytes per parse chunk (the unit of work a worker claims).
    chunk_bytes: usize = 32 * 1024 * 1024,
};

pub const LoadStats = struct {
    rows_loaded: u64,
    bytes_read: u64,
    elapsed_ns: u64,
    rejected_rows: u64,
};

/// TSV → table loader: parallel parse, strictly-ordered single-memtable
/// insert.
///
/// The file is divided into fixed-size byte chunks. Workers claim chunk
/// indices from an atomic cursor and parse their chunk independently
/// (positional reads — the file is never loaded whole). A chunk owns every
/// line that STARTS inside its byte range; a line's tail may extend past the
/// range end, so the reader extends until the final newline.
///
/// Insertion order is exact input order: after parsing chunk k, a worker
/// waits for the insert ticket (`insert_cursor == k`), inserts its batches
/// into the table's single memtable, and passes the ticket on. Upsert /
/// unique resolution therefore sees rows in the same order as a serial
/// load. Run-ahead is naturally bounded at one parsed chunk per worker.
pub fn loadTsv(
    allocator: Allocator,
    io: std.Io,
    t: *thindb.Table,
    tsv_path: []const u8,
    opts: LoadOptions,
) !LoadStats {
    const cwd = std.Io.Dir.cwd();
    var probe = try cwd.openFile(io, tsv_path, .{});
    const file_len: u64 = try probe.length(io);
    probe.close(io);

    const chunk_bytes = @max(opts.chunk_bytes, 1024 * 1024);
    const n_chunks: usize = @intCast((file_len + chunk_bytes - 1) / chunk_bytes);
    const want_threads = thindb.api.resolveCompactThreads(allocator, opts.threads);
    const n_workers = @max(1, @min(want_threads, n_chunks));

    var ctx = LoadCtx{
        .allocator = allocator,
        .io = io,
        .table = t,
        .path = tsv_path,
        .file_len = file_len,
        .chunk_bytes = chunk_bytes,
        .n_chunks = n_chunks,
        .opts = opts,
        .start_ts = std.Io.Clock.awake.now(io),
    };

    if (n_workers == 1) {
        loadWorker(&ctx);
    } else {
        const handles = try allocator.alloc(std.Thread, n_workers - 1);
        defer allocator.free(handles);
        var spawned: usize = 0;
        for (handles) |*h| {
            h.* = std.Thread.spawn(.{}, loadWorker, .{&ctx}) catch break;
            spawned += 1;
        }
        loadWorker(&ctx);
        for (handles[0..spawned]) |h| h.join();
    }

    if (ctx.first_err) |e| return e;

    return .{
        .rows_loaded = ctx.rows_loaded.load(.acquire),
        .bytes_read = file_len,
        .elapsed_ns = @intCast(ctx.start_ts.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds()),
        .rejected_rows = ctx.rejected_rows.load(.acquire),
    };
}

const LoadCtx = struct {
    allocator: Allocator,
    io: std.Io,
    table: *thindb.Table,
    path: []const u8,
    file_len: u64,
    chunk_bytes: usize,
    n_chunks: usize,
    opts: LoadOptions,
    start_ts: std.Io.Timestamp,

    next_chunk: std.atomic.Value(usize) = .init(0),
    insert_cursor: std.atomic.Value(usize) = .init(0),
    rows_loaded: std.atomic.Value(u64) = .init(0),
    rejected_rows: std.atomic.Value(u64) = .init(0),
    /// max_rows reached — workers stop claiming chunks.
    stop: std.atomic.Value(bool) = .init(false),
    failed: std.atomic.Value(bool) = .init(false),
    err_mutex: std.atomic.Mutex = .unlocked,
    first_err: ?anyerror = null,
    /// Serialized by the insert ticket.
    last_progress: u64 = 0,

    fn fail(self: *LoadCtx, e: anyerror) void {
        while (!self.err_mutex.tryLock()) std.atomic.spinLoopHint();
        if (self.first_err == null) self.first_err = e;
        self.err_mutex.unlock();
        self.failed.store(true, .release);
    }
};

fn loadWorker(ctx: *LoadCtx) void {
    var file = std.Io.Dir.cwd().openFile(ctx.io, ctx.path, .{}) catch |e| return ctx.fail(e);
    defer file.close(ctx.io);

    while (!ctx.failed.load(.acquire) and !ctx.stop.load(.acquire)) {
        const k = ctx.next_chunk.fetchAdd(1, .monotonic);
        if (k >= ctx.n_chunks) return;
        workChunk(ctx, &file, k) catch |e| {
            ctx.fail(e);
            // Keep the ticket chain alive so waiting workers can bail.
            bumpTicketTo(ctx, k);
            return;
        };
    }
}

fn bumpTicketTo(ctx: *LoadCtx, k: usize) void {
    while (ctx.insert_cursor.load(.acquire) != k) {
        if (ctx.insert_cursor.load(.acquire) > k) return;
        std.atomic.spinLoopHint();
    }
    ctx.insert_cursor.store(k + 1, .release);
}

fn workChunk(ctx: *LoadCtx, file: *std.Io.File, k: usize) !void {
    const allocator = ctx.allocator;

    // ---- Read the chunk's bytes (plus the boundary context they need) ----
    const chunk_start: u64 = @as(u64, k) * ctx.chunk_bytes;
    const chunk_end: u64 = @min(chunk_start + ctx.chunk_bytes, ctx.file_len);
    // Read from one byte before the range: byte chunk_start-1 tells whether a
    // line starts exactly at chunk_start (previous byte is '\n') or the range
    // begins mid-line (that line belongs to chunk k-1).
    const read_base: u64 = if (k == 0) 0 else chunk_start - 1;

    var data: std.ArrayList(u8) = .empty;
    defer data.deinit(allocator);
    try readRange(ctx.io, file, &data, allocator, read_base, chunk_end);

    // Extend until the last line that starts inside the range has its newline
    // (or EOF). Lines are bounded in practice; cap extensions defensively.
    const tail_step: usize = 1024 * 1024;
    var extended: usize = 0;
    while (true) {
        const last_nl = std.mem.lastIndexOfScalar(u8, data.items, '\n');
        if (last_nl != null and read_base + @as(u64, last_nl.?) + 1 >= chunk_end) break;
        const cur_end = read_base + data.items.len;
        if (cur_end >= ctx.file_len) break;
        if (extended >= 64 * tail_step) return error.RowTooLong;
        try readRange(ctx.io, file, &data, allocator, cur_end, @min(cur_end + tail_step, ctx.file_len));
        extended += tail_step;
    }

    // ---- Parse every line that starts inside [chunk_start, chunk_end) ----
    var pos: usize = 0;
    if (k != 0) {
        if (data.items.len == 0) return;
        if (data.items[0] == '\n') {
            pos = 1;
        } else {
            const nl = std.mem.indexOfScalar(u8, data.items, '\n') orelse return; // no line starts here
            pos = nl + 1;
        }
    }

    var batches: std.ArrayList(Batch) = .empty;
    defer {
        for (batches.items) |*b| b.deinit();
        batches.deinit(allocator);
    }
    var cur: ?Batch = try Batch.init(allocator, ctx.table.schema.columns);
    errdefer if (cur) |*b| b.deinit();
    var rejected: u64 = 0;

    while (read_base + pos < chunk_end and pos < data.items.len) {
        const line_end = std.mem.indexOfScalarPos(u8, data.items, pos, '\n') orelse data.items.len;
        var line = data.items[pos..line_end];
        pos = line_end + 1;
        if (line.len > 0 and line[line.len - 1] == '\r') line = line[0 .. line.len - 1];
        if (line.len == 0) continue;

        parseAndAppend(allocator, &cur.?, line) catch {
            rejected += 1;
            continue;
        };
        if (cur.?.row_count >= ctx.opts.batch_rows) {
            try batches.append(allocator, cur.?);
            cur = null;
            cur = try Batch.init(allocator, ctx.table.schema.columns);
        }
    }
    if (cur.?.row_count > 0) {
        try batches.append(allocator, cur.?);
        cur = null;
    } else {
        cur.?.deinit();
        cur = null;
    }
    _ = ctx.rejected_rows.fetchAdd(rejected, .monotonic);

    // ---- Insert ticket: strict chunk order, single memtable ----
    while (ctx.insert_cursor.load(.acquire) != k) {
        if (ctx.failed.load(.acquire)) return error.LoadAborted;
        std.atomic.spinLoopHint();
    }
    defer ctx.insert_cursor.store(k + 1, .release);

    for (batches.items) |*b| {
        var take = b.row_count;
        if (ctx.opts.max_rows != 0) {
            const loaded = ctx.rows_loaded.load(.monotonic);
            if (loaded >= ctx.opts.max_rows) {
                ctx.stop.store(true, .release);
                return;
            }
            take = @min(take, ctx.opts.max_rows - @as(usize, @intCast(loaded)));
        }
        if (take == 0) continue;

        const views = try allocator.alloc(thindb.storage.ColumnView, b.stores.len);
        defer allocator.free(views);
        for (b.stores, views) |*s, *v| v.* = s.view();
        try ctx.table.insertBatch(b.schema, views, take);

        const loaded_now = ctx.rows_loaded.fetchAdd(@intCast(take), .monotonic) + take;
        if (ctx.opts.progress_every > 0 and
            loaded_now / ctx.opts.progress_every > ctx.last_progress / ctx.opts.progress_every)
        {
            ctx.last_progress = loaded_now;
            const ns: u64 = @intCast(ctx.start_ts.durationTo(std.Io.Clock.awake.now(ctx.io)).toNanoseconds());
            const elapsed_s = @as(f64, @floatFromInt(ns)) / 1e9;
            const rate = @as(f64, @floatFromInt(loaded_now)) / elapsed_s;
            std.debug.print("  …loaded {d} rows ({d:.0} rows/s)\n", .{ loaded_now, rate });
        }
        if (ctx.opts.max_rows != 0 and loaded_now >= ctx.opts.max_rows) {
            ctx.stop.store(true, .release);
            return;
        }
    }
}

/// Append bytes [from, to) of `file` onto `data`.
fn readRange(
    io: std.Io,
    file: *std.Io.File,
    data: *std.ArrayList(u8),
    allocator: Allocator,
    from: u64,
    to: u64,
) !void {
    if (to <= from) return;
    const len: usize = @intCast(to - from);
    const old_len = data.items.len;
    try data.resize(allocator, old_len + len);
    const got = try file.readPositionalAll(io, data.items[old_len..], from);
    if (got != len) return error.UnexpectedEof;
}

// =============================================================================
// Per-line parser
// =============================================================================

fn parseAndAppend(allocator: Allocator, batch: *Batch, line: []const u8) !void {
    var cursor: usize = 0;
    var col_idx: usize = 0;
    while (col_idx < batch.schema.len) : (col_idx += 1) {
        const field_end = nextFieldEnd(line, cursor);
        const field = line[cursor..field_end];
        // For non-last cols we must have seen a tab; if we hit EOL
        // before consuming all columns, the row is malformed.
        if (field_end >= line.len and col_idx + 1 < batch.schema.len) {
            return error.RowFieldCountMismatch;
        }
        try appendField(allocator, batch, col_idx, field);
        cursor = field_end + 1; // skip the tab (no-op past EOL)
    }
    batch.row_count += 1;
}

fn nextFieldEnd(line: []const u8, from: usize) usize {
    var i = from;
    while (i < line.len and line[i] != '\t') : (i += 1) {}
    return i;
}

fn appendField(allocator: Allocator, batch: *Batch, col_idx: usize, field: []const u8) !void {
    const store = &batch.stores[col_idx];
    const col_type = batch.schema[col_idx].type;
    switch (col_type) {
        .int => try store.data.int.append(allocator, try parseIntOrZero(i32, field)),
        .bigint => try store.data.bigint.append(allocator, try parseIntOrZero(i64, field)),
        .smallint => try store.data.smallint.append(allocator, try parseIntOrZero(i16, field)),
        .tinyint => try store.data.tinyint.append(allocator, try parseIntOrZero(i8, field)),
        .largeint => try store.data.largeint.append(allocator, try parseIntOrZero(i128, field)),
        .boolean => {
            const v: u8 = if (field.len == 0) 0 else if (field[0] == '1' or field[0] == 't' or field[0] == 'T') 1 else 0;
            try store.data.boolean.append(allocator, v);
        },
        .float => try store.data.float.append(allocator, try parseFloatOrZero(f32, field)),
        .double => try store.data.double.append(allocator, try parseFloatOrZero(f64, field)),
        .date => try store.data.date.append(allocator, try parseDateOrZero(field)),
        .datetime => try store.data.datetime.append(allocator, try parseDateTimeOrZero(field)),
        .varchar, .string, .char => try store.data.string.appendValue(allocator, field),
        .decimal64, .decimal128, .uuid => return error.UnsupportedType,
    }
}

fn parseIntOrZero(comptime T: type, s: []const u8) !T {
    if (s.len == 0) return 0;
    return std.fmt.parseInt(T, s, 10) catch 0;
}

fn parseFloatOrZero(comptime T: type, s: []const u8) !T {
    if (s.len == 0) return 0;
    return std.fmt.parseFloat(T, s) catch 0;
}

/// Parse "YYYY-MM-DD" → days since 1970-01-01. Falls back to 0 on
/// malformed input.
fn parseDateOrZero(s: []const u8) !i32 {
    if (s.len != 10) return 0;
    const year = std.fmt.parseInt(i32, s[0..4], 10) catch return 0;
    const month = std.fmt.parseInt(u32, s[5..7], 10) catch return 0;
    const day = std.fmt.parseInt(u32, s[8..10], 10) catch return 0;
    return ymdToDays(year, month, day);
}

/// Parse "YYYY-MM-DD HH:MM:SS" → microseconds since 1970-01-01T00:00:00Z.
/// Falls back to 0 on malformed input.
fn parseDateTimeOrZero(s: []const u8) !i64 {
    if (s.len < 19) return 0;
    const year = std.fmt.parseInt(i32, s[0..4], 10) catch return 0;
    const month = std.fmt.parseInt(u32, s[5..7], 10) catch return 0;
    const day = std.fmt.parseInt(u32, s[8..10], 10) catch return 0;
    const hour = std.fmt.parseInt(u32, s[11..13], 10) catch return 0;
    const minute = std.fmt.parseInt(u32, s[14..16], 10) catch return 0;
    const second = std.fmt.parseInt(u32, s[17..19], 10) catch return 0;
    const days: i64 = ymdToDays(year, month, day);
    const secs: i64 = days * 86_400 + @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second);
    return secs * 1_000_000;
}

/// Hinnant's civil-from-days for the inverse direction. Returns days
/// from 1970-01-01.
fn ymdToDays(year: i32, month: u32, day: u32) i32 {
    var y: i32 = year;
    if (month <= 2) y -= 1;
    const era: i32 = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe: u32 = @intCast(y - era * 400);
    const m_adj: i32 = @intCast(if (month > 2) month - 3 else month + 9);
    const doy: u32 = @intCast(@divFloor(153 * m_adj + 2, 5) + @as(i32, @intCast(day)) - 1);
    const doe: u32 = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + @as(i32, @intCast(doe)) - 719468;
}
