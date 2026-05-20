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
};

pub const LoadStats = struct {
    rows_loaded: u64,
    bytes_read: u64,
    elapsed_ns: u64,
    rejected_rows: u64,
};

/// TSV → table loader. Reads the whole file into memory then splits
/// on newlines. v1 simplicity — fine up to a few GB. Switching to a
/// streaming reader is a follow-up when we need to handle the full
/// 75 GB dataset.
pub fn loadTsv(
    allocator: Allocator,
    io: std.Io,
    t: *thindb.Table,
    tsv_path: []const u8,
    opts: LoadOptions,
) !LoadStats {
    const cwd = std.Io.Dir.cwd();
    const contents = try cwd.readFileAlloc(io, tsv_path, allocator, .unlimited);
    defer allocator.free(contents);

    var batch = try Batch.init(allocator, t.schema.columns);
    defer batch.deinit();

    var stats = LoadStats{
        .rows_loaded = 0,
        .bytes_read = @intCast(contents.len),
        .elapsed_ns = 0,
        .rejected_rows = 0,
    };

    const start_ts = std.Io.Clock.awake.now(io);

    var line_iter = std.mem.splitScalar(u8, contents, '\n');
    while (line_iter.next()) |line_raw| {
        var line = line_raw;
        if (line.len > 0 and line[line.len - 1] == '\r') line = line[0 .. line.len - 1];
        if (line.len == 0) continue;

        parseAndAppend(allocator, &batch, line) catch {
            stats.rejected_rows += 1;
            continue;
        };

        if (batch.row_count >= opts.batch_rows) {
            try batch.flush(t);
            stats.rows_loaded += @intCast(batch.row_count);
            try batch.reset();

            if (opts.progress_every > 0 and stats.rows_loaded % opts.progress_every == 0) {
                const ns: u64 = @intCast(start_ts.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
                const elapsed_s = @as(f64, @floatFromInt(ns)) / 1e9;
                const rate = @as(f64, @floatFromInt(stats.rows_loaded)) / elapsed_s;
                std.debug.print("  …loaded {d} rows ({d:.0} rows/s)\n", .{ stats.rows_loaded, rate });
            }
        }

        if (opts.max_rows != 0 and stats.rows_loaded + batch.row_count >= opts.max_rows) break;
    }

    // Final partial flush.
    if (batch.row_count > 0) {
        try batch.flush(t);
        stats.rows_loaded += @intCast(batch.row_count);
    }

    stats.elapsed_ns = @intCast(start_ts.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
    return stats;
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
