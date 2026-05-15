//! Public thinDB API: Database, Table, Config, TableOptions.
//!
//! v0.1 scope:
//!   - open a Database backed by a directory
//!   - createOrOpen a Table with a strict schema
//!   - row-oriented insert (with comptime field validation)
//!   - manual flush (memtable → on-disk segment + manifest update)
//!   - segment scanning is provided by `src/exec/` (operators)

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const types = @import("../types.zig");
const Type = types.Type;
const Schema = types.Schema;
const Column = types.Column;
const TypeTag = types.TypeTag;
const Value = types.Value;
const ValueTag = types.ValueTag;

const storage = @import("../storage/storage.zig");
const engine = @import("../engine/engine.zig");
const exec = @import("../exec/exec.zig");

pub const Error = error{
    SchemaMismatch,
    UnsupportedUniqueKeyType,
};

pub const Config = struct {
    /// Default rows per row-group in flushed segments.
    row_group_size: usize = 65_536,

    /// Auto-flush triggers. A flush fires inline (from `insert` or `delete`)
    /// when ANY of these conditions hold against the current memtable.
    auto_flush_bytes: usize = 64 * 1024 * 1024,
    auto_flush_rows: u64 = 1_000_000,
    /// Seconds since the memtable's first write. 0 disables the time trigger.
    auto_flush_secs: u32 = 5,
    /// Time-based trigger only fires once both these are met (avoids tiny
    /// segments on low-volume tables).
    auto_flush_min_rows: u64 = 1_000,
    auto_flush_min_bytes: usize = 1 * 1024 * 1024,

    /// LRU cache budget for decompressed column blocks. 0 disables caching.
    cache_size_bytes: usize = 256 * 1024 * 1024,
};

pub const TableOptions = struct {
    /// Required for create. On reopen via `db.table(...)`, must match the
    /// persisted schema's order key. Not used by `db.openTable(...)`.
    order_key: []const []const u8,
    unique: bool = false,
    /// Overrides Database.config.row_group_size for this table when set.
    row_group_size: ?usize = null,
};

/// Used by `openTable` — no schema or order_key, just runtime tunables.
pub const OpenOptions = struct {
    row_group_size: ?usize = null,
};

pub const Database = struct {
    allocator: Allocator,
    io: Io,
    data_dir: Io.Dir,
    config: Config,
    tables: std.StringHashMap(*Table),

    /// Open a database rooted at `data_dir`. Caller retains ownership of
    /// `data_dir` and must keep it open for the lifetime of the returned
    /// `Database`.
    pub fn open(
        allocator: Allocator,
        io: Io,
        data_dir: Io.Dir,
        config: Config,
    ) !*Database {
        const self = try allocator.create(Database);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .data_dir = data_dir,
            .config = config,
            .tables = .init(allocator),
        };
        return self;
    }

    pub fn close(self: *Database) void {
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.close();
        }
        self.tables.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    /// Create-or-open a table with the given schema. If the table already
    /// exists on disk, its persisted schema must match the one passed here.
    pub fn table(
        self: *Database,
        name: []const u8,
        schema: Schema,
        options: TableOptions,
    ) !*Table {
        try schema.validate();

        if (self.tables.get(name)) |existing| {
            if (schemaFingerprint(schema) != existing.schema_fingerprint) {
                return Error.SchemaMismatch;
            }
            return existing;
        }

        const t = try Table.open(
            self.allocator,
            self.io,
            self.data_dir,
            name,
            schema,
            self.config,
            options.row_group_size orelse self.config.row_group_size,
        );
        errdefer t.close();

        try self.tables.put(t.name, t);
        return t;
    }

    /// Open an existing table by name. The schema is loaded from the
    /// persisted `schema.bin`. Errors if the table doesn't exist.
    pub fn openTable(
        self: *Database,
        name: []const u8,
        options: OpenOptions,
    ) !*Table {
        if (self.tables.get(name)) |existing| return existing;

        const t = try Table.open(
            self.allocator,
            self.io,
            self.data_dir,
            name,
            null,
            self.config,
            options.row_group_size orelse self.config.row_group_size,
        );
        errdefer t.close();

        try self.tables.put(t.name, t);
        return t;
    }
};

pub const Table = struct {
    allocator: Allocator,
    io: Io,
    name: []u8,
    schema_owner: storage.schema_file.SchemaOwner,
    schema: Schema,
    schema_fingerprint: u64,
    row_group_size: usize,
    order_key_indices: []usize,

    /// LRU cache of decompressed column-block bytes. Lifetime = Table.
    cache: storage.cache.Cache,

    // Auto-flush configuration (copied from Database.Config at open time).
    auto_flush_bytes: usize,
    auto_flush_rows: u64,
    auto_flush_secs: u32,
    auto_flush_min_rows: u64,
    auto_flush_min_bytes: usize,
    /// Timestamp at which the current (post-flush) memtable received its
    /// first row. Reset to `null` after every flush. Drives the time trigger.
    first_write_ts: ?Io.Timestamp = null,

    table_dir: Io.Dir,
    segments_dir: Io.Dir,

    manifest: storage.Manifest,
    memtable: engine.Memtable,

    fn open(
        allocator: Allocator,
        io: Io,
        data_dir: Io.Dir,
        name: []const u8,
        maybe_schema: ?Schema,
        cfg: Config,
        row_group_size: usize,
    ) !*Table {
        var table_dir = try data_dir.createDirPathOpen(io, name, .{});
        errdefer table_dir.close(io);

        var schema_owner = try acquireSchema(allocator, io, table_dir, maybe_schema);
        errdefer schema_owner.deinit();
        const schema = schema_owner.view();

        // Enforce v0.2 limitation: unique-key upsert is implemented only for
        // single-column bigint keys. Reject anything else at open time so
        // users don't get silent no-op upserts.
        if (schema.unique) try validateUniqueKey(schema);

        const fp = schemaFingerprint(schema);

        var segments_dir = try table_dir.createDirPathOpen(io, "segments", .{});
        errdefer segments_dir.close(io);

        var manifest = try storage.readManifest(allocator, io, table_dir, fp);
        errdefer manifest.deinit();

        var memtable = try engine.Memtable.init(allocator, schema);
        errdefer memtable.deinit();

        const name_copy = try allocator.dupe(u8, name);
        errdefer allocator.free(name_copy);

        const order_key_indices = try allocator.alloc(usize, schema.order_key.len);
        errdefer allocator.free(order_key_indices);
        for (schema.order_key, 0..) |k, i| {
            order_key_indices[i] = schema.columnIndex(k) orelse return Error.SchemaMismatch;
        }

        const self = try allocator.create(Table);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .io = io,
            .name = name_copy,
            .schema_owner = schema_owner,
            .schema = schema,
            .schema_fingerprint = fp,
            .row_group_size = row_group_size,
            .order_key_indices = order_key_indices,
            .cache = storage.cache.Cache.init(allocator, cfg.cache_size_bytes),
            .auto_flush_bytes = cfg.auto_flush_bytes,
            .auto_flush_rows = cfg.auto_flush_rows,
            .auto_flush_secs = cfg.auto_flush_secs,
            .auto_flush_min_rows = cfg.auto_flush_min_rows,
            .auto_flush_min_bytes = cfg.auto_flush_min_bytes,
            .table_dir = table_dir,
            .segments_dir = segments_dir,
            .manifest = manifest,
            .memtable = memtable,
        };
        return self;
    }

    fn close(self: *Table) void {
        const allocator = self.allocator;
        const io = self.io;
        self.cache.deinit();
        self.memtable.deinit();
        self.manifest.deinit();
        self.segments_dir.close(io);
        self.table_dir.close(io);
        self.schema_owner.deinit();
        allocator.free(self.order_key_indices);
        allocator.free(self.name);
        allocator.destroy(self);
    }

    /// Append rows to the in-memory memtable. Rows are validated against the
    /// schema at compile time (column names and Zig types must match) and at
    /// runtime (every non-nullable schema column must appear).
    ///
    /// If the table was created with `unique = true`, any inserted row whose
    /// order key matches an existing row (in the memtable or any segment)
    /// causes the old row to be tombstoned. The new row becomes the visible
    /// value. (StarRocks "last writer wins" semantics.)
    pub fn insert(self: *Table, rows: anytype) !void {
        const was_empty = self.memtable.isEmpty();
        try self.memtable.insertRows(rows);
        if (was_empty and !self.memtable.isEmpty()) {
            self.first_write_ts = Io.Clock.awake.now(self.io);
        }
        if (self.schema.unique) {
            try applyUpsertResolution(self);
        }
        try self.maybeAutoFlush();
    }

    /// Flush the memtable to a new segment on disk and update the manifest
    /// atomically. Rows are written sorted by the table's order key. No-op
    /// if the memtable is empty.
    pub fn flush(self: *Table) !void {
        if (self.memtable.isEmpty()) {
            self.first_write_ts = null;
            return;
        }

        const row_count = self.memtable.row_count;
        const seg_id = self.manifest.nextSegmentId();

        var name_buf: [32]u8 = undefined;
        const file_name = try std.fmt.bufPrint(&name_buf, "{d:0>20}.dat", .{seg_id});

        var snapshot = try self.memtable.buildSortedSnapshot(self.allocator, self.order_key_indices);
        defer snapshot.deinit();

        var info = try storage.writeSegment(
            self.allocator,
            self.io,
            self.segments_dir,
            file_name,
            self.schema,
            seg_id,
            self.schema_fingerprint,
            self.row_group_size,
            snapshot.views,
        );
        defer info.deinit(self.allocator);

        try self.manifest.appendSegment(.{ .segment_id = seg_id, .row_count = row_count });
        try storage.writeManifest(self.io, self.table_dir, self.manifest);

        self.memtable.clear();
        self.first_write_ts = null;
    }

    /// Called from `insert`/`delete` after mutation. Flushes the memtable if
    /// any auto-flush trigger is satisfied.
    fn maybeAutoFlush(self: *Table) !void {
        if (self.memtable.isEmpty()) return;
        const rows = self.memtable.row_count;
        const bytes = self.memtable.byteSize();

        if (rows >= self.auto_flush_rows or bytes >= self.auto_flush_bytes) {
            try self.flush();
            return;
        }

        if (self.auto_flush_secs > 0) {
            if (self.first_write_ts) |start| {
                const now_ts = Io.Clock.awake.now(self.io);
                const elapsed_ns: u64 = @intCast(start.durationTo(now_ts).toNanoseconds());
                const threshold_ns: u64 = @as(u64, self.auto_flush_secs) * std.time.ns_per_s;
                if (elapsed_ns >= threshold_ns and
                    rows >= self.auto_flush_min_rows and
                    bytes >= self.auto_flush_min_bytes)
                {
                    try self.flush();
                }
            }
        }
    }

    pub fn segmentCount(self: Table) usize {
        return self.manifest.segments.items.len;
    }

    pub fn segmentFileName(buf: []u8, seg_id: u64) ![]u8 {
        return std.fmt.bufPrint(buf, "{d:0>20}.dat", .{seg_id});
    }

    /// Delete every row matching `pred`. Scans all segments + the memtable,
    /// emits tombstones for matches in segments, and rebuilds the memtable
    /// without them. Returns the number of rows deleted.
    pub fn delete(self: *Table, pred: exec.Predicate) !usize {
        return execDelete(self, pred);
    }

    /// Merge all segments into a single new segment. Drops tombstoned rows.
    /// No-op if there's at most one segment.
    pub fn compact(self: *Table) !void {
        if (self.manifest.segments.items.len <= 1) return;
        try execCompact(self);
    }

    fn deleteSegmentFiles(self: *Table, seg_id: u64) !void {
        var dat_buf: [32]u8 = undefined;
        const dat_name = try Table.segmentFileName(&dat_buf, seg_id);
        self.segments_dir.deleteFile(self.io, dat_name) catch {};

        var tomb_buf: [32]u8 = undefined;
        const tomb_name = try storage.tombstone.fileNameFor(&tomb_buf, seg_id);
        self.segments_dir.deleteFile(self.io, tomb_name) catch {};
    }
};

fn execDelete(t: *Table, pred: exec.Predicate) !usize {
    const col_idx = t.schema.columnIndex(pred.col) orelse return exec.Error.ColumnNotFound;
    const col_type = t.schema.columns[col_idx].type;
    if (ValueTag.fromType(col_type) != std.meta.activeTag(pred.val)) {
        return exec.Error.PredicateTypeMismatch;
    }
    // v0.2: only eq/neq on strings
    if (col_type.isString() and pred.op != .eq and pred.op != .neq) {
        return exec.Error.UnsupportedOperatorForType;
    }

    var total: usize = 0;

    // ---- Segments ----
    for (t.manifest.segments.items) |entry| {
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
        var seg = try storage.readSegment(t.allocator, t.io, t.segments_dir, file_name, t.schema);
        defer seg.deinit();

        var deleted: std.ArrayList(u32) = .empty;
        defer deleted.deinit(t.allocator);

        var row_offset: u32 = 0;
        for (seg.info.row_groups, 0..) |rg, rg_idx| {
            // Quick prune via stats for fixed-width columns.
            if (!col_type.isString() and !exec.statsOverlapPredicate(rg.stats[col_idx], pred.op, pred.val)) {
                row_offset += rg.row_count;
                continue;
            }

            var col = try seg.decodeColumn(t.allocator, t.schema, rg_idx, col_idx);
            defer col.deinit(t.allocator);

            const n = rg.row_count;
            var i: u32 = 0;
            while (i < n) : (i += 1) {
                if (evalRow(col.view(), i, pred)) {
                    try deleted.append(t.allocator, row_offset + i);
                }
            }
            row_offset += rg.row_count;
        }

        if (deleted.items.len > 0) {
            try storage.tombstone.merge(
                t.allocator,
                t.io,
                t.segments_dir,
                entry.segment_id,
                deleted.items,
            );
            total += deleted.items.len;
        }
    }

    // ---- Memtable ----
    if (t.memtable.row_count > 0) {
        const n: usize = @intCast(t.memtable.row_count);
        const keep = try t.allocator.alloc(bool, n);
        defer t.allocator.free(keep);
        const view = t.memtable.columns[col_idx].view();
        for (0..n) |i| keep[i] = !evalRow(view, @intCast(i), pred);

        const kept = try t.memtable.retainRows(keep);
        total += n - kept;
    }

    return total;
}

/// Merge all segments into a single new segment, dropping tombstoned rows.
fn execCompact(t: *Table) !void {
    var work = try engine.Memtable.init(t.allocator, t.schema);
    defer work.deinit();

    // Walk every segment, decode every row group, apply tombstones, append.
    for (t.manifest.segments.items) |entry| {
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
        var seg = try storage.readSegment(t.allocator, t.io, t.segments_dir, file_name, t.schema);
        defer seg.deinit();

        const tombs = try storage.tombstone.read(t.allocator, t.io, t.segments_dir, entry.segment_id);
        defer if (tombs) |x| t.allocator.free(x);

        var row_offset: u32 = 0;
        for (seg.info.row_groups, 0..) |rg, rg_idx| {
            // Decode all columns for this row group.
            const decoded = try t.allocator.alloc(storage.OwnedColumn, t.schema.columns.len);
            defer t.allocator.free(decoded);

            var decoded_inited: usize = 0;
            defer for (decoded[0..decoded_inited]) |*c| c.deinit(t.allocator);

            for (t.schema.columns, 0..) |_, ci| {
                decoded[ci] = try seg.decodeColumn(t.allocator, t.schema, rg_idx, ci);
                decoded_inited += 1;
            }

            // Build keep mask using tombstones (if any).
            const mask = try t.allocator.alloc(bool, rg.row_count);
            defer t.allocator.free(mask);
            @memset(mask, true);
            if (tombs) |arr| {
                const rg_end = row_offset + rg.row_count;
                const lo = std.sort.lowerBound(u32, arr, row_offset, cmpU32);
                const hi = std.sort.lowerBound(u32, arr, rg_end, cmpU32);
                for (arr[lo..hi]) |off| {
                    mask[off - row_offset] = false;
                }
            }

            var kept: usize = 0;
            for (mask) |m| if (m) {
                kept += 1;
            };

            if (kept > 0) {
                for (work.columns, 0..) |*dst, ci| {
                    try engine.memtable.appendMaskedColumn(t.allocator, decoded[ci].view(), mask, dst);
                }
                work.row_count += @intCast(kept);
            }

            row_offset += rg.row_count;
        }
    }

    // Capture old IDs for cleanup.
    var old_ids: std.ArrayList(u64) = .empty;
    defer old_ids.deinit(t.allocator);
    for (t.manifest.segments.items) |e| try old_ids.append(t.allocator, e.segment_id);

    if (work.row_count == 0) {
        // Everything was tombstoned. Just empty the manifest + delete files.
        t.manifest.segments.clearRetainingCapacity();
        try storage.writeManifest(t.io, t.table_dir, t.manifest);
        for (old_ids.items) |old| try t.deleteSegmentFiles(old);
        return;
    }

    // Sort + write as a new segment.
    const new_seg_id = t.manifest.nextSegmentId();
    var name_buf: [32]u8 = undefined;
    const file_name = try Table.segmentFileName(&name_buf, new_seg_id);

    var snapshot = try work.buildSortedSnapshot(t.allocator, t.order_key_indices);
    defer snapshot.deinit();

    var info = try storage.writeSegment(
        t.allocator,
        t.io,
        t.segments_dir,
        file_name,
        t.schema,
        new_seg_id,
        t.schema_fingerprint,
        t.row_group_size,
        snapshot.views,
    );
    defer info.deinit(t.allocator);

    // Atomically swap the manifest contents.
    const new_row_count: u64 = work.row_count;
    t.manifest.segments.clearRetainingCapacity();
    try t.manifest.appendSegment(.{ .segment_id = new_seg_id, .row_count = new_row_count });
    try storage.writeManifest(t.io, t.table_dir, t.manifest);

    // Clean up old segment + tomb files.
    for (old_ids.items) |old| try t.deleteSegmentFiles(old);
}

fn cmpU32(target: u32, item: u32) std.math.Order {
    return std.math.order(target, item);
}

/// Implements the StarRocks-style upsert semantics on unique-key tables.
/// After `insertRows`, every newly-inserted row whose order key already
/// exists somewhere in the table (older memtable row, or a flushed segment)
/// causes the older copy to be tombstoned. Always keeps the LAST occurrence
/// in the memtable.
fn applyUpsertResolution(t: *Table) !void {
    std.debug.assert(t.order_key_indices.len > 0);

    var arena = std.heap.ArenaAllocator.init(t.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    const n: usize = @intCast(t.memtable.row_count);
    if (n == 0) return;

    // ---- 1. Intra-memtable dedup: keep only the last occurrence per key. --
    const keep = try t.allocator.alloc(bool, n);
    defer t.allocator.free(keep);
    @memset(keep, true);

    var last_seen: std.StringHashMapUnmanaged(u32) = .empty;
    // Owned by arena; no explicit deinit needed.

    for (0..n) |i| {
        const key_bytes = try compoundKeyFromColumnStores(aa, t.memtable.columns, t.order_key_indices, @intCast(i));
        const gop = try last_seen.getOrPut(aa, key_bytes);
        if (gop.found_existing) keep[gop.value_ptr.*] = false;
        gop.value_ptr.* = @intCast(i);
    }

    _ = try t.memtable.retainRows(keep);

    // ---- 2. Build a set of surviving keys to probe segments with. --------
    const surviving_n: usize = @intCast(t.memtable.row_count);
    if (surviving_n == 0 or t.manifest.segments.items.len == 0) return;

    var surviving_set: std.StringHashMapUnmanaged(void) = .empty;
    try surviving_set.ensureTotalCapacity(aa, @intCast(surviving_n));
    for (0..surviving_n) |i| {
        const key_bytes = try compoundKeyFromColumnStores(aa, t.memtable.columns, t.order_key_indices, @intCast(i));
        surviving_set.putAssumeCapacity(key_bytes, {});
    }

    // ---- 3. For each segment, scan row groups, find matching keys. --------
    for (t.manifest.segments.items) |entry| {
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
        var seg = try storage.readSegment(t.allocator, t.io, t.segments_dir, file_name, t.schema);
        defer seg.deinit();

        var deleted: std.ArrayList(u32) = .empty;
        defer deleted.deinit(t.allocator);

        var row_offset: u32 = 0;
        for (seg.info.row_groups, 0..) |rg, rg_idx| {
            // For all-scalar order keys, we *could* use per-column min/max to
            // prune. For compound keys with any string component, we'd need
            // string stats (not in v0.3). For simplicity in v0.3, always scan
            // the row group's key columns. Optimize later.

            // Decode all order-key columns for this row group.
            const decoded_keys = try aa.alloc(storage.OwnedColumn, t.order_key_indices.len);
            for (t.order_key_indices, 0..) |col_idx, i| {
                decoded_keys[i] = try seg.decodeColumn(t.allocator, t.schema, rg_idx, col_idx);
            }
            defer for (decoded_keys) |*c| {
                var d = c.*;
                d.deinit(t.allocator);
            };

            const rg_n = rg.row_count;
            var row: u32 = 0;
            while (row < rg_n) : (row += 1) {
                const key_bytes = try compoundKeyFromOwnedColumns(aa, decoded_keys, row);
                if (surviving_set.contains(key_bytes)) {
                    try deleted.append(t.allocator, row_offset + row);
                }
            }
            row_offset += rg.row_count;
        }

        if (deleted.items.len > 0) {
            try storage.tombstone.merge(
                t.allocator,
                t.io,
                t.segments_dir,
                entry.segment_id,
                deleted.items,
            );
        }
    }
}

/// Pack the order-key columns of `row` from a memtable's `ColumnStore` array
/// into a contiguous byte slice suitable for hashing/comparison. Allocated
/// in `aa`; lifetime = arena.
///
/// Layout per column:
///   - INT (i32):    4 bytes LE
///   - BIGINT (i64): 8 bytes LE
///   - BOOLEAN (u8): 1 byte
///   - VARCHAR/STRING: 4-byte LE length + bytes
fn compoundKeyFromColumnStores(
    aa: std.mem.Allocator,
    columns: []const engine.ColumnStore,
    key_indices: []const usize,
    row: u32,
) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    for (key_indices) |ci| {
        try appendColumnValueBytes(aa, &buf, columns[ci].view(), row);
    }
    return buf.toOwnedSlice(aa);
}

/// Same as above but for the per-row-group decoded `OwnedColumn` array used
/// during segment scans.
fn compoundKeyFromOwnedColumns(
    aa: std.mem.Allocator,
    decoded: []storage.OwnedColumn,
    row: u32,
) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    for (decoded) |c| {
        try appendColumnValueBytes(aa, &buf, c.view(), row);
    }
    return buf.toOwnedSlice(aa);
}

fn appendColumnValueBytes(
    aa: std.mem.Allocator,
    buf: *std.ArrayList(u8),
    view: storage.ColumnView,
    row: u32,
) !void {
    switch (view.data) {
        .int => |s| try storage.format.appendI32(aa, buf, s[row]),
        .bigint => |s| try storage.format.appendI64(aa, buf, s[row]),
        .boolean => |s| try buf.append(aa, s[row]),
        .varchar => |sv| {
            const bytes = sv.rowBytes(row);
            try storage.format.appendU32(aa, buf, @intCast(bytes.len));
            try buf.appendSlice(aa, bytes);
        },
        .string => |sv| {
            const bytes = sv.rowBytes(row);
            try storage.format.appendU32(aa, buf, @intCast(bytes.len));
            try buf.appendSlice(aa, bytes);
        },
    }
}

fn evalRow(view: storage.ColumnView, row: u32, pred: exec.Predicate) bool {
    return switch (view.data) {
        .int => |s| cmpVal(i32, s[row], pred.val.int, pred.op),
        .bigint => |s| cmpVal(i64, s[row], pred.val.bigint, pred.op),
        .boolean => |s| cmpVal(u8, s[row], @intFromBool(pred.val.boolean), pred.op),
        .varchar => |sv| cmpStr(sv.rowBytes(row), pred.val.text, pred.op),
        .string => |sv| cmpStr(sv.rowBytes(row), pred.val.text, pred.op),
    };
}

fn cmpVal(comptime T: type, a: T, b: T, op: exec.PredicateOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .lte => a <= b,
        .gt => a > b,
        .gte => a >= b,
    };
}

fn cmpStr(a: []const u8, b: []const u8, op: exec.PredicateOp) bool {
    const eq = std.mem.eql(u8, a, b);
    return switch (op) {
        .eq => eq,
        .neq => !eq,
        else => unreachable, // pre-validated
    };
}

/// Either loads the persisted schema, validates against the caller-provided
/// one, or — when no schema.bin exists — clones the caller schema and
/// persists it.
fn acquireSchema(
    allocator: Allocator,
    io: Io,
    table_dir: Io.Dir,
    maybe_schema: ?Schema,
) !storage.schema_file.SchemaOwner {
    if (storage.schema_file.readSchema(allocator, io, table_dir)) |loaded| {
        if (maybe_schema) |s| {
            if (!storage.schema_file.schemasEqual(s, loaded.view())) {
                var owner = loaded;
                owner.deinit();
                return storage.schema_file.Error.SchemaMismatch;
            }
        }
        return loaded;
    } else |err| switch (err) {
        error.FileNotFound => {
            const s = maybe_schema orelse return storage.schema_file.Error.SchemaRequired;
            try s.validate();
            var owner = try storage.schema_file.SchemaOwner.clone(allocator, s);
            errdefer owner.deinit();
            try storage.schema_file.writeSchema(io, table_dir, s, allocator);
            return owner;
        },
        else => return err,
    }
}

/// Validate that the schema's order key is supported for unique upsert.
/// v0.3: any compound key built from scalar types (`INT`, `BIGINT`, `BOOLEAN`,
/// `VARCHAR(N)`, `STRING`) is allowed. No-op currently — all supported types
/// pass.
fn validateUniqueKey(schema: Schema) !void {
    if (schema.order_key.len == 0) return Error.UnsupportedUniqueKeyType;
    for (schema.order_key) |k| {
        const idx = schema.columnIndex(k) orelse return Error.SchemaMismatch;
        _ = idx;
        // All v0.3 column types are supported in compound keys.
    }
}

/// Stable hash of a schema's column names, types, order key, and unique flag.
/// Used to detect schema drift when reopening a table.
pub fn schemaFingerprint(schema: Schema) u64 {
    var hasher = std.hash.Wyhash.init(0);
    for (schema.columns) |c| {
        hasher.update(c.name);
        const tag: u8 = @intFromEnum(@as(TypeTag, c.type));
        hasher.update(&[_]u8{tag});
        hasher.update(&[_]u8{@intFromBool(c.nullable)});
        switch (c.type) {
            .varchar => |n| {
                var b: [4]u8 = undefined;
                std.mem.writeInt(u32, &b, n, .little);
                hasher.update(&b);
            },
            else => {},
        }
    }
    for (schema.order_key) |k| hasher.update(k);
    hasher.update(&[_]u8{@intFromBool(schema.unique)});
    return hasher.final();
}

test {
    _ = @import("api_test.zig");
}
