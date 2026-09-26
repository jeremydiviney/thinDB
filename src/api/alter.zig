//! ALTER TABLE orchestration. Per DESIGN.md §9.2: derive the new schema
//! from the ops, create a shadow directory next to the table, stream
//! every row group of every segment through the projection, set the
//! original aside, rename the shadow into place, delete the original, then
//! re-init the Table's in-memory state.
//!
//! Writers are paused for the entire duration (we hold `table.mutex`).
//! Active scans hold their own refcounted memtable snapshot, so they
//! continue to see the pre-alter state in the memtable — BUT the
//! Table's segment files get replaced under them; readers that haven't
//! captured all the segment data they need will fail when they hit a
//! changed segment. Caller responsibility: no active scans during ALTER.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const types = @import("../types.zig");
const Type = types.Type;
const Value = types.Value;
const Column = types.Column;
const TableSchema = types.TableSchema;
const ValueTag = types.ValueTag;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const api = @import("api.zig");
const NsSchema = api.Schema;
const Table = api.Table;
const AlterOp = api.AlterOp;

/// Directory names under this prefix belong to an ALTER swap: no table is
/// created, renamed, listed or opened under one.
pub const reserved_table_prefix = "__alter_";
/// The rewritten table, and the original set aside while the rewrite takes
/// its name. Neither prefix starts the other, so one table's shadow never
/// names another table's aside.
const shadow_prefix = reserved_table_prefix ++ "new_";
const aside_prefix = reserved_table_prefix ++ "old_";

pub fn isReservedTableName(name: []const u8) bool {
    return std.mem.startsWith(u8, name, reserved_table_prefix);
}

/// Delete the shadow and the aside an ALTER of `table_name` may have left.
pub fn deleteAlterLeftovers(io: Io, schema_dir: Io.Dir, table_name: []const u8) !void {
    var name_buf: [256]u8 = undefined;
    inline for (.{ shadow_prefix, aside_prefix }) |prefix| {
        const name = try std.fmt.bufPrint(&name_buf, prefix ++ "{s}", .{table_name});
        try storage.retryTransientWindowsRefusal(io, Io.Dir.deleteTree, .{ schema_dir, io, name });
    }
}

/// Resolve the ALTER swaps a crash or a persistent refusal interrupted in
/// `schema_dir`. A swap commits when its shadow takes the table's name, so
/// an interrupted one rolls back: an aside returns to its table's name, and
/// a shadow is deleted once its table stands. Runs as the schema opens,
/// before any of its tables does.
pub fn recoverInterruptedAlters(allocator: Allocator, io: Io, schema_dir: Io.Dir) !void {
    var names: std.ArrayList([]u8) = .empty;
    defer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }
    // Collected first: the fixes below rename and delete entries of the
    // directory being walked.
    var it = schema_dir.iterate();
    while (try it.next(io)) |entry| {
        if (entry.kind != .directory or !isReservedTableName(entry.name)) continue;
        const name = try allocator.dupe(u8, entry.name);
        errdefer allocator.free(name);
        try names.append(allocator, name);
    }
    for (names.items) |name| {
        if (!std.mem.startsWith(u8, name, aside_prefix)) continue;
        const table_name = name[aside_prefix.len..];
        if (try dirExists(io, schema_dir, table_name)) continue;
        try storage.retryTransientWindowsRefusal(io, Io.Dir.rename, .{ schema_dir, name, schema_dir, table_name, io });
    }
    for (names.items) |name| {
        if (std.mem.startsWith(u8, name, aside_prefix)) {
            if (try dirExists(io, schema_dir, name)) try deleteLeftover(io, schema_dir, name);
            continue;
        }
        // A bare `__alter_<name>` predates the aside step. That swap deleted
        // the original before renaming the shadow in, so a crash between the
        // two left a complete shadow as the table's only copy.
        const table_name = name[if (std.mem.startsWith(u8, name, shadow_prefix)) shadow_prefix.len else reserved_table_prefix.len..];
        if (!try dirExists(io, schema_dir, table_name) and try hasManifest(io, schema_dir, name)) {
            try storage.retryTransientWindowsRefusal(io, Io.Dir.rename, .{ schema_dir, name, schema_dir, table_name, io });
        } else {
            try deleteLeftover(io, schema_dir, name);
        }
    }
}

// A leftover the restored table no longer needs; one that stays only costs
// disk until the next open, so a refusal must not keep the schema closed.
fn deleteLeftover(io: Io, schema_dir: Io.Dir, name: []const u8) !void {
    storage.retryTransientWindowsRefusal(io, Io.Dir.deleteTree, .{ schema_dir, io, name }) catch |err| switch (err) {
        error.AccessDenied => {},
        else => return err,
    };
}

fn dirExists(io: Io, parent: Io.Dir, name: []const u8) !bool {
    const dir = parent.openDir(io, name, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    dir.close(io);
    return true;
}

fn hasManifest(io: Io, parent: Io.Dir, name: []const u8) !bool {
    const dir = try parent.openDir(io, name, .{});
    defer dir.close(io);
    dir.access(io, storage.manifest.manifest_filename, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

/// Source of data for one column of the new schema.
const ColumnSource = union(enum) {
    /// Carry data from this column index in the OLD schema (rename = same
    /// index with a renamed slot; drop = removed; keep = unchanged).
    from_old: usize,
    /// Synthesize N rows of this default value (add case).
    add_with_default: Value,
    /// Synthesize N SQL NULL rows for a newly-added nullable column.
    add_null,
};

/// Resolved plan: the new column list + how each one is populated.
/// All allocated memory is owned by `arena`.
pub const AlterPlan = struct {
    arena: std.heap.ArenaAllocator,
    new_columns: []Column,
    new_order_key: [][]const u8,
    new_unique: bool,
    sources: []ColumnSource,

    pub fn deinit(self: *AlterPlan) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn newSchema(self: *const AlterPlan) TableSchema {
        return .{
            .columns = self.new_columns,
            .order_key = self.new_order_key,
            .unique = self.new_unique,
        };
    }
};

/// Apply ops to old schema, return the resolved plan. Validates as it goes:
///   - No duplicate column names.
///   - No dropping a column that's part of the order key.
///   - `add` default tag matches the new column's type.
pub fn planAlter(parent_allocator: Allocator, old: TableSchema, ops: []const AlterOp) !AlterPlan {
    var arena = std.heap.ArenaAllocator.init(parent_allocator);
    errdefer arena.deinit();
    const aa = arena.allocator();

    // Working state, built up by applying ops in order.
    var cols: std.ArrayList(Column) = .empty;
    var sources: std.ArrayList(ColumnSource) = .empty;

    // Seed from the old schema: each column carries its data forward.
    for (old.columns, 0..) |c, idx| {
        const name_copy = try aa.dupe(u8, c.name);
        try cols.append(aa, .{
            .name = name_copy,
            .type = c.type,
            .nullable = c.nullable,
            .default_value = if (c.default_value) |v| try cloneValue(aa, v) else null,
            .default_now = c.default_now,
            .auto_increment = c.auto_increment,
        });
        try sources.append(aa, .{ .from_old = idx });
    }

    for (ops) |op| switch (op) {
        .rename => |r| {
            const idx = findColumn(cols.items, r.from) orelse return api.Error.ColumnNotFound;
            if (findColumn(cols.items, r.to) != null) return api.Error.ColumnAlreadyExists;
            cols.items[idx].name = try aa.dupe(u8, r.to);
        },
        .drop => |name| {
            const idx = findColumn(cols.items, name) orelse return api.Error.ColumnNotFound;
            // Forbid dropping any column that's part of the order key.
            for (old.order_key) |key| {
                if (std.mem.eql(u8, key, name)) return api.Error.UnsupportedAlterOp;
            }
            _ = cols.orderedRemove(idx);
            _ = sources.orderedRemove(idx);
        },
        .add => |add| {
            if (findColumn(cols.items, add.name) != null) return api.Error.ColumnAlreadyExists;
            if (add.default) |default| {
                if (!valueTagMatchesType(default, add.type)) return api.Error.UnsupportedAlterOp;
            } else if (!add.nullable) {
                return api.Error.UnsupportedAlterOp;
            }
            const name_copy = try aa.dupe(u8, add.name);
            const default_owned = if (add.default) |v| try cloneValue(aa, v) else null;
            try cols.append(aa, .{
                .name = name_copy,
                .type = add.type,
                .nullable = add.nullable,
                .default_value = default_owned,
            });
            try sources.append(aa, if (default_owned) |v| .{ .add_with_default = v } else .add_null);
        },
    };

    // Translate the order key: each entry must still refer to a still-extant
    // column (we forbade dropping order-key cols), under whatever its name
    // is NOW (rename may have changed it).
    var new_ok = try aa.alloc([]const u8, old.order_key.len);
    for (old.order_key, 0..) |old_key, i| {
        const old_idx = old.columnIndex(old_key) orelse return api.Error.ColumnNotFound;
        var found: ?usize = null;
        for (sources.items, 0..) |s, j| switch (s) {
            .from_old => |oi| if (oi == old_idx) {
                found = j;
                break;
            },
            else => {},
        };
        const pos = found orelse return api.Error.ColumnNotFound;
        new_ok[i] = cols.items[pos].name;
    }

    const owned_cols = try cols.toOwnedSlice(aa);
    const owned_sources = try sources.toOwnedSlice(aa);

    const schema_view: TableSchema = .{
        .columns = owned_cols,
        .order_key = new_ok,
        .unique = old.unique,
    };
    schema_view.validate() catch return api.Error.SchemaMismatch;

    return .{
        .arena = arena,
        .new_columns = owned_cols,
        .new_order_key = new_ok,
        .new_unique = old.unique,
        .sources = owned_sources,
    };
}

fn findColumn(cols: []const Column, name: []const u8) ?usize {
    for (cols, 0..) |c, i| {
        if (@import("../types.zig").columnNameEql(c.name, name)) return i;
    }
    return null;
}

fn valueTagMatchesType(v: Value, t: Type) bool {
    const vt: ValueTag = v;
    return switch (t) {
        .int => vt == .int,
        .bigint => vt == .bigint,
        .boolean => vt == .boolean,
        .float => vt == .float,
        .double => vt == .double,
        .date => vt == .date,
        .datetime => vt == .datetime,
        .tinyint => vt == .tinyint,
        .smallint => vt == .smallint,
        .largeint => vt == .largeint,
        .decimal64 => vt == .decimal64,
        .decimal128 => vt == .decimal128,
        .uuid => vt == .uuid,
        .varchar, .string, .char, .json => vt == .text,
    };
}

fn cloneValue(allocator: Allocator, v: Value) !Value {
    return switch (v) {
        .text => |s| Value{ .text = try allocator.dupe(u8, s) },
        else => v,
    };
}

/// Shadow-rewrite the table. Holds `compact_lock` (so it waits out an
/// in-flight compaction, which rewrites the same segment files under no
/// ddl_lock during its merge), then `ddl_lock` exclusive AND `table.mutex`
/// for the duration — blocks readers (waiting on in-flight scans),
/// writers (via the existing mutex), and any other DDL.
pub fn execAlter(s: *NsSchema, t: *Table, ops: []const AlterOp) !void {
    t.compact_lock.lockUncancelable(t.io);
    defer t.compact_lock.unlock(t.io);
    t.ddl_lock.lockUncancelable(t.io);
    defer t.ddl_lock.unlock(t.io);
    t.mutex.lockUncancelable(t.io);
    defer t.mutex.unlock(t.io);

    var plan = try planAlter(t.allocator, t.schema, ops);
    defer plan.deinit();
    const new_schema = plan.newSchema();
    const new_fp = api.schemaFingerprint(new_schema);

    // 1. Flush the active memtable so all live data is in segments.
    try t.flushLocked();

    // 2. Create the shadow directory, clearing what a failed alter left.
    try deleteAlterLeftovers(t.io, s.schema_dir, t.name);
    var shadow_name_buf: [256]u8 = undefined;
    const shadow_name = try std.fmt.bufPrint(&shadow_name_buf, shadow_prefix ++ "{s}", .{t.name});
    var aside_name_buf: [256]u8 = undefined;
    const aside_name = try std.fmt.bufPrint(&aside_name_buf, aside_prefix ++ "{s}", .{t.name});

    var shadow_dir = try s.schema_dir.createDirPathOpen(t.io, shadow_name, .{});
    var shadow_segs = try shadow_dir.createDirPathOpen(t.io, "segments", .{});
    // After we close + rename below we don't want defer to double-close,
    // so close explicitly when done and set flags.
    var shadow_open = true;
    defer if (shadow_open) {
        shadow_segs.close(t.io);
        shadow_dir.close(t.io);
    };

    // 3. Rewrite every segment under the new schema.
    var new_manifest = storage.Manifest.empty(t.allocator, new_fp, @intCast(new_schema.columns.len));
    defer new_manifest.deinit();

    const sync = t.syncEnabled();
    const new_lk_idx: ?usize = if (new_schema.order_key.len > 0)
        new_schema.columnIndex(new_schema.order_key[0]) orelse return api.Error.SchemaMismatch
    else
        null;
    for (t.manifest.segments.items) |entry| {
        const info = try rewriteSegment(t, &plan, shadow_segs, entry, new_schema, new_fp, sync);
        defer info.deinit(t.allocator);
        try carrySidecars(t, shadow_segs, entry.segment_id, sync);
        try new_manifest.appendSegment(
            try storage.manifest.entryFromSegmentInfo(t.allocator, info, new_lk_idx, new_schema.columns),
        );
    }

    // 4. Write new schema and manifest into the shadow, manifest last:
    //    recovery reads a manifest as a complete shadow.
    try storage.schema_file.writeSchema(t.io, shadow_dir, new_schema, t.allocator, sync);
    if (sync) try storage.syncDirectory(t.io, shadow_segs);
    try storage.writeManifest(t.io, shadow_dir, new_manifest, sync);

    // 5. Swap on disk: close current + shadow handles, set the original tree
    //    aside, rename the shadow into place, delete the original. The WAL
    //    file lives in the original tree, so the writer is closed before the
    //    swap and recreated in reInitTableState. A writer that outlives the
    //    swap keeps a stale dir handle whose fd number gets reused, and its
    //    next truncate lands in whatever directory owns that number by then
    //    (2026-08-29: the segments dir — a log replay never looked at). Step 1
    //    flushed, so the log carries nothing live. Mirrors NsSchema.renameTable.
    const had_wal = t.wal != null;
    if (had_wal) {
        t.wal.?.deinit();
        t.wal = null;
    }
    t.segments_dir.close(t.io);
    t.table_dir.close(t.io);
    t.dirs_open = false;
    shadow_segs.close(t.io);
    shadow_dir.close(t.io);
    shadow_open = false;
    // Cached segment handles hold their files open, and Windows refuses to
    // rename a directory while a file inside it is open. Their parsed footers
    // embed the old schema anyway.
    t.seg_handles.clear(t.allocator);
    // Nothing on disk has moved yet, so a refusal to set the original aside
    // reopens it unchanged.
    storage.retryTransientWindowsRefusal(t.io, Io.Dir.rename, .{ s.schema_dir, t.name, s.schema_dir, aside_name, t.io }) catch |err| {
        reInitTableState(s, t, t.schema_fingerprint, had_wal) catch t.requireRecovery();
        return err;
    };
    // Past this point a failure leaves the tree mid-swap, which only
    // `recoverInterruptedAlters` resolves, so fence the table until reopen
    // instead of letting later operations reach through dead handles
    // (`close` skips them via dirs_open).
    errdefer t.requireRecovery();
    if (sync) storage.syncDirectory(t.io, s.schema_dir) catch return api.Error.DurabilityUncertain;
    // The swap commits here; until this rename lands, the next open puts the
    // original back.
    try storage.retryTransientWindowsRefusal(t.io, Io.Dir.rename, .{ s.schema_dir, shadow_name, s.schema_dir, t.name, t.io });
    if (sync) storage.syncDirectory(t.io, s.schema_dir) catch return api.Error.DurabilityUncertain;

    // 6. Re-open Table state under the new schema.
    try reInitTableState(s, t, new_fp, had_wal);
    // Committed: an original the delete leaves behind is only garbage, which
    // the next open of the schema or alter of the table removes.
    storage.retryTransientWindowsRefusal(t.io, Io.Dir.deleteTree, .{ s.schema_dir, t.io, aside_name }) catch {};
}

/// Build a new segment in `shadow_segs` carrying `entry`'s rows but reshaped
/// per `plan`. Decodes the old segment row group at a time, populating one
/// big ColumnStore per new column.
fn rewriteSegment(
    t: *Table,
    plan: *const AlterPlan,
    shadow_segs: Io.Dir,
    entry: storage.ManifestEntry,
    new_schema: TableSchema,
    new_fp: u64,
    sync: bool,
) !storage.format.SegmentInfo {
    var name_buf: [32]u8 = undefined;
    const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);

    var seg = try storage.readSegment(t.allocator, t.io, t.segments_dir, file_name, t.schema);
    defer seg.deinit();

    var new_stores = try t.allocator.alloc(ColumnStore, new_schema.columns.len);
    var inited: usize = 0;
    defer {
        for (new_stores[0..inited]) |*c| c.deinit(t.allocator);
        t.allocator.free(new_stores);
    }
    for (new_schema.columns, 0..) |c, i| {
        new_stores[i] = try ColumnStore.init(t.allocator, c.type, c.nullable);
        inited = i + 1;
    }

    for (seg.info.row_groups, 0..) |rg, rg_idx| {
        for (plan.sources, 0..) |src, new_idx| switch (src) {
            .from_old => |old_idx| {
                var decoded = try seg.decodeColumn(t.allocator, t.schema, rg_idx, old_idx);
                defer decoded.deinit(t.allocator);
                try engine.transform.appendAllColumn(t.allocator, decoded.view(), &new_stores[new_idx]);
            },
            .add_with_default => |val| {
                try fillDefault(t.allocator, &new_stores[new_idx], new_schema.columns[new_idx], val, rg.row_count);
            },
            .add_null => {
                try fillNull(t.allocator, &new_stores[new_idx], rg.row_count);
            },
        };
    }

    const new_views = try t.allocator.alloc(ColumnView, new_stores.len);
    defer t.allocator.free(new_views);
    for (new_stores, 0..) |c, i| new_views[i] = c.view();

    return try storage.writeSegment(
        t.allocator,
        t.io,
        shadow_segs,
        file_name,
        new_schema,
        entry.segment_id,
        new_fp,
        t.row_group_size,
        new_views,
        // No global view here: ALTER may add/drop columns, so old per-column
        // sketches don't align with the new schema. Gate on segment-local NDV
        // only (matches the prior per-block behaviour — no regression).
        &.{},
        sync,
        t.compact_threads,
    );
}

/// A segment's deletes (`.tomb`) and key filter (`.bloom`) live in
/// sidecars beside it, not in its rows. The rewrite keeps segment ids, row
/// order and key values, so both carry over byte for byte. Without the
/// tombstones every deleted or replaced row comes back.
fn carrySidecars(t: *Table, shadow_segs: Io.Dir, seg_id: u64, sync: bool) !void {
    var tomb_buf: [32]u8 = undefined;
    var bloom_buf: [32]u8 = undefined;
    const names = [_][]const u8{
        try storage.tombstone.fileNameFor(&tomb_buf, seg_id),
        try Table.segmentBloomFileName(&bloom_buf, seg_id),
    };
    for (names) |name| {
        const bytes = t.segments_dir.readFileAlloc(t.io, name, t.allocator, .unlimited) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        defer t.allocator.free(bytes);
        try storage.writeFileSynced(t.io, shadow_segs, name, bytes, sync);
    }
}

/// Append `n` copies of `val` to `out`. For a nullable column, also marks
/// all `n` rows as valid (the default IS a real value, not NULL).
fn fillDefault(
    allocator: Allocator,
    out: *ColumnStore,
    schema_col: Column,
    val: Value,
    n: u32,
) !void {
    const start_row = out.data.rowCount();
    var i: u32 = 0;
    switch (out.data) {
        .int => |*list| while (i < n) : (i += 1) try list.append(allocator, val.int),
        .bigint => |*list| while (i < n) : (i += 1) try list.append(allocator, val.bigint),
        .boolean => |*list| while (i < n) : (i += 1) try list.append(allocator, @intFromBool(val.boolean)),
        .tinyint => |*list| while (i < n) : (i += 1) try list.append(allocator, val.tinyint),
        .smallint => |*list| while (i < n) : (i += 1) try list.append(allocator, val.smallint),
        .largeint => |*list| while (i < n) : (i += 1) try list.append(allocator, val.largeint),
        .float => |*list| while (i < n) : (i += 1) try list.append(allocator, val.float),
        .double => |*list| while (i < n) : (i += 1) try list.append(allocator, val.double),
        .date => |*list| while (i < n) : (i += 1) try list.append(allocator, val.date),
        .datetime => |*list| while (i < n) : (i += 1) try list.append(allocator, val.datetime),
        .decimal64 => |*list| while (i < n) : (i += 1) try list.append(allocator, val.decimal64),
        .decimal128 => |*list| while (i < n) : (i += 1) try list.append(allocator, val.decimal128),
        .uuid => |*list| while (i < n) : (i += 1) try list.append(allocator, val.uuid),
        .varchar => |*ss| while (i < n) : (i += 1) try ss.appendValue(allocator, val.text),
        .string => |*ss| while (i < n) : (i += 1) try ss.appendValue(allocator, val.text),
        .char => |*ss| while (i < n) : (i += 1) try ss.appendValue(allocator, val.text),
        .json => |*ss| while (i < n) : (i += 1) try ss.appendValue(allocator, val.text),
    }
    if (schema_col.nullable) {
        var j: u32 = 0;
        while (j < n) : (j += 1) try out.appendValidBit(allocator, start_row + j, true);
    }
}

fn fillNull(
    allocator: Allocator,
    out: *ColumnStore,
    n: u32,
) !void {
    const start_row = out.data.rowCount();
    var i: u32 = 0;
    while (i < n) : (i += 1) try out.data.appendNullPlaceholder(allocator);
    var j: u32 = 0;
    while (j < n) : (j += 1) try out.appendValidBit(allocator, start_row + j, false);
}

/// Re-initialize the Table after the on-disk swap. Reopens dir handles,
/// reloads schema + manifest, replaces the memtable with a fresh one
/// matching the new schema. Caller holds `table.mutex`.
fn reInitTableState(s: *NsSchema, t: *Table, new_fp: u64, recreate_wal: bool) !void {
    const allocator = t.allocator;
    const io = t.io;

    t.table_dir = try s.schema_dir.openDir(io, t.name, .{});
    t.segments_dir = t.table_dir.openDir(io, "segments", .{}) catch |err| {
        t.table_dir.close(io);
        return err;
    };
    t.dirs_open = true;
    if (recreate_wal) {
        t.wal = try engine.wal.WalWriter.create(allocator, io, t.table_dir, new_fp);
    }

    var new_owner = try storage.schema_file.readSchema(allocator, io, t.table_dir);
    t.schema_owner.deinit();
    t.schema_owner = new_owner;
    t.schema = new_owner.view();
    t.schema_fingerprint = new_fp;

    const new_manifest = try storage.readManifest(allocator, io, t.table_dir, new_fp);
    t.manifest.deinit();
    t.manifest = new_manifest;
    t.loadKeyBloomSidecars();

    // The rewritten table restarts segment IDs and reshapes columns, so the
    // old generation's cached blocks must become unreachable: purge and move
    // to a fresh uid. execAlter already dropped the parsed footers.
    t.cache.purgeTable(t.cache_uid);
    t.cache_uid = storage.cache.newTableUid();

    const new_indices = try allocator.alloc(usize, t.schema.order_key.len);
    for (t.schema.order_key, 0..) |k, i| {
        new_indices[i] = t.schema.columnIndex(k) orelse return api.Error.SchemaMismatch;
    }
    allocator.free(t.order_key_indices);
    t.order_key_indices = new_indices;

    // Fresh empty memtable matching the new schema. Old one is retired —
    // any pinned scan continues to read pre-alter rows (which match the
    // pre-alter schema; consumers that captured a scan before ALTER must
    // finish or be discarded before the schema mismatch matters).
    const new_mt = try engine.Memtable.create(allocator, t.schema);
    t.installMemtableLocked(new_mt);
}
