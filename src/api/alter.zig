//! ALTER TABLE orchestration. Per DESIGN.md §9.2: derive the new schema
//! from the ops, create a shadow directory next to the table, stream
//! every row group of every segment through the projection, atomically
//! rename the shadow into place, then re-init the Table's in-memory state.
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
const TypeTag = types.TypeTag;
const ValueTag = types.ValueTag;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const api = @import("api.zig");
const NsSchema = api.Schema;
const Table = api.Table;
const AlterOp = api.AlterOp;

/// Source of data for one column of the new schema.
const ColumnSource = union(enum) {
    /// Carry data from this column index in the OLD schema (rename = same
    /// index with a renamed slot; drop = removed; keep = unchanged).
    from_old: usize,
    /// Synthesize N rows of this default value (add case).
    add_with_default: Value,
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
        try cols.append(aa, .{ .name = name_copy, .type = c.type, .nullable = c.nullable });
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
            if (!valueTagMatchesType(add.default, add.type)) return api.Error.UnsupportedAlterOp;
            const name_copy = try aa.dupe(u8, add.name);
            // If the default is a string-like, copy its bytes into the arena
            // so the plan owns them.
            const default_owned: Value = switch (add.default) {
                .text => |s| Value{ .text = try aa.dupe(u8, s) },
                else => add.default,
            };
            try cols.append(aa, .{ .name = name_copy, .type = add.type, .nullable = add.nullable });
            try sources.append(aa, .{ .add_with_default = default_owned });
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
        .varchar, .string, .char => vt == .text,
    };
}

/// Shadow-rewrite the table. Holds `ddl_lock` exclusive AND `table.mutex`
/// for the duration — blocks readers (waiting on in-flight scans),
/// writers (via the existing mutex), and any other DDL.
pub fn execAlter(s: *NsSchema, t: *Table, ops: []const AlterOp) !void {
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

    // 2. Create (or recreate, after a prior failed alter) the shadow directory.
    var shadow_name_buf: [256]u8 = undefined;
    const shadow_name = try std.fmt.bufPrint(&shadow_name_buf, "__alter_{s}", .{t.name});
    s.schema_dir.deleteTree(t.io, shadow_name) catch {};

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
    const new_has_stats = try @import("table.zig").buildColumnHasStats(t.allocator, new_schema);
    defer t.allocator.free(new_has_stats);
    for (t.manifest.segments.items) |entry| {
        const info = try rewriteSegment(t, &plan, shadow_segs, entry, new_schema, new_fp, sync);
        defer info.deinit(t.allocator);
        try new_manifest.appendSegment(
            try storage.manifest.entryFromSegmentInfo(t.allocator, info, new_lk_idx, new_has_stats),
        );
    }

    // 4. Write new schema and manifest into the shadow.
    try storage.schema_file.writeSchema(t.io, shadow_dir, new_schema, t.allocator);
    try storage.writeManifest(t.io, shadow_dir, new_manifest, sync);

    // 5. Swap on disk: close current + shadow handles, delete original tree,
    //    rename shadow into place.
    t.segments_dir.close(t.io);
    t.table_dir.close(t.io);
    shadow_segs.close(t.io);
    shadow_dir.close(t.io);
    shadow_open = false;

    try s.schema_dir.deleteTree(t.io, t.name);
    try s.schema_dir.rename(shadow_name, s.schema_dir, t.name, t.io);

    // 6. Re-open Table state under the new schema.
    try reInitTableState(s, t, new_fp);
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
        sync,
    );
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
    }
    if (schema_col.nullable) {
        var j: u32 = 0;
        while (j < n) : (j += 1) try out.appendValidBit(allocator, start_row + j, true);
    }
}

/// Re-initialize the Table after the on-disk swap. Reopens dir handles,
/// reloads schema + manifest, replaces the memtable with a fresh one
/// matching the new schema. Caller holds `table.mutex`.
fn reInitTableState(s: *NsSchema, t: *Table, new_fp: u64) !void {
    const allocator = t.allocator;
    const io = t.io;

    t.table_dir = try s.schema_dir.openDir(io, t.name, .{});
    t.segments_dir = try t.table_dir.openDir(io, "segments", .{});

    var new_owner = try storage.schema_file.readSchema(allocator, io, t.table_dir);
    t.schema_owner.deinit();
    t.schema_owner = new_owner;
    t.schema = new_owner.view();
    t.schema_fingerprint = new_fp;

    const new_manifest = try storage.readManifest(allocator, io, t.table_dir, new_fp);
    t.manifest.deinit();
    t.manifest = new_manifest;

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
    const old_mt = t.memtable;
    t.memtable = new_mt;
    old_mt.retire();
    old_mt.release();
}
