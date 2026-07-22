//! FileScan materializes a single external file into normal columnar batches.
//! It is intentionally an execution source, not a storage load path: CTAS and
//! INSERT ... SELECT persist rows by composing this source with existing sinks.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const api = @import("../api/api.zig");
const ir = @import("../ir/ir.zig");
const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const exec = @import("exec.zig");
const Batch = exec.Batch;
const Query = exec.Query;

pub const FileScan = struct {
    allocator: Allocator,
    schema: []Column = &.{},
    stores: []ColumnStore = &.{},
    views: []ColumnView = &.{},
    stats_buf: []exec.ColStat = &.{},
    row_count: usize = 0,
    emitted: bool = false,

    pub fn create(
        allocator: Allocator,
        io: Io,
        access: api.FileScanAccess,
        spec: ir.FileScan,
        needed: ?[]const []const u8,
    ) !Query {
        const self = try allocator.create(FileScan);
        self.* = .{ .allocator = allocator };
        errdefer {
            self.deinit();
        }

        var csv_options = spec.options.csv;
        const format = try resolveFormat(spec.format, spec.path, &csv_options);
        const bytes = try readFileBytes(allocator, io, access, spec.path);
        defer allocator.free(bytes);

        switch (format) {
            .csv => try self.loadCsv(bytes, csv_options, needed),
            .json => try self.loadJson(bytes, spec.options.json, needed),
            .parquet => return error.FileScanUnsupportedParquet,
            .auto => unreachable,
        }

        return exec.makeQuery(allocator, self);
    }

    pub fn next(self: *FileScan) !?Batch {
        if (self.emitted) return null;
        self.emitted = true;
        return Batch{
            .schema = self.schema,
            .values = self.views,
            .row_count = self.row_count,
        };
    }

    pub fn deinit(self: *FileScan) void {
        for (self.stores) |*store| store.deinit(self.allocator);
        self.allocator.free(self.stores);
        self.allocator.free(self.views);
        self.allocator.free(self.stats_buf);
        for (self.schema) |c| self.allocator.free(c.name);
        self.allocator.free(self.schema);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *FileScan) []const Column {
        return self.schema;
    }

    pub fn addPrune(_: *FileScan, _: exec.Predicate) !void {}

    pub fn stats(self: *FileScan) exec.PipelineStats {
        return .{
            .upper_rows = @intCast(self.row_count),
            .column_stats = self.stats_buf,
        };
    }

    pub fn accountant(_: *FileScan) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(self: *FileScan, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainIndent(out, allocator, depth);
        try out.appendSlice(allocator, "FileScan rows=");
        var buf: [32]u8 = undefined;
        try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{self.row_count}));
        try out.append(allocator, '\n');
    }

    fn loadCsv(self: *FileScan, bytes: []const u8, opts_in: ir.CsvOptions, needed: ?[]const []const u8) !void {
        const opts = normalizeCsvOptions(opts_in);
        var parsed = try parseCsvRecords(self.allocator, bytes, opts);
        defer parsed.deinit(self.allocator);

        if (parsed.records.len == 0) return error.FileScanEmptyFile;
        const header = opts.header orelse if (opts.auto_detect) detectHeader(parsed.records) else false;
        const first_data: usize = if (header) 1 else 0;
        const field_count = parsed.records[0].fields.len;

        var infos = try self.allocator.alloc(ColumnInfo, field_count);
        defer {
            for (infos) |info| self.allocator.free(info.name);
            self.allocator.free(infos);
        }

        for (infos, 0..) |*info, i| {
            const raw_name = if (header) parsed.records[0].fields[i] else "";
            const fallback = try std.fmt.allocPrint(self.allocator, "column{d}", .{i});
            defer self.allocator.free(fallback);
            info.* = .{
                .name = try uniqueName(self.allocator, if (raw_name.len == 0) fallback else raw_name, infos[0..i]),
                .kind = if (opts.all_varchar) .string else .null,
                .nullable = false,
            };
        }

        for (parsed.records[first_data..]) |record| {
            for (record.fields, 0..) |field, i| {
                if (isCsvNull(field, opts.nullstr)) {
                    infos[i].nullable = true;
                    continue;
                }
                if (!opts.all_varchar) infos[i].kind = mergeKind(infos[i].kind, inferTextKind(field));
            }
        }
        for (infos) |*info| {
            if (info.kind == .null) {
                info.kind = .string;
                info.nullable = true;
            }
        }

        const selected = try selectedColumns(self.allocator, infos, needed);
        defer self.allocator.free(selected);
        try self.initOutput(infos, selected, parsed.records.len - first_data);
        errdefer self.clearOutput();

        for (parsed.records[first_data..]) |record| {
            for (selected, 0..) |src_idx, out_idx| {
                const field = record.fields[src_idx];
                const maybe_value: ?[]const u8 = if (isCsvNull(field, opts.nullstr)) null else field;
                try appendTextCell(self.allocator, &self.stores[out_idx], self.schema[out_idx].type, maybe_value);
            }
        }
        self.finishViews();
    }

    fn loadJson(self: *FileScan, bytes: []const u8, opts: ir.JsonOptions, needed: ?[]const []const u8) !void {
        _ = opts;
        var json_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer json_arena.deinit();

        var rows: std.ArrayList(std.json.Value) = .empty;
        defer rows.deinit(self.allocator);
        try parseJsonRows(json_arena.allocator(), self.allocator, bytes, &rows);

        var infos: std.ArrayList(JsonColumnInfo) = .empty;
        defer {
            for (infos.items) |info| self.allocator.free(info.name);
            infos.deinit(self.allocator);
        }

        for (rows.items, 0..) |row, row_idx| {
            if (row != .object) return error.FileScanMalformedJson;
            var it = row.object.iterator();
            while (it.next()) |entry| {
                const key = entry.key_ptr.*;
                const idx = jsonColumnIndex(infos.items, key) orelse blk: {
                    const name = try uniqueJsonName(self.allocator, key, infos.items);
                    try infos.append(self.allocator, .{
                        .name = name,
                        .key = key,
                        .kind = .null,
                        .nullable = row_idx > 0,
                        .seen = 0,
                    });
                    break :blk infos.items.len - 1;
                };
                infos.items[idx].seen += 1;
                const value = entry.value_ptr.*;
                if (value == .null) {
                    infos.items[idx].nullable = true;
                } else {
                    infos.items[idx].kind = mergeKind(infos.items[idx].kind, inferJsonKind(value));
                }
            }
        }
        for (infos.items) |*info| {
            if (info.seen < rows.items.len) info.nullable = true;
            if (info.kind == .null) {
                info.kind = .string;
                info.nullable = true;
            }
        }

        const selected = try selectedJsonColumns(self.allocator, infos.items, needed);
        defer self.allocator.free(selected);
        try self.initJsonOutput(infos.items, selected, rows.items.len);
        errdefer self.clearOutput();

        for (rows.items) |row| {
            for (selected, 0..) |src_idx, out_idx| {
                const info = infos.items[src_idx];
                const maybe_value = row.object.get(info.key);
                try appendJsonCell(self.allocator, &self.stores[out_idx], self.schema[out_idx].type, maybe_value);
            }
        }
        self.finishViews();
    }

    fn initOutput(self: *FileScan, infos: []const ColumnInfo, selected: []const usize, rows_cap: usize) !void {
        self.schema = try self.allocator.alloc(Column, selected.len);
        errdefer self.allocator.free(self.schema);
        self.stores = try self.allocator.alloc(ColumnStore, selected.len);
        errdefer self.allocator.free(self.stores);
        self.views = try self.allocator.alloc(ColumnView, selected.len);
        errdefer self.allocator.free(self.views);
        self.stats_buf = try self.allocator.alloc(exec.ColStat, selected.len);
        errdefer self.allocator.free(self.stats_buf);

        for (selected, 0..) |src_idx, out_idx| {
            const info = infos[src_idx];
            const ty = typeFromKind(info.kind);
            self.schema[out_idx] = .{
                .name = try self.allocator.dupe(u8, info.name),
                .type = ty,
                .nullable = info.nullable,
            };
            self.stores[out_idx] = try ColumnStore.initCapacity(self.allocator, ty, info.nullable, rows_cap, 0);
            self.stats_buf[out_idx] = .{};
        }
        self.row_count = rows_cap;
    }

    fn initJsonOutput(self: *FileScan, infos: []const JsonColumnInfo, selected: []const usize, rows_cap: usize) !void {
        self.schema = try self.allocator.alloc(Column, selected.len);
        errdefer self.allocator.free(self.schema);
        self.stores = try self.allocator.alloc(ColumnStore, selected.len);
        errdefer self.allocator.free(self.stores);
        self.views = try self.allocator.alloc(ColumnView, selected.len);
        errdefer self.allocator.free(self.views);
        self.stats_buf = try self.allocator.alloc(exec.ColStat, selected.len);
        errdefer self.allocator.free(self.stats_buf);

        for (selected, 0..) |src_idx, out_idx| {
            const info = infos[src_idx];
            const ty = typeFromKind(info.kind);
            self.schema[out_idx] = .{
                .name = try self.allocator.dupe(u8, info.name),
                .type = ty,
                .nullable = info.nullable,
            };
            self.stores[out_idx] = try ColumnStore.initCapacity(self.allocator, ty, info.nullable, rows_cap, 0);
            self.stats_buf[out_idx] = .{};
        }
        self.row_count = rows_cap;
    }

    fn finishViews(self: *FileScan) void {
        for (self.stores, 0..) |store, i| self.views[i] = store.view();
    }

    fn clearOutput(self: *FileScan) void {
        for (self.stores) |*store| store.deinit(self.allocator);
        self.allocator.free(self.stores);
        self.allocator.free(self.views);
        self.allocator.free(self.stats_buf);
        for (self.schema) |c| self.allocator.free(c.name);
        self.allocator.free(self.schema);
        self.schema = &.{};
        self.stores = &.{};
        self.views = &.{};
        self.stats_buf = &.{};
        self.row_count = 0;
    }
};

const CsvOptionsNorm = struct {
    header: ?bool,
    delim: u8,
    quote: u8,
    escape: u8,
    nullstr: []const u8,
    skip: u64,
    sample_size: u64,
    auto_detect: bool,
    all_varchar: bool,
};

const ColumnInfo = struct {
    name: []u8,
    kind: InferredKind,
    nullable: bool,
};

const JsonColumnInfo = struct {
    name: []u8,
    key: []const u8,
    kind: InferredKind,
    nullable: bool,
    seen: usize,
};

const InferredKind = enum { null, boolean, bigint, double, date, datetime, string };

fn resolveFormat(format: ir.FileFormat, path: []const u8, csv_options: *ir.CsvOptions) !ir.FileFormat {
    if (format != .auto) return format;
    if (endsWithIgnoreCase(path, ".csv")) return .csv;
    if (endsWithIgnoreCase(path, ".tsv")) {
        if (csv_options.delim == null) csv_options.delim = "\t";
        return .csv;
    }
    if (endsWithIgnoreCase(path, ".json") or endsWithIgnoreCase(path, ".ndjson") or endsWithIgnoreCase(path, ".jsonl")) return .json;
    if (endsWithIgnoreCase(path, ".parquet") or endsWithIgnoreCase(path, ".pq")) return .parquet;
    return error.FileScanUnknownFormat;
}

fn readFileBytes(allocator: Allocator, io: Io, access: api.FileScanAccess, path: []const u8) ![]u8 {
    switch (access) {
        .disabled => return error.FileScanDisabled,
        .process => return Io.Dir.cwd().readFileAlloc(io, path, allocator, .unlimited),
        .root => |root| {
            if (std.fs.path.isAbsolute(path) or std.mem.indexOfScalar(u8, path, ':') != null or hasParentTraversal(path)) {
                return error.FileScanPathOutsideRoot;
            }
            var dir = try Io.Dir.cwd().openDir(io, root, .{});
            defer dir.close(io);
            return dir.readFileAlloc(io, path, allocator, .unlimited);
        },
    }
}

fn hasParentTraversal(path: []const u8) bool {
    var it = std.mem.tokenizeAny(u8, path, "/\\");
    while (it.next()) |part| {
        if (std.mem.eql(u8, part, "..")) return true;
    }
    return false;
}

fn normalizeCsvOptions(opts: ir.CsvOptions) CsvOptionsNorm {
    return .{
        .header = opts.header,
        .delim = singleByteOrDefault(opts.delim, ','),
        .quote = singleByteOrDefault(opts.quote, '"'),
        .escape = singleByteOrDefault(opts.escape, '"'),
        .nullstr = opts.nullstr orelse "",
        .skip = opts.skip,
        .sample_size = opts.sample_size,
        .auto_detect = opts.auto_detect,
        .all_varchar = opts.all_varchar,
    };
}

fn singleByteOrDefault(value: ?[]const u8, default: u8) u8 {
    const s = value orelse return default;
    if (s.len == 0) return default;
    return s[0];
}

const CsvRecord = struct {
    fields: [][]u8,

    fn deinit(self: CsvRecord, allocator: Allocator) void {
        for (self.fields) |field| allocator.free(field);
        allocator.free(self.fields);
    }
};

const CsvParsed = struct {
    records: []CsvRecord,

    fn deinit(self: *CsvParsed, allocator: Allocator) void {
        for (self.records) |record| record.deinit(allocator);
        allocator.free(self.records);
    }
};

fn parseCsvRecords(allocator: Allocator, bytes: []const u8, opts: CsvOptionsNorm) !CsvParsed {
    var records: std.ArrayList(CsvRecord) = .empty;
    errdefer {
        for (records.items) |record| record.deinit(allocator);
        records.deinit(allocator);
    }
    var fields: std.ArrayList([]u8) = .empty;
    defer fields.deinit(allocator);
    var field: std.ArrayList(u8) = .empty;
    defer field.deinit(allocator);

    var in_quote = false;
    var field_started = false;
    var skipped: u64 = 0;
    var expected_fields: ?usize = null;
    var i: usize = 0;
    while (i < bytes.len) {
        const ch = bytes[i];
        if (in_quote) {
            if (ch == opts.quote) {
                if (i + 1 < bytes.len and bytes[i + 1] == opts.quote) {
                    try field.append(allocator, opts.quote);
                    i += 2;
                    continue;
                }
                in_quote = false;
                i += 1;
                continue;
            }
            if (opts.escape != opts.quote and ch == opts.escape and i + 1 < bytes.len) {
                try field.append(allocator, bytes[i + 1]);
                i += 2;
                continue;
            }
            try field.append(allocator, ch);
            i += 1;
            continue;
        }

        if (ch == opts.quote and !field_started and field.items.len == 0) {
            in_quote = true;
            field_started = true;
            i += 1;
            continue;
        }
        if (ch == opts.delim) {
            try finishCsvField(allocator, &fields, &field);
            field_started = false;
            i += 1;
            continue;
        }
        if (ch == '\n' or ch == '\r') {
            try finishCsvRecord(allocator, &records, &fields, &field, opts.skip, &skipped, &expected_fields);
            field_started = false;
            if (ch == '\r' and i + 1 < bytes.len and bytes[i + 1] == '\n') {
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }
        try field.append(allocator, ch);
        field_started = true;
        i += 1;
    }
    if (in_quote) return error.FileScanMalformedCsv;
    if (field_started or field.items.len > 0 or fields.items.len > 0) {
        try finishCsvRecord(allocator, &records, &fields, &field, opts.skip, &skipped, &expected_fields);
    }

    return .{ .records = try records.toOwnedSlice(allocator) };
}

fn finishCsvField(allocator: Allocator, fields: *std.ArrayList([]u8), field: *std.ArrayList(u8)) !void {
    const owned = try field.toOwnedSlice(allocator);
    try fields.append(allocator, owned);
    field.* = .empty;
}

fn finishCsvRecord(
    allocator: Allocator,
    records: *std.ArrayList(CsvRecord),
    fields: *std.ArrayList([]u8),
    field: *std.ArrayList(u8),
    skip: u64,
    skipped: *u64,
    expected_fields: *?usize,
) !void {
    try finishCsvField(allocator, fields, field);
    if (fields.items.len == 1 and fields.items[0].len == 0 and records.items.len == 0) {
        allocator.free(fields.items[0]);
        fields.clearRetainingCapacity();
        return;
    }
    if (skipped.* < skip) {
        skipped.* += 1;
        for (fields.items) |f| allocator.free(f);
        fields.clearRetainingCapacity();
        return;
    }
    const count = fields.items.len;
    if (expected_fields.*) |expected| {
        if (count != expected) return error.FileScanMalformedCsv;
    } else {
        expected_fields.* = count;
    }
    const owned_fields = try fields.toOwnedSlice(allocator);
    try records.append(allocator, .{ .fields = owned_fields });
    fields.* = .empty;
}

fn detectHeader(records: []const CsvRecord) bool {
    if (records.len < 2) return false;
    const first = records[0].fields;
    const second = records[1].fields;
    for (first, 0..) |cell, i| {
        if (i >= second.len) return false;
        const a = inferTextKind(cell);
        const b = inferTextKind(second[i]);
        if (a == .string and b != .string and b != .null) return true;
    }
    return false;
}

fn isCsvNull(field: []const u8, nullstr: []const u8) bool {
    const trimmed = std.mem.trim(u8, field, " \t");
    if (std.mem.eql(u8, trimmed, nullstr)) return true;
    return std.ascii.eqlIgnoreCase(trimmed, "null");
}

fn inferTextKind(text: []const u8) InferredKind {
    const s = std.mem.trim(u8, text, " \t");
    if (s.len == 0 or std.ascii.eqlIgnoreCase(s, "null")) return .null;
    if (parseBool(s) != null) return .boolean;
    if (std.fmt.parseInt(i64, s, 10)) |_| return .bigint else |_| {}
    if (std.fmt.parseFloat(f64, s)) |_| {
        if (hasFloatMarker(s)) return .double;
    } else |_| {}
    if (parseDate(s)) |_| return .date else |_| {}
    if (parseDateTime(s)) |_| return .datetime else |_| {}
    return .string;
}

fn inferJsonKind(value: std.json.Value) InferredKind {
    return switch (value) {
        .null => .null,
        .bool => .boolean,
        .integer => .bigint,
        .float => .double,
        .number_string => |s| inferTextKind(s),
        .string => |s| inferTextKind(s),
        .array, .object => .string,
    };
}

fn mergeKind(current: InferredKind, new: InferredKind) InferredKind {
    if (new == .null) return current;
    if (current == .null) return new;
    if (current == new) return current;
    if ((current == .bigint and new == .double) or (current == .double and new == .bigint)) return .double;
    if ((current == .date and new == .datetime) or (current == .datetime and new == .date)) return .datetime;
    return .string;
}

fn typeFromKind(kind: InferredKind) Type {
    return switch (kind) {
        .boolean => .boolean,
        .bigint => .bigint,
        .double => .double,
        .date => .date,
        .datetime => .datetime,
        .null, .string => .string,
    };
}

fn appendTextCell(allocator: Allocator, store: *ColumnStore, ty: Type, maybe_text: ?[]const u8) !void {
    const row = store.rowCount();
    if (maybe_text == null) {
        try appendNullPlaceholder(allocator, store, ty);
        try store.appendValidBit(allocator, row, false);
        return;
    }
    const text = std.mem.trim(u8, maybe_text.?, " \t");
    switch (ty) {
        .boolean => try store.data.boolean.append(allocator, @intFromBool(parseBool(text) orelse return error.FileScanCoercionFailed)),
        .bigint => try store.data.bigint.append(allocator, try std.fmt.parseInt(i64, text, 10)),
        .double => try store.data.double.append(allocator, try std.fmt.parseFloat(f64, text)),
        .date => try store.data.date.append(allocator, try parseDate(text)),
        .datetime => try store.data.datetime.append(allocator, parseDateTime(text) catch @as(i64, try parseDate(text)) * std.time.us_per_day),
        .string => try store.data.string.appendValue(allocator, maybe_text.?),
        else => return error.FileScanCoercionFailed,
    }
    try store.appendValidBit(allocator, row, true);
}

fn appendJsonCell(allocator: Allocator, store: *ColumnStore, ty: Type, maybe_value: ?std.json.Value) !void {
    const value = maybe_value orelse {
        const row = store.rowCount();
        try appendNullPlaceholder(allocator, store, ty);
        try store.appendValidBit(allocator, row, false);
        return;
    };
    if (value == .null) {
        const row = store.rowCount();
        try appendNullPlaceholder(allocator, store, ty);
        try store.appendValidBit(allocator, row, false);
        return;
    }
    const row = store.rowCount();
    switch (ty) {
        .boolean => try store.data.boolean.append(allocator, @intFromBool(switch (value) {
            .bool => |b| b,
            .string => |s| parseBool(s) orelse return error.FileScanCoercionFailed,
            else => return error.FileScanCoercionFailed,
        })),
        .bigint => try store.data.bigint.append(allocator, switch (value) {
            .integer => |x| x,
            .number_string => |s| try std.fmt.parseInt(i64, s, 10),
            .string => |s| try std.fmt.parseInt(i64, std.mem.trim(u8, s, " \t"), 10),
            else => return error.FileScanCoercionFailed,
        }),
        .double => try store.data.double.append(allocator, switch (value) {
            .integer => |x| @floatFromInt(x),
            .float => |x| x,
            .number_string => |s| try std.fmt.parseFloat(f64, s),
            .string => |s| try std.fmt.parseFloat(f64, std.mem.trim(u8, s, " \t")),
            else => return error.FileScanCoercionFailed,
        }),
        .date => try store.data.date.append(allocator, switch (value) {
            .string => |s| try parseDate(std.mem.trim(u8, s, " \t")),
            else => return error.FileScanCoercionFailed,
        }),
        .datetime => try store.data.datetime.append(allocator, switch (value) {
            .string => |s| blk: {
                const text = std.mem.trim(u8, s, " \t");
                break :blk parseDateTime(text) catch @as(i64, try parseDate(text)) * std.time.us_per_day;
            },
            else => return error.FileScanCoercionFailed,
        }),
        .string => {
            const text = try jsonValueString(allocator, value);
            defer allocator.free(text);
            try store.data.string.appendValue(allocator, text);
        },
        else => return error.FileScanCoercionFailed,
    }
    try store.appendValidBit(allocator, row, true);
}

fn appendNullPlaceholder(allocator: Allocator, store: *ColumnStore, ty: Type) !void {
    switch (ty) {
        .boolean => try store.data.boolean.append(allocator, 0),
        .bigint => try store.data.bigint.append(allocator, 0),
        .double => try store.data.double.append(allocator, 0),
        .date => try store.data.date.append(allocator, 0),
        .datetime => try store.data.datetime.append(allocator, 0),
        .string => try store.data.string.appendValue(allocator, ""),
        else => return error.FileScanCoercionFailed,
    }
}

fn parseJsonRows(json_allocator: Allocator, outer_allocator: Allocator, bytes: []const u8, rows: *std.ArrayList(std.json.Value)) !void {
    const trimmed = std.mem.trim(u8, bytes, " \t\r\n");
    if (trimmed.len == 0) return;
    if (trimmed[0] == '[') {
        const value = try std.json.parseFromSliceLeaky(std.json.Value, json_allocator, trimmed, .{});
        if (value != .array) return error.FileScanMalformedJson;
        for (value.array.items) |item| try rows.append(outer_allocator, item);
        return;
    }
    var it = std.mem.splitScalar(u8, bytes, '\n');
    while (it.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r\n");
        if (line.len == 0) continue;
        const value = try std.json.parseFromSliceLeaky(std.json.Value, json_allocator, line, .{});
        try rows.append(outer_allocator, value);
    }
}

fn jsonValueString(allocator: Allocator, value: std.json.Value) ![]const u8 {
    return switch (value) {
        .string => |s| try allocator.dupe(u8, s),
        .bool => |b| try allocator.dupe(u8, if (b) "true" else "false"),
        .integer => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
        .float => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
        .number_string => |s| try allocator.dupe(u8, s),
        .array, .object => blk: {
            var out: std.Io.Writer.Allocating = .init(allocator);
            errdefer out.deinit();
            try std.json.Stringify.value(value, .{}, &out.writer);
            break :blk try out.toOwnedSlice();
        },
        .null => try allocator.dupe(u8, ""),
    };
}

fn selectedColumns(allocator: Allocator, infos: []const ColumnInfo, needed: ?[]const []const u8) ![]usize {
    var selected: std.ArrayList(usize) = .empty;
    errdefer selected.deinit(allocator);
    for (infos, 0..) |info, i| {
        if (wantsColumn(needed, info.name)) try selected.append(allocator, i);
    }
    return selected.toOwnedSlice(allocator);
}

fn selectedJsonColumns(allocator: Allocator, infos: []const JsonColumnInfo, needed: ?[]const []const u8) ![]usize {
    var selected: std.ArrayList(usize) = .empty;
    errdefer selected.deinit(allocator);
    for (infos, 0..) |info, i| {
        if (wantsColumn(needed, info.name)) try selected.append(allocator, i);
    }
    return selected.toOwnedSlice(allocator);
}

fn wantsColumn(needed: ?[]const []const u8, name: []const u8) bool {
    const names = needed orelse return true;
    for (names) |candidate| {
        if (types.columnNameEql(candidate, name)) return true;
        if (std.mem.lastIndexOfScalar(u8, candidate, '.')) |dot| {
            if (types.columnNameEql(candidate[dot + 1 ..], name)) return true;
        }
    }
    return false;
}

fn uniqueName(allocator: Allocator, raw: []const u8, prior: []const ColumnInfo) ![]u8 {
    const base_trimmed = std.mem.trim(u8, raw, " \t\r\n");
    const base = if (base_trimmed.len == 0) "column" else base_trimmed;
    var suffix: usize = 0;
    while (true) : (suffix += 1) {
        const candidate = if (suffix == 0)
            try allocator.dupe(u8, base)
        else
            try std.fmt.allocPrint(allocator, "{s}_{d}", .{ base, suffix });
        var collides = false;
        for (prior) |p| {
            if (types.columnNameEql(candidate, p.name)) {
                collides = true;
                break;
            }
        }
        if (!collides) return candidate;
        allocator.free(candidate);
    }
}

fn uniqueJsonName(allocator: Allocator, raw: []const u8, prior: []const JsonColumnInfo) ![]u8 {
    const base_trimmed = std.mem.trim(u8, raw, " \t\r\n");
    const base = if (base_trimmed.len == 0) "column" else base_trimmed;
    var suffix: usize = 0;
    while (true) : (suffix += 1) {
        const candidate = if (suffix == 0)
            try allocator.dupe(u8, base)
        else
            try std.fmt.allocPrint(allocator, "{s}_{d}", .{ base, suffix });
        var collides = false;
        for (prior) |p| {
            if (types.columnNameEql(candidate, p.name)) {
                collides = true;
                break;
            }
        }
        if (!collides) return candidate;
        allocator.free(candidate);
    }
}

fn jsonColumnIndex(infos: []const JsonColumnInfo, key: []const u8) ?usize {
    for (infos, 0..) |info, i| {
        if (std.mem.eql(u8, info.key, key)) return i;
    }
    return null;
}

fn parseBool(text: []const u8) ?bool {
    if (std.ascii.eqlIgnoreCase(text, "true") or std.ascii.eqlIgnoreCase(text, "t") or
        std.ascii.eqlIgnoreCase(text, "yes") or std.ascii.eqlIgnoreCase(text, "on"))
        return true;
    if (std.ascii.eqlIgnoreCase(text, "false") or std.ascii.eqlIgnoreCase(text, "f") or
        std.ascii.eqlIgnoreCase(text, "no") or std.ascii.eqlIgnoreCase(text, "off"))
        return false;
    return null;
}

fn hasFloatMarker(text: []const u8) bool {
    return std.mem.indexOfAny(u8, text, ".eE") != null;
}

fn parseDate(s: []const u8) !i32 {
    if (s.len != 10 or s[4] != '-' or s[7] != '-') return error.FileScanCoercionFailed;
    const y = try std.fmt.parseInt(i32, s[0..4], 10);
    const m = try std.fmt.parseInt(u32, s[5..7], 10);
    const d = try std.fmt.parseInt(u32, s[8..10], 10);
    if (m < 1 or m > 12 or d < 1 or d > 31) return error.FileScanCoercionFailed;
    return daysFromCivil(y, m, d);
}

fn parseDateTime(s: []const u8) !i64 {
    if (s.len < 19) return error.FileScanCoercionFailed;
    const sep = s[10];
    if (sep != ' ' and sep != 'T') return error.FileScanCoercionFailed;
    const days = try parseDate(s[0..10]);
    if (s[13] != ':' or s[16] != ':') return error.FileScanCoercionFailed;
    const hh = try std.fmt.parseInt(u32, s[11..13], 10);
    const mm = try std.fmt.parseInt(u32, s[14..16], 10);
    const ss = try std.fmt.parseInt(u32, s[17..19], 10);
    if (hh > 23 or mm > 59 or ss > 60) return error.FileScanCoercionFailed;
    var micros: u64 = 0;
    if (s.len > 19) {
        if (s[19] != '.') return error.FileScanCoercionFailed;
        var idx: usize = 20;
        var digits: usize = 0;
        while (idx < s.len and digits < 6) : ({
            idx += 1;
            digits += 1;
        }) {
            if (s[idx] < '0' or s[idx] > '9') return error.FileScanCoercionFailed;
            micros = micros * 10 + (s[idx] - '0');
        }
        if (digits == 0) return error.FileScanCoercionFailed;
        while (idx < s.len) : (idx += 1) {
            if (s[idx] < '0' or s[idx] > '9') return error.FileScanCoercionFailed;
        }
        while (digits < 6) : (digits += 1) micros *= 10;
    }
    const day_us: i64 = 86_400 * 1_000_000;
    const secs: i64 = @intCast(hh * 3600 + mm * 60 + ss);
    return @as(i64, days) * day_us + secs * 1_000_000 + @as(i64, @intCast(micros));
}

fn daysFromCivil(year: i32, month: u32, day: u32) i32 {
    var y = year;
    if (month <= 2) y -= 1;
    const era = @divFloor(y, 400);
    const yoe: u32 = @intCast(y - era * 400);
    const mp: u32 = if (month > 2) month - 3 else month + 9;
    const doy: u32 = (153 * mp + 2) / 5 + day - 1;
    const doe: u32 = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return @intCast(era * 146097 + @as(i32, @intCast(doe)) - 719468);
}

fn endsWithIgnoreCase(s: []const u8, suffix: []const u8) bool {
    if (s.len < suffix.len) return false;
    return std.ascii.eqlIgnoreCase(s[s.len - suffix.len ..], suffix);
}
