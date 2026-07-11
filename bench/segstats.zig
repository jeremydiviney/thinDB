//! Diagnostic: verify per-row-group footer stats + manifest column_stats
//! against actually-decoded values for one column of one table.
//! Usage: segstats <table_dir> <column_name>

const std = @import("std");
const thindb = @import("thindb");
const storage = thindb.storage;

fn usToStr(buf: []u8, us: i128) []const u8 {
    // rough date from µs epoch, UTC — good enough for eyeballing
    const s = @divFloor(us, 1_000_000);
    const days = @divFloor(s, 86400);
    const secs_in_day = @mod(s, 86400);
    const epoch_day: i64 = @intCast(days);
    var year: i64 = 1970;
    var d = epoch_day;
    while (true) {
        const leap = (@mod(year, 4) == 0 and @mod(year, 100) != 0) or @mod(year, 400) == 0;
        const ylen: i64 = if (leap) 366 else 365;
        if (d < ylen) break;
        d -= ylen;
        year += 1;
    }
    const month_days = [_]i64{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    var month: usize = 0;
    while (month < 12) : (month += 1) {
        const leap = (@mod(year, 4) == 0 and @mod(year, 100) != 0) or @mod(year, 400) == 0;
        var ml = month_days[month];
        if (month == 1 and leap) ml += 1;
        if (d < ml) break;
        d -= ml;
    }
    return std.fmt.bufPrint(buf, "{d}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}", .{
        year,                             month + 1,                      d + 1,
        @divFloor(secs_in_day, 3600),     @mod(@divFloor(secs_in_day, 60), 60), @mod(secs_in_day, 60),
    }) catch "?";
}

var g_is_string: bool = false;

fn encFmt(buf: []u8, enc: i128) []const u8 {
    return if (g_is_string) prefixToStr(buf, enc) else usToStr(buf, enc);
}

fn prefixToStr(buf: []u8, enc: i128) []const u8 {
    // invert format.encodeStringPrefix: flip top bit, big-endian 16 bytes
    if (enc == std.math.maxInt(i128)) return "<maxsent>";
    if (enc == std.math.minInt(i128)) return "<minsent>";
    const u: u128 = @as(u128, @bitCast(enc)) ^ (@as(u128, 1) << 127);
    var raw: [16]u8 = undefined;
    std.mem.writeInt(u128, &raw, u, .big);
    var n: usize = 0;
    for (raw) |ch| {
        buf[n] = if (ch >= 0x20 and ch < 0x7f) ch else '.';
        n += 1;
    }
    return buf[0..n];
}

pub fn main(init: std.process.Init) !u8 {
    const allocator = init.gpa;
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var args_iter = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args_iter.deinit();
    _ = args_iter.skip();
    const table_path = args_iter.next() orelse {
        std.debug.print("usage: segstats <table_dir> <column_name>\n", .{});
        return 1;
    };
    const col_name = args_iter.next() orelse {
        std.debug.print("usage: segstats <table_dir> <column_name>\n", .{});
        return 1;
    };

    const cwd = std.Io.Dir.cwd();
    var table_dir = try cwd.openDir(io, table_path, .{});
    defer table_dir.close(io);

    var schema_owner = try storage.schema_file.readSchema(allocator, io, table_dir);
    defer schema_owner.deinit();
    const schema = schema_owner.view();

    var col_idx: usize = std.math.maxInt(usize);
    for (schema.columns, 0..) |c, i| {
        if (std.mem.eql(u8, c.name, col_name)) col_idx = i;
    }
    if (col_idx == std.math.maxInt(usize)) {
        std.debug.print("column '{s}' not found\n", .{col_name});
        return 1;
    }
    std.debug.print("column '{s}' idx={d} type={t} nullable={}\n", .{ col_name, col_idx, schema.columns[col_idx].type, schema.columns[col_idx].nullable });

    g_is_string = switch (schema.columns[col_idx].type) {
        .varchar, .string, .char, .json => true,
        else => false,
    };

    const fp = thindb.api.schemaFingerprint(schema);
    var manifest = try storage.readManifest(allocator, io, table_dir, fp);
    defer manifest.deinit();

    var segments_dir = try table_dir.openDir(io, "segments", .{});
    defer segments_dir.close(io);

    var true_global_max: i128 = std.math.minInt(i128);
    var claimed_global_max: i128 = std.math.minInt(i128);
    var violations: usize = 0;
    var buf1: [64]u8 = undefined;
    var buf2: [64]u8 = undefined;
    var buf3: [64]u8 = undefined;
    var buf4: [64]u8 = undefined;

    for (manifest.segments.items) |entry| {
        var name_buf: [64]u8 = undefined;
        const fname = try std.fmt.bufPrint(&name_buf, "{d:0>20}.dat", .{entry.segment_id});

        var seg = storage.readSegment(allocator, io, segments_dir, fname, schema) catch |err| {
            std.debug.print("segment {d}: OPEN FAILED {t}\n", .{ entry.segment_id, err });
            continue;
        };
        defer seg.deinit();

        const man_cs: ?storage.format.Stats = if (col_idx < entry.column_stats.len) entry.column_stats[col_idx] else null;

        // fold footer RG stats for the column
        var fold_min: i128 = std.math.maxInt(i128);
        var fold_max: i128 = std.math.minInt(i128);
        var seg_true_min: i128 = std.math.maxInt(i128);
        var seg_true_max: i128 = std.math.minInt(i128);
        var seg_violations: usize = 0;

        for (seg.info.row_groups, 0..) |rg, rgi| {
            const claimed = rg.stats[col_idx];
            if (claimed.min <= claimed.max) {
                if (claimed.min < fold_min) fold_min = claimed.min;
                if (claimed.max > fold_max) fold_max = claimed.max;
            }

            var col = seg.decodeColumn(allocator, schema, rgi, col_idx) catch |err| {
                std.debug.print("segment {d} rg {d}: DECODE FAILED {t}\n", .{ entry.segment_id, rgi, err });
                continue;
            };
            defer col.deinit(allocator);
            const view = col.view();

            var actual_min: i128 = std.math.maxInt(i128);
            var actual_max: i128 = std.math.minInt(i128);
            var actual_nulls: u64 = 0;
            switch (view.data) {
                .datetime, .bigint, .decimal64 => |vals| {
                    for (vals, 0..) |v, r| {
                        if (!view.isValid(r)) {
                            actual_nulls += 1;
                            continue;
                        }
                        const vi: i128 = v;
                        if (vi < actual_min) actual_min = vi;
                        if (vi > actual_max) actual_max = vi;
                    }
                },
                .varchar, .string, .char, .json => |sv| {
                    var r: usize = 0;
                    while (r < rg.row_count) : (r += 1) {
                        if (!view.isValid(r)) {
                            actual_nulls += 1;
                            continue;
                        }
                        const enc = storage.format.encodeStringPrefix(sv.rowBytes(r));
                        if (enc < actual_min) actual_min = enc;
                        if (enc > actual_max) actual_max = enc;
                    }
                },
                else => {
                    std.debug.print("unsupported column view type\n", .{});
                    return 1;
                },
            }

            if (actual_min > actual_max) {
                // all-null RG: claimed should be inverted sentinel
                if (claimed.min <= claimed.max) {
                    std.debug.print("segment {d} rg {d}: ALL-NULL actual but claimed min={d} max={d}\n", .{ entry.segment_id, rgi, claimed.min, claimed.max });
                    seg_violations += 1;
                }
            } else {
                if (actual_min < seg_true_min) seg_true_min = actual_min;
                if (actual_max > seg_true_max) seg_true_max = actual_max;
                const bad_sentinel = claimed.min > claimed.max;
                const understated = !bad_sentinel and (actual_max > claimed.max or actual_min < claimed.min);
                if (bad_sentinel or understated) {
                    std.debug.print("segment {d} rg {d} rows={d}: VIOLATION claimed[{d},{d}] actual[{d},{d}] ({s}..{s}) nulls claimed={d} actual={d}\n", .{
                        entry.segment_id,          rgi,                        rg.row_count,
                        claimed.min,               claimed.max,                actual_min,
                        actual_max,                encFmt(&buf1, actual_min), encFmt(&buf2, actual_max),
                        claimed.null_count,        actual_nulls,
                    });
                    seg_violations += 1;
                }
                if (claimed.null_count != actual_nulls and !bad_sentinel) {
                    std.debug.print("segment {d} rg {d}: NULLCOUNT claimed={d} actual={d}\n", .{ entry.segment_id, rgi, claimed.null_count, actual_nulls });
                    seg_violations += 1;
                }
            }
        }

        violations += seg_violations;
        if (seg_true_max > true_global_max) true_global_max = seg_true_max;
        if (fold_max > claimed_global_max) claimed_global_max = fold_max;

        std.debug.print("segment {d}: rows={d} rgs={d} | footer_fold[{d},{d}] max={s} | true[{d},{d}] max={s} | manifest[{d},{d}] max={s} | violations={d}\n", .{
            entry.segment_id,
            entry.row_count,
            seg.info.row_groups.len,
            fold_min,
            fold_max,
            encFmt(&buf1, fold_max),
            seg_true_min,
            seg_true_max,
            encFmt(&buf2, seg_true_max),
            if (man_cs) |cs| cs.min else 0,
            if (man_cs) |cs| cs.max else 0,
            encFmt(&buf3, if (man_cs) |cs| cs.max else 0),
            seg_violations,
        });
        if (man_cs) |cs| {
            if (cs.max != fold_max or cs.min != fold_min) {
                std.debug.print("segment {d}: MANIFEST-VS-FOOTER MISMATCH manifest[{d},{d}] footer_fold[{d},{d}]\n", .{ entry.segment_id, cs.min, cs.max, fold_min, fold_max });
            }
        }
    }

    std.debug.print("\nTOTAL violations={d}\nclaimed global max = {d} ({s})\ntrue    global max = {d} ({s})\n", .{
        violations,
        claimed_global_max,
        encFmt(&buf1, claimed_global_max),
        true_global_max,
        encFmt(&buf2, true_global_max),
    });
    _ = &buf4;
    return 0;
}
