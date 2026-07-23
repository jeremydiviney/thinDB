//! thindb-server — standalone server binary that opens a single thinDB
//! Catalog and exposes it via any combination of the MySQL, Postgres, and
//! native wire protocols. Each enabled wire listens on its own port; all
//! share one Catalog so DDL across wires is consistent.

const std = @import("std");
const builtin = @import("builtin");
const thindb = @import("thindb");
const config = @import("config.zig");

const Io = std.Io;

const default_bind: []const u8 = "0.0.0.0";

/// Argument source for the parse loop: the config-file-derived tokens followed
/// by the real command-line tokens, so a command-line flag overrides the same
/// setting given in the file. A tiny slice cursor exposing just `next`.
const ArgvIter = struct {
    items: []const []const u8,
    idx: usize = 0,

    fn next(self: *ArgvIter) ?[]const u8 {
        if (self.idx >= self.items.len) return null;
        const v = self.items[self.idx];
        self.idx += 1;
        return v;
    }
};

/// One acceptable global: the SIGINT/Ctrl+C handler can't carry context.
/// `main` owns the flag's lifetime; the signal handler only writes,
/// listener threads only read.
var stop_flag: std.atomic.Value(bool) = .{ .raw = false };

const usage_text =
    \\thindb-server — embedded thinDB exposed as a standalone server.
    \\
    \\Usage:
    \\  thindb-server --data-dir PATH [flags]
    \\
    \\Required:
    \\  --data-dir PATH         Root directory for the Catalog (will be created if missing).
    \\
    \\Optional:
    \\  --config PATH           Read settings from a flat `key = value` file (keys mirror these
    \\                          flag names without the leading --, e.g. `mysql-port = 13310`).
    \\                          A command-line flag overrides the same setting from the file.
    \\                          A password set in the file is NOT exposed in `ps`. Also read
    \\                          from THINDB_CONFIG if the flag is absent.
    \\  --mysql-port PORT       MySQL wire listener port (default 3306; 0 disables).
    \\  --pg-port PORT          Postgres wire listener port (default 5432; 0 disables).
    \\  --native-port PORT      Native thinDB wire listener port (default 7878; 0 disables).
    \\  --bind ADDR             Interface to bind (default 0.0.0.0).
    \\  --max-connections N     Cap on concurrent client connections across all wires (default 256).
    \\  --xa-timeout-secs N     Roll back orphaned PREPARED XA branches (Flink exactly-once) older
    \\                          than N seconds (default 86400 = 24h; 0 disables). Must exceed the
    \\                          Flink checkpoint interval + max tolerable downtime.
    \\  --idle-timeout-secs N   Close a connection after N seconds of read silence (default 0 = disabled).
    \\  --no-wal                Disable the write-ahead log (default: on). Without it, rows acked
    \\                          but not yet flushed are lost on a crash or kill — only for
    \\                          disposable bench data dirs where data is reimportable.
    \\  --max-dop N             Max worker threads per query for the parallel scan leaf (default 1 =
    \\                          serial). >1 hands row-group ranges to up to N workers; the per-query
    \\                          count is also bounded by a global ~(cores-1) worker-slot budget.
    \\  --compact-threads N     Worker threads for block encode+compress inside a compaction merge
    \\                          (default 0 = auto: ~25% of the physical cores; 1 = serial).
    \\  --query-memory-budget B Per-query memory budget in bytes (default: auto, ~25% of physical
    \\                          RAM, floored at 256 MiB; also gates the hash-vs-sort GROUP BY
    \\                          decision). Separate bucket from --cache-size. 0 disables tracking.
    \\  --memory-budget B       Process-shared query-memory pool ALL queries draw from, so
    \\                          concurrent queries can't sum past the box (default: auto, ~50% of
    \\                          physical RAM, floored at 256 MiB). Accepts raw bytes or a K/M/G
    \\                          suffix. 0 disables the shared pool (per-query budgets still apply).
    \\  --cache-size B          GLOBAL decompressed-block buffer-pool budget, shared by every
    \\                          table across all databases (default: auto, ~35% of physical RAM,
    \\                          floored at 256 MiB). Accepts raw bytes or a K/M/G suffix (e.g.
    \\                          8G). The LRU evicts the coldest block process-wide; blocks in
    \\                          use are pinned and never evicted.
    \\  --file-root PATH        Enable SQL file scans for paths under PATH. Without this flag,
    \\                          thindb-server rejects read_csv/read_json/read_parquet and
    \\                          FROM 'file.csv' sources.
    \\  --force-group-by S      Diagnostic: force GROUP BY path — auto (default) | hash | sort | radix.
    \\                          Bypasses cardinality routing; for hash-vs-sort benchmarking only.
    \\  --trace-group-by        Diagnostic: print each GROUP BY hash-vs-sort decision (est groups,
    \\                          per-group bytes, budget cutoff, each key's NDV / OOB) to stderr.
    \\  --profile-ops           Print a per-operator INCLUSIVE time breakdown to stderr after each
    \\                          query (diagnostic; ~zero overhead when off). Self time of an operator
    \\                          is its inclusive minus its upstream's in a linear pipeline.
    \\  --mysql-password PW     Require this password on the MySQL wire (mysql_native_password).
    \\                          Without this flag the MySQL wire is in trust mode and accepts any
    \\                          password. Does not affect the PG or native wires.
    \\  --pg-password PW        Require this password on the PG wire (SCRAM-SHA-256). Without this
    \\                          flag the PG wire is in trust mode and accepts any password. Does
    \\                          not affect the MySQL or native wires.
    \\  --help                  Show this help and exit.
    \\  --version               Print version and exit.
    \\
    \\Environment:
    \\  THINDB_CONFIG=PATH      Config file to read when --config is not given.
    \\  THINDB_MYSQL_PROFILE=1  Log per-connection MySQL protocol phase timings.
    \\
    \\Examples:
    \\  thindb-server --data-dir /var/lib/thindb
    \\  thindb-server --data-dir ./db --mysql-port 0 --pg-port 5433
    \\
;

pub fn main(init: std.process.Init) !u8 {
    const gpa = init.gpa;
    const io = init.io;

    var args_iter = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer args_iter.deinit();
    _ = args_iter.skip();

    // Arena for every argument string: the real CLI tokens (duped) plus the
    // synthetic tokens the config file expands to. Lives the whole of `main`,
    // so `Config` slices that point into these tokens stay valid.
    var arg_arena = std.heap.ArenaAllocator.init(gpa);
    defer arg_arena.deinit();
    const aa = arg_arena.allocator();

    var stderr_buf0: [4096]u8 = undefined;
    var stderr_writer0 = std.Io.File.stderr().writer(io, &stderr_buf0);
    const early_err = &stderr_writer0.interface;

    var cli_args: std.ArrayList([]const u8) = .empty;
    while (args_iter.next()) |a| try cli_args.append(aa, try aa.dupe(u8, a));

    // Config path: --config <path> / --config=PATH wins; else THINDB_CONFIG.
    var config_path: ?[]const u8 = null;
    {
        var i: usize = 0;
        while (i < cli_args.items.len) : (i += 1) {
            const a = cli_args.items[i];
            if (std.mem.eql(u8, a, "--config")) {
                if (i + 1 < cli_args.items.len) config_path = cli_args.items[i + 1];
            } else if (std.mem.startsWith(u8, a, "--config=")) {
                config_path = a["--config=".len..];
            }
        }
    }
    if (config_path == null) {
        if (init.environ_map.get("THINDB_CONFIG")) |p| {
            if (p.len > 0) config_path = try aa.dupe(u8, p);
        }
    }

    // File tokens are applied BEFORE the CLI tokens so a command-line flag
    // overrides the same setting from the file.
    var effective_args: std.ArrayList([]const u8) = .empty;
    if (config_path) |p| {
        const text = Io.Dir.cwd().readFileAlloc(io, p, aa, .limited(1 << 20)) catch |err| {
            try early_err.print("thindb-server: failed to read --config '{s}': {t}\n", .{ p, err });
            try early_err.flush();
            return 1;
        };
        config.parseInto(aa, text, &effective_args) catch |err| {
            try early_err.print("thindb-server: invalid config file '{s}': {t}\n", .{ p, err });
            try early_err.flush();
            return 1;
        };
    }
    try effective_args.appendSlice(aa, cli_args.items);
    var iter = ArgvIter{ .items = effective_args.items };

    var data_dir_opt: ?[]const u8 = null;
    var mysql_port: u16 = thindb.mysql_default_port;
    var pg_port: u16 = thindb.pg_default_port;
    var native_port: u16 = thindb.default_port;
    var bind: []const u8 = default_bind;
    var max_connections: u32 = 256;
    // Orphaned prepared XA branches (Flink job died without committing) older
    // than this are rolled back by the background sweep. 0 disables. Default 24h
    // — must exceed Flink's checkpoint interval + max tolerable downtime.
    var xa_timeout_secs: u32 = 24 * 3600;
    // Background segment compaction. On by default; --no-compaction disables the
    // compactor thread. INTERIM ESCAPE HATCH while #136 (a compactor-vs-write
    // race → double-free/leak under sustained writes) is unfixed — lets a
    // write-heavy sink run stably at the cost of unmerged segments.
    var enable_compaction: bool = true;
    // WAL on by default (matches the library): crash between ack and flush
    // must not shed acknowledged rows. --no-wal for disposable bench dirs.
    var enable_wal: bool = true;
    var idle_timeout_secs: u32 = 0;
    var query_memory_budget: ?usize = null;
    var memory_budget: ?usize = null;
    var cache_size_bytes: ?usize = null;
    var max_dop: ?u32 = null;
    var compact_threads: ?u32 = null;
    var file_root: ?[]const u8 = null;
    var mysql_password: ?[]const u8 = null;
    var pg_password: ?[]const u8 = null;
    const mysql_profile = envFlag(init.environ_map, "THINDB_MYSQL_PROFILE");

    var stderr_buf: [4096]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writer(io, &stderr_buf);
    const err_w = &stderr_writer.interface;
    var stdout_buf: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buf);
    const out_w = &stdout_writer.interface;

    while (iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            try out_w.writeAll(usage_text);
            try out_w.flush();
            return 0;
        }
        if (std.mem.eql(u8, arg, "--version")) {
            try out_w.print("thindb-server {s}\n", .{thindb.version});
            try out_w.flush();
            return 0;
        }
        // Already resolved and expanded in the pre-scan; consume and skip.
        if (try takeValue(arg, "--config", &iter, err_w)) |_| continue;
        if (std.mem.eql(u8, arg, "--profile-ops")) {
            thindb.exec.prof.enabled = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--trace-group-by")) {
            thindb.exec.trace_group_by = true;
            continue;
        }
        if (try takeValue(arg, "--scan-batch", &iter, err_w)) |v| {
            thindb.exec.scan_sub_batch = std.fmt.parseInt(usize, v, 10) catch {
                try err_w.print("thindb-server: --scan-batch must be an integer, got: {s}\n", .{v});
                try err_w.flush();
                return 1;
            };
            continue;
        }
        if (try takeValue(arg, "--force-group-by", &iter, err_w)) |v| {
            if (std.mem.eql(u8, v, "hash")) {
                thindb.exec.force_group_by = .hash;
            } else if (std.mem.eql(u8, v, "sort")) {
                thindb.exec.force_group_by = .sort;
            } else if (std.mem.eql(u8, v, "radix")) {
                thindb.exec.force_group_by = .radix;
            } else if (std.mem.eql(u8, v, "auto")) {
                thindb.exec.force_group_by = .auto;
            } else {
                try err_w.print("thindb-server: --force-group-by must be auto|hash|sort|radix, got: {s}\n", .{v});
                try err_w.flush();
                return 1;
            }
            continue;
        }
        if (try takeValue(arg, "--data-dir", &iter, err_w)) |v| {
            data_dir_opt = v;
            continue;
        }
        if (try takeValue(arg, "--bind", &iter, err_w)) |v| {
            bind = v;
            continue;
        }
        if (try takePort(arg, "--mysql-port", &iter, err_w)) |p| {
            mysql_port = p;
            continue;
        }
        if (try takePort(arg, "--pg-port", &iter, err_w)) |p| {
            pg_port = p;
            continue;
        }
        if (try takePort(arg, "--native-port", &iter, err_w)) |p| {
            native_port = p;
            continue;
        }
        if (try takeU32(arg, "--xa-timeout-secs", &iter, err_w)) |v| {
            xa_timeout_secs = v;
            continue;
        }
        if (std.mem.eql(u8, arg, "--no-compaction")) {
            enable_compaction = false;
            continue;
        }
        if (std.mem.eql(u8, arg, "--no-wal")) {
            enable_wal = false;
            continue;
        }
        if (try takeU32(arg, "--max-connections", &iter, err_w)) |v| {
            max_connections = v;
            continue;
        }
        if (try takeU32(arg, "--idle-timeout-secs", &iter, err_w)) |v| {
            idle_timeout_secs = v;
            continue;
        }
        if (try takeU32(arg, "--max-dop", &iter, err_w)) |v| {
            max_dop = v;
            continue;
        }
        if (try takeU32(arg, "--compact-threads", &iter, err_w)) |v| {
            compact_threads = v;
            continue;
        }
        if (try takeValue(arg, "--query-memory-budget", &iter, err_w)) |v| {
            query_memory_budget = std.fmt.parseInt(usize, v, 10) catch {
                try err_w.print("thindb-server: invalid --query-memory-budget: {s}\n", .{v});
                try err_w.flush();
                return 1;
            };
            continue;
        }
        if (try takeValue(arg, "--memory-budget", &iter, err_w)) |v| {
            memory_budget = parseSize(v) catch {
                try err_w.print("thindb-server: invalid --memory-budget: {s} (use bytes, or a K/M/G suffix)\n", .{v});
                try err_w.flush();
                return 1;
            };
            continue;
        }
        if (try takeValue(arg, "--cache-size", &iter, err_w)) |v| {
            cache_size_bytes = parseSize(v) catch {
                try err_w.print("thindb-server: invalid --cache-size: {s} (use bytes, or a K/M/G suffix)\n", .{v});
                try err_w.flush();
                return 1;
            };
            continue;
        }
        if (try takeValue(arg, "--file-root", &iter, err_w)) |v| {
            file_root = v;
            continue;
        }
        if (try takeValue(arg, "--mysql-password", &iter, err_w)) |v| {
            mysql_password = v;
            continue;
        }
        if (try takeValue(arg, "--pg-password", &iter, err_w)) |v| {
            pg_password = v;
            continue;
        }
        try err_w.print("thindb-server: unknown argument: {s}\n\n", .{arg});
        try err_w.writeAll(usage_text);
        try err_w.flush();
        return 1;
    }

    const data_dir = data_dir_opt orelse {
        try err_w.writeAll("thindb-server: --data-dir is required\n\n");
        try err_w.writeAll(usage_text);
        try err_w.flush();
        return 1;
    };

    if (mysql_port == 0 and pg_port == 0 and native_port == 0) {
        try err_w.writeAll("thindb-server: no wires enabled (all ports set to 0)\n");
        try err_w.flush();
        return 1;
    }

    var data_root = Io.Dir.cwd().createDirPathOpen(io, data_dir, .{}) catch |err| {
        try err_w.print("thindb-server: failed to open --data-dir '{s}': {t}\n", .{ data_dir, err });
        try err_w.flush();
        return 1;
    };
    defer data_root.close(io);

    const cfg: thindb.Config = .{
        .max_connections = max_connections,
        .idle_timeout_secs = idle_timeout_secs,
        .query_memory_budget = query_memory_budget orelse (thindb.Config{}).query_memory_budget,
        .memory_budget = memory_budget orelse (thindb.Config{}).memory_budget,
        .cache_size_bytes = cache_size_bytes orelse (thindb.Config{}).cache_size_bytes,
        .max_dop = if (max_dop) |v| v else (thindb.Config{}).max_dop,
        .data_root_path = data_dir,
        // Server default is auto (half the cores) rather than the embedded
        // library's serial default — background merges are the server's job.
        .compact_threads = if (compact_threads) |v| v else 0,
        .file_scan_access = if (file_root) |root| .{ .root = root } else .disabled,
        .wal_enabled = enable_wal,
    };
    var catalog = thindb.Catalog.open(gpa, io, data_root, cfg) catch |err| {
        try err_w.print("thindb-server: failed to open catalog at '{s}': {t}\n", .{ data_dir, err });
        try err_w.flush();
        return 1;
    };
    defer catalog.close();
    _ = catalog.createOrOpenDatabase("main") catch |err| {
        try err_w.print("thindb-server: failed to open default main/public namespace: {t}\n", .{err});
        try err_w.flush();
        return 1;
    };
    catalog.xa.gc_max_age_us = @as(i64, xa_timeout_secs) * 1_000_000;

    var shared_limiter = thindb.ConnectionLimiter.init(max_connections);

    // Process-wide registry for cross-connection cancellation. Shared
    // across all wire frontends so a KILL from MySQL can target a PG
    // connection (and vice versa).
    var shared_registry = thindb.ConnectionRegistry.init(gpa);
    defer shared_registry.deinit();

    installSignalHandler();

    try out_w.print("thindb-server starting\n  data-dir: {s}\n", .{data_dir});

    var listeners: [3]Listener = undefined;
    var n_listeners: usize = 0;
    var closed: bool = false;
    defer if (!closed) for (listeners[0..n_listeners]) |*l| l.close();

    const wire_specs = [_]WireSpec{
        .{ .label = "MySQL wire ", .port = mysql_port, .kind = .mysql },
        .{ .label = "PG wire    ", .port = pg_port, .kind = .pg },
        .{ .label = "native wire", .port = native_port, .kind = .native },
    };
    for (wire_specs) |spec| {
        if (spec.port == 0) continue;
        const addr = parseBind(bind, spec.port) catch |err| {
            try err_w.print("thindb-server: invalid --bind {s}: {t}\n", .{ bind, err });
            try err_w.flush();
            return 1;
        };
        listeners[n_listeners] = switch (spec.kind) {
            .mysql => blk: {
                const s = try thindb.serveMysql(gpa, io, catalog, addr, &shared_limiter);
                s.auth_password = mysql_password;
                s.registry = &shared_registry;
                s.profile = mysql_profile;
                break :blk .{ .mysql = s };
            },
            .pg => blk: {
                const s = try thindb.servePg(gpa, io, catalog, addr, &shared_limiter);
                s.setAuthPassword(pg_password);
                s.registry = &shared_registry;
                break :blk .{ .pg = s };
            },
            .native => .{ .native = try thindb.serveTcpCatalog(gpa, io, catalog, addr, &shared_limiter) },
        };
        n_listeners += 1;
        try out_w.print("  {s} listening on {s}:{d}\n", .{ spec.label, bind, spec.port });
    }
    if (mysql_profile and mysql_port != 0) {
        try out_w.writeAll("  MySQL profiling enabled (THINDB_MYSQL_PROFILE)\n");
    }
    try out_w.flush();

    var threads: [3]std.Thread = undefined;
    for (listeners[0..n_listeners], 0..) |*l, i| {
        threads[i] = try std.Thread.spawn(.{}, runListener, .{l});
    }

    // Always-on background flush sweep. Drives the time-based auto-flush
    // trigger so a memtable that crosses `auto_flush_secs` gets persisted
    // even if no further inserts arrive. 1s poll is short enough that
    // worst-case visible latency stays at ~auto_flush_secs + 1s.
    var flusher_ctx: FlusherCtx = .{ .catalog = catalog, .io = io };
    const flusher_thread = try std.Thread.spawn(.{}, FlusherCtx.run, .{&flusher_ctx});

    // Always-on background compactor. Each sweep merges at most one tier
    // group per table, so a burst of flushes (or a bulk load that lands as
    // many small segments) gets consolidated incrementally rather than
    // leaving the table fragmented. The heavy merge runs lock-free; only
    // the manifest swap briefly excludes readers.
    var compactor_ctx: CompactorCtx = .{ .catalog = catalog, .io = io };
    const compactor_thread: ?std.Thread = if (enable_compaction)
        try std.Thread.spawn(.{}, CompactorCtx.run, .{&compactor_ctx})
    else
        null;

    // net_read_timeout enforcement — see ReaperCtx.
    const net_read_timeout_secs = envU64(init.environ_map, "THINDB_NET_READ_TIMEOUT_SECS", 30);
    var reaper_ctx: ReaperCtx = .{
        .registry = &shared_registry,
        .io = io,
        .timeout_ms = net_read_timeout_secs * 1000,
    };
    const reaper_thread: ?std.Thread = if (net_read_timeout_secs > 0)
        try std.Thread.spawn(.{}, ReaperCtx.run, .{&reaper_ctx})
    else
        null;

    waitForStop(io);

    for (listeners[0..n_listeners]) |*l| l.close();
    closed = true;
    for (threads[0..n_listeners]) |t| t.join();
    flusher_thread.join();
    if (compactor_thread) |t| t.join();
    if (reaper_thread) |t| t.join();

    try out_w.writeAll("thindb-server shutting down\n");
    try out_w.flush();
    return 0;
}

const WireKind = enum { mysql, pg, native };

const WireSpec = struct {
    label: []const u8,
    port: u16,
    kind: WireKind,
};

const Listener = union(WireKind) {
    mysql: *thindb.MysqlServer,
    pg: *thindb.PgServer,
    native: *thindb.TcpServer,

    fn close(self: *Listener) void {
        switch (self.*) {
            inline else => |s| s.close(),
        }
    }

    fn run(self: *Listener) void {
        switch (self.*) {
            inline else => |s| s.run(&stop_flag) catch {},
        }
    }
};

fn runListener(l: *Listener) void {
    l.run();
}

fn envFlag(map: *std.process.Environ.Map, key: []const u8) bool {
    const value = map.get(key) orelse return false;
    if (value.len == 0) return false;
    if (std.mem.eql(u8, value, "0")) return false;
    if (std.ascii.eqlIgnoreCase(value, "false")) return false;
    if (std.ascii.eqlIgnoreCase(value, "off")) return false;
    if (std.ascii.eqlIgnoreCase(value, "no")) return false;
    return true;
}

fn envU64(map: *std.process.Environ.Map, key: []const u8, default: u64) u64 {
    const value = map.get(key) orelse return default;
    return std.fmt.parseInt(u64, value, 10) catch default;
}

const FlusherCtx = struct {
    catalog: *thindb.Catalog,
    io: Io,

    fn run(self: *FlusherCtx) void {
        self.catalog.runBackgroundFlusher(self.io, 1000, &stop_flag);
    }
};

const CompactorCtx = struct {
    catalog: *thindb.Catalog,
    io: Io,

    fn run(self: *CompactorCtx) void {
        self.catalog.runBackgroundCompactor(self.io, 2000, &stop_flag);
    }
};

/// net_read_timeout reaper (#164): sweep the connection registry and shut
/// down any socket stuck mid-packet longer than `timeout_ms`. Mirrors
/// MySQL's net_read_timeout (default 30 s); override with
/// `THINDB_NET_READ_TIMEOUT_SECS` (0 disables). A wedged mid-packet read
/// otherwise hangs until the client gives up — the 2026-07-11 incident
/// held a Flink sink connection at zero packets for 559 s.
const ReaperCtx = struct {
    registry: *thindb.ConnectionRegistry,
    io: Io,
    timeout_ms: u64,

    fn run(self: *ReaperCtx) void {
        const poll: Io.Duration = .fromMilliseconds(5000);
        while (!stop_flag.load(.acquire)) {
            Io.sleep(self.io, poll, .awake) catch return;
            const now_ns = Io.Clock.awake.now(self.io).nanoseconds;
            const now_ms: u64 = @intCast(@divTrunc(@max(now_ns, 0), std.time.ns_per_ms));
            _ = self.registry.reapStalledReads(self.io, now_ms, self.timeout_ms);
        }
    }
};

/// If `arg` is `name`, pull the next argv token. If `arg` is `name=VAL`,
/// return the inline value. Returns null if `arg` is unrelated.
fn takeValue(
    arg: []const u8,
    name: []const u8,
    iter: *ArgvIter,
    err_w: *std.Io.Writer,
) !?[]const u8 {
    if (std.mem.eql(u8, arg, name)) {
        const v = iter.next() orelse {
            try err_w.print("thindb-server: {s} requires a value\n", .{name});
            try err_w.flush();
            return error.MissingFlagValue;
        };
        return v;
    }
    if (std.mem.startsWith(u8, arg, name) and arg.len > name.len and arg[name.len] == '=') {
        return arg[name.len + 1 ..];
    }
    return null;
}

fn takePort(
    arg: []const u8,
    name: []const u8,
    iter: *ArgvIter,
    err_w: *std.Io.Writer,
) !?u16 {
    const v = try takeValue(arg, name, iter, err_w) orelse return null;
    return std.fmt.parseInt(u16, v, 10) catch |err| {
        try err_w.print("thindb-server: {s} expects a port number, got '{s}': {t}\n", .{ name, v, err });
        try err_w.flush();
        return error.BadPortValue;
    };
}

fn takeU32(
    arg: []const u8,
    name: []const u8,
    iter: *ArgvIter,
    err_w: *std.Io.Writer,
) !?u32 {
    const v = try takeValue(arg, name, iter, err_w) orelse return null;
    return std.fmt.parseInt(u32, v, 10) catch |err| {
        try err_w.print("thindb-server: {s} expects a non-negative integer, got '{s}': {t}\n", .{ name, v, err });
        try err_w.flush();
        return error.BadIntValue;
    };
}

fn parseBind(bind: []const u8, port: u16) !std.Io.net.IpAddress {
    return std.Io.net.IpAddress.parse(bind, port);
}

/// Parse a byte size: a plain integer, or an integer with a single binary
/// suffix `K`/`M`/`G` (×1024, ×1024², ×1024³). Case-insensitive on the suffix.
fn parseSize(s: []const u8) !usize {
    if (s.len == 0) return error.Empty;
    const last = s[s.len - 1];
    const mult: usize = switch (last) {
        'k', 'K' => 1024,
        'm', 'M' => 1024 * 1024,
        'g', 'G' => 1024 * 1024 * 1024,
        else => 1,
    };
    const digits = if (mult == 1) s else s[0 .. s.len - 1];
    const n = try std.fmt.parseInt(usize, digits, 10);
    return std.math.mul(usize, n, mult);
}

test "parseSize handles plain bytes and K/M/G suffixes" {
    try std.testing.expectEqual(@as(usize, 256), try parseSize("256"));
    try std.testing.expectEqual(@as(usize, 8 * 1024 * 1024 * 1024), try parseSize("8G"));
    try std.testing.expectEqual(@as(usize, 512 * 1024 * 1024), try parseSize("512m"));
    try std.testing.expectEqual(@as(usize, 64 * 1024), try parseSize("64K"));
    try std.testing.expectError(error.InvalidCharacter, parseSize("8GB"));
}

// ---------------------------------------------------------------------------
// Signal handling — set stop_flag from a Ctrl+C / SIGINT / SIGTERM handler
// ---------------------------------------------------------------------------

fn waitForStop(io: Io) void {
    const duration: Io.Duration = .fromMilliseconds(200);
    while (!stop_flag.load(.acquire)) {
        Io.sleep(io, duration, .awake) catch return;
    }
}

fn installSignalHandler() void {
    switch (builtin.os.tag) {
        .windows => installWindowsHandler(),
        else => installPosixHandler(),
    }
}

fn installPosixHandler() void {
    if (builtin.os.tag == .windows) return;
    // The handler's parameter type is OS-specific in std (enum on linux, i32
    // elsewhere) — derive it from the canonical Sigaction.handler_fn alias.
    const SigParam = @typeInfo(@typeInfo(std.posix.Sigaction.handler_fn).pointer.child).@"fn".params[0].type.?;
    const handler = struct {
        fn onSignal(_: SigParam) callconv(.c) void {
            stop_flag.store(true, .release);
        }
    };
    const act: std.posix.Sigaction = .{
        .handler = .{ .handler = handler.onSignal },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &act, null);
    std.posix.sigaction(std.posix.SIG.TERM, &act, null);
}

extern "kernel32" fn SetConsoleCtrlHandler(
    HandlerRoutine: ?*const fn (DwCtrlType: u32) callconv(.winapi) c_int,
    Add: c_int,
) callconv(.winapi) c_int;

fn windowsCtrlHandler(_: u32) callconv(.winapi) c_int {
    stop_flag.store(true, .release);
    return 1; // TRUE — we handled it, suppress default termination
}

fn installWindowsHandler() void {
    if (builtin.os.tag != .windows) return;
    _ = SetConsoleCtrlHandler(windowsCtrlHandler, 1);
}
