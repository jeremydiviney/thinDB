//! thindb-server — standalone server binary that opens a single thinDB
//! Catalog and exposes it via any combination of the MySQL, Postgres, and
//! native wire protocols. Each enabled wire listens on its own port; all
//! share one Catalog so DDL across wires is consistent.

const std = @import("std");
const builtin = @import("builtin");
const thindb = @import("thindb");

const Io = std.Io;

const default_bind: []const u8 = "0.0.0.0";

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
    \\  --mysql-port PORT       MySQL wire listener port (default 3306; 0 disables).
    \\  --pg-port PORT          Postgres wire listener port (default 5432; 0 disables).
    \\  --native-port PORT      Native thinDB wire listener port (default 7878; 0 disables).
    \\  --bind ADDR             Interface to bind (default 0.0.0.0).
    \\  --max-connections N     Cap on concurrent client connections across all wires (default 256).
    \\  --idle-timeout-secs N   Close a connection after N seconds of read silence (default 0 = disabled).
    \\  --mysql-password PW     Require this password on the MySQL wire (mysql_native_password).
    \\                          Without this flag the MySQL wire is in trust mode and accepts any
    \\                          password. Does not affect the PG or native wires.
    \\  --pg-password PW        Require this password on the PG wire (SCRAM-SHA-256). Without this
    \\                          flag the PG wire is in trust mode and accepts any password. Does
    \\                          not affect the MySQL or native wires.
    \\  --help                  Show this help and exit.
    \\  --version               Print version and exit.
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

    var data_dir_opt: ?[]const u8 = null;
    var mysql_port: u16 = thindb.mysql_default_port;
    var pg_port: u16 = thindb.pg_default_port;
    var native_port: u16 = thindb.default_port;
    var bind: []const u8 = default_bind;
    var max_connections: u32 = 256;
    var idle_timeout_secs: u32 = 0;
    var mysql_password: ?[]const u8 = null;
    var pg_password: ?[]const u8 = null;

    var stderr_buf: [4096]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writer(io, &stderr_buf);
    const err_w = &stderr_writer.interface;
    var stdout_buf: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buf);
    const out_w = &stdout_writer.interface;

    while (args_iter.next()) |arg| {
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
        if (try takeValue(arg, "--data-dir", &args_iter, err_w)) |v| {
            data_dir_opt = v;
            continue;
        }
        if (try takeValue(arg, "--bind", &args_iter, err_w)) |v| {
            bind = v;
            continue;
        }
        if (try takePort(arg, "--mysql-port", &args_iter, err_w)) |p| {
            mysql_port = p;
            continue;
        }
        if (try takePort(arg, "--pg-port", &args_iter, err_w)) |p| {
            pg_port = p;
            continue;
        }
        if (try takePort(arg, "--native-port", &args_iter, err_w)) |p| {
            native_port = p;
            continue;
        }
        if (try takeU32(arg, "--max-connections", &args_iter, err_w)) |v| {
            max_connections = v;
            continue;
        }
        if (try takeU32(arg, "--idle-timeout-secs", &args_iter, err_w)) |v| {
            idle_timeout_secs = v;
            continue;
        }
        if (try takeValue(arg, "--mysql-password", &args_iter, err_w)) |v| {
            mysql_password = v;
            continue;
        }
        if (try takeValue(arg, "--pg-password", &args_iter, err_w)) |v| {
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
    const compactor_thread = try std.Thread.spawn(.{}, CompactorCtx.run, .{&compactor_ctx});

    waitForStop(io);

    for (listeners[0..n_listeners]) |*l| l.close();
    closed = true;
    for (threads[0..n_listeners]) |t| t.join();
    flusher_thread.join();
    compactor_thread.join();

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

/// If `arg` is `name`, pull the next argv token. If `arg` is `name=VAL`,
/// return the inline value. Returns null if `arg` is unrelated.
fn takeValue(
    arg: []const u8,
    name: []const u8,
    iter: *std.process.Args.Iterator,
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
    iter: *std.process.Args.Iterator,
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
    iter: *std.process.Args.Iterator,
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
    const handler = struct {
        fn onSignal(_: i32) callconv(.c) void {
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
