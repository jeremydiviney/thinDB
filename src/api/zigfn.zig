//! `CREATE FUNCTION name LANGUAGE zig AS $$...$$` — the server-side
//! compile pipeline for Zig table UDFs (TABLE_UDF_PLAN.md P2).
//!
//! The submitted source is a complete SDK function file (`spec` + `Input` +
//! `Output` + `process`, importing the SDK as `@import("tdb")`). The server
//! writes it into a scratch build directory next to its own EMBEDDED SDK
//! files (physically inside this binary via @embedFile — the SDK can never
//! be stale or missing) plus a STATIC wrapper that reflects the user's
//! declarations at comptime and exports a minimal C-ABI handshake:
//!
//!   thindb_tvf_abi_version() u32
//!   thindb_tvf_descriptor()  *const Desc   (name/shape/execution, flat)
//!   thindb_tvf_process(ctx, part, out) i32 (0 = ok)
//!
//! `zig build-lib -dynamic -OReleaseFast` produces a shared library; the
//! server dlopens it, validates the handshake, and registers a TableUdf
//! whose process is a tiny shim: `user_data` carries the library's process
//! pointer, so the exec operator needs no changes at all. Pointer payloads
//! (TvfPartition/TvfOutput) cross the boundary as-is — safe because the
//! library is compiled by the EXACT same compiler version against the
//! byte-identical SDK the server embeds (enforced: `zig version` must
//! equal the server's builtin.zig_version).
//!
//! Persistence = the SOURCE, written to `<db>/_functions/<name>.zig` and
//! recompiled on catalog open — the data directory stays portable across
//! platforms; the compiled library is a cache, not a format.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const types = @import("../types.zig");
const udf_mod = @import("../udf.zig");

pub const Error = error{
    FunctionInvalidDefinition,
    FunctionAlreadyExists,
    ZigToolchainNotFound,
    ZigToolchainVersionMismatch,
    ZigCompileFailed,
};

pub const ABI_VERSION: u32 = 1;

const EmbeddedFile = struct { path: []const u8, data: []const u8 };

/// The SDK's complete transitive import closure — all std-only (the
/// import-hygiene refactor keeps the storage subsystem and its C libs out).
const EMBEDDED_SDK = [_]EmbeddedFile{
    .{ .path = "udf_sdk.zig", .data = @embedFile("../udf_sdk.zig") },
    .{ .path = "udf.zig", .data = @embedFile("../udf.zig") },
    .{ .path = "types.zig", .data = @embedFile("../types.zig") },
    .{ .path = "storage/column.zig", .data = @embedFile("../storage/column.zig") },
    .{ .path = "engine/store.zig", .data = @embedFile("../engine/store.zig") },
};

/// Static wrapper: comptime-reflects the user's declarations INSIDE the
/// library, so no per-function codegen is needed on the server.
const WRAPPER_MAIN =
    \\const std = @import("std");
    \\const tdb = @import("tdb");
    \\const userfn = @import("fn_src.zig");
    \\
    \\const desc_gen = tdb.descriptorFor(userfn);
    \\
    \\pub const ColDesc = extern struct {
    \\    name_ptr: [*]const u8,
    \\    name_len: usize,
    \\    type_tag: u8,
    \\    nullable: bool,
    \\};
    \\pub const Desc = extern struct {
    \\    name_ptr: [*]const u8,
    \\    name_len: usize,
    \\    execution: u8,
    \\    n_in: usize,
    \\    in_cols: [*]const ColDesc,
    \\    n_out: usize,
    \\    out_cols: [*]const ColDesc,
    \\};
    \\
    \\fn colDescs(comptime cols: anytype) [cols.len]ColDesc {
    \\    var out: [cols.len]ColDesc = undefined;
    \\    for (cols, 0..) |c, i| {
    \\        out[i] = .{
    \\            .name_ptr = c.name.ptr,
    \\            .name_len = c.name.len,
    \\            .type_tag = @intFromEnum(@as(tdb.TypeTag, c.type)),
    \\            .nullable = c.nullable,
    \\        };
    \\    }
    \\    return out;
    \\}
    \\
    \\const in_descs = colDescs(desc_gen.input_schema);
    \\const out_descs = colDescs(desc_gen.output_schema);
    \\const the_desc = Desc{
    \\    .name_ptr = desc_gen.name.ptr,
    \\    .name_len = desc_gen.name.len,
    \\    .execution = @intFromEnum(desc_gen.execution),
    \\    .n_in = in_descs.len,
    \\    .in_cols = &in_descs,
    \\    .n_out = out_descs.len,
    \\    .out_cols = &out_descs,
    \\};
    \\
    \\export fn thindb_tvf_abi_version() callconv(.c) u32 {
    \\    return 1;
    \\}
    \\export fn thindb_tvf_descriptor() callconv(.c) *const Desc {
    \\    return &the_desc;
    \\}
    \\export fn thindb_tvf_process(
    \\    ctx: *const anyopaque,
    \\    part: *const anyopaque,
    \\    out: *anyopaque,
    \\) callconv(.c) i32 {
    \\    const c: *const tdb.TvfContext = @ptrCast(@alignCast(ctx));
    \\    const p: *const tdb.TvfPartition = @ptrCast(@alignCast(part));
    \\    const o: *tdb.TvfOutput = @ptrCast(@alignCast(out));
    \\    desc_gen.process(c, p, o) catch return -1;
    \\    return 0;
    \\}
;

/// Server-side mirrors of the wrapper's handshake structs.
pub const ColDesc = extern struct {
    name_ptr: [*]const u8,
    name_len: usize,
    type_tag: u8,
    nullable: bool,
};
pub const Desc = extern struct {
    name_ptr: [*]const u8,
    name_len: usize,
    execution: u8,
    n_in: usize,
    in_cols: [*]const ColDesc,
    n_out: usize,
    out_cols: [*]const ColDesc,
};

const DllProcess = *const fn (*const anyopaque, *const anyopaque, *anyopaque) callconv(.c) i32;

/// The shim registered as every Zig function's TvfProcess: `user_data`
/// carries the library's process pointer (threaded through TvfContext by
/// the exec operator), so no per-function server code exists.
fn dllShim(ctx: *const udf_mod.TvfContext, part: *const udf_mod.TvfPartition, out: *udf_mod.TvfOutput) anyerror!void {
    const f: DllProcess = @ptrCast(@alignCast(ctx.user_data.?));
    if (f(ctx, part, out) != 0) return error.TableFnKernelError;
}

/// A loaded Zig function: keeps the library alive for the registry
/// entry's lifetime and remembers the source for SHOW CREATE FUNCTION.
/// std.DynLib has no Windows backend in this Zig version; tiny portable
/// wrapper over LoadLibrary/dlopen.
pub const Dyn = struct {
    handle: if (builtin.os.tag == .windows) std.os.windows.HMODULE else std.DynLib,

    pub fn open(path: []const u8) !Dyn {
        if (builtin.os.tag == .windows) {
            var w_buf: [std.os.windows.PATH_MAX_WIDE:0]u16 = undefined;
            const n = try std.unicode.wtf8ToWtf16Le(&w_buf, path);
            w_buf[n] = 0;
            const h = LoadLibraryW(w_buf[0..n :0]) orelse
                return error.DllLoadFailed;
            return .{ .handle = h };
        }
        return .{ .handle = try std.DynLib.open(path) };
    }

    pub fn lookup(self: *Dyn, comptime T: type, name: [:0]const u8) ?T {
        if (builtin.os.tag == .windows) {
            const p = GetProcAddress(self.handle, name.ptr) orelse return null;
            return @ptrCast(@alignCast(p));
        }
        return self.handle.lookup(T, name);
    }

    pub fn close(self: *Dyn) void {
        if (builtin.os.tag == .windows) {
            _ = FreeLibrary(self.handle);
        } else {
            self.handle.close();
        }
    }
};

pub const ZigFnHandle = struct {
    /// Lowercased function name (owned).
    name: []const u8,
    lib: Dyn,
    /// The submitted source (owned).
    source: []const u8,

    pub fn deinit(self: *ZigFnHandle, allocator: Allocator) void {
        self.lib.close();
        allocator.free(self.name);
        allocator.free(self.source);
    }
};

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;
extern "kernel32" fn FreeLibrary(hModule: std.os.windows.HMODULE) callconv(.winapi) std.os.windows.BOOL;
extern "kernel32" fn LoadLibraryW(lpLibFileName: [*:0]const u16) callconv(.winapi) ?std.os.windows.HMODULE;
extern "kernel32" fn GetProcAddress(hModule: std.os.windows.HMODULE, lpProcName: [*:0]const u8) callconv(.winapi) ?*anyopaque;

/// Locate the Zig toolchain: THINDB_ZIG_PATH (a zig executable path) →
/// `zig/zig(.exe)` beside the server executable → `zig` on PATH.
/// Returned string is allocated.
pub fn findZigExe(allocator: Allocator) ![]u8 {
    if (getenv("THINDB_ZIG_PATH")) |v| return allocator.dupe(u8, std.mem.span(v));
    return allocator.dupe(u8, "zig");
}

fn runZig(
    allocator: Allocator,
    io: Io,
    argv: []const []const u8,
    cwd: []const u8,
    err_out: *std.ArrayList(u8),
) !void {
    var child = try std.process.spawn(io, .{
        .argv = argv,
        .cwd = .{ .path = cwd },
        .stdout = .ignore,
        .stderr = .pipe,
        .stdin = .ignore,
    });
    errdefer std.process.Child.kill(&child, io);
    var buf: [4096]u8 = undefined;
    var reader = child.stderr.?.reader(io, &buf);
    while (reader.interface.takeByte()) |b| {
        try err_out.append(allocator, b);
    } else |_| {}
    const term = try std.process.Child.wait(&child, io);
    switch (term) {
        .exited => |code| if (code != 0) return Error.ZigCompileFailed,
        else => return Error.ZigCompileFailed,
    }
}

/// Compile `source` and load the result. On success the function is
/// registered in `registry` and the returned handle owns the library.
/// `scratch_dir` is the build directory (created by the caller, under the
/// database dir); `dll_seq` disambiguates artifact names so a replaced
/// function never collides with a still-loaded library file.
pub fn compileAndLoad(
    allocator: Allocator,
    io: Io,
    scratch_dir: Io.Dir,
    scratch_path: []const u8,
    name: []const u8,
    source: []const u8,
    dll_seq: u64,
    registry: *udf_mod.UdfRegistry,
    compile_log: *std.ArrayList(u8),
) !ZigFnHandle {
    // Lay down the module set.
    try scratch_dir.createDirPath(io, "storage");
    try scratch_dir.createDirPath(io, "engine");
    for (EMBEDDED_SDK) |f| {
        try scratch_dir.writeFile(io, .{ .sub_path = f.path, .data = f.data });
    }
    try scratch_dir.writeFile(io, .{ .sub_path = "fn_src.zig", .data = source });
    try scratch_dir.writeFile(io, .{ .sub_path = "dll_main.zig", .data = WRAPPER_MAIN });

    const zig_exe = try findZigExe(allocator);
    defer allocator.free(zig_exe);

    // Exact-version gate: the library shares pointer-rich structs with the
    // server, so only the byte-identical compiler is acceptable.
    {
        var ver_out: std.ArrayList(u8) = .empty;
        defer ver_out.deinit(allocator);
        var child = std.process.spawn(io, .{
            .argv = &.{ zig_exe, "version" },
            .stdout = .pipe,
            .stderr = .ignore,
            .stdin = .ignore,
        }) catch return Error.ZigToolchainNotFound;
        var buf: [256]u8 = undefined;
        var reader = child.stdout.?.reader(io, &buf);
        while (reader.interface.takeByte()) |b| {
            try ver_out.append(allocator, b);
        } else |_| {}
        _ = std.process.Child.wait(&child, io) catch {};
        const got = std.mem.trim(u8, ver_out.items, " \r\n\t");
        const want = builtin.zig_version_string;
        if (!std.mem.eql(u8, got, want)) {
            std.debug.print("[zigfn] toolchain version mismatch: server built with {s}, found {s} at {s}\n", .{ want, got, zig_exe });
            return Error.ZigToolchainVersionMismatch;
        }
    }

    const dll_name = try std.fmt.allocPrint(allocator, "{s}_{d}.{s}", .{
        name,
        dll_seq,
        if (builtin.os.tag == .windows) "dll" else "so",
    });
    defer allocator.free(dll_name);
    const emit_arg = try std.fmt.allocPrint(allocator, "-femit-bin={s}", .{dll_name});
    defer allocator.free(emit_arg);

    // udf.zig is reachable both as a module ("udf_zigfn_internal", used by
    // the wrapper for the shared struct types) and relatively from
    // udf_sdk.zig — same file, same layout.
    runZig(allocator, io, &.{
        zig_exe,
        "build-lib",
        "-dynamic",
        "-OReleaseFast",
        emit_arg,
        "--dep",
        "tdb",
        "-Mroot=dll_main.zig",
        "-Mtdb=udf_sdk.zig",
    }, scratch_path, compile_log) catch |err| {
        if (err == Error.ZigCompileFailed) {
            std.debug.print("[zigfn] compile failed for '{s}':\n{s}\n", .{ name, compile_log.items });
            return Error.FunctionInvalidDefinition;
        }
        return err;
    };

    const dll_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ scratch_path, dll_name });
    defer allocator.free(dll_path);
    var lib = Dyn.open(dll_path) catch return Error.FunctionInvalidDefinition;
    errdefer lib.close();

    const abi_fn = lib.lookup(*const fn () callconv(.c) u32, "thindb_tvf_abi_version") orelse
        return Error.FunctionInvalidDefinition;
    if (abi_fn() != ABI_VERSION) return Error.FunctionInvalidDefinition;
    const desc_fn = lib.lookup(*const fn () callconv(.c) *const Desc, "thindb_tvf_descriptor") orelse
        return Error.FunctionInvalidDefinition;
    const process_fn = lib.lookup(DllProcess, "thindb_tvf_process") orelse
        return Error.FunctionInvalidDefinition;
    const desc = desc_fn();

    // DDL name must match the source's declared spec.name.
    const decl_name = desc.name_ptr[0..desc.name_len];
    if (!std.ascii.eqlIgnoreCase(decl_name, name)) {
        std.debug.print("[zigfn] name mismatch: DDL '{s}' vs spec.name '{s}'\n", .{ name, decl_name });
        return Error.FunctionInvalidDefinition;
    }
    if (desc.execution > @intFromEnum(udf_mod.TvfExecution.either)) return Error.FunctionInvalidDefinition;

    var cols_arena = std.heap.ArenaAllocator.init(allocator);
    defer cols_arena.deinit();
    const input_schema = try descCols(cols_arena.allocator(), desc.in_cols[0..desc.n_in]);
    const output_schema = try descCols(cols_arena.allocator(), desc.out_cols[0..desc.n_out]);

    try registry.registerTable(.{
        .name = name,
        .input_schema = input_schema,
        .output_schema = output_schema,
        .execution = @enumFromInt(desc.execution),
        .process = dllShim,
        .user_data = @constCast(@ptrCast(process_fn)),
    });

    return .{
        .name = try std.ascii.allocLowerString(allocator, name),
        .lib = lib,
        .source = try allocator.dupe(u8, source),
    };
}

fn descCols(arena: Allocator, descs: []const ColDesc) ![]types.Column {
    const cols = try arena.alloc(types.Column, descs.len);
    for (descs, cols) |d, *c| {
        if (d.type_tag == 0 or d.type_tag > @intFromEnum(types.TypeTag.uuid)) {
            return Error.FunctionInvalidDefinition;
        }
        const tag: types.TypeTag = @enumFromInt(d.type_tag);
        c.* = .{
            .name = try arena.dupe(u8, d.name_ptr[0..d.name_len]),
            .type = switch (tag) {
                .varchar => .{ .varchar = 0 },
                .char => .{ .char = 0 },
                .decimal64, .decimal128 => return Error.FunctionInvalidDefinition,
                inline else => |t| @unionInit(types.Type, @tagName(t), {}),
            },
            .nullable = d.nullable,
        };
    }
    return cols;
}
