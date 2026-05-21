const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ---- zstd C library (allyourcodebase/zstd via build.zig.zon) ----
    const zstd_dep = b.dependency("zstd", .{
        .target = target,
        .optimize = optimize,
    });

    // ---- thinDB library module ----
    const thindb_mod = b.addModule("thindb", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    thindb_mod.linkLibrary(zstd_dep.artifact("zstd"));

    // ---- Library artifact (so we have something to `zig build`) ----
    const lib = b.addLibrary(.{
        .name = "thindb",
        .root_module = thindb_mod,
        .linkage = .static,
    });
    b.installArtifact(lib);

    // ---- Demo executable: src/main.zig — used as a smoke-test runner ----
    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe_mod.addImport("thindb", thindb_mod);

    const exe = b.addExecutable(.{
        .name = "thindb_demo",
        .root_module = exe_mod,
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    const run_step = b.step("run", "Run the demo executable");
    run_step.dependOn(&run_cmd.step);

    // ---- Unit tests: every `test` block in src/root.zig and its imports ----
    const lib_tests = b.addTest(.{ .root_module = thindb_mod });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    // ---- Integration tests: tests/integration/all.zig pulls in scenario files ----
    const integration_mod = b.createModule(.{
        .root_source_file = b.path("tests/integration/all.zig"),
        .target = target,
        .optimize = optimize,
    });
    integration_mod.addImport("thindb", thindb_mod);
    const integration_tests = b.addTest(.{ .root_module = integration_mod });
    const run_integration_tests = b.addRunArtifact(integration_tests);

    // ---- Client/server integration tests: tests/integration_client/all.zig ----
    // Exercises the new thindb.local() / Connection / ClientQuery surface
    // that will eventually back the TCP transport. Kept separate from the
    // existing integration tests so the long-standing library-level API
    // tests stay unchanged.
    const integration_client_mod = b.createModule(.{
        .root_source_file = b.path("tests/integration_client/all.zig"),
        .target = target,
        .optimize = optimize,
    });
    integration_client_mod.addImport("thindb", thindb_mod);
    const integration_client_tests = b.addTest(.{ .root_module = integration_client_mod });
    const run_integration_client_tests = b.addRunArtifact(integration_client_tests);

    const test_step = b.step("test", "Run unit + integration tests");
    test_step.dependOn(&run_lib_tests.step);
    test_step.dependOn(&run_integration_tests.step);
    test_step.dependOn(&run_integration_client_tests.step);

    // ---- Benchmarks: bench/main.zig ----------------------------------------
    const bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    bench_mod.addImport("thindb", thindb_mod);

    const bench_exe = b.addExecutable(.{
        .name = "thindb_bench",
        .root_module = bench_mod,
    });
    b.installArtifact(bench_exe);

    const run_bench = b.addRunArtifact(bench_exe);
    run_bench.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_bench.addArgs(args);
    const bench_step = b.step("bench", "Run benchmarks (recommended: -Doptimize=ReleaseFast)");
    bench_step.dependOn(&run_bench.step);

    // ---- ClickBench loader: bench/clickbench/main.zig ----------------------
    const clickbench_mod = b.createModule(.{
        .root_source_file = b.path("bench/clickbench/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    clickbench_mod.addImport("thindb", thindb_mod);

    const clickbench_exe = b.addExecutable(.{
        .name = "thindb_clickbench",
        .root_module = clickbench_mod,
    });
    b.installArtifact(clickbench_exe);

    const run_clickbench = b.addRunArtifact(clickbench_exe);
    run_clickbench.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_clickbench.addArgs(args);
    const clickbench_step = b.step("clickbench", "Load ClickBench TSV into a fresh DB (recommended: -Doptimize=ReleaseFast). First arg = TSV path, second arg = max rows.");
    clickbench_step.dependOn(&run_clickbench.step);

    // ---- ClickBench probe: bench/clickbench/probe.zig --------------------
    const probe_mod = b.createModule(.{
        .root_source_file = b.path("bench/clickbench/probe.zig"),
        .target = target,
        .optimize = optimize,
    });
    probe_mod.addImport("thindb", thindb_mod);
    const probe_exe = b.addExecutable(.{
        .name = "thindb_probe",
        .root_module = probe_mod,
    });
    b.installArtifact(probe_exe);
    const run_probe = b.addRunArtifact(probe_exe);
    run_probe.step.dependOn(b.getInstallStep());
    const probe_step = b.step("probe", "Open .clickbench-db and dump discovered databases/schemas");
    probe_step.dependOn(&run_probe.step);

    // ---- thindb-server executable: standalone multi-wire server -------------
    const server_mod = b.createModule(.{
        .root_source_file = b.path("src/cmd/server.zig"),
        .target = target,
        .optimize = optimize,
    });
    server_mod.addImport("thindb", thindb_mod);

    const server_exe = b.addExecutable(.{
        .name = "thindb-server",
        .root_module = server_mod,
    });
    b.installArtifact(server_exe);

    const run_server = b.addRunArtifact(server_exe);
    run_server.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_server.addArgs(args);
    const server_step = b.step("server", "Run thindb-server (pass --data-dir etc via -- ...)");
    server_step.dependOn(&run_server.step);
}
