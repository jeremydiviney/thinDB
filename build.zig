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

    const test_step = b.step("test", "Run unit + integration tests");
    test_step.dependOn(&run_lib_tests.step);
    test_step.dependOn(&run_integration_tests.step);

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
}
