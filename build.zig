const std = @import("std");

/// Run a test binary as a plain process (pass/fail = exit code) instead of
/// the build runner's `--listen` IPC mode: on Windows the IPC read wedges
/// intermittently ("test runner failed to respond") while the exact same
/// binaries pass standalone every time — an unresolved upstream issue
/// (ziggit.dev/t/6079 class: lingering threads interfere with the
/// listen-mode stdin read). Revert to `addRunArtifact` when fixed.
fn runTestStandalone(b: *std.Build, test_exe: *std.Build.Step.Compile) *std.Build.Step.Run {
    const run = std.Build.Step.Run.create(b, b.fmt("run {s}", .{test_exe.name}));
    run.addArtifactArg(test_exe);
    return run;
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ---- zstd C library (allyourcodebase/zstd via build.zig.zon) ----
    const zstd_dep = b.dependency("zstd", .{
        .target = target,
        .optimize = optimize,
    });

    // ---- lz4 C library (allyourcodebase/lz4 via build.zig.zon) ----
    // Always ReleaseFast: the whole point of LZ4-cached string blocks is the
    // multi-GB/s block decompress on the read path; a Debug lz4 would invert
    // every measurement (same reasoning as the zstd Debug-server trap).
    const lz4_dep = b.dependency("lz4", .{
        .target = target,
        .optimize = .ReleaseFast,
    });

    // ---- build options: dev profilers ----
    // `-Dprofiling=true` compiles in the developer-only execution traces
    // (THINDB_V2_PIPELINE_TRACE / _WORKER_PROFILE / _CHUNK_PROFILE and the
    // harness-core timing prints). When false (the default, and every production
    // build) the `comptime build_options.profiling` gates eliminate that code
    // entirely — no branches, no globals, nothing in the binary. The runtime
    // `--profile-ops` operator profiler is a separate product feature and stays.
    const build_opts = b.addOptions();
    build_opts.addOption(bool, "profiling", b.option(bool, "profiling", "compile in developer execution-trace profilers") orelse false);

    // ---- thinDB library module ----
    const thindb_mod = b.addModule("thindb", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    thindb_mod.linkLibrary(zstd_dep.artifact("zstd"));
    thindb_mod.linkLibrary(lz4_dep.artifact("lz4"));
    thindb_mod.addOptions("build_options", build_opts);

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
    const test_filters = b.option([]const []const u8, "test-filter", "only run tests whose name contains this substring") orelse &.{};
    const lib_tests = b.addTest(.{ .root_module = thindb_mod, .filters = test_filters });
    const run_lib_tests = runTestStandalone(b, lib_tests);

    // ---- Integration tests: tests/integration/all.zig pulls in scenario files ----
    const integration_mod = b.createModule(.{
        .root_source_file = b.path("tests/integration/all.zig"),
        .target = target,
        .optimize = optimize,
    });
    integration_mod.addImport("thindb", thindb_mod);
    const integration_tests = b.addTest(.{ .root_module = integration_mod, .filters = test_filters });
    const run_integration_tests = runTestStandalone(b, integration_tests);

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
    const integration_client_tests = b.addTest(.{ .root_module = integration_client_mod, .filters = test_filters });
    const run_integration_client_tests = runTestStandalone(b, integration_client_tests);

    // ---- Server config-file parser tests (src/cmd/config.zig, pure std) ----
    const config_mod = b.createModule(.{
        .root_source_file = b.path("src/cmd/config.zig"),
        .target = target,
        .optimize = optimize,
    });
    const config_tests = b.addTest(.{ .root_module = config_mod, .filters = test_filters });
    const run_config_tests = runTestStandalone(b, config_tests);

    const test_step = b.step("test", "Run unit + integration tests");
    test_step.dependOn(&run_lib_tests.step);
    test_step.dependOn(&run_integration_tests.step);
    test_step.dependOn(&run_integration_client_tests.step);
    test_step.dependOn(&run_config_tests.step);

    // ---- V2-engine integration tests: tests/integration/v2_group_topn_test.zig ----
    // A focused battery for the grouped V2 handlers, kept as its own step so
    // `zig build test-v2` iterates on them without the full suite.
    const v2_integration_mod = b.createModule(.{
        .root_source_file = b.path("tests/integration/v2_group_topn_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    v2_integration_mod.addImport("thindb", thindb_mod);
    const v2_integration_tests = b.addTest(.{ .root_module = v2_integration_mod, .filters = test_filters });
    const run_v2_integration_tests = runTestStandalone(b, v2_integration_tests);
    const test_v2_step = b.step("test-v2", "Run V2-engine integration tests (default engine)");
    test_v2_step.dependOn(&run_v2_integration_tests.step);

    // ---- ReleaseFast thinDB module for performance tooling -----------------
    // Benchmarks (and the ClickBench loader) are only meaningful against
    // production-optimized code, so they always build at ReleaseFast
    // regardless of -Doptimize. A plain `zig build bench` in the default
    // Debug mode would otherwise report ~7x-inflated numbers.
    const fast_zstd = b.dependency("zstd", .{ .target = target, .optimize = .ReleaseFast });
    const thindb_fast = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    thindb_fast.linkLibrary(fast_zstd.artifact("zstd"));
    thindb_fast.linkLibrary(lz4_dep.artifact("lz4"));

    // ---- Benchmarks: bench/main.zig ----------------------------------------
    const bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/main.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    bench_mod.addImport("thindb", thindb_fast);

    const bench_exe = b.addExecutable(.{
        .name = "thindb_bench",
        .root_module = bench_mod,
    });

    // The bench is in-process (it imports thindb_fast directly and spins up
    // any server it needs on a thread). It must NOT depend on the global
    // install step: doing so rebuilds + reinstalls `thindb-server` in the
    // default (Debug) optimize mode, silently clobbering a ReleaseFast
    // server binary in zig-out/bin — which then makes every subsequent
    // wire benchmark ~6x slower. `addRunArtifact` already builds bench_exe.
    const run_bench = b.addRunArtifact(bench_exe);
    if (b.args) |args| run_bench.addArgs(args);
    const bench_step = b.step("bench", "Run benchmarks (always built ReleaseFast)");
    bench_step.dependOn(&run_bench.step);

    // ---- Isolated GROUP BY probe microbench: bench/groupby_micro.zig -------
    // Standalone (imports group_table.zig directly — no thindb module), so it
    // exercises only the high-card probe kernel with zero pipeline confounders.
    const gbmicro_mod = b.createModule(.{
        .root_source_file = b.path("bench/groupby_micro.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    // Import the whole thindb library: the bench reaches the probe kernels via
    // `thindb.exec.group_table` and drives the REAL exec.Aggregate operator on
    // synthetic in-memory batches (the faithful operator-machinery model).
    gbmicro_mod.addImport("thindb", thindb_fast);
    const gbmicro_exe = b.addExecutable(.{ .name = "gbmicro", .root_module = gbmicro_mod });
    const run_gbmicro = b.addRunArtifact(gbmicro_exe);
    if (b.args) |args| run_gbmicro.addArgs(args);
    const gbmicro_step = b.step("gbmicro", "Run the isolated GROUP BY probe microbench (ReleaseFast)");
    gbmicro_step.dependOn(&run_gbmicro.step);

    // ---- Real-data regex throughput microbench: bench/regex_real.zig -------
    // Opens .clickbench-db, pulls the real Referer column into RAM (untimed),
    // then times ONLY the Pike-VM replace loop at 1/2/4/8 threads — isolates
    // raw regex throughput + scaling from the query pipeline.
    const regex_real_mod = b.createModule(.{
        .root_source_file = b.path("bench/regex_real.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    regex_real_mod.addImport("thindb", thindb_fast);
    const regex_real_exe = b.addExecutable(.{ .name = "regex_real", .root_module = regex_real_mod });
    const run_regex_real = b.addRunArtifact(regex_real_exe);
    if (b.args) |args| run_regex_real.addArgs(args);
    const regex_real_step = b.step("regexreal", "Run the real-data regex throughput microbench (ReleaseFast)");
    regex_real_step.dependOn(&run_regex_real.step);

    // ---- Simplified ClientIP GROUP BY top-N pipeline: bench/clientip_pipeline.zig
    // Uses the real scan/decode path, then a purpose-built fixed pipeline:
    // scan -> bucket partition -> bucket-owned GROUP BY -> ORDER BY/LIMIT top-N.
    const clientip_mod = b.createModule(.{
        .root_source_file = b.path("bench/clientip_pipeline.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    clientip_mod.addImport("thindb", thindb_fast);
    const clientip_exe = b.addExecutable(.{ .name = "clientip_pipeline", .root_module = clientip_mod });
    const run_clientip = b.addRunArtifact(clientip_exe);
    if (b.args) |args| run_clientip.addArgs(args);
    const clientip_step = b.step("clientip", "Run the simplified ClientIP GROUP BY top-N pipeline benchmark (ReleaseFast)");
    clientip_step.dependOn(&run_clientip.step);

    // ---- ClickBench loader: bench/clickbench/main.zig ----------------------
    const clickbench_mod = b.createModule(.{
        .root_source_file = b.path("bench/clickbench/main.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    clickbench_mod.addImport("thindb", thindb_fast);

    const clickbench_exe = b.addExecutable(.{
        .name = "thindb_clickbench",
        .root_module = clickbench_mod,
    });

    // Same reasoning as `bench` above: don't pull in the global install step,
    // which would reinstall a Debug `thindb-server` over a ReleaseFast one.
    const run_clickbench = b.addRunArtifact(clickbench_exe);
    if (b.args) |args| run_clickbench.addArgs(args);
    const clickbench_step = b.step("clickbench", "Load ClickBench TSV into a fresh DB (always built ReleaseFast). First arg = TSV path, second arg = max rows.");
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

    // ---- segstats: per-row-group stats invariant checker (diagnostic) ----
    const segstats_mod = b.createModule(.{
        .root_source_file = b.path("bench/segstats.zig"),
        .target = target,
        .optimize = optimize,
    });
    segstats_mod.addImport("thindb", thindb_mod);
    const segstats_exe = b.addExecutable(.{
        .name = "segstats",
        .root_module = segstats_mod,
    });
    const run_segstats = b.addRunArtifact(segstats_exe);
    if (b.args) |args| run_segstats.addArgs(args);
    const segstats_step = b.step("segstats", "Verify per-RG footer stats vs decoded values: -- <table_dir> <column>");
    segstats_step.dependOn(&run_segstats.step);

    // ---- compact-table: force compaction sweeps to a fixed point (diagnostic) ----
    const compact_tool_mod = b.createModule(.{
        .root_source_file = b.path("bench/compact_table.zig"),
        .target = target,
        .optimize = optimize,
    });
    compact_tool_mod.addImport("thindb", thindb_mod);
    const compact_tool_exe = b.addExecutable(.{
        .name = "compact-table",
        .root_module = compact_tool_mod,
    });
    const run_compact_tool = b.addRunArtifact(compact_tool_exe);
    if (b.args) |args| run_compact_tool.addArgs(args);
    const compact_tool_step = b.step("compact-table", "Force compaction sweeps on a database dir: -- <database_dir> [table]");
    compact_tool_step.dependOn(&run_compact_tool.step);

    // ---- TVF flagship A/B: bench/tvf_walk_ab.zig (always ReleaseFast) ----
    const tvf_ab_mod = b.createModule(.{
        .root_source_file = b.path("bench/tvf_walk_ab.zig"),
        .target = target,
        .optimize = optimize,
    });
    tvf_ab_mod.addImport("thindb", thindb_mod);
    const tvf_ab_exe = b.addExecutable(.{
        .name = "tvf_walk_ab",
        .root_module = tvf_ab_mod,
    });
    b.installArtifact(tvf_ab_exe);
    const run_tvf_ab = b.addRunArtifact(tvf_ab_exe);
    run_tvf_ab.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_tvf_ab.addArgs(args);
    const tvf_ab_step = b.step("tvf-walk-ab", "Rollforward gap-fill walk: table UDF vs 6-CTE chain, values + wall-clock (pass -Doptimize=ReleaseFast)");
    tvf_ab_step.dependOn(&run_tvf_ab.step);

    // ---- rf_custom: purpose-built wayroll rollforward pipeline (task #183) ----
    const rf_custom_mod = b.createModule(.{
        .root_source_file = b.path("bench/rf_custom.zig"),
        .target = target,
        .optimize = optimize,
    });
    rf_custom_mod.addImport("thindb", thindb_mod);
    const rf_custom_exe = b.addExecutable(.{
        .name = "rf_custom",
        .root_module = rf_custom_mod,
    });
    const rf_custom_step = b.step("rf-custom", "Build the purpose-built rollforward pipeline probe (pass -Doptimize=ReleaseFast)");
    rf_custom_step.dependOn(&b.addInstallArtifact(rf_custom_exe, .{}).step);

    // Region recognizer IR-dump tool (debugging aid for the keyed-region compiler).
    const region_dump_mod = b.createModule(.{
        .root_source_file = b.path("bench/region_dump.zig"),
        .target = target,
        .optimize = optimize,
    });
    region_dump_mod.addImport("thindb", thindb_mod);
    const region_dump_exe = b.addExecutable(.{
        .name = "region_dump",
        .root_module = region_dump_mod,
    });
    const region_dump_step = b.step("region-dump", "Dump post-pass IR for a SQL file (region recognizer input)");
    region_dump_step.dependOn(&b.addInstallArtifact(region_dump_exe, .{}).step);

    // ---- thindb-server executable: standalone multi-wire server -------------
    const server_mod = b.createModule(.{
        .root_source_file = b.path("src/cmd/server.zig"),
        .target = target,
        .optimize = optimize,
        .strip = b.option(bool, "strip", "strip debug info (release bundles)") orelse false,
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

    // ---- dist: install ONLY the server executable ---------------------------
    // Used by scripts/make_dist.mjs to cross-compile release bundles. The
    // default install step drags in bench/harness tools that use host-only
    // APIs; a distribution needs just the server (the UDF SDK is embedded).
    const dist_step = b.step("dist", "Install only thindb-server (for release bundles; pass -Dtarget/-Doptimize)");
    dist_step.dependOn(&b.addInstallArtifact(server_exe, .{}).step);
}
