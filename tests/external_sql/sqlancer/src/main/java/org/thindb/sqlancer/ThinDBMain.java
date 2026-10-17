package org.thindb.sqlancer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import sqlancer.Randomly;

public final class ThinDBMain {
    private ThinDBMain() { }

    public static void main(String[] arguments) throws Exception {
        Map<String, String> args = new HashMap<>();
        List<String> allowed = List.of("--output", "--seed", "--queries", "--max-rows", "--query-timeout", "--reduce-attempts", "--replay");
        for (int i = 0; i < arguments.length; i += 2) {
            if (i + 1 >= arguments.length || !allowed.contains(arguments[i]) || args.put(arguments[i], arguments[i + 1]) != null) {
                throw new IllegalArgumentException("Unknown, duplicate, or incomplete option: " + arguments[i]);
            }
        }
        Path output = Path.of(args.get("--output"));
        Files.createDirectories(output);
        long seed = Long.parseLong(args.getOrDefault("--seed", "0"));
        var options = new ThinDBProvider.Options();
        options.queries = bounded(args, "--queries", 100, 1, 100000);
        options.maxRows = bounded(args, "--max-rows", 32, 0, 256);
        options.queryTimeout = bounded(args, "--query-timeout", 10, 1, 300);
        options.reductionAttempts = bounded(args, "--reduce-attempts", 64, 0, 1000);
        var state = new ThinDBProvider.State();
        state.output = output;
        state.setRandomly(new Randomly(seed));
        state.setDbmsSpecificOptions(options);
        state.setDatabaseName("main");
        var provider = new ThinDBProvider();
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("sqlancer", "2.0.0");
        report.put("provider", "thindb");
        report.put("oracle", "TLP_WHERE");
        report.put("seed", seed);
        report.put("requested", args.containsKey("--replay") ? 1 : options.queries);
        report.put("java", System.getProperty("java.version"));
        long started = System.nanoTime();
        boolean passed = false;
        try (var trace = Files.newBufferedWriter(output.resolve("statements.sql"));
                var cases = Files.newBufferedWriter(output.resolve("checks.jsonl"))) {
            state.trace = trace;
            state.cases = cases;
            try (var connection = provider.createDatabase(state)) {
                state.setConnection(connection);
                try {
                    if (args.containsKey("--replay")) {
                        var reproduction = ThinDBTlpOracle.JSON.fromJson(Files.readString(Path.of(args.get("--replay"))),
                                ThinDBTlpOracle.Reproduction.class);
                        if (reproduction.format() != 1 || reproduction.setup() == null || reproduction.queries() == null) {
                            throw new IllegalArgumentException("Invalid reproduction format");
                        }
                        state.current = reproduction;
                        Files.writeString(output.resolve("replay.json"), ThinDBTlpOracle.JSON.toJson(reproduction));
                        for (String sql : reproduction.setup()) {
                            state.setup(sql);
                        }
                        state.attempted++;
                        var result = ThinDBTlpOracle.evaluate(reproduction.queries(), state::query);
                        state.cases.write(ThinDBTlpOracle.JSON.toJson(new ThinDBTlpOracle.Check(reproduction.check(),
                                reproduction.queries(), ThinDBTlpOracle.fingerprint(result.getFirst()))) + "\n");
                        state.completed++;
                    } else {
                        provider.generateAndTestDatabase(state);
                    }
                    passed = true;
                } catch (Exception | AssertionError failure) {
                    if (state.current != null) {
                        Files.writeString(output.resolve("repro.sql"), ThinDBTlpOracle.sql(state.current));
                    }
                    Map<String, Object> diagnostic = new LinkedHashMap<>();
                    diagnostic.put("error", failure.toString());
                    diagnostic.put("reproduction", state.current);
                    if (failure instanceof ThinDBTlpOracle.Mismatch mismatch) {
                        diagnostic.put("results", mismatch.results);
                    }
                    // Preserve the original evidence before a deadline can interrupt reduction.
                    Files.writeString(output.resolve("failure.json"), ThinDBTlpOracle.JSON.toJson(diagnostic));
                    if (failure instanceof ThinDBTlpOracle.Mismatch mismatch) {
                        report.put("results", mismatch.results);
                        if (options.reductionAttempts > 0) {
                            try {
                                var reduced = ThinDBTlpOracle.reduce(state.current, mismatch.kind,
                                        options.reductionAttempts, state::query, state);
                                Files.writeString(output.resolve("reduced.json"), ThinDBTlpOracle.JSON.toJson(reduced));
                                Files.writeString(output.resolve("reduced.sql"), ThinDBTlpOracle.sql(reduced));
                            } catch (Exception | AssertionError reductionFailure) {
                                report.put("reduction_error", reductionFailure.toString());
                            }
                        }
                    }
                    throw failure;
                }
            }
        } catch (Exception | AssertionError failure) {
            passed = false;
            StringWriter stack = new StringWriter();
            failure.printStackTrace(new PrintWriter(stack));
            report.put("error", stack.toString());
            System.err.println(failure);
        } finally {
            report.put("status", passed ? "passed" : "failed");
            report.put("attempted", state.attempted);
            report.put("completed", state.completed);
            report.put("seconds", (System.nanoTime() - started) / 1e9);
            Files.writeString(output.resolve("provider.json"), ThinDBTlpOracle.JSON.toJson(report));
        }
        System.out.printf("seed %d: %d/%d TLP checks %s%n", seed, state.completed, options.queries, passed ? "passed" : "failed");
        if (!passed) {
            System.exit(1);
        }
    }

    private static int bounded(Map<String, String> args, String key, int fallback, int min, int max) {
        int value = Integer.parseInt(args.getOrDefault(key, Integer.toString(fallback)));
        if (value < min || value > max) {
            throw new IllegalArgumentException(key + " must be between " + min + " and " + max);
        }
        return value;
    }
}
