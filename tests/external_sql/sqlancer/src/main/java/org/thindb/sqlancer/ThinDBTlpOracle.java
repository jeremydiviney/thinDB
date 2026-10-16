package org.thindb.sqlancer;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import sqlancer.common.oracle.TestOracle;

final class ThinDBTlpOracle implements TestOracle {
    static final Gson JSON = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();

    record Cell(String kind, String value) { }
    record Result(List<String> columns, List<List<Cell>> rows) { }
    record Reproduction(int format, long seed, int check, List<String> setup, List<String> queries) { }
    record Check(int index, List<String> queries, String fingerprint) { }

    static final class Mismatch extends AssertionError {
        final String kind;
        final List<Result> results;

        Mismatch(String kind, List<Result> results) {
            super("TLP " + kind + " mismatch: " + JSON.toJson(results));
            this.kind = kind;
            this.results = results;
        }
    }

    @FunctionalInterface
    interface Query {
        Result execute(String sql) throws Exception;
    }

    private final ThinDBProvider.State state;

    ThinDBTlpOracle(ThinDBProvider.State state) {
        this.state = state;
    }

    static Result read(ResultSet rows) throws SQLException {
        var metadata = rows.getMetaData();
        List<String> columns = new ArrayList<>();
        for (int c = 1; c <= metadata.getColumnCount(); c++) {
            columns.add(switch (metadata.getColumnType(c)) {
                case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT -> "integer";
                case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR -> "text";
                default -> throw new SQLException("Unexpected generated result type: " + metadata.getColumnTypeName(c));
            });
        }
        List<List<Cell>> values = new ArrayList<>();
        while (rows.next()) {
            if (values.size() >= 10000) {
                throw new SQLException("Generated result exceeded the row limit");
            }
            List<Cell> row = new ArrayList<>();
            for (int c = 1; c <= columns.size(); c++) {
                String value = rows.getString(c);
                if (rows.wasNull()) {
                    row.add(new Cell("null", null));
                } else {
                    String kind = columns.get(c - 1);
                    row.add(new Cell(kind, kind.equals("integer") ? new BigInteger(value).toString() : value));
                }
            }
            values.add(List.copyOf(row));
        }
        return new Result(List.copyOf(columns), List.copyOf(values));
    }

    private static Map<List<Cell>, Long> multiset(List<List<Cell>> rows) {
        Map<List<Cell>, Long> result = new HashMap<>();
        for (List<Cell> row : rows) {
            result.merge(row, 1L, Long::sum);
        }
        return result;
    }

    static void compare(List<Result> results) {
        if (results.size() != 4) {
            throw new IllegalArgumentException("TLP requires an original query and three partitions");
        }
        Result original = results.getFirst();
        List<List<Cell>> combined = new ArrayList<>();
        for (Result result : results) {
            if (!original.columns().equals(result.columns())
                    || result.rows().stream().anyMatch(row -> row.size() != result.columns().size())) {
                throw new Mismatch("shape", results);
            }
        }
        for (int i = 1; i < 4; i++) {
            combined.addAll(results.get(i).rows());
        }
        if (!multiset(original.rows()).equals(multiset(combined))) {
            throw new Mismatch("rows", results);
        }
    }

    static List<Result> evaluate(List<String> queries, Query query) throws Exception {
        if (queries.size() != 4 || queries.stream().anyMatch(sql -> !sql.startsWith("SELECT "))) {
            throw new IllegalArgumentException("Replay requires four SELECT queries");
        }
        List<Result> results = new ArrayList<>();
        for (String sql : queries) {
            results.add(query.execute(sql));
        }
        compare(results);
        return results;
    }

    static String fingerprint(Result result) throws Exception {
        List<String> rows = result.rows().stream().map(JSON::toJson).sorted().toList();
        String canonical = JSON.toJson(result.columns()) + "\n" + String.join("\n", rows);
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8)));
    }

    static String sql(Reproduction reproduction) {
        return "-- Original query, then TRUE / FALSE / NULL partitions; compare as row multisets.\n"
                + String.join(";\n", reproduction.setup()) + ";\n\n"
                + String.join(";\n", reproduction.queries()) + ";\n";
    }

    @Override
    public void check() throws Exception {
        var tables = state.getSchema().getDatabaseTables();
        var table = tables.get(state.getRandomly().getInteger(0, tables.size()));
        List<String> queries = new ThinDBGenerator(state.getRandomly(), table).queries();
        state.current = new Reproduction(1, state.getRandomly().getSeed(), state.attempted++, List.copyOf(state.setup), queries);
        Files.writeString(state.output.resolve("replay.json"), JSON.toJson(state.current));
        List<Result> results = evaluate(queries, state::query);
        state.cases.write(JSON.toJson(new Check(state.completed, queries, fingerprint(results.getFirst()))) + "\n");
        state.cases.flush();
        state.completed++;
    }

    static Reproduction reduce(Reproduction original, String kind, int attempts, Query query,
            ThinDBProvider.State state) throws Exception {
        List<String> retained = new ArrayList<>(original.setup());
        List<String> tables = retained.stream().filter(sql -> sql.startsWith("CREATE TABLE t"))
                .map(sql -> sql.split(" ")[2]).toList();
        if (tables.isEmpty() || tables.stream().anyMatch(table -> !table.matches("t[0-9]+"))) {
            throw new IllegalArgumentException("Reduction only supports generated tables");
        }
        long deadline = System.nanoTime() + 30_000_000_000L;
        int remaining = attempts;
        for (int chunk = Math.max(1, retained.size() / 2); chunk >= 1; chunk /= 2) {
            for (int start = 0; start < retained.size() && remaining > 0 && System.nanoTime() < deadline; start += chunk) {
                List<String> candidate = new ArrayList<>(retained);
                int end = Math.min(start + chunk, candidate.size());
                boolean removed = false;
                for (int i = end - 1; i >= start; i--) {
                    if (candidate.get(i).startsWith("INSERT INTO ")) {
                        candidate.remove(i);
                        removed = true;
                    }
                }
                if (!removed) {
                    continue;
                }
                remaining--;
                for (String table : tables) {
                    state.execute("DROP TABLE IF EXISTS " + table);
                }
                for (String sql : candidate) {
                    state.execute(sql);
                }
                try {
                    evaluate(original.queries(), query);
                } catch (Mismatch mismatch) {
                    if (mismatch.kind.equals(kind)) {
                        retained = candidate;
                        Reproduction reduced = new Reproduction(1, original.seed(), original.check(), retained, original.queries());
                        Files.writeString(state.output.resolve("reduced.json"), JSON.toJson(reduced));
                        Files.writeString(state.output.resolve("reduced.sql"), sql(reduced));
                    }
                }
            }
        }
        return new Reproduction(1, original.seed(), original.check(), retained, original.queries());
    }
}
