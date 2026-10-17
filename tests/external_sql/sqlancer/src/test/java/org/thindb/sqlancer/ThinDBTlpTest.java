package org.thindb.sqlancer;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Types;
import java.sql.Connection;
import java.sql.Statement;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import sqlancer.SQLConnection;
import sqlancer.Randomly;
import org.thindb.sqlancer.ThinDBProvider.Column;
import org.thindb.sqlancer.ThinDBProvider.Table;
import org.thindb.sqlancer.ThinDBProvider.Type;
import org.thindb.sqlancer.ThinDBTlpOracle.Cell;
import org.thindb.sqlancer.ThinDBTlpOracle.Mismatch;
import org.thindb.sqlancer.ThinDBTlpOracle.Result;

class ThinDBTlpTest {
    private static Result values(String... values) {
        List<List<Cell>> rows = new ArrayList<>();
        for (String value : values) {
            rows.add(List.of(new Cell(value == null ? "null" : "text", value)));
        }
        return new Result(List.of("text"), rows);
    }

    @Test
    void reorderedPartitionsRetainDuplicates() {
        ThinDBTlpOracle.compare(List.of(values("a", "b", "a", null), values("b", "a"), values("a"), values((String) null)));
        assertThrows(Mismatch.class, () -> ThinDBTlpOracle.compare(List.of(values("a", "a"), values("a"), values(), values())));
        assertThrows(Mismatch.class, () -> ThinDBTlpOracle.compare(List.of(values("a"), values("a", "a"), values(), values())));
    }

    @Test
    void nullEmptyWhitespaceAndLiteralNullAreDistinct() {
        for (String wrong : new String[] {"", " ", "NULL", "null"}) {
            assertThrows(Mismatch.class, () -> ThinDBTlpOracle.compare(List.of(values((String) null), values(wrong), values(), values())));
        }
        assertThrows(Mismatch.class, () -> ThinDBTlpOracle.compare(List.of(values(" a "), values("a"), values(), values())));
    }

    @Test
    void wrongValuesAndLargeIntegersAreDetected() {
        Result large = new Result(List.of("integer"), List.of(List.of(new Cell("integer", "9007199254740993"))));
        Result rounded = new Result(List.of("integer"), List.of(List.of(new Cell("integer", "9007199254740992"))));
        Result empty = new Result(List.of("integer"), List.of());
        assertThrows(Mismatch.class, () -> ThinDBTlpOracle.compare(List.of(large, rounded, empty, empty)));
    }

    @Test
    void emptyResultsStillCheckColumnCountsAndTypes() {
        assertThrows(Mismatch.class, () -> ThinDBTlpOracle.compare(List.of(values(),
                new Result(List.of("text", "text"), List.of()), values(), values())));
        assertThrows(Mismatch.class, () -> ThinDBTlpOracle.compare(List.of(values(),
                new Result(List.of("integer"), List.of()), values(), values())));
    }

    @Test
    void rowBoundariesAndCellBoundariesCannotCollide() {
        Result original = new Result(List.of("text", "text"), List.of(List.of(new Cell("text", "a|b"), new Cell("text", "c"))));
        Result changed = new Result(List.of("text", "text"), List.of(List.of(new Cell("text", "a"), new Cell("text", "b|c"))));
        Result empty = new Result(original.columns(), List.of());
        assertThrows(Mismatch.class, () -> ThinDBTlpOracle.compare(List.of(original, changed, empty, empty)));
    }

    @Test
    void errorsConnectionLossAndTimeoutsAbortImmediately() {
        List<String> queries = List.of("SELECT 1", "SELECT 2", "SELECT 3", "SELECT 4");
        for (SQLException failure : List.of(new SQLException("syntax", "42000"),
                new SQLException("disconnected", "08006"), new SQLTimeoutException("deadline"))) {
            AtomicInteger calls = new AtomicInteger();
            Exception actual = assertThrows(SQLException.class, () -> ThinDBTlpOracle.evaluate(queries, sql -> {
                calls.incrementAndGet();
                throw failure;
            }));
            assertSame(failure, actual);
            assertEquals(1, calls.get());
        }
    }

    private static List<List<String>> generated(long seed) {
        var table = new Table("t0", List.of(new Column("c0", Type.INTEGER), new Column("c1", Type.TEXT)));
        var generator = new ThinDBGenerator(new Randomly(seed), table);
        List<List<String>> queries = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            queries.add(generator.queries());
        }
        return queries;
    }

    @Test
    void seedsReplayExactlyAndGenerateDifferentWorkloads() {
        assertEquals(generated(42), generated(42));
        assertNotEquals(generated(42), generated(43));
        assertTrue(generated(42).stream().allMatch(queries -> queries.size() == 4 && queries.get(3).endsWith("IS NULL")));
        assertEquals("'a''b'", ThinDBGenerator.literal("a'b"));
    }

    @Test
    void fingerprintsIgnoreOrderingButPreserveMultiplicityAndShape() throws Exception {
        assertEquals(ThinDBTlpOracle.fingerprint(values("a", "b")), ThinDBTlpOracle.fingerprint(values("b", "a")));
        assertNotEquals(ThinDBTlpOracle.fingerprint(values("a", "a")), ThinDBTlpOracle.fingerprint(values("a")));
        assertNotEquals(ThinDBTlpOracle.fingerprint(values()),
                ThinDBTlpOracle.fingerprint(new Result(List.of("text", "text"), List.of())));
    }

    @Test
    void jdbcReadingPreservesExactValuesAndEmptyMetadata() throws Exception {
        var metadata = new RowSetMetaDataImpl();
        metadata.setColumnCount(2);
        metadata.setColumnType(1, Types.BIGINT);
        metadata.setColumnType(2, Types.VARCHAR);
        try (var rows = RowSetProvider.newFactory().createCachedRowSet()) {
            rows.setMetaData(metadata);
            assertEquals(List.of("integer", "text"), ThinDBTlpOracle.read(rows).columns());
            rows.moveToInsertRow();
            rows.updateString(1, "9007199254740993");
            rows.updateString(2, "  a  ");
            rows.insertRow();
            rows.moveToCurrentRow();
            rows.beforeFirst();
            assertEquals(List.of(new Cell("integer", "9007199254740993"), new Cell("text", "  a  ")),
                    ThinDBTlpOracle.read(rows).rows().getFirst());
        }
    }

    @Test
    void reducerRemovesIrrelevantRowsAndKeepsTheSameFailure(@TempDir Path output) throws Exception {
        List<String> stored = new ArrayList<>();
        Statement statement = (Statement) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {Statement.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("execute")) {
                        String sql = (String) args[0];
                        if (sql.startsWith("DROP TABLE")) stored.clear();
                        if (sql.startsWith("INSERT")) stored.add(sql);
                        return false;
                    }
                    return null;
                });
        Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {Connection.class},
                (proxy, method, args) -> method.getName().equals("createStatement") ? statement : null);
        var state = new ThinDBProvider.State();
        state.setConnection(new SQLConnection(connection));
        state.setDbmsSpecificOptions(new ThinDBProvider.Options());
        state.output = output;
        List<String> setup = List.of("CREATE TABLE t0 (c0 INT)", "INSERT INTO t0 VALUES (1)",
                "INSERT INTO t0 VALUES (7)", "INSERT INTO t0 VALUES (9)");
        List<String> queries = List.of("SELECT original", "SELECT true_partition", "SELECT false_partition", "SELECT null_partition");
        var original = new ThinDBTlpOracle.Reproduction(1, 7, 3, setup, queries);
        try (var trace = Files.newBufferedWriter(output.resolve("trace.sql"))) {
            state.trace = trace;
            var reduced = ThinDBTlpOracle.reduce(original, "rows", 64,
                    sql -> sql.equals("SELECT original") && stored.contains("INSERT INTO t0 VALUES (7)") ? values("7") : values(), state);
            assertEquals(List.of(setup.get(0), setup.get(2)), reduced.setup());
            assertEquals(original.queries(), reduced.queries());
            assertEquals(original.seed(), reduced.seed());
            assertTrue(Files.exists(output.resolve("reduced.json")));
        }
    }
}
