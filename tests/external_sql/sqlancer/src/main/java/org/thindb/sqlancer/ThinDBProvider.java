package org.thindb.sqlancer;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.TableIndex;

public final class ThinDBProvider extends SQLProviderAdapter<ThinDBProvider.State, ThinDBProvider.Options> {
    public ThinDBProvider() {
        super(State.class, Options.class);
    }

    public enum Oracle implements OracleFactory<State> {
        TLP_WHERE;

        @Override
        public TestOracle create(State state) {
            return new ThinDBTlpOracle(state);
        }
    }

    public static final class Options implements DBMSSpecificOptions<Oracle> {
        int queries = 100;
        int maxRows = 32;
        int queryTimeout = 10;
        int reductionAttempts = 64;

        @Override
        public List<Oracle> getTestOracleFactory() {
            return List.of(Oracle.TLP_WHERE);
        }
    }

    public enum Type { INTEGER, TEXT }

    public static final class Column extends AbstractTableColumn<Table, Type> {
        public Column(String name, Type type) {
            super(name, null, type);
        }
    }

    public static final class Table extends AbstractTable<Column, TableIndex, State> {
        public Table(String name, List<Column> columns) {
            super(name, columns, List.of(), false);
            columns.forEach(c -> c.setTable(this));
        }

        @Override
        public long getNrRows(State state) {
            try {
                return Long.parseLong(state.query("SELECT COUNT(*) FROM " + getName()).rows().getFirst().getFirst().value());
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public static final class Schema extends AbstractSchema<State, Table> {
        public Schema(List<Table> tables) {
            super(tables);
        }
    }

    public static final class State extends SQLGlobalState<Options, Schema> {
        final List<Table> tables = new ArrayList<>();
        final List<String> setup = new ArrayList<>();
        Path output;
        BufferedWriter trace;
        BufferedWriter cases;
        int completed;
        int attempted;
        ThinDBTlpOracle.Reproduction current;

        @Override
        protected Schema readSchema() {
            // The provider owns all DDL; avoiding information_schema keeps generation independent of metadata extensions.
            return new Schema(tables);
        }

        void log(String sql) throws Exception {
            trace.write(sql + ";\n");
            trace.flush();
        }

        void execute(String sql) throws Exception {
            log(sql);
            try (var statement = getConnection().createStatement()) {
                statement.setQueryTimeout(getDbmsSpecificOptions().queryTimeout);
                statement.execute(sql);
            }
        }

        ThinDBTlpOracle.Result query(String sql) throws Exception {
            log(sql);
            try (var statement = getConnection().createStatement()) {
                statement.setQueryTimeout(getDbmsSpecificOptions().queryTimeout);
                try (var result = statement.executeQuery(sql)) {
                    return ThinDBTlpOracle.read(result);
                }
            }
        }

        void setup(String sql) throws Exception {
            setup.add(sql);
            Files.writeString(output.resolve("setup.sql"), String.join(";\n", setup) + ";\n");
            execute(sql);
        }
    }

    @Override
    public String getDBMSName() {
        return "thindb";
    }

    @Override
    public SQLConnection createDatabase(State state) throws SQLException {
        int port = Integer.parseInt(System.getenv("THINDB_SQLANCER_PORT"));
        String password = System.getenv("THINDB_SQLANCER_PASSWORD");
        if (port <= 0 || port > 65535 || port == 13310 || password == null
                || !password.startsWith("thindb-external-sql-")) {
            throw new IllegalArgumentException("Use the disposable-server launcher; production endpoints are forbidden");
        }
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", password);
        properties.setProperty("sslMode", "DISABLED");
        properties.setProperty("allowPublicKeyRetrieval", "false");
        properties.setProperty("connectTimeout", "5000");
        properties.setProperty("socketTimeout", Integer.toString(state.getDbmsSpecificOptions().queryTimeout * 1000));
        properties.setProperty("useServerPrepStmts", "false");
        properties.setProperty("allowMultiQueries", "false");
        return new SQLConnection(DriverManager.getConnection("jdbc:mysql://127.0.0.1:" + port + "/main", properties));
    }

    @Override
    public void generateDatabase(State state) throws Exception {
        Randomly random = state.getRandomly();
        int tableCount = random.getInteger(1, 4);
        for (int t = 0; t < tableCount; t++) {
            List<Column> columns = new ArrayList<>();
            int integers = random.getInteger(1, 4);
            int strings = random.getInteger(1, 3);
            for (int c = 0; c < integers + strings; c++) {
                columns.add(new Column("c" + c, c < integers ? Type.INTEGER : Type.TEXT));
            }
            Table table = new Table("t" + t, columns);
            state.tables.add(table);
            List<String> definitions = new ArrayList<>();
            for (Column column : columns) {
                definitions.add(column.getName() + (column.getType() == Type.TEXT ? " VARCHAR(64)"
                        : random.getInteger(0, 2) == 0 ? " INT" : " BIGINT"));
            }
            state.setup("CREATE TABLE " + table.getName() + " (" + String.join(", ", definitions) + ")");
            int rows = random.getInteger(0, state.getDbmsSpecificOptions().maxRows + 1);
            String previous = null;
            for (int r = 0; r < rows; r++) {
                List<String> values = new ArrayList<>();
                for (Column column : columns) {
                    values.add(r == 0 || random.getInteger(0, 5) == 0 ? "NULL"
                            : column.getType() == Type.INTEGER ? Integer.toString(random.getInteger(-100, 101))
                            : ThinDBGenerator.literal(ThinDBGenerator.text(random)));
                }
                String insert = "INSERT INTO " + table.getName() + " VALUES (" + String.join(", ", values) + ")";
                if (r > 0 && random.getInteger(0, 4) == 0) {
                    insert = previous;
                }
                state.setup(insert);
                previous = insert;
            }
        }
        state.updateSchema();
    }

    @Override
    public void generateAndTestDatabase(State state) throws Exception {
        generateDatabase(state);
        TestOracle oracle = state.getDbmsSpecificOptions().getTestOracleFactory().getFirst().create(state);
        // The bounded runner owns reporting and cleanup. SQLancer's generic loop can suppress IgnoreMeException.
        for (int i = 0; i < state.getDbmsSpecificOptions().queries; i++) {
            oracle.check();
        }
    }
}
