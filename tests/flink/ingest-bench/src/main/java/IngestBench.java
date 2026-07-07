import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.SerializableSupplier;
import com.mysql.cj.jdbc.MysqlXADataSource;

import javax.sql.XADataSource;

// MySQL src.events -> Flink -> thinDB main.events throughput bench.
//   args[0] = "at-least-once" | "exactly-once"   args[1] = row count
public class IngestBench {
    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "at-least-once";
        int n = args.length > 1 ? Integer.parseInt(args[1]) : 1_000_000;
        int par = 2;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(par);
        env.enableCheckpointing(5000); // exactly-once commits fire on checkpoint

        RowTypeInfo rowType = new RowTypeInfo(
                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        JdbcInputFormat in = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl("jdbc:mysql://mysql:3306/src")
                .setUsername("root").setPassword("root")
                .setQuery("SELECT id,a,b,c,d,e FROM events WHERE id BETWEEN ? AND ?")
                .setRowTypeInfo(rowType)
                .setParametersProvider(new JdbcNumericBetweenParametersProvider(1, n).ofBatchNum(par * 4))
                .finish();
        DataStream<Row> src = env.createInput(in, rowType);

        String sql = "INSERT INTO events (id,a,b,c,d,e) VALUES (?,?,?,?,?,?)";
        JdbcStatementBuilder<Row> stmt = (ps, r) -> {
            ps.setInt(1, (int) r.getField(0));
            ps.setInt(2, (int) r.getField(1));
            ps.setLong(3, (long) r.getField(2));
            ps.setString(4, (String) r.getField(3));
            ps.setDouble(5, (double) r.getField(4));
            ps.setString(6, (String) r.getField(5));
        };
        JdbcExecutionOptions exec = JdbcExecutionOptions.builder()
                .withBatchSize(1000).withBatchIntervalMs(0).withMaxRetries(0).build();

        if (mode.equals("exactly-once")) {
            SerializableSupplier<XADataSource> xaSupplier = () -> {
                MysqlXADataSource ds = new MysqlXADataSource();
                try {
                    ds.setUrl("jdbc:mysql://host.docker.internal:13306/main?rewriteBatchedStatements=true");
                    ds.setUser("root");
                    ds.setPassword("");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return ds;
            };
            src.addSink(JdbcSink.exactlyOnceSink(sql, stmt, exec,
                    JdbcExactlyOnceOptions.builder()
                            .withTransactionPerConnection(true) // MySQL XA: one txn per connection
                            .build(),
                    xaSupplier)).name("thindb-exactly-once");
        } else {
            src.addSink(JdbcSink.sink(sql, stmt, exec,
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl("jdbc:mysql://host.docker.internal:13306/main?rewriteBatchedStatements=true")
                            .withDriverName("com.mysql.cj.jdbc.Driver")
                            .withUsername("root").withPassword("")
                            .build())).name("thindb-at-least-once");
        }

        long t0 = System.currentTimeMillis();
        env.execute("ingest-bench-" + mode);
        double secs = (System.currentTimeMillis() - t0) / 1000.0;
        System.out.printf("BENCH mode=%s rows=%d wall=%.1fs rate=%.0f rows/s%n", mode, n, secs, n / secs);
    }
}
