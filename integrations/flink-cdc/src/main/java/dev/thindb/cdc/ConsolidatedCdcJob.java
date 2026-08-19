package dev.thindb.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Consolidated CDC job: ONE binlog connection carrying every configured table,
 * replacing N per-table SQL jobs (each of which pulled the full binlog
 * firehose over the WAN — the error-surface multiplier behind the 2026-08-10
 * truncated-read incident).
 *
 * Semantics preserved from the per-table SQL jobs: at-least-once, ODKU upserts
 * + keyed DELETEs, parallelism 1, ordered application per binlog order (single
 * sink operator; buffers flushed on size/interval AND on checkpoint barriers).
 *
 * Config: JSON file, path in env CDC_CONFIG or arg[0]. See example-config.json.
 */
public class ConsolidatedCdcJob {

  public static void main(String[] args) throws Exception {
    String cfgPath = args.length > 0 ? args[0] : System.getenv("CDC_CONFIG");
    JsonNode cfg = new ObjectMapper().readTree(Files.readString(Paths.get(cfgPath)));
    JsonNode src = cfg.get("source");
    JsonNode snk = cfg.get("sink");

    List<TableCfg> tables = new ArrayList<>();
    for (JsonNode t : cfg.get("tables")) tables.add(TableCfg.of(t));
    String db = src.get("database").asText();
    String[] tableList = tables.stream().map(t -> db + "." + t.name).toArray(String[]::new);

    Properties jdbcProps = new Properties();
    jdbcProps.setProperty("tcpKeepAlive", "true");
    jdbcProps.setProperty("socketTimeout", "600000");
    Properties dbzProps = new Properties();
    dbzProps.setProperty("decimal.handling.mode", "string");
    dbzProps.setProperty("connect.keep.alive.interval.ms", "30000");

    StartupOptions startup;
    String mode = src.path("startupMode").asText("initial");
    switch (mode) {
      case "timestamp" -> startup = StartupOptions.timestamp(src.get("startupTimestampMs").asLong());
      case "latest-offset" -> startup = StartupOptions.latest();
      case "earliest-offset" -> startup = StartupOptions.earliest();
      default -> startup = StartupOptions.initial();
    }

    MySqlSource<String> source = MySqlSource.<String>builder()
        .hostname(src.get("hostname").asText())
        .port(src.get("port").asInt())
        .username(src.get("username").asText())
        .password(src.get("password").asText())
        .databaseList(db)
        .tableList(tableList)
        .serverId(src.get("serverId").asText())
        .serverTimeZone("UTC")
        .startupOptions(startup)
        .fetchSize(4096)
        .splitSize(65536)
        // Snapshot-phase backfill grows unbounded against a churning source
        // (2026-07-08 TM GC-death) — safe to skip for PK-upsert sinks, where
        // re-delivered rows dedup on key. No effect on timestamp/earliest modes.
        .skipSnapshotBackfill(true)
        .heartbeatInterval(java.time.Duration.ofSeconds(15))
        .connectTimeout(java.time.Duration.ofSeconds(60))
        .jdbcProperties(jdbcProps)
        .debeziumProperties(dbzProps)
        .deserializer(new JsonDebeziumDeserializationSchema())
        .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(30_000);
    env.getCheckpointConfig().setCheckpointTimeout(15 * 60 * 1000);
    env.getCheckpointConfig().setTolerableCheckpointFailureNumber(20);
    env.getCheckpointConfig().setExternalizedCheckpointCleanup(
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql-cdc-consolidated")
        .addSink(new RoutingJdbcSink(
            snk.get("url").asText(), snk.get("username").asText(), snk.get("password").asText(),
            snk.path("flushRows").asInt(2000), snk.path("flushIntervalMs").asLong(1000), tables))
        .name("thindb-routing-sink");

    env.execute("cdc-consolidated");
  }

  /** Per-table config: name, ordered columns with types, pk column names. */
  public static class TableCfg implements Serializable {
    String name;
    List<String> cols = new ArrayList<>();
    List<String> types = new ArrayList<>();
    List<String> pk = new ArrayList<>();

    static TableCfg of(JsonNode n) {
      TableCfg t = new TableCfg();
      t.name = n.get("name").asText();
      for (JsonNode c : n.get("columns")) {
        t.cols.add(c.get("name").asText());
        t.types.add(c.get("type").asText());
      }
      for (JsonNode k : n.get("pk")) t.pk.add(k.asText());
      return t;
    }

    String upsertSql(int rows) {
      String colList = String.join(",", cols.stream().map(c -> "`" + c + "`").toList());
      String group = "(" + String.join(",", Collections.nCopies(cols.size(), "?")) + ")";
      List<String> upd = cols.stream().filter(c -> !pk.contains(c))
          .map(c -> "`" + c + "`=VALUES(`" + c + "`)").toList();
      String updates = upd.isEmpty() ? "`" + pk.get(0) + "`=`" + pk.get(0) + "`" : String.join(",", upd);
      return "INSERT INTO `" + name + "` (" + colList + ") VALUES "
          + String.join(",", Collections.nCopies(rows, group))
          + " ON DUPLICATE KEY UPDATE " + updates;
    }

    String deleteSql() {
      return "DELETE FROM `" + name + "` WHERE "
          + String.join(" AND ", pk.stream().map(c -> "`" + c + "`=?").toList());
    }
  }

  /** One buffered event: which table, upsert-or-delete, extracted values. */
  record Op(String table, boolean delete, Object[] vals) implements Serializable {}

  /**
   * Single sink operator applying events in binlog order. Buffer flushes on
   * size, interval, and checkpoint (snapshotState), so a completed checkpoint
   * implies rows are in thinDB = at-least-once, matching the SQL JDBC sink.
   */
  public static class RoutingJdbcSink extends RichSinkFunction<String> implements CheckpointedFunction {
    private final String url, user, pass;
    private final int flushRows;
    private final long flushIntervalMs;
    private final List<TableCfg> tables;

    private transient Map<String, TableCfg> byName;
    private transient ObjectMapper om;
    private transient Connection conn;
    private transient List<Op> buffer;
    private transient long lastFlush;
    private transient ListState<byte[]> dummyState; // required by the interface; buffer is flushed, never stored

    private static final DateTimeFormatter DT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    RoutingJdbcSink(String url, String user, String pass, int flushRows, long flushIntervalMs,
                    List<TableCfg> tables) {
      this.url = url;
      this.user = user;
      this.pass = pass;
      this.flushRows = flushRows;
      this.flushIntervalMs = flushIntervalMs;
      this.tables = tables;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      om = new ObjectMapper();
      byName = new LinkedHashMap<>();
      for (TableCfg t : tables) byName.put(t.name, t);
      buffer = new ArrayList<>();
      lastFlush = System.currentTimeMillis();
      connect();
    }

    private void connect() throws Exception {
      if (conn != null) try { conn.close(); } catch (Exception ignore) {}
      conn = DriverManager.getConnection(url, user, pass);
      conn.setAutoCommit(true);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
      JsonNode env = om.readTree(value);
      String table = env.path("source").path("table").asText();
      TableCfg t = byName.get(table);
      if (t == null) return; // not a table we sink
      String op = env.path("op").asText();
      if ("d".equals(op)) {
        JsonNode before = env.get("before");
        Object[] vals = new Object[t.pk.size()];
        for (int i = 0; i < t.pk.size(); i++) {
          String col = t.pk.get(i);
          vals[i] = convert(before.get(col), t.types.get(t.cols.indexOf(col)));
        }
        buffer.add(new Op(table, true, vals));
      } else { // c, r, u -> upsert from after image
        JsonNode after = env.get("after");
        if (after == null || after.isNull()) return;
        Object[] vals = new Object[t.cols.size()];
        for (int i = 0; i < t.cols.size(); i++) {
          vals[i] = convert(after.get(t.cols.get(i)), t.types.get(i));
        }
        buffer.add(new Op(table, false, vals));
      }
      if (buffer.size() >= flushRows
          || System.currentTimeMillis() - lastFlush >= flushIntervalMs) {
        flush();
      }
    }

    /** Debezium JSON semantic types -> JDBC values thinDB accepts. */
    static Object convert(JsonNode v, String type) {
      if (v == null || v.isNull()) return null;
      return switch (type) {
        case "INT", "TINYINT" -> v.asInt();
        case "BIGINT" -> v.asLong();
        case "DATE" -> LocalDate.ofEpochDay(v.asLong()).toString(); // io.debezium.time.Date = epoch days
        case "DATETIME" -> { // io.debezium.time.MicroTimestamp = epoch micros (UTC)
          long us = v.asLong();
          LocalDateTime ldt = LocalDateTime.ofInstant(
              Instant.ofEpochSecond(us / 1_000_000, (us % 1_000_000) * 1000), ZoneOffset.UTC);
          yield ldt.format(DT);
        }
        case "DECIMAL" -> v.asText(); // decimal.handling.mode=string
        default -> v.isNumber() ? v.numberValue() : v.asText();
      };
    }

    /**
     * Flush with changelog compaction. The correctness invariant for these
     * last-writer-wins PK tables is PER-KEY order, not global binlog order:
     * within one flush window only the LAST op per (table, pk) determines the
     * row's final state, and cross-key/cross-table order is irrelevant (no FKs,
     * no cross-row constraints). So the buffer compacts to a final image per
     * key and each table applies as at most one DELETE batch plus one
     * multi-VALUES upsert batch.
     *
     * The previous strictly-ordered form (consecutive same-table/same-op runs
     * as separate sequential batches) collapsed to hundreds of tiny round-trips
     * per flush wherever the binlog interleaves tables — fine for at-head
     * trickle, ~600 rows/s during multi-day replays. Compaction also shrinks
     * replay work outright: regeneration waves rewrite the same keys many
     * times, and only the last version is applied.
     */
    private synchronized void flush() throws Exception {
      if (buffer.isEmpty()) { lastFlush = System.currentTimeMillis(); return; }

      // Last op per key wins; LinkedHashMap keeps a stable (first-seen) walk order.
      Map<String, Op> lastByKey = new LinkedHashMap<>();
      for (Op op : buffer) lastByKey.put(keyOf(op), op);

      Map<String, List<Op>> deletes = new LinkedHashMap<>();
      Map<String, List<Op>> upserts = new LinkedHashMap<>();
      for (Op op : lastByKey.values()) {
        (op.delete() ? deletes : upserts).computeIfAbsent(op.table(), k -> new ArrayList<>()).add(op);
      }

      for (int attempt = 0; ; attempt++) {
        try {
          for (TableCfg t : tables) {
            executeDeletes(deletes.get(t.name), t);
            executeUpserts(upserts.get(t.name), t);
          }
          buffer.clear();
          lastFlush = System.currentTimeMillis();
          return;
        } catch (Exception e) {
          if (attempt >= 3) throw e; // fail the task -> Flink restores from checkpoint
          Thread.sleep(1000L * (attempt + 1));
          connect(); // reconnect and retry the WHOLE compacted set (idempotent upserts/deletes)
        }
      }
    }

    // Chunk size bounds the interpolated packet, not a param-count limit; the
    // Aug-17 backfill proved 20K-row multi-VALUES ODKU against thinDB's wire.
    private static final int ROWS_PER_STATEMENT = 5000;

    /**
     * One multi-VALUES statement per chunk. JDBC addBatch of single-row SQL
     * executes at thinDB's ~4ms per-STATEMENT floor (multi-statement packets
     * don't help — the server still runs each statement) = the 220-row/s
     * replay crawl. Upserts ONLY: multi-row predicates (IN lists, OR-chains)
     * bypass the keyed-DELETE fast path and full-scan the giants.
     */
    private void executeUpserts(List<Op> ops, TableCfg t) throws Exception {
      if (ops == null || ops.isEmpty()) return;
      for (int from = 0; from < ops.size(); from += ROWS_PER_STATEMENT) {
        List<Op> chunk = ops.subList(from, Math.min(ops.size(), from + ROWS_PER_STATEMENT));
        try (PreparedStatement ps = conn.prepareStatement(t.upsertSql(chunk.size()))) {
          int p = 1;
          for (Op op : chunk) for (Object v : op.vals()) ps.setObject(p++, v);
          ps.executeUpdate();
        }
      }
    }

    /**
     * Deletes stay single-row point statements on the full PK — the shape
     * thinDB's chain-loop coalescing batches internally (bloom-pruned, no
     * scan). rewriteBatchedStatements packs them into multi-statement packets.
     */
    private void executeDeletes(List<Op> ops, TableCfg t) throws Exception {
      if (ops == null || ops.isEmpty()) return;
      try (PreparedStatement ps = conn.prepareStatement(t.deleteSql())) {
        for (Op op : ops) {
          Object[] vals = op.vals();
          for (int p = 0; p < vals.length; p++) ps.setObject(p + 1, vals[p]);
          ps.addBatch();
        }
        ps.executeBatch();
      }
    }

    /** Identity of the row an op targets: table + its primary-key values. */
    private String keyOf(Op op) {
      TableCfg t = byName.get(op.table());
      StringBuilder sb = new StringBuilder(op.table());
      if (op.delete()) {
        // delete ops carry exactly the pk values, in pk order
        for (Object v : op.vals()) sb.append('\u0000').append(v);
      } else {
        for (String pkCol : t.pk) sb.append('\u0000').append(op.vals()[t.cols.indexOf(pkCol)]);
      }
      return sb.toString();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
      flush(); // checkpoint barrier => everything before it is durably in thinDB
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
      dummyState = ctx.getOperatorStateStore()
          .getListState(new ListStateDescriptor<>("noop", byte[].class));
    }

    @Override
    public void close() throws Exception {
      if (buffer != null && !buffer.isEmpty()) flush();
      if (conn != null) conn.close();
    }
  }
}
