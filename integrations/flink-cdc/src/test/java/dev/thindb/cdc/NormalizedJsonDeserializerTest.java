package dev.thindb.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NormalizedJsonDeserializerTest {
  /** 2026-08-11 00:00:00 UTC: the first prod exchange-rate date that landed in 1970. */
  private static final long AUG_11_2026_MILLIS = 1_786_406_400_000L;
  private static final long NOON_ISH_MILLIS = 45_296_789L; // 12:34:56.789

  private static Schema named(SchemaBuilder builder, String name) {
    return builder.name(name).optional().build();
  }

  private static JsonNode render(Schema schema, Object value) {
    return NormalizedJsonDeserializer.node(schema, value);
  }

  @Test
  void rendersEveryDebeziumTemporalType() {
    assertEquals("2026-08-11", render(named(SchemaBuilder.int32(), "io.debezium.time.Date"),
        (int) LocalDate.of(2026, 8, 11).toEpochDay()).asText());
    assertEquals("2026-08-11 00:00:00.000000",
        render(named(SchemaBuilder.int64(), "io.debezium.time.Timestamp"), AUG_11_2026_MILLIS).asText());
    assertEquals("2026-08-11 00:00:00.123456",
        render(named(SchemaBuilder.int64(), "io.debezium.time.MicroTimestamp"), AUG_11_2026_MILLIS * 1_000L + 123_456L).asText());
    assertEquals("2026-08-11 00:00:00.123456",
        render(named(SchemaBuilder.int64(), "io.debezium.time.NanoTimestamp"), AUG_11_2026_MILLIS * 1_000_000L + 123_456_789L).asText());
    assertEquals("2026-08-11 00:30:00.500000",
        render(named(SchemaBuilder.string(), "io.debezium.time.ZonedTimestamp"), "2026-08-11T02:30:00.5+02:00").asText());
    assertEquals("2026-08-11 00:30:00.000000",
        render(named(SchemaBuilder.string(), "io.debezium.time.ZonedTimestamp"), "2026-08-11T00:30:00Z").asText());
    assertEquals("12:34:56.789000",
        render(named(SchemaBuilder.int32(), "io.debezium.time.Time"), (int) NOON_ISH_MILLIS).asText());
    assertEquals("12:34:56.789012",
        render(named(SchemaBuilder.int64(), "io.debezium.time.MicroTime"), NOON_ISH_MILLIS * 1_000L + 12L).asText());
    assertEquals("12:34:56.789012",
        render(named(SchemaBuilder.int64(), "io.debezium.time.NanoTime"), NOON_ISH_MILLIS * 1_000_000L + 12_345L).asText());
    assertEquals("00:30:00.500000",
        render(named(SchemaBuilder.string(), "io.debezium.time.ZonedTime"), "02:30:00.5+02:00").asText());
    JsonNode year = render(named(SchemaBuilder.int32(), "io.debezium.time.Year"), 2026);
    assertTrue(year.isInt());
    assertEquals(2026, year.asInt());
  }

  @Test
  void rendersConnectLogicalTypesUsedByConnectPrecisionMode() {
    java.util.Date day = new java.util.Date(AUG_11_2026_MILLIS);
    assertEquals("2026-08-11", render(org.apache.kafka.connect.data.Date.SCHEMA, day).asText());
    assertEquals("12:34:56.789000",
        render(org.apache.kafka.connect.data.Time.SCHEMA, new java.util.Date(NOON_ISH_MILLIS)).asText());
    assertEquals("2026-08-11 00:00:00.000000",
        render(org.apache.kafka.connect.data.Timestamp.SCHEMA, day).asText());
    assertEquals("12.34", render(Decimal.schema(2), new BigDecimal("12.34")).asText());
    assertEquals("0.00000001", render(Decimal.schema(8), new BigDecimal("1E-8")).asText());

    Schema variable = SchemaBuilder.struct().name("io.debezium.data.VariableScaleDecimal")
        .field("scale", Schema.INT32_SCHEMA).field("value", Schema.BYTES_SCHEMA).build();
    Struct s = new Struct(variable).put("scale", 2).put("value", new BigInteger("-1234").toByteArray());
    assertEquals("-12.34", render(variable, s).asText());
  }

  @Test
  void rendersPreEpochAndOutOfDayValues() {
    assertEquals("1969-12-31 23:59:59.500000",
        render(named(SchemaBuilder.int64(), "io.debezium.time.Timestamp"), -500L).asText());
    assertEquals("1969-12-31 23:59:59.999999",
        render(named(SchemaBuilder.int64(), "io.debezium.time.MicroTimestamp"), -1L).asText());
    assertEquals("1969-12-31", render(named(SchemaBuilder.int32(), "io.debezium.time.Date"), -1).asText());
    assertEquals("1000-01-01 00:00:00.000000",
        render(named(SchemaBuilder.int64(), "io.debezium.time.Timestamp"), -30_610_224_000_000L).asText());
    assertEquals("-01:00:00.000000",
        render(named(SchemaBuilder.int64(), "io.debezium.time.MicroTime"), -3_600_000_000L).asText());
    assertEquals("838:59:59.000000",
        render(named(SchemaBuilder.int64(), "io.debezium.time.MicroTime"), 838L * 3_600_000_000L + 59L * 60_000_000L + 59_000_000L).asText());
  }

  @Test
  void keepsPhysicalTypesInTheStockJsonShape() {
    assertTrue(render(Schema.INT8_SCHEMA, (byte) 1).isIntegralNumber());
    assertEquals(1, render(Schema.INT8_SCHEMA, (byte) 1).asInt());
    assertTrue(render(Schema.INT16_SCHEMA, (short) 2).isIntegralNumber());
    assertEquals(2, render(Schema.INT16_SCHEMA, (short) 2).asInt());
    assertEquals(3, render(Schema.INT32_SCHEMA, 3).asInt());
    assertEquals(4L, render(Schema.INT64_SCHEMA, 4L).asLong());
    assertEquals(1.5, render(Schema.FLOAT32_SCHEMA, 1.5f).asDouble());
    assertEquals(2.25, render(Schema.FLOAT64_SCHEMA, 2.25).asDouble());
    assertEquals(BooleanNode.TRUE, render(Schema.BOOLEAN_SCHEMA, true));
    assertEquals("x", render(Schema.STRING_SCHEMA, "x").asText());
    assertEquals("AQID", render(Schema.BYTES_SCHEMA, new byte[] {1, 2, 3}).asText());
    assertEquals("AQID", render(Schema.BYTES_SCHEMA, ByteBuffer.wrap(new byte[] {1, 2, 3})).asText());
    assertEquals(NullNode.getInstance(), render(Schema.OPTIONAL_STRING_SCHEMA, null));
    assertEquals("active", render(named(SchemaBuilder.string(), "io.debezium.data.Enum"), "active").asText());
    assertEquals("12.5", render(Schema.STRING_SCHEMA, "12.5").asText());

    Schema array = SchemaBuilder.array(named(SchemaBuilder.int64(), "io.debezium.time.Timestamp")).build();
    assertEquals("2026-08-11 00:00:00.000000", render(array, List.of(AUG_11_2026_MILLIS)).get(0).asText());
    Schema map = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
    assertEquals(7, render(map, Map.of("k", 7)).get("k").asInt());
  }

  private static final class ListCollector implements Collector<String> {
    final List<String> out = new ArrayList<>();

    @Override
    public void collect(String record) {
      out.add(record);
    }

    @Override
    public void close() {}
  }

  private static Schema rowSchema() {
    return SchemaBuilder.struct().optional().name("wayroll.currency_exchange_rate.Value")
        .field("id", Schema.INT32_SCHEMA)
        .field("date", named(SchemaBuilder.int64(), "io.debezium.time.Timestamp"))
        .field("rate", Schema.STRING_SCHEMA)
        .field("deleted", Schema.BOOLEAN_SCHEMA)
        .field("updatedAt", named(SchemaBuilder.int64(), "io.debezium.time.MicroTimestamp"))
        .build();
  }

  private static Schema envelopeSchema(Schema row) {
    Schema source = SchemaBuilder.struct()
        .field("db", Schema.STRING_SCHEMA)
        .field("table", Schema.STRING_SCHEMA)
        .field("ts_ms", Schema.INT64_SCHEMA)
        .field("snapshot", named(SchemaBuilder.string(), "io.debezium.data.Enum"))
        .build();
    return SchemaBuilder.struct().name("wayroll.currency_exchange_rate.Envelope")
        .field("op", Schema.STRING_SCHEMA)
        .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
        .field("source", source)
        .field("before", row)
        .field("after", row)
        .field("transaction", SchemaBuilder.struct().optional().field("id", Schema.STRING_SCHEMA).build())
        .build();
  }

  private static Struct row(Schema schema, int id, long dateMillis) {
    return new Struct(schema).put("id", id).put("date", dateMillis).put("rate", "1.2345")
        .put("deleted", false).put("updatedAt", AUG_11_2026_MILLIS * 1_000L + 42L);
  }

  private static JsonNode roundTrip(Struct envelope) throws Exception {
    NormalizedJsonDeserializer d = new NormalizedJsonDeserializer();
    ListCollector c = new ListCollector();
    d.deserialize(new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "t", envelope.schema(), envelope), c);
    assertEquals(1, c.out.size());
    return new ObjectMapper().readTree(c.out.get(0));
  }

  @Test
  void envelopeKeepsTheSinkContractAndFixesTheMillisecondDatetime() throws Exception {
    Schema row = rowSchema();
    Schema env = envelopeSchema(row);
    Struct source = new Struct(env.field("source").schema())
        .put("db", "wayroll").put("table", "currency_exchange_rate").put("ts_ms", 1L).put("snapshot", "false");
    Struct update = new Struct(env).put("op", "u").put("ts_ms", 2L).put("source", source)
        .put("before", row(row, 9, AUG_11_2026_MILLIS - 86_400_000L)).put("after", row(row, 9, AUG_11_2026_MILLIS));

    JsonNode json = roundTrip(update);
    assertEquals("u", json.path("op").asText());
    assertEquals("currency_exchange_rate", json.path("source").path("table").asText());
    assertEquals("false", json.path("source").path("snapshot").asText());
    assertTrue(json.get("transaction").isNull());
    assertEquals("2026-08-10 00:00:00.000000", json.path("before").path("date").asText());
    assertEquals("2026-08-11 00:00:00.000000", json.path("after").path("date").asText());
    assertEquals("2026-08-11 00:00:00.000042", json.path("after").path("updatedAt").asText());
    assertEquals(9, json.path("after").path("id").asInt());
    assertEquals("1.2345", json.path("after").path("rate").asText());
    assertTrue(json.path("after").path("deleted").isBoolean());

    Struct delete = new Struct(env).put("op", "d").put("source", source).put("before", row(row, 9, AUG_11_2026_MILLIS));
    JsonNode deleted = roundTrip(delete);
    assertEquals("d", deleted.path("op").asText());
    assertTrue(deleted.get("after").isNull());
    assertEquals(9, deleted.path("before").path("id").asInt());
  }

  @Test
  void ignoresRecordsWithoutAStructValue() throws Exception {
    ListCollector c = new ListCollector();
    new NormalizedJsonDeserializer().deserialize(
        new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "t", null, null), c);
    assertTrue(c.out.isEmpty());
  }

  @Test
  void sinkPassesRenderedTemporalTextThroughAndRejectsUnrenderedValues() {
    assertEquals("2026-08-11 00:00:00.000000",
        ConsolidatedCdcJob.RoutingJdbcSink.convert(new TextNode("2026-08-11 00:00:00.000000"), "DATETIME"));
    assertEquals("2026-08-11", ConsolidatedCdcJob.RoutingJdbcSink.convert(new TextNode("2026-08-11"), "DATE"));
    assertEquals("12:34:56.789000", ConsolidatedCdcJob.RoutingJdbcSink.convert(new TextNode("12:34:56.789000"), "TIME"));
    assertNull(ConsolidatedCdcJob.RoutingJdbcSink.convert(NullNode.getInstance(), "DATETIME"));
    assertNull(ConsolidatedCdcJob.RoutingJdbcSink.convert(null, "DATETIME"));
    for (String type : List.of("DATE", "DATETIME", "TIME", "TIMESTAMP")) {
      assertThrows(IllegalStateException.class,
          () -> ConsolidatedCdcJob.RoutingJdbcSink.convert(new LongNode(AUG_11_2026_MILLIS), type), type);
    }
    assertEquals(1, ConsolidatedCdcJob.RoutingJdbcSink.convert(BooleanNode.TRUE, "TINYINT"));
    assertEquals(0, ConsolidatedCdcJob.RoutingJdbcSink.convert(BooleanNode.FALSE, "TINYINT"));
    assertEquals(7L, ConsolidatedCdcJob.RoutingJdbcSink.convert(new LongNode(7L), "BIGINT"));
    assertEquals("1.2345", ConsolidatedCdcJob.RoutingJdbcSink.convert(new TextNode("1.2345"), "DECIMAL"));
    assertEquals(2.5, ConsolidatedCdcJob.RoutingJdbcSink.convert(new DoubleNode(2.5), "DOUBLE"));
    assertEquals("x", ConsolidatedCdcJob.RoutingJdbcSink.convert(new TextNode("x"), "STRING"));
  }
}
