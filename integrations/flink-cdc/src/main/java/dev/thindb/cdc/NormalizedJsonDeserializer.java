package dev.thindb.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Debezium envelope -> JSON with every temporal value already rendered as the
 * UTC text thinDB stores. The unit of a temporal field is only knowable from
 * the Kafka Connect schema name Debezium attaches to it (MySQL DATETIME(0-3)
 * arrives as epoch millis, DATETIME(4-6) as epoch micros, TIMESTAMP as an
 * ISO-8601 string), and the stock JSON deserializer drops that schema, so the
 * rendering has to happen here. The envelope keeps the shape the stock
 * deserializer produced (op, ts_ms, source, before, after) for the sink.
 */
public final class NormalizedJsonDeserializer implements DebeziumDeserializationSchema<String> {
  private static final long serialVersionUID = 1L;

  static final DateTimeFormatter DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;
  private static final long NANOS_PER_HOUR = 3_600_000_000_000L;
  private static final long NANOS_PER_MINUTE = 60_000_000_000L;
  private static final long NANOS_PER_SECOND = 1_000_000_000L;

  private transient ObjectMapper om;

  @Override
  public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
    if (!(record.value() instanceof Struct value)) return;
    if (om == null) om = new ObjectMapper();
    out.collect(om.writeValueAsString(struct(value)));
  }

  @Override
  public TypeInformation<String> getProducedType() {
    return BasicTypeInfo.STRING_TYPE_INFO;
  }

  static JsonNode node(Schema schema, Object v) {
    if (v == null) return NODES.nullNode();
    if (schema.name() != null) {
      JsonNode rendered = logical(schema.name(), v);
      if (rendered != null) return rendered;
    }
    return switch (schema.type()) {
      case INT8, INT16, INT32, INT64, FLOAT32, FLOAT64 -> number((Number) v);
      case BOOLEAN -> NODES.booleanNode((Boolean) v);
      case STRING -> NODES.textNode(v.toString());
      case BYTES -> NODES.textNode(Base64.getEncoder().encodeToString(bytes(v)));
      case STRUCT -> struct((Struct) v);
      case ARRAY -> array(schema, (List<?>) v);
      case MAP -> map(schema, (Map<?, ?>) v);
    };
  }

  /**
   * Every temporal and decimal logical type the Debezium MySQL connector can
   * emit under any time.precision.mode / decimal.handling.mode. Returns null
   * for names that need no rendering (enums, JSON, geometry, ...), which then
   * fall through to their physical type.
   */
  static JsonNode logical(String name, Object v) {
    return switch (name) {
      case "io.debezium.time.Date" -> NODES.textNode(LocalDate.ofEpochDay(longValue(v)).toString());
      case "io.debezium.time.Timestamp" -> NODES.textNode(dateTime(longValue(v), 1_000L));
      case "io.debezium.time.MicroTimestamp" -> NODES.textNode(dateTime(longValue(v), 1_000_000L));
      case "io.debezium.time.NanoTimestamp" -> NODES.textNode(dateTime(longValue(v), NANOS_PER_SECOND));
      case "io.debezium.time.ZonedTimestamp" ->
          NODES.textNode(LocalDateTime.ofInstant(ZonedDateTime.parse(v.toString()).toInstant(), ZoneOffset.UTC).format(DATE_TIME));
      case "io.debezium.time.Time" -> NODES.textNode(timeOfDay(longValue(v), 1_000L));
      case "io.debezium.time.MicroTime" -> NODES.textNode(timeOfDay(longValue(v), 1_000_000L));
      case "io.debezium.time.NanoTime" -> NODES.textNode(timeOfDay(longValue(v), NANOS_PER_SECOND));
      case "io.debezium.time.ZonedTime" ->
          NODES.textNode(OffsetTime.parse(v.toString()).withOffsetSameInstant(ZoneOffset.UTC).format(TIME));
      case "org.apache.kafka.connect.data.Date" ->
          NODES.textNode(LocalDate.ofEpochDay(Math.floorDiv(longValue(v), 86_400_000L)).toString());
      case "org.apache.kafka.connect.data.Time" -> NODES.textNode(timeOfDay(longValue(v), 1_000L));
      case "org.apache.kafka.connect.data.Timestamp" -> NODES.textNode(dateTime(longValue(v), 1_000L));
      case "org.apache.kafka.connect.data.Decimal" -> NODES.textNode(((BigDecimal) v).toPlainString());
      case "io.debezium.data.VariableScaleDecimal" -> {
        Struct s = (Struct) v;
        yield NODES.textNode(new BigDecimal(new BigInteger(bytes(s.get("value"))), s.getInt32("scale")).toPlainString());
      }
      default -> null;
    };
  }

  /** Epoch-based instant in unitsPerSecond units, floor-divided so pre-1970 values render correctly. */
  static String dateTime(long amount, long unitsPerSecond) {
    long nanosPerUnit = NANOS_PER_SECOND / unitsPerSecond;
    Instant instant = Instant.ofEpochSecond(
        Math.floorDiv(amount, unitsPerSecond), Math.floorMod(amount, unitsPerSecond) * nanosPerUnit);
    return LocalDateTime.ofInstant(instant, ZoneOffset.UTC).format(DATE_TIME);
  }

  /** Time of day in unitsPerSecond units; MySQL TIME spans -838:59:59..838:59:59, so hours are unbounded and signed. */
  static String timeOfDay(long amount, long unitsPerSecond) {
    long nanos = amount * (NANOS_PER_SECOND / unitsPerSecond);
    long magnitude = Math.abs(nanos);
    long hours = magnitude / NANOS_PER_HOUR;
    long rest = magnitude % NANOS_PER_HOUR;
    long minutes = rest / NANOS_PER_MINUTE;
    rest %= NANOS_PER_MINUTE;
    long seconds = rest / NANOS_PER_SECOND;
    long micros = (rest % NANOS_PER_SECOND) / 1_000L;
    return String.format("%s%02d:%02d:%02d.%06d", nanos < 0 ? "-" : "", hours, minutes, seconds, micros);
  }

  private static long longValue(Object v) {
    if (v instanceof Number n) return n.longValue();
    if (v instanceof java.util.Date d) return d.getTime();
    throw new IllegalArgumentException("not a temporal scalar: " + v.getClass().getName());
  }

  private static byte[] bytes(Object v) {
    if (v instanceof ByteBuffer buf) {
      ByteBuffer view = buf.duplicate();
      byte[] out = new byte[view.remaining()];
      view.get(out);
      return out;
    }
    return (byte[]) v;
  }

  private static JsonNode number(Number n) {
    if (n instanceof Integer i) return NODES.numberNode(i);
    if (n instanceof Long l) return NODES.numberNode(l);
    if (n instanceof Short s) return NODES.numberNode(s);
    if (n instanceof Byte b) return NODES.numberNode(b);
    if (n instanceof Float f) return NODES.numberNode(f);
    if (n instanceof Double d) return NODES.numberNode(d);
    if (n instanceof BigDecimal d) return NODES.numberNode(d);
    if (n instanceof BigInteger i) return NODES.numberNode(i);
    return NODES.numberNode(n.doubleValue());
  }

  private static ObjectNode struct(Struct s) {
    ObjectNode out = NODES.objectNode();
    for (Field f : s.schema().fields()) out.set(f.name(), node(f.schema(), s.get(f)));
    return out;
  }

  private static ArrayNode array(Schema schema, List<?> items) {
    ArrayNode out = NODES.arrayNode(items.size());
    for (Object item : items) out.add(node(schema.valueSchema(), item));
    return out;
  }

  private static ObjectNode map(Schema schema, Map<?, ?> entries) {
    ObjectNode out = NODES.objectNode();
    for (Map.Entry<?, ?> e : entries.entrySet()) {
      out.set(String.valueOf(e.getKey()), node(schema.valueSchema(), e.getValue()));
    }
    return out;
  }
}
