# thinDB — Design

A single-node, columnar analytics database with a tight, fast core and deliberately small surface area. Inspired by StarRocks/Doris in storage shape, but stripped of every multi-node, optimizer, and ecosystem concern that doesn't earn its keep on one machine.

This document describes the current single-server engine. It includes an embedded API, SQL compilation, MySQL/PostgreSQL/native listeners, joins, CTEs, windows, UDFs, and parallel analytical execution. The execution pipeline is documented in [docs/simple_query_pipeline.md](docs/simple_query_pipeline.md).

## 1. Goals & Non-goals

- Run analytical workloads efficiently on one server, with an embeddable core and optional network frontends.
- Keep columnar execution, memory ownership, error handling, and persistence transitions explicit.
- Select general physical techniques from query shape and available metadata before execution. This work adds no query-specific recognizers, join-order optimizer, or group-key width policy.
- Support strict schemas, immutable segments, tombstones, WAL recovery, and background compaction.
- Keep distribution and replication outside the core. Ordinary SQL transaction verbs do not provide rollback or isolation; the supported staged-write XA protocol is described in section 8.2.

---

## 2. Architecture overview

thinDB is a Zig library with an optional standalone server. A process embeds it via `@import("thindb")` and opens a `Catalog` or the `Database.open` convenience wrapper. SQL frontends parse into IR, compile physical operators, and execute against the same storage core.

Three internal subsystems:

```
┌────────────────────────────────────────────────────────────┐
│                   public API (src/api/)                     │
│   Database · Table · Query builder · .pipe() composition    │
└────────────┬───────────────────────────────┬───────────────┘
             │                               │
       ┌─────▼──────┐                  ┌─────▼──────┐
       │ Write Path │                  │ Read Path  │
       │ (engine/)  │                  │  (exec/)   │
       │            │                  │            │
       │ memtable   │                  │ manifest   │
       │ flush      │                  │ snapshot   │
       │ compaction │                  │ operators  │
       │ deletes    │                  │ cache      │
       └─────┬──────┘                  └─────┬──────┘
             │                               │
             └───────────────┬───────────────┘
                             │
                       ┌─────▼──────┐
                       │   Storage  │
                       │ (storage/) │
                       │            │
                       │ segments   │
                       │ manifest   │
                       │ tombstones │
                       │ encodings  │
                       └────────────┘
```

Per-table mutexes serialize writes. Reads capture segment and memtable snapshots, then execute on caller and worker threads. Background flush and compaction use the same ownership and publication rules as foreground operations.

---

## 3. Data model

### 3.1 Types

| Category | Type | Backing |
|---|---|---|
| Integer | `TINYINT` | i8 |
| | `SMALLINT` | i16 |
| | `INT` | i32 |
| | `BIGINT` | i64 |
| | `LARGEINT` | i128 |
| Float | `FLOAT` | f32 |
| | `DOUBLE` | f64 |
| Decimal | `DECIMAL(p, s)` | i64 (`p ≤ 18`) or i128 (`p ≤ 38`). `p` is total digits, `s` is digits after the decimal point. |
| String | `CHAR(N)` | Fixed-width bytes |
| | `VARCHAR(N)` | Variable-width bytes. `N` is recorded but not enforced: a longer value is stored whole, as in DuckDB |
| | `STRING` | Variable-width bytes, unbounded (effective limit: 64 MB / value) |
| Temporal | `DATE` | i32 days since 1970-01-01 UTC |
| | `DATETIME` | i64 microseconds since 1970-01-01 UTC. No timezone awareness — applications convert at boundaries. |
| Boolean | `BOOLEAN` | u8 (0/1) |

Dates run from 0000-01-01 to 9999-12-31 in the proleptic Gregorian
calendar, where year 0 is a leap year, as in StarRocks. So `0000-02-29` is a
date, 0000-01-01 is a Saturday, `TO_DAYS('0000-01-01')` is 0 and
`FROM_DAYS(59)` is 0000-02-29. MySQL counts year 0 as 365 days with no
February 29, which moves its weekdays and day numbers before 0000-03-01 by
one. Date arithmetic whose result leaves this range is NULL, as in
StarRocks: `DATE_ADD('9999-12-31', INTERVAL 1 DAY)` is NULL. Date
arithmetic returns a DATETIME, as in StarRocks: a DATE moves as its midnight,
so `DATE '2026-01-31' + INTERVAL 1 MONTH` is 2026-02-28 00:00:00, and a
DATETIME written into a DATE column lands as its day. One deliberate
exception: StarRocks' `CAST('0000-02-29' AS DATE)` is NULL, because its text
reader, unlike its calendar, has no February 29 in year 0. thinDB reads it as
the day it is, so every reader agrees with the one calendar.

`CAST(text AS DATE)`, `CAST(text AS DATETIME)` and `DATE(text)` read text
the way StarRocks' backend casts a column (`scalar_fn_common.textToDate`,
`textToDatetime`):
- Space around the text is ignored. Ten characters are read as
  `YYYY?MM?DD`, where each `?` is any one character but a digit
  (`'2026/01/01'`).
- Any other text is read as fields: year, month, day, hour, minute, second
  and a fraction of up to six digits.
  - Punctuation separates fields (`'2026-1-1 1:2:3'`, `'2026.01.01 10.30.00'`).
    Space separates only the day from the time, and a `T` after the day
    also starts the time.
  - Digits alone have fixed widths: a four-digit year when the run is 4, 8
    or at least 14 long (`'20260101103000'`), a two-digit one otherwise
    (`'260101'`).
  - A two-digit year below 70 is in the 2000s and any other in the 1900s.
  - Reading stops at a letter or at anything that can't continue a field,
    and the rest is ignored: `'2026-01-01T10:30:00+08:00'` is 10:30:00.
  - Fewer than three fields is NULL.
- A value outside its range is NULL: month 13, `'2026-02-30'`, a year past
  9999 and, for a DATETIME, hour 24 or minute or second 60.
- `CAST(text AS DATE)` checks only the date, so `'2026-01-01 25:00:00'` is
  2026-01-01. `DATE(text)` reads the text as a DATETIME and takes its day,
  so there it is NULL.
- A number is read by its size as `YYMMDD`, `YYYYMMDD`, `YYMMDDhhmmss` or
  `YYYYMMDDhhmmss`, a double truncated toward zero. Its time of day must be
  valid for a DATE too: `CAST(20260101240000 AS DATE)` is NULL. StarRocks
  rejects a DECIMAL here; thinDB reads it as a double.

StarRocks' frontend folds a constant CAST by rules of its own: it reads a
`Z` or `+08:00` suffix as a time zone, and it reads `'20260101 '` and
`20260101240000`, which the backend makes NULL. thinDB reads a constant the
way the backend reads a column, so a literal and a column holding the same
text agree. A text literal meets a function as a text column does first, so
`COALESCE('2026/1/1', DATE '2026-01-02')` is the text, as in StarRocks.
Where only a DATE or DATETIME fits, text is read as a CAST to that type
reads it, and text that doesn't read is NULL (`UNIX_TIMESTAMP('garbage')`).
A literal is read once at plan time; a column or any other text expression
is read per row the same way. Text takes a DATETIME parameter before a DATE
one, as StarRocks casts it, so `DATE_ADD('2026-01-01 10:30:00', INTERVAL 1
DAY)` is 2026-01-02 10:30:00 and `DATE_ADD('2026-01-31', INTERVAL 1 DAY)` is
2026-02-01 00:00:00; a function that takes only a DATE there takes the text's
day. A number there, once nothing else fits, is read the same way as a CAST
reads it, so `YEAR(20260131)` is 2026 and `YEAR(2026)` is NULL; a
function that returns one of its arguments (GREATEST, COALESCE) doesn't
read it so, since its arguments meet at their common type instead. Typed
literals (`DATE '2026-01-01'`) and
INSERT … VALUES keep the strict `YYYY-MM-DD[ hh:mm:ss[.ffffff]]` form, and
comparisons read text as MySQL does (below).

A DATETIME carries no zone. CONVERT_TZ and FROM_UNIXTIME(n, format, zone)
read a zone's text with one reader (`exec/time_zone.zig`). It accepts:
- A fixed offset. That covers MySQL's `+H:MM` from -13:59 to +14:00, and
  StarRocks' `+h`, `+hh`, `+hhmm`, `+hh:mm`, `+hhmmss` and `+hh:mm:ss` up
  to 18 hours, bare or after `UTC`, `GMT` or `UT`. It also covers cctz's
  `Fixed/UTC+hh:mm:ss`, `Z`, `CST` (+08:00, as StarRocks reads it), and
  `SYSTEM` or `UTC` in any case, since thinDB's clock runs in UTC.
- A tz database name (`America/New_York`), read from its TZif file (RFC
  8536) under `TZDIR`, else /usr/share/zoneinfo, where cctz (and so
  StarRocks) reads it. The file's changes give the offset through its last
  one, and its footer's POSIX TZ rule gives it after that. A name is
  `/`-separated parts of letters, digits, `_`, `-`, `+` and `.`, none empty
  or starting with `.`, so no name leaves the directory. StarRocks also
  takes `Asia/../Asia/Shanghai`. Whether case matters is up to the file
  system: on Linux it does, as in StarRocks. A leap-second (`right/`) file,
  or a footer the reader can't parse, is no zone. Windows has no zone files,
  so there a name is known only under `TZDIR`. The tests set `TZDIR` to the
  fixtures in tests/fixtures/zoneinfo, so every platform sees the same zones.

Parsed zones are cached once per process and shared by every database, so a
tzdata update takes effect when the process restarts. A spin lock guards the
map only; files are read outside it, and when two threads load one zone at
once, one copy is kept. Names that are no zone are cached too, up
to 4096. A kernel remembers the previous row's zone, so a constant zone
costs one lookup per batch.

A local time that a change skips or repeats reads with the offset in effect
before the change, as cctz's `pre` does. So 02:30 on New York's
spring-forward day is 07:30 UTC, and 01:30 on its fall-back day is 05:30
UTC. A zone the reader doesn't know makes CONVERT_TZ NULL, and FROM_UNIXTIME
renders it as UTC, as in StarRocks. An empty zone is NULL in both.

CONVERT_TZ converts every value, as StarRocks does. MySQL returns a value
whose instant is outside 1970-01-01 00:00:01 to 3001-01-18 23:59:59 UTC
unchanged. A result outside years 0 to 9999 is NULL, where StarRocks gives a
value it can't print. FROM_UNIXTIME reads a count up to 9999-12-31 23:59:59
UTC and gives NULL for a local time past that. StarRocks gives NULL for any
count from 253402243200 on.

Floats compare by value: `-0.0 = 0.0`, and every NaN is one value that sorts
after `+inf`. GROUP BY, DISTINCT, joins, unique keys and zone-map pruning all
follow this, so `-0.0` and `0.0` form one group. MIN and MAX skip NaN.

Values of different types compare by value, as in StarRocks and MySQL
(`predicate.typesComparable`):
- Numbers compare across integer, decimal and float types, integers and decimals exactly. That holds in a join as well, even when no 38-digit decimal holds both keys (a LARGEINT beside any decimal, a BIGINT beside DECIMAL(38,20)); StarRocks compares such a pair as DOUBLE.
- A DATE meets a DATETIME at midnight.
- Text meets a number by reading the text as one, the way a CAST reads it. Text that doesn't read as a number compares as NULL.
- Text meets a DATE or DATETIME by reading it the way MySQL's `str_to_datetime` does. That reader takes any punctuation between fields (`'2026-9-1'`, `'2026/09/01'`), digits alone (`'20260901'`, `'260901'`) and a partial time (`'2026-09-01 10:00'`), and it ignores text after the last field. It rejects impossible values: `'2026-09-31'`, month 13, hour 24 and minute or second 60, fewer than three date fields, a zero date or zero date part, and a year past 9999. A text row that doesn't read as a date compares as NULL. A string constant the statement spells that doesn't read as one fails the statement with `InvalidTemporalLiteral` (§9.8); the same text bound as a parameter matches nothing. NULLIF compares its arguments this way too: `NULLIF(d, '2026-9-26')` is NULL on that day and the DATE on any other.
- A number meets a DATE or DATETIME the way MySQL compares them, in a literal comparison or a column pair (`predicate.placeTemporalNumber`). A number reads as a datetime by its size, as MySQL's `number_to_datetime` does: `YYMMDD`, `YYYYMMDD`, `YYMMDDhhmmss` or `YYYYMMDDhhmmss`, with any fraction as microseconds. A DATE column takes the datetime's day, so `d = 20260926000001` matches 2026-09-26. A zero month or day, or a day past its month's end (`20260900`, `20260230`), lies just before the next real day. Any other number (`2026`, a month of 13, an hour of 25) is compared with each row's own number (YYYYMMDD or YYYYMMDDhhmmss). A number column compared with a DATE or DATETIME reads the temporal as its number. Join keys, subquery results and correlated keys never pair a number with a temporal: the statement fails (`PredicateTypeMismatch`, or `JoinKeyTypeMismatch` for a join key).

So `code = 12` matches `'12'`, `'12.0'` and `' 12 '` but not `'12abc'`. A
text column compared with a number is evaluated row by row, with no zone-map
or Bloom pruning. An IN list compares each element on its own, so
`code IN ('x', 12)` means `code = 'x' OR code = 12`. StarRocks instead reads
the whole list as numbers.

A value that stands where a condition does (`WHERE x`, `x OR y`, `NOT x`,
`IF(x, ...)`, `CASE WHEN x`, `XOR`) reads as `CAST(x AS BOOLEAN)`, as in
StarRocks (`parse_predicate.truthPredicate`). A number is TRUE unless it is
zero. Trimmed text in any case is TRUE or FALSE when it spells `true`,
`false` or an INT (`' TRUE '`, `'-3'`, `'0'`); any other text is UNKNOWN. So
`'abc' OR 0` is NULL and `WHERE s` keeps neither `'abc'` nor `'0.5'`. MySQL
reads the number the text starts with instead, which makes `'abc' OR 0`
give 0 and `'0.5'` TRUE. `x IS [NOT] TRUE` and `x IS [NOT] FALSE` test that
truth and are never UNKNOWN. A comparison (`s = 1`) still reads text as a
number, as above. Assigning text to a BOOLEAN column keeps MySQL's rule:
`'0.5'` stores TRUE, and text that reads as no number fails. In the MySQL
dialect `||` is OR and `!` is NOT, as in StarRocks and MySQL without
`PIPES_AS_CONCAT`, which `SET sql_mode` doesn't turn on. The other dialects
read `||` as concatenation.

Explicitly **out of scope for v1**: `JSON`, `ARRAY`, `MAP`, `STRUCT`, `BITMAP`, `HLL`, `PERCENTILE`, `TIMESTAMPTZ`.

MySQL DDL type names without a type of their own map onto the table above:
- `FIXED` is `DECIMAL`.
- `BINARY`, `VARBINARY` and the `BLOB` family are `STRING`, which holds any bytes. `BINARY(n)` is not zero-padded.
- `BIT(1)` is `BOOLEAN`, and a wider `BIT(n)` is `BIGINT`.
- `YEAR` is `SMALLINT`.
- `TIME` is `STRING`, holding the `HH:MM:SS[.ffffff]` text.
- `ENUM` and `SET` are `STRING`; their label lists are not enforced.

A TIME function (`HOUR`, `TIME_TO_SEC`, `TIME`, `TIMEDIFF`, `ADDTIME` and the rest) reads a number argument as MySQL does. It reads the `HHMMSS` digits, or a DATETIME from 14 digits on. A number past 838:59:59, or one whose minute or second is past 59, is no TIME and gives NULL. Text past the range clamps to 838:59:59 instead.

`NOW()` and its synonyms (`CURRENT_TIMESTAMP`, `LOCALTIME`, `LOCALTIMESTAMP`, `SYSDATE`, `UTC_TIMESTAMP`) read the clock once per statement. In the MySQL dialect the bare forms give whole seconds and `NOW(n)` truncates to `n` fraction digits, as MySQL does. The other dialects keep microseconds. A column's `DEFAULT CURRENT_TIMESTAMP` also keeps microseconds, since a DATETIME stores no declared precision.

A DATETIME column may declare `ON UPDATE CURRENT_TIMESTAMP`, as in MySQL 8.4. The rule applies to a single-table UPDATE, a joined UPDATE, and the update branch of `INSERT ... ON DUPLICATE KEY UPDATE`:
- The statement sets the column to its timestamp in each row where some other column changes value.
- A SET list that assigns the column itself wins, even when it assigns the value the row already holds. So the CDC upsert, which assigns every column from the new row, keeps its source's timestamp.
- The statement reads the clock once. Every row it stamps, and every row a `DEFAULT CURRENT_TIMESTAMP` fills, gets the same value, kept to the microsecond whatever precision the clause declares.
- A key column is never stamped, since the key finds the row.

REPLACE and INSERT use the DEFAULT as usual. The WAL logs an updated row's values, so replay never reads the clock again. `SHOW CREATE TABLE`, `SHOW COLUMNS` and `information_schema.COLUMNS.EXTRA` report the clause as MySQL 8.4 does.

### 3.2 Schema and order key

Every table requires an **order key** at creation. The order key is one or more columns by which rows in every segment are physically sorted. It is the engine's only mechanism for:

- Range pruning at scan time (min/max per segment and per row group)
- Uniqueness enforcement (when `unique = true`)
- Efficient compaction (sorted merge of segments)

The order key may be marked `unique = true` or `unique = false` (default).

The order key is the only index and PRIMARY KEY the only enforced constraint. In SQL DDL:
- Secondary indexes (`KEY`, `INDEX`, `FULLTEXT`, `SPATIAL`, `CREATE INDEX`) are accepted and dropped, since they are advisory to a columnar scan.
- `UNIQUE`, `FOREIGN KEY` and `CHECK` are accepted as informational and not enforced, as in Snowflake, Redshift and BigQuery; they hold in the OLTP database the data comes from.
- Generated columns compute values, so they are rejected rather than dropped.

Columns are **NOT NULL by default**. To allow nulls, mark explicitly:

```zig
.{ .name = "note", .type = .string, .nullable = true }
```

### 3.3 Null representation

`NOT NULL` columns carry no null metadata. Nullable columns carry a **1-bit-per-row null bitmap**, co-located with the column data in each row group. Standard Arrow/Parquet layout.

### 3.4 Arithmetic

Integer arithmetic matches StarRocks (4.0.10): result types widen one level
and BIGINT wraps silently. Decimal arithmetic follows DuckDB: precise, and an
error on overflow at row level.

**Integer operators** (both operands integers; BOOLEAN counts as the narrowest):

| Operation | Result type | Overflow |
|---|---|---|
| `a + b`, `a - b`, `a * b` | common type widened one level: TINYINT→SMALLINT, SMALLINT→INT, INT→BIGINT, BIGINT→BIGINT | BIGINT wraps (two's complement): `BIGINT_MAX + 1 = BIGINT_MIN`, `BIGINT_MAX * 2 = -2` |
| `-a` | `a`'s type widened one level (parsed as `0 - a`) | `-BIGINT_MIN = BIGINT_MIN` |
| `a DIV b`, `a % b` | common type | `INT_MIN DIV -1 = INT_MIN`, `BIGINT_MIN DIV -1 = BIGINT_MIN`, `x % -1 = 0`; a zero divisor gives NULL |
| `ABS(a)` | SMALLINT→INT, INT→BIGINT, BIGINT→BIGINT | `ABS(BIGINT_MIN) = BIGINT_MIN` |
| `a / b` | `DOUBLE` (or `DECIMAL`) | a zero divisor gives NULL |

An integer literal in integer arithmetic takes the narrowest type that holds
it, so `SELECT 2147483647 + 1` is INT + TINYINT → BIGINT `2147483648`, and
`smallint_col + 1` is INT. The result type is decided once, in
`scalar_fn.intArithResultType`; kernels convert both operands to it and use
wrapping ops. LARGEINT operands stay LARGEINT. A widened result passed to a
function's narrower integer parameter narrows back, as in StarRocks (see
implicit type coercion).

A numeric literal with a fraction and no exponent is an exact DECIMAL of its
own digits: `1.10` is DECIMAL(3,2), `.5` is DECIMAL(1,1) and `0.000` is
DECIMAL(4,3). So `0.1 + 0.2` is exactly `0.3`, and `price * 1.1` over a
DECIMAL column stays DECIMAL. MySQL, StarRocks and DuckDB type literals the
same way. The tree carries the literal as its digits cast to its own type
(`expr.decimalLiteral`), and Compute folds that into one typed constant.
Beside a DOUBLE operand the literal converts to DOUBLE, as any decimal does.
A comparison reads it as the double nearest its digits when that double
prints as those digits, as it does for any literal of up to 15 significant
digits. That double lands exactly on a decimal or integer column (see DOUBLE
to DECIMAL below), so `x > 1.5` stays a leaf and keeps zonemap pruning. A
longer literal no double holds (`123456789012345678.5`) compares as the
exact decimal it is, without pruning. An INSERT's literal row writes such a
literal as its digits, which a DECIMAL column reads exactly; rows with an
expression cell are read as expressions, where every fraction is its
DECIMAL. The exponent form
(`1e3`, `2.5E-3`, `6.02e+23`) is DOUBLE, and so is a literal of more than
38 digits. A literal beyond the DOUBLE range (`1e400`) is an error, not ±inf.
An integer literal past BIGINT is DECIMAL(n,0) of its digits. Past 38
digits it is LARGEINT while it fits, as StarRocks types it
(`170141183460469231731687303715884105727`), and DOUBLE after that; MySQL
keeps DECIMAL to 65 digits, but thinDB's DECIMAL stops at 38.
`-9223372036854775808` is BIGINT, as in MySQL.

In the MySQL dialect a hex literal (`0x41`, `X'41'`) is a byte string, as
MySQL types it, stored like BINARY columns as text. As in MySQL it is the
integer it spells in a numeric context: in arithmetic, as an argument a
function reads as a number, beside a number in a comparison, IN or BETWEEN,
and in SUM, AVG and the other aggregates that take only numbers (`SUM(0x41)`
adds 65, `MAX(0x41)` is `'A'`). That integer is BIGINT UNSIGNED, held in a
LARGEINT, so `0x7FFFFFFFFFFFFFFF + 1` is 9223372036854775808; unlike MySQL, a
result below 0 is negative rather than an error (`0x01 - 2` is -1). Stored in
a numeric column, by INSERT, UPDATE, a column DEFAULT or a SELECT feeding an
INSERT, it is that integer (`0x3132` stores 12594 in an INT); in any other
column, its bytes. `CONV(X'FF', 16, 10)` is 255, as MySQL converts the
literal's integer whatever `from_base` says. A bit literal (`b'101'`, `0b101`) is
an integer, since BIT columns are integers here. A charset introducer
(`_utf8mb4'…'`, `_binary X'…'`) is dropped, and one naming a charset other
than UTF-8 or binary admits only ASCII text, since thinDB does not transcode.
Adjacent string literals concatenate (`'a' 'b'` is `'ab'`).

A DATE or DATETIME in a numeric context is its YYYYMMDD or YYYYMMDDhhmmss number, as in MySQL (`d + 0`). A DATETIME's fraction rounds to the second, since the declared precision isn't stored; `CAST(x AS LARGEINT)`, which MySQL lacks, drops it as StarRocks does. A number and a date that meet in one result (IF and CASE branches, COALESCE, IFNULL, GREATEST, LEAST, UNION arms) meet as that number, as in StarRocks (`cast.commonType`). Beside an integer or boolean, a DATE is an INT and a DATETIME a BIGINT, widened to the integer's type. Beside a decimal, either is DOUBLE, and so is a DATETIME beside a float. A float and a DATE meet as text, as StarRocks meets them. In the MySQL dialect, the aggregates that take only numbers read a temporal input as that number. These are SUM, AVG, their DISTINCT forms, the STDDEV and VARIANCE family and BIT_AND/OR/XOR, so `SUM(d)` adds YYYYMMDD values. The other dialects reject them with `AggregateUnsupportedType`, as StarRocks does.

A number, boolean, DATE or DATETIME is its text where a string is expected: in a string function, one-argument `CONCAT`, `LIKE` (`12 LIKE '1%'`), a cast to text, or a text column. A boolean is `1` or `0` there, as in MySQL and StarRocks; only PostgreSQL's cast to text spells it `true` or `false`. `REPEAT`, `LPAD`, `RPAD` and `SPACE` return NULL rather than build a result longer than 16 MiB, the `max_allowed_packet` thinDB reports, as MySQL does. The MySQL wire splits a row longer than one packet across packets, so a long result never drops the connection.

**`a DIV b` with a DOUBLE or DECIMAL operand** divides exactly, as MySQL
does, reading a double as its shortest digits, and truncates the quotient
toward zero into a BIGINT: `5.5 DIV 2` is 2, `5.5 DIV 0.5` is 11 and
`0.3e0 DIV 0.1e0` is 3. A quotient past BIGINT's range raises, like MySQL's
error 1690.

Known difference: StarRocks returns LARGEINT for `ABS(BIGINT)`, so
`ABS(BIGINT_MIN)` is `9223372036854775808` there. thinDB keeps BIGINT, which
wraps to `BIGINT_MIN`.

**Bit operators** `& | ^ ~ << >>` follow MySQL in the MySQL dialect. Each
operand reads as BIGINT UNSIGNED: a negative integer as its 64 two's-complement
bits, a double or decimal as the BIGINT MySQL rounds it to, text as the
integer it starts with. The result is BIGINT UNSIGNED, held in a LARGEINT, so
`~1` is 18446744073709551614, `-1 >> 1` is 9223372036854775807 (zeros shift
in), and a shift count outside 0..63 gives 0. The other dialects lower the
operators to StarRocks' `bitand`, `bitor`, `bitxor`, `bitnot`,
`bit_shift_left` and `bit_shift_right`, which keep BIGINT's two's complement,
as StarRocks, DuckDB and PG do: `~1` is -2 and `>>` keeps the sign. The choice
is made once, in `scalar_fn.bitOperatorFn`. The aggregates BIT_AND/OR/XOR
follow MySQL the same way in the MySQL dialect: each value reads as BIGINT
UNSIGNED, as an operator's operand does, and the result is one, held in a
LARGEINT. No rows, or only NULLs, give the operation's identity rather than
NULL: BIT_AND 18446744073709551615, BIT_OR and BIT_XOR 0. The parser picks
these unsigned forms by dialect (`AggNames.mysql`). The other dialects keep
StarRocks' aggregates, which return a signed BIGINT, NULL over no rows, and
read a LARGEINT input as its 64 bits.

A LARGEINT stays exact where a result takes it: COALESCE, IFNULL, NULLIF,
GREATEST and LEAST return LARGEINT, where MySQL returns a DECIMAL of the same
digits. No decimal holds a LARGEINT's 39 digits, so where a LARGEINT meets a
decimal in one result (those functions, IF and CASE branches, UNION arms), the
result is DOUBLE when the decimal has a fraction, as in StarRocks. Beside a
DECIMAL(p,0) the result is LARGEINT, which holds every value of both and
prints the same digits StarRocks does; StarRocks says DECIMAL(38,0) and lets
its values run past 38 digits, which thinDB's decimals don't
(`cast.commonType`). An integer cast to a narrower integer type is NULL
where it doesn't fit, as in StarRocks and in every dialect (`cast.narrowInt`):
a LARGEINT past BIGINT's range cast to BIGINT is NULL. The one exception is
MySQL's own cast spelling in the MySQL dialect, which reads a value past
BIGINT as MySQL 8.4 does (`scalar_fn.integerSpellingCast`). An integer,
integer text or DECIMAL with scale 0 (what an integer literal past BIGINT is
here, where MySQL's is BIGINT UNSIGNED) keeps its 64 bits:
`CAST(x AS SIGNED)` wraps [2^63, 2^64) into BIGINT by two's complement
(`cast.mysqlSigned`), so `CAST(~5 AS SIGNED)` over the unsigned bit
operators is -6 and `CAST(18446744073709551615 AS SIGNED)` is -1, and
`CAST(x AS UNSIGNED)` reads those 64 bits unsigned into a LARGEINT
(`cast.mysqlUnsigned`; `CAST(-1 AS UNSIGNED)` is 18446744073709551615). One
past 64 bits is still NULL. A DOUBLE, FLOAT or DECIMAL with a fraction
clamps instead, as MySQL does with a warning: SIGNED to BIGINT's range,
UNSIGNED of a double to SIGNED's bits (`CAST(1e19 AS UNSIGNED)` is
9223372036854775807) and of a decimal to [-2^63, 2^64 - 1], a negative value
then read as its bits. The fraction is truncated, as every integer CAST here
truncates, where MySQL rounds. Elsewhere `SIGNED` and `UNSIGNED` are
`CAST AS BIGINT`. An integer of any width, LARGEINT
included, becomes text digit for digit (`CAST(… AS CHAR)`, CONCAT and every
other text context). The MySQL wire presents a LARGEINT column as BIGINT in a
text result, and as DECIMAL(39, 0) in a prepared statement's binary result,
whose 8-byte BIGINT cell can't hold values past 2^63 - 1 beside negative ones.
MySQL says BIGINT UNSIGNED.

`BIT_COUNT(n)` counts the one bits of `n` as BIGINT UNSIGNED, so a negative
value of any width has 64. `CONV(n, from_base, to_base)` reads `n` as its
text, as MySQL does, in base |from_base|, up to the first byte that isn't a
digit. It reads signed BIGINT when `from_base` is negative and BIGINT
UNSIGNED otherwise, and writes uppercase digits, signed when `to_base` is
negative: `CONV(-1, 10, 16)` is `FFFFFFFFFFFFFFFF`. `BIN(n)` is
`CONV(n, 10, 2)`, so `BIN(2.7)` is `10`. A boolean is MySQL's 1 or 0 there, and
a hex literal the number it spells (`BIN(0x41)` is `1000001`). A base outside
2..36 or an empty `n` gives NULL.

**Math functions** return NULL where the result would be NaN or ±inf: a
domain error (`SQRT(-1)`, `LN(0)`, `ASIN(2)`, `LOG(1, x)`), overflow
(`EXP(1000)`, `POW(10, 400)`), or a zero divisor in a float `%`, `MOD` or
`FMOD`. This is StarRocks' rule; MySQL agrees on domain errors and raises an
error on overflow.

**Division by zero** is NULL for every numeric type: `/`, `DIV`, `%`, `MOD`
and `PMOD`, over integers, DOUBLE and DECIMAL alike. MySQL and StarRocks
answer it the same way. Such a function registers the `.zero_divisor` null
strategy, and Compute nulls each row whose divisor is 0 wherever the
expression appears: a projection, a filter, an aggregate's input. The result
is nullable unless the divisor is a nonzero literal, so `x / 100` over a NOT
NULL column stays NOT NULL. A nonzero divisor otherwise follows IEEE: an
infinite operand or an overflow still gives ±inf.

**Aggregates**:

| Input column type | `SUM` result |
|---|---|
| Any integer type up to `BIGINT` | `BIGINT`. Wraps on overflow, like the operators. |
| `LARGEINT` | `LARGEINT` |
| `FLOAT`, `DOUBLE` | `DOUBLE` |
| `DECIMAL(p, s)` | `DECIMAL(38, s)`. Errors at the i128 ceiling. |

Every SUM path (generic, V2 handlers, radix, region, SMA metadata, affine
reduction) accumulates exactly in i128 and truncates to BIGINT at emit. That
equals the wrapped sum, because truncation commutes with addition mod 2^64.
ORDER BY and HAVING on a SUM see the emitted, wrapped value. `AVG` divides the
exact sum and returns `DOUBLE`. `MIN`/`MAX` return the input type. `COUNT`
returns `BIGINT`. `SUM(DISTINCT x)` and `AVG(DISTINCT x)` add up the exact
distinct values and have the same types as `SUM(x)` and `AVG(x)`.

`STD`, `STDDEV` and `VARIANCE` are the population statistics (`STDDEV_POP`,
`VAR_POP`), as in MySQL and StarRocks, except in the PostgreSQL dialect, where
`STDDEV` and `VARIANCE` are the sample ones. The parser picks the function
once (`AggNames`), so every aggregate path computes the same one. A
population statistic over one value is 0; a sample one is NULL below two.

`SELECT DISTINCT` over a grouped query dedups the grouped rows after HAVING
and before ORDER BY and LIMIT. An ORDER BY key the SELECT list doesn't carry
orders each distinct row by its first occurrence in that order, as MySQL does:
by the key's least value ascending, its greatest descending.

A statement computes each distinct aggregate call once. The SELECT list, HAVING,
ORDER BY, window specs and post-aggregate expressions can all repeat a call:
`SELECT k, MIN(s), COUNT(*) ... HAVING MIN(s) <> '' AND COUNT(*) > 1` runs two
aggregates, not four. The parser applies the rule where it builds the GroupBy's
aggregate list, so every route (V1 generic, V2 lowcard and silo, radix,
partitioned, sorted, group-topN) sees the shorter list. A repeated call's
references read the first call's output. A repeated SELECT item becomes a copy
of it. Two calls are equal when all of these match:

- the function, with DISTINCT counted as part of it;
- the argument expressions, compared structurally, with identifiers matched
  case-insensitively;
- the parameters: the GROUP_CONCAT separator, DISTINCT and ORDER BY, and the
  percentile fraction.

A call with a volatile argument (`RAND()`, a volatile UDF) keeps its own
evaluation, as does a volatile aggregate UDF. Each repeat would otherwise hold
its own state per group. On the 100M-row ClickBench table (6.2M groups) the
pair above raised the statement's accounted peak from 10.0 GB to 13.9 GB
(#466).

A subquery's aggregate over only enclosing columns (`(SELECT SUM(x.v) FROM y
...)`) moves into the enclosing grouping during subquery resolution, after
parsing. That is the step where names bind, so it is where the call is known to
be the enclosing query's. The move applies the same rule, through the shared
`ir.sameAggCall` and `ir.aggCallRepeatable`, to the grouping's own aggregates
and to those moved from earlier subqueries. An equal call reads the existing
one instead of adding its own:

- **Argument comparison:** an argument is compared as the expression it reads
  over the grouping's input. A column computed below either grouping counts as
  its expression.
- **Column spelling:** an unambiguous unqualified column is spelled with its
  relation's qualifier, so `SUM(v)` in the enclosing query equals `SUM(x.v)`
  in a subquery.
- **Fresh name:** an equal call that the grouping already computes is read
  through a copy under a fresh name. The subquery could otherwise bind the
  aggregate's own name to one of its own columns.

#473 covers this.

**Decimal precision/scale propagation**:

| Operation | Result type |
|---|---|
| `DECIMAL(p1, s1) + DECIMAL(p2, s2)` | `DECIMAL(max(p1-s1, p2-s2) + max(s1, s2) + 1, max(s1, s2))` |
| `DECIMAL(p1, s1) - DECIMAL(p2, s2)` | same as `+` |
| `DECIMAL(p1, s1) * DECIMAL(p2, s2)` | `DECIMAL(p1 + p2, s1 + s2)` |
| `DECIMAL(p1, s1) / DECIMAL(p2, s2)` | `DECIMAL(p1 + s2 + 4, s1 + 4)` |

Result precisions exceeding 38 are clamped to 38, with overflow → error rather than truncation. Mixed decimal/integer arithmetic promotes the integer to decimal first.

**DOUBLE to DECIMAL** goes through the double's shortest round-trip digits (the digits it prints as), rounded half away from zero to the target scale. `1.005e0` is 1.005 although its binary value is just below it, so it lands on 1.01 at scale 2, as in MySQL. One rule (`scalar_fn_common.floatDigits`) covers every conversion: `CAST`, an INSERT of a float literal into a DECIMAL column, and a float literal compared with a decimal or integer column. Text written into a DECIMAL column rounds the same way, and a value past the column's precision is `ValueOutOfRange`.

**A float function takes a decimal's value.** A function with only DOUBLE parameters, such as `POWER`, `SQRT`, `LN`, `EXP`, the trigonometric functions and `SIGN`, converts a DECIMAL argument to DOUBLE, as MySQL and StarRocks do. Functions with decimal overloads (`ROUND`, `FLOOR`, `CEIL`, `ABS`, `TRUNCATE`, `MOD`, `GREATEST`, `LEAST`) keep DECIMAL.

**`ROUND(x, n)` and `TRUNCATE(x, n)`** over DECIMAL(p,s) with a literal `n` below `s` return DECIMAL(p, max(n, 0)), as in MySQL and DuckDB. So `ROUND(1.005, 2)` is `1.01`, and a negative `n` rounds left of the point: `ROUND(15.5, -1)` is `20`. A place computed per row keeps DECIMAL(p,s), with the dropped digits zeroed.

---

## 4. On-disk format

### 4.1 Directory layout

```
<data_dir>/
  <table_name>/
    manifest                       ← table-level manifest (atomically updated)
    schema.json                    ← static schema (immutable post-create)
    segments/
      <seg_id>.dat                 ← immutable segment file
      <seg_id>.tomb                ← tombstones for that segment (sparse, append-only)
  __alter_new_<table_name>/        ← ALTER TABLE's rewritten table before the swap (transient)
  __alter_old_<table_name>/        ← the original, set aside during an ALTER or REFRESH swap (transient)
  __ctas_<n>/                      ← a CTAS, CREATE MATERIALIZED VIEW or REFRESH target while its query runs (transient)
```

`<seg_id>` is a monotonically increasing u64. `.tomb` files are absent until the first delete that hits that segment.

A catalog root holds databases, each holding schemas of tables laid out as above, beside the engine's own directories:

```
<root>/
  <database>/
    <schema>/<table_name>/...
    _functions/                    ← persisted function sources
  _xa/                             ← XA branch records and the commit journal
  _temp/                           ← per-session temporary tables (swept at open)
  _zigfn_build/                    ← zig function build scratch
```

Every other directory under the root is a database. The engine's names are reserved (`Catalog.reserved_database_names`, compared without case), and one check, `Catalog.validateDatabaseName`, guards every path that maps a name to a root directory: discovery at open skips them, and CREATE DATABASE, DROP DATABASE, USE and COM_INIT_DB refuse them with `InvalidDatabaseName` (§9.8). The same check refuses a name that isn't a single path component: empty, `.`, `..`, or containing `/`, `\` or NUL. The set is explicit rather than every name with a leading underscore, so a user database such as `_staging` keeps working; a new engine directory at the root must join it. DROP DATABASE removes only a database the catalog loaded, never a directory by name alone.

### 4.2 Manifest

The manifest selects the active immutable segments. Flush and compaction build a candidate without changing the published in-memory list. They finish the referenced output files, atomically replace `manifest` via `manifest.tmp`, then install the new in-memory state. Failed publication retains the old input ownership. TRUNCATE follows the same rule: publish an empty manifest and WAL checkpoint before replacing the memtable and reclaiming old files; segment IDs remain monotonic while deferred deletion is possible. Late deletes found during compaction reconciliation are written to the output tombstone before that output is selected.

Manifest v11 has a 56-byte header, including a 16-byte WAL generation and the covered physical byte offset. This checkpoint makes a published flush recoverable even if subsequent WAL replacement fails. WAL v2 has a 32-byte header with its generation. Readers also accept manifest v10 and WAL v1. Older binaries cannot read newly written formats; downgrade testing must use an untouched snapshot. The one exception is the WAL's `statement` record (type 6, §5.5), added within v2: a clean stop leaves the log with no records (§5.5), which the previous release opens, and a release older than the record rejects one it meets (`WalUnknownRecord`) instead of skipping it.

`schema.bin` has a similar exception. A column's `ON UPDATE CURRENT_TIMESTAMP` is bit 2 of its DEFAULT presence byte, added within schema v5 (`src/storage/schema_file.zig`). Every existing schema reads unchanged, and a table that declares no such column is written byte for byte as before. A release older than the bit takes a column that sets it for one with a DEFAULT value, so it fails on or misreads that table's schema. Downgrading past the bit is safe only for tables that don't declare the clause.

The exact binary layouts live in [src/storage/manifest.zig](src/storage/manifest.zig) and [src/engine/wal.zig](src/engine/wal.zig). Segment entries include row/byte counts, leading-key statistics, per-column statistics, and cardinality sketches.

Durable mode syncs output files before publishing references, and syncs affected parent directories on POSIX before reclaiming the old WAL or inputs. Windows performs file sync and same-volume replacement; the directory-sync helper is unsupported there. `sync_mode = .none` remains the default and does not promise power-loss durability.

### 4.3 Segment file

Each `.dat` file is a self-describing columnar container. Rows within a segment are physically sorted by the order key. A segment is partitioned into **row groups** (default **64K rows per group**, configurable per database).

```
┌─ Header ─────────────────────────────────────────┐
│ magic "tDBS", version, schema fingerprint,        │
│ segment id, total row count, row group count      │
├─ Row group 1 ─────────────────────────────────────┤
│ ┌─ Column 0 block ─┐                              │
│ │ encoding, compression, null-bitmap? (if nullable),│
│ │ min, max, data                                 │ │
│ └──────────────────┘                              │
│ ┌─ Column 1 block ─┐ … one per column             │
├─ Row group 2 ─────────────────────────────────────┤
│ …                                                 │
├─ Footer ──────────────────────────────────────────┤
│ row group offsets, per-row-group per-column stats │
│ (min/max/sum/null_count), checksums, footer       │
│ length, magic "tDBS"                              │
└───────────────────────────────────────────────────┘
```

Footer is read first (via the trailing length + magic). Row group offsets in the footer let scans skip to the relevant byte ranges without parsing the whole file.

Per-row-group per-column stats are small materialized aggregates: min/max (zone maps, per-type i128 encoding), an exact `null_count`, and a per-type `sum` slot (integer sum; f64 sum for floats; for strings, the blank-excluded min prefix used by ORDER BY pruning). A bare global `SUM` / `AVG` / `COUNT(col)` / `MIN` / `MAX` over a tombstone-free table answers from these without touching data; any tombstone or unflushed memtable row makes the stats-dependent lane fall back to the scan path.

### 4.4 Column block encodings

Encoding is chosen per row group at flush time based on the column's data characteristics. The block header records which encoding was used; scanners handle each one.

| Encoding | When chosen | Data layout |
|---|---|---|
| **Plain** | Fixed-width numeric types, fallback for strings | Raw values back-to-back |
| **RLE** (run-length) | Repetitive low-cardinality data | `(value, run_length)` pairs |
| **Dictionary** | Strings with < 128 distinct values in the block | Dictionary + integer indexes |
| **Frame-of-reference** | Integer columns where `max - min` is small | Min value + bit-packed deltas |
| **FSST** | High-NDV strings dict declines, when it saves ≥ 12.5% | Block-local symbol table + per-row compressed slices (random access preserved; stays compressed in cache, decoded only at materialization) |
| **Fixed-width** | `CHAR(N)` always | N bytes per row |
| **Offsets + bytes** | `VARCHAR(N)`/`STRING` when dictionary is not chosen | `u32` offsets + flat byte buffer (Arrow-style) |

After encoding, each column block is **zstd-compressed** as a final pass. Both the encoding and the zstd-compressed size are recorded in the block header.

### 4.5 Tombstone files

For each segment that has any deleted rows, a sibling `<seg_id>.tomb` file exists. Format:

```
magic "tDBT", version, count, [u32 row_offset]...
```

Row offsets are the 0-indexed positions within the segment (across all row groups). The file is append-only — new deletes append more offsets. At scan time the file is read, sorted/deduped in memory, and converted to a bitset that is ANDed into the filter step.

When a segment is compacted away, its `.tomb` file is deleted alongside the `.dat`.

---

## 5. Write path

### 5.1 Memtable

Each table has an in-memory memtable that buffers writes between flushes. Internal layout mirrors a segment: **column-oriented**, each column a growing `ArrayList`. Rows accumulate in insertion order.

For tables with `unique = true` on the order key, the memtable additionally holds a hash map (`order_key_value → row_index`) for O(1) duplicate detection at insert time.

### 5.2 Inserts

Two API surfaces, same memtable underneath:

- **Row-oriented (primary, ergonomic)**: caller passes a slice of row structs. Engine transposes into column buffers in a single O(n) pass per column. Negligible cost relative to the rest of insert work.
- **Columnar (bulk path)**: caller passes pre-built column arrays. Engine appends directly. No transposition.

Insert sequence (per batch):

1. Sort the incoming batch by order key.
2. Detect intra-batch duplicates (unique tables only) — error if found.
3. Check the memtable's hash index for cross-batch duplicates within the in-memory buffer (unique tables only).
4. For unique tables, check existing segments via per-segment `[min_key, max_key]` range — most segments are skipped without reading; overlapping segments are probed via binary search on the row-group min/max ladder.
5. If clean, append columns to the memtable. For unique tables, update the hash index.
6. If a duplicate was found at any step, return `thindb.Error.UniqueKeyViolation` and the entire batch is rejected. No partial inserts.

### 5.3 Flush triggers

The memtable becomes a new segment when **any** trigger fires:

| Trigger | Default |
|---|---|
| Memtable column data exceeds size | 64 MB |
| Memtable row count exceeds | 1,000,000 |
| Memtable has been non-empty for ≥ time, and exceeds min size | 5 seconds + 1,000 rows / 1 MB |
| Manual `db.flush(table)` call | — |

The min-size guard on the time trigger prevents pathologically tiny segments on low-volume tables.

### 5.4 Flush procedure

1. Atomically detach the current memtable (becomes immutable from the engine's perspective). Allocate a fresh empty memtable for new writes — they continue uninterrupted.
2. Compute a sort permutation from the order-key column on the detached memtable.
3. Apply the permutation to each column (one allocation per column, vectorized memcpy).
4. Open a new `<seg_id>.dat` file. Stream out row groups (64K rows each):
   a. For each column, choose an encoding based on cardinality / range statistics over that row group.
   b. Encode, then zstd-compress.
   c. Write the block, accumulating offsets + min/max for the footer.
5. Write the footer.
6. `fsync` the segment file (optional in v1; non-durable mode skips this).
7. Update the manifest: read current, append the new segment, write `manifest.new`, `rename` over `manifest`.
8. Discard the detached memtable.

### 5.5 Deletes

DELETE is **predicate-based** (Model B): users may delete by any condition the filter operator can evaluate.

```zig
try orders.delete(.{ .col = "status", .op = .eq, .val = .{ .string = "cancelled" } });
```

Execution, as one statement (below):

1. Scan the memtable. Its matching rows are removed at once, by swapping in a memtable without them (the memtable hasn't been flushed yet, so true removal is fine). This is the statement's first step.
2. For each segment the manifest lists when the statement starts:
   a. Scan its row groups, evaluating the predicate.
   b. Each row group's matching in-segment offsets are one more step, staged.
3. Commit: log the steps, then merge the staged offsets into the `<seg_id>.tomb` files.

The WAL records the rows a DELETE removed, never its predicate. Replay therefore has no predicate evaluator that could drift from the live one: it removes exactly what the live DELETE removed. Older binaries logged the predicate (`delete` / `delete_expr` records). Replay still reads those from a log such a binary left behind, but nothing writes them.

A delete that runs concurrently with reads is invisible to them — readers see the manifest snapshot taken at their query start, including the tomb file state at that moment. New deletes append to the tomb file; readers using an older snapshot just see fewer tombstoned rows than the live state.

UPDATE is delete + insert in steps: the memtable's matching rows form one step, and each matching row group of a segment forms another. A step's log payload carries its deletes (the retracted memtable rows, or segment offsets) together with the replacement rows. Replay retracts memtable rows, appends the replacements and merges the offsets into the `.tomb` files. On a plain table a retracted row removes one equal row. On a unique table it removes every row with its key, since until the post-replay upsert pass the recovered memtable still holds the versions that later inserts superseded. The replacement rows go to the memtable, and an UPDATE matches only the segments listed when it started, so it never matches its own output.

**Statement atomicity.** An UPDATE or DELETE commits once, through `Table.Statement` (#335). Its steps change the live memtable at once. The statement holds the table mutex throughout, and readers capture the memtable under that mutex, so no reader sees part of a statement. Everything else is staged: the segment offsets (the statement's own and, on a unique table, the older versions of the keys an UPDATE writes) and each step's log payload. At commit the statement writes one WAL record, then the `.tomb` files:

- A one-step statement logs a plain `replace` record, byte for byte what earlier releases wrote.
- A statement of more steps logs a `statement` record (type 6): every step's `replace` payload under one checksum. Replay applies all of it, or none of it once torn.
- A statement whose only change is offsets in one segment (a point DELETE of a flushed key) logs nothing. Its one atomic `.tomb` replacement commits it.

A failure before the record takes the memtable back (the statement keeps the memtable it started from until it commits) and leaves nothing on disk. After the record the statement stands: a failed `.tomb` write fences the database with `RecoveryRequired`, and a reopen replays the record. A failed auto-flush after the commit doesn't fail the statement, since a retry would apply it twice; the next write or the background flusher retries the flush.

**Known limitation: the staging bound.** A statement stages at most `Table.STATEMENT_STAGE_BYTES` (64 MB, the bound INSERT ... SELECT also stages under) of change: removed memtable rows, replacement rows and segment offsets. Past it, the statement commits what it staged, flushes if due, and continues as a new chunk, so its memory stays bounded. Such a statement is atomic per chunk, not as a whole: a failure in a later chunk leaves the earlier chunks committed. If whole-statement atomicity for big UPDATEs ever matters, the path is to spill a statement's replacement rows to segments it doesn't publish until commit, and publish them with its tombstones in one manifest swap.

**Clean stop and downgrade.** A clean close flushes every table. The flush retires the WAL only after everything its records cover is durable: the segment is written, the manifest is published and the tombstones are written. When the memtable is empty, there is nothing to flush, but the log is retired anyway: every committed statement wrote its tombstone files before returning, and whatever the log's records added to the memtable, a later record took back. So a clean stop always leaves a log with no records. The previous release opens that log, so downgrading after a clean stop is safe, while a `statement` record left by a crash stops it loudly.

---

## 6. Read path

### 6.1 Snapshot isolation

Every query begins by reading the manifest once. That snapshot is fixed for the lifetime of the query: which segments exist, which `.tomb` files apply, and at what size. Subsequent writes do not affect the in-flight query.

### 6.2 Operator pipeline

Reads execute as a chain of **vectorized operators**, each producing **batches** of up to **1024 rows** at a time. Each operator implements:

```zig
pub fn next(self: *Self) ?Batch          // null → end of stream
pub fn schema(self: *Self) Schema
pub fn deinit(self: *Self) void
```

**Built-in operators (v1)**:

| Operator | Purpose |
|---|---|
| `Scan` | Read row groups from a table; prunes via manifest stats; applies tombstones via bitset |
| `Filter` | Evaluates predicate; produces a bitmap-selected batch |
| `Project` | Selects / renames / excludes columns |
| `Compute` | Derived columns via scalar functions (with implicit coercion) |
| `Aggregate` | Hash-group + standard aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) plus statistical (`STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP`), `COUNT_DISTINCT`, `PERCENTILE_CONT`, `GROUP_CONCAT` |
| `Sort` | Materializes, sorts by key columns, streams sorted batches |
| `Limit` | Stops after N rows |
| `Join` (hash) | Build smaller side, probe with the larger; compound keys via order-preserving byte encoding |
| `SortMergeJoin` | Materialize + sort both sides, streaming merge; merge-only fast-path when manifest stats prove pre-sorted |
| `NestedLoopJoin` | Cartesian eval with per-pair predicates; used for pure-range, opaque-callback, or no-equi-keys joins |
| `RangeSweepJoin` | Single inequality `a OP b` between two side-sorted columns — cursor-style merge, ~2× faster than NLJ on pure-range shapes |
| `Sink` | Terminal — collects results or yields batches to caller |

Join routing (`.algorithm = .auto`): opaque predicate → NLJ; pure single-range shape → range_sweep; both sides sorted on the join keys (per manifest stats) → SMJ; otherwise hash. Hash join's build phase runs Misra-Gries sampling — under heavy skew it transfers ownership of the built columns to an SMJ at execute time.

A key pair can be null-safe (`KeyPair.null_safe`): an ON conjunct `a <=> b` or `a IS NOT DISTINCT FROM b` across the two inputs keys the join like `a = b`, except that a NULL key matches a NULL key. SMJ drops NULL keys, so a join with a null-safe key never takes SMJ or the skew re-route. The hash join keeps NULL as a value of that key, NLJ compares it as one, and build-key scan hints skip a null-safe key whose build side holds a NULL. When a key's types differ and a conversion can turn a value into NULL, the pair also gets a plain key on both sides' null flags, so a converted NULL matches only a NULL. An outer join's preserved-side ON conditions fold into a plain key. When every key is null-safe, they fold into a constant key pair.

A table function's input relation carries each declared input column by name and in order, and a nullable column can't feed a NOT NULL field. A column of another type converts to the declared type as an INSERT into a column of that type would (§9.8 `ValueOutOfRange`). The casts `cast.assignmentCastExpr` gives (text, DATE or DATETIME into DATE or DATETIME, a number or text into DECIMAL) run as a `Compute` over the input. The pairs an INSERT converts as its rows land (one number type into another, a number or date into text, text into a number) convert each drained batch with `cast.assignColumn`. When every type already matches, the call reads the input's columns without a copy, as before. A pair the rule refuses, such as a number into a DATE, is `TableFnInputMismatch` when the call is built. A value that fails its conversion, such as text that isn't a date or a number the field can't hold, is `TableFnInputMismatch` when its batch is drained, with a diagnostic line naming the column. A converted partition or order key is sorted again even when the input arrives ordered, since converting can reorder it.

**Memtable scan**: every Scan also reads from the (potentially non-empty) memtable of the table. Memtable rows are processed identically to segment rows. This gives read-your-writes consistency.

### 6.3 Execution model

Scan pruning resolves column references with the same rules as row evaluation.
Alias, projection, and compute wrappers map hints back to unchanged source
columns. Limits and windows stop hints; aggregates only forward group-key hints
when their output is uncapped. A hint must not change the rows used to calculate
a window, an aggregate value, or a limited result.

The staged SQL compiler also derives join scans' required columns from
their ancestors within the current query block. This keeps unused payloads out
of lookup scans and hash builds even when a sibling CTE contains a wildcard.
It retains join keys, filters, expressions, grouping and ordering inputs through
filter/compute wrappers. It declines at wildcards or column-scope boundaries it
cannot resolve. This pre-execution column projection leaves join order and SQL
results intact.

The parser decides which join input each `JOIN ... ON` column belongs to. A
qualified column goes to the input its qualifier names. An unqualified one goes
to the single input whose output has that name, as MySQL resolves it. Base-table
columns come from the session's catalog through the parse context, derived
tables and CTEs from their projection, and table functions from their declared
output. A name that both inputs have is an error (`SqlOnColumnAmbiguous`, MySQL
1052). If an input's columns can't be listed (a file scan, or a parse without
the catalog), unqualified names on that join don't resolve.

`JOIN ... USING (c, ...)` is an equi join on each named column. The parser
merges each `c` into one bare output column holding the left value, the right
value under RIGHT JOIN, or their COALESCE under FULL JOIN. `left.c` and
`right.c` stay addressable, so `LEFT JOIN r USING (id) WHERE r.id IS NULL`
works. `NATURAL JOIN` is USING over every column name both sides show, which
requires both inputs' columns to be listable. A bare `*` over such a FROM
clause becomes the explicit list MySQL shows: the merged columns in the first
input's order, then that input's other columns, then the second's. The right
input counts as first under RIGHT JOIN. An exclude drops each column a name
spells or resolves to, so hidden join keys staged below an aliased input leave
`*` too.

An unaliased computed SELECT item is shown under its dialect's name. MySQL and
the neutral dialect use the item's SQL text as written, cut to 255 bytes. A lone
literal or column, even in parentheses, is named by itself: a string by its
value, a number by its spelling, NULL as `NULL`, a column by its bare name.
PostgreSQL applies its `FigureColname` rules: `count`, `concat`, `case`, a
cast's operand name, and `?column?` for everything else. The name is computed
from the item's text, never from the lowered expression. The plan keeps its own
internal name for the item, and the final projection renames the item. Derived
tables, CTEs, views and `CREATE TABLE ... AS SELECT` expose the shown name. ORDER
BY and GROUP BY can reach an item by a quoted shown name that isn't a plain
identifier, such as `` `n+1` ``. A result's columns stay uniquely named, so a
shown name that repeats an earlier one gets a `_N` suffix. MySQL and PostgreSQL
would allow the duplicate.

A bare name in the SELECT list, a window spec or an aggregate argument reads the
FROM column of that name, as MySQL binds it, even after an item aliased to the
same name: `SELECT 'x' AS n, n` shows `x` and the column. The parser computes
such an item under a private name and shows it under its alias. An aggregate
gives way only to a column the GROUP BY keeps. A name no FROM column has still
reads an earlier item (`SELECT a + 1 AS b, b * 2`), which MySQL rejects. Beside
`*` the item takes the slot of the column it names (`SELECT *, f(n) AS n`), where
MySQL shows both. A bare ORDER BY name and QUALIFY read the alias. HAVING reads
a grouped column of that name, else the alias. An ORDER BY expression reads the
column. A bare GROUP BY name reads the FROM column too, in every dialect, as
MySQL, StarRocks, PostgreSQL and DuckDB bind it: `SELECT n % 2 AS n ... GROUP BY
n` makes one group per `n`. It reaches an alias only when FROM has no such
column. `SELECT n, t.n FROM t` names the repeat `n_1`, as `SELECT n, n` does.

Before preparing a fused hash join, a pure filter over an existing materialized
stage may check that stage for a surviving probe row. An empty probe skips the
lookup builds through a chain of non-FULL joins. The check reuses stage buffers,
retains parallel probing for nonempty inputs, and does not add a materialization
boundary or choose a different join order or algorithm.

The group count those plans are priced and sized from is the product of the
keys' NDVs, capped at the input's row bound. Both come from the stats every
operator reports (`PipelineStats`), and both stay upper bounds (issue #478). A
table scan reports its snapshot's rows and each column's merged segment sketch.
Once a filter's prune hints skip segments or row groups, it reports only the
surviving row groups' rows, and NDVs from the surviving segments' sketches. A
filter whose predicate the column stats prove empty reports no rows and no
values. When a consumer narrows the columns a scan-fused filter emits, the
filter's tightened column stats follow their columns by name. A UNION ALL sums
its arms' rows and NDVs, except where both arms read the same column of the
same table snapshot. That column's NDV then caps the sum, because a value both
arms hold counts once. An arm that reports no rows adds nothing to the union,
so an operator reports 0 rows only when it proves it emits none; a group-topN
without a LIMIT reports its table's rows. Each column carries that
provenance (`ColStat.origin`) while operators only filter, project, rename,
join or group it; anything that computes new values drops it. A scan also marks
its rows as its snapshot's (`PipelineStats.row_origin`), and filters,
projections, computes and limits keep the mark, while joins, aggregates and
windows drop it. When every key of a grouping reads a column of the marked
snapshot, each key tuple is one table row's, so the group count is also capped
at the snapshot's rows: a UNION of two arms over one table can't have more
distinct rows than the table. An INTERSECT's or EXCEPT's grouping still holds
every distinct row of both arms, but the groups it keeps are rows of the left
arm, and for INTERSECT of the right arm too. Its output is bounded by those
arms' distinct rows and their columns' NDVs.

A keyed GROUP BY whose keys aren't a sorted prefix of its input picks its plan
(hash, partitioned hash, or sort then stream) by pricing each against the
query's memory headroom. The price starts from the input's size: its row count
times each column's width. A string column's width comes from the stage or
realized buffer that measured it. Over a table scan nothing has, so the router
samples the table (issue #397): 64 row groups spread evenly across the scan's
segments, each counted once and kept on the segment's cached handle, so later
plans read nothing. A raw block's header gives its string bytes without
reading its body. A dict or FSST block is read through the block cache, which
the scan that follows reuses, and counted from its codes or its recorded byte
count. Filters, projections, renames and computes pass the request down for the
columns they carry unchanged, so a filtered scan is priced at the width of the
rows it reads. Nothing is stored on disk. Only a column no sample reaches is
priced at the 32-byte guess.

A hash aggregate keeps its group table, accumulator cells, per-group key list
and batch scratch on the operator's tracked allocator, so a grow frees the
array it outgrew and the accountant sees every byte. Its arena holds only the
key copies, string values and complex states, which live until the emit
(issue #464). When the router picks partitioned hash cores it hands each core
its share of the estimated groups, capped at what the headroom could hold at
8/3 slots a group. A core starts at no more than 65,536 groups and, on its
first overflow, grows straight to that share instead of doubling up to it. A
core's emitted columns become its partition's output without a copy, and the
emit reserves every output column's rows, plus a lone string key's bytes, up
front. The router prices a hash plan from what that layout holds. Each table
is the power of two that its share of the groups plus one batch needs, times
the slot width. The arena is each group's key and string payload with a third
of slack. The output is each group's row, with a quarter of slack on string
bytes. A partitioned plan peaks at the larger of absorbing a round (its chunk,
the partitioned index, two windows and the tables) and emitting (the tables,
the arena and every partition's output). A string key's payload is priced at
its distinct width, not its row width, because a group copies its key once
however many rows repeat it. Search phrases average 9 bytes a row, mostly the
empty string, against 58 bytes a distinct value. A string MIN, MAX, ANY_VALUE,
FIRST, LAST or MAX_BY keeps one of its group's values, priced at the wider of
its column's row and distinct widths: under a skewed key most groups hold few
rows, and the row average weighs their values by the big groups' rows instead.
The width sample therefore also hashes 4,096 strided rows from 16 of its 64
row groups for each column a group keeps, weighs each distinct value once,
and caches the count on the segment's handle. That reads fewer row groups
than the row sample because a block's distinct values need its body
decompressed, where a raw block's row bytes come from its header. A stage or
realized buffer samples 65,536 rows of those columns on demand. The partitioned
sort cores keep pricing at the row width.

Radix, the serial aggregate over a packed integer key with compact fixed-width
states, keeps its group table, cells and batch scratch on the operator's
tracked allocator as well, each allocated at its exact size, and frees each
array it outgrows (issue #476). A group's cells are its packed key and its
aggregates' state words. They are sized for every group the table holds under
its 0.75 load factor, so they grow only when the table does. The router hands
radix the keys' NDV estimate, capped at the groups whose table and cells the
headroom could hold (8/3 slots and 2 cells a group); a forced radix route does
the same. The table starts at no more than 65,536 groups and, on its first
overflow, grows straight to the estimate. Without an estimate it starts at
4,096 groups and doubles. Radix frees its table and scratch before it emits,
and reserves its output columns' rows exactly. The router prices it from that
layout. A slot is its key tier's width (8, 16 or 32 bytes); a cell is 16 bytes
plus 8 per state word; an output row is its columns' widths, plus a validity
bit per nullable column. The price is the larger of two phases. The drain
holds the table that the estimate plus one batch needs, its cells, the batch
scratch, and the table it outgrew, held while its groups move. The emit holds
the cells and the output. Radix is priced only where it can run: a key of at
most 128 bits with no nullable column, compact-state aggregates, and no bare
LIMIT.

Parallel grouped aggregation initially reserves at most one 8,192-row batch's
worth of groups per bucket and allocates its state slab only when rows arrive.
This keeps small tables' setup allocations out of the workers' allocation
traffic while bounding speculative memory for large hints. When a table grows,
it forecasts capacity from the observed number of distinct composite keys per
source row, counting weighted run partials by their original row counts. The
forecast has 25% headroom and is capped by the existing statistical reservation
hint; actual insertions can always grow beyond either estimate. This changes
memory reservation only, preserving operator order, grouping, and aggregate
semantics. Reused workspaces retain their allocated capacity and reset the
observed row counters. Developer profiles report actual groups, hash bytes,
state capacity/bytes, growth count, and worker-summed allocation time.

Group accumulator storage grows in pages of 8,192 records. A small first page
can grow up to that bound; subsequent growth adds pages without moving existing
records. Key packing and record widths are unchanged. State pages reserve for
the next batch rather than copying a large slab to match a speculative forecast;
the hash table retains its bounded forecast. Published staging buffers transfer
ownership to the group queue, and replacement buffers are acquired only when
another append needs them. Numeric count-ranked top-N compares count and key
before constructing a full candidate record.

Grouped programs carrying variable-length aggregate inputs give ready grouping
work priority over staging and scanning. Staging yields after one chunk batch
so consumers can release payloads before producers allocate more. Numeric-only
programs retain their existing scheduling policy. The shared raw/group recycle
pool for variable-length inputs retains at most 2 GiB of slab, reference and
payload capacity, further capped at one eighth of the query and shared memory
budgets. Surplus idle buffers are freed; live buffers and aggregate state remain
subject to the ordinary allocation budget. These programs use reclaimable
workspace allocation even when the diagnostic arena-workspace option is set.

Count-only grouped programs use immediate updates while a bucket's live state
is below 2 MiB. Larger live states prefetch existing accumulator records and hold
up to sixteen pending count increments per worker's batch in stack storage. This
physical-kernel choice uses actual live bytes and preserves the query plan. The
batch reserves state capacity before probing, so these record addresses remain
valid until every pending increment is applied. New groups initialize their
counts immediately. A separate kernel keeps deferred-buffer branches and live
registers out of the ordinary update loop. Other aggregate programs retain their
existing update order; pending counts are drained before batch completion can
release work credits.

Consumers that explicitly support encoded input may consume pinned raw or RLE
blocks during a scan callback. The callback completes before pins are released,
so ordinary batch lifetimes do not change. Integer global reductions fold RLE
values and lengths directly and borrow raw views. Count-only grouped pipelines
can merge non-null integer key runs without expanding their logical rows first.
Derived inputs, filters, unsupported encodings, and tombstoned segments retain
the ordinary scan path. Results are computed on each execution; this does not
cache aggregates or change the stored representation.

Block-pruned top-N workers borrow raw probe columns, retaining pins through
predicate evaluation and candidate selection. Only retained candidate keys are
copied; output payloads are fetched after selection. Non-viewable encodings use
owned decoding, and each worker reuses its view and predicate-mask buffers.

Parallel grouping collects its final candidates only after every scan producer
has closed and all published staged rows have been aggregated. A row stays
counted as unfinished while queued, being partitioned, held in a partial bucket
buffer, or being aggregated. Weighted run partials count once in this work
counter; their weights still determine aggregate values. Empty queue snapshots
cannot establish completion during a hand-off. Failed folds retain their work
count until the query aborts, and aborted workers skip candidate collection.

The pipeline runs in pull mode (Volcano-style): `Sink.next()` pulls from upstream, which pulls from its upstream, etc. Each operator's `next()` returns a `Batch` — a small struct holding column slices for the rows currently in flight.

Hot kernels (predicate evaluation, aggregation accumulators, arithmetic) use `@Vector(N, T)` for SIMD. Vector width is platform-dependent; code is written generically and the compiler chooses.

### 6.4 Caching

Three-tier caching, only one of which we explicitly manage:

| Tier | What | Storage |
|---|---|---|
| 1. Always resident | Manifest, segment footers, schemas | In-memory, loaded on open / on manifest update |
| 2. LRU-bounded | Decoded row group column blocks | In-memory, keyed by `(segment_id, row_group_id, column_id)` |
| 3. Free | Raw segment bytes | OS page cache |

The LRU cache is bounded by a configurable size (default **2 GB**, set at `Database.open`). Eviction is strict LRU. **Cache entries are never invalidated** — segments are immutable, so cached decoded data is correct forever. When a segment is garbage-collected after compaction, its cache entries become unreachable from new queries and age out naturally.

---

### 6.5 Keyed pipeline regions

SQL blocks declared with `WITH KEYED BY (...)` can compile a supported CTE
subtree into one region: partition once at entry, execute the operator chain
within each shard, and publish its output as an ordinary materialized stage.
The compiler verifies the key contract at each candidate boundary. It may
select an inner CTE below a join, window, alias, or primary TVF input; the
outer operations then execute through the staged engine. The declaration
requests regional execution wherever compatible; a block with no valid
region uses ordinary execution. Incompatible window partitions, coarser
groups, and global sort/limit boundaries can feed a later region through
staged ingress. Earlier compatible CTEs can independently form regions within
that ingress, preserving the user's operator order. Invalid SQL still raises
its ordinary error. Stage provenance and region traces distinguish actual
engagement from fallback.

SQL windows use the ordinary window evaluator on complete shard-local
partitions. The compiler resolves partition, order, argument, and output
names once, and verifies that every window partition retains the routed key.
Different specs share sorting when their partition/order keys match; frame
and NULL behavior remains per call. Worker instances borrow input columns
and retain output buffers. Ranking, distribution, offset, value, and aggregate
window functions therefore share semantics with ordinary SQL rather than
requiring separate regional implementations.

Frame evaluation distinguishes physical rows (`ROWS`), peers (`RANGE`), and
peer groups (`GROUPS`). Bounded `RANGE` currently accepts one numeric order
column; decimal offsets respect the order column's scale. `FIRST_VALUE`,
`LAST_VALUE`, and `NTH_VALUE` honor the actual frame, including empty frames
and `IGNORE NULLS`. Temporal range offsets and `EXCLUDE` remain unsupported.

Entry expressions may replace SQL-visible input names while retaining their
original inputs under distinct physical slots. Projection expansion uses the
same rules as ordinary SQL. Cached runs reconstruct the same entry recipe
against fresh snapshots.

Ordinary `UNION ALL` can feed a region without table functions. Its branches
and entry projections/filters compile through the staged SQL compiler,
preserving positional column naming, type widening, duplicates, and shared
or explicitly materialized CTEs. The resulting stream is partitioned once;
supported downstream operations execute within the region. Union ingress
currently uses one scatter worker, with normal parallel execution available
inside its SQL branches. When there are fewer input streams than workers,
bucket sort buffers are reserved after ingress and the independent sorts run
in the parallel shard phase. This preserves arrival-order tie breaks without
concurrent allocation from the input worker's arena.
Every cached run rebuilds the source against fresh
snapshots. Grouping exactly by the current range keys is supported as one
aggregate group per range, including NULL keys and all-NULL values.

`UNION ALL` branches that reference the same CTE input can instead remain
inside one region. The compiler retains the shared frame, executes each
branch's filters, projections, expressions, compatible windows and joins,
then concatenates their outputs within each declared-key partition. Nested
unions use the same rule. Frame snapshots borrow column buffers; each
branch owns its result buffers until the union consumes them. Filters and
joins retain empty range positions so branches can be aligned without
another exchange. Positional names, casts and NULLability use the ordinary
union's column planner, and left-before-right order preserves window ties.

Fusion requires value-identical partition keys in corresponding output
positions and an exclusively consumed shared input. Forced materialization,
externally shared CTEs, key replacements, incompatible windows, branch
aggregation/table functions and joins depending on the retained input keep
ordinary staging. A failed fusion attempt retries the existing staged-ingress
path at the same boundary. Cached programs recheck both the source versions
and the branch-sharing recipe before reuse. These are structural SQL rules;
no query text, table name or UDF name selects the optimization.

Branch collection checks join kind, range conditions and residual predicates
before preparing sources or lookup tables. A branch already filtered to false
also retains ordinary staging so its joins can be pruned. These structurally
unsupported forks do not execute speculative join inputs before fallback;
compatible regions above or below the fork remain available.

Failures that still require data-dependent proofs, such as duplicate lookup
keys, can reuse a bounded per-database rejection hint. Its fingerprint includes
the input/table versions, declared keys, CTE sharing, session/compile context
and immutable scalar kernel identity. Changed inputs retry; volatile calls,
unversionable sources and UDFs without an immutable execution contract do not
retain rejection hints. The hint stores no rows and only skips fusion; the
ordinary fallback always recompiles. Join-input compilation/execution errors
retain their error identity instead of becoming cached eligibility failures.

Regions whose program folds to only emission use the ordinary scan path,
avoiding an exchange and consolidation without shard-local work. Column
movement supports every stored payload type, including Boolean and UUID
values and their validity bits. Broadcast and co-partitioned join payloads
retain their right-side qualifier; they cannot overwrite a same-named left
column. Explicitly selected right join keys retain their own values and NULLs.
Cached lookup and emptiness proofs include the table's cache UID and
tombstone generation, so recreating a table or deleting only persisted rows
invalidates them even when memtable and manifest counters are unchanged.

Consecutive entry projections preserve their evaluation order: only the
lowest projection is absorbed into the scan entry; later projections and
their intervening computes execute in the region. An unordered, row-aligned
table function with an `.either` execution contract does not fix the initial
range granularity when a later operation requires a different partition.
The existing partition checks still determine whether each call can run
per range or over the complete shard.
Passthrough TVF outputs use their declared string-family type even when the
input uses another compatible string type. Borrowed views preserve the
original bytes and NULL bitmap without copying or changing input columns.
A TVF input whose type differs from its declaration converts in a region only
through a cast between DATE and DATETIME, which can't drop a value, and only
on a column that is neither a range key nor the routed column. Any other
conversion (§6.2) leaves the query to ordinary execution, so both paths
return the same rows and raise the same errors.
Frame-replacing TVFs retain routed-key provenance only under their existing
`ordered_output` contract: the call's partition columns must be present in
the output and preserve their values. The compiler binds the route to that
new physical output column. Unmarked kernels use ordinary execution.
Aggregation similarly carries provenance through an unchanged group key,
and remaps constant-column bookkeeping to the new frame. Windows and
co-partitioned joins check this physical identity, not a reused SQL alias;
computing a replacement key does not inherit it.
Entry-filter constants survive a replacing TVF only when its partition-value
contract preserves them, and survive aggregation only through group columns.
Reusing their names for changed outputs cannot keep an earlier literal join
shortcut.

For an unchanged declaration, the cache remembers which inner CTE boundary
compiled successfully. Repeated queries validate that boundary directly,
avoiding repeated evaluation of join inputs for unsupported outer candidates.
The declaration fingerprint covers the original subtree, source table
versions, and table-function identities before compilation's shared-IR
rewrites; the stored anchor hash identifies its cached program.
The declared keys, kernel identities, consumed table versions, and fresh scan
schemas are revalidated. Operations above the cached boundary compile
normally against current data.
For temporary tables held entirely in memory, up to 16,384 rows and 4 MiB,
cache validation fingerprints the complete read schema and ordered contents
under the table lock. Identical replacements can reuse the program across
sessions; changed values, NULLs, types, or ordering invalidate it. Larger or
spilled temporary tables disable this program cache instead of relying on
reusable allocation addresses as table identities.

Regional windows append results in the original row positions, so independent
window orders can coexist. Integer sums follow §3.4: `SUM` over an integer
up to BIGINT accumulates in i128 and returns BIGINT, wrapped; `SUM(LARGEINT)`
uses checked i128 accumulation and returns `LARGEINT`.
Consolidation keys retain all 64 integer bits plus a distinct NULL marker;
adjacent BIGINT values must never collapse into one partition.

See [REGION_PLAN.md](docs/plans/REGION_PLAN.md) and
[REGION_ELIGIBILITY_PLAN.md](docs/plans/REGION_ELIGIBILITY_PLAN.md) for the
runtime design, supported constructs, and remaining work.

### 6.6 Recursive CTEs

`WITH RECURSIVE` lets any CTE in its list read itself; ordinary CTEs and
column lists work in the same list. The rules are MySQL 8.4's. A recursive
body is one or more anchor SELECTs that do not read the CTE, then `UNION ALL`
or `UNION [DISTINCT]`, then one or more recursive SELECTs. Each recursive
SELECT reads the CTE exactly once, in its FROM clause (joins are fine, but not
as the nullable side of an outer join), never in a subquery or derived table.
A recursive SELECT may not aggregate, use a window function, GROUP BY, ORDER
BY, LIMIT or SELECT DISTINCT, or sit under INTERSECT or EXCEPT. Each rule has
its own parse error (`SqlRecursiveCte*`). A LIMIT/OFFSET over the whole body
is allowed and stops the recursion once enough rows exist.

The anchor fixes the columns: names from the column list, else the anchor's;
types from the anchor, except that an integer narrower than BIGINT widens to
BIGINT. MySQL types an integer literal BIGINT where thinDB types it by its
value, and a counter seeded by `SELECT 1` must not stop at INT. Recursive
rows land in those types as a write into a table's columns does (§9.8
`ValueOutOfRange`): a fraction rounds half away from zero into an integer,
a number becomes its text in a string column, a value the column can't hold
fails the statement. Pairs outside that rule, such as a wider DECIMAL, convert
as CAST converts them, and a type with no conversion is a `TypeMismatch`.
Strings keep their full value; thinDB does not enforce
`CHAR(n)`/`VARCHAR(n)` lengths, so a string that grows past the anchor's
length is kept where MySQL's strict mode fails with 1406. Every column is
nullable.

Execution is semi-naive. The parser marks the CTE's `materialize` node with
the anchor, the recursive arms and the union kind; each self-reference is a
marker node. The CTE is an ordinary stage (§6.3), so later references, joins
and aggregates read its materialized result like any other CTE's. The stage's
query is one iteration driver (`src/net/recursive_cte.zig`). It runs the
anchor, and then, while the latest iteration added rows, compiles the
recursive arms afresh with every self-reference bound to a one-stage working
set holding exactly those rows, and runs them. Each iteration's rows are
streamed out and kept only as the next working set. Under `UNION DISTINCT`
the driver keeps every row it has produced as key bytes and drops a row it
has already seen, so a cycle ends once it adds nothing new. Under `UNION ALL`
a cycle needs its own bound. Mixing ALL and DISTINCT after the anchor counts
as DISTINCT. The rows of the current and next iterations and the DISTINCT key
set count against the query's memory accountant.

The driver aborts before iteration 1001 if the working set is not empty yet
(`RecursiveCteDepthExceeded`), MySQL's default `cte_max_recursion_depth`;
the limit is fixed, as thinDB has no session variable for it. A recursive CTE
is compiled at run time, so it can't be encoded for the native protocol or an
XA branch.

### 6.7 Correlated subqueries

A correlated subquery is rewritten before execution onto operators the
engine already has (`src/net/subquery_resolve.zig`); nothing runs it again
per outer row. Its names bind as SQL scopes them (§9.8), and the ones it
reads from enclosing queries, one level out or more, are its correlation.
Two strategies apply, the first that fits.

Keyed. When the subquery's WHERE ties its own columns to enclosing values
only by equalities (`inner = outer`) and ranges (`inner < outer`), its inner
block runs once without those terms and projects their inner sides. The
outer side may be an expression over the outer row (`y.id = x.k + 10`),
which the enclosing operator computes first. EXISTS, IN and a single
aggregate compared in a WHERE become a lookup each outer row probes with its
own values; any other scalar is grouped by its keys and LEFT JOINed back.
A DELETE or UPDATE predicate and a join's ON take it first too; the rest of
theirs is below.

The rows a lookup probes are sorted by key when drained, so each outer row
finds its key by binary search, in O(log rows) rather than a scan of every
row. Validation brings the keys to the outer columns' types; where that
leaves them out of order, as text against a number (`'10'` sorts before
`'9'`), or the key is a float, double or JSON, the lookup scans instead. A
lookup runs only for the rows the operator's cheaper conjuncts, which run
first, left passing.

Inner keys that differ can come to one outer value: text compared with a
number, a DATE or a DATETIME is read as that type (§3.1), so `'7'`, `'07'`
and `'7.0'` are all 7. A range lookup then reads every group with that key.
The forms that aggregate (the keyed scalar lookup, and the scalar grouped and
joined back) group the inner rows by each such key as the comparison reads
it, so the rows that read as one value form one group, as the comparison
makes them one. Grouped as written, an outer 7 would meet three groups: the
join back would return its row three times, and the lookup would find three
aggregates, none of them the subquery's. Such a key is grouped, counted for a
LIMIT, looked up and joined on its read value. Any other key is grouped as
written: its comparison, if it converts at all, keeps distinct values
distinct, as numbers meeting in a common type and text meeting text do. A
keyed scalar planned where the enclosing operator's rows aren't known (a
join's extra predicate) can't read its keys' types, groups as written, and
raises `UnsupportedCorrelatedSubquery` for an outer row that finds two
groups rather than compare one.

A WHERE that ORs equality and range ties where no one key covers all of them
(`i.v = o.v + 1 OR i.k = o.k`) is keyed once per disjunct, the other
conjuncts in each copy. A row passes the WHERE iff it passes some copy's,
so EXISTS and IN hold iff they hold for some copy, and NOT EXISTS and NOT IN
iff for none: the lookups are ORed, or for the negated forms ANDed. That
holds only for a block that keeps or drops each row by itself, so one that
groups or windows isn't split, and every disjunct must key, or the subquery
goes to the domain whole.

Domain. Any other correlated subquery in a filter or an expression is lifted
onto its domain: the distinct combinations of the enclosing values it reads,
drawn from the rows the enclosing operator reads. The subquery's FROM joins
the domain. A WHERE conjunct comparing a FROM column with a domain value by
`=` becomes a hash key, and one by `<`, `<=`, `>` or `>=` a range; every
other correlated term (under OR, reading both rows, `<>`) filters the joined
pairs. Each GROUP BY and window partition also groups by the domain values,
and a LIMIT keeps its rows per domain row, numbered in the ORDER BY under
it. An aggregate without GROUP BY below the top of the block still yields a
row for every domain row, over no rows where none reached it. The result,
keyed by the domain values, is LEFT JOINed back to the enclosing operator's
rows:

- EXISTS and IN read whether a marker row matched. IN compares its columns
  with `=`.
- A scalar aggregate reads its value. An outer row whose domain row reached
  no inner row reads the aggregate over no rows: COUNT is 0, the others NULL.
- Any other scalar reads its row through SINGLE_ROW: `SubqueryMultipleRows`
  for an outer row that matched more than one, NULL for none (§9.8).

In the join with the FROM, a NULL enclosing value compares with plain `=`
and matches no inner row; the join back matches NULL to NULL, so its outer
row still finds its domain row (a count of 0, a NULL scalar). NOT IN keeps
thinDB's IN-set dialect on every path: NULLs among the subquery's values are
skipped, and a NULL compared value never passes.

A FROM that itself reads enclosing values, as a LATERAL derived table would,
is lifted in place rather than joined. A relation that reads them carries the
domain values through its own operators, its WHERE staying an ordinary
filter over the domain rows it joined; a join between two such relations
matches on them too, and a relation that reads none is crossed with the
domain. A FULL JOIN with a side that reads them isn't lifted: its unmatched
rows would need every domain row. A UNION inside the subquery lifts each
arm the same way, an arm that reads nothing enclosing crossed with the
domain so every domain row sees its rows. A `SELECT *` in a lifted block is
first spelled out from the columns its relations carry; a star it can't
spell out, such as one over a join whose columns share names, isn't lifted.

A join's ON inside the subquery may read enclosing columns too. An inner
join's ON filters the pairs it makes as a WHERE above it would, so either
strategy takes its correlated terms as a WHERE's (`JOIN b ON a.id = b.id
AND b.v = x.v`). An outer join's conjunct that reads enclosing columns and
nothing of the preserved input filters the other input before the join: it
holds or fails for a whole row of that input, whatever row it would pair
with, so every preserved row is still kept (`LEFT JOIN b ON a.id = b.id AND
b.v = x.v`). One that also reads the preserved input, or a value the ON
computes, stays in the join's residual, which checks each pair once both
inputs carry the domain values. A FULL JOIN's isn't lifted, as above. A name
in an ON belongs to the subquery's FROM before any enclosing query: one
qualified by a relation of the same FROM that the ON can't see (`FROM a, b
JOIN c ON a.id = c.bid`) raises `SqlOnRefsUnknownTable`, as at the top level.

An aggregate in a subquery whose arguments read only enclosing columns
(`SUM(x.v)`) belongs to the enclosing query, as SQL scopes it. Before either
strategy runs it moves there: into the enclosing GROUP BY, or a new global
aggregate when the enclosing SELECT has none, and the subquery reads it as an
enclosing value, keyed or lifted like any other. An expression over such
aggregates and the subquery's own (`SUM(x.v) + COUNT(*)`) reads the
subquery's one aggregate row, joined to every outer row. Nested in another
aggregate, or beside an ungrouped outer column, it has no query to aggregate
in, and the statement fails as it does in SQL.

A DELETE or UPDATE predicate that compares a correlated scalar with a
literal or a value over the target's row, the subquery on either side
(`(SELECT COUNT(*) ...) = 1`), is read as the parser spells the subquery on
the right (`1 = (SELECT ...)`), so the keyed path takes it and the statement
filters its own scan, on a target with or without a primary key. A
DELETE or UPDATE whose predicate no keyed path takes lifts it over the
target's own rows, as a SELECT whose domain is drawn from them, and writes
the rows that SELECT keeps, found again by the target's primary key, as the
multi-table form does. The values its predicate computes first (a scalar
under arithmetic, IS NULL or COALESCE) and the correlated values an UPDATE
assigns read those rows as their domain too. A target without a primary key
has no sound row identity to find them by, so the statement raises
`UnsupportedCorrelatedSubquery`. An outer join whose ON no keyed path takes
numbers each input's rows once in a buffer. The pairs its keys and ranges
match are the domain for the condition and the values it computes; the pairs
the condition keeps, as number pairs, are what the outer join then matches
on, so a row with no kept pair is still NULL-extended.

The enclosing operator's conjuncts that read no subquery narrow the domain's
source first. A source cheap and deterministic to run again (a base-table
scan under filters, projections and computes free of volatile functions such
as RAND, NOW or UUID, or a buffer already materialized) is replayed to build
the domain. Any other, such as a join, aggregate or window, is materialized
once, charged to the statement's memory accountant, and read by both.

A lifted join with neither a key nor a single range (only OR, `<>` or mixed
terms, as a scalar tied by OR or an EXISTS one of whose ORed ties doesn't
key) runs as a nested loop. It buffers the subquery's FROM rows and the
domain rows, charged to the accountant (`MemoryBudgetExceeded` past the
budget), and streams the pairs. The budget bounds those buffers, not the
pair count: the time is |FROM| × |domain| pairs, about 50–70 million a
second on one core. The loop checks for cancellation every 65,536 pairs, so
KILL and a disconnect stop it promptly (§8.3).

A lifted item keeps the name its select gives it. One that returns an
enclosing column as-is (`SELECT x.k`) reads the domain's column once
lifted, yet is still named `k`. A select's output names bind in the
operators above it before any column does, so an item named by text that
opens with an enclosing qualifier (`SELECT x.k + z.id`, whose name reads as
column `k + z.id` of `x`) is that item, not an enclosing column.

A shape neither strategy carries raises `UnsupportedCorrelatedSubquery`
(§9.8); it is never compiled with an outer name bound to an inner column.

---

## 7. Compaction

### 7.1 Triggers

Compaction runs on the writer thread when **any**:

- The table has more than 8 small segments.
- The oldest small segment is older than 15 minutes.
- Explicit `db.compact(table)` call.

A "small" segment is one below ~256 MB of compressed data. Large segments are only re-compacted when enough smaller neighbors accumulate.

### 7.2 Strategy

Tiered. Compaction picks a set of adjacent (by `segment_id`) small segments, k-way merges them on the order key, and writes a single new segment. Tombstoned rows from the input segments are dropped (not carried into the output).

Steps:

1. Pick the input segment set.
2. Open scanners for each, loading their `.tomb` files into bitsets.
3. K-way merge by order key, skipping tombstoned rows.
4. Stream into a new `<seg_id>.dat` file with the same encoding logic as flush.
5. Update the manifest: remove the inputs, add the output.
6. Schedule the input `.dat` and `.tomb` files for deletion. Files are deleted after a grace period (default 30 seconds) to let in-flight readers finish.

### 7.3 Concurrency

Compaction builds output away from the table mutex. The commit phase reconciles concurrent deletes under the mutex, publishes output tombstones, and then publishes the candidate manifest. A separate compaction lock prevents overlapping compactions and excludes XA rollback from an in-flight merge.

The background compactor holds a statement lease only while it resolves a table and picks its group. The merge and its commit run under the compaction lock alone, so DDL and XA COMMIT on other tables never queue behind a merge. Every path that frees or rewrites a table takes that lock first: DROP, RENAME, ALTER, TRUNCATE, XA COMMIT, and schema teardown (DROP SCHEMA, DROP DATABASE, shutdown).

Those paths often hold the catalog gate exclusively, which queues every new statement, so they never wait out a background merge of their table. They raise the table's preemption request (`Table.lockCompactPreempting`) before taking the lock. The merge checks for it before each input opens, before each output row group is encoded, before the output file is written, and while it waits to commit. On seeing it, the merge deletes whatever it wrote, releases the lock and reports no progress; its inputs stay published for a later sweep. A DDL therefore waits for one row group's encode or the output file's write, not for the whole merge. An explicit `Table.compact()` is not preemptible. Shutdown waits for the sweep before tearing schemas down, so it lets a merge finish rather than stopping it.

A background commit waits at most 60 seconds for the table's readers to drain. The table's DDL lock prefers readers, so on a table that is never idle the commit would otherwise hold the compactor, which serves every table, indefinitely. After that it deletes its unpublished output and leaves the inputs for a later sweep. DDL never waits on this deadline, since it preempts the wait.

The background flush sweep, by contrast, keeps a statement lease on a table for the whole of that table's flush. An XA COMMIT's rollback restores its tables' manifests from the journal and would drop a segment a concurrent flush had published. The flush is bounded by the memtable's auto-flush thresholds (§5.3), and a foreground write that crosses them runs the same flush inline under its own lease, so the sweep stalls DDL no longer than a write already can.

---

## 8. Concurrency

- **Per-table mutex** serializes memtable + WAL mutations. Multiple writer threads may call `insert`/`upsert`/`delete`/`flush` concurrently; they line up at the mutex one record at a time. Each `Table` has its own mutex, so writes to different tables run in parallel.
- **Many reader threads.** Scans capture snapshots under the table mutex, then release it. Query lifetime leases prevent destructive catalog teardown and XA publication from invalidating borrowed table state.
- **Manifest update is atomic** via `rename`. Readers always see either the pre- or post-state, never partial.
- **Memtable snapshot isolation.** Scans pin a refcounted snapshot of the memtable at start; concurrent writers see a fresh memtable. Long readers and active writers never block each other.

The atomic-rename semantics of `manifest` are load-bearing. On Windows, `MoveFileEx` with `MOVEFILE_REPLACE_EXISTING` provides the same atomicity for same-volume renames.

### 8.0 Memtable snapshot isolation

The memtable is heap-allocated and reference-counted:

- The `Table` holds one reference. A scan that captures the memtable holds another via `Memtable.acquire`.
- `flush`, `delete`, `upsert` (when they would mutate existing rows) all do **retire-replace**: allocate a new empty memtable, atomically swap the table's `memtable` pointer, mark the old one retired, release the table's reference. The old memtable's columns are never mutated again, so any reader iterating it is safe.
- When a reader releases its reference and refcount drops to zero, the retired memtable's buffers are freed.

The remaining hazard is a writer extending the **active** memtable while a reader is iterating it — an `ArrayList` append that triggers realloc would invalidate the reader's pointer. We close this with a small twist: `scan()` captures the memtable under the table mutex, and **if the captured memtable has rows, it forces a retire-replace right there**. The scan's snapshot becomes a frozen, retired memtable that no writer will ever touch again. The active memtable becomes empty; writers append to it without ever endangering this scan. Single overhead: one empty `Memtable.create` per scan against a non-empty memtable.

This is structurally similar to MVCC by full-snapshot — each long reader pins one retired memtable in memory until it finishes. Memory bound:

```
(1 active + N retired) × auto_flush_bytes
```

where N is the number of concurrent long readers. Bounded by workload, freed automatically when readers complete.

### 8.1 WAL group commit

When `sync_mode = .per_flush` AND `wal_enabled = true`, each `insert`/`delete` is durable on return. The naive implementation — fsync the WAL inside the table mutex — would serialize every writer through one fsync. Instead, we use leader-follower group commit:

1. Under the table mutex: mutate memtable, append WAL bytes, capture the cumulative `write_offset`. Release the table mutex.
2. Outside the table mutex: call `WalWriter.awaitDurable(target_offset)`.
3. In `awaitDurable`: if another fsync is in-flight (`in_progress == true`), park on a condition variable. Otherwise become leader.
4. **Adaptive coalescing pause** (key to amortization):
   - Leader spins for `coalesce_probe_ns` (20 µs) regardless of contention. Single-writer cost: ~20 µs added latency.
   - If `waiters` grew during the probe, the leader restarts the dwell clock and keeps spinning, up to `coalesce_max_ns` (200 µs total).
   - Otherwise fsync immediately.
5. Snap `write_offset`, call `file.sync()`, then broadcast. Followers wake; those whose target is now covered return immediately; others retry as the next leader.

The probe is unconditional because the "is anyone arriving" signal only appears *during* a pause — checking before the leader pauses would always see `waiters == 1` (just the leader itself, which has just incremented the counter on entry to `awaitDurable`).

Bench numbers (8 OS threads, `sync_mode=.per_flush`, tight insert loop, Windows / NVMe):

| Threads | Wall clock | fsyncs | inserts/fsync |
|--------:|----------:|-------:|--------------:|
| 1 | 67 ms | 250 | 1.0 |
| 2 | 77 ms | 256 | 1.95 |
| 4 | 87 ms | 270 | 3.70 |
| 8 | 127 ms | 370 | 5.41 |

Throughput scales sub-linearly with thread count (each fsync is now amortized over multiple writers), single-writer pays ~3% latency overhead vs. the no-pause baseline.

Truncate (called at end of flush) coordinates with `awaitDurable`: it drains the current leader, then bumps `synced_offset` to the pre-truncate `write_offset` so any pending waiters from before the truncate become no-ops (their data is now in a segment, not the WAL).

Tombstones never get ahead of the log. A tombstone can hide a row whose replacement, from an UPDATE or a unique-key upsert, so far lives only in the WAL. So every `.tomb` write (`Table.writeTombstoneFile`, `Table.mergeTombstones`) syncs the WAL first whenever sync is on.

---

### 8.2 Ownership and XA staged writes

A Catalog obtains an exclusive OS lock on `.thindb.lock` before recovery or temporary-file cleanup and holds it through close. Schema table initialization is serialized so one name has one Table/WAL owner. Dropping names remain reserved until teardown finishes.

Normal statements and API scans/writes hold shared catalog leases. XA commit and destructive DDL take an exclusive lease, from SQL or the API: `Schema.dropTable`, `alterTable` and `renameTable` take it themselves, as `dropSchema` and `dropDatabase` do. Nested calls reuse their calling thread's lease; attempting to upgrade while that thread still owns a query returns `TableBusy`.

A `Table` is freed only when no other statement can hold a reference to it: under the exclusive lease, or because it was never published where another statement could resolve it. A reader resolves a table before it takes the table's `ddl_lock`, so the `ddl_lock` a drop waits on does not cover that gap; the exclusive lease does. `CREATE TABLE ... AS SELECT` runs under a shared lease, since its query may run long, so it builds its table unpublished: under a reserved `__ctas_<n>` directory the schema's map does not hold, which no name resolves to and no listing shows. Once the query succeeds, the build is renamed to its name and registered in one step under the schema's table mutex, after a flush that leaves its log nothing live. A table or directory that took the name meanwhile fails the publish with `TableAlreadyExists`, or leaves `IF NOT EXISTS` a no-op. A failed query frees the build, which no other statement can have resolved. Nothing is logged under the final name before the publish, and a schema deletes every `__ctas_` directory as it opens, so recovery never brings back a failed or interrupted CTAS. A temporary table's CTAS creates it directly, since only its own session, busy with the statement, can resolve it.

CREATE MATERIALIZED VIEW fills its backing table as such a build too. It registers the view only once the table is published, and drops a table it created if the definition then fails to persist, so a failed CREATE leaves neither. REFRESH MATERIALIZED VIEW and CREATE OR REPLACE MATERIALIZED VIEW use a replacing build (`Schema.beginTableReplacement`), whose publish takes the name from the table that holds it. That table keeps its rows until the publish, which frees it, so a replacing build holds the exclusive lease from its start through its publish; on the wire those statements are exclusive DDL already. The publish is ALTER's swap (§9.2) with the build in the shadow's place: set the old table aside as `__alter_old_<name>`, rename the build into the name, then delete the aside. The build's rename commits. An open after a crash before it puts the aside back and deletes the build; after it, deletes the aside. So a reopen finds the old table or the new one under the name, never neither, and never both, since no name resolves to an aside or a build. A refused rename before the commit puts the old table back, or, if that rename is refused too, fences it until an open restores it. REFRESH keeps the backing table's columns, keys, compression and row group size, whatever an ALTER made of them.

Catalog close rejects new work and waits for live leases. Borrowed database/schema/table pointers are invalidated by drop or close; callers racing destructive DDL must hold a statement lease across lookup and use, as the wire handlers do. Asynchronous allocation cleanup retains a separate lifetime reference, keeping the catalog allocator and memory pool alive without holding up subsequent statements.

XA stores encoded write statements, their database, and their originating schema. PREPARE succeeds only after its bounded recovery record is atomically persisted. COMMIT keeps the branch recoverable while it validates targets, takes the exclusive visibility lease, snapshots manifest/WAL/tombstone metadata into `_xa/commit`, applies statements, and flushes each affected table. A durable completion marker decides recovery. Without it, startup restores the old metadata; with it, startup retains the committed data and removes the prepared record. Journal retirement uses a directory rename before cleanup so interrupted cleanup cannot turn a completed commit into rollback.

Statement errors trigger undo and leave the branch prepared for retry. If undo or the durable decision cannot be resolved, the catalog rejects further work with `RecoveryRequired`. Absent-branch COMMIT remains an idempotent success for the existing CDC integration. Prepared branches do not expire by default.

This is a staged-write protocol, not general SQL transactional isolation: reads inside ACTIVE do not see an uncommitted write set, and expressions are evaluated at commit. DDL is not part of the staged write set.

### 8.3 Query memory and cancellation

One thread-safe query resource context follows SQL physical operators and worker backends. Allocation wrappers charge requested live capacities, including variable-length payloads, hash state, sort permutations, and worker buffers, against per-query and shared limits. Estimates remain useful for planning, but do not enforce these limits. Rejected growth returns `MemoryBudgetExceeded`; there is no spill fallback yet.

A refused allocation, like any other error, must reach the client as the statement's error. Run-once barriers (a shared stage's run, a table function's execution, a window's drain) remember the error of a failed run and return it on every later call. They never run again: the failed run has already consumed part of its pipeline, so a second run would return only the rows after the failure point, often none, as a complete result. A consumer that would read a stage's result in place (a table function or window borrowing its columns) propagates the stage's error. It never falls back to pulling the stage through a scan.

Result buffers and metadata remain charged while owned. Retained region-pool capacity is charged when borrowed by a query and detached when returned to the separately capped pool. Asynchronous frees return reservations only when the corresponding storage is released. Allocator bookkeeping, allocator-internal rounding/freelists, parser/protocol buffers, database metadata/memtables, and the separate source cache are not an exact process-RSS ceiling.

Worker-side scratch (stage, window and partition arenas, parallel-scan decode buffers, table-function and row-location scratch) comes from one seam, `memory.workerAllocator`. It draws from the process-wide retaining scratch pool and charges every block the query holds at its whole size class. Pooling saves the OS round trip between queries; it never takes a live block out of its query's budget. Idle pooled blocks are uncharged and bounded by the pool's own cap.

Some stages are read in place by a join build, window or table function. Such a stage materializes one contiguous worker-allocated store per column. The serial fill grows each store from the rows that arrive, and each growth frees the buffer it outgrew. The parallel fill sizes each store once, from the exact totals it collects first. The stage's compile-time row bound is only a ceiling and never sizes these stores: it can be millions of rows over a result of a few.

A watchdog compares process memory with what the budgets explain: resident memory minus the block cache, the idle scratch pool and all accounted query bytes. It samples at stage boundaries, at every 1/16 of the budget of accounted growth (64 MiB–1 GiB), and at statement end. When the gap passes max(2 GiB, budget/4) it logs one `[mem-watch]` line per statement, naming the statement and PROCESSLIST ids.

Wire handlers reset their cancellation token at statement acceptance, before parsing/compilation. Compilation and eager subqueries share the token with execution. Scans, worker scheduling, sort partitions/passes, regional operations, nested-loop joins, and merge loops check it cooperatively. `QueryCancelled` unwinds ordinary resource ownership. Polling does not preempt a native UDF callback or an operating-system I/O call; this is cooperative cancellation, not a hard latency guarantee.

The server trips the same token when a client disconnects mid-statement, on either wire. Its connection reaper probes each connection's socket every 5 s, and cancels a statement whose only product is its result set (not a write, DDL or EXPLAIN) once the peer has closed. Writes run to completion, as in MySQL.

Each connection records what it is doing: its user and client address, current schema, command, and when that command began, plus up to 1 KiB of the running statement's text. `SHOW [FULL] PROCESSLIST` lists that record for every connection, so a runaway statement's id can be found and passed to `KILL`. The same records are the read-only relations `information_schema.PROCESSLIST` (and `performance_schema.processlist`) on the MySQL wire and `pg_stat_activity` on the PostgreSQL wire, so tools can filter, sort and project them. Every wire draws connection ids from the one registry, so an id names a single connection whichever wire lists or kills it.

The same reaper bounds a stalled socket transfer. Every send on a MySQL or PostgreSQL connection carries a write mark, and a send that has not completed within `THINDB_NET_WRITE_TIMEOUT_SECS` (default 60 s, MySQL's `net_write_timeout`; 0 disables) ends the connection. A client that stops reading its result otherwise keeps the statement's lease, and with it every exclusive DDL behind it, and its core slot, for as long as it likes. On the MySQL wire a read inside a packet is bounded the same way by `THINDB_NET_READ_TIMEOUT_SECS` (default 30 s, `net_read_timeout`). The reaper polls every 5 s, so either limit can run up to 5 s long. It aborts the connection instead of closing the handle: POSIX `shutdown` fails a blocked send or read at once. On Windows a graceful shutdown completes neither, so the reaper resets the connection (an abortive AFD disconnect), which does. The statement then fails with a write error, and the thread unwinds as for a client disconnect.

`KILL QUERY <id>` and `pg_cancel_backend(pid)` trip the target's token. `KILL [CONNECTION] <id>` and `pg_terminate_backend(pid)` also mark the target for closing and shut its socket down (never close it) under the registry lock, which excludes the target's unregister, so the handle stays valid. The target's thread then leaves through the same path as a client disconnect: its blocked read or write fails, or its command loop sees the mark, and unwinding releases the statement lease, temp tables, an ACTIVE XA branch and the session. On Windows a shutdown does not complete a read already pending, so an idle target leaves only once its client closes its end or the stack times the half-closed connection out. A connection that kills itself gets the statement's interruption error (MySQL 1317, PostgreSQL FATAL 57P01 for a terminate) before it closes.

## 9. API

The public Zig API. v1 has no other client surface.

### 9.1 Open/close a database

```zig
const thindb = @import("thindb");

var db = try thindb.Database.open(allocator, .{
    .path = "C:/data/mydb",
    .cache_size_bytes = 2 * 1024 * 1024 * 1024,
    .flush_interval_secs = 5,
});
defer db.close();
```

### 9.2 Create/alter/drop tables

```zig
try db.createTable("orders", &.{
    .{ .name = "id",        .type = .bigint },
    .{ .name = "user_id",   .type = .bigint },
    .{ .name = "total",     .type = .{ .decimal = .{ .precision = 18, .scale = 2 } } },
    .{ .name = "status",    .type = .{ .varchar = 32 } },
    .{ .name = "placed_at", .type = .datetime },
    .{ .name = "note",      .type = .string, .nullable = true },
}, .{
    .order_key = &.{"id"},
    .unique = true,
});

try db.alterTable("orders", &.{
    .{ .add    = .{ .name = "discount", .type = ..., .default = .{ .decimal = 0 } } },
    .{ .drop   = "legacy_status" },
    .{ .rename = .{ .from = "total", .to = "amount" } },
    .{ .change_type = .{ .name = "user_id", .new_type = .bigint } },
});

try db.renameTable("orders", "orders_v2");
try db.dropTable("orders_v2");
```

`ALTER TABLE` is implemented as orchestrated copy-and-swap:
1. Build the rewritten table in a shadow directory, `__alter_new_<name>`, writing its manifest last.
2. Set the original aside as `__alter_old_<name>`.
3. Rename the shadow into place. This rename commits the swap.
4. Delete the original.

Each rename is followed by a directory sync. When a schema opens, it resolves any swap that a crash or a persistent refusal interrupted. Before the commit, it puts the original back. After the commit, it deletes what is left over. DROP TABLE and RENAME TABLE delete an original that a committed swap failed to delete, so the next open can't restore it under the old name. A replacing table build (REFRESH MATERIALIZED VIEW, §8.2) publishes with steps 2 to 4, its build in the shadow's place, and recovers the same way. Names starting `__alter_` or `__ctas_` (a table build, §8.2) are reserved: no table can be created or renamed under one, and none is listed or opened.

Writes are paused during the copy; reads see the old version until swap, the new version after. Each segment is rewritten under its own id with its rows in the same order, so its `.tomb` and `.bloom` sidecars are copied into the shadow unchanged: deletes and key filters carry over.

### 9.3 Inserts

```zig
const orders = try db.table("orders");

// Row-oriented (primary surface)
try orders.insert(&.{
    .{ .id = 1, .user_id = 10, .total = .{...}, .status = "paid",    .placed_at = ..., .note = null },
    .{ .id = 2, .user_id = 11, .total = .{...}, .status = "paid",    .placed_at = ..., .note = "rush" },
    .{ .id = 3, .user_id = 10, .total = .{...}, .status = "pending", .placed_at = ..., .note = null },
});

// Columnar (bulk path)
try orders.insertColumns(.{
    .id        = &id_arr,
    .user_id   = &user_arr,
    .total     = &total_arr,
    .status    = &status_arr,
    .placed_at = &placed_at_arr,
    .note      = &note_arr,
});
```

### 9.4 Deletes

```zig
try orders.delete(.{ .col = "status", .op = .eq, .val = .{ .string = "cancelled" } });
```

Predicate-based; scans all segments and the memtable, emits tombstones for matches. See §5.5.

### 9.5 Queries

Each builder method returns a `Query` value carrying its output schema as a comptime type parameter. Queries are lazy — no work happens until `.next()` or `.collect()`.

```zig
var q = orders.scan()
    .filter(.{ .col = "total", .op = .gt, .val = .{ .decimal = ... } })
    .project(&.{ "id", "total", "placed_at" })
    .order_by(&.{ .{ .col = "placed_at", .desc = true } })
    .limit(100);
defer q.deinit();

while (try q.next()) |batch| {
    const ids   = batch.column(i64, "id");
    const totals = batch.column(Decimal, "total");
    // ...
}
```

`batch.column(T, "name")` is a comptime check: typos or stale column references are compile errors.

### 9.6 Composition: `.pipe()`

`Query` values are themselves sources. Variables hold intermediate stages; functions over `Source` are reusable transforms; `.pipe()` glues them.

```zig
fn last7Days(source: anytype) @TypeOf(source.filter(undefined)) {
    return source.filter(.{ .col = "placed_at", .op = .gt, .val = .{ .datetime = now() - 7*day_us } });
}

fn topUsersByRevenue(source: anytype) Source(.{ .user_id = .bigint, .revenue = .decimal }) {
    return source
        .group_by(&.{"user_id"})
        .aggregate(&.{ .{ .col = "total", .op = .sum, .as = "revenue" } })
        .order_by(&.{ .{ .col = "revenue", .desc = true } });
}

const top = orders.scan()
    .pipe(last7Days)
    .pipe(topUsersByRevenue)
    .limit(10);
```

`.pipe()` also accepts a placeholder-rooted chain:

```zig
const recent_paid = thindb.placeholder(OrdersSchema)
    .filter(.{ .col = "status", .op = .eq, .val = .{ .string = "paid" } })
    .filter(.{ .col = "placed_at", .op = .gt, .val = .{ .datetime = since } });

const q = orders.scan().pipe(recent_paid).limit(10);
```

Both forms are fully typed at comptime. `.pipe(f)` is zero-cost — Zig inlines it to `f(source)`.

### 9.7 Streaming vs materializing

- `.next()` — pull one batch at a time. No materialization. Default for forward-only consumption.
- `.collect(allocator)` — run the query to completion, materialize into an in-memory `Table` that is itself a `Source`. Useful when the result is small and you want to fork or re-query it.

### 9.8 Errors

All fallible API calls return a Zig error union. The public error surface is split between `thindb.Error` (API/catalog) and `thindb.exec.Error` (query execution):

**API-level (`src/api/api.zig`):**
```
SchemaMismatch, UnsupportedUniqueKeyType, UpsertRequiresUniqueKey,
TableNotFound, TableAlreadyExists, ColumnNotFound,
ColumnAlreadyExists, UnsupportedAlterOp,
FunctionAlreadyExists, FunctionInvalidDefinition,
WalOrphaned, XaBranchTooLarge, XaInvalidXid,
DatabaseInUse, TableBusy, ReservedTableName, RecoveryRequired, DurabilityUncertain, DatabaseClosed,
InvalidDatabaseName, NoDatabaseSelected,
```

`WalOrphaned`: a `wal` file sits inside the table's `segments/` directory. Replay only reads the log beside the manifest, so that file holds acknowledged rows a normal open would silently drop; the table refuses to open until an operator moves the log into place (same schema fingerprint) or aside.

**Execution-level (`src/exec/exec.zig`):**
```
ColumnNotFound, TypeMismatch, PredicateTypeMismatch,
InvalidTemporalLiteral, UnsupportedOperatorForType,
SortNoKeys,
AggregateNoSpecs, AggregateColumnRequired,
AggregateUnsupportedType, AggregateInvalidParam,
ArithmeticOverflow, ValueOutOfRange, SubqueryMultipleRows,
UnsupportedCorrelatedSubquery,
ComputeNoColumns, ComputeNameCollision, ComputeUnsupportedExpr,
ComputeNoSuchOverload, ComputeTooManyArgs,
JoinUnsupportedType, JoinEmptyOnClause, JoinKeyTypeMismatch,
JoinColumnNameCollision,
MemoryBudgetExceeded, QueryCancelled, WindowUnsupported,
RecursiveCteDepthExceeded,
TableFnExecutionMismatch, TableFnInputMismatch, TableFnOutputMismatch,
```

Plus standard Zig errors (`OutOfMemory`, IO errors via `std.Io`, etc.) propagated unchanged.

`ArithmeticOverflow` comes from decimal arithmetic and casts that leave the declared precision, and from `SUM(LARGEINT)` past the i128 range. Integer arithmetic and integer `SUM` up to BIGINT wrap instead of raising it (§3.4).

`ValueOutOfRange` means INSERT or UPDATE wrote a value its column's type can't hold, such as 300 into a TINYINT or 127.5 into a TINYINT after rounding. MySQL's strict mode fails the statement the same way, as does StarRocks' strict INSERT; a CAST gives NULL instead (§3.4).

`InvalidTemporalLiteral` means a DATE or DATETIME was compared with a string constant that doesn't read as a date or datetime (§3.1), as in `d = ''`, `d = 'abc'` or `d < '2026-09-31'`. The statement fails when it is planned, before it returns or changes any rows. The constant comes from the statement itself: written in its text, the value of a constant expression or a scalar subquery, or a user variable it reads. The rule is the same for every comparison form: `=`, `<>`, `<`, `<=>`, BETWEEN, IN, CASE, HAVING, a JOIN's ON and NULLIF. A NULL, a text column, a CAST, and rows an IN subquery returns never raise it. Neither does a bound parameter (MySQL's COM_STMT_EXECUTE, a PostgreSQL Bind) or a value an embedded caller passes in a predicate or an expression: the comparison matches no row, as MySQL's binary protocol returns no rows with a warning. Text equal to one of the statement's bound parameters counts as bound wherever it appears. DML staged in an XA branch replays from its encoded plan, which doesn't record where a value came from, so a bad constant there matches nothing instead of failing XA COMMIT. The MySQL wire reports the error as 1525 (`HY000`) with MySQL's message, `Incorrect DATE value: 'abc'`. The PostgreSQL wire reports it as `22007` with the same message. MySQL raises 1525 for the same constants in the text protocol, with two exceptions that only warn and skip the value: BETWEEN and an IN list of two or more values. thinDB fails those too. MySQL's UPDATE and DELETE raise 1292 (`Incorrect date value`) where thinDB raises 1525.

Scalar functions reject bad arguments with their own errors, which reach a client under their names: `JsonInvalid` (malformed JSON text or JSONB bytes), `JsonNullMemberName` (a NULL key in `JSON_OBJECT` / `JSON_OBJECTAGG`), `IncorrectArgumentsToSleep` (a NULL or negative `SLEEP`), `RegexInvalidPattern`, `RegexInvalidMatchType` (a `match_type` letter outside `c i m n u`), `RegexInvalidReturnOption` (a `REGEXP_INSTR` return option other than 0 or 1) and `RegexIndexOutOfBounds` (a `REGEXP_*` position below 1 or past the end of the subject). MySQL raises the same conditions as errors. The MySQL wire reports `IncorrectArgumentsToSleep` as 1210 (`HY000`), as MySQL does; the others take the unrecognized-error code below.

`SubqueryMultipleRows` means a scalar subquery returned more than one row where one value was needed. A correlated scalar subquery raises it only for an outer row whose correlation key matched several inner rows; a key that matched none reads NULL.

`UnsupportedCorrelatedSubquery` means a subquery reads an enclosing query's columns in a form neither decorrelation strategy carries (§6.7): a FULL JOIN inside the subquery with a side or an ON that reads them, a `SELECT *` the lift can't spell out (a star over a join whose columns share names), an aggregate over only enclosing columns nested in another aggregate or beside an ungrouped outer column, a keyed scalar in a join's extra predicate that finds two aggregates for one outer row (inner text keys `'07'` and `'7'` compared with the number 7; elsewhere such keys group as one), or a DELETE or UPDATE on a target without a primary key whose predicate no keyed path takes or that assigns a correlated value (there's no sound row identity to write the rows it selects by). A subquery's names bind as SQL scopes them: a qualified name by its qualifier, whatever the inner relation is (table, view, CTE or derived table), and an unqualified one to the innermost block that has the column. A subquery correlated by equalities and ranges alone has its inner block materialized once without its correlation terms, which are then applied per outer row as lookup keys; any other is joined with the distinct enclosing values it reads. It is never compiled with an outer-qualified name left in it, since that name would bind to an inner column of the same bare name.

`RecursiveCteDepthExceeded` means a `WITH RECURSIVE` CTE was still adding rows after 1000 iterations of its recursive arms (§6.6). The MySQL wire reports it as 3636 (`HY000`) with MySQL's message, `Recursive query aborted after 1001 iterations. Try increasing @@cte_max_recursion_depth to a larger value.`; the PostgreSQL wire as `54000`.

`TableFnInputMismatch` means a table function's input relation doesn't fit its declared input (§6.2): a column is missing, extra, out of order, nullable where the field isn't, or of a type an INSERT into the declared type refuses, or a value failed its conversion. `TableFnExecutionMismatch` means the call's PARTITION BY contradicts the declared execution mode; `TableFnOutputMismatch` means the callback left its output columns of unequal length.

`ReservedTableName` rejects creating or renaming a table under the `__alter_` or `__ctas_` prefix, which ALTER TABLE's swap and table builds use (§9.2, §8.2).

`InvalidDatabaseName` rejects a database name reserved for the engine's own root directories, or one that isn't a single path component (§4.1), even under IF EXISTS or IF NOT EXISTS, since MySQL checks the name first too. The MySQL wire reports it as 1102 (`42000`, `Incorrect database name`), the PostgreSQL wire as `42602`.

`NoDatabaseSelected` means a statement needed the session's current database and the session has none. A session holds its current database by name (`Session.current_db`, null for none). It loses it by dropping that database, as a MySQL session does, or when another session drops it: each statement treats a name the catalog no longer holds as none. Without a current database, USE, CREATE and DROP DATABASE, SHOW DATABASES, FROM-less SELECTs and fully qualified table references keep working. An unqualified table reference fails with this error, as do statements that act on the current database: functions, views, schemas and XA START. `DATABASE()` and `current_database()` return NULL and PROCESSLIST shows a NULL db. MySQL keeps the dropped name in a session whose database another session dropped; thinDB reports none there too. The MySQL wire reports it as 1046 (`3D000`, `No database selected`), the PostgreSQL wire as `3D000`.

`DatabaseInUse` means another catalog owns the root's OS lock. `TableBusy` rejects an unsafe same-thread upgrade from a live query lease to destructive DDL. `DatabaseClosed` rejects new operations during close. `DurabilityUncertain` means a file replacement succeeded but parent-directory sync failed. The affected table/catalog is fenced at the persistence boundary, before releasing the mutation lock; queued writers recheck that state after acquiring the table lock. `RecoveryRequired` means that publication or an XA persistence/rollback outcome requires restart recovery; operations are rejected until reopening resolves the journal. XA admission rejects records exceeding its 64 MiB serialized recovery limit (`XaBranchTooLarge`) or invalid XIDs (`XaInvalidXid`, at most 1024 bytes).

**Wire codes.** Both wire protocols classify an error by name once (`src/net/error_map.zig`), and each encoder turns the class into its own code, so every send site, whether before a result set or after its column definitions, reports the same failure the same way. On the MySQL wire, only an error of the parser's own set (`sql.ParseError`) is 1064 (`42000`, ER_PARSE_ERROR), so a client never reads an execution failure as a syntax error. The parser's refusals of forms MySQL runs (`SqlFoundRowsUnsupported`, `SqlOnNonEquiUnsupported`, `SqlPrepareExecuteUnsupported` and the like), and the planner's (`UnsupportedQueryShape`, `UnsupportedCorrelatedSubquery`, `WindowUnsupported`, `JoinUnsupportedType`, `ComputeUnsupportedExpr`), are 1235 (`42000`, ER_NOT_SUPPORTED_YET). `MemoryBudgetExceeded` is 3170 (`HY000`, ER_CAPACITY_EXCEEDED), MySQL's error for a statement past a configured memory cap such as `parser_max_mem_size`. `OutOfMemory` is 1037 (`HY001`). `QueryCancelled` is 1317 (`70100`). `ArithmeticOverflow` is 1690 (`22003`), and `ValueOutOfRange` and `NumericOverflow` (an UPDATE value outside its column's integer type) are 1264 (`22003`). `InvalidTemporalLiteral` stays 1525 (`HY000`), which MySQL 8.4 gives a malformed date constant compared with a DATE or DATETIME column (1292 is a truncation warning, not this error). These new classes keep the error's name as the message. An error none of the classes recognizes is 1105 (`HY000`, ER_UNKNOWN_ERROR) under its name. There is no division-by-zero class: `x / 0` and `x DIV 0` are NULL in a SELECT, as in MySQL. The PostgreSQL wire maps the same classes to `42601` (parse), `0A000` (not supported), `53200` (memory), `22023` (SLEEP arguments) and `XX000` (unrecognized). The protocol layer's own 1064s for malformed packets and unknown commands stay as they are.

Errors propagate to callers. Outside the XA commit protocol, an error is not a blanket guarantee that no effect occurred: durable publication can succeed before later cleanup fails. Retrying non-idempotent writes after an I/O error requires inspecting/recovering the state. A statement's own failure is narrower: an INSERT whose source query fails part way, or one of whose rows the table refuses, leaves no rows behind, since its rows are gathered and the table validates the batch whole before taking any of it. The guarantee holds up to 64 MiB of gathered rows; a larger INSERT ... SELECT writes what it gathered and streams the rest, keeping what it wrote if a later batch fails. A CTAS publishes its table only once its query succeeds (§8.2), so a failed one leaves no table. A failed CREATE MATERIALIZED VIEW leaves neither table nor view, and a REFRESH or CREATE OR REPLACE MATERIALIZED VIEW whose query fails leaves the rows and definition that stood. Ordinary SQL BEGIN/COMMIT/ROLLBACK currently maintain protocol session status, not a multi-statement undo transaction. For the same reason the MySQL dialect accepts SAVEPOINT, RELEASE SAVEPOINT, ROLLBACK TO SAVEPOINT, LOCK/UNLOCK TABLES and FLUSH as statements without effect; ANALYZE/OPTIMIZE/CHECK/REPAIR TABLE answer MySQL's status rows without doing work, since there are no optimizer statistics to gather and compaction runs on its own. SQL-level PREPARE/EXECUTE is rejected (`SqlPrepareExecuteUnsupported`); prepared statements go through the binary protocol. MySQL's SELECT hints (STRAIGHT_JOIN, SQL_NO_CACHE, HIGH_PRIORITY, index hints, `/*+ … */`) are ignored, since the plan follows the written order anyway; SQL_CALC_FOUND_ROWS is ignored too, but FOUND_ROWS() is rejected (`SqlFoundRowsUnsupported`) rather than answering a count the session never kept.

---

## 10. Configuration

`Database.open` takes a `Config` struct. No config files in v1.

| Field | Default | Notes |
|---|---|---|
| `path` | (required) | Directory on disk. Created if missing. |
| `cache_size_bytes` | 2 GB | LRU bound for the decoded row group cache. |
| `flush_interval_secs` | 5 | Time-based flush trigger. |
| `min_time_flush_rows` | 1,000 | Guard against tiny flushes. |
| `min_time_flush_bytes` | 1 MB | Guard against tiny flushes. |
| `row_group_size` | 65,536 | Rows per row group in a segment. |
| `max_columns` | 1,024 | Per table. |
| `max_string_bytes` | 64 MB | Per string value. |
| `compaction_threshold_segments` | 8 | Trigger compaction when small segments exceed. |
| `compaction_threshold_secs` | 900 | Or when oldest small segment is older than this. |
| `gc_grace_secs` | 30 | Delay before deleting compacted-away files. |
| `durable_writes` | false | Reserved for v2 — currently does nothing. |

---

## 11. Limits

| Limit | v1 value |
|---|---|
| Max columns per table | 1,024 |
| Max precision for DECIMAL | 38 |
| Max string value | 64 MB |
| Max segments per table | 2^32 |
| Max rows per segment | 2^32 |
| Row group size | 65,536 (configurable per database) |

---

## 12. Alternatives considered (and rejected)

- **Rust or C++ for the engine.** Rejected: user preference. Zig also has stronger SIMD ergonomics than Rust for vectorized kernels and a cleaner C ABI for future bindings.
- **Go for the engine.** Rejected: GC pauses during scans, no first-class SIMD, awkward FFI for future client libraries.
- **Row-oriented storage as a co-equal option.** Rejected: doubles the engine surface for an OLTP workload that isn't the target.
- **Parquet for the segment format.** Rejected: significant external dependency surface and ABI complexity for benefits we don't need (cross-engine interop is a non-goal in v1).
- **Postgres wire protocol for the future server.** Rejected in favor of MySQL wire-compat (v3, task #139). MySQL has more BI-tool / ORM ecosystem in the StarRocks-adjacent space we sit in, and our scalar-function naming already aligns with MySQL via the parity work.
- **In-place column updates / inline tombstones inside segment files.** Rejected: breaks immutability, which is the foundation of lock-free concurrent reads.
- **Strict schema as a perf concern.** Rejected based on review: strict schema is actually faster than dynamic, not slower. No tradeoff.
- **Runtime query optimizer.** Rejected: explicitly out of scope. Query execution order is what the user wrote. Pre-execution rewrites are allowed (constant folding, predicate normalization) but no plan-cost-based reordering.

---

## 13. Build & layout

### 13.1 Repo layout

```
src/
  api/                          public Database, Table, Query builder, Connection
  engine/                       writer thread, memtable, flush, compaction, alter
  exec/                         operators (scan, filter, project, compute, sort, limit,
                                aggregate, joins, nlj, smj, range_sweep, cast, cell_io, skew)
  storage/                      segment reader/writer, manifest, encodings, compression, tombstones
  ir/                           operator-tree IR + serialization (foundation for v2 SQL parser)
  net/                          in-process Connection + TCP transport stubs
  cache/                        LRU row-group cache
  util/                         allocator helpers, small primitives
tests/
  integration/                  end-to-end scenarios
  integration_client/           Connection-mediated query surface
bench/
  main.zig                      entry + dispatch
  join_bench.zig                join algorithm benchmarks
  durability_bench.zig          WAL / sync mode benchmarks
  compact_bench.zig             compaction scenarios
  tcp_bench.zig                 transport overhead vs in-process
  harness.zig                   shared timer + report helpers
build.zig
DESIGN.md
CLAUDE.md
README.md
```

### 13.2 Build & test

```
zig build              # debug build
zig build test         # runs all `test` blocks
zig build -Doptimize=ReleaseFast
zig build bench        # runs benchmarks
```

Target Zig version: 0.16.

---

## 14. Roadmap — v2 and beyond

### Shipped in v1 (originally planned for later)

| Feature | Notes |
|---|---|
| **Joins** | hash / SMJ / NLJ / range_sweep. `.auto` routing via manifest stats + Misra-Gries skew detection that re-routes hash → SMJ in-place when one key dominates the build side. |
| **Range / opaque predicates** | Single inequality `a OP b`, multi-range (BETWEEN), `extra_predicate` post-join filter, opaque callback via NLJ. Skew detection + auto-route on top. |
| **Upserts** | StarRocks-style last-writer-wins on tables with `unique = true`. Insert auto-resolves; `Table.upsert()` is the self-documenting alias. |
| **Crash durability** | WAL with leader-follower group commit (§8.1). `wal_enabled = true` + `sync_mode = .per_flush`. |
| **Implicit type coercion** | DuckDB/StarRocks-style: numeric widening, int → float/double, bool → ints, date → datetime. Exact-match overload selection takes the fast path; coercion is cost-ranked when no exact overload exists. Only when no overload is reachable by widening does an integer argument narrow to a narrower integer parameter, NULL where it doesn't fit, as an explicit `CAST` narrows. StarRocks casts function arguments the same way, so `date_add(d, n + 1)` still resolves although `n + 1` is BIGINT (§3.4), and `LEFT(s, 4294967297)` is NULL. A double or decimal argument to an integer parameter rounds as MySQL reads it (`REPEAT('a', 2.5)` is `aaa`), and is NULL past the parameter's range, as in StarRocks, where MySQL clamps it: `LEFT(s, 1e15)` is NULL. A table function's integer argument binds narrowed to its declared type the same way; a NULL reaching a parameter the function can't take NULL for raises `TableFnInputMismatch`. An `INTERVAL` amount counts whole units: a fractional literal rounds half away from zero (`INTERVAL 1.5 WEEK` is 14 days) and a text amount reads its leading integer, as MySQL does; any other amount takes an integer type. The count is an INT, as in StarRocks: a count past INT's range moves a date to NULL. A string literal meets a function as a string column does first; where only a date or datetime fits, it is read once at plan time, as a CAST to that type reads it, a DATETIME before a DATE (§3.1), and text that doesn't read is NULL. `CAST('…' AS DATE)` and `DATE('…')` read a literal once with their own kernels. A string column converts only by explicit `CAST`, which yields NULL for text that isn't a value of the target type: a date, or a number of the target's kind (an integer type takes only a trimmed integer literal, and BOOLEAN `true`, `false` or an INT, §3.1). An explicit `CAST` of a number into an integer type truncates toward zero and yields NULL outside the target's range, as StarRocks does; a DECIMAL target raises on a value past its precision. INSERT … SELECT parses text into a DATE/DATETIME column and rejects text that isn't a date. Every write (INSERT, UPDATE, ON DUPLICATE KEY UPDATE) converts a value into a DECIMAL column as `CAST` to the column's type does: it rounds half away from zero to the scale, raises past the precision, and rejects text that isn't a number. |
| **Statistical / set-oriented aggregates** | `STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP`, `COUNT_DISTINCT`, `PERCENTILE_CONT`, `GROUP_CONCAT`. |
| **In-process Connection** | `thindb.local(...)` returns a Connection that mediates queries — same surface a future remote-mode Connection will expose. |

### v2 — next major band (user-facing query surface)

The biggest piece is a **compiled query-plan tree** as IR — most of v2 builds on it.

| Feature | Notes |
|---|---|
| Multi-source pipelines / CTEs | Compile builder calls into an explicit plan tree before exec. Foundation for everything else in v2. |
| SQL parser + execution | MySQL/StarRocks dialect; parser emits the same IR as the builder. |
| Database / namespace system | 2-level (catalog.schema.table) per Postgres/Iceberg/BI-tool convention. Each level a directory under the Database root. |
| Temp tables + per-connection sessions | Sessions own a temp-table overlay isolated from other connections. Per-session timezone, isolation knobs later. |
| EXPLAIN plan output | Render the plan tree as text (and later JSON). Cheap once the plan-tree IR exists. |
| Zig UDFs | Trusted in-process scalar and aggregate functions registered on the catalog. Scalar UDFs participate in existing overload/coercion resolution; aggregate UDFs use a generic state-backed path. |
| Window functions | `ROW_NUMBER`, `RANK`, `LAG`/`LEAD`, framed aggregates (`OVER PARTITION BY … ORDER BY … ROWS BETWEEN …`). Likely a new `Window` operator. |
| Column defaults + auto-increment | New column metadata (`default`, `auto_increment`, future: `on_update`). Memtable insert resolves defaults / picks next ID when the row omits the field. |
| `TIMESTAMPTZ` | New type alongside `DATETIME`; existing columns unaffected. |
| Non-Zig client libraries | Each library builds the operator-tree IR locally and sends it over the wire protocol. |
| SIMD optimization pass | Audit hot paths for `@Vector(N, T)` opportunities (cast kernels, filter, aggregate accumulators, join key compare). |

### v3 — later (server / parallelism / extensibility)

| Feature | Notes |
|---|---|
| MySQL wire-protocol compatibility | Listener that speaks the MySQL client/server protocol so any mysql/MariaDB client connects. Replaces the "design our own protocol" path. |
| Table-valued UDFs | TVFs act as pipeline operators; invokable from SQL once the parser ships. |
| Auto-partitioned parallel execution | Split safely-partitionable queries into N parallel sub-queries; partial graph splits where safe. Single-threaded today; revisit after the plan-tree IR. |
| ML/RL-driven query tuning | Learn per-query and per-data-shape settings for execution knobs such as scan tile size, chunk rows, route block rows, group bucket count, bucket granularity, flush thresholds, and scheduler/backlog thresholds. Start with offline benchmark traces and cardinality/runtime stats; later allow safe online exploration with guardrails for memory and tail latency. |
| Partition key on tables | Per-key-value or hash-bucket physical partitioning. Natural parallelism axis for the auto-parallel work above. |
| Schema evolution via in-place changes | Order-key changes, column reorder. v1's copy-and-swap covers most needs. |

### Explicitly deferred — revisit much later

| Feature | Notes |
|---|---|
| External sort / spillable operators | Memory accountant exists; spill-to-disk for Sort and Aggregate when over budget. Today they throw `MemoryBudgetExceeded`. |
| Property-based tests | Random-input invariants (round-trip, join-algorithm equivalence, aggregate split-invariance). |

### Not planned

| Feature | Notes |
|---|---|
| General SQL transactions | Staged XA write commits exist (section 8.2); ordinary SQL transactional reads, rollback, and isolation remain unimplemented. |
| Replication / multi-node | Explicitly out of scope. |
| Cost-based optimizer / statistics-driven plans | The "thin" ethos rejects this. Pre-execution rewrites (constant folding, predicate normalization) are fine; plan-cost reordering is not. |
| Implicit string ↔ number coercion in arithmetic and function arguments | Footgun-prone (MySQL behavior); explicit `CAST` / `to_int` / `to_string` instead (Postgres/DuckDB/StarRocks consensus). Comparisons are the exception: text meets a number by value (§3.1). |

---

## 15. Client/server (v2 trajectory)

Going forward, **all user queries flow through a `Connection`**. Existing `Database` / `Table` / `Query` types remain — they are the *server's* internals (and what tests use directly). The user-facing API is:

```zig
var conn = try thindb.local(allocator, io, data_dir, .{});  // in-process
// or, future:
// var conn = try thindb.connect(io, "tcp://host:5432");    // remote
defer conn.close();

var q = conn.scan("orders").limit(10);   // builds operator IR
defer q.deinit();
while (try q.next()) |batch| { ... }
```

The Connection abstracts a **transport**:

- **In-process** (today): client and server in the same address space. The client encodes operator IR into bytes; the server-side dispatcher decodes and runs against the in-process `Database`. Exercises the wire path for tests with no socket overhead. (Walking skeleton currently passes `Batch` values directly across the boundary; batch wire-encoding lands with the TCP transport.)
- **TCP** (later): same `Connection` API, bytes flow over a socket.

Accepted MySQL sockets enable `TCP_NODELAY` on supported POSIX platforms to avoid
holding a short result tail behind a delayed ACK. Socket-option helpers remain
best effort. On Windows, Zig 0.16 exposes AFD handles without a public socket-option
setter, so these helpers currently leave the OS defaults in place.

### 15.1 Operator IR

A single binary tree describes a query: tagged tree, each operator carries its upstream encoded immediately after the operator's payload. Format defined in `src/ir/ir.zig`. Versioned header (`tDBQ` magic + `u16` version) so future tag additions are forward-compatible.

Walking-skeleton scope today: `Scan(table_name)` and `Limit(n)`. Roadmap:
- `Where(predicate)` / `Filter` — alias for `where` at the canonical name
- `Select(columns)` — whitelist projection
- `Exclude(columns)` — drop columns; downstream cannot reference them
- `OrderBy(specs)`, `GroupBy(keys, aggs)`
- `Pipe(fn)` — compose a sub-pipeline (`fn(ClientQuery) → ClientQuery`)
- (post-server) `PipeUdf(name)` — invoke a server-registered UDF; see §17

### 15.2 User-defined functions

Current scope: embedded applications register trusted in-process Zig UDFs on the catalog through `Database.registerScalarUdf` / `Database.registerAggregateUdf`. UDF definitions are process-local configuration, not persisted catalog objects. Scalar UDFs receive vectorized `ColumnView` inputs and append into a `ColumnStore`; aggregate UDFs declare a state size/alignment plus `init`, `update_one`, optional batch/combine hooks, `finalize`, and optional `destroy`. Bad UDF code is trusted native code and can crash the process.

SQL references registered scalar UDFs as ordinary calls (`SELECT my_fn(col) ...`). Creating a scalar UDF fails with `FunctionAlreadyExists` under a name `scalar_fn.isReservedScalarUdfName` reserves: any name the builtin resolver answers to (`scalar_fn.nameResolvable`), itself or as an alias the parser rewrites (`ucase`), which covers the functions syntax lowers to, such as `date_add_hours` for `+ INTERVAL n HOUR`; or any name starting `__`, which the engine's internal functions use. A UDF under such a name would otherwise take over those calls. Only creating a function checks this (`Catalog.registerScalarUdf`), not `UdfRegistry`: udf.zig is embedded in the `LANGUAGE zig` SDK without the builtin table, and should UDFs ever persist, one loaded back must load even when a later builtin took its name. Registered aggregate names are recognized during parse/analyze and run through a generic state-backed aggregate operator; built-in aggregates keep the specialized hash/radix/streaming paths. UDAFs currently support regular grouped and global aggregation; table, window, SQL-defined, dynamic-library, WASM, Python, and JS UDFs remain deferred.

Endgame: clients in many languages (Rust, Zig, C, JS, TS, Python, Go) author UDFs and register them with the server. The server holds a UDF registry; queries reference UDFs by name via SQL or `.pipeUdf("name")`. Two runtime tiers behind a common adapter interface:

| Tier | Runtime | Languages | Speed | Sandbox |
|---|---|---|---|---|
| **Native** | `dlopen` + C ABI | C, Zig, Rust, Go (`-buildmode=c-shared`), ... | Full native | None — trusted operator only |
| **WASM** | wasmtime sandbox | C, Zig, Rust, AssemblyScript, others compiling to WASM | ~10–30% slower than native | Yes — multi-tenant safe |
| **Scripting** (eventual) | QuickJS / MicroPython | JS, TS (transpiled), Python | 30–100× slower than native | Yes (engine-provided) |

The wire-level UDF contract is a single C header (Arrow-style flat `Batch` struct + `OutputBuilder` accessors). Each supported language ships an idiomatic helper crate that wraps the raw struct. Embedded `.pipe(&op)` was considered as a stepping stone and dropped — the multi-language registry is the canonical UDF path; embedded users hit the same surface via the in-process Connection transport.

---

## 16. References

- StarRocks columnar storage and compaction model (background influence — not used as a code source)
- DuckDB decimal & overflow semantics (modeled after for arithmetic rules)
- Apache Arrow column block layout (informed encoding choices)
- LSM-tree compaction tiering (informed the tiered-compaction strategy)
