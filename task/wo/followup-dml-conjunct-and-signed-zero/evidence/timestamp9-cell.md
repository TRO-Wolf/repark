# TIMESTAMP(9) cell — the created table's format version and Iceberg type, both doors

Measurement only (Part 1 of the 2026-11-08 extension). Model `claude-haiku-5-5`. Each statement ran on a fresh table.
Every insert wrote one row, `id = 1`, with the literal `TIMESTAMP '2026-11-08 12:34:56.123456789'`, and the table was
read back in `ORDER BY id`. The session zone is `America/New_York` on the Spark leg and the facade legs. The
native door was not given a zone. The format version and the `ts` Iceberg type come from the newest metadata JSON
after the insert. The native door has no session zone set, so its readbacks are wall-clock values. Raw cells:
`raw/timestamp9-*.jsonl`.

## Result table

| Door | Statement (`ts` column) | Create outcome | format-version | `ts` Iceberg type | Insert | Readback: value, Arrow type |
|---|---|---|---|---|---|---|
| RePark facade (`spark.sql`), no v3 flag | `TIMESTAMP(9)` | ANSWERED | 2 | `timestamptz` | ANSWERED | 17:34:56.123456 UTC (= 12:34:56 New York), `timestamp[us, tz=UTC]` |
| RePark facade, no v3 flag | `TIMESTAMP(6)` (control) | ANSWERED | 2 | `timestamptz` | ANSWERED | 17:34:56.123456 UTC, `timestamp[us, tz=UTC]` |
| RePark facade, no v3 flag | `timestamp_ns` (control) | REFUSED: `DataInvalid`, "Invalid schema for v2: Invalid type for ts: timestamp_ns is not supported until v3" | (none) | (none) | NOT-RUN | (none) |
| RePark facade, v3 flag on, `TBLPROPERTIES ('format-version'='3')` | `TIMESTAMP(9)` | ANSWERED | 3 | `timestamptz` | ANSWERED | 17:34:56.123456 UTC, `timestamp[us, tz=UTC]` |
| RePark facade, v3 flag on | `TIMESTAMP(6)` (control) | ANSWERED | 3 | `timestamptz` | ANSWERED | 17:34:56.123456 UTC, `timestamp[us, tz=UTC]` |
| RePark facade, v3 flag on | `timestamp_ns` (control) | ANSWERED | 3 | `timestamp_ns` | ANSWERED | 12:34:56.123456000 wall clock (raw 1794141296123456000), `timestamp[ns]` |
| RePark native (`repark.sql()`), no flag | `TIMESTAMP(9)` | ANSWERED | 3 | `timestamp_ns` | ANSWERED | 12:34:56.123456000 wall clock, `timestamp[ns]` |
| RePark native, no flag | `TIMESTAMP(6)` (control) | ANSWERED | 2 | `timestamp` | ANSWERED | 12:34:56.123456 wall clock, `timestamp[us]` |
| RePark native, no flag | `timestamp_ns` (control) | ANSWERED | 3 | `timestamp_ns` | ANSWERED | 12:34:56.123456000 wall clock, `timestamp[ns]` |
| RePark native, flag on (extra control leg) | the three statements above | identical to the native no-flag rows | 3 / 2 / 3 | `timestamp_ns` / `timestamp` / `timestamp_ns` | ANSWERED | identical |
| Spark 4.1.2 (`spark.sql`, Iceberg 1.11.0 runtime, Hadoop catalog) | `TIMESTAMP(9)` | REFUSED: `ParseException [UNSUPPORTED_DATATYPE] Unsupported data type "TIMESTAMP(9)"`, SQLSTATE 0A000 | (none) | (none) | NOT-RUN | (none) |
| Spark 4.1.2 | `TIMESTAMP(6)` (control) | REFUSED: same class, `"TIMESTAMP(6)"` | (none) | (none) | NOT-RUN | (none) |
| Spark 4.1.2 | `timestamp_ns` (control) | REFUSED: same class, `"TIMESTAMP_NS"` | (none) | (none) | NOT-RUN | (none) |
| Spark 4.1.2 | the three v3 variants (`TBLPROPERTIES ('format-version'='3')`) | REFUSED at the column type, before the property is read | (none) | (none) | NOT-RUN | (none) |

The Spark refusals match the brief's expectation of parse errors for `TIMESTAMP(9)` and `timestamp_ns`. Spark also
refuses `TIMESTAMP(6)`, so the `TIMESTAMP(p)` spelling is not Spark syntax at any precision. The v3 variants on Spark
were not measured with a supported type, so Spark's v3 behaviour for a `TIMESTAMP` column is not in this table.

## What the table shows

1. **The facade accepts `TIMESTAMP(9)` and stores it as microseconds.** The column is a v2 `timestamptz` and the
   readback has six digits. No refusal is raised for the nanosecond precision.
2. **The nanosecond digits do not survive any door.** The literal was truncated before the write on every door that
   accepted it: the readbacks end in `.123456` (microseconds) or `.123456000` (nanoseconds).
3. **`TIMESTAMP(6)` means different Iceberg types on the two RePark doors.** The facade gives `timestamptz`, which
   reads as 12:34:56 New York. The native door gives `timestamp` (no zone), which reads as 12:34:56 wall clock. The
   readbacks differ by exactly 5 hours (18000 seconds), which is the session offset on 2026-11-08 (EST, UTC-5).
4. **The native door creates a v3 `timestamp_ns` table with no format-version clause and no flag.** `TIMESTAMP(9)` and
   `timestamp_ns` both do this. The flag changes nothing on the native door (the extra control leg is identical to the
   no-flag leg). The facade refuses `timestamp_ns` on v2 and creates it on v3 only with the flag. The brief expected
   the native `timestamp_ns` create to refuse on v2; it did not.
5. **Spark 4.1.2 refuses all three spellings at the column type.** None of points 1 to 4 has a Spark counterpart in
   this table.

## Source pointers (read-only, the branch head)

- `crates/repark-sql/src/create_table.rs`, `iceberg_create_format_version`: the flag is consulted only when a
  `format_version` clause is present. A native create with no clause returns v2 from this function.
- `crates/repark-sql/src/create_table.rs`, `iceberg_v3_named_primitive`: matches `timestamp_ns` and `timestamptz_ns` by
  the spelled type name only. `TIMESTAMP(9)` is not matched here, yet the native door still produced a v3 `timestamp_ns`
  table for it. The source path that makes this happen was not located, so the mechanism is not traced.

## Not measured

- The Spark plain `TIMESTAMP` type (the Spark-side reference for the facade's `timestamptz` mapping). The comparison in
  point 3 rests on the Spark type semantics, not on a Spark run in this table.
- A nanosecond value that is not a SQL literal (for example a cast from a string or an `ns` column written by a
  different route). Point 2 covers the literal only.
- The native v3 controls with a `format-version` clause (the brief limits v3 to the facade).

Up: [map.md](map.md).
