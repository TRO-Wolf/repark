# Unit ledger — ICE-TSNS-SQL-1 · `timestamp_ns` / `timestamptz_ns` on the SQL door

## Round 1 (2026-09-17) — run 21

**Date:** 2026-09-17 · **Branch:** `feat/ice-tsns-sql-1` · **Base:** `origin/main` `126b8285` ·
**Model:** claude-opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `ICE-TSNS-SQL-1` (rating row V3-06, and the `timestamp_ns` half of V3-04).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Ruling Q-21c-6 (orchestrator, binds this unit), quoted:**

> On the SQL door, with a format-v3 Iceberg target:
> 1. `CAST(<string> AS timestamp_ns)` parses up to nine fractional digits exactly into Arrow
>    `Timestamp(ns)`; `CAST(<string> AS timestamptz_ns)` into `Timestamp(ns, "UTC")`, honouring an
>    explicit offset and otherwise the session time zone exactly as RePark's existing `timestamp`
>    (µs, tz) cast does. The type names are case-insensitive. Invalid strings fail the way the
>    existing `timestamp` cast fails under the same ANSI mode.
> 2. A value of a microsecond timestamp type (`TIMESTAMP`, `TIMESTAMP_NTZ`) or a string, written
>    into an Iceberg `timestamp_ns` / `timestamptz_ns` column by `INSERT … VALUES`,
>    `INSERT … SELECT`, `INSERT OVERWRITE`, MERGE insert/update and CTAS, is WIDENED losslessly to
>    nanoseconds by the write path's conform step — never a raw Arrow error. (µs → ns is exact;
>    there is no narrowing in this direction.)
> 3. `days(ts)` / `hours(ts)` on ns columns written through the SQL door partition at the true
>    boundary, and the partition values PyIceberg reads back equal RePark's `.partitions` answer.
> 4. `CAST(<ns value> AS STRING)` is lossless: the fraction keeps up to nine digits with trailing
>    zeros trimmed, following the same trimming rule RePark already uses for microsecond
>    timestamps (`…00:00:00.000000001`, `…05.123456789`, `…05.5`, and no fraction at all when it
>    is zero).
> 5. Predicates compare at nanosecond precision: `WHERE ts = CAST('…05.123456789' AS timestamp_ns)`
>    matches that row and not a neighbour one nanosecond away.
> 6. Out of scope tonight, recorded in the ledger and the registry: `DESCRIBE` type names for ns
>    columns; the write-direction oracle (another engine writes v3 ns, RePark reads) — blocked on
>    PyIceberg #1551, future oracle the Iceberg Java API; format v2 tables keep refusing ns types
>    at CREATE as they do today.

**Why the oracle is not Spark.** Spark 4.1.2 cannot read or write `timestamp_ns` /
`timestamptz_ns` (`UnsupportedOperationException: Cannot convert unsupported type to Spark:
timestamp_ns`, measured by the 2026-09-16 rating). The SQL door therefore answers the Iceberg v3
spec (`timestamp_ns` / `timestamptz_ns` are int64 nanoseconds since the epoch, the latter
UTC-adjusted), and a second engine — PyIceberg 0.12.0, read side — must read back exactly what
RePark wrote. PyIceberg 0.12.0 refuses to write format v3 (`NotImplementedError: Writing V3 is
not yet supported`, apache/iceberg-python#1551), so the write-direction oracle is residue R-002.

**Oracle harness.** `python/repark/tests/_record_ice_tsns_sql_1_oracle.py` (two processes: RePark
`write` under the repo venv, PyIceberg `record` / `check` under a caller-supplied interpreter with
`pyiceberg==0.12.0` + `pyarrow` — not a repository dependency), fixture
`python/repark/tests/ice_tsns_sql_1_oracle.json`, pins `python/repark/tests/test_ice_tsns_sql_1.py`.

## PROPOSITION LEDGER — ICE-TSNS-SQL-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Ruling clause 1: string casts to `timestamp_ns` / `timestamptz_ns` keep nine digits, honour an offset or the session zone, are case-insensitive, and fail / NULL like the `TIMESTAMP` cast under the same ANSI mode. | `test_cast_string_as_*`, `test_cast_timestamptz_ns_reads_a_zoneless_string_in_the_session_zone`, `test_cast_type_names_are_case_insensitive`, `test_malformed_string_*`; mutation proof. | **OPEN** | Red on `126b8285` (§2). |
| C-002 | Ruling clause 2: INSERT VALUES / SELECT, INSERT OVERWRITE, MERGE insert/update and CTAS widen microsecond `TIMESTAMP` values and strings into ns columns exactly — never a raw Arrow error. | `test_insert_*`, `test_insert_overwrite_widens_and_keeps_ns`, `test_merge_insert_and_update_keep_ns`, `test_ctas_carries_ns_casts`; fixture `check` leg; mutation proof. | **OPEN** | Red on `126b8285` (§2). |
| C-003 | Ruling clause 3, `days(ts)`: SQL-door writes partition at the true day boundary and `.partitions` equals the PyIceberg read-back. | `test_days_partitions_equal_the_pyiceberg_read_back`, `test_insert_select_ns_cast_into_a_days_table`; `check` leg on `sql_days`. | **OPEN** | Red on `126b8285` (§2). |
| C-004 | Ruling clause 4: `CAST(<ns> AS STRING)` keeps up to nine digits with the microsecond trimming rule. | `test_cast_ns_column_as_string_is_lossless`, `test_cast_ns_cast_as_string_round_trips`; mutation proof. | **OPEN** | Red on `126b8285`: `…05.123456789` renders `…05.123456` (§2). |
| C-005 | Ruling clause 5: predicates compare at nanosecond precision. | `test_predicate_compares_at_nanosecond_precision`. | **OPEN** | Red on `126b8285` (§2). |
| C-006 | Ruling clause 6: the out-of-scope items are recorded as dated residues here and in the registry; format v2 keeps refusing ns types at CREATE. | `test_format_v2_keeps_refusing_ns_at_create` (fence, green before and after); residues R-001, R-002 below; registry row. | **OPEN** | Fence green on `126b8285`. |
| C-007 | Ruling clause 3, `hours(ts)`: SQL-door writes into `hours(tz)` on ns partition at the true hour boundary and the partitions equal the spec-derived values. | `test_hours_partitions_equal_the_spec`; `check` leg on `sql_hours`. | **OPEN** | Blocked on the fork (§1): its `hour` array transform has no `Timestamp(ns)` arm. |
| C-008 | Registry row, maps, gates and the fixture `check` leg are green on the release native. | §5 counts. | **OPEN** | — |

## 1. Measured before any product change (release native, `126b8285`)

- Every red row of the brief's table reproduces (`Unsupported SQL type timestamp_ns` /
  `timestamptz_ns`; the raw `Arrow error: … expected Timestamp(ns) but found Timestamp(µs, "UTC")`
  for a `TIMESTAMP` literal or a string in `INSERT … VALUES`; `CAST(ts AS STRING)` truncating to
  six digits).
- One more red shape: `INSERT … SELECT` from `TIMESTAMP` columns into ns columns fails in the
  parquet writer (`Incompatible type. Field 'ts' has type Timestamp(ns), array has type
  Timestamp(µs, "UTC")`).
- Root causes. (a) DataFusion's planner types `TIMESTAMP` as `Timestamp(ns, None)`, and RePark's
  `spark_ltz_timestamp_cast` analyzer rule rewrites every cast to a nanosecond timestamp into the
  Spark LTZ microsecond type — so any ns cast the INSERT planner adds is undone. (b) A `VALUES`
  node keeps the schema the planner typed (`Timestamp(ns)`) after the analyzer retypes its
  expressions to microseconds, so `SELECT * FROM (VALUES (1, TIMESTAMP '…'))` fails with the raw
  Arrow error even without Iceberg. (c) `__repark_timestamp_to_string__` floors every tick to
  microseconds before rendering.
- `hours()` on ns: the DataFrame-door control `hours(tz)` write is refused by the pinned fork
  (`4151b488`): `FeatureUnsupported => Unsupported data type for hour transform:
  Timestamp(Nanosecond, Some("UTC"))`. `crates/iceberg/src/transform/temporal.rs` `Hour::transform`
  handles only `Timestamp(Microsecond, _)` (its `transform_literal` already handles
  `TimestampNs`); `Day` / `Month` / `Year` handle ns. The fixture's `sql_hours` expectation is
  therefore derived from the spec (`hour = floor(tz_ns / 3_600_000_000_000)`), and C-007 is
  BLOCKED-ON-FORK F-TSNS-HOUR-1.
- PyIceberg read-back of the DataFrame-door control (fixture `control.ctl_days`): schema
  `ts: timestamp_ns`, `tz: timestamptz_ns`, spec `ts_day: day(ts)`, values
  `1767323045123456789`, `1767398399999999999`, `1767398400000000001`, … and partitions
  `2026-01-02` (5), `2026-01-03` (2), `2026-01-04` (1) — equal to RePark's `.partitions` answer.

## 2. Red run (step 1, `126b8285` + the pins, release native)

`python -m pytest python/repark/tests/test_ice_tsns_sql_1.py` — 37 failed, 2 passed. The two
passes are the fixture self-check and the clause-6 v2 fence, green by design. The two
`Regex pattern did not match` rows are the malformed-string ANSI pins: the `TIMESTAMP` cast raises
`CAST_INVALID_INPUT` and the ns cast raises `Unsupported SQL type` instead.

```
PASSED test_fixture_control_agrees_with_the_literals
FAILED test_cast_string_as_timestamp_ns_keeps_nine_digits[1] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_string_as_timestamp_ns_keeps_nine_digits[2] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_string_as_timestamp_ns_keeps_nine_digits[3] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_string_as_timestamp_ns_keeps_nine_digits[6] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_string_as_timestamp_ns_keeps_nine_digits[7] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_string_as_timestamp_ns_keeps_nine_digits[8] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_string_as_timestamptz_ns_honours_the_offset[1] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamptz_ns
FAILED test_cast_string_as_timestamptz_ns_honours_the_offset[2] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamptz_ns
FAILED test_cast_string_as_timestamptz_ns_honours_the_offset[3] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamptz_ns
FAILED test_cast_string_as_timestamptz_ns_honours_the_offset[6] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamptz_ns
FAILED test_cast_string_as_timestamptz_ns_honours_the_offset[7] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamptz_ns
FAILED test_cast_string_as_timestamptz_ns_honours_the_offset[8] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamptz_ns
FAILED test_cast_timestamptz_ns_reads_a_zoneless_string_in_the_session_zone - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamptz_ns
FAILED test_cast_type_names_are_case_insensitive - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type TIMESTAMP_NS
FAILED test_malformed_string_fails_like_the_timestamp_cast_under_ansi[timestamp_ns] - AssertionError: Regex pattern did not match.
FAILED test_malformed_string_fails_like_the_timestamp_cast_under_ansi[timestamptz_ns] - AssertionError: Regex pattern did not match.
FAILED test_malformed_string_is_null_like_the_timestamp_cast_without_ansi[timestamp_ns] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_malformed_string_is_null_like_the_timestamp_cast_without_ansi[timestamptz_ns] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamptz_ns
FAILED test_insert_values_writes_every_shape_exactly - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_insert_values_widens_a_timestamp_literal - PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Timestamp(ns) but found Timestamp(µs, "UTC"
FAILED test_insert_values_widens_a_string - PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Timestamp(ns) but found Timestamp(µs, "UTC"
FAILED test_insert_select_ns_cast_into_a_days_table - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_insert_select_widens_a_microsecond_column - PySparkException: Unexpected => Failed to write using parquet writer., source: Arrow: Incompatible type. Field 'ts' has type Timestamp(ns), array has type Timestamp(µs, "
FAILED test_insert_overwrite_widens_and_keeps_ns - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_merge_insert_and_update_keep_ns - PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Timestamp(ns) but found Timestamp(µs, "UTC"
FAILED test_ctas_carries_ns_casts - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_days_partitions_equal_the_pyiceberg_read_back - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_hours_partitions_equal_the_spec - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamptz_ns
FAILED test_cast_ns_column_as_string_is_lossless - AssertionError: {'id': 1, 's': '2026-01-02 03:04:05.123456', 'z': '2026-01-02 03:04:05.123456'}
FAILED test_cast_ns_cast_as_string_round_trips[1] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_ns_cast_as_string_round_trips[2] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_ns_cast_as_string_round_trips[3] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_ns_cast_as_string_round_trips[6] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_ns_cast_as_string_round_trips[7] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_ns_cast_as_string_round_trips[8] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_predicate_compares_at_nanosecond_precision[ts] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_predicate_compares_at_nanosecond_precision[tz] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamptz_ns
PASSED test_format_v2_keeps_refusing_ns_at_create
37 failed, 2 passed
```

Fixture `check` leg on the same tree (PyIceberg 0.12.0 over the step-1 warehouse):

```
FAIL sql_ctas: unreadable (FileNotFoundError: no metadata for sql_ctas under the warehouse)
FAIL sql_days: unreadable (ValueError: Cannot get a snapshot as the table does not have any.)
FAIL sql_hours: unreadable (ValueError: Cannot get a snapshot as the table does not have any.)
FAIL sql_merge: unreadable (ValueError: Cannot get a snapshot as the table does not have any.)
FAIL sql_overwrite: unreadable (ValueError: Cannot get a snapshot as the table does not have any.)
5 failure(s)
```
