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
| C-001 | Ruling clause 1: string casts to `timestamp_ns` / `timestamptz_ns` keep nine digits, honour an offset or the session zone, are case-insensitive, and fail / NULL like the `TIMESTAMP` cast under the same ANSI mode. | `test_cast_string_as_*`, `test_cast_timestamptz_ns_reads_a_zoneless_string_in_the_session_zone`, `test_cast_type_names_are_case_insensitive`, `test_malformed_string_*`; mutation proof. | **PROVEN** | Pins green on the step-2 native (18 cells: nine digits, offset, session zone, case, ANSI on/off); `keyword_lower` unit tests; Rust door `string_casts_keep_nine_digits_and_the_types`; mutation §4 turns all of them red. |
| C-002 | Ruling clause 2: INSERT VALUES / SELECT, INSERT OVERWRITE, MERGE insert/update and CTAS widen microsecond `TIMESTAMP` values and strings into ns columns exactly — never a raw Arrow error. | `test_insert_*`, `test_insert_overwrite_widens_and_keeps_ns`, `test_merge_insert_and_update_keep_ns`, `test_ctas_carries_ns_casts`; fixture `check` leg; mutation proof. | **PROVEN** | `test_insert_values_*`, `test_insert_select_*`, `test_insert_overwrite_widens_and_keeps_ns`, `test_merge_insert_and_update_keep_ns`, `test_ctas_carries_ns_casts` green; `check` leg `ok` on `sql_days`, `sql_overwrite`, `sql_merge`, `sql_ctas` (§3); Rust door INSERT tests; mutation §4 turns the seven write pins red with the original Arrow / parquet errors. |
| C-003 | Ruling clause 3, `days(ts)`: SQL-door writes partition at the true day boundary and `.partitions` equals the PyIceberg read-back. | `test_days_partitions_equal_the_pyiceberg_read_back`, `test_insert_select_ns_cast_into_a_days_table`; `check` leg on `sql_days`. | **PROVEN** | `test_days_partitions_equal_the_pyiceberg_read_back` and `test_insert_select_ns_cast_into_a_days_table` green; `check` leg `ok sql_days.partitions` (PyIceberg reads `2026-01-02` 5, `2026-01-03` 2, `2026-01-04` 1 from the SQL-door table). |
| C-004 | Ruling clause 4: `CAST(<ns> AS STRING)` keeps up to nine digits with the microsecond trimming rule. | `test_cast_ns_column_as_string_is_lossless`, `test_cast_ns_cast_as_string_round_trips`; mutation proof. | **PROVEN** | `test_cast_ns_column_as_string_is_lossless` and `test_cast_ns_cast_as_string_round_trips` green; unit `nanosecond_ticks_render_every_digit`; mutation §4 turns the five sub-microsecond cells red. |
| C-005 | Ruling clause 5: predicates compare at nanosecond precision. | `test_predicate_compares_at_nanosecond_precision`. | **PROVEN** | `test_predicate_compares_at_nanosecond_precision[ts|tz]` green (`…789` matches id 1 only, `…788` id 8 only, the open interval between them holds id 1 only); Rust `ns_string_rendering_and_predicates_keep_nanoseconds`. |
| C-006 | Ruling clause 6: the out-of-scope items are recorded as dated residues here and in the registry; format v2 keeps refusing ns types at CREATE. | `test_format_v2_keeps_refusing_ns_at_create` (fence, green before and after); residues R-001, R-002 below; registry row. | **PROVEN** | Fence `test_format_v2_keeps_refusing_ns_at_create` green before and after; residues R-001 / R-002 dated in §6 and as registry rows `ICE-TSNS-SQL-1-R-001` / `-R-002` in `docs/spark-sql-iceberg-parity.md`. |
| C-007 | Ruling clause 3, `hours(ts)`: SQL-door writes into `hours(tz)` on ns partition at the true hour boundary and the partitions equal the spec-derived values. | `test_hours_partitions_equal_the_spec`; `check` leg on `sql_hours`. | **OPEN** | BLOCKED-ON-FORK F-TSNS-HOUR-1 (registry `ICE-TSNS-SQL-1-R-003`): the fork's `Hour::transform` refuses `Timestamp(ns)`. `test_hours_partitions_equal_the_spec` xfails on exactly that message; `check` prints `BLOCKED sql_hours`. Closes at the fork repin: the pin then runs fully against the spec-derived fixture. |
| C-008 | Registry row, maps, gates and the fixture `check` leg are green on the release native. | §7 counts. | **PROVEN** | §7: every gate run with counts; the three facade-suite failures and the parity-suite flake are shown independent of this change (clock window, load timing, shared-path race). |
| C-009 | Ruling Q-21c-8 (L-01): `CAST(<ns column> AS TIMESTAMP)` is Spark's µs LTZ type — floored to microseconds, a `timestamp_ns` wall read in the session zone, a `timestamptz_ns` instant kept — and equals the DataFrame spelling `.cast("timestamp")`; `CAST(CAST(ns AS TIMESTAMP) AS STRING)` renders six digits. | `test_cast_ns_column_as_timestamp_floors_like_the_dataframe_spelling` (UTC and America/New_York); Rust door `cast_ns_columns_as_timestamp_floor_to_microsecond_instants`; unit `narrowing_*`; mutation proof. | **PROVEN** | Round 2 §R2.2 / §R2.4: both spellings answer `timestamp[us, tz=UTC]` with the floored ticks (`-1 ns` → `-1 µs`) in UTC and New York; the mutation turns the SQL cells red. |
| C-010 | Ruling Q-21c-8 (L-02): a `TIMESTAMP`-typed `VALUES` cell (`TIMESTAMP '…'`, `CAST(… AS TIMESTAMP)`) written into an ns column stores its µs value × 1000, exactly as `INSERT … SELECT` stores it; a bare string keeps nine digits. | `test_timestamp_typed_values_floor_like_insert_select` (a nine-digit literal); Rust door `timestamp_typed_values_floor_to_microseconds_and_strings_keep_nine_digits`; mutation proof. | **PROVEN** | Round 2 §R2.2 / §R2.4: VALUES and SELECT both store `1767323045123456000`; the string row keeps `…789`; the mutation re-parses the typed cells at nine digits and turns the pin red. |
| C-011 | L-03: `EXPLAIN` lowers `timestamp_ns` / `timestamptz_ns` casts in the statement it explains. | `test_explain_lowers_ns_casts`; Rust door `explain_lowers_ns_casts`. | **PROVEN** | Round 2 §R2.2: `EXPLAIN … CAST(… AS timestamp_ns)` plans `__repark_cast_timestamp_ns__(…)` where it refused `Unsupported SQL type` before. |

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

## 3. Implementation (step 2)

- `crates/repark-functions/src/timestamp_ns_cast.rs` — the embedded casts
  `__repark_cast_timestamp_ns__` / `__repark_cast_timestamptz_ns__` (registered through
  `instant_ts::functions()`): strings through Arrow's `string_to_datetime` to nine digits; a
  zone-carrying string is an instant, a zoneless one a wall (localized in the session zone for
  the zoned target); an instant into the naive target is its session-zone wall; `TIMESTAMP` of
  any unit and `DATE` widen exactly; ANSI-mode malformed / overflow errors, NULL without ANSI.
  `conform_values_timestamp_columns`, called by `spark_ltz_timestamp_cast`, keeps a `VALUES`
  node's schema in step with its retyped rows (root cause (b)).
- `crates/repark-spark/src/keyword_lower.rs` — `CAST(x AS timestamp_ns | timestamptz_ns)` (any
  case, `::` too) lowers to the embedded calls; `router.rs` applies the same lowering to MERGE's
  pieces, which plan outside the passthrough.
- `crates/repark-spark/src/insert_timestamp_ns.rs` — the INSERT conform, called by `spark_ast`
  before analysis (the planner's ns casts over literals and `VALUES` rows become the embedded
  cast, so root cause (a) cannot narrow them) and after it (a temporal source whose analyzed type
  differs from the ns target is wrapped in the same cast).
- `crates/repark-functions/src/timestamp_cast.rs` — `wall_clock_from_ticks` renders a
  `Timestamp(ns)` tick from its own nanoseconds (root cause (c)).
- Python: no product change; the facade forwards the SQL.

After step 2 the pins run 38 passed, 1 xfailed (`test_hours_partitions_equal_the_spec`,
`BLOCKED-ON-FORK F-TSNS-HOUR-1`). One pin changed after the red commit, and only its arithmetic:
`test_cast_timestamptz_ns_reads_a_zoneless_string_in_the_session_zone` compared the ns value
floored to µs with the microsecond cast's tick count divided by 1000 again; it now compares
with the tick count itself. Its expected value (the fixture's `foreign_zone_instant_ns`) is
unchanged.

The recorder's `check` leg over a warehouse the step-2 tree wrote (PyIceberg 0.12.0):

```
ok   sql_ctas.rows
ok   sql_ctas.schema
ok   sql_ctas.spec
ok   sql_days.partitions
ok   sql_days.rows
ok   sql_days.schema
ok   sql_days.spec
BLOCKED sql_hours: F-TSNS-HOUR-1: FeatureUnsupported => Unsupported data type for hour transform: Timestamp(Nanosecond, Some("UTC")) (Cannot get a snapshot as the table does not have any.)
ok   sql_merge.rows
ok   sql_merge.schema
ok   sql_merge.spec
ok   sql_overwrite.rows
ok   sql_overwrite.schema
ok   sql_overwrite.spec
0 failure(s)
```

The recorder gained one fixture key after the red commit, `blocked_on_fork` (derived from the
recorded `hours_control` answer), so `check` reports `sql_hours` as BLOCKED instead of an
unreadable table while the fork refuses it; a re-record from the step-2 warehouse reproduces every
other fixture value byte for byte. It also stopped reading `partition` from PyIceberg's
`inspect.partitions()` on unpartitioned tables, where the column does not exist.

## 4. Mutation proofs (release native, one revert at a time, then restored)

**Clause 1 (C-001)** — `lower_timestamp_ns_cast` made to return `false` first (no lowering):

```
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
FAILED test_insert_select_ns_cast_into_a_days_table - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_insert_overwrite_widens_and_keeps_ns - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_merge_insert_and_update_keep_ns - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_ctas_carries_ns_casts - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_days_partitions_equal_the_pyiceberg_read_back - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_hours_partitions_equal_the_spec - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamptz_ns
FAILED test_cast_ns_cast_as_string_round_trips[1] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_ns_cast_as_string_round_trips[2] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_ns_cast_as_string_round_trips[3] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_ns_cast_as_string_round_trips[6] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_ns_cast_as_string_round_trips[7] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_cast_ns_cast_as_string_round_trips[8] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_predicate_compares_at_nanosecond_precision[ts] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamp_ns
FAILED test_predicate_compares_at_nanosecond_precision[tz] - UnsupportedOperationException: This feature is not implemented: Unsupported SQL type timestamptz_ns
{'passed': 6, 'failed': 33, 'xfailed': 0}
```

**Clause 2 (C-002)** — `insert_timestamp_ns::before_analysis` / `after_analysis` results
discarded in `spark_ast` and `conform_values_timestamp_columns` replaced by the identity in
`instant_ts`:

```
FAILED test_insert_values_writes_every_shape_exactly - PySparkException: datafusion engine error: Execution error: Inconsistent types in ScalarValue::iter_to_array. Expected Timestamp(Nanosecond, None), go
FAILED test_insert_values_widens_a_timestamp_literal - PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Timestamp(ns) but found
FAILED test_insert_values_widens_a_string - PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Timestamp(ns) but found
FAILED test_insert_select_widens_a_microsecond_column - PySparkException: Unexpected => Failed to write using parquet writer., source: Arrow: Incompatible type. Field 'ts' has type Timestamp(ns), array has 
FAILED test_insert_overwrite_widens_and_keeps_ns - PySparkException: datafusion engine error: Execution error: Inconsistent types in ScalarValue::iter_to_array. Expected Timestamp(Microsecond, Some("UT
FAILED test_merge_insert_and_update_keep_ns - PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Timestamp(ns) but found
FAILED test_days_partitions_equal_the_pyiceberg_read_back - PySparkException: datafusion engine error: Execution error: Inconsistent types in ScalarValue::iter_to_array. Expected Timestamp(Nanosecond, None), go
XFAIL  test_hours_partitions_equal_the_spec
{'passed': 31, 'failed': 7, 'xfailed': 1}
```

**Clause 4 (C-004)** — the `TimeUnit::Nanosecond` arm of `wall_clock_from_ticks` removed (ticks
floor to microseconds again):

```
XFAIL  test_hours_partitions_equal_the_spec
FAILED test_cast_ns_column_as_string_is_lossless - AssertionError: {'id': 1, 's': '2026-01-02 03:04:05.123456', 'z': '2026-01-02 03:04:05.123456'}
FAILED test_cast_ns_cast_as_string_round_trips[1] - AssertionError: assert '2026-01-02 03:04:05.123456' == '2026-01-02 0...:05.123456789'
FAILED test_cast_ns_cast_as_string_round_trips[2] - AssertionError: assert '2026-01-02 23:59:59.999999' == '2026-01-02 2...:59.999999999'
FAILED test_cast_ns_cast_as_string_round_trips[3] - AssertionError: assert '2026-01-03 00:00:00' == '2026-01-03 0...:00.000000001'
FAILED test_cast_ns_cast_as_string_round_trips[8] - AssertionError: assert '2026-01-02 03:04:05.123456' == '2026-01-02 0...:05.123456788'
{'passed': 33, 'failed': 5, 'xfailed': 1}
```

`round_trips[6]` (`…05.5`) and `[7]` (no fraction) stay green under the clause-4 mutation, as they
must: microsecond rendering already answers them.

## 5. Assumptions taken (the ruling leaves these open; the narrowest option was chosen)

- **A-1.** A zone-carrying string cast to (or written into) `timestamp_ns` becomes the instant's
  session-zone wall. Spark's own `CAST(<string> AS TIMESTAMP_NTZ)` drops the zone instead; the
  INSERT planner types a `TIMESTAMP '…'` literal and a string literal identically, so one rule
  covers both, and it is the rule a `TIMESTAMP` value into a naive column already follows.
- **A-2.** *(Revised in round 2 by ruling Q-21c-8; the round-1 text read a `TIMESTAMP '…'`
  literal in `VALUES` at nanosecond precision.)* The nine-digit re-parse of a `VALUES` cell
  written into an ns column applies only to a cell that is not `TIMESTAMP`-typed: an explicit
  `timestamp_ns` / `timestamptz_ns` cast (lowered before planning) and a bare string (A-3). A
  `TIMESTAMP '…'` literal or a `CAST(… AS TIMESTAMP)` cell is Spark's µs type, floored to
  microseconds and widened × 1000, exactly as `INSERT … SELECT` stores it. The planner types a
  bare string and a `TIMESTAMP` cell identically, so the door reads the cell's type name from the
  statement before planning.
- **A-3.** "A string written by `INSERT … SELECT`" is read as "where RePark accepts a string for a
  `TIMESTAMP` column": string literals in `VALUES` widen; a string COLUMN (and a string literal
  projected by `INSERT … SELECT`) stays refused by the WI-2 store-assignment gate, exactly as for
  microsecond columns (Spark's ANSI store assignment excludes string → timestamp). MERGE with a
  string source column stays refused by the MERGE gate for the same reason.
- **A-4.** The malformed-string error keeps the `TIMESTAMP` cast's class and SQLSTATE
  (`[CAST_INVALID_INPUT] … 22018`) and names the actual target (`"TIMESTAMP_NS"` /
  `"TIMESTAMPTZ_NS"`). Widening that overflows int64 nanoseconds (outside roughly 1677–2262)
  raises `[CAST_OVERFLOW] … 22003` under ANSI and is NULL without it.
- **A-5.** The embedded casts are `Volatility::Volatile`, like `__repark_timestamp_to_string__`,
  so constant folding never leaves a `Timestamp(ns)` literal for a later run of
  `spark_ltz_timestamp_cast` to narrow. Cost: a `WHERE ts = CAST('…' AS timestamp_ns)` predicate
  is evaluated above the Iceberg scan instead of being pushed into it — correct, not pruned.
- **A-6.** The `VALUES` retype is general: `SELECT * FROM (VALUES (1, TIMESTAMP '…'))` now
  answers `Timestamp(µs, "UTC")` where it raised the raw Arrow error before (root cause (b)).
  `INSERT OVERWRITE … VALUES` needs it, because its source is planned as exactly that query.
- **A-7.** The `timestamptz_ns` Arrow annotation is `"UTC"`, the annotation the Iceberg read
  path already gives the column.
- **A-8.** The `hours(tz)` expectation is derived from the spec, because the DataFrame-door
  control is refused by the same fork gap.

## 6. Residues

- **R-001** (registry `ICE-TSNS-SQL-1-R-001`, OPEN 2026-09-17) — `DESCRIBE` answers
  `timestamp_ntz` / `timestamp` for ns columns. Out of scope by ruling clause 6.
- **R-002** (registry `ICE-TSNS-SQL-1-R-002`, OPEN 2026-09-17) — the write-direction oracle:
  PyIceberg 0.12.0 refuses to write format v3 (apache/iceberg-python#1551); the future oracle is
  the Iceberg Java API writing v3 ns files for RePark to read.
- **R-003** (registry `ICE-TSNS-SQL-1-R-003`, BLOCKED-ON-FORK F-TSNS-HOUR-1) — `hours()` on ns
  refuses in the fork's `Hour::transform`; C-007 stays OPEN until the repin.
- **R-004** — `TRY_CAST(… AS timestamp_ns)` still refuses `Unsupported SQL type` (the lowering
  takes `CAST` and `::` only); stated in the registry row.
- **R-005** (observed, not ns-specific) — `INSERT INTO t SELECT id, ts, ts FROM s` refuses
  `Projections require unique expression names` on any table (Spark accepts a repeated source
  column); the pins alias distinct columns instead.

## 7. Gates (release native of the step-2 tree)

1. Comment ban — `comment_ban.py <clone> origin/main`: see the hand-back (run on the final
   commit); the staged-diff grep in the brief printed nothing before each commit.
2. `cargo test` per crate:

```
repark-functions: 772 passed, 0 failed, 1 ignored across 2 test binaries
repark-iceberg: 448 passed, 0 failed, 0 ignored across 2 test binaries
repark-core: 623 passed, 0 failed, 2 ignored across 5 test binaries
repark-spark: 1200 passed, 0 failed, 4 ignored across 10 test binaries
```

   `repark-functions` owns the cast; `repark-sql` is untouched.
3. `pytest test_ice_tsns_sql_1.py test_v3_create_opt_in.py` — 43 passed, 1 xfailed
   (`test_hours_partitions_equal_the_spec`, F-TSNS-HOUR-1).
4. `pytest python/repark/tests -n 8` — 9722 passed, 398 skipped, 48 xfailed, 3 failed. None of
   the three touches this change:
   `test_spark_sql_grammar_1.py::test_q14_current_date_bare_and_paren[ansi-off|ansi-on]` compares
   the UTC session's `current_date` with the host-local `date.today()`, and the run crossed UTC
   midnight while the host (EDT) was still on the 17th (RePark answered 2026-09-18, the host
   2026-09-17; it reproduces alone for the same reason and passes outside the 20:00–24:00 EDT
   window); `test_perf_ice_catalog_io_1.py::test_the_second_statement_on_a_many_manifest_table_is_under_the_target`
   is a wall-clock budget that failed while `make verify` loaded the host, and the file passes
   alone (34 passed, 2 skipped).
5. `pytest python/repark-parity/tests -n 8` — 756 passed, 3 skipped, 12 xfailed on the third
   run. The first two runs had one failure each,
   `torture/test_torture_v3_dv.py::test_v3_dv_dataframe_door` (`PySparkException` from
   `to_arrow`), which passes alone (7 passed, 1 skipped). Its fixture materializes into one fixed
   host-wide directory (the fixture module's `CANONICAL_TABLE_DIR`), and every materializer (any worker, any clone on the
   host) deletes and re-copies that tree holding the lock only for the copy, so a concurrent
   reader can lose the table mid-read. The read path it exercises (`INT` / `STRING` columns, DV
   deletes) is not touched by this unit.
6. `make verify` — exit 0; 3824 Rust tests passed, 0 failed, 7 ignored across 57 binaries;
   every static gate clean (crate DAG, lib-rs, file-size, lib-py, conventions, docstrings, ledger
   check and grammar, docs compaction and links, parity dual-wire).
7. Fixture `check` leg (PyIceberg 0.12.0) — 0 failures; `sql_hours` BLOCKED (§3).
8. Mutation transcripts — §4.

## Round 2 (2026-09-18) — run 21c

**Base:** round 1 rebased onto `origin/main` `6617d215` (fork pin RP-24 `8fb44a39`), head
`b0e19783`. **Model:** claude-opus-5. **Reviews closed:** Critic-3 logic (NEEDS_REMEDIATION:
L-01 P1, L-02 P2, L-03 P3) and the Rust performance review (PASS with P-01..P-04 P2, P-05..P-07
P3).

**Ruling Q-21c-8 (orchestrator, extends Q-21c-6), summarized:** the SQL type name `TIMESTAMP`
is always Spark's microsecond LTZ type — the type name in `CAST(… AS TIMESTAMP)`, the literal
`TIMESTAMP '…'` and a planned `Cast(_, Timestamp(ns))` standing for it — whatever the source (an
ns column included) and the statement shape (`VALUES` included). Extra fraction digits floor to
microseconds, matching RePark's existing µs cast. Only `timestamp_ns` / `timestamptz_ns` keep
nine digits. A µs value written into an ns column widens exactly (× 1000), and no path re-parses
a `TIMESTAMP`-typed value at nine digits. The SQL spelling answers what `.cast("timestamp")`
answers.

### R2.1 Measured on `b0e19783` (release native)

- L-01 reproduced: `CAST(ts AS TIMESTAMP)` on `timestamp_ns` / `timestamptz_ns` answered
  `timestamp[ns]` with every digit. `.cast("timestamp")` answered `timestamp[us, tz=UTC]`, but
  through Arrow's plain cast: `-1 ns` became `0` (truncation toward zero, not a floor), and in
  America/New_York a `timestamp_ns` wall was read as UTC (`1767323045123456`, where the µs
  `TIMESTAMP_NTZ` → `TIMESTAMP` rule gives `1767341045123456`). The two spellings agreed only in
  UTC after the epoch.
- L-02 root cause: the planner types a bare string, `TIMESTAMP '…'` and `CAST(… AS TIMESTAMP)`
  in a `VALUES` row for a `timestamp_ns` column identically
  (`CAST(Utf8("…") AS Timestamp(ns))`), so no plan-level rule can tell them apart. The type
  name is only in the statement.

### R2.2 Implementation

- **L-01** — `spark_ltz_timestamp_cast` puts `__repark_narrow_timestamp_ns__` (in
  `timestamp_ns_cast.rs`) in place of a cast from a nanosecond source to `Timestamp(ns, None)`
  (the SQL `TIMESTAMP`) or to `Timestamp(µs, "UTC")` (the DataFrame `"timestamp"`). The kernel
  floors ticks to microseconds with `div_euclid`, keeps a zoned instant, and localizes a
  zoneless wall in the session zone with the same `localize_wall_micros_in_zone` the µs rule
  uses. It accepts any timestamp unit: a `UNION`'s schema can still say ns after its arms were
  rewritten to µs (found by `plan_partitioning` in `cargo test -p repark-spark`), and those
  ticks convert exactly. Its field keeps the argument's nullability, so `current_timestamp()`
  stays non-null (found by `test_dogfood_gaps.py`).
- **L-02** — `insert_timestamp_ns::timestamp_typed_values_cells` reads the (row, column) of
  every `TIMESTAMP`-typed `VALUES` cell (`TIMESTAMP '…'`, `CAST(… AS TIMESTAMP)`, parenthesized
  or not) from the statement before planning. `spark_ast` passes them to `before_analysis`, and
  `conform_values_rows` leaves those cells to the µs rule. The `VALUES` conform then widens them
  × 1000, as `after_analysis` widens a `SELECT`.
- **L-03** — `spark_ast` applies `lower_timestamp_ns_casts` to the statement an `EXPLAIN`
  wraps.
- **P-01** — `or_overflow` takes `impl Display`, so the value is formatted only on the overflow
  arm.
- **P-02** — `values_actions` reads only nanosecond-declared columns by reference. A `VALUES`
  with none, or with nothing to change, is returned without a clone. When a change is needed,
  the rows are moved, not cloned.
- **P-03** — MERGE clones and lowers its AST only when the read-only probe
  `has_timestamp_ns_cast` finds a `timestamp_ns` / `timestamptz_ns` cast.
- **P-04** — same-kind widening (a zoned source into `timestamptz_ns`, a naive one into
  `timestamp_ns`) is Arrow's vectorized checked cast (`safe: true`, nulls on overflow). Under
  ANSI, an overflow is found by comparing null counts and raises the row path's
  `[CAST_OVERFLOW]` naming the first overflowing value. Overflow is real here: µs values past
  2262-04-11 or before 1677-09-21 do not fit i64 ns (unit
  `same_kind_instants_widen_and_overflow_like_the_row_path`). Both ns casts are one shared
  `LazyLock` UDF instance each.
- **P-05** — `before_analysis` / `after_analysis` inspect the plan by reference and return a
  non-insert plan without moving it into a box.
- P-06 and P-07 are left as they are (P3; one loop-invariant match and one extra pattern arm).

### R2.3 Assumptions taken

- **A-2** is revised in place (§5).
- **A-9.** The DataFrame spelling `.cast("timestamp")` on an ns column goes through the same
  kernel as the SQL spelling, so the two agree in every zone and on both sides of the epoch.
  The DataFrame answer changes where Arrow's cast diverged from RePark's µs rule: pre-epoch
  sub-microsecond ticks now floor, and a `timestamp_ns` wall in a non-UTC session is read in
  the session zone.
- **A-10.** A bare string in `VALUES` keeps nine digits (A-3, fixture row 5). The ruling's "only
  an explicit ns cast" is read against its principle, "no path re-parses a `TIMESTAMP`-typed
  value": a string literal is not `TIMESTAMP`-typed, and the recorded PyIceberg read-back of
  `sql_days` row 5 holds `…000000001`.

### R2.4 Mutation proofs (release native, one revert at a time, then restored)

L-01 — `rewrite_cast`'s narrowing arm disabled (`if false && matches!(source, …)`):

```
FAILED test_ice_tsns_sql_1.py::test_cast_ns_column_as_timestamp_floors_like_the_dataframe_spelling[America/New_York]
FAILED test_ice_tsns_sql_1.py::test_cast_ns_column_as_timestamp_floors_like_the_dataframe_spelling[UTC]
E   AssertionError: ('ts', ts: timestamp[ns] … tz: timestamp[ns] …)
2 failed, 40 passed, 1 xfailed
```

L-02 — `conform_values_rows` re-parses `TIMESTAMP`-typed cells again
(`if false && timestamp_cells.binary_search(…).is_ok()`):

```
FAILED test_ice_tsns_sql_1.py::test_timestamp_typed_values_floor_like_insert_select
E   At index 0 diff: {'id': 1, 'ts': 1767323045123456789, 'tz': 1767323045123456789}
      != {'id': 1, 'ts': 1767323045123456000, 'tz': 1767323045123456000}
1 failed, 41 passed, 1 xfailed
```

The L-02 pin uses a nine-digit literal (`…05.123456789`) and asserts
`floored != wall("1")`, so the floor and the re-parse cannot look alike.

### R2.5 Residues

- **R-004** — unchanged (`TRY_CAST(… AS timestamp_ns)`). L-03 closed rather than joining it.
- **R-006** (observed 2026-09-18) — under `spark.sql.timestampType=TIMESTAMP_NTZ`,
  `CAST(<ns column> AS TIMESTAMP)` still keeps `Timestamp(ns)`: the NTZ-mode rewrite
  (`rewrite_cast_as_ntz`) has no nanosecond-source arm. Ruling Q-21c-8 rules on the default LTZ
  type. Named in the registry row.
- **R-007** (registry `ICE-TSNS-SQL-1-R-007`, OPEN 2026-09-18) — in a non-UTC session, a MERGE
  insert/update writes a `TIMESTAMP` into a `timestamp_ns` column as its UTC wall. INSERT stores
  the session-zone wall (A-1). MERGE widens through the Iceberg write path's Arrow cast. Round
  2 did not change it.

### R2.6 Gates (release native of the round-2 tree)

1. Comment ban: `comment_ban.py <clone> origin/main` reports `comment-ban hits=0`, and the
   staged-diff grep in the brief printed nothing.
2. `cargo test` per crate: `repark-functions` 777 passed, 0 failed, 1 ignored; `repark-spark`
   1204 passed, 0 failed, 4 ignored across 10 binaries; `repark-iceberg` 448 passed, 0 failed.
   The first `repark-spark` run failed 7 `plan_partitioning` tests on the stale-`UNION`-schema
   case (R2.2), which the unit-robust kernel fixed. The `mem::take` spelling clippy asked for
   afterwards is covered by `make verify`.
3. `pytest test_ice_tsns_sql_1.py test_v3_create_opt_in.py`: 47 passed, 1 xfailed
   (`test_hours_partitions_equal_the_spec`, F-TSNS-HOUR-1).
4. `pytest python/repark/tests -k "timestamp or values or cast or merge" -n 4`: 1324 passed,
   47 skipped, 12 xfailed. The first run failed
   `test_dogfood_gaps.py::test_current_timestamp_arrow_type_is_microsecond_utc` on the
   narrowing field's nullability (R2.2), which is fixed.
5. `make verify`: exit 0. 3833 Rust tests passed, 0 failed, 7 ignored across 57 binaries, and
   every static gate is clean.
6. Fixture `check` leg (PyIceberg 0.12.0): 0 failures; `sql_hours` BLOCKED (F-TSNS-HOUR-1).
7. Mutation transcripts: R2.4.

## Orchestrator ruling Q-21c-9 (2026-09-18): the two round-2 readings are confirmed

- **A-9 is confirmed.** Casting to `TIMESTAMP` from a nanosecond source gives one answer through both spellings, the SQL
  `CAST(… AS TIMESTAMP)` and the DataFrame `.cast("timestamp")`. A `timestamp_ns` wall is localized in the session zone,
  as Spark's NTZ → LTZ cast does. A `timestamptz_ns` instant is kept. Both are floored to microseconds. The DataFrame
  spelling's earlier answer came from Arrow's plain cast: it truncated toward zero before the epoch, and it read an NTZ
  wall as UTC outside UTC. That answer was silently wrong, and correcting it is part of L-01, not a side effect.
- **A-10 is confirmed.** A bare string written into an ns column is assigned at the column's own precision (nine
  digits), the same as `CAST(<string> AS timestamp_ns)`. A string is not TIMESTAMP-typed. Ruling Q-21c-8 floors only
  values whose SQL type is `TIMESTAMP`.
