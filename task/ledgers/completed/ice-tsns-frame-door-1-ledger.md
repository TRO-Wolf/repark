# Unit ledger — ICE-TSNS-FRAME-DOOR-1 · pandas and polars nanosecond timestamps enter as microseconds, as Spark does

**Scope (2026-10-13):** Spark's `createDataFrame` turns pandas `datetime64[ns]` into
microseconds, floored, with no warning. RePark carried nanoseconds in, so a pandas frame
silently became a `timestamp_ns` source. After this unit a top-level nanosecond timestamp
column from pandas or polars takes the path a microsecond column takes. Python only; no Rust
file, no session setting.

**Date:** 2026-10-13 · **Branch:** `fix/ice-tsns-frame-door-1` · **Base:** `8c51fbc4` ·
**Model:** Claude Opus 5.5 (executor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** the owner's ruling 2 of 2026-10-13 (parity, fixed at the frame door), work order
ICE-TSNS-FRAME-DOOR-1; design of record: the ICE-TSNS-CREATE-DOOR-1 design sketch (sections 5,
5a, 7, 8) and census. **This ledger closes when the unit's pull request merges.**

**Out of scope, untouched:** how a naive wall is read (zone, LTZ or NTZ); pandas
`datetime64[ms]` and `[s]`; nested `ArrowDtype` nanoseconds (Spark refuses those frames; they
stay carried); `schema="c timestamp_ns"`; Arrow tables; the Parquet read door; `write.parquet`;
the keep-nanoseconds switch (the create-door order).

## PROPOSITION LEDGER — ICE-TSNS-FRAME-DOOR-1 — 2026-10-13

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Spark 4.1.2's answer for a pandas nanosecond frame is recorded live by a committed recorder. | The oracle JSON and its recorder. | **PROVEN** | `python/repark/tests/ice_tsns_frame_door_1_spark.json`, 48 cells (eight sources, two session zones, three schemas), recorder `_record_ice_tsns_frame_door_1.py`; §1. The 22 cells shared with the design sketch's probe are identical. |
| C-002 | A top-level `timestamp[ns]` or `timestamp[ns, tz]` column answers exactly what the same column floored to `datetime64[us]` by pandas answers: schema, nullability, values, in both sessions, under no schema, `c timestamp` and `c timestamp_ntz`. | `test_nanosecond_column_equals_its_microsecond_twin`, 42 cells. | **PROVEN** | Red on main (30 of the 42), green on head; §2. Naive stays `timestamp[us]`, New York stays `timestamp[us, tz=America/New_York]` under no schema. |
| C-003 | An aware nanosecond column holds the instants and nullability Spark recorded. | `test_aware_nanosecond_values_equal_the_spark_oracle`, 16 cells. | **PROVEN** | Seed `1767323045123456`; the column with no NULL is nullable on both engines. |
| C-004 | Nanoseconds before the epoch floor as Spark floors: `-1`, `-1001`, `-999999999999` ns are `-1`, `-2`, `-1000000000` µs. | `test_pre_epoch_nanoseconds_floor_as_spark_floors`. | **PROVEN** | Aware cells equal the oracle, naive cells equal the twin. `cast(safe=False)` gives `0`, `-1`, `-999999999` (mutant m2). |
| C-005 | `schema="c timestamp_ntz"` over a naive nanosecond column answers Spark's microseconds. | `test_declared_timestamp_ntz_over_nanoseconds_answers_microseconds`, 6 cells. | **PROVEN** | Main raised `ArrowInvalid … would lose data` in all 6; head equals the oracle's type, nullability and values. |
| C-006 | `schema="c timestamp"` over a nanosecond column answers what main answers, in all 14 cells. | `test_declared_timestamp_over_nanoseconds_is_unchanged`. | **PROVEN** | Main's cells recorded on `8c51fbc4` before the source edit (§3). Ruling Q1 (§5). |
| C-007 | A column that is not a top-level nanosecond timestamp keeps main's Arrow type and values; a list or struct of nanoseconds is still carried. | `test_columns_that_are_not_nanosecond_timestamps_are_untouched` (9 kinds), `test_nested_arrowdtype_nanoseconds_are_still_carried` (2). | **PROVEN** | `datetime64[us]` naive and UTC, `[ms]`, `[s]`, date, int, string, decimal, a list of microsecond timestamps: equal to main's recorded cells (§3). |
| C-008 | No warning is raised, and the column is nullable also with no NULL. | `test_no_warning_is_raised_for_a_nanosecond_frame`, 14 cells; nullability in C-002 and C-003. | **PROVEN** | Spark's 48 cells hold no warning. |
| C-009 | A table created from a pandas nanosecond frame holds a microsecond column. | `test_pandas_frame_create_doors_make_microsecond_tables`, 12 cells. | **PROVEN** | CTAS from a temp view (default format version), `writeTo().create()`, `saveAsTable`; naive and UTC; the Iceberg type read from the metadata JSON is `timestamp` / `timestamptz`, the value read back is `1767323045123456`. All 12 red on main. |
| C-010 | A polars `Datetime("ns")` column arrives as microseconds, floored, naive and UTC. | `test_polars_nanosecond_column_arrives_as_microseconds`, 8 cells, not skipped in the gate environment. | **PROVEN** | polars 1.43.2; equal to its polars microsecond twin. |
| C-011 | The 235 tests the fix reds keep their meaning with no expectation or fixture changed; only their seeds move off pandas. | The four files, and the fixtures unchanged in the diff. | **PROVEN** | §4. `frame_of` and the SQL test's control seed register an Arrow table through the stream seam; one boundary-shapes row re-recorded live into its microsecond twin's class. Two body hashes updated (ruling Q2, §5). |
| C-012 | `schema="c timestamp"` over an aware **microsecond** column whose zone is not UTC answers `timestamp[us, tz=UTC]`, Spark's type. | `test_declared_timestamp_over_an_aware_microsecond_column_answers_utc`, 2 cells. | **PROVEN** | Moved from main in type label only (`America/New_York` to `UTC`), same instants; consequence of ruling Q1 (§5). Under no schema the label is still the source's. |
| C-013 | The cost gate holds as re-based: a frame with no nanosecond column within 1.05 of main; a nanosecond column costs at most 25 ms per million rows. | Five interleaved sets. | **PROVEN** | `task/wo/ice-tsns-frame-door-1/evidence/cost.md`: ratio 1.009; 17.1 ms per million rows per nanosecond column (largest set 17.39). Ruling Q3 (§5). |
| C-014 | The work order's six mutants and the Q1 mutant are killed by the pin file. | Each mutant run against `test_ice_tsns_frame_door_1.py`. | **PROVEN** | §6: nine variants, nine killed. |
| C-015 | The gates of the work order pass. | Real exit codes. | **PROVEN** | §7. |
| C-016 | The parity registry carries the dated row and the ledger carries the release-note sentence. | Slice 3. | **PROVEN** | Row `ICE-TSNS-SQL-1-R-019` in `docs/spark-sql-iceberg-parity.md`, FIXED 2026-10-13; §10. |

## 1. The oracle (C-001)

`_record_ice_tsns_frame_door_1.py` ran on Spark 4.1.2 with pandas 3.0.6 and pyarrow 25.0.1,
one session per zone (UTC, America/New_York), ANSI on, a warehouse of its own under the system
temporary directory. Sources: naive ns, UTC ns, New York ns, naive ns before the epoch, UTC ns
before the epoch, a naive `ArrowDtype` ns, a UTC ns column with no NULL, and (added for ruling
Q1) a New York microsecond column. Every answering cell is `timestamp[us, tz=UTC]` or, for
`c timestamp_ntz`, `timestamp[us]`; nullable; floored; no warning. Spark refuses
`c timestamp_ntz` over an aware column (`FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME`, 10 cells).

Spark reads a naive wall in the zone of the **driver machine** (`driver_zone` EST/EDT in the
file): the naive seed under no schema is `1767341045123456` in both sessions. That is the
out-of-scope zone question, so no pin compares a naive cell's instant to Spark; naive cells
are compared to their microsecond twin through the same door on the same build.

## 2. The fix (C-002 to C-005, C-008 to C-010)

`_floor_nanosecond_timestamp_column` is the first statement of
`_normalize_frame_arrow_column` in `python/repark/src/repark/spark/session/create_dataframe_arrow.py`,
the one function every pandas and polars column passes. It is
`pyarrow.compute.floor_temporal(column, unit="microsecond")` under a safe cast to
`timestamp("us", tz=<same>)`; a column of any other type is returned as the same object. Arrow
kernels on whole columns; no row is read. It runs before the dictionary, infinity and declared
type branches, so a declared type sees microseconds.

Red first, on main's source with the pin file as written: 66 failed, 70 passed (the
unchanged, untouched, nested and no-warning pins pass on main, as they must).

## 3. Main's recorded answers (C-006, C-007)

Recorded by running `createDataFrame(...).to_arrow()` on `8c51fbc4` **before** the source was
edited; the pin file holds them as literal tables (`MAIN_DECLARED_TIMESTAMP`, `MAIN_UNTOUCHED`,
`MAIN_NESTED`). The work order names no fixture file for them.

- Declared `c timestamp`, all 14 cells `timestamp[us, tz=UTC]`, nullable. UTC session: seed
  `1767323045123456`, before the epoch `[-1, -2, -1000000000]` for every source. New York
  session: aware sources the same; naive sources read in the session zone
  (`1767341045123456`, `[17999999999, 17999999998, 17000000000]`).
- Untouched, both sessions: `timestamp[us]`, `timestamp[us, tz=UTC]`, `timestamp[ms]`
  (`1767323045123`), `timestamp[s]` (`1767323045`), `date32[day]` (`20455`), `int64`,
  `string`, `decimal128(38, 18)`, `list<item: timestamp[us, tz=UTC]>`.
- Nested, both sessions: `list<item: timestamp[ns, tz=UTC]>` and
  `struct<x: timestamp[ns, tz=UTC]>` holding `1767323045123456789`.

## 4. The tests that moved (C-011)

With the step and before any re-seat, 235 tests fail in four files, the design sketch's
number: `test_ice_tsns_merge_wall_1.py` 132, `test_ice_tstzns_wall_1.py` 99,
`test_ice_tsns_sql_1.py` 3, `test_boundary_shapes_parity.py` 1. They feed a DataFrame store
door a nanosecond frame built through pandas.

- `_ice_tsns_merge_wall_1_doors.py::frame_of` registers the Arrow table with
  `register_arrow_stream_as_temp_view` under a fresh name and returns `spark.table(name)`. The
  seam carries every type the callers use, the fourteen nested layouts and the run-end and
  dictionary encodings among them. `ice_tsns_merge_wall_1_main.json` is unchanged.
- `test_ice_tsns_sql_1.py::write_control` builds the same values as an Arrow table and seeds
  through the same seam. `ice_tsns_sql_1_oracle.json` is unchanged.
- `test_boundary_shapes_parity.py`: `_record_boundary_shapes_goldens.py --emit` on live Spark
  4.1.2 reproduces every Spark half; the row `pandas_timestamp_unit_from_pandas_ns` is now a
  disclosure (Spark exports `datetime64[ns]`, RePark `datetime64[us]`), the class of
  `pandas_timestamp_unit_from_pandas_us`. The other nine rows keep their class; the corpus is now five
  equalities and five disclosures.
- `test_production_file_size.py`: the frozen body hashes of `_normalize_frame_arrow_column`
  and `_localize_naive_timestamp_column` move, because both bodies changed on purpose.

## 5. Rulings (orchestrator, provisional, 2026-10-13)

The owner sees each in the pull request and may flip it.

- **Q1.** For `schema="c timestamp"` over a New York-aware nanosecond column main and Spark
  both answer `timestamp[us, tz=UTC]`; main's microsecond path kept the New York label.
  Ruling: the two cells keep main's answer, which is Spark's. `_localize_naive_timestamp_column`
  owns that answer and its contract is `timestamp[us, tz=UTC]`: its aware-microsecond shortcut
  now returns the column relabelled to UTC (a cast between zone labels of one unit, no row
  read, same instants; a column already labelled UTC is returned as the same object).
  Consequence, recorded: declared `c timestamp` over an aware microsecond column whose zone is
  not UTC moves in type label only, toward Spark (C-012). Every path that declares no zoned
  timestamp keeps the source label. Whole facade suite after the relabel: no other test
  changed its answer (§7).
- **Q2.** Yes: the two hash lines in `test_production_file_size.py` and its map row, in the
  same commit.
- **Q3.** The kernel stays. A frame with no nanosecond column: 1.05 of main. A frame with
  one: main does no work there and Spark does the same conversion, so the absolute cost is
  recorded with a ceiling of 25 ms per million rows per nanosecond column on this box. The
  declared-`timestamp` path is a number with no gate: 15.3 s on main, 9.6 s on head.

## 6. Mutants (C-014)

Run by the executor against the pin file (138 tests green unmutated); an independent verifier
may re-run them.

| # | Mutant | Failed | Killed by |
|---|---|---|---|
| m1 | the step returns the column unchanged | 64 | twin, oracle, pre-epoch, declared ntz, create doors, polars |
| m2 | `cast(target, safe=False)` in place of the floor | 28 | pre-epoch, twin, oracle, declared ntz, declared timestamp, polars |
| m3 | aware columns skipped | 36 | twin, oracle, create doors, polars, pre-epoch |
| m4a | zone dropped on aware columns | 40 | twin (type), oracle, declared timestamp, create doors |
| m4b | aware columns relabelled to UTC inside the step | 2 | twin (type, New York under no schema) |
| m5 | the step placed after the declared-type branch | 20 | declared ntz, twin |
| m6a | the step also floors `timestamp[ms]` | 2 | untouched |
| m6b | the step descends into lists | 2 | nested |
| m7 | the shortcut keeps the source zone label (Q1 undone) | 4 | declared timestamp (2), aware microsecond (2) |

## 7. Gates (C-015)

| Gate | Result |
|---|---|
| `make py-lint py-format-check check-lib-py check-python-conventions check-docstring-presence` | each 0 |
| `make check-ledgers check-ledger-grammar check-docs-links check-docs-compaction spell-check` | each 0 |
| `bash scripts/check_map_md.sh --base 8c51fbc4`, `python3 scripts/sync_map_md.py --check` | 0, 0 |
| the six files of the order's §5 (`-n 8`) | 0 failed; the polars pin ran |
| `pytest python/repark/tests -k "pandas or polars or createDataFrame or interchange"` | 0 failed; 6 skipped (5 need `pyspark`, 1 needs the moto environment) |
| the whole facade suite (`-n 8`), after the Q1 relabel | 16,620 passed, 1 failed, 482 skipped, 151 xfailed. The failure is `test_casesens_1.py::test_s5_iceberg_ddl_binds_exactly`, which picks the latest metadata file by modification time: run alone it fails 5 of 6 times on main's Python source and 2 of 3 on head, and it passed in the full run before the relabel. Not this unit's; reported, not re-pinned. |
| cost | §5 Q3 and the evidence file |
| comment gate over the branch | 0 hits |

## 8. Cells this unit moves

Measured here, not re-run against the census probes: the 235 tests of §4; 6 declared
`timestamp_ntz` cells over naive sources from `ArrowInvalid` to Spark's answer (C-005); 8
declared `timestamp_ntz` cells over aware nanosecond sources from `ArrowInvalid` to the
microsecond twin's answer, a UTC wall, where Spark refuses (the twin already answered so on
main); 12 create-door cells from a nanosecond column or `Invalid schema for v2:` to a
microsecond table (C-009); 2 declared-`timestamp` aware microsecond cells in type label
(C-012). The census counts of the work order (30 pandas NOT_ASKED cells, 26 v2 creates, 4
`saveastable_v3`) are the census's and were not re-measured.

## 9. Observed, out of scope

- Spark reads an `ArrowDtype` naive column in the driver zone even under `schema="c timestamp"`
  in a UTC session (`1767341045123456`), where a numpy-backed naive column is read in the
  session zone. No pin compares it.
- `_localize_naive_timestamp_column` and `_validate_decimal_column_envelope` read rows with
  `to_pylist` on main; the declared-`timestamp` path costs seconds per million rows.
- A value a nanosecond pandas frame appends into an existing `timestamp_ns` column is now cut
  to microseconds; keeping it is the create-door order's session setting.

## 10. Registry row and release note (C-016)

Parity row `ICE-TSNS-SQL-1-R-019`, the next free number of the family on this base, dated
2026-10-13. It names the two declared-`timestamp` cells that moved in type label under ruling
Q1 and what did not change.

**Release note (1.5.6):** `createDataFrame` now turns pandas nanosecond timestamps into
microseconds, as Spark does.

## 11. Coverage attestation

Filed by the executor from the runs recorded above. No independent Critic has run on this
unit; the orchestrator's verify stage re-attests.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-tsns-frame-door-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every behaviour clause has a pin that is red on main's source (66 of the first 136 tests failed there) and green on head; the unchanged, untouched and nested pins are green on both by design.
      artifacts: [python/repark/tests/test_ice_tsns_frame_door_1.py, task/ledgers/completed/ice-tsns-frame-door-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: Values one, 1001 and 999999999999 nanoseconds before the epoch, a NULL cell, a column with no NULL, a naive, a UTC and a New York column, numpy-backed and ArrowDtype-backed, pandas and polars, each in two session zones.
      artifacts: [python/repark/tests/test_ice_tsns_frame_door_1.py, python/repark/tests/ice_tsns_frame_door_1_spark.json]
    - id: AT-3
      status: ATTACKED
      evidence: The declared timestamp_ntz path that raised ArrowInvalid now answers and is pinned to Spark; nested nanosecond frames keep main's carried answer; no warning is raised, checked with warnings turned into errors.
      artifacts: [python/repark/tests/test_ice_tsns_frame_door_1.py]
    - id: AT-4
      status: N/A
      justification: The step is a pure function of one Arrow column; no session, cache or shared state is read or written.
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, path or deserialization; Arrow compute kernels on an in-memory column.
    - id: AT-6
      status: ATTACKED
      evidence: Wrong-value risk is the floor direction and the zone label. The pre-epoch pin and mutant m2 hold the direction; the twin pin's type assertion and mutants m4a, m4b and m7 hold the label on the no-schema and the declared paths; nine untouched column kinds equal main's recorded cells.
      artifacts: [python/repark/tests/test_ice_tsns_frame_door_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: Five interleaved cost sets. A frame with no nanosecond column is 1.009 of main; a nanosecond column costs 17.1 ms per million rows under a 25 ms ceiling; no row loop is added.
      artifacts: [task/wo/ice-tsns-frame-door-1/evidence/cost.md, python/repark/tests/_time_ice_tsns_frame_door_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: pyarrow 25.0.1 cast(safe=False) truncates toward zero, measured; the step uses floor_temporal and a safe cast, so a kernel that stops flooring fails the cast instead of storing a wrong value. No manifest or lock change.
      artifacts: [python/repark/src/repark/spark/session/create_dataframe_arrow.py]
    - id: AT-9
      status: N/A
      justification: The unit adds no error path and no message; one former raw ArrowInvalid becomes an answer.
    - id: AT-10
      status: ATTACKED
      evidence: Nine mutants, nine killed, each by the pin the work order names. The whole facade suite ran after the last source change; its one failure reproduces on main's source.
      artifacts: [python/repark/tests/test_ice_tsns_frame_door_1.py, task/ledgers/completed/ice-tsns-frame-door-1-ledger.md]
  reattested: []
  complete: true
```
