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
| C-017 | Fold 1: the floor is computed on the int64 ticks and never through local time. It answers in every repeated and skipped hour and for every int64 nanosecond value, the lowest 807 and `pd.Timestamp.min` / `.max` among them. | `test_instants_around_a_zone_transition_floor_on_the_ticks` (36), `test_the_lowest_and_highest_nanoseconds_floor` (16), `test_the_int64_minimum_floors_without_wrapping` (2). | **PROVEN** | §12.1. All 54 red on `c4efd79f` (`Local time is ambiguous`; `would lose data: 9223372036854775616`), green on the fold. Verifier findings F-01, F-02. |
| C-018 | Fold 1: a pandas categorical of nanosecond timestamps enters as microseconds. | `test_a_categorical_of_nanoseconds_arrives_as_microseconds` (12). | **PROVEN** | §12.2. The step runs after the dictionary decode. The naive categorical under `c timestamp_ntz` equals Spark's cell. F-04. |
| C-019 | Fold 1: under a declared type that is not a timestamp kind a nanosecond column answers what main answers. | `test_a_declared_type_that_is_not_a_timestamp_keeps_main` (36): `c bigint`, `c string`, `c date` over six sources. | **PROVEN** | §12.3. Main's cells recorded on `8c51fbc4`; 20 of the 36 were red on `c4efd79f`. F-05. |
| C-020 | Fold 1: each mutant listed in §12.5 and §13.1 is killed by the pin file or shown equivalent. No claim is made for mutants not listed. | Each listed mutant run against the pin file. | **PROVEN** | §12.5: the executor's 25 variants, 24 killed, 1 equivalent; the first verifier's survivor (the UTC relabel only for New York) fails 12 tests (F-07). §13.1: the second verifier ran 34 at `36382b62`, 31 killed, 2 equivalent, and 1 survived, so this row's earlier wording ("every non-equivalent mutant is killed") was wrong at that head; follow-up 3's pin fails 8 times on that survivor. |
| C-021 | Fold 1: on the verifier's catalogue no cell that answered on main errors on the fold, and the only no-nanosecond cells that differ from main are the 12 of the named move. | The verify stage's recorder run on the fold, against its recording of main. | **PROVEN** | §12.6 and `task/wo/ice-tsns-frame-door-1/evidence/fold-1.md`: 1388 cells; 0 main-answered and fold-raises (87 at `c4efd79f`); 12 of 520 no-nanosecond cells differ, the named move; 0 of 204 declared non-timestamp cells differ. |
| C-022 | Fold 1: the cost gate holds for a naive and for an aware nanosecond column. | Five interleaved sets. | **PROVEN** | §12.7 and `evidence/cost.md`: no nanosecond column 1.011 of main; per million rows at most 16.71 ms naive, 16.61 ms UTC, 15.80 ms New York; ceiling 25. F-03. |
| C-023 | Fold 1: the ledger and row R-019 name the Spark-side disclosures, every surface of the named move, and the append doors. | The text. | **PROVEN** | §12.4, §12.8, §12.9 and row `ICE-TSNS-SQL-1-R-019`. F-06, F-08, F-09. |

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

**The executor's own attestation**, filed from the runs recorded in this ledger and
re-attested after fold 1. It is not an independent verdict; the two independent verdicts are
in §11.1.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-tsns-frame-door-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every behaviour clause has a pin that was red before its change and is green after it. The fold's 82 pins were committed red on c4efd79f's source and are green on the fold.
      artifacts: [python/repark/tests/test_ice_tsns_frame_door_1.py, task/ledgers/completed/ice-tsns-frame-door-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: The int64 minimum, the lowest 807 nanoseconds, pd.Timestamp.min and max, values around the epoch, NULL cells, instants on both passes of a repeated hour and on both sides of a skipped hour in New York, London and Lord Howe, a categorical, pandas and polars, two session zones.
      artifacts: [python/repark/tests/test_ice_tsns_frame_door_1.py, python/repark/tests/ice_tsns_frame_door_1_spark.json]
    - id: AT-3
      status: ATTACKED
      evidence: The verify stage's 1388-cell catalogue re-run on the fold has no cell where main answered and the unit raises (87 before the fold). The declared timestamp_ntz path that raised now answers; no warning is raised.
      artifacts: [task/wo/ice-tsns-frame-door-1/evidence/fold-1.md, python/repark/tests/test_ice_tsns_frame_door_1.py]
    - id: AT-4
      status: N/A
      justification: The step is a pure function of one Arrow column; no session, cache or shared state is read or written.
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, path or deserialization; Arrow compute kernels on an in-memory column.
    - id: AT-6
      status: ATTACKED
      evidence: Wrong-value risks are the floor direction, a local-time round trip, int64 wrap, the zone label and a silent change under a declared non-timestamp type. Each has a pin and a killed mutant; 204 declared bigint, string and date catalogue cells equal main and 12 no-nanosecond cells differ, all the named move.
      artifacts: [python/repark/tests/test_ice_tsns_frame_door_1.py, task/wo/ice-tsns-frame-door-1/evidence/fold-1.md]
    - id: AT-7
      status: ATTACKED
      evidence: Five interleaved cost sets after the fold. No nanosecond column is 1.011 of main; a naive, a UTC and a New York nanosecond column each cost at most 16.71 ms per million rows under a 25 ms ceiling; no row loop is added.
      artifacts: [task/wo/ice-tsns-frame-door-1/evidence/cost.md, python/repark/tests/_time_ice_tsns_frame_door_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: pyarrow 25.0.1 floor_temporal raises on a zoned column in a repeated hour and wraps near the int64 minimum, and cast(safe=False) truncates toward zero, all measured; the step depends on integer divide, multiply, less and subtract only. No manifest or lock change.
      artifacts: [python/repark/src/repark/spark/session/create_dataframe_arrow.py]
    - id: AT-9
      status: N/A
      justification: The unit adds no error path and no message; former raw ArrowInvalid cells become answers.
    - id: AT-10
      status: ATTACKED
      evidence: 25 mutants, 24 killed and 1 equivalent, the verify stage's survivor among the killed. The whole facade suite ran on the fold's source, 16727 passed and 0 failed.
      artifacts: [task/wo/ice-tsns-frame-door-1/evidence/fold-1.md, python/repark/tests/test_ice_tsns_frame_door_1.py]
  reattested: [AT-1, AT-2, AT-3, AT-6, AT-7, AT-8, AT-10]
  complete: true
```

### 11.1 Independent verification: re-attestation after remediation

The ledger grammar admits one `COVERAGE_ATTESTATION` block per ledger, and it carries the
`reattested` list above. The two independent verdicts are recorded here in prose; neither
verifier wrote this ledger.

**Verifier 1 (a fresh Opus 5.5 session, high effort): FAIL on `c4efd79f`.** Ten findings,
and their closure in fold 1 as the second verifier judged it:

| Finding | What | Closure |
|---|---|---|
| F-01 (S1) | an aware nanosecond instant in a repeated hour raised; main answered | CLOSED: the floor is on the ticks (C-017) |
| F-02 | the lowest 807 nanoseconds raised; main answered | CLOSED: the same kernel (C-017) |
| F-03 | an aware column cost four times the ceiling | CLOSED: 14.7 to 16.9 ms per million rows (C-022) |
| F-04 | a categorical of nanoseconds was still carried | CLOSED: the step runs after the decode (C-018) |
| F-05 | declared `bigint` and `string` changed value from main | CLOSED: main's path under a non-timestamp type (C-019) |
| F-06 | where Spark is lossy the unit differs from Spark | CLOSED by ruling: the true floor stays, disclosed (§12.4) |
| F-07 | a mutant survived (the relabel only for New York) | CLOSED: it fails 12 tests (C-020) |
| F-08 | the named move also shows in `toPandas` and `show` | CLOSED: §12.8 and row R-019 |
| F-09 | appends into `timestamp_ns` store microsecond precision | CLOSED: §12.9, row R-019, the release note |
| F-10 | the re-seated tests no longer pass through `createDataFrame` | not in the fold's scope; recorded in §12.11 |

**Verifier 2 (a fresh Opus 5.5 session, high effort): PASS on `36382b62`.** Its stage counts:

- 0 of 1388 catalogue cells where main answered and head raises (87 at `c4efd79f`).
- 12 of 520 no-nanosecond cells differ from main, all the named move.
- 0 of 2288 cells under a declared non-timestamp type over nanoseconds differ from main
  (values, error text, Arrow schema, `df.schema`, both `explain` texts), and 0 of the
  catalogue's 204.
- 336,624 stored values checked against Python's floor over every 1880 to 2037 transition of
  New York, Lord Howe and Kolkata: 0 off the floor, 0 with a wrong type or label; the kernel
  alone on 8,640,405 values: 0 wrong.
- 34 mutants: 31 killed, 2 equivalent (the step widened to a declared `date`; the relabel
  also recasting a UTC column), 1 survivor that is not equivalent (V2-01), closed by §13.1.
- Cost, five interleaved sets: no nanosecond column 1.01 of main for two microsecond
  columns; a nanosecond column 14.7 to 16.9 ms per million rows, naive, UTC, New York and
  polars; ceiling 25.
- Four findings, V2-01 (S3), V2-02 (S3), V2-03 and V2-04 (S4): §13.

**What verifier 2 did not reach:** it did not re-run the whole facade suite (the pin file,
the four re-seated files and its probes only); it did not record Spark on the Lord Howe and
Kolkata transition frames (Spark was recorded on the first verifier's catalogue only).

**After the verified head:** §13 (follow-up 3) is tests and documents only, written after
`36382b62`. No line under `python/repark/src/` changed, so the verified product code is the
shipped product code. The pin of §13.1 and the text of §13 have not been independently
verified.

## 12. Fold 1 (2026-10-13), after an independent verify failed `c4efd79f`

The verify stage found two classes of cell where main answered and the unit raised (F-01, F-02),
an aware-column cost four times the ceiling (F-03), a categorical still carried (F-04), a silent value
change under declared `bigint` and `string` (F-05), and four record gaps (F-06 to F-09). Each
of F-01, F-02, F-04 and F-05 was reproduced on `c4efd79f` before any change. The rulings below
are the orchestrator's, provisional, 2026-10-13; the owner sees each in the pull request.
This section supersedes §2's kernel and seat, C-013's numbers and §6's table; the pin file
is 244 tests and the oracle 90 cells.

### 12.1 Ruling 1: the floor is on the ticks (C-017)

`pyarrow.compute.floor_temporal` on a zoned column converts to local time and back. It raised
`Local time is ambiguous` for any instant in a repeated hour (New York, London, Lord Howe and
every zone with a backward transition) under all three schemas, pandas and polars, and it
wrapped int64 for the lowest 807 nanoseconds, where the safe cast then refused. Main answered
all of these. This amends ruling F-3 of the work order, which named `floor_temporal`.

The step now casts the column to its int64 ticks, divides by 1000 (Arrow truncates), subtracts
one where the ticks are below the quotient times 1000, and views the result as `timestamp[us]`
with the zone label re-attached. Whole-column Arrow kernels, nulls stay null, no row loop. No
intermediate overflows: the product's magnitude never exceeds the ticks', and the lowest
quotient minus one is far inside int64. Checked at `-2**63`, `-2**63 + 1`, `-2**63 + 808`,
`-1001`, `-1000`, `-999`, `-1`, `0`, `1`, `999`, `1000`, `2**63 - 1` and NULL against Python's
floor division.

### 12.2 Ruling 2: after the dictionary decode (C-018)

One call, placed where the column is finally a nanosecond timestamp: after the dictionary
decode, the infinity refusal and the decimal envelope, none of which a timestamp column
enters. This amends ruling F-1 ("first step"). Spark's default path infers a struct for an
**aware** categorical, so only the naive categorical has a Spark cell; the aware one is pinned
to the plain aware column's answer.

### 12.3 Ruling 3: a declared non-timestamp type keeps main's answer (C-019)

The floor applies when no type is declared or the declared Arrow type is a timestamp
(`timestamp`, `timestamp_ntz`). Under any other declared type the column takes main's path
unchanged. The declared type is resolved at the point main resolved it, so no other step is
re-ordered. Main's cells, the same in both sessions: `c bigint` the nanosecond integer
(`1767323045123456789`, `[-1, -1001, -999999999999]`); `c string` nine digits
(`2026-01-02 03:04:05.123456789`, with `Z` or `-0500` for an aware column); `c date` the day.

### 12.4 Ruling 4: the true floor where Spark is lossy (C-023)

Spark's own conversion loses the instant in three places. The unit keeps `floor(ns / 1000)`
of the instant, which is also main's answer under `c timestamp`. No pin compares these cells
to Spark; they are recorded in the oracle file as Spark's answers and disclosed here and in
the parity row.

Corrected by follow-up 3 (§13.2): the first table named one pass per schema, which was true
of one frame only.

In the driver machine's repeated hour Spark stores an aware instant up to one hour early or
late. Which pass it moves depends on the **other values in the column** and on the schema, as
well as on the driver zone. Two frames, both New York-aware, both recorded live on Spark
4.1.2 with driver zone EST/EDT, in both sessions:

| Frame | Instant | Floor, the unit's answer | Spark, no schema | Spark, `c timestamp` |
|---|---|---|---|---|
| the oracle's `pd_ny_ns_transitions` (5 rows) | `05:30:00.000000001Z`, first pass | `1793511000000000` | `1793514600000000`, one hour late | right |
| the same | `06:30:00.000000001Z`, second pass | `1793514600000000` | right | `1793511000000000`, one hour early |
| the second verify's frame (8 rows and a NULL; a UTC-aware twin answers the same) | `05:30:00.000000001Z`, first pass | `1793511000000000` | right | right |
| the same | `06:30:00.000000001Z`, second pass | `1793514600000000` | `1793511000000000`, one hour early | `1793511000000000`, one hour early |
| the same | `06:00:00.000000001Z`, second pass | `1793512800000000` | `1793509200000000`, one hour early | `1793509200000000`, one hour early |

Dates are 2026-11-01. So no rule of the form "Spark loses the first pass" holds for every
frame; the class is "an aware instant in the driver zone's repeated hour". The other lossy
cells:

| Cells | Floor, the unit's answer | Spark |
|---|---|---|
| UTC-aware int64 extremes (`pd.Timestamp.min`, `.max`), no schema and `c timestamp` | `-9223372036854776`, `9223372036854775` | `9223368674854775`, `9223368436854775`: wrapped and an hour off |
| naive int64 extremes | the same floor, `timestamp[us]` | raises (`OverflowError`, `OutOfBoundsDatetime`, or `nonexistent time`) |
| `1883-11-18 17:00:00.000000001Z`, local mean time (both verify stages) | `-2717650800000000` | `-2717650562000000`, 238 seconds off |

### 12.5 Ruling 5: mutants (C-020)

`test_declared_timestamp_over_an_aware_microsecond_column_answers_utc` now runs over New
York, Asia/Tokyo and `+05:30`, each recorded on Spark. It is green on the fold and fails 4
times on the verifier's survivor (the relabel applied only to New York); 8 transition tests
fail too. The full table is in `evidence/fold-1.md`: 25 variants, the executor's nine and the
verifier's thirteen re-expressed on the tick kernel plus six of the fold's own (the old
kernel, the step before the decode, the step under every declared type, the step skipped
under a declared naive timestamp, a sign test in place of the remainder test, a floor on the
local wall). 24 are killed. The two the verifier calls equivalent:

- `V-o10` (an unsafe cast after `floor_temporal`): no counterpart, the tick kernel has no narrowing cast left.
- `V-o11` (the relabel also recasts a UTC column): equivalent, same type and values, only object identity differs.

### 12.6 The catalogue (C-021)

Main answered and the fold raises: 0 of 1388 (87 at `c4efd79f`: 28 repeated-hour cells and 59
at the lowest 807 nanoseconds). No-nanosecond cells that differ from main: 12 of 520, all the
named move. Nanosecond cells under `c bigint`, `c string`, `c date`: 0 of 204 differ (132 at
`c4efd79f`). The whole facade suite on the fold: 16,727 passed, 0 failed (the
`test_casesens_1` flake passed in this run).

### 12.7 Ruling 7: cost (C-022)

Five interleaved sets, main then head. No nanosecond column: 4.671 ms against 4.721 ms, ratio
1.011. One nanosecond column, head minus main per million rows: naive 16.13 to 16.71 ms, UTC
15.45 to 16.61 ms, New York 14.53 to 15.80 ms. The ceiling is 25 ms. Before the fold the aware
columns cost about 100 and 113 ms.

### 12.8 Ruling 6a: every surface of the named move (C-023)

The move of C-012 (a declared `c timestamp` over an aware microsecond column whose zone is
not UTC) is visible on: the Arrow type of `to_arrow` (`timestamp[us, tz=UTC]`, was the source
zone), so the Arrow schema string and the IPC bytes; the `toPandas()` dtype
(`datetime64[us, UTC]`, was `datetime64[us, America/New_York]`) and the printed offset of its
values; and the `show()` text (`2026-01-02 03:04:05.123456+00:00`, was
`2026-01-01 22:04:05.123456-05:00`). Not changed: the instant, `df.schema`, `dtypes`,
`collect()`, plain and extended `explain`. The 12 catalogue cells: pandas New York, pandas
`+05:30`, `ArrowDtype` Asia/Tokyo, `ArrowDtype` `+05:30`, polars New York, chunked New York,
in both sessions.

### 12.9 Ruling 6b: appends (C-023)

A pandas or polars nanosecond frame appended to a `timestamp_ns` column now stores
microsecond precision. Measured on a v3 table `(ts timestamp_ns, tz timestamptz_ns)` through
`writeTo().append()`, `insertInto`, `INSERT … SELECT` from a temp view and `saveAsTable` in
append mode, pandas and polars, two sessions: 16 of 16 cells store
`[1767323045123456000, -1000, -1000, -2000, -1000000000000]` with no error and no warning,
where main stored all nine digits in 16 of 16. Keeping the digits is the create-door order's
session setting.

**Release note (1.5.6), replacing §10's:** `createDataFrame` now turns pandas and polars
nanosecond timestamps into microseconds, as Spark does. A pandas or polars nanosecond frame
appended to a `timestamp_ns` column now stores microsecond precision.

### 12.10 Gates of the fold

`make py-lint py-format-check check-lib-py check-python-conventions check-docstring-presence`,
`make check-ledgers check-ledger-grammar check-docs-links check-docs-compaction spell-check`,
`check_map_md.sh --base 8c51fbc4`, `sync_map_md.py --check`: each 0. The pin file: 244
passed, the polars pins among them. The six files of the order's §5 and the
`-k "pandas or polars or createDataFrame or interchange"` selection: 0 failed. The comment
gate over the branch: 0 hits.

### 12.11 Observed in the fold, not acted on

- Spark's default path infers `struct<__module__: string>` for an aware categorical and for
  `ArrowDtype` columns; only its Arrow path answers them.
- The 231 wall-door tests no longer pass through `createDataFrame` (the re-seat of §4), so
  the pandas `ArrowDtype` coverage of nested, run-end and dictionary layouts they gave by
  accident is gone; the verify stage compared all 540 seat calls and found no weakening.

## 13. Follow-up 3 (2026-10-13), after the second verify passed `36382b62`

Tests and documents only. No product change.

### 13.1 V2-01: the pin for the surviving mutant (C-020)

The second verifier's mutant `R7h` keeps the floor only under no schema and a declared
`timestamp_ntz`, skipping it under a declared zoned `timestamp`. It passed all 244 pins and
is not equivalent: 12 catalogue answers become main's errors again. The extremes test declared
`c timestamp` only over the UTC-aware source, where main's row loop already answered.

`test_the_extremes_under_a_declared_timestamp_are_floored_first` declares `c timestamp` over
the naive int64 extremes (pandas and polars), the New York-aware extremes (pandas and polars)
and the polars int64-minimum column, in both sessions, and expects head's answers:
`timestamp[us, tz=UTC]`; `[-9223372036854776, 9223372036854775]` for an aware column and for a
naive one in a UTC session; `[-9223354274854776, 9223386436854775]` for a naive one in a New
York session, which is the floored wall read in New York (local mean time, `-04:56:02`, in
1677 and `-04:00` in 2262), checked against `zoneinfo`. The pin file is 254 tests: 254 passed
on head; with `R7h` applied 8 of the 10 new tests fail and no other; restored, 254 passed. The
two that pass on the mutant are the naive extremes in a UTC session, where main answered too.

### 13.2 V2-03: the repeated-hour disclosure (C-023)

§12.4's table and row R-019 are rewritten. The earlier text said Spark stores the first pass
an hour late under no schema and the second an hour early under `c timestamp`; that was one
frame's result. On the second verifier's frame Spark is right for the first pass and an hour
early for the second under both schemas.

### 13.3 V2-04: declared `c timestamp` where main raised (C-006)

C-006 holds for its 14 matrix cells. Row R-019 said a declared `c timestamp` over nanoseconds
"answers what main answered"; it now says "where main answered". Where main raised, head
answers the floor. The second verifier's 12 catalogue cells:

| Frame | Sessions | Main | Head |
|---|---|---|---|
| New York-aware lowest value (`-9223372036854775807`) | UTC and New York, default and `TIMESTAMP_NTZ` default (4) | `OutOfBoundsDatetime` | `-9223372036854776` |
| polars int64 minimum | UTC and New York, default and `TIMESTAMP_NTZ` default (4) | `NaTType does not support astimezone` / `utcoffset` | `-9223372036854776`, read in the session zone when naive |
| naive int64 maximum; naive extremes with the epoch; `pd.Timestamp.min` and `.max`; polars naive extremes | New York, default (4) | `OverflowError: Python int too large to convert to C long` | the floored wall read in New York |

Its wider probe found more cells of the same class and no other class.

### 13.4 What stays open

- **V2-02: rows are still read in Python under a declared `c timestamp`** for a naive
  nanosecond column, and for every nanosecond column in a session whose default is
  `TIMESTAMP_NTZ`, as on main: the pre-existing `to_pylist` loops in
  `_localize_naive_timestamp_column`. The floor step reads no row. Not changed by this unit;
  the second verifier measured about 4.7 s per million rows there on head and more on main.
- The Lord Howe and Kolkata transition frames have no Spark recording.
- §2 still describes the first kernel and seat; §12 supersedes it.
