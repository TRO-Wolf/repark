# Charter ledger — DF-STREAM-BATCH-1 · the streaming-named DataFrame surface on a batch frame

**Date:** 2026-09-14 · **Branch:** `feat/df-stream-batch-1` · **Base:** `4d6b1ab0`
(docs(roadmap): PySpark functions and transformations parity moves into 1.5)
· **Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DF-STREAM-1` filed DECLARED; `DF-DECL-rdd`, `DF-DECL-pandas_api`,
`DF-DECL-plot` filed DECLARED.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The 1.5 PySpark-parity campaign requires every public name on the PySpark
4.1.2 DataFrame surface to either answer Spark or carry a dated declared refusal with
Spark's own error class. Every repark DataFrame is a batch DataFrame, so Spark's recorded
batch answers are the parity target for `writeStream`, `withWatermark` /
`with_watermark`, and `dropDuplicatesWithinWatermark` /
`drop_duplicates_within_watermark`; `rdd`, `pandas_api`, and `plot` need layers repark
does not have (the JVM RDD, pandas-on-Spark, a plotting backend) and become declared
`NOT_IMPLEMENTED` refusals in Spark Connect's refusal shape.

**Not in this unit:** Structured Streaming itself (no `DataStreamReader`/`DataStreamWriter`);
the `withWatermark_nested_col` Spark `INTERNAL_ERROR` on `"s.t"` (R-3 — out of scope,
repark returns `self`); the deferred `plot` accessor port over `toPandas` (DF-DECL-plot).

## PROPOSITION LEDGER — DF-STREAM-BATCH-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `DataFrame.writeStream` is a property that raises `AnalysisException` with errorClass `WRITE_STREAM_NOT_ALLOWED`, message ``[WRITE_STREAM_NOT_ALLOWED] `writeStream` can be called only on streaming Dataset/DataFrame. SQLSTATE: 42601``, at attribute access (R-1); `isStreaming`/`is_streaming` stay `False`. | `test_df_stream_batch_1.py::test_write_stream_refuses_with_spark_error_class`, cell `writeStream_batch`. | **PROVEN** | Red on the base: `writeStream` sat in `__getattr__`'s `_oos` table and raised `UnsupportedOperationException`, not `AnalysisException`. Green after: bound as a `property` in `streaming_batch.py`, error carries the class via the `_integral.py` `_spark_error_class` attach pattern plus `getErrorClass`/`getCondition`/`getMessageParameters`/`getSqlState` (42601) — repark's native `AnalysisException` takes no kwargs, so the attached-metadata precedent is the only door. `isStreaming`/`is_streaming` asserted `False`. pins: df-stream-batch-1/C-001 |
| C-002 | `withWatermark(eventTime, delayThreshold)` (both spellings) validates in Spark's order — non-str args raise `PySparkTypeError` `NOT_STR` (R-1: the Connect arg-check shape, not classic's `CANNOT_CONVERT_COLUMN_INTO_BOOL`, which Spark's Column check produces by accident), unparsable delay raises `AnalysisException` `CANNOT_PARSE_INTERVAL` (`[CANNOT_PARSE_INTERVAL] Unable to parse 'banana'.`…), negative delay raises `IllegalArgumentException` `requirement failed: delay threshold (-1 minute) should not be negative.` echoing the input string — and then returns `self` unchanged (R-2: the batch planner eliminates the watermark node). | The four dedicated pins: `test_with_watermark_returns_the_same_batch_frame` (cells `withWatermark_batch`/`not_ts`/`bad_col`), `test_with_watermark_parses_spark_interval_forms` (cell `withWatermark_zero` + 11 forms), `test_with_watermark_rejects_non_str_arguments` (cell `withWatermark_delay_not_str` + Column arg), `test_with_watermark_rejects_unparsable_delay` (cell `withWatermark_bad_delay`), `test_with_watermark_rejects_negative_delay` (cell `withWatermark_negative`). | **PROVEN** | Red on the base: `withWatermark` was an `_oos` entry (`UnsupportedOperationException`); `with_watermark` raised `PySparkAttributeError`. Green after: a small interval parser in `streaming_batch.py` (optional `interval` prefix, optional sign, integer or decimal amounts, multi-unit groups over nanosecond…year) — `grep` found no existing `CANNOT_PARSE_INTERVAL`/`parse_interval` in the facade to reuse. Cells pinned: timestamp column, non-timestamp column, unresolvable name, and `"s.t"` (R-3 out-of-scope: Spark's `INTERNAL_ERROR` is JVM node-copy machinery; repark returns `self`, recorded as an out-of-scope observation) all return `self` with byte-identical columns/schema/rows against the recorded cells. pins: df-stream-batch-1/C-002 |
| C-003 | `dropDuplicatesWithinWatermark(subset=None)` (both spellings) validates subset shape first — non-list/tuple → `PySparkTypeError` `NOT_LIST_OR_TUPLE`, non-str member → `NOT_STR` — then column resolution (missing name → `AnalysisException` `_LEGACY_ERROR_TEMP_1201` `Cannot resolve column name "zz" among (k, v).`, case-insensitive matching the frame's existing resolution), and only then raises the batch refusal `AnalysisException` `_LEGACY_ERROR_TEMP_3102` whose first line is exactly `dropDuplicatesWithinWatermark is not supported with batch DataFrames/DataSets;`. It raises at the call (R-4: Spark appends a logical-plan dump repark does not have — the pin asserts the first line; registry DF-STREAM-1 records the tail difference). | `test_drop_duplicates_within_watermark_rejects_bad_subset` (cell `ddww_bad_subset`), `test_drop_duplicates_within_watermark_reports_missing_column` (cells `ddww_missing_col`, `ddww_order`, plus `["K", "zz"]` case-insensitivity), `test_drop_duplicates_within_watermark_refuses_on_batch` (cells `ddww_batch`, `ddww_batch_noargs`). | **PROVEN** | Red on the base: both spellings raised `PySparkAttributeError` (names absent). Green after: validation ordering proven by the pins — a `["k", "zz"]` subset hits `_LEGACY_ERROR_TEMP_1201` before the batch refusal. `PySparkTypeError`'s default params-dump str (`[NOT_LIST_OR_TUPLE] arg_name='subset', arg_type='str'`) did not match Spark's templated message, so the exact templated string is passed positionally with `errorClass`/`messageParameters` retained — byte-exact parity on the recorded cells. pins: df-stream-batch-1/C-003 |
| C-004 | `DataFrame.rdd` (property), `DataFrame.pandas_api(index_col=None)`, and `DataFrame.plot` (property) each raise `PySparkNotImplementedError` errorClass `NOT_IMPLEMENTED` with `{"feature": <name>}` and the exact str `[NOT_IMPLEMENTED] <name> is not implemented.`; a column named `rdd` does not shadow the property. | `test_rdd_property_raises_spark_not_implemented` (cell `connect_rdd_shape` + the `rdd`-column frame), `test_pandas_api_raises_spark_not_implemented`, `test_plot_property_raises_spark_not_implemented`. | **PROVEN** | Red on the base: `rdd` was an `_oos` entry (`UnsupportedOperationException`); `pandas_api`/`plot` raised `PySparkAttributeError`. Green after: the three names plus `pandas_api` ship in `DECLARED_MEMBERS`, bound on the class so `__getattr__` column access can never reach them — the `rdd`-named column still resolves via `df["rdd"]` while `df.rdd` answers the refusal. Oracle `rdd`/`plot` cells record the classic return types (`RDD`, `PySparkPlotAccessor`) and `connect_rdd_shape` records the Connect refusal repark matches; `pandas_api`'s recorded `PACKAGE_NOT_INSTALLED` is environment-specific — the ruling names the declared `NOT_IMPLEMENTED` refusal instead (registry DF-DECL-pandas_api says so). pins: df-stream-batch-1/C-004 |
| C-005 | Registry rows `DF-STREAM-1`, `DF-DECL-rdd`, `DF-DECL-pandas_api`, `DF-DECL-plot` sit at the end of §5 with pins; the fixture copy is byte-identical; every touched `map.md` is in lockstep; the staging ledger is listed in `task/ledgers/staging/map.md`. | The files. | **PROVEN** | `cmp` confirms the fixture is a byte-identical copy of `/tmp/oc-worker/run15b/oracle/facade_dataframe_streaming_declared_oracle.json`. Four rows added after `FA-4`, each naming its pin and recording the oracle cell ids; `python/repark/tests/map.md` carries the test + fixture row, `python/repark/src/repark/spark/dataframe/map.md` carries the `streaming_batch.py` row, `task/ledgers/staging/map.md` carries this ledger. pins: df-stream-batch-1/C-005 |
| C-006 | No regression: `python/repark/tests/test_dataframe*.py` and every test that greps the DataFrame attribute inventory (`dir(DataFrame)`, api freeze, `_dfcore_1_expected`) stay green, with the frozen surfaces updated for the eight chartered new public names. | The suites. | **PROVEN** | The frozen inventory gained exactly the eight names (`writeStream`, `withWatermark`, `with_watermark`, `dropDuplicatesWithinWatermark`, `drop_duplicates_within_watermark`, `rdd`, `pandas_api`, `plot`), the `withWatermark`/`with_watermark` and `dropDuplicatesWithinWatermark`/`drop_duplicates_within_watermark` alias pairs, and the `streaming_batch` module in both the core and package submodule sets — expected, since these are newly chartered public names. `test_facade_hygiene.py`'s OOS pin now expects `PySparkNotImplementedError` for `rdd` and `AnalysisException` for `writeStream`. Suites: `test_df_stream_batch_1.py` 12 passed; `test_dfcore_1_exports.py` + `test_dfcore_4b_exports.py` + `test_facade_hygiene.py` 28 passed; `test_dataframe_actions.py` + `test_dataframe_x3_census.py` 73 passed. pins: df-stream-batch-1/C-006 |
| C-007 | Critic round 1 (L-001..L-005) remediated: delay negativity is Spark's net `CalendarInterval` under `IntervalUtils.isNegative` (months, days, microseconds with daysPerMonth=31), empty `eventTime`/`delayThreshold` hit the `not s` gate as `NOT_STR`, `-0 seconds` accepts, the dropDuplicatesWithinWatermark edge shapes are pinned, and `df["rdd"]`/`df["plot"]`/`df["writeStream"]` plus the `hasattr` probes are pinned. Plus the example-coverage fix for the four enumerated names. | `test_with_watermark_net_interval_sign`, `test_with_watermark_rejects_empty_strings`, `test_drop_duplicates_within_watermark_edge_shapes`, `test_streaming_named_columns_and_attribute_probes`; `docs/examples/dataframe/batch_streaming_names.py`. | **PROVEN** | Red-first on bcaefa53: `test_with_watermark_net_interval_sign` FAILED (per-token minus refused `1 hour -30 minutes`, `1 hour - 30 minutes`, `1 hour-30 minutes`, `-1 hour 2 hours`, `8 days -1 week`, `2 months -1 month`, `-0 seconds`) and `test_with_watermark_rejects_empty_strings` FAILED (`""` returned `self`; `"ts",""` raised `CANNOT_PARSE_INTERVAL` instead of `NOT_STR`). After: every accept form returns `self`, `1 day -25 hours` refuses echoing the input, `""` raises `NOT_STR` with `arg_type` `str`. Green-on-first-run: `test_drop_duplicates_within_watermark_edge_shapes` and `test_streaming_named_columns_and_attribute_probes` pin today's answers as mutation guards — tuple/empty/duplicate/casefold subsets, `["s.a"]` → `_LEGACY_ERROR_TEMP_1201`, `[1]`/`[None]` → `NOT_STR arg_name=subset`, getitem Columns, `hasattr(df,"plot")` raising `PySparkNotImplementedError` and `hasattr(df,"writeStream")` raising `AnalysisException` — the critic probed these as already-correct; the pins exist so a resolver/getattr regression cannot hide. `hasattr(df,"plot")` raising is an accepted behaviour change from the previous `ATTRIBUTE_NOT_SUPPORTED` (consequence of R-5; Spark classic returns an accessor, repark's declared refusal surfaces through the getter). Example coverage: `check_example_coverage.py` walked 934 names (930 → 934); the four enumerated names are covered by the new example, the inventory snapshot regenerated, and `test_ex_0_example_coverage.py` moved to `len(rows) == 934`. pins: df-stream-batch-1/C-007 |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

## Per-name decisions

| Name | Verdict | Reason |
|---|---|---|
| `writeStream` | declared refusal | Batch-only engine; raises Spark's own `WRITE_STREAM_NOT_ALLOWED` at attribute access (registry DF-STREAM-1 context, cell `writeStream_batch`). |
| `withWatermark` / `with_watermark` | implemented | Spark's batch answer IS the parity target: validate, then return `self` (R-2). |
| `dropDuplicatesWithinWatermark` / `drop_duplicates_within_watermark` | declared refusal (message tail) | Spark's class and first line; the appended plan dump is JVM logical-plan text repark does not have (R-4, registry DF-STREAM-1). |
| `rdd` | declared refusal | Needs the JVM RDD layer; Spark Connect's `NOT_IMPLEMENTED` refusal shape (registry DF-DECL-rdd). |
| `pandas_api` | declared refusal | Needs pandas-on-Spark; declared `NOT_IMPLEMENTED` per ruling over the oracle's environment-specific `PACKAGE_NOT_INSTALLED` (registry DF-DECL-pandas_api). |
| `plot` | declared refusal | Reachable in principle over `toPandas` + plotly — DECLARED, deferred (registry DF-DECL-plot). |

## Out-of-scope observations

- `withWatermark_nested_col`: Spark classic raises a JVM `INTERNAL_ERROR` (EventTimeWatermark
  node-copy machinery) on the nested-column name `"s.t"`; R-3 puts this out of scope — repark
  accepts it like any other unresolvable batch name and returns `self`. Recorded for the
  registry sweep, no row filed (not a reachable divergence class here).
- `pandas_api`'s recorded oracle answer is `PACKAGE_NOT_INSTALLED` (pandas absent in the
  oracle environment), not the name's real return; the ruling's declared `NOT_IMPLEMENTED`
  refusal is what the registry row and pin hold.
- **R-6 UNMEASURED (2026-09-14, kept as answered today; each queued for the next oracle
  round):** `INTERVAL '1' MINUTE` → `CANNOT_PARSE_INTERVAL` (ANSI SQL-literal spelling;
  whether `fromIntervalString` falls back to the SQL parser was not established);
  `interval1 minute` → `CANNOT_PARSE_INTERVAL` (repark requires `interval\s+`; Spark's
  4.1 regex-vs-`startsWith` split was not established); `1 nanosecond` → accept
  (`nanosStr` exists in the 4.1.2 ParseState, likely accept, unmeasured); fractional
  month/year/week/day amounts (e.g. `1.5 days`, `0.5 months`) → `CANNOT_PARSE_INTERVAL`
  (Spark's `stringToInterval` rule for a fraction on the months/days fields was not
  established, so the card's sanctioned fallback is the parse refusal).

## What changed

| File | Change |
|---|---|
| `python/repark/src/repark/spark/dataframe/streaming_batch.py` | New, ~230 lines. The interval parser, the `_raise_analysis` structured-error helper (reusing `_integral.py`'s attach functions), the three refusal properties + `pandas_api` in `DECLARED_MEMBERS`, `with_watermark`, and `drop_duplicates_within_watermark`. Critic round 1: negativity moved to Spark's net `CalendarInterval` (`isNegative` with daysPerMonth=31), the empty-string `not s` gate joined the `NOT_STR` checks, and fractional months/days refuse `CANNOT_PARSE_INTERVAL` (UNMEASURED). |
| `python/repark/src/repark/spark/dataframe/core.py` | Net-zero at the exact 4044 baseline: `streaming_batch` folded into the existing `cache_handle` import line; three `_oos` entries removed; four class bindings added. |
| `python/repark/tests/test_df_stream_batch_1.py` | New. 16 cell-driven pins (critic round 1 added the net-interval sign, empty-string, ddww edge, and getattr probes). |
| `docs/examples/dataframe/batch_streaming_names.py` | New. Covers the four enumerated streaming names for `check_example_coverage.py`. |
| `docs/examples/dataframe/map.md`, `docs/examples/inventory.txt`, `python/repark-parity/tests/test_ex_0_example_coverage.py`, `python/repark-parity/tests/map.md` | Example-coverage lockstep: inventory snapshot regenerated, surface count 930 → 934. |
| `python/repark/tests/facade_dataframe_streaming_declared_oracle.json` | Byte-identical copy of the oracle fixture. |
| `python/repark/tests/_dfcore_1_expected.py` | Frozen surfaces gain the eight names, two alias pairs, and the `streaming_batch` submodule (C-006). |
| `python/repark/tests/test_dfcore_1_exports.py` | One-line docstring notes the new dir/membership delta. |
| `python/repark/tests/test_facade_hygiene.py` | OOS pin updated: `rdd` → `PySparkNotImplementedError`, `writeStream` → `AnalysisException`. |
| `docs/spark-sql-iceberg-parity.md` | Rows `DF-STREAM-1`, `DF-DECL-rdd`, `DF-DECL-pandas_api`, `DF-DECL-plot` at the end of §5. |
| `python/repark/tests/map.md`, `python/repark/src/repark/spark/dataframe/map.md`, `task/ledgers/staging/map.md` | Lockstep rows. |
| `STATUS.md`, `briefs/next-sequence.md`, `Cargo.toml`, `Cargo.lock`, `pyproject.toml`, `uv.lock`, `.github/` | Untouched. |
