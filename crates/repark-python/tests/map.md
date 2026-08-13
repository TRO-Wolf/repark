# map — crates/repark-python/tests

## Purpose

Rust integration tests for the `repark-python` cdylib. They boot an embedded Python interpreter (the
pyo3 `auto-initialize` **dev**-dependency) and drive the pyclasses directly — no built wheel required.

## Contents

- `bindings.rs` — builds a `PyReparkSession` (constructor takes `Python` for GIL-detach on catalog
  registration; process-wide shared Tokio runtime is unit-tested in `session.rs`). **Audit SAF-006
  constructor contract** (`session_constructs_with_builder_knobs`): `memory_limit_gb=0` opts out of
  the bounded pool and still builds, while `batch_size=0` and `target_partitions=0` each **refuse**
  with `Error::Config` (they were previously silently treated as "unset"). These are ENGINE knobs —
  Spark's `spark.sql.execution.arrow.maxRecordsPerBatch <= 0` "no limit" sentinel is translated to
  `None` facade-side (`python/repark/src/repark/session.py` — lands PR-5), so a legal PySpark
  program never reaches this refusal; only a direct `_native.PyReparkSession(...)` call trips it. Both refusals
  are additionally pinned from Python in `python/repark/tests/test_session_config_knobs.py`
  (**that suite lands phase-3 PR-5**; not in the tree yet). Also runs a `sql`
  round-trip, asserts the **streaming** Arrow PyCapsule (`__arrow_c_stream__`) exports a consumable
  stream with correct int64/utf8 values (`arrow_c_stream_exports_a_consumable_stream_with_correct_values`)
  and — value AND Arrow type end-to-end across the streaming FFI export over Int64/Decimal128/Utf8
  columns (`arrow_c_stream_streams_values_and_types_end_to_end`; **U2:** `1.5` is
  decimal128(2,1)); and — end-to-end **laziness** —
  `arrow_c_stream_defers_execution_and_does_not_collect_up_front` (through `session.sql` on an
  erroring query: the export returns a capsule WITHOUT materializing, so the deferred CAST error only
  surfaces on a full drain — a collect-then-wrap dunder would drain + raise at export instead, F-BR-4;
  asserts return-vs-raise, NOT batch ordering, per F-BR-5), and a `libpython_links` smoke. (The
  reader-level laziness/multi-batch/error pins and the counting-source end-to-end laziness pin
  `arrow_c_stream_export_is_lazy_and_does_not_materialize_up_front` live as `dataframe.rs`
  `#[cfg(test)]` units.)
  `show(py, n)` takes a row limit
  (engine-side). Also drives the `PyDataFrame` transform surface directly:
  `with_column` (a `PyColumn` operator expression), `filter`/`filter_sql`, `sort` (desc + nulls
  ordering), and `join_on_names` (key-merge). **G4b** adds the semi-family trio over the shared
  `semi_family_batch` helper (left keys 1/2/NULL against right keys 1/NULL, so match, no-match and
  the `NULL = NULL is unknown` arm are all live in one fixture):
  `join_on_names_left_semi_keeps_matching_left_rows_only`,
  `join_on_names_left_anti_keeps_unmatched_left_rows_including_null_keys` (the exact complement,
  so neither can pass vacuously) and `join_on_names_semi_family_never_merges_a_key_column`
  (the no-key-merge invariant stated against the inner-join baseline, over every accepted
  spelling). WG2 date/window bindings are covered too (via the
  `int32_column` helper that reads an `Int32Array`): `date_function_year_extracts_the_calendar_year`
  (`PyColumn::column("d").year()` → Int32 `2024`), `row_number_over_window_numbers_rows_in_order`
  (`PyColumn::row_number().over(...)` numbers rows by `ORDER BY v ASC`, result is Int32 for Spark
  parity), and `over_rejects_a_non_window_column` (`over()` on a plain column is an error, not a
  silent no-op). **r20 G2:** `over(...)` takes optional frame units/start/end (`None` = default
  frame). T1b TA bindings (via the `float64_column` helper): `ta_window_ema_over_matches_the_kernel`
  (`PyColumn::ta_window("ta_ema", [close, lit(3)]).over(ORDER BY ts)` is `to_bits`-identical to the
  `repark_ta::ema` kernel) and `ta_window_rejects_an_unknown_function` (an unknown TA name errors).
  **Group E** set/aggregate bindings (via the `string_column` helper + a `lit_i64` builder):
  `aggregate_group_by_sum_names_group_first_then_agg` (group column leads, then `sum(x)`),
  `aggregate_count_star_counts_rows_count_col_skips_nulls` (`count(*)` via a literal-1 argument
  counts every row, `count(col)` skips the NULL — the load-bearing count divergence),
  `union_positional_keeps_left_names_by_name_resolves` (positional union keeps left names + is UNION
  ALL; `by_name=true` pairs columns by name despite reversed order),
  `distinct_dedups_and_distinct_on_keeps_one_per_key`, and
  `with_column_renamed_renames_present_and_noops_absent`.
  WG2 config mapping: `config_driven_memory_catalog_registers_through_the_constructor` passes a
  source-publish-job-shaped `spark.sql.catalog.glue_alt.*` block (`type=memory`, AWS-free) as the
  constructor's `config` dict → the catalog registers → namespace (`create_namespace(py, cat, ns,
  location=None)` — the optional `location` is `None` here; the memory catalog's temp fallback
  applies) + CTAS + `table_exists` succeed, and a malformed block (`type=hive`) raises at
  construction naming the key.
  (The full Python-facade surface — `functions`, `types`, `Column`, the DataFrame
  ops, `Window`/`row_number`, and the date functions — is tested in
  `python/repark/tests/test_columns.py` and `python/repark/tests/test_functions_dates.py`, which
  **land phase-3 PR-5**; this PR builds no wheel and claims no facade coverage.)
  SAF-007: the `PyColumn` constructor/operator call sites `.expect(...)` their results — those
  methods now return `PyResult<Self>` because every `#[pymethods]` body routes through the shared
  panic fence (`src/fence.rs`); the fence's own behavior pins (panic → `PySparkException`, FFI-poll
  panic → clean Arrow error, both mutation-proven) live as `#[cfg(test)]` units in `fence.rs`,
  `session.rs`, and `dataframe.rs`.

## Pointers

- Up: [../map.md](../map.md)
- Design: [docs/design/python-facade.md](../../../docs/design/python-facade.md) §9 PR-3;
  ledger [p3c-binding-ledger.md](../../../docs/history/port-v2/p3c-binding-ledger.md)

## Debug

First checks: `cargo test -p repark-python` (NOT `--all-features` — that enables `extension-module` and
breaks linking the standalone test binary). Escalate to: [../map.md#debug](../map.md).

- **Neutral-fixture scrub (2026-08-10, hardening-prep):** an owner-approved, forward-only,
  comment-and-fixture-only pass moved this directory's doc text and example literals to
  neutral placeholders — the upstream job the acceptance shape mirrors is named generically
  ("the source publish job"), and example table/view/entity names are placeholders carrying no
  domain vocabulary. Outcome-neutral: every renamed fixture moved together with the assertions
  that read it. Sites here: `bindings.rs` and the WG2 config-mapping entry
  above (doc text only; no fixture in this directory changed).
