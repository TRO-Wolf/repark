# map — repark/spark/session/

## Purpose

Package split of monolithic `session.py` (r26 T1 MOVE-ONLY). Re-homed under
`repark.spark.session` (Q1, 2026-08-14).

## Contents

- `_coerce.py` — **PYC-2:** `range_bound_as_int` and `sql_clause_end_after`, lifted
  out of nested defs in `session_core.py` so that file can ratchet its exact
  `check_lib_py` exception baseline.
- `_funcs.py` — compatibility router for the pre-split free-function namespace. It imports each
  responsibility module, binds the measured cross-module edges, and re-exports every prior name.
- `session_configuration.py` — SQLConf defaults, DataFusion configuration validation and
  forwarding, and display-style normalization. `_SQLCONF_DEFAULTS` keeps the declared nested-dict
  inference default and the session-timezone and timestamp-type defaults. Runtime temp-directory
  SET remains refused because the Rust builder already applied it. DataFusion `SET` values route
  through the shared Spark string-literal helper.
- `catalog_resolution.py` — catalog selection, aliasing, namespace defaults, and table-name
  resolution. Its relation-parser edge is bound by the compatibility router to avoid a cycle.
- `session_state.py` — active-session context state and the drop-in warning lifecycle.
- `reader_support.py` — reader option sets and integer bounds, JDBC integer options, reader paths,
  JSON empty-input checks, CSV promotion, and reader schema-field normalization.
- `create_dataframe_values.py` — scalar normalization, SQL literals, schema parsing, and Spark-type
  to SQL-type mapping. String cells route through the shared Spark literal helper. Non-binary
  collated strings still refuse and `NullType` still maps to VOID.
- `create_dataframe_schema.py` — DDL parsing, pandas and Polars null witnesses, and schema-name
  permutation.
- `create_dataframe_rows.py` — named-row binding, pandas and Polars row extraction, VALUES and
  Arrow memtable materialization, and scratch-view cleanup. Materialized frames retain their
  `_source_view_name` tag for `declareSorted`.
- `create_dataframe_inference.py` — nested Arrow inference, struct merging, decimal-envelope
  checks, and SQL-to-Arrow type conversion. VOID and NULL still map to `pa.null()`.
- `create_dataframe_arrow.py` — pandas and Polars Arrow-column normalization, timestamp
  localization, and decimal-column validation.
- `create_dataframe_tuples.py` — tuple-to-Arrow conversion and scalar/list merge refusal rules,
  including dense FixedSizeList and sparse ML-vector reshape.
- `sql_udf_parsing.py` — SQL lexical scanning, comment-safe select-list splitting, and simple UDF
  call parsing.
- `sql_udf_discovery.py` — registry-UDF discovery and trailing-clause peeling.
- `sql_udf_residual.py` — WHERE-residual base-projection planning.
- `sql_udf_materialization.py` — expression-UDF materialization, ORDER BY alias planning, and
  public UDF error cleanup.
- `sql_udf_rewrite.py` — the select-list Python-UDF rewrite assembly.
- `sql_relations.py` — SQL statement-shape patterns, string/comment masking, CTE discovery,
  relation scanning, and multipart table-identifier parsing.
- `builder_conf.py` — SparkContext, RuntimeConfig.
  **G15:** `RuntimeConfig.set` refuses session keys containing `collation` (silent-ignore path).
  **S-1 R1:** RuntimeConfig docs — `datafusion.runtime.memory_limit` swaps a new
  `FairSpillPool` (same pool type as the builder; one truth, not two knobs).
  **S-1 R2:** `datafusion.runtime.temp_directory` is build-time; runtime SET refuses
  and names `TMPDIR`.
  **S-1 R3:** RAM-relative FairSpillPool default (cap 8 GiB); documents the
  `sort_spill_reservation_bytes × target_partitions` floor. `_funcs.py` one-truth
  strings flipped in lockstep.
- `session_core.py` — ReparkSession (sql/catalog methods stay here).
  **G15:** `Builder._set_config_entry` refuses collation `SQLConf` keys (silent-ignore path).
  getOrCreate reuse fold also calls `refuse_collation_session_key` so a planted
  `_config` key cannot silently store (SEC-003).
  **F-3 (2026-08-17):** `probe`, the temp-view existence closure inside `resolve_table_name`,
  gained a docstring; docstring-only, and the file stayed at its then-current source-size ceiling.
  **PYC-2 (2026-08-22):** `range` / SQL-clause helpers move to `_coerce.py`; `probe`
  lifts to `_temp_view_home_ref` (it sat under `if`, so the gate never counted it).
- `reader.py` — DataFrameReader (**`smartCsv` method body** — Q7 MOVE MAP destination).
  **B4 (round 4 salvage):** `sep` / `delimiter` resolved with the `is not None`
  idiom (empty does not fall through); refuse empty / multi-char / newline / CR /
  quote via `_require_single_char_delimiter` (L3 single-source). Auto-detect stays
  origin/main agreement-first in `_csv_smart.detect_delimiter`.
- `sql_udf.py` — UDFRegistration
- `create_dataframe.py` — region marker + SparkSession/ReParkSession aliases
- `catalog.py` — re-export binding region note (r27 T1 no-stub mark)
- `timestamp_type.py` — **Q10:** `spark.sql.timestampType` facade half (ONE key
  spelling, default `TIMESTAMP_LTZ`, parse refuses naming both legal tokens,
  builder whitespace normalize, `active_timestamp_type` / inference helpers).
  Engine resolves at `SparkExtension.configure`; runtime `conf.set` is
  store-only (ansi.enabled precedent).
- `session_time_zone.py` — the `spark.sql.session.timeZone` conf surface (H-1a): the ONE key
  spelling, the `UTC` default, `warn_runtime_session_time_zone_not_applied` (a runtime
  `conf.set`/`unset` of this build-time knob is accepted for drop-in, warned once **per process**,
  and NEITHER validated NOR stored, so `conf.get` never reports a zone the engine does not have),
  and `normalize_session_time_zone_config` (whitespace-only normalization of the builder value,
  matching the engine's own `trim` — the ENGINE remains the sole validator). Consumed by
  `_funcs.py` (`_SQLCONF_DEFAULTS` gains the key so `conf.get` reads it back), `builder_conf.py`
  (`RuntimeConfig.set` / `unset` accept-warn-don't-store) and `session_core.py` (the normalization
  call in `get_or_create`, and the key joining the engine-knob set the reuse path must not fold
  into the live session's conf). Its module docstring also carries the **user-visible statement of
  what the zone reaches** (updated 2026-08-13, TZ-4 PR-2): extraction over an INSTANT-typed
  TIMESTAMP honors it; zoneless LTZ inputs localize in this zone; NTZ stays naive;
  `CAST(ts AS DATE)` / `to_date` honor it (TZ-8); `datediff` rides CAST;
  `last_day` / `date_add` over TIMESTAMP stay residual. Helpers:
  `active_session_time_zone`, `localize_naive_datetime_to_utc`,
  `collect_timestamp_as_session_wall`. That paragraph ships in the
  wheel, so it is a lockstep obligation whenever the engine's coverage changes.
- `__init__.py` — frozen public re-exports and shared facade-class binding for every extracted
  free-function module.

## MOVE MAP (Q7)

| Symbol | Pre-split | Post-split |
|---|---|---|
| `DataFrameReader.smartCsv` | `session.py` | `session/reader.py` |

- r26 morning: T2 smartCsv samplingRows body re-seated into reader.py

- octo C1: smartCsv reads samplingRows from option map

- octo C2: samplingRows must be integral int > 0

## Pointers

- Up: [../map.md](../map.md)
- Engine half of the session-timezone knob: `crates/repark-core/src/session_time_zone.rs`

## Debug

| Symptom | First check |
|---|---|
| `spark.conf.set("spark.sql.session.timeZone", …)` warns and does nothing | Intended (H-1a): the zone is resolved once at session build. The call is accepted so a drop-in script (and PySpark's own `sql_conf` helper) still runs, warned once, and NOT stored — so `conf.get` keeps reporting the engine's real zone. Set it on the builder and build a new session. |
| the same `conf.set` is now completely SILENT | Expected: the disclosure is once per **process** (the OTH-010 `_warn_master_once` idiom), not once per session. A second session in the same interpreter gets a silent no-op. Recorded as divergence-registry row TZ-3. |
| a garbage zone passed to `conf.set` is not refused | Intended and knowingly laxer than PySpark (which raises `[INVALID_CONF_VALUE.TIME_ZONE]`): validation is the ENGINE's and happens once at build, so the runtime setter neither validates nor applies. The warning says so. |
| `spark.conf.get("spark.sql.session.timeZone")` returns `UTC` on a session that never set it | Intended: the default lives in `_SQLCONF_DEFAULTS`. It is `UTC`, not the host zone — a declared divergence from Spark's JVM-local default. |
| A second `getOrCreate` with a different zone warns and does not apply | Intended: the session zone joins the engine-knob set in `session_core.py`, so reuse never folds a zone the live engine session does not have into the facade conf. An INVALID zone on that path is not validated either (no session is built) — deliberate, D-A1, pinned by `test_getorcreate_reuse_with_an_invalid_zone_warns_and_does_not_raise`. |
| `conf.get` returns a trimmed zone though the builder value was padded | Intended: `normalize_session_time_zone_config` strips the value exactly as the engine does before parsing, so facade and engine report the same zone. Whitespace only — validity is still decided in the engine. |
| The zone is set but timestamp extraction did not move | Expected in this unit — the conf surface landed without the extraction fix; the recorded rows in `python/repark/tests/test_session_timezone_parity.py` pin the current divergence honestly. |
**SQM round 7 (R7-1):** `resolve_table_name`'s temp-view arm no longer returns the BARE name. The
temp-view home probe in `session_core.py` (`_temp_view_home_ref`, PYC-2) calls native
`resolve_temp_view_home_ref` and returns the
view's HOME segments (`_funcs.py` parameter renamed `temp_view_exists` → `temp_view_home_ref`), so
`spark.table`, the free-SQL bare-name expander and every writer/reader path emit
`"datafusion"."public"."v"` for a session-local view instead of a bare reference the engine would
re-resolve against the live `datafusion.catalog.default_catalog`. `createDataFrame`'s scratch view
is named through `repark.spark._temp_views.scratch_view_name`.

**Group-1 confirmation (C-1).** Three `session_core.py` docstrings still said one-part temp views
"stay bare" after R7-1 changed that — `_expand_bare_table_names_in_sql` (the statement-shape list),
`_expand_from_join_table_refs_in_sql`, and `table`. MEASURED false:
`_expand_bare_table_names_in_sql("SELECT * FROM tv")` → `SELECT * FROM "datafusion"."public"."tv"`,
and `_sql_table_ref_resolved("tv", prefer_temp_view=True)` → `"datafusion"."public"."tv"`. All three
trued (prose only, line-neutral — the file sat exactly at its then-current ceiling); this paragraph was
already correct and is what they now agree with.
