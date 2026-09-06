# map — repark/spark/session/

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).
CC-2 close: restored base function docstrings hashed by `test_moved_symbol_bodies_match_the_integrated_baseline`.

## Purpose

Package split of the former monolithic `session.py`. One responsibility per module; `_funcs.py`
is the compatibility router that re-exports every prior name.

## Contents

| Path | Role |
|---|---|
| `_coerce.py` | `range_bound_as_int`, `sql_clause_end_after` — lifted so `session_core.py` can ratchet its exact `check_lib_py` exception baseline. |
| `_funcs.py` | Compatibility router: imports each responsibility module, binds cross-module edges, re-exports every prior name. |
| `session_configuration.py` | SQLConf defaults, DataFusion validation/forwarding, display-style normalization. `_SQLCONF_DEFAULTS` holds declared inference, session-timezone, and timestamp-type defaults. Runtime temp-directory `SET` stays refused (the Rust builder already applied it). DataFusion `SET` values route through the shared Spark string-literal helper. |
| `catalog_resolution.py` | Catalog selection, aliasing, namespace defaults, table-name resolution; relation-parser edge bound by the router to avoid a cycle. |
| `session_state.py` | Active-session context state and the drop-in warning lifecycle. |
| `reader_support.py` | Reader option sets and integer bounds, JDBC integer options, reader paths, JSON empty-input checks, CSV promotion (bigint/double/boolean/timestamp/date after nullValue and CSV inferSchema; timestamp only when a cell has `:`, date only when it does not). InferSchema CSV is Utf8-forced so CAST(str AS TIMESTAMP) sees the raw text: offset-bearing cells keep their instant, offset-free cells localize in the session zone. JSON also runs the leftover ntz recast; DataFusion infers Utf8 so it is a no-op. pins: nullability-2/C-006 |
| `create_dataframe_values.py` | Scalar normalization, SQL literals, schema parsing, Spark-type mapping. String cells use the shared literal helper; non-binary collated strings refuse; `NullType` maps to VOID. **FN-FIX-1:** `float('nan')` is kept and emitted as `CAST('NaN' AS DOUBLE)`. pins: fn-fix-1-registry-rows/C-002 |
| `create_dataframe_schema.py` | DDL parsing, pandas/Polars null witnesses, schema-name permutation. |
| `create_dataframe_rows.py` | Named-row binding, pandas/Polars extraction, VALUES and Arrow-memtable materialization, scratch-view cleanup. Materialized frames keep the `_source_view_name` tag for `declareSorted`. **FN-FIX-1:** Sparse integer/bool nan-fill stays SQL null; Sparse[object] NaN is DOUBLE. pins: fn-fix-1-registry-rows/C-002 |
| `create_dataframe_inference.py` | Nested Arrow inference, struct merging, decimal-envelope checks, SQL-to-Arrow types. VOID/NULL map to `pa.null()`. |
| `create_dataframe_arrow.py` | pandas/Polars Arrow-column normalization, timestamp localization, decimal validation. **FN-FIX-1:** object-dtype all-NaN keeps DOUBLE NaN. pins: fn-fix-1-registry-rows/C-002 |
| `create_dataframe_tuples.py` | Tuple-to-Arrow conversion, scalar/list merge refusals, dense FixedSizeList and sparse ML-vector reshape. **PERF-FACADE-CDF-1:** the duplicate-name refuse, the all-null CAST parse (keeps `DECIMAL(p, s)` parens intact while stripping the CAST wrapper paren) and the Arrow build wrap are module functions shared with the column-wise path, called by the tuple converter. |
| `create_dataframe_columns.py` | **PERF-FACADE-CDF-1:** column-wise census, inference and conversion. One `set(map(type, …))` census per column; single-kind scalar columns convert straight to Arrow; mixed/exotic columns normalize through the shared cell normalizer and build through the unchanged tuple converter. Explicit schemas dispatch to the legacy row-wise path. |
| `sql_udf_parsing.py` | SQL lexical scanning, comment-safe select-list splitting, simple UDF-call parsing. |
| `sql_udf_discovery.py` | Registry-UDF discovery, trailing-clause peeling. |
| `sql_udf_residual.py` | WHERE-residual base-projection planning. |
| `sql_udf_materialization.py` | Expression-UDF materialization, ORDER BY alias planning, UDF error cleanup. |
| `sql_udf_rewrite.py` | Select-list Python-UDF rewrite assembly. |
| `sql_relations.py` | Statement-shape patterns, string/comment masking, CTE discovery, relation scanning, multipart identifier parsing. |
| `builder_conf.py` | SparkContext, RuntimeConfig. `set` refuses session keys containing `collation` (silent-ignore path). `datafusion.runtime.memory_limit` swaps a new `FairSpillPool` (one truth, not two knobs); `datafusion.runtime.temp_directory` is build-time (runtime SET refuses, names `TMPDIR`); RAM-relative FairSpillPool default (cap 8 GiB) with the `sort_spill_reservation_bytes × target_partitions` floor. |
| `session_core.py` | ReparkSession (SQL/catalog methods). `Builder._set_config_entry` refuses collation keys; the getOrCreate reuse fold calls `refuse_collation_session_key` so a planted `_config` key cannot silently store. |
| `reader.py` | DataFrameReader incl. `smartCsv`. `sep`/`delimiter` resolve with the `is not None` idiom; empty/multi-char/newline/CR/quote values refuse via `_require_single_char_delimiter`; auto-detect stays origin/main agreement-first in `_csv_smart.detect_delimiter`. CSV `inferSchema` (and `nullValue`) promotes from the Utf8 scan then runs the leftover ntz recast (round 4). pins: nullability-2/C-006 |
| `sql_udf.py` | UDFRegistration. |
| `create_dataframe.py` | Region marker + `SparkSession`/`ReParkSession` aliases. |
| `catalog.py` | Re-export-binding region note (no-stub mark). |
| `timestamp_type.py` | `spark.sql.timestampType` facade half: one key spelling, default `TIMESTAMP_LTZ`, parse refuses naming both legal tokens, builder whitespace normalize, runtime `conf.set` is store-only. Engine resolves at `SparkExtension.configure`. |
| `session_time_zone.py` | `spark.sql.session.timeZone` facade half: one key spelling, `UTC` default, `warn_runtime_session_time_zone_not_applied` (runtime set/unset accepted for drop-in, warned once per process, neither validated nor stored), `normalize_session_time_zone_config` (whitespace-only, matching the engine's own trim — the engine stays the sole validator). The module docstring carries the user-visible statement of what the zone reaches; it ships in the wheel, so it is a lockstep obligation whenever engine coverage changes. |
| `__init__.py` | Frozen public re-exports and shared facade-class binding. |

## The column-wise createDataFrame path (PERF-FACADE-CDF-1)

`_create_dataframe_from_rows_inner` ends at one dispatcher,
`_arrow_table_from_raw_tuples`: explicit `StructType` / DDL schemas run the legacy row-wise
build (`_arrow_table_from_raw_tuples_legacy`, the moved pre-unit block, kept callable as the
pins' oracle); inferred schemas run the column-wise build. The old-vs-new bench pair and every
equality pin swap the rows-module dispatcher between those two in a `finally`, the way
PERF-FACADE-1 swapped the row converter.

Why the answers are identical by construction, not by reimplementation: every shared rule is
the same function object on both paths (the cell normalizer, the all-null witness, the tuple
converter for nested columns, the decimal-envelope validator, the three helpers tuples.py now
shares). The fast path skips work only where the census proves it a no-op — a `{int}` column
cannot carry a second merge kind, so the refusal walk is skipped; a pure-`None` column is
all-null, so normalization is skipped. Single-kind checks use `type()`, never `isinstance`,
because `bool` subclasses `int` and `datetime` subclasses `date`.

Two error-precedence facts, both multi-failure only (every single-failure input raises the
byte-identical error): the envelope scan reports the row-major-first violation across decimal
columns while slow columns validate inside their own build, and a slow column's inference error
raises in the build phase rather than the inference phase. The ledger names the pairs.

The tuple loop skips `_apply_permutation` when the permutation is the identity (hoisted
once, not per row) — the profiler charged the rebuild ~60 ms at 1e5. Only
namedtuple-plus-reorder takes the permuting arm.

pins: perf-facade-cdf-1/C-002, C-003, C-004

## Pointers

- Up: [../map.md](../map.md)
- Engine half of the session-timezone knob: `crates/repark-core/src/session_time_zone.rs`
- Temp-view home resolution: `session_core.py` `_temp_view_home_ref` calls native
  `resolve_temp_view_home_ref`; every product path emits the home-qualified spelling
  (`"datafusion"."public"."v"`) for a session-local view. `createDataFrame`'s scratch view is
  named through `repark.spark._temp_views.scratch_view_name`.

## Debug

| Symptom | First check |
|---|---|
| `spark.conf.set("spark.sql.session.timeZone", …)` warns and does nothing | Intended: the zone is resolved once at session build; the call is accepted so a drop-in script (and PySpark's own `sql_conf` helper) still runs. Set it on the builder and build a new session. |
| The same `conf.set` is completely SILENT | Expected: the disclosure fires once per process, not once per session (divergence-registry row TZ-3). |
| A garbage zone passed to `conf.set` is not refused | Intended: validation is the engine's and happens once at build; the runtime setter neither validates nor applies. |
| `spark.conf.get("spark.sql.session.timeZone")` returns `UTC` on a never-set session | Intended: the default is `UTC`, not the host zone — a declared divergence from Spark's JVM-local default. |
| A second `getOrCreate` with a different zone warns and does not apply | Intended: the session zone joins the engine-knob set, so reuse never folds a zone the live engine session does not have into the facade conf. |
| `conf.get` returns a trimmed zone though the builder value was padded | Intended: `normalize_session_time_zone_config` strips whitespace exactly as the engine does; validity stays the engine's. |
| The zone is set but timestamp extraction did not move | The conf surface landed without the extraction fix; the rows in `python/repark/tests/test_session_timezone_parity.py` pin the current divergence honestly. |
