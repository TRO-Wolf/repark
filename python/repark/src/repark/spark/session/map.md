# map — repark/spark/session/

## Purpose

Package split of monolithic `session.py` (r26 T1 MOVE-ONLY). Re-homed under
`repark.spark.session` (Q1, 2026-08-14).

## Contents

- `_funcs.py` — free functions (shared name binding for class modules); includes
  `createDataFrame` Arrow reshape for dense FixedSizeList / sparse ML vectors (mixed dense
  widths refuse loud — layout home `repark.ml.linalg`).
  `_SQLCONF_DEFAULTS` sets `spark.sql.pyspark.inferNestedDictAsStruct.enabled` to `"true"`
  (FA-4 owner flip, 2026-08-16 — declared divergence from PySpark's `false`; registry row
  in the divergence registry; `"false"` restores byte-identical PySpark inference).
  **G15:** `_data_type_to_sql_type` refuses a non-binary `StringType` (the silently-wrong-count
  path: collation was stripped to `STRING`).
  **S-1 R2:** `_apply_builder_datafusion_conf` skips `datafusion.runtime.temp_directory`
  (already applied at Rust `build()`; a runtime SET of it refuses and names `TMPDIR`).
  **F-3 (2026-08-17):** the last undocumented public name here, `int_size_to_ok` inside
  `_supported_array_typecodes`, gained a docstring; docstring-only, ceiling unmoved.
  **SE-1 PR-B (2026-08-17):** both `__repark_cdf_*` materializers
  (`_materialize_values_as_memtable_frame`, `_materialize_arrow_as_memtable_frame`) stamp
  `frame._source_view_name = view_name` on the frame they hand back. That tag is the only
  thing `DataFrame.declareSorted` will declare against, and no `_spawn` path copies it — so
  the door reaches exactly the createDataFrame source frames and refuses loud on every
  transform of one. Ledger: `task/se1-declared-sorted-ledger.md`.
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
  gained a docstring; docstring-only, and the file stays under its 2500-line ceiling.
- `reader.py` — DataFrameReader (**`smartCsv` method body** — Q7 MOVE MAP destination)
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
- `__init__.py` — frozen public re-exports

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
