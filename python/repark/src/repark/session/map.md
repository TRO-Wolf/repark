# map — repark/session/

## Purpose

Package split of monolithic `session.py` (r26 T1 MOVE-ONLY).

## Contents

- `_funcs.py` — free functions (shared name binding for class modules); includes
  `createDataFrame` Arrow reshape for dense FixedSizeList / sparse ML vectors (mixed dense
  widths refuse loud — layout home `repark.ml.linalg`).
  **G15:** `_data_type_to_sql_type` refuses a non-binary `StringType` (the silently-wrong-count
  path: collation was stripped to `STRING`).
- `builder_conf.py` — SparkContext, RuntimeConfig.
  **G15:** `RuntimeConfig.set` refuses session keys containing `collation` (silent-ignore path).
- `session_core.py` — ReparkSession (sql/catalog methods stay here).
  **G15:** `Builder._set_config_entry` refuses collation `SQLConf` keys (silent-ignore path).
- `reader.py` — DataFrameReader (**`smartCsv` method body** — Q7 MOVE MAP destination)
- `sql_udf.py` — UDFRegistration
- `create_dataframe.py` — region marker + SparkSession/ReParkSession aliases
- `catalog.py` — re-export binding region note (r27 T1 no-stub mark)
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
  what the zone reaches** (updated 2026-08-10, H-1a split B rework): extraction over an
  INSTANT-typed TIMESTAMP honors it; a **zoneless** timestamp input (registry row TZ-7) and
  `to_date` / `CAST(ts AS DATE)` / `datediff` (row TZ-8) still do not. That paragraph ships in the
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
