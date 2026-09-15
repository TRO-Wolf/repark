# Unit ledger — SQL-SET-DOOR-1 · `SET` / `RESET` / `SET TIME ZONE` through `spark.sql`

**Date:** 2026-09-14 · **Branch:** `feat/sql-set-door-1` · **Base:** `origin/main`
**Model:** swe-2-high · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) · **Path:** STANDARD.
**Card:** [../../../briefs/](../../../briefs/) SQL-SET-DOOR-1 — registry row B-TZ-5; run 15c,
2026-09-14. Oracle `fixtures-batch1.json` cells `BTZ5-0`…`BTZ5-19` (PySpark 4.1.2, one session,
in order); `repark-batch1.json` has today's answers (every one refuses).

**Gap.** Every `spark.*`/`repark.*` `SET` through the facade SQL door reaches DataFusion, which
refuses `Could not find config namespace "spark"` — there is no SQL path to the runtime conf at
all. `SET TIME ZONE 'x'` is a parse-time `UnsupportedOperationException`, `SET <key>` / `RESET` /
`SET -v` are `ParseException`s, and `current_timezone()` is `Invalid function`.

**Mechanism.** A new module `python/repark/src/repark/spark/session/sql_set_statements.py`
recognises the D-1 shapes after whitespace + one-trailing-`;` normalisation; anything else —
including every `datafusion.*` key and every `spark.wap.*` assignment/unset — falls through to
the engine unchanged (the `datafusion.*` exclusion is load-bearing: `RuntimeConfig.set` forwards
those keys through `session.sql("SET …")`, so intercepting them would loop; the `spark.wap.*`
exclusion is fail-closed — repark does not implement WAP and the engine's loud refusal, not a
silent conf store, is the REF-3-pinned answer). `RESET` of a collation key calls
`refuse_collation_session_key` before `conf.unset`, mirroring the engine's G15
`refuse_collation_reset_variable` valve the intercept would otherwise bypass (`conf.unset`
carries no collation guard). `SparkSession.sql` gains one hook line that returns the
module's DataFrame when it answers. Every read/write goes through `self.conf`
(`RuntimeConfig.set`/`get`/`unset`) — D-4's "same effect `spark.conf.set` has" by construction.
Result frames are `pa.Table`s materialised through `_materialize_arrow_as_memtable_frame`, which
is the only construction that preserves the non-nullable `key`/`value` fields and the zero-column
`RESET` answer (`createDataFrame` drops `nullable=False` on export and refuses an empty schema).
Spark-class errors the conf layer cannot raise are produced in the module:
`CANNOT_MODIFY_STATIC_CONFIG` → `AnalysisException` (pre-check against `_SQLCONF_STATIC_KEYS`,
which stays the SSOT for "static"), `INVALID_CONF_VALUE.TIME_ZONE` → `IllegalArgumentException`
(zone validated against `zoneinfo.ZoneInfo` + the engine's fixed-offset grammar — `Tz::from_str`
accepts `±HH[:MM]` — because `conf.set` of that key validates nothing), and
`INVALID_CONF_VALUE.TYPE_MISMATCH` → `IllegalArgumentException` for the two typed keys this unit
touches (`spark.sql.shuffle.partitions` int, `spark.sql.ansi.enabled` boolean). `SET TIME ZONE
LOCAL` is a dated DECLARED refusal (`UnsupportedOperationException`) — repark never reads the
host zone (TZ-2). `SET TIME ZONE '<zone>'` strips the literal's quotes; `SET k = 'v'` keeps them
(D-1, BTZ5-4).

## D-4 effect measurement (recorded 2026-09-14, this clone's `.venv`)

Measured on the base tree before writing the intercept:

- `conf.set("spark.sql.ansi.enabled", "false")` → stored (`conf.get` → `'false'`), **not
  applied**: `SELECT 1/0` still raises `PySparkException [DIVIDE_BY_ZERO]`. The flag is a
  build-time carrier (`repark_functions::ansi::SparkAnsiConfig`, `PREFIX repark.ansi`, `set`
  refuses) installed once by `SparkExtension::configure` from the builder map. → residue row
  **SET-ANSI-RUNTIME-1** (new §7 row).
- `conf.set("spark.sql.session.timeZone", "America/New_York")` → accepted, warns once
  ("accepted for source compatibility but NOT applied at runtime, and its value is NOT
  validated"), **stores nothing**: `conf.get` keeps reporting `UTC`. Carrier
  `SessionTimeZoneConfig` (`repark.session`) likewise refuses `set`. → residue row **TZ-3**
  (extended: the SQL door inherits the identical contract).
- `conf.set("spark.sql.session.timeZone", "Invalid/Zone")` → **accepted** (no validation) —
  the SQL door therefore validates itself for the `INVALID_CONF_VALUE.TIME_ZONE` class.
- `conf.set("spark.sql.warehouse.dir", "/tmp/x")` → plain `Exception: Cannot modify the value
  of static config: spark.sql.warehouse.dir` — the module pre-checks `_SQLCONF_STATIC_KEYS`
  and raises `AnalysisException [CANNOT_MODIFY_STATIC_CONFIG]` with Spark's message shape
  (minus the recorded no-SQLSTATE delta).
- `conf.set("spark.sql.shuffle.partitions", "abc")` → **accepted and stored** (`conf.get` →
  `'abc'`) — no runtime type check exists, so the module produces
  `INVALID_CONF_VALUE.TYPE_MISMATCH` itself.
- `SET datafusion.execution.batch_size = '64'` through `sql()` → engine answers an empty frame
  (0 columns, 0 rows); `conf.set` forwards through the same statement — passthrough kept.

Decisions taken under the card (recorded, not invented):

- **Result echo is the effective conf value, not the raw `v`.** `SET k = v` answers
  `(key, conf.get(key))` after the set; for keys `conf.set` does not store (the timezone) the row
  honestly reports the zone the live session has (`UTC`) instead of echoing a value `conf.get`
  would contradict — the same "never a lying conf" rule TZ-3 encodes. For every stored key the
  echo equals Spark's.
- **`SET` / `SET -v` list the runtime store** (keys `conf.set`/`SET` put there and `RESET`
  clears), not builder config: repark cannot tell a seeded-SQLConf builder key from a SparkConf
  one, and store-only is self-consistent — `SET` lists what `RESET` clears. Spark also lists
  builder-seeded SQLConf entries; that half is a noted divergence. Secret-shaped values mask as
  `***` like `getAll`.
- **`SET k` of a never-set key answers `<undefined>`** (D-2 fallback; the fixture has no such
  cell — Spark's `getConfString(key, "<undefined>")`). Spark-defined keys with SQLConf defaults
  repark does not model (`spark.sql.ansi.enabled` → `true`, `spark.sql.shuffle.partitions` →
  `200`) also answer `<undefined>` where Spark answers the default — noted divergence, no
  fixture cell.
- **`RESET` / `RESET k` unset via `conf.unset`** (tombstone semantics); a `RESET`ed
  non-defaulted key then reads `<undefined>` where Spark reads the restored default.
- **Conf keys stay case-sensitive** (Spark's SQLConf lookup is case-insensitive); `SET
  SPARK.SQL.SHUFFLE.PARTITIONS = 2` stores a differently-cased key — noted divergence.
- **`spark.app.name` and other non-`_SQLCONF_STATIC_KEYS` statics** are not refused (repark's
  static set is the SSOT) — Spark raises `CANNOT_MODIFY_STATIC_CONFIG` for them; noted
  divergence, no fixture cell.
- **`spark.wap.*` assignment/unset defers to the engine**, not `conf.set`: `conf.set` silently
  stores `spark.wap.branch` (a pre-existing conf-door hole this card cannot fix — `builder_conf`
  is frozen), and intercepting would turn REF-3's fail-closed refusal into a silent store.
  `SET spark.wap.branch` (read) is still answered from the store — honest readback.
- **`RESET <collation key>` refuses** via `refuse_collation_session_key` (same helper
  `conf.set` uses) — the engine's `refuse_collation_reset_variable` valve survives the door.
- **`repark.*` keys intercept** (D-4): `repark.cache.max_bytes`/`max_total_bytes` are real
  runtime knobs `conf.set` honours, so SQL `SET` now stores them (the L-004 pin asserted the
  pre-door namespace error — re-pinned in `test_eager_budget_1.py` to the conf contract);
  `repark.cache.retained_bytes` refuses `INVALID_CONF_VALUE.REQUIREMENT` on both SET and
  RESET via `conf`'s own read-only guard — a strictly better error than the old namespace one.

## PROPOSITION LEDGER — SQL-SET-DOOR-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `shapes`: every D-1 form is recognised case-insensitively with surrounding whitespace and one trailing `;` tolerated — `SET`, `SET -v`, `SET k`, `SET k = v` / `SET k=v` (value = raw text after `=`, trimmed, quotes kept), `RESET`, `RESET k`, `SET TIME ZONE '<z>'`, `SET TIME ZONE LOCAL` — and anything else reaches the engine unchanged; result frames match D-2 (pair frame non-null `key`/`value`; `RESET` zero columns zero rows; `SET`/`SET -v` sorted store keys, `-v` adds empty `meaning`/`Since version`). | `python/repark/tests/test_sql_set_door_1.py` green; red-first output pasted. | PROVEN | 20/20 green on the implementation; the two baseline pins were green on the red run (engine passthrough unchanged). Arrow `to_arrow` asserts type AND nullability. |
| C-002 | `errors`: static conf → `AnalysisException [CANNOT_MODIFY_STATIC_CONFIG]`; unresolvable zone (both `SET k = v` and `SET TIME ZONE '<v>'`) → `IllegalArgumentException [INVALID_CONF_VALUE.TIME_ZONE]` with the value echoed verbatim; non-int `spark.sql.shuffle.partitions` → `IllegalArgumentException [INVALID_CONF_VALUE.TYPE_MISMATCH] … 'int'`; `SET TIME ZONE LOCAL` → `UnsupportedOperationException` DECLARED refusal. Never a bare `Exception`. | Error pins green; classes are the facade's existing types. | PROVEN | `test_set_static_sql_conf_refuses_with_spark_error`, `test_set_quoted_time_zone_value_refuses_with_spark_error`, `test_set_unresolvable_time_zone_refuses_with_spark_error`, `test_set_time_zone_literal_with_an_unresolvable_zone_refuses`, `test_set_shuffle_partitions_non_integer_refuses_with_spark_error`, `test_set_time_zone_local_is_a_declared_refusal` — all assert class name in message AND exception type. |
| C-003 | `effect_measurement`: a `SET` has exactly the effect `spark.conf.set` of the same key has today — the D-4 measurements above are recorded and the residues named (TZ-3 zone, SET-ANSI-RUNTIME-1 ANSI); nothing `conf.set` does not apply is fixed here. | Measurement section above; pins assert the stored-but-not-applied behaviour. | PROVEN | D-4 measurement section recorded 2026-09-14 before the intercept was written; `test_set_ansi_enabled_stores_but_does_not_apply` (stored, `1/0` still `DIVIDE_BY_ZERO`), `test_set_session_time_zone_is_accepted_but_not_applied` (accepted, not stored, `current_timezone` unchanged), `test_set_repark_namespaced_key_round_trips`. |
| C-004 | `current_timezone`: `SELECT current_timezone()` on the facade SQL door answers the session zone string non-null, reading the same `repark_functions::session_time_zone` carrier the facade reads; a session built with a non-UTC zone answers it. | `current_timezone_udf` registered via `datetime`/`register_all`; Rust pin + facade pin green. | PROVEN | `current_timezone_udf` in `session_time_zone.rs`, registered via `instant_ts::functions()` (`lib.rs` stays on its 175 ceiling — a separate registration line was tried and reverted when it broke `check_lib_rs`). Rust pins `current_timezone_answers_the_carrier_zone_as_a_non_null_string` + `current_timezone_defaults_to_utc_without_the_carrier` green; facade `test_current_timezone_answers_the_session_zone` green incl. a zoned-session session-stop/rebuild leg returning `America/New_York`. |
| C-005 | `registry_and_dbt`: B-TZ-5 becomes a dated FIXED note with pins naming what `SET` applies and pointing at the residue rows; TZ-3 extended to the SQL door; new §7 rows SET-ANSI-RUNTIME-1 and SET-TZ-LOCAL-1; dbt pin `R-SET-CONF` moves out of `_refused()` into a positive `_served()` case. | Registry edits land; `pytest python/dbt-repark/tests/test_statement_surface.py` green. | PROVEN | `docs/spark-sql-iceberg-parity.md`: B-TZ-5 dated FIXED note (pins, what applies, residue pointers), TZ-3 extension paragraph + pins, new §7 row SET-ANSI-RUNTIME-1, new §4 row SET-TZ-LOCAL-1, queue departure note updated. `R-SET-CONF` → `S-SET-CONF` in `_served()`; `pytest python/dbt-repark/tests/test_statement_surface.py` → 30 passed. `docs/guide/dbt-on-repark.md` session-configuration section corrected (was "not a statement RePark accepts"). |
| C-006 | `no_regression`: the full facade suite, the dbt statement surface, the parity suite, ruff lint/format, `cargo test -p repark-functions` + clippy/fmt, and the comment fence all pass; `session_core.py` holds its exact 2304 baseline. | §Gates counts pasted. | OPEN | |

VERDICT: 6 clauses, 5 PROVEN, 1 OPEN, 0 REJECTED.

## Red first

`python/repark/tests/test_sql_set_door_1.py` on the base tree (before any implementation):

```
FAILED test_sql_set_door_1.py::test_set_key_value_returns_the_pair_frame
FAILED test_sql_set_door_1.py::test_set_key_read_reports_the_stored_value
FAILED test_sql_set_door_1.py::test_set_key_read_of_a_never_set_key_answers_undefined
FAILED test_sql_set_door_1.py::test_set_session_time_zone_is_accepted_but_not_applied
FAILED test_sql_set_door_1.py::test_current_timezone_answers_the_session_zone
FAILED test_sql_set_door_1.py::test_set_quoted_time_zone_value_refuses_with_spark_error
FAILED test_sql_set_door_1.py::test_set_unresolvable_time_zone_refuses_with_spark_error
FAILED test_sql_set_door_1.py::test_set_ansi_enabled_stores_but_does_not_apply
FAILED test_sql_set_door_1.py::test_reset_key_returns_an_empty_frame
FAILED test_sql_set_door_1.py::test_reset_all_clears_the_runtime_keys
FAILED test_sql_set_door_1.py::test_set_static_sql_conf_refuses_with_spark_error
FAILED test_sql_set_door_1.py::test_set_repark_namespaced_key_round_trips
FAILED test_sql_set_door_1.py::test_set_time_zone_literal_returns_the_pair
FAILED test_sql_set_door_1.py::test_set_time_zone_literal_with_an_unresolvable_zone_refuses
FAILED test_sql_set_door_1.py::test_set_time_zone_local_is_a_declared_refusal
FAILED test_sql_set_door_1.py::test_set_shuffle_partitions_non_integer_refuses_with_spark_error
FAILED test_sql_set_door_1.py::test_set_bare_lists_the_explicitly_set_keys_sorted
FAILED test_sql_set_door_1.py::test_set_verbose_lists_the_same_keys_with_empty_metadata
18 failed, 2 passed in 0.68s
```

Representative failure: `SET spark.sql.shuffle.partitions = 2` →
`repark.errors.PySparkException: datafusion engine error: Invalid or Unsupported Configuration:
Could not find config namespace "spark"` — the B-TZ-5 refusal. The two green cases are the
baseline pins: `SET datafusion.execution.batch_size = '64'` already reaches the engine (empty
frame) and an unrecognised `SET … TO …` already raises `PySparkException`.

## Gates

(filled at step 4)
