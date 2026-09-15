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
- **`RESET ALL` is the bare-`RESET` spelling** (Spark treats the two identically), routed to
  `_reset_all` before the key pattern can read `ALL` as a conf name.
- **Comments are stripped before matching** (`--` and `/* … */` outside string literals —
  Spark's parser removes them), so `SET x = 1 -- c` stores `1` and `SET k = 'a;b'` stores the
  quoted value; a surviving `;` outside literals (multi-statement) or an unterminated quote
  defers to the engine, which answers with the same error the pre-door tree produced.

## PROPOSITION LEDGER — SQL-SET-DOOR-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `shapes`: every D-1 form is recognised case-insensitively with surrounding whitespace and one trailing `;` tolerated — `SET`, `SET -v`, `SET k`, `SET k = v` / `SET k=v` (value = raw text after `=`, trimmed, quotes kept), `RESET`, `RESET k`, `SET TIME ZONE '<z>'`, `SET TIME ZONE LOCAL` — and anything else reaches the engine unchanged; result frames match D-2 (pair frame non-null `key`/`value`; `RESET` zero columns zero rows; `SET`/`SET -v` sorted store keys, `-v` adds empty `meaning`/`Since version`). | `python/repark/tests/test_sql_set_door_1.py` green; red-first output pasted. | PROVEN | 20/20 green on the implementation; the two baseline pins were green on the red run (engine passthrough unchanged). Arrow `to_arrow` asserts type AND nullability. |
| C-002 | `errors`: static conf → `AnalysisException [CANNOT_MODIFY_STATIC_CONFIG]`; unresolvable zone (both `SET k = v` and `SET TIME ZONE '<v>'`) → `IllegalArgumentException [INVALID_CONF_VALUE.TIME_ZONE]` with the value echoed verbatim; non-int `spark.sql.shuffle.partitions` → `IllegalArgumentException [INVALID_CONF_VALUE.TYPE_MISMATCH] … 'int'`; `SET TIME ZONE LOCAL` → `UnsupportedOperationException` DECLARED refusal. Never a bare `Exception`. | Error pins green; classes are the facade's existing types. | PROVEN | `test_set_static_sql_conf_refuses_with_spark_error`, `test_set_quoted_time_zone_value_refuses_with_spark_error`, `test_set_unresolvable_time_zone_refuses_with_spark_error`, `test_set_time_zone_literal_with_an_unresolvable_zone_refuses`, `test_set_shuffle_partitions_non_integer_refuses_with_spark_error`, `test_set_time_zone_local_is_a_declared_refusal` — all assert class name in message AND exception type. |
| C-003 | `effect_measurement`: a `SET` has exactly the effect `spark.conf.set` of the same key has today — the D-4 measurements above are recorded and the residues named (TZ-3 zone, SET-ANSI-RUNTIME-1 ANSI); nothing `conf.set` does not apply is fixed here. | Measurement section above; pins assert the stored-but-not-applied behaviour. | PROVEN | D-4 measurement section recorded 2026-09-14 before the intercept was written; `test_set_ansi_enabled_stores_but_does_not_apply` (stored, `1/0` still `DIVIDE_BY_ZERO`), `test_set_session_time_zone_is_accepted_but_not_applied` (accepted, not stored, `current_timezone` unchanged), `test_set_repark_namespaced_key_round_trips`. |
| C-004 | `current_timezone`: `SELECT current_timezone()` on the facade SQL door answers the session zone string non-null, reading the same `repark_functions::session_time_zone` carrier the facade reads; a session built with a non-UTC zone answers it. | `current_timezone_udf` registered via `datetime`/`register_all`; Rust pin + facade pin green. | PROVEN | `current_timezone_udf` in `session_time_zone.rs`, registered via `instant_ts::functions()` (`lib.rs` stays on its 175 ceiling — a separate registration line was tried and reverted when it broke `check_lib_rs`). Rust pins `current_timezone_answers_the_carrier_zone_as_a_non_null_string` + `current_timezone_defaults_to_utc_without_the_carrier` green; facade `test_current_timezone_answers_the_session_zone` green incl. a zoned-session session-stop/rebuild leg returning `America/New_York`. |
| C-005 | `registry_and_dbt`: B-TZ-5 becomes a dated FIXED note with pins naming what `SET` applies and pointing at the residue rows; TZ-3 extended to the SQL door; new §7 rows SET-ANSI-RUNTIME-1 and SET-TZ-LOCAL-1; dbt pin `R-SET-CONF` moves out of `_refused()` into a positive `_served()` case. | Registry edits land; `pytest python/dbt-repark/tests/test_statement_surface.py` green. | PROVEN | `docs/spark-sql-iceberg-parity.md`: B-TZ-5 dated FIXED note (pins, what applies, residue pointers), TZ-3 extension paragraph + pins, new §7 row SET-ANSI-RUNTIME-1, new §4 row SET-TZ-LOCAL-1, queue departure note updated. `R-SET-CONF` → `S-SET-CONF` in `_served()`; `pytest python/dbt-repark/tests/test_statement_surface.py` → 30 passed. `docs/guide/dbt-on-repark.md` session-configuration section corrected (was "not a statement RePark accepts"). |
| C-006 | `no_regression`: the full facade suite, the dbt statement surface, the parity suite, ruff lint/format, `cargo test -p repark-functions` + clippy/fmt, and the comment fence all pass; `session_core.py` holds its exact 2304 baseline. | §Gates counts pasted. | PROVEN | §Gates: 24 SQL-door pins, 6144 facade, 30 dbt surface, 757 parity, 448 repark-functions, clippy/fmt/ruff clean, `make ci` all green; `session_core.py` 2304, `lib.rs` 175. |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

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

On the final tree (edge-hardening commit `b9c88921`):

- `.venv/bin/python -m pytest python/repark/tests/test_sql_set_door_1.py -q` → **24 passed**
  (20 original pins + RESET ALL spelling, multi-statement deferral, comment stripping,
  secret masking).
- `.venv/bin/python -m pytest python/repark/tests -q` (full facade suite) →
  **6144 passed, 358 skipped, 71 warnings** on the hardened tree (6140 + the four
  edge pins).
- `.venv/bin/python -m pytest python/dbt-repark/tests/test_statement_surface.py -q` →
  **30 passed**.
- `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark-parity/tests -q`
  → **757 passed, 2 skipped, 12 xfailed**.
- `cargo test -p repark-functions` → **448 passed, 0 failed, 1 ignored**.
- `cargo clippy -p repark-functions --all-targets` → clean (the `&'static str` pedantic fix);
  `cargo fmt -p repark-functions -- --check` → clean.
- `uvx ruff@0.15.22 check` + `format --check` on the touched files → clean.
- Comment fence `git diff --cached -- '*.rs' '*.py' '*.toml' '*.sh' '*.yml' | grep -P '^\+\s*(//|#(?!\[|!\[| noqa))'`
  → empty on every commit.
- Size gates: `session_core.py` = 2304 exactly, `crates/repark-functions/src/lib.rs` = 175
  exactly, `sql_set_statements.py` ≈ 333 (< the 1000 default ceiling), all reported by the
  pre-commit `lib-py`/`lib-rs`/`rust-file-size` hooks.
- `make ci` → every displayed gate green (map-sync 262, crate-dag, lib-rs, rust-file-size,
  lib-py, docstring-presence, docs-compaction, manifest, taplo, ledger-grammar 134 live
  ledgers / 929 clauses, docs-links 5443, matrix-test liveness, parity-live dual-wire).

## Coverage attestation

```
COVERAGE_ATTESTATION:
  pr_unit: sql-set-door-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every D-1 shape and D-2 frame contract was re-derived clause by clause from the card and the measured fixture cells BTZ5-0..BTZ5-19, not paraphrased — 18 of the 20 base pins failed red on the base tree (output pasted under "Red first"; the two greens are the engine-passthrough baselines), and all 24 pins pass on the final tree. Result rows AND Arrow type/nullability are asserted on the to_arrow path for every shape class (pair frame, four-column -v, zero-column RESET).
      artifacts: [python/repark/tests/test_sql_set_door_1.py, fixtures-batch1.json]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary and adversarial inputs exercised: never-set keys answer <undefined>; RESET ALL vs a conf key literally named ALL; multi-statement SET x = 1; SET y = 2 defers instead of mis-storing "1; SET y = 2"; -- and /* */ comments strip outside literals while 'a;b' inside quotes stays a value; unterminated quotes defer to the engine's tokenizer error; SET -v = x is refused interception (keys cannot start with -); whitespace/case variants of every keyword.
      artifacts: [python/repark/tests/test_sql_set_door_1.py, python/repark/src/repark/spark/session/sql_set_statements.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every error path is pinned loud with a facade exception type, never a bare Exception: AnalysisException [CANNOT_MODIFY_STATIC_CONFIG] (static keys pre-checked against _SQLCONF_STATIC_KEYS), IllegalArgumentException [INVALID_CONF_VALUE.TIME_ZONE] on both SET k = v and SET TIME ZONE '<v>' spellings, [INVALID_CONF_VALUE.TYPE_MISMATCH] on the int and boolean typed keys, UnsupportedOperationException for the dated SET TIME ZONE LOCAL refusal and for collation-key RESET (the engine's G15 valve the intercept would otherwise bypass), and the engine's fail-closed PySparkException for spark.wap.* assignment/unset.
      artifacts: [python/repark/tests/test_sql_set_door_1.py, python/repark/tests/test_eager_budget_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: One conf store, no second state: every read/write goes through session.conf (RuntimeConfig.set/get/unset), so SQL SET and spark.conf.set cannot diverge and RESET/RESET ALL clear exactly what the store holds — pinned by the round-trip and reset-all pins. Session reuse is exercised end to end: the zoned-session leg of the current_timezone pin stops and rebuilds a session with America/New_York and the UDF answers it, proving the carrier is per-session, not global.
      artifacts: [python/repark/tests/test_sql_set_door_1.py]
    - id: AT-5
      status: ATTACKED
      evidence: Secret-shaped values in the SET and SET -v listings mask as *** via _prop_key_is_secret — the same classifier RuntimeConfig.getAll uses — pinned by test_set_listing_masks_secret_shaped_values. Injection is bounded by construction: a surviving ; outside string literals defers to the engine (no multi-statement smuggling), and the value grammar never re-parses stored text as SQL.
      artifacts: [python/repark/tests/test_sql_set_door_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: Schema compatibility is pinned on the Arrow path: non-nullable key/value pair frames, the four non-nullable SET -v columns, and the zero-column zero-row RESET frame — all through _materialize_arrow_as_memtable_frame because createDataFrame silently drops nullable=False and refuses empty schemas. The result rows read back through conf.get, so an accepted-but-unstored key (the timezone, TZ-3) honestly reports the live session zone rather than a lying echo.
      artifacts: [python/repark/tests/test_sql_set_door_1.py, python/repark/src/repark/spark/session/sql_set_statements.py]
    - id: AT-7
      status: N/A
      justification: One regex parse over a single-statement string plus a dict store — no unbounded growth, no N+1, no hot loop; the listing frames are O(number of explicitly set keys). Nothing system-breaking is reachable.
    - id: AT-8
      status: ATTACKED
      evidence: The RuntimeConfig contract is honored by construction and load-bearing exclusions are pinned: every datafusion.* statement defers because RuntimeConfig.set forwards through this same session.sql entry point (intercepting would recurse), and spark.wap.* defers because the engine's namespace refusal is the REF-3-pinned answer — both deferral pins exist. Error classes are the facade's existing exception types matching spark.conf.set. The Rust UDF rides instant_ts::functions() because lib.rs sits at its 175-line ceiling, and session_core.py holds its exact 2304 baseline via the folded dispatch loop.
      artifacts: [python/repark/tests/test_sql_set_door_1.py, crates/repark-functions/src/instant_ts.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The silent-failure class is diagnosed: conf.set of the timezone key emits the existing one-time "accepted but NOT applied at runtime" warning through the same path the SQL door calls (warn_runtime_session_time_zone_not_applied), and every refusal names the offending key and Spark error class in the exception message. No recognized shape swallows an error.
      artifacts: [python/repark/src/repark/spark/session/sql_set_statements.py]
    - id: AT-10
      status: ATTACKED
      evidence: Span semantics held — every element of the enumerated D-1 domain (9 statement shapes) plus every declared exclusion (datafusion.*, spark.wap.*, multi-statement, unterminated quote, unrecognized SET ... TO ...) carries its own pin; red-first output is pasted. Self-review after the first green CI found three real branch-level defects (RESET ALL as key, ; injection, unstripped comments) and each is now pinned — the branch-liveness attack worked. Rust side: two unit pins cover the configured-zone and default-UTC branches of the UDF.
      artifacts: [python/repark/tests/test_sql_set_door_1.py, crates/repark-functions/src/session_time_zone/tests.rs]
  complete: true
```
