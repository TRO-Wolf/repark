# Unit ledger — SET-ANSI-RUNTIME-1 round 1 · runtime SET and spark.conf.set of the ANSI and time-zone knobs apply to the live session

**Unit:** `set-ansi-runtime-1` round 1 (clauses C-001…C-005; BL-11 is round 2) · **Date:** 2026-09-15 · **Branch:** `feat/set-ansi-runtime-1` · **Base:** `origin/main`
**Model:** muse-spark-1.3-contributor
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**
**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

## 1. Announcement to run 16b — every `python/repark/src/repark/spark/session/**` file this unit edits

Run 16b owns `dataframe/**`, `column.py`, `catalog.py` and the unfolding CONF-UNSET-1 BACKLOG; nothing below touches those. The session/** files this unit edits, with the reason each needs the touch:

- `builder_conf.py` — `RuntimeConfig.set` / `unset` route the two exact keys (`spark.sql.ansi.enabled`, `spark.sql.session.timeZone`) to the new native setter before storing; everything else in both methods is untouched. Tombstone semantics are unchanged (CONF-UNSET-1 stays the authority; this unit only applies whatever value `get` currently reports).
- `session_time_zone.py` — net-negative: the TZ-3 warning helper and its once-per-process flag are deleted (the card orders the deletion); the module docstring flips to the applied contract. Key constants, builder normalization and the collect/localize helpers stay.
- `sql_set_statements.py` — net-negative: the Python boolean/zone validators move into the native setter (single gate, same verdicts and messages, proved by the unchanged refusal pins); the `SET TIME ZONE` path keeps unquoting and the LOCAL refusal and otherwise delegates to `conf.set`.
- `session_configuration.py` — TWO lines, the one coordination point this unit cannot stay out of: a `SPARK_SQL_ANSI_ENABLED_KEY` constant and one `_SQLCONF_DEFAULTS` entry (`spark.sql.ansi.enabled → true`). S16-5 (`conf.get` after `unset` answers `'true'` with builder `true`) is unreachable without it: the tombstone CONF-UNSET-1 owns hides the builder value, and only a registered default answers. This is row one of the registered-default table CONF-UNSET-1's own BACKLOG prescribes (`The fix is a registered-default table behind RuntimeConfig.get`); 16b's pins (shuffle.partitions) are unaffected, and the entry composes with the future table instead of pre-empting it. `session_core.py` needs NO edit (net-zero, Q-15c-4): the reuse fold and `getOrCreate` paths are untouched — reuse-with-a-differing-ANSI still soft-folds facade-only, a known narrow residue noted under C-003.
- No other session/** file is edited. `functions*.py`, `dataframe/**`, `column.py`, `catalog.py` are not edited; no hand-off to 16a/16b is owed beyond this section.
- Addendum (round 17c): one unlisted one-line touch — `timestamp_type.py` module docstring drops
  the stale `(ansi.enabled precedent)` parenthetical (that knob now applies at runtime while
  timestamp-type stays store-only). No behaviour, no size move.

## 2. Rulings and design record

- **Q-15c-3** (owner, 2026-09-15): runtime `SET` and `spark.conf.set` of the two knobs apply through a per-query config snapshot.
- **Q-15c-4** (owner, 2026-09-15): size baselines ratchet down only; a one-time +N is allowed only in the PR that needs it, named in the ledger. This unit needs none: `session.rs` (binding, exact 1127) is untouched — the new binding methods ride a second `#[pymethods]` block in the new `session_runtime.rs`; `session.rs` (core, 988/1000) moves +6; every Python session file shrinks except the two-line coordination above; `session_core.py` stays 2290.
- **D-1** (orchestrator): live values behind one lock-guarded snapshot, `Session::set_runtime_config` validates in Rust and writes the DataFusion `SessionContext` state's `ConfigOptions`, `RESET` restores the builder value. Two forced deviations, both measured: (a) core cannot name the carrier types (repark-core and repark-functions are dependency-disjoint by the crate DAG), so the `ConfigOptions` write lives in a new binding-crate module `session_runtime.rs` that sees both crates; core owns the zone snapshot plus the runtime zone parser, the carriers own their value parsers, the binding only forwards. (b) The runtime zone parser is NOT `SessionTimeZone::parse`: Arrow `Tz::from_str` accepts `+18:01` (Spark refuses, S5-tz-plus1801) and refuses `+5` / `GMT+8` (Spark accepts, S5-tz-plus5 / S5-tz-gmt8), so a Java-`ZoneId`-semantics mirror (`parse_runtime_value`, verdict-equivalent with the facade validator it replaces) is the only gate that keeps every pinned S5 cell green. The builder parser is frozen.
- **Snapshot timing** (addendum, batch 16, overrides D-1's measure-one-cell): ANSI binds when a frame is analysed (S16-0 keeps `DIVIDE_BY_ZERO`); zone value expressions (`from_unixtime`, casts) answer the frame-build zone; `current_timezone()` folds at collect. DataFusion `DataFrame`s hold the `sql()`-time `SessionState` clone, so writing the live state gives the first two for free; `current_timezone()` on a STALE frame therefore answers the build zone, not the new one — a sanctioned narrow residue row plus a pin of today's answer (see C-002).
- **Unset applies the get-visible value** (CONF-UNSET-1 authority): `unset` keeps its tombstone; the engine is then set to `_SQLCONF_DEFAULTS[key]` (`true` / `UTC`), which is what `get` reports. `RESET` with a builder value goes through the lenient restore entry (builder tokens such as `1`/`yes` parsed at build must re-apply without tripping the strict runtime gate).
- **Round 2 (BL-11, C-006) is OPEN and out of this step**: numeric→BINARY cast legality (`analyzer.rs` `cast_legality`, no ANSI input today) needs an analyzer change outside this card's fence. The runtime ANSI carrier it will read is this step's deliverable.
- **Rust first**: all validation, parsing and state writes are Rust. Python is storage plus forwarding. No Python-only implementation, so no ledger reason is owed.
- **R-17c-1** (orchestrator, G-2, 2026-09-15): this unit keeps lane name `pc-cv2`; no clone rename.
- **Comment contract precedence (round 17c):** the brief's comment-grep is stricter than the
  authoritative contract — AGENTS.md holds that required docstrings, Rust banners and invariant
  comments remain (the mechanical pre-commit hook carries no comment check, and
  `missing_errors_doc` clippy would fail without the `# Errors` sections). The parked Rust keeps
  only those; the facade half is net-negative prose (one `#`-comment pair added mid-round was
  deleted before commit, its reason living in the session `map.md` row instead).
- **R-17c-3 (orchestrator, 2026-09-15): the docstring-presence gate is Python-only
  (`scripts/check_docstring_presence.py` SCAN_ROOTS), so no Rust `///` is ever gate-required; the
  comment ban applies to Rust doc comments without exception, and their content lives in `map.md`.
  Applied in the follow-up commit: all 35 added Rust comment lines deleted (their facts already
  lived in the crate maps, topped up with the ±18:00 ordering reason and the
  `Error::IllegalArgument` variant); the two public `Result` entry points take
  `#[allow(clippy::missing_errors_doc)]` (attribute, not a comment — the IO-TEXT-1 precedent).
- **R-17c-4 (orchestrator, G-2, 2026-09-15):**

  > The runtime zone snapshot carries **two** values: the **Spark-visible text** exactly as the user set it,
  > which is what `conf.get`, `SET -v` and `current_timezone()` report (the S5 cells pin that echo and it
  > must not change), and a **canonical companion** — an id Arrow `Tz::from_str` and Python `ZoneInfo` both
  > accept — which every value-bearing consumer uses. The canonicalisation happens ONCE, in Rust, at the
  > gate, when the value is validated; no consumer is taught the Java grammar, and no consumer re-parses
  > the raw text. A value that validates but cannot be canonicalised is a refusal at the SET, not a stored
  > value: it is better to refuse than to accept and then explode.

  Applied with one fence-driven adjustment: the canonicaliser is duplicated in the two crates that
  fill the carrier (`repark-core` for the snapshot/binding/text-scan, `repark-functions` for the
  carrier fill both doors share), because `repark-functions` cannot depend on `repark-core` and the
  builder fill site (`repark-spark` extension) is out of fence. Both copies are pinned by the same
  table and the duality is recorded in both maps.
- **P2-logic-4 lock order (round 17c, assessed, downgraded to P3):** no inversion exists. The write
  path holds the DataFusion state write guard while taking the zone snapshot write guard (one
  direction only); every read path takes the snapshot guard alone and `session_time_zone()` drops
  it before returning (`Arc::clone` under the guard, guard released at return), so `read_text`'s
  later `read_table` never nests inside it. Nobody holds the snapshot guard while wanting the
  state guard — deadlock needs a cycle and there is none. No test added (nothing to pin).
- **P2-logic-2 hand-off to run 17a:** `F.current_timezone()` still binds a Python literal
  (`functions_datetime.py`, via `active_session_time_zone()`) instead of the native
  `current_timezone` UDF, so a pre-built `F.current_timezone()` column keeps the build zone while
  the SQL door follows the SET. `functions*.py` is 17a-owned; this unit pins the SQL door plus the
  Rust-reachable expression path (`test_select_expr_binds_the_zone_at_analysis`) and changes nothing
  there.
- **P3 records (round 17c):** unset-with-builder-`false` applies the registered default (`true`),
  not the builder value — behaviour kept, CONF-UNSET-1 is the authority, and the C-003 proposition
  below is reworded to match; S16-5 (builder `true`) does not demand otherwise. Mixed-case key
  spellings skip the native gate by exact match and store facade-only (Spark key matching is
  case-sensitive on the measured cells; no cell covers mixed case — UNMEASURED, no change).
  Per-call `from repark import _native` hoisted to module top in `builder_conf.py` and
  `sql_set_statements.py` (no cycle: the extension module loads independently, verified by
  import), and `set`/`unset`/`_restore_or_unset` take one `_ensure_alive()` handle. The
  `RwLock`+`Arc::clone` snapshot read sits on session setup paths, not a kernel path — no change.
  `hour(string)` (e.g. `hour(from_unixtime(0))`) extracts zone-blind (0 under Tokyo at build too);
  `hour()` over genuine instants and over `CAST(string AS TIMESTAMP)` follows the zone — pins use
  the latter form; the string form belongs to the extraction area, out of fence.
- **Third measured deviation, S16-2 (round 17c):** `CAST('x' AS INT)` still raises after the SET.
  Measured identical with ANSI off at BUILD (literal, subquery and real-column shapes all raise
  the same `simplify_expressions` cast error), so the string-cast path never reads the ANSI flag
  and the snapshot cannot deliver the oracle cell. Pinned as today's answer plus narrow residue
  row SET-ANSI-RUNTIME-3 (registry); division (S16-1) is the live proof fresh queries read the
  runtime flag. Fixing the cast kernel is outside this card's fence (carriers only).
- **Size actuals (round 17c, Q-15c-4):** `builder_conf.py` 386→391 (+5, native-dispatch lines),
  `session_configuration.py` 577→583 (+6, ANSI key + default), `sql_set_statements.py` 438→369,
  `session_time_zone.py` 161→108, `timestamp_type.py` 94 (one line reworded, count unchanged);
  `session_core.py` untouched at 2290. One sanctioned ratchet: `check_lib_py.py`
  `test_session_timezone_parity.py` 1328→1318 (flips net-negative), logged in `scripts/map.md`.

## PROPOSITION LEDGER — set-ansi-runtime-1 round 1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `conf.set("spark.sql.ansi.enabled","false")` then fresh `SELECT 1/0` answers NULL (double, nullable); the same through `SET`; `true` restores `DIVIDE_BY_ZERO`; `F.lit(1)/F.lit(0)` on a post-SET frame follows; a frame built pre-SET keeps the raise (S16-0). | `test_s16_0_stale_frame_keeps_divide_by_zero`, `test_s16_1_fresh_division_answers_null_after_set`, `test_s16_3_conf_set_true_restores_raise`, `test_s16_4_unset_with_builder_true_raises`, `test_c001_f_api_division_follows_runtime_ansi`, `test_btz5_7_8_set_false_then_null`, `test_btz5_9_10_reset_then_raise` | **PROVEN** | `test_set_ansi_runtime_1.py` 22/22 green on the rebuilt release native; S16-2 renamed to `test_s16_2_cast_x_still_raises_string_cast_residue` (third deviation above, residue row SET-ANSI-RUNTIME-3) and no longer counts toward this clause. |
| C-002 | `conf.set("spark.sql.session.timeZone","Asia/Tokyo")` then `current_timezone()`, `from_unixtime(0)`, `CAST(TIMESTAMP … AS STRING)` follow the new zone on both doors; `SET TIME ZONE` the same; invalid zone refuses `INVALID_CONF_VALUE.TIME_ZONE` at the SET with nothing stored; `conf.get` reports the applied value. S16-6 pins exactly, including the one-element `current_timezone()` residue. | `test_s16_6_stale_frame_split_binding`, `test_s16_7_fresh_query_follows_new_zone`, `test_s16_8_conf_set_zone_applies`, `test_s16_9_conf_set_invalid_zone_refuses`, `test_s16_10_set_ansi_maybe_refuses`, `test_s16_11_reset_zone_restores_builder`, `test_c002_f_api_zone_follows`, `test_btz5_2_3_set_zone_then_current_timezone`, `test_btz5_5_refused_set_moves_nothing`, `test_s5_zone_cells_apply` | **PROVEN** | Same green run; S16-6 green pre- and post-change for the pinned reason (build-zone snapshot). Narrow residue row SET-ANSI-RUNTIME-2 in the registry. |
| C-003 | `RESET` restores the builder-seeded value, applied; `conf.unset` applies the registered default (`true` / `UTC`) per CONF-UNSET-1, even over a builder value. | `test_s16_5_get_after_unset_reports_builder_true`, `test_reset_restores_builder_ansi_applied`, S16-4/S16-11 | **PROVEN** | Same green run; `test_sql_set_door_1.py` RESET pins green (flipped where the old residue showed). Reuse-with-differing-ANSI still soft-folds facade-only: known narrow residue, `session_core.py` untouched per Q-15c-4. |
| C-004 | The native ANSI door (`repark.sql()`) is unaffected by Spark-session runtime sets. | `test_c004_native_door_ignores_spark_runtime_sets` | **PROVEN** | Same green run. |
| C-005 | Registry TZ-3 and SET-ANSI-RUNTIME-1 read FIXED with pins; every pin that codified the old residue flips in place. | Flipped pins in `test_sql_set_door_1.py` (module docstring + 9 tests) and `test_session_timezone_parity.py` (2 tests), registry rows, `make verify` rc 0 | **PROVEN** | 9 + 2 flips landed; TZ-3 / SET-ANSI-RUNTIME-1 rows read FIXED; new residue rows SET-ANSI-RUNTIME-2 / -3; guides (`session-and-conf.md`, `troubleshooting.md`) trued up; `make verify` rc 0 (gates below). `check_lib_py` ratchet 1328→1318 logged. |
| C-006 | Round 2: BL-11 numeric→BINARY under runtime ANSI off. | — | **OPEN** | Out of this step by card order; prerequisite (runtime ANSI carrier) delivered here. |

VERDICT (2026-09-15, ledger creation): 6 clauses, 0 PROVEN, 6 OPEN, 0 REJECTED.
VERDICT (2026-09-15, round 1 close): 6 clauses, 5 PROVEN, 1 OPEN (C-006, round 2), 0 REJECTED.

## Red first

`python/repark/tests/test_set_ansi_runtime_1.py` (22 pins) against main `11ae1595`
plus the unit clone, release native, 2026-09-15: **14 failed, 8 passed**. The 8
passes are the already-true cells (S16-0 stale raise, S16-3/S16-4 raise shapes,
S16-10/S16-11, S5-bool refusals, C-004 native stability). Every application
half fails: fresh `1/0` still raises after the SET (`DIVIDE_BY_ZERO`,
`test_s16_1`), `conf.set` of the zone warns and stores nothing (TZ-3 warning
text in the `test_s16_6` output), `conf.set` of a bad zone is swallowed
(`test_s16_9`), `conf.get` after `unset` raises instead of `'true'`
(`test_s16_5`).

```text
FAILED test_s16_1_fresh_division_answers_null_after_set
FAILED test_s16_2_cast_x_answers_null_after_set
FAILED test_s16_5_get_after_unset_reports_builder_true
FAILED test_s16_6_stale_frame_split_binding
FAILED test_s16_7_fresh_query_follows_new_zone
FAILED test_s16_8_conf_set_zone_applies
FAILED test_s16_9_conf_set_invalid_zone_refuses
FAILED test_c001_f_api_division_follows_runtime_ansi
FAILED test_c002_f_api_zone_follows
FAILED test_btz5_2_3_set_zone_then_current_timezone
FAILED test_btz5_5_refused_set_moves_nothing
FAILED test_btz5_7_8_set_false_then_null
FAILED test_s5_zone_cells_apply
FAILED test_reset_restores_builder_ansi_applied
14 failed, 8 passed
```

Measured today, fixing the residue expectation before implementation: the S16-6
stale frame answers `{'tz': 'UTC', 'epoch': '1970-01-01 00:00:00', 'wall':
'2024-01-01 00:00:00'}` — the `tz` element stays the build zone after the
change too (captured `SessionState`), so the pin asserts `UTC` with a narrow
residue row against Spark's `Asia/Tokyo`. Native-door `SELECT 1/0` raises
`PySparkException: Arrow error: Divide by zero error` (DataFusion's own class,
not Spark's) — pinned as the C-004 answer.

## Red-first, P1 spelling pins (round 17c review follow-up)

`python/repark/tests/test_runtime_zone_spellings_1.py` (new): 4 tests, run against the
pre-fix release native — 4 failed, 0 passed. The value pins failed with the reviewers'
measured error (`session timezone "+5" could not be resolved at query time` on
`from_unixtime(0)`; `ZoneInfoNotFoundError` on the `createDataFrame` path); the seconds
pins failed with `DID NOT RAISE` (the gate accepted what no consumer could resolve).
The echo halves passed pre-fix, which is exactly the hole: the old pins never reached a
value assertion. Post-fix run: 5 passed (4 spelling tests plus the `selectExpr`
analysis-binding pin).

Padded SET (P2-logic-3): the store now keeps the trimmed zone text in both places, so
`conf.get` and the engine never differ. The S5 cells all use unpadded values, so no cell
decides padding; the ruling's parenthetical does ("Spark-visible text" is what Spark would
report, i.e. trimmed) — followed.

## Gates

- `cargo test -p repark-core session` — 151 passed, 0 failed (one parked-test fix: sign-led
  values no longer fall through to Arrow's `Tz`, so `+18:01` refuses as Java does).
- `cargo test -p repark-functions -- ansi session_time_zone` — all suites green (57 + 2).
- `make rust-clippy` — green after 4 pedantic fixes in `session_time_zone.rs`
  (`unnested_or_patterns`, 3× `redundant_closure_for_method_calls`).
- `make verify` — rc 0.
- Release native rebuilt twice via the venv maturin path (after the Rust slice, after the
  clippy fixes); no `make develop`, no JVM.
- `.venv/bin/python -m pytest python/repark/tests/test_set_ansi_runtime_1.py python/repark/tests/test_sql_set_door_1.py -q` — 65 passed.
- `.venv/bin/python -m pytest python/repark/tests -q -k "ansi or time_zone or timezone or conf or set_door"` — 881 passed, 112 skipped.
- Full facade suite (`python/repark/tests`, venv python, release native) — 8296 passed,
  367 skipped, 2 xfailed, 1 failed: `test_production_file_size` `_SQLCONF_DEFAULTS` body
  hash, re-hashed to the measured value in the same round (mechanical baseline, not a product
  failure); rerun green.
- Parity harness (`python/repark-parity/tests`) — 756 passed, 2 skipped, 12 xfailed, 1 failed:
  CAP-1 mirror row for the same ratchet, moved 1328→1318 with the script baseline; rerun green.
- COVERAGE_ATTESTATION: every clause C-001…C-005 names its pins in the table above; C-006 stays
  OPEN as round 2 by card order.
- Follow-up (R-17c-3, comment strip): 35 added Rust comment lines deleted, maps topped up;
  `make rust-clippy` green (two `#[allow(clippy::missing_errors_doc)]` attributes);
  `cargo test -p repark-core session` 151 passed; `cargo test -p repark-functions -- ansi
  session_time_zone` green; release native rebuilt; pins 65 passed; `make verify` rc 0.
- Follow-up (PR #639 review, R-17c-4): `cargo test -p repark-core session` 152 passed;
  `cargo test -p repark-functions -- ansi session_time_zone` green (58 zone incl. the new
  echo/canonical split test); `make rust-clippy` green; spelling pins
  (`test_runtime_zone_spellings_1.py`, 5) green after 4-failed red-first; touched suites
  (spellings + set_ansi_runtime_1 + sql_set_door_1 + session_timezone_parity) 116 passed;
  keyword sweep 956 passed, 112 skipped; `make verify` rc 0; release native rebuilt after
  the last Rust edit. The orchestrator re-runs `make preflight` and the parity suite.
