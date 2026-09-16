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

## 2. Rulings and design record

- **Q-15c-3** (owner, 2026-09-15): runtime `SET` and `spark.conf.set` of the two knobs apply through a per-query config snapshot.
- **Q-15c-4** (owner, 2026-09-15): size baselines ratchet down only; a one-time +N is allowed only in the PR that needs it, named in the ledger. This unit needs none: `session.rs` (binding, exact 1127) is untouched — the new binding methods ride a second `#[pymethods]` block in the new `session_runtime.rs`; `session.rs` (core, 988/1000) moves +6; every Python session file shrinks except the two-line coordination above; `session_core.py` stays 2290.
- **D-1** (orchestrator): live values behind one lock-guarded snapshot, `Session::set_runtime_config` validates in Rust and writes the DataFusion `SessionContext` state's `ConfigOptions`, `RESET` restores the builder value. Two forced deviations, both measured: (a) core cannot name the carrier types (repark-core and repark-functions are dependency-disjoint by the crate DAG), so the `ConfigOptions` write lives in a new binding-crate module `session_runtime.rs` that sees both crates; core owns the zone snapshot plus the runtime zone parser, the carriers own their value parsers, the binding only forwards. (b) The runtime zone parser is NOT `SessionTimeZone::parse`: Arrow `Tz::from_str` accepts `+18:01` (Spark refuses, S5-tz-plus1801) and refuses `+5` / `GMT+8` (Spark accepts, S5-tz-plus5 / S5-tz-gmt8), so a Java-`ZoneId`-semantics mirror (`parse_runtime_value`, verdict-equivalent with the facade validator it replaces) is the only gate that keeps every pinned S5 cell green. The builder parser is frozen.
- **Snapshot timing** (addendum, batch 16, overrides D-1's measure-one-cell): ANSI binds when a frame is analysed (S16-0 keeps `DIVIDE_BY_ZERO`); zone value expressions (`from_unixtime`, casts) answer the frame-build zone; `current_timezone()` folds at collect. DataFusion `DataFrame`s hold the `sql()`-time `SessionState` clone, so writing the live state gives the first two for free; `current_timezone()` on a STALE frame therefore answers the build zone, not the new one — a sanctioned narrow residue row plus a pin of today's answer (see C-002).
- **Unset applies the get-visible value** (CONF-UNSET-1 authority): `unset` keeps its tombstone; the engine is then set to `_SQLCONF_DEFAULTS[key]` (`true` / `UTC`), which is what `get` reports. `RESET` with a builder value goes through the lenient restore entry (builder tokens such as `1`/`yes` parsed at build must re-apply without tripping the strict runtime gate).
- **Round 2 (BL-11, C-006) is OPEN and out of this step**: numeric→BINARY cast legality (`analyzer.rs` `cast_legality`, no ANSI input today) needs an analyzer change outside this card's fence. The runtime ANSI carrier it will read is this step's deliverable.
- **Rust first**: all validation, parsing and state writes are Rust. Python is storage plus forwarding. No Python-only implementation, so no ledger reason is owed.

## PROPOSITION LEDGER — set-ansi-runtime-1 round 1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `conf.set("spark.sql.ansi.enabled","false")` then fresh `SELECT 1/0` answers NULL (double, nullable); the same through `SET`; `true` restores `DIVIDE_BY_ZERO`; `F.lit(1)/F.lit(0)` on a post-SET frame follows; a frame built pre-SET keeps the raise (S16-0). | `test_s16_0_stale_frame_keeps_divide_by_zero`, `test_s16_1_fresh_division_answers_null_after_set`, `test_s16_2_cast_x_answers_null_after_set`, `test_s16_3_conf_set_true_restores_raise`, `test_s16_4_unset_with_builder_true_raises`, `test_c001_f_api_division_follows_runtime_ansi`, `test_btz5_7_8_set_false_then_null`, `test_btz5_9_10_reset_then_raise` | **OPEN** | Red run below. |
| C-002 | `conf.set("spark.sql.session.timeZone","Asia/Tokyo")` then `current_timezone()`, `from_unixtime(0)`, `CAST(TIMESTAMP … AS STRING)` follow the new zone on both doors; `SET TIME ZONE` the same; invalid zone refuses `INVALID_CONF_VALUE.TIME_ZONE` at the SET with nothing stored; `conf.get` reports the applied value. S16-6 pins exactly, including the one-element `current_timezone()` residue. | `test_s16_6_stale_frame_split_binding`, `test_s16_7_fresh_query_follows_new_zone`, `test_s16_8_conf_set_zone_applies`, `test_s16_9_conf_set_invalid_zone_refuses`, `test_s16_10_set_ansi_maybe_refuses`, `test_s16_11_reset_zone_restores_builder`, `test_c002_f_api_zone_follows`, `test_btz5_2_3_set_zone_then_current_timezone`, `test_btz5_5_refused_set_moves_nothing`, `test_s5_zone_cells_apply` | **OPEN** | Red run below. |
| C-003 | `RESET` / `conf.unset` restore the builder-seeded value, applied; with no builder value the registered default applies (`true` / `UTC`). | `test_s16_5_get_after_unset_reports_builder_true`, `test_reset_restores_builder_ansi_applied` (in `test_sql_set_door_1.py` flip), S16-4/S16-11 | **OPEN** | Red run below. |
| C-004 | The native ANSI door (`repark.sql()`) is unaffected by Spark-session runtime sets. | `test_c004_native_door_ignores_spark_runtime_sets` | **OPEN** | Red run below. |
| C-005 | Registry TZ-3 and SET-ANSI-RUNTIME-1 read FIXED with pins; every pin that codified the old residue flips in place. | Flipped pins in `test_sql_set_door_1.py` (module docstring + 9 tests) and `test_session_timezone_parity.py` (2 tests), registry rows, `make verify` rc 0 | **OPEN** | Red run below. |
| C-006 | Round 2: BL-11 numeric→BINARY under runtime ANSI off. | — | **OPEN** | Out of this step by card order; prerequisite (runtime ANSI carrier) delivered here. |

VERDICT (2026-09-15, ledger creation): 6 clauses, 0 PROVEN, 6 OPEN, 0 REJECTED.

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

## Gates

(TBD — commands with real exit codes as each clause group lands.)
