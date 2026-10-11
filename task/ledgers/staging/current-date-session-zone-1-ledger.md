# Unit ledger — CURRENT-DATE-SESSION-ZONE-1 · `current_date()` answers the session-zone date

**Date:** 2026-10-10 · **Branch:** `fix/current-date-session-zone-1` · **Base:** `2c33f289` (`origin/main`)
**Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Under a non-UTC session time zone `current_date()` answers the UTC date. With
`spark.sql.session.timeZone=Asia/Tokyo` at 19:38 UTC, Spark 4.1.2 answers 2026-10-11; RePark
main answers 2026-10-10, while `current_timestamp()` renders the session-zone wall clock. Same
on the batch door and both streaming doors (defect report, Opus verifier, 2026-10-10).

**Not in this unit:** `STATUS.md`, `.github/`, any `Cargo.toml` / pin change, `time_travel/microbatch_source.rs`
(MB-4's branch owns the micro-batch state snapshot), SQL `curdate()` resolution (R-1, open row).

## Step 0 — measured on main, before any product change (2026-10-10)

Spark oracle: live PySpark 4.1.2 (`/tmp/sparkenv/bin/python`, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`,
`local[1]`). RePark: base `2c33f289` via `make develop` (module 1.5.4). Run window 22:16–22:23 UTC:
UTC date 2026-10-10; `Pacific/Kiritimati` (+14) and `Asia/Tokyo` (+9) are on 2026-10-11 (differ),
`Pacific/Pago_Pago` (-11) is on 2026-10-10 (same-date control). Matrix: 2 engines × ANSI on/off ×
(session build with the zone, `conf.set` after a UTC start) × 4 zones. 22 compared cells per block,
352 compared cells; setup cells (`where_setup`, `insert_ddl`, `insert_run`, `insert_ns`) green on
both engines everywhere. The pattern is identical across ANSI modes and setting styles unless noted.

| Expression | Spark (UTC / same-date zone) | Spark (Kiritimati, Tokyo) | RePark main (every zone) | Verdict |
|---|---|---|---|---|
| `current_date()` | 2026-10-10 | 2026-10-11 | UTC date (2026-10-10) | DIFF in the 2 differing zones × 4 blocks |
| `current_date` (bare) | 2026-10-10 | 2026-10-11 | UTC date | DIFF, same 8 blocks |
| `curdate()` (SQL) | 2026-10-10 / 2026-10-11 | 2026-10-11 | `UNRESOLVED_ROUTINE` everywhere | DIFF all 16 blocks, other cause (R-1) |
| `CAST(current_timestamp() AS DATE)` | 2026-10-10 | 2026-10-11 | same as Spark | OK |
| `to_date(current_timestamp())` | 2026-10-10 | 2026-10-11 | same as Spark | OK |
| `date(now())` | 2026-10-10 | 2026-10-11 | same as Spark | OK |
| `current_timestamp()` | instant 22:17:45Z | instant 22:17:45Z | same instant (22:21:55Z run) | OK as instants (render tz differs, see N-2) |
| `now()` | same instant as `current_timestamp()` | same | same instant | OK as instants |
| `localtimestamp()` | 2026-10-10 22:17 UTC wall / local | Tokyo wall 2026-10-11 07:17 | Tokyo wall 2026-10-11 07:21 | OK |
| `current_timezone()` | zone name | zone name | zone name | OK all 16 blocks |
| `date_add(current_date(), 1)` | +1 of UTC date | 2026-10-12 | +1 of UTC date | DIFF, same 8 blocks (input is `current_date`) |
| `year/month/day(current_date())` | 2026/10/10 | 2026/10/11 | 2026/10/10 | DIFF, same 8 blocks |
| `datediff(current_date(), DATE '2026-01-01')` | 282 | 283 | 282 | DIFF, same 8 blocks |
| `trunc(current_date(), 'MM')` | 2026-10-01 | 2026-10-01 | 2026-10-01 | OK (same month both sides; control) |
| `unix_date(current_date())` | 20736 | 20737 | 20736 | DIFF, same 8 blocks |
| `SELECT current_date() a, current_date() b, now() n1, now() n2` | a==b; n1==n2 to the µs | a==b==10-11; n1==n2 | a==b==10-10; n1==n2 identical | dates DIFF same 8 blocks; per-query fix holds both |
| multi-row `VALUES (1),(2),(3)` + `current_date()` | constant per query | constant 10-11 | constant 10-10 | DIFF same 8 blocks; constant both |
| `F.current_date()` (DataFrame) | 2026-10-10 / 2026-10-11 | 2026-10-11 | UTC date | DIFF, same 8 blocks |
| `F.curdate()` (DataFrame) | 2026-10-10 / 2026-10-11 | 2026-10-11 | UTC date | DIFF, same 8 blocks |
| `WHERE d = current_date()` | matches UTC-today row | matches 10-11 row | matches UTC-today row | DIFF same 8 blocks |
| `COUNT(*) WHERE d <= current_date()` | 2 | 3 | 2 | DIFF same 8 blocks |
| `INSERT INTO <DATE col> SELECT current_date()`, read back | stored 10-11 (parquet) | stored 10-11 | stored 10-10 (Iceberg) | DIFF same 8 blocks |

120 of 352 compared cells differ: 104 share the one cause (13 `current_date`-rooted rows × the 8
differing-zone blocks), 16 are SQL `curdate()` (R-1). No UTC cell and no Pago_Pago cell differs
except SQL `curdate()`.

- **N-1, per-query stability.** Spark fixes the value per query: `n1 == n2` to the microsecond and
  `a == b`, constant across the three `VALUES` rows. RePark main does the same (`n1 == n2`
  identical micros); only the date leg is UTC-based. The fix must keep the per-query fix.
- **N-2, timestamp rendering.** Spark `collect()` renders LTZ in the driver-local zone (EDT here);
  RePark `to_arrow()` renders `timestamp[us, tz=UTC]`. Both carry the same instant (Spark
  18:17:45 EDT = 22:17:45Z; RePark 22:21:55Z, a later run). Not a divergence.
- **N-3, `conf.set` after start.** The `set` blocks match the `build` blocks on both engines: the
  batch door already honours a post-start zone change for every expression that reads the zone.
  The micro-batch snapshot issue stays with MB-4.
- **R-1, open row (other cause, not fixed).** SQL `curdate()` fails on main with
  `[UNRESOLVED_ROUTINE]` in every zone including UTC, while `F.curdate()` answers (as
  `current_date`). Missing SQL registration, not zone evaluation; fixing it would move UTC cells,
  which this unit forbids. Carried as clause C-006 OPEN.

## PROPOSITION LEDGER — CURRENT-DATE-SESSION-ZONE-1 — 2026-10-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Step-0 table above is the measured main behaviour: 352 compared cells, 104 same-cause diffs, 16 R-1 diffs, every other cell equal. | The 32 probe JSON files (kept outside the repo); this section. | PROVEN | Spark 4.1.2 live, RePark base `2c33f289`, 2026-10-10 22:16–22:23 UTC. pins: current-date-session-zone-1/C-001 |
| C-002 | The fix lands at the one place every door passes: `current_date` (parens, bare, DataFrame, `F.curdate`) answers the session-zone date in force when the query starts. | Step-1 cause paragraph in the hand-back; the diff. | PROVEN | DataFusion's builtin folds at plan time from `execution.time_zone`, which RePark never sets (parity doc declines that knob, D-B2), so it answered UTC under every zone. The owned `SparkCurrentDate` (`crates/repark-functions/src/current_date.rs`) folds the query start time through the `SessionTimeZoneConfig` carrier instead, `Stable`/`Date32`/`today` alias kept, no `return_field` delta. SQL text resolves the owned UDF wherever `register_all` ran (Spark facade sessions via the extension's `register`, `F.expr` fragments via the shared parse context); the DataFrame door builds it directly via `current_date::current_date()` (line-neutral dispatch rewire). The native SQL door never calls `register_all` and keeps the builtin (`current_timezone()` is `UNRESOLVED_ROUTINE` there, measured 2026-10-11). Streaming batch sessions clone the parent session state (read, not driven), so they resolve whichever `current_date` the parent registered. pins: current-date-session-zone-1/C-002 |
| C-003 | After the fix the 104 same-cause cells answer Spark, both setting styles, both ANSI modes, and the stored-value cell reads the session-zone date back. | `test_current_date_session_zone_1.py` + re-run of the step-0 matrix. | PROVEN | Matrix re-run on the fix: 320/320 checks against the zoneinfo-expected zone date (all 16 blocks; UTC date had rolled to 10-11, so the differing zone is Pago_Pago there). Facade file: 11 passed. The pre-existing strict-xfail TZ-9 pin flips to equality and the registry row reads FIXED. pins: current-date-session-zone-1/C-003 |
| C-004 | Under UTC every cell answers exactly as on main; no expression that matches Spark today moves; the per-query fix (N-1) still holds. | UTC control in the facade pins; Rust pins at the fix site. | PROVEN | Rust `sql_under_utc_answers_the_utc_date` + facade `test_utc_control_matches_the_utc_date` (both styles); `today_alias_and_repeat_reference_agree` + facade stability pin hold N-1; full libs green (functions 917, spark 2692, sql 394). `today()` under non-UTC is zone-aware now (it is the same UDF; nothing resolves it). pins: current-date-session-zone-1/C-004 |
| C-005 | Each pin fails for its reason: the fix reverted by hand turns the pins red, restored byte for byte after. | Revert-proof runs in the hand-back. | PROVEN | Revert = `simplify` reads `"UTC"` (one-spot hand edit): Rust 2 zone tests red (20737 vs 20736), facade 9 zone pins red with the 2 UTC controls green; `git checkout --` restore, tree clean of the edit. pins: current-date-session-zone-1/C-005 |
| C-006 | SQL `curdate()` resolves with Spark's answer. | None in this unit. | OPEN | R-1: other cause (missing registration, fails at UTC too); carried, not fixed. |
| C-007 | When the carrier is absent the owned fold reads `execution.time_zone` exactly as the builtin does (the zone date when set, UTC when unset); when the carrier is present it wins, even when set to UTC. | Rust fallback + control pins with a hand-revert run; the native-door facade pin red on the pre-fix module, green after the rebuild. | PROVEN | Fold 1, 2026-10-11. Presence, not value, tells the two apart: the Spark `configure` hook always installs the carrier (even for UTC), native sessions never do. Revert run: the fallback pin red (20737 vs 20736), both controls green; file restored byte for byte. Facade: new pin red on the pre-fix module (DataFrame 10-11 vs zone 10-10), file 12/12 green after the rebuild. pins: current-date-session-zone-1/C-007 |
| C-008 | A fixed-instant pin sits 30 minutes either side of local midnight in `Asia/Kolkata` and `America/New_York`, and goes red under a one-hour shift of the folded instant. | Verifier mutant M10 applied by hand and restored byte for byte. | PROVEN | Fold 1, 2026-10-11. M10 red on the Kolkata after-midnight leg (20736 vs 20737); the before-midnight legs catch the symmetric +1h shift. pins: current-date-session-zone-1/C-008 |

No `COVERAGE_ATTESTATION` is filed while C-006 is OPEN.

## Fold 1 — carrier-absent fallback + midnight pin (2026-10-11)

Verifier verdict FAIL on `0df026f0`: one S1 (F1), two S3s (F2, F3), one note (F4).
F1: on the native door after `SET TIME ZONE`, the DataFrame pair answered the UTC date
while native SQL answered the zone date; main answered the zone date on both. Cause: the
dispatch arm is shared by both doors and the owned UDF read only the carrier (absent on a
native session, so UTC); the native SQL door never calls `register_all`, so it kept the
builtin, which reads `execution.time_zone`. F2 is the ledger sentence corrected in C-002
above.

Ruling (made, not re-opened): the native `SET TIME ZONE` / `execution.time_zone` surface
must not move. Carrier absent means falling back to `execution.time_zone` exactly as the
builtin does (UTC when unset or unparsable); carrier present means it wins, as before.

The two states are told apart by carrier presence
(`options.extensions.get::<SessionTimeZoneConfig>()`), never by comparing values: the
Spark `configure` hook installs the carrier on every session, including an explicit-UTC
one, while no native session installs it — so an explicit-UTC Spark session with the knob
set elsewhere still answers UTC (pinned in both directions by
`carrier_present_wins_over_execution_time_zone`).

Pins: `carrier_absent_falls_back_to_execution_time_zone` (also asserts owned == builtin in
the same zone), `carrier_absent_without_execution_zone_answers_utc`,
`carrier_present_wins_over_execution_time_zone`, facade
`test_native_door_dataframe_agrees_with_sql_under_set_time_zone`. Revert runs are recorded
under C-007/C-008; every touched file was restored byte for byte (hash re-verified) after
its red run.

Mutant M10 (folded instant shifted one hour, survived the first pin set) is killed by
`thirty_minutes_either_side_of_local_midnight` (C-008). M8 (Stable to Volatile) still
survives, judged equivalent: `simplify` folds before volatility is consulted.

F4 (not a defect): on the facade the carrier wins over a user-set
`datafusion.execution.time_zone`, which is the Spark-consistent answer (main showed the
knob's date while `current_timezone()` said UTC). Fold 1 does not move the facade: the
carrier-present path keeps the first landing's logic.
