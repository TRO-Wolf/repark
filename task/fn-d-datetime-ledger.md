# Unit ledger — FN-D datetime

**Unit:** FN-D · conductor-13 Track T1 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-fnc` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-fnc` · **Branch:** `grok/fn-d-datetime` ·
**Base (FROZEN):** `cd0db4f459e62994b45f8aadd1d5b58f040d90a5`
(independent of FN-C #115; no FN-C names imported).

**Charter:** `FN-MANIFEST.md` FN-D GO-18 + conductor-13 Addendum A1–A12.
**SEPMO:** acc + C4. Floor S1. `max_cycles=2`. Sequential hat-switch.

Registry / STATUS / lockfiles / `.github` / board / `crates/` closed.

## GO / deferred

| Name | Class | Disposition |
|---|---|---|
| day | ALIAS dayofmonth | SHIPPED |
| curdate | ALIAS current_date | SHIPPED |
| now | ALIAS current_timestamp | SHIPPED |
| dateadd | ALIAS date_add | SHIPPED |
| datepart | ALIAS date_part | SHIPPED |
| to_unix_timestamp | ALIAS unix_timestamp | SHIPPED as alias of the R-FN-BATCH1 UOE (printf pattern) |
| unix_date | THIN-WIRE | SHIPPED as SHIM `CAST(date AS INT)` (SparkUnixDate simplify) |
| unix_seconds | THIN-WIRE | SHIPPED as toward-zero of `CAST(ts AS DOUBLE)` — not TZ-5 floor |
| unix_millis | THIN-WIRE | SHIPPED as toward-zero of `CAST(ts AS DOUBLE) * 1000` |
| unix_micros | THIN-WIRE | **DEFERRED** — no `call_scalar` arm; f64×1e6 is not bit-exact; `date_part(microsecond)` reconstruct fails on negatives |
| make_date | THIN-WIRE | **DEFERRED** — no `call_scalar` arm; `F.expr` cannot bind columns |
| make_interval | THIN-WIRE | **DEFERRED** — no `call_scalar` arm; `collect` of MonthDayNano is NIY |
| make_dt_interval | THIN-WIRE | **DEFERRED** — no `call_scalar` arm |
| date_from_unix_date | SHIM | SHIPPED `date_add(lit(date(1970,1,1)), n)` |
| current_timezone | SHIM | SHIPPED foldable session `spark.sql.session.timeZone` (no env reads) |
| date_diff | SEMANTIC-HAZARD | **DEFERRED** — cannot pin 2-arg DATE form equal to `datediff` without replacing the `functions_expr` UOE (would red out-of-fence `test_fn_batch1.py`) |
| localtimestamp | SEMANTIC-HAZARD | **DEFERRED** — SQL `localtimestamp` missing; `CAST(... AS timestamp_ntz)` not on the facade allowlist (`column.py` closed) |
| to_timestamp_ntz | SEMANTIC-HAZARD | **DEFERRED** — same NTZ producer gap |
| make_timestamp_ltz / make_timestamp_ntz | ENGINE-WORK | **DEFERRED** (charter) |
| make_ym_interval | ENGINE-WORK | **DEFERRED** (charter) |
| to_timestamp_ltz | ENGINE-WORK | **DEFERRED** (charter) |
| convert_timezone | ENGINE-WORK | **DEFERRED** (charter) |
| timestamp_add / timestamp_diff | ENGINE-WORK | **DEFERRED** (charter) |

`_PRE_SPLIT_ALL` pin move: 253 → 264 (11 shipped names). Declared in the PR body.

## ACC

- Risk tier: standard. `max_cycles=2`. `severity_floor=S1`. acc + C4.
- Cycle 1 Critic-1 Q-001 S2: `unix_millis` is `CAST(ts AS DOUBLE) * 1000` toward-zero —
  f64 mantissa holds millis through practical years; not bit-exact at extreme
  instants. ACCEPTED_FLAGGED (below floor; `unix_micros` deferred for this reason).
- Cycle 1 Critic-1 Q-002 S2: `now()` ≡ `current_timestamp()` same-select equality
  could flake if the engine minted two instants. Probe + pin showed one statement
  timestamp. ACCEPTED_FLAGGED.
- Critic-2 CLEAN: no env reads (`$TZ` pin); zone baked via `lit()` not SQL concat;
  epoch date is a fixed `DATE '1970-01-01'` literal.
- C4: 11/11 shipped in `__all__`; 14 deferred names absent; no `crates/` / lockfile
  / FN-C imports; `_PRE_SPLIT_ALL` 253→264; `functions_datetime.py` 119/2500;
  `functions.py` 1820/2500. No EXCEPTIONS raise.
- Cycle 2: CLEAN (no S1 remediations). Early-stop.
- Label: `ACC-CONVERGED`.
- `make verify` exit **0** (2026-08-15).
- `make preflight` exit **0**. Facade pytest: **3147 passed, 71 skipped**
  (`test_functions_d.py` + split-identity included). `make audit` +
  `make workflows-lint` green (inside preflight).

## Files

- `python/repark/src/repark/spark/functions_datetime.py` — defs (new sibling)
- `python/repark/src/repark/spark/functions.py` — late import + `__all__` only
- `python/repark/tests/test_functions_d.py` — Arrow value+type pins
- `python/repark/tests/test_functions_split_identity.py` — pin move
- `python/repark/src/repark/spark/map.md`, `python/repark/tests/map.md`, `task/map.md`

## Mutation-proof pins (name the test that reds if the def is dropped)

| Behavior | Test |
|---|---|
| `day` ≡ `dayofmonth` | `test_day_alias_of_dayofmonth` |
| `curdate` ≡ `current_date` | `test_curdate_alias_of_current_date` |
| `now` ≡ `current_timestamp` | `test_now_alias_of_current_timestamp` |
| `dateadd` ≡ `date_add` | `test_dateadd_alias_of_date_add` |
| `datepart` ≡ `date_part` | `test_datepart_alias_of_date_part` |
| `to_unix_timestamp` ≡ `unix_timestamp` UOE | `test_to_unix_timestamp_aliases_unix_timestamp_loud_gap` |
| `unix_date(1970-01-02) == 1` | `test_unix_date_days_since_epoch` |
| `unix_seconds(-1.5s) == -1` not `-2` | `test_unix_seconds_truncates_toward_zero_not_floor` |
| `unix_millis(±1.5s)` | `test_unix_millis_truncates_toward_zero` |
| `date_from_unix_date` epoch offset | `test_date_from_unix_date_epoch_offset` |
| unix_date ↔ date_from_unix_date | `test_unix_date_round_trips_date_from_unix_date` |
| current_timezone default UTC / `$TZ` ignored | `test_current_timezone_default_is_utc_not_host_tz` |
| current_timezone builder zone | `test_current_timezone_follows_session_builder_zone` |
| current_timezone foldable beside `sum` | `test_current_timezone_stays_string_beside_an_aggregate` |
| deferred names absent | `test_fn_d_deferred_names_are_absent` |
| `__all__` pin 264 | `test_functions_all_matches_pre_split_inventory` |
