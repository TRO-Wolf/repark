# Unit ledger — FN-F try / session / bitwise

**Unit:** FN-F · conductor-13 Track T1 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-fnc` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-fnc` · **Branch:** `grok/fn-f-try-bitwise` ·
**Base (FROZEN):** `cd0db4f459e62994b45f8aadd1d5b58f040d90a5`
(independent of FN-C #115 / FN-D #119 / FN-E #122).

**Charter:** `FN-MANIFEST.md` FN-F GO-22 + conductor-13 A7/A8.
**SEPMO:** acc + C4. Floor S1. `max_cycles=2`.

Registry / STATUS / lockfiles / `.github` / board / `crates/` closed.

## GO / deferred

| Name | Class | Disposition |
|---|---|---|
| bitwise_not | THIN-WIRE | SHIPPED as SHIM `bitwiseXOR(lit(-1))` (`call_scalar` has no arm; `~` is boolean NOT) |
| bitwiseNOT | ALIAS bitwise_not | SHIPPED (lands with bitwise_not) |
| broadcast | SHIM | SHIPPED — DataFrame `hint("broadcast")` no-op + ColumnOrName identity |
| current_user / user | SHIM | SHIPPED — foldable `"repark"` (ADR-0004: no env / OS-user read) |
| current_catalog | SHIM | SHIPPED — foldable snapshot of `Catalog.current_catalog` |
| current_database / current_schema | SHIM | SHIPPED — foldable snapshot of `Catalog.current_database` |
| version | SEMANTIC-HAZARD | SHIPPED — `repark-<pep440>` matching `session.version`, **not** DF `version()` |
| uuid | THIN-WIRE | SHIPPED via `PyColumn.sql("uuid()")` (nullary; pin type + uniqueness) |
| bit_count | THIN-WIRE | **SHIPPED FN-GT1** (2026-08-17); GT1-FIX 2026-08-18 |
| getbit | ALIAS bit_get | **SHIPPED FN-GT1** (2026-08-17); GT1-FIX 2026-08-18 |
| shiftleft / shiftright / shiftrightunsigned | THIN-WIRE | **SHIPPED FN-GT1** (2026-08-17); GT1-FIX 2026-08-18 |
| shiftLeft / shiftRight / shiftRightUnsigned | ALIAS | **DEFERRED** — land together with the snake names (A8) |
| try_sum | THIN-WIRE | **DEFERRED** — no `call_scalar` / aggregate arm; overflow→NULL is engine |
| assert_true | SHIM | **DEFERRED** — `raise_error` is construction-time UOE, not an evaluable Column |
| try_add / try_avg / try_divide / try_multiply / try_subtract / try_mod | ENGINE-WORK | **DEFERRED** (charter) |
| try_element_at | ENGINE-WORK | **DEFERRED** (charter) |
| try_to_date / try_to_number / try_to_binary | ENGINE-WORK | **DEFERRED** (charter) |
| to_number / to_binary | ENGINE-WORK | **DEFERRED** (charter) |

`_PRE_SPLIT_ALL` pin move: 253 → 263 (10 shipped names). Declared in the PR body.

## ACC

- Risk tier: standard. `max_cycles=2`. `severity_floor=S1`. acc + C4.
- Cycle 1 Critic-1: `call_scalar` miss on shift/bit_count/getbit/try_sum — deferred
  (A8). `~` is boolean NOT — remediable via `bitwiseXOR(-1)` (values pinned).
  `raise_error` construction UOE — `assert_true` deferred. DF `version()` is
  DataFusion — remediable as `repark-<pep440>`.
- Critic-1 Q-002 S2: composed SHIM display `bitwise_not(x)` forced; `user` shares
  `current_user()` display. ACCEPTED_FLAGGED (below floor).
- Critic-2: `broadcast(123)` no longer fail-opens to `lit(123)` (`NOT_COLUMN_OR_STR`).
  Catalog snapshots are Session-only (no env).
- C4: 10/10 shipped in `__all__`; deferred names `hasattr` False; no `crates/` /
  lockfile / STATUS / registry edits; no FN-C/D/E names imported.
- Label: `ACC-CONVERGED`.

## Gates

- `make verify` green (exit 0).
- `make preflight` green (exit 0). Facade pytest: **3138 passed, 71 skipped**
  (`test_functions_f.py` + split-identity included).
- `lib-py`: 60 files clean (new siblings under default 2500; no EXCEPTIONS).

## Files

- `python/repark/src/repark/spark/functions_bitwise.py` — bitwise defs
- `python/repark/src/repark/spark/functions_session.py` — session / uuid / broadcast / version
- `python/repark/src/repark/spark/functions.py` — late import + `__all__` only
- `python/repark/tests/test_functions_f.py` — Arrow value+type pins
- `python/repark/tests/test_functions_split_identity.py` — pin move
- `python/repark/src/repark/spark/map.md`, `python/repark/tests/map.md`, `task/map.md`

## Mutation-proof pins (name the test that reds if the def is dropped)

| Behavior | Test |
|---|---|
| `bitwise_not` / `bitwiseNOT` values + int64 | `test_bitwise_not_and_alias` |
| `broadcast` DataFrame + column identity | `test_broadcast_dataframe_and_column_are_identity` |
| `current_user` / `user` type + stability | `test_current_user_and_user_are_stable_repark_string` |
| catalog / database / schema track Session | `test_current_catalog_database_schema_track_session` |
| `version` is repark string, not DataFusion | `test_version_is_repark_string_not_datafusion` |
| `uuid` string + uniqueness | `test_uuid_type_and_uniqueness` |
| deferred names absent | `test_deferred_fn_f_names_are_absent` |
