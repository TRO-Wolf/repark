# Unit ledger — FN-A ordering / null / math

**Unit:** FN-A · conductor-12 Track T3 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-fna` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-fna` · **Branch:** `grok/fn-a-ordering-null-math` ·
**Base (FROZEN):** `a2b385f4113a725a3b013553d2ee99fcf8278cfb`
(FN-SPLIT already on this SHA).

**Charter:** `FN-MANIFEST.md` FN-A GO-32 + conductor-12 Addendum A6/A7 +
`BRIEF-fn-wave.md` per-function contract. **SEPMO:** acc + C4. Floor S1.
`max_cycles=2`.

Registry / STATUS / lockfiles / `.github` / board / `crates/` closed.

## GO / deferred

| Name | Class | Disposition |
|---|---|---|
| sign | ALIAS signum | SHIPPED (`_scalar("sign")` — already on the `call_scalar` arm) |
| ifnull | ALIAS coalesce 2-arg | SHIPPED |
| nvl | ALIAS coalesce 2-arg | SHIPPED |
| asc | SHIM `Column.asc` (nulls first) | SHIPPED |
| desc | SHIM `Column.desc` (nulls last) | SHIPPED |
| asc_nulls_first | ALIAS asc | SHIPPED |
| desc_nulls_last | ALIAS desc | SHIPPED |
| e | SHIM foldable `exp(1)` | SHIPPED — not bare `lit(math.e)` (SQL global-agg DECIMAL trap; Critic-1 Q-001) |
| pi | THIN-WIRE DF `pi` | SHIPPED via foldable `PyColumn.sql("pi()")` (`call_scalar` has no `pi` arm) |
| negative | THIN-WIRE | SHIPPED as `-(col)` (`Column.__neg__` already displays `negative(x)`) |
| positive | SHIM unary plus = identity | SHIPPED |
| pmod | THIN-WIRE | SHIPPED as SHIM `((a % n) + n) % n` (`call_scalar` has no `pmod` arm) |
| expm1 | THIN-WIRE | SHIPPED as SHIM `exp(col) - 1` |
| ln | ALIAS log | SHIPPED (`_scalar("ln")`) |
| log2 | SHIM `log/log(2)` | SHIPPED (no `call_scalar` `log2` arm) |
| log1p | SHIM `log(1+col)` | SHIPPED |
| degrees | SHIM via `pi()` | SHIPPED |
| radians | SHIM via `pi()` | SHIPPED |
| nvl2 | THIN-WIRE | SHIPPED as SHIM `when(~isnull(c1), c2).otherwise(c3)` |
| nullif | THIN-WIRE | SHIPPED as SHIM `when(c1 == c2, None).otherwise(c1)` |
| equal_null | SHIM `Column.eqNullSafe` | SHIPPED |
| zeroifnull | SHIM `coalesce(col, lit(0))` | SHIPPED |
| nullifzero | SHIM `nullif(col, lit(0))` | SHIPPED |
| isnotnull | SHIM `~isnull(col)` | SHIPPED |
| cbrt | SHIM `pow(col, 1/3)` | SHIPPED with negative arm `-pow(-col, 1/3)` (IEEE `pow` is NaN on negatives) |
| rint | THIN-WIRE | **SHIPPED FN-GT1** (2026-08-17); GT1-FIX 2026-08-18 |
| factorial | THIN-WIRE | **SHIPPED FN-GT1** (2026-08-17); GT1-FIX 2026-08-18 |
| bin | THIN-WIRE | **SHIPPED FN-GT1** (2026-08-17); GT1-FIX 2026-08-18 |
| hex | THIN-WIRE | **SHIPPED FN-GT1** (2026-08-17); GT1-FIX 2026-08-18 |
| unhex | THIN-WIRE | **SHIPPED FN-GT1** (2026-08-17); GT1-FIX 2026-08-18 |
| asc_nulls_last | SEMANTIC-HAZARD | **DEFERRED** — `_sort_specs` sets `nulls_first = ascending` and ignores `_sort_nulls_first`; shipping a Column flag that `orderBy` drops is a lie. Out of T3 file grant (`dataframe/core.py`). |
| desc_nulls_first | SEMANTIC-HAZARD | **DEFERRED** — same |
| typeof | ENGINE-WORK | **DEFERRED** (charter; do not implement) |
| bround | ENGINE-WORK | **DEFERRED** (charter; do not implement) |
| conv | ENGINE-WORK | **DEFERRED** (charter; do not implement) |

`_PRE_SPLIT_ALL` pin move: 207 → 232 (25 shipped names). Declared in the PR body.

## ACC

- Risk tier: standard. `max_cycles=2`. `severity_floor=S1`. acc + C4.
- Cycle 1 Critic-1 Q-001 S1: `F.e()` as `lit(math.e)` became `decimal128` on
  `select(sum, e())`. REMEDIATED — `exp(1)` + `test_e_stays_double_beside_an_aggregate`.
- Critic-1 Q-002 S2: composed SHIMs keep operator/CASE display (not `pmod(a,b)`).
  ACCEPTED_FLAGGED (below floor; value+type pins are the charter test surface).
- Critic-2: CLEAN (no injection on `PyColumn.sql("pi()")`; `pmod(…,0)` fail-loud ANSI).
- C4: 25/25 shipped in `__all__`; 10 deferred absent; no `crates/` / lockfile edits.
- Label: `ACC-CONVERGED`.

## Files

- `python/repark/src/repark/spark/functions_expr.py` — defs
- `python/repark/src/repark/spark/functions.py` — late import + `__all__` only
- `python/repark/tests/test_functions_a.py` — Arrow value+type pins
- `python/repark/tests/test_functions_split_identity.py` — pin move
- `python/repark/src/repark/spark/map.md`, `python/repark/tests/map.md`, `task/map.md`

## Mutation-proof pins (name the test that reds if the def is dropped)

| Behavior | Test |
|---|---|
| `sign` value+type ≡ `signum` | `test_sign_alias_of_signum` |
| `ifnull` / `nvl` fill | `test_ifnull_and_nvl_are_two_arg_coalesce` |
| `ln` ≡ `log` | `test_ln_alias_of_log` |
| `asc`/`desc` null placement | `test_asc_and_desc_null_ordering` |
| alias null placement | `test_asc_nulls_first_and_desc_nulls_last_are_aliases` |
| `e` / `pi` constants | `test_e_and_pi_are_foldable_constants` |
| `e()` stays DOUBLE beside `sum` | `test_e_stays_double_beside_an_aggregate` |
| `negative` / `positive` | `test_negative_and_positive` |
| `pmod(-10, 3) == 2` | `test_pmod_positive_remainder` |
| `expm1` / `log2` / `log1p` | `test_expm1_ln_log2_log1p` |
| `degrees(pi) == 180` | `test_degrees_and_radians` |
| `cbrt(-8) == -2` | `test_cbrt_real_root_including_negatives` |
| `nvl2` | `test_nvl2_picks_present_or_absent` |
| `nullif` / `nullifzero` | `test_nullif_and_nullifzero` |
| `equal_null` NULL=NULL | `test_equal_null_is_null_safe` |
| `zeroifnull` / `isnotnull` | `test_zeroifnull_and_isnotnull` |
| `__all__` pin 232 | `test_functions_all_matches_pre_split_inventory` |
