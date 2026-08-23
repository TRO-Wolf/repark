# Unit ledger — TA-4 volume-family kernel port

**Unit:** TA-4 · **Date:** 2026-08-15 ·
**Lane:** T2 / conductor-13 · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-ta` · **Branch:** `grok/ta4-volume-kernels` ·
**Base:** `da643e0df4ea80b36cdc446d7a15ecf1f8200fd6` (TA-3 #123 tip)
**Charter:** `planning/grok/BRIEF-ta-wave-13.md` TA-4 + TA-3-COMPLETE.md porting notes.
**Engine:** octo + C4 (numerics — `to_bits` is the judge). Floor S1. Sequential hat-switch.

Stacked on TA-3; isolation=none. Does **not** rewrite TA-3 goldens/recorder.

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | Port `ad`/`adosc`/`obv`/`mfi` in new `crates/repark-ta/src/volume.rs`. | PROVEN |
| C-002 | Numerics contract: statement-order C mirror, no FMA, incremental accumulators, `TA_IS_ZERO` only where C uses it (none of these four), NaN lookback prefix, `unsafe` forbidden. | PROVEN — C4 goldens |
| C-003 | Follow TA-3 porting notes exactly (AD CLV order / `tmp > 0.0`; ADOSC no standalone `ema`, PER_TO_K, seed both EMAs; OBV first output = volume[0]; MFI not Wilder, neg-first, hard `< 1.0`, no clamp). | PROVEN — notes + goldens |
| C-004 | SPECS +4, all single-output. Facade fns in `ta.py` with polars_talib keywords. | PROVEN |
| C-005 | Contract tests (short/empty/param) + `to_bits` goldens from TA-3 + UDF wiring tests. NEW `test_ta_volume.py` (no edit of `test_ta.py`). | PROVEN |
| C-006 | Honest cut: any kernel that will not converge ships NOTHING for that name. | PROVEN — all four converged; no cut |
| C-007 | Do not change existing kernels except SPECS/facade additions. No TA-3 golden rewrite. | PROVEN — diff names |
| C-008 | `make verify` then `make preflight` before `gh pr create`. PR base `grok/ta3-volume-goldens`. Do not merge. | PROVEN — §4 |

---

## 0. Porting (C4)

C source (read-only, not committed): `/tmp/grok-ta-reference/extracted/ta-lib/src/ta_func/`.
Porting notes from TA-3-COMPLETE.md are BINDING.

| Kernel | Lookback | Converged (walk + flat `to_bits`) |
|---|---|---|
| `ad` | 0 | YES — `ad.bin` / `flat_ad.bin` |
| `adosc(3,10)` | 9 | YES — `adosc_3_10.bin` / `flat_adosc_3_10.bin` |
| `obv` | 0; `[0] = volume[0]` | YES — `obv.bin` / `flat_obv.bin` |
| `mfi(14)` | 14 | YES — `mfi_14.bin` / `flat_mfi_14.bin` |

Honest cut: **none**.

Shared `calculate_ad` is C's `CALCULATE_AD` increment (`tmp > 0.0`, CLV order). ADOSC inlines the EMA as `(k*ad)+(one_minus_k*ema)` after seeding both with the first AD — does not call `ema()`. MFI rolling buffer + classify-neg-first + hard `< 1.0`.

## 1. Surface

- Kernels: `repark_ta::{ad, adosc, obv, mfi}` (68 public kernels; was 64).
- Window UDFs: `ta_ad` / `ta_adosc` / `ta_obv` / `ta_mfi` (81 entry points; was 77). All single-output.
- Facade: `ta.ad` / `ta.adosc(..., fastperiod=, slowperiod=)` / `ta.obv` / `ta.mfi(..., timeperiod=)`.

## 2. Tests

- `volume.rs` unit: CLV order vs `(2c-h-l)` rewrite, flat-bar hold, OBV seed/equal-close, ADOSC lookback + inverted periods, MFI neg-first + `< 1.0` → 0.0, empty, length mismatch.
- `tests/goldens.rs`: `volume_family_matches_c_talib` + `volume_family_flat_guard_branches_match_c_talib`.
- `tests/contract.rs`: `mfi` in `kernels()`; `volume_family_argument_contract` for AD/OBV/ADOSC.
- `udf.rs`: SPECS + `compute_volume_kernels_match_the_kernel` + lookup names.
- `python/repark/tests/test_ta_volume.py`: DataFrame door `to_bits` vs goldens.

## 3. Files

| Path | Role |
|---|---|
| `crates/repark-ta/src/volume.rs` | NEW kernels |
| `crates/repark-ta/src/lib.rs` | module + re-exports |
| `crates/repark-ta/src/udf.rs` | SPECS + TaFn + compute + wiring test |
| `crates/repark-ta/tests/goldens.rs` | `to_bits` vs TA-3 bins |
| `crates/repark-ta/tests/contract.rs` | short/empty/param |
| `python/repark/src/repark/spark/ta.py` | facade + `__all__` |
| `python/repark/tests/test_ta_volume.py` | NEW facade suite |
| `crates/repark-ta/map.md` + `src/map.md` + `tests/map.md` | 68/81 + volume.rs |
| `python/repark/src/repark/spark/map.md` + `tests/map.md` | facade + test row |
| `task/ta4-volume-kernels-ledger.md` + `task/map.md` | this ledger |

CLOSED (untouched): TA-3 goldens/recorder, `test_ta.py`, `ta_window.rs`, `ta_toll.rs`, lockfiles, STATUS, registry, functions package.

## 4. Gates (real exit codes)

| Gate | Exit |
|---|---|
| `make verify` | **0** |
| `make preflight` | **0** |
| `make py-test-facade` (inside preflight) | **0** (3125 passed, 71 skipped; +6 vs TA-3's 3119) |
| `make audit` | **0** |
| `make workflows-lint` | **0** |
| pre-commit hook | fires (`check_map_md` / crate-dag / lib-rs / file-size / lib-py / manifest / fmt / taplo / typos) |

`udf.rs` 2167 / 2200 ceiling. `ta.py` 1671 / 2500.

## 5. ACC

Sequential octo + C4. `to_bits` is the judge. All four names converged on both fixtures (walk 5000 + flat 600). No honest cut.

**Label: `ACC-CONVERGED`.**
