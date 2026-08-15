# Unit ledger — TA-3 volume-family goldens + C-source recon

**Unit:** TA-3 · **Date:** 2026-08-15 ·
**Lane:** T2 / conductor-13 · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-ta` · **Branch:** `grok/ta3-volume-goldens` ·
**Base (FROZEN):** `cd0db4f459e62994b45f8aadd1d5b58f040d90a5`
**Charter:** `planning/grok/BRIEF-ta-wave-13.md` TA-3 + conductor-13 A12.
**Engine:** recon + acc. Floor S1. Sequential: recon (C source + recorder) →
Actor → Critic-1 → Critic-2.

This ledger does **not** edit `crates/repark-ta/src/**`, `ta.py`, `test_ta.py`,
`ta_window.rs`, `ta_toll.rs`, lockfiles, STATUS, or the parity registry. Kernel
ports are TA-4.

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | Recorder extended with `ad` / `adosc` / `obv` / `mfi` on both fixtures. | PROVEN — `walk_cases` + `flat_cases` |
| C-002 | Wrapper 0.1.5 / C 0.4.0 asserts unchanged; recorder refuses otherwise. | PROVEN — `EXPECTED_*` constants untouched |
| C-003 | Positive `volume` on both fixtures; dedicated RNGs; OHLC/periods bytes unchanged. | PROVEN — seeds 4242 / 77; sha256 of existing fixture bins match freeze |
| C-004 | TA-Lib 0.4.0 C source fetched to `/tmp/grok-ta-reference/` (never committed). | PROVEN — `ta-lib-0.4.0-src.tar.gz` sha256 `9ff41efc…0a651`; four `ta_*.c` extracted |
| C-005 | Porting note per function (loop, accumulators, lookback, Wilder-order hazards). | PROVEN — §0 + TA-3-COMPLETE.md |
| C-006 | No kernel / facade / SQL-door edits. | PROVEN — diff names |
| C-007 | Additive goldens only; existing indicator `.bin` files unmodified. | PROVEN — `git diff --name-only` on goldens/ is `manifest.json` + `map.md` + 10 new bins |
| C-008 | `make verify` exit 0. `make preflight` before `gh pr create`. | PROVEN — §4 |
| C-009 | map.md lockstep + this ledger linked from `task/map.md` in the same change. | PROVEN — listed in §2 |

---

## 0. Recon (C source + recorder)

### C tarball

- URL: `https://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz`
- Path (OUTSIDE repo): `/tmp/grok-ta-reference/ta-lib-0.4.0-src.tar.gz`
- Size: 1 330 299 bytes. gzip, last-modified 2007-09-15. sha256
  `9ff41efcb1c011a4b4b6dfc91610b06e39b1d7973ed5d4dee55029a0ac4dc651`
- Extracted: `ta_AD.c`, `ta_ADOSC.c`, `ta_OBV.c`, `ta_MFI.c` under
  `extracted/ta-lib/src/ta_func/`. **Never committed.**

### Recorder / oracle

- `polars_talib` 0.1.5 bundles C TA-Lib `0.4.0 (Jan 24 2025 12:01:05)`.
- API (explicit args, defaults): `ad(h,l,c,v)`, `adosc(..., fastperiod=3,
  slowperiod=10)`, `obv(c,v)`, `mfi(..., timeperiod=14)`.
- Existing OHLC fixtures use `default_rng(42)` (walk) and `default_rng(7)`
  (flat). `periods` already avoids the OHLC RNG. Volume must do the same.

### Volume generator (documented)

| Fixture | OHLC seed | Volume seed | Formula |
|---|---|---|---|
| 5000-row walk | 42 | **4242** | `exp(Normal(ln(1e6), 0.35))` |
| 600-row plateau | 7 | **77** | same |

Strictly positive. Volume is **not** flattened on the price plateau — the
guards (AD `tmp > 0.0`, OBV equal-close, MFI zero typical-price delta) fire
from **price** being flat, with real positive volume still present.

### Porting notes (for TA-4; no kernels here)

**`AD` (`ta_AD.c`)** — Chaikin A/D Line. Lookback **0**.
- Accumulator `ad = 0.0`; every bar emits the running total.
- `tmp = high - low`; increment **only** `if (tmp > 0.0)` — a strict `> 0.0`,
  **not** `TA_IS_ZERO`. Plateau `high == low` re-emits `ad` unchanged.
- CLV statement order is load-bearing:
  `ad += (((close-low)-(high-close))/tmp)*((double)volume)`.
  Do **not** rewrite as `(2*close-high-low)/tmp` (different rounding). No FMA.
- Incremental cumulative sum — do not recompute from bar 0 each row (drift).

**`ADOSC` (`ta_ADOSC.c`)** — Chaikin A/D Oscillator. Lookback
`LOOKBACK_CALL(EMA)(slowestPeriod)` = `slowest - 1` (unstable 0) → **9** at
defaults (3, 10).
- Inlines the same `CALCULATE_AD` macro as `AD` (`tmp > 0.0`).
- **Do not call the standalone `ema()` kernel.** C seeds **both** EMAs with
  the first AD value (not an SMA seed) then
  `ema = (k*ad)+(one_minus_k*ema)` — **not** repark's
  `(x − prev)*k + prev` statement order, and **not** Wilder
  `*= (p-1); += x; /= p`.
- `PER_TO_K(p) = 2.0 / (p + 1)`. `fastEMA`/`slowEMA` names follow the
  *parameter* slots, not which period is actually faster: `ADOSC(10,3)`
  inverts the sign vs `ADOSC(3,10)`.
- Unstable skip: compute EMAs from `startIdx - lookback` through
  `startIdx - 1` without writing output.

**`OBV` (`ta_OBV.c`)** — On Balance Volume. Lookback **0**.
- Seed: `prevOBV = inVolume[startIdx]`; `prevReal = inReal[startIdx]`.
  First output **is the first volume**, not 0. (Pinned: `obv[0]` bits ==
  `fixture_volume[0]`.)
- Then `if (close > prev) += vol; else if (close < prev) -= vol;` — equal
  close holds. Strict `>`/`<`, no epsilon.
- First-bar `tempReal == prevReal` so the seed is emitted unchanged.

**`MFI` (`ta_MFI.c`)** — Money Flow Index. Lookback
`period + TA_GLOBALS_UNSTABLE_PERIOD(MFI)` = **14** at default (unstable 0).
- **Not Wilder smoothing.** Rolling sum of signed money-flow over a circular
  buffer of `(positive, negative)` pairs, size = period. The brief's
  "Wilder-adjacent" warning is the **pos/neg accumulator order**, not the
  RSI/ATR three-statement Wilder update.
- Typical price: `(high+low+close)/3.0` (three adds, one divide — no FMA).
  Then `prevValue = typical`; then `typical *= volume`.
- Classification order is load-bearing (#1727704): `delta < 0` → negative
  only; `delta > 0` → positive only; **else both 0** (equal typical price
  is *neither*, not positive).
- Incremental window: **subtract** the trailing circbuf slot **before**
  computing the new typical. Do not rescan the window.
- Output: `temp = posSumMF+negSumMF`; `if (temp < 1.0) 0.0; else
  100.0*(posSumMF/temp)`. This is a **hard `< 1.0`**, **not** `TA_IS_ZERO`
  (±1e-8). Do not clamp to `[0, 100]` — incremental add/subtract can leave
  `posSumMF` slightly negative (flat golden min ≈ `-2.13e-14`).
- Period range `2..=100000`. Short input → all-NaN (C `outNBElement = 0`).

### Recorded series (10 new)

Walk (5000): `fixture_volume`, `ad`, `adosc_3_10`, `obv`, `mfi_14`.
Flat (600): `fixture_flat_volume`, `flat_ad`, `flat_adosc_3_10`, `flat_obv`,
`flat_mfi_14`.

---

## 1. Implementation

- `python/repark-parity/record_ta_goldens.py` — `positive_volume`, volume
  column on both fixtures, four walk + four flat cases, fixture writes.
- `crates/repark-ta/tests/goldens/*.bin` — 10 new files; `manifest.json`
  148 → 158.
- `crates/repark-ta/tests/goldens.rs` — `CONSUMED` +
  `volume_family_goldens_are_recorded` shape pin (no kernel calls). Required
  so `manifest_and_tests_cover_the_same_series` stays closed after the
  recorder updates the manifest. Not a `src/` kernel edit.

---

## 2. Files

- `python/repark-parity/record_ta_goldens.py`
- `python/repark-parity/map.md`
- `crates/repark-ta/tests/goldens.rs`
- `crates/repark-ta/tests/goldens/map.md`
- `crates/repark-ta/tests/goldens/manifest.json`
- `crates/repark-ta/tests/goldens/{fixture_volume,fixture_flat_volume,ad,adosc_3_10,obv,mfi_14,flat_ad,flat_adosc_3_10,flat_obv,flat_mfi_14}.bin`
- `crates/repark-ta/tests/map.md`
- `task/ta3-volume-goldens-ledger.md`
- `task/map.md`

CLOSED (untouched): `crates/repark-ta/src/**`, `ta.py`, `test_ta.py`,
`ta_window.rs`, `ta_toll.rs`, lockfiles, STATUS, registry.

---

## 3. ACC

### Critic-1

- Q-001 S2: `VOLUME_LOG_MEAN` was a transcribed `13.815510557964274` literal.
  Remediated: `float(np.log(1_000_000.0))`. Re-record: volume bins byte-identical.
- Q-002 S2: shape pin covered walk lookbacks only. Remediated: same 0 / 9 / 14
  pins + OBV seed on the flat fixture.
- Q-003 S3: `goldens.rs` CONSUMED + shape pin (not on the ALLOWED shortlist).
  Kept — `manifest_and_tests_cover_the_same_series` is two-way; a recorder
  manifest bump without CONSUMED is a red `make verify`. No kernel calls.
  CLOSED list is `src/**`.

### Critic-2

- Independent re-read of `ta_AD.c` / `ta_ADOSC.c` / `ta_OBV.c` / `ta_MFI.c`
  against the porting notes: CLV statement order, ADOSC first-AD seed +
  `(k*ad)+(one_minus_k*ema)` (not standalone `ema()`, not Wilder), OBV
  `prevOBV = volume[0]`, MFI neg-first classification + hard `< 1.0` (not
  `TA_IS_ZERO`) all match C. No additional S1/S2.
- Existing fixture sha256s match freeze. `git diff` on `goldens/*.bin` is
  empty (additive only).
- **Label: `ACC-CONVERGED`.**

---

## 4. Gates

| Gate | Exit |
|---|---|
| `make verify` | **0** |
| `make py-test-facade` (via preflight) | **0** (3119 passed, 71 skipped) |
| `make audit` | **0** |
| `make workflows-lint` | **0** |
| `cargo test -p repark-ta --test goldens` | **0** (40 passed, incl. `volume_family_goldens_are_recorded`) |

Existing OHLC / periods fixture sha256s match freeze `cd0db4f`. Existing indicator `.bin` files unmodified.

---

## 6. Handoff (paste-true for TA-4)

- Goldens are in `crates/repark-ta/tests/goldens/` under the names in §0.
- Port from `/tmp/grok-ta-reference/extracted/ta-lib/src/ta_func/ta_{AD,ADOSC,OBV,MFI}.c`
  (re-fetch the 0.4.0 tarball if that tree is gone). Numerics contract:
  `crates/repark-ta/src/lib.rs`.
- Honest cut: any kernel whose goldens will not converge ships nothing for
  that name.
