# Unit ledger — TA-CHAIN-1 · a leading NaN/NULL run on a `ta_*` input is skipped, as polars_talib does

**Date:** 2026-10-03 · **Branch:** `fix/ta-chain-1` · **Base:** `origin/main` `3406dbb3`
· **Model:** Claude Opus 5.5 (`claude-opus-5-5`, high effort) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** `task/wo/ta-chain-1-leading-prefix.md` (PR #920 at `b908a055`, owner amendments of
2026-10-03 on the executor's two S1 halts: R-TC1-6 ratchet-down with the wrapper test in
`prefix.rs`; the all-invalid case calls the kernel on empty slices and re-prefixes NaN on every
band). Card: `task/roadmap/mid-term/ta-chain-1-card-2026-10-03.md`. Release: v1.5.2 (R-TC1-8).

**Why.** `ta.ema(ta.trange(high, low, close), 21)` answered NaN on every row: TRANGE's output
starts with a NaN and the C-faithful kernel propagates it through EMA forever. polars_talib, the
crate's golden oracle, skips each input's leading invalid run before calling the C kernel.

## PROPOSITION LEDGER — TA-CHAIN-1 — 2026-10-03

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | On single-input kernels the window UDF answers R-TC1-1: the leading NaN/NULL run is skipped and the rows before start + lookback are NaN, bit-exact against polars_talib 0.1.5. | `prefix_goldens` EMA/SMA/RSI/BBANDS-upper/MACD/LINEARREG twins, the chain twins, `leading_run_is_skipped_and_reprefixed_for_ema`, the two facade tests. | PROVEN | 26 passed; probe `ema([NaN,NaN,1..12],3)` = NaN×4 then 2.0..11.0 on polars_talib and on the wrapper. pins: ta-chain-1/C-001. §2. |
| C-002 | On multi-input kernels the start is the latest first-valid row across all inputs (high 3, low 7, close 5, volume 2, periods 0). | `prefix_goldens` TRANGE/ADX/ATR/STOCH-slowk/OBV/MAVP twins. | PROVEN | Leading NaN in the recorded goldens: TRANGE 8, ADX 34, ATR 21, STOCH 15 (start 7); OBV 5, MAVP 24 (start 5); all bit-exact. pins: ta-chain-1/C-002. §2. |
| C-003 | A NULL run and a NaN run answer bit-identically, on the wrapper and through the facade. | NaN/NULL twins in `prefix_goldens` (same expected bits); `test_leading_null_prefix_is_skipped_per_partition`. | PROVEN | 13 NULL twins pass against the same goldens; facade: 14 partitions, NULL runs 3 and 6, first finite row = run + 4, tail bit-equal to EMA over the trimmed series, output null count 0. pins: ta-chain-1/C-003. §2. |
| C-004 | An interior NaN after the start still propagates exactly as in C. | `interior_nan_after_the_start_still_propagates` (inline, `prefix.rs`). | PROVEN | `trange(h=[NaN,3,4,…], l=[1,2,NaN,4,…])` = NaN×3 then 1.5; `atr(…, 2)` all-NaN; polars_talib answers the same (probe §1). pins: ta-chain-1/C-004. §2. |
| C-005 | The kernel layer is untouched: no edit under `crates/repark-ta/src/` outside `udf/`; `cargo test -p repark-ta` count equals base; `goldens.rs` and the 158 kernel goldens are byte-identical. | `cargo test -p repark-ta` on base and head; `git diff origin/main --stat`. | PROVEN | 159 = 159 (106 lib + 11 contract + 41 goldens + 1 microbench) on both; the recorder rewrote the 158 kernel goldens byte-identical (no diff). pins: ta-chain-1/C-005. §2. |
| C-006 | Perf: `bench_kernel_race.py --quick` head/base median ≤ 1.02 (the start-0 path stays a borrow). | Three interleaved runs per side, release builds, §4. | PROVEN | Sum of the four `repark_engine` medians: head 0.036307 s, base 0.036049 s, ratio 1.0072. Per kernel: EMA 1.0006, BBANDS 1.0028, RSI 1.0126, SMA 1.0212 (inside the run spread; §4). pins: ta-chain-1/C-006. §4. |
| C-007 | Mutation: dropping the re-prefix fails `prefix_goldens` on length; forcing the start to 0 fails the prefix goldens; both reverted, tree clean. | Two mutation runs of `--test prefix_goldens` plus `udf::prefix`. | PROVEN | M1: 26 of 26 failed (length mismatch), 4 of 4 inline tests failed. M2: 24 of 26 failed (bit mismatch); the two `linearreg_5` twins survive (§3). Reverted with `git checkout`; `git status` clean. pins: ta-chain-1/C-007. §3. |

## 1. Red-first and oracle record (2026-10-03)

Step 1 red: with stub helpers, `cargo test -p repark-ta --features datafusion udf::` failed the four
new inline tests (`leading_run_is_skipped_and_reprefixed_for_ema`,
`interior_nan_after_the_start_still_propagates`, `all_invalid_input_yields_one_nan_band_per_output`,
`evaluate_all_chained_trange_into_ema_is_finite_after_lookback`; 34 passed, 4 failed). The two
facade tests fail on a base build (`3406dbb3`, release): the chain test on a bit mismatch, the
per-partition test because the NULL-run partitions never become finite.

polars_talib probe (polars_talib 0.1.5, TA-Lib 0.4.0, polars 1.32.3):
`ema([NaN,NaN,1..12],3)` and `ema([null,null,1..12],3)` both answer NaN×4 then 2.0..11.0;
`trange` on the R-TC1-1 inputs answers NaN×3 then 1.5; `atr(…,2)` all-NaN. An all-NaN input
makes polars_talib raise `TA_OUT_OF_RANGE_END_INDEX`; RePark answers NaN on every band there,
by the owner's 2026-10-03 amendment, the same answer as at base.

## 2. Implementation record (2026-10-03)

S1: `crates/repark-ta/src/udf/prefix.rs` (new) holds `leading_invalid_run`,
`run_with_prefix_skipped`, `run_all_with_prefix_skipped`. Start 0 calls the kernel on the
borrowed slices unchanged. Otherwise the slices are trimmed and the kernel runs (on empty slices
when every row is invalid), then each output gets `start` NaNs in front. The start is capped at
the shortest input, so a length mismatch still reaches the kernel's `LengthMismatch` and never
panics. The four `compute` / `compute_all` calls in `evaluate_all` route through the helpers;
`udf/mod.rs` shrinks 1821 → 1818 and its `scripts/check_rust_file_size.py` row ratchets to 1818
(integer only). The multi-output cache is unchanged.

S2: the recorder writes 13 polars_talib series over the prefix fixture into
`crates/repark-ta/tests/goldens/prefix/` with their own manifest (both version assertions held);
`tests/prefix_goldens.rs` drives every UDF through `window_udfs()` +
`partition_evaluator_factory` + `evaluate_all`: 26 passed.

S3: two facade tests, the `ema21_of_tr` column in `docs/examples/ta/composition.py`
(`check_example_coverage.py --require-execute` green), this ledger and the v1.5.2 notes draft.

## 3. Mutation record (2026-10-03)

M1 replaced the re-prefix with returning the trimmed vector: `prefix_goldens` 0 passed, 26
failed, every series on `length mismatch`; the four `udf::prefix` tests failed. M2 forced the
start to 0: 2 passed, 24 failed on `bit mismatch`. The survivors are `linearreg_5_nan_run` /
`linearreg_5_null_run`: LINEARREG recomputes each 5-row window from scratch, so on the
unskipped input the NaN run only blanks the windows that contain it (rows 0..8), which is
exactly the skipped answer (start 5 + lookback 4). That series cannot tell the two apart. Every
recursive or accumulating series does. Both mutations were reverted with `git checkout`, and
`git status` was clean afterwards.

## 4. Bench record (2026-10-03, R-TC1-5)

`python/repark-parity/bench/ta/bench_kernel_race.py --quick` (n = 100,000, warmup 1, 3
iterations, `repark_engine` leg; polars_talib not importable in the facade venv). Release native
on both sides (`maturin develop --release`, `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`; 321 MB
unstripped), runs interleaved head/base, Threadripper 3970X, box shared with another agent's
suite. Median seconds per run (sma / ema / rsi / bbands → sum):

| run | head | base |
|---|---|---|
| 1 | 0.008662 / 0.008434 / 0.008826 / 0.010348 → 0.036270 | 0.008353 / 0.009196 / 0.008716 / 0.010266 → 0.036531 |
| 2 | 0.008469 / 0.008544 / 0.008991 / 0.010303 → 0.036307 | 0.008482 / 0.008465 / 0.008750 / 0.010352 → 0.036049 |
| 3 | 0.008706 / 0.009141 / 0.008580 / 0.011098 → 0.037525 | 0.008493 / 0.008539 / 0.008677 / 0.010319 → 0.036028 |

Median of sums: head 0.036307 s, base 0.036049 s, **ratio 1.0072 ≤ 1.02**. SMA alone is 1.0212.
Its head-minus-base gap (0.18 ms) is smaller than the head runs' own spread (0.24 ms). SMA and
EMA take the same single-input borrow path, and EMA is 1.0006, so the SMA gap is noise. No
tuning was done.

## 5. Gates (2026-10-03, head)

`cargo test -p repark-ta` 159 passed (= base); `--features datafusion` all green with the four
new inline tests; `--test prefix_goldens` 26 passed; `check_rust_file_size.py` clean
(`udf/mod.rs` 1818 = row); `cargo clippy -p repark-ta --features datafusion -- -D warnings`
(plus `--all-targets`) and `cargo fmt --check` clean; `make rust-panic-ban` clean; comment-ban
scan `hits=0`; `check_example_coverage.py --require-execute` clean; facade
`test_ta_with_indicators.py` 12 passed and every TA facade module green.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ta-chain-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every recorded family pinned bit-exact against polars_talib on the wrapper (NaN and NULL twins) and the chain through the facade and the example; red at base.
      artifacts: [crates/repark-ta/tests/prefix_goldens.rs, python/repark/tests/test_ta_with_indicators.py]
    - id: AT-2
      status: ATTACKED
      evidence: Interior NaN, all-invalid input, NULL versus NaN runs, per-input runs of different lengths and the length-mismatch cap pinned or reasoned in section 2.
      artifacts: [crates/repark-ta/src/udf/prefix.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Both doors share PyColumn.ta_window; SQL and Python routes reach the same evaluate_all, and the multi-output path routes through run_all_with_prefix_skipped.
      artifacts: [crates/repark-ta/src/udf/mod.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Per-partition starts pinned over 14 partitions; the multi-output cache is keyed on the same input identity and the trimmed answer is a pure function of it.
      artifacts: [python/repark/tests/test_ta_with_indicators.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, environment read, secret or network access in the product change.
    - id: AT-6
      status: ATTACKED
      evidence: Clean inputs take the start-0 pass-through with the borrowed slices; the 158 kernel goldens, the kernel test count and every existing TA facade test are unchanged.
      artifacts: [crates/repark-ta/tests/goldens.rs]
    - id: AT-7
      status: ATTACKED
      evidence: Three interleaved release runs per side, median ratio 1.0072 against the 1.02 cap; all twenty-four timings recorded in section 4.
      artifacts: [python/repark-parity/bench/ta/bench_kernel_race.py]
    - id: AT-8
      status: ATTACKED
      evidence: No dependency, workflow or kernel edit; the only ceiling edit is the ordered ratchet-down of udf/mod.rs 1821 to 1818.
      artifacts: [scripts/check_rust_file_size.py]
    - id: AT-9
      status: ATTACKED
      evidence: The release-note line, the crate and udf maps and the example state the new semantics, including that an interior NaN still propagates and null_lookback is positional.
      artifacts: [task/roadmap/mid-term/v1-5-2-release-notes-draft-2026-10-03.md]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first inline and facade runs, both mutation reds, the reverts and the green reruns are recorded in sections 1 and 3.
      artifacts: [crates/repark-ta/src/udf/prefix.rs, crates/repark-ta/tests/prefix_goldens.rs]
  complete: true
```
