# map — repark-ta

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

RePark's own pure-Rust technical-analysis kernels (crate-DAG tier 3, a door-neutral capability
crate): bit-exact hand-ports of the TA-Lib C 0.4.0 algorithms (no C compiled/linked/vendored and
no third-party TA crate). This is the kernel layer only — plain `&[f64]` in → `Vec<f64>` out,
dependency-light (runtime dep: `thiserror`; dev-dep: `serde_json` for the goldens
manifest), independently publishable. **68 public kernel functions** (the frozen 64 plus the TA-4 volume
four: AD, ADOSC, OBV, MFI). The optional `datafusion` feature adds the
window-UDF wrapper layer (`udf` module — **81 `WindowUDF`s**: one per entry point, so the
multi-output kernels — BBANDS, MAMA, AROON, the MACD family, the stochastics — are split one UDF
per output) plus the door-neutral `TaExtension`. Consumers: `repark-spark` (the Spark door
composes `TaExtension` at v1's registration position) and, in phase 3, `repark-python` (DataFrame
API — the `repark.ta` Python namespace is built on it). **68/68 functions, 81/81 entry points.**

## Contents

- `Cargo.toml` — workspace member; runtime dep `thiserror` + optional `datafusion` and
  `repark-core` (both behind the `datafusion` feature, which turns on the `udf` wrapper module
  and the `extension` module), dev-deps `serde_json` + `tokio` + crate-level `criterion` 0.8
  (`async_tokio` + `html_reports`, never `[workspace.dependencies]`) and `[[bench]] ta_kernels`
  (`harness = false`). Workspace lints (`unsafe_code = "forbid"`, clippy pedantic) apply.
- [benches/](benches/map.md) — criterion kernel baseline (`ema`/`sma`/`rsi`/`bbands` plus
  volume representatives at n=1e6; BBANDS one-run, three-run, and one-run-plus-clones shapes).
  Measure-only; no `src/` instrumentation.
- `NOTICE` — TA-Lib BSD-3-Clause attribution (algorithms ported by reference; carry this into
  any distribution).
- [src/](src/map.md) — the kernels, grouped by TA-Lib category. C-mirrored `*_idx` local names
  are a deliberate house-style exception (see `src/lib.rs` attribution note). Window UDF
  periods must be whole numbers (non-integral `f64` fails loud — no silent truncate).
- [tests/](tests/map.md) — the golden bit-exactness gate + recorded fixtures.

## I want to...

| ...do this | go to |
|---|---|
| Add / fix a kernel | [src/map.md](src/map.md) — port from the C reference, mind the numerics contract |
| Understand the numerics contract (no FMA, incremental accumulators, …) | `src/lib.rs` crate docs |
| Re-record or extend the goldens | `python/repark-parity/record_ta_goldens.py` → [tests/map.md](tests/map.md) |
| Call an indicator from Rust | `repark_ta::{sma, ema, rsi, adx, atr, bbands, mama, sar, sarext, mavp, apo, ppo, macdext, stoch, ad, adosc, obv, mfi, …}` (68 kernels; `ma(_, _, 7)` / APO·PPO·MACDEXT·STOCH* matype 7 = MAMA) |
| Install the TA UDFs on a session | `extension` module (feature `datafusion`): `TaExtension` — a `repark_core::SessionExtension`; `SparkExtension` composes it, native sessions install it directly |
| Call an indicator from SQL / DataFrame | `udf` module (feature `datafusion`): `ta_ema(close, 21) OVER (…)`; Python `repark.ta.ema(...)` — see [src/map.md](src/map.md) |
| Run the P-1 kernel criterion baseline | `cargo bench -p repark-ta --bench ta_kernels -- --quick` — see [benches/map.md](benches/map.md) |

## Component contract

- **Owns:** pure-Rust bit-exact TA-Lib 0.4.0 kernels (68 functions); under the optional `datafusion`
  feature, the window-UDF wrapper layer (81 `WindowUDF`s — one per entry point) + the door-neutral
  `TaExtension`.
- **Does not own:** the session install position (the Spark door composes `TaExtension`); the Python
  `repark.ta` namespace (repark-python builds on this).
- **Public inputs:** `&[f64]` slices (kernels); a session (`TaExtension` install); `ta_*(col, period)
  OVER (…)` (window UDFs).
- **Public outputs:** `Vec<f64>` kernel results; registered TA window UDFs; a `SessionExtension`.
- **State & lifecycle:** kernels are stateless (`&[f64]` in → `Vec<f64>` out); the optional UDF layer
  holds a thread-local multi-output cache so split siblings share one kernel run.
- **Allowed internal deps:** `repark-core` **only under the `datafusion` feature** (the `TaExtension`);
  the kernel core is dependency-light (runtime dep `thiserror`).
- **Failure model:** `thiserror` kernel errors; a non-integral window period fails loud (no silent
  truncation).
- **Extension points:** add / fix a kernel (port from the C reference, mind the numerics contract);
  add its window UDF.
- **Test strategy:** `cargo test -p repark-ta` — the golden bit-exactness gate (oracle = bundled C
  TA-Lib 0.4.0) + lib unit + numerics-contract tests.
- **Known limitations:** `linearreg_angle` may differ by a few ulp off glibc-x86-64 (`atan` is not
  required to be correctly rounded); goldens are recorded on glibc x86-64.
- **2026-08-29:** Multi-output UDF cache keys omit Float64 validity-buffer and nested
  dictionary-child identity. Distinct inputs sharing values/keys buffers and null count can collide
  and silently reuse a sibling result in BBANDS, MACD/FIX/EXT, STOCH/F/RSI, AROON, or MAMA. A
  separate fix must bypass nullable/non-Float64 caching or key complete array identity, with
  shared-buffer regression pins.

## Pointers

- Up: [../map.md](../map.md)
- Golden recorder: `python/repark-parity/record_ta_goldens.py` (oracle = `polars_talib`'s
  bundled C TA-Lib 0.4.0 — the binary the pipeline's models were trained against).
- Scope SSOT for what lands next: task/todo.md Group T (MarciEngine's frozen 64-function
  inventory).

## Debug

| Symptom | First check |
|---|---|
| A golden test fails after editing a kernel | You changed operation order. Diff against the C reference (`ta-lib 0.4.0 src/ta_func/ta_<NAME>.c`); the failure message prints row + both bit patterns |
| A golden test fails with NO kernel edit | Oracle drift — someone re-recorded goldens with a different TA-Lib build; the recorder asserts both the bundled TA-Lib (0.4.0) and the `polars_talib` wrapper (0.1.5) versions |
| ONLY `linearreg_angle` goldens fail, on a new platform | Known libm caveat (`atan` is not required to be correctly rounded — see the crate docs "Known, deliberate divergences"); goldens are recorded/tested on glibc x86-64 |
| New kernel is "close but not exact" (≤ a few ulp) | Look for `mul_add`, reordered accumulation, or a recomputed-per-window sum that C keeps incremental |
| Values differ only late in a long series | Accumulator-drift mismatch: C's running totals were replaced by per-window recomputation (or vice versa) |
| Three BBANDS columns ~3× slower than one | Check the thread-local multi-output cache in `src/udf/mod.rs` and the `tests/p1c_microbench.rs` shape |
| Need a recorded kernel ns/row | P-1 criterion: `cargo bench -p repark-ta --bench ta_kernels -- --quick` — [benches/map.md](benches/map.md); numbers stay planning-side |

First checks: `cargo test -p repark-ta` (lib unit tests + goldens + contract). Escalate to:
[../map.md#debug](../map.md).
