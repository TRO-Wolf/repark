# Unit ledger — UDFX per-family UDF module split

**Unit:** UDFX · conductor-15 Track T4 · **Date:** 2026-08-15 ·
**Lane:** UDFX · **Executor:** Grok (grok-4.6) ·
**Worktree:** worktree `grok-udfx` · **Branch:** `grok/udfx-udf-split` ·
**Base (FROZEN):** `8cbde88bb076cbf09976fa0bfbc702472f267fca` (conductor-14
closeout on origin/main).

**Charter:** `planning/grok/BRIEF-conductor-15.md` T4 + A9/A12. **SEPMO:** acc.
Floor S1. Risk tier: mechanical (zero-behavior extract).

CLOSED: kernel files (`overlap.rs` / `momentum.rs` / `volatility.rs` /
`volume.rs` / `price_transform.rs` / `statistic.rs` / `math_operator.rs`),
goldens, T5 `pub fn rsi` / `pub fn sma`, STATUS, registry, `.github/`,
`[patch.crates-io]`, Cargo.lock, primary checkout, benches (T1 leftover).

## Charge

`udf.rs` was 2167/2200 and its EXCEPTIONS note said "RATCHET: after per-family
UDF modules". Convert to:

| Path | Role |
|---|---|
| `crates/repark-ta/src/udf/mod.rs` | cache, densify, param checks, `evaluate_all`, SPECS, `TaFn`, statistic + math_operator dispatch, `register_all` / `window_udf*` |
| `crates/repark-ta/src/udf/overlap.rs` | overlap-family dispatch arms |
| `crates/repark-ta/src/udf/momentum.rs` | momentum-family dispatch arms |
| `crates/repark-ta/src/udf/volatility.rs` | `trange`/`atr`/`natr` |
| `crates/repark-ta/src/udf/volume.rs` | `ad`/`adosc`/`obv`/`mfi` |
| `crates/repark-ta/src/udf/price.rs` | `avgprice`/`medprice`/`typprice`/`wclprice` |
| `crates/repark-ta/src/udf/map.md` | new directory map |

`statistic` + `math_operator` stay in `udf/mod.rs` this wave (A9). `lib.rs`
already has `pub mod udf;` — after deleting `udf.rs` it resolves `udf/mod.rs`.
ZERO numeric/behavior change.

## What landed

| Artifact | Path | Role |
|---|---|---|
| Shared UDF module | `crates/repark-ta/src/udf/mod.rs` | former `udf.rs` minus family compute tables |
| Family dispatch | `crates/repark-ta/src/udf/{overlap,momentum,volatility,volume,price}.rs` | `compute` / `compute_all` arms |
| Directory map | `crates/repark-ta/src/udf/map.md` | new |
| Parent map | `crates/repark-ta/src/map.md` | udf paragraph only (T5 kernel rows untouched) |
| File-size SSOT | `scripts/check_rust_file_size.py` | delete `udf.rs` key; add `udf/mod.rs` ratcheted DOWN |
| This ledger | `task/udfx-udf-split-ledger.md` | unit record |

No kernel-math edit. No golden edit.

## Tests

Existing UDF unit battery stays in `udf/mod.rs` (cache, densify, `evaluate_all`
siblings, bit-exact vs public kernels). NEW
`compute_routes_every_spec_to_a_family_or_shared_arm` iterates SPECS and
refuses a `family_dispatch_miss` (router vs family table cannot silently
drop an arm).

## EXCEPTIONS

| Key | Before | After |
|---|---|---|
| `crates/repark-ta/src/udf.rs` | 2200 (measured 2098 / then 2167) | **deleted** (stale path = fail-closed) |
| `crates/repark-ta/src/udf/mod.rs` | — | **2100** (measured 2020) |

Family files sit under the default ceiling; no new EXCEPTIONS rows.

## Residuals

- `crates/repark-ta/benches/ta_kernels.rs` still says "UDF TLS cache lives in
  `src/udf.rs`" — T1-owned bench file; not edited.
- `crates/repark-ta/map.md` debug table still cites `src/udf.rs`.
- `crates/repark-ta/src/extension/map.md` still cites `../udf.rs` (`SPECS`).
- `task/p1-ta-kernel-benches-ledger.md` still names `src/udf.rs`.

## Gates (real exit codes)

| Gate | Exit |
|---|---|
| `make verify` pieces (`ci` statics + `make test`) | **0** |
| `make rust-clippy` / `make rust-panic-ban` | **0** |
| `cargo test -p repark-ta --features datafusion` | **0** (lib 142, contract 11, goldens 41, p1c 1) |
| `make develop` (maturin 1.14.1) | **0** |
| `make py-test-facade` | **0** (3230 passed, 71 skipped) |
| `make audit` | **0** |
| `make workflows-lint` | **0** |
| `make preflight` roster | verify + facade + audit + workflow lint all 0 |

Hooks: `scripts/check_map_md.sh` on the staged set. Hygiene two-pass before push.

## FINDINGS

None that change the extract. Goldens + contract + facade green untouched.
Kernel files not edited. T5 overlap/momentum kernel-row sentences not
rewritten.
