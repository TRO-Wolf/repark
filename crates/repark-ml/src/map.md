# map — repark-ml/src

## Purpose

Kernel sources for native estimators: pure math + streaming accumulators.

## Contents

| Path | Role |
|---|---|
| `lib.rs` | Crate root; re-exports; `MAX_FEATURES = 4096` |
| `error.rs` | `MlError` / `Result` (thiserror) |
| `cholesky.rs` | In-place Cholesky + fallible forward/back solve; **SAF-006** `checked_mul` on dimension² |
| `linear_regression.rs` | Streaming `XᵀX`/`Xᵀy` OLS + param gates |
| `logistic_regression.rs` | IRLS passes reusing Cholesky; `max_iter=0` = cold-start zeros |
| `kmeans.rs` | initMode validation, random indices, Lloyd; `max_iter=0` = init-only; checked distances; **SAF-004** max-attempts rejection sampling + sequential fill (`k=n` pin) |

## I want to...

| Task | Go to |
|---|---|
| Add a solver primitive | `cholesky.rs` or new module + re-export from `lib.rs` |
| Tighten singular threshold | `cholesky.rs` `PIVOT_*` constants |

## Pointers

- Up: [../map.md](../map.md)

## Debug

Unit tests live in each module (`#[cfg(test)]`). Run: `cargo test -p repark-ml`.
