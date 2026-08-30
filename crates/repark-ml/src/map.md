# map — repark-ml/src

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Pure math and streaming accumulators for native estimators.

## Contents

| Path | Role |
|---|---|
| `lib.rs` | Crate root; re-exports; `MAX_FEATURES = 4096` |
| `error.rs` | `MlError` / `Result` and the pinned `Singular` diagnostic |
| `cholesky.rs` | In-place Cholesky + fallible forward/back solve; checked dimension² |
| `linear_regression.rs` | Streaming `XᵀX`/`Xᵀy` OLS and parameter gates |
| `logistic_regression.rs` | IRLS passes reusing Cholesky; `max_iter=0` returns cold-start zeros |
| `kmeans.rs` | `initMode` validation, bounded random indices, Lloyd, and `max_iter=0` init-only behavior |

## I want to...

| Task | Go to |
|---|---|
| Add a solver primitive | `cholesky.rs` or a new module re-exported from `lib.rs` |
| Tighten the singular threshold | `cholesky.rs` `PIVOT_*` constants |

## Known limitations

Measured 2026-08-29; these pre-existing contracts remain separate from comment condensation:

- **BF-CC2-ML-001 (S2):** `observe_dense` accepts non-empty features with empty labels.
- **BF-CC2-ML-002 (S2):** NaN `elastic_net_param` passes validation.
- **BF-CC2-ML-003 (S2):** `predict_probability` truncates mismatched coefficient and feature lengths.
- **BF-CC2-ML-004 (S2):** `XorShift64::next_index(0)` panics despite its precondition.
- **BF-CC2-ML-005 (S2):** finite distance overflow can select the wrong first center.
- **BF-CC2-ML-006 (S1):** repeated same-sign finite KMeans values can overflow a cluster sum;
  `max_iter=1` can then return a non-finite center.
- **BF-CC2-ML-007 (S2):** `cholesky_solve` allocates dimension-sized buffers before validation.
- **BF-CC2-ML-008 (S1):** finite OLS values can overflow `Xᵀy` and return non-finite coefficients.

## Pointers

- Up: [../map.md](../map.md)

## Debug

Unit tests live in each module (`#[cfg(test)]`). Run: `cargo test -p repark-ml`.
