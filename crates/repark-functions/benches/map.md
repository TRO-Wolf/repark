# map — crates/repark-functions/benches

## Purpose

PERF-10 (r24 G10) criterion **ratio** micro-benches for Spark function shims. Absolute wall is
runner-noise; each bench asserts a subject/baseline ratio against a **PROVISIONAL** ceiling
(morning mega sets finals = tip measured × 1.5).

## Contents

| File | What |
|---|---|
| [ratio_string_datetime.rs](ratio_string_datetime.rs) | `date_format` vs `to_char`; Spark `substring` vs `upper` |
| [map.md](map.md) | this file |

## I want to…

| I want to… | Go to |
|---|---|
| Run the ratio gate locally | `cargo bench -p repark-functions --bench ratio_string_datetime -- --quick` |
| Change a provisional ceiling | constants at top of `ratio_string_datetime.rs` + ledger note |
| Wire CI path filter | the benches CI workflow (not yet ported; see [../../../.github/map.md](../../../.github/map.md)) |

## Pointers

- Up: [../map.md](../map.md)
- Criterion pin: crate-level `Cargo.toml` dev-dep (never `[workspace.dependencies]`)
- Ledger: v1 `task/g10-enforcement-ledger.md` (not ported; see [../../../task/map.md](../../../task/map.md))

## Debug

| Symptom | Check |
|---|---|
| ratio assert fires | re-measure on release; raise only with ledger note; morning owns finals ×1.5 |
| `to_char` plan error | DataFusion built-in chrono format (`%Y-%m-%d`), not Java pattern |
| clippy disallowed_methods on benches | general gate uses `-A clippy::disallowed_methods`; benches may use expect |

<!-- 2026-08-04 (r24 morning): PROVISIONAL ceilings replaced with finals from the mega tip
  (max of 3 samples x 1.5): date_format/to_char 2.0, substring/upper 3.0. -->
