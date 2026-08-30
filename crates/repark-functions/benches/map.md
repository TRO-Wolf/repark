# map — crates/repark-functions/benches

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

PERF-10 (r24 G10) criterion ratio micro-benches for Spark function shims. Each bench asserts a
subject/baseline ratio against the final r24 ceilings; absolute wall time is runner noise.

## Contents

| File | What |
|---|---|
| [ratio_string_datetime.rs](ratio_string_datetime.rs) | `date_format` vs `to_char`; Spark `substring` vs `upper` |
| [map.md](map.md) | this file |

## I want to…

| I want to… | Go to |
|---|---|
| Run the ratio gate locally | `cargo bench -p repark-functions --bench ratio_string_datetime -- --quick` |
| Change a ceiling | constants at top of `ratio_string_datetime.rs` + ledger note |
| Wire CI path filter | the benches CI workflow (see [../../../.github/map.md](../../../.github/map.md)) |

## Pointers

- Up: [../map.md](../map.md)
- Criterion pin: crate-level `Cargo.toml` dev-dep (never `[workspace.dependencies]`)
- Ledger: `task/g10-enforcement-ledger.md` (see [../../../task/map.md](../../../task/map.md))

## Debug

| Symptom | Check |
|---|---|
| ratio assert fires | re-measure on release; raise only with a ledger note |
| `to_char` plan error | DataFusion built-in chrono format (`%Y-%m-%d`), not Java pattern |
| clippy disallowed_methods on benches | general gate uses `-A clippy::disallowed_methods`; benches may use expect |
