# Card CLUSTER-FEATURE-CI-1: no CI job builds or tests the `cluster` feature

**Date:** 2026-10-09. **Filed by:** Claude (Opus 5.5), ENC-1 fold 2, from the Opus re-verify of PR #1019.

**Status:** open. Not scheduled.

**Retires:** when a CI job runs `cargo test --locked -p repark-distributed --features cluster`, or the owner rules that the feature stays outside CI.

## Why

`repark-distributed` keeps its Ballista executor behind the `cluster` feature. No `make` target and no workflow enables it, so its tests never run in CI. ENC-1 fold 1 wrapped every catalog in a guard whose `Debug` text named the guard; `iceberg_provider.rs::catalog_spec_from_debug` reads the catalog kind from that text, and six codec tests went red with no gate to show it. An independent verifier found it by running the feature by hand. Fold 2 fixed the cause (the guard's `Debug` is transparent) and added the command to the unit's own gates.

One test fails on main as well: `date_and_timestamp_predicates_measure_the_pushdown_surface` in `tests/codec.rs`. Because `tests/codec.rs` fails, cargo stops before `tests/iceberg_scan.rs` unless `--no-fail-fast` is passed.
