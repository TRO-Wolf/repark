# Card CLUSTER-FEATURE-CI-1: no CI job builds or tests the `cluster` feature

**Date:** 2026-10-09. **Filed by:** Claude (Opus 5.5), ENC-1 fold 2, from the Opus re-verify of PR #1019.

**Status:** open. Not scheduled.

**Retires:** when a CI job runs `cargo test --locked -p repark-distributed --features cluster`, or the owner rules that the feature stays outside CI.

## Why

`repark-distributed` keeps its Ballista executor behind the `cluster` feature. No `make` target and no workflow enables it, so its tests never run in CI. ENC-1 fold 1 wrapped every catalog in a guard whose `Debug` text named the guard; `iceberg_provider.rs::catalog_spec_from_debug` reads the catalog kind from that text, and six codec tests went red with no gate to show it. An independent verifier found it by running the feature by hand. Fold 2 fixed the cause (the guard's `Debug` is transparent) and added the command to the unit's own gates.

One test fails on main as well: `date_and_timestamp_predicates_measure_the_pushdown_surface` in `tests/codec.rs`. Because `tests/codec.rs` fails, cargo stops before `tests/iceberg_scan.rs` unless `--no-fail-fast` is passed.

## Owner ruling 2026-10-10

- Add the cluster-feature CI job: `cargo test --locked -p repark-distributed --features cluster`
  runs in CI.
- First fix the failing test `date_and_timestamp_predicates_measure_the_pushdown_surface`
  (`crates/repark-distributed/tests/codec.rs`) on main, in its own small pull request, so the
  job is green on the day it lands.
- The job is not a required check until it has been green for a week.

## CLUSTER-CODEC-TEST-1 (2026-10-10)

The failing test is fixed on branch `fix/cluster-codec-pushdown-test-1`, in the small PR
the 2026-10-10 owner ruling asked for: the expectation was stale, not the product. The
TIMESTAMP refusal became a sound drop and the DATE drop became a correct push, both in
RP-27 ([ledger](../../ledgers/staging/cluster-codec-test-1-ledger.md) carries the
before/after surface). `cargo test --locked --no-fail-fast -p repark-distributed
--features cluster --test codec --test iceberg_scan` answers 20 of 20. The card stays
open: it retires when the CI job exists.

## CLUSTER-TESTS-2 (2026-10-10)

The two remaining reds are fixed on branch `fix/cluster-tests-2`, in the second small PR
the 2026-10-10 owner ruling asked for: the seed was stale, not the product. Both cancel
tests built their long job from `range(100000000)`, which has planned the uncarried
`StreamingTableExec` since RANGE-TVF-ID-1; the seed is now `generate_series(0,
99999999)`, whose `LazyMemoryExec` the codec carries
([ledger](../../ledgers/staging/cluster-tests-2-ledger.md) carries the cause and the
test-side decision). `cargo test --locked --no-fail-fast -p repark-distributed --features
cluster` answers 39 of 39, five consecutive runs. The card stays open: it retires when
the CI job exists.

## The CI job (2026-10-10)

Job `rust-test-cluster` in `ci.yml` runs `make rust-test-cluster`: `cargo test --locked
--no-fail-fast -p repark-distributed --features cluster`, 39 tests, green five times in a row
locally after CLUSTER-CODEC-TEST-1 and CLUSTER-TESTS-2. It runs tests only: a package-scoped
clippy with the cluster feature fails on two lints in `repark-core` when its `postgres`
feature is off (`unused_self`, `unused_async` in `named_sources.rs`), recorded here and not
fixed. The job is not a required check until it has been green for a week; the owner
adds it to branch protection then. The card retires at that point.
