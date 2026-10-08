# map — repark-connect/src/provider/scan

## Purpose

The unit-test child of `../scan.rs` (`#[cfg(test)] mod tests;`), for what an integration test
cannot reach: the private `place_until_refusal`. CL-6 allows test children.

## Contents

- `tests.rs` — **C-2d residual (2026-10-07):** builds a `ScanPlan` offline, since `QueryPool::new`
  over `PostgresConnector::new` connects only at checkout, with a test localiser that refuses
  New York's 2026-03-08 spring-forward gap. Three pins: rows before the gap wall clock are
  emitted once, placed, then the refusal once, then the stream ends; a gap on the first row emits
  the refusal alone; a batch without a gap is emitted whole. The verifier's mutation M1 (the
  refusal chained through `filter(|_| false)`) turns the first red. pins: c-2/C-119
  **C-3 fold 1 (2026-10-08):** the hand-built `ScanPlan` carries `partition: None`, the
  unpartitioned value. No partitioned case is added here: a partitioned scan calls the same
  `place_until_refusal` on each connection's batches with the same plan, so the placement
  refusal has one behaviour, and the per-connection contract is the live cell
  `a_refused_value_keeps_its_contract_in_every_stride`. pins: c-3/C-005

## Pointers

- Up: [../map.md](../map.md)
- The implementation: [../scan.rs](../scan.rs)

## Debug

First checks: `cargo test -p repark-connect --lib provider::scan`.
