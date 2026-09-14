# map — docs/cutover

## Purpose

The production cutover record: which workloads of the cutover pipeline move to RePark, in what
order, under single-writer-per-table, with the measured evidence behind each stage, the
acceptance checks, the rollback story, the canary plan and the owner's rulings. The pipeline is
named only as "the cutover pipeline" here.

## Contents

- [inventory.md](inventory.md) — **the cutover inventory (2026-09-04):** six workloads in run
  order, table ownership before and after, the matrix rows already measured (SQL-HARDEN-1/2,
  DATE-FN-1), the six acceptance checks, rollback by snapshot, the canary steps C0–C6, and the
  four owner rulings of 2026-09-04 (match Spark on nullability → `CUTOVER-SCHEMA-1`; queue
  `DBT-1`; shadow namespace `<ns>_silver_repark`, 14-day retention; the daily diff as an
  Airflow task → pipeline-side `SHADOW-1`). C6 (gold on RePark) measured green on Glue on
  2026-09-05 through DBT-1's acceptance leg.
- [production-iceberg-status-2026-09-14.md](production-iceberg-status-2026-09-14.md) — **the
  production Iceberg assessment at `c9b03c67` / fork `3ebf7d36` (2026-09-14):** 36 capability
  rows, what moved since the 2026-09-06 assessment, the C0–C6 state, eleven ranked gaps, and §10
  the owner's rulings Q-ICE-1..7 with the nightly-run correction. Row C2 / gap G-1 (Glue had no
  replace-publish path, so a second gold `dbt run` failed) is stamped **FIXED 2026-09-14 at
  `edc38c6a`** (F-GLUE-REPLACE-1 + RP-20); the twice legs and gold twice in `aws-acceptance.yml`
  (ICE-GOLD-TWICE-1) are the live proof, first run pending the post-merge dispatch.
  Inventory §8 carries the rulings.

## Pointers

- Parent: [../map.md](../map.md)
- State: [../../STATUS.md](../../STATUS.md) "What happens next" item 3
- Evidence: [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) rows `CUTOVER-*`,
  `V3-COV-7`; the slate [../../briefs/next-sequence.md](../../briefs/next-sequence.md)
