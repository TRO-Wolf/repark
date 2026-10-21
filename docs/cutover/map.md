# map — docs/cutover

## Purpose

The production cutover record: which workloads of the cutover pipeline move to RePark, in what
order, under single-writer-per-table, with the measured evidence behind each stage, the
acceptance checks, the rollback story, the canary plan and the owner's rulings. The pipeline is
named only as "the cutover pipeline" here.

## Contents

- [production-audit-2026-10-11.md](production-audit-2026-10-11.md) — findings-only production readiness review at main `07be1dc9`: ranked security, safety, data-integrity, resource, validation and documentation findings, with qualified scope and remediation acceptance checks. A dated-rows section after the findings files later cards without touching the PA-NN sections ([REPARK-SQL-LOCAL-DDL-1](../../task/roadmap/mid-term/repark-sql-local-ddl-1-card-2026-10-15.md), [LOCAL-FS-CREATE-TABLE-LOCATION-1](../../task/roadmap/mid-term/local-fs-create-table-location-1-card-2026-10-15.md), [LOCAL-FS-WRITE-ROOT-1](../../task/roadmap/mid-term/local-fs-write-root-1-card-2026-10-15.md), 2026-10-15).
- [production-audit-evidence-2026-10-11.md](production-audit-evidence-2026-10-11.md) — reproduction scripts, fresh module identity, live Spark classification, functional outputs and gate outcomes for that review.

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
  replace-publish path, so a second gold `dbt run` failed) is **FIXED at pin `edc38c6a`, live
  pending** (F-GLUE-REPLACE-1 + RP-20); the twice legs and gold twice in `aws-acceptance.yml`
  (ICE-GOLD-TWICE-1) await their first run — the post-merge dispatch.
  Inventory §8 carries the rulings. §12 (2026-10-09) is a readiness note: the qualified scope, the
  v3 declared subset with links to each row or card, and the two qualification gaps. The
  assessment is superseded on its own header (2026-09-16); §12 points and adds no rows.
- [p2-report-ice-struct-pairing-1-2026-10-15.md](p2-report-ice-struct-pairing-1-2026-10-15.md) —
  **the P2 report for #1062, ICE-STRUCT-PAIRING-1 (2026-10-15):** verifier verdicts by time
  (FAIL at 19:00:31 EDT on 2026-10-14, FAIL at 02:14:43 EDT on 2026-10-15, PASS at 18:29:18
  EDT, merged `5531311b` at 18:51 EDT), acceptance run `38820033176` (12 cells: acceptance
  module 11 passed in 456.58s, dbt gold module 1 passed in 61.87s), the write-path change,
  and the follow-up cards from U-6.
- [p2-report-ice-tsns-type-planner-1-2026-10-16.md](p2-report-ice-tsns-type-planner-1-2026-10-16.md) —
  **the P2 report for #1082, ICE-TSNS-TYPE-PLANNER-1 (2026-10-16):** verifier verdict PASS at
  05:54:23 EDT, merged `c7121437` at 07:40 EDT on the owner's word, acceptance run `38895333180`
  (12 cells: acceptance module 11 passed in 439.61s, dbt gold module 1 passed in 63.82s), the
  write-path change, ruling 111's checklist, and the follow-up card for ruling 120.
- [p2-report-update-set-subquery-walk-1-2026-10-16.md](p2-report-update-set-subquery-walk-1-2026-10-16.md) —
  **the P2 report for #1083, UPDATE-SET-SUBQUERY-WALK-1 (2026-10-16):** verifier verdicts PASS at
  08:28 and 09:16 EDT (ruling 117's items), merged `41cdd957` at 09:40 EDT, acceptance run
  `38909759805` (12 cells: acceptance module 11 passed in 364.79s, dbt gold module 1 passed in
  61.25s), the write-path change, ruling 117's three items, and the follow-ups (V-003, then 106).
- [p2-report-pa-local-fs-policy-1-2026-10-16.md](p2-report-pa-local-fs-policy-1-2026-10-16.md) —
  **the P2 report for #1075, PA-LOCAL-FS-POLICY-1 (U-5) (2026-10-16):** verifier verdicts by time
  (FAIL at 16:26 EDT on 2026-10-15, FAIL at 03:49 EDT on 2026-10-16, PASS at 13:01 EDT, merged
  `14ad3c6d` at 17:28 EDT under ruling 126 (a)), acceptance run `38969694348` (12 cells:
  acceptance module 11 passed in 415.51s, dbt gold module 1 passed in 55.60s), the write-path
  change, and the follow-ups (ruling 105's carded findings, then 106 and EXPLAIN-DML-WRITE-1).
- [p2-report-rp-58-fork-repin-2026-10-17.md](p2-report-rp-58-fork-repin-2026-10-17.md) —
  **the P2 report for #1095, RP-58: the fork repin to `409ff330` (ICE-COMMIT-REBASE-DUP-1)
  (2026-10-17):** fork verifier verdicts by time (FAIL, FAIL, FAIL on test gaps, PASS at 10:56
  EDT), fork #368 merged `409ff330` at 16:49 EDT and #1095 merged `1ce895a0` at 17:27 EDT,
  acceptance run `39071066809` (12 cells: acceptance module 11 passed in 340.82s, dbt gold module
  1 passed in 59.07s), the write-path change, and the follow-ups (ENC-1 at the new rev, ruling
  143's fork card, the U-9 live drill).
- [p2-report-repark-sql-update-subquery-where-1-2026-10-17.md](p2-report-repark-sql-update-subquery-where-1-2026-10-17.md) —
  **the P2 report for #1090, REPARK-SQL-UPDATE-SUBQUERY-WHERE-1 (106) (2026-10-17):** verifier
  verdicts by time (FAIL at 22:13 EDT on 2026-10-16, Opus Max PASS at 18:57 EDT on 2026-10-17,
  merged `0b9195ca` at 19:24 EDT under ruling 132), acceptance run `39078930301` (12 cells:
  acceptance module 11 passed in 382.96s, dbt gold module 1 passed in 59.94s), the write-path
  change with owner ruling 146 verbatim, and the follow-ups (EXPLAIN-DML-WRITE-1, VF-1, VF-2).
- [p2-report-explain-dml-write-1-2026-10-21.md](p2-report-explain-dml-write-1-2026-10-21.md) —
  **the P2 report for #1137, EXPLAIN-DML-WRITE-1 (2026-10-21):** verifier verdicts by time (FAIL at
  `eccd920a` on a surviving trailing-semicolon mutant, the 225/125 dated-row split, the
  `repark.sql()` plan-first class and the prepared name's quoting; fix round 1 to `4b72871b`; PASS
  in round 2; merged `b7a20167` at 19:32 EDT), owner rulings 140 to 144 and the Q-B1 ruling, the
  gate tables (no live AWS acceptance run recorded), the write-path change (the
  `refuse_wrapped_write` seam, three call sites, the two ruled texts, the four-door never-worse
  census, the mutant tables), and the follow-ups (the native-door plan-first class, two S3 nits).

## Pointers

- Parent: [../map.md](../map.md)
- State: [../../STATUS.md](../../STATUS.md) "What happens next" item 3
- Evidence: [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) rows `CUTOVER-*`,
  `V3-COV-7`; the slate [../../briefs/next-sequence.md](../../briefs/next-sequence.md)
