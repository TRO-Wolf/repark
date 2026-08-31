# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [fnp-15-16-ledger.md](fnp-15-16-ledger.md) — **FNP-15/16 (2026-08-30), PROVEN 17/17:** the six
  unreachable names and the four D-7 families (56 names, independently re-counted) are loud
  refusing surfaces on every door; registry §9 keeps unreachable vs deferred-by-cost distinct.
  Critic F-1..F-6 + O-1 all remediated (crate ANSI-door roster pin, per-family strip-check).
- [f-y10-1-int-overflow-ledger.md](f-y10-1-int-overflow-ledger.md) — **F-Y10-1 (2026-08-30), PROVEN 5/5:**
  checked integer arithmetic raises where Spark raises on typed INT/BIGINT operands (ANSI knob,
  DEC U5 shape); untyped literal arithmetic keeps the intended Int64 literal-width split
  (Critic F-1, Option A). Names preserved, AnsiDialect installs at session build, matrix cells
  pinned; SMALLINT wrap is a dated residue. Unblocks FNP-7b.
- [mw-10-s3tables-mor-ledger.md](mw-10-s3tables-mor-ledger.md) — **MW-10 (2026-08-28 →
  2026-08-30), PROVEN 6/6:** the S3 Tables merge-on-read leg the intake called "MW-4b",
  measure-first on OD-3b. The first owner dispatch (run 33333274383, on merged `main`) answered
  `PutTableData` **allow**; no denial registry row; docs and roadmap slots filled.
- [v3-3-dml-ledger.md](v3-3-dml-ledger.md) — **V3-3 (2026-08-30), chartered from RP-3 C-004
  red cells:** v3 `UPDATE` and `MERGE` stay a pre-write `V3-COW-1` keep-refusal (Spark
  preserves `_row_id`; the engine rewrite reassigns). Sequential COW DELETE lineage
  (F-rp3-c7) stays a fork finding. Three PROVEN clauses.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
