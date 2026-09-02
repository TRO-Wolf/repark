# map — task/ledgers/archive/2026-09/

## Purpose
Ledgers archived in 2026-09; immutable — corrections are dated errata at the top.
One line per ledger, and off the normal read path: grep this directory for a unit; do not read this file whole.

## Contents
- [2026-09-01-rp-5-fork-repin-ledger.md](2026-09-01-rp-5-fork-repin-ledger.md) — **RP-5 (2026-09-01), complete:** fork pin `00cdde0`.
- [2026-09-02-rp-6-fork-repin-ledger.md](2026-09-02-rp-6-fork-repin-ledger.md) — **RP-6 (2026-09-01), completed:** fork repin `00cdde0` → `fb0cacfa` (PR-1..PR-7).
- [2026-09-02-v3-7-merge-lineage-ledger.md](2026-09-02-v3-7-merge-lineage-ledger.md) — **V3-7 (2026-09-02), completed:** carry `_row_id` through the RePark-owned MERGE writer; lift `V3-COW-1` MERGE where Spark-equal.
- [2026-09-02-v3-8-subquery-where-lineage-ledger.md](2026-09-02-v3-8-subquery-where-lineage-ledger.md) — V3-8 — subquery-`WHERE` COW DML keeps v3 row lineage; `V3-COW-1` FIXED

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../../AGENTS.md](../../../../AGENTS.md) "Markdown document lifecycle"
