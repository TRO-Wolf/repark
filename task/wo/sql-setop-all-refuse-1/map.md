# task/wo/sql-setop-all-refuse-1/ — SQL-SETOP-ALL-REFUSE-1

The owner ruling of 2026-11-12 ("Go with the defaults", signed-zero part B, Q2): SQL `INTERSECT ALL`,
`EXCEPT ALL` and `MINUS ALL` refuse at the statement level on both SQL doors, because RePark answers
the wrong multiset for every column type. Its own PR in v1.5.7; it blocks ICE-V2-FLAT-AWS-LABEL-1. The
real fix is the card SQL-SETOP-ALL-MULTISET-1
([../../roadmap/mid-term/sql-setop-all-multiset-1-card-2026-11-12.md](../../roadmap/mid-term/sql-setop-all-multiset-1-card-2026-11-12.md)).

- [evidence/map.md](evidence/map.md) — the step-0 table on main against Spark 4.1.2, the same cells on
  the branch, the scripts that measured them, the seat and the red/green record.

Up: [../map.md](../map.md).
