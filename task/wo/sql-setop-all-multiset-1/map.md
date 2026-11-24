# task/wo/sql-setop-all-multiset-1/ — SQL-SETOP-ALL-MULTISET-1

The owner ruling of 2026-11-22: replace the declared refusal SQL-SETOP-ALL-REFUSE-1 with Spark's multiset
answer for `INTERSECT ALL` and `EXCEPT ALL` on both SQL doors, `MINUS ALL` on the Spark door only (native
refuses `MINUS ALL` as it refuses `MINUS`, owner ruling 2026-11-23), fragments, stored view bodies, and the
DataFrame `intersectAll` / `exceptAll`. Card:
[../../roadmap/mid-term/sql-setop-all-multiset-1-card-2026-11-12.md](../../roadmap/mid-term/sql-setop-all-multiset-1-card-2026-11-12.md);
the retired refusal's record: [../sql-setop-all-refuse-1/map.md](../sql-setop-all-refuse-1/map.md).

- [evidence/summary.md](evidence/summary.md) — the domain table against Spark 4.1.2 on main, the mutant and head,
  the candidate chosen and its cost, the red/green record and the residue.

Up: [../map.md](../map.md).
