# task/wo/filter-string-null-order-1/ — FILTER-STRING-NULL-ORDER-1

FILTER-STRING-NULL-ORDER-1 (S2, owner ruling 2026-11-13 "Merge anything you need. Then proceed with
defaults.", second builder): an `ORDER BY` inside a string `filter` / `where` places NULLs as Spark
does on Spark-door frames (first for `ASC`, last for `DESC`, an explicit `NULLS FIRST` / `NULLS LAST`
wins); native-door frames keep `nulls_max`. The card, with the inventory, the seat and three
findings, is [filter-string-null-order-1-card-2026-11-13.md](../../roadmap/mid-term/filter-string-null-order-1-card-2026-11-13.md).

- [evidence/](evidence/map.md) — step 0 on the base `35c5de77` against PySpark 4.1.2, and the same probe on the fix.

Up: [../map.md](../map.md).
