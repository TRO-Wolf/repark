# task/wo/nan-write-canonical-1/ — NAN-WRITE-CANONICAL-1

NAN-WRITE-CANONICAL-1 (owner ruling 2026-11-13, "Defaults", Q11 option C): RePark's Iceberg write stores every NaN
as the canonical NaN, as Spark's Parquet write does; no other value changes. Built after the inventory critic; card
[NAN-WRITE-CANONICAL-1](../../roadmap/mid-term/nan-write-canonical-1-card-2026-11-13.md).

- [evidence/](evidence/map.md) — step 0 on main `1d9d2808` and on the head `e100717f` against Spark 4.1.2, and the write cost row.

Up: [../map.md](../map.md).
