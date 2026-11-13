# task/wo/fexpr-window-null-order-1/ — FEXPR-WINDOW-NULL-ORDER-1

FEXPR-WINDOW-NULL-ORDER-1 (S2, owner ruling 2026-11-12 "Go with the defaults", second builder): an
`ORDER BY` inside an `F.expr` fragment places NULLs as Spark does (first for `ASC`, last for `DESC`,
an explicit `NULLS FIRST` / `NULLS LAST` wins). The card, with the cause, the seat and three findings,
is [fexpr-window-null-order-1-card-2026-11-12.md](../../roadmap/mid-term/fexpr-window-null-order-1-card-2026-11-12.md).

- [evidence/](evidence/map.md) — step 0 on main `a3f8c7d9` against PySpark 4.1.2, and the same probe on the fix.

Up: [../map.md](../map.md).
