# Card STRUCT-POSITION-GROUPING-SETS-1: forms inside GROUPING SETS, ROLLUP and CUBE refuse with a schema error

**Date:** 2026-11-11. **Filed by:** the records clerk (`claude-haiku-5-5`), from finding F1 of the Opus 5.5 verifier's round 1 on PR #1295 (`verify-fam-opus-out/verdict.md`).

**Status:** filed 2026-11-11, not dispatched. Severity **S3** (the result moves from by-name wrong rows, S2, to a refusal). Introduced by STRUCT-COMPARE-SEAT-FAMILY-1 for the cells that base answered by name; pre-existing for the `=` and `IN` arms, which #1292 already had.

**Measured:**

- Repro: `SELECT CASE s WHEN rv THEN 'x' ELSE 'y' END AS g, count(*) AS n FROM F2 GROUP BY ROLLUP (CASE s WHEN rv THEN 'x' ELSE 'y' END)`, with `s STRUCT<a,b>` and `rv STRUCT<b,a>`. Spark answers `[["x",2],["y",1],[null,3]]`; base answers `[["x",1],["y",2],[null,3]]`; head refuses with a classless schema error.
- Head refusals: G03 (`CUBE (nullif)`), G04 and G18 (`BETWEEN`), G14 (`NOT BETWEEN`), G06 and G07 (DataFrame `rollup` and `cube` over `F.expr`), and G13, where even the plain `gid` grouping column becomes unresolvable.
- P11 was EQUAL on base by coincidence (Spark `[["x",1],["y",3],[null,4]]`) and is now refused. It has no carded entry before this one.
- Pre-existing on main, with the same mechanism: #1292's `=` and `IN` arms (G01, G05, G08, G12), and `spark_integral_literal` on a scalar `ROLLUP (CASE i WHEN 1 ...)`.

**Mechanism (inferred by the verifier):** the relabel inside a `GroupingSet` loses the aggregate output name that the projection references.

**Close:** the forms inside the grouping constructs answer as Spark does (G01 to G18 and P11), or an owner ruling declines the refusal.
