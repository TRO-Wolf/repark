# task/wo/asof-join-1/evidence/jar/ — `javap -c -p` excerpts

- `javap-asof.txt` — Spark 4.1.2's as-of join, filtered to object creation, calls, loads and branches:
  `AsOfJoin$` (`apply`, `makeAsOfCond`, `makeOrderingExpr`, the tolerance assertion lambda), `AsOfJoin` (`output`,
  `resolved`, the constructor's join-type `require`), `AsOfJoinDirection$.apply`, `RewriteAsOfJoin`'s rule body,
  `classic.Dataset.joinAsOf` (both overloads, `createAsOfJoinPlan$1`, the `usingColumns` lambdas), and the lexer and
  join-type grammar facts (no `ASOF` token). Extracted from `spark-catalyst_2.13-4.1.2.jar`,
  `spark-sql_2.13-4.1.2.jar` and `spark-sql-api_2.13-4.1.2.jar` (the oracle's `pyspark/jars`) into a scratch
  directory, read only.

Up: [../map.md](../map.md).
