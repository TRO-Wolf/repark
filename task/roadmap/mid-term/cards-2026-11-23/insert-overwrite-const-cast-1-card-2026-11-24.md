# Card INSERT-OVERWRITE-CONST-CAST-1: an empty `INSERT OVERWRITE` with an invalid constant cast wipes the table under ANSI

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside review, measured here.
**Severity:** S2 (owner, 2026-11-24). **Status:** narrow fix built (2026-11-25), on branch
`fix/insert-overwrite-const-cast-1-narrow`. It replaces parked PR #1431, whose broad guard copied Spark's constant
folding and false-refused writes Spark makes in two review rounds.

## Measured

Measured on a build of main at `fa7d75e4` and on Spark 4.1.2 with Iceberg 1.11.0, on a hadoop catalog. The table
holds one row first.

| Statement | ANSI | Spark 4.1.2 | RePark |
|---|---|---|---|
| `INSERT OVERWRITE t SELECT CAST(s AS DOUBLE) FROM (SELECT 'x' AS s) src WHERE 1 = 0` | on | `[CAST_INVALID_INPUT]`; the table keeps its row | **succeeds; 0 rows** |
| the same, `CAST(s AS TIMESTAMP)` | on | `[CAST_INVALID_INPUT]`; the table keeps its row | **succeeds; 0 rows** |
| either statement | off | succeeds; 0 rows | succeeds; 0 rows |

The source is empty, so both engines agree on what the overwrite writes. They differ on whether it runs. Spark folds
the constant cast and refuses before writing. RePark writes the empty overwrite, so the table's data is gone.

No shape with a non-empty source writes rows Spark refuses: RePark's cast raises on the row (#1431's step 0, and
the narrow pins replay it). So the card stays S2. The outside review's further casts measured on main
`57c92b41`: `TIMESTAMP_NTZ` from `'x'`, through a subquery and as a literal, wipes where Spark raises
`[CAST_INVALID_INPUT]`; `CAST(1e20 AS DECIMAL(10,2))`, through a subquery and as a literal, wipes where Spark raises
`[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]`.

## The narrow fix (owner ruling, 2026-11-25)

The guard acts only where the answer is certain. Everything else stays exactly as on main, wipe included.

- **Doors:** SQL `INSERT OVERWRITE` with and without `TABLE`, with static and dynamic partition specs and under a
  dynamic `partitionOverwriteMode`; `writeTo(t).overwrite(...)` and `overwritePartitions()`; and `write.mode("overwrite")`
  with `insertInto` and with `saveAsTable` on an existing table. No other statement changes: `INSERT INTO`, CTAS, SQL
  `REPLACE TABLE ... AS SELECT`, `writeTo().replace()` / `createOrReplace()`, `MERGE`, `UPDATE`, `DELETE`, reads and
  `EXPLAIN` answer as on main.
- **Shapes:** under ANSI, a source that reads no table or view and plans to projections, filters and aliases over a
  `FROM`-less `SELECT`. A subquery, a CTE and a DataFrame built by `spark.sql` under the same settings qualify.
- **Casts:** a cast of a string literal, directly or through those projections, that is an output column. The value
  must be ASCII letters only. The target must be `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `TIMESTAMP` or
  `TIMESTAMP_NTZ`, or `FLOAT` / `DOUBLE` when the value is not `nan`, `inf` or `infinity` (any case, with or without a
  `d` / `f` suffix). On 83 such strings Spark 4.1.2 and RePark's own cast both raise `[CAST_INVALID_INPUT]` for every
  listed type. The guard then runs RePark's cast on the value and refuses only with that cast's error, so it never
  refuses a value RePark's cast accepts.
- **Where:** once per statement, before the door wipes, replaces or commits; the table is unchanged.

## Still unlike Spark (left as on main, each measured on main and the narrow head)

Measured on main `57c92b41` and Spark 4.1.2; each item is outside the guard's scope, so the narrow head answers as
main (the head re-measure is recorded with the PR).

- **Reads:** `SELECT CAST(s AS T) FROM (SELECT 'x' AS s) src WHERE 1 = 0` answers empty where Spark raises.
- **Writes that do not replace data, and RTAS outside `saveAsTable`:** `INSERT INTO`, CTAS, SQL `CREATE OR REPLACE
  TABLE ... AS SELECT`, `writeTo().append()` / `createOrReplace()` and a `saveAsTable` append succeed where Spark
  raises (the RTAS forms replace the table with an empty one).
- **Shapes outside the chain, on every overwrite door:** `UNION ALL`, joins, `GROUP BY`, `DISTINCT`, windows, `EXISTS`,
  scalar subqueries, `LIMIT`, SQL temp views and catalog views. Main wipes for `DOUBLE`, `FLOAT`, `TIMESTAMP` and
  `TIMESTAMP_NTZ` and refuses with the generic W03 text for the integral, `DECIMAL`, `BOOLEAN` and `DATE` types.
- **`VALUES` sources:** Spark does not raise there (it evaluates the filter over the inline rows first). Main matches
  Spark for `TIMESTAMP` and `TIMESTAMP_NTZ` and raises at analysis for `DOUBLE` and `FLOAT` (`SparkFloatStringify`).
  `LIMIT 0` behaves the same way.
- **Values outside the table:** blank, padded, signed or digit-bearing strings, non-ASCII letters, control characters,
  and `nan` / `inf` / `infinity` with a `d` or `f` suffix (Spark raises; RePark's cast accepts them). Main wipes.
  So does a value Spark rejects by grammar alone, such as `'1'` to `DATE` or `TIMESTAMP`.
- **`DATE`, `DECIMAL` and `BOOLEAN` targets:** RePark's cast raises a generic Arrow error, not Spark's class, so they
  are not in the table. Main refuses with W03 on the whole-table doors and wipes on the partition doors,
  `writeTo().overwrite(...)` and a `saveAsTable` overwrite.
- **`INT` and `BIGINT` on the whole-table doors:** main's W03 check refuses first, with its generic text; the table
  keeps its rows. The guard runs after it, so these answers do not move.
- **Decimal from double:** `CAST(1e20 AS DECIMAL(10,2))`, through a subquery or as a literal, wipes where Spark raises
  `[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]`: not a string literal.
- **BY NAME and any-schema tables:** an overwrite routed by name (`insert_by_name.rs`) is not guarded.
- **DataFrames built under other settings:** such a frame is a closed scan, so it is outside the guard.
- **ANSI off:** unchanged; main's W03 check still refuses some empty overwrites Spark makes.
- **#1431's false-refusal families:** outer joins over unions, `OFFSET` or CTEs, 7-digit years, control-character
  trimming, `SystemV/*` zones, casts inside `EXISTS` and windows, and the round-1 strings are each outside the guard
  (a join, union, `OFFSET`, subquery or window, or a value with digits, blanks or control characters). The
  facade pin replays one cell of each family on three doors (60 cells); all answer as main.

Evidence: [task/wo/insert-overwrite-const-cast-1-narrow/evidence/](../../../wo/insert-overwrite-const-cast-1-narrow/evidence/).

Up: [map.md](map.md).
