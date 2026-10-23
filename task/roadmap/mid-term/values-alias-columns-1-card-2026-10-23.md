# Card VALUES-ALIAS-COLUMNS-1: `MERGE … USING (VALUES …) AS s(id, name)` fails at resolution on RePark where Spark 4.1.2 refuses it at parse

**Date:** 2026-10-23. **Filed by:** Muse Spark (`muse-spark-1.3-contributor`), builder of ICE-INTEGRITY-PINS-1 fix round 2, for the orchestrator; premise corrected the same day by the orchestrator (Claude Fable 5.1, `claude-fable-5-1`) after measuring the exact MERGE text on the oracle.

**Status:** open, not scheduled. Found as F2 of the first live run of legs U/X (run 39936752073); the legs now carry a source both engines accept, and this card holds the refusal-text gap.

**Retires:** when RePark refuses the MERGE text below at parse with Spark's `[COLUMN_ALIASES_NOT_ALLOWED]` text and a pin holds it, or the gap is disposed by an owner ruling.

## Why

The leg-U MERGE statement sourced its rows from an inline table with column aliases, `(VALUES …) AS s(id, name)`. That text is not valid Spark: Spark 4.1.2 refuses it at parse. RePark accepts the parse and fails later at resolution with `UNRESOLVED_COLUMN` on `s.id`, offering `column1`/`column2`. The first live run lost every merge cell to it (18 cells, control included, on both catalogs). The difference is the error, not a stored value: both engines refuse the statement and write nothing. The legs now spell the source as `(SELECT * FROM VALUES … AS v(id, name)) AS s`, which both engines resolve. The plain `INSERT … SELECT` over the aliased source, and the bare `SELECT` below, resolve on both engines; Spark's refusal is specific to the MERGE source alias.

## Measured

RePark on main `75b9e43b`, the ci-profile module, a `type=hadoop` warehouse under `tmp_path`, session zone UTC. The oracle is PySpark 4.1.2 with `iceberg-spark-runtime-4.1_2.13` 1.11.0, the Iceberg extensions and a hadoop catalog, `local[1]`.

**The exact leg statement (old `SOURCE`):**

```sql
MERGE INTO loc.ns.t AS t USING (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS s(id, name) ON t.id = s.id WHEN MATCHED AND s.id = 2 THEN UPDATE SET name = 'm' WHEN NOT MATCHED THEN INSERT *
```

**Spark 4.1.2 refuses it at parse:**

```
org.apache.spark.sql.catalyst.parser.ParseException:
[COLUMN_ALIASES_NOT_ALLOWED] Column aliases are not allowed in MERGE. SQLSTATE: 42601
```

**RePark refuses it at resolution** (verbatim, from `test_the_leg_source_resolves_on_the_local_catalog` on the old text):

```
repark.errors.AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `id` cannot be resolved. Did you mean one of the following? [`column1`, `column2`]. SQLSTATE: 42703
```

**The bare SELECT resolves on both engines:**

```sql
SELECT * FROM (VALUES (1, 'a'), (2, 'm')) AS s(id, name)
```

answers `id: int32, name: string` with rows `[(1, 'a'), (2, 'm')]` on RePark, equal to the oracle's rows.

## The ask

Step 0, for a guided executor on a branch and never on `main`:

- Pin the MERGE text above on the local catalog: RePark raises Spark's `[COLUMN_ALIASES_NOT_ALLOWED] Column aliases are not allowed in MERGE. SQLSTATE: 42601` as a parse error, and the table is unchanged.
- Refuse at the MERGE parse, where the source alias carries a column list; the bare `SELECT`, the `INSERT … SELECT` and the legs' `(SELECT * FROM VALUES … AS v(id, name)) AS s` source stay green throughout.
- The pin reds before the fix and greens after.
