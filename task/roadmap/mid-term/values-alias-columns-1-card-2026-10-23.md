# Card VALUES-ALIAS-COLUMNS-1: MERGE … USING (VALUES …) AS s(id, name) loses the column aliases where Spark 4.1.2 resolves them

**Date:** 2026-10-23. **Filed by:** Muse Spark (`muse-spark-1.3-contributor`), builder of ICE-INTEGRITY-PINS-1 fix round 2, for the orchestrator.

**Status:** open, not scheduled. Found as F2 of the first live run of legs U/X (run 39936752073); the legs carry the workaround source, and this card holds the gap.

**Retires:** when the legs' MERGE source reverts to the `(VALUES …) AS s(id, name)` form with `test_the_leg_source_resolves_on_the_local_catalog` green on that text, or the gap is disposed by an owner ruling.

## Why

The leg-U MERGE statement sources its rows from an inline table with column aliases, `(VALUES …) AS s(id, name)`. Spark 4.1.2 resolves that text; RePark fails the statement with `UNRESOLVED_COLUMN` on `s.id`, offering `column1`/`column2`. The first live run lost every merge cell to this (18 cells, control included, on both catalogs). The plain `INSERT … SELECT` over the same source resolves, so the alias loss sits on the MERGE USING path, not in VALUES-alias parsing: the bare `SELECT` below resolves on RePark too. The legs now spell the source as `(SELECT * FROM VALUES … AS v(id, name)) AS s`, which resolves everywhere.

## Measured

All on main `75b9e43b`, the ci-profile module, a `type=hadoop` warehouse under `tmp_path`, session zone UTC.

**The exact leg statement (old `SOURCE`):**

```sql
MERGE INTO loc.ns.leg_source_merge AS t USING (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS s(id, name) ON t.id = s.id WHEN MATCHED AND s.id = 2 THEN UPDATE SET name = 'm' WHEN NOT MATCHED THEN INSERT *
```

**RePark's error text** (verbatim, from `test_the_leg_source_resolves_on_the_local_catalog` on the old text):

```
repark.errors.AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `id` cannot be resolved. Did you mean one of the following? [`column1`, `column2`]. SQLSTATE: 42703
```

**The bare SELECT resolves on RePark:**

```sql
SELECT * FROM (VALUES (1, 'a'), (2, 'm')) AS s(id, name)
```

answers schema `id: int32, name: string` with rows `[(1, 'a'), (2, 'm')]`, which equal the oracle's rows `[(1, 'a'), (2, 'm')]` for that statement. Spark 4.1.2 resolves the MERGE text above (first-run report F2).

## The ask

Step 0, for a guided executor on a branch and never on `main`:

- Reproduce with the pin: set `SOURCE` in `python/repark/tests/parity/test_aws_acceptance_commit_unknown.py` back to the `(VALUES …) AS s(id, name)` form and run `test_the_leg_source_resolves_on_the_local_catalog` (reds today, greens with the workaround).
- Find where the USING-arm aliases are dropped between the MERGE plan and the resolve, fix it, and revert the legs' `SOURCE` to the short form in the same PR.
- The pin reds before the fix and greens after; the bare SELECT above stays green throughout.
