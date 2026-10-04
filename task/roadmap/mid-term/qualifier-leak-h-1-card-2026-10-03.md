# Card QUALIFIER-LEAK-H-1 — AMBIGUOUS_REFERENCE names a generated `_repark_jl_` qualifier

**Date:** 2026-10-03 · **Filed by:** Grok 4.7, guided execution · **Source:** owner delegate
ruling Q5 of the same day: card the internal-qualifier leak with the other two side findings,
in one docs PR, scheduled post-1.5.2. The bug predates the ATTR-ID-1 stack. No product code
in this filing.

## Why

Replay cell `r3.F_cp_bare_getU_join_parent`, case-insensitive (`spark.sql.caseSensitive`
false):

```python
d = spark.createDataFrame([(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "v", "Data"])
o = spark.createDataFrame([(10, "x"), (30, "y")], ["v", "tag"])
fr = d.select(d.v, d["V"])
fr.join(o, d.v == o.v)
```

`fr` collects on both engines: columns `v`, `V`, both `bigint`, rows `[10, 10]`, `[20, 20]`,
`[30, 30]`. The join is the divergence. Measured 2026-10-03 on Spark 4.1.2 and the installed
RePark wheel (`repark-1.5.1`).

| | Spark 4.1.2 | RePark |
|---|---|---|
| join result | columns `v`, `V`, `v`, `tag`; rows (collect order) `[10, 10, 10, "x"]`, `[30, 30, 30, "y"]` | `AnalysisException` |
| condition | none (no exception) | `AMBIGUOUS_REFERENCE` |
| SQLSTATE | none | `42704` |
| message parameters | none (no exception) | `getMessageParameters()` returned null |

Spark's message is empty because Spark does not raise. RePark's message, this run:

```text
Error during planning: [AMBIGUOUS_REFERENCE] Reference `_repark_jl_e5a34e47f20c4cc4bb7077acc73718f1`.`V` is ambiguous, could be: [`_repark_jl_e5a34e47f20c4cc4bb7077acc73718f1`.`V`, `_repark_jl_e5a34e47f20c4cc4bb7077acc73718f1`.`V`]. SQLSTATE: 42704
```

The qualifier is `_repark_jl_` plus 32 hex digits, and the field text is `V`. That qualifier
matches the replay freezer's internal-qualifier pattern, which that freezer writes as `__H`.
The hex above is the id from this run.

## Decisions (proposed)

Fix direction (*inferred*): resolve `d.v` on the parent frame, where it is one column, and
have `AMBIGUOUS_REFERENCE` drop a generated `_repark_jl_*` scratch qualifier the way `sql_id`
in `crates/repark-core/src/session/df_guards/case_bind.rs` already drops `_repark_` and
`__repark_` relations. `ambiguous_message` in `crates/repark-core/src/column_resolution.rs`
still prints the qualifier it is given. The scratch name is the `_repark_jl_` temp view minted
for a condition join.

| id | decision |
|---|---|
| D-1 | This cell returns Spark's two joined rows. |
| D-2 | An `AMBIGUOUS_REFERENCE` message does not contain a generated `_repark_jl_*` or `_repark_jr_*` qualifier. |

## Steps

| step | worker | what |
|---|---|---|
| 0 | clerk | Done in this card: the cell on both engines, Spark's rows, RePark's message. No product code. |
| 1 | executor | D-1 and D-2. Re-measure the cell against live Spark, including a true ambiguity whose message must stay user-facing. |

**Home:** `DataFrame._join_on_condition_h1` in
`python/repark/src/repark/spark/dataframe/core.py` (the `_repark_jl_` / `_repark_jr_` scratch
views) and `ambiguous_message` in `crates/repark-core/src/column_resolution.rs`. **Gates:**
`make verify` and a pin of this cell against Spark's rows, plus one real ambiguity whose
message names only user-facing qualifiers. This filing's gate is the docs checks.

## Pointers

- Up: [map.md](map.md)
- Siblings: [variance-alias-1-card-2026-10-03.md](variance-alias-1-card-2026-10-03.md),
  [coalesce-nan-double-1-card-2026-10-03.md](coalesce-nan-double-1-card-2026-10-03.md)
