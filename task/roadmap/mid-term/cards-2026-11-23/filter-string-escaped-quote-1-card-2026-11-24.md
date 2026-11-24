# Card FILTER-STRING-ESCAPED-QUOTE-1: a backslash-escaped quote in a `DataFrame.filter` string drops rows

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside review of the SQL text
rewrites and a measurement here. **Severity:** S1 (owner, 2026-11-24): a silent wrong answer. **Status:** filed, not
dispatched.

## Measured

Measured on a build of main at `fa7d75e4` and on Spark 4.1.2, in both ANSI modes, with the same answers in each.
`df` is `SELECT 1 AS id, 'it''s' AS s UNION ALL SELECT 2, 'ok'`.

| Call | Spark 4.1.2 | RePark |
|---|---|---|
| `df.filter("s = 'it\\'s' AND id = 1").select("id")` | 1 | **no rows** |
| `df.filter("s = 'it\\'s'").select("id")` | 1 | **no rows** |
| `df.filter("s = 'it''s' AND id = 1").select("id")` (doubled quote) | 1 | 1 |
| `spark.sql("… WHERE s = 'it\\'s'")` (the same predicate in SQL) | 1 | 1 |

Only the `filter(str)` door is wrong. SQL with the same predicate answers correctly.

## Cause (read, not yet traced)

- `filter(str)` backtick-quotes column names before it plans the text (`_quote_filter_sql_identifiers` in
  `python/repark/src/repark/spark/column_fields.py`, using `filter_quote.py`).
- Its string-literal pattern knows the doubled quote `''` but not the backslash escape `\'`. It ends the literal at
  `\'`, reads the `s` after it as a column, and quotes it. The literal then no longer equals `it's`.

## Not yet known

- The other `filter(str)` and `selectExpr` shapes on the same splitter: comments holding a quote, and columns named
  `and`, `not` or `r`. Most of them refuse loudly today.
- Whether the fix is a real tokenizer in that pass, or removing the pass in favour of planner-side name resolution.

Up: [map.md](map.md).
