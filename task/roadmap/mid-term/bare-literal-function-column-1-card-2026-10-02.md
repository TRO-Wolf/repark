# BARE-LITERAL-FUNCTION-COLUMN-1 — a STRING column named current_timestamp/current_date resolves as the function

**Filed: 2026-10-02 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 91;
#894's round-6 re-check, finding **VT7-1**; verified present 2026-10-02).
**Severity: S1** (silent wrong data). **Status:** pre-existing on base, not
#894. **Target:** mid-term.

## The divergence it records

A STRING column named `current_timestamp` / `current_date` resolves as the
function: `SELECT` returns `now()` and stores the current time where Spark
uses the column. Cause, verbatim from the source line:
`bare_nullary.rs:9` lacks column-first for literal functions. (The file is
`crates/repark-spark/src/bare_nullary.rs`, verified present at this head.)
24 cells.

## Repro

The source line states the answers qualitatively — Spark uses the column,
RePark returns and stores the current time — but carries no verbatim
cell dump, so the 24 cells are not yet measured in this card. Repro to run
on the day, against live Spark 4.1.2:

```python
df = createDataFrame([("2024-01-02 03:04:05",)], ["current_timestamp"])
df.select("current_timestamp").collect()
df = createDataFrame([("2024-01-02",)], ["current_date"])
df.select("current_date").collect()
```

plus the matching `spark.sql` SELECT, INSERT and store legs that make up
the 24 cells.

## Scope

Column-first resolution for bare literal-function names
(`current_timestamp`, `current_date`, and any other name the
`bare_nullary` mapping answers) on every door that reaches it.

## Out of scope

- VT6-1 / VT6-2 (folded into #894, merged `5f917f93`).
- Genuine bare-function calls with no such column present: they must keep
  answering `now()` / the current time.

## Clauses (draft)

- **C-001** The repro frames answer the column's values, not the clock.
- **C-002** The 24 VT7-1 cells answer Spark.
- **C-003** Bare-function calls without a same-named column are unchanged.

## Pointers

- Up: [map.md](map.md)
