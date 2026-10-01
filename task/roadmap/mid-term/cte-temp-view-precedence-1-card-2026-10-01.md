# CTE-TEMP-VIEW-PRECEDENCE-1 — CTEs must shadow same-named temp views on every write door

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, lines 69, 77
and 81; #894 re-verify 2 and 3). **Severity:** wrong results, not yet rated
(owner) — silent wrong data. **Status:** pre-existing on base. **Target:**
mid-term.

## The divergence it records

CTE-versus-temp-view name precedence gives silent wrong data on column-list
INSERT, static-partition OVERWRITE and CTAS. A CTE should shadow a same-named
temp view, as in Spark. (Line 69, verbatim.)

The facade's SQL region walker expands a CTE reference to the temp-view home
(`datafusion.public.<name>` or `spark_catalog.default.<name>`) when a
column-list INSERT precedes WITH (`INSERT INTO t (a,b) WITH x AS (...) SELECT
... FROM x`). This breaks even unshadowed CTEs on that door, and is related to
the CTE-versus-temp-view precedence card. (Line 77, verbatim.)

VT4-2: in a case-insensitive session a quoted CTE name whose case differs from
an unquoted reference (`WITH \`XV\` … FROM xv`) resolves to the temp view.
VT4-3: the `PARTITION (p)` and `PARTITION (p='v')` INSERT INTO doors have the
same facade CTE rewrite as the column-list door. Same on base. (Line 81,
verbatim.)

## Repro, verbatim from the source lines

```sql
INSERT INTO t (a,b) WITH x AS (...) SELECT ... FROM x;
-- RePark: CTE reference expanded to datafusion.public.x / spark_catalog.default.x (wrong)

WITH `XV` AS (...) SELECT ... FROM xv;
-- case-insensitive session; RePark: resolves to the temp view; Spark 4.1.2: the CTE
```

## Cause and suggested fix direction (a suggestion, not a decision)

The facade CTE rewrite fires on the write doors (column-list INSERT,
`PARTITION (p)` / `PARTITION (p='v')` INSERT, static-partition OVERWRITE,
CTAS) without CTE-first precedence. Any fix direction is suggested by the
verifier/orchestrator, not a decision.

## Clauses (draft)

- **C-001** A CTE shadows a same-named temp view on all five doors.
- **C-002** Unshadowed CTEs survive the column-list door unexpanded.
- **C-003** VT4-2 and VT4-3 pinned; one card, closed together.

## Pointers

- Up: [map.md](map.md)
