# MERGE-UPDATE-TS-WIDENING-1 — MERGE and UPDATE refuse STRING-plus-TIMESTAMP widening

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 70;
#894 re-verify 2). **Severity:** not rated. **Status:** pre-existing on base.
**Target:** mid-term.

## The divergence it records

MERGE and UPDATE refuse the STRING-plus-TIMESTAMP widening that Spark stores.

## Repro (from the source description; re-measure on the day)

```sql
-- target column TIMESTAMP, source expression STRING on the MERGE / UPDATE door
MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET ts = s.ts_string;
UPDATE t SET ts = '2024-01-01 10:00:00';
-- RePark: refusal; Spark 4.1.2: stores
```

## Cause and suggested fix direction (a suggestion, not a decision)

The MERGE/UPDATE store-assignment check does not admit the widening Spark
admits on the same cells. Any fix direction is suggested by the
verifier/orchestrator, not a decision.

## Clauses (draft)

- **C-001** Both doors store the widening with Spark's value.
- **C-002** The neighbouring STORE-TS widening cells (#894's VT2 family) are re-measured on the day.
- **C-003** No new bypass: refusals Spark keeps must still refuse.

## Pointers

- Up: [map.md](map.md)
