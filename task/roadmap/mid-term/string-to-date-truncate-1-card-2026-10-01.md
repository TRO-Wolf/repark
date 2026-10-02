# STRING-TO-DATE-TRUNCATE-1 — string_to_date refuses trailing junk Spark truncates

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 61;
finding **VN6-4**, S3, pre-existing cast leaf). **Severity:** wrong results,
not yet rated (owner). **Target:** mid-term.

## The divergence it records

`string_to_date` (`crates/repark-functions/src/cast_map/leaf.rs:562-576`)
rejects `'2024-01-01 junk'` and `'2024-01-01Tjunk'`, where Spark truncates to
the date. #888's nullif now reaches it with ANSI off:
`nullif(DATE '2024-01-01', '2024-01-01 junk')` returns the date where Spark
returns NULL.

Note (2026-10-01): at base `02c2f1be` the cited file
(`crates/repark-functions/src/cast_map/leaf.rs`) holds 455 lines, so the
`:562-576` range resolves against the #888 head, not this base; the facts
above are carried verbatim from the source line.

## Repro, verbatim from the source line

```sql
nullif(DATE '2024-01-01', '2024-01-01 junk')
-- RePark (ANSI off): 2024-01-01; Spark 4.1.2: NULL
```

## Cause and suggested fix direction (a suggestion, not a decision)

The cast leaf refuses trailing junk where Spark truncates to the date. Any fix
direction (truncate-then-compare in the leaf, or a nullif-side guard) is
suggested by the verifier/orchestrator, not a decision.

## Clauses (draft)

- **C-001** The two refused spellings truncate to the date, matching Spark.
- **C-002** The `nullif` repro answers NULL with ANSI off.
- **C-003** VN6-4 closes; neighbouring ANSI-on refusal cells still refuse as Spark does.

## Pointers

- Up: [map.md](map.md)
