# SPARK-STRING-TS-FUNCTIONS-1 — register Spark string(), timestamp() and current_user()

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 82;
function coverage, #894 fold 4). **Severity:** not rated. **Target:**
mid-term.

## The divergence it records

RePark registers no Spark `string()`, `timestamp()` or `current_user()`
function (datafusion-spark 54.1 ships none). `SELECT string(1)` raises
`UNRESOLVED_ROUTINE` where Spark answers.

## Repro, verbatim from the source line

```sql
SELECT string(1);
-- RePark: UNRESOLVED_ROUTINE; Spark 4.1.2: answers
```

The same gap covers `timestamp()` and `current_user()`.

## Cause and suggested fix direction (a suggestion, not a decision)

The names are absent from the registry because the underlying datafusion-spark
54.1 ships none; RePark must add its own shims with Spark's semantics. Any fix
direction is suggested by the verifier/orchestrator, not a decision.

## Clauses (draft)

- **C-001** All three functions answer Spark on both SQL doors.
- **C-002** Unknown-function refusals still raise `UNRESOLVED_ROUTINE`.
- **C-003** Oracle cells recorded against live Spark 4.1.2 on the day.

## Append 2026-11-16: `int(c)` and `bigint(c)` (records clerk, `claude-haiku-5-5`)

`int(c)` and `bigint(c)` raise `UNRESOLVED_ROUTINE` in both ANSI modes (the critic's s05 and s06, re-measured at step 0 of CAST-LITERAL-UNWRAP-1). Spark answers each as `CAST`. This card names `string()` and `timestamp()` only. The step 0 default, S3: append the integer cast-name functions to this card. Once registered they must lower to the same `Cast`, so the recommended seat of CAST-LITERAL-UNWRAP-1 (seat W, the shield) covers them too. Source: [sketch.md](../../wo/cast-literal-unwrap-1/sketch.md), section 6, item 6.

## Pointers

- Up: [map.md](map.md)
