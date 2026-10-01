# CONST-EVAL-CEILING-1 — replace the mirror constant evaluator with a ceiling check (owner's card)

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, lines 76 and
67; owner ruling, 09-30 evening). **Severity:** wrong results, not yet rated
(owner) — false equalities admit oversized arrays. **Target:** mid-term.

## The defect it records

Replace the mirror constant evaluator (`cardinality.rs`
const_i128/const_f64/exact_i128, and `cardinality_nullif.rs`) with a ceiling
check on DataFusion's ExprSimplifier/ConstEvaluator output. Rounds 5–10 of
#888 were all this defect class. (Owner ruling, verbatim from line 76.)

At base `02c2f1be` the existing home is
`crates/repark-functions/src/cardinality.rs` (verified present 2026-10-01;
`const_i128`/`const_f64` defined there); no `cardinality_nullif.rs` file
exists at this base, so that half of the ruling resolves against the #888
head.

## Known instance (line 67, carried verbatim)

Pre-existing in base's cardinality evaluator: `const_i128`'s
coalesce/greatest/least/CASE arms treat an unknown (e.g.
nullable-cast-wrapped) branch as NULL and skip it, so
`nullif(101, coalesce(CAST('0' AS INT), 101))` proves a false equality and a
101-element array passes a ceiling of 100. Base and #888 both do this. Fix
direction: distinguish 'unknown' from 'known NULL' in the constant evaluator.
That direction is suggested by the verifier/orchestrator, not a decision.

## Repro, verbatim from the source line

```sql
nullif(101, coalesce(CAST('0' AS INT), 101))
-- RePark (base and #888): proves equality (wrong); Spark 4.1.2: unknown, no proof
```

## Clauses (draft)

- **C-001** The mirror evaluator is gone; ceilings check DataFusion's ConstEvaluator output.
- **C-002** The `nullif`/`coalesce` repro no longer proves the false equality.
- **C-003** Every round-5–10 #888 defect-class cell is re-measured on the day.

## Pointers

- Up: [map.md](map.md)
