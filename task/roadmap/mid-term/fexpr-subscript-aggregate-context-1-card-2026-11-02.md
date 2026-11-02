# Card FEXPR-SUBSCRIPT-AGGREGATE-CONTEXT-1: an `F.expr` map subscript inside `groupBy`, `agg` or a scalar-key filter refuses, where Spark answers

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` (records clerk) for the orchestrator, from the MAP-STRUCT-KEY-SUBSCRIPT-1 round-3 verifier verdict (F5) and the Muse MAX critic's round-3 hand-back (S3-1).

**Status:** filed; grade C, P3; not dispatched. Pre-existing: head equals base.

**Why:** A map subscript written as `F.expr` text is planned as a lookup in a DataFrame select, `withColumn`, `withColumns`, `selectExpr` and `orderBy` (the `bound_projection` route, `map_array_subscripts_as_lookups`). In a `groupBy`, inside `agg`, and in a scalar-key filter the same subscript still reaches `array_element` on a map, which refuses with `array_element does not support type Map`.

**Measured (Spark 4.1.2; RePark refuses all of them on the head and the base):**

```text
c.groupBy(F.expr("m[named_struct('j', 1, 'k', 2)]").alias("v")).count().orderBy(F.col("v").asc_nulls_first())
  Spark: [[None, 1], [7, 1]]    RePark: refuses
c.agg(F.max(F.expr("m[named_struct('j', 1, 'k', 2)]")).alias("mx"))
  Spark: [[7]]                  RePark: refuses
sc.filter(F.expr("mi[1] = 'one'")).select("id")
  Spark: [[1]]                  RePark: refuses
```

The Muse critic's S3-1 is the same shape: `c.agg(F.max(F.expr("m[named_struct('j', 1, 'k', 2)]")))` over rows `(1, {(1,2): 7})` and `(2, {(1,5): 9})` gives Spark `[[7]]`. The SQL `max` of the same subscript answers.

**Fix (proposed):** plan an `F.expr` map subscript as a lookup in the aggregate and the scalar-key filter contexts, as the select route already does, so `array_element` never reaches a map in those contexts. The pins are the Spark answers above.

**Retires:** the card when the three cells and the Muse S3-1 cell answer Spark's values in the writing session and in a fresh session, pinned.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) clauses C-045 and C-040 (3); the select route is C-038.

Up: [map.md](map.md).
