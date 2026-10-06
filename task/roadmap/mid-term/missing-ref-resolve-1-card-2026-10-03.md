# Card MISSING-REF-RESOLVE-1 — non-join missing references resolve the way Spark does

**Date:** 2026-10-03 · **Filed by:** ATTR-ID-1 SJ-6 (card only, no implementation) · **Source:**
the verifier's V-3 (`p2_lineage` probes) and the self-join design's §3 "V-3 is its own card"
paragraph ([attr-id-1-ledger.md](../../ledgers/staging/attr-id-1-ledger.md), SJ-6 correction).

## Why

A parent Column whose attribute id is absent from the child frame falls through
`_bind_stable_id_column`'s pass-through and binds by display name. Option A closed that
fall-through for join-made absences (`renewed_absent`, the preparer's MISSING arms), but
Spark resolves non-join absences — `withColumn` replace, alias and rename swaps, `toDF` —
through a different mechanism (`ResolveMissingReferences`), and RePark still binds them by
name. Measured on live Spark 4.1.2 (`p2_lineage.spark.json`, `d = [(1,10),(2,20),(3,30)]`):

| Cell | Shape | Spark answer |
|---|---|---|
| `F_wc_replace_sel_parent` | `d.withColumn('v', d.v + 1).select(d.v)` | `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` naming `v` |
| `F_wc_replace_filter_parent` | `d.withColumn('v', -d.v).filter(d.v > 15)` | rows `[[2,-20],[3,-30]]` |
| `F_sel_swap_alias_sel_parent` | `d.select(d.v.alias('id'), d.id.alias('v')).select(d.v)` | `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` naming `v` |
| `F_sel_swap_alias_filter_parent` | same frame, `.filter(d.v > 15)` | rows `[[20,2],[30,3]]` |
| `F_wcr_sel_parent` | `d.withColumnRenamed('v', 'w').select(d.v)` | `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT` naming `v` |
| `F_wcr_filter_parent` | same frame, `.filter(d.v > 15)` | rows `[[2,20],[3,30]]` |
| `F_tosdf_swap_sel_parent` | `d.toDF('v', 'id').select(d.v)` | `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` naming `v` |
| `T_wc_replace_sel_parent` | the replace-select under `caseSensitive=true` | `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` naming `v` |

`select` refuses on a missing id; `filter` and `orderBy` resolve through the child's input.
The union, view and SQL shapes in the same probe file decide the rest of the rule.

## Decisions (proposed)

| id | decision |
|---|---|
| D-1 | `select` refuses `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` on a missing id, with Spark's name echo. |
| D-2 | `filter` and `orderBy` resolve a missing parent reference through the child's input, answering Spark's rows. |
| D-3 | The card flips `test_s4_replaced_output_parent_ref_reads_new_value` (today asserts the new value `[11]`) and un-xfails the two V-1 pins `test_sj4_v1_replaced_parent_filter` (Spark `[[2,-20],[3,-30]]`) and `test_sj4_v1_swapped_alias_parent_select` (Spark MISSING naming `v`). |
| D-4 | Measure first: the full `p2_lineage` grid under both case rules, plus the union, view, SQL, aggregate, cast, `toDF` and `selectExpr` shapes, before ruling the per-surface split. A shape Spark answers keeps answering; a shape Spark refuses refuses. |

### D-4 orderBy grid (2026-10-06, `halt3.py`, `frame = [(1,30),(2,None),(3,10),(4,20)]`)

| Cell | Head | Main | Spark |
|---|---|---|---|
| `single_v_desc` | `[[-10,3],[-20,4],[-30,1],[null,2]]` | same as head | `[[-30,1],[-20,4],[-10,3],[null,2]]` |
| `single_v_plain` | `[[null,2],[-30,1],[-20,4],[-10,3]]` | same as head | `[[null,2],[-10,3],[-20,4],[-30,1]]` |
| `withColumn_replace_desc` | `[[3,-10],[4,-20],[1,-30],[2,null]]` | same as head | `[[1,-30],[4,-20],[3,-10],[2,null]]` |
| `withColumn_replace_sort_plain` | `[[2,null],[1,-30],[4,-20],[3,-10]]` | same as head | `[[2,null],[3,-10],[4,-20],[1,-30]]` |
| `flip_twins_desc` | `[[-10,11,3],[-20,21,4],[-30,31,1],[null,null,2]]` | `ERR AMBIGUOUS_REFERENCE` | `[[-30,31,1],[-20,21,4],[-10,11,3],[null,null,2]]` |
| `flip_twins_asc` | `[[null,null,2],[-30,31,1],[-20,21,4],[-10,11,3]]` | `ERR AMBIGUOUS_REFERENCE` | `[[null,null,2],[-10,11,3],[-20,21,4],[-30,31,1]]` |

SM-1 moves `flip_twins_desc` / `flip_twins_asc` from the head rows above to
`ERR AMBIGUOUS_REFERENCE`, equal to main; Spark still answers through the hidden
projection.

## Steps

| step | worker | what |
|---|---|---|
| 0 | clerk | Measure (live Spark): the D-4 grid, recorded beside the probe. No product code. |
| 1 | executor | D-1/D-2 wiring on the pass-through path; D-3 flips; pins per surface. |

**Home:** the `_bind_stable_id_column` pass-through in
`python/repark/src/repark/spark/column_fields.py`, `test_attr_id_1_s4.py`,
`test_attr_id_1_sj4.py`, a new oracle pin file. **Gates:** `make verify`, `make preflight`,
the parity suite, the S0 replay with zero moves away from Spark.

## Pointers

- Up: [map.md](map.md)
