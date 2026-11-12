## 7. Table-completeness check (Q3): every named seat, its cells, its disposition

Seats are named by the brief (key sites, doors, types, values), by sketch §3's node list, by sketch §6–§7, and by the
review-cap memo §7 (seat-2 idempotence). "Clause" means the cell's float rows must reach the truth after part 1;
"control" means the row must not move; "declared" means a registry row, not a fix; "card" means out of part 1 with
an id; NOT MEASURED means no cell ran in step 0 and the builder adds it in item 1.

| Named seat | Cells (site ids) | Doors measured | Disposition |
|---|---|---|---|
| `Aggregate::group_expr`, column, computed and key-list keys | GB-1, GB-2, GB-3, GB-7, GB-9 | all four (GB-7, GB-9: spark-sql, native) | clause |
| Group key **output value** (Spark outputs `0.0`) | GB-8, and the value columns of GB-1/DI-1 | spark-sql, native | clause (part B normalises group outputs, sketch §6) |
| `GroupingSet` members: ROLLUP, CUBE, GROUPING SETS | GB-4, GB-5, GB-6 | spark-sql, typed, native | clause |
| `Repartition` Hash / `DISTRIBUTE BY` then aggregate | GB-10; item 1's X-repartition, X-repartition-typed | typed, native (spark-sql refuses `DISTRIBUTE BY` for every type) | clause on typed and native as plain group keys; the Spark-door refusal is carded `SPARK-DISTRIBUTE-BY-1` (decimal twin refuses too). **Item 1:** no RePark cell reaches a `Repartition` node: native GB-10's text has no `DISTRIBUTE BY`, `DISTRIBUTE BY` has no physical plan on native either, and the typed `repartition(4, 'd')` leaves no `Repartition` in the optimized plan ([plans](../item1/plans.md)). The node row stays a plan-shape pin over a hand-built plan |
| Pivot value discovery | GB-11 (typed) | typed | **confounded**: the decimal twin is WRONG-READ (empty pivot cells: Spark NULL, RePark 0). Carded `PIVOT-EMPTY-CELL-COUNT-1`; the float rows join the clause once the card is fixed or the cell is re-cut to `sum` |
| `Distinct::All` (`SELECT DISTINCT`, one and two columns) | DI-1, DI-2 | all four / three | clause |
| Distinct aggregates: single (`single_distinct_to_groupby`), multi-distinct, under a group key, `sum(DISTINCT)` | DI-3, DI-4, DI-5, DI-7 | all four / three | clause (DI-3, DI-4); DI-5 and DI-7 are EQUAL by seed (each group holds one sign; a sum of zeros is zero) and stay as controls |
| `dropDuplicates` | DI-6 | typed | clause |
| `Distinct::On` | item 1's X-distinct-on | native | MEASURED in item 1 ([plans](../item1/plans.md)): at EXPLAIN it is already an `Aggregate` on `d` with `first_value(… ORDER BY d, id)` and a `Sort`, as sketch §3 says; ORACLE-MISSING (Spark has no `DISTINCT ON`; E9). Plan-shape pin only |
| `Window` `partition_by` | WI-1, WI-6 | all four / three | clause |
| `Window` `order_by`: rank, dense_rank, RANGE peers, row_number with a tie-breaker | WI-2, WI-3, WI-4, WI-5 | all four / three | clause. WI-2 f-expr and WI-2/3/5 native are **confounded** (decimal twin wrong: native and `F.expr` window ORDER BY put NULLs last; section 4 lists them) |
| `Window` distinct arguments | none | — | NOT EXPRESSIBLE on Spark (no `DISTINCT` window aggregates); plan-shape pin only |
| `Sort` with a tie-breaker; `TopK`; descending | OB-1, OB-2, OB-3 | all four / three | clause. The classifier says ORDER-DIFFER for OB-1/OB-3 although the order is fully determined by the tie-breaker: Spark ties the zeros and RePark sorts `-0.0` first, so these are wrong orders and must reach EQUAL. Native OB-1/OB-2 are **confounded** (NULLS LAST default) |
| `Sort`, zero ties only, no tie-breaker | OB-4 | spark-sql, native | declared ORDER-DIFFER (sketch §6, registry row) |
| `min` / `max` over zeros | MM-1 | spark-sql, typed, native | declared SIGN-ONLY (ruling G "min/max sign and zero tie order declared") |
| Non-literal comparisons in any node: `= <> < <= <=> IS DISTINCT FROM`, in a filter and in a projection | CM-1…CM-5, CM-10, CM-11 | all four / three | clause. CM-11 on spark-sql is **confounded**: `d IS DISTINCT FROM e` (two columns) refuses on the Spark door for every type, on main and on a pre-part-A build (`idf_check.py`); carded `SPARK-DOOR-IS-DISTINCT-COLUMNS-1` |
| `Between` with non-literal bounds; non-literal `InList` | CM-6, CM-7 | spark-sql, typed, native | clause |
| Simple `CASE e WHEN w`, non-literal `WHEN` (D8, left to part B by part A's F2) | CM-8 | spark-sql, f-expr, native | clause |
| Simple `CASE` over a zero literal | CM-9 | spark-sql, f-expr, native | control (part A, EQUAL on main) |
| Aggregate `FILTER` clause | CM-12 | spark-sql, native | clause |
| `Join::on`: inner, left, right, full, semi, anti | JN-inner … JN-anti, both single-sign builds | all four | clause |
| `Join::on`: USING, NATURAL, key list (float + int), two float keys, null-safe `<=>`, comma join (`eliminate_cross_join`) | JN-using, JN-natural, JN-keylist, JN-twofloat, JN-nullsafe, JN-comma | spark-sql, native (+ typed where a form exists) | clause; JN-keylist / un is EQUAL by seed (control) |
| `Join::on` across columns (`t.k = u.k2`) | JN-filter | spark-sql, native | clause; the un build is EQUAL by seed (control) |
| `Join::filter`, non-literal `<` | JN-nonequi | spark-sql, native | clause (up); un EQUAL by seed |
| Sort-merge physical join | JN-sortmerge | spark-sql, native | clause |
| Decorrelated `IN`, `EXISTS`, `NOT EXISTS`, scalar subquery | SQ-in, SQ-exists, SQ-notexists, SQ-scalar | spark-sql, native | clause |
| `NOT IN` subquery (null-aware anti join) | SQ-notin, SQ-notin-ieee | spark-sql, native | **owner question Q1**: Spark's default plan keeps `-0.0` distinct here (null-aware broadcast anti join), and the same query with that join off, `NOT EXISTS` and a literal `NOT IN` all compare IEEE (`raw/spark-naaj-check.jsonl`). Default: the IEEE row (SQ-notin-ieee) is the clause and SQ-notin's Spark answer is declared an oracle defect |
| `= ANY (subquery)` (`SetComparison`) | SQ-any | spark-sql, native | out: Spark 4.1.2 refuses the syntax for every type (ACCEPTED-WHERE-SPARK-REFUSED on the decimal twin too); not a key-site question |
| `UNION` distinct; a both-sign input deduplicated | SO-union, SO-union-in | spark-sql, typed, native | clause |
| `UNION ALL` | SO-unionall | spark-sql, typed, native | control (stored signs must survive: EQUAL today) |
| `INTERSECT`, `EXCEPT` | SO-intersect, SO-except | spark-sql, typed, native | clause (SO-intersect / up EQUAL by seed) |
| `INTERSECT ALL`, `EXCEPT ALL` | SO-intersectall, SO-exceptall | spark-sql, native (typed refuses by design) | **confounded**: the decimal twin is WRONG-READ on both doors (multiset counts lost: `INTERSECT ALL` returns every left duplicate, `EXCEPT ALL` removes all of them). A non-signed-zero silent wrong read inside the label's scope; carded `SQL-SETOP-ALL-MULTISET-1` (owner question Q2) |
| `RecursiveQuery { is_distinct }` | SO-recursive; item 1's X-recursive (plan shape) | spark-sql, native | out: Spark refuses `UNION` in a recursive CTE; plan-shape pin only ([plans](../item1/plans.md)) |
| Temporary view over a float table | TV-1 | spark-sql (native has no temp views) | clause |
| Native `PREPARE` / `EXECUTE`, group key and join key (review-cap memo §7: seat-2 idempotence) | PR-1, PR-2 | native; spark-sql runs the literal twin | clause; plus the EXPLAIN one-wrapper pin |
| MERGE `ON` float key: matched update, not-matched insert | WR-merge-on, WR-merge-notmatched | spark-sql, three kinds; native refuses MERGE | clause (merge-notmatched / un EQUAL by seed) |
| MERGE cardinality with both zeros in the source (Table B m1) | WR-merge-card | spark-sql, three kinds | clause: RePark must raise `MERGE_CARDINALITY_VIOLATION` like Spark (a user-visible change, sketch §6) |
| DELETE / UPDATE with an `IN` or `EXISTS` subquery on a float key | WR-delete-in, WR-delete-exists, WR-update-in | spark-sql, three kinds; native refuses a subquery in DML | clause |
| INSERT … `NOT IN` subquery | WR-insert-notin, WR-insert-notin-ieee | spark-sql (three kinds), native | as Q1 |
| CTAS from `GROUP BY` / `DISTINCT` (stored group output) | WR-ctas-group, WR-ctas-distinct | spark-sql, three kinds; native CTAS refuses on the memory catalog | clause (stores `0.0`, like Spark) |
| Identity partition on a float column: stored values, GROUP BY, count(DISTINCT), the `.partitions` metadata table | PT-1…PT-4 | spark-sql | PT-1 control (stored signs kept); PT-2, PT-4 clause; PT-3 (`.partitions` merges `-0.0` with `0.0` and NaN with NULL, `partition_check.py`) is metadata-table code outside the plan: carded `ICEBERG-PARTITIONS-TABLE-FLOAT-1` |
| NaN payloads (`sqrt(-1.0)`, `-NaN`) in group, distinct, join, order, window and count(DISTINCT) keys | NN-1…NN-6 | spark-sql, native | clause (ruling G "canonical positive NaN for the UDF") |
| Types: DOUBLE, FLOAT, decimal control | every site | — | carried; decimal is the control column (section 3b) |
| A FLOAT key joined to a DOUBLE key (coercion cast; order §5) | item 1's X-float-double | spark-sql, native | MEASURED in item 1: WRONG-READ on both doors (the zero rows match by sign only); clause, the wrap goes outside the cast |
| Values: `0.0`, `-0.0`, NaN, mixed groups | seeds `t`, `up`, `un`, `ub`, `nn` | — | carried; every seed's stored signs and NaN bits are read back (section 6) |
| Spill with float keys (sketch D16) | item 1's SP-group, SP-window-order, SP-window-partition, SP-sortmerge | spark-sql, native | MEASURED in item 1 ([spill](../item1/spill.md)): every cell spills (`spill_count` 2 to 8) under a 96M pool with 4 partitions on 2M rows, and all 16 are WRONG-READ on the base; clause |
| Hash-join dynamic filters and sort elision on wrapped keys | none | — | NOT MEASURED here: a cost item (order, cost gates) |
| `TableScan.filters` untouched by seat 2 | item 1's X-compare-filter (plan shape) | spark-sql, native | pin (sketch §3); no oracle. The base plans are recorded verbatim in [plans](../item1/plans.md) |
| The flat live matrix's floatkeys cells | section 8 | spark-sql | clause; the seed finding (Q3) is resolved on main by #1307 ([q3-rerun](../q3-rerun/table.md)) |

## 8. The flat matrix's floatkeys cells (G6) and the part 1 cells that cover them

The 28 floatkeys cells (`_flat_matrix_cells.py`: 14 shapes × DOUBLE, FLOAT) were run locally with their exact seeds
and statements (variant `matrix`), and again with the seeds spelled `CAST('-0.0' AS <type>)` (variant `signed`).

**Measured finding.** The matrix seeds write `-0.0` as a bare literal. On the Spark door that literal is a decimal and
has no sign, so **both engines store `+0.0`** (section 6, `fk-matrix-*`). All 28 cells are EQUAL on main, and stay
EQUAL whether or not part 1 lands: they cannot detect a signed-zero key. With signed seeds, all 28 are non-EQUAL on
main (22 WRONG-READ, 2 SIGN-ONLY+WRONG-READ, 4 WRONG-WRITE). Q3: resolved on main by #1307, whose seeds give the
same 28 non-EQUAL labels on `276dc503` ([q3-rerun](../q3-rerun/table.md)).

| Matrix shape | matrix seed | signed seed | Part 1 cells that cover it |
|---|---|---|---|
| K1-group-by | EQ / EQ | WR / WR | GB-1 (all doors), GB-8 |
| K2-distinct | EQ / EQ | WR / WR | DI-1 |
| K3-count-distinct | EQ / EQ | WR / WR | DI-3 |
| K4-inner-join | EQ / EQ | WR / WR | JN-inner / un |
| K5-left-semi | EQ / EQ | WR / WR | JN-semi / un |
| K6-left-anti | EQ / EQ | WR / WR | JN-anti / un |
| K7-full-outer | EQ / EQ | WR / WR | JN-full / un |
| K8-window-partition | EQ / EQ | WR / WR | WI-6, WI-1 |
| K9-intersect | EQ / EQ | SO+WR / SO+WR | SO-intersect / un |
| K10-except | EQ / EQ | WR / WR | SO-except / un |
| K11-union-distinct | EQ / EQ | WR / WR | SO-union / un |
| K12-in-subquery | EQ / EQ | WR / WR | SQ-in / un |
| W1-merge-on | EQ / EQ | WW / WW | WR-merge-on / un |
| W2-delete-in | EQ / EQ | WW / WW | WR-delete-in / un |

(DOUBLE / FLOAT.) Every matrix shape has a part 1 cell on the same site, the same single-sign build (`u = {-0.0}`)
and both types; part 1's cells add the opposite build, the decimal control, NaN and three more doors.
