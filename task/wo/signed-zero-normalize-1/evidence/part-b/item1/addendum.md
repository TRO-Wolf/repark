# Item 1 addendum: the cells Fable's review asked for, and three code readings

Model `claude-opus-5-5`, 2026-11-12, main `276dc503`'s release build against Spark 4.1.2. Fable ran these on PyPI 1.5.6;
every cell here was re-measured on main with this directory's own `addendum_probe.py`. No Fable script was run or
copied. Raw legs: `raw/repark-276dc503-addendum.jsonl` and `raw/spark-addendum.jsonl` (the Spark leg also read
RePark's tables). Labels (part A's `label_of`): [addendum/table.md](addendum/table.md); bit patterns:
[addendum/bits.md](addendum/bits.md). There are 103 RePark cells: EQUAL 52, WRONG-READ 35, WRONG-WRITE 8, ORDER-DIFFER
2, REFUSED-WHERE-SPARK-ACCEPTED 3, NATIVE-REFUSED 3.

## 1. Results by group

| Group (request) | Cells | Result on main |
|---|---|---|
| **Q1**: one- and two-column `NOT IN` | N1, N2 | Spark: N1 keeps `-0.0` (`[1, 2]`), N2 compares IEEE (`[2]`), as Fable said. RePark: N1 EQUAL on both doors. **N2 refuses on both doors**: "Too many columns! The subquery should only return one column". The second witness exists only on Spark's side; RePark's refusal is a pre-existing over-refusal |
| **Q7**: delete-only MERGE, both zeros in the source match one target row | `Q7-merge-delete-only` × v2-cow, v2-mor, v3 | Spark deletes and does not raise (`[2]` left). **RePark: EQUAL on all three kinds today**; seat 2 must keep it that way |
| **Q2** as writes, INT columns | SW-except-all, SW-intersect-all, the `WITH … INSERT` and `INSERT … WITH` CTE forms; Spark door and native | `INSERT … EXCEPT ALL` stores nothing (Spark: 1, 1). `INTERSECT ALL` stores 1, 1, 1, 2, 2 (Spark: 1, 2, 2). WRONG-WRITE on both doors and in the `INSERT … WITH` form. `WITH … INSERT` refuses on both RePark doors ("not implemented") where Spark accepts |
| **Q6**: `F.expr` window NULL placement | F6-* (11 forms) | `F.expr` asc, `F.expr` desc, `first_value` desc and the dedup `withColumn(… row_number() OVER (ORDER BY m DESC)).filter('r = 1')` are WRONG-READ. The dedup keeps id 2, the NULL row, where Spark keeps id 3. `spark.sql`, `selectExpr`, `orderBy(F.expr)` and `sort` are EQUAL |
| **Integer-derived float keys** (Fable §5.1) | IK-group, IK-distinct, IK-join, IK-window, IK-cast-group | Spark door: all five WRONG-READ. `a / b` over (0, 5), (0, -5) gives two groups where Spark gives one; the join matches 0 where Spark matches 1; `CAST(0 AS DOUBLE) * -1` keys split. Native: `/` on integers is integer division (the native dialect), so only IK-group (integer key type) and IK-cast-group differ |
| **Sign-set NaN** (Fable §4.2) | NS-*, with NC-* as the canonical-NaN control | `sqrt(-1.0)`, `acosh(0.5)`, `atanh(2.0)` and `pow(-8.0, 0.5)` return `fff8…` on both doors; Spark returns `7ff8…`. Over (1.5, `sqrt(-1.0)`, 9.5): `max`/`min` give 9.5 and NaN (Spark: NaN and 1.5); `greatest`/`least` give 100 and NaN (Spark: NaN and -100); `ORDER BY` puts the NaN row first (ORDER-DIFFER). The canonical-NaN control is EQUAL everywhere. `WHERE d > 9.5` returns no rows on both engines, Spark included (the literal `9.5` is a decimal there), so it does not witness the sign |
| **NaN partition value** (Fable §4.1, Q10) | NP-* | See §2 |
| **The log family** | LG-* | On the Spark door, `ln`, `log10` and `log2` of -1.0 return NaN, and of 0.0 return -Infinity, where Spark returns NULL: WRONG-READ (6 cells, `LN-NONPOSITIVE-NULL-1` widened). `log(x)`, `log(b, x)` and `log1p` match Spark on the Spark door, but `log` gives NaN on native. Controls (`ln(1)`, `log(2, 8)`, `log1p(1)`) are EQUAL |

## 2. NaN in a float partition column (Q10)

Rows NaN, -0.0, 0.0, NULL and 1.5, written by RePark into `(id, d DOUBLE) PARTITIONED BY (d)` and the FLOAT twin
(Hadoop catalog), and read by both engines:

| Check | RePark's partitioned table | RePark, unpartitioned control | Spark reading RePark's partitioned table | Spark's own partitioned table |
|---|---|---|---|---|
| rows (`id 3`) | NaN | NaN | **NULL** | NaN |
| `WHERE isnan(d)` | **no rows** | `[3]` | **no rows** | `[3]` |
| `WHERE d = CAST('NaN' AS …)` | **no rows** | `[3]` | **no rows** | `[3]` |
| `.files` partition for the NaN file | **`d = NULL`** (two NULL partitions) | — | **`d = NULL`** | `d = NaN` |

- **This reproduces on main for DOUBLE and FLOAT: a wrong write.** The manifest's partition value is NULL.
- RePark's own `isnan` and `= NaN` lose the row, because partition pruning trusts the NULL value.
- PT-1 stays EQUAL because a plain read returns the data file's NaN.

## 3. Code readings

**9. What keeps a seat-2 wrapper out of `TableScan.filters` on the next pass: nothing today.**
- `push_down_filter` offers every non-volatile, subquery-free Filter predicate to the provider. It copies each one
  that is not `Unsupported` into `TableScan.filters` (`datafusion-optimizer-54.1.0/src/push_down_filter.rs:1155-1189`).
- `__repark_normalize_zero__` is `Immutable`, so it is not volatile.
- The fork's Iceberg provider answers `Inexact` for every filter (`crates/integrations/datafusion/src/table/mod.rs:236-242`,
  and `static_provider.rs:181-186`, fork `66901f02`). Its stated reason: "the scanner drops the filters it cannot push
  down".
- So on pass 2, a Filter that seat 2 wrapped in pass 1 (a non-literal comparison, CM-*) is copied into the scan's
  filters as `normalize(d) = normalize(e)`. Nothing new reaches an Iceberg evaluator, because the scanner cannot convert
  a UDF call and drops it. But the order's halt "a wrapper in `TableScan.filters`" would fire.
- **The design must name a mechanism.** The candidates are:
  - (a) seat 2 strips its own wrappers from `TableScan.filters`;
  - (b) a classification that reports `Unsupported` for any filter that contains the UDF;
  - (c) restate the halt as "no new predicate reaches an Iceberg evaluator", which is what the sketch's reason needs.
- This is a gap in the design, not a broken premise: sketch §3 only says seat 2 does not rewrite the scan's filters.

**10. MERGE does not depend on the `ON` key's shape for a float key.**
- The join is DataFusion's own plan of SQL text, `… FROM source JOIN target ON {on} …`
  (`crates/repark-iceberg/src/write/merge/merge_sql.rs:132-145`, run by `ctx.sql`, `merge/mod.rs:516`).
- Target pruning parses the user's `ON` text, not the optimized plan (`bare_equalities_from_on`, `write/scan_prune.rs:32`,
  called from `merge/target_scan.rs:19`). It prunes only integer keys of identical width
  (`identical_int_key_width`, `scan_prune.rs:248`, used at `:361`).
- So a float key never prunes today, and seat 2's wrapper cannot move a MERGE off a pruning path. The code-cited
  sentence suffices for design guard (c); no MERGE timing row is needed for that reason.

**11. Where the NaN partition value is lost: located to the fork, not confirmed to the line.**
- RePark routes rows with the fork's `PartitionValueCalculator` (`crates/repark-iceberg/src/write/distribution/router.rs:22, 33-36`;
  fork `crates/iceberg/src/arrow/partition_value_calculator.rs:151`).
- The arrow-to-literal step keeps NaN (`Literal::double`, fork `crates/iceberg/src/arrow/value.rs:324-332`).
- The fork's `Literal::try_into_json` maps a NaN float or double to JSON `null` (`crates/iceberg/src/spec/values/literal.rs:630-640`,
  `serde_json::Number::from_f64` returns `None` for NaN). That is the likely seat if the partition struct passes through
  it on its way to the manifest.
- **Not confirmed within this item's budget:** that the manifest path uses it, and whether RePark's `.files` reader adds
  its own NULL.
- **Sizing, if that is the seat:** a fork change. NaN cannot be a JSON number, so either the partition struct must not
  go through JSON, or the fork must use the Avro double directly. A small change, one or two files of the fork plus a
  fork pin bump in RePark, with a cross-engine cell (Spark reads RePark's NaN partition) as the pin. Not fixed.
