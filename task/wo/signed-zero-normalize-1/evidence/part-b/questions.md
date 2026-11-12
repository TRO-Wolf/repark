# SIGNED-ZERO-NORMALIZE-1 part B: questions the order needs (Q3 resolved on main; the rest NOT RULED)

Drafted by `claude-opus-5-5`, 2026-11-12. Every question here except Q3 is **NOT RULED**; Q3 is resolved on main by
#1307. None is covered by the rulings the order quotes. Each open question comes with a default and a one-line reason. Cell ids are from [step0/table.md](step0/table.md).

## Q1. `NOT IN (subquery)`: which Spark answer is the truth? — NOT RULED

**Measured** (`step0/raw/spark-naaj-check.jsonl`):
- Spark 4.1.2's default plan for `d NOT IN (SELECT d FROM u)` is a null-aware broadcast hash anti join. It keeps
  `-0.0` distinct from `0.0`: `-0.0 NOT IN (0.0)` is true. This holds with broadcast allowed or not.
- With `spark.sql.optimizeNullAwareAntiJoin = false`, Spark answers with IEEE equality.
- The same query as `NOT EXISTS`, and a literal `NOT IN`, are IEEE in every configuration.
- RePark today matches the default plan (SQ-notin EQUAL, WR-insert-notin EQUAL). Against the IEEE answer it is
  wrong (SQ-notin-ieee WRONG-READ, WR-insert-notin-ieee WRONG-WRITE).

**Default:** the truth is the IEEE answer, from SQ-notin-ieee and WR-insert-notin-ieee. Seat 2 wraps null-aware
anti-join keys too. Spark's default-plan answer is declared an oracle defect, as `NAAJ-WRONG`, on the PRUNED-WRONG
precedent.

**Reason:** a Spark answer that depends on the physical plan is not Spark SQL semantics, and every other Spark plan
for the same predicate is IEEE.

## Q2. SQL `INTERSECT ALL` / `EXCEPT ALL` return wrong multisets for every type — NOT RULED

**Measured.** On the decimal control:
- `INTERSECT ALL` returns every left duplicate; Spark keeps the minimum multiplicity.
- `EXCEPT ALL` removes all copies; Spark subtracts counts.

Both happen on the Spark door and on the native door (SO-intersectall, SO-exceptall: decimal twin WRONG-READ). This is
a silent wrong read inside the label's scope. It is not signed zero. The facade already refuses the DataFrame forms
for this reason (`core.py:2632-2660`); the SQL forms are not guarded.

**Default:** card it as `SQL-SETOP-ALL-MULTISET-1`, S2, outside part B. Until it is fixed, a refusal on the Spark door
like the DataFrame one, or a line on the label's exclusion list. Part 1 still wraps these operators' float pairs, and
its pin checks the wrap, not the multiset.

**Reason:** fixing multiplicity is a set-operation rewrite, not a key wrap, and it would grow part B past its cap.

## Q3. The flat matrix's floatkeys seeds — RESOLVED on main by #1307 (no change in part 1)

**Step 0** (main `ae247767`): `_flat_matrix_cells.py` seeded `-0.0` as a bare literal, which the Spark door types as a
sign-less decimal. Both engines stored `+0.0`, so all 28 floatkeys cells were EQUAL and could not detect a signed-zero
key. With signed seeds, all 28 were non-EQUAL (table §8).

**Resolved on main.** #1307 (`276dc503`) changed the seeds to `CAST('-0.0' AS DOUBLE)` (`NEGATIVE_ZERO`) for both the
DOUBLE and the FLOAT tables. Re-run of the step-0 floatkeys part on `276dc503`, with the matrix's own seeds
([q3-rerun/table.md](q3-rerun/table.md)):
- all 28 cells are non-EQUAL: 22 WRONG-READ, 2 SIGN-ONLY+WRONG-READ (K9-intersect), 4 WRONG-WRITE (W1, W2);
- every cell's label equals its `signed` variant's, on both types;
- the FLOAT column keeps the sign: `CAST('-0.0' AS DOUBLE)` inserted into `FLOAT` reads back as `-0.0` on both
  engines (`fk-matrix-FLOAT-t`, `fk-matrix-FLOAT-u`);
- the old bare seeds (variant `bare`) still give 28 EQUAL, as step 0 recorded.

**Part 1 changes no seed.** Its PR drops the 28 float cells from `KNOWN_OPEN` (`known_open_unit`,
`_flat_matrix_cells.py:288`), since they XPASS under strict xfail once the keys are fixed; G6's floatkeys job runs
after the merge.

## Q4. The cost base: `04d84ab9` (ruled) or the branch base `ae247767` — NOT RULED

The ruled base, PR A's merged head, is `04d84ab9`. The branch sits on `ae247767`, which adds #1303 (CTAS cleanup),
#1304 (LISTVIEW key-name join) and #1310 (tests). #1304 touches a join path that part 1 also touches.

**Default:** gate against `04d84ab9` as ruled, and report `ae247767` beside it as a diagnostic. A result that fails
against `04d84ab9` but passes against `ae247767` is a HALT for a ruling, not a pass.

**Reason:** this keeps the ruled bar and still separates part B's cost from #1304's.

## Q5. What the two per-door switches are — NOT RULED (their existence and "both on" are ruled)

**Default:**
- A door-internal policy value: `literals` (seats 1/1b) and `keys` (seat 2). It is set by the door's
  `SessionExtension`, both `true` on both doors.
- No user-visible session config.
- Every rule is installed on every door and reads the policy at rewrite time, so the rule-list pins are one list.
- `literals = false` also turns off part A's two seats.

**Reason:** this matches sketch §8 and adds no config surface. The fallback carve-out needs only `keys = false` on
native.

## Q6. Window `ORDER BY` NULL ordering on the Spark door's `F.expr` route — NOT RULED

**Measured.** `F.expr('rank() OVER (ORDER BY m)')` puts NULLs last on the Spark door. Spark puts them first, and the
typed and `spark.sql` routes match Spark (WI-2 `f-expr` decimal twin WRONG-READ). The native door puts NULLs last on
every type, which is DataFusion's default (WI-2/3/5, OB-1/2 native decimal twins).

**Default:**
- Card the `F.expr` case as `FEXPR-WINDOW-NULL-ORDER-1` (S2, Spark door).
- Leave native NULL ordering as the native dialect (ORACLE-MISSING until E9).
- Judge part 1's float cells on those routes with the NULL row excluded.

**Reason:** neither is signed zero.

## Q7. The MERGE cardinality change (Table B m1) is in the clause — NOT RULED

**Measured.** With both zeros in the MERGE source, Spark raises `MERGE_CARDINALITY_VIOLATION` on three kinds, and
RePark updates (WR-merge-card: ACCEPTED-WHERE-SPARK-REFUSED). After seat 2, RePark raises too. A statement that
succeeds today will refuse.

**Default:** in the clause, with a release-note line.

**Reason:** it follows the truth, and sketch §6 already names it as a user-visible change. Ruling G's defaults do not
name it.

## Q8. Cards found by step 0 (outside part 1) — NOT RULED

**Default:** the records clerk files them; none is built in part 1. Each was confirmed on its decimal twin or by a
side check, so none is signed zero.

| Card | Severity | Evidence |
|---|---|---|
| `SQL-SETOP-ALL-MULTISET-1` | S2 | Q2 |
| `PIVOT-EMPTY-CELL-COUNT-1` | S2 | GB-11 typed: `pivot(...).count()` gives 0 where Spark gives NULL for an absent pair |
| `FEXPR-WINDOW-NULL-ORDER-1` | S2 | Q6 |
| `SPARK-DOOR-IS-DISTINCT-COLUMNS-1` | S3 over-refusal | CM-11 spark-sql: `a IS DISTINCT FROM b` over two columns refuses with `UNRESOLVED_COLUMN`, on main and before part A (`idf_check.py`) |
| `SPARK-DISTRIBUTE-BY-1` | S3 over-refusal | GB-10 spark-sql: `DISTRIBUTE BY` refuses "Physical plan does not support DistributeBy partitioning" |
| `ICEBERG-PARTITIONS-TABLE-FLOAT-1` | S3 | PT-3: `.partitions` merges `-0.0` with `0.0` and NaN with NULL. `.files` shows the NaN partition as NULL; the manifest value was not measured |
| `ANY-SUBQUERY-ACCEPTED-1` | S4 | SQ-any: `= ANY (subquery)` answers where Spark 4.1.2 refuses the syntax |
| `LN-NONPOSITIVE-NULL-1` | S3 | `ln(-1.0)`: Spark NULL, RePark NaN (dropped from the NaN seed; provenance) |

**Reason:** each is a measured divergence with its own owning layer.

## Q9. NaN bit patterns written to Iceberg — NOT RULED

**Measured.** Spark's Iceberg write stores sign-set NaNs as canonical `7ff8…`, while RePark keeps `fff8…` (table §6).
After part 1 the keys agree either way, because the UDF canonicalises them. Raw `-NaN` ordering outside key positions
stays different (`NAN-PAYLOAD-1`, already carded by the sketch).

**Default:** no change in part 1. Add the bit evidence to `NAN-PAYLOAD-1`.

**Reason:** stored values are never normalised (sketch §6).
