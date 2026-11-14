# Card DOUBLE-DECIMAL-STORE-ROUND-1: storing the DOUBLE `1.255` into DECIMAL(10,2) writes `1.25`; Spark writes `1.26`

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** the DECIMAL-DOUBLE-UNION-COERCION-1
design sketch (#1324, Q5′; [sketch](../../wo/decimal-double-union-coercion-1/sketch.md),
[domain table](../../wo/decimal-double-union-coercion-1/domain-table.md)); owner, 2026-11-13, about 14:52 UTC, verbatim
"Defaults, and merge #1323 when green".

**Status:** filed 2026-11-13; **FIXED 2026-11-13** on branch `fix/double-decimal-store-round-1`; **NARROWED TO STORES
2026-11-14** (step 1 of the split plan, see [Narrowed to stores](#narrowed-to-stores-2026-11-14); not merged at writing).
Severity **S1**, a wrong write. **Label-blocking** as the decimal unit's prerequisite: the owner's ruling of Q5′ puts it
before the DECIMAL-DOUBLE-UNION-COERCION-1 build.

## Measured (main `04d84ab9`, Spark 4.1.2, both ANSI modes)

| cell | Spark | RePark | verdict |
|---|---|---|---|
| `ins_values_dec_round` | `decimal(10,2)`: 1=0.10, 2=1.26 | 1=0.10, 2=**1.25** | WRONG-WRITE |
| `ins_union_dec_round` | 1=0.10, 2=1.26 | 1=0.10, 2=1.26 | EQUAL today |

`ins_union_dec_round` is EQUAL only because today's `UNION` types the column `decimal(30,15)` first; once the decimal
unit widens it to `double`, it takes the same store rounding and turns WRONG. That is why this card lands first.

## Relation

[CAST-DOUBLE-TO-DECIMAL-ROUNDING-1](cast-double-to-decimal-rounding-1-card-2026-11-02.md) (P3): `CAST` from DOUBLE to
DECIMAL rounds the binary value where Spark rounds the double's shortest text (`1.005` to `1.00` against `1.01`).
Likely the same mechanism on the store door; not measured as one.

## Close

A DOUBLE stored into a DECIMAL column rounds as Spark does (`1.255` to `1.26`), pinned on `INSERT … VALUES` and the
`UNION` source in both ANSI modes.

**Narrowed 2026-11-14:** the card closes on the implicit store doors listed below, both doors, both ANSI modes. `ins_values_dec_round` (a VALUES cell the planner types to the target) stays wrong until step 5 of the split plan (seat V), and the written casts until step 4; both are strict xfails in the pin file.

## Narrowed to stores (2026-11-14)

**Ruling.** The owner, 2026-11-14, about 13:38 UTC, verbatim: "Merge and go with the split plan", adopting Fable's
guidance (`Fable reply — DECIMAL-DOUBLE stack 2026-11-14.md`). Also ruled: the cost exception covers stores only; Q2
stands; Q8 is re-ruled (Spark's MERGE `-0.0` miss is declared in the PRUNED-WRONG class; the coercion unit's record).

**The split plan:**

1. this card, narrowed to stores: JDK 17's kernel (K1) runs only where a FLOAT/DOUBLE is stored into a DECIMAL column;
   the float branch leaves `bool_decimal_cast`, so no comparison or written cast moves;
2. [ROUND-DOUBLE-EXACT-1](round-double-exact-1-card-2026-11-14.md), after this merges;
3. the signed-zero constant comparison, its own unit;
4. coercion part 1: seat E, the exact DECIMAL→DOUBLE cast (K2), and K1 on written casts;
5. coercion part 2: seat V and a tripwire.

**The seats** (five product files: `java_double/mod.rs` keeps K1; `conform.rs`, `conform_stage.rs`,
`store_overflow.rs`, `repark-core` `df_guards.rs`; `bool_decimal.rs` is main's code with one more pin):

- the batch store (`conform.rs`) and a store UDF over K1, `__repark_store_float_decimal__`;
- the conform stage's `leaf_store` calls it for a FLOAT/DOUBLE leaf with a DECIMAL target;
- the store expression seat (`store_overflow.rs` `conform_one_store_expr`, used by the Spark door's DML store rule and
  the stage's output conform) stores a FLOAT/DOUBLE, its cast to exactly the target DECIMAL, or a `CASE` whose
  branches are such casts (MERGE `UPDATE SET` and `NOT MATCHED BY SOURCE … UPDATE` wrap the value in one);
- `FloatDecimalStoreRule` (installed on every session in `df_guards.rs`) does the same for a DML input's top-level
  store columns, the only store seat the native door's `INSERT` has.

Only a store's value is rewritten, never a predicate or a `CASE` condition. The stage and the batch store alone left
`INSERT … SELECT`, the `CASE` UPDATE, MERGE UPDATE and every DataFrame writer at main's value (the pin file: 34 failed,
22 passed); the store expression seat brought the doors to 177/245, and the `CASE` branches and the rule to 201/245.

**Doors** (verifier 2's 245 door-group cells per mode, Spark 4.1.2 on JDK 17; main `d843d37e` 49/245, the narrowed head
201/245, both ANSI modes; [stores.md](../../wo/double-decimal-store-round-1/evidence/fix/stores.md)):

- **Fixed, equal to Spark:** `INSERT … SELECT` (positional, by name, NOT NULL), `INSERT … UNION`, `INSERT OVERWRITE`
  (static, dynamic, a static partition), a table partitioned by the DECIMAL column, `UPDATE SET` (copy-on-write,
  merge-on-read, NOT NULL, a `CASE` over every row, a column), MERGE `UPDATE SET` and `INSERT` on both table kinds
  (`*`, an expression, mixed, NOT NULL, `NOT MATCHED BY SOURCE`), `writeTo().append()`, `overwrite(cond)`,
  `overwritePartitions()`, `saveAsTable`, `insertInto` (also with overwrite); on the native door `INSERT … SELECT`,
  `INSERT … UNION` and `UPDATE SET`. The native door's SQL `MERGE` refuses on main and the head alike ("Unsupported SQL
  statement"), so it has no MERGE store.
- **Still wrong until step 4 (written casts):** `CAST`, `TRY_CAST`, `arrow_cast` and `F.col().cast` to DECIMAL in a
  SELECT or a `WHERE`, CTAS and RTAS with a cast, `CAST(TIMESTAMP AS DECIMAL(38,18))` (it answers main's
  `1704067200.123454970761576448` again; verifier 2's F2), and CAST-DOUBLE-TO-DECIMAL-ROUNDING-1's cells.
- **Still wrong until step 5 (seat V):** `INSERT … VALUES` of a DOUBLE cell the planner types to the target
  (`ins_values_dec_round`), also NOT NULL, and the card row `VALUES (1, 0.1), (2, CAST(1.255 AS DOUBLE))`.

**No comparison moves.** Against main, cell for cell, both ANSI modes: verifier 2's coercion fuzz (165,846 cells per
mode) is byte-identical; the structured `d = v` set (60,000 cells) keeps the same 2,501 matches; the DML cells
(`flip.py`, Spark door, and its native-door copy [flip_native.py](../../wo/double-decimal-store-round-1/evidence/fix/flip_native.py))
are identical, apart from the native copy's store line. The accepted strict xfail
(`CAST(ROUND(v, 3) AS DECIMAL(10,3)) = v`) now answers `false` as main and Spark do; the xfail is removed because the
comparison is main's again, and the pin stays as a plain test.

**Pins.** Rust: `bool_decimal::tests::a_float_cast_to_decimal_is_left_to_its_cast` (red on `ade7c628`'s rule: the
plan showed `__repark_float_to_decimal__`), `conform_stage::tests::every_stage_site_stores_a_float_leaf_by_the_jdk_17_kernel`
(red: the plan showed `CAST(d AS Decimal128(10, 2))`), the kernel tests moved to
`repark-functions/src/java_double/tests_unscaled.rs`. Facade
`python/repark/tests/sql/test_double_decimal_store_round_1.py`: on main 36 failed, 10 passed, 16 xfailed; on the head
46 passed, 16 xfailed (the written-cast and VALUES doors, strict xfails that name steps 4 and 5).

## FIXED (appended 2026-11-13 by the builder, `claude-opus-5-5`)

Rulings: the owner's 20:50 UTC "defaults" (option A: up to five product files for the label-scope SQL and DML doors;
NaN, ±Infinity and ANSI-off overflow keep today's refusals; JDK 17's text; the F.expr literal fold and `fillna` go to
[DOUBLE-DECIMAL-DATAFRAME-1](double-decimal-dataframe-1-card-2026-11-13.md), nested leaves to
[DOUBLE-DECIMAL-NESTED-LEAF-1](double-decimal-nested-leaf-1-card-2026-11-13.md)); the 23:59 UTC "Go with defaults"
(the coercion-fuzz halt, option A). Evidence: [task/wo/double-decimal-store-round-1/](../../wo/double-decimal-store-round-1/map.md).

- **Spark's rule** (v4.1.2 source): a FLOAT/DOUBLE becomes a DECIMAL through JDK 17's `Double.toString` of the double
  (a FLOAT widened first), half up at the target scale. RePark rounded Arrow's f64 product `10^s·v` instead.
- **The seats (four product files):** the kernel `java_double_unscaled` (`repark-common` `java_double/mod.rs`, the
  existing JDK 17 port); the end-of-list `bool_decimal_cast` rule also rewrites FLOAT/DOUBLE `CAST`, `TRY_CAST` and
  `arrow_cast` to `Decimal128`; the conform stage's `leaf_store` casts a float leaf to its DECIMAL target in the plan
  at every write site (MERGE INSERT, the merge-on-read UPDATE, the copy-on-write UPDATE before its union); the batch
  store cast in `conform.rs`. The fifth allowed file (`uuid_presentation.rs:134`) was not needed.
- **Pins:** Rust at each seat; `python/repark/tests/sql/test_double_decimal_store_round_1.py` over 21 doors × 12
  cells × both ANSI modes, the card rows above and the kept refusals. Red on main `69aba5df` (46 failed, 8 passed),
  green on the head (all passed). Spark 4.1.2 answers every pinned cell. The step 0 table on the head: 321 wrong
  cells per mode turn equal, no other cell moves.
- **Accepted by the owner's 23:59 ruling:** `CAST(ROUND(v, 3) AS DECIMAL(10,3)) = v` with `v = 2.6750000000000003`
  answered Spark's `false` on main by accident and answers `true` on the head; RePark compares a DECIMAL with a DOUBLE
  as `decimal(30,15)` where Spark compares DOUBLEs. A strict xfail pins it; DECIMAL-DOUBLE-UNION-COERCION-1 retires it.
  The same fuzz moves 171 cells per mode the other way.
- **Pre-existing, unchanged:** `CAST(v AS STRING)` over a `UNION` of a DOUBLE and a DECIMAL refuses on main and on the
  head alike (`'__repark_float_to_string__' expects a FLOAT or DOUBLE argument, got Decimal128(30, 15)`).
- **Cost:** see [Cost](#cost) below.

## Cost

### Stores only (2026-11-14, the narrowed head)

The owner's 2026-11-14 ruling puts the cost exception on stores only. Main `d843d37e` against the stores head
`d2b517c6`, 10M rows, five alternating pairs under `flock -x` measure then both cargo locks, cores 32-47 (16 cores);
[stores.md](../../wo/double-decimal-store-round-1/evidence/fix/stores.md#cost):

| case | query | base median (5) | head median (5) | head/base | pairwise head/base |
|---|---|---|---|---|---|
| cast | `select_cast_10_2` | 0.0629 | 0.0549 | **0.873** | 0.816, 0.956, 0.821, 1.189, 1.005 |
| cast | `select_cast_38_18` | 0.0604 | 0.0618 | **1.023** | 0.966, 0.886, 0.871, 1.258, 1.324 |
| cast | `filter_decimal_eq_double` | 0.0902 | 0.0893 | **0.991** | 0.94, 1.111, 0.857, 1.051, 0.946 |
| cast | `store_10_2` | 0.1784 | 0.1617 | **0.906** | 1.098, 0.781, 0.738, 1.397, 1.108 |
| cast | `store_38_18` | 0.2995 | 0.3944 | **1.317** | 1.911, 0.598, 1.299, 2.444, 1.328 |
| mixed | `project_decimal` | 0.0770 | 0.0728 | **0.945** | 1.047, 0.989, 1.039, 0.912, 0.928 |
| mixed | `project_double` | 0.0464 | 0.0456 | **0.983** | 0.995, 1.002, 1.09, 0.98, 0.981 |
| mixed | `filter_decimal` | 0.0433 | 0.0441 | **1.019** | 0.968, 1.025, 1.035, 0.95, 0.967 |
| mixed | `filter_double` | 0.0325 | 0.0324 | **0.998** | 0.977, 1.032, 0.976, 1.011, 1.007 |
| mixed | `aggregate_both` | 0.1177 | 0.1171 | **0.994** | 0.982, 0.947, 0.977, 0.994, 1.043 |
| mixed | `store_decimal_20_6` | 0.2466 | 0.2411 | **0.977** | 0.976, 0.761, 0.975, 0.978, 0.663 |
| double | `project_double` | 0.0516 | 0.0506 | **0.981** | 0.989, 0.984, 1.034, 0.93, 0.999 |
| double | `store_double` | 0.1816 | 0.1782 | **0.982** | 1.028, 0.734, 0.965, 1.288, 1.015 |

The store into DECIMAL(38,18) is the exception (1.32x, about +0.095 s per 10M rows on 16 cores). Both `CAST` rows and
the `decimal = double` filter equal main again; every no-cast row holds 1.05x.

### The earlier runs (all casts, before the narrowing)

10M rows, median seconds; each sample a fresh process under `flock -x` measure, then opus-cargo and opus2-cargo.
`base-aa` is main measured again, the noise control. Evidence:
[seats.md](../../wo/double-decimal-store-round-1/evidence/fix/seats.md#the-cost-row) (the previous builder's run) and
[cost-finish.md](../../wo/double-decimal-store-round-1/evidence/fix/cost-finish.md) (the finishing run, 2026-11-14).

**Plans that cast a FLOAT/DOUBLE to a DECIMAL (the exception).** Before: base `69aba5df`, head `3e093a45`, five
alternating pairs. Now: base `6f9f2aaa`, the merged head `3eed6799`, five rotated runs.

| query | before base / head | ratio | now base / head | ratio | now base-aa/base |
|---|---|---|---|---|---|
| `SELECT max(CAST(v AS DECIMAL(10,2)))` | 0.0585 / 0.0679 | 1.16 | 0.0615 / 0.0692 | **1.13** | 0.92 |
| `SELECT max(CAST(v AS DECIMAL(38,18)))` | 0.0616 / 0.1576 | 2.56 | 0.0625 / 0.1556 | **2.49** | 0.99 |
| `count(*) WHERE d = v` (DECIMAL(16,4) = DOUBLE) | 0.0913 / 0.1831 | 2.01 | 0.0990 / 0.1771 | **1.79** | 0.93 |
| `INSERT … SELECT` into DECIMAL(10,2) | 0.1554 / 0.1580 | 1.02 | 0.1508 / 0.1657 | **1.10** | 1.03 |
| `INSERT … SELECT` into DECIMAL(38,18) | 0.2990 / 0.4048 | 1.35 | 0.3121 / 0.4024 | **1.29** | 0.98 |

**Plans without that cast (each must hold 1.05x).** Base `6f9f2aaa`, the merged head `3eed6799`.

| set or query | head/base, first run (5 repeats) | base-aa/base | head/base, re-run (15 repeats) | base-aa/base |
|---|---|---|---|---|
| C-012 like-for-like (43,889 cells), median of 5 | **0.9841** (spread 0.040) | 0.9808 (spread 0.104) | — | — |
| `mixed` `max(d * 2 + d)` (DECIMAL projection) | 1.0435 | 1.0315 | **0.9880** | 0.9939 |
| `mixed` `max(v * 2 + v)` (DOUBLE projection) | 1.0653 | 1.0584 | **1.0037** | 1.0049 |
| `mixed` `count(*) WHERE d > 5000000` | 1.0632 | 1.0395 | **1.0147** | 1.0027 |
| `mixed` `count(*) WHERE v > 5000` | 1.0103 | 1.0292 | **1.0018** | 0.9960 |
| `mixed` `GROUP BY id % 100`: `sum(d)`, `avg(v)`, `max(d)`, `min(v)` | 0.9965 | 1.0073 | **1.0008** | 0.9925 |
| `mixed` store DECIMAL(16,4) into DECIMAL(20,6) | 1.0045 | 1.0252 | **1.0010** | 1.0136 |
| `double` DOUBLE projection (no DECIMAL) | 0.9122 | 0.9356 | **0.9869** | 1.0220 |
| `double` DOUBLE store (no DECIMAL) | 1.0160 | 1.0407 | **0.9934** | 1.0199 |

In the first run two no-cast rows read above 1.05x, with the control on the same rows at 1.040 and 1.058. `EXPLAIN`
of every no-cast query is identical on base and head, and the fifteen-repeat re-run (chosen before it ran) holds every
row at 1.015x or below. The plan rewrite fires only on a FLOAT/DOUBLE operand cast to `Decimal128`, the store seats
only on a float source with a DECIMAL target.

**Superseded 2026-11-14 by the split plan** (the exception covers stores only, above). The question as asked (about 01:06 UTC): The cast rows sit above 1.05x because Spark's
value needs JDK 17's exact digits per row. The options put to the owner:

- **A:** accept a cost exception for plans that cast a FLOAT/DOUBLE to a DECIMAL, provided the standard sets and the
  plans without that cast hold 1.05x (the second table), with a performance card:
  [DOUBLE-DECIMAL-CAST-PERF-1](double-decimal-cast-perf-1-card-2026-11-14.md) (PROPOSED);
- **B:** shortest digits (a Ryu-class algorithm), much cheaper, which reverses Q6 (`1e23` into DECIMAL(38,0) would
  store `10^23`, where Spark 4.1.2 on JDK 17 stores `99999999999999990000000`);
- **C:** a faster exact JDK 17 path first (that card's work, before this unit merges).
