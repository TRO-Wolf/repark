# Card DOUBLE-DECIMAL-STORE-ROUND-1: storing the DOUBLE `1.255` into DECIMAL(10,2) writes `1.25`; Spark writes `1.26`

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** the DECIMAL-DOUBLE-UNION-COERCION-1
design sketch (#1324, Q5′; [sketch](../../wo/decimal-double-union-coercion-1/sketch.md),
[domain table](../../wo/decimal-double-union-coercion-1/domain-table.md)); owner, 2026-11-13, about 14:52 UTC, verbatim
"Defaults, and merge #1323 when green".

**Status:** filed 2026-11-13; **FIXED 2026-11-13** on branch `fix/double-decimal-store-round-1` (see the append below; not merged at writing); its cast cost exception is **owner word owed (asked 2026-11-14)**, see [Cost](#cost). Severity **S1**, a wrong write. **Label-blocking** as the decimal unit's
prerequisite: the owner's ruling of Q5′ puts it before the DECIMAL-DOUBLE-UNION-COERCION-1 build.

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

**Owner word owed (asked 2026-11-14, about 01:06 UTC; not ruled).** The cast rows sit above 1.05x because Spark's
value needs JDK 17's exact digits per row. The options put to the owner:

- **A:** accept a cost exception for plans that cast a FLOAT/DOUBLE to a DECIMAL, provided the standard sets and the
  plans without that cast hold 1.05x (the second table), with a performance card:
  [DOUBLE-DECIMAL-CAST-PERF-1](double-decimal-cast-perf-1-card-2026-11-14.md) (PROPOSED);
- **B:** shortest digits (a Ryu-class algorithm), much cheaper, which reverses Q6 (`1e23` into DECIMAL(38,0) would
  store `10^23`, where Spark 4.1.2 on JDK 17 stores `99999999999999990000000`);
- **C:** a faster exact JDK 17 path first (that card's work, before this unit merges).

This branch is made merge-ready under A; it merges only on the owner's word.
