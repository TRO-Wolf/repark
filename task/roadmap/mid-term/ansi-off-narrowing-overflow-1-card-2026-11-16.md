# Card ANSI-OFF-NARROWING-OVERFLOW-1: with ANSI off, a narrowing numeric cast that overflows refuses where Spark wraps or answers NULL

**Date:** 2026-11-16. **Filed by:** the CAST-LITERAL-UNWRAP-1 builder (`claude-opus-5-5`), under the owner's ruling 2
of 2026-11-16 ("adopt Fable's, run the string-cast sizing": accept the refusal, with a disclosure).

**Status:** FIXED (2026-11-23, `fix/ansi-off-narrowing-overflow-1`). User-written narrowing numeric casts on the Spark
door now wrap integers (or saturate-then-wrap FLOAT/DOUBLE, with NaN -> 0) and answer NULL for overflowing DECIMAL
targets when `spark.sql.ansi.enabled = false`, and raise Spark 4.1.2's `[CAST_OVERFLOW]` /
`[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]` (SQLSTATE `22003`) when ANSI is on. `stats_barrier.rs` (`keeps_order`)
bars every narrowing numeric cast that can wrap or overflow to NULL so Parquet footer min/max and pruning never answer
silently wrong. Pinned by `python/repark/tests/types/test_ansi_off_narrowing_overflow_1.py` (oracle
`ansi_off_narrowing_overflow_1_spark_oracle.json`), `crates/repark-functions/src/cast_map/narrow_tests.rs`, and
`crates/repark-core/src/session/df_guards/cast_shield/stats_barrier_tests.rs`. Uncast store assignment, reader schema
casts, `TRY_CAST`, the native `repark.sql` door, and explicit `CAST` inside SQL `VALUES` / `INSERT ... VALUES`
([VALUES-EXPLICIT-CAST-1](cards-2026-11-23/values-explicit-cast-1-card-2026-11-23.md), inherited from PR #1413's
`LogicalPlan::Values` guard) keep main's policy.

**Severity: S2.** A refusal, never a wrong answer, on an ordinary cast with ANSI off. It blocks no label.

## Finding

With `spark.sql.ansi.enabled = false`, Spark's narrowing numeric casts never fail: an integer target wraps
(`CAST(4294967297 AS INT)` is 1, from `BIGINT` or `DECIMAL(38,0)`; `CAST(128.00 AS TINYINT)` is -128) and a
DECIMAL target too small for the value is NULL. RePark's cast kernel (Arrow) refuses each of them: `Cast error: Can't
cast value … to type Int32`, `value of … is out of range Int8`, `… is too large to store in a Decimal128 of
precision 3`. Until CAST-LITERAL-UNWRAP-1 the unwrap hid this whenever the cast was compared with a constant, because
the cast never ran; the shield keeps the cast, and its `preimage` declines a cast that can overflow, so the cast runs
per row and refuses on the first overflowing value. The kernel check of that unit's step 0 found the same refusals
with nothing to unwrap (`task/wo/cast-literal-unwrap-1/domain-table.md`, kernel check, ANSI off).

## Measured (CAST-LITERAL-UNWRAP-1's bar, head `88999454`, ANSI off)

- **EQUAL→REFUSE (Q2), 122 cells**: right on main by luck, refused now. Example: `CAST(b AS INT) = 5` over a
  `BIGINT` column holding 2147483648 answers no rows on main and on Spark, and refuses on the head.
- **WRONG→REFUSE (Q2), 648 cells**: wrong on main, refused now (for example `CAST(b AS INT) = 1`, where Spark also
  keeps 4294967297).
- Kinds: `BIGINT`→`INT`, `INT`→`SMALLINT`, `DECIMAL(10,2)`→`TINYINT` / `SMALLINT`, `DECIMAL(38,0)`→`INT`,
  `DECIMAL(10,2)`→`DECIMAL(3,2)`, `DECIMAL(10,3)`→`DECIMAL(3,2)`; every door, storage and DML verb. With ANSI on,
  the same cells raise as Spark does.

The EQUAL→REFUSE (Q2) cells by name (`kind`: storage/position/constant/operator/side, as in step 0's table):

- `dec_tinyint_overflow` (6): `view/where/c2/eq/r`, `view/where/c2/eq/l`, `view/where/c2/ne/r`, `view/where/c2/ne/l`, `view/where/c2/ns/r`, `view/where/c2/ns/l`.
- `dec38_int_overflow` (15): `view/where/c1/eq/r`, `view/where/c1/eq/l`, `view/where/c1/ne/r`, `view/where/c1/ne/l`, `view/where/c1/ns/r`, `view/where/c1/ns/l`, `parquet/where/c1/eq/r`, `parquet/where/c1/eq/l`, `parquet/where/c1/ns/r`, `ice_files/where/c1/eq/r`, `ice_files/where/c1/eq/l`, `ice_files/where/c1/ns/r`, `ice_part/where/c1/eq/r`, `ice_part/where/c1/eq/l`, `ice_part/where/c1/ns/r`.
- `dec_prec_overflow` (60): `view/where/c0/eq/r`, `view/where/c0/eq/l`, `view/where/c0/ns/r`, `view/where/c0/ns/l`, `view/where/c1/eq/r`, `view/where/c1/eq/l`, `view/where/c1/ns/r`, `view/where/c1/ns/l`, `view/where/c2/eq/r`, `view/where/c2/eq/l`, `view/where/c2/ns/r`, `view/where/c2/ns/l`, `view/where/c3/eq/r`, `view/where/c3/eq/l`, `view/where/c3/ns/r`, `view/where/c3/ns/l`, `view/where/in`, `view/where/between`, `view/case/eq`, `view/having/eq`, `view/join/eq`, `parquet/where/c0/eq/r`, `parquet/where/c0/eq/l`, `parquet/where/c0/ns/r`, `parquet/where/c1/eq/r`, `parquet/where/c1/eq/l`, `parquet/where/c1/ns/r`, `parquet/where/in`, `parquet/where/between`, `parquet/join/eq`, `ice_files/where/c0/eq/r`, `ice_files/where/c0/eq/l`, `ice_files/where/c0/ns/r`, `ice_files/where/c1/eq/r`, `ice_files/where/c1/eq/l`, `ice_files/where/c1/ns/r`, `ice_files/where/in`, `ice_files/where/between`, `ice_files/join/eq`, `ice_part/where/c0/eq/r`, `ice_part/where/c0/eq/l`, `ice_part/where/c0/ns/r`, `ice_part/where/c1/eq/r`, `ice_part/where/c1/eq/l`, `ice_part/where/c1/ns/r`, `ice_part/where/in`, `ice_part/where/between`, `ice_part/join/eq`, `view/df/col_eq`, `view/df/lit_eq`, `view/df/isin`, `view/df/between`, `view/df/fexpr_eq`, `view/df/filter_str_eq`, `ice_part/df/col_eq`, `ice_part/df/lit_eq`, `ice_part/df/isin`, `ice_part/df/between`, `ice_part/df/fexpr_eq`, `ice_part/df/filter_str_eq`.
- `dec_round_overflow` (17): `view/where/c1/eq/r`, `view/where/c1/eq/l`, `view/where/c1/ns/r`, `view/where/c1/ns/l`, `view/where/c2/eq/r`, `view/where/c2/eq/l`, `view/where/c2/ns/r`, `view/where/c2/ns/l`, `parquet/where/c1/eq/r`, `parquet/where/c1/eq/l`, `parquet/where/c1/ns/r`, `ice_files/where/c1/eq/r`, `ice_files/where/c1/eq/l`, `ice_files/where/c1/ns/r`, `ice_part/where/c1/eq/r`, `ice_part/where/c1/eq/l`, `ice_part/where/c1/ns/r`.
- `bigint_int` (15): `view/where/c1/eq/r`, `view/where/c1/eq/l`, `view/where/c1/ne/r`, `view/where/c1/ne/l`, `view/where/c1/ns/r`, `view/where/c1/ns/l`, `parquet/where/c1/eq/r`, `parquet/where/c1/eq/l`, `parquet/where/c1/ns/r`, `ice_files/where/c1/eq/r`, `ice_files/where/c1/eq/l`, `ice_files/where/c1/ns/r`, `ice_part/where/c1/eq/r`, `ice_part/where/c1/eq/l`, `ice_part/where/c1/ns/r`.
- `int_smallint` (6): `view/where/c2/eq/r`, `view/where/c2/eq/l`, `view/where/c2/ne/r`, `view/where/c2/ne/l`, `view/where/c2/ns/r`, `view/where/c2/ns/l`.
- `critic-reads` (3): `b02`, `i01`, `i03`.

Every non-EQUAL cell: `task/wo/cast-literal-unwrap-1/evidence/built/raw/nonequal.tsv`; counts in
[../../wo/cast-literal-unwrap-1/evidence/built/bar.md](../../wo/cast-literal-unwrap-1/evidence/built/bar.md).

## Not yet known

The seat: a Spark-semantics cast kernel for the ANSI-off narrowing family (wrap for an integer target, NULL for a
DECIMAL one), beside [CAST-FLOAT-DECIMAL-OVERFLOW-NULL-1](cast-float-decimal-overflow-null-1-card-2026-11-15.md),
which is the FLOAT and DOUBLE source twin.

## Append (2026-11-16): FLOAT and DOUBLE sources, from Fable's sweep

This card's kinds are integer and DECIMAL sources. Fable's type-pair sweep of 2026-11-16 (family 12) measured the same ANSI-off refusal from FLOAT and DOUBLE to integer targets, on the overflowing rows:

- `CAST` of FLOAT `1e10` to TINYINT: Spark answers `-1`; main refuses, `Can't cast value 10000000000.0 to type Int8` (cell `f:cast:FLOAT:TINYINT`, class R).
- `CAST` of DOUBLE `9007199254740992.0` to INT: Spark answers `2147483647`; main refuses, `Can't cast value 9007199254740992.0 to type Int32` (cell `f:cast:DOUBLE:INT`, class R).

Spark's values follow Java's narrowing: the value saturates to an int, then narrows for TINYINT. The card does not rule the mechanism. With ANSI on both engines raise (class B for these cells). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. Reproduction: `python3 <scratchpad>/sweep/v.py f:cast:FLOAT:TINYINT f:cast:DOUBLE:INT`, with `<scratchpad>` the Fable sweep scratch folder (not committed).

Up: [map.md](map.md).

## Append (2026-11-17): the ANSI-off literal half

The ANSI-off half of [DECIMAL-LITERAL-OVERFLOW-1](decimal-common-type-1-side/decimal-literal-overflow-1-card-2026-11-17.md) sits beside this card. That card is a string-to-decimal literal refused at planning (S2, not label-blocking, ruled 2026-11-17). This card's own severity is unchanged.
