# Card DOUBLE-DECIMAL-STORE-ROUND-1: storing the DOUBLE `1.255` into DECIMAL(10,2) writes `1.25`; Spark writes `1.26`

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** the DECIMAL-DOUBLE-UNION-COERCION-1
design sketch (#1324, Q5′; [sketch](../../wo/decimal-double-union-coercion-1/sketch.md),
[domain table](../../wo/decimal-double-union-coercion-1/domain-table.md)); owner, 2026-11-13, about 14:52 UTC, verbatim
"Defaults, and merge #1323 when green".

**Status:** filed 2026-11-13; **FIXED 2026-11-13** on branch `fix/double-decimal-store-round-1` (see the append below; not merged at writing). Severity **S1**, a wrong write. **Label-blocking** as the decimal unit's
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
- **Cost:** see the evidence's `build/seats.md` (the 1.05x bar).
