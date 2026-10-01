# CAST-OVERFLOW-INSERT-1 — a DOUBLE that overflows an integer column refuses with Spark's `CAST_OVERFLOW_IN_TABLE_INSERT` text

**Filed:** 2026-09-28 by the orchestrating session on the owner's ruling of the same day ("card it for after 1.5.1, say target 1.5.2"). **Target: v1.5.2.**

## The difference (measured, INTDIV-1 residue R-INTDIV-9)

Storing a DOUBLE that does not fit the target integer column (1e19 into BIGINT, NaN, ±Infinity; also `0/0` into BIGINT) is refused by both engines, and neither stores anything. The error text differs:
- **Spark 4.1.2:** ``[CAST_OVERFLOW_IN_TABLE_INSERT] Fail to assign a value of "DOUBLE" type to the "BIGINT" type column or variable `v` due to an overflow. Use `try_cast` on the input value to tolerate overflow and return NULL instead. SQLSTATE: 22003``. It says `"INT"` for INT columns. Spark also answers this class for `0/0` into BIGINT.
- **RePark:** `Optimizer rule 'simplify_expressions' failed` for a constant, an Arrow `Can't cast value 1e19 to type Int64` for a column, and `DIVIDE_BY_ZERO` for `0/0`.

Measured cells: the `r_intdiv_1` section of the INTDIV-1 hand-back and the refusal-only `st-*` cells in `python/repark/tests/intdiv_1_spark_oracle.json`.

## Scope

Every DOUBLE/FLOAT (and DECIMAL, if Spark measures the same class) source stored into an integer column, on every write door: INSERT VALUES, INSERT … SELECT, INSERT OVERWRITE, UPDATE SET, both MERGE arms and the DataFrame append. Not only division results.

## Done condition

Each door refuses with Spark's class, SQLSTATE 22003 and message body, naming the target type and column. A probe records Spark's answer for each door and source type first. Integer→integer and string store refusals are unchanged.
