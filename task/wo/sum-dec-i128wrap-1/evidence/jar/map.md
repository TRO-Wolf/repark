# task/wo/sum-dec-i128wrap-1/evidence/jar/ — the Spark 4.1.2 bytecode read for step 0

- `javap-excerpts.txt` — `javap -c -p` excerpts from `spark-catalyst_2.13-4.1.2.jar`: `Sum.shouldTrackIsEmpty`,
  `resultType`, `add`, `initialValues`, `updateExpressions`, `mergeExpressions`, `evaluateExpression` and the
  one-argument constructor; `TrySumExpressionBuilder$.build`; `CheckOverflowInSum.eval`;
  `DecimalAddNoOverflowCheck.nullSafeEval`; `UnsafeRow.setDecimal`. Read in [../../sketch.md](../../sketch.md) §2.
- `javap-revision.txt` — the revision's excerpts: `codegen.UnsafeRowWriter.write(int, Decimal, int, int)`, the
  decimal write template `GenerateUnsafeProjection$` emits, and from the Iceberg 1.11.0 runtime jar
  `SparkParquetWriters$FixedDecimalWriter.write` and `DecimalUtil.toReusedFixLengthBytes` (the per-value precision
  check). Read in the sketch's §2 and the inventory's row I-5.

Up: [../map.md](../map.md).
