# task/wo/avg-decimal-round-1/evidence/jar/ — Spark 4.1.2 bytecode readings

- `javap-excerpts.txt` — `javap -c -p` excerpts (constant-pool indexes stripped): `Average.resultType`,
  `sumDataType`, `evaluateExpression` and `add`; `DecimalDivideWithOverflowCheck.eval`; `Decimal.$div` and the
  `MATH_CONTEXT` initialiser in `Decimal$`; `Divide.resultDecimalType` and its decimal `div` arm;
  `BinaryArithmetic.checkDecimalOverflow`; the method references of
  `DecimalPrecisionTypeCoercion$$anonfun$nondecimalAndDecimal$1`; `DataTypeUtils$.fromLiteral`. Read in
  [../../sketch.md](../../sketch.md) §2.

Up: [../map.md](../map.md).
