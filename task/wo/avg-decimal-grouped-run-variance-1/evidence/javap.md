# `javap -c -p` excerpts, Spark 4.1.2 (`spark-catalyst_2.13-4.1.2.jar`)

Classes extracted into `N/avg-variance-out/jar/` and read with zulu 17's `javap -c -p`. Only the lines the rule rests
on; constant-pool numbers as printed.

## `aggregate.Average`

`sumDataType$lzycompute` (decimal arm): `DecimalType$Fixed$.unapply`, `bipush 10`, `DecimalType$.bounded(II)` —
the buffer is `decimal(min(p + 10, 38), s)`.

`add(Expression, Expression)`: for a decimal left side, `new DecimalAddNoOverflowCheck(left, right, left.dataType)`;
otherwise `Add$.apply(left, right, evalMode)`:

```text
 8: instanceof    #47   // class org/apache/spark/sql/types/DecimalType
14: new           #431  // class org/apache/spark/sql/catalyst/expressions/DecimalAddNoOverflowCheck
24: invokespecial #434  // DecimalAddNoOverflowCheck."<init>":(Expression;Expression;DataType;)V
40: invokevirtual #444  // Add$.apply:(Expression;Expression;Enumeration$Value;)Add
```

`updateExpressions$lzycompute`: `sum = add(sum, coalesce(child.cast(sumDataType), Literal.default(sumDataType)))`;
`count = If(child.isNull, count, count + 1L)` (`#449 sum`, `#533 cast`, `#473 Literal$.default`, `#583 coalesce`,
`#496 add`, `#513 If`, `#587 isNull`, `#591 longToLiteral`).

`mergeExpressions$lzycompute`: `sum = add(sum.left, sum.right)`; `count = count.left + count.right`
(`#492 RichAttribute.left`, `#494 .right`, `#496 add`, `#506 $plus`).

`evaluateExpression$lzycompute` (decimal arm): `If(EqualTo(count, 0), Literal(null),
DecimalDivideWithOverflowCheck(sum, count.cast(DecimalType.LongDecimal), resultType, context, evalMode != ANSI))`
(`#513 If`, `#515 EqualTo`, `#525 DecimalDivideWithOverflowCheck`, `#529 LongDecimal`, `#543 EvalMode$.ANSI`).

`initialValues$lzycompute`: `Literal.default(sumDataType)` (zero) and `Literal(0L)`.

## `UnsafeRow.setDecimal(int, Decimal, int)` (the hash aggregate's buffer write)

```text
  6: invokestatic  #27   // Decimal.MAX_LONG_DIGITS:()I
  9: if_icmpgt     36
 ...
109: ifnull        124
115: invokevirtual #184  // Decimal.scale:()I
118: invokevirtual #187  // Decimal.changePrecision:(II)Z
121: ifne          149
126: invokevirtual #167  // setNullAt:(I)V
```

Above 18 digits, a value that `changePrecision(precision, scale)` cannot hold is written as NULL.

## `codegen.UnsafeRowWriter.write(int, Decimal, int, int)` (a partial's output row)

```text
 1: invokestatic  #133  // Decimal.MAX_LONG_DIGITS:()I
15: invokevirtual #138  // Decimal.changePrecision:(II)Z
18: ifeq          33
35: invokevirtual #96   // setNullAt:(I)V
```

The same check when a partial's buffer is written to its output row.

## `DecimalDivideWithOverflowCheck.eval`

```text
 5: invokeinterface Expression.eval   // left (the sum)
10: ifnonnull     35
14: invokevirtual #130  // nullOnOverflow:()Z
17: ifeq          22
29: ldc           #183  // String try_avg
31: invokevirtual #187  // QueryExecutionErrors$.overflowInSumOfDecimalError:(QueryContext;String;)
69: invokevirtual #218  // Decimal$DecimalIsFractional$.div
```

A NULL sum with rows counted raises `ARITHMETIC_OVERFLOW` (the `try_avg` hint) under ANSI and answers NULL otherwise;
a non-NULL sum is divided and `toPrecision(p, s, ROUND_HALF_UP, nullOnOverflow)` checks the quotient
(`NUMERIC_VALUE_OUT_OF_RANGE`).

Up: [map.md](map.md).
