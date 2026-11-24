# STRING-HOUR-MINUTE-SECOND-1 — the `javap -c -p` lines Spark's rule rests on

Classes extracted from `spark-catalyst_2.13-4.1.2.jar` (and `AnyTimestampType$` from `spark-sql-api_2.13-4.1.2.jar`)
into `N/string-hms-out/jar/` and read with the zulu 17 `javap -c -p`. The full dumps stay there; these are the
lines each claim of [../sketch.md](../sketch.md) §1 rests on. `#n` constant-pool indices are trimmed.

## 1. `hour(x)` builds `Hour(x)` unless `x` is `TIME`

`HourExpressionBuilder$.build` (the same shape in `MinuteExpressionBuilder$` and `SecondExpressionBuilder$`):

```
66: invokevirtual Expression.dataType:()Lorg/apache/spark/sql/types/DataType;
73: instanceof    class org/apache/spark/sql/types/TimeType
79: new           class org/apache/spark/sql/catalyst/expressions/HoursOfTime
92: new           class org/apache/spark/sql/catalyst/expressions/Hour
101: invokevirtual Hour$.apply$default$2:()Lscala/Option;
104: invokespecial Hour."<init>":(Lorg/apache/spark/sql/catalyst/expressions/Expression;Lscala/Option;)V
```

`Minute` builds `MinutesOfTime` / `Minute`, `Second` builds `SecondsOfTime` / `Second`. A `STRING` argument is not
`TimeType`, so it reaches `Hour`, `Minute`, `Second`.

## 2. `Hour`, `Minute`, `Second` ask for `AnyTimestampType` with implicit casts

`public class Hour extends UnaryExpression implements GetTimeField` (and `Minute`, `Second`, `SecondWithFraction`).

`interface GetTimeField extends TimeZoneAwareExpression, ImplicitCastInputTypes`:

```
public default scala.collection.immutable.Seq<AbstractDataType> inputTypes();
   4: getstatic     Field org/apache/spark/sql/types/AnyTimestampType$.MODULE$
  10: invokespecial scala/collection/immutable/$colon$colon."<init>"
```

`GetDateField` (`Year`, `Quarter`, `Month`, `DayOfMonth`, `DayOfWeek`, `DayOfYear`, `WeekOfYear`, `WeekDay`, the
`YearOfWeek` builder) asks for `DateType` instead:

```
public default scala.collection.immutable.Seq<AbstractDataType> inputTypes();
   4: getstatic     Field org/apache/spark/sql/types/DateType$.MODULE$
```

## 3. `STRING` to `AnyTimestampType` is `TimestampType` in both ANSI modes, never `spark.sql.timestampType`

`TypeCoercion$.implicitCast(DataType, AbstractDataType)` (ANSI off):

```
627: invokevirtual scala/Tuple2._1:()Ljava/lang/Object;
630: instanceof    class org/apache/spark/sql/types/StringType
636: getstatic     Field org/apache/spark/sql/types/AnyTimestampType$.MODULE$
641: invokevirtual java/lang/Object.equals:(Ljava/lang/Object;)Z
647: getstatic     Field org/apache/spark/sql/types/AnyTimestampType$.MODULE$
650: invokevirtual AnyTimestampType$.defaultConcreteType:()Lorg/apache/spark/sql/types/DataType;
```

`AnsiTypeCoercion$.implicitCast(DataType, AbstractDataType)` (ANSI on): a `StringType` source and an `AtomicType`,
`NumericType`, `DecimalType` or `AnyTimestampType` target take `target.defaultConcreteType`:

```
312: instanceof    class org/apache/spark/sql/types/StringType
369: getstatic     Field org/apache/spark/sql/types/AnyTimestampType$.MODULE$
374: invokevirtual java/lang/Object.equals:(Ljava/lang/Object;)Z
400: invokevirtual AbstractDataType.defaultConcreteType:()Lorg/apache/spark/sql/types/DataType;
```

`AnyTimestampType$.defaultConcreteType` (`spark-sql-api`):

```
public org.apache.spark.sql.types.DataType defaultConcreteType();
   0: getstatic     Field org/apache/spark/sql/types/TimestampType$.MODULE$
   3: areturn
```

So the target is `TimestampType` (the instant, `TIMESTAMP_LTZ`). No `SQLConf` read sits on this path.

## 4. The implicit cast is an ordinary `Cast`, its mode from `spark.sql.ansi.enabled`

`TypeCoercion$.$anonfun$implicitCast$1` (`AnsiTypeCoercion$` has the same body):

```
27: new           class org/apache/spark/sql/catalyst/expressions/Cast
36: invokevirtual Cast$.apply$default$3:()Lscala/Option;
42: invokevirtual Cast$.apply$default$4:()Lscala/Enumeration$Value;
45: invokespecial Cast."<init>":(Expression;DataType;Lscala/Option;Lscala/Enumeration$Value;)V
```

`Cast$.apply$default$4` and `EvalMode$.fromSQLConf`:

```
public scala.Enumeration$Value apply$default$4();
   6: invokevirtual SQLConf$.get:()Lorg/apache/spark/sql/internal/SQLConf;
   9: invokevirtual EvalMode$.fromSQLConf:(Lorg/apache/spark/sql/internal/SQLConf;)Lscala/Enumeration$Value;
public scala.Enumeration$Value fromSQLConf(org.apache.spark.sql.internal.SQLConf);
   1: invokevirtual SQLConf.ansiEnabled:()Z
   4: ifeq          12
   8: invokevirtual ANSI:()Lscala/Enumeration$Value;
  13: invokevirtual LEGACY:()Lscala/Enumeration$Value;
```

`apply$default$3` is the zone `None`, which the analyzer's `ResolveTimeZone` fills with the session zone. The
implicit cast is therefore the same expression as a written `CAST(c AS TIMESTAMP)` under `TIMESTAMP_LTZ`: ANSI on
raises `CAST_INVALID_INPUT` for malformed text, ANSI off answers NULL.

## 5. The field is read in the session zone (UTC for `TIMESTAMP_NTZ` only)

`GetTimeField.nullSafeEval` and `zoneIdInEval`:

```
 1: invokeinterface func:()Lscala/Function2;
14: invokeinterface zoneIdInEval:()Ljava/time/ZoneId;
19: invokeinterface scala/Function2.apply
public default java.time.ZoneId zoneIdInEval();
13: invokevirtual Expression.dataType:()Lorg/apache/spark/sql/types/DataType;
16: invokeinterface zoneIdForType:(Lorg/apache/spark/sql/types/DataType;)Ljava/time/ZoneId;
```

`TimeZoneAwareExpression.zoneIdForType`:

```
 3: instanceof    class org/apache/spark/sql/types/TimestampNTZType
 9: getstatic     Field java/time/ZoneOffset.UTC:Ljava/time/ZoneOffset;
17: invokeinterface zoneId:()Ljava/time/ZoneId;
```

`Hour.func` calls `DateTimeUtils$.getHours:(JLjava/time/ZoneId;)I`; `Minute` `getMinutes`; `Second` `getSeconds`;
`SecondWithFraction` `getSecondsWithFraction:(JLjava/time/ZoneId;)Lorg/apache/spark/sql/types/Decimal;` with
`DecimalType.<init>(8, 6)` as its type.

## 6. `extract` and `date_part` build the same expressions

`DatePartExpressionBuilder$.build` calls `Extract$.createExpr(String, Expression, Expression)`; `createExpr` tests
`StringType` for the field (and folds it), then `AnsiIntervalType` / `CalendarIntervalType` for the source, and
otherwise calls `DatePart$.parseExtractField(String, Expression)`. Its constant pool holds the field names
`YEAR Y YEARS YR YRS`, `YEAROFWEEK`, `QUARTER QTR`, `MONTH MON MONS MONTHS`, `WEEK W WEEKS`, `DAY D DAYS`,
`DAYOFWEEK DOW`, `DAYOFWEEK_ISO DOW_ISO`, `DOY`, `HOUR H HOURS HR HRS`, `MINUTE M MIN MINS MINUTES`,
`SECOND S SEC SECONDS SECS`, and the classes it builds: `Year`, `YearOfWeek`, `Quarter`, `Month`, `WeekOfYear`,
`DayOfMonth`, `DayOfWeek`, `WeekDay` (+ 1 for `DAYOFWEEK_ISO`), `DayOfYear`, `Hour`, `Minute`,
`SecondWithFraction`. So `extract(HOUR FROM c)` is `Hour(c)` and takes §3–§5; `extract(SECOND FROM c)` is
`SecondWithFraction(c)`, a `decimal(8,6)`.

## 7. The neighbours

- `DateFormatClass.inputTypes`: `(TimestampType, StringTypeWithCollation)`: the same implicit cast as §3–§4.
- `TruncTimestamp.inputTypes`: `(StringTypeWithCollation, TimestampType)`: the same.
- `ToTimestamp.inputTypes` (`UnixTimestamp`'s parent): `(TypeCollection(StringTypeWithCollation, DateType,
  TimestampType, TimestampNTZType), StringTypeWithCollation)`: a `STRING` is **not** cast; it is parsed with the
  format (default `yyyy-MM-dd HH:mm:ss`), and an unparsable text raises `CANNOT_PARSE_TIMESTAMP` (ANSI on) or answers
  NULL.
