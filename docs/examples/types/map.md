# map — docs/examples/types/

## Purpose

Worked examples for the `types` module surface (the `pyspark.sql.types`
constructors used by scripts, casts, and schemas). Type construction and
display answers are measured on live PySpark 4.1.2 and identical on repark;
every example runs JVM-free. `types.repark_type_to_arrow` and
`types.struct_type_from_arrow` are repark extensions (`hasattr` False on live
PySpark 4.1.2, EX-22 leg 1) and are taught in
[arrow_schema_roundtrip.py](arrow_schema_roundtrip.py). All 28 roster names are
covered; no `types` name measured divergent. Examples keep the house form: one
module docstring, the `main()` one-liner, and bare helpers.

## Contents

- [atomic_numeric.py](atomic_numeric.py) — IntegerType, LongType, ShortType,
  ByteType, FloatType, DoubleType.
- [string_and_bool.py](string_and_bool.py) — StringType (default and collated),
  CharType, VarcharType, BinaryType, BooleanType.
- [temporal_types.py](temporal_types.py) — DateType (with the day-ordinal
  conversion round trip), TimestampType, TimestampNTZType, CalendarIntervalType.
- [interval_types.py](interval_types.py) — TimeType, DayTimeIntervalType,
  YearMonthIntervalType field ranges and display strings.
- [decimal_null_variant.py](decimal_null_variant.py) — DecimalType (including the
  boundary `decimal(39,0)` and `decimal(5,7)` spellings), NullType, VariantType.
- [complex_types.py](complex_types.py) — ArrayType (including the nested
  array-of-struct display), MapType, StructField,
  StructType (field access, `add`, `toDDL`, `treeString`) and a
  `createDataFrame` with the explicit schema.
- [datatype_from_ddl.py](datatype_from_ddl.py) — `DataType.typeName` and
  `fromDDL` parsing.
- [arrow_schema_roundtrip.py](arrow_schema_roundtrip.py) — the two repark-only
  Arrow helpers.

## Pointers

- Up: [../map.md](../map.md)
- Registry: [../../spark-sql-iceberg-parity.md](../../spark-sql-iceberg-parity.md) §7
