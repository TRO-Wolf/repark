# AUD-016 domain table: map subscript key matching (RePark head 44bcf4e1 vs Spark 4.1.2)

Measured 2026-11-02. Group `array_elem`: a literal `map(array(CAST(v AS T1)), 7)[array(CAST(v AS T2))]` for every pair of 14 element types, under ANSI on and off (392 cells). Group `struct_key`: twelve struct-key shapes (exact, reordered, upper, mixed, missing, extra, renamed, widened, string value, nested exact, nested upper, array of upper) under ANSI on and off and `spark.sql.caseSensitive` false and true (48 cells). 440 cells per engine.

| class | cells |
|---|---|
| EQUAL_REFUSE | 211 |
| EQUAL | 189 |
| REPARK_ANSWERS_SPARK_REFUSES | 39 |
| REPARK_REFUSES_SPARK_ANSWERS | 1 |

## RePark answers where Spark refuses (all ANSI off, `array_elem`)

| key element | probe element | Spark | RePark |
|---|---|---|---|
| int | float | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| int | double | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| int | decimal(5,2) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| int | decimal(6,3) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| int | decimal(2,1) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| bigint | float | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| bigint | double | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| bigint | decimal(5,2) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| bigint | decimal(6,3) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| bigint | decimal(2,1) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| smallint | float | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| smallint | double | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| smallint | decimal(5,2) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| smallint | decimal(6,3) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| smallint | decimal(2,1) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| tinyint | float | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| tinyint | double | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| tinyint | decimal(5,2) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| tinyint | decimal(6,3) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| tinyint | decimal(2,1) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| decimal(5,2) | int | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["None"]] |
| decimal(5,2) | bigint | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["None"]] |
| decimal(5,2) | smallint | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["None"]] |
| decimal(5,2) | float | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| decimal(5,2) | double | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| decimal(5,2) | decimal(6,3) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| decimal(6,3) | int | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["None"]] |
| decimal(6,3) | bigint | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["None"]] |
| decimal(6,3) | smallint | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["None"]] |
| decimal(6,3) | float | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| decimal(6,3) | double | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| decimal(2,1) | int | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["None"]] |
| decimal(2,1) | bigint | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["None"]] |
| decimal(2,1) | smallint | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["None"]] |
| decimal(2,1) | tinyint | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["None"]] |
| decimal(2,1) | float | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| decimal(2,1) | double | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| decimal(2,1) | decimal(5,2) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |
| decimal(2,1) | decimal(6,3) | ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | rows [["7"]] |

## RePark refuses where Spark answers

- `binary` key, `string` probe, ANSI false: Spark answers `rows [["7"]]`; RePark refuses with `ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`

## Reading

1. Every `struct_key` cell is equal under both ANSI settings and both case settings: the round-5 fix (field names match case-insensitively unless the session is case-sensitive) holds across the whole struct domain, nested and array-of-struct included.
2. The only divergence class is the ANSI-off implicit cast on array elements: an integer-family or decimal key element against a float, double or decimal probe element answers on RePark where Spark refuses with the type-mismatch error. Under ANSI on every pair is equal. This is the engine's ANSI-off legacy cast rule, one card (ANSI-OFF-LEGACY-CAST-1), and this table is its clause and its pin.
3. One over-refusal: a binary key element against a string probe element under ANSI off refuses on RePark where Spark answers.

Cells: the cells file beside this table (one JSON object per cell).
