# Card SETOP-MIXED-TYPE-COERCION-1: mixed-type set operations keep left schemas and refuse lossy multiset round-trips

**Date:** 2026-11-23. **Filed by:** Gemini 4 Neon (`gemini-4-neon-xhigh`), the delta-2 builder of
SQL-SETOP-ALL-MULTISET-1 (PR #1416), under orchestrator ruling F-5. **Severity:** S3 proposed
(owner to rate). **Status:** filed, not dispatched.

## Measured

Measured on live Spark 4.1.2 (session zone UTC, ANSI `true` and `false`), RePark `main`
(`e80e6bc8`), PR #1416 `head3` (`1b1a00b3`), and PR #1416 `head4` (delta 2):

| Query | Mode | Spark 4.1.2 | `main` (`e80e6bc8`) | `head3` (`1b1a00b3`) | `head4` (delta 2) |
|---|---|---|---|---|---|
| `('01','01') EXCEPT ALL (1)` | ANSI on | `[[1]]` (`bigint`) | refused (`SETOP-ALL-REFUSE-1`) | `[['1']]` (`string`) [mutated] | `AnalysisException` (`SETOP-MIXED-TYPE-COERCION-1`) |
| `('01','01') EXCEPT ALL (1)` | ANSI off | `[['01'], ['01']]` (`string`) | refused (`SETOP-ALL-REFUSE-1`) | `[['1']]` (`string`) [mutated] | `AnalysisException` (`SETOP-MIXED-TYPE-COERCION-1`) |
| `('01') EXCEPT (1)` (distinct) | ANSI on | `[]` (`bigint`) | `[]` (`string`) | `[]` (`string`) | `[]` (`string`) [unchanged] |
| `('01') EXCEPT (1)` (distinct) | ANSI off | `[['01']]` (`string`) | `[]` (`string`) [pre-existing] | `[]` (`string`) | `[]` (`string`) [unchanged] |
| `('01') INTERSECT (1)` (distinct) | ANSI off | `[]` (`string`) | `[['01']]` (`string`) [pre-existing] | `[['01']]` (`string`) | `[['01']]` (`string`) [unchanged] |
| `('1.00','1.00') EXCEPT ALL (1.0D)` | ANSI on / off | `[[1.0]]` (`double`) / `[['1.00'], ['1.00']]` (`string`) | refused (`SETOP-ALL-REFUSE-1`) | `[['1.0']]` (`string`) [mutated] | `AnalysisException` (`SETOP-MIXED-TYPE-COERCION-1`) |
| `('2024-01-01','2024-01-01') EXCEPT ALL (DATE '2024-01-01')` | ANSI on / off | `[[2024-01-01]]` (`date`) / `[['2024-01-01']]` (`string`) | refused (`SETOP-ALL-REFUSE-1`) | `[['2024-01-01']]` (`string`) | `AnalysisException` (`SETOP-MIXED-TYPE-COERCION-1`) |
| `(INT 16777217, 16777217) EXCEPT ALL (FLOAT 16777216.0)` | ANSI on / off | `[16777217.0, 16777217.0]` (`double`) / `[16777216.0]` (`float`) | refused (`SETOP-ALL-REFUSE-1`) | `[[16777216]]` (`int`) [mutated] | `AnalysisException` (`SETOP-MIXED-TYPE-COERCION-1`) |
| `(BIGINT 2^53+1, 2^53+1) EXCEPT ALL (DOUBLE 2^53)` | ANSI on / off | `[[9007199254740992.0]]` (`double`) | refused (`SETOP-ALL-REFUSE-1`) | `[[9007199254740992]]` (`bigint`) [mutated] | `AnalysisException` (`SETOP-MIXED-TYPE-COERCION-1`) |
| `(STRUCT('01'), STRUCT('01')) EXCEPT ALL (STRUCT(1))` | ANSI on / off | `[{x:1}]` (`bigint`) / `[{x:'01'}, {x:'01'}]` (`string`) | refused (`SETOP-ALL-REFUSE-1`) | `[[{'x':'1'}]]` (`struct<x:string>`) [mutated] | `AnalysisException` (`SETOP-MIXED-TYPE-COERCION-1`) |
| `(ARRAY('01'), ARRAY('01')) EXCEPT ALL (ARRAY(1))` | ANSI on / off | `[[[1]]]` (`array<bigint>`) / `[[['01']], [['01']]]` (`array<string>`) | refused (`SETOP-ALL-REFUSE-1`) | `[[['1']]]` (`array<string>`) [mutated] | `AnalysisException` (`SETOP-MIXED-TYPE-COERCION-1`) |
| `(INT 1, 1) EXCEPT ALL (BIGINT 1, 5000000000)` | ANSI on / off | `[[1]]` (`bigint`) | refused (`SETOP-ALL-REFUSE-1`) | `[[1]]` (`int`) | `[[1]]` (`int`) [kept: lossless] |
| `(FLOAT 1.5, 1.5) EXCEPT ALL (DOUBLE 1.5)` | ANSI on / off | `[[1.5]]` (`double`) | refused (`SETOP-ALL-REFUSE-1`) | `[[1.5]]` (`float`) | `[[1.5]]` (`float`) [kept: lossless] |
| `(DECIMAL(10,2) 1.50, 1.50) EXCEPT ALL (DOUBLE 1.5)` | ANSI on / off | `[[1.5]]` (`double`) | refused (`SETOP-ALL-REFUSE-1`) | `[[1.5]]` (`double`) | `[[1.5]]` (`double`) [matches Spark] |

The distinct twin's ANSI-off answer on `main` (`('01') EXCEPT (1)` -> `[]` where Spark gives
`[['01']]`, and `(1) EXCEPT ('01')` -> `[]` where Spark gives `[['1']]`) is pre-existing and left
unchanged.

## Refused pairs in multiset set operations (`INTERSECT ALL` / `EXCEPT ALL` / `MINUS ALL`)

`setop_all_multiset` (`crates/repark-core/src/pre_execute/setop_all.rs`) refuses loudly during
analysis (before any write) with `AnalysisException` naming `SETOP-MIXED-TYPE-COERCION-1` whenever
casting a left key from its output type through DataFusion's coerced key type and back to its
output type is not lossless:

- `STRING` (`Utf8` / `LargeUtf8` / `Utf8View`) output coerced to any non-string key type:
  `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, unsigned integers, `FLOAT`, `DOUBLE`, `Float16`,
  `DECIMAL(p, s)`, `DATE`, `TIMESTAMP`, `TIME`, `BOOLEAN`, `BINARY`.
- `INT` / `UInt32` output coerced to `FLOAT` (`Float32`, 24-bit significand loses precision past
  `2^24`).
- `BIGINT` / `UInt64` output coerced to `FLOAT` (`Float32`) or `DOUBLE` (`Float64`, 53-bit
  significand loses precision past `2^53`).
- `DECIMAL(p_out, s_out)` output coerced to `FLOAT` / `DOUBLE` (when not already widened to
  `DOUBLE` by `spark_decimal_float_widen`) or to a `DECIMAL(p_key, s_key)` with `s_key < s_out` or
  `p_key - s_key < p_out - s_out`.
- Integer output coerced to `DECIMAL(p_key, s_key)` with fewer integer digits than the integer
  type's decimal width, signed-to-unsigned casts, `UInt64 -> Int64`, and narrowing numeric casts.
- `DATE` output coerced to `TIMESTAMP`, `TIMESTAMP(Second|Millisecond|Microsecond)` coerced to
  `TIMESTAMP(Nanosecond)` (narrowed year range 1677–2262), or mismatched timestamp timezones.
- Any `STRUCT` or `ARRAY` (`List*`) containing a lossy field or element pair (for example
  `STRUCT<x:STRING>` vs `STRUCT<x:INT>`, `ARRAY<STRING>` vs `ARRAY<INT>`), or a nested `NULL`
  field/element that Arrow cannot cast back to `Null`.

Lossless widenings keep answering with the distinct twin's output schema: exact integer widenings
(`TINYINT`/`SMALLINT`/`INT`/`BIGINT`), exact float widenings (`FLOAT`/`DOUBLE`), exact
`DECIMAL(p_1, s_1)` -> `DECIMAL(p_2, s_2)` widenings, `INT`/`BIGINT` -> wide-enough `DECIMAL`,
`INT` -> `DOUBLE`, `DECIMAL`/`DOUBLE` (already widened to `DOUBLE` on the Spark door), and
top-level untyped `NULL` left columns (emitted as `NULL` with `void` schema).

## Cause

DataFusion plans `INTERSECT` / `EXCEPT` (`DISTINCT` and `ALL`) as `LeftSemi` / `LeftAnti` joins
whose output schema is `join.left.schema()`, while `type_coercion` coerces only the join keys to a
common comparison type (and coerces `STRING` vs `INT` to `INT` regardless of ANSI mode). Distinct
forms emit the uncast left column, keeping `'01'` in `INTERSECT` but still disagreeing with Spark
on output type and ANSI-off string promotion. `setop_all_multiset` groups on the coerced key and
casts back to the output schema, which mutated `'01'` into `'1'` and `2^53+1` into `2^53` until
delta 2 fenced lossy cast-backs.

## First step

Implement Spark's `TypeCoercion.findWiderTypeForTwo` (both ANSI modes) over set operations
(`Union`, distinct `INTERSECT`/`EXCEPT`, and `INTERSECT ALL`/`EXCEPT ALL`) on the Spark door so
both sides project to Spark's wider common output type before `type_coercion` runs; then retire
this refusal once `out_type == key_type` holds for every Spark-valid pair.

Up: [map.md](map.md).
