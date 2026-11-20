# CSV-STRING-FIELD-TEXT-1 — the kernel's oracle: Spark 4.1.2's answer per declared type (revision, 2026-11-20)

The recorded answers CSV-FIELD-KERNEL-1 tests against. Each cell is Spark 4.1.2 live: `spark.read.schema("x <type>, k
STRING").csv(<one-row file>)`, header off, session zone UTC, the value of `x` as PySpark collects it. Probe:
[evidence/probes/revision.py](evidence/probes/revision.py) (`oracle`, `zones`); raw cells:
[evidence/raw/](evidence/raw/map.md) (`revision-cells-spark.jsonl.gz`, `revision-tables.md.txt`).

## The converters (the jar, `UnivocityParser.makeConverter`; the critic's `javap` agrees)

| type | Spark's converter | the kernel must |
|---|---|---|
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | `StringOps.toByte`, `toShort`, `toInt`, `toLong` (`$2`, `$4`, `$6`, `$8`) | Java's radix-10 parse: one sign, leading zeros, no space, tab, `_`, `0x`, fraction or exponent; out of range is NULL |
| `FLOAT`, `DOUBLE` | the `nanValue` (`NaN`), `positiveInf` (`Inf`), `negativeInf` (`-Inf`) tokens by equality, then `toFloat`/`toDouble` (`$10`, `$12`) | Java's `parseFloat`/`parseDouble`: it trims, takes `Infinity`, `1d`, `1f`, hex floats (`0x1p3`), keeps `-0`'s sign; `inf`, `nan`, `+Inf` are NULL; `FLOAT` rounds to binary32 |
| `DECIMAL(p,s)` | `new BigDecimal(text.replaceAll(",", ""))`, then `Decimal(bd, p, s)` (`$16`) | strip every comma, parse exponents, no spaces; round HALF_UP to the scale; NULL when the precision overflows |
| `BOOLEAN` | `StringOps.toBoolean` (`$14`) | `true`/`false` in any case, nothing else (no `t`, `1`, `yes`, spaces) |
| `DATE` | `dateFormatter.parse`, then `cleanLegacyTimestampStr` + `stringToDate` (`$18`; the fallback holds with no `dateFormat` and a non-LEGACY policy) | `stringToDate`: a year alone (`2024`), `2024-1`, `2024-1-5`, leading spaces, a time or `T` suffix ignored; `20240115` is NULL |
| `TIMESTAMP` | `timestampFormatter.parse`, then `stringToTimestamp(s, zoneId)` (`$21`) | the session zone for zone-less text, the text's own offset or `Z` otherwise |
| `TIMESTAMP_NTZ` | `parseWithoutTimeZone(s, false)` (`$24`), no fallback | **never the session zone**: a daylight-saving-gap wall clock stays as written (unit 0's F2); text with an offset or `Z` is NULL |
| `STRING` | `UTF8String.fromString` (`$26`) | the text |
| `BINARY` | `String.getBytes` (`$28`) | the text's UTF-8 bytes |
| intervals | `$30` (calendar), `$32` (year-month), `$34` (day-time) | parse the interval text (`1 02:03:04` is one day 02:03:04 for `DAY TO SECOND`), or refuse by name; RePark has no interval column today |
| `VARIANT` | `VariantValueConverter` | refuse by name (Spark infers a variant value per field) |

`nullSafeDatum` runs first for every type: the `nullValue` token (default `""`) is NULL. A converter that throws is a
misfit: NULL under `PERMISSIVE`. **Spark's answers do not depend on ANSI: 0 of 1,184 cell pairs differ.**

**Two Spark cells are client artifacts, not Spark answers:** `DATE`/`TIMESTAMP`/`TIMESTAMP_NTZ` over `32768` and
`40000` read a year-32768 or year-40000 value that Python's `datetime` cannot hold, and every year-month interval
raises `NOT_IMPLEMENTED` in PySpark's collect. The kernel unit records those through `CAST(x AS STRING)`.

## Against RePark today [M]

| | ANSI on | ANSI off |
|---|---|---|
| unit 1's package, EQUAL of 1,184 (main is byte-equal) | 359 | 445 |
| the all-text scan with `TRY_CAST`, EQUAL of 1,184 | 984 | 984 |

In the table, **†** marks a cell where unit 1 differs from Spark and **‡** one where all-text + `TRY_CAST` differs
(ANSI on). `None` is NULL.

| text | TINYINT | SMALLINT | INT | BIGINT | FLOAT | DOUBLE | DECIMAL(10,2) | BOOLEAN | DATE | TIMESTAMP | TIMESTAMP_NTZ | STRING | BINARY | YEAR-MONTH | DAY-SECOND | VARIANT |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `0` | 0 | 0 | 0 | 0 | 0.0 | 0.0 | 0.00 | None†‡ | None† | None† | None† | 0 | b'0'† | RAISE NOT_IMPLEMENTED | None†‡ | 0†‡ |
| `-0` | 0 | 0 | 0 | 0 | -0.0† | -0.0† | 0.00 | None† | None† | None† | None† | -0† | b'-0'† | RAISE NOT_IMPLEMENTED | None†‡ | 0†‡ |
| `007` | 7 | 7 | 7 | 7 | 7.0 | 7.0 | 7.00 | None† | None† | None† | None† | 007† | b'007'† | RAISE NOT_IMPLEMENTED | None†‡ | 7†‡ |
| `+5` | 5 | 5 | 5 | 5 | 5.0 | 5.0 | 5.00 | None† | None† | None† | None† | +5 | b'+5' | RAISE NOT_IMPLEMENTED | None†‡ | 5†‡ |
| `-007` | -7 | -7 | -7 | -7 | -7.0 | -7.0 | -7.00 | None† | None† | None† | None† | -007† | b'-007'† | RAISE NOT_IMPLEMENTED | None†‡ | -7†‡ |
| `127` | 127 | 127 | 127 | 127 | 127.0 | 127.0 | 127.00 | None† | None† | None† | None† | 127 | b'127'† | RAISE NOT_IMPLEMENTED | None†‡ | 127†‡ |
| `128` | None† | 128 | 128 | 128 | 128.0 | 128.0 | 128.00 | None† | None† | None† | None† | 128 | b'128'† | RAISE NOT_IMPLEMENTED | None†‡ | 128†‡ |
| `-129` | None† | -129 | -129 | -129 | -129.0 | -129.0 | -129.00 | None† | None† | None† | None† | -129 | b'-129'† | RAISE NOT_IMPLEMENTED | None†‡ | -129†‡ |
| `200` | None† | 200 | 200 | 200 | 200.0 | 200.0 | 200.00 | None† | None† | None† | None† | 200 | b'200'† | RAISE NOT_IMPLEMENTED | None†‡ | 200†‡ |
| `32767` | None† | 32767 | 32767 | 32767 | 32767.0 | 32767.0 | 32767.00 | None† | RAISE year 32767 is out of range‡ | RAISE year 32767 is out of range† | RAISE year 32767 is out of range‡ | 32767 | b'32767'† | RAISE NOT_IMPLEMENTED | None†‡ | 32767†‡ |
| `32768` | None† | None† | 32768 | 32768 | 32768.0 | 32768.0 | 32768.00 | None† | RAISE year 32768 is out of range‡ | RAISE year 32768 is out of range† | RAISE year 32768 is out of range‡ | 32768 | b'32768'† | RAISE NOT_IMPLEMENTED | None†‡ | 32768†‡ |
| `40000` | None† | None† | 40000 | 40000 | 40000.0 | 40000.0 | 40000.00 | None† | RAISE year 40000 is out of range‡ | RAISE year 40000 is out of range† | RAISE year 40000 is out of range‡ | 40000 | b'40000'† | RAISE NOT_IMPLEMENTED | None†‡ | 40000†‡ |
| `2147483647` | None† | None† | 2147483647 | 2147483647 | 2147483648.0 | 2147483647.0 | None† | None† | None† | None† | None† | 2147483647 | b'2147483647'† | RAISE NOT_IMPLEMENTED | None†‡ | 2147483647†‡ |
| `2147483648` | None† | None† | None† | 2147483648 | 2147483648.0 | 2147483648.0 | None† | None† | None† | None† | None† | 2147483648 | b'2147483648'† | RAISE NOT_IMPLEMENTED | None†‡ | 2147483648†‡ |
| `9223372036854775807` | None† | None† | None† | 9223372036854775807 | 9.223372036854776e+18 | 9.223372036854776e+18 | None† | None† | None† | None† | None† | 9223372036854775807 | b'9223372036854775807'† | RAISE NOT_IMPLEMENTED | None†‡ | 9223372036854775807†‡ |
| `9223372036854775808` | None† | None† | None† | None† | 9.223372036854776e+18 | 9.223372036854776e+18 | None† | None† | None† | None† | None† | 9223372036854775808 | b'9223372036854775808' | RAISE NOT_IMPLEMENTED | None†‡ | 9223372036854775808†‡ |
| `99999999999999999999` | None† | None† | None† | None† | 1.0000000200408773e+20 | 1e+20 | None† | None† | None† | None† | None† | 99999999999999999999 | b'99999999999999999999' | RAISE NOT_IMPLEMENTED | None†‡ | 99999999999999999999†‡ |
| ` 7` | None† | None† | None† | None† | 7.0‡ | 7.0‡ | None†‡ | None† | None† | None† | None† |  7 | b' 7' | RAISE NOT_IMPLEMENTED | None†‡ | " 7"†‡ |
| `7 ` | None† | None† | None† | None† | 7.0‡ | 7.0‡ | None†‡ | None† | None† | None† | None† | 7  | b'7 ' | RAISE NOT_IMPLEMENTED | None†‡ | "7 "†‡ |
| `\t7` | None† | None† | None† | None† | 7.0‡ | 7.0‡ | None†‡ | None† | None† | None† | None† | 	7 | b'\t7' | RAISE NOT_IMPLEMENTED | None†‡ | "\t7"†‡ |
| ` 7 ` | None† | None† | None† | None† | 7.0‡ | 7.0‡ | None†‡ | None† | None† | None† | None† |  7  | b' 7 ' | RAISE NOT_IMPLEMENTED | None†‡ | " 7 "†‡ |
| `1.50` | None† | None† | None† | None† | 1.5 | 1.5 | 1.50 | None† | None† | None† | None† | 1.50† | b'1.50'† | RAISE NOT_IMPLEMENTED | None†‡ | 1.50†‡ |
| `1.5` | None† | None† | None† | None† | 1.5 | 1.5 | 1.50 | None† | None† | None† | None† | 1.5 | b'1.5'† | RAISE NOT_IMPLEMENTED | None†‡ | 1.5†‡ |
| `5.` | None† | None† | None† | None† | 5.0 | 5.0 | 5.00 | None† | None† | None† | None† | 5.† | b'5.'† | RAISE NOT_IMPLEMENTED | None†‡ | 5†‡ |
| `.5` | None† | None† | None† | None† | 0.5 | 0.5 | 0.50 | None† | None† | None† | None† | .5† | b'.5'† | RAISE NOT_IMPLEMENTED | None†‡ | 0.5†‡ |
| `1e3` | None† | None† | None† | None† | 1000.0 | 1000.0 | 1000.00‡ | None† | None† | None† | None† | 1e3† | b'1e3'† | RAISE NOT_IMPLEMENTED | None†‡ | 1000†‡ |
| `1E-2` | None† | None† | None† | None† | 0.009999999776482582 | 0.01 | 0.01‡ | None† | None† | None† | None† | 1E-2† | b'1E-2'† | RAISE NOT_IMPLEMENTED | None†‡ | 0.01†‡ |
| `1.555` | None† | None† | None† | None† | 1.5549999475479126 | 1.555 | 1.56 | None† | None† | None† | None† | 1.555 | b'1.555'† | RAISE NOT_IMPLEMENTED | None†‡ | 1.555†‡ |
| `-1.555` | None† | None† | None† | None† | -1.5549999475479126 | -1.555 | -1.56 | None† | None† | None† | None† | -1.555 | b'-1.555'† | RAISE NOT_IMPLEMENTED | None†‡ | -1.555†‡ |
| `1.554` | None† | None† | None† | None† | 1.5540000200271606 | 1.554 | 1.55 | None† | None† | None† | None† | 1.554 | b'1.554'† | RAISE NOT_IMPLEMENTED | None†‡ | 1.554†‡ |
| `99999999.995` | None† | None† | None† | None† | 100000000.0 | 99999999.995 | None† | None† | None† | None† | None† | 99999999.995† | b'99999999.995'† | RAISE NOT_IMPLEMENTED | None†‡ | 99999999.995†‡ |
| `123456789012` | None† | None† | None† | 123456789012 | 123456790528.0 | 123456789012.0 | None† | None† | None† | None† | None† | 123456789012 | b'123456789012'† | RAISE NOT_IMPLEMENTED | None†‡ | 123456789012†‡ |
| `1e400` | None† | None† | None† | None† | inf | inf | None† | None† | None† | None† | None† | 1e400† | b'1e400'† | RAISE NOT_IMPLEMENTED | None†‡ | "1e400"†‡ |
| `1,5` | None† | None† | None† | None† | None† | None† | 15.00†‡ | None† | None† | None† | None† | 1,5 | b'1,5' | RAISE NOT_IMPLEMENTED | None†‡ | 15†‡ |
| `1,000.5` | None† | None† | None† | None† | None† | None† | 1000.50†‡ | None† | None† | None† | None† | 1,000.5 | b'1,000.5' | RAISE NOT_IMPLEMENTED | None†‡ | 1000.5†‡ |
| `1d` | None† | None† | None† | None† | 1.0‡ | 1.0‡ | None† | None† | None† | None† | None† | 1d | b'1d' | RAISE NOT_IMPLEMENTED | None†‡ | "1d"†‡ |
| `1f` | None† | None† | None† | None† | 1.0‡ | 1.0‡ | None† | None† | None† | None† | None† | 1f | b'1f' | RAISE NOT_IMPLEMENTED | None†‡ | "1f"†‡ |
| `0x10` | None† | None† | None† | None† | None† | None† | None† | None† | None† | None† | None† | 0x10 | b'0x10' | RAISE NOT_IMPLEMENTED | None†‡ | "0x10"†‡ |
| `0x1p3` | None† | None† | None† | None† | 8.0†‡ | 8.0†‡ | None† | None† | None† | None† | None† | 0x1p3 | b'0x1p3' | RAISE NOT_IMPLEMENTED | None†‡ | "0x1p3"†‡ |
| `1_000` | None† | None† | None† | None† | None† | None† | None† | None† | None† | None† | None† | 1_000 | b'1_000' | RAISE NOT_IMPLEMENTED | None†‡ | "1_000"†‡ |
| `NaN` | None† | None† | None† | None† | nan | nan | None† | None† | None† | None† | None† | NaN | b'NaN'† | RAISE NOT_IMPLEMENTED | None†‡ | "NaN"†‡ |
| `nan` | None† | None† | None† | None† | None†‡ | None†‡ | None† | None† | None† | None† | None† | nan† | b'nan'† | RAISE NOT_IMPLEMENTED | None†‡ | "nan"†‡ |
| `Inf` | None† | None† | None† | None† | inf | inf | None† | None† | None† | None† | None† | Inf | b'Inf' | RAISE NOT_IMPLEMENTED | None†‡ | "Inf"†‡ |
| `+Inf` | None† | None† | None† | None† | None†‡ | None†‡ | None† | None† | None† | None† | None† | +Inf | b'+Inf' | RAISE NOT_IMPLEMENTED | None†‡ | "+Inf"†‡ |
| `-Inf` | None† | None† | None† | None† | -inf | -inf | None† | None† | None† | None† | None† | -Inf | b'-Inf' | RAISE NOT_IMPLEMENTED | None†‡ | "-Inf"†‡ |
| `inf` | None† | None† | None† | None† | None†‡ | None†‡ | None† | None† | None† | None† | None† | inf† | b'inf'† | RAISE NOT_IMPLEMENTED | None†‡ | "inf"†‡ |
| `Infinity` | None† | None† | None† | None† | inf | inf | None† | None† | None† | None† | None† | Infinity | b'Infinity' | RAISE NOT_IMPLEMENTED | None†‡ | "Infinity"†‡ |
| `-Infinity` | None† | None† | None† | None† | -inf | -inf | None† | None† | None† | None† | None† | -Infinity | b'-Infinity' | RAISE NOT_IMPLEMENTED | None†‡ | "-Infinity"†‡ |
| `true` | None† | None† | None† | None† | None† | None† | None† | True | None† | None† | None† | true | b'true'† | RAISE NOT_IMPLEMENTED | None†‡ | true†‡ |
| `TRUE` | None† | None† | None† | None† | None† | None† | None† | True | None† | None† | None† | TRUE† | b'TRUE'† | RAISE NOT_IMPLEMENTED | None†‡ | true†‡ |
| `True` | None† | None† | None† | None† | None† | None† | None† | True | None† | None† | None† | True† | b'True'† | RAISE NOT_IMPLEMENTED | None†‡ | true†‡ |
| `false` | None† | None† | None† | None† | None† | None† | None† | False | None† | None† | None† | false | b'false'† | RAISE NOT_IMPLEMENTED | None†‡ | false†‡ |
| `t` | None† | None† | None† | None† | None† | None† | None† | None†‡ | None† | None† | None† | t | b't' | RAISE NOT_IMPLEMENTED | None†‡ | "t"†‡ |
| `1` | 1 | 1 | 1 | 1 | 1.0 | 1.0 | 1.00 | None†‡ | None† | None† | None† | 1 | b'1'† | RAISE NOT_IMPLEMENTED | None†‡ | 1†‡ |
| `y` | None† | None† | None† | None† | None† | None† | None† | None†‡ | None† | None† | None† | y | b'y' | RAISE NOT_IMPLEMENTED | None†‡ | "y"†‡ |
| `yes` | None† | None† | None† | None† | None† | None† | None† | None†‡ | None† | None† | None† | yes | b'yes' | RAISE NOT_IMPLEMENTED | None†‡ | "yes"†‡ |
| ` true` | None† | None† | None† | None† | None† | None† | None† | None†‡ | None† | None† | None† |  true | b' true' | RAISE NOT_IMPLEMENTED | None†‡ | " true"†‡ |
| `2024` | None† | 2024 | 2024 | 2024 | 2024.0 | 2024.0 | 2024.00 | None† | 2024-01-01†‡ | 2024-01-01 00:00:00† | 2024-01-01 00:00:00†‡ | 2024 | b'2024'† | RAISE NOT_IMPLEMENTED | None†‡ | 2024†‡ |
| `2024-1` | None† | None† | None† | None† | None† | None† | None† | None† | 2024-01-01†‡ | 2024-01-01 00:00:00 | 2024-01-01 00:00:00†‡ | 2024-1 | b'2024-1' | RAISE NOT_IMPLEMENTED | None†‡ | "2024-01-01 00:00:00+00:00"†‡ |
| `2024-1-5` | None† | None† | None† | None† | None† | None† | None† | None† | 2024-01-05 | 2024-01-05 00:00:00 | 2024-01-05 00:00:00†‡ | 2024-1-5 | b'2024-1-5' | RAISE NOT_IMPLEMENTED | None†‡ | "2024-01-05 00:00:00+00:00"†‡ |
| `2024-01-15` | None† | None† | None† | None† | None† | None† | None† | None† | 2024-01-15 | 2024-01-15 00:00:00 | 2024-01-15 00:00:00 | 2024-01-15 | b'2024-01-15'† | RAISE NOT_IMPLEMENTED | None†‡ | "2024-01-15"†‡ |
| `20240115` | None† | None† | 20240115 | 20240115 | 20240116.0 | 20240115.0 | 20240115.00 | None† | None†‡ | None† | None† | 20240115 | b'20240115'† | RAISE NOT_IMPLEMENTED | None†‡ | 20240115†‡ |
| ` 2024-01-15` | None† | None† | None† | None† | None† | None† | None† | None† | 2024-01-15†‡ | 2024-01-15 00:00:00 | 2024-01-15 00:00:00†‡ |  2024-01-15 | b' 2024-01-15' | RAISE NOT_IMPLEMENTED | None†‡ | "2024-01-15 00:00:00+00:00"†‡ |
| `2024-01-15T10:30:00` | None† | None† | None† | None† | None† | None† | None† | None† | 2024-01-15 | 2024-01-15 10:30:00 | 2024-01-15 10:30:00 | 2024-01-15T10:30:00† | b'2024-01-15T10:30:00'† | RAISE NOT_IMPLEMENTED | None†‡ | "2024-01-15 10:30:00+00:00"†‡ |
| `2024-01-15 10:30:00` | None† | None† | None† | None† | None† | None† | None† | None† | 2024-01-15 | 2024-01-15 10:30:00 | 2024-01-15 10:30:00 | 2024-01-15 10:30:00 | b'2024-01-15 10:30:00'† | RAISE NOT_IMPLEMENTED | None†‡ | "2024-01-15 10:30:00+00:00"†‡ |
| `2024-01-15 10:30` | None† | None† | None† | None† | None† | None† | None† | None† | 2024-01-15†‡ | 2024-01-15 10:30:00 | 2024-01-15 10:30:00†‡ | 2024-01-15 10:30 | b'2024-01-15 10:30' | RAISE NOT_IMPLEMENTED | None†‡ | "2024-01-15 10:30:00+00:00"†‡ |
| `2024-01-15T10:30:00Z` | None† | None† | None† | None† | None† | None† | None† | None† | 2024-01-15 | 2024-01-15 10:30:00 | None†‡ | 2024-01-15T10:30:00Z† | b'2024-01-15T10:30:00Z'† | RAISE NOT_IMPLEMENTED | None†‡ | "2024-01-15 10:30:00+00:00"†‡ |
| `2024-01-15 10:30:00+02:00` | None† | None† | None† | None† | None† | None† | None† | None† | 2024-01-15 | 2024-01-15 08:30:00 | None†‡ | 2024-01-15 10:30:00+02:00† | b'2024-01-15 10:30:00+02:00'† | RAISE NOT_IMPLEMENTED | None†‡ | "2024-01-15 08:30:00+00:00"†‡ |
| `2024-01-15 10:30:00.123456` | None† | None† | None† | None† | None† | None† | None† | None† | 2024-01-15 | 2024-01-15 10:30:00.123456 | 2024-01-15 10:30:00.123456 | 2024-01-15 10:30:00.123456 | b'2024-01-15 10:30:00.123456'† | RAISE NOT_IMPLEMENTED | None†‡ | "2024-01-15 10:30:00.123456+00:00"†‡ |
| `1705314600` | None† | None† | 1705314600 | 1705314600 | 1705314560.0 | 1705314600.0 | None† | None† | None† | None† | None† | 1705314600 | b'1705314600'† | RAISE NOT_IMPLEMENTED | None†‡ | 1705314600†‡ |
| `1-2` | None† | None† | None† | None† | None† | None† | None† | None† | None† | None† | None† | 1-2 | b'1-2' | RAISE NOT_IMPLEMENTED | None†‡ | "1-2"†‡ |
| `1 02:03:04` | None† | None† | None† | None† | None† | None† | None† | None† | None† | None† | None† | 1 02:03:04 | b'1 02:03:04' | RAISE NOT_IMPLEMENTED | 1 day, 2:03:04†‡ | "1 02:03:04"†‡ |
| `INTERVAL '1-2' YEAR TO MONTH` | None† | None† | None† | None† | None† | None† | None† | None† | None† | None† | None† | INTERVAL '1-2' YEAR TO MONTH | b"INTERVAL '1-2' YEAR TO MONTH" | RAISE NOT_IMPLEMENTED | None†‡ | "INTERVAL '1-2' YEAR TO MONTH"†‡ |
| `abc` | None† | None† | None† | None† | None† | None† | None† | None† | None† | None† | None† | abc | b'abc' | RAISE NOT_IMPLEMENTED | None†‡ | "abc"†‡ |

## Non-UTC zones (Los Angeles, New York, Tokyo) [M]

`CAST(x AS STRING)`, and `unix_timestamp(x)` for `TIMESTAMP`, so both engines answer in the session zone. Of 168 cells per variant: unit 1 EQUAL 126 of 168; all-text + `TRY_CAST` EQUAL 152. Every `TIMESTAMP` cell over date or timestamp text is EQUAL on both. The rows below are every cell that is not EQUAL on one of them; the four `2024-03-10 02:30:00` `TIMESTAMP_NTZ` rows are F2: unit 1's typed scan keeps the wall clock, `TRY_CAST` moves it an hour forward.

| zone | companion | type | text | Spark | unit 1 | all-text + `TRY_CAST` |
|---|---|---|---|---|---|---|
| Los_Angeles | alone | STRING | `2024-01-15T10:30:00Z` | 2024-01-15T10:30:00Z | 2024-01-15 10:30:00 | 2024-01-15T10:30:00Z |
| Los_Angeles | alone | STRING | `2024-01-15 10:30:00+02:00` | 2024-01-15 10:30:00+02:00 | 2024-01-15 08:30:00 | 2024-01-15 10:30:00+02:00 |
| Los_Angeles | alone | STRING | `007` | 007 | 7 | 007 |
| Los_Angeles | alone | DATE | `007` | None | RAISE DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGEST | None |
| Los_Angeles | alone | TIMESTAMP | `007` | None | 1969-12-31 16:00:07 | None |
| Los_Angeles | alone | TIMESTAMP_NTZ | `2024-01-15T10:30:00Z` | None | 2024-01-15 10:30:00 | 2024-01-15 10:30:00 |
| Los_Angeles | alone | TIMESTAMP_NTZ | `2024-01-15 10:30:00+02:00` | None | 2024-01-15 08:30:00 | 2024-01-15 10:30:00 |
| Los_Angeles | alone | TIMESTAMP_NTZ | `2024-03-10 02:30:00` | 2024-03-10 02:30:00 | 2024-03-10 02:30:00 | 2024-03-10 03:30:00 |
| Los_Angeles | alone | TIMESTAMP_NTZ | `007` | None | RAISE DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTIO | None |
| Los_Angeles | beside | STRING | `007` | 007 | 7 | 007 |
| Los_Angeles | beside | DATE | `007` | None | RAISE DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGEST | None |
| Los_Angeles | beside | TIMESTAMP | `007` | None | 1969-12-31 16:00:07 | None |
| Los_Angeles | beside | TIMESTAMP_NTZ | `2024-01-15T10:30:00Z` | None | 2024-01-15 10:30:00 | 2024-01-15 10:30:00 |
| Los_Angeles | beside | TIMESTAMP_NTZ | `2024-01-15 10:30:00+02:00` | None | 2024-01-15 08:30:00 | 2024-01-15 10:30:00 |
| Los_Angeles | beside | TIMESTAMP_NTZ | `2024-03-10 02:30:00` | 2024-03-10 02:30:00 | 2024-03-10 02:30:00 | 2024-03-10 03:30:00 |
| Los_Angeles | beside | TIMESTAMP_NTZ | `007` | None | RAISE DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTIO | None |
| New_York | alone | STRING | `2024-01-15T10:30:00Z` | 2024-01-15T10:30:00Z | 2024-01-15 10:30:00 | 2024-01-15T10:30:00Z |
| New_York | alone | STRING | `2024-01-15 10:30:00+02:00` | 2024-01-15 10:30:00+02:00 | 2024-01-15 08:30:00 | 2024-01-15 10:30:00+02:00 |
| New_York | alone | STRING | `007` | 007 | 7 | 007 |
| New_York | alone | DATE | `007` | None | RAISE DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGEST | None |
| New_York | alone | TIMESTAMP | `007` | None | 1969-12-31 19:00:07 | None |
| New_York | alone | TIMESTAMP_NTZ | `2024-01-15T10:30:00Z` | None | 2024-01-15 10:30:00 | 2024-01-15 10:30:00 |
| New_York | alone | TIMESTAMP_NTZ | `2024-01-15 10:30:00+02:00` | None | 2024-01-15 08:30:00 | 2024-01-15 10:30:00 |
| New_York | alone | TIMESTAMP_NTZ | `2024-03-10 02:30:00` | 2024-03-10 02:30:00 | 2024-03-10 02:30:00 | 2024-03-10 03:30:00 |
| New_York | alone | TIMESTAMP_NTZ | `007` | None | RAISE DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTIO | None |
| New_York | beside | STRING | `007` | 007 | 7 | 007 |
| New_York | beside | DATE | `007` | None | RAISE DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGEST | None |
| New_York | beside | TIMESTAMP | `007` | None | 1969-12-31 19:00:07 | None |
| New_York | beside | TIMESTAMP_NTZ | `2024-01-15T10:30:00Z` | None | 2024-01-15 10:30:00 | 2024-01-15 10:30:00 |
| New_York | beside | TIMESTAMP_NTZ | `2024-01-15 10:30:00+02:00` | None | 2024-01-15 08:30:00 | 2024-01-15 10:30:00 |
| New_York | beside | TIMESTAMP_NTZ | `2024-03-10 02:30:00` | 2024-03-10 02:30:00 | 2024-03-10 02:30:00 | 2024-03-10 03:30:00 |
| New_York | beside | TIMESTAMP_NTZ | `007` | None | RAISE DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTIO | None |
| Tokyo | alone | STRING | `2024-01-15T10:30:00Z` | 2024-01-15T10:30:00Z | 2024-01-15 10:30:00 | 2024-01-15T10:30:00Z |
| Tokyo | alone | STRING | `2024-01-15 10:30:00+02:00` | 2024-01-15 10:30:00+02:00 | 2024-01-15 08:30:00 | 2024-01-15 10:30:00+02:00 |
| Tokyo | alone | STRING | `007` | 007 | 7 | 007 |
| Tokyo | alone | DATE | `007` | None | RAISE DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGEST | None |
| Tokyo | alone | TIMESTAMP | `007` | None | 1970-01-01 09:00:07 | None |
| Tokyo | alone | TIMESTAMP_NTZ | `2024-01-15T10:30:00Z` | None | 2024-01-15 10:30:00 | 2024-01-15 10:30:00 |
| Tokyo | alone | TIMESTAMP_NTZ | `2024-01-15 10:30:00+02:00` | None | 2024-01-15 08:30:00 | 2024-01-15 10:30:00 |
| Tokyo | alone | TIMESTAMP_NTZ | `007` | None | RAISE DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTIO | None |
| Tokyo | beside | STRING | `007` | 007 | 7 | 007 |
| Tokyo | beside | DATE | `007` | None | RAISE DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGEST | None |
| Tokyo | beside | TIMESTAMP | `007` | None | 1970-01-01 09:00:07 | None |
| Tokyo | beside | TIMESTAMP_NTZ | `2024-01-15T10:30:00Z` | None | 2024-01-15 10:30:00 | 2024-01-15 10:30:00 |
| Tokyo | beside | TIMESTAMP_NTZ | `2024-01-15 10:30:00+02:00` | None | 2024-01-15 08:30:00 | 2024-01-15 10:30:00 |
| Tokyo | beside | TIMESTAMP_NTZ | `007` | None | RAISE DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTIO | None |

Up: [map.md](map.md).
