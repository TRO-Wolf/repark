# CSV-FIELD-KERNEL-1 — the bar (2026-11-20)

Head `ff85b57e`, package `S/ddu-work/pkg-kernel1-ff85b57e` (`BUILT_FROM` names it); main's package
`S/ddu-work/pkg-rp1base-8bc26cba`. Spark 4.1.2's answers are the recorded cells of [../oracle.md](../oracle.md): step 0's
oracle and zone cells and this unit's added texts ([raw/added-spark.jsonl](raw/added-spark.jsonl)). The tables in
`crates/repark-functions/src/csv/field/tests.rs` and the facade pin come from those cells through
[probes/tables.py](probes/tables.py).

## The converter table

Per declared type: cells passed of cells (step 0's 74 texts; the 62 added texts; the four re-measured texts; the
zone cells of Los Angeles, New York and Tokyo).

| type | oracle | added | re-measured | zones | Spark's converter, as the kernel implements it |
|---|---|---|---|---|---|
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | 74 / 74 each | 60 / 62 each, **2 declared** | — | — | Java's radix-10 parse: one sign, leading zeros, ASCII digits, NULL out of range |
| `FLOAT`, `DOUBLE` | 74 / 74 each | 62 / 62 each | — | — | the `NaN`, `Inf`, `-Inf` tokens, then Java's grammar (trim of code points up to U+0020, a signed `NaN`/`Infinity`, one `fFdD` suffix, hex floats), correctly rounded |
| `DECIMAL(10,2)` | 74 / 74 | 60 / 62, **2 declared** | — | — | commas stripped, `BigDecimal`'s grammar, HALF_UP to the scale, NULL on a precision overflow |
| `DECIMAL(38,10)`, `DECIMAL(3,0)` | — | 60 / 62 each, **2 declared** | 4 / 4 each | — | the same |
| `DECIMAL(4,4)` | — | 62 / 62 | 4 / 4 | — | the same |
| `BOOLEAN` | 74 / 74 | 62 / 62 | — | — | `equalsIgnoreCase` (`falſe` and `FALſE` answer `false`: implemented, not declared) |
| `DATE` | 74 / 74 | 62 / 62 | — | 21 / 21 | `stringToDate` (years of four to seven digits, a ` ` or `T` suffix dropped), then the `GMT`-stripped retry |
| `TIMESTAMP` | 74 / 74 | 62 / 62 | — | 21 / 21 | `stringToTimestamp` in the session zone, then the `GMT`-stripped retry |
| `TIMESTAMP_NTZ` | 74 / 74 | 62 / 62 | — | 21 / 21, and the same 21 under UTC | `parseWithoutTimeZone`: an offset, a zone or a time alone is NULL; no zone is ever read, so F2's gap cells (`2024-03-10 02:30:00` in Los Angeles and New York) keep the wall clock |
| `STRING` | 74 / 74 | 62 / 62 | — | 21 / 21 | the text |
| `BINARY` | 74 / 74 | 62 / 62 | — | — | the text's UTF-8 bytes |
| `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `VARIANT` | refused by name | refused by name | — | — | Spark answers (or its client raises for year-month); RePark has no such column, so the builder raises `[UNSUPPORTED_DATATYPE] a CSV field of type … is not supported by repark yet` when the frame is planned, and the UDF refuses every type without a converter |

**Totals:** oracle 962 of 962; added 978 of 978 plus 14 declared; re-measured 12 of 12; zones 84 of 84.

- **The declared difference (14 cells).** Unicode decimal digits (`١٢٣`, `１２`): Spark's
  `Integer.parseInt` and `BigDecimal` take them for the four integer types and the three decimals that fit; the
  kernel keeps ASCII digits and answers NULL (Fable §10 item 5). The tests assert NULL there and that Spark answers.
- **Unmeasured:** `DECIMAL` over `1e1000000000`: the kernel answers NULL from the digit count and the exponent
  alone (a test pins it, with `1e-1000000000` as `0` and `1e2147483647` as NULL); Spark was not run there.
- **The ANSI flag** plays no part: Spark's 1,184 oracle pairs answer alike in both modes (step 0), and the kernel
  reads no ANSI setting.
- **`nullValue` stays outside the kernel.** An empty text is the converter's input: `STRING` answers `''`, `INT` a
  misfit.
- **A misfit is reported apart from NULL input:** `misfit_rows(texts, fields)` is the rows whose text is present
  and whose field is NULL (test `a_misfit_is_reported_apart_from_null_input`).

## Where the cells run

- **Rust** (`cargo test -p repark-functions csv::field`): the four tables above, the misfit report, the bounded
  exponent, the refusals, the string type marker for every target, and the registered UDF through SQL.
- **Through the built package** (`python/repark/tests/io/test_csv_field_kernel_1.py`): the Python builder
  `csv_field` over a text frame, the oracle (962), the added cells (978 and 14 declared), the zone cells and the
  refusals. 9 passed on `pkg-kernel1-ff85b57e`; 9 failed on main's package (`csv_field` does not exist there).
- **A finding the package run caught:** the first build passed the type as a typed NULL literal, and the facade's
  integer literal narrowing turned `BIGINT`'s `Int64(NULL)` into `Int32`, so `2147483648` answered NULL. The marker
  is now the Arrow type's name as a string literal (`ff85b57e`), and a test pins `BIGINT` over `2147483648`.

## No answer moves

Step 0's domain-table probe (`../evidence/probes/domain.py`, all 14 parts: S, N, F, T, M, L and E in both ANSI
modes) on main's package and on the head's, one after the other per part, `TZ=UTC`, each run printing its
`repark._native` path (14 runs on `pkg-rp1base-8bc26cba`, 14 on `pkg-kernel1-ff85b57e`):

- **7,718 records per build** (7,704 cells and 14 `EXPLAIN` records); **0 differ**, byte for byte, with no
  normalising (`raw/nomove-summary.txt`, per part). Every part ran with exit code 0, and the tags match one for
  one.
- The reader is untouched (`git diff e5e90a1b..HEAD -- python/repark/src` is empty), so this is the expected
  answer: nothing reads through the kernel until the wiring unit.
