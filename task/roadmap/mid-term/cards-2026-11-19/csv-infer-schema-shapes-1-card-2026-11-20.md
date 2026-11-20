# Card CSV-INFER-SCHEMA-SHAPES-1: a CSV read with `inferSchema=true` infers other types than Spark for integer columns, `5.`, a 20-digit integer and ` 7 `

**Date:** 2026-11-20. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-20 (the owner's delegation, about 00:22 UTC). **Source:** the Fable reply of 2026-11-20 in the agents' lessons folder (file `Fable reply — CSV-STRING-FIELD-TEXT-1 step 0 2026-11-20.md`), §5, kept outside the repository; the cells are in the [domain table §3](../../../wo/csv-string-field-text-1/domain-table.md) (the `inferSchema=true` set T) and the [sketch §7](../../../wo/csv-string-field-text-1/sketch.md) (Q5).

**Status:** filed 2026-11-20 as a card only; not dispatched.

**Severity: S2, silent.** Fable §5: "Card it S2, but not as a new finding, and hold the label line for the owner."

**Label-blocking: open, held for the owner.** Fable §5 files the card "with label-blocking: open, owner's call". The label line is held: neither `kept-divergences.md` nor the label text is edited. Fable's lean for the owner: exclude `inferSchema=true` reads from the label by name. The 20-digit and ` 7 ` cells meet the same test that made CSV-STRING-FIELD-TEXT-1 label-blocking. This is Fable's guidance under the owner's delegation, not an owner ruling.

## Finding

**[M] The cells.** The `inferSchema=true` set has 192 WRONG cells, none in the reads of the other cards. 120 are type only: Spark infers `int`, RePark infers `bigint`, for `42`, `007`, `+5`, `"007"` and the `nullValue` file. 72 are values:

- `5.`: Spark infers `decimal(1,0)` and answers 5; RePark infers `double` and answers 5.0.
- ` 7 `: Spark infers `double` and answers 7.0; RePark infers text.
- the 20-digit integer: Spark infers `decimal(20,0)`; RePark infers `double`.

**Existing registry rows.** 144 of the 192 cells are rows that already exist in [docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md), and this card cites them rather than restating them: EX-IO-3 and CSV-INFER-INT32-WIDTH (the latter BACKLOG 2026-09-10) for the 120 type-only cells, and CSV-INFER-20DIGIT (DECLARED 2026-09-06, pinned) for the 24 cells of the 20-digit integer. The new shapes are `5.` and ` 7 `, 48 cells, which this card adds.

## First step

None set by Fable. The fix Fable names is Spark's `CSVInferSchema` in Rust, a unit about this size again, in its words.

Up: [map.md](map.md).
