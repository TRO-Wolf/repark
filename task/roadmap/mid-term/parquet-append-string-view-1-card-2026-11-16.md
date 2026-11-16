# Card PARQUET-APPEND-STRING-VIEW-1: a second Parquet append of a cast string refuses on `string` versus `string_view`

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from item 2 of section 6 of the step 0 sketch of CAST-LITERAL-UNWRAP-1, [sketch.md](../../wo/cast-literal-unwrap-1/sketch.md). The owner ruled on 2026-11-16, about 16:25, verbatim: "adopt Fable's, run the string-cast sizing", which adopted the four side cards. **Evidence:** [append-side-repark.txt](../../wo/cast-literal-unwrap-1/evidence/raw/append-side-repark.txt) and [append-side-spark.txt](../../wo/cast-literal-unwrap-1/evidence/raw/append-side-spark.txt).

**Status:** filed 2026-11-16, not dispatched. **Severity: S3.**

## Measured

`SELECT 1 AS id, CAST('1' AS STRING) AS c` appended twice to one path.

- On RePark the second append raises `cannot append column 'c': type mismatch source=string vs path=string_view`.
- A plain `'a'` literal appended twice works, and so does an INT append twice.
- Spark appends all three shapes, the cast string and the two controls.

Up: [map.md](map.md).
