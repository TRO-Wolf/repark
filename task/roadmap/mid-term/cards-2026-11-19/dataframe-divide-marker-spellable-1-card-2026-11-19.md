# Card DATAFRAME-DIVIDE-MARKER-SPELLABLE-1: a written `CAST(CAST(x AS DOUBLE) AS DOUBLE)` on both sides of `/` is taken for the DataFrame divide marker

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's rulings of the 2026-11-18 window (item 7; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md)). **Source:** `S/verify-ddl/verdict.md`, F2.

**Status:** filed 2026-11-19, not dispatched.

**Severity: S3, not label-blocking** (owner, 2026-11-18, about 19:23 UTC, verbatim "Go with a, merge 1361"; decision H (a), with the card filed not label-blocking under ruling 7).

## Finding

Class (a), S3. `dataframe_divide_operand` (`literal_step.rs:96-99`) marks a DataFrame operand as `CAST(CAST(x AS DOUBLE) AS DOUBLE)`, and `dataframe_divide` and `marked_operand` (`:141-178`) strip any such pair. A user who writes the same pair in SQL or `F.expr` gets the strip too: `SELECT CAST(CAST(d10_2 AS DOUBLE) AS DOUBLE) / CAST(CAST(3 AS DOUBLE) AS DOUBLE)` answers `decimal(14,6)` `2.416667` on the head. Six cells per mode (`attack1 sql/{d10_2,d28_2,d38_10}/dd`, `df/*/expr_dd`), EQUAL → TYPE, silent.

It needs the redundant double cast on both operands, which no Spark-generated plan and no ordinary query writes. A marker that cannot be spelled in SQL (a named marker function, as the suffix literal uses) would close it.

**Main:** `double` `2.4166666666666665`, as Spark answers it. The head answers `decimal(14,6)` `2.416667`.

Up: [map.md](map.md).
