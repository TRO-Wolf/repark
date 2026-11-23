# Card EXTRACT-FIELD-ALIASES-1: `extract` and `date_part` refuse or leave unparsed `YEAROFWEEK`, `DAYOFWEEK`, `DAYOFWEEK_ISO`, and the aliases `H` and `HRS`, on the SQL doors

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling of 2026-11-19 (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default for the neighbours' severities). **Source:** [domain table](../../../wo/string-hour-minute-second-1/domain-table.md) §6, row N6 (the step 0 of STRING-HOUR-MINUTE-SECOND-1, merged as #1369, `81ae984e`).

**Status:** filed 2026-11-19; **FIXED 2026-11-23 on branch `fix/sql-explode-extract-fields-1`** (not merged), with [SQL-EXPLODE-1](../cards-2026-11-23/sql-explode-1-card-2026-11-23.md); measured scope, fix and residue in the [registry row](../../../../docs/spark-sql-iceberg-parity.md) EXTRACT-FIELD-ALIASES-1.

**Severity: S2, not label-blocking** (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default, "the loud ones (N2, N5, N6, N9, N10) S2").

## Finding

- **Loud.** `extract` and `date_part` of `YEAROFWEEK`, `DAYOFWEEK`, `DAYOFWEEK_ISO`, and the aliases `H` and `HRS`, on the SQL doors, are refused or unparsed. 48 to 64 cells per text (N6).

## Fixed (2026-11-23, branch `fix/sql-explode-extract-fields-1`)

Measured on main `efa72bb7`: `date_part('<field>', x)` already answered every spelling on the SQL doors;
`extract(<field> FROM x)` failed to parse for 20 Spark spellings and raised for `DAYOFWEEK`. The front-door
canonicalizer (`crates/repark-spark/src/spark_rewrites/extract_field.rs`) now spells those fields as
`date_part('<field>', x)`. **Named addition:** `extract(DOW FROM x)` parsed and answered from Sunday 0 (`3` for a
Wednesday, Spark `4`), a silent wrong value at the same seat; it is in the rewritten set and answers as Spark.
Pins: `python/repark/tests/timestamps/test_extract_field_aliases_1.py`. Residue: the registry row.

## First step

None is given in the source.

Up: [map.md](map.md).
