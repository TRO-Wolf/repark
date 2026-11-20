# Card CSV-SQL-PATH-DOOR-1: ``SELECT * FROM csv.`path` `` and `CREATE TEMPORARY VIEW … USING csv` raise on RePark where Spark answers

**Date:** 2026-11-20. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-20 (the owner's delegation, about 00:22 UTC). **Source:** the Fable reply of 2026-11-20 in the agents' lessons folder (file `Fable reply — CSV-STRING-FIELD-TEXT-1 step 0 2026-11-20.md`), §6, kept outside the repository; the cells are in the [domain table §3 and §6.4](../../../wo/csv-string-field-text-1/domain-table.md) and the [sketch §7](../../../wo/csv-string-field-text-1/sketch.md) (Q6).

**Status:** filed 2026-11-20; not dispatched.

**Severity: S3, loud.** Fable §6: "Both raise at planning or parse; no value is wrong." **Not label-blocking.** This is Fable's guidance under the owner's delegation, not an owner ruling.

## Finding

**[M] The path form.** ``SELECT * FROM csv.`path` `` with no options (header off): 76 cells, all RAISE on main, with `table 'datafusion.csv./…' not found`. RePark has no route for the form. The domain table counts these as the SQL-door planning errors.

**[M] The `USING csv` form.** `CREATE OR REPLACE TEMPORARY VIEW … USING csv OPTIONS (path …)` answers `_c0, _c1` text rows on Spark, and main raises at parse. With a column list `(x STRING, k STRING)` and `header 'true'`, Spark answers (`007`, `abc`), and main raises at parse. These are the two `USING csv` cells.

## First step

None set by Fable.

Up: [map.md](map.md).
