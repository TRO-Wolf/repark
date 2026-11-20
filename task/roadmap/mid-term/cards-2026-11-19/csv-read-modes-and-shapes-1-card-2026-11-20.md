# Card CSV-READ-MODES-AND-SHAPES-1: a CSV read refuses `DROPMALFORMED` and `FAILFAST`, and its header names, short rows and multi-file widths disagree with Spark

**Date:** 2026-11-20. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-20 (the owner's delegation, about 00:22 UTC). **Source:** the Fable reply of 2026-11-20 in the agents' lessons folder (file `Fable reply — CSV-STRING-FIELD-TEXT-1 step 0 2026-11-20.md`), §2, §4 and §11, kept outside the repository; the cells are in the [domain table §2, §3 and §6.3](../../../wo/csv-string-field-text-1/domain-table.md) and the [sketch §8](../../../wo/csv-string-field-text-1/sketch.md).

**Status:** filed 2026-11-20; not dispatched.

**Severity: S3, loud.** **Not label-blocking.** This is Fable's guidance under the owner's delegation, not an owner ruling.

## Finding

**[M] The two modes.** RePark refuses both modes before it reads: `reader option mode='DROPMALFORMED' is not supported by repark yet` (688 cells) and `mode='FAILFAST'` (688 cells, 420 of them BOTHERR, where Spark's `FAILFAST` also raises). The mode set is 1,376 cells: 956 RAISE and 420 BOTHERR on main. On Spark, `DROPMALFORMED` drops the row (`abc` in an `INT` field answers no row), and `FAILFAST` raises `FAILED_READ_FILE.NO_HINT` on the first misfit.

**[M] Header names across files.** Two files, one with headers `x,k` (1 row) and one with `k,x` (2 rows), read with no schema: Spark answers the names `k, x`, taken from the larger file, with positional values. Main raises `Fail to merge schema`. This shows only when files of one read disagree on their header (Fable §2). Do not pin this cell as EQUAL on a local path, because the listing is in readdir order.

**[Fable] Short rows and multi-file widths.** The sketch's §8 row names these as "the critic's cells", which the step 0 table does not hold. Fable §11 item 3: every measured file has one to three rows, and the 1,000-record inference window is unmeasured, so the table understates the loud half on real files.

**Not carded here.** The late misfit (a 1,202-row file whose rows 1,201 and 1,202 are `abc` and `1.5`: main raises for `STRING`, `INT` and the default read, and Spark answers all three) is pinned by the wiring unit (Fable §2). The header-off wide case (a schema of 3 fields over a 2-column file, which raises `reader schema has 3 fields but source has 2 columns`) is the wiring unit's to fold (Fable §8). If its refusal sits elsewhere, it is carded in this card, S3, with the raise pinned.

## First step

Measure the short-row and multi-file width cells on Spark 4.1.2 and on main, which the step 0 table does not hold (sketch §8).

Up: [map.md](map.md).
