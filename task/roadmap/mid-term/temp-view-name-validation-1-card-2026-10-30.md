# Card TEMP-VIEW-NAME-VALIDATION-1: the facade's unquoted-name grammar and its temporary-view name check differ from Spark 4.1.2's parser

**Date:** 2026-10-30. **Filed by:** the SPARK-DOOR-TEMP-VIEW-TARGET-1 records round 3 (`claude-haiku-5-5`), from the Opus 5.5 verifier's round-3 findings F3-2 and F3-3 on `bfce6032`.

**Status:** open. Grade C. Not built; not dispatched; awaiting the owner's charter. Each item is loud or harmless: none changes a table.

**Why:** Two name-level differences between RePark's Python facade and Spark 4.1.2, both pre-existing and outside the write-target fix of [SPARK-DOOR-TEMP-VIEW-TARGET-1](spark-door-temp-view-target-1-card-2026-10-29.md). Both were measured on the verifier's probe and are recorded as clauses C-026 and C-027 of the unit's ledger.

**Item 1: unquoted digit-led names are refused (F3-2, S2, loud).** Spark 4.1.2 accepts `1t` and `1T` as unquoted identifiers. The facade's identifier grammar refuses them with "invalid table identifier '1t': invalid unquoted segment '1t'". The refusal is `_is_plain_ident` in `python/repark/src/repark/spark/sql_relations.py` (line 792), unchanged since 2026-08-27. It covers `tableExists`, `table`, `read.table`, `getTable`, `listColumns`, the cache APIs (`cacheTable`, `isCached`, `uncacheTable`) and `writeTo`. Three further rows differ:

- under `spark.sql.caseSensitive=true`, an unquoted `1t` in SQL with no view of that key reads "table 'datafusion.public.1t' not found": it does not qualify to the `USE` namespace, where Spark reads the table;
- the six write routes and `writeTo` through the same name are refused, where Spark writes the table (28 rows);
- a digit-led write through a view gets DataFusion's text ("Insert into not implemented" or "DELETE operation on table"), where Spark answers `EXPECT_TABLE_NOT_VIEW`.

No table changes in any row. The unit's design note that a digit-led segment reaches core holds for backticked names only.

**Item 2: the temporary-view name check is a character test (F3-3, S3, records and class).** `_register_temp_view` in `python/repark/src/repark/spark/catalog_surface.py` admits an unquoted, undotted name by `[A-Za-z0-9_]+`. That is neither Spark's identifier parser nor its literal rule:

- RePark registers `1d`, `1D`, `1L`, `1S`, `1Y`, `1BD`, `1F`, `123`, `1`, `1e3`, `1E3`, `"t"` and `` `t ``, which Spark refuses with `_LEGACY_ERROR_TEMP_1321`;
- RePark refuses `t--x` and `t/*x*/`, which Spark accepts as `t`.

Separately, `tableExists`, `table`, `read.table`, `getTable` and `listColumns` on `'"t"'` answer on RePark, where Spark raises `PARSE_SYNTAX_ERROR`. Both rules gave the same result for every probed name. The unit's C-021 is amended to bound the names it pins.

**Fix shape (not built).** Spark 4.1.2's own parser is the oracle. Measure each probed name on Spark 4.1.2 before any change; then pin each item on the measured outcome and change the grammar or the check to match. Items 1 and 2 both decide what an unquoted name is, so one charter should decide them together.

**Retires:** the card when each item matches Spark 4.1.2's parser on the probe and is pinned, or the charter declines an item (then the item closes as declined in this card).

**Relation:** [SPARK-DOOR-TEMP-VIEW-TARGET-1 card](spark-door-temp-view-target-1-card-2026-10-29.md) and its [unit ledger](../../ledgers/completed/spark-door-temp-view-target-1-ledger.md), clauses C-026 and C-027; [TEMP-VIEW-SURFACE-LEADS-1](temp-view-surface-leads-1-card-2026-10-29.md), whose Lead 2 and Lead 3 touch the same name check and the same unquoted grammar.
