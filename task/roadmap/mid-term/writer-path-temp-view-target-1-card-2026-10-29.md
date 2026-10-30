# Card WRITER-PATH-TEMP-VIEW-TARGET-1: the DataFrame writer paths do not consult temporary views, and bare names on the Rust door do not follow USE

**Date:** 2026-10-29. **Filed by:** the SPARK-DOOR-TEMP-VIEW-TARGET-1 clerk round (`claude-haiku-5-5`), from the builder's two leads.

**Status:** open. Grade C, narrowed 2026-10-29 (see the dated paragraph below). Not dispatched; awaiting the owner's charter.

**Regraded 2026-10-29 (verification round 1, F-2; measured on Spark 4.1.2 by the verifier and copied in the fix round's step 0):** the expectation in the Why paragraph above is wrong. Spark refuses only `writeTo` (`append`, `overwritePartitions`, `overwrite` and the option form of `append`) with `_LEGACY_ERROR_TEMP_1011`, and SPARK-DOOR-TEMP-VIEW-TARGET-1's fix round fixes and pins those refusals in the facade. `insertInto` (plain, `overwrite=True` and `mode('overwrite')`), `writeTo(...).createOrReplace()` and `saveAsTable` (append and overwrite) write the table on Spark, and RePark matched them at the base, so they need no route change; they are pinned as controls. What remains is the Rust door's bare names. A bare name does not follow `USE`: on the Rust door alone, with `default_catalog` and `default_schema` set to the table's catalog and namespace, `SELECT` reads the table while every write refuses as a view. That is loud and table-safe. Under `spark.sql.caseSensitive=true` a bare upper-case read misses by construction, because DataFusion folds unquoted references before the temporary-view lookup (expected, not measured). The card narrows to those bare names.

**Why:** The parent unit refuses a SQL write whose one-part target is a temporary view name (INSERT, INSERT OVERWRITE, TRUNCATE TABLE, UPDATE, MERGE INTO and DELETE). The DataFrame writer paths `insertInto`, `writeTo` and `saveAsTable` still resolve their target with `prefer_temp_view=False`, so on those paths a view with the same name as a table may still be shadowed by the table. The builder expects Spark's `insertInto` onto a temporary view to refuse, as its SQL form does; that expectation is not measured.

**Second lead:** on the Rust door alone, a bare name does not follow `USE`. DataFusion's default catalog does not move with `USE`, so a bare name resolves to the temporary-view home. This is pre-existing and the facade compensates for it. The builder saw it while writing the Rust pins of the parent unit: a bare-name control fails there with DataFusion's own text.

**Measured:** for the three writer paths, see the dated regrade above (2026-10-29, measured on Spark 4.1.2 at verification round 1). The Rust-door `USE` behaviour was observed by the builder while writing the parent unit's Rust pins (2026-10-29), not as a Spark comparison.

**Fix (not built):** the Rust door alone resolves a bare or two-part name through DataFusion's `default_catalog`/`default_schema`, not `USE`. Make the Rust door's bare reads and writes follow the session namespace and the temporary-view lookup, or record that the Rust door makes no Spark name-resolution claim.

**Retires:** the card when the Rust door's bare-name reads and writes either match the facade's resolution (pinned per route) or are recorded as out of the Spark claim, with the reason. The `writeTo`, `insertInto` and `saveAsTable` measurement is done (C-012) and needs no further work.

**Relation:** [SPARK-DOOR-TEMP-VIEW-TARGET-1 card](spark-door-temp-view-target-1-card-2026-10-29.md); [SPARK-DOOR-TEMP-VIEW-TARGET-1 ledger](../../ledgers/staging/spark-door-temp-view-target-1-ledger.md), clause C-010.
