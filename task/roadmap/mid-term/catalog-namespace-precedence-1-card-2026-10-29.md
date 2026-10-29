# Card CATALOG-NAMESPACE-PRECEDENCE-1: a two-part name whose first part is also a catalog name resolves to the namespace on the Spark door

**Date:** 2026-10-29. **Filed by:** the second-audit records round (`claude-haiku-5-5`), from the second independent audit of the format-v2 label, finding AUD-009 ([the report](../../../docs/audits/iceberg-v2-label-independent-reaudit-2026-10-29.md), section 3).

**Status:** open. Grade C (clerk). Not dispatched; no build is owed until the owner charters it.

**Why:** When two catalogs are registered and the current namespace sits inside one of them, a two-part name whose first part is also the name of the other catalog resolves to a namespace table on RePark. Reads and DML agree with each other, so RePark is self-consistent, but Spark resolves the first part as a catalog and refuses the statement with `TABLE_OR_VIEW_NOT_FOUND`. The audit rates it Low because RePark's reads and writes agree with each other, but a write that Spark refuses is still accepted and changes a table. The fault predates the second audit: it is identical on the base `321c6095`.

**Site:** the Spark door's qualification of two-part names, on the read and write routes alike. The audit located the behaviour by probe and did not name the function, so the charter's first step is to name it from the code.

**Fix:** A two-part name whose first part is a registered catalog resolves the catalog first, as Spark does, and a table that is not found under it refuses with `TABLE_OR_VIEW_NOT_FOUND`, on the SELECT, INSERT, DELETE and the other write routes alike, with the table on the other side unchanged. The unit adds one pin per route, shaped as the audit's name-collision probe run through the Spark door, with the expected values taken from Spark. The unit may share its resolver with SPARK-DOOR-TEMP-VIEW-TARGET-1 if the charter finds that both name the same function; the two cards then merge into one unit.

**Measured (copied from the second audit; the probe registers two catalogs on both engines, with the current namespace inside the first and a two-part name whose first part is the second catalog's name):** `SELECT` of that two-part name: RePark returns three rows, Spark refuses with `TABLE_OR_VIEW_NOT_FOUND`. `INSERT` and `DELETE` through the same name: RePark writes, Spark refuses. The three-part name of the same table reads ids 1, 2, 3 on Spark, and on RePark after the writes reads ids 2, 3, 4. The base `321c6095` gives the same results.

**Retires:** the card when the two-part name resolves as Spark resolves it on every route, each route has its pin, and the targeted re-audit (or the next full audit) confirms the probe. The label ledger's AUD-009 row (`ice-v2-label-1` C-066) then closes by citation.

**Relation:** the second audit's AUD-009 (section 3); the parity registry's DML-2 dated line of 2026-10-29, which carries the Spark-door resolution finding; [SPARK-DOOR-TEMP-VIEW-TARGET-1](spark-door-temp-view-target-1-card-2026-10-29.md), the same family of name-resolution findings on the write routes.
