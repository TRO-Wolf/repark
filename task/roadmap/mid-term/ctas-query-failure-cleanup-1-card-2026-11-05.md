# Card CTAS-QUERY-FAILURE-CLEANUP-1: a CTAS whose query fails leaves metadata that a path read takes for an empty committed table

**Date:** 2026-11-05. **Filed by:** the records clerk (`claude-haiku-5-5`), from the format-v2 label re-audit ([report](../../../docs/audits/iceberg-v2-label-corpus-audit-2026-11-05.md), finding AUD-025).

**Status:** filed, awaiting the owner's dispatch decision; grade not set. Severity S2, a blocker of the format-v2 label (ledger [ice-v2-label-1-ledger.md](../../ledgers/staging/ice-v2-label-1-ledger.md) C-101).

**Why:** On the `type=hadoop` catalog, a `CREATE TABLE ... AS SELECT` into a new table whose query fails at execution leaves `<table>/metadata/00000-<uuid>.metadata.json`. Spark leaves no table directory, and its path read of that location refuses with `TABLE_OR_VIEW_NOT_FOUND`. RePark's path read answers an empty table with the CTAS schema on both doors, because the file is reached through the leading-integer listing of C-008. A reader therefore takes a table that never committed for a committed one. The new session's catalog door does not see it. Family 5's staged-publish claim covers failures at publish time; this failure happens before publish. The C-008 premise, that no Java or RePark writer produces such names, does not hold for this writer.

**Proposed fix:** extend the cleanup of [STAGED-PUBLISH-CLEANUP-1](staged-publish-cleanup-1-card-2026-11-04.md), which removes the files of a rejected staged publish, to the query-failure path, so that no file of a failed new-table CTAS remains.

**Close:** a pin in which the failing-query CTAS leaves no metadata on either door and a path read refuses as Spark does, then a targeted re-audit of AUD-025.
