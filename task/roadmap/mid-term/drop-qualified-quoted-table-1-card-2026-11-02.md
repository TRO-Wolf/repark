# Card DROP-QUALIFIED-QUOTED-TABLE-1: `DROP TABLE <cat>.<ns>."t"` drops the table where Spark 4.1.2 raises a parse error

**Status (2026-11-02): folded into IDENT-DIALECT-1 (2026-11-02).** Its classes are rows of [ident-dialect-1-card-2026-11-02.md](ident-dialect-1-card-2026-11-02.md). This card is kept as the record of its finding; nothing here is built or dispatched.
**Closed 2026-11-02 by IDENT-DIALECT-1 at `914e5bbb`:** class 1, quoted segment; ledger clause C-002.

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 round-4 records round (`claude-haiku-5-5`), for the orchestrator, from the Opus 5.5 round-4 verdict's lead L10.

**Status:** open. Data-deleting: the grade and the priority are for the orchestrator to set. Not built; not dispatched; awaiting the owner's charter.

**Why:** A three-part name whose last part is double-quoted, `DROP TABLE <cat>.<ns>."t"`, drops the table on RePark. Spark 4.1.2 answers `PARSE_SYNTAX_ERROR` at the quoted token and drops nothing (verdict 4, Leads, L10). The round-4 verifier measured the statement with and without a temporary view `t` in scope, with the same result in both, since the qualified name names the table. In the round-4 hunt it was the only RePark row that changed a table where Spark held it (verdict 4, Claim 1). SPARK-DOOR-TEMP-VIEW-DDL-1 round 4 pins the one-part quoted target (C-039); whether its refusal reaches the qualified form was not measured on the fix head.

**Fix shape (not built):** measure the qualified quoted spellings of `DROP TABLE`, `DROP VIEW` and `TRUNCATE TABLE` on Spark 4.1.2 and on RePark, on both catalogs, with and without a view, and compare the table before and after. Then pin the measured parse refusal, or let the charter decide. Do not change the DROP route on a guess.

**Retires:** the card when the qualified quoted DROP and TRUNCATE spellings answer as Spark 4.1.2 does, pinned, or the charter declines them.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md) (lead L10, carded in the round-4 records; not that unit's claim).
