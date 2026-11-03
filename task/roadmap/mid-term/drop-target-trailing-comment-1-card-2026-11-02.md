# Card DROP-TARGET-TRAILING-COMMENT-1: `DROP TABLE` with a trailing SQL comment refuses where Spark 4.1.2 drops the object

**Status (2026-11-02): folded into IDENT-DIALECT-1 (2026-11-02).** Its classes are rows of [ident-dialect-1-card-2026-11-02.md](ident-dialect-1-card-2026-11-02.md). This card is kept as the record of its finding; nothing here is built or dispatched.
**Closed 2026-11-02 by IDENT-DIALECT-1 at `914e5bbb`:** class 4, over-refusal (trailing comment); ledger clause C-004.

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 records round (`claude-haiku-5-5`), for the orchestrator, from the round-5 Opus 5.5 delta verdict, lead L13, at `618df399`.

**Status:** open. Lead L13: an over-refusal, both objects are held and no data changes. Carded, not fixed in SPARK-DOOR-TEMP-VIEW-DDL-1: the unit is past its cycle cap (owner rule 2026-11-02). Not built; not dispatched; awaiting the owner's charter.

**Why:** `DROP TABLE t -- c` and `DROP TABLE t/* c */` raise ``invalid table identifier 't -- c': invalid unquoted segment …`` on RePark, on both catalogs, with the view and the table held. Spark 4.1.2 drops the temporary view, or the table when no view of the name is in scope. The facade's DROP name list keeps the trailing comment, and the Rust check added by this unit deliberately lets `--` and `/*` pass, so the segment check refuses. The round-5 verdict found the gap pre-existing by code reading.

**Fix shape (not built):** before any change, write pins for the two statements on both catalogs, with and without the view, against Spark 4.1.2's answer (which object is dropped), and see them fail on the current head. Then make the DROP name list stop at the comment as Spark does, without widening the segment check for any other trailing text.

**Retires:** the card when both statements drop the object Spark drops, on both catalogs, with pins, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md), clauses C-039 and C-040 (the DROP name list); the round-5 verdict, lead L13.
