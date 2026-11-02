# Card IDENT-DIALECT-1: quoted identifier segments, non-ASCII word characters and over-refusals on the Spark door

**Date:** 2026-11-03. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 closure records round (`claude-haiku-5-5`), for the orchestrator, from the measured domain table at `4fd5c6c1` ([spark-door-temp-view-ddl-1-domain-table.md](../../ledgers/staging/spark-door-temp-view-ddl-1-domain-table.md); cells in [spark-door-temp-view-ddl-1-domain-cells.jsonl](../../ledgers/staging/spark-door-temp-view-ddl-1-domain-cells.jsonl)).

**Unit:** IDENT-DIALECT-1. **Grade:** A. **Priority:** P1 for the v2 label, because it carries S1 cells (class 1 on the deleting heads with no view in scope).

**Status:** open. Carded, not built, not dispatched; awaiting the owner's charter.

**Why:** These are dialect rules of the identifier grammar, not properties of a temporary view. SPARK-DOOR-TEMP-VIEW-DDL-1 closes on the table for the view-scoped clause; the classes below sit outside that clause and are carded with their measured cells.

**Clause (the table's cells for these spellings, on `mem` and `hd`):**
- **Class 1, quoted identifier segment.** A double- or single-quoted segment anywhere in a table name. RePark accepts the name and changes the table where Spark 4.1.2 refuses with `PARSE_SYNTAX_ERROR` at the quoted token, and the table is held on Spark. The changed cells, by spelling and head:
  - `dq` (no view in scope only): alter_add, alter_rename, delete, insert, insert_ow, update.
  - `sq` (no view in scope only): alter_add, alter_rename.
  - `catdq_bare` (both): alter_add, alter_rename, delete, insert, insert_ow, update.
  - `cat_sq` (both): alter_add, alter_rename, delete, insert, insert_ow, truncate, update.
  - `cat_dq` and `ns_dq` (both): alter_add, alter_rename, delete, drop_purge, drop_table, drop_table_ie, insert, insert_ow, truncate, update.
  - Severity: S1 on the deleting heads (DELETE, UPDATE, INSERT OVERWRITE, TRUNCATE, DROP). The table's severity field reads S1 on every changed cell, and its legend says S2 on the other heads; the ledger's closure section records this for the orchestrator.
- **Class 2, non-ASCII word characters.** Unicode letters and digits (`ü1`, `١`, `t²`, fullwidth `ｔ`; spellings `unicode`, `fullwidth`) reach the temporary-view registry: `DROP TABLE`, `DROP TABLE IF EXISTS`, `DROP TABLE PURGE` and `DROP VIEW` drop the view where Spark refuses the identifier, and the table is held. The rule is that the registry accepts only the characters Spark's identifier grammar accepts.
- **Class 4, over-refusals.** A digit-leading unquoted name (`1t`, spelling `digit`) is refused on ten heads where Spark accepts it; a trailing `-- c` or `/* c */` after the target (spellings `trail_comment`, `block_comment`) is refused on DROP, TRUNCATE and UPDATE where Spark strips it. No data changes; both objects are held.
- **Unclassed cells, ruling pending.** The spelling `multi` on DELETE (`DELETE FROM a, t WHERE id = 1`, four cells on both catalogs, view present or absent): RePark answers ok with no state change where Spark refuses at `,`. The table's readings do not name these cells; the orchestrator rules their class.

**Pin:** one table-driven test per door (the facade's DDL and write doors, and the Rust Spark-door parser) that reads the cells of the domain-cells file for these classes and asserts each answer against Spark's recorded answer. The builder writes the pin red before the fix.

**Outside this card:** the RENAME capability rows (reading 3 of the table, recorded as not a finding); [ALTER-VIEW-RENAME-OVER-TABLE-1](alter-view-rename-over-table-1-card-2026-11-02.md) (lead L14); [BARE-READ-DEFAULT-NAMESPACE-1](bare-read-default-namespace-1-card-2026-11-02.md).

**Folded cards (status line added 2026-11-03, cards kept):** [QUOTED-TARGET-NO-VIEW-1](quoted-target-no-view-1-card-2026-11-02.md) (round 5, F17, class 1 with no view in scope); [UNICODE-WORD-IDENTIFIER-1](unicode-word-identifier-1-card-2026-11-02.md) (round 5, F18, class 2); [DROP-TARGET-TRAILING-COMMENT-1](drop-target-trailing-comment-1-card-2026-11-02.md) (lead L13, class 4); [DROP-QUALIFIED-QUOTED-TABLE-1](drop-qualified-quoted-table-1-card-2026-11-02.md) (lead L10, class 1, data-deleting). The Muse MAX round-5 findings R5F1 and R5F2 (qualified quoted DROP and TRUNCATE that delete where Spark refuses) are cells of class 1.
