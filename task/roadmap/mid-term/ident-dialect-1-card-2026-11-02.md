# Card IDENT-DIALECT-1: quoted identifier segments, non-ASCII word characters and over-refusals on the Spark door

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 closure records round (`claude-haiku-5-5`), for the orchestrator, from the measured domain table at `4fd5c6c1` ([spark-door-temp-view-ddl-1-domain-table.md](../../ledgers/staging/spark-door-temp-view-ddl-1-domain-table.md); cells in [spark-door-temp-view-ddl-1-domain-cells.jsonl](../../ledgers/staging/spark-door-temp-view-ddl-1-domain-cells.jsonl)).

**Unit:** IDENT-DIALECT-1. **Grade:** A. **Priority:** P1 for the v2 label, because it carries S1 cells (class 1 and class 2 on the deleting heads).

**Status:** closed 2026-11-02 at `914e5bbb` (the IDENT-DIALECT-1 PR). Built under the owner ruling of 2026-11-02 (one round; the owner's words today: lets proceed; merge both when green and start IDENT-DIALECT-1), with the one-round rule applied as one build, one critique, one delta fix and one delta re-check.

**Why:** These are dialect rules of the identifier grammar, not properties of a temporary view. SPARK-DOOR-TEMP-VIEW-DDL-1 closes on the table for the view-scoped clause; the classes below sit outside that clause and are carded with their measured cells.

**Clause (the table's cells for these spellings, on `mem` and `hd`):**
- **Class 1, quoted identifier segment.** A double- or single-quoted segment anywhere in a table name. RePark accepts the name and changes the table where Spark 4.1.2 refuses with `PARSE_SYNTAX_ERROR` at the quoted token; for a single quote after a qualifier (`hd.ns.'t'`), Spark names the preceding dot. The changed cells, by spelling and head:
  - `dq` (no view in scope only): alter_add, alter_rename, delete, insert, insert_ow, update.
  - `sq` (no view in scope only): alter_add, alter_rename.
  - `catdq_bare` (both): alter_add, alter_rename, delete, insert, insert_ow, update.
  - `cat_sq` (both): alter_add, alter_rename, delete, insert, insert_ow, truncate, update.
  - `cat_dq` and `ns_dq` (both): alter_add, alter_rename, delete, drop_purge, drop_table, drop_table_ie, insert, insert_ow, truncate, update.
  - Multi-name target, no state change: `DELETE FROM a, t WHERE id = 1` (spelling `multi`, four cells: both catalogs, view present or absent). RePark answers ok where Spark refuses at the comma. This is the grammar rule of reading 5 of the table; the DROP heads already refuse at the comma.
  - Severity: S1 on the deleting heads (DELETE, INSERT OVERWRITE, TRUNCATE, DROP), S2 on ALTER, INSERT INTO and UPDATE. The table's severity field and its legend agree after the regeneration of 2026-11-02.
- **Class 2, non-ASCII word characters.** Unicode letters and digits (`ü1`, `١`, `t²`, fullwidth `ｔ`; spellings `unicode` and `fullwidth`). Spark refuses every head with `INVALID_IDENTIFIER`; RePark resolves the name. With a view of that name in scope, `DROP TABLE`, `DROP TABLE IF EXISTS`, `DROP TABLE PURGE` and `DROP VIEW` drop the view (S1, 16 cells), and the table is held. `SELECT` reads the view (S2, no state change). The rule: the registry accepts only the characters Spark's identifier grammar accepts.
- **Class 4, over-refusals.** A digit-leading unquoted name (`1t`, spelling `digit`) is refused on ten heads where Spark accepts it; a trailing `-- c` or `/* c */` after the target (spellings `trail_comment`, `block_comment`) is refused on DROP, TRUNCATE and UPDATE where Spark strips it. No data changes; both objects are held (S2).

**Pin:** one table-driven test per door (the facade's DDL and write doors, and the Rust Spark-door parser) that reads the cells of the domain-cells file for these classes and asserts each answer against Spark's recorded answer. The builder writes the pin red before the fix.

**Outside this card:** the RENAME capability rows (reading 3 of the table, recorded as catalog capability and not a finding); [ALTER-VIEW-RENAME-OVER-TABLE-1](alter-view-rename-over-table-1-card-2026-11-02.md) (lead L14); [BARE-READ-DEFAULT-NAMESPACE-1](bare-read-default-namespace-1-card-2026-11-02.md).

**Folded cards (status line added 2026-11-02, cards kept):** [QUOTED-TARGET-NO-VIEW-1](quoted-target-no-view-1-card-2026-11-02.md) (round 5, F17, class 1 with no view in scope); [UNICODE-WORD-IDENTIFIER-1](unicode-word-identifier-1-card-2026-11-02.md) (round 5, F18, class 2); [DROP-TARGET-TRAILING-COMMENT-1](drop-target-trailing-comment-1-card-2026-11-02.md) (lead L13, class 4); [DROP-QUALIFIED-QUOTED-TABLE-1](drop-qualified-quoted-table-1-card-2026-11-02.md) (lead L10, class 1, data-deleting). The Muse MAX round-5 findings R5F1 and R5F2 (qualified quoted DROP and TRUNCATE that delete where Spark refuses) are cells of class 1.


## Closure (2026-11-02, at `914e5bbb`)

- **Ledger:** [ident-dialect-1-ledger.md](../../ledgers/completed/ident-dialect-1-ledger.md), clauses C-001 to C-012; C-012 is OPEN.
- **Ruling:** a table identifier follows Spark's grammar on every head and both doors (the Spark door through the facade and the Rust router door). A double- or single-quoted segment is PARSE_SYNTAX_ERROR at the token; a second target after a comma is PARSE_SYNTAX_ERROR at the comma; a trailing comment is stripped; a non-ASCII unquoted identifier is INVALID_IDENTIFIER (SQLSTATE 42602). After the delta, every relation after a top-level comma in a FROM clause reads through the same grammar.
- **Counts before the fix (red):** class 1, quoted segment, 252 of 312; class 2, non-ASCII unquoted, 104 of 104; class 4, over-refusals, 54 of 156; class 5, multi-name target, 32 of 52; controls, 0 of 520 moved. Delta: 16 of 28 red at `c3c2a76b`, 28 of 28 green at `914e5bbb`.
- **Verification:** round 1, Opus 5.5 FAIL on F1 (S1, a relation after a comma escapes the grammar), F2 (S3, the JOIN arm unpinned) and F3 (S3, a re-pin drops the reopen check); Muse MAX PASS with no findings. Delta re-check, Opus 5.5 FAIL on G1 (S1, a relation after a VALUES row list or a LATERAL subquery escapes the comma grammar); Muse MAX PASS with no findings. Round 1 findings are closed in the delta.
- **Owner ruling A (2026-11-02):** the round cap is exhausted, so G1 is ruled out of this unit's clause, and the unit ships at `914e5bbb` on the union of PASS over the ruled clause. G1 is carded as [IDENT-DIALECT-1B](ident-dialect-1b-card-2026-11-02.md) (grade A, P1 for the v2 label).
- **Moved pins:** eight AUD-015 pins now assert the measured answer: `ALTER TABLE "t"` through a view name is PARSE_SYNTAX_ERROR at `'"t"'`, and two `dml.rs` quoted DELETE targets and the `metadata_tables.rs` `$` spelling are back-quoted; body hashes are re-pinned.
- **Residual:** 18 unit cells answer as their head's back-quoted control, with Spark's state change. The residual is the head's own divergence, already recorded in the AUD-015 closure of SPARK-DOOR-TEMP-VIEW-DDL-1 (its ledger, [spark-door-temp-view-ddl-1-ledger.md](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md) on the closure branch) (its rows C-039 and C-040). Recorded as control, not as open cells.
- **Scope:** the ANSI-dialect `repark.sql()` door keeps standard delimited identifiers (a scope note, no card). Identifier positions outside the head target and the FROM, JOIN and comma relations are carded as [IDENT-DIALECT-2](ident-dialect-2-card-2026-11-02.md).
- **Folded cards** are closed by their own status lines: `drop-qualified-quoted-table-1`, `quoted-target-no-view-1`, `unicode-word-identifier-1` and `drop-target-trailing-comment-1`.
