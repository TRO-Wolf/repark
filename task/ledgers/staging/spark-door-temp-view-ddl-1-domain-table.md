# AUD-015 domain table: DDL and write heads x target spellings x temp view x catalog (RePark head 4fd5c6c1 vs Spark 4.1.2)

Measured 2026-11-03. 13 heads x 22 spellings x view present or absent = 572 Spark cells; the same on RePark for the memory and hadoop catalogs = 1144 cells. Spark ran once (hadoop catalog); every RePark cell is a fresh table state.

| class | cells | meaning |
|---|---|---|
| REFUSE_TEXT | 380 | both refuse; different condition or text (S3) |
| EQUAL | 292 | same effect, same answer kind |
| EQUAL_REFUSE | 170 | both refuse with the same condition |
| REPARK_ACTS_SPARK_REFUSES_CHANGED | 168 | RePark changes state where Spark refuses (S1 on a deleting head, S2 otherwise) |
| REPARK_REFUSES_SPARK_ACTS | 54 | RePark refuses where Spark acts (over-refusal, S2) |
| ANSWER_DIFFERS | 50 | same effect, different return shape (S3) |
| REPARK_ACTS_SPARK_REFUSES_NOOP | 30 | RePark answers without a state change where Spark refuses (S2) |

## Classes that change state where Spark refuses

| spelling | Spark | heads (view absent / present) |
|---|---|---|
| `bare` | other | alter_rename / - |
| `block_comment` | other | alter_rename / - |
| `bt` | other | alter_rename / - |
| `cat_bare` | other | alter_rename / alter_rename |
| `cat_bt` | other | alter_rename / alter_rename |
| `cat_dq` | PARSE_SYNTAX_ERRO | alter_add, alter_rename, delete, drop_purge, drop_table, drop_table_ie, insert, insert_ow, truncate, update / alter_add, alter_rename, delete, drop_purge, drop_table, drop_table_ie, insert, insert_ow, truncate, update |
| `cat_sq` | PARSE_SYNTAX_ERRO | alter_add, alter_rename, delete, insert, insert_ow, truncate, update / alter_add, alter_rename, delete, insert, insert_ow, truncate, update |
| `catdq_bare` | PARSE_SYNTAX_ERRO | alter_add, alter_rename, delete, insert, insert_ow, update / alter_add, alter_rename, delete, insert, insert_ow, update |
| `digit` | other | alter_rename / - |
| `dq` | PARSE_SYNTAX_ERRO | alter_add, alter_rename, delete, insert, insert_ow, update / - |
| `fullwidth` | other | - / drop_purge, drop_table, drop_table_ie, drop_view |
| `inner_bt` | other | alter_rename / - |
| `nbsp` | other | alter_rename / - |
| `ns_bare` | other | alter_rename / alter_rename |
| `ns_bt` | other | alter_rename / alter_rename |
| `ns_dq` | PARSE_SYNTAX_ERRO | alter_add, alter_rename, delete, drop_purge, drop_table, drop_table_ie, insert, insert_ow, truncate, update / alter_add, alter_rename, delete, drop_purge, drop_table, drop_table_ie, insert, insert_ow, truncate, update |
| `sq` | PARSE_SYNTAX_ERRO | alter_add, alter_rename / - |
| `unicode` | other | - / drop_purge, drop_table, drop_table_ie, drop_view |

## Classes where RePark refuses and Spark acts

| spelling | heads |
|---|---|
| `block_comment` | drop_purge, drop_table, drop_table_ie, drop_view, truncate, update |
| `digit` | delete, drop_purge, drop_table, drop_table_ie, insert, insert_ow, merge, select, truncate, update |
| `trail_comment` | drop_purge, drop_table, drop_table_ie, drop_view, truncate |

## Reading

1. A double- or single-quoted segment anywhere in a table identifier (`"t"`, `'t'`, `ns."t"`, `cat.ns."t"`, `cat.ns.'t'`, `"cat".ns.t`) is accepted as a name by RePark on every head and refused by Spark with PARSE_SYNTAX_ERROR at that token, on both catalogs, with and without a view. The unqualified `"t"` and `'t'` on DROP and TRUNCATE refuse since fix round 4; every other head and every qualified form does not. This is one dialect rule, not a temp-view property: the fix is the identifier scanner, and the pin is this table.
2. Non-ASCII word characters in a name (`ü1`, fullwidth `ｔ`) reach the temp-view registry: with a view of that name, DROP TABLE, DROP TABLE IF EXISTS, DROP TABLE PURGE and DROP VIEW drop the view where Spark refuses the identifier. The table is held. One rule: the registry accepts only the characters Spark's identifier grammar accepts.
3. `ALTER TABLE ... RENAME TO` with any unquoted spelling: Spark's hadoop catalog refuses RENAME (an unsupported-operation error, not a parse error); RePark's catalogs rename. Catalog capability, not a dialect divergence; recorded, not a finding.
4. Over-refusals: a digit-leading unquoted name (`1t`) is refused by RePark on ten heads where Spark accepts it; a trailing `-- c` or `/* c */` after the target is refused on DROP, TRUNCATE and UPDATE where Spark strips it. No data risk; one dialect rule each.
5. With a temporary view in scope, the only cells where RePark changes state and Spark refuses are classes 1 and 2 above, and both hold identically without a view. The temp-view-specific claim of SPARK-DOOR-TEMP-VIEW-DDL-1 (no head through a view's name reaches the table) has no remaining counterexample in this table.

Cells: `spark-door-temp-view-ddl-1-domain-cells.jsonl` beside this file (one JSON object per cell: catalog, head, spelling, view, statement, class, severity, Spark kind, both answers, both state changes).
