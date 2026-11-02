# AUD-015 domain table: DDL and write heads x target spellings x temp view x catalog (RePark head 4fd5c6c1 vs Spark 4.1.2)

Measured 2026-11-02. 13 heads x 22 spellings x view present or absent = 572 Spark cells (hadoop catalog); the same on RePark for the memory and hadoop catalogs = 1144 cells, each from a fresh table state. Severity: S1 when RePark changes state on a deleting head (DROP TABLE, DROP TABLE IF EXISTS, DROP ... PURGE, DROP VIEW, TRUNCATE, DELETE, INSERT OVERWRITE) where Spark refuses; S2 for the other heads, for an answer without a state change where Spark refuses, and for a refusal where Spark acts; S3 for a text or return-shape difference.

| class | cells | meaning |
|---|---|---|
| REFUSE_TEXT | 380 | both refuse; different condition or text |
| EQUAL | 292 | same effect, same answer kind |
| EQUAL_REFUSE | 170 | both refuse with the same condition |
| REPARK_ACTS_SPARK_REFUSES_CHANGED | 168 | RePark changes state where Spark refuses |
| REPARK_REFUSES_SPARK_ACTS | 54 | RePark refuses where Spark acts |
| ANSWER_DIFFERS | 50 | same effect, different return shape |
| REPARK_ACTS_SPARK_REFUSES_NOOP | 30 | RePark answers without a state change where Spark refuses |

Severity over the divergent cells: {'S2': 164, 'S3': 430, 'S1': 88}.

## Classes that change state where Spark refuses

| spelling | Spark | heads (view absent / present) |
|---|---|---|
| `bare` | other:Cannot rename Hadoop tables | alter_rename / - |
| `block_comment` | other:Cannot rename Hadoop tables | alter_rename / - |
| `bt` | other:Cannot rename Hadoop tables | alter_rename / - |
| `cat_bare` | other:Cannot rename Hadoop tables | alter_rename / alter_rename |
| `cat_bt` | other:Cannot rename Hadoop tables | alter_rename / alter_rename |
| `cat_dq` | PARSE_SYNTAX_ERROR | alter_add, alter_rename, delete, drop_purge, drop_table, drop_table_ie, insert, insert_ow, truncate, update / alter_add, alter_rename, delete, drop_purge, drop_table, drop_table_ie, insert, insert_ow, truncate, update |
| `cat_sq` | PARSE_SYNTAX_ERROR | alter_add, alter_rename, delete, insert, insert_ow, truncate, update / alter_add, alter_rename, delete, insert, insert_ow, truncate, update |
| `catdq_bare` | PARSE_SYNTAX_ERROR | alter_add, alter_rename, delete, insert, insert_ow, update / alter_add, alter_rename, delete, insert, insert_ow, update |
| `digit` | other:Cannot rename Hadoop tables | alter_rename / - |
| `dq` | PARSE_SYNTAX_ERROR | alter_add, alter_rename, delete, insert, insert_ow, update / - |
| `fullwidth` | INVALID_IDENTIFIER | - / drop_purge, drop_table, drop_table_ie, drop_view |
| `inner_bt` | other:Cannot rename Hadoop tables | alter_rename / - |
| `nbsp` | other:Cannot rename Hadoop tables | alter_rename / - |
| `ns_bare` | other:Cannot rename Hadoop tables | alter_rename / alter_rename |
| `ns_bt` | other:Cannot rename Hadoop tables | alter_rename / alter_rename |
| `ns_dq` | PARSE_SYNTAX_ERROR | alter_add, alter_rename, delete, drop_purge, drop_table, drop_table_ie, insert, insert_ow, truncate, update / alter_add, alter_rename, delete, drop_purge, drop_table, drop_table_ie, insert, insert_ow, truncate, update |
| `sq` | PARSE_SYNTAX_ERROR | alter_add, alter_rename / - |
| `unicode` | INVALID_IDENTIFIER | - / drop_purge, drop_table, drop_table_ie, drop_view |

## Classes where RePark answers without a state change and Spark refuses

| spelling | Spark | heads |
|---|---|---|
| `cat_dq` | PARSE_SYNTAX_ERROR | select |
| `cat_sq` | PARSE_SYNTAX_ERROR | select |
| `catdq_bare` | PARSE_SYNTAX_ERROR | select |
| `dq` | PARSE_SYNTAX_ERROR | select |
| `fullwidth` | INVALID_IDENTIFIER | select |
| `multi` | PARSE_SYNTAX_ERROR | delete |
| `ns_dq` | PARSE_SYNTAX_ERROR | select |
| `sq` | PARSE_SYNTAX_ERROR | select |
| `unicode` | INVALID_IDENTIFIER | select |

## Classes where RePark refuses and Spark acts

| spelling | heads |
|---|---|
| `block_comment` | drop_purge, drop_table, drop_table_ie, drop_view, truncate, update |
| `digit` | delete, drop_purge, drop_table, drop_table_ie, insert, insert_ow, merge, select, truncate, update |
| `trail_comment` | drop_purge, drop_table, drop_table_ie, drop_view, truncate |

## Reading

1. A double- or single-quoted segment anywhere in a table identifier (`"t"`, `'t'`, `ns."t"`, `cat.ns."t"`, `cat.ns.'t'`, `"cat".ns.t`) is accepted as a name by RePark on every head and refused by Spark with PARSE_SYNTAX_ERROR at that token, on both catalogs, with and without a view. The unqualified `"t"` and `'t'` on DROP and TRUNCATE refuse since fix round 4; every other head and every qualified form does not. One dialect rule, not a temp-view property: the fix is the identifier scanner, and the pin is this table.
2. Non-ASCII word characters in a name (`ü1`, fullwidth `ｔ`): Spark refuses every head with INVALID_IDENTIFIER (the unquoted identifier is invalid and must be back quoted); RePark resolves the name, so with a view of that name DROP TABLE, DROP TABLE IF EXISTS, DROP TABLE PURGE and DROP VIEW drop the view and SELECT reads it. The table is held. One rule: an unquoted identifier accepts only the characters Spark's grammar accepts.
3. `ALTER TABLE ... RENAME TO` on every spelling Spark parses: Spark's hadoop catalog refuses RENAME (`Cannot rename Hadoop tables`, not a parse error); RePark's memory and hadoop catalogs rename. Spark was measured on the hadoop catalog only; this is catalog capability, recorded, not a finding.
4. Over-refusals: a digit-leading unquoted name (`1t`) is refused by RePark on ten heads where Spark accepts it; a trailing `-- c` or `/* c */` after the target is refused on DROP, TRUNCATE and UPDATE where Spark strips it. No data risk; one dialect rule each.
5. A multi-name target on a non-DROP head (`DELETE FROM a, t`): RePark answers without a state change where Spark refuses at the comma; the DROP heads refuse at the comma since fix round 4. Part of class 1's grammar rule.
6. With a temporary view in scope, the only cells where RePark changes state and Spark refuses are classes 1 and 2 above, and both hold identically without a view. The temp-view-specific claim of SPARK-DOOR-TEMP-VIEW-DDL-1 (no head through a view's name reaches the table) has no remaining counterexample in this table.

Cells: the cells file beside this table (one JSON object per cell: catalog, head, spelling, view, statement, class, severity, Spark kind, both answers in full, both state changes).
