# Notes: struct comparison step 0 table

## Deviations

1. The generator (`gen_cmp.py`) and driver (`drive_cmp.py`) are new files. The parked `struct_domain_grid.py` and `struct_domain_driver.py` were fetched unmodified into `src/` for reference. Their operator and DataFrame vocabulary was carried over; their fixtures, seat set, label logic and error-class handling were not. The axes of this table differ from the parked grid (no value seats, no `insert_union_all`, a two-field fixture R with reordered structs in several shapes), so reusing the parked cells by key would not have matched any cell here. The parked `struct_domain_cells.jsonl` was not used.
2. Fixture R replaces the wide W table: `id, v, p STRUCT<a,b>, q STRUCT<b,a>, n` (nested), `nq` (nested reordered), `ap`/`aq` (arrays of reordered structs), `mp` (map of struct). Literal right operands carry the type variants (BIGINT, DOUBLE, DECIMAL(12,2), STRING, BOOLEAN) instead of W columns.
3. Re-measure. The first Spark and main passes sorted each result multiset, so the two multi-row string seats (`expr_select`, `selectexpr_select`, 456 cells) had no row identity: `{false, true}` is what both by-name and positional pairing give there. Those two seats were changed to filter `id = 1` first, and only those 456 cells were re-run on both legs. The pass-1 records are kept in `superseded/`. After the change, these cells match the brief's cell 31: Spark `false`, 1.5.6 `true`.
4. Labels. `ORACLE-MISSING` is used for Spark `IllegalArgumentException` (see Q1); the brief's §5 rule fails such cells closed. `SNAPSHOT-DIFF` is added for cells where the read-back is equal but the snapshot count differs. `BOTH-REFUSED` is used whenever both legs refuse, because 1.5.6 refusals carry no bracketed Spark class, so the class cannot be compared (Q3). `REFUSED-DIFFERENT-CLASS` is defined but never fires.
5. Row compare. Rows are sorted by their JSON form before comparison. Multi-row answers are compared as multisets, so a swap is invisible unless the seat is row-identifying. Every other read seat filters to `id = 1` or to a `WHERE` that selects one row.
6. DML compare. Each DML cell gets a fresh Iceberg table. The read-back covers `id, v, p, q, n, nq, ap, aq, mp`. "Changed" is read-back before against after. Snapshot counts come from `<table>.snapshots` on both legs; a failed query is recorded as an error string, and the SNAPSHOT-DIFF label needs both counts to be integers.

## Spelling choices

- Cell 39 (brief §1) is `WHEN MATCHED THEN DELETE` in the brief. This table uses `merge_on`, `merge_matched_and` and `merge_not_matched_and` with `UPDATE SET v = 77`, as the parked grid did. No MERGE DELETE action is measured. Question Q6.
- Cell 16 (`IN (subquery)`) uses a subquery over the whole fixture, `SELECT named_struct('b', 4, 'a', 3) FROM R`, which yields two rows. Spark answered `2`, a positional match, not the brief's `false`. The statement differs from the brief's.
- Cell 22 (join key) uses `x.p = y.q` over a self-join of R. Spark matched `(1,2),(2,1)`, not the brief's "no match". The statement differs from the brief's.
- Typed doors (`df_typed`) spell both operands as `F.expr(<sql operand>)`. `Column` methods carry the operator: `==`, `eqNullSafe`, `<`, `<=`, `isin`, `~isin`, `~eqNullSafe`. The `join_on` typed seat is `w.alias('x').join(w.alias('y'), cond)`.
- String doors: `filter(str)`, `where(str)`, `F.expr` in `select`, and `selectExpr`. All four take the SQL predicate text unchanged.
- `in_sub` is only on the `where` seat (sql door).
- Control `control_union_all_reord` (`UNION ALL` over reordered structs) is recorded as a control because unit C's refusal must stand after this unit. On 1.5.6 it answers by name, so it is `WRONG-ANSWER` here.

## Pruned crosses (kept in the generator; counts after pruning)

Full cross is not built. Totals after pruning: 4389 cells, below the 6000 cap.

| axis | rule | why |
|---|---|---|
| operator | all 10 operators (`=`, `<=>`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `NOT IN`, `NOT <=>`) on the 16 base shapes; only `=`, `<=>`, `<`, `IN` on the 15 type variants and the case shape | size; the type variants test pairing and widening, which the four operators exercise |
| seat x door | SQL door: 12 seats; typed door: 3 seats (`select`, `where`, `join_on`); string door: 4 seats | a DataFrame spelling exists only for these seats |
| DML | sql door only | DataFrame DML is not in the clause (`insert_into` and `append` are value seats, dropped per §4.4) |
| case | `sensitive` and `insensitive` on the one case shape only | the case axis is measured where names differ by case |
| ANSI | `off` on 3 shapes x 3 operators (`=`, `<=>`, `IN`) x 3 seats (`select`, `where`, `delete`) = 27 | the §9 row asks for one ANSI-off row per affected shape |
| probes | 11 probes plus 3 controls | the §4.6 and §6 rows |

Pruned without generation: 6 operators x 15 variant shapes x 19 seat-door combinations = 1710 cells; in_sub outside `where`. Not measured: the native door, the distributed door, Trino.

## Controls (brief §5)

- Same-order controls with `int/int` types (`ctrl_same_lit`, `ctrl_same_col`, `typed_null_same`): 382 cells. Labels: EQUAL 344, SNAPSHOT-DIFF 28, ORACLE-MISSING 10 (the DELETE literal, Q1).
- Same-order typed controls: 571 cells. The `int_string` clash rows are not controls (Spark refuses them): `ACCEPTED-WHERE-SPARK-REFUSED` 57. Widening rows (`int_bigint`, `int_double`, `int_dec12`, same order) are EQUAL.
- Unit C `UNION ALL`: WRONG-ANSWER on 1.5.6 (expected; see the spelling note).
- `unionByName` nested reorder: EQUAL.
- Positional insert control: EQUAL.

## Findings the orchestrator should read first

- Spark refuses a reordered pair when the field types differ, even when the types widen. Each reordered `int/bigint`, `int/double`, `int/dec12` and `int/string` group (76 cells each) is refused by Spark with `DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES` on the select seat, and 1.5.6 answers them by name (`ACCEPTED-WHERE-SPARK-REFUSED`). The reordered `int/boolean` group (76 cells) is refused by both legs. Same-order widening answers. The parked clause section 4.2(1) lists numeric widening as an answer case, which holds only for same-order pairs. The unit must refuse reordered widened pairs with Spark's class (304 cells in the four groups, plus the `int/boolean` group, which both legs already refuse).
- `GROUP BY s` and `SELECT DISTINCT s` over reordered pairs are `WRONG-ANSWER` on 1.5.6. The probe reaches the pairing through `UNION ALL`, which is also wrong on 1.5.6 (control), so it cannot separate grouping from union (Q2).
- `PREPARE` is a parse error on the Spark door (`PARSE_SYNTAX_ERROR`); on 1.5.6 the facade gives `ParseException`. The Spark door does not reach `PREPARE`.
- D6, `INSERT ... BY NAME` with nested reorder (three probes): EQUAL on 1.5.6. Read-back matches Spark.
- A `map(named_struct('a',..,'b',..), v)` key lookup with reordered names: Spark refuses with `DATATYPE_MISMATCH.MAP_FUNCTION_DIFF_TYPES`, 1.5.6 answers (`ACCEPTED-WHERE-SPARK-REFUSED`). Out of clause, card.
- `collect_list(p)` and `row_number() OVER (PARTITION BY p)` are EQUAL on 1.5.6.
- `DISTINCT`/`GROUP BY` on the same-named pair is measured only through union (Q2).

## Questions

Q1. Spark 4.1.2 with Iceberg 1.11.0 throws `IllegalArgumentException: Cannot create expression literal from ...GenericInternalRow` for every `DELETE` whose predicate contains a struct literal. Reproduced on a minimal two-column table, v1 and v2, with `CAST` and with `ConstantFolding` excluded (`probe_delete_literal.py` in this directory). The brief's §1 row 38 says Spark deleted row 2 for that statement. Something in the evaluator's session must differ. Until it is known, 72 cells are `ORACLE-MISSING` (`delete` on `sql`, every shape with a literal operand), plus the `probe_delete_metadata` row. The column-operand form of DELETE does run on Spark (`reord_col` DELETE: Spark no change, 1.5.6 deletes; 13 cells WRONG-WRITE). The metadata-only door is therefore unmeasured on Spark for literal predicates. Needs the owner or the evaluator.
Q2. The GROUP BY/DISTINCT probe rests on `UNION ALL`. Should it be carded or re-measured with a different construct? No unrefused expression builds a reordered pair in one column.
Q3. 1.5.6 refusals have no bracketed Spark class. Exception names: `UnsupportedOperationException` 647, `AnalysisException` 439, `AMBIGUOUS_REFERENCE` 10, `ParseException` 1. The brief's class comparison cannot run on main. Should the main leg map its refusals to Spark classes, or is `BOTH-REFUSED` enough?
Q4. Label additions: `SNAPSHOT-DIFF` (145 cells; Spark commits an empty snapshot on a no-op DML and 1.5.6 does not, read-back equal) and `ORACLE-MISSING` for the Spark internal error. Confirm both.
Q5. Shall the same-order typed clash rows stay in the table as data, or move to a separate control group?
Q6. Cell 39 as a MERGE with `WHEN MATCHED THEN DELETE` is not measured. Add a delete-action variant?
Q7. Cells 16 and 22 differ from the brief's spellings (two-row subquery; self-join with `x.p = y.q`). Their Spark results also differ from the brief (16: row 2; 22: two matches). Accept these spellings, or pin the brief's exact statements?

## Pass 2 (coordinator follow-up)

Appended records carry `"pass": 2`. Pass-2 records are in `raw/spark.jsonl` and `raw/main.jsonl`; the new cells are the last four lines of `cells.jsonl`. Scripts: `pass2_cmp.py` (new cells and derived twins) and `render2.py` (re-renders `table.md` and `groups.txt`). `drive_cmp.py` is unchanged.

Answers recorded:
- Q1: the coordinator reproduced the struct-literal DELETE failure on copy-on-write, merge-on-read, and `id > 0 AND <struct pred>`. The 72 cells stay `ORACLE-MISSING` with the derived column added below.
- Q2: the GROUP BY and DISTINCT probes are labelled "out of clause, confounded by UNION ALL". Not carded.
- Q3: class comparison is accepted as out of scope on 1.5.6; it matters on the fixed head.
- Q4: SNAPSHOT-DIFF is the empty-DML guard's declared divergence (ruling A, #1277). ORACLE-MISSING confirmed.
- Q5: same-order `int_string` and `int_boolean` rows moved out of the control group into shape `clash_same_order` (data, typed family). Re-rendered only; not re-run.
- Q6: MERGE with a DELETE action added (P2-39 and its column-key twin P2-39T).
- Q7: own spellings kept; Fable's cells 16 and 22 added beside them (P2-16, P2-22).

Derived oracle (item 1): for each of the 72 `ORACLE-MISSING` DELETE cells, a fresh copy of fixture R was created on Spark, the twin `SELECT id, v, p, q, n, nq, ap, aq, mp FROM <table> WHERE <same predicate>` was run with the cell's case setting, and the expected after-state was the fixture minus exactly the selected rows (multiset). Labels: `DERIVED-EQUAL` / `DERIVED-WRONG-WRITE` (main's post-state differs) / `DERIVED-REFUSED` (main refused) / `DERIVED-SPARK-REFUSES` (the twin refused; none did). Results: DERIVED-EQUAL 14, DERIVED-WRONG-WRITE 40, DERIVED-REFUSED 18.

Fable's cells, reconstructed: brief §1 gives no verbatim statement for cell 16 or cell 22 (cell 16 is described in the §1 text; cell 22 is `JOIN ... ON l.s = r.s` with reordered declarations). The two statements were built to the table's own description, on the brief's fixture `t(id, s STRUCT<a,b>)` with a second table `R(id, s STRUCT<b,a>)` carrying the same values declared in the other order:
- P2-16: `SELECT l.id FROM L l WHERE l.s IN (SELECT r.s FROM R r)`.
- P2-22: `SELECT l.id AS lid, r.id AS rid FROM L l JOIN R r ON l.s = r.s`.
- P2-39 (Fable's cell 39 exactly, on the brief fixture `T1 = t(id, s STRUCT<a,b>)`, rows (1,{3,4}), (2,{4,3})): `MERGE INTO T1 t USING (SELECT named_struct('b',3,'a',4) AS k) u ON t.s = u.k WHEN MATCHED THEN DELETE`.
- P2-39T (column twin): the same MERGE with `USING (SELECT k FROM U)`, where U holds one row `k = named_struct('b',3,'a',4)`.

Pass-2 outcomes that differ from the brief's claims (report, do not force):
- P2-39 (Spark): deleted id 1, not id 2. Positionally k = (3,4) matches s = (a3,b4), so Spark's answer is row 1. The brief says Spark matches `{4,3}` only. P2-39 (1.5.6): deleted id 2 (by name); the brief says 1.5.6 deletes every row. Neither matches the brief.
- P2-39T: identical to P2-39 on both legs.
- P2-16: both legs return both rows (EQUAL). Positionally each left row matches a right row, so Spark's answer is not the brief's `false`.
- P2-22: Spark pairs (1,2),(2,1); 1.5.6 pairs (1,1),(2,2). The brief says no match on Spark; Spark gives two matches.

Clash rows: as Q5 says, the int/string and int/boolean same-order rows are no longer controls. Same-order controls: `same_order_typed` now EQUAL 367, SNAPSHOT-DIFF 52 (widening rows only).
