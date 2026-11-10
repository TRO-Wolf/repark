# Card STRUCT-COMPARE-BY-POSITION-1: a struct comparison pairs fields by name where Spark pairs them by position

**Date:** 2026-11-09. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Fable evaluation of 2026-11-09 (sections 1, 2 and 6) and the Fable brief of 2026-11-09 (sections 0 to 3).

**Status:** filed 2026-11-09, ruled 2026-11-09 (owner: defaults): carve from #1267 onto a branch from main; runs before the v2 label as a fix unit; target v1.5.7; S1 with DML reach; blocks the v2 label. Not dispatched. Ledger row C-116 in [ice-v2-label-1-ledger.md](../../ledgers/staging/ice-v2-label-1-ledger.md) stays OPEN until this unit merges and the narrow re-audit passes. Pre-existing on 1.5.6 and head; earlier wheels not measured.

**Why:** When a comparison operator, an `IN` list or a join key compares a struct with a struct whose fields carry the same names in a different order, RePark pairs the fields by name. Spark pairs them by position. Measured on Spark 4.1.2 (ANSI on), on RePark head and on the published 1.5.6 wheel, with table `t(id INT, s STRUCT<a: INT, b: INT>)` and rows `(1, {a:3,b:4})` and `(2, {a:4,b:3})`, under the predicate `s = named_struct('b', 4, 'a', 3)`:

- `SELECT named_struct('a',1,'b',2) = named_struct('b',2,'a',1)` answers `true` on RePark and `false` on Spark;
- the `WHERE` built from that predicate selects row 1 on RePark and row 2 on Spark;
- `DELETE` with that predicate deleted row 1 on RePark, on the 1.5.6 wheel too (shipped);
- `UPDATE ... SET id = 99` with that predicate updated row 1 on RePark;
- `MERGE INTO t USING (SELECT named_struct('b',3,'a',4) AS k) u ON t.s = u.k WHEN MATCHED THEN DELETE` deleted every row on RePark where Spark deletes the row with `{4,3}` only.

The same pairing reaches the DataFrame typed door (`==`, `isin`) and the DataFrame string doors (`filter`, `selectExpr`, `F.expr`), and it is the cause of the `<` result. The pairing comes from pinned upstream DataFusion code, so it has been in every RePark release on DataFusion 54.1. Step 3.2 of the master plan was a refusal unit under R2a Option B with no severity reading and no release gate; it sat behind unit C and was never built. The lesson is recorded in `task/lessons.md` (2026-11-09).

Re-measured by the orchestrator 2026-11-09 on repark 1.5.6 with the facade `spark.sql` over a copy-on-write v2 table: the `=` answers `true`, the `WHERE` selects row 1, the `DELETE` deletes row 1, and the `UPDATE` updates row 1. Spark 4.1.2 answers `false` and selects row 2.

**Proposed fix:** the comparison arms of `StructOperandsByPosition` from the parked branch of PR #1267 (head `8e929978`), carved onto a branch from main as this unit and not cherry-picked. The right operand's struct is renamed to the left operand's field names before `type_coercion` runs, so the type level finds equal types, inserts no cast and the arrow fast path pairs positionally. The rule is seated before `type_coercion` in both analyzer lists, with the insertion inventory of the 2026-11-09 rule answered at each seat. The shape is the orchestrator's proposal, carried by the owner's defaults ruling of 2026-11-09 (D1: carve from PR #1267).

**Close:** table-driven pins over the measured table on each door (the Spark SQL door, the DataFrame typed door and the DataFrame string doors) under both ANSI profiles, plus a DML-reach probe of `UPDATE`, `DELETE` and `MERGE` with a reordered key; then a narrow re-audit of the classes in C-116. The Status line of this card is updated on merge.

**Correction (2026-11-09, clerk):** (1) The `MERGE` bullet above: measured on fresh tables, Spark deletes id 1 by position and 1.5.6 deletes id 2 by name, which is still a wrong write. The earlier statement that the `MERGE` "deleted every row" came from a probe that ran MERGE on a table that UPDATE and DELETE had already changed. (2) Spark 4.1.2 with Iceberg 1.11.0 cannot run a DELETE with a struct-literal predicate (`Cannot create expression literal`); those cells are held to the derived oracle, the rows Spark's `SELECT` with the same predicate selects (owner, "default", 2026-11-09). (3) The step-0 table (4393 cells, Spark 4.1.2 against 1.5.6) supersedes the evaluation's cells 16, 22, 38 and 39, which do not reproduce. (4) Table finding: Spark refuses a reordered pair whose field types differ, even when they would widen. The rule's widening applies only to same-order pairs.

**Status (2026-11-10, clerk):** PR #1292, verified at `647fd64a`: Opus 5.5 FAIL on F-1 only (pre-existing, outside the three tables) and Muse MAX PASS. By the owner's Q1 ruling, "on 1 lets go with Fables recommendation", F-1 is carded with its family as `STRUCT-COMPARE-SEAT-FAMILY-1` (card [struct-compare-seat-family-1-card-2026-11-10.md](struct-compare-seat-family-1-card-2026-11-10.md), ledger C-118). #1292 merges on the owner's word.
