# Unit ledger — ICE-META-DELETE-1 · a DELETE that covers whole files deletes the files (IPI-08)

**Date:** 2026-09-19 · **Branch:** `fix/ice-meta-delete-1` · **Base:** `5770a0f0` (origin/main + the RP-39 pin bump)
**Model:** claude-opus-5 (round 1, steps 1–7) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** (a routing decision above the DELETE write path; no
table-format semantics of our own — the commit is the fork's `DeleteFilesAction`).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

Clause ids are the brief's C-1…C-8 written in the gate's three-digit form: C-001 = C-1, …,
C-008 = C-8.

**Why now.** Owner direction: 1:1 parity with Spark's Iceberg integration. Spark decides,
ABOVE `write.delete.mode`, whether a DELETE can be answered by removing whole data files; RePark
never asked, so every predicate DELETE wrote position deletes / DVs (merge-on-read) or rewrote
files (copy-on-write), and a no-match DELETE committed nothing at all.

**Measured (the fixture).** Spark 4.1.2 + Iceberg 1.11.0, 18 shapes × {v2, v3} × {mor, cow} =
72 cells, each holding the surviving rows, every snapshot's operation and the summary counters,
and the live data files' record counts. Recorded by `/tmp/oc-worker/pd-oracle/record_meta_delete.py`,
committed as `python/repark/tests/ice_meta_delete_1_spark_oracle.json`.

**Spark's rule.** `SparkTable.canDeleteWhere(Predicate[])` converts every conjunct to an Iceberg
expression — a conjunct it cannot convert answers false — and then asks
`canDeleteUsingMetadata`: true when the predicate selects whole partitions, else true when every
data file the scan plans for that predicate STRICTLY matches it. True issues
`DeleteFiles.deleteFromRowFilter(expr)`, one `delete` snapshot that removes those files and
writes no delete file, in BOTH `write.delete.mode` values. A predicate that plans NO file is
vacuously true, which is where Spark's empty `delete` snapshot on a no-match comes from.

**The decision point (step 1).** Both doors reach the row-level path through
`repark_iceberg::write::predicate_dml::execute_predicate_dml`, but only for the shapes the
identity allow-list accepts (`try_allowed_delete_in`, `try_allowed_update_in`,
`plain::try_allowed_plain_identity` — a scalar comparison only). `DELETE FROM t`, `WHERE true`,
`IS NULL`, `LIKE`, and an `IN` list are planned by DataFusion onto the fork's `TableProvider`
instead, so a decision placed inside `execute_predicate_dml` would miss five of the eighteen
recorded shapes. The decision therefore sits one level up, at the two door seats that own a
`Statement::Delete`, and is ONE shared Rust function:

| seat | file | where |
|---|---|---|
| shared decision + commit | `crates/repark-iceberg/src/write/meta_delete.rs` | `try_metadata_delete` |
| Spark SQL door | `crates/repark-spark/src/router.rs` | `execute_delete`, after every existing refusal, before `execute_passthrough` |
| native `repark.sql` door | `crates/repark-sql/src/router.rs` | `execute_identity_or_delegate`, after the multi-spec guard, before the identity allow-list |

**Not in this unit:** `STATUS.md`, `Cargo.toml`, `Cargo.lock`, UPDATE (Spark has no metadata
UPDATE), MERGE, branch-targeted DELETE (see C-007).

## PROPOSITION LEDGER — ICE-META-DELETE-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A DELETE whose predicate strictly selects whole data files removes those files in ONE `delete` snapshot with the oracle's `deleted-data-files` / `deleted-records`, writing no delete file and no rewritten data file, in both `write.delete.mode` values, on v2 and v3, on both doors. | The `whole_one_file_*`, `whole_two_files_*`, `string_eq_whole_*`, `partition_select_*`, `partition_select_bucket_*`, `partition_plus_metrics_*`, `or_whole_*`, `is_null_whole_*`, `nondeterministic_like_*`, `prior_deletes_then_whole_*` cells; Rust pins. | OPEN | step 3–4 |
| C-002 | A DELETE that matches no row commits an empty `delete` snapshot — a new snapshot, operation `delete`, nothing added or removed — on both modes and doors. | The `no_match_*` cells; Rust pins. | OPEN | step 3–4 |
| C-003 | A partial (row-level) DELETE is unchanged and the metadata route never swallows one: the decision is made BEFORE the commit, never as a failed commit plus a fallback. | The `partial_only_*`, `mixed_partial_and_whole_*`, `partition_partial_*`, `prior_deletes_then_rest_*`, `not_in_whole_*_mor` cells; Rust pins. | OPEN | step 3–4 |
| C-004 | `DELETE FROM t` with no predicate and `WHERE true` follow the `all_rows_*` cells. | The `all_rows_true_*`, `all_rows_nopred_*` cells. | OPEN | step 3–4 |
| C-005 | Prior position deletes / DVs do not push a later whole-file delete onto the wrong route. | The `prior_deletes_then_whole_*` and `prior_deletes_then_rest_*` cells on both modes. | OPEN | step 3–4 |
| C-006 | The decision and the commit carry the session's case sensitivity to the fork. | Rust pins; the pinned door behaviour is stated here. | OPEN | step 3–4 |
| C-007 | Branch-targeted DELETE: the branch reaches `can_delete_using_metadata` and the commit, or the ledger states the door cannot target one. | Reading + pin. | OPEN | step 3–4 |
| C-008 | Reverting the routing turns C-001 and C-002 red; the exact mutation and the failing test names are recorded. | Step 5. | OPEN | step 5 |

## Gates

(step 6)
