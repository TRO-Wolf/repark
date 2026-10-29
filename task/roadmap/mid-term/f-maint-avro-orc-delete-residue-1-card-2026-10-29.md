# Card F-MAINT-AVRO-ORC-DELETE-RESIDUE-1: maintenance leaves Avro and ORC delete files that Spark removes

**Date:** 2026-10-29. **Filed by:** the second-audit records round (`claude-haiku-5-5`), from the second independent audit of the format-v2 label, finding AUD-013 ([the report](../../../docs/audits/iceberg-v2-label-independent-reaudit-2026-10-29.md), section 3 and section 7).

**Status:** open. Grade C (clerk). Target: the owned `iceberg-rust` fork, reached through RePark's maintenance procedures. Not dispatched; no build is owed until the owner charters it.

**Why:** On Avro and ORC merge-on-read tables, the fork's `rewrite_data_files` leaves a delete file that Spark removes, and `rewrite_position_delete_files` skips Avro and ORC position deletes altogether. The second is the carded F-EQDEL-CONVERT-BATCH-1 second item, and the audit's measurement matches that card. Rows are correct on both engines, so the residue is maintenance divergence, not a wrong answer. Since #1209 RePark writes Avro and ORC delete files again, so both residues are reachable from ordinary RePark tables.

**Site:** the fork's `crates/iceberg/src/maintenance/rewrite_position_delete_files.rs`, where the position-delete planner skips Avro and ORC files (about line 565), and the delete-file retention of `rewrite_data_files`, whose line the audit did not locate. The charter names the second site from the code.

**Fix:** `rewrite_position_delete_files` compacts Avro and ORC position deletes as it does Parquet ones, which is the carded F-EQDEL-CONVERT-BATCH-1 second item, and `rewrite_data_files` removes the delete files whose rows it has applied on Avro and ORC tables, as Spark does. Pins: after each procedure on a merge-on-read table in each of the three data formats, the delete-file count and the row set equal Spark's. The Parquet control stays as it is.

**Measured (copied from the second audit, 2026-10-29; each engine, a merge-on-read table with seven rows in two inserts, one delete and one update, then the two procedures):** on Avro, `rewrite_position_delete_files` returns zeros on RePark (rewritten and added counts 0, bytes 0) and `3, 3, 2507, 2507` on Spark. After `rewrite_data_files`, RePark keeps one delete file and Spark keeps none. On ORC the same shape gives zeros on RePark and `1, 1, 1108, 1108` on Spark, and one delete file on RePark against none on Spark. The row sets are equal on both engines in both formats. The Parquet control leaves no delete file on either engine after `rewrite_data_files`.

**Retires:** the card when both procedures leave Spark's delete-file count on Avro and ORC, the Parquet control stays green, and F-EQDEL-CONVERT-BATCH-1's second item is marked as merged in its own card. The label ledger's AUD-013 row (`ice-v2-label-1` C-070) then closes by citation.

**Relation:** [F-EQDEL-CONVERT-BATCH-1](f-eqdel-convert-batch-1-card-2026-10-27.md), whose second item this card carries; [F-EQDEL-DROPPED-KEY-2](f-eqdel-dropped-key-2-card-2026-10-29.md), the other Avro and ORC residue of the same fork delete family, on the read path; the parity registry's F-EQDEL-DROPPED-KEY-1 dated line of 2026-10-29, which carries this finding.
