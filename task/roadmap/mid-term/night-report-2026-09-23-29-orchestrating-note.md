# Runs 28 and 29 — orchestrating note (2026-09-23 08:40 → 2026-09-24 10:40 EDT)

Run 28 (day, four Opus 5.5 orchestrator lanes xo-opus58..61) worked the run-27 close-out slate; at 16:5x the owner asked for a stopping point and Opus 5.5 high executors for the finish, so the lanes wound down with hand-off reports (the `day-report-2026-09-23-28-*` files) and run 29 (night, xo-opus62..65, same units, `opus` executor tier, GPT-6 Sol critic) picked them up under the owner's override grant for the night. All four run-29 lanes reached DONE by 09:13; the orchestrating session finished the two items they handed back (views PR6, fork #348) directly.

## Merged

RePark (all tree-equal through the merge driver): #805 #806 #807 #808 #809 #811 #812 #814 (run 28); #810 #813 #815 #816 #817 #818 #819 #820 #821 #822 #823 #824 #825 (run 29 and the morning). Fork: #347 (Hadoop metadata naming on MemoryCatalog). Units closed: IPI-23 metadata tables (#805 #806), IPI-30 orphan files (#807, #821), U1 memory layout (#814) and Hadoop naming (#820 + fork #347), U4 DESCRIBE / SHOW CREATE / SHOW TABLE EXTENDED (#810 #813 #816), U10 DataFrame path load and `_deleted` (#819 #822), IPI-40 views PR2–PR6 (#812 #815 #818 #823 #825), IPI-20 input_file_name (#811).

Open on the fork at the time of writing: #348 (Hadoop-mode drop_table removes the metadata chain; gate all zero, disposition recorded, held only on fork CI) and #349 (F-SNAPORDER-1, snapshots serialized in commit order; three critic rounds, disposition recorded, held on fork CI). Fork CI has failed at `docker-up` since about 13:05Z because quay.io answers `unauthorized` to anonymous pulls of the MinIO fixture images; a tag-only reference (#350, closed) fails the same way, Docker Hub carries no MinIO images, and upstream had no run in the window to compare. The fixture image source is an owner decision if it persists.

## Scoreboard 2026-09-24 (main 970ac11a, stage from 09-23)

638 EQUAL of 842 (+17 against 621), 22 gains, gate 83 non-EQUAL cells Spark answers. Five cells read EQUAL → non-EQUAL and none is a code regression: four catalog cells (CAT-CURRENT-CATALOG, CAT-DEFAULT-CATALOG, CAT-USE-CATALOG-NS, P-CALL-NO-CATALOG) are exposed by the harness change that declares the `hc` catalog through configuration, which disables the auto-registration of `spark_catalog`, so the cells' restore statement fails and later cells inherit the wrong current catalog — a real parity gap (Spark keeps `spark_catalog` whatever catalogs are configured), filed for U11. The fifth, TP-SUMMARY-PARTITION-LIMIT, is a flake with a pinned cause: the fork wrote the `snapshots` list in HashMap order (F-SNAPORDER-1, fork #349). The harness's warehouse root was hard-coded to the 09-21 directory and is now rewritten per day by `stage.sh`.

## Rulings under the override grant (owner may reverse any)

- SLICE2 route (B): fork Catalog trait method for the staged create in Hadoop naming mode — FINAL.
- HMETA-DROPNS: drop_namespace on a namespace that holds tables refuses (NamespaceNotEmpty equivalent), lean adopted; HMETA-REGHINT residue.
- HMETA-HARNESS (a): the scoreboard's RePark `hc` leg declares `type=hadoop` through configuration, matching the Spark leg.
- D-NS-NESTED: carve-out C-2 dated 2026-09-24 — nested namespaces leave v1.5.0 for the v1.6.0 card.
- TP-ACCEPT-ANY-SCHEMA-DF and its six W-*MERGE-SCHEMA* / W-ACCEPT-ANY-INSERT-VALUES siblings: refusal parity (Spark's class and message shape), not a flag.
- MCDEL: ruling (b) applied — prose-only findings after the tenth round fold without a re-gate.
- R6: harness normalisation of the copy-table staging id; finish time extended to 09:30 with a critic cap of 6.
- PR6 Q1: a SIGSEGV introduced by a PR is never a shippable residue — route (3), nested temp-view planning on a large-stack thread, landed in #825.
- F-SNAPORDER-1: fork serializes snapshots sorted by sequence number (then timestamp, then id).
- Dispositions after a critic cap: #821 Q-R5-HOLD (one test-only round, queued without a fifth critic), fork #348 Q-H4 (one test-and-ledger round, queued without a seventh critic), fork #349 round 3 (direct tie test, no fourth critic).

## Process

The owner's morning review of lane efficiency and permission prompts is filed as HARNESS-LOOP-REBALANCE-1 (#824, deferred). Measured on this run: 96 Opus executor and 110 Sol critic rounds for about 14 PRs; 373 orchestrator wakes, most on non-actionable status changes; no severity in the critic hand-back; build slots not a bottleneck. From 08:50 the orchestrating session runs work directly, one PR per lane, three critic rounds then a recorded disposition, until the loop is reorganised. Claude Code auto-updates were switched off after a reinstall killed a round on 09-23.

## Residues carried (numbered in the lane reports)

See each lane report's residue list: lane A (R-MC-RESERVED-NAME-SCAN, R-MC-RESERVED-NAME-CLASS, R-MC-IFN-COMMA-JOIN, R-DF-LOAD-SPELLINGS), lane B (views R1–R8, incl. DESCRIBE EXTENDED on a view and ALTER VIEW SET TBLPROPERTIES 'provider'), lane C (R-R5, R-H4, R-PI, HMETA-REGHINT, the fork pin bump and slice 2 after #348), lane D (R-U4-7, R-U4-12, R-U4-15, R-U4-16). The after-merge VIEWS replay for #825 runs with the next scoreboard.
