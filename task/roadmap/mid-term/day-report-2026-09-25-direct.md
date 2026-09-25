# Direct orchestration — day report (2026-09-24 20:00 → 2026-09-25 11:00 EDT)

The orchestrating session ran the v1.5.0 remainder directly (no tick-driven lanes): one PR in
flight per lane, Opus 5.5 high executor rounds, Opus 5.5 medium critics until 07:10, and from
07:10 a hand-dispatched medium-effort sub-agent on every hand-back and every troublesome lane
(owner rule, mid-turn; Fable 5.1 until 11:30, Opus 5.5 from then on). This report lists what merged, what the scoreboard says, every ruling taken under the
owner's override grant, the dispositions, and what the night cost.

## Merged

RePark, all tree-equal through the merge driver, in merge order: #830 (U7 PR1 — DataFrame writer
`save(name)`, `bucketBy`, `output-spec-id`), #831 (U5 PR2a — `ALTER COLUMN … COMMENT`), #835
(U7 PR2 slice 1 — `saveAsTable` overwrite is Spark's RTAS with field ids kept by name on every
replace door; the writer branch option writes main), #833 (U8 PR1 — `INSERT INTO … REPLACE
WHERE`, `INSERT INTO … PARTITION`, positional sources with repeated names; a 584-case Spark
oracle replayed on both doors), #834 (U5 PR2b — `WRITE ORDERED BY` transform terms, `CREATE
BRANCH` on an empty table, `format-version` 1 create with a loud v1 ref guard), #836 (RP-50 —
fork repin to `b2698e56`).

Fork, squash-merged tree-equal with the two Docker fixture jobs red (quay.io refuses anonymous
pulls of `minio/minio`; owner option 1): #353 (F-V1-REFS-1 — `TableMetadataV1` keeps its `refs`
on write and read, Java's shape), #354 (F-REPLACE-IDS-1 — `begin_replace` assigns fresh field
ids by name as Java's `buildReplacement` does; `schemas` / `partition-specs` / `sort-orders`
serialize in id order).

Open at the time of writing: #837 (U7 PR2 slice 2 — `writeTo.overwrite(condition)` on U8's
overwrite-by-filter kernel; rebased onto main as one commit; its round-1 verifier found and
confirmed a real defect — a frame with an extra and a missing column committed shifted columns
where Spark refuses `INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS` — fixed by moving the by-name
binding into the Rust REPLACE WHERE door; the round-2 verifier and gate are running).

## Scoreboard 2026-09-25 (main `9e3bf2dd`, staged from 09-24)

| | 09-24 | 09-25 |
|---|---|---|
| EQUAL | 638 | 668 |
| Non-EQUAL cells Spark answers | 83 | 45 |
| Progress | 88.5 % | 93.7 % |

Zero regressions. Thirty cells moved to EQUAL; eight moved to SPARK-CANNOT because U6 made
RePark refuse where Spark refuses. Of the 45 remaining, ten are addressed by today's merges and
open PR (the three U8 write cells, the two U7 slice-1 cells, the two U7 slice-2 cells,
`D-WRITE-ORDERED-TRANSFORM`, `D-REF-BRANCH-ON-EMPTY`, `D-CREATE-V1` — the last needs the RP-50
repin for `md.refs`); the matrix is re-run tomorrow morning on the merged main. The remaining
35: catalog (`CAT-CURRENT-CATALOG`, `CAT-TYPE-MEMORY`, `CAT-DEFAULT-CATALOG`,
`CAT-USE-CATALOG-NS`), DDL (`D-RENAME-TABLE`, `D-NS-NESTED` carve-out C-2,
`D-SHOW-TBLPROPERTIES[-KEY]`, `D-X-PARTITIONED-COLDEF`), edge (`E-CASE-PARTITION-FIELD`,
`E-CASE-SELECT`, `E-CATALOG-LISTDATABASES`, `E-TZ-TIMESTAMP-AS-OF`), procedures
(`P-RDF-PARTIAL-PROGRESS`, `P-RTP-*` ×3, `P-CALL-NO-CATALOG`), properties
(`TP-DELETE-GRANULARITY-FILE`, `TP-MANIFEST-MIN-MERGE`, `TP-FORMAT-V1-DELETE`), reads
(`R-MC-ROW-ID-V3`, `R-STREAM-READ[-SKIP]` carve-out C-1), types (`TY-TIMESTAMP-NTZ[-V3]`,
`TY-VARIANT-V3`, `TY-MAP`, `TY-TIMESTAMP-LTZ`, `TY-UNKNOWN-VOID`, `TY-UUID-READ`),
`L-INSERT-OVERWRITE`, `W-STREAM-WRITE-FILESRC` (C-1), `W-MERGE-NESTED` and
`W-UPDATE-NESTED-FIELD` (U8 PR2, next on the U8 lane).

## Rulings under the override grant (owner may reverse any)

- U8 Q1 — `REPLACE WHERE` is Spark-only syntax in PR1 (DML-6 precedent); a native-door steer is
  a follow-up. Q2 — the brief's premise corrected: the bucketed-insert failure was DataFusion
  naming a cast by its input, fixed by aliasing repeated names in positional INSERT sources
  only; a plain SELECT keeps ID-3. Q3 — `ice-overwrite-mode-1/C-013`'s refusal text superseded
  by Spark's `CAST_INVALID_INPUT` (dated note in its staging ledger). Q4 — U6's by-name
  routing exclusion narrowed to `INSERT OVERWRITE … PARTITION` on accept-any tables.
- U5 Q1 — the v1 `refs` gap is a fork unit (F-V1-REFS-1, merged) and RePark guards every v1
  ref write loudly until the repin; Q2 — PR2b ships as one PR, one commit per cell; Q3 — the
  fork downgrade text and same-transaction branch retention are later fork-parity residues.
- U7 Q1 — slice 2 stays stacked on U8 PR1 until it merges, then rebases (done); Q2 — the unit
  ships as two PRs on the slice boundary; Q1 (round 3) — the fork assigns by-name ids itself
  (F-REPLACE-IDS-1, merged); RePark keeps its `replacement_schema` call until the next repin.
- Dispositions after the critic cap of three: U8 PR1 (three Opus critics, each with measured
  defects: NULL partition keys under `<`/`<=`, one-element `NOT IN`, out-of-range literal folds
  on `<=>` and BIGINT; round 4 fixed V-001..V-003/V-005/V-007 and dated residues R-11 (typed,
  scientific, DECIMAL and DATE-string literals Spark converts and RePark refuses) and R-12 (a
  NULL key written in the same manifest as matched keys under `<`/`<=`); a Fable verifier
  passed the disposition head with one text-only rendering finding, folded). U5 PR2b (two Opus
  critics, one Fable verifier; the last found an invented hex token rendering and a pre-existing
  commit on a trailing comma in `WRITE ORDERED BY`, both fixed in the disposition fold). U7
  slice 1 (two Opus critics found the RTAS field ids assigned by position — wrong rows on old
  refs — on the DataFrame door and then on the SQL doors; the third passed; the native-door pins
  and a double table load folded by Fable).

## Process

- The session stalled from about 22:55 to 05:10 EDT: the U5 and U7 hand-backs (22:52, 23:28)
  and the U8 round-2 verdict (23:59) were delivered only at 05:10, so nothing ran for six
  hours. Cause not established; the launches that should have run overnight ran 05:11–07:00.
- Owner rule 07:10: every hand-back and any troublesome lane gets a hand-dispatched Fable 5.1
  medium sub-agent. Twelve dispatches today: verifiers on fork #354, U8 (disposition head), U5 (fold), U7 slice 2
  (twice); fixers for the fork pins, U7 native pins, U5 fold, U5 disposition, U8 fractional
  rendering, U7 slice-2 by-name binding; the U7 slice-2 rebase. Every verifier measured its own
  Spark shapes; three found real defects (U5 hex rendering, U8 decimal-on-INT regression class,
  U7 slice-2 shifted columns). At 11:30 the owner moved these agents to Opus 5.5.
- CI-red causes after green local gates, each fixed in minutes and recorded as a lesson: the
  docs example for the retired bucketBy ruling (local gates do not execute examples); a clippy
  `too_many_lines` on a grown test table (the local gate does not run clippy `--all-targets`
  with CI's flags); the V3-COV doc test's 3000-character row window; a semantic merge break
  after rebasing U5 over merged U8 (caught by a targeted `cargo test --lib` before the gate).
- Repin gate: `test_live_spark_race_scan_forwards` fails when `test_ice_branch_ops_1.py`
  precedes `test_ice_hadoop_vn_1.py` under the shared live session, on the old pin as well as
  the new one — residue LIVE-ORDER-BRANCHOPS-VN-1 (test hygiene), not a repin defect.
- Memory: the box's 32 GB swap filled to 99 % with idle desktop processes (three rust-analyzer
  servers, VS Code) while RAM kept 100 GB free; the watcher's swap-only alert is now
  advisory when MemAvailable stays above 60 GB. Clearing it is the owner's `swapoff`/`swapon`.
- An Opus worker commit carried a co-author trailer the pre-push hook bans; the tip was
  amended before the push.

## Spend

Owner numbers: 39 % weekly usage at 19:00 on 09-24, 49 % at 06:00 on 09-25 (the noon number is
requested). Opus executor
rounds since 19:00: U8 four rounds ≈ $183, U7 four rounds ≈ $104 (PR2 + three fixes), U5 three
rounds ≈ $86, fork two rounds ≈ $5; Opus critics ≈ $30. Plan stated to the owner: Opus 5.5 high
executors to 18:00, Muse high executors from 75 %, critics on Fable 5.1 medium; check-ins at
noon and 18:00.

## Next

Re-run the matrix on the merged main; U8 PR2 (nested-field UPDATE/MERGE assignment); U5 PR3
(refusal texts); U9/U11 buildable cells with the pending owner rulings; the repin follow-up that
drops `refuse_ref_on_format_v1` and U7's `replacement_schema` call now that both fork units are
pinned.
