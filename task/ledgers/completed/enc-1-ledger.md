# Unit ledger — ENC-1 · first write to a table carrying `encryption.key-id` refuses

**Date:** 2026-10-09 · **Branch:** `fix/enc-1-first-write-refusal` · **Base:** `40fc916f` (main at pickup)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. Step 1 (survey + red test) landed and halted on the unruled surface;
round 2 landed step 1b (the live-Spark recording the interim rulings name as their oracle)
first, then the refusal plus the 35 Spark-door pins and the flipped `v3_cow` pin; round 3
landed the ANSI (15), stamp (2) and facade (9) pins, the registry flip, and this ledger's
steps 2–4 record. All work runs under the interim rulings in §Interim rulings (the owner can
overturn any of them; see §Decisions for owner).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Owner priority 2026-10-09, target v1.5.4. The 2026-10-01 ruling (ES-3) flips
registry row ENC-1 from a dated DECLARED exclusion into a product card: a table whose
properties carry `encryption.key-id` refuses at the first write, because this engine has no
encryption support and would otherwise write plaintext into a table that asked for encryption.
The readiness review of 2026-10-09 restated the ruling and shipped a probe whose second test
(`encrypted_table_write_must_refuse_without_encryption_support`) is red on main.

**Not in this unit:** the review probe's first test (timestamp wall time) is a different
card; the v2-CREATE refusal Spark shows in E23 (out of scope — the ruling keeps CREATE
succeeding; see §Step 1b); push; PRs; `gh`; AWS.

## PROPOSITION LEDGER — ENC-1 — 2026-10-09

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The review's second test is ported into the crate's test tree | `crates/repark-spark/src/tests/enc_1.rs` carries the probe's behavior (v3 CREATE with the key, INSERT, assert refusal) | **PROVEN** | File committed this step; the test cites `pins: enc-1/C-001, C-002` |
| C-002 | The ported test is red on the unmodified tree | `cargo test --locked -p repark-spark --lib enc_1` fails with the probe's message | **PROVEN** | §Measurements: `write unexpectedly succeeded with encryption requested and no KMS`; pinned by the same test |
| C-003 | The refusal class, condition and text are ruled | Interim ruling IR-1 (§Interim rulings), enacted in `write::encryption` and pinned on every door | **PROVEN** | `crates/repark-iceberg/src/write/encryption.rs`; pins cite `enc-1/C-003` in `write/map.md` |
| C-004 | The metadata-only scope (expiry, orphan sweep, pointer moves, ALTER-added key, CTAS-with-key) is ruled | Interim ruling IR-2 (§Interim rulings), enacted: rewrites refuse, expiry/sweep/moves/ALTER/SELECT run | **PROVEN** | §Step 2 seats; run-pins in `repark-spark/src/tests/enc_1.rs`; `write/map.md` cites `enc-1/C-004` |
| C-005 | Steps 2–4 land per the brief (one helper, pins per path, registry flip) | One helper + seats (§Step 2), 35 + 15 + 2 + 9 pins + the flip (§Step 3), registry/maps/ledger (§Step 4) | **PROVEN** | §Step 2, §Step 3, §Step 4; every pin file's map row cites `enc-1/C-005` |
| C-006 | The live-Spark recording is committed as the pins' source | `python/repark-parity/tests/live_spark/enc1_encryption_oracle.{py,json,sha256}` | **PROVEN** | §Step 1b; map rows cite `pins: enc-1/C-006` |
| C-007 | Fold 1: every writing shape and every orphan or pointer shape of the Opus verdict is a pin, on v2 and v3 | `crates/repark-spark/src/tests/enc_1_fold.rs` and `crates/repark-core/src/microbatch/run_tests.rs` assert the refusal and that snapshots, refs, the metadata pointer and the full recursive file listing are unchanged | **PROVEN** | §Fold 1; red on `47ec4e41` (11 of 13 fold pins, the two controls green), green after the chokepoints |
| C-008 | Fold 1: the refusal holds by construction at the two places every write passes | The catalog guard in `crates/repark-iceberg/src/catalog/encryption_guard.rs`: the file-create guard on the table's `FileIO`, the commit guard on `Catalog::update_table` and the two publish calls | **PROVEN** | §Fold 1 chokepoints; `crates/repark-iceberg/src/catalog/map.md` cites `enc-1/C-008` |
| C-009 | Fold 1: a source-scan test keeps the two chokepoints closed | `crates/repark-iceberg/src/tests/enc_1_gate.rs` fails when a catalog, a table handle or a `FileIO` is built in `crates/` outside the guarded wrappers | **PROVEN** | §Fold 1 gate; the hand mutants of §Fold 1 |

## Ruling row

The ruling's own rows, read 2026-10-09. None names an error class or message text.

1. `task/roadmap/epic-term/release-roadmap-2026-08-29.md:365` (2026-10-01, the four open
   questions): "**(3) ES-3:** the first write to a table carrying `encryption.key-id` refuses,
   as Spark does without a KMS; `CREATE` keeps succeeding; ENC-1 becomes a product card."
2. `task/roadmap/epic-term/contracts-ahead-of-code-2026-10-01.md:98` (ES-3): "**ruled
   2026-10-01: refuse** — the first write to a table carrying `encryption.key-id` refuses, as
   Spark does without a KMS, and `CREATE` keeps succeeding as in Spark; a product card, release
   the owner's, flipping the ENC-1 pin on purpose".
3. The registry row (§Registry) repeats row 2 verbatim as the 2026-10-01 amendment.
4. The readiness restatement (2026-10-09) is the review probe
   (`readiness_review_probe.rs:38-50`, the readiness-review evidence of 2026-10-09, embedded in the R-007 card):
   it asserts only `result.is_err()` — no class, no text. Its log confirms both probe tests
   fail on main for their own reasons.

The superseded row: `task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md:63,95` (owner
2026-08-24): the whole feature is a dated DECLARED exclusion, pinned by
`v3_cow.rs::v3_create_with_encryption_key_id_still_scans_without_a_kms`.

## Registry row

`docs/spark-sql-iceberg-parity.md:1684-1704`, `#### ENC-1 — Iceberg table encryption keys are
stored, never applied`. State on main: **DECLARED** (owner 2026-08-24), with the 2026-10-01
refusal ruling appended as the amendment that a product card will enact. Recorded behavior:
CREATE with the property succeeds; INSERT writes ordinary unencrypted Parquet (`PAR1`,
measured 2026-09-18) with no error; SELECT returns the rows; metadata `encryption-keys`
stays empty. Spark cell: with a configured KMS the key encrypts data, delete, manifest and
manifest-list files; without a KMS the Spark session fails to write (oracle: documented —
no value oracle, this engine never talks to a KMS). Pin:
`crates/repark-spark/src/tests/v3_cow.rs::v3_create_with_encryption_key_id_still_scans_without_a_kms`.

## Write-path survey

Every path that can put a data, delete or metadata file into an Iceberg table, read from
`crates/repark-iceberg/src/write/map.md`, `crates/repark-spark/src/map.md`, the two routers and
the `call/` dispatch on 2026-10-09. No path checks `encryption.key-id` today: the only
`encryption` mentions in `crates/` are the constant and assertions inside the old pin.

| # | Path | Door | RePark entry | Pre-file shared site | Fork role |
|---|---|---|---|---|---|
| W-01 | INSERT VALUES / SELECT, plain | Spark | `router.rs::execute_insert_routed` → `passthrough_after_p11` | the router arm itself | DataFusion plans onto the fork's `IcebergTableProvider::insert_into`; files written by the fork's `TaskWriter` |
| W-02 | INSERT with write options or session write conf | Spark | `execute_insert_routed` → `execute_append_with_options` | `write_options.rs::append_with_statement_options` → `commit_append_with_summary` | staging/commit actions only |
| W-03 | INSERT BY NAME / evolution | Spark | `insert_by_name::execute_insert_by_name` | owned; commits through the W-02 family | staging/commit actions only |
| W-04 | INSERT OVERWRITE whole table | Spark | `execute_insert_overwrite` | `commit_overwrite_replace_all_with_summary` and the `commit_overwrite_*` family | `OverwriteFiles` action |
| W-05 | INSERT … REPLACE WHERE | Spark | `insert_positional/replace_where.rs` preparse | owned dynamic-overwrite commits (W-04 family) | `OverwriteFiles` action |
| W-06 | CTAS | Spark | `ctas.rs::execute_ctas` (staged create) | the CTAS arm before `StagedTableTransaction::begin_create` stages files | staged transaction + owned staging |
| W-07 | RTAS / CREATE OR REPLACE / REPLACE TABLE AS | Spark | `execute_ctas` replace mode / `normalize::replace_table` | the replace arm before `begin_replace` stages files | staged transaction + owned staging |
| W-08 | MERGE | Spark + ANSI | `merge::execute_merge_statement` / `repark-sql` `merge::execute_merge` | `merge/snapshot_commit.rs::commit_overwrite_on_ref` + `commit_row_delta_kind_on_ref` | `OverwriteFiles` / `RowDelta` actions |
| W-09 | UPDATE / DELETE, identity shapes (IN / EXISTS / plain) | Spark + ANSI | `predicate_dml::execute_predicate_dml` (claimed in `spark_ast.rs` / `commit_identity_dml`) | the one `execute_predicate_dml` seat | COW overwrite / MoR row-delta actions |
| W-10 | DELETE, whole-file (metadata) | Spark + ANSI | `meta_delete::{try_metadata_delete, plan/commit_metadata_delete}` | the one meta-delete seat | `DeleteFilesAction::delete_from_row_filter` |
| W-11 | UPDATE / DELETE, all other shapes | Spark + ANSI | DataFusion delegate (`spark_ast::execute_passthrough` / `delegate`) | the router arm before delegation | fork provider delete/update |
| W-12 | TRUNCATE | Spark + ANSI | `truncate::execute_truncate` | `truncate.rs::commit_truncate_to` | snapshot commit |
| W-13 | CALL rewrite_data_files | Spark | `call::execute_call` → `execute_rewrite_data_files` | the `execute_call` dispatch | fork `RewriteDataFiles` |
| W-14 | CALL rewrite_manifests | Spark | `call::execute_call` | the `execute_call` dispatch | fork `RewriteManifestsAction` |
| W-15 | CALL rewrite_position_delete_files | Spark | `call::execute_call` | the `execute_call` dispatch | fork rewrite action |
| W-16 | CALL add_files | Spark | `call::execute_call` → `execute_add_files` | the `execute_call` dispatch | fork `AddFiles` (adopts parquet into the table) |
| W-17 | DataFrameWriterV2 append / overwritePartitions / overwrite(condition) | Facade | composed SQL through a temp view: INSERT INTO, INSERT OVERWRITE, INSERT … REPLACE WHERE | W-01 / W-04 / W-05 | as the composed statement |
| W-18 | DataFrameWriterV1 saveAsTable / save / insertInto | Facade | composed CREATE / CREATE OR REPLACE / INSERT / INSERT OVERWRITE | W-01 / W-04 / W-06 / W-07 | as the composed statement |
| W-19 | DataFrameWriterV2 create / createOrReplace | Facade | composed CTAS / CREATE OR REPLACE through a temp view | W-06 / W-07 | as the composed statement |
| W-20 | Micro-batch streaming sink (stamped commits) | Rust arms | `SiteStamp` arms in `write_options.rs` / `merge/snapshot_commit.rs` / `commit_stamp_only` | the W-02 / W-08 commit functions the arms call | as the arm |
| W-21 | INSERT (all spellings that reach the provider) | ANSI | `repark-sql` `router.rs::execute_insert_routed` → `session_insert` or `delegate` | the router arm before `delegate` | fork `insert_into` |
| W-22 | INSERT OVERWRITE PARTITION | ANSI | `insert_overwrite.rs` | owned partition-overwrite commits | `OverwriteFiles` action |
| W-23 | MERGE / UPDATE / DELETE / TRUNCATE | ANSI | `merge.rs` / `commit_identity_dml` / meta-delete / `truncate.rs` | shared with W-08 / W-09 / W-10 / W-12 | as the Spark twin |

Not file-putting, recorded for the scope ruling (Q2):

| # | Path | Effect |
|---|---|---|
| M-01 | CALL expire_snapshots | metadata-only: drops snapshot refs, writes table metadata; deletes no live file and stages none |
| M-02 | CALL remove_orphan_files | deletes unreachable files; stages no file and commits no snapshot |
| M-03 | CALL rollback / fast_forward / cherrypick / set_current / rollback_to_timestamp | snapshot-pointer moves; metadata-only |
| M-04 | ALTER TABLE … SET TBLPROPERTIES ('encryption.key-id' = …) | metadata-only property set; the key lands on the table and later writes refuse |
| M-05 | CALL run_maintenance / apply_partitioning / register_table / plan_partitioning | maintenance planning, spec evolution, catalog adoption; no data-file write on the target |
| M-06 | `DataFrame.writeStream` on a batch frame | already refuses (`WRITE_STREAM_NOT_ALLOWED`); no sink path exists behind it |

Single-site answer: **no one RePark function** covers W-01…W-23 — the fork-planned shapes
(W-01, W-11, W-21) never enter RePark write code. The smallest covering set is one helper
plus dispatch-level call sites: the helper reads the target table's properties (exact key
`encryption.key-id`) and refuses; it is called (a) in the Spark router's INSERT / DELETE /
UPDATE / MERGE / CTAS / TRUNCATE arms before planning or delegation, (b) in the ANSI
router's twins, (c) once in `call::execute_call` for the file-writing procedures, (d) in the
CTAS/replace arm against the staged creation properties. No fork change is needed: every
check sits before the first file is staged or planned, so a refused write leaves no orphan.
W-17…W-20 funnel into the same seats (composed SQL; the sink's commit functions).

## CREATE vs ALTER-added key

CREATE (`create_table.rs:160-168`): TBLPROPERTIES are copied verbatim into the creation
properties; only `format-version` is consumed and `owner` refused. `encryption.key-id`
passes through on any format version, and CTAS copies its properties the same way
(`ctas.rs` staged creation). Per the ruling, CREATE keeps succeeding.

ALTER (`alter.rs` → `format_version.rs::alter_set_tblproperties`): only `format-version` is
consumed; every other key is set verbatim through `set_properties_and_format_version`. Adding
the key later succeeds as a metadata-only commit; every later W-path then refuses. Removing
it with UNSET restores writability. Neither door validates the key's value.

## Step-2-ready survey

Recommended shape for the follow-up unit (not implemented here — halted on C-003):

1. One helper in `repark-iceberg` (crate-private to the workspace): load the target table,
   refuse when `metadata().properties()` contains `encryption.key-id`. Exact-key match only;
   other `encryption.*`-looking keys stay unmoved (controls per the brief).
2. Call sites: the §Survey seats (a)–(d). Each site refuses before planning, staging or
   delegation, so no orphan file survives a refusal.
3. Pins: one per W-path asserting the refusal AND an unchanged snapshot list and file
   listing; the old pin flips to create-and-scan-as-main plus first-write-refuses under a
   truthful name; controls without the property and with lookalike properties stay green.
4. Registry ENC-1 moves from DECLARED to the refusal with the ruled class and text.

## Step 1b — the live-Spark recording (2026-10-09)

Step 1b ran before any product edit, on Spark 4.1.2 + Iceberg 1.11.0
(`iceberg-spark-runtime-4.1_2.13-1.11.0.jar` from `~/.ivy2`), Hadoop catalog, `local[2]`,
no KMS configured — the MB-0 bench shape. Recorder, recording and sha:
`python/repark-parity/tests/live_spark/enc1_encryption_oracle.{py,json,sha256}` (C-006).
Two runs, identical outcomes (E01–E22 run, E23 refuses); snapshot ids and the warehouse
path vary between runs.

The head finding: **Spark stores the key and then ignores it.** The v3 table properties
carry `encryption.key-id` (E01–E02 `table_state`), and every write shape runs and lands
plaintext Parquet (`PAR1` magic read off the E02 data file in the warehouse). There is no
Spark refusal class to mirror on any v3 write path — the Q1 fallback governs everywhere.

| Cell | Shape | Spark | Table after |
|---|---|---|---|
| E01 | CREATE v3 with the key | runs | exists, 0 snapshots, 0 files |
| E02 | INSERT VALUES | runs | 1 `append`, 2 files, 2 rows |
| E03 | INSERT SELECT | runs | 1 `append`, 1 file, 2 rows |
| E04 | INSERT OVERWRITE | runs | 1 `overwrite`, 1 file, 1 row |
| E05 | CTAS with the key | runs | exists, 1 `append`, 1 file, 1 row |
| E06 | CREATE OR REPLACE AS with the key | runs | exists, 1 `overwrite`, 1 file, 1 row |
| E07 | MERGE | runs | 1 `append`, 1 file, 1 row |
| E08 | UPDATE | runs | 1 `overwrite`, 0 files, 0 rows (empty table) |
| E09 | DELETE row-level | runs | 1 `delete`, 0 files, 0 rows (empty table) |
| E10 | DELETE whole table | runs | 1 `delete`, 0 files, 0 rows (empty table) |
| E11 | TRUNCATE | runs | 1 `delete`, 0 files, 0 rows (empty table) |
| E12 | ALTER adds the key to a table with data, then INSERT | both run | 2 `append`, 2 files, 2 rows |
| E13 | `df.writeTo(...).append()` | runs | 1 `append`, 1 file, 1 row |
| E14 | CALL rewrite_data_files (2-file table) | runs, rewrote 0 | 1 `append`, 2 files, 2 rows |
| E15 | CALL rewrite_manifests | runs | 1 `append`, 2 files, 2 rows |
| E16 | CALL expire_snapshots retain_last 1 | runs | 2 `append`, 3 files, 3 rows |
| E17 | CALL remove_orphan_files | runs | 1 `append`, 2 files, 2 rows |
| E18 | SELECT | runs | 2 rows back |
| E19 | empty value + INSERT | runs | 1 `append`, 1 file, 1 row |
| E20 | `encryption.keyid` + INSERT | runs | 1 `append`, 1 file, 1 row |
| E21 | `encryption.key-id-x` + INSERT | runs | 1 `append`, 1 file, 1 row |
| E22 | no-key control INSERT | runs | 1 `append`, 1 file, 1 row |
| E23 | CREATE v2 with the key, then INSERT | CREATE refuses; INSERT never runs (no table) | table absent |

E23's refusal is `IllegalArgumentException` with no condition and no SQLSTATE, text
`Invalid properties for v2: [encryption.key-id]` — a CREATE-time property validation,
outside ENC-1 (the ruling keeps CREATE succeeding). Out-of-scope observation, carried in
the hand-back; this unit does not touch the v2 CREATE surface.

IR-2 classification (see §Interim rulings): every shape that writes data, delete or
manifest files refuses in RePark as a dated divergence (Spark runs it in plaintext);
expire_snapshots, remove_orphan_files, pointer moves, ALTER, planning/adoption and SELECT
run as in Spark and on main.

## Interim rulings (orchestrator, 2026-10-09)

Interim answers to §Halt Q1–Q3, from the owner's ES-3 words ("the first write to a table
carrying `encryption.key-id` refuses, as Spark does without a KMS; CREATE keeps
succeeding") plus the §Step 1b measurement. Each is listed under §Decisions for owner so
the owner can overturn it.

- IR-1 (Q1 — class and text): the class and condition are Spark's as recorded, per write
  path. Spark refuses no v3 write path, so the fallback governs everywhere:
  `UnsupportedOperationException` (the step-1 lean; the house loud-refusal shape). The
  text is RePark's own, one sentence: it names the table, the property
  `encryption.key-id`, that RePark has no table encryption and will not write plaintext
  into a table that asks for it, and the registry id ENC-1. It never echoes the key id's
  value.
- IR-2 (Q2 — which shapes refuse): a shape Spark refuses, RePark refuses; a shape Spark
  runs without writing a data, delete or manifest file, RePark runs. A shape Spark runs
  that writes data or manifests in plaintext despite the key refuses in RePark — never
  write plaintext into a table that carries the key — each as a dated divergence row.
  CTAS/RTAS carrying the key refuse as a whole statement, leaving no table behind (Spark
  runs them, so no Spark "table created, write refused" shape applies).
- IR-3 (Q3 — matching): the exact key `encryption.key-id`, any value including empty.
  Spark draws no empty-vs-set distinction (E19 runs exactly like E02), so the default
  stands. Lookalike keys are unmoved (E20–E21 run in Spark; they run in RePark too).

## Decisions for owner

0. **The divergence (2026-10-09, listed first):** the ruling's phrase "as Spark does
   without a KMS" does not match what Spark 4.1.2 + Iceberg 1.11.0 does. The recording
   (§Step 1b) shows Spark IGNORING the key on a v3 table and writing plaintext on every
   write shape (E02–E17, E19 run), and refusing CREATE on v2 (E23). RePark refuses the
   first write regardless and keeps CREATE succeeding on every format version, per the
   owner's ES-3 words and the never-write-plaintext rule. Recorded as one dated row in
   registry ENC-1 and §Divergence. Overturning re-scopes the helper's call sites and
   re-writes every refusal pin.
1. IR-1 (class `UnsupportedOperationException` + RePark's one-sentence text on every
   refused write path): Spark refuses no v3 write path, so there is no Spark class to
   mirror; the alternative is a new Spark-condition-shaped class. Overturning IR-1
   re-texts every pin.
2. IR-2 (refuse every data- or manifest-writing shape as a dated divergence though Spark
   runs each in plaintext; run expiry, orphan sweep, pointer moves, ALTER and SELECT):
   the alternative is matching Spark exactly (writing plaintext), which the owner's ES-3
   words forbid. Overturning IR-2 re-scopes the helper's call sites.
3. IR-3 (exact key, any value including empty; lookalikes unmoved): Spark treats empty,
   set and absent identically (all run), so the empty-included reading is a judgment
   call, not a measurement. Overturning IR-3 to "empty counts as absent" moves the E19
   pin from refuse to run.

## Measurements

Unmodified tree (`40fc916f` + step-1 test only), 2026-10-09:

```
$ cargo test --locked -p repark-spark --lib enc_1
test tests::enc_1::encrypted_table_write_must_refuse_without_encryption_support ... FAILED
write unexpectedly succeeded with encryption requested and no KMS
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 2635 filtered out
```

Step 1b (2026-10-09), live Spark 4.1.2 + Iceberg 1.11.0, no KMS:

```
$ ENC1_WAREHOUSE=/tmp/enc1-warehouse2 JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 \
    /tmp/sparkenv/bin/python python/repark-parity/tests/live_spark/enc1_encryption_oracle.py
recorded E01: ok ... recorded E22: ok, recorded E23: error (23 cells)
$ python3 -m json.tool enc1_encryption_oracle.json > /dev/null  # exit 0
$ sha256sum -c enc1_encryption_oracle.sha256  # OK
$ head -c 4 <E02 data file>  # PAR1
```

## Halt

Step 1 of the brief is complete and committed. Steps 2–4 need decisions the brief did not
make: the ruling (§Ruling) orders a refusal but names no error class, no condition details
and no message text, and it is silent on every metadata-only and creation-time shape. The
brief orders a halt with a proposal in exactly this case. Proposed answers below; the owner
rules.

- Q1 (RULING — class, condition, text): refuse with the house loud-refusal shape
  `UnsupportedOperationException` (the class of the V3-GEO-1 and ORC/Avro declared
  refusals), condition = the target table's stored properties contain `encryption.key-id`
  (exact key, any value), text naming the table, the property, ENC-1 and the 2026-10-01
  ruling, e.g. `Table <name> carries 'encryption.key-id' (<value redacted): RePark has no
  KMS or table-encryption support, so the write refuses (ENC-1, ruled 2026-10-01)`. Lean:
  adopt the house shape; Spark's own no-KMS text is unmeasured (the registry records a
  documented oracle only) and mirroring it needs a live-Spark probe first.
- Q2 (OWNER — scope): refuse every W-path (anything that commits data, delete or manifest
  files, including CTAS/RTAS-with-the-key as a whole statement with no table left behind,
  TRUNCATE, meta-delete and add_files); allow M-01…M-06 (pure metadata expiry, orphan
  sweep, pointer moves, ALTER that adds the key, planning/adoption). Lean: adopt the
  brief's step-2 default exactly as written.
- Q3 (OWNER — key matching): match only the exact property `encryption.key-id`; a table
  carrying other `encryption.*`-looking keys, or the key with an empty value, is
  (a) refused like any value vs (b) unmoved. Lean: (a) for the exact key with any value
  including empty (the table asked for encryption); other `encryption.*` keys unmoved.

## Step 2 — the refusal (round 2, commit `8f900a71`)

One helper, `crates/repark-iceberg/src/write/encryption.rs`: `refuse_encrypted_properties`
(exact key `encryption.key-id`, any value including empty; lookalikes unmoved),
`refuse_encrypted_table` and `refuse_encrypted_write` (a missing table loads-fail-open —
the door's own missing-table error owns that shape). Every seat refuses with
`UnsupportedOperationException` and one sentence naming the table, the property, no table
encryption, no plaintext and ENC-1, never echoing the key value. Four seat families cover
the §Write-path survey; every seat sits before staging, planning or delegation, so a
refusal leaves no orphan file:

| Family | Seats | Survey paths |
|---|---|---|
| Entry checks (both doors) | Spark `router.rs` INSERT arm, `router/delete_update.rs` DELETE/UPDATE arms, `merge.rs`, `insert_overwrite.rs`, `insert_by_name.rs`, `insert_positional/replace_where.rs`, `append_with_options.rs`, `truncate.rs`; ANSI `router.rs`, `merge.rs`, `insert_overwrite.rs`, `truncate.rs`, `guards.rs` | W-01…W-05, W-08, W-09, W-11, W-12, W-21…W-23 |
| Post-plan DML-target checks | Spark `spark_ast.rs` (closes case-folding and `EXPLAIN ANALYZE` holes); ANSI `delegate_plan` (same `Analyze` unwrap) | W-09, W-11, W-21 |
| CTAS/replace property checks | Spark `ctas.rs` (staged-creation properties on both arms, replace target); ANSI `create_table.rs` (staged properties, replace target) | W-06, W-07 |
| File-writing CALLs + WAP | `call/rewrite_data_files.rs`, `call/rewrite_manifests.rs`, `call.rs` position-delete rewrite, `call/add_files.rs`, `call/branch_ops.rs` `publish_changes` | W-13…W-16, WAP (below) |
| Commit backstops | `write_options.rs` (5 commit fns), `overwrite_commit.rs`, `merge/snapshot_commit.rs`, `predicate_dml.rs`, `meta_delete.rs`, `overwrite_filter.rs`, `partition_overwrite.rs`, `commit_target.rs` | W-02, W-08, W-09, W-10, W-20 |
| Sink stamp | `sink_offsets.rs::commit_stamp_only` → `EncryptedSinkRefused` before scope claim | W-20 |

Two survey gaps found while seating: WAP `publish_changes` (M-03 listed pointer moves but
not publish, which commits a snapshot — it refuses) and RTAS-without-the-key onto a keyed
table (`begin_replace` merges properties so the key persists — it refuses). Expiry, orphan
sweep, pointer moves, `ALTER`, dry-run planning and `SELECT` run. `router.rs` sat at its
1000-line ceiling, so the Spark DELETE/UPDATE doors moved to `router/delete_update.rs`
(pure move).

## Step 3 — pins (rounds 2–3)

Every refusal pin asserts the class + text AND the unchanged snapshot list and file
listing under the table location (no orphan data, delete, manifest or metadata file):

| Battery | File | Pins |
|---|---|---|
| Spark door (round 2) | `crates/repark-spark/src/tests/enc_1.rs` | 35: INSERT VALUES/SELECT/options/BY NAME/OVERWRITE/REPLACE WHERE, CTAS (no table left), CREATE OR REPLACE with and without the key, MERGE, UPDATE (plain + case-folded), DELETE (predicate, whole, aliased), TRUNCATE, the three rewrites, add_files, WAP publish, direct commit backstop, dry-run/expiry/sweep/rollback run-pins, UNSET restores writes, empty value, lookalikes, SELECT, CREATE + empty scan, EXPLAIN runs / EXPLAIN ANALYZE refuses, v2 CREATE-with-key, no-key control |
| Flipped pin (round 2) | `crates/repark-spark/src/tests/v3_cow.rs::v3_create_with_encryption_key_id_refuses_first_write` | create and scan as main; first write refuses; `encryption-keys` still empty |
| ANSI door (round 3) | `crates/repark-sql/tests/enc_1.rs` | 15 over v2 tables (the refusal is version-independent): INSERT VALUES/SELECT/OVERWRITE PARTITION, CTAS with key, CREATE OR REPLACE without key onto keyed, MERGE, UPDATE, DELETE × 2, TRUNCATE, ALTER-added key, empty value, lookalikes, CREATE + SELECT, no-key control |
| Stamp (round 3) | `crates/repark-iceberg/src/write/encryption_tests.rs` | 2: `commit_stamp_only` onto a keyed sink refuses `EncryptedSinkRefused` with snapshots and objects unchanged; a lookalike key stamps one snapshot |
| Facade (round 3) | `python/repark/tests/test_enc_1.py` | 9: V2 append/overwritePartitions/overwrite(condition)/create-with-key/createOrReplace, V1 saveAsTable append+overwrite, insertInto append+overwrite, unkeyed + lookalike controls |

WAP `publish_changes` is Spark-only (the ANSI door refuses `CALL`); the v2 CREATE-with-key
case is pinned on both SQL doors. The round-2 flip commit went in unrun; round 3 ran it
first (green, no fix).

## Step 4 — registry, maps, ledger (round 3)

Registry row ENC-1 (`docs/spark-sql-iceberg-parity.md`) moves DECLARED → FIXED 2026-10-09
with the Before/After, the live-Spark oracle citation, the pin list and the one dated
divergence row (§Divergence). North-star rows amended in place, dated:
`task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md` lines 63 (§3 row) and 95 (gate-audit
row 5). Map rows: `repark-spark/src/tests`, `repark-spark/src/router`, `repark-spark/src/call`,
`repark-iceberg/src/write`, `repark-iceberg/src/microbatch`, `repark-sql/src`,
`repark-sql/tests`, `python/repark/tests`, `python/repark-parity/tests/live_spark` (step 1b).

## Divergence (2026-10-09, by ruling — §Decisions for owner item 0)

ONE row. The ruling's phrase "as Spark does without a KMS" does not match what Spark
4.1.2 + Iceberg 1.11.0 does. The recording (§Step 1b, `enc1_encryption_oracle.json`)
shows Spark IGNORING the key on a v3 table and writing plaintext on every write shape
(CREATE E01 runs; INSERT VALUES E02 / SELECT E03 / OVERWRITE E04 run; CTAS E05 and RTAS
E06 run; MERGE E07, UPDATE E08, DELETE E09–E10, TRUNCATE E11 run; ALTER-added key + INSERT
E12 runs; `df.writeTo().append()` E13 runs; `rewrite_data_files` E14 and `rewrite_manifests`
E15 run; empty value E19 and lookalikes E20–E21 run), and refusing CREATE on v2 (E23,
`IllegalArgumentException`, no condition). RePark refuses the first write regardless and
keeps CREATE succeeding on every format version — the owner's ES-3 words plus the
never-write-plaintext rule in round 2. Mirrored in registry row ENC-1.

## Hand mutants (2026-10-09, round 3)

One per seat family; each removal reddened exactly its pin and the restore went green:

| Mutant | Removal | Pin | Result |
|---|---|---|---|
| M1 entry | Spark `router.rs` INSERT-arm `refuse_encrypted_write_target` call | `enc_1::encrypted_table_write_must_refuse_without_encryption_support` | FAILED, then 35/35 green on restore |
| M2 backstop | `write_options.rs::commit_append_with_summary` `refuse_encrypted_table` | `enc_1::encrypted_table_direct_append_commit_refuses` | FAILED, then green on restore |
| M3 stamp | `sink_offsets.rs::commit_stamp_only` key block | `encryption_tests::stamp_only_commit_onto_keyed_sink_refuses_without_writing` | FAILED, then 2/2 green on restore |
| M4 CTAS | Spark `ctas.rs` create-arm `refuse_encrypted_properties` | `enc_1::encrypted_ctas_refuses_without_leaving_a_table` | FAILED, then green on restore |

## Measurements (rounds 2–3)

```
$ cargo test --locked -p repark-spark --lib tests::enc_1
test result: ok. 35 passed; 0 failed
$ cargo test --locked -p repark-spark --lib tests::v3_cow::v3_create_with_encryption_key_id_refuses_first_write
test result: ok. 1 passed; 0 failed
$ cargo test --locked -p repark-sql --test enc_1
test result: ok. 15 passed; 0 failed
$ cargo test --locked -p repark-iceberg --lib write::encryption_tests
test result: ok. 2 passed; 0 failed
$ pytest python/repark/tests/test_enc_1.py -q
9 passed
```

## Coverage attestation

```
COVERAGE_ATTESTATION:
  pr_unit: enc-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause C-001..C-006 is pinned per its proof obligation; the quantified W-path survey (W-01..W-23) carries one pin per path on the door that spells it, plus the M-path run-pins and the lookalike/no-key controls.
      artifacts: [crates/repark-spark/src/tests/enc_1.rs, crates/repark-sql/tests/enc_1.rs, python/repark/tests/test_enc_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Empty key value refuses, lookalike keys and unrelated properties run, ALTER-added and ALTER-removed keys flip writability both ways, case-folded and aliased DML targets refuse, EXPLAIN plans without writing while EXPLAIN ANALYZE refuses.
      artifacts: [crates/repark-spark/src/tests/enc_1.rs, crates/repark-sql/tests/enc_1.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal seat sits before staging, planning or delegation; each refusal pin asserts unchanged snapshots, live files and warehouse objects, so a refused write leaves no orphan and a retry refuses identically.
      artifacts: [crates/repark-spark/src/tests/enc_1.rs, crates/repark-iceberg/src/write/encryption_tests.rs]
    - id: AT-4
      status: N/A
      justification: The checks are synchronous property reads on the already-loaded table or a fresh catalog load; no shared state, no ordering, no new concurrency.
    - id: AT-5
      status: ATTACKED
      evidence: The refusal text never echoes the key id value (asserted in every battery); no credential, path or injection surface is added — the key is matched, never interpreted.
      artifacts: [crates/repark-iceberg/src/write/encryption.rs, python/repark/tests/test_enc_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: CTAS-with-key leaves no table, CREATE OR REPLACE keeps the existing table and rows, UNSET restores writes, and metadata-only operations (expiry, sweep, ref moves) keep their behavior with rows asserted after.
      artifacts: [crates/repark-spark/src/tests/enc_1.rs]
    - id: AT-7
      status: N/A
      justification: One property-map lookup per write, plus one catalog load where the door had not already loaded the table; no loop, no growth, nothing system-breaking.
    - id: AT-8
      status: ATTACKED
      evidence: The refusal maps to UnsupportedOperationException through the house engine_err contract on every door; the Spark-without-KMS behavior it replaces is measured, not presumed, in the committed live recording.
      artifacts: [python/repark-parity/tests/live_spark/enc1_encryption_oracle.json]
    - id: AT-9
      status: ATTACKED
      evidence: The refusal names the table, the property, the missing capability, the no-plaintext reason and the registry id ENC-1 in one sentence, identical on every door and the stamp path.
      artifacts: [crates/repark-iceberg/src/write/encryption.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Four hand mutants (entry, backstop, stamp, CTAS) each reddened exactly their pin and went green on restore; every added branch (key present/absent, empty value, lookalike, missing table, Analyze unwrap) has a nameable input that flips it.
      artifacts: [task/ledgers/staging/enc-1-ledger.md]
  reattested: []
  complete: true
```
