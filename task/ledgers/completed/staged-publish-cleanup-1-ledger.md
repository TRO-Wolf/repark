# Unit ledger — STAGED-PUBLISH-CLEANUP-1 · a rejected staged publish removes its files, so no reader takes it for a commit

**Date:** 2026-11-04 · **Base:** `4a7d1701` (the branch's merge base with main; the commit table's before state is main `bb7983b6`) · **Head:** `20389129` · **Squash:** `ca06fec6` on main (PR #1261)
**Model:** builder `claude-opus-5-5` (every fix, test and merge commit of the branch) · verifiers `claude-opus-5-5` and `muse-spark-1.3-contributor` (three rounds each) · records `claude-haiku-5-5`
**Policy:** [../../../AGENTS.md](../../../AGENTS.md) · **Path:** STANDARD.

**Status:** completed 2026-11-04 at `20389129` (squashed to main as `ca06fec6`, PR #1261) after the two verifiers. The Opus 5.5 verdicts were FAIL (round one, F1 S1), FAIL (round two, F5 S1) and PASS (round three); each FAIL was remediated by a delta round. The Muse verdicts were PASS in all three rounds. Round three's F6 (S4 by the verifier, filed at the grammar's S3 ceiling) is OPEN as a test-adequacy lead. The records pass that writes this file closes the card STAGED-PUBLISH-CLEANUP-1, files the card STAGED-PUBLISH-OPTIONS-PATH-1 and records the leads. Clauses C-001 to C-007 are PROVEN or records; C-008 to C-013 are OPEN leads.

**Why.** Commit-outcome rule D2 ([commit-rule-2026-11-04.md](../../../docs/corpus/commit-rule-2026-11-04.md)): a staged CTAS, RTAS or CREATE TABLE whose publish call failed (not a CommitStateUnknown) left its staged metadata file on disk, and the reopen read that file as the committed table. Twelve cells, FAILURE_REPORTED_COMMITTED_ON_REOPEN, on both doors and both catalogs.

**Clause.** Java's rules, read from apache-iceberg-1.11.0 before the first edit: `HadoopTableOperations.commit` (the commit is the rename of a temp metadata file; a failed rename deletes the temp file and throws CommitFailedException); `BaseTransaction` (CommitStateUnknownException is rethrown before any cleanup; other failures clean the snapshot producers when `!requireStrictCleanup || CleanableFailure`); `BaseMetastoreTableOperations.checkCurrentMetadataLocation` (a publish counts as landed when its location is current or in the previous-files list); and `HadoopTableOperations` line 157 (the next version after a failed rename is current + 1). RePark's cleanup deletes the attempt's files once a `load_table` probe shows the publish did not land. That is Hive's commit-status-check shape, not Java's exception-class shape: Java deletes data files only for a CleanableFailure, and RePark has no cleanable marker (builder question 2).

## PROPOSITION LEDGER — STAGED-PUBLISH-CLEANUP-1 — 2026-11-04

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | On a publish that a probe shows did not land, the statement's own files are removed (the handed metadata file, the create-mode begin file, the new snapshots' manifest lists and manifests, and the statement's data files), and the six call sites route through the one helper. | The table tests on both doors (the D2 rows: fresh, reopen and walk read the previous state, no new metadata file, orphans 0), the rejected-schema-only and rejected-format-version pins, and the red check with the cleanup disabled. | PROVEN | Site `crates/repark-iceberg/src/write/staged_publish.rs` (new; `commit_staged`, `publish_applied`, `discard_rejected`). Six call sites: the Spark door's CTAS (fast and options path), its schema-only CREATE and REPLACE, and the native door's CREATE TABLE (fast and options path). Pins: the Spark and native table tests, and the schema-only reject pin on both doors. Red: with the cleanup disabled, the table tests, the format-version-3 pins and the next-write pins go red on both doors (Opus round one; Muse round one, mutation M1); the schema-only reject pin goes red on both doors (delta one, round two). |
| C-002 | The keep rules: the staged files stay when the outcome is CommitStateUnknown; when the probe's load fails with anything but a create-mode not-found; when a replace's table is not found at the probe (ruling, commit 312e2fca); when the publish landed, meaning (a) the handed location is in the current metadata log, (b) a snapshot the publish added is among the current snapshots, or (c) for a create, the table's uuid is the handed uuid; and when a file is referenced by the current table (the walk over the current metadata log, manifest lists, manifests and their live data and delete entries). | The unknown pins, the lost-acknowledgement pins, the built-over pins, the schema-only log-search pin, the dropped-table pin and the landed-log-gone pin, on both doors. The walk-failure keep rule is not pinned; it is carried as the open lead C-009. | PROVEN (the walk-failure rule is unpinned: lead C-009) | Sites `publish_absent`, `publish_landed`, `referenced_files`, `snapshot_files` in `staged_publish.rs`. Condition (a) is the Java port of `checkCurrentMetadataLocation` (lines 334-340). Conditions (b) and (c) and the referenced-files set are the unit's extensions. Delta one (commit 63149bd6) added (a) and (b) after the round-one race (Opus F1, S1); delta two (commit 7a3344e6) added the referenced-files set after the round-two case (Opus F5, S1). Round three measured the walk against the delete manifests of v2 and v3 tables and against a bounded base history; those cases are measured, not pinned. |
| C-003 | The enumerated cases each have their pin: a rejected CTAS, RTAS and CREATE TABLE (memory and hadoop); an unknown outcome keeps its files and reports unknown; a landed publish keeps its files; a rejected format-version-3 create or replace leaves the previous state; the next successful write after a rejected hadoop replace takes the freed version; the rejected native-door publish. | The table tests, the probe tests on both doors (ten tests per door, one of them the table test). Kill-at-publish (case 5) is unchanged and stays pinned by the existing kill suite, which this unit does not bind. | PROVEN | Builder round-one hand-back, eight enumerated cases, against Java rules R-a, R-b and R-c. Opus round one and Muse round one confirmed the eight cases (Muse claim 2: twelve attack probes green, three mutations bite). Pins are listed in the pins file, not named here. |
| C-004 | The table after the unit: the D2 group moves from FAILURE_REPORTED_COMMITTED_ON_REOPEN to EQUAL (12 of 12 on both doors); D1 (32), G1 (12), D3 (8), R1 (13) and the 287 ungrouped cells keep their class; the four hadoop metadata-rename rows for CTAS and CREATE TABLE lose a stray metadata file; the two CTAS rows also lose an orphan data file. | The two table tests (the D2 rows and their controls). The 364-cell corpus re-run is records evidence: the harness was copied in untracked, run and removed. The committed table is `docs/corpus/commit-cells-2026-11-04.jsonl`. | PROVEN | Each round re-ran the corpus at its head: round one (builder, Opus, Muse), round two (Opus, Muse) and round three (Opus, Muse), with 0 class moves and 0 text moves against the previous round. Classes after the unit: EQUAL 299, FAILURE_REPORTED_COMMITTED 32, REFUSED_BEFORE_COMMIT 13, EQUAL_REGISTERED 12, STATE_DIFFERS 8. The injected v1 squatter is kept in all four metadata-rename rows. |
| C-005 | The ENC-1 gate admits the forwarding wrapper: `repark-iceberg/src/write/staged_publish.rs` joins the `impl Catalog for` allowlist beside `append_fence.rs` (ruling on builder question 1). The wrapper forwards every Catalog method (30 of 30) to the inner catalog, and only the publish methods record the handed table. | The two ENC-1 gate tests in `crates/repark-iceberg/src/tests/enc_1_gate.rs`, green at the head. Forwarding checked method by method and by a live probe of 26 of 26 outcomes (Muse round two). | PROVEN | Allowlist row at `enc_1_gate.rs`, commit 9c8c85e9. The alternative (a public materialize-then-publish split in the fork) was not taken: the brief bars fork changes. Opus F2 and Muse R1F1 (the twelve unforwarded default methods) were closed by commit 63149bd6 in delta one. |
| C-006 | Boundary and gates: the unit diff is eight files (the new module, `write/mod.rs`, the ENC-1 allowlist, the six call sites and the two test files), with no fork, Cargo, `.github`, reader or DML change. At the head: repark-iceberg 1061 passed, 2 ignored; repark-spark 3176 passed, 5 ignored; repark-sql 670 passed, 4 ignored; repark-core 1601 passed, 2 ignored; facade `pytest python/repark/tests -n 8` 23347 passed, 683 skipped, 150 xfailed, 0 failed; `make rust-panic-ban`, `make rust-clippy` and `make check-map-size` exit 0. | none (records) | PROVEN | The counts are the delta-two hand-back's, matched by Opus and Muse at the head in round three. Round one: repark-core had one timing-bound failure in a lifecycle test, untouched by the diff, which passed on a clean rerun with --no-fail-fast. |
| C-007 | Hygiene: no code comment in the added Rust, Python, TOML or shell lines; each unit commit ends with one `Authored-By` trailer; no local path, session link, account id, bucket, region or ARN in the added lines. | none (records) | PROVEN | Builder and both verifiers' diff scans: 0 added comment lines (the two grep hits in round three are `#[tokio::test]` attributes). `make rust-panic-ban` exit 0. The merge commits carry the orchestrator's trailer, as ruled. |
| C-008 | Lead L1 (builder question 4; Muse's out-of-scope observation, confirmed in the fork code): on the options and session-write-conf path, a hadoop RTAS writes its metadata through `Transaction::apply_locally`, which takes `with_next_version` of the staged vN. The pointer can then land on v<N+2> with v<N+1> missing. A uuid create leaves the begin-create `00000` file as stray metadata on success. | Closing question: measure both doors on the options path, then pin or fix. | OPEN | Card [STAGED-PUBLISH-OPTIONS-PATH-1](../../roadmap/mid-term/staged-publish-options-path-1-card-2026-11-04.md) (S3). Not measured by any verifier in this unit. |
| C-009 | Lead L2 (Opus round three F6, S4 by the verifier, filed at the grammar's S3 ceiling; test adequacy): the walk-failure keep rule in `discard_rejected` has no pin. Replacing its early return with an empty set leaves both table test files green. Also Opus round three O2: on a walk error, the handed metadata location could still be deleted when the current table does not reference it; not ruled. | Closing question: a rejected replace whose current non-base snapshot has an unreadable manifest list keeps the attempt's files, pinned on both doors; the O2 refinement ruled or declined. | OPEN | No card by this pass. The behaviour at the head is measured correct (Opus round three, the manifest-list read failure during the walk). |
| C-010 | Lead L3 (Opus round two observation under F5; Opus round three O1): a schema-only landed version file that a later commit's truncated metadata log no longer lists is deleted (on hadoop, v3 under a current v4). A walk from a hand-set stale hint below the gap stops there. Java with write.metadata.delete-after-commit.enabled leaves the same gap. | Closing question: a ruling that this residue is Java-equal, recorded on the label draft, or a pin. | OPEN | No card by this pass. The real version-hint walk reads the newest version (measured on both catalogs). |
| C-011 | Lead L4 (Opus round three O4; cost, no correctness effect): on the error path, when the current snapshot is a base snapshot, the walk still loads every manifest whose adding snapshot was expired from the base. A sequence-number bound would skip them. | Closing question: the bound, measured for cost, or declined. | OPEN | No card by this pass. |
| C-012 | Lead L5 (Opus round three O3; outside the unit): on hadoop, a side writer cannot commit while a staged attempt holds the next version (version file already exists), and a failed side INSERT leaves one unreferenced data file. That is the plain INSERT conflict path, not the attempt's file. | Closing question: the INSERT conflict residue on hadoop, as its own unit if the owner wants it. | OPEN | No card by this pass. |
| C-013 | Lead L6 (not measured): a rejected RTAS over a merge-on-read table with a deletion vector (held by construction and by a v3 base-file probe); a torn metadata write mid-flush (needs an I/O fault); the probe arms with no handed table (needs a FileIO fault during materialize); the native session-conf options path through SQL (the same tested function); the live PySpark JVM oracle (no JVM in the verifier environment; the measured table and the Java source are the oracle); snapshot-id reuse across tables and the create-mode uuid collision (code-read only; either can only keep files). Muse's round-two P2c diagnostic (the same case as Opus F5 at a1c998db) was not re-run at the head in its round three; the head measurement of that case is Opus round three's truncate-and-expire row. | Closing question: fault hooks for the first four rows; a head run of the P2c case. | OPEN | No card by this pass. |

## Evidence

**Verifier verdicts.** Opus 5.5 (`claude-opus-5-5`), three rounds: round one FAIL (F1 S1; F2, F3 and F4 at S4 by the verifier), round two FAIL (F5 S1), round three PASS (F6 S4). Muse (`muse-spark-1.3-contributor`), three rounds: PASS in each (round one R1F1 and R1F2 at S3; none new in rounds two and three). The reports are untracked; their verdict lines and closure tables are transcribed here.

**Findings closed.** Opus F1 (the race: a landed publish misreported as an error, then built over before the probe) = C-002, remediated in delta one and re-run closed in rounds two and three. Opus F2 (unforwarded Catalog methods) = C-005, remediated in delta one. Opus F3 (no native lost-acknowledgement pin) = C-003, remediated in delta one. Opus F4 (report of the state moves) = C-004, remediated by the delta report. Opus F5 (a truncated metadata log plus an expired snapshot made the cleanup delete a live data file) = C-002, remediated in delta two. Muse R1F1 = C-005, closed in round two. Muse R1F2 (the schema-only replace reject arm had no committed pin) = C-001, closed in delta one. Opus F6 is open as C-009.

**Coordinator rulings** (recorded in the hand-backs): question 1, the ENC-1 allowlist row for the forwarding wrapper, = C-005; the Replace-mode not-found keeps every file (commit 312e2fca), with both native and Spark pins = C-002; the landed probe follows Java's metadata-log check, with conditions (b) and (c) as the unit's extensions, and the Opus round-two case was closed by the referenced-files set rather than by a change to the rule = C-002.

**Builder.** Round one: the site, the eight enumerated cases, the table and four questions. Delta one: the Java metadata-log probe, the wrapper and the race pins. Delta two: the referenced-files set before any delete, the walk-failure keep rule and the truncate-and-expire pins.

**Commits.** Round one: `e2d21b6a` (the fix), `e1f28c11` (the D2 pins on both doors), `9c8c85e9` (the ENC-1 allowlist row), `c63afcd8` (a merge of main). Delta one: `63149bd6` (the landed probe: metadata log and snapshots, and the wrapper forwarding), `39f91dd7` (the built-over and schema-only pins), `312e2fca` (the Replace-mode not-found ruling), `a1c998db` (a merge). Delta two: `7a3344e6` (the referenced-files set), `48c30840` (the landed-and-gone pins) and `20389129` (the Spark side writer on one session, and a clippy fix), which is the head.

## Scope notes

- The owner's cap-exception ruling of 2026-11-04 allowed the delta-two round for Opus F5 (S1) beyond the one-round cap. The round-three verifier re-checked that delta and found nothing new at S1 or S2.
- The kill-at-publish rows (G1, 12 cells) are unchanged. A killed writer runs no cleanup, so the registered ICE-LOCAL-COMMIT-POINT-1 reading (a killed publish reads back as committed) stands.
- The D1 rows (native-door post-commit reporting, 32 cells) and the D3 rows (DROP TABLE leftovers, 8 cells) are not changed by this unit. The label draft's exclusions name them under NATIVE-DOOR-POST-PUBLISH-1 and DROP TABLE leftovers.
- The rule's registry row for ICE-LOCAL-COMMIT-POINT-1 lives in `docs/spark-sql-iceberg-parity.md`; the card the brief named for its dated line is not in the tree, so that line is not written by this pass.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: staged-publish-cleanup-1
  complete: true
  reattested: [AT-1, AT-3, AT-6, AT-8, AT-10]
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The 364-cell corpus was re-run at every round's head by both verifiers, with the D2 group, the controls and the state moves compared against the committed table and the previous round. Round one found the state-move report incomplete (F4), closed in delta one.
      artifacts: [crates/repark-iceberg/src/write/staged_publish.rs, docs/corpus/commit-cells-2026-11-04.jsonl]
    - id: AT-2
      status: N/A
      justification: N/A — untouched by origin/main..20389129, as both verifiers classified it.
    - id: AT-3
      status: ATTACKED
      evidence: Rejected, unknown, lost-acknowledgement, probe-down, race-rejected and race-landed publishes on memory and hadoop; truncated metadata logs, expired snapshots, dropped tables and unreadable manifest lists during the walk. Findings Opus F1 and F5 (both S1) were closed by delta rounds with live evidence.
      artifacts: [crates/repark-iceberg/src/write/staged_publish.rs, crates/repark-spark/tests/staged_publish_cleanup.rs, crates/repark-sql/tests/staged_publish_cleanup.rs]
    - id: AT-4
      status: N/A
      justification: N/A — untouched by origin/main..20389129. The concurrent-writer probes are filed under AT-3 and AT-6 as the cleanup cases.
    - id: AT-5
      status: N/A
      justification: N/A — untouched by origin/main..20389129; no authentication, input-injection or secrets surface in the diff.
    - id: AT-6
      status: ATTACKED
      evidence: Committed-file deletion under each landed condition, the squatter metadata file, earlier snapshots of an RTAS target, format-version-3 tables, deletion vectors and position deletes carried by the current table, many base snapshots, and the other writer's files under a recreate.
      artifacts: [crates/repark-iceberg/src/write/staged_publish.rs]
    - id: AT-7
      status: N/A
      justification: N/A — untouched by origin/main..20389129 beyond one extra walk on the error path, which the verifiers measured as a cost (lead C-011) and not as a regression.
    - id: AT-8
      status: ATTACKED
      evidence: The Catalog trait compared method by method against the wrapper (30 of 30 forwarded), the ENC-1 allowlist row, and the error passthrough. Findings Opus F2 and Muse R1F1 (S3) were closed in delta one.
      artifacts: [crates/repark-iceberg/src/write/staged_publish.rs, crates/repark-iceberg/src/tests/enc_1_gate.rs]
    - id: AT-9
      status: N/A
      justification: N/A — untouched by origin/main..20389129; no logging, metrics or alarm surface in the diff.
    - id: AT-10
      status: ATTACKED
      evidence: Mutations that disable the cleanup, remove the unknown exemption, force the probe to absent, and disable the cleanup on the schema-only path; the delta-stashed red count; the walk-error mutation, which the round-three verifier found unpinned (lead C-009).
      artifacts: [crates/repark-spark/tests/staged_publish_cleanup.rs, crates/repark-sql/tests/staged_publish_cleanup.rs]
```

## Findings

```yaml
FINDING:
  id: OPUS-F1
  severity: S1
  category: AT-3
  clause: C-002
  claim: A Replace whose publish landed under a non-unknown error, then was built over by another writer before the probe, was classified as not landed, and the cleanup deleted the committed snapshot's manifest list, manifest, data file and metadata file.
  evidence: Opus round-one verdict, F1 with its reproduction; the orchestrator reproduced it; the round-one attack log.
  disposition: REMEDIATED (delta one, commits 63149bd6 and 39f91dd7; round two re-ran the case closed on memory and hadoop)
```

```yaml
FINDING:
  id: OPUS-F2
  severity: S3
  category: AT-8
  clause: C-005
  claim: The forwarding wrapper did not override twelve defaulted Catalog methods, so they fell through to the trait defaults rather than the inner catalog. Not reachable today, because the commit path calls only the publish methods.
  evidence: Opus round-one verdict, F2 (graded S4 by the verifier; filed at the grammar's S3 ceiling).
  disposition: REMEDIATED (delta one, commit 63149bd6; round two checked 30 of 30 methods mechanically)
```

```yaml
FINDING:
  id: OPUS-F3
  severity: S3
  category: AT-10
  clause: C-003
  claim: The native door had no lost-acknowledgement pin, so forcing the probe to absent left the native suite green.
  evidence: Opus round-one verdict, F3 (graded S4 by the verifier; filed at the grammar's S3 ceiling).
  disposition: REMEDIATED (delta one, commit 39f91dd7; round two showed the pin red under the forced probe)
```

```yaml
FINDING:
  id: OPUS-F4
  severity: S3
  category: AT-1
  clause: C-004
  claim: The round-one hand-back reported one orphan move; the four hadoop metadata-rename rows also lost a stray metadata file (class unchanged).
  evidence: Opus round-one verdict, F4 (graded S4 by the verifier; filed at the grammar's S3 ceiling); the delta hand-back states the four moves.
  disposition: REMEDIATED (the delta report states the four moves; the class is unchanged)
```

```yaml
FINDING:
  id: OPUS-F5
  severity: S1
  category: AT-3
  clause: C-002
  claim: A landed Replace whose metadata location was truncated out of the log and whose snapshot was expired before the probe was classified as not landed, and the cleanup deleted a data file still live in the current snapshot.
  evidence: Opus round-two verdict, F5 with its probe; the round-three re-run on memory and hadoop.
  disposition: REMEDIATED (delta two, commits 7a3344e6 and 48c30840; the referenced-files set, the owner's cap-exception ruling of 2026-11-04)
```

```yaml
FINDING:
  id: OPUS-F6
  severity: S3
  category: AT-10
  clause: C-009
  claim: The walk-failure keep rule added in delta two has no pin; replacing its early return with an empty set leaves both test files green. The verifier graded it S4; filed at the grammar's S3 ceiling.
  evidence: Opus round-three verdict, F6 with its reproduction (both doors green under the mutation).
  disposition: OPEN (lead L2, C-009; no card by this pass)
```

```yaml
FINDING:
  id: MUSE-R1F1
  severity: S3
  category: AT-8
  clause: C-005
  claim: The same twelve unforwarded defaulted Catalog methods as Opus F2 (unreachable today).
  evidence: Muse round-one hand-back, R1F1.
  disposition: REMEDIATED (delta one, commit 63149bd6; closed in Muse round two with a live in-module probe of the forwarding)
```

```yaml
FINDING:
  id: MUSE-R1F2
  severity: S3
  category: AT-10
  clause: C-001
  claim: The schema-only CREATE OR REPLACE rejected arm had no committed pin; a probe showed it green and correct.
  evidence: Muse round-one hand-back, R1F2, with its probe output.
  disposition: REMEDIATED (delta one, commit 39f91dd7; the pin is red on both doors with the cleanup disabled)
```
