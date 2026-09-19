# Run 23a report — the night of 2026-09-18 → 19: the fork residue lane

**Run:** 23a (unit `night-23a`, lane prefix `na-`). **Window:** 17:23 → 03:45 EDT. **Orchestrator:** claude-opus-5.

**Actors:**
- Devin SWE-2: 13 units' rounds; runs in `/tmp/devin-worker/runs.tsv`, lanes `na-fork` and `na-fork2`.
- Claude Opus 5 at high effort: one round, #301 round 4, the delete-file sequence GC. The unit moved up a tier after a gate-rejected round.

**Reviewers:** Grok 4.6. Each PR got a perf review (persona `critic-quality`), a logic review, and a verification critic with mutation.

**Beside:** run 23b, which owns every pin bump and RePark PR. I announced each fork merge in `claims.txt`.

**Box:**
- Every cargo job ran capped (`repark.slice`) with `CARGO_BUILD_JOBS=6`.
- At most two cargo things of mine at once, where a building Grok critic counts as one.
- One GitHub fork clone plus one `--shared` checkout. Rebases ran in short-lived `git worktree`s that were removed right after.
- No AWS of any kind.

## 1. The list — before / after

| # | Unit | Before tonight | After | Fork PR |
|---|---|---|---|---|
| 1 | F-SHED-295: relocated code sheds its comments | #294 had carried 54 comment lines and #295 had carried 149 | **MERGED** `f25c619d`. All 203 lines deleted; the gate over `8477b249..HEAD` reports 0. #296 had none | #297 |
| 2 | F-LIST-NULL-ACCESSOR-1 (Q-22a-D) | `DELETE/UPDATE/SELECT … WHERE xs IS NULL` on a list/map/struct column failed loud | **MERGED** `9f36da97`. It answers all 128 measured Spark cells: 4 shapes × v2/v3 × CoW/MoR × DELETE/UPDATE × 4 predicates | #299 |
| 3 | F-RP-SUMMARY-USER-1 (Q-22b-E) | A caller's `replace-partitions` value was overwritten with `true` | **MERGED** `18ab9761`. The caller's value wins, as in Java. Cherry-pick reads the marker case-insensitively (`parseBoolean`) | #298 |
| 4a | ICE-RDF-GRANULARITY-1 | `target_small` answered 8→2; Spark answers 8→4 | **MERGED** `e3eef24f`. The planner follows Java's read splits. **Measured:** `max_group_size` and `partial_progress_groups` were already Java's answer on fork-written file sizes. The RePark xfails stay while files are larger than Spark's (see #306) | #302 |
| 4b | ICE-RDF-COW-BYTES-1 (+ DANGLE-2) | `rewritten_bytes` missed a file; `removed_delete_files_count` was 1 where Spark gives 0 | **MERGED** `29ea7f6d`. Measured as one root cause: the fork removed parquet deletes by reference, which Java does only for DVs. The fix also ports Java's `dropDeleteFilesOlderThan` sequence GC, so the RPD→RDF residue now ends at 0 deletes, as Spark's does | #301 |
| 4c | ICE-RDF-RPD-COMMITS-1 | Per-group commits; 8 deletes became 2 partition-scoped ones | **MERGED** `587d3592`. One `replace` commit, file-scoped outputs, dangling positions dropped, data sequence preserved | #304 |
| 5 | C-1 ICE-AWS-UNKNOWN-DRILL-1, step 1 (Glue seam) | The seam existed only under `cfg(test)` (#252) | **VERIFIED, HELD as draft** (Q-23a-9). Feature-gated property seam; unknown outcomes carry the operation id | #303 |
| + | F-ROWID-ORDER-1 (23b's ask) | v3 `first_row_id` was nondeterministic (6 mappings in 12 runs) | **MERGED** `50350e33`. Deterministic: ascending partition value, then task index. **Measured:** Spark's order is its hash task order, not partition value (Q-23a-5) | #300 |
| + | F-LIST-NULL-ACCESSOR-2 (23b's ask) | RePark CoW DELETE with AND/OR over a nested IS NULL failed loud | **Diagnosed as RePark-side**; 23b took it. The cause is `predicate_dml::plain::is_scalar_comparison`, which accepts AND/OR. The fork hardening branch is left unopened (§5) | — |
| + | F-DANGLING-DV-COMMIT-1 (#301 verifier's named gap) | Only `rewrite_data_files` dropped the DVs of removed data files | **MERGED** `43fcd243`. Every merging commit now drops them, as Java's `removeDanglingDeletesFor` does. The changelog keeps them as existing deletes | #305 |
| + | F-PARQUET-SIZE-1 (measured from 4a) | Fork files were 1,430 B against Spark's 1,146 B | **MERGED** `171deddf`. The footer carries Java's key-values: `iceberg.schema` in, `ARROW:schema` out, `delete-type` on delete files. The file is now 1,098 B | #306 |

## 2. Pull requests (all in the fork, TRO-Wolf/iceberg-rust)

| PR | Unit | Actor, rounds | Reviews (Grok 4.6) | Comment gate | State |
|---|---|---|---|---|---|
| #297 | F-SHED-295 | orchestrator (a scripted deletion of the gate's list) + an ASF-header fix | none needed (gate plus tests) | 0 | **MERGED** `f25c619d` 18:06 |
| #298 | F-RP-SUMMARY-USER-1 | Devin r1, r2 (the actor's own cherry-pick residual folded in) | perf LOOKS-GOOD · logic **PASS** · verify **PASS** | 0 | **MERGED** `18ab9761` 19:21 |
| #299 | F-LIST-NULL-ACCESSOR-1 | Devin r1, r2 (Binary/Set pins) + orchestrator ledger line | perf LOOKS-GOOD · logic **PASS** (2 P3, needed a proceed resume) · verify **PASS** | 0 | **MERGED** `9f36da97` 19:50 |
| #300 | F-ROWID-ORDER-1 | Devin r1, r2 | perf LOOKS-GOOD · logic NEEDS_REMEDIATION (L-001, L-002 P2) · verify **PASS** | 0 | **MERGED** `50350e33` 20:18 |
| #301 | F-RDF-COW-BYTES-1 | Devin r1 (stalled, no commit), r2 (**gate-rejected**, one truncated `//!`), r3; **Opus 5 high r4** (sequence GC, 106 turns, $16.16) | perf R-01 **P1** (fixed in r4) · logic **PASS** · verify **PASS** (row safety) | 0 | **MERGED** `29ea7f6d` 21:23 |
| #302 | F-RDF-GRANULARITY-1 | Devin r1, r2 (perf P2s), r3 (after the #301 rebase: seven `cow_bytes` re-pins) | perf LOOKS-GOOD · logic **PASS** · verify **PASS** · re-verify **PASS** | 0 | **MERGED** `e3eef24f` 22:32 (first merge attempt CHECKS-RED) |
| #303 | F-GLUE-FAULT-SEAM-1 (C-1 step 1) | Devin r1, r2 (new session) | perf COMMENT · logic NEEDS_REMEDIATION (L-001, L-002 P2) · verify **PASS** | 0 | **DRAFT, held**: Cargo.toml feature line needs owner approval (Q-23a-9); `security_audit` red from RUSTSEC-2026-0285 |
| #304 | F-RPD-COMMITS-1 | Devin r1 (**gate-rejected**, three `///` lines), r2, r3 | perf ACCEPT_WITH_P2 · logic NEEDS_REMEDIATION (L-001 P2) · verify **PASS** | 0 | **MERGED** `587d3592` 23:51 |
| #305 | F-DANGLING-DV-COMMIT-1 | Devin r1, r2 (perf), r3 (order pin), r4 (changelog planner, after fork CI red) | perf LOOKS GOOD · logic **PASS** · verify **PASS** · focused verify **PASS** | 0 | **MERGED** `43fcd243` 03:41 (first attempt CI red on a changelog cell, fixed in r4) |
| #306 | F-PARQUET-SIZE-1 | Devin r1, r2, r3 (production `expect` removed), r4 (raw-reader test helper, after fork CI red) | perf LOOKS-GOOD · logic **PASS** · verify **PASS** | 0 | **MERGED** `171deddf` 03:14 |
| — | F-LIST-NULL-ACCESSOR-2 hardening | Devin r1, r2 | logic NEEDS_REMEDIATION (S1: `with_filter` widening), fixed in r2 | 0 | branch `fix/f-list-null-accessor-2` @`93bc707b`, **not opened** (the root cause is RePark-side, §3) |

**Costs:**
- Grok 4.6: **$21.69** across 34 review runs (874 turns); `/tmp/grok-worker/runs.tsv`, lanes `na-rv-*`.
- Claude Opus 5 (high): one round, **$16.16**, 106 turns.
- Devin SWE-2 (free tier): 27 rounds over 11 sessions; `/tmp/devin-worker/runs.tsv`, lanes `na-fork`, `na-fork2`.
- Grok stall mode seen three times: a reviewer ends after 1 turn with a status summary. It was resumed with a proceed follow-up, or re-run fresh when the resume stalled too.

## 3. What the measurements showed

- **Relocated comments (#297).** The gate listed 203 lines, which I deleted by script (Q-23a-1). The fork CI's licence check then caught that my new ledger lacked the ASF header, which I fixed.
- **List-null (#299).** In the Spark oracle an empty list is not null, `[NULL]` is not null, and a struct with a NULL child is not null. The Grok logic critic then found that Java binds container IS NULL exactly: Java has `PositionAccessor`s for list, map and struct fields. The fork's #299 approach instead drops the term to a residual. That is correct wherever DataFusion re-applies the filter, but it is not Java's mechanism (follow-up, §6).
- **Row-id order (#300).**
  - Spark's file commit order, measured with 8 categories: hash distribution with AQE gives `z,x,m,a,q,b,c,d` on every run; with AQE off it gives `b,z,x,m,a,q,c,d`; range distribution is roughly lexical.
  - 23b's a/b/c shape happens to be where Spark's hash order coincides with lexical order.
  - The fork's rule is deterministic and equals Spark's answer on that shape only; 23b adopted this as Q-23b-2.
- **COW-BYTES (#301).**
  - RePark's "vanished-sum" pin counted a position delete that the fork removed and Spark keeps.
  - Spark keeps it because the rewrite outputs inherit the starting sequence number (data seq 9 = the delete's seq 9).
  - Round 4 ported the sequence GC. The verifier cited Java 1.11.0 `MergingSnapshotProducer` 973-1020 and `ManifestFilterManager` line by line, and attacked row safety: same sequence, equality deletes, branches, v3 DVs, retry.
- **Granularity (#302).**
  - Fork-written files are 1,694 B against Spark's ~1,153 B on the RePark shape. Java's own rules give 8/8/8 + 3 commits on fork sizes, which is what the fork now answers.
  - After the rebase onto #301, seven #301 `cow_bytes` cells asserted output counts written under the old one-output-per-group model. They were re-pinned to Java's packed-task counts; a focused verifier recomputed each by hand.
- **RPD (#304).** Spark's DELETE writes 8 file-scoped deletes, each referencing one data file at data seq 9. RPD rewrites them 8→8 in one `replace` and preserves seq 9. The #301 residue cell was re-pinned from 2 to 16, Spark's count.
- **Parquet size (#306).**
  - arrow-rs's `ARROW:schema` key-value is +528 B per file, and Java never writes it. Java writes `iceberg.schema` (194 B), which the fork lacked.
  - With both fixed, the file drops from 1,430 B to 1,098 B, against Spark's 1,146 B.
  - Two arrow-rs vs parquet-mr differences stay recorded (dictionary fallback, encoding labels).
- **RUSTSEC-2026-0285** (rustls < 0.23.45) fails the fork's `security_audit` (cargo-deny) on any PR that touches a Cargo.toml. RePark's own lock already carries 0.23.45 (23b checked).

## 4. Rulings (G-2)

- **Q-23a-1.** I deleted F-SHED-295's lines by script from the gate's exact list. No worker round was involved.
- **Q-23a-2.** COW-BYTES and DANGLE-2 are one root cause (measured).
- **Q-23a-3.** I accepted 23b's F-ROWID-ORDER-1 ask and slotted it before the RDF rows.
- **Q-23a-4.** COW-BYTES ran beside ROWID, ahead of GRANULARITY, because its root cause was already measured: most certain first.
- **Q-23a-5.** ROWID L-002 was settled by measurement. Spark's order is hash task order. The fork rule is deterministic and equals Spark on a/b/c only; the ledger and PR say exactly that.
- **Q-23a-6.** #301's perf P1 (no sequence GC, so kept deletes grow without bound) is in scope. I ported the GC in #301 and moved the unit up to Opus high, because round 2 had been rejected by the comment gate.
- **Q-23a-7.** I accepted the Opus round's rulings on #301:
  - R-04: a row-safe, named divergence for foreign-partition DVs.
  - A manifest-scope approximation that is sound.
- **Q-23a-8.** A gate-rejected round's mechanical fix-up (deleting comment lines) stays on the same actor. The tier move applies to the unit's next design round.
- **Q-23a-9.** I hold #303 as a draft. It adds `[features]` to `crates/catalog/glue/Cargo.toml`, and the fork AGENTS.md forbids any Cargo.toml edit without explicit owner approval. For the same reason I did not open a rustls lockfile bump.
- **Q-23a-10.** LIST-NULL-2: widen unbindable nested terms only in prune-only scans and in conflict/row-delta validation. `with_filter` and incremental scans stay loud; the critic's probe showed widening there returned 4 rows where Java returns 1.
- **Comment-gate practice.** Two rounds were rejected by the gate and sent back:
  - #301 round 2 truncated a `//!` line.
  - #304 round 1 added three `///` lines.

  I stripped nothing myself; I did delete my own F-SHED lines and fixed ledger wording in markdown.

## 5. Owner questions (with recommendations)

1. **Q-23a-A: fork #303, the Glue fault seam.** It adds `[features] commit-fault-injection = []` to `crates/catalog/glue/Cargo.toml`, with no dependency change. The fork AGENTS.md forbids any Cargo.toml edit without explicit approval. *Recommendation:* approve the feature line and merge. It is fully reviewed, and a compile-time gate is the safer design. The alternative is a property-only seam, which ships the injector in every build.
2. **Q-23a-B: RUSTSEC-2026-0285** (rustls < 0.23.45). The fork's cargo-deny `security_audit` fails on any PR that touches a Cargo.toml. The fix is a lockfile bump: rustls 0.23.45, aws-lc-rs 1.18.1, aws-lc-sys 0.45.0, rustls-webpki 0.103.15. `cargo deny check advisories` is clean after it and the glue/rest/iceberg crates compile. *Recommendation:* approve the bump (a TLS advisory should not be a dated ignore). RePark's own lock already carries 0.23.45.
3. **Q-23a-C: container accessors (Java parity).** Java binds `IS NULL` on list/map/struct exactly, through `PositionAccessor`s. The fork has accessors only for primitives, so #299 drops those terms to a DataFusion residual. That is correct under DataFusion, but a direct `table.scan().with_filter(xs IS NULL)` still errors loud. *Recommendation:* a fork card for container accessors plus Arrow row-filter support. It would also let nested null tests prune.
4. **Q-23a-D: fork branch `fix/f-list-null-accessor-2`** (not opened). Prune-only scans and conflict/row-delta validation filters widen unbindable nested terms; row filters stay loud. It is reviewed, but no in-tree consumer needs it once 23b fixes RePark's `predicate_dml`. *Recommendation:* leave it closed unless a raw-API consumer appears.
5. **Q-23a-E: fork-writer parity residue.** Two arrow-rs vs parquet-mr differences remain (§3): the first-page dictionary fallback, and the INSERT path's deliberate dictionary-off default (F-TARGET-FILE-SIZE-1). *Recommendation:* measure RePark's GRANULARITY cells after the #306 bump before deciding anything further.

## 6. Next run

1. **23b / the next bump.** Carry fork `171deddf` (#306), plus #305 once it merges. Measure the ICE-RDF-GRANULARITY-1 cells: with 1,098 B files, Java's rules pack two files per 2,500 B group, which may match Spark's 4. Measure the RPD, COW-BYTES and DANGLE-2 flips as well.
2. The owner's calls on #303 and the rustls bump (Q-23a-A, Q-23a-B).
3. A container-accessor fork card (Q-23a-C).
4. C-1 step 2 (after #303 merges): RePark's acceptance harness sets `glue.fault.drop-update-table-response` and asserts that `CommitStateUnknown` carries the `engine.operation-id` and that a reload finds the commit. AWS; owner-gated.

## 7. STATUS.md

This run made no STATUS.md edit; it had no fork-side STATUS clauses.

## 8. Rust-first roll-call

Every behaviour change tonight is fork Rust. RePark changed nothing in this lane; 23b owns the pins.

## 9. Scripts

New in `/tmp/oc-worker/_lib/`:
- `na-launch.sh`: `r7-launch` without the box-wide cargo wait.
- `na-review.sh`, and `na-review2.sh`, which takes `REF=` to pin a review to a specific head.
- `na-union.py`: union-resolves `task/todo.md`.
- `na-ceil.py`: resolves size-ceiling conflicts by taking the minimum.

## 10. Disk

- About 1.3 T free at launch and 1.1 T free at the end.
- The two lanes are **still on disk**: `/tmp/na-fork` (64 G) and its `--shared` dependent `/tmp/na-fork2` (23 G). The harness refused to remove them from inside this session.
  - Every branch head in them is on GitHub, or superseded by a pushed rebase.
  - Remove `/tmp/na-fork2` first, then `/tmp/na-fork`.
- Every `na-rv-*` review clone was removed as soon as its report was read. Stray reviewer report files were moved to `/tmp/oc-worker/na-fork/`.
- Durable artifacts:
  - `/tmp/oc-worker/na-fork/`: briefs, follow-ups, PR bodies, and every reviewer `out.json`.
  - `/tmp/oc-worker/na-oracle/`: Spark 4.1.2 oracles and their recorders — `list_null_truth`, `cow_bytes_truth`, `rpd_truth` and `rowid_order_truth`.
  - `/tmp/oc-worker/na-fork2/`: the #301 Opus round's brief, log and hand-back.

## 11. Fork main at the end

`43fcd243`. Tonight's merges are #297, #298, #299, #300, #301, #302, #304, #306 and #305, all tree-equal. 23b bumped through RP-33 (`587d3592`). **#306 (`171deddf`) and #305 (`43fcd243`) still await a RePark bump** (claims 03:15 and 03:41), with measurement at the bump:
- **#306:** the GRANULARITY cells.
- **#306:** the raw-reader type change, from `large_binary` to `binary`.
- **#305:** v3 delete-file counts.

## 12. Orchestrating-session note (added when this report was filed)

- The two leftover lanes of §10 were removed by the orchestrating session at 05:10 EDT on 2026-09-19 (the shared checkout first).
- Q-23a-A (the `[features]` line of fork draft #303) and Q-23a-B (the rustls lockfile bump) stay with the owner; run 24's
  preamble tells every fork lane to design around Cargo.toml edits until they are ruled.
- Run 24 launched at 05:05 EDT: the v1.5.0 read-performance campaign (24a, 24b) beside the parity campaign (24c, 24d).
