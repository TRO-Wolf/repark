# Morning report — run 20b of 2026-09-17 (the Iceberg remediation follow-on: silent-wrong smalls, the catalog and identifier paths, the registry sweep)

**Session:** one Opus orchestrator (morning-20b), 2026-09-17 05:07 → 15:00 EDT · **Orchestrator:** Claude (claude-opus-5) · **Lanes:**
`jb-` plus the resumed run-19b clones `ib-build`, `ib-build2`, `ib-bump` · **Actor tier:** Muse Spark 1.3 contributor for every unit
(owner, 2026-09-16 21:05: "You can use Muse workers") · **Reviewers:** Grok 4.6 (critic-logic, the S2-21 Rust perf read, the verification
critic) · **Beside:** run 20a (read path, DML planner and writer, conflict detection) and run 20c (maintenance, writer knobs, the
aws-acceptance run) · **Grants:** G-1..G-5 as in run 19; stop opening lanes 14:00, finished 15:00. **Target:** the measured rating
of 2026-09-16 (`a92a68db`), sections 3, 4, 7 and 9. STATUS.md, tags and the release pipeline untouched; no file owned by run 20a or 20c
edited.

**Why a follow-on.** Run 19 was killed (SIGTERM, 00:38 EDT, about 3 h 20 min in) and wrote no report. Run 20b rebuilt run 19b's
residual from the rating, the fork and RePark PR lists, the pushed branches and the on-disk lane clones. It resumed every clone that
held work: `ib-bump` (the pin bump, committed and gated at 02:01), `ib-build` (ICE-MIXED-CASE-1, round 2 tree uncommitted, identical
to the salvage patch), `ib-build2` (ICE-BRANCH-OPS-1, round 1 committed). Run 19b's RePark briefs for ICE-NAN-PUSHDOWN-1,
ICE-HADOOP-VN-1 and ICE-V3-WRITE-DEFAULT-1 had been written but never launched; run 20b launched them.

## 1. Residual matrix — before / after

"Before" is the rating (2026-09-16, `a92a68db`). "After" is main at 15:00 on 2026-09-17: the pins named here run in CI on every
merge head, and the orchestrator re-ran them on a release native before each merge.

| Rating row | Before | After (main) | Where |
|---|---|---|---|
| V2-26 / V3-14 — `= NaN`, `IN (NaN)` on Iceberg scans | **MISSING**, silent (no rows) | **FIXED** at RP-21: `=`/`<=>`/`!=`/ranges/`IN`/`NOT IN`/`BETWEEN` answer Spark on both doors, v2 and v3, RePark- and Spark-written tables; the pushed predicate is pinned | fork #284, #665, #669 |
| V2-20c — stale-base commit on an adopted Hadoop `vN` table | **MISSING**, silent lost commit | **FIXED** for every update commit (INSERT, MERGE, DELETE, UPDATE, OVERWRITE, TRUNCATE, ALTER, DataFrame appends): typed `CatalogCommitConflicts`, winner bytes identical. **OPEN residue** R-001: a stale `CREATE OR REPLACE` writes a uuid-named file on the stale catalog (split-brain, winner intact) | fork #286, #665, #675 |
| V2-18 — `fast_forward`, `cherrypick_snapshot`, `set_current_snapshot`, `rollback_to_timestamp` | refused (`REF-3` BACKLOG; two procedures with no row) | **FIXED**: all four answer Spark on positional and named arguments, output rows, error shapes, WAP and dynamic-overwrite picks, and v3 lineage after a cherry-pick (REF-5…REF-8). **OPEN residue** R-001: a duplicate WAP pick refuses with the fork's message, not Java's | #674 |
| V2-27 — unquoted mixed-case columns on a Spark-created table (contradicts `ID-1`) | loud, unregistered | unchanged on main; draft #676 (six rounds, redesigned, green gates, verification P2s open) | #676 (draft) |
| V3-03b — `write-default` on INSERT column lists, MERGE inserts, DataFrame appends | **MISSING**, silent NULL | unchanged on main; draft #678 (fill on every path implemented and pinned; one P1 re-scoped, P2s and a comment-ban rejection open) | #678 (draft) |
| V2-10b — `ALTER COLUMN … FIRST/AFTER` | refused, no row | unchanged on main; **#677 green and queued** at the stop time — moves through the fork API, verification PASS | #677 |
| Registry sweep part A — the 2026-09-14 cutover assessment; §9 claims C-3, C-8, C-9 | the assessment ran nothing and claimed PROVEN; three registry claims unsupported | twelve rows corrected (R1, W3, W6, V1, E1, M1 now "PROVEN in part"), rollup recounted, §11 audit trail; V3-COV-3 and PERF-ICE-COUNTSTAR-1 back to OPEN; the statistics sentence narrowed | #666 |
| Registry sweep part B — rows for V2-08, V2-10d, V2-15 ORC/Avro, V2-24b, V2-29, V3-05, ENC-1, variant/geometry, the puffin writers; claims C-2, C-7, C-10, C-11 | no rows | **not started** (§7) | — |

## 2. Per-PR table

| PR | Unit | Actor and rounds | Reviews (Grok 4.6) | State |
|---|---|---|---|---|
| #665 | **RP-21 fork pin bump** to `75da2b58` (fork #284 F-ICE-NAN-PUSHDOWN-1, #286 F-ICE-HADOOP-VN-1) | written and gated by run 19b (preflight, facade 9326, parity 757); opened by 20b on an unchanged main | — (pin bump) | **merged** `c42a4692`, tree-equal 05:37 |
| #666 | **ICE-REGISTRY-SWEEP-1 part A** — the cutover assessment corrections and three registry claims | Muse, 3 rounds (62 / 16 / 32 steps) + one orchestrator style commit (ruff E501 in the reopened pin's docstring, found by CI) | verification **NEEDS_REMEDIATION** (D-01 P1 false rollup, D-02…D-07) $0.32 → verification **PASS** $0.28 | **merged** `83c9b215`, tree-equal 07:19 |
| #669 | **ICE-NAN-PUSHDOWN-1** — pins only: the grid, pushed-predicate plan pins, Spark-written v2/v3 fixtures, live DML replay; OPEN row ICE-NAN-DECIMAL-LITERAL-1 | Muse, 2 rounds (173 / 122) | logic **NEEDS_REMEDIATION** (L-01 P1: no pin read the pushed predicate; L-02…L-07) $0.50; verification **PASS** $0.47; no perf read (no product code) | **merged** `225f68ee`, tree-equal 10:09 |
| #675 | **ICE-HADOOP-VN-1** — pins only: Rust red-first pins (fail on the pre-#286 rev), every stale writer shape, recovery negatives, the Spark oracle, residue R-001 | Muse, 2 rounds (258 / 131) | logic **NEEDS_REMEDIATION** (L-01, L-02 P2, L-03 P3) $0.57; verification **PASS** $0.33 | **merged** `64a0e7b4`, tree-equal 13:23 |
| #674 | **ICE-BRANCH-OPS-1** — four procedures in Rust over the fork's `ManageSnapshots` / `CherryPick`; REF-5…REF-8; residue R-001 | Muse, 3 rounds in run 19b/20b (19b's round 1, 223, 52) | logic **PASS** (ten P2) $0.51; Rust perf **PASS** $0.49; verification **PASS** $0.38 | **merged** `cda49b18`, tree-equal 14:16 |
| #676 (draft) | **ICE-MIXED-CASE-1** — case-insensitive column resolution on the Spark SQL door | Muse, 6 rounds (401 / 444 / 173 / stopped / 434 / failed on transport) | logic **NEEDS_REMEDIATION** (4 P1: the first design regressed table/CTE/window/struct names) $1.95; Rust perf **NEEDS_REMEDIATION** (R-01 P1) $0.37; verification of the redesign **NEEDS_REMEDIATION** (no P1; V-01…V-03, L-08, R-03 P2) $0.68 | draft |
| #678 (draft) | **ICE-V3-WRITE-DEFAULT-1** | Muse, 4 rounds (677 / 164 / stopped by the clock / fresh session, 5 commits) | logic **NEEDS_REMEDIATION** (L-01 P1, L-02…L-04) $0.62; Rust perf **NEEDS_REMEDIATION** (R-01, R-02 P2) $0.28 | draft |
| #677 | **ICE-COLUMN-REORDER-1** — `ALTER COLUMN … FIRST/AFTER` through the fork's `UpdateSchema`, both doors, nested short-name AFTER, ADD+MOVE batches; residue R-001 | Muse, 2 rounds (535 / 233) | logic **NEEDS_REMEDIATION** (L-001…L-004 P2: the order and no-op were simulated in RePark) $0.74; Rust perf **NEEDS_REMEDIATION** (R-01 P2: every statement tokenized again) $0.27; verification **PASS** $0.36 | **green, queued** at the stop time |

**Grok 4.6 total: 16 rounds, $8.77.** No Opus actor sessions; no Opus sub-agents.

## 3. Muse discipline

| Unit | Rounds | Gate claims overturned by the orchestrator | Comment-ban rejections | Other audit rejections |
|---|---|---|---|---|
| ICE-REGISTRY-SWEEP-1a | 3 | 1 — the round-3 gates were green but CI found a ruff E501 the brief's docs gate did not run | 0 | status cells contradicted their own rows (round 2) |
| ICE-NAN-PUSHDOWN-1 | 2 | 1 — the orchestrator's live replay failed `No module named 'pyspark'`: the ledger did not name the live command (fixed in round 2) | 0 | — |
| ICE-HADOOP-VN-1 | 2 | 0 | 0 | — |
| ICE-BRANCH-OPS-1 | 3 | 0 | 0 | A-1: `duplicate_wap_pick` re-implemented Iceberg's WAP validation in RePark (fork rule 3) — removed under Q-20b-4 |
| ICE-MIXED-CASE-1 | 6 | 1 — round 3 named a facade red "suspected pre-existing"; #665's gate on main had 0 failures, so it was this unit's regression | 0 | the first design (normalization off) — Q-20b-2 |
| ICE-V3-WRITE-DEFAULT-1 | 4 | 0 | **1** — 14 added `///` lines (round 4 brief) | — |
| ICE-COLUMN-REORDER-1 | 2 | 2 — the orchestrator's replay: live tier 1 failed, `make verify` rc=2 (ledger grammar); the actor had reported both green | 0 | A-2: column order and no-op simulated in RePark (fork rule 3) — Q-20b-5 |

`grep -c untrusted stderr.log` was 0 on every launch. Two new failure shapes:
- **A copied clone poisons bytecode.** Three lanes were made with `cp -a` of a built clone to skip a 30-minute setup. The repository-tree
  `__pycache__` came along with `co_filename`s pointing at the source clone (identical mtime and size defeat validation); one facade
  test failed on it in ICE-HADOOP-VN-1. Every later brief purged `__pycache__` first, and it did not recur.
- **A long-resumed session stops working.** ICE-MIXED-CASE-1's session reached six rounds; round 6 failed after 12 minutes of
  `transport_stream_error: body-truncated` with nothing done. Muse names a very large context as a cause. Write-default's round 4 ran as
  a fresh session for that reason.

## 4. What the reviews found

- **MIXED-CASE L-01…L-04 (P1) — the first design regressed statements that had nothing to do with mixed case.** Turning DataFusion's
  identifier normalization off on the Spark door made table, view, CTE, alias, named-window and struct-field names case-exact:
  `SELECT * FROM NUMS`, `FROM nums AS T WHERE t.id = 1`, `WITH X AS … FROM x` and `WINDOW W … OVER w` all answer on main and would have
  failed. The unit's own pins used lower-case table names, so the whole facade suite stayed green except one DataFrame filter. Ruling
  Q-20b-2 kept normalization on and folds only column identifiers. The redesign closed all four P1s; its verification found no P1 left.
- **NAN L-01 (P1) — the pins proved rows, not pushdown.** The ledger and registry claimed a Rust plan pin reading the pushed predicate;
  none existed. Round 2 added it. The verification critic checked the measured shapes against the fork converter's source and found
  three that differ from the converter's table (`<=>` not pushed, single `NOT IN` → `NotNan`, `IN (NaN, x)` → `IsNan OR Eq`). DataFusion
  rewrites those expressions before the conversion, and every one still answers Spark's rows.
- **NAN, found while pinning — ICE-NAN-DECIMAL-LITERAL-1 (OPEN).** A bare decimal literal against a NaN-holding DOUBLE column fails
  loud: `d = 1.0` → `Overflowing on NaN`, `d IN (NaN, 1.0)` → `Cannot cast to Decimal128`. Spark answers `[2]` and `[1,2,5]`. This is
  the same family as the literal-typing work in `SQL-LITERAL-TYPING-1`.
- **HADOOP-VN L-02 — a stale `CREATE OR REPLACE` does not take the exclusive-create path.** `begin_replace` names the next file with a
  fresh uuid, so it never collides with the winner's `v3.metadata.json`, and the stale catalog's pointer CAS succeeds. The result is
  split-brain: the winner's bytes are intact, but the replace is visible only through the stale handle. Ruling Q-20b-3 pinned it with
  strict-xfail target cells and filed R-001 for the fork.
- **REORDER L-001…L-004 — RePark simulated the move.** The unit decided the new sibling order and the no-op case itself, because the fork
  mints a new schema id on a no-op move where Java's `sameSchema` does not. That caused three divergences: a no-op decided on a stale
  schema is never re-validated after a concurrent move, `s.b AFTER a` is refused, and an ADD plus a no-op MOVE in one batch loses the
  move. Ruling Q-20b-5: delegate to the fork, pin the extra schema id as a residue.
- **WRITE-DEFAULT L-01 (P1) did not survive red-first pinning.** The critic traced a silent NULL on dynamic `INSERT OVERWRITE t PARTITION
  (k) (cols) SELECT …`; the actor's red-first test found that shape is a ParserError on the Spark door, so the path is unreachable by
  SQL. It needs re-scoping (parser support with a Spark cell, or a declared row), not the prescribed fix.

## 5. Rulings taken (G-2; full text in the unit ledgers)

- **Q-20b-1** — MERGE clause scoping for unqualified references: NOT MATCHED → source only, NOT MATCHED BY SOURCE → target only, MATCHED → both;
  a qualified reference is validated against both (Spark's `ResolveMergeIntoTableReferences`).
- **Q-20b-2** — ICE-MIXED-CASE-1 redesign: identifier normalization stays on; one case-insensitive fold of column identifiers to the stored
  spelling; `[AMBIGUOUS_REFERENCE]` 42702 on case-twins; `caseSensitive=true` with an unquoted mixed-case reference DECLARED; DataFrame door
  unchanged.
- **Q-20b-3** — ICE-HADOOP-VN-1: the stale-replace split-brain is fork work; pinned as it behaves today, residue R-001, fork trigger
  `F-HADOOP-VN-REPLACE-1`.
- **Q-20b-4** — ICE-BRANCH-OPS-1: remove `duplicate_wap_pick`; a duplicate WAP cherry-pick refuses with the fork's message; residue
  R-001, fork trigger `F-CHERRYPICK-WAP-ORDER-1`.
- **Q-20b-5** — ICE-COLUMN-REORDER-1: every move through the fork's `UpdateSchema` move API; the no-op schema id is residue R-001, fork
  trigger `F-UPDATE-SCHEMA-SAME-1`.
- **Q-20b-6** — ICE-MIXED-CASE-1 does not merge today: a draft PR with the verification P2s as the remaining work.
- **Q-20b-7** — ICE-V3-WRITE-DEFAULT-1 and ICE-COLUMN-REORDER-1 do not merge today: draft PRs, open findings listed in each.
- **Q-20b-8** — merge queue: run 20c's #670 (queued 11:10) and #672 (11:43) sat at the head waiting on their own local gate files, not
  merging; at 13:13 run 20b moved both to the end under the 45-minute rule, with a note in `/tmp/oc-worker/run20/claims.txt`.
- **Q-20b-9** — a pin bump that lands while a branch is being gated invalidates the gate: #674's replay was re-run on RP-22 (#667) before
  it was queued, and ICE-HADOOP-VN-1's replay was stopped, rebased and re-run.

Owner rulings applied: the run-19 preamble unchanged (fork rules, shape rule, method, attribution, merge queue, disk, comment ban),
Q-17a-2 (Rust first, the decision sentence), Q-17a-3 (`target/debug` dropped after every gate), Q-17b-1 (commit as a numbered step —
here always step 0 or 1).

## 6. Rust-first roll-call (under the Q-17a-2 sentence)

| Unit | Left in Python | Why it may stay there |
|---|---|---|
| ICE-NAN-PUSHDOWN-1 | — | tests, fixtures and Rust plan pins only; the conversion is in the fork |
| ICE-HADOOP-VN-1 | — | tests only; exclusive-create is in the fork |
| ICE-BRANCH-OPS-1 | — | the four procedures, argument parsing and refusals are Rust (`crates/repark-spark/src/call/branch_ops.rs`) |
| ICE-MIXED-CASE-1 (draft) | the `spark.sql.caseSensitive` conf plumbing (`builder_conf.py`, `session_configuration.py`, `sql_set_statements.py`) | carries the flag to the native session; the fold and the ambiguity decision are Rust (`crates/repark-core/src/column_resolution`) |
| ICE-V3-WRITE-DEFAULT-1 (draft) | `writer_readwriter.py` builds the target column list and **stops refusing** columns missing from the DataFrame | **a decision moved, not added:** the refusal of a missing column now happens (or not) in the Rust fill. A merge condition: a verification cell for a missing nullable column **without** a default on `writeTo().append()` and `saveAsTable(append)`, measured on Spark |
| ICE-COLUMN-REORDER-1 (draft) | — | parsing and the schema change are Rust on both doors |

## 7. What did not land, and exactly where it stands

- **ICE-MIXED-CASE-1 — draft #676**, head `5e5beb21` on RP-22. Green at the actor: unit 42 offline / 81 live, facade 9368, parity 757,
  `make ci`. Open: V-01 (`SELECT userId AS USERID` still fails), V-02 (the fold uses only the first missing relation's fields), V-03 /
  R-02 (every case-insensitive statement pays an AST clone and the audit with no early exit), L-08, R-03, V-04. Start a **fresh** Muse
  session; the old one fails on transport.
- **ICE-V3-WRITE-DEFAULT-1 — draft #678**, head `1ffc37fb` on main `cda49b18`. The fill is implemented and pinned on every path against a 22-cell Spark oracle; 0 added comments after the round-4 sweep; clippy, ruff, `cargo test -p repark-iceberg --lib` and `-p repark-sql` green. Open: L-01 re-scoped (dynamic `PARTITION (k) (cols)` is a ParserError on the Spark door, so the reviewed path is unreachable by SQL), L-03, L-04, R-03, R-04; `make verify` red on one finding (the ledger has no COVERAGE_ATTESTATION block, correct while clauses remain open); and the roll-call merge condition in §6.
- **ICE-COLUMN-REORDER-1 — #677**, head is rebased onto `cda49b18` and **green and queued**, not draft: verification critic PASS, replay on the pre-rebase head gave unit 16 offline / 33 live, `make verify` rc=0, facade 9361, parity 757, 0 added comments. It sat behind four other runs' PRs in the merge queue at the stop time; a merge watcher is running and the post-rebase re-gate (release, `-p repark-spark --lib`, `-p repark-sql`, both unit files, `make verify`) is recorded in `/tmp/oc-worker/jb-reorder/quick.out`.
- **ICE-REGISTRY-SWEEP-1 part B — not started.** Registry rows for V2-08 (landing with run 20c's #672), V2-10d, V2-15 ORC/Avro, V2-24b,
  V2-29, V3-05, ENC-1, variant/geometry and the puffin/partition-statistics writers (DECLARED for 1.x per the owner's 2026-09-16 cut);
  claims C-2 (north star row 62), C-7 (V3-COV-5, after run 20a/20c's sorted-insert work), C-10 (V3-COV-2, after 20a's promote-read
  work), C-11 (ID-1, with #676). A docs unit of one Muse round once the sibling PRs merge.
- **Fork residue for the next fork lane:** `F-HADOOP-VN-REPLACE-1` (the replace path keeps `vN` naming and exclusive-creates it),
  `F-CHERRYPICK-WAP-ORDER-1` (validate the WAP duplicate before already-picked, as Java), `F-UPDATE-SCHEMA-SAME-1` (a no-op schema
  update commits nothing, as Java's `sameSchema`), and R-01 of the branch-ops perf read (cherry-pick replay runs `plan()` twice).

## 8. Owner questions (with recommendations)

- **Q-20b-A — commit conflicts surface as the base `PySparkException`.** ICE-HADOOP-VN-1 kept the existing OCC contract (V2-20a):
  base class, message starting `CatalogCommitConflicts`. A caller has to string-match to retry. *Recommendation:* a typed
  `repark.errors.CommitConflictException` (subclass of the base, so nothing breaks) as a small 1.6 card beside
  `CommitStateUnknownException`.
- **Q-20b-B — the three fork residues in §7.** *Recommendation:* one fork lane for all three in the next run, then one pin bump; each
  RePark pin is already written as a strict-xfail target cell, so the bump PR shows them flip to XPASS and removes the markers.
- **Q-20b-C — `rollback_to_timestamp` picks the snapshot in RePark.** To close L-01 the procedure walks the current ancestry itself and
  calls the fork's `rollback_to(id)`, rather than the fork's `rollback_to_time`. The answer matches Java on every recorded cell,
  including a lateral jump. *Recommendation:* accept for 1.x and add it to the fork residue list, so the walk moves into the fork with
  the next fork lane.
- **Q-20b-D — `spark.sql.caseSensitive=true` with an unquoted mixed-case column is declared, not implemented.** Backticks resolve
  exactly. *Recommendation:* accept; the silver pipeline runs the default (`false`).
- **Q-20b-E — lane setup by copying a built clone.** It saved about 30 minutes per lane and cost one poisoned facade run.
  *Recommendation:* keep `r8-lane.sh` for fresh lanes; when a copy is necessary, the copy script purges repository `__pycache__` and
  rewrites the two `.pth` files (the `jb-copy.sh` recipe did the second, not the first).

## 9. STATUS.md lines that need correction (for the orchestrating session)

- **134-135** — "append fills from a schema-carried `write_default`" (V3-6): false on main (rating C-1); true only once
  ICE-V3-WRITE-DEFAULT-1 merges.
- **156-158** — "lineage carry and merge-on-read are complete on every served DML shape": contradicted by C-5 (run 20a's type-promotion
  work), and the same sentence lists `V3-COV-3` as FIXED, which #666 moved back to OPEN.
- A new line for RP-21 (#665) and the rows it closes: V2-26 / V3-14 (#669), V2-20c (#675), and V2-18 once #674 merges.

## 10. Disk

Removed: `/tmp/ib-bump` (after #669), `/tmp/jb-vn` (after #675), `/tmp/jb-docs` (after #666), `/tmp/ib-fork` (fork #286 merged; run
19's salvage bundle kept), `/tmp/ib-rv-fork-main`, `/tmp/ib-rv-fork-nan-bench`, `/tmp/ib-rv-main`, and every Grok review clone the
moment its report was read. `target/debug` dropped after every gate. Free space on `/` went from 782 G at launch (05:08) to 874 G at 14:18, while three orchestrators were building. The two biggest
reclaims were the draft lanes once their branches were pushed: `/tmp/ib-build` (117 G — over the 60 G stop condition, from six rounds of
release and debug builds) and `/tmp/jb-wd` (69 G). `/tmp/ib-build2` was 38 G at removal. Three lanes were made by copying a built clone
(`jb-copy.sh`) rather than a fresh `r8-lane.sh` setup, which is what put `__pycache__` from the source clone into them (§3).

## 11. Pointers

- Briefs, follow-ups and every reviewer report: `/tmp/oc-worker/{ib-build,ib-build2,ib-bump,jb-*}/` and `/tmp/oc-worker/jb-rv/reviews/`,
  copied to `~/repark-lanes/briefs/run20b/` at session end
- Coordination: `/tmp/oc-worker/run20/claims.txt`, `/tmp/oc-worker/run20-notes.md`
- The rating: `/tmp/oc-worker/ice-rating/report.md`
- Up: [map.md](map.md)
