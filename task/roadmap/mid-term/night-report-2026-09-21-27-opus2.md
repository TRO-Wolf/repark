# run27 — xo-opus2 report (v8 FINAL, 2026-09-21 21:10 EDT) — lane closed at the run's finish time

## UNIT 1 — IPI-32 catalog and session commands — CLOSED, 0 RESIDUES
Decision (a) "finish repark#758 as it stands" over the packet's suggested split. Rationale: the
3,588 added lines were overwhelmingly tests and mechanical registration, and the one genuinely
contested change (the planner-default flip that caused the 27-test smoke regression) was already
isolated and revertible, so a split would have paid the rebase/critic cost three times over for no
extra safety. Nine critic rounds. Merged 10:48, TREE-EQUAL, zero comment-ban hits.
Replay 0/11 -> 11/11 at merged main cbe6ec97df0584f375eac65438c9bd473123eab7
(ticks-xo-opus2/021/matrix-at-cbe6ec97.json). Clones /tmp/xb-cat and /tmp/xr-xb-cat REMOVED.

## UNIT 2 — IPI-05 write-audit-publish — 4/6 EQUAL, 2 HARNESS-FIXED pending re-score
Baseline was 2/6 EQUAL, not the packet's stated 0/6 (packet headline stale; verified three times, at
3dc40d98, at main 4f9acc89 and at the RP-45 pin dc233ebe, all identical).

### Fork ask — MERGED
iceberg-rust#341 (`with_stage_only` plus the second provider seam, bundled) merged TREE-EQUAL at
5a317f074133abf899dc038f2306c157750cf5eb after three critic rounds. ONE pin bump, claimed. The
run-25 draft #330 was superseded rather than revived.

### RePark PR — repark#787, OPEN, head 77a4bfe3d94447bee8320a96f15e5970f8d015e8
Root cause fixed: `write_to_branch.rs:375` bailed before the provider seam. The fix adds a separate
staged route (`wap_id_route_target` + `commit_write_staged`) using the plain target table with
`with_stage_only(true)` and `with_snapshot_properties({"wap.id": id})`; `publish_changes` is
registered as the 21st SUPPORTED_PROCEDURES entry and REF-3's refusal is retired.
Receipts: local gate CB=0 R=0 T=0 U=0 L=0 · lint 3/3 · comment-ban 0 · CI 9 pass / 2 skip / 1 pending
(the map.md guard that was red at tick 46 now PASSES) · critic r1 PASS (stale head, findings null).

### Critic r3 — NEEDS_REMEDIATION at 77a4bfe3, two classes, both in remediation
r3 confirmed the substance: the fork pin is consistent, all six required mutations went RED, the
near-miss fall-throughs are pinned, comment-ban is 0 and no production unwrap sits on the new path.
Its seven findings are two classes, neither touching the shipped behaviour.

**Class STALE-PROSE (V-001, V-002, V-003, V-007) — CLOSED by my own hand, commit 25d6d0b7.**
I re-measured the class across all six prose files in the PR rather than trusting the count: exactly
four hits, no others. (`call/map.md`'s "all twenty jar procedures" is CORRECT — params.rs is
unchanged by this branch and already carried publish_changes; 20 arms counted.) Fixed: the parity
REF-3 bullet now says a *plain INSERT* stages and names DELETE/UPDATE/INSERT OVERWRITE as staying on
main; the crates map no longer calls `spark.wap.id` inert; the python tests map no longer says
publish_changes and the wap confs fail closed; the call.rs count reads twenty-one.

**Class WEAK-PIN (V-004, V-005, V-006) — remediated, round ACCEPTED at 21:01, NOT YET PUSHED.**
Three pins asserted less than the recorded Spark cell they close: a no-op write, an extra ref, or a
wrong snapshot count would still have passed them, and the empty-refs first-write shape of
SC-SET-SQL-WAP was unpinned because every Rust test seeds first. One muse round (work order
ticks-xo-opus2/049/wo-xbwap-r4-weakpins.md, round /tmp/muse-worker/xb-wap/20260922T003020Z/, exit 0)
fixed all three and swept the WHOLE PR for the class. I accepted it on this evidence:
- diff vs 25d6d0b7 touches FIVE files, all tests and maps — `tests/wap_id.rs` (+200),
  `tests/refs_and_wap.rs` (+15), `python/.../test_ref_branch_tag_wap.py` (+35), two map.md. **Zero
  production files touched** (`write_to_branch.rs`, `wap.rs`, `call*.rs`, `Cargo.*` all untouched).
- class sweep verdict recorded per test: 13 HIT-and-fixed, 1 CLEAN-with-reason (a pure rewrite pin
  that performs no write), no test skipped.
- red-then-revert evidence per new assertion, with the mutation named and the failure text quoted
  (extra-ref break, silent-no-op break, first-write-falls-through break, mangled-stamp break); the
  native rebuild shows MATURIN_EXIT=0 and `_native.abi3.so` mtime 20:55:18.
- executor gates 5/5 exit 0 (wap_id, refs_and_wap, `make rust-fmt-check rust-clippy rust-panic-ban`,
  the python WAP test file, comment_ban).
- my own checks: comment-ban hits=0 at HEAD; skip-worktree list empty; 7/7 commits authored AND
  committed as TRO-Wolf with exactly the canonical `Authored-By: Muse Spark` trailer and no
  co-author trailer; the map rows are in the LAST commit (6a44ed78).
Premise correction the round volunteered and I accept: `TableMetadata` has no public refs enumerator
(`refs` is pub(crate) in the fork), so refs shape is pinned through the suite's established `.refs`
SQL door rather than a Rust accessor.
**Lane head is 6a44ed78b83a77fe51afcc7b8af2e30f52f9046a, LOCAL ONLY.** The remote branch and the PR
are still at 77a4bfe3d94447bee8320a96f15e5970f8d015e8. Nothing was pushed and nothing was queued: the
run's finish time passed 8 minutes after the hand-back, and a push here would have started a ~25 min
CI run and a ~35 min critic round that no one would be awake to read. Both classes' work is banked in
the clone; the next four steps are written out below and in the claims handover line.

### Cell status, measured at the PR head with the rebuild-#5 binary
| cell | before | at 77a4bfe3 |
|---|---|---|
| W-INSERT-WAP-BRANCH | EQUAL | EQUAL |
| SC-WAP-BRANCH-READ | EQUAL | EQUAL |
| W-INSERT-WAP-ID | DIFFERENT | **EQUAL** |
| SC-SET-SQL-WAP | DIFFERENT | **EQUAL** |
| P-CHERRYPICK-WAP | DIFFERENT | HARNESS-FIXED, pending re-score (snapshot-id only) |
| P-PUBLISH-CHANGES | REFUSED-REGISTERED | HARNESS-FIXED, pending re-score (snapshot-id only) |
Guards held: TP-WAP-ENABLED-NO-ID, W-INSERT-BRANCH, W-INSERT-SNAPSHOT-PROP-CONF all EQUAL, and the
regression trap P-PUBLISH-CHANGES-MISSING-ERR stayed "both refuse" — the bare-message pin worked.

### The two open cells were WRONGLY AUTHORED CELLS — ruled, fixed in the harness, pending re-score
P-CHERRYPICK-WAP and P-PUBLISH-CHANGES differ ONLY in raw 64-bit snapshot ids, which are generated
nondeterministically per engine and per run, so the cells as written were unsatisfiable by ANY correct
implementation. Every other key matches and the shape matches exactly (source_snapshot_id ==
current_snapshot_id in both engines, i.e. both fast-forward). harness.py:163/:238 already keeps a
snapmap that rewrites snapshot ids to symbolic S0/S1 (that is why W-INSERT-WAP-ID could go EQUAL);
cells_proc.py `_cp_wap` and `_pub` simply never re-read `.snapshots` after the WAP insert, so the
staged id and the CALL's returned ids were observed raw.
Raised as QUESTION `xo-opus2 ipi05-procid-symbolization`. **RULED ACCEPTED by the orchestrating session
at 19:57**: the cells are wrongly authored, not RePark. It applied the fix itself in the scoreboard copy of
cells_proc.py (backup `.bak-1958`) — exactly two lines, an `x.snaps()` after the WAP insert in each
cell, so the staged snapshot becomes S3 in both engines. I verified that diff: two lines, no other
cell's observations touched.
Status for the v1.5.0 gate: **HARNESS-FIXED, PENDING RE-SCORE — not EQUAL yet, NOT residue**, with the
RePark behaviour measured correct at 77a4bfe3. They score EQUAL on the morning matrix run on main.
I did not re-score them myself: the spark side of the comparison is a recorded baseline taken with the
old cells, so a repark-only re-run would compare symbolic ids against raw ones and prove nothing.
This also retires critic r1's P2 question V-002 by measurement, on top of the code evidence at
branch_ops.rs:271 (selection is BY wap id, not "latest staged").

## Two traps published to every lane this run
1. The map.md lockstep gate is PER-DIRECTORY, not one row: every directory holding a changed
   `*.rs`/`*.py`/`Cargo.toml` needs its own map.md in the same branch diff. Run
   `scripts/check_map_md.sh --base origin/main` yourself before a PR's first push.
2. compare.py's "zz" naming convention does NOT win: `repark-zz-` sorts BEFORE `repark-zzz-` because
   '-' < 'z', and later files overwrite earlier by cell id. Also harness.py does not write matrix.json
   — compare.py does. Together these silently reported three already-fixed cells as still DIFFERENT.
   Derive verdicts with your own out file loaded last (ticks-xo-opus2/047/verdicts-t47.txt).

## UNIT 3 — not started; gated on Unit 2 closing.

## CLOSE-OUT — what is owed on repark#787 (OPEN, nothing faked to hit the deadline)
Lane `xb-wap` (clone /tmp/xb-wap, branch feat/ipi-05-wap) and critic clone /tmp/xr-xb-wap are LEFT IN
PLACE deliberately: the PR has not merged, so its clones are not removable under the disk rule.
State at close: local head 6a44ed78 (clean, comment-ban 0, lint 3/3 green from the round), remote head
77a4bfe3, main 13b174d1db7e30f5d301016ca07e17a91481fbcc, #787 OPEN, isDraft=false, mergeable=MERGEABLE,
CI 10 SUCCESS / 2 SKIPPED at the PRIOR head. Disk 540G free.
The banked local gate (CB=0 R=0 T=0 U=0 L=0, 19:28) is **VOID**: main moved at tick 50 and its file
list OVERLAPS mine on two map.md files, so it no longer proves anything about main.
Next five steps, in this order:
1. Rebase /tmp/xb-wap onto main (re-read `git ls-remote` first). The two map.md conflicts resolve
   **ADDITIVELY — keep both sides**; then `scripts/check_map_md.sh --base origin/main` = 0 and the map
   rows must still sit in the LAST commit.
2. `git -C /tmp/xb-wap ls-files -v | grep '^S'` empty, then `make rust-fmt-check rust-clippy
   rust-panic-ban`, then `xgate.sh` — all five codes 0.
3. Push with `xpr.sh xb-wap "<title>" <body-file>` (pre-staged title/body in ticks-xo-opus2/044; a body
   carrying a Claude attribution line is refused).
4. A FRESH critic at the new head: `xreview.sh xb-wap <brief>` (LANE name, never the `xr-` name; ONE
   grok round at a time — a second round races the single per-user sandbox file and refuses in seconds).
   The r3 verdict is stale the moment the rebase lands; it does NOT authorise a merge.
5. Queue only on critic PASS **at the current head** + CI green + gate 5x0 + comment-ban 0 +
   isDraft=false and mergeable re-read in the SAME minute. Entry `787 IPI-05 <HH:MM>` (bare number),
   then `drive-merge.sh 787` under systemd-run.

## RESIDUES: NONE
Four of the six IPI-05 cells are MEASURED EQUAL at the PR head. The other two are HARNESS-FIXED
PENDING RE-SCORE under a ruling the orchestrating session ACCEPTED (wrongly authored cells, raw
snapshot ids), which is not a residue and is not a RePark defect. I did not invent residues to
decorate the close, and I did not queue #787 on a stale PASS to claim a merge.

## Ledger
Merges this lane: repark#758 (IPI-32, 11/11 replay) and iceberg-rust#341 (fork, TREE-EQUAL) — plus the
one claimed pin bump. Open at close: repark#787. Critic rounds: 9 on #758, 3 on #341, 3 on #787.
QUESTION raised: 1 (`ipi05-procid-symbolization`), ruled ACCEPTED. Unit 3 never opened — Unit 2 did
not close, so asking for a third unit would have been dishonest bookkeeping.
