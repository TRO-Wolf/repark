# xo-opus — run 27 report (closing out the work stopped at 07:54)
Started tick 1; this version written at tick 4, 10:40 EDT. Run ends 23:00 EDT. **IN PROGRESS.**

---

## THE TWO FINDINGS THAT MATTER BEYOND MY LANES

### F-1. One failure class explained the reds in two units — the exception CLASS, not the message
Units 1 (IPI-43) and 3 (IPI-22) both had red refusal tests, and neither was a semantics bug. In every
case the refusal MESSAGE was already correct and the exception CLASS was generic: the test asserts
Java's class (`IllegalArgumentException`, `UnsupportedOperationException`) and got `PySparkException`.
The tree's mechanism is `illegal_argument_error` / `IllegalArgumentMarker` in
`crates/repark-core/src/error_map.rs` (marker :13, constructor :24, classified :60-61, mapped :104),
with the established catch-and-re-raise pattern at `crates/repark-core/src/time_travel.rs:189`.
I corrected myself once here and it is worth recording: at tick 3 I wrote that IPI-22 needed a NEW
`UnsupportedOperationMarker`. **It does not.** The Unsupported path already exists end to end —
`ErrorKind::FeatureUnsupported` -> `Error::NotImplemented` (`error_map.rs:135`) -> `ErrorClass::Unsupported`
(`repark-common/src/lib.rs:70`) -> `UnsupportedOperationException` (`repark-python/src/lib.rs:50`). The
real defect is that the error never REACHES that path: the observed message carried the literal prefix
`External error: `, which proves `classify_datafusion_error` hit its `DataFusionError::External` arm
(`error_map.rs:57-67`), failed the `downcast_ref::<iceberg::Error>()`, and fell to `Other`. Note that arm
`return`s instead of continuing the peel loop, so a nested `DataFusionError` is never unwrapped.
Anyone else with a refusal test red on the class while the message matches: this is the fix, and it is a
site-local re-raise, never a global remap of `classify_iceberg_error`'s shared `DataInvalid` arm.

### F-2. A lane clone can be silently patched to a LOCAL fork checkout — green local gate, red CI
This one cost me a wrong assessment and would have cost a failed PR. In `/tmp/rc-incr` the root
`Cargo.toml` is marked **skip-worktree** (`git ls-files -v Cargo.toml` -> `S`) and its
`[patch.crates-io]` points at `/tmp/rc-fork/crates/iceberg` instead of the committed
`{ git = "…/iceberg-rust", rev = "3ed905c6…" }`. Because it is skip-worktree, `git status` is CLEAN and
the branch shows NO diff on `Cargo.toml`. Both halves of the damage were real:
- **False RED.** The gate's `T=1` was `cargo test -p repark-spark --test ice_read_perf_pins`, panicking at
  `benches/ice_read_perf/pins.rs:341`, `assertion failed: environment["fork_pin"].is_string()`.
  `fork_pin()` (`benches/ice_read_perf/report.rs:385`) parses the root `Cargo.toml` for `rev = "`; a path
  patch has none. The branch touches neither `benches/` nor `Cargo.toml`. A lane artifact, not a
  regression. `/tmp/ndbuild`, whose `Cargo.toml` is normal and carries the real rev, gated `T=0`.
- **False GREEN, the expensive half.** The path patch let rc-incr COMPILE against fork API NOT IN THE PIN.
  `crates/repark-iceberg/src/catalog/changelog.rs:16` does
  `use iceberg::arrow::{ArrowReaderBuilder, ChangelogReader, changelog_arrow_schema};`, and those symbols
  come from fork commit e0bfab85, which is not in pinned 3ed905c6. A RePark PR opened from that branch
  today would fail to COMPILE in CI.
Two commands, no build, before any PR: `git -C /tmp/<lane> ls-files -v Cargo.toml` and
`grep -n '^iceberg = {' /tmp/<lane>/Cargo.toml`. Written to claims.txt at 10:34 for every lane.

---

## PER UNIT

### 1. IPI-43 sort/zorder string parser — lane ra-rdfsort, branch fix/ipi-43-rdf-sort-zorder
**Sound in the stopped work:** all 3 commits clean, Rust fully green, comment gate clean. Nothing redone.
**Assessment:** gate RED `CB=0 R=0 T=0 U=1 L=1` — and `U=1 L=1` is ONE failure class counted twice, because
the gate's `L` is a pytest re-run against release, not a lint code. 5 of 13 tests in
`python/repark/tests/test_ice_rdf_sort_parse_1.py` failed, two causes, both inside existing packet rulings:
 - CAUSE 1 (4 tests): helper `_file_ids` (:124) selects `partition` from `{table}.files`, but the
   UNPARTITIONED `_flat_table` fixture (:93) has no `partition` column — that column is IPI-20's (unit 5,
   unlanded). Packet §11 A-1 rules `SELECT file_path FROM t.files` + pyarrow and says verbatim "Do not wait
   on IPI-20". Test-side fix; the mk3 partitioned assertions stay byte-identical.
 - CAUSE 2 (`test_rdf_zorder_refusals_are_the_forks`, cell C-009): F-1 above, scoped to the z-order
   validation path only so `call_rewrite_unknown_strategy_matches_spark_message` keeps its class.
Fork half #323 is MERGED and on main via RP-40, so packet T-2 is satisfied.
**Trap recorded:** the pytest file is `test_ice_rdf_sort_parse_1.py`, not `test_ice_rdf_sort_1.py` as
packet §8/A-5 says; crate entries are `repark-spark,repark-iceberg`, never `--test+alter_write_order`.
**WOs cut:** round 4 (`ticks-xo-opus/002/wo-ra-rdfsort-r4.md`). **Muse rounds:** r4 in flight.
**Critic:** not yet. **PR:** none yet. **Cells:** pending.

### 2. #733 session write confs (IPI-14) — lane ndbuild, PR repark#733 (OPEN)
**Sound in the stopped work:** round 8's four CI regressions were ALREADY FIXED by three
committed-but-unpushed commits (045f1e9e, 6fd98b63, e3da96be); I read all three and judged them sound.
The reported semantic conflict with main in `crates/repark-spark/src/write_to_branch.rs` was **STALE** — no
clerk rebase was needed. Local gate all zero.
**What was actually left:** ONE line. `Rust lint` was failing with `error: unused import: super::super::*`
at `crates/repark-spark/src/tests/parquet_dictionary.rs:1` (`-D unused-imports` via `-D warnings`), from
commit 6fd98b63. Ruling: DELETE line 1, do NOT `#[allow]`; `use super::common::*` is used and stays.
**WOs cut:** `ticks-xo-opus/003/wo-ndbuild-lintclerk.md` (Devin clerk — free tier, correct for a red lint).
Because clippy ABORTS at the first failing crate, the order tells Devin to iterate `make rust-fmt-check` +
`make rust-clippy` to 0 rather than stop at the known error. **In flight.**
**Critic:** to run on the ROUND-8 DELTA ONLY (87701e3e..HEAD), not the 52-commit branch.
**PUSH TRAP, hit twice:** `xpr.sh` failed with "stale info" — `--force-with-lease` cannot resolve a lease
ref when pushing to a URL rather than a named remote and the lane has no remote-tracking ref. It was a pure
fast-forward, so I pushed plainly.

### 3. IPI-22 incremental and changelog reads — lane rc-incr, branch fix/ice-changelog-1
The hardest unit on the slate, and it resolved in the opposite direction from the one the unit list feared.
**Sound in the stopped work:** 3 commits, 39 files/+4716, comment gate hits=0, RePark half (packet steps
8-13) present. 28 of 32 pytest pass.
**The changelog SEMANTICS are not the problem.** All four failures are refusal tests and all four are F-1.
The refusal text is already right: `DataInvalid => Starting snapshot (exclusive) <id> is not a parent
ancestor of end snapshot <id>` and `FeatureUnsupported => Delete files are currently not supported in
changelog scans`. This **proves addendum A-1 is implemented** (D-7 was wrong: Spark does REFUSE delete
files in a changelog scan, and the refusal fires with the right text) and that A-6's "the refusals live in
Python" holds. So no QUESTION on changelog semantics was needed.
I pinned the assertion shape to be sure: `_assert_error` asserts the class EXACTLY at
`test_ice_changelog_1.py:126` and the message as a SUBSTRING at :127 (after blanking 4+ digit runs). Line
126 is what fires — so these are purely class failures, and an added message prefix is harmless.
**`T=1` diagnosed: not a bug.** See F-2 — the perf-pin failure is the lane's path-patched `Cargo.toml`.
**CORRECTION I OWE THE CAMPAIGN — this unit has an unlanded FORK half.** At tick 3 I wrote that addendum
A-10 made the fork DataFusion seam optional and steps 1-7 legitimately skipped. Wrong. The fork half
exists: `/tmp/rc-fork`, branch `fix/f-changelog-reader-1` @ e0bfab85, F-CHANGELOG-READER-1 — an Arrow
reader for `ChangelogScanTask`. ONE clean PR-sized commit on top of CURRENT fork main (6 files, +514,
ledger + GAP_MATRIX R123 amended), comment gate hits=0, TRO-Wolf identity correct, not a `wip:` commit.
It was UNPUSHED, had NO PR, and lane `rc-fork` was in nobody's lane list. I have taken it.
I am LEAVING its `Authored-By: Claude (claude-opus-5)` trailer: the trailer names the model that actually
wrote the code, and Claude Opus did write it in an earlier run. A truthful trailer should not be rewritten
to satisfy the current executor-tier ruling.
**Sequencing this creates:** RePark IPI-22 can only go green in CI after the fork branch merges AND a pin
bump carries it. **RP-42 is therefore a dependency of this unit, not only of the fork units.**
**WOs cut:** `ticks-xo-opus/004/wo-rc-incr-r1.md` (RePark side, exception classes only; written, waiting on
a Muse slot). **Critic:** fork half launched 10:33 (lane xr-rc-fork) with five named soft spots — chiefly
what `ChangelogReader` does with a task kind it does not handle (a silent skip would be a P1 short
changelog), and whether `changelog_arrow_schema` is byte-identical to what the reader actually appends.
**Done condition:** 11 cells EQUAL, ROW ORDER INCLUDED (packet §10).

### 4. IPI-19/56/37 schema evolution on write — lane re-build2, branch fix/ipi-19-56-37-schema-evo-write
**The stopped 16-file wip is SOUND and on-ruling — I read the diff myself and it is not being redone.**
- `MergeSpec` + the new `schema_evolution` flag moved to `write/merge/spec.rs` WITH the
  `scripts/check_rust_file_size.py` ratchet in the same commit = exactly A-8.
- All five types moved intact; the ~49-line shrink is `///` doc comments correctly SHED ON THE MOVE, not
  lost code. `NotMatchedBySourceClause` correctly stayed in its own module. Comment gate on the whole
  branch: hits=0.
- `merge/mod.rs:106-117` unions the schema FIRST, then builds `write_schema`, then `expand_star_clauses`
  against it = A-8's "stars against the EVOLVED schema".
- Session-conf template in `crates/repark-functions/src/merge_schema/` = A-7. D-6's sniffer
  `strip_schema_evolution` exists with tests; the flag threads router.rs:220 -> merge.rs:57 -> spec ->
  merge/mod.rs:106. `merge.py:139 withSchemaEvolution()` exists. 27 tests in test_ice_merge_schema_1.py,
  A-10 pinned, A-4's dual-alias route taken.
**Four gaps, all in the ready WO** (`ticks-xo-opus/003/wo-re-build2-r2.md`): G-1 prove A-5/D-3 is ONE commit
by measuring `len(md.snapshots)` — the pin is MISSING and `union_source_schema` returning a re-loaded Table
is the shape of a separate metadata commit; G-2 A-9(c)'s DataFrame snapshot pin as its own assertion;
G-3 the staging ledger is missing; G-4 `docs/spark-sql-iceberg-parity.md` untouched and EX-DF-9 must be
NARROWED (A-11). The WO also orders `git reset --soft d87e3c7a` so no `wip:` title survives.
**Open question raised** (claims.txt 10:38, `QUESTION IPI-19/56/37 A-11`): keep RePark's `target.`/`source.`
qualifiers and bare-`"id"` sugar once Spark's short-name form works? Spark RAISES on them. My lean and the
WO's scope: A-4's dual-alias route — Spark cells EQUAL, RePark spellings keep working, EX-DF-9 narrowed not
retired, because Spark-exact breaks `python/repark/tests/test_examples_dataframe_b.py:52-77` and any user
who copied the RePark docs; commit 64409e13 already took this route. The orchestrating session replied at
10:29: proceed on the lean (it is INDEX.md decision 22's standing recommendation), say in the PR body and
registry row that RePark accepts qualifiers Spark rejects, and do not hold the queue for it. **Not blocked.**

### 5. IPI-20/23 metadata columns and time travel — lane rc-meta — NOT STARTED
Assessed only. NO real commits: one wip 7cc3b6f9 (7 files, +1014) — `metadata_columns.rs` (389) in
repark-core, `catalog/metadata_columns.rs` (308) in repark-iceberg, `tests/metadata_columns.rs` (288),
router.rs +20. No ledger, no map.md, no pytest, no registry row. This is an unfinished FIRST round, not a
near-done one, and it is the largest remaining build on my slate.
This unit owns the missing `$files.partition` column. Unit 1 works around its absence per packet A-1; if
rc-meta lands it, that workaround stays valid and must NOT be reverted.

### 6a. FORK container accessors — lane rd-ca, fork draft PR iceberg-rust#326
**Two S1 findings from the earlier critics, both silent wrong answers, both worth recording beyond this
unit:** V-01 — on a parquet file with no embedded field ids, a container `IS NOT NULL` keeps ZERO rows
(`build_fallback_field_id_leaf_lists` maps only top-level positions). L-01 — `PageIndexEvaluator::not_null`
uses `MissingColBehavior::CantMatch`, and a container field id is a parquet GROUP, so `skip_all_rows()`:
`st IS NOT NULL` returns empty where Spark answers `[1,3,4]`. Both bite any lane reading parquet written
outside Iceberg (add_files/migrate) or pushing a predicate on a struct/list/map column.
**WOs cut:** round 2a = brief steps 1-4 (`ticks-xo-opus/001/wo-rd-ca-r2a.md`), in flight. Nine steps will
not fit 400 Muse steps, so 2b = steps 5-9 (uncaught mutations V-02/V-03, Q-26d-7, mutation evidence for
ledger §7, ledger/maps, gates). My preamble orders Muse to JUDGE the wip c9e8d246 rather than trust it
(`page_index_evaluator.rs` +9 = partial S1-B, two partial test fixtures) and to `reset --soft` onto
fea36608 so no `wip:` commit survives.
**Note for fork lanes:** muse warns `/tmp/rd-ca/CLAUDE.md is ignored because AGENTS.md takes precedence`.
Harmless here because the WO carries the rules — but if a Muse round ever ignores house rules in a fork
lane, this is why.

### 6b. FORK position_deletes scan F-POSDEL-SCAN-1 — lane rd-posdel — CRITIC RETURNED
**Sound in the stopped work:** Devin's round (which finished after the 07:54 stop) concluded cleanly —
8 commits, own gates exit 0, comment-ban hits=0, 7 self-reported mutations.
**Critic verdict: NEEDS_REMEDIATION — three P2 findings, NO P1.** The scan code is sound; the LEDGER
overclaims. All four soft spots I named came back clean: the no-filter `FeatureUnsupported` refusal at
`position_deletes.rs:375-381` exists and is an honest declared divergence (unreachable under `AlwaysTrue`,
NOT a silent drop); `from_fields_unchecked` is `pub(crate)` with one caller, not public as I had feared;
the `partition.rs` `#[path]` test split dropped no assertion (62/62 survive); and the critic independently
RE-MEASURED `pd_evolved_v2` and CONFIRMED the copy-on-write answer the evolved-spec pin rests on.
The three findings, all leans adopted: **V-001** C-010 claims both `ManifestEvaluator`s are load-bearing,
but sabotaging both left 7/7 green — because both filters are `AlwaysTrue` and `eval` short-circuits;
demote to OPEN (a real prune pin needs a filter surface this unit declared out of scope). **V-002**
`MetadataScope::CurrentSnapshot` is implemented but UNPINNED — every fixture rewrites only the current
snapshot's manifest list, so an `AllSnapshots` swap would redden nothing; add a parent-only decoy delete
file and assert zero rows. **V-003** C-014 claims no doc says scan-refused, but three still do
(`position_deletes.rs:70`, `metadata_table.rs:68-70`, `:218-219` — a file this branch never touches).
**WOs cut:** `ticks-xo-opus/004/wo-rd-posdel-r2.md` — written, waiting on a Muse slot. It requires the
V-002 pin be PROVEN load-bearing (flip to `AllSnapshots`, see it fail, restore) and gives an explicit
comment-ban procedure for V-003: edit the stale line to make it true, and if the gate flags it, DELETE the
false clause rather than rewrite it — never add a comment line.

### 6c. FORK ORC and Avro data files (IPI-41) — lane rb-fork — NOT STARTED
Assessed only. NO commits: one wip 60ee3593 (16 files, +3889) that is **ALL ORC and ZERO Avro**. So the
unit list's "Avro first if it is separable" does not apply to the existing work — the wip is one ORC writer
(`orc_writer.rs` + 7 submodules + 6 test files, a 778-line `orc_writer_tests.rs`) plus
`spec/table_properties.rs` +9. Too large for one PR. Cut along the submodule seams (orc_type/encode/column,
then footer_write/metrics/null_repair, then writer + tests), or split Avro out as its own later unit.
Packet owner ruling at its top: every ORC default = what Java Iceberg / Spark does, and ORC `Cargo.toml`
edits ARE approved — the one exception to COMMON.md trap 2.

### 7. RP-42 pin bump — NOT CLAIMED, correctly
No fork PR of mine has merged yet. Main already carries RP-41 (882f1300 -> 3ed905c6) and RP-40. Per F-2 and
unit 3, the bump must carry F-CHANGELOG-READER-1 along with whichever of #326 / posdel / ORC land. I will
claim it in claims.txt before bumping and only when no other bump is open across all lanes.

---

## RESIDUES (numbered, with cell names) — provisional
1. **C-010 evaluator keying** (F-POSDEL-SCAN-1 ledger) — demoted PROVEN -> OPEN. Cannot be closed without a
   filter parameter on `PositionDeletesTable::scan`, declared out of scope for IPI-45.
2. **IPI-22 cells (11, incl. row order)** — cannot read EQUAL in CI until the fork half merges and RP-42
   lands. Sequencing residue, not a semantics residue.
3. **`$files.partition`** (IPI-20, unit 5) — absent; unit 1's cells are closed via the packet A-1 pyarrow
   route rather than the metadata column.

## HOUSEKEEPING NOTES WORTH KEEPING
- `gh run view --log-failed` refuses while a run is in progress, but
  `gh api repos/TRO-Wolf/repark/actions/jobs/<job_id>/logs` returns the FULL log of any job that has already
  CONCLUDED, even with siblings pending. Get `<job_id>` from the URL `gh pr checks <n>` prints. This got me
  #733's lint failure immediately instead of waiting out a 25-minute run. Never reproduce a red CI check
  locally just to read it.
- The local gate's five codes (CB R T U L) do NOT include fmt/clippy; CI does. A green local gate is not a
  green CI on a Rust branch. And `L` is a pytest re-run against release, not a lint code.
- `r7-launch.sh` BLOCKS up to 600s while any cargo/rustc/maturin runs box-wide — always wrap it in
  `systemd-run`. Muse cap is TWO at once; critics, gates and orchestrator reproductions do not count.
- A large NEGATIVE line delta on a moved block is EXPECTED under the no-comments rule. Count the moved
  TYPES, not the lines, before calling it lost code — this is how unit 4's wip cleared.

---

## TICK 5 (10:43–10:50 EDT) — appended, not a rewrite

### Unit 2 (#733) — the last red CI check is CLOSED
Devin clerk round `/tmp/devin-worker/ndbuild/20260920T143435Z/` exit=0, hand-back CONCLUDED. It made exactly
the ruled change and nothing else: deleted the unused `use super::super::*;` glob at
`crates/repark-spark/src/tests/parquet_dictionary.rs:1` (the file resolves everything through
`super::common::*` and the `super::session_write_conf` module path, so the glob bound nothing), plus a 3-line
note in `crates/repark-spark/src/tests/map.md` that the lockstep pre-commit hook demanded in the same commit.
`make rust-fmt-check` 0 and `make rust-clippy` 0 — the workspace `--all-targets` run surfaced nothing behind
the known first error. My checks: comment gate hits=0; diff read in full (2 files, +3/-1); lane Cargo.toml `H`
with the honest 3ed905c6 pin. Commit 604d8f74, trailer `Authored-By: Devin (SWE-2) <noreply@cognition.ai>`,
committer TRO-Wolf. Pushed 41cf0aa6..604d8f74 as a verified fast-forward (the `xpr.sh --force-with-lease`
"stale info" trap again). CI at 10:47: **6 pass / 3 pending / 0 fail.**
Remaining for this unit: CI green -> Grok critic on the round-8 delta (87701e3e..HEAD) ONLY -> merge queue.

### Two cross-lane findings (these are the tick's real product)

**FINDING 3 — the tick-4 lane check I published was incomplete; it passes a damaged lane.**
`/tmp/rc-meta` had `Cargo.toml` marked `H` with the honest git rev, but **`Cargo.lock` skip-worktree and
patched on disk**, with the `source = "git+…iceberg-rust?rev=…"` line for the `iceberg` package stripped — the
lock resolved iceberg as a PATH dependency. `git status` clean, `git diff` empty, `ls-files -v Cargo.toml`
says `H`. The sole symptom is that `git checkout`/`git rebase` refuses with *"Your local changes to the
following files would be overwritten by checkout: Cargo.lock"*. On an otherwise-clean lane, that message is
proof of patching, not a nuisance. Corrected check and repair recipe are in claims.txt and in my state file.
I swept all nine of my lanes: only rc-incr (both files, already known) and rc-meta (lock only, new) were hit.
Fork lanes legitimately read `path = "./crates/iceberg"` with no git source — not damage.
rc-meta repaired (`update-index --no-skip-worktree` + `checkout --`) and rebased.

**FINDING 4 — a stale fork pin arising from plain lane lag, with no diff to show for it.**
ra-rdfsort, re-build2 and rc-meta each carried `rev = "882f1300…"` (pre-RP-41) while main carries
`3ed905c6…`, purely because they were 2–3 commits behind; none of the three edits `Cargo.toml`. Consequence:
**their local gates ran against the previous fork.** A green local gate on a lagging lane is not evidence
about current main. re-build2 and rc-meta rebased by me (re-build2 -> the rebased head, comment gate still hits=0;
rc-meta -> f9c4ffb8); ra-rdfsort deferred only because a Muse round is live in its clone, and its hand-back
procedure now begins with the rebase and a re-gate.
Derived trap, hit immediately: rebasing **rewrites SHAs and staleness a waiting work order.** My own
`wo-re-build2-r2.md` pinned `git reset --soft d87e3c7a`, dead the moment I rebased; repointed to 0ad9979f
(and 541e8c50->the rebased head, 64409e13->b7c81042, 47c496f9->acf2cdbc).

### Unit 5 (IPI-20/23) assessed — the wip overreaches and must NOT be launched as it stands
The 7-file / +1014 wip is RePark-only and tries to serve all five metadata columns. The packet is **fork-first**
(§5 steps 1–8, including a pin bump, precede the RePark half). Measured against the current fork
(/tmp/rc-fork @ e0bfab85): `unified_partition_type` exists, so that much compiles — but **`MetadataScope` has
only `CurrentSnapshot | AllSnapshots`** (A-3's `Snapshot(i64)` unbuilt) and **`include_deleted` appears nowhere**
(A-2's `_deleted` extraction unbuilt). The wip's own `deleted_returns_the_deleted_row_marked` therefore cannot
pass: the survival mask lives in `reader.rs:1035-1086` and is computed after the transformer runs, so RePark
cannot see it, and A-2 explicitly forbids the `Datum::bool(false)` fallback. A-1 likewise puts `_partition` in
the fork (a struct cannot ride the primitive-only `constant_fields`).
Ruling recorded: the separable first PR is packet step 10, **RePark-only `_file` + `_pos`**, which the fork
serves today. `_spec_id` / `_partition` / `_deleted` and the AS-OF composition are a fork unit plus a pin bump
and will not fit before 23:00 — they are tracked as a residue. Two greps saved a wasted Muse round.

### Residues added this tick
4. **IPI-20/23 fork prerequisites unbuilt** — `MetadataScope::Snapshot(i64)` (addendum A-3) and the
   `apply_pos_aware_batch` extraction plus `include_deleted` flag (addendum A-2), together with the
   struct-constant `ColumnSource` for `_partition` (A-1). Without these, cells for `_spec_id`, `_partition`,
   `_deleted` and every `t.<meta> … AS OF <v>` composition cannot read EQUAL. Needs a fork unit and a pin bump.
5. **rc-incr's lane patch is intentionally still in place** — repairing it before RP-42 would stop the lane
   compiling (`changelog.rs:16` needs fork symbols absent from pinned 3ed905c6). Repair is a post-RP-42 step,
   not an outstanding defect.

## TICK 6 (10:54–11:02 EDT)
- **Unit 1 IPI-43 (ra-rdfsort): Muse round 4 handed back GREEN** — exit 0, local gate `CB=0 R=0 T=0 U=0 L=0`,
  13/13 pytest, comment gate hits=0. Two commits: `03c2ec15` partition-free `_file_ids_flat` for the four
  flat-fixture tests (mk3 partitioned assertions byte-identical, packet §11 A-1 honoured, IPI-20 not waited
  on) and `f6233083` re-raising the fork's sort/z-order validation refusals as IllegalArgument.
  Muse reported a THIRD defect the brief's two-cause framing had masked: the fork's unsorted-table refusal
  ("Cannot sort data without a valid sort order") also surfaced as PySparkException, so the gate could not
  have gone green on two causes alone. I read the diff: the remap is a three-needle text match applied ONLY
  at the `action.execute()` call site inside `run_rewrite`, so the unknown-strategy and binpack class pins
  (parsed before execute) are untouched — accepted, and flagged to the critic as the judgement call it is.
  Then the hand-back procedure: rebase onto main (pin `882f1300` -> `3ed905c6`) -> `2de66930`, comment gate
  still 0, local gate RE-RUN against current main (running at tick end). Next: fmt+clippy, PR, critic, queue.
- **Unit 3 fork half (rc-fork): critic returned NEEDS_REMEDIATION** — one P1, two P2, all leans adopted.
  P1 V-001: `read_one_task` never inspects the task kind, so a `DeletedRows` task emits every
  live-before-change row labelled DELETE — a silently wrong changelog; the ledger's "DeletedRows is still
  unread" is false. P2 V-002: served-batch schema equality against the public `changelog_arrow_fields` is
  unpinned (flipping reserved-field nullability left all 3 tests green). P2 V-003: ledger C-005 overclaims a
  message pin that is kind-only. What the critic CONFIRMED sound: the `_change_type`/`_change_ordinal`/
  `_commit_snapshot_id` wrapping, the C-002/C-003 mutation-proven pins, reserved names/ids, no production
  `unwrap`, no comments, no weakened test. Work order `.../006/wo-rc-fork-r2.md` cut (66 lines, three steps,
  both P2s requiring the mutation be shown load-bearing); V-003 ruled as ledger-only so the branch does not
  reach into another unit's planner test.
- **Unit 6b (rd-posdel): Muse round 2 LAUNCHED** on the critic-remediation work order, after rebasing the
  lane onto fork main `0cb6ebd3` (8 commits replayed clean, comment gate 0) and repointing the work order's
  stale head SHA `aad970c0` -> `2146a1d3`.
- **Unit 6c (rb-fork): rebased onto `0cb6ebd3`** — which is exactly the rustls-chain bump the packet's ORC
  dependency work needs; doing it now saves the executor a hand re-resolution.
- **Unit 2 (#733): CI 7 pass / 2 pending / 0 FAIL**, including the `Rust lint` check the clerk round fixed.
  The four round-8 regressions are closed. Awaiting `build + import smoke` and `Rust test (workspace)`.

## Tick 7 — 11:02-11:18 EDT

**Unit 2 (#733) reached CI-green and went to the critic.** `Rust test (workspace)` turned green
(12m46s), so every required check on repark#733 now passes — 9 pass, 2 skipped, and the only thing
still moving is `build + import smoke (debug, host)`, which is not required. I cut
`ticks-xo-opus/007/critic-ndbuild.md` and launched it (lane `xr-ndbuild`). The brief is deliberately
SCOPED: the branch is 52 commits and its earlier rounds were already reviewed, so the critic reviews the
round-8 delta only — `045f1e9e`, `6fd98b63`, `e3da96be`, `d89f8130`, `604d8f74` — and is told in so many
words that everything arriving through the merge commit `41cf0aa6` (the RP-41 pin bump and other lanes'
night reports) is out of scope. There is no IPI-14 plan packet (it is a run-26 unit), so the brief points
at `task/ledgers/staging/ice-session-write-conf-1-ledger.md` and the parity-doc section instead, and
names the round's two judgement calls explicitly: the parquet-dictionary default in `6fd98b63` (is it
really Java's, and is it pinned by a test that fails if it flips?) and the mixed-case SET resolution in
`045f1e9e` (case-insensitive the way Spark is, or lowercased at one call site?).

**Unit 6c (ORC) — the packet's required measurement, made, and it changes nothing except that the work
is now defensible.** The wip is 3,889 lines of hand-rolled ORC container encoding — its own magic,
postscript, stripe footer, varints and the deflate crate compression — while `orc-rust` is ALREADY a fork
dependency and ships `orc_rust::arrow_writer::ArrowWriterBuilder`. The packet's owner ruling (2) demands
that choice be measured, not assumed, so I measured it: orc-rust hard-codes `attributes: vec![]` on
every ORC type (`arrow_writer.rs:209`) with no builder to set them, so it cannot write Java Iceberg's
`iceberg.id` / `iceberg.required` attributes; and `close(self) -> Result<()>` returns nothing and there
is no statistics surface, so packet fact M-5's per-column metrics are unreachable through it. 0.9.0 has
both holes. Verdict: the hand-rolled writer stays, orc-rust keeps the read path and — the part worth
protecting — the independent write ORACLE in the tests (`orc_writer_tests.rs:269` writes with orc-rust,
`:426` reads our file back with it). Written to the claims file as the ledger's required "choice and its
reason". I also ran the gates the stopped round never ran: comment gate `hits=0`, and every file under
the fork's 1000-line ratchet (largest 778).

**The ORC split, and why it is vertical.** `make check-clippy` on this fork is
`-D warnings`, so a foundation-first PR whose modules nothing yet calls dies on `dead_code`. The seam
therefore has to be vertical: PR-1 = the writer that actually writes (orc_type, encode, column,
footer_write, null_repair, the writer core and its tests) carrying `record_count` and file size only;
PR-2 = `metrics.rs` + `metrics_tests.rs` + `spec/table_properties.rs`, which is purely additive. Work
order `ticks-xo-opus/007/wo-rb-fork-r1.md` (103 lines) cut accordingly: `reset --soft 0cb6ebd30` so the
`wip:` title dies, six commits along the submodule seams, and the one piece of real code work named
plainly — severing `use metrics::OrcMetricsCollector` from `orc_writer.rs` with no stub, no `todo!()`,
no `#[allow(dead_code)]`, and every statistics-asserting test listed under `deferred_to_pr2` rather than
quietly dropped. Also recorded: the unit list's "Avro first if separable" does NOT apply here — the wip
is 16 files and all of them are ORC; there is no Avro code at all.

**Unit 1 (ra-rdfsort):** `cargo fmt --all --check` rc=0. That is one of the two CI-only checks the local
gate does not cover; clippy waits for the gate's cargo to finish. Gate still running, no `.done`.

Nothing else could be launched: both Muse slots stayed full all tick (rd-ca r2a and rd-posdel r2 both
still writing `out.jsonl`), and Devin is clerk-tier only with nothing red to give it.

## Tick 9 (11:08-11:2x EDT)
Nothing landed since tick 7: both Muse rounds (rd-ca r2a, rd-posdel r2), the ndbuild critic and the
ra-rdfsort local gate were all still running, so no launch slot existed. Tick spent on judgment only.
- **Unit 5 (IPI-20/23, rc-meta)** — read the stopped wip in full and REFINED tick 7's ruling from
  "do not launch it" to "narrow it". Sound: the provider delegates to
  `table.scan().select(names).project_current_schema()`, so it serves exactly what the fork serves,
  and one constant (`METADATA_COLUMN_NAMES`) governs the served set. Unsound: that constant lists all
  five reserved columns while the pinned fork can serve two, which would plan and then fail at read
  time (packet F-1's latent bug); and the comment gate, never run by the stopped round, reports
  **hits=22**.
- Work order cut: `ticks-xo-opus/009/wo-rc-meta-r1.md` (119 lines) — packet step 10 only, `reset
  --soft d89f8130` so no `wip:` title survives, five IPI-20 cells replayed as their exact SELECT statements,
  a typed `[ICE-MC-1]` refusal test for the three unserved names, four tests explicitly
  `deferred_to_pr2`, and `R-MC-POS-MOR` ordered MEASURED rather than assumed.
- Claims appended with the measurement and the ruling.

## TICK 10 (11:27-11:50 EDT)

**Unit 1 IPI-43 (ra-rdfsort) — PR OPENED: repark#755.** Local gate after the 3ed905c6 rebase returned
`CB=0 R=0 T=0 U=0 L=0`; comment gate 0; fmt 0. Five commits, 22 files, +1873/-314. Pushed and opened
#755 with a body that states both judgement calls in the open (the three-message-needle refusal remap
confined to the `execute()` call site; the partition-free `_file_ids_flat` reader taken under packet
§11 A-1 rather than waiting on IPI-20). Critic launched on the PR head with a brief that attacks
exactly those two calls. Push mechanics: after the rebase the remote branch was the pre-rebase
29d8b221, so `--force-with-lease` could not resolve a lease; verified the remote head was my own
pre-rebase commit and force-pushed through the explicit GitHub URL (the lane's `origin` is
push-disabled by design), then re-ran xpr.sh to create the PR.

**Unit 2 #733 (ndbuild) — CRITIC PASS.** Grok confirmed both round-8 claims: C-063 (owned plain UPDATE
resolves a mixed-case SET target under `spark.sql.caseSensitive=false`, checked against the recorded
MC-UPD-01/02 oracle, mutation-pinned — restoring `rewrite_fragment_case` reds both pins with the CI
error shape) and C-064 (the RePark Parquet writer takes Java's `parquet.enable.dictionary` default ON
when unset, explicit false off, TRUE on, while the owned plain INSERT keeps the fork insert-exec rule so
a session conf cannot change INSERT bytes; four footer pins, mutation-proven). No weakened assertion, no
production unwrap, no comments. ONE residual P3 — `crates/repark-spark/src/tests/map.md:132` cites
C-064 on the C-051/C-052 paragraph. Devin clerk launched for the one-line restore; then push, one CI
cycle, queue.

**Unit 6b F-POSDEL-SCAN-1 (rd-posdel) — ROUND 2 LANDED, CRITIC RUNNING.** Muse exit 0, head ab2c7555,
all five gates 0. V-002 closed with a genuinely parent-only decoy and a proven-load-bearing scope pin;
V-001 closed as ordered by demoting ledger C-010 to OPEN with no code change; V-003 closed by deleting
the three false sentences after a rewrite tripped the comment ban. Fresh critic launched on the delta.
**Residue 1 of this unit is now fixed:** C-010's "both ManifestEvaluators are load-bearing" is OPEN, not
proven — sabotaging both leaves the suite green because both filters are AlwaysTrue and `eval`
short-circuits.

**Unit 3 fork half (rc-fork) — MUSE ROUND 2 LAUNCHED** on the three changelog-reader findings. Both Muse
slots full again.

## Tick 11 (~11:50 EDT)
- **Unit 6b (F-POSDEL-SCAN-1, rd-posdel)** — Grok critic on the round-2 delta: **PASS**, no P1/P2.
  V-001/V-002/V-003 all closed; the `AllSnapshots` flip proved the new parent-snapshot decoy pin
  load-bearing (red 1-vs-0, restored 14/0). Fork **PR iceberg-rust#332** opened, body declares
  residue 1 (ledger C-010 OPEN: both ManifestEvaluators run under `AlwaysTrue`, a real prune pin
  needs a filter parameter on `scan()`), the two honest `FeatureUnsupported` refusals, and the two
  pre-existing not-findings (C-011 YAML fence, java_schema_shape.rs FB-2 notes).
- **Unit 2 (#733, ndbuild)** — Devin clerk hand-back verified: `crates/repark-spark/src/tests/map.md`
  only, +1/-1, restores `pins: ice-session-write-conf-1/C-051, C-052`; comment gate 0. Trailer
  normalised to the main-line form `Authored-By: Devin SWE-2 (swe-2-high) <noreply@cognition.ai>`
  (Devin had written `Generated with [Devin]`). Pushed 604d8f74..950762c9. Awaiting one CI cycle,
  then the queue. Critic verdict on this unit was already PASS.
- **Unit 3 (rc-fork)** — the 11:28 Muse launch had silently died at start: systemd
  `Failed to set up standard output` / `status=209/STDOUT` because `/tmp/muse-worker/rc-fork/` did not
  exist. One lost hour. Relaunched 11:48 after `mkdir -p`; it is waiting on the box-wide build slot.
- **Unit 1 (#755, ra-rdfsort)** — CI: Rust lint PASS, Rust test PASS (my local-clippy worry was
  unfounded), two reds. (a) `check_ledger_grammar.py` fails with 11 findings because the unit ledger
  `task/ledgers/staging/ice-rdf-sort-parse-1-ledger.md` was never written while eleven map.md rows pin
  its clauses C-001..C-011 — ruling: write the ledger, do NOT quiet the pins. (b) ruff E501 at
  `python/repark/tests/_record_ice_rdf_sort_parse_1_oracle.py:224`. WO `011/wo-ra-rdfsort-r2.md` cut,
  first in the launch queue; the critic running on 2de66930 will need a delta pass afterwards.

## Tick 12 (12:08 EDT)
- **Unit 2 (IPI-14, repark#733)**: CI all green on 950762c9 (9 pass / 2 skipped; the previously pending
  non-required `build + import smoke` also passed). Appended `733 ice-session-write-conf-1 12:08` to
  run16/merge-queue.txt and started `drive-merge.sh 733`. Critic verdict backing the merge: PASS
  (xr-ndbuild 20260920T145947Z), local gate CB=0 R=0 T=0 U=0 L=0, comment gate clean.
- **Unit 1 (IPI-43, repark#755)**: Muse round 2 launched on the work order that writes the missing
  `ice-rdf-sort-parse-1` ledger (11 pinned clause rows, derived from the pinned tests) and clears the
  ruff E501. Uses the Muse slot the dead rc-fork service had been silently holding.
- **Unit 3 (IPI-22)**: fork round 2 has now failed to start twice (systemd 209/STDOUT trap the first time,
  build-slot contention the second). No code change. Rank 1 for the next free slot; no Opus executor.
- **Unit 6b (F-POSDEL-SCAN-1, iceberg-rust#332)**: 13 fork CI checks green, only `build (windows-latest)`
  outstanding. Merge via la-fork-merge.sh on green.
- Housekeeping: waiting work orders for units 4, 6c and 5 re-verified against lane heads the rebased head /
  bee08f5fd / f9c4ffb8 — all SHAs still valid, no repointing.

## TICK 13 (12:24–12:30 EDT)

**Root cause of the three silent rc-fork launch failures (cost ~1h across ticks 10–12).**
`r7-launch.sh` spawns its worker with `-p StandardOutput=append:/tmp/oc-worker/$LANE/launch.log`.
When `/tmp/oc-worker/<lane>/` is absent systemd kills the unit at once (209/STDOUT) while r7-launch
prints `launched muse on <lane>` and exits 0. `rc-fork` was the only lane of mine without that
directory — every other lane had one, which is why only this unit ever failed. My earlier hypothesis
(`/tmp/<tool>-worker/<lane>/`) was wrong and pre-creating those dirs changed nothing. Fixed by
`mkdir -p /tmp/oc-worker/rc-fork`; unit 3 round 2 is now genuinely running.

**Unit 6a (rd-ca) round 2a — hand-back accepted.** 4 titled commits on `fea36608`, no `wip:` title
survives. Both S1 findings closed with recorded mutations: V-01 (id-less parquet resolved a container
`IS NOT NULL` to zero rows) fixed by stamped-schema RowFilter resolution + name-guarded positional
fallback + post-decode residual; L-01 (page-index `CantMatch` on a parquet GROUP skipped all rows)
fixed by `calc_row_selection` returning select-all for group field ids, with all six evaluators audited
for fail-open. V-04 verified, plus two term-bind expectations red since round 1. `reader.rs` ratcheted
9756→9691 by extracting `row_filter_plan.rs`. My own checks: comment gate hits=0, 27 files
+2242/-870, no co-author trailer. Two Java divergences evidenced for the round-2b ledger (Branch-3
non-dense positional ids; Branch-2 top-level-only name mapping). V-02/V-03/V-05 and steps 7–9 remain
as round 2b. Pushed 5b176d02..1a586446 (fast-forward) to refresh #326's stale CI.
Trailer nit for the pre-PR clerk pass: these commits read `Authored-By: Muse (…) <noreply@meta.com>`
where main's form is `Authored-By: Muse Spark (…) <noreply@meta.ai>`.

**Unit 6b (rd-posdel) — #332 all 14 checks pass**, queued as `fork#332 f-posdel-scan-1 12:27` and
`la-fork-merge.sh 332 "<subject>" /tmp/rd-posdel` started. Note for the record: that script takes
three arguments and waits for a `fork#<n>` line to reach the head of the queue; a one-argument call
exits silently, which cost one attempt at 12:25.

**Unit 2 (#733)** — every required check passes; `mergeStateStatus` is BLOCKED only on the
non-required `build + import smoke (debug, host)`. `drive-merge.sh 733` still driving.

**Unit 1 (#755)** — Muse round 2 confirmed running (stamp `20260920T161831Z`, out.jsonl growing).

## TICK 14 (12:45–12:58 EDT)
- **Unit 1 IPI-43 (repark#755) — the important finding of this tick.** Round 2 handed back CONCLUDED
  (2 commits, `2e8fc1be` unit ledger with 11 OPEN clauses, `dd8247ac` pinned-ruff clean; comment gate
  hits=0; my read of the delta: 5 files, +67/-5, ledger + cosmetic reflow only, no assertion changed).
  Both red CI checks are addressed and the head is pushed `2de66930..dd8247ac` (fast-forward).
  **BUT** I re-read the 11:30 verification critic (`/tmp/grok-worker/xr-ra-rdfsort/20260920T153023Z/`):
  its verdict is **NEEDS_REMEDIATION with two P1s**, and round 2 was scoped to the CI reds only, so
  V-001..V-005 are all still open. My tick-13 note ("next: gate, push, critic, queue") was wrong —
  this unit needs a round 3 before any critic or queue.
  - V-001 P1 ROOT-CAUSED BY ME: `call/rewrite_data_files.rs:141-145` re-raises with
    `illegal_argument_error(error.to_string())`; `iceberg::Error`'s Display is `"<Kind> => <message>"`,
    so every fork validation refusal answers `IllegalArgumentException: DataInvalid => …` where Java
    answers the bare message. Ruled: re-raise the message verbatim via the fork's public message
    accessor, and re-pin all three refusals by EQUALITY on the full message, not `contains`.
  - V-002 P1: the two `test_rewrite_data_files_options.py` facade pins assert the pre-unit R135
    refusal this unit deliberately replaces (Muse's own `out_of_scope_observed` flagged the same two).
    Ruled: re-pin both to the new measured behaviour; no delete, no xfail.
  - V-003/V-004/V-005 P2 ruled: tighten the hollow maintenance-call pin; retire `RDF-SORT-1` and file
    `RDF-SORT-TRANSFORM-1` in the parity registry; add `md.sort-order` + `operation=replace` to
    `_assert_cell`.
  - WO cut: `/tmp/oc-worker/run27/ticks-xo-opus/014/wo-ra-rdfsort-r3.md` (94 lines, 7 steps).
    Queued for the next free Muse slot; the 11:30 critic must NOT be reused as a PASS.
- **Unit 6a rd-ca round 2b LAUNCHED** (Muse slot 1, freed by ra-rdfsort r2):
  `/tmp/oc-worker/run27/ticks-xo-opus/014/wo-rd-ca-r2b.md` = steps 5-9 (V-02/V-03/V-05, the mutation
  round, ledger + maps incl. the two declared Java divergences, full gates), wrapper
  `launch-rd-ca-164543.service` alive. WO carries the trailer correction and a do-not-touch rule for
  the `list_null_tests` red.
- **Unit 6a #326 CI triaged**: the refired run has exactly ONE failure,
  `iceberg-datafusion physical_plan::list_null_tests::select_where_xs_is_null_returns_the_null_row`
  (parquet `struct_array.rs:142` panic). The file came from fork main (#299) and EXISTS on
  `origin/main` — so "red on main too" is plausible but NOT yet established; a clerk reproduction on
  `0cb6ebd3` vs `1a586446` is deferred because the only fork clone is occupied by round 2b.
- **Merges confirmed**: repark#733 MERGED; iceberg-rust#332 MERGED `RESULT=TREE-EQUAL 7f59b3c4`.
  Unit 2's cell replay is pending a free box cargo slot (both Muse rounds hold it).
- **RP-42 deliberately still NOT claimed**: only one bump may be open across all lanes and the bump
  should carry F-CHANGELOG-READER-1 (rc-fork round 2 still running) together with #332/#326.
- **Unit 3 rc-fork round 2 CONCLUDED at 12:46** (four commits `04291e96`/`bd083042`/`0f5dfda2`/
  `33315c4d`): V-001's `DeletedRows` refusal at entry with kind+exact-message pin, V-002's served-batch
  schema pinned against `changelog_arrow_fields()`, V-003's ledger C-005 + GAP_MATRIX R123 restated,
  plus a one-token clippy `expect_err` fix. Worker gates all 0 and both mutations exit 101. My checks:
  comment gate hits=0, scope exactly 6 files +593/-1, TRO-Wolf identity, `incremental.rs` (another
  unit's pin) untouched. Trailer nit: `Muse (…) <noreply@meta.com>` vs main's `Muse Spark (…)
  <noreply@meta.ai>`. Delta critic launched on `33315c4d` (`xreview-rcfork-r2-165032`, brief
  `.../014/critic-rc-fork-r2.md`), explicitly scoped to "is each finding actually closed, and did
  closing it break anything else".
- **Unit 1 round 3 launched 12:49** into the Muse slot rc-fork freed. Both Muse slots busy again
  (rd-ca r2b, ra-rdfsort r3); the critic does not count against the cap.
- Claims line appended for the other orchestrators: the critic-verdict trap, the
  `error.to_string()` / `"<Kind> => <msg>"` prefix bug (IPI-22's refusals use the same mechanism), and
  the #326 `list_null_tests` red.

## Tick 15 (13:06–13:10 EDT)
- **Unit 3 critic was NOT running.** The tick-14 launch (`xreview-rcfork-r2-165032`) died silently:
  I had wrapped `xreview.sh` in my own `systemd-run`, and the nested unit never produced a
  `grok-xr-rc-fork-*` unit or a stamp (only the 10:33 stamp existed). RELAUNCHED unwrapped —
  `grok-xr-rc-fork-170712.service` verified ALIVE on `33315c4d`. Lesson recorded: xreview.sh already
  does its own systemd-run; never wrap it, and always verify the inner `grok-xr-<lane>-*` unit.
- **Unit 1 root-caused why repark#755 has no CI at all.** `gh` reports zero check-runs and zero
  workflow runs for `dd8247ac`: the pull request is `mergeable=CONFLICTING / DIRTY` since main took
  repark#733, and GitHub fires no `pull_request` workflow when it cannot compute the merge commit.
  So the tick-14 expectation "the two red checks go green on the refired run" was never testable.
  Measured the conflict in a throwaway probe clone (`/tmp/probe-755`, lane untouched): merge-base
  `d89f8130`, main `82952d40`, exactly THREE conflicts, all keep-both —
  `crates/repark-spark/src/tests/mod.rs` (mod list), `scripts/map.md` (ratchet entries),
  `task/ledgers/staging/map.md` (ledger bullets). Clerk work order written and held:
  `.../015/wo-ra-rdfsort-rebase-clerk.md` — it cannot launch until Muse round 3 releases the lane.
- **Unit 6a:** created fork lane `rd-carepro` (`fork-lane.sh`, exit 0) and LAUNCHED the free Devin
  clerk reproduction of `select_where_xs_is_null_returns_the_null_row` on `0cb6ebd30` vs `1a586446`,
  work order `.../015/wo-rd-carepro-clerk.md`, wrapper `launch-rd-carepro-170921` alive. This settles
  whether #326 reddened the test or inherited it.
- Confirmed fork main is now `7f59b3c45` (#332 squashed) — RP-42 must bump to that or later.
- Both Muse rounds (rd-ca r2b, ra-rdfsort r3) still running; no cells closed this tick.

## Tick 16 (13:30 EDT)
- **Unit 3 fork half (F-CHANGELOG-READER-1)** — critic verdict on `33315c4d`:
  **PASS** (`/tmp/grok-worker/xr-rc-fork/20260920T170712Z/out.json`), no P1/P2, V-001/V-002/V-003
  all confirmed closed, both mutations exit 101, `incremental.rs` untouched.
  Merge probe against fork main `7f59b3c4` in a throwaway clone: clean (GAP_MATRIX auto-merged).
  Lane was missing the fork pre-push hook (xpr.sh refuses without it) — installed from
  `~/CodeRepos/LocalRepark/repark/.git/hooks/pre-push`, as `fork-lane.sh` does.
  Pushed and opened **iceberg-rust#333**, MERGEABLE, CI running. Comment gate hits=0.
- Unit 1 (ra-rdfsort round 3) and unit 6a (rd-ca round 2b) both still running; Devin still on
  the rd-carepro red-test reproduction. No slot free, so nothing new launched.
- #326 CI unchanged (13 pass, the one `list_null_tests` failure on `1a586446`) — the reproduction
  is what settles it.

## Tick 17 (13:46–13:52 EDT)
- **Unit 6a, fork #326 — the CI red is OURS.** Devin clerk round on lane `rd-carepro`
  (`/tmp/devin-worker/rd-carepro/20260920T171804Z/`) measured
  `physical_plan::list_null_tests::select_where_xs_is_null_returns_the_null_row`: PASS on base
  `0cb6ebd30` (module 10/10 green), exit 101 on head `1a586446`; `parquet` = v58.4.0 on BOTH
  commits. Muse's "already red on main" defence is refuted. Panic is
  `Option::expect` "child with nullable parents must have definition level" at
  parquet-58.4.0 `struct_array.rs:142`, reached through `ReadPlanBuilder::with_predicate_options`
  — the row-filter predicate read path that round 1 introduced in `row_filter_plan.rs`.
  Work order **wo-rd-ca-r2c.md** cut (tick dir 017): diagnose first, fix the predicate
  `ProjectionMask` ancestor path for leaves under an optional parent, add a red-first fork test;
  no test weakening, no ignore/xfail, no Cargo edits. Launches the moment round 2b hands back.
- **Method residue worth keeping:** a bare test name with `--exact` selects ZERO tests and exits 0
  *vacuously*. The first reproduction brief had that bug; Devin caught it and used the
  fully-qualified path. Any gate that "passes" on a single `--exact` test name is suspect.
- **Unit 2 (#733) cell replay LAUNCHED** on Devin, lane `ndbuild`, WO
  `017/wo-ndbuild-replay-clerk.md`: fetch merged main, rebuild the release native with
  local-gate.sh's own invocation, run both 106-cell oracles
  (`test_ice_session_write_conf_1.py`, `…_paths.py`), record before/after counts, restore the lane.
  Measurement only — no commits.
- **Unit 3 fork half:** iceberg-rust#333 CI at 13:47 = 13 pass, 0 fail, 1 pending
  (`build (windows-latest)`). Nothing to do until it concludes.
- Muse rounds ra-rdfsort r3 (session `01a0bfc2-c36b-7f90-a9c4-1629048dd1b0`) and rd-ca r2b
  (session `01a0bfbe-c6f0-7943-bee0-3f54643e8476`) both alive and tool-calling at 13:45.

## Tick 18 (14:04-14:08 EDT)
- **Unit 3 fork half (#333)**: CI finished 14/14 green on 33315c4d and the critic verdict was PASS, but
  `gh api compare` showed the head **behind main by 1** (#332's squash) — `la-fork-merge.sh` refuses that
  with RESULT=NOT-UP-TO-DATE. Rebased `/tmp/rc-fork` onto fork main `7f59b3c4` myself (pure git, Devin
  busy): all 5 commits replayed with **zero conflicts**, 33315c4d -> bcac02c2. Comment gate hits=0 on the
  new range; trailers and TRO-Wolf identity intact. Pushed with an explicit `--force-with-lease=…:33315c4d`.
  Carried the existing critic PASS forward: a conflict-free rebase over a disjoint unit (#332 posdel scan)
  is not a content delta. Queued `fork#333` and launched `forkmerge-333-180538.service`, which re-watches
  the fresh CI run itself.
- Three workers alive and healthy at tick end: Muse rd-ca r2b, Muse ra-rdfsort r3 (now committing V-005
  work, head 4ee51c9a -> fe7dae69), Devin IPI-14 cell replay (release build in progress).
- No launches this tick: both Muse slots and the single Devin slot are occupied.

## Tick 19 (14:08-14:12 EDT)

### Unit 2 — IPI-14 / repark#733 cell replay: DONE, ALL EQUAL
Devin clerk round `/tmp/devin-worker/ndbuild/20260920T175653Z/` CONCLUDED exit 0.
Replayed the unit's oracle on **merged main `edfa1e38`** (which contains the unit commit `82952d40`),
built gate-exact (`maturin@1.14.1 develop --release`, exit 0):

| oracle file | result |
|---|---|
| `python/repark/tests/test_ice_session_write_conf_1.py` | 27 passed, 1 skipped, 2 xfailed (rc 0) |
| `python/repark/tests/test_ice_session_write_conf_1_paths.py` | 38 passed (rc 0) |

**Before/after: 0 failing -> 0 failing; 65 passing cells + 1 skip + 2 xfail = 68 collected, zero
regressions on merged main.** No failure nodes, so no residue from the replay.

**Measurement correction for the record:** my unit list and state called this a "106-cell oracle".
The two named files collect **68** cells on merged main, not 106. The tests directory also holds
`ice_session_write_conf_1_spark_oracle.json` (a fixture) and three
`_record_ice_session_write_conf_1_{oracle,paths,rounds}.py` recorder scripts — those are generators, not
pytest oracles, which is where the larger number came from. The 68 collected cells are the whole pytest
surface of IPI-14 and every one of them answers EQUAL.
Logs: `/tmp/ndbuild-stamp-ipi14/{release.log,unit-main.log,unit-paths.log}`.
Lane restored to `fix/ice-session-write-conf-1` @ `950762c9`, `git status --porcelain` empty,
`ls-files -v` = `H Cargo.lock` / `H Cargo.toml` (not skip-worktree). **Unit 2 is fully closed.**
Minor lane crumb, harmless: the clerk's scratch branch `replay/ipi-14` still exists in the lane.

### Work order cut this tick
`ticks-xo-opus/019/wo-re-build2-rebase-clerk.md` — Devin clerk, pure git: rebase lane `re-build2`
(4 commits, behind main by 4) onto `e26d1349`, keep-both on any `map.md` conflict, abort-and-report on
any `.rs`/`.py`/`.toml` conflict, no push, no build. Its required output is the **old->new SHA mapping**
for `0ad9979f` and the `wip:` head `the rebased head`, because the waiting round-2 work order orders
`git reset --soft 0ad9979f` and a rebase rewrites that SHA. Launched under `launch-re-build2-180914`.

### Unit 1 — the #755 rebase recipe, re-measured against `e26d1349` (tick 19)
Main moved `edfa1e38 -> e26d1349` (it took #751 IPI-21/25/42 and #752 MAP-PR-GATE-1), so I re-probed the
#755 conflict set in a throwaway clone of the lane at `fe7dae69` rather than assume the tick-17 answer.
Merge-base is still `d89f8130`; main carries 147 files / +15559 since it. The conflict set grew from
three to **FOUR**, and every one is a disjoint adjacent insert — **KEEP BOTH SIDES**, no content merge:

1. `crates/repark-spark/src/tests/mod.rs` — ours `mod sort_order_parse;` vs main's
   `mod session_write_conf;` + `mod session_write_conf_removals;`. Keep all three, alphabetical, before
   `mod spark_dialect;`.
2. `scripts/map.md` — ours the `ICE-RDF-SORT-PARSE-1` ratchet row vs main's two `ICE-SESSION-WRITE-CONF-1`
   ratchet rows. Ours goes above theirs; main's round-6 reconciliation row below the hunk is untouched.
3. `task/ledgers/staging/map.md` — both sides add a ledger bullet block. Keep both blocks.
4. **NEW, from #752's per-directory map.md lockstep:** `crates/repark-spark/src/map.md` — ours adds the
   `sort_order_parse.rs` bullet, main adds the `namespace_ddl/` bullet, both at the same insertion point.
   Keep both, and the ordering is forced by the file's convention: `sort_order_parse.rs` first, then
   `namespace_ddl/`, then the existing `namespace_ddl.rs` bullet (the directory bullet must sit
   immediately above the module bullet it delegates to).

This is the recipe I execute myself the moment Muse round 3 concludes and hands the lane back. Caveat
recorded for my next tick: round 3 also files a `RDF-SORT-TRANSFORM-1` row in
`docs/spark-sql-iceberg-parity.md` (finding V-004), so a fifth conflict may appear in that registry once
the round's commits land — re-probe rather than trust this list wholesale.

## Tick 20 (14:29–14:36 EDT)

**Unit 1 (IPI-43) — round 3 accepted, branch rebased and pushed, PR #755 finally has CI.**
Muse round 3 (`/tmp/muse-worker/ra-rdfsort/20260920T170007Z/`, exit 0, CONCLUDED) closed all five
critic findings in seven commits. Accepted after reading the hand-back: V-001 re-raises
`error.message()` so the `DataInvalid => ` prefix is gone and all fork-refusal pins assert the full
message by equality; V-002 re-pins the two facade tests to measured behaviour (neither deleted nor
xfailed); V-003 exact class + message; V-004 RDF-SORT-1 retired FIXED and RDF-SORT-TRANSFORM-1 filed;
V-005 pins sort state and `operation=replace` per cell against a re-recorded live Spark 4.1.2 oracle.
Its one non-zero gate is the brief's literal clippy form, which reds only on the repo-wide pre-existing
`expect/unwrap` test corpus; both canonical Makefile forms exit 0 — not a finding.
I rebased the lane onto main `e26d1349` myself. The tick-19 forecast of four-to-five conflicts was
wrong: exactly ONE landed, the module list in `crates/repark-spark/src/tests/mod.rs`, resolved keep-both
alphabetically. comment_ban 0, ledger-grammar 0, `make check-map-md` green (after refreshing the lane's
stale `refs/remotes/origin/main`, which otherwise reds on main's own untouched map.md files). Pushed
`dd8247ac..dcbfe685` under an explicit lease. Local gate `gate-ra-rdfsort-183132` and a fresh critic
`grok-xr-ra-rdfsort-183227` are both running on the new head.

**Unit 4 (IPI-19/56/37) — rebase finished by hand, round 2 launched.**
The Devin clerk aborted correctly: a content conflict in `crates/repark-iceberg/src/write/mod.rs` is a
`.rs` conflict and the brief forbids a clerk guessing at one. Its hand-back was accurate down to the
hunk line numbers, which made finishing it cheap. I resolved both hunks keep-both (main's
`session_write_conf` / `illegal_argument` items alongside this unit's `schema_evolution` items,
alphabetised); the wip commit then conflicted in turn, taking the wip side of the import block and
setting `scripts/check_rust_file_size.py`'s baseline for `merge/mod.rs` to the measured 1661 — that
guard is an EXACT baseline and reds when a file shrinks below it as well as when it grows. comment_ban
0 after the rebase. SHA map acf2cdbc->9f9e7652, b7c81042->2532f89b, 0ad9979f->3775cf3b,
the rebased head->ecbd9415; the round-2 work order was copied to `020/` and patched with those SHAs, the 1661
baseline note and the new map.md lockstep clause, then launched on Muse.

**Environment note recorded for every lane:** `xreview.sh` died instantly once with a bwrap
write-deny-mount error naming an absent `~/.grok/managed_config.toml`, while a different grok
critic ran fine. An immediate relaunch worked. Transient, retry once.

Work orders cut this tick: 1 (`020/wo-re-build2-r2.md`, a SHA-patched reissue).
Critic briefs cut: 1 (`020/critic-brief-ra-rdfsort-r3.md`).

## Tick 21 (~14:55 EDT)
- **fork#333 MERGED TREE-EQUAL** (`886b94c1`, driver forkmerge-333-180538 finished). Unit 3's FORK half is CLOSED.
- **RP-42 CLAIMED AND OPENED** in claims.txt at 14:55: `BUMP OPEN RP-42 886b94c1… by xo-opus`, payload fork#332 (posdel scan) + fork#333 (changelog reader) over the RP-41 pin `3ed905c6`. #326 explicitly excluded. Lane `ra-pin42` / branch `chore/rp-42-fork-pin` building under `setup-ra-pin42-185045`.
- **Two rebases done by hand, both ZERO conflicts** (git-only, cheaper than a clerk round):
  - `rb-fork` (unit 6c, ORC) onto fork main `886b94c1`: `bee08f5fd -> 117c524aa`, comment gate hits=0. WO re-cut with the new SHAs at `ticks-xo-opus/021/wo-rb-fork-r1.md` (`reset --soft 886b94c1b`).
  - `rc-meta` (unit 5) onto RePark main `f16a7bcd`: `f9c4ffb8 -> e35ba588`. WO re-cut at `ticks-xo-opus/021/wo-rc-meta-r1.md` (`reset --soft f16a7bcd`). Comment gate still hits=22 — the wip's own comments, which that WO orders deleted.
- RePark main has moved again (`e26d1349 -> f16a7bcd`); #755 is MERGEABLE/**BEHIND**, CI 8 pass / 1 pending / 2 skipping.
- In flight and unchanged: Muse rd-ca r2b, Muse re-build2 r2, gate + critic on ra-rdfsort `dcbfe685`.

## Tick 22 (15:08–15:14 EDT)
- **RP-42 pin bump committed** — /tmp/ra-pin42 `35708a10` on branch `chore/rp-42-fork-pin`:
  `make bump-fork-pin REV=886b94c1bbc68f43b791221761b8360dec4767d0` (5 pin lines across Cargo.toml +
  Cargo.lock) plus the RP-42 pin-history row in map.md naming #332 F-POSDEL-SCAN-1 (carrying its
  C-010 PROVEN->OPEN demotion as a declared residue) and #333 F-CHANGELOG-READER-1. comment-ban
  hits=0, identity TRO-Wolf, my own `Authored-By: Claude (claude-opus-5)` trailer. Lane hygiene before
  the bump: Cargo.toml/Cargo.lock both `H`, pin was `git+…?rev=3ed905c6`. Gate `ra-pin42` queued 15:10
  over repark-iceberg + repark-spark:--lib + repark-sql:--lib and the metadata-table / meta-delete /
  rm-deletes / merge-scan-prune pytest cohort.
- **Unit 1 IPI-43 — critic round 3 = NEEDS_REMEDIATION.** V-001..V-005 confirmed genuinely closed and
  every recorded Spark answer matched. Three NEW P2 findings, all leans adopted: V-006 the CALL
  transform refusal is substring-pinned on text ALTER also emits (A-8 hole); V-007 map.md claims an
  ALTER `zorder(...)` refusal that no test in the branch actually exercises; V-008 the A-5 Rust twin
  (`call_rewrite_sort_order_sorts_and_does_compact`) asserts only `files_after < files_before` and
  survives the always-BinPack mutation. Work order `022/wo-ra-rdfsort-r4.md` cut for exactly those
  three, with a named mutation required as evidence per finding; Muse round 4 launched 15:09. #755
  stays open and unqueued (local gate CB=R=T=U=L=0, CI 9 pass / 2 skipping, BEHIND main by 1).
- **Unit 6a rd-ca round 2b accepted and pushed** — 4 titled commits, comment hits=0, correct
  `<noreply@meta.ai>` trailers; pushed `1a586446..c159b8d4` as a fast-forward onto fork #326. Two
  Java divergences recorded (non-dense top-level id stamping; nested predicates going post-decode
  residual). Devin clerk launched to re-measure the known CI panic on the new head before r2c is cut —
  the old r2c work order is written against the superseded SHA.
- Residues unchanged. No QUESTION written.

## Tick 22–23 (15:09–15:35 EDT)

### Unit 7 — RP-42 fork pin bump
Committed `35708a10` in lane `ra-pin42`: `3ed905c6 -> 886b94c1`, carrying fork #332
(F-POSDEL-SCAN-1 position_deletes scan) and #333 (F-CHANGELOG-READER-1 Arrow reader).
`make bump-fork-pin` rewrote 5 pin lines across Cargo.toml + Cargo.lock; the RP-42 pin-history row
sits in map.md directly after RP-41's. comment-ban hits=0. Local gate queued 15:10 and was still
running at the end of tick 23 (comment-ban 0, release build rc=0 at 15:24). Not yet a PR.

### Unit 1 — IPI-43 sort/zorder parser (repark#755)
Critic round 3 (`/tmp/grok-worker/xr-ra-rdfsort/20260920T183227Z/out.json`, 37 KB) =
NEEDS_REMEDIATION. V-001..V-005 confirmed genuinely closed, with every recorded Spark answer
matching. Three NEW P2 findings, all leans adopted:
- V-006 the CALL transform refusal is pinned by SUBSTRING, so a CALL door reusing ALTER's wording
  would stay green (A-8 forbids) -> equality-pin the full CALL message, class + text.
- V-007 map.md:827-839 pins `WRITE ORDERED BY zorder(id)` to the ALTER transform refusal but no
  test runs ALTER -> add an ALTER `zorder(id)` pin (exact message + no metadata commit), repoint
  the claim.
- V-008 `call_rewrite_sort_order_sorts_and_does_compact` only asserts `files_after < files_before`
  and survives always-BinPack -> order-pin the Rust twin; leave the Python facade twin as-is.
Muse round 4 launched 15:09 against these three findings (`ticks-xo-opus/022/wo-ra-rdfsort-r4.md`),
running at end of tick 23.

### Unit 6a — fork container accessors (iceberg-rust#326)
Round 2b concluded and was PUSHED `1a586446..c159b8d4` (fast-forward): four titled commits —
accessor-absence + `xs.element` bind + is_present pins; skip default `is_optional` in accessor JSON
+ frozen optional-primitive predicate bytes; 17-mutation evidence + partition-evaluator pin + `st.b`
oracle cells; findings/attestation docs. comment gate hits=0, identity TRO-Wolf, trailers correct.
(r2a's four commits carry the wrong trailer `<noreply@meta.com>` — cosmetic, on the record, not
rewritten.)
A free Devin clerk round then re-measured the one CI red on the NEW head: verdict **REPRODUCES** —
`physical_plan::list_null_tests::select_where_xs_is_null_returns_the_null_row` (crate
`iceberg-datafusion`, not `iceberg` as my earlier brief said) exits 101 with a panic identical in
message, site, parquet version and path to the 1a586446 measurement: `Option::expect` "child with
nullable parents must have definition level" at parquet-58.4.0 `struct_array.rs:142`, reached via
`ReadPlanBuilder::with_predicate_options` — the row-filter predicate read path added by round 1.
Round 2b's commits did not touch it. Conclusion: #326 needs a real fix round, not a fresh critic.
Work order re-cut against the new measurement at `ticks-xo-opus/023/wo-rd-ca-r2c.md`, ordering a
committed root-cause diagnosis and a red-first `iceberg`-crate reproduction before any fix; it waits
on a free Muse slot. #326 stays DRAFT and must not ride a later pin bump until it is green with a
fresh critic PASS.

## Tick 24 (15:47–15:55 EDT)

### RP-42 pin bump — the gate's T=1 root-caused and fixed (orchestrator commit)
The 15:10 gate returned **CB=0 R=0 T=1 U=0 L=0**. The single red was
`repark-spark tests::metadata_tables::metadata_table_projection_honor_all_types`
(`crates/repark-spark/src/tests/metadata_tables.rs:546`), panicking on
`position_deletes collect must refuse` with a served 5-field RecordBatch
(file_path, pos, row{id,name}, spec_id, delete_file_path; row_count 0).

**This is the bump's payload landing, not a regression.** The pin
`rp-1-fork-repin/C-012` asserted position_deletes was schema-only and that `collect()`
refuses with "not yet ported". Fork #332 (F-POSDEL-SCAN-1) ports exactly that scan, so
at pin `886b94c1` the refusal is unreachable. This is the "registry change rides the
bump" case the tick-23 note warned to watch for.

Verified no map.md row or ledger row cites the pin — the stale claim lived only in the
test's `///` doc comment, so it was deleted rather than reworded (a deletion adds no
comment line; comment gate stays hits=0).

Fix `1b2458f5` **drops the special case entirely** rather than inverting it, so
position_deletes now runs the same battery as every other `MetadataTableType`:
`SELECT *` schema non-empty, `count(*)` equal to the `SELECT *` row total, and
partial first-column projection. That is a **strictly stronger pin** than the old
refusal. MEASURED green:
`cargo test -p repark-spark --lib -- --exact tests::metadata_tables::metadata_table_projection_honor_all_types`
-> `ok. 1 passed`. Pre-commit hook additionally required a rustfmt blank-line fix and a
`crates/repark-spark/src/tests/map.md` lockstep row (RP-42 clause added at :672).
Gate requeued on `1b2458f5` at 15:52.

### Unit 4 re-build2 — round 2 CONCLUDED, rebased onto main by hand
Muse round 2 (`/tmp/muse-worker/re-build2/20260920T183931Z/`, exit 0) closed all four
gaps: G-1 MEASURED ONE (`test_schema_evolution_commits_one_snapshot` — the schema
transaction emits AddSchema+SetCurrentSchema and never AddSnapshot; true single-
Transaction composition is impossible from RePark because the fork's `TransactionAction`
is `pub(crate)` — recorded as a shared-with-Spark atomicity residue); G-2 delta pin
added; G-3 the brief's `ice-merge-schema-1-ledger.md` filename was **superseded** — the
unit ledger already exists under the `ipi-19-56-37-schema-evolution-write` slug and a
second ledger would fail grammar rule B, so it was completed instead (13/13 PROVEN);
G-4 EX-DF-9 NARROWED, not retired. Ten commits, all carrying the Muse trailer; the
`ecbd9415` wip was reset away and re-committed as titled commits (zero `wip:` survives).
Its eight own gates exit 0; the ninth, plain `make check-map-md`, exited 1 — the
handback attributed it to a stale local `origin/main`, and that diagnosis is
**confirmed**: after the rebase it is rc=0.

Rebase onto `ed15699b` done by hand (13 commits, two `.rs` conflicts, both resolved by
me as composition, not guesswork):
1. `router.rs` `execute_inner` head — main extracted `rewrite_sql_for_execute` (cast-map
   + system-function rewrite); ours added the schema-evolution strip. Resolved by
   layering the strip onto main's helper, keeping both rewrites.
2. `router.rs` — main's new helper fn vs our `#[allow(clippy::too_many_lines)]` on
   `execute_inner`. Kept both.
Lane hygiene `H`/`H`, pin `3ed905c6`, comment gate hits=0 against the new base.
HEAD `8f870acf`. Gate queued 15:53.

### Launches
- Unit 6a `rd-ca` round 2c launched on the freed Muse slot with the current WO
  (`.../023/wo-rd-ca-r2c.md`, head `c159b8d4`); `launch-rd-ca-194731` wrapper alive.
- Unit 1 `ra-rdfsort` round 4 still running (no `exit` file).

## Tick 25 (16:11–16:20 EDT)

### Unit 7 RP-42 — the 15:52 gate's U=2 L=2 was a GATE-INVOCATION defect, not the branch
`ra-pin42-localgate.done` read `CB=0 R=0 T=0 U=2 L=2`. Root cause: that gate was queued with
**crates only and no pytest paths**, and `local-gate.sh` passes `"$@"` straight to pytest — with no
paths pytest collects the whole repo and dies on
`python/dbt-repark/tests/test_{gold_models,aws_acceptance_gold}.py` with
`ModuleNotFoundError: No module named 'yaml'` (2 collection errors, both halves rc=2). Unrelated to
the bump: an environment gap in the lane venv, in a dbt package the pin does not touch. The rust half
banked clean on `1b2458f5` — comment-ban 0, release rc=0, 1321 + 383 + 0 tests passed, `rust rc=0`.
Re-queued 16:14 with an explicitly named path: `python/repark/tests/test_metadata_tables.py`, the
suite that carries this bump's one behavior change (the C-012 flip).
**Correction to my own notes:** the "prior gate's pytest half was 113 passed / 1 xfailed" figure could
not be reconstructed — it belongs to a different lane's path set, not ra-pin42's (repark-parity/tests
collects 800, python/repark/tests 12186, test_metadata_tables.py 19). The oracle for RP-42 is now an
explicitly named path rather than a remembered count.

### Unit 1 IPI-43 — round 4 CONCLUDED, accepted, rebased, pushed, critic running
Muse round 4 (`/tmp/muse-worker/ra-rdfsort/20260920T191929Z/`, exit 0) closed V-006/V-007/V-008 in
four commits, **test-and-map.md only**: `alter_write_order.rs` +31, `call_rdf_sort.rs` +23,
`call_rewrite_options.rs` +71, `test_ice_rdf_sort_parse_1.py` +5/-2, `map.md` +7/-4. All seven of its
gates exit 0; comment-ban hits=0 (re-measured by me against `ed15699b`).
Mutation evidence is the strongest of this run — three mutations, each turning a **named** test red
with the exact diverging message quoted, each reverted, each with its blast radius recorded:
(1) rendering ALTER's wording in the CALL arm reds `call_rdf_sort_order_parse_refusals_match_java`
plus the python twin; (2) `parse_order_segment` accepting `zorder(x)` as an identity field reds
`write_order_zorder_term_refuses_and_commits_nothing`; (3) `resolve_strategy` returning BinPack on an
omitted strategy reds `call_rewrite_sort_order_sorts_and_does_compact` with
`left: Some([4,2,5,1,3]) | right: Some([1,2,3,4,5])` — exactly the order pin V-008 asked for.
My own read of the diff confirms both judgment-bearing edits: the python pin is now a full-message
equality assert (V-006), and the `map.md` claim at :836 no longer pins the ALTER half to C-001..C-003
but cites the real test `alter_write_order.rs::write_order_zorder_term_refuses_and_commits_nothing`
(V-007).
**Rebased onto `ed15699b` by me** — 18 commits, ZERO conflicts, lane hygiene `H`/`H` — and pushed
`dcbfe685 -> 9f721971` (explicit lease on the old remote head). Done before the critic deliberately,
so the critic and the gate both judge the final head and no second pass is spent on the rebase.
Fresh critic launched on `9f721971` (`grok-xr-ra-rdfsort-201555.service` alive, stamp
`20260920T201555Z`), brief `.../025/critic-brief-ra-rdfsort-r4.md`, carrying V-001..V-005 forward as
closed with their evidence and listing pass-2's out_of_scope items as recorded residues. Local gate
re-queued on the new head. CI restarted by the push.

### Muse slot freed and refilled
Round 4's conclusion freed slot 1; unit 3's RePark diagnosis round launched into it
(`launch-rc-incr-201406.service` alive, WO `.../004/wo-rc-incr-r1.md`). The diagnosis half needs no
pin, so it runs in parallel with RP-42 rather than behind it.

## Tick 26 (16:34–16:50 EDT)

- **Unit 7 RP-42 pin bump** — local gate on `1b2458f5` returned CB=0 R=0 T=0 U=0 L=0 (the 15:52 U=2
  L=2 was trap 11, a gate queued with no pytest paths; the 15:10 T=1 was the C-012 payload flip,
  fixed by dropping the special case). **PR repark#760 opened.** Body carries fork #332
  (F-POSDEL-SCAN-1) and #333 (F-CHANGELOG-READER-1), the
  `3ed905c6...886b94c1` compare link, the C-012 behaviour change and the carried C-010
  PROVEN->OPEN residue. Critic launched on `xr-ra-pin42` @ `1b2458f5`.
- **Unit 4 IPI-19/56/37 schema evolution on write** — local gate on `8f870acf` all five 0;
  `check_ledger_grammar.py` rc=0. **PR repark#761 opened** (the remote branch did not exist, so a
  plain push — no lease; `xpr.sh` first refused for a missing pre-push hook, copied in). Body carries
  G-1 (MEASURED one snapshot), G-2, G-3 (the ledger slug the tree actually uses), G-4/A-11 EX-DF-9
  NARROWED with the 10:54 owner ruling, A-10, the `pub(crate) TransactionAction` atomicity residue
  and the two out-of-scope unblockers. Critic launched on `xr-re-build2` @ `8f870acf`.
- **Unit 1 IPI-43 sort/zorder parser** — critic pass 3 on `9f721971` = **PASS, zero questions**:
  V-006/V-007/V-008 closed, V-001..V-005 stay closed, no new findings, comment-ban 0, three
  mutations each reverted and each named with the test it reddened and the diverging value.
  #755 confirmed to carry its ledger (`task/ledgers/staging/ice-rdf-sort-parse-1-ledger.md`), so
  `check_ledger_grammar.py` will not red CI. CI: 8 pass, 2 skipping, 1 pending (build + import
  smoke). Queue is blocked only on `gate-ra-rdfsort-201614`.
- **Unit 5 rc-meta** — rebased onto `ed15699b` (1 commit, 0 conflicts), `origin/main` ref updated
  past trap 8; work order SHA-patched to `ticks-xo-opus/026/wo-rc-meta-r1.md`
  (`reset --soft ed15699b`, wip now `3a44b115`). Ready to launch the moment a Muse slot frees.
- Both Muse slots still held (rc-incr r1, rd-ca r2c). No Muse round could be launched this tick.

## Tick 27 (2026-09-20 16:53-17:00 EDT)

### Unit 1 — IPI-43 sort/zorder parser: **MERGED (repark#755)**
Local gate on `9f721971` concluded all five zero (CB=0 R=0 T=0 U=0 L=0); CI 9 pass / 2 skipping;
critic pass 3 PASS with zero questions. Queued (`755 IPI-43 16:53`) and `drive-merge.sh 755`
launched; the PR reports MERGED in the same tick. Pre-merge cell counts recorded for the replay:
unit leg 31 passed / 3 skipped, live leg 34 passed. **The post-merge replay on merged main is still
owed** — it needs a clone at the new main since the lane branch is squashed away.
Residues carried from the round-3 critic (not regressions): retired RDF-SORT-1 still carries a
2026-08-31 bullet claiming bare `sort_order` binpacks (needs a MULTI-FILE live-Spark cell); the fork
serialises snapshots in HASH order so `snapshots[-1]` is never a valid head.
RDF-SORT-TRANSFORM-1 stays a declared CALL-door refusal.

### Unit 7 — RP-42 pin bump (#760): critic **NEEDS_REMEDIATION**, and CI agrees
The critic (34 turns, real) confirms the bump itself is sound in every dimension it could measure:
five Cargo.toml rev lines + every Cargo.lock `iceberg*` source at `886b94c1…`, no leftover
`3ed905c6`, lane `H` not `S`, no added comments, no production unwraps, no source edits outside the
pin commit and `1b2458f5`, and the C-012 rewrite is **stronger** than the pin it replaces (dropping
the special case is endorsed explicitly). Both findings are the same root cause — records that still
claim the old refusal after fork #332 ported `PositionDeletesTable::scan`:
- **V-001 (P1)** `python/repark/tests/_v3_statement_coverage_repark.py` still records
  `ERROR "External error: FeatureUnsupported => position_deletes metadata table scan i"` and the
  golden keeps `meta-position-deletes` at DIVERGES. **CI independently measured the true answer** on
  `1b2458f5`: `{'probes': [['OK', [[1]]]]}` — this is exactly #760's single red check, so the critic
  finding and the red CI check are one thing, not two.
- **V-002 (P2)** four living rows outside C-012 still say the scan refuses:
  `docs/spark-sql-iceberg-parity.md` V3-COV-6, `crates/repark-spark/src/map.md:1014-1015`, the
  2026-09-14 cutover status and `docs/design/v3-statement-coverage.md`.
Both leans adopted as measured smallest fixes; no QUESTION needed. **Devin clerk round launched** on
lane `ra-pin42` with `.../027/wo-ra-pin42-r1.md`, which hands it the measured value rather than
asking it to re-derive one, and forbids relaxing the assertion. A fresh critic follows the fix.

### Unit 4 — IPI-19/56/37 (#761): the one red check was mine, and is fixed
`Python` failed in 8s on ruff **SIM109** at
`python/repark/src/repark/spark/session/builder_conf.py:208`. Root cause is **my tick-24 rebase**,
not the executor: main's #733 added the `key in WAP_SESSION_KEYS` arm to the same `if`, and our
`key == MERGE_SCHEMA_KEY` arm composed onto it produced two equality comparisons in one condition.
Fixed by me as `73f53142` (one membership check), `uvx ruff check python/` all clean,
`make check-map-md` rc=0, comment-ban hits=0, pushed fast-forward `8f870acf..73f53142` (no lease
needed). Every other check on #761 was already green; `build + import smoke` is re-running.
The critic `grok-xr-re-build2` is still judging `8f870acf`; the delta it has not seen is lint-only.

## Tick 28 — 2026-09-20 17:12–17:25 EDT

**Three rounds concluded at once; both Muse slots recycled inside the tick.**

### Unit 4 (IPI-19/56/37, repark#761) — critic verdict in: NEEDS_REMEDIATION, round 3 launched
`/tmp/grok-worker/xr-re-build2/20260920T203624Z/out.json` (30947 bytes, exit 0). It CONFIRMS the
substance: twelve cells pinned to Spark's recorded values and types, G-1/G-2 snapshot deltas are real
table measurements (not vacuous), EX-DF-9 narrowed not retired, ledger slug correct, comment-ban
clean, router composition correct, 43/43 unit Python tests pass, and both of its own mutations
reddened cargo tests with the tree restored. Two findings, both about pins that a WRONG
implementation would still pass:
- **V-001 (P1)** — C-003's two Python pins never enter `evolve_schema`. Both fixtures
  (`test_merge_schema_missing_column_writes_null`, `test_merge_schema_does_not_narrow_a_wider_column`)
  send frames with NO extra source columns, so `columns_to_add` returns `[]`
  (`insert_by_name/evolution.rs:38-40`) and the write takes the ordinary by-name path — the same
  path as with `mergeSchema` omitted. I verified this by reading the two fixtures
  (`test_ice_merge_schema_1.py:144-165`): confirmed, the pins are hollow.
- **V-002 (P2)** — C-012's `unset` pin sets, asserts, evolves, then unsets in a `finally` and never
  observes anything after the unset. A no-op unset passes. Verified at :520-529: confirmed.
Both leans ADOPTED (each is a test repair, not a design change; no QUESTION). WO
`ticks-xo-opus/028/wo-re-build2-r3.md` launched on Muse — it orders extras-bearing fixtures that
actually run `evolve_schema`, a post-unset refusal observation, the C-003/C-012 ledger citations
rewritten, and THREE named mutations (union→replace, merge narrows `id`, unset→no-op) each of which
must red a named test.

**Also found this tick: #761 has TWO CI reds, neither visible to the local gate** (the gate's five
codes do not cover the repo's registry-lockstep tests). Both are one-line registry updates and both
are consequences of work already accepted:
1. `python/repark-parity/tests/test_cap_1_source_file_line_cap.py:35` still carries
   `("crates/repark-iceberg/src/write/merge/mod.rs", 1701)` while the branch's file is 1656 lines and
   `scripts/check_rust_file_size.py:90` was correctly ratcheted to 1656. The two registries must
   agree → change :35 to 1656.
2. `python/repark/tests/_dfcore_1_expected.py` `EXPECTED_NEW_PACKAGE_SUBMODULES` lacks
   `writer_schema`, the new dataframe submodule this unit adds → add it.
Deliberately NOT folded into round 3 (it was already running, with a closed file list); I apply both
myself when round 3 hands back, so the branch takes ONE more push and ONE more CI cycle.

### Unit 3 (IPI-22 RePark half, rc-incr) — round 1 CONCLUDED green
`/tmp/muse-worker/rc-incr/20260920T202408Z/` exit 0. All four refusal tests now raise Spark's
exception classes with the refusal text byte-identical; xgate CB=0 R=0 T=0 U=0 L=0, both Rust suites,
fmt and clippy clean. My own read: 2 commits, 6 files, +135/-8, every map.md row updated,
comment-ban hits=0.
**The diagnosis overturned my standing suspicion and is worth recording:** the refusal never reached
`classify_datafusion_error` at all — the mid-stream Arrow FFI stringifies the error and
`_export_engine_error` always returns `PySparkException` (proven by the DOUBLE `External error: `
prefix in the pyarrow cause). So neither brief vehicle applied literally. The fix moves the changelog
refusal to PLAN time (a `plan_files` probe in `ChangelogTableProvider::scan`, surfacing only
`FeatureUnsupported` through the already-pinned classifier arm) and catches the three incremental
`DataInvalid` refusals site-locally at the `read_table_at` Incremental arm — the global `DataInvalid`
mapping is untouched, as ruled. No new marker, constructor or `Error` variant.
Out-of-scope worth carrying: `test_time_travel.py::test_incremental_snapshot_bounds_reach_the_incremental_scan`
is red at HEAD **and** on main's code — it loads `mem.ns.events` without the `multi_snapshot` fixture
and dies in `load_table`, strictly before either changed function runs. Independent of this round;
re-measure after the rebase. Also a perf note (plan_files now runs twice: probe + execute) left as a
declared follow-up to keep the round exception-class-only.
rc-incr stays parked until RP-42 merges — its lock patch and rebase are deliberate.

### Unit 6a (fork container accessors, #326) — round 2c CONCLUDED, PUSHED, the red is ROOT-CAUSED
`/tmp/muse-worker/rd-ca/20260920T195734Z/` exit 0; 5 commits, gates all 0 including
`cargo test -p iceberg-datafusion --lib list_null_tests`, `make check-clippy`, the file-size script
and comment-ban. Fast-forward pushed `c159b8d4..7559073b` (identity TRO-Wolf, Muse trailers correct,
comment-ban 0, verified `merge-base --is-ancestor` first).
**My tick-23 hypothesis was WRONG and the round proved it wrong with a measurement.** The predicate
mask was correct all along and the SYNC parquet reader decodes it fine. The panic comes from parquet
58.4's async predicate-result CACHE: the push decoder wraps any predicate leaf overlapping the scan
projection in `CachedArrayReader`, which reports no definition levels, and its `without_nested_types`
filter excludes multi-leaf roots and LIST roots but MISSES single-leaf STRUCT roots — so the nullable
`xs` `StructArrayReader` panics on its `expect`. Lists pass because LIST roots are excluded; maps pass
because a map root spans two leaves; the base passed because it never mapped container ids to leaves
at all. The fix KEEPS the pushdown and disables only that page-sharing cache
(`with_max_predicate_cache_size(0)`) when the pushed mask selects the leaf of a single-leaf non-LIST
group root. I read the diff myself (`row_filter_plan.rs`: a `PushedRowFilter` carrying the flag,
`pushed_mask_disables_predicate_cache`, `group_is_list`): deliberately conservative, and it also
covers list readers, which fail as errors rather than panics on level-less children.
Upstream note for the ledger: a parquet-side fix to `without_nested_types` would make the workaround
unnecessary; dependency changes are out of scope here.

### Unit 6c (fork ORC PR-1) — launched
The tick-21 WO (`ticks-xo-opus/021/wo-rb-fork-r1.md`, SHA-patched) went to Muse in the slot rd-ca
freed. No changes to the vertical split ruling.

### Unit 7 (RP-42, repark#760) — Devin clerk mid-round, healthy
Three commits landed (the two Python coverage records, the parity-registry row, the map/doc rows) and
it is into the gates. It found and respected the `python/repark/tests/map.md` lockstep on its own.
#760's only red remains the one V-001 predicted. Nothing for me to do until its hand-back.

### Owed / not started
Unit 1's post-merge cell replay (Devin busy with RP-42) and unit 5's PR-1 (both Muse slots taken;
still the most droppable item on the slate).

## Tick 29 (2026-09-20 17:17-17:23 EDT)

### Unit 7 — RP-42 (#760): Devin remediation CONCLUDED, rebased, pushed, fresh critic running
Devin round 1 (`/tmp/devin-worker/ra-pin42/20260920T210443Z/`, exit 0) handed back CONCLUDED with
five gates at zero (`test_v3_statement_coverage.py`, ruff, ledger grammar, `make check-map-md`,
comment-ban) and three commits. I read the diff myself: the recorded repark probe for
`meta-position-deletes` is now exactly CI's measured `["OK", [[1]]]` (the ERROR/FeatureUnsupported
record deleted, not inverted), the golden verdict flips DIVERGES -> EQUAL, and the four living
refusal claims (parity registry V3-COV-6, `repark-spark/src/map.md`, the 2026-09-14 cutover status,
`docs/design/v3-statement-coverage.md`) are corrected while the dated historical narration is left
intact. V-001 and V-002 both closed. Trailers and identity correct; lock/Cargo.toml lane flags `H`/`H`.
Rebased onto merged main `1127d216` (clean, 5 commits replayed), `origin/main` ref refreshed past
trap 8, comment-ban 0, and force-pushed with an explicit lease `1b2458f5 -> d01ac684`.
**A THIRD registry copy reddened CI on the new head** — the same family as #761's two reds, and
neither Devin's gates nor the local gate can see it: `python/repark-parity/tests/test_v3_cov_docs.py`
keeps its OWN `_TOTALS` of §1 and counts the §3 matrix against it, so the verdict flip made the
document read 74 EQUAL / 6 DIVERGES while the test copy still read 73/7 (three failures:
`assert 74 == 73`, the totals-table string, `assert 6 == 7`). I applied it myself: `_TOTALS`
73 -> 74 and 7 -> 6 (`9d61a20f`), then the `python/repark-parity/tests/map.md` lockstep row the
pre-commit hook warned about and `make check-map-md` then failed on (`18bde5b1`). Both pushed;
`make check-map-md` rc=0, `check_ledger_grammar.py` rc=0, comment-ban 0, and the file's own ten tests
pass. **Rule extended: a verdict flip moves THREE registries — the document, the golden dict, and
`test_v3_cov_docs.py::_TOTALS`.**
Fresh critic launched on `xr-ra-pin42` at `d01ac684`
(`/tmp/grok-worker/xr-ra-pin42/20260920T211836Z/`, brief
`ticks-xo-opus/029/critic-brief-ra-pin42-r2.md`) — it judges the remediation delta, is told not to
re-litigate what the first pass confirmed, and is asked to rule on the carried hazard (an empty stub
scan would still pass C-012). The delta it has not seen is my two registry commits, which are
counts-only. Local gate `gate-ra-pin42-211801` running on `d01ac684`.

### Unit 1 — IPI-43 post-merge replay: the three cells are GREEN on merged main
Merged main `1127d216` IS the IPI-43 squash. Lane `ra-rdfsort` re-pointed to it as
`replay/ipi-43-main` and the cells replayed there. First pass (the unit's own file alone,
`test_ice_rdf_sort_parse_1.py`): **13 passed unit, 13 passed live, 0 skipped** — P-RDF-SORT,
P-RDF-SORT-TABLE-ORDER and P-RDF-ZORDER all answer on merged main. The banked BEFORE counts
(31 passed / 3 skipped, 34 passed) came from a THREE-file path list, recovered from the old gate
unit's journal line (`test_ice_rdf_sort_parse_1.py`, `test_rewrite_data_files_options.py`,
`test_write_order_dist_1.py`); the like-for-like replay with those exact paths is queued as
`gate-ra-rdfsort-212112`. **Trap recorded: a gate log is overwritten by the next gate on the same
lane — `journalctl --user -u gate-<lane>-<stamp>` still holds the exact pytest path list.**

### In flight, unchanged this tick
Muse re-build2 round 3 (#761) and Muse rb-fork ORC PR-1 both still running (launched 17:13); fork
#326's `Tests (default)`, `build` and `check` legs still pending on `7559073b`.

## Tick 30 (2026-09-20 ~17:41 EDT)

### Unit 1 — IPI-43 sort/zorder parser — **CLOSED, cells EQUAL**
The like-for-like post-merge replay finished. Gate `gate-ra-rdfsort-212112` ran on merged main
`1127d216` (the IPI-43 squash) with exactly the banked three-file path list recovered from the old
gate unit's journal (`test_ice_rdf_sort_parse_1.py`, `test_rewrite_data_files_options.py`,
`test_write_order_dist_1.py`). All five codes zero (CB=0 R=0 T=0 U=0 L=0).

| replay | BEFORE (banked, pre-merge) | AFTER (merged main `1127d216`) | verdict |
|---|---|---|---|
| unit  | 31 passed, 3 skipped | **31 passed, 3 skipped** | **EQUAL** |
| live  | 34 passed            | **34 passed**            | **EQUAL** |

P-RDF-SORT, P-RDF-SORT-TABLE-ORDER and P-RDF-ZORDER all answer on merged main. Unit 1 is closed:
merged as repark#755, critic PASS (pass 3, zero questions), gate all zero, cells EQUAL.
Residues carried (out_of_scope, not regressions, both pre-existing): (1) retired RDF-SORT-1 still
carries a 2026-08-31 bullet claiming bare `sort_order` binpacks — needs a MULTI-FILE live-Spark cell;
(2) the fork serialises snapshots in HASH order, so `snapshots[-1]` is never a valid head (same
latent bug in `test_write_order_dist_1.py`). RDF-SORT-TRANSFORM-1 stays a declared CALL-door refusal.

### Unit 7 — RP-42 pin bump (#760) — critic pass 2 **NEEDS_REMEDIATION**; Devin round 2 cut
`/tmp/grok-worker/xr-ra-pin42/20260920T211836Z/out.json` (34636 bytes, exit 0). It **closed both
pass-1 findings** on the evidence: V-001's recorded `meta-position-deletes` is exactly CI's measured
`{"statements": [["OK", None]], "probes": [["OK", [[1]]]]}` and the golden reads EQUAL; V-002's four
named rows now speak the present tense correctly. It also confirmed the pin survived the `1127d216`
rebase (five Cargo.toml revs + six iceberg lock sources at `886b94c1`, comment-ban 0). Its two own
mutations behaved (recorded `[[1]]`→`[[99]]` RED; golden EQUAL→DIVERGES **GREEN** — that is finding
V-002 below).

Four new findings, all clerical or pin-strength, none touching design. I verified every premise in
the tree myself before ruling. **Three leans adopted, one overridden:**

- **V-001 (P2) — adopted in the *held* form only, status column NOT rewritten.** The critic asked to
  stamp `ice-parity-inventory-2026-09-19.md` row 52 / IPI-45 closed because it still reads
  "registered refusal". **Measurement first:** that file's own preamble says *"Measured on RePark main
  `6a140eb3` (fork pin `18ab9761`)"* — its status column records what was measured AT THAT PIN, and
  the file has not been touched since its two creating commits. IPI-43 **merged this morning** and its
  row 54 is likewise unchanged. So a status rewrite would falsify the document's measurement contract
  and desynchronise it from IPI-43. But the document defines its own convention for later work, in
  that same preamble — *held*, "re-measure, not assume closed" — and rows 53/54 already carry it.
  RULING: put IPI-45 into the *held* form naming RP-42 and pin `886b94c1`; leave the status column,
  and every other row, alone.
- **V-002 (P2) — adopted, and it is the strongest of the four.** Mutating the golden
  meta-position-deletes EQUAL→DIVERGES leaves the offline suite GREEN (84 passed / 81 skipped),
  because `VERDICTS` values are only read under `REPARK_PARITY_LIVE`
  (`test_v3_statement_coverage.py:330-331`). The EQUAL flip this whole unit turns on is therefore not
  pinned offline. Fix: derive it — assert `_verdict(REPARK[n], SPARK[n]) == VERDICTS[n]` for every
  program in `test_v3_coverage_inventory_carries_every_program_once`. (Same family as every V-001/
  V-002 on #760 and #761: *a pin that a feature-OFF path also satisfies is not a pin.*)
- **V-003 (P3) — adopted.** `docs/spark-sql-iceberg-parity.md:156` still reads, present tense, "the
  `position_deletes` metadata table **is** schema-only — FIXED 2026-09-20". Precedent is V3-COV-3 at
  :2143, whose heading states the FIXED fact instead of keeping the defect name. Retitle, keep `####`.
- **V-004 (P3) — OVERRIDDEN, with reason.** The critic's lean was to drop V3-COV-6 from
  `test_the_fork_routed_rows_name_a_trigger`. That tuple has exactly one element, so dropping it
  leaves an **empty loop that passes vacuously** — a false pin, the very failure family this critic
  polices, and the same shape as trap 6. The finding also exposed a REAL latent bug the critic was
  right about: the test slices `registry[start:start+3000]` and is currently green only because it
  spills into ICE-TT-RESOLVE-1's "TRIGGER: none" — it has never actually read V3-COV-6's own section.
  RULING: bound the window to the row's own section (to the next `^#{3,4} `), keep V3-COV-6 under
  guard, and accept the retired wording (`TRIGGER:` **or** `TRIGGER fired`, which its body now says).

Carried hazard, re-raised by the critic and now partly closed on the record: an empty-stub scan
would still pass C-012 (its CTAS+INSERT fixture has zero delete files, so 0==0), **but** this branch
closes the hole at the facade — the V3-COV program DELETE statements id=2 on a v3 MoR seed whose delete_files
probe is `[[1,'PUFFIN',1]]` and pins `SELECT pos → [[1]]`, which an empty or parquet-only stub fails.
`F-CHANGELOG-READER-1` still has no RePark test — residue.

**Devin clerk round 2 launched** (free tier; this is registry/doc/test-assertion work, no production
code): WO `.../030/wo-ra-pin42-r2.md`, wrapper `launch-ra-pin42-214057` live. It orders the four
rulings above, three named mutations (each must red a named test; mutation 3 asks plainly whether
ANY test observes the inventory row, and to say so if none does), and the full cheap-check gate
including `make check-map-md`.

**#760 CI is now ALL GREEN on `18bde5b1`** (9 pass / 2 skipping) — my two self-applied registry
commits cleared the third registry copy. It is `BEHIND` new main `3dd7b754`, so it needs a rebase
after the Devin round, then a re-gate and a THIRD critic before the queue. Not queued this tick.

### Unit 4 (#761) and unit 6c — rounds still running
Muse r3 on re-build2 and Muse r1 on rb-fork are both alive and growing at 17:41. #761's CI reds are
confirmed to be exactly the two I root-caused last tick (`Python`, `build + import smoke`) — I apply
those two one-line registry fixes myself after the hand-back, for one push and one CI cycle.
NOTE: `gate-re-build2-213229` is running against `/tmp/re-build2` **while Muse r3 edits that same
tree** — its result is not trustworthy and will be discarded; re-gate after the hand-back.

### Unit 6a — fork#326 is **14/14 GREEN and MERGEABLE CLEAN** on `7559073b`
`Tests (default)`, `build` and `check` all passed — the parquet predicate-cache fix holds in CI. The
r2b/r2c ledger round (two evidenced Java divergences, the upstream `without_nested_types` note, and
the `arrow/map.md` Contents gap for `PushedRowFilter` + `row_filter_nested_tests`) is clerk-tier and
is next in line for the first free executor slot; both Muse slots and Devin are spoken for right now.

---

## Tick 31 — 2026-09-20 18:00–18:10 EDT

Both Muse rounds launched at tick 30 concluded exit 0 and both were accepted; both freed slots were
refilled the same tick; all three units that were waiting on a verdict now have a critic running on
their FINAL head.

### Unit 4 (IPI-19/56/37, repark#761) — round 3 accepted, my two registry fixes turned out to be three
**Muse round 3** `/tmp/muse-worker/re-build2/20260920T211312Z/` exit 0, 3 commits, five gates 0
(incl. xgate CB=0 R=0 T=0 U=0 L=0). It closes both critic findings:
- **V-001**: both C-003 fixtures now carry an extra BIGINT column, so `columns_to_add` is non-empty and
  `evolve_schema` actually runs; the missing-column pin asserts `cat` survives with NULL, the narrowing
  pin asserts `id` stays LongType.
- **V-002**: the C-012 pin unsets mid-test, asserts `conf.get` with a default is not `true`, then proves
  the engine-side unset with a second BY NAME write that refuses.
**One deviation, declared and accepted:** the work order named `EXTRA_COLUMNS` as the post-unset refusal
class, but that class is unreachable on an accept-any-schema table — `columns_to_add` errors first with
Java's `IllegalArgumentException: Field extra2 not found in source schema`, and the green C-011 matrix pin
asserts `EXTRA_COLUMNS` is *absent* in exactly that situation, so the literal reading would have reddened
a reviewed pin. The substituted pin still proves `unset` works (a broken unset would evolve, not refuse).
The critic has been asked to judge this substitution independently.
Three mutations, each naming its reddened test with the quoted divergence, all reverted. Zero Rust changes;
`merge/mod.rs` untouched at 1656.

**My own work (orchestrator commits `bba8c522`, `d161c34b`, now `d13efae0` after rebase):** the two CI reds
I had root-caused needed a **third** fix I had not found — the same parity file also carried
`writer_readwriter.py` at **1091** while the branch's file is **1077** (the schema-evolution work moved the
writer's schema options into `writer_schema.py`). `scripts/check_lib_py.py` had already been ratcheted;
only the parity mirror lagged. Fixes applied and verified green (`33 passed`):
1. `test_cap_1_source_file_line_cap.py:35` `merge/mod.rs` 1701 → 1656
2. `test_cap_1_source_file_line_cap.py:71` `writer_readwriter.py` 1091 → 1077
3. `_dfcore_1_expected.py` `EXPECTED_NEW_PACKAGE_SUBMODULES` gains `writer_schema`
plus the `python/repark-parity/tests/map.md` lockstep row, which `make check-map-md` failed on until added
(the pre-commit hook only WARNED — the fifth time that trap has fired).
All six cheap repo guards then rc=0. Rebased onto main `3dd7b754`, comment-ban hits=0, pushed with an
explicit lease (`73f53142` → `d13efae0`). Gate `gate-re-build2-220318` re-queued on the final head; the
concurrent `gate-re-build2-213229` was discarded unread as untrustworthy (it ran while Muse edited the tree).
**Critic pass 2 launched on `d13efae0`** with the deviation and the three registry numbers named as
specific verification targets.

### Unit 7 (RP-42, repark#760) — Devin round 2 accepted, all four pass-2 findings closed
`/tmp/devin-worker/ra-pin42/20260920T215059Z/` CONCLUDED, five commits, five gates 0. Each ruling was
implemented as ruled, including the one I overrode: the TRIGGER window is now bounded to the row's own
section (`^#{3,4} `) with the row kept under guard, rather than dropped from the tuple.
**Mutation 2 is the round's best evidence:** it reddened the bounded test AND replayed the old
`[start:start+3000]` window as a control, proving it stayed green on the *neighbour's* "TRIGGER: none" —
the latent false pin I suspected, now measured.
**Mutation 3 honestly reports a negative result:** no test observes inventory row 52 at all (64
doc-reading tests stayed green with the held marker removed), so the held marker is review-held only.
That is an acceptable answer, and the critic has been asked to confirm it.
Rebased onto `3dd7b754` (the PR was BEHIND), pin verified to survive the rebase (`886b94c1` in the lock),
comment-ban 0, `make check-map-md` rc=0, pushed with lease (`18bde5b1` → `9e07a919`). Gate re-queued.
**Critic pass 3 launched on `9e07a919`**, briefed with all four rulings and told not to re-raise the
inventory status column.
Two stale references the round was forbidden to touch are recorded as dated-paragraph artifacts:
`STATUS.md` ~:157 and `v1-0-iceberg-v3-northstar.md:118`. STATUS.md is never edited by rule.

### Unit 6c (IPI-41 ORC, fork) — PR 1/2 is open: **iceberg-rust#334** (draft)
`/tmp/muse-worker/rb-fork/20260920T211312Z/` exit 0. The vertical split held: 13 files, **+3217,
additive only**, nothing outside `writer/file_writer/`. 51 lib tests pass including the `orc-rust`
round-trip oracle; 1 ORC integration test passes; fmt, clippy `-D warnings`, file-size and comment-ban
all 0. **No stub, no `todo!()`, no `#[allow(dead_code)]`** — the round achieved that by *deleting*
`OrcColumn::field_id` when clippy's `dead_code` fired, and PR 2 re-adds it.
Two honest deviations reported rather than hidden: the brief excluded `spec/table_properties.rs`, so
PR 1 carries four property constants as module-private consts with byte-identical values (PR 2 moves them
and deletes the locals); and the history is 10 commits rather than 6 because folding was not authorized.
It also found and removed a **phantom** `any_writer.rs` map-card row documenting an
`AnyFileWriterBuilder::for_format` that exists nowhere in the tree.
PR 2's recovery path is recorded by blob SHA against the dangling pre-split wip `117c524aa`, with all
twelve deferred tests named. **Critic launched** with the encoder, the Java defaults, `null_repair.rs`
and the independence of the `orc-rust` oracle named as targets.

### Unit 6a (fork#326) — ledger round cut and launched
CI went 14/14 green, so the round is unblocked. Scope turned out **smaller than my notes said**: the
ledger's §13 already carries both declared Java divergences. What is genuinely missing is the round-2c
predicate-cache workaround, which lives only in `task/round-2c-predicate-cache-struct-panic.md` and
reached neither §8 nor the `arrow/` map card. Work order
`.../031/wo-rd-ca-ledger.md` orders exactly two files: the §8 upstream-parquet bullet, and the map card's
`row_filter_plan.rs` row plus a new `row_filter_nested_tests.rs` row. Launched on Muse.

### Unit 5 (rc-meta) — launched
`.../026/wo-rc-meta-r1.md` launched unchanged on Muse (lane head still `3a44b115`, so the work order's
SHAs are valid). It will need a re-rebase before gating.

**Residue added this tick:** none new. **Questions:** none — no ruling was invented.

## Tick 32 — 2026-09-20 ~18:26 EDT

**Critic verdicts (all three landed):**
- **Unit 7 RP-42 (#760) pass 3 on `9e07a919`: PASS.** All four pass-2 rulings implemented as ruled
  (V-001 narrowed to the held marker with the status column untouched; V-002 golden-verdict pin bites;
  V-003 heading takes the V3-COV-3 shape; V-004 window bounded, row kept). Pin verified: five Cargo.toml
  revs and six Cargo.lock `iceberg*` sources all `886b94c1…`; lock diff is only those six lines.
- **Unit 4 IPI-19/56/37 (#761) pass 2 on `d13efae0`: PASS.** V-001/V-002 closed; the critic
  independently confirmed my accepted deviation — `EXTRA_COLUMNS` is unreachable on an accept-any-schema
  table (`columns_to_add` at `evolution.rs:38-47` errors first) and C-011's matrix asserts its absence in
  exactly that case, so the substituted pin is the correct M-9 row-3 error. Mutation 2b is the valuable
  one: a Python-only unset tombstone left the native flag true and `extra2` DID NOT RAISE — the write pin
  is load-bearing. Registry 1656/1077 confirmed against both the SSOT scripts and the parity mirror.
- **Unit 6c fork #334 on `51f16bc60`: NEEDS_REMEDIATION** — one P1, two P2, one P3. I verified every
  premise in the tree before adopting: the nested oracle really does assert only lengths and null bits
  (`orc_writer_tests.rs:441-468`), `map.md:67` really links a `task/ledgers/staging/` path that does not
  exist, and `encode.rs:74` really does `value << 1` on i128 while the writer accepts DECIMAL precision 38
  (`orc_type.rs:213`). All four ADOPTED.

**Actions:**
- **#760 and #761 QUEUED and driven** (`760 RP-42`, `761 IPI-19-56-37` appended to the run16 queue;
  `merge-760-182448` and `merge-761-182448` running). `drive-merge.sh` blocks until its own number is at
  the queue head, so the two serialise by themselves. Both PRs: gate all five zero, critic PASS,
  comment-ban 0, CI all green except the still-running `build + import smoke (debug, host)`.
- **Unit 6a rd-ca ledger round ACCEPTED** (`/tmp/muse-worker/rd-ca/20260920T221128Z/`, exit 0): two
  docs-only commits, 9 insertions / 1 deletion, exactly the two briefed files, comment-ban 0, fmt clean.
  Hand-back's `out_of_scope_observed` corrected two of my own brief's facts: the round-2c finding lives at
  `docs/findings/round-2c-predicate-cache-struct-panic.md` (no `task/` copy), and the FORK HAS NO
  `check-map-md` TARGET (its doc gates are `check-agent-artifacts` / `check-comment-blocks`). Pushed
  fast-forward `7559073b..cc10fb4d` to #326 and launched the FINAL critic
  (`/tmp/grok-worker/xr-rd-ca/20260920T222603Z/`).
- **Unit 6c round 2 launched on Muse** (`/tmp/oc-worker/run27/ticks-xo-opus/032/wo-rb-fork-r2.md`,
  `muse-rb-fork-222558`): tighten the nested oracle to pin list/map/struct VALUES; zigzag in wrapping
  space plus a precision-38 pin and a DECIMAL(38) round trip; make the Java-default claim exact
  (rowIndexStride 0 / no ROW_INDEX and DIRECT vs DIRECT_V2 named as divergences; the 256KiB buffer is
  ORC's `bufferSize`, not the HDFS `write.orc.block-size-bytes`); and **ship the ledger in this PR** at
  `task/f-orc-avro-write-1-ledger.md` (the fork convention, per `task/f-container-accessor-1-ledger.md`)
  with clause C-001 carrying the measured orc-rust rejection evidence, fixing the map.md link.
  Mutation duty ordered on both value pins.

## Tick 33 (2026-09-20 18:32 EDT) — wait tick + unit 6c handed over
**Nothing launched.** All four in-flight items from tick 32 were still running; the correct action was to
measure why the two queued PRs are BLOCKED, complete the 6c handover the owner ordered, and end.

**Unit 6c (IPI-41 ORC/Avro, lane rb-fork) — HANDED OVER, no longer mine.** The owner's 18:25 direction
moves it to xo-muse3, who claimed it at 18:30 and adopted my running round 2 rather than relaunching it.
I appended a final section to `/tmp/oc-worker/run27/handover-6c.md` (round dir, how to judge the
hand-back, the two out-of-scope CI red families, lane state, the fork-specific traps and the trailer
ruling) and dropped `rb-fork` from LANES. Carried forward for the record: critic on #334 returned
NEEDS_REMEDIATION with V-001 (P1 hollow nested oracle), V-002 (P2 Java-default overclaim), V-003 (P3
dangling ledger link), V-004 (P2 i128 zigzag overflow at DECIMAL precision 38); I verified all four
premises in the tree before adopting them and cut round 2 to exactly those four.

**Units 4 and 7 (#761, #760) — diagnosis: not stuck, just slow.** Both are `mergeStateStatus=BLOCKED`,
and I confirmed that is for one reason only: `build + import smoke (debug, host)` is still `in_progress`
(started 22:06:51Z and 22:10:22Z UTC, about 20 minutes into a ~25-minute check). `behind_by=0` on both,
so neither needs a rebase; every other required check is pass or skipping and there are **zero**
failures. Both merge drivers are alive, and `drive-merge.sh` retries exactly this case
(CHECKS-RED-with-zero-failures = still pending), so no intervention was warranted. Recording the
measurement matters because "BLOCKED" reads like a red and would otherwise invite a wasted repair round.

**Unit 6a (rd-ca) — final critic still running** on head `cc10fb4d`; `out.json` is 0 bytes, which is not
a verdict.

**Unit 5 (rc-meta) — round 1 still running**, `out.jsonl` 1.68MB and growing.

**Unit 3 (rc-incr) — blocked by design, not by neglect.** Its `Cargo.lock` is deliberately
skip-worktree-pinned to the new fork rev, so it cannot honestly gate until RP-42 lands on main. It is the
next unit to move and starts the moment #760 merges.

## Tick 34 (2026-09-20 18:48–18:52 EDT)

- **repark#760 (RP-42) MERGED** 22:37:04Z; RePark main `3dd7b754` -> **`6fb68b7b`**, fork pin now
  `886b94c1`. That unblocked unit 3.
- **Unit 3 (IPI-22, rc-incr) — PR OPENED: repark#763.** Undid the deliberate `S`/`S` skip-worktree
  lock patch, rebased 5 commits onto `6fb68b7b`. Two rebase conflicts, both the same import line in
  `crates/repark-core/src/time_travel.rs`; resolved as the union of both sides (main's
  `spark_error` + the branch's `AppendWindow`/`ChangelogTableProvider`/
  `IncrementalAppendTableProvider`). Lock inherits the new pin. comment-ban hits=0;
  `make check-map-md` rc=0; guard sweep `check_lib_py` `check_docstring_presence`
  `check_example_coverage` `check_ledger_grammar` `check_rust_file_size` all rc=0. Parity registry
  row and the staging ledger both ship in the PR.
  **xpr.sh refused the branch once**: two Muse commits carried author `<noreply@meta.com>`
  ("STOP: a commit carries a foreign identity"). Branch was unpushed, so I rewrote exactly those two
  with `filter-branch` — author -> TRO-Wolf, trailer corrected to the canonical
  `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`. Head `91251332`.
  Critic launched: `grok-xr-rc-incr-225032`, `/tmp/grok-worker/xr-rc-incr/20260920T225032Z/`.
  Local gate re-running on the new base.
- **Unit 4 (#761)**: went DIRTY/CONFLICTING the moment #760 merged. Rebased 19 commits onto
  `6fb68b7b` with NO conflicts; pin `886b94c1`; comment-ban 0; 55 files +2196/-243. Force-with-lease
  against the remote head `d13efae0` -> **`21dc2466`**. Re-queued (`761 IPI-19-56-37 18:49`) and
  `merge-761-224910` restarted. Gate re-running on the new base, since the earlier green was
  evidence about the OLD fork pin. The critic PASS stands: the delta is rebase-only.
- **Unit 6a (rd-ca) — FINAL CRITIC: PASS.** Head `cc10fb4d` implements all 13 Spark oracle cells plus
  `st.b`; both mutations behaved (forcing `pushed_mask_disables_predicate_cache` false re-panics the
  nested pin; disabling `field_id_names_a_group` fail-open reds only the page-index oracle cell);
  comment-ban 0; no pin weakened. Two DOCS-ONLY findings, both of which I verified myself against
  `/tmp/oc-worker/rd/container/rv-verify.json` and `rv-logic.json` before adopting:
  V-001 — ledger §8 claims the predicate-cache workaround "also covers list readers" while the code
  requires `!group_is_list`; V-002 — ledger §9 attributes the page-index fix to V-01 (it was L-01),
  the id-less residual to V-04 (it was V-01), and gives original V-04 (the `schema_accessor_charge`
  pin) no row at all. Both premises CONFIRMED. One free Devin clerk round cut for exactly those two
  (`034/wo-rd-ca-ledger-ids.md`, launched `launch-rd-ca-224759`); #326 then un-drafts and queues.

## Tick 35 (2026-09-20 ~19:25 EDT)

**Unit 6a (fork container accessors).** Devin docs clerk round `20260920T225801Z` verified by me:
one commit `6b7d9dc8` touching only `task/f-container-accessor-1-ledger.md` (+11/-9), author
TRO-Wolf, trailer `Authored-By: Devin SWE-2 (swe-2-high) <noreply@cognition.ai>`, comment-ban hits=0,
fork make checks 0. It closes critic V-001 (deleted the false "also covers list readers" claim in §8,
verified against `row_filter_plan.rs:246`'s `!group_is_list`) and V-002 (§9 finding ids retitled to
the verified ids, plus two same-defect stale cross-references in the §10 audit table).
FAST-FORWARD pushed to fork#326 (`cc10fb4d..6b7d9dc8`, ancestry checked, no force, no lease).
Awaiting fork CI, then un-draft and `la-fork-merge.sh 326 … /tmp/rd-ca`.

**Unit 4 (#761) — CI red root-caused, and the code is RIGHT.** The one failing check,
`build + import smoke (debug, host)`, is two pytest pins, not a build break: 2 failed / 11664 passed.
Both pin RePark-invented refusal strings that this unit deliberately retired —
`writer_schema.py` lowers a by-name append carrying extra columns to `INSERT INTO … BY NAME` so the
ENGINE owns the `write.spark.accept-any-schema` decision. I measured the recorded Spark oracle:
`python/repark-parity/fixtures/torture/data/ice_v3_write_default_1/truth.json` cell
`writeto_extra_col` records real Spark as `[INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS] … Cannot
write extra columns \`zzz\`` — byte-for-byte what RePark now emits. The old pin
(`match="extra in the DataFrame"`) pinned a string Spark never prints. The second cell
(`test_writer.py::test_save_as_table_append_extra_column_raises`) now gets
`INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS`; `ice_rtas_byname_1_spark_oracle.json` shows
Spark uses BOTH codes on different surfaces (named SELECT → arity, VALUES → EXTRA_COLUMNS), so the
saveAsTable surface may NOT be reasoned from the writeTo one — the work order orders it measured, and
a numbered residue if no oracle exists. Stopped `gate-re-build2-224910` (its pytest paths never
collected either test) and launched **Muse re-build2 round CI-1** on
`.../035/wo-re-build2-r-ci.md` for exactly those two pins.

**Unit 3 (#763) — critic verdict NEEDS_REMEDIATION**, `/tmp/grok-worker/xr-rc-incr/20260920T225032Z/`.
Two P1s, two P2s, one residual. I verified every premise against the tree before adopting:
- V-001 (P1, ADOPT, confirmed): the unit replaced main's green
  `test_incremental_snapshot_bounds_still_loud` with
  `test_incremental_snapshot_bounds_reach_the_incremental_scan`, which omits the `multi_snapshot`
  fixture — `TABLE` never exists, so `load()` dies `TableNotFound` before either refusal fires.
  This corrects my own earlier framing: the code is fine, the REWRITTEN TEST is the defect.
- V-002 (P1, ADOPT, confirmed): `_commit_snapshot_id` appears only in two SELECT * name lists
  (`test_ice_changelog_1.py:406,:498`) and not at all in the Rust `rendered()` output — a reader
  filling 0 or the scan's `to` id on every row passes every pin. Addendum A-8 predicted this.
- V-003 (P2, ADOPT, confirmed): the only timestamp cell is start-timestamp = 0; no between-snapshots
  pin, so three different resolvers satisfy it equally.
- V-004 (P2, ADOPT, confirmed): `test_denylist_semantic_keys_fail_loud` parametrizes the two
  snapshot-id keys only, not the two timestamp keys.
- V-005 (ADOPT AS RESIDUE, not a fix): `ChangelogReader::read_one_task`'s `DeletedRows` is a
  stream-item error after a successful `plan_files`, so it still surfaces as `PySparkException`;
  RePark never sets `with_row_level_deletes(true)`, so Spark-matching MoR is covered.
The critic also confirmed the probe design works (breaking either refusal map turns a named test red)
and comment-ban hits=0.
**#763's CI "Python" red is separate and trivial**: 8 × `RUF100 unused noqa: B017` in
`test_ice_changelog_1.py`. Folded into the same remediation work order rather than a separate clerk
round. Work order written (`.../035/wo-rc-incr-r2.md`), launch deferred: both Muse slots are full
and the rc-incr local gate is still running on that lane.

## Tick 36 (2026-09-20 19:31 EDT)

### Unit 4 — IPI-19/56/37 schema evolution on write (repark#761)
Muse round **re-build2 CI-1** CONCLUDED exit=0, three commits, delta `21dc2466..4cfba5a0`
(3 files, +17/-4: the two pins + the ledger). No production change — confirming the tick-35
diagnosis that both CI reds were STALE PINS, not code.
- **Cell A** `test_ice_v3_write_default_1.py::test_insert_into_and_extra_column_refuse` now derives
  its expected text from the recorded oracle itself — `_truth()["cells"]["writeto_extra_col"]
  ["message"]` with the catalog token swapped — and asserts it is contained in the raised message.
  This is the strongest available form: the pin can no longer drift from the oracle.
- **Cell B** `test_writer.py::test_save_as_table_append_extra_column_raises` took the ordered
  fallback branch (c). I VERIFIED the executor's premise: `writeto_extra_col` is the ONLY
  extra-column cell in any fixture, and no saveAsTable extra-column oracle exists, so the
  surface-specific code could not be borrowed (per the standing rule that error codes are
  surface-dependent). The pin asserts RePark's measured emission
  `INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS` + quoted table + data-columns list, and the
  ledger carries **numbered residue R-1** for the unmeasured refusal code.
- Gates in hand-back: pytest 49 passed / 1 skipped, comment-ban 0, `check-map-md`,
  `check-ledger-grammar`, `check-ledgers` all 0. I re-ran the comment gate myself: hits=0.
- **PUSHED as a fast-forward** `21dc2466..4cfba5a0` (ancestry verified first); CI re-running.
- **No delta critic owed**: the delta touches only the two test files and the ledger, which is
  exactly the carve-out the tick-35 plan named; critic pass 2 PASS stands.
- Local gate relaunched as `gate-re-build2-233112` **naming all three reachable pytest paths** —
  the previous gate's paths never collected either failing test, which is how both reds reached CI.

### Unit 3 — IPI-22 incremental + changelog (repark#763)
`rc-incr` gate came back **CB=0 R=0 T=0 U=0 L=0**. With the re-build2 slot freed, launched
**Muse rc-incr r2** (`launch-rc-incr-233026`) carrying the remediation work order: critic findings
V-001..V-004 plus the 8 `RUF100` ruff reds, so it closes the critic and CI in one round.

### Unit 6a — fork container accessors (fork#326)
Fork CI after the Devin docs push: **13 pass, 1 pending** (`build (windows-latest)`), zero failures.
Un-draft + queue + `la-fork-merge.sh` as soon as windows concludes.

## Tick 37 (2026-09-20 ~19:52 EDT)
- **fork#326 (6a, F-CONTAINER-ACCESSOR-1)**: all 14 fork CI checks green on `6b7d9dc8`, but the PR was
  `behind_by=3` (diverged), which `la-fork-merge.sh` refuses. Rebased `/tmp/rd-ca` onto fork main
  `886b94c1` — clean, 22 commits replayed, three-dot diffstat IDENTICAL before/after (32 files
  +3118/-862), so no semantic delta and critic PASS still stands. comment-ban hits=0. Pushed
  force-with-lease `6b7d9dc8 -> e9445794`. PR un-drafted (`gh pr ready`) and retitled off the
  hand-over-draft wording. CI re-running on the rebased head; merge (queue line + `la-fork-merge.sh 326
  "<subject>" /tmp/rd-ca`) is the first action of the next tick once green.
- **repark#761 (unit 4)**: re-gated re-build2 with all three reachable pytest paths — `.done` reads
  `CB=0 R=0 T=0 U=0 L=0`. Critic PASS, comment gate 0. One CI check (`build + import smoke (debug, host)`)
  still pending; everything else green. Queue as soon as it concludes.
- **Muse at cap**: rc-meta r1 (unit 5) and rc-incr r2 (unit 3) both alive and progressing. Nothing
  launched this tick. Merge queue empty; pin-bump slot free but unused (no fork merge landed yet).

## Tick 38 (2026-09-20 ~20:08 EDT)

### Unit 4 — IPI-19/56/37 schema evolution (repark#761): QUEUED FOR MERGE
Last CI check (`build + import smoke`) concluded green: 0 failing, 0 pending, `behind_by=0`
(compare API, not mergeStateStatus), mergeable=MERGEABLE/CLEAN. Critic PASS, local gate 0/0/0/0/0,
comment gate 0 were already standing. Appended `repark#761 IPI-19-56-37 20:07` to the merge queue
(queue was empty, so it is at the head) and launched `drive-merge.sh 761` under systemd-run.
Cells to be replayed after the merge lands.

### Unit 5 — IPI-20/23 metadata cols (rc-meta): ROUND 1 CONCLUDED, ACCEPTED WITH ONE GAP
Muse `20260920T221044Z` exit 0, status CONCLUDED, 7 commits, +1426/-7 across 19 files. The narrowing
I ordered was carried out: the Spark door serves **only `_file` and `_pos`** (the two the pinned fork
actually serves); `_spec_id`/`_partition`/`_deleted` refuse typed `[ICE-MC-1]`; `SELECT *` still
returns user columns only. Design mirrors the existing `lineage_columns` layer — a
`prepare_metadata_column_sql` rewrite stage inserted in `router.rs` ahead of the lineage stage, with
`MetadataColumnPins` released on the same path. Registries all moved together (map.md ×7, ledger,
`check_lib_rs.py`, parity doc) and `check_map_md`/`check_ledger_grammar`/`ledger_lifecycle` are 0.
`R-MC-POS-MOR` came back **MEASURED and Spark-equal** (`[[2,0],[3,0],[4,1]]`) — not a residue.

**Accepted deviation (A-6):** `plain_columns_named_like_delete_file_columns_still_resolve` is
deferred, not kept. Root cause measured at the pin and recorded in `out_of_scope_observed`: the fork's
`TableScanBuilder::build` consults `is_metadata_column_name` (whose vocabulary includes the
delete-file `pos`/`file_path`) **before** the table schema, so a user column named `pos` or
`file_path` — and even `SELECT *` over such a table — resolves to reserved field ids and dies "field
not found" in the transformer. Keep-whole and all-gates-green are jointly unsatisfiable at this pin.
This needs a fork schema-first name-resolution fix; no fork work was done here. Five PR-2 deferrals
total, four of them waiting on fork capability that does not exist at the pin (`_deleted` visibility,
struct `_partition`, per-file spec ids / `MetadataScope::Snapshot`).

**GAP — no mutation evidence.** The hand-back carries `residues` but **no `mutations` key**: not one
pin is shown turning red against a mutated implementation. Under my own acceptance rule that is the
difference between a pin and a hollow pin (the same V-001 class xo-muse3 hit on #334). I did not
relaunch for it — the critic's standing charge is exactly "wrong implementations that still pass", so
the sweep is folded into the critic brief and, if it comes back NEEDS_REMEDIATION, into that round.

### THE LANE-DAMAGE FAMILY WIDENS — `.cargo/config.toml` paths override (NEW, COST: ONE VOID GATE)
Muse's own residue 2 disclosed that rc-meta's builds **were not measuring the Cargo pin**.
`.cargo/config.toml` was **skip-worktree (`S`) with an added `[patch.crates-io]` block** redirecting
all five `iceberg*` crates to `/tmp/rc-meta-fork/crates/…` — a clone sitting on the **unmerged** fork
branch `fix/f-metadata-cols-1` @ `8c123edb`, which already contains the sibling fork unit that serves
`_spec_id`/`_partition`/`_deleted`. RePark CI builds pin `3ed905c6`. So the round's green
`CB=0 R=0 T=0 U=0 L=0` was **evidence about a fork that does not exist upstream** and is void.
Exactly the patched-lane failure mode, through a file my hygiene check never looked at:
`git status` clean, `git diff` empty, only the `-v` flag betrays it.
- **Repair applied:** `git update-index --no-skip-worktree .cargo/config.toml` then
  `git checkout -- .cargo/config.toml` → flag back to `H`, override gone.
- **Swept all nine lanes.** Only rc-meta was overridden. rc-incr also carries the `S` flag but its
  content is **byte-identical to its HEAD blob** — flag only, no override, its gate stands.
- Lane then rebased onto `6fb68b7b` (7 commits, clean), `update-ref` on origin/main, comment-ban 0.
- **Re-gated against the true pin** naming ten reachable test files, not one: the rewrite stage runs
  on *every* statement through the Spark door, so `test_metadata_tables.py`, `test_fnp_8_sql_door.py`,
  `test_sql_passthrough_parity.py`, `test_describe_table.py`, `test_e1_errorclass.py`,
  `test_ice_branch_ops_1.py`, `test_writer.py`, `test_spark_sql_grammar_1.py` and `test_sql_alias.py`
  join `test_ice_metadata_cols_1.py`. Muse's gate named the new file alone.
- **Hygiene rule extended:** the pre-gate check is now `git -C /tmp/<lane> ls-files -v` over the whole
  index — not just `Cargo.toml Cargo.lock` — and any `S` on `.cargo/config.toml` must be diffed
  against its HEAD blob before any gate result from that lane is believed.

### Units 3 / 6a — in flight, unchanged this tick
rc-incr r2 running and committing (`a0189c38`, the V-002 `_commit_snapshot_id` pin, Python + Rust).
fork#326 rebased and un-drafted last tick; 14 checks green, **one `build (windows-latest)` still
pending**, `behind_by=0` — merges the moment it concludes.

## Tick 39 (20:26–20:32 EDT)

**Unit 4 (IPI-19/56/37) MERGED as repark#761** — merge commit `9bfd03bf` at 00:26:14Z. Main moved
`6fb68b7b` → `9bfd03bf`. Post-merge cell replay LAUNCHED: `/tmp/re-build2` checked out as
`replay/ipi-19-56-37-main` at merged main and gated with the unit's exact five test paths
(`gate-re-build2-003030`). BEFORE (banked, pre-merge unit branch): unit leg **80 passed / 1 skipped**,
release leg **81 passed**. AFTER: pending that gate.
Note for the record: `/tmp/nc-build` (the `run-engine.sh repark` venv named by COMMON.md §2) **no
longer exists on the box**, so the inventory-harness replay route is unavailable; the like-for-like
pytest replay on merged main — the same route unit 1 used — is what these counts come from.

**Unit 6a (fork#326)** — all 15 fork checks SUCCESS, `behind_by=0`. Appended
`fork#326 F-CONTAINER-ACCESSOR-1 20:28` to the merge queue and launched
`la-fork-merge.sh 326 … /tmp/rd-ca` (`fmerge-326-002708`). It is SECOND in the queue behind another
orchestrator's `764`; the script's `until` loop polls for its own line at the head every 30s, so the
launch is safe and it will fire by itself once 764 clears.

**Unit 3 (IPI-22, repark#763) — round 2 ACCEPTED, pushed, fresh critic running.**
Hand-back `/tmp/muse-worker/rc-incr/20260920T234029Z/` exit 0, CONCLUDED, 6 commits, **no product-code
change**. It carries a full `mutations` sweep — four entries, each naming its test, quoting the
diverging value, `reverted: true`:
1. V-001 — restoring the pre-fix shape (no `multi_snapshot` fixture, literal `1` bounds) gives
   `AnalysisException TableNotFound` instead of the two `IllegalArgumentException` refusals.
2. V-002 — shifting the mapping to `snaps[(ordinal+1) % len]` reddens
   `test_changes_relation_whole_history`; changing the Rust literal `100`→`999` reddens
   `carryover_removal_drops_the_rewritten_but_unchanged_row`.
3. V-003 — adding row `[3,'c','INSERT',0,s1]` to the expected rows reddens the BETWEEN pin.
4. V-004 — dropping `start-timestamp` from `_ICEBERG_INCREMENTAL_OPTIONS` reddens both timestamp pins.
That is the pin-vs-hollow-pin charge answered properly, and it closes the V-002 hollow-pin finding.

**CI red caught by me, not by the round — `out_of_scope_observed` was WRONG.** Muse declared
`ruff format --check` drift on `test_ice_changelog_1.py` as "pre-exists on the base tree … left
untouched per narrow-fix discipline". Both halves are false: (a) ci.yml:198 runs
`uvx ruff@0.15.22 format --check .` repo-wide as a required step; (b) measured with
`git show origin/main:<path> | ruff format --check --stdin-filename <path> -`, **origin/main is clean
on both offending files** — `reader.py` exit 0, `test_facade_polish.py` exit 0 — and
`test_ice_changelog_1.py` does not exist on main at all, so the drift arrived with this unit and the
Python job would have gone red exactly as it did in round 1. **Lesson: an executor's "pre-existing"
defence is a CLAIM; check it against the base blob, and check it with the repo's own config — my
first check ran ruff in a scratch dir with no `pyproject.toml` and falsely said main was dirty too.**
Fixed in `8b4dee78` (my commit, my trailer): `ruff format` on the two files. Verified reflow-only —
whitespace-stripped md5 of `reader.py` is IDENTICAL to its HEAD blob; the test file's only
non-whitespace effect is merging implicitly-concatenated f-string literals (read the -U1 diff in
full). Re-ran `ruff check`, `ruff format --check .`, `check_lib_py`, `check_python_conventions`,
`check_docstring_presence`, `make check-map-md` — all clean, so the reflow did not break reader.py's
1022-line baseline or any registry row.

Lane work: `.cargo/config.toml` skip-worktree flag on rc-incr confirmed byte-identical to its HEAD
blob (flag only, no paths override — the rc-meta family did not recur here), flag cleared; rebased
onto merged main `9bfd03bf` (11 commits, clean); `origin/main` ref refreshed; comment gate hits=0;
pushed `91251332` → `8b4dee78` with a lease on the remote head.
Gate `gate-rc-incr-002844` launched naming **eleven** reachable test files (the round named three):
changelog, time_travel, facade_polish, ice_tt_resolve_1, ice_procs_route_1, ice_spark_table_1,
metadata_tables, spark_sql_grammar_1, writer, writer_v2, sql_passthrough_parity — the reader-option
denylist and the router rewrite stage reach all of them.
Fresh critic `grok-xr-rc-incr-002944` launched on `8b4dee78` (remediation of a NEEDS_REMEDIATION
verdict, so a fresh verdict is owed). Brief charges it with: confirming each of V-001…V-004 closed
for the stated reason, checking the RUF100 deletions did not delete a genuinely needed `B017`
narrowing, and confirming `8b4dee78` is reflow-only; it declares V-005 a numbered residue and the
plan-time-probe design as ruled and not to be re-litigated.

## Tick 40 (20:48–21:05 EDT)

### Unit 4 (IPI-19/56/37) — POST-MERGE REPLAY DONE; **UNIT CLOSED**

`gate-re-build2-003030` finished 20:47 on `/tmp/re-build2` parked at `replay/ipi-19-56-37-main` =
merged main `9bfd03bf`, gated with the unit's exact five test paths. **All five codes 0**
(`CB=0 R=0 T=0 U=0 L=0`).

AFTER counts on merged main:
- unit leg **115 passed / 1 skipped / 0 failed** (116 collected)
- live leg **116 passed / 0 failed**
- release leg built and installed clean; Rust legs green (repark-core 651p/1i, repark-iceberg 636p,
  repark-spark all green).

**A correction to the banked BEFORE.** My tick-39 note banked BEFORE as "unit 80 passed / 1 skipped,
release 81 passed". Those numbers came from a **narrower pytest path list** than the five paths the
replay used, so 80→115 is not a delta in behaviour and must not be reported as one. I measured the
like-for-like instead, which is the stronger statement anyway:

    pytest <the five paths> --collect-only   on merged main 9bfd03bf  → 116 collected
    pytest <the five paths> --collect-only   on pre-merge tip 4cfba5a0 → 116 collected
    diff of the sorted node-id lists          → IDENTICAL, 116 = 116

So the merge added and lost **no test**, and on merged main every one of them answers: 115 pass, 1
skip (the single pre-existing skip), 0 fail. That is the unit's cells reading EQUAL on merged main.
**Unit 4 is CLOSED.** Residues unchanged: R-1 (no saveAsTable extra-column oracle exists; codes are
SURFACE-SPECIFIC) and the single-Transaction composition residue (the fork's `TransactionAction` is
`pub(crate)`).

Method note for the record: `/tmp/nc-build` is gone box-wide, so COMMON.md's `run-engine.sh repark`
leg is unavailable; the like-for-like pytest replay on merged main (unit 1's route) is the evidence,
and the collect-only node-set diff is what makes it like-for-like rather than a bare count.

### Unit 5 (IPI-20/23 metadata cols) — GATE GREEN, REBASED, **PR repark#765 OPEN**, CRITIC RUNNING

`gate-rc-meta-000841` finished 20:28 all five codes 0. Lane hygiene re-checked the full way:
`ls-files -v` shows **no** skip-worktree flag anywhere, `.cargo/config.toml` diffs byte-identical to
its HEAD blob, status clean, pin `886b94c1` — so the green gate is evidence about the real fork.

Rebased `bcd23e7a` → `4845a8ad` onto merged main `9bfd03bf`. The three-dot diffstat is **byte-identical
before and after** (19 files, +1426/-7), so the rebase carries no delta and the gate result stands.
comment-ban 0. Every commit authored `TRO-Wolf <64240326+TRO-Wolf@…>`.

Guard sweep before the PR — all clean: `ruff check .` and `ruff format --check .` (the two CI Python
legs the local gate does **not** cover), `check_lib_py`, `check_python_conventions`,
`check_docstring_presence`, `check_example_coverage`, `check_ledger_grammar`, `check_rust_file_size`,
`check_lib_rs`, `make check-map-md` (rc=0, after `update-ref` of `origin/main` to `9bfd03bf`),
`cargo fmt --all --check`.

**PR repark#765** opened. Critic launched on `/tmp/xr-rc-meta` at `4845a8ad`. The brief carries the
**mutation charge as its primary task** — this unit's hand-back shipped no `mutations` key at all, so
the critic is the first mutation evidence it will have; the brief names five specific lines to break
(`_file` value, `_pos` value, the `[ICE-MC-1]` refusal for each unserved name, the
`prepare_metadata_column_sql`/lineage stage order, `SELECT *`) and asks explicitly which pins a
feature-OFF door would still satisfy. A-6 is handed over as DECLARED with its premise to verify.

### Unit 3 (IPI-22, repark#763) — **CRITIC PASS**, but gate red and CI red

**Critic `grok-xr-rc-incr-002944` returned PASS with ZERO findings** on `8b4dee78`. It confirmed all
four r2 mutations independently, re-derived the Java 1.11.0 `SparkReadConf` string, confirmed the
RUF100 noqa deletions are not a weakened pin, and confirmed `8b4dee78` (my ruff-format fix) is
reflow-only. It ran two of its own mutations, both caught by tests. Its "wrong implementations that
would still pass" list names four, none of which reopens V-001..V-004. V-005 stands as an honest
numbered residue.

Three reds remain, all found this tick, none of them touching the critic's verdict:

1. **CI on #763 — REAL, and rc-incr's to fix.**
   `test_production_file_size.py::test_moved_symbol_bodies_match_the_integrated_baseline` fails.
   This is the **registry lesson again**: rc-incr restructured `time_travel.rs` into a `time_travel/`
   module tree, and the moved-symbol baseline was not moved with it.
2. **Local gate T=1 — rust doctest of repark-core.** `rustdoc --test crates/repark-core/src/lib.rs`
   fails with four errors: `can't find crate for repark_iceberg` (namespace_create.rs:6), and
   `no illegal_argument_error / OverwriteIntent in the root` (range_table.rs:21, session.rs:31,
   time_travel.rs:17). **Suspected not branch-caused:** rc-incr does not touch
   `crates/repark-core/src/lib.rs` at all, and that file is byte-identical to main — it *does* export
   `illegal_argument_error` and `OverwriteIntent` at lines 103 and 86 on both trees. Three of the four
   erroring files are untouched by the branch. The ordinary `cargo test` legs compile the crate fine;
   only the doctest build fails.
3. **Local gate L=1 — 229 errors in the live leg, ONE root cause.** Every one is a teardown error
   reading "`test_writer_v2.py::…` stopped the shared PySpark SparkContext the live oracle runs on".
   `test_writer_v2.py` calls `.stop()` on a session, killing the context every later test needs;
   515 tests still passed. **Suspected an artifact of my own widened path list** — tick 39 I widened
   this gate from the round's three paths to eleven, and `test_writer_v2.py` is one I added.

**Control launched to settle 2 and 3 rather than guess.** `/tmp/re-build2` is parked at merged main
`9bfd03bf` and free, so it is a clean control: same three crates (controls the doctest, item 2) and
the same pytest paths minus `test_ice_changelog_1.py`, which is the only branch-only file of the
eleven (controls the teardown cascade, item 3 — the `.stop()` offender `test_writer_v2.py` and all
its victims are on main). If main reds identically, items 2 and 3 are not rc-incr's and #763 needs
only the item-1 baseline fix.

## TICK 41 (2026-09-20 ~21:22 EDT)

### Unit 3 (IPI-22, repark#763) — the one real red is FIXED and pushed
Devin r3 (`/tmp/devin-worker/rc-incr/20260921T010430Z/`, exit 0) came back CONCLUDED and I verified
every claim myself rather than taking the hand-back's word:
- `git show --numstat HEAD` = **1 file, 1 insertion, 1 deletion** — exactly the one line ordered.
- The changed line is the `_UNSUPPORTED_SEMANTIC_READER_OPTIONS` entry only:
  `c13ff63f…` -> `a54ea8bc…`, matching the value CI's "Differing items" line reported.
- `EXPECTED_SYMBOL_HASHES` holds **172** entries before and after (a value swap cannot change the
  count, and the count re-measures at 172).
- Author `TRO-Wolf <64240326+TRO-Wolf@users.noreply.github.com>`, trailer
  `Authored-By: Devin SWE-2 (swe-2-high) <noreply@cognition.ai>`, no co-author trailer.
- `mutations` is a real sweep, not a claim: it reverted the entry, recorded
  `test_moved_symbol_bodies_match_the_integrated_baseline` going red with the diverging pair quoted,
  then restored and re-verified green. This is the shape a mutation entry should have.
- Its six ruff/guard gates plus comment-ban all exit 0; 82 passed across the three-file pytest leg.
- `out_of_scope_observed` flagged the pre-commit map.md lockstep WARNING. I checked rather than
  assumed: `make check-map-md` in the lane is **rc=0**, and CI's "Repo guards" job is green — the
  unit's map.md rows shipped in an earlier commit and the warning is cosmetic.

Then: rebased 13 commits `9bfd03bf` -> `e0b91bca` (repark#764 landed) cleanly, comment-ban 0, lane
hygiene clean (`ls-files -v` no flags, status clean), and force-pushed with a lease on the old remote
head `8b4dee78` -> **`2a1dfc77`**. `behind_by` is now **0**; CI is re-running.

**Critic re-run, on purpose.** The PASS verdict was for `8b4dee78` and the head is now `2a1dfc77`,
so by the CURRENT HEAD rule that verdict no longer covers what would merge. Launched
`grok-xr-rc-incr-011540` with a **delta brief** that explicitly forbids re-litigating V-001..V-005
and charges it with the only question that matters: is the frozen-hash refresh a legitimate baseline
update or does it hide a regression? It must independently confirm that `start-snapshot-id` /
`end-snapshot-id` are genuinely SERVED now (not merely un-refused — that distinction is the P1),
recompute the hash from the tree, prove exactly one of 172 entries moved, and check the rebase left
the unit's changes whole.

### Unit 5 (IPI-20/23, repark#765) — rebased with a proven-zero delta
Rebased 7 commits onto `e0b91bca`. The three-dot diffstat **md5 is identical before and after**
(`ebe72551…`, 19 files, +1426/-7), so the rebase carries no delta and the critic verdict still in
flight on `4845a8ad` will apply unchanged to the new head `daff9448`. comment-ban 0,
`make check-map-md` rc=0, hygiene clean. Force-pushed with a lease; `behind_by` **0**; CI re-running.
The critic `grok-xr-rc-meta-005101` is still running (out.json empty ~31 min) — no verdict yet.

### Unit 7 — PIN BUMP RP-43 CLAIMED AND LAUNCHED
fork#326 **merged**; fork main is now `df62cdee26daa8c337d07fbc87e879ec9fd02f1a`. The bump slot was
free (no outstanding claim in claims.txt), so I claimed it at 21:20 and prepared lane `ra-pin42`
myself rather than spending an executor round on branch mechanics: fresh branch
`chore/rp-43-fork-pin` off repark main `e0b91bca`, identity and pre-push hook verified, 5 `Cargo.toml`
revs and 6 `Cargo.lock` `iceberg*` sources confirmed still at the old pin `886b94c1`.
Devin clerk round `launch-pin43-011501` launched with `/tmp/xo-xo-opus/wo-pin43.md`, which pins the
acceptance numbers as greps (0/0 old, 5 toml + 6 lock new, exactly two files changed) and — the part
that matters — **forbids touching any test**: a newly-red refusal test at a new pin is the payload
arriving, a design question for me, not a bug for a clerk to paper over.

### Control gate (unit 3 reds 2 and 3) — still running, no reading yet
`gate-re-build2-005323` has been up ~22 min with no `.done`. Its verdict is still the instrument that
decides whether the rustdoc leg and the 229-error `.stop()` teardown cascade are environmental or
branch-caused. Nothing ordered on either until it reads.

## TICK 42 (2026-09-20 ~21:33-21:40 EDT)

### Unit 3 (IPI-22, repark#763 @ `2a1dfc77`) — BOTH LOCAL REDS RESOLVED AS NOT-OURS; DELTA CRITIC PASS
**Delta critic `grok-xr-rc-incr-011540` on the CURRENT head `2a1dfc77`: PASS, ZERO questions.**
It closed the P1 it was charged with: `start-snapshot-id`/`end-snapshot-id` are **SERVED, not merely
un-refused** — traced `DataFrameReader.load`/`.table` (reader.py:365,460) -> `load_incremental` ->
`_native.read_iceberg_incremental` -> `IncrementalWindow::from_options` -> `TimeTravelSpec::Incremental`
-> `IncrementalAppendTableProvider` (time_travel.rs:328-335), with served rows pinned by
`test_incremental_window_between_two_snapshots` (QI-S0-S2) and
`test_incremental_window_open_end_runs_to_the_current_snapshot` (QI-S0-OPEN). It re-measured the
refreshed hash (`a54ea8bc…` = the tree's), ran the revert-mutation (1 failed / 10 passed, "Omitting 171
identical items"), and proved rebase integrity by `git range-diff` `=` on all 12 prior commits with the
unit's six production files byte-identical at `8b4dee78` vs HEAD. Disclosed honestly what the frozen-hash
test still would NOT catch (`_ICEBERG_INCREMENTAL_OPTIONS`, `collect_incremental_window`, `reader.py`,
`reader_incremental.py`, comment/whitespace edits) — those doors are pinned by `test_ice_changelog_1.py` /
`test_facade_polish.py` instead. Recorded, not a residue.

**CONTROL GATE `gate-re-build2-005323` RETURNED (.done mtime 21:18:49 > queue 20:53:23): CB=0 R=0 T=0 U=0 L=1.**
Decisive on both legs, and they split:
- **Red 3 (live pytest, 229 errors) is PRE-EXISTING / ENVIRONMENTAL — NOT rc-incr's.** The control ran on a
  lane parked on merged main, with the branch-only file removed, and reproduced the cascade exactly:
  458 hits of "stopped the shared PySpark SparkContext", **229 errors**, 478 passed. Same count, same
  victims (`test_sql_passthrough_parity.py` et al). `test_writer_v2.py`'s `.stop()` is on main and poisons
  every file collected after it. Nothing is owed from the branch on this axis.
- **Red 2 (repark-core RUSTDOC) did NOT reproduce: control T=0**, zero hits of `can't find crate` /
  `no illegal_argument_error` / `OverwriteIntent` in its repark-core leg. So that red is **lane-local to
  `/tmp/rc-incr`**, consistent with the stale-`target/` hypothesis and with CI's rust jobs all green, and
  NOT explained by the control. Re-gated rc-incr at 21:38:04 on the current head `2a1dfc77` with the same
  three crates and the path list **narrowed to 10 (minus `test_writer_v2.py`)** to settle it on the lane's
  own terms rather than assume.
**#763 status:** behind_by **0**, head = the critic's head, `mergeable=MERGEABLE`, `mergeStateStatus=BLOCKED`
with **zero failures and exactly one pending check** ("build + import smoke (debug, host)") — i.e. a
required check still running, not a red. Queue as soon as that check lands green and the re-gate reads clean.

### Unit 5 (IPI-20/23, repark#765 @ `daff9448`) — CRITIC: NEEDS_REMEDIATION, 4 findings, ALL TEST-SIDE
CI is **all green** (9 SUCCESS, 2 SKIPPED, 0 pending, 0 fail) and behind_by **0**. The critic confirmed the
substance: the door **serves Spark-equal values** for all five cells (`R-MC-FILE`
`[[2,true,true],[3,true,true],[4,true,true]]`, `R-MC-FILE-DISTINCT`/`R-MC-FILE-FILTER` `[[3]]`, `R-MC-POS`
`[[2,0],[3,0],[4,0]]`, `R-MC-POS-MOR` `[[2,0],[3,0],[4,1]]` — measured in `spark-core.json`, confirming it is
NOT a residue), serves the `[ICE-MC-1]` tag for the three unserved names, no production `unwrap`/`expect`,
comment-ban 0, no weakened tests, IPI-43 untouched. **Deviation A-6's premise VERIFIED TRUE at pin
`886b94c1`** (`TableScanBuilder::build` consults `is_metadata_column_name` before the table schema) — the
deferral stands on a true premise.
**The mutation sweep I put in the brief as its PRIMARY charge paid for itself** — it was this unit's first
mutation evidence, and it found three hollow pins:
- (a) prefixing every `_file` with `mutated-` left all five tests green (C-001 is only LIKE predicates,
  C-002 only a DISTINCT count, C-003 a tautology); a constant `bogus.parquet` did fail.
- (b) `_pos` constant `0` still passed C-004; only C-005 (MoR) caught it.
- (c) a refusal message with no column name, or one always naming `_spec_id`, left all tests green.
- (d) swapping `prepare_metadata_column_sql` / `prepare_lineage_sql` left all tests green.
- (e) door-off (`Ok(None)`) failed every test — so the compose legs ARE real pins; only the bare `SELECT *`
  assertion inside the star test is feature-off-satisfiable.
**CLASS NAMED: "hollow pin — an assertion a deliberately wrong implementation still satisfies."**
Work order `/tmp/xo-xo-opus/wo-rc-meta-r2.md` cut and Muse launched (r2): V-001 assert the refusal names the
column (rust + pytest twin); V-002 add a pin that reddens when the two router stages are swapped (preferred:
a format-v3 `SELECT _file, _row_id`; if v3 lineage is unreachable at the pin, MEASURE that and pin the
rewritten SQL instead, declaring the fallback); V-003 KEEP the Spark-equal LIKE cell and ADD an identity pin
of each `_file` against `$files.file_path`; V-004 correct ledger M-1's fork SHA `3ed905c6` -> `886b94c1` and
drop the inapplicable skip-worktree story. The order requires a CLASS SWEEP of every assertion in both test
files and a `mutations` key in the hand-back, and forbids any production edit (a mutation that exposes a real
production defect is escalated to me as a `questions` entry, not fixed by the executor).

## Tick 43 (2026-09-20 21:56–22:03 EDT)

### Unit 3 — IPI-22 incremental + changelog (repark#763)
CI went **fully green**: 9 SUCCESS / 2 SKIPPED / **0 pending / 0 failures**, including the
"build + import smoke (debug, host)" check that was the last pending required one (24m44s).
Critic PASS stands on the current head `2a1dfc77`. The narrowed re-gate
(`gate-rc-incr-013804`, queued 21:38:04, release leg rc=0 at 21:57) is the only open item;
the decision rule for its two possible readings was fixed in advance last tick and has not
been changed after seeing any result.

### Unit 7 — RP-43 fork pin bump: **PR OPEN as repark#766**
Devin clerk `devin-ra-pin42-011742` concluded at 21:52 with zero questions. Verified by my own
measurement rather than its claim: old rev `886b94c1` appears **0** times, new rev `df62cdee`
**5** times in `Cargo.toml` and **6** in `Cargo.lock` (the sixth is the transitive
`iceberg-sketches`); the three-dot diff names **exactly two files** (+11/-11); author
`TRO-Wolf`, trailer `Authored-By: Devin SWE-2`; no test edited, which is the point of a bump —
a newly-red refusal test at a new pin is the payload arriving and my design call, not a
clerk's edit. Lane hygiene clean (`ls-files -v` no flags, status clean), pre-push hook present,
already on current main `e0b91bca` so no rebase was owed.

**A red that was my own work order's fault, and the lesson it bought.** CI's Repo guards job
failed 19s in: `ERROR: map.md was not updated on this branch (map.md lockstep rule)`. My work
order had explicitly *forbidden* touching map.md, so Devin correctly reported the pre-commit
hook's map.md warning as out-of-scope — and on tick 41 an identical warning genuinely *was*
cosmetic. The difference: that branch carried other registry rows, this one carries nothing but
`Cargo.toml` and `Cargo.lock`. Checking the precedent settled it in one read — **every** bump
RP-39 through RP-42 appends exactly one bullet to the fork-pin list at `map.md:91`. I wrote the
RP-43 row myself (`c2c2e6ad`, `Authored-By: Claude`), naming F-CONTAINER-ACCESSOR-1 `#326` and
its root cause; `make check-map-md` rc=0, comment-ban 0, fast-forward pushed, CI re-running.
**Standing correction for any pin-bump work order: say "append the RP-<n> row to map.md in the
LAST commit" — never "do not touch map.md".**

Gate `gate-ra-pin42-015800` queued 21:58:00 over `repark-core,repark-iceberg,repark-spark` plus
the fork#326 payload's blast radius (nested-container parity, nested dict/struct, nested evo,
promote-read, read-formats, metadata tables). Critic `grok-xr-ra-pin42-015923` launched on
`304a43be` with a bump-shaped brief: exactly-two-files, rev arithmetic re-counted from the
tree, every changed `Cargo.lock` package must be an `iceberg*` package, and a mutation charge
that reverts the pin to the old rev — with **both** outcomes declared informative in advance,
so the critic is not pushed toward inventing a finding when a bump is legitimately invisible to
RePark's own suite.

### Unit 5 — IPI-20/23 metadata cols (repark#765)
The Muse r2 round launched at 21:36 inside a backgrounded shell **did not survive the tick
boundary** — no stamp directory and no systemd unit, exactly the failure the last tick told
itself to check for. Relaunched at 21:57 **under `systemd-run --collect`**
(`launch-rcmeta-r2-215738`) so it now owns its own unit and cannot die with the tick. It is in
`r7-launch`'s normal cargo-wait behind three gates.
**Rule: launch an executor under systemd-run, never from a tick's backgrounded shell.**

## Tick 44 (2026-09-20 22:20 EDT)

### Unit 3 — IPI-22 incremental + changelog: **#763 QUEUED**
All four merge conditions met on the CURRENT head `2a1dfc77`:
- critic `grok-xr-rc-incr-011540` **PASS**, zero questions, for that exact head;
- CI **9 SUCCESS / 2 SKIPPED / 0 pending / 0 fail**;
- `behind_by` **0** measured through the compare API immediately before queuing;
- comment gate 0; re-gate `.done` @22:16:04 (queued 21:38:04, so fresh) **CB=0 R=0 T=0 U=0 L=1**.

The T red of the previous run **cleared to 0**, which confirms the control-lane reading: it was a
lane-local stale `target/`, not branch-caused.

**The L=1 was root-caused and dismissed, per the rule fixed BEFORE the result was known.** The run
has **0 FAILED**; all 195 non-passes are teardown ERROR lines raised by `conftest.py:52`'s
`_shared_oracle_context_guard`, and the guard **names its own culprit**:
`tests/test_ice_procs_route_1.py::test_live_oracle_matches_recorded stopped the shared PySpark
SparkContext` (`test_ice_procs_route_1.py:69 session.stop()`). That file is **not touched and not
modified by this branch** (`git diff --name-only origin/main...HEAD` and
`git log origin/main..HEAD -- <path>` both empty), and 481 tests passed. This is the same
environmental `.stop()` cascade family already proven on merged main at 229 errors — a different
`.stop()` file surfacing because this gate's path list is narrower. No code round ordered.
**Numbered residue R-3: the live pytest leg cannot be read as a pass/fail signal while any
`.stop()`-calling test shares its process; the count varies with the path list, not with the branch.**
Queued `763 IPI-22 22:20`; `drive-merge-763-222057.service` running.

### Unit 7 — RP-43 pin bump (#766)
CI on `c2c2e6ad`: **Repo guards PASS** — my map.md lockstep row fixed the only red. Eleven checks
pass, two skip, one ("build + import smoke") pending.

**The first critic was not a verdict.** `grok-xr-ra-pin42-015923` returned `status: HALT`,
`num_turns: 1`, zero tool calls, summary "starting verification critic…". Per the standing rule a
1-turn HALT is not a verdict whatever its first token says — discarded, not read as a result.

Relaunched on the **CURRENT head `c2c2e6ad`**, which discharges the delta-critic debt directly
instead of reviewing `304a43be` and arguing the delta. The brief was corrected first: it charged
"exactly two files, any third is a P1", which the map.md row now (correctly) violates. It now
charges **exactly three files — `Cargo.toml`, `Cargo.lock`, `map.md` — a fourth is a P1**, plus:
the map.md change is REQUIRED (CI enforces the lockstep), must be **exactly one appended bullet**
under the fork-pin list in the RP-39..RP-42 shape, any other row touched is a P1, and
`make check-map-md` must pass on the branch itself.

**Lesson banked:** when a self-inflicted CI fix adds a file to a branch, the critic brief's
file-count charge is now WRONG and will manufacture a P1 against my own fix. Re-read the brief
after every commit that changes the diff's file set.

### Unit 5 — IPI-20/23 (#765)
Muse r2 confirmed **alive** this tick (`muse-rc-meta-020741.service`, stamp
`/tmp/muse-worker/rc-meta/20260921T020741Z/`) — the launch-survival rule worked: launching under
`systemd-run` produced a unit that outlived the tick boundary that killed tick 42's round.

## Tick 45 (2026-09-20 22:45 EDT)

### Unit 3 — IPI-22 incremental + changelog: **MERGED (repark#763) — CLOSED**
Merged 2026-09-21T02:21:07Z, merge commit `d5c39802` (now repark main). Head at merge `2a1dfc77`,
delta critic `grok-xr-rc-incr-011540` **PASS** for that exact head, CI 9 SUCCESS / 2 SKIPPED,
`behind_by` 0 measured at 22:20, re-gate `.done` @22:16:04 `CB=0 R=0 T=0 U=0 L=1`.
The single L was root-caused and dismissed before the queue: **0 FAILED**, 481 passed, 195 teardown
ERROR lines from `conftest.py:52`'s `_shared_oracle_context_guard`, culprit named by the guard itself as
`tests/test_ice_procs_route_1.py::test_live_oracle_matches_recorded` (`:69 session.stop()`), a file
the branch neither touches nor modifies (both ownership greps empty).

**Cell replay on merged main.** Lane `ra-rdfsort` re-parked as `replay/ipi-22-main` at `d5c39802`
(hygiene clean, `ls-files -v` no non-`H` entries). Collect-only NODE-ID sets over the unit's ten
pytest paths: **486 on merged main, 486 on the pre-merge tip `2a1dfc77`, `diff` empty** — identical
node sets, so **CELLS EQUAL** on the like-for-like instrument (never a bare count).
Replay gate `gate-ra-rdfsort-224129` queued 22:41:29 over the same crates and **nine** of the ten
paths; `test_ice_procs_route_1.py` is excluded **by design** — it is the proven `.stop()` culprit and
its presence makes the live leg unreadable rather than red.

**Residues carried:** V-005 (`read_one_task` `DeletedRows` is a stream-item error after a successful
`plan_files`; RePark never sets `with_row_level_deletes(true)`; `F-CHANGELOG-READER-1` unpinned) and
**R-3** — the live pytest leg is not a pass/fail signal while a `.stop()`-calling test shares its
process; the error count tracks the path list, not the branch (229 on one list, 195 on another).

### Consequence for the other PRs
A merge makes every open PR stale within the minute, and it did: **#765 behind_by 1 and now
CONFLICTING**, **#766 behind_by 1, diverged**. Neither was rebased this tick, and both holds are
deliberate: `rc-meta` is being edited by Muse r2 right now, and `ra-pin42` is mid-gate
(`gate-ra-pin42-015800`). Rebasing or gating a lane under an executor, or rebasing a lane under a
gate, voids the very evidence the rebase is meant to preserve.

## Tick 46 (2026-09-20 23:04 EDT)

### Unit 7 — RP-43 fork pin bump (repark#766)
- **Critic PASS** — `/tmp/grok-worker/xr-ra-pin42/20260921T022012Z/out.json`, exit 0, `num_turns 26`,
  `status CONCLUDED`. Verdict was for head `c2c2e6ad`. It re-counted the rev arithmetic from the tree
  (0 occurrences of the old rev `886b94c1`, 5 in `Cargo.toml`, 6 `Cargo.lock` source lines, every changed
  lock package an `iceberg*` package, the sixth being transitive `iceberg-sketches`), confirmed exactly
  three files, one appended `map.md` bullet at `:92` in the RP-39..RP-42 shape, `make check-map-md
  BASE=origin/main` exit 0, and ran the brief's charged mutation: revert `Cargo.toml` to the OLD pin,
  `cargo test -p repark-core -p repark-iceberg -p repark-spark --offline` exit 0, 0 failed. Both outcomes
  were declared informative in advance, so the green is the NORMAL result: the bump is behaviourally
  invisible to RePark's own suite because the payload is covered fork-side.
- **Local gate** `gate-ra-pin42-015800`: `.done` @22:43:32 (> queue time 21:58:00), **CB=0 R=0 T=0 U=0 L=0**.
- **Rebase onto merged main** `d5c39802` after the gate finished (never mid-gate): clean, `c2c2e6ad` ->
  `2d937e9f`. Rebase-delta instrument: three-dot diffstat md5 **`37f23fd6` identical** before and after, so
  the gate and the critic verdict both stand on the new head. `update-ref`, comment-ban hits=0,
  `make check-map-md` exit 0, force-with-lease push (`+ c2c2e6ad...2d937e9f (forced update)`).
  **behind_by re-measured 0**; CI re-running (10 pending, 2 skipping).
- **Two OWNER questions, both RECORD-ONLY** (the critic's own lean and mine agree):
  - **V-001 P2 coverage gap** — no RePark test would go red if fork#326 were absent. RePark's closest
    tests (`crates/repark-spark/src/tests/list_null_compound.rs:150,:180`) DELETE then `SELECT id`, which
    never projects the single-leaf struct and so never runs `plan_row_filter`. The fork's own tests pin the
    payload. Smallest RePark follow-up, if ever wanted: `CREATE TABLE (id INT, xs STRUCT<a:INT>)`, insert a
    null struct + a `named_struct`, then `SELECT xs WHERE xs IS NULL`. **Numbered residue, not a blocker.**
  - **V-002 P3** — the branch is two commits and the `map.md` lockstep follow-up `that head`'s child carries
    my `Authored-By: Claude (claude-opus-5)` trailer rather than Devin's. Left as-is: it is an honest record
    of who wrote that commit, and no co-author trailer exists on either commit.
- Next: queue on CI green (PASS + gate all-zero + green + behind_by 0 are then all satisfied), then release
  the bump slot with a `BUMP MERGED RP-43` claims line.

### Unit 5 — IPI-20/23 metadata columns (repark#765) — Muse r2 still running
Three remediation commits already in the lane: `cf61892b` (V-002 metadata-before-lineage order pinned on a
v3 table), `80b044c9` (V-003 `_file` identity pinned against `$files.file_path`), `02869592` (V-004 ledger
M-1 corrected to fork pin `886b94c1`). V-001 and the ordered CLASS SWEEP / `mutations` are still in flight.
#765 is left **CONFLICTING and behind_by 1 deliberately** — the rebase onto `d5c39802` is owed only after
the hand-back, because rebasing a lane an executor is editing voids the evidence.

### Unit 3 — replay gate still running
`gate-ra-rdfsort-024129` (queued 22:41:29) is the merged-main replay over nine of the unit's ten pytest
paths. The node-set half of the evidence is already banked (486 = 486, `diff` empty).

## Tick 47 (2026-09-20 23:18 EDT) — a pure watch tick, no action owed
All three open threads were mid-flight and none had reached a decision point:
- **RP-43 / repark#766** @ `2d937e9f`: `behind_by` re-measured **0** (compare API `ahead 2 / behind 0 /
  status ahead`). PASS + gate all-zero + comment-ban 0 already satisfied and carried across the tick-46
  rebase by the diffstat-md5 identity (`37f23fd6`). CI: 9 pass, 2 skipping, **1 in progress** —
  `build + import smoke (debug, host)` (run `35556011363`, started 03:00:20Z). That single job is the only
  remaining condition; the PR is queued the moment it is green with `behind_by` re-measured 0.
- **Unit 5 / rc-meta**: `muse-rc-meta-020741.service` still ALIVE (out.jsonl 3.8 MB), no `handback.json`.
  The r2 remediation round is unfinished, so the owed rebase onto `d5c39802` (CONFLICTING) is correctly
  deferred — never rebase a lane an executor is editing. repark#765 remains CONFLICTING/DIRTY @ `daff9448`
  and therefore gets no CI at all, as expected.
- **Unit 3 replay gate**: `gate-ra-rdfsort-024129.service` ACTIVE, `.done` **absent** — absent means
  RUNNING (xgate deletes it at the start), which is the correct reading rather than the stale 17:23 file
  that was there last tick.
main(repark) unmoved at `d5c39802`. No Muse or Devin launched: unit 5's slot is occupied by the running
round and every other unit on the slate is closed or merged. Nothing in the merge queue.

## Tick 48 (2026-09-20 23:35 EDT) — RP-43 QUEUED; units 3 and 5 still measuring

**RP-43 / repark#766 — every merge condition closed, queued.** CI finished fully green on head
`2d937e9f`: 10 SUCCESS / 2 skipping, the last job `build + import smoke (debug, host)` passing in
19m50s (run `35556011363`). Re-measured `behind_by` in the minute before queuing, as the
merge-readiness lesson requires: `ahead 2 / behind 0 / status ahead`, `mergeable true`,
`mergeable_state clean`. The other three conditions were already satisfied and carry to this head by
the rebase-delta argument (three-dot diffstat md5 `37f23fd6` identical across `e0b91bca` ->
`d5c39802`): critic PASS `grok-xr-ra-pin42-022012`, local gate all five codes 0, comment-ban 0.
Appended `766 RP-43 23:33` to `/tmp/oc-worker/run16/merge-queue.txt` and started
`merge-766-233337.service` (`drive-merge.sh 766`). A `BUMP MERGED RP-43` claims line releases the
single bump slot once it lands. Its two owner questions stay RECORD-ONLY: V-001 P2 numbered residue
(no RePark test goes red without fork#326) and V-002 P3 (the map.md lockstep commit carries my
trailer, which is honest).

**Unit 3 replay gate** `gate-ra-rdfsort-024129` still running on merged main `d5c39802`: rust rc=0
and unit rc=0 (449 passed / 3 skipped / 2 xfailed) at 23:33, live leg outstanding, `.done` absent.
The node-set half of the replay evidence is already banked (486 = 486, `diff` empty).

**Unit 5 / rc-meta Muse r2** alive, out.jsonl 4.4 MB at 23:29, no hand-back. Worth recording: at
22:59 the worktree was dirty on `crates/repark-core/src/metadata_columns.rs`, which I flagged as
either a mutation in flight or a forbidden production edit. It is now clean there — the dirt is
`crates/repark-spark/src/tests/map.md`, `python/repark/tests/map.md` and the staging ledger only,
and none of the three commits touches production. That is a MUTATION, reverted: the round is
honouring the work order's no-production-edit rule, and the earlier reading was the instrument
working, not a violation.

## Tick 49 (2026-09-20 23:51 EDT) — UNIT 7 CLOSED, UNIT 3 REPLAY CLOSED
- **RP-43 / repark#766 MERGED** at 2026-09-21T03:33:47Z (23:33 EDT) by `merge-766-233337.service`
  (now inactive). main `d5c39802` -> `5b1391ab`; fork pin `886b94c1` -> `df62cdee26daa8c337d07fbc87e879ec9fd02f1a`,
  carrying fork#326 (container accessors) and fork#332 (position_deletes scan) into RePark.
  **`BUMP MERGED RP-43` filed in claims.txt — the single pin-bump slot is free for every lane.**
  Unit 7 CLOSED. Residues unchanged: V-001 P2 (no RePark test reds without fork#326; smallest follow-up
  = `CREATE TABLE (id INT, xs STRUCT<a:INT>)` + `SELECT xs WHERE xs IS NULL`), V-002 P3 (map.md lockstep
  commit carries my trailer), C-012 Rust delete cell, `F-CHANGELOG-READER-1` still unpinned in RePark.
- **Unit 3 (IPI-22) replay COMPLETE.** Gate on merged main `d5c39802`, lane parked as `replay/ipi-22-main`:
  `.done` @23:34:39 reads **CB=0 R=0 T=0 U=0 L=0**; unit leg **449 passed / 3 skipped / 2 xfailed**,
  live leg **452 passed / 2 xfailed**. With tick 45's identical 486-node collect-only sets across the
  unit's ten gate paths (`2a1dfc77` vs `d5c39802`), **CELLS EQUAL** on both halves of the evidence.
  Unit 3 CLOSED. Residue V-005 and the R-3 live-leg disclosure stand as recorded.
- **Unit 5 (IPI-20/23) is the only open unit.** Muse r2 (`muse-rc-meta-020741.service`) still ALIVE at
  23:50 — out.jsonl 4.6 MB, no `handback.json`; its own gate `gate-rc-meta-032857` active. Lane head
  `02869592`, worktree dirt = two map.md + the staging ledger only (no production edit).
  **New at this tick: main's pin is now `df62cdee`, so the V-004 ledger M-1 correction to `886b94c1`
  becomes stale the moment rc-meta rebases** — the post-handback rebase must re-point M-1 at `df62cdee`.

## Tick 51 (2026-09-21 00:25 EDT) — unit 5 IPI-20/23, round 2 closed out and re-verified

Muse round 2 (`/tmp/muse-worker/rc-meta/20260921T020741Z/`, exit 0) CONCLUDED with a hand-back that
carries the `mutations` key the work order demanded: **20 mutations (F1–F16 incl. F3b/F7b/F13b/F14b),
each naming the test it reddened, each `reverted: true`**, zero `questions`, six commits. The round is
**test- and ledger-side only** — `git diff --stat daff9448..49d401b7` touches exactly
`crates/repark-spark/src/tests/metadata_columns.rs`, `python/repark/tests/test_ice_metadata_cols_1.py`,
two `map.md` files and the staging ledger. The four critic findings are closed in order: V-001 names the
refused column (rust loop + backtick leg + pytest twin), V-002 pins the metadata-before-lineage stage
order with the PREFERRED form (a format-v3 `SELECT _file, _row_id`, no fallback needed), V-003 adds the
`_file` == `$files.file_path` identity pin beside the kept LIKE cell, V-004 rewrote ledger M-1.

Rebase onto merged main `5b1391ab`: **GitHub reported CONFLICTING, the rebase replayed all 13 commits
with zero conflicts**, and the rebase-delta md5 (`git diff --stat <base>...HEAD | md5sum`) is
`7cc40ca9…` on both sides — the branch's unique content is untouched. Recorded as a lesson: *a GitHub
CONFLICTING flag predicts a merge, not a rebase, and is not by itself proof that the old evidence dies.*

The pin bump RP-43 arriving in the new base made ledger M-1 stale a second time, so commit `26d9744f`
re-points it `886b94c1` -> `df62cdee` and states honestly that the values were first measured at
`886b94c1` and re-run at `df62cdee`. **Deviation A-6's premise re-verified at the new pin** by reading
the cargo cache directly: `crates/iceberg/src/scan/mod.rs:461` still tests `is_metadata_column_name`
before the table schema, so the A-6 deferral stands unchanged.

Pre-push checks at the final rebased head, all clean: comment-ban 0, `make check-map-md` 0, the standard
guard sweep (`check_lib_py` `check_docstring_presence` `check_example_coverage` `check_ledger_grammar`
`check_rust_file_size`) 5/5 zero, `uvx ruff@0.15.22 check .` 0 and `format --check .` 0, lane hygiene
`ls-files -v` fully `H`. Pushed with lease `daff9448` -> `26d9744f`; **#765 is now MERGEABLE, behind_by
0, ahead_by 14**, with CI running.

In flight at tick end: my own local gate (`gate-rc-meta-042512`), `make rust-clippy` + `rust-panic-ban`
(`lint-rcmeta-002519`), and the **round-2 critic** (`grok-xr-rc-meta-042600`) on the current head. The
r2 brief is delta-shaped: it charges the four closures, makes the **hollow-pin class sweep** its primary
charge (including the `_pos`-constant trap and a door-off `Ok(None)` re-measurement), demands
`git range-diff` rebase integrity, orders M-1 checked against the TREE, and — added this tick — orders
the critic to **re-run at least four of Muse's own 20 mutations**, because a mutation battery in a
hand-back is a claim like any other.

## Tick 52 (00:43 EDT) — unit 5 / repark#765 pre-queue evidence banked
- **Lint at head `26d9744f`: `CLIPPY=0` `PANICBAN=0`** (`/tmp/xo-xo-opus/lint-rcmeta.done`, written 00:27).
- **CI at `26d9744f`: 8 pass / 2 skipping / 1 pending, ZERO red.** Pass: Python, Repo guards, Rust lint
  (fmt+clippy+check), Rust test (workspace) 11m0s, cargo-deny, taplo, typos, zizmor. Pending: `build +
  import smoke (debug, host)` job `106213516302`. Table banked at `ticks-xo-opus/052/ci-765.txt`.
  PR state word is `blocked` = that one pending required check, NOT a red.
- **`behind_by` 0, ahead_by 14, `mergeable: true`** — re-measured this tick.
- **Lane hygiene re-verified**: `ls-files -v` fully `H`; `Cargo.lock` `iceberg` pins
  `df62cdee26daa8c337d07fbc87e879ec9fd02f1a` (the RP-43 rev, correct); comment-ban **0**.
- Still running: `gate-rc-meta-042512` (`.done` absent = RUNNING) and `grok-xr-rc-meta-042600`
  (out.json 0 bytes, unit ACTIVE = RUNNING, not dead). No launches made; 0 of 2 Muse slots used by me.

## TICK 53 (01:01-01:05 EDT) — UNIT 5 MERGED; THE SLATE IS CLOSED BUT FOR ONE GATE

### Unit 5 — IPI-20/23 metadata columns + time travel — **MERGED (repark#765)**
All five queue preconditions met at the CURRENT head `26d9744f`, in one tick:
- **Critic `grok-xr-rc-meta-042600`: PASS**, `num_turns` 42, zero questions, on the current head.
  V-001..V-004 all CLOSED and each closure checked by its own mutation, not by the commit message
  (V-004 was verified from the TREE: `Cargo.toml:162-166` + all six `Cargo.lock` `iceberg*` sources
  agree on `df62cdee`, no `.cargo/config.toml` override).
- **The hollow-pin CLASS SWEEP — the brief's primary charge — found NO fifth hollow pin.** Door-off
  (`prepare_metadata_column_sql` -> `Ok(None)`) fails 0 of 7 Rust pins; the constant-`0` `_pos` trap
  reds `pos_is_the_file_position_after_a_merge_on_read_delete` (`[(2,0),(3,0),(4,0)]` vs `...(4,1)`).
  M-8 recorded: the FILTER `count(*)==3` leg is hollow for nullness; C-010 plus the empty-projection
  count pin cover it.
- **THE SIXTH FACE OF THE EXECUTOR-CLAIM LESSON IS SATISFIED.** The critic independently reproduced
  F2, F10 and F11 of Muse's 20 claimed mutations, quoting the diverging values. A `mutations` block
  is now evidence, not a claim.
- **Rebase integrity confirmed by a second party**: `range-diff` three `!` are conflict-resolution
  context only (`catalog/mod.rs` gained IPI-22's `incremental_append`/`scan_batches`; the map.md files
  gained changelog rows); `metadata_columns.rs` byte-identical; 13-commit delta md5
  `7cc40ca90da03679ba3bb8527b272ce0` matches my own claim.
- **Local gate `.done` @00:47:51 (fresh, after the 00:25 queue time): CB=0 R=0 T=0 U=0 L=0.**
- CLIPPY=0 / PANICBAN=0 and comment-ban 0 banked at this head; CI table **9 SUCCESS / 2 skipping /
  ZERO fail** (`build + import smoke` passed at 18m51s — it was the last pending required check);
  `behind_by` 0 re-measured in the minute before queuing.
- Queued `765 IPI-20/23 01:01`; `merge-765-010141.service` merged it in **18s**.
  **main `5b1391ab` -> `0eca6de0afec5e64c8ad090f53abc0d851a39135`.**

### Unit 5 replay (per COMMON.md) — node sets IDENTICAL
`/tmp/ra-rdfsort` parked as `replay/ipi-20-23-main` at `0eca6de0`, whole index `H`.
`--collect-only` over the unit's three gate paths (`test_ice_metadata_cols_1.py`,
`test_metadata_tables.py`, `test_time_travel.py`):
**BEFORE (pre-merge tip `26d9744f`) 59 — AFTER (merged main `0eca6de0`) 59 — `diff` EMPTY, node-id
lists IDENTICAL.** Like-for-like: the same path list and the same runner on both sides, per unit 4's
correction. Banked at `ticks-xo-opus/053/nodes-{pre,post}-765.txt`.
Replay gate queued 01:02:59 on the merged-main lane (`gate-ra-rdfsort-050259.service`) — the second
half of the replay evidence. **The `.done` sitting on disk is unit 3's 23:34 reading, already banked;
compare the mtime against 01:02:59 before believing it.**

### NOTE FOR EVERY OTHER LANE
A merge puts every other open PR behind within the minute: **#759, #769 and xb-ddl are now
`behind_by >= 1` against `0eca6de0`** and must re-measure before they queue.

### Unit 5 replay — SECOND HALF IN: merged-main gate ALL FIVE ZEROS
`gate-ra-rdfsort-050259.service` (queued 01:02:59, head `0eca6de0`) finished 01:32:39 and wrote a
FRESH `.done` — mtime 01:32:39 is after the queue time, and the stale 23:34 file was deleted by
xgate, so the mtime trap named in tick 54 is closed and this reading is this gate's.
**`CB=0 R=0 T=0 U=0 L=0`** — log header `== head 0eca6de0 01:08:40`, `release rc=0 01:17`,
`rust rc=0 01:32`, `unit rc=0 01:32` (59 passed), `live rc=0 01:32` (59 passed).
Crates `repark-core,repark-iceberg,repark-spark`; paths `test_ice_metadata_cols_1.py`,
`test_metadata_tables.py`, `test_time_travel.py`. Banked at
`ticks-xo-opus/055/ra-rdfsort-localgate-013239.done`.

**BEFORE/AFTER for unit 5, both halves, like-for-like:**
| | pre-merge tip `26d9744f` | merged main `0eca6de0` |
|---|---|---|
| collected nodes (3 gate paths) | 59 | 59 — `diff` EMPTY, node-id lists IDENTICAL |
| local gate CB/R/T/U/L | 0/0/0/0/0 (@00:47:51) | 0/0/0/0/0 (@01:32:39) |
No red leg, so no false-red family had to be applied and **no post-merge finding exists for unit 5**.

## SLATE CLOSED
All seven units are merged and every unit's replay is banked. Nothing is owed to any executor,
critic, PR or gate. Muse slots 0 of 2; no PR of mine open; pin-bump slot free.
Standing residues are unchanged and listed per unit above: V-005, R-1, R-3, V-001 P2, V-002 P3,
unit 5's five PR-2 deferrals (four waiting on fork capability absent at the pin), unit 6b's C-010
PROVEN->OPEN demotion, and the record-only out_of_scope items. **STATUS: DONE at tick 55, 01:37 EDT.**
