# Run 21a report — 2026-09-17 → 18 overnight: Iceberg read path, DML, conflict detection, nested evolution

> **Orchestrating-session note (2026-09-18 07:30 EDT).** Where this report says the campaign directory "came back from an
> earlier copy" or that `/tmp/oc-worker` "was a real directory, not the symlink": that is an inference, and the measurement
> does not support it. The durable directory holds files written in every hour of the night (3 / 7 / 14 / 21 / 35 / 38 /
> 21 / 10 / 9 files for the hours 20:00 through 04:00), and the merge queue inside it was last written at 04:34, so the
> symlink was in place until the freeze. Individual late files are missing — most likely never flushed during four hours
> of memory thrash before the hard reset — and every lane under `/tmp` was lost, as the report says.

**Run:** 21a (unit `overnight-21a`, lane prefix `ka-`). **Window:** 19:50 → 07:00 EDT. The box ran out of memory from 00:10
(OOM kills), froze at 04:46, and was rebooted at 05:02. **Orchestrator:** claude-opus-5.
**Actors:** Claude Opus 5 at high effort (design units), Devin SWE-2 (fork units), Muse Spark 1.3 contributor (one pins round).
**Reviewers:** Grok 4.6. **Slice:** run 20a's (`/tmp/oc-worker/run19/list-a.md`).

## 1. Residual matrix, measured on merged main (`e7857f9f`) at 05:35

| Rating row | Before tonight | After | Where |
|---|---|---|---|
| V2-10e — MERGE/UPDATE/DELETE after ADD / RENAME COLUMN refuse; a rename swap wrote the other field's values | MISSING (fix on an unmerged branch) | **FIXED on main**: 183 recorded Spark 4.1.2 cells (38 added tonight for the critic's shapes) on both doors, v2/v3 × CoW/MoR, live leg re-derives all 183 | #687 `75c0d96a` |
| V2-24b — `partitionOverwriteMode=dynamic` replaced the whole table | MISSING (draft #682) | **FIXED on main**: typed static-overwrite flag, BY NAME and column-list overwrites reconciled with main, 24 BY NAME cells incl. one measured tonight | #682 `50740088` |
| V2-20a / DML-5 — serializable DML aborts on any concurrent commit | fork half pinned only after RP-24 (#685, tonight) | **FIXED on main for MERGE and DELETE**: partition-scoped MERGE/DELETE and MERGE-vs-INSERT answer Spark (4/4, 2/2), and Spark's own refusals are matched. Still OPEN: plain-WHERE UPDATE 1/4 (fork exec, fork #294) and the 16-INSERT storm 5/16 against Spark's 16/16 (fork retry budget, BACKLOG) | #692 `e7857f9f` |
| V2-10d — a Spark-added nested child makes the table unreadable; nested DDL refused | MISSING | **fork half merged and pinned** (fork #292 `64705c99`, RP-25 #690 `e3e3a946`): Spark tables whose struct gained a child are readable on main. The RePark half (nested DDL, adoption pins) is round 1 on a draft, 5 P2s open | fork #292, #690, #695 |
| V3-05 — multi-argument transforms | MISSING (loud) | unchanged; not opened | card |

**Measurement that overturns the rating.** Spark 4.1.2 + Iceberg 1.11.0 were measured on this box tonight. The rating's
headline V2-20a probe is 8 concurrent disjoint-KEY MERGEs on an unpartitioned merge-on-read table, and **Spark commits 1 of 8
there, exactly as RePark did**. The losers say `Found conflicting files that can contain records matching true`. The real gap
is the partition- and range-scoped shapes, where Spark commits 4/4 and 2/2. The recordings are in #692's fixture
`python/repark-parity/fixtures/torture/data/ice_occ_scoped_1/`.

## 2. Pull requests

| PR | Repo | Unit | Actor, rounds | Reviews (Grok 4.6) | Comment gate | State |
|---|---|---|---|---|---|---|
| #685 | RePark | RP-24 pin bump → `8fb44a39` (fork #291) | orchestrator | — | 0 | **MERGED** `2c28bec7`, tree-equal |
| #687 | RePark | ICE-EVO-DML-1 | Opus 19a + Muse 20a (2) + **Muse 21a (1)** | logic **PASS** (L-01, L-02 P2; 3 P3), $0.64 | 0 | **MERGED** `75c0d96a`, tree-equal |
| #682 | RePark | ICE-DYN-OVERWRITE-1 | Muse 20a (2) + orchestrator comment strip + **Opus 21a (1)** | first verification of the typed-flag rewrite **PASS** (5 P3), $0.73 | 0 (12 lines stripped) | **MERGED** `50740088`, tree-equal |
| fork #292 | fork | F-NESTED-EVO-1 | Muse 20a (1) + **Devin 21a (3)** | logic NEEDS_REMEDIATION (2 P1), $0.60; Rust perf NEEDS_REMEDIATION (P1 ~4000× per batch), $0.51; verification 1 NEEDS_REMEDIATION (3 P2, mutation-proof pins), $1.61; verification 2 NEEDS_REMEDIATION (W-01 P2), $1.37; W-01 closed under Q-21a-4 and re-mutated by the orchestrator (3/34 red) | 0 beyond ASF headers | **MERGED** `64705c99`, tree-equal (two merge attempts failed on GitHub TLS timeouts first) |
| #690 | RePark | RP-25 pin bump → `64705c99` (fork #292) | orchestrator | — | 0 | gated after the reboot on main + #692 (release facade 10,049 passed / 0 failed, parity 755 / 0, `make verify` 3,878 / 0); queued 06:11, **MERGED** `e3e3a946`, tree-equal (06:35) |
| #692 | RePark | ICE-OCC-SCOPED-1 (RePark half) | **Opus 21a (2)** | logic PASS (3 P2 → round 2), $0.52; Rust perf PASS (3 P3), $0.36; verification PASS (2 P3), $2.07 | 0 | **MERGED** `e7857f9f`, tree-equal (05:31, after a post-reboot `make verify` lane) |
| fork #294 | fork | F-OCC-EXEC-1 (plain-WHERE UPDATE/DELETE exec scopes by its own scan filter) | Devin 21a (1; round 2 lost) | logic+verification NEEDS_REMEDIATION: F-01 P2, a CAST around a FLOAT column with an underflowing literal (`f < 1e-50` → `f < 0.0`) narrows both the conflict filter AND the scan's file pruning (a pre-existing silent DELETE miss on an all-zero FLOAT file); mutation proof passed, $0.99 | 0 beyond ASF header (6 reworded-comment lines deleted by the orchestrator) | **DRAFT** at round 1 |
| #695 | RePark | ICE-NESTED-EVO-1 (RePark half) | Opus 21a (1; round 2 lost) | logic NEEDS_REMEDIATION (5 P2), $0.91; Rust perf PASS (4 P3), $0.24 | 0 | **DRAFT** at round 1 |
| — | fork | F-LIST-INSERT-1 | Devin 21a (1) | — | — | **LOST**: three local commits (red, fix, footer proof), never pushed |

Grok 4.6 across 12 review rounds: **$10.54**. Devin SWE-2 (free tier): 5 rounds. Muse: 1 round (134 steps). The Opus actors ran
5 rounds (ICE-OCC-SCOPED-1 ×2, #682 ×1, nested ×2 of which round 2 was lost). They ran in `claude -p` text mode, which records no
`total_cost_usd`; that is a gap in this report's accounting.

## 3. Lost to the freeze, and what each item needs

The OOM freeze wiped `/tmp`, and the campaign directory came back from an earlier copy, so most `ka-` logs written after about
20:15 and several reviewer reports are gone. Everything below was ONLY in a lane:

- **#692 ICE-OCC-SCOPED-1**: `make verify` on the rebased head was running. A single post-reboot lane (`/tmp/ka-v692`,
  build-slot lock, 6 jobs) re-ran it on `bf6f62cd` — result: `make verify` rc 0 (57 test binaries, 3878 passed, 0 failed); #692 was queued at 05:28. Before the freeze, the release-native facade suite ran
  with **0 failures**. The parity suite was green except `test_torture_v3_dv`, which uses a fixed `/tmp` path, collides with
  other lanes, and passes alone (7/7).
- **#690 RP-25**: its gates never ran before the freeze. A second post-reboot lane re-ran them on main + #692, all green, and
  it was queued at 06:11. 21b's fork #293 (`8477b249`) is already on fork main and rides RP-26 (21b's state).
- **#695 nested round 2**: the Opus round on the five P2s was mid-run. Lost entirely.
- **fork #294 round 2**: Devin's fix for F-01 was mid-run. Lost.
- **F-LIST-INSERT-1**: lost entirely. Its brief is intact at `/tmp/oc-worker/ka-fork3/brief-list-insert-1.md` if the directory
  survived; the defect text is reproduced in §5.
- Local run logs, gate logs, and several Grok reports (evo, dyn, occ, nested, fx).

## 4. Rulings

- **Q-21a-1**: the 16-line ASF license header in a NEW fork file is not a banned comment. It is legally required, and the
  fork's license gate enforces it. Every fork diff is gated net of those lines.
- **Q-21a-2**: the orchestrator stripped #682's 12 doc-comment lines itself, using `#[allow(clippy::missing_errors_doc)]` where
  needed (precedent Q-20c-10). The Opus round went to the real design issue it then exposed: main's INSERT … BY NAME called the
  overwrite entry with the old arity (E0061), and BY NAME ignored the dynamic mode. That round also found and repaired one of
  my rebase resolutions, which had dropped `PARTITION_OVERWRITE_MODE_KEY` from `RuntimeConfig.unset`'s restore tuple.
- **Q-21a-3**: #687 critic L-01 (a schema-only ALTER between a DML's plan and commit is not guarded by a current-schema
  assertion) is Iceberg-wide, since Java's snapshot producer carries the same requirements. It is recorded as a residue, not a
  unit divergence.
- **Q-21a-4**: in a nested struct that mixes stamped and unstamped field ids, when a missing target child shares a name with an
  unstamped source child, the read fails loud with `DataInvalid` pointing at `schema.name-mapping.default`. Two review rounds
  showed that struct-local information cannot tell the live field from a dropped same-named one; guessing either way is silent.
  Residue F-NESTED-MIXED-ID-1 (measure Java's reader on a hand-built file).
- **Q-21a-5**: a FLOAT/DOUBLE range predicate leaves the conflict filter unscoped (`AlwaysTrue`). This is stricter than Spark,
  which scopes it and carries the fork metrics' NaN/±0 gap: a float-range DML can abort where Spark commits, but never commits a
  conflict.
- **Q-21a-6**: RP-25 went out to `64705c99` (fork #292 only) at 04:10 instead of waiting for #294, F-LIST-INSERT-1 and 21b's
  #293, so the nested RePark half could start. The later fork PRs ride RP-26.
- The actors' own rulings keep their ledger numbers: Q-21a-DYN-1…3 in #682's ledger, Q-21a-OCC-1…3 in #692's, and Q-21a-1…5 in
  #695's round-1 ledger. The last set was written before the prefix convention; renumber it to Q-21a-NEST-N in round 2.

## 5. Findings for the owner

1. **Every INSERT into an Iceberg `ARRAY` column fails on main (loud).** It fails with
   `column types must match schema types, expected List(Int32, field: 'element', metadata: {"PARQUET:field_id": "3"}) but found List(Int32, field: 'element')`,
   raised in the fork writer's `apply_write_defaults` (`crates/iceberg/src/writer/write_defaults.rs`). `writeTo().append()`,
   `insertInto` and `saveAsTable(append)` fail the same way; struct and map columns are fine. This is not a regression: the
   2026-09-10 build refused ARRAY columns at CREATE. **Recommendation:** re-run F-LIST-INSERT-1 first in the next fork lane:
   relabel nested fields to the Iceberg types while keeping element field ids in the parquet footer.
2. **A pre-existing silent wrong DELETE/UPDATE in the fork's DataFusion exec (fork #294 critic F-01).**
   `DELETE … WHERE f < 1e-50` on a FLOAT column prunes away a data file holding only `0.0`, because the CAST strip plus `as f32`
   underflow turns the pushed predicate into `f < 0.0`. **Recommendation:** fix it inside #294's round 2 (exact-or-drop
   conversion); the brief is at `/tmp/oc-worker/ka-fork2/followup-occ-exec-2.md`.
3. **The rating's V2-20a premise was wrong** (see §1). The registry rows in #692 now carry Spark's measured refusals, so nobody
   "fixes" a shape Spark itself refuses.
4. **Test hygiene:** three tests use fixed `/tmp` paths and collide across concurrent lanes: `test_torture_v3_dv`
   (`/tmp/repark-torture-v3dv`), `crates/repark-spark/src/tests/call_register.rs` (`/tmp/repark-v3-1-spark-mor`, in-process
   lock only), and the q14 `current_date` pin, which fails locally between 20:00 and 24:00 EDT (session UTC vs local date).
   **Recommendation:** a small hygiene unit (tmp_path everywhere, and compare `current_date` against the session zone).
5. **The freeze itself:** about nine concurrent cargo invocations across three runs took 101 GB. This run contributed up to six
   heavy jobs at once (three Opus actors building, two Devin fork builds, gates). The new caps (one cargo job per orchestrator,
   6 jobs, build-slot lock) are the right rule.

## 6. Next run, in order

1. #692 merged at 05:31 (`e7857f9f`).
2. RP-25 is on main. Next, RP-26 (fork #293 plus whatever of #294 / F-LIST-INSERT-1 has landed).
3. Fork F-LIST-INSERT-1 (re-run from the brief), fork #294 round 2 (F-01).
4. #695 round 2 (the five P2s, brief `/tmp/oc-worker/ka-nest/brief-2.md`), verification, gates after RP-25.
5. V3-MULTIARG-1 card: the v3 spec reserves multi-argument transforms, and Spark 4.1.2 refuses `bucket(4, id, name)` at DDL.
   The first step is a hand-built v3 metadata fixture with `source-ids`, to see whether RePark can at least LOAD such a table.

## 7. STATUS.md

The residue entry on main no longer carries V2-24b or V2-10e; #682 and #687 each deleted their clause. #692 narrows the V2-20a
clause to the plain-WHERE UPDATE (equal bytes). V2-10d stays until #695 merges.

## 8. Rust-first roll-call

Every change tonight is Rust: the conflict-filter derivation (`repark-iceberg/src/write/conflict_filter.rs`), the dynamic
overwrite decision (`insert_overwrite::overwrite_is_dynamic`), the evolved-DML scan (fork API `project_current_schema`), the
nested projector (fork `arrow/nested_projection.rs`), nested DDL (`repark-spark/src/nested_column_ddl.rs`,
`repark-sql/src/alter/nested.rs`, `repark-iceberg/src/write/nested_column.rs`), and the fork exec filter. No decision was added
to Python.

## 9. Disk

1.3 T free at launch; 623 G at the low point (04:05, three runs). Removed after merge: `/tmp/ka-bump`, `/tmp/ka-dyn` (67 G),
`/tmp/ka-evo`, and the review clones. The reboot removed the rest. At 05:15, 1.3 T free.
