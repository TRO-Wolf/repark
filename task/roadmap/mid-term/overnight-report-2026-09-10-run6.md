# Overnight report — run 6 of 2026-09-10 (the review-fix run)

**Session:** Opus 5 orchestrator, alone on the box, 15:06 → 00:07 local (2026-09-11), across **two
processes**: the first ended on its 400-turn cap at ~21:24 local mid-rebase (not on a decision);
the second resumed from live state at 21:25 · **Grants:** G-1, G-2, G-3 stop 06:30 local, G-4 Muse
only (first process) then **Muse + Devin SWE-2 + Grok fallback** (owner, evening — §5), G-5 ·
**Order:** [review-fix-slate-2026-09-10.md](review-fix-slate-2026-09-10.md) §1 rows 1–11 plus M-0
and O-1 · **Cards:** [review-1-findings-2026-09-10.md](review-1-findings-2026-09-10.md) §3,
[cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) (R-22, DISPLAY-LAZY-1,
CONF-UNREAD-1 as narrowed by RF-4).

## 1. What landed

| Row | Unit | PR | Result | Worker | Rounds |
|---|---|---|---|---|---|
| 1 | REVIEW-FIX-4 | [#478](https://github.com/TRO-Wolf/repark/pull/478) | **merged `59910754`**, tree-equal | Muse | 1 |
| 2 | REVIEW-FIX-7 (+ RF-3 warning as D-3) | [#479](https://github.com/TRO-Wolf/repark/pull/479) | **merged `8b2673fd`**, tree-equal | Muse | 1 |
| 4 | REVIEW-FIX-10 | [#480](https://github.com/TRO-Wolf/repark/pull/480) | **merged `2220db62`**, tree-equal | Muse | 2 |
| O-1 | `check-docs-links` dual-wired into ci.yml guards | [#481](https://github.com/TRO-Wolf/repark/pull/481) | **merged (#481)**, tree-equal | orchestrator | — |
| 5 | REVIEW-FIX-12 | [#483](https://github.com/TRO-Wolf/repark/pull/483) | **merged `ec73ae40`**, tree-equal | Muse | 1 |
| 3 | REVIEW-FIX-5 (+ RF-2 as D-3, RF-6 as D-4) | [#482](https://github.com/TRO-Wolf/repark/pull/482) | **merged `e4dafea7`**, tree-equal | Muse | 4 |
| 6 | REVIEW-FIX-1 + REVIEW-FIX-2 | [#484](https://github.com/TRO-Wolf/repark/pull/484) | **merged `2f82df95`**, tree-equal | Muse | 2 |
| M-0 | BALLISTA-M1-A…D ledger departure | [#485](https://github.com/TRO-Wolf/repark/pull/485) | **merged `51e18132`**, tree-equal | Muse | 1 |
| 9 | REVIEW-FIX-6 + REVIEW-FIX-11 | [#487](https://github.com/TRO-Wolf/repark/pull/487) | **merged `cadf2382`**, tree-equal | Muse | 2 |
| 7 | DISPLAY-LAZY-1 (both steps) | [#489](https://github.com/TRO-Wolf/repark/pull/489) | **merged `fd09db62`**, tree-equal | Muse | 3 |
| 10a | REVIEW-FIX-8 | [#488](https://github.com/TRO-Wolf/repark/pull/488) | **merged `553a41d8`**, tree-equal | Muse | 2 |
| 11 | REVIEW-FIX-13 + REVIEW-FIX-15 | [#490](https://github.com/TRO-Wolf/repark/pull/490) | **merged `43db9a82`**, tree-equal | Muse | 2 |
| 8 | REVIEW-FIX-3 + -9 + -14 | [#492](https://github.com/TRO-Wolf/repark/pull/492) | **merged `2fd72326`**, tree-equal | Devin SWE-2 | 1 |
| 10b | CONF-UNREAD-1 (both steps) | [#493](https://github.com/TRO-Wolf/repark/pull/493) | **merged `60e2c553`**, tree-equal | Muse (step 1) + Devin (step 2) | 3 |
| O-2 | BALLISTA-M2-A step-0 seed (`datafusion-proto` behind `cluster`) | [#494](https://github.com/TRO-Wolf/repark/pull/494) | **merged `974e66ae`**, tree-equal | orchestrator | — |

**Every review-fix-slate §1 row 1–11 merged, plus M-0, O-1 and O-2 — 16 PRs, all tree-equal.** With the
list exhausted at 23:39, the run did only the O-2 seed, as instructed, and stopped; no BALLISTA-M2-A
worker round was opened.

Every merge followed runbook §5 with auto-merge off: `gh pr update-branch`, checks watched in the
foreground, explicit squash, then the tree-equality check. The whole parity suite and `make verify`
were re-run by the orchestrator before every push, including after every rebase.

## 2. Decisions taken under G-2 (one line each)

- **R5-A / A-2 / DL-1 round 3 — no size baseline was raised, three times.** `check_rust_file_size.py`
  and `check_lib_py.py` say an increase needs explicit owner approval, which the orchestrator does
  not hold. Each time a fix grew a file over its exact baseline, the lines were funded by deleting
  legacy comment lines in the same file under the no-code-comments ruling (facts moved to the
  directory `map.md`), and the row ratcheted **down**: `catalog_config.rs` 1044 → 1028 (FIX-5),
  `core.py` 4487 → 4486 (FIX-6) → 4485 (DISPLAY-LAZY-1). DISPLAY-LAZY-1's round 2 had raised the
  row to 4489 as "a reasoned SSOT edit"; that was reverted and its ledger/map/commit record corrected.
- **R5-B — `prop_key_is_secret` stands for DESCRIBE TABLE EXTENDED.** RF-6's parenthetical ("the one
  DESCRIBE NAMESPACE EXTENDED uses") was wrong about the tree — NAMESPACE uses Spark's
  `Utils.redact` regexes. The named predicate wins because RF-6's stated reason (Spark prints
  `s3.access-key-id`) only holds for it. Two DESCRIBE surfaces now use two predicates by decision.
- **R5-C — a disclosed regression was fixed, not filed.** FIX-5's worker disclosed that only test
  sessions installed the Owner extension, so production DESCRIBE would print `Owner unknown`. Home
  was extended to `repark-core/src/session.rs`; the type moved (same name, same prefix) to a new
  `session_owner` module installed once at build; a production-session pin was added.
- **R10-A / R10-B — REVIEW-FIX-10 D-1's line-start anchor was amended.** The worker measured that it
  reds the whole-tree gate on two frozen `completed/` ledgers whose `**Path:** READING` sits
  mid-line. Adopted: field anywhere outside code spans, value = leading `[A-Za-z0-9_-]+` run.
- **FIX-2 D-2 spelling.** Neither offered spelling was available (in-process env mutation is
  `unsafe` against `unsafe_code = "forbid"`; the builder takes no env); the control session uses the
  forced empty staged file.
- **FIX-14 D-2** (ceiling on `repark.display.max_rows`): delegated to the Devin round with an
  instruction to choose, pin and record it. It chose **refuse-loud above 10,000**
  (`INVALID_CONF_VALUE.REQUIREMENT`), recorded in the ledger's C-002 row.
- **CONF-UNREAD-1** (RF-4's four keys): three wired, `datafusion.execution.coalesce_batches`
  refused loud — DataFusion 54.1.0 defines it but no engine path reads it. User-visible: setting it
  now raises.
- **STATUS.md.** Gate-only units (FIX-10, FIX-12, FIX-8, FIX-13/15, M-0) got no STATUS sentence;
  user-visible ones fold into one rolling "REVIEW-1 fixes" paragraph. STATUS sat at its 25,000 B
  ceiling all night; every addition was paid for by compacting archived or superseded prose, never
  by raising the ceiling.
- **The `_Last updated:` stamp pin** in `test_v1_gate_docs.py` moved 2026-09-07 → 2026-09-10, as
  the DFCORE slate-close truth-up had moved it before.

## 3. Measurements and catches worth keeping

- **FIX-8's first CI red.** `test_target_file_size_applies_at_table` passed here and failed on the
  runner (`assert 5 > 5`). Diagnosed as the MERGE-rewrite file count tracking the core-count-derived
  partition count (21 vs 5 here, 3 vs 3 at `target_partitions=2`); the pins now fix
  `target_partitions=16` and count rewrite-only files. The superseded "50 vs 5" stays in the
  completed ledger under a prepended errata note. **Lesson for briefs:** every pin that counts files,
  batches or partitions pins the parallelism inputs.
- **FIX-12's D-5** uncovered a genuinely stale same-file anchor in
  `docs/spark-sql-iceberg-parity.md` (the `V3-COV-8` heading gained a suffix); fixed in the same PR.
- **FIX-13** re-measured the Ballista audit from a scratch clone of tag 54.1.0 with the command
  beside every number; the unreproducible `20110` figure is gone.
- **DISPLAY-LAZY-1** closed a gap its own worker disclosed: a plain `localCheckpoint()` rendered
  lazy against D-2; the checkpoint arm now records `_eager_shape` (C-007).
- **Disk-full, self-inflicted (~21:35).** Lane clones reach 31–44 GB each after `make develop` +
  `make verify`; eleven merged-but-kept clones took `/` from 438 GB free to 25 GB. It killed the
  CONF-UNREAD-1 Muse round (ENOSPC, empty `exit`, no hand-back), one orchestrator verify and two
  of the Devin round's own verifies. 363 GB reclaimed by removing the merged clones; the Muse round
  was resumed from its uncommitted tree. Runbook §5 already says to remove a clone at merge — that
  step is now done every time (memory `disk-consumers` updated).
- **Renaming a clone is not free.** Moving `/tmp/oc-cu1` → `/tmp/dv-cu1` for the Devin lane left
  test binaries with the old `CARGO_MANIFEST_DIR` baked in; fixture tests failed `read src` under
  `cargo test --workspace`. The Devin worker diagnosed it and ran `cargo clean`. Future lane
  switches clone fresh instead.
- **Orchestrator lane-prep error, twice.** Copying the live `.venv` into a clone does not bring the
  native module (it lives in the source tree, gitignored) and leaves `.pth` files pointing at the
  live checkout. Two workers (FIX-1, FIX-6) found no `repark._native`; one stopped honestly, one
  restored it from the uv wheel cache. Every clone now gets its `.pth` files repointed and
  `make develop` run by the orchestrator before launch.

## 4. Open questions for the owner

- **Dead-variable citations (FIX-15).** Moving `pins:` citations into tests under the comment ban
  produced `_pins = "pins: …"` string bindings in test bodies. It satisfies the grammar gate and the
  card, but it is a new idiom; if the owner prefers citations only in `map.md`, the card needs a
  re-ruling.
- **FIX-7's warning channel.** The ambient-cloud-catalog warning prints with `eprintln!`; when CFG-1
  step 5 lands R-19's database warning the two should share one channel.
- **`R-DESCRIBE-TWO-PART`** stays in the registry: dbt sessions complete to the unregistered
  `datafusion` catalog until the facade current-catalog and the engine default are unified.
- **Owner-merged runbook change mid-run.** #486 (G-4 grants `devin-worker`) landed on `main` at
  ~20:20 while the first process held a "Muse only" grant; that process kept to Muse. The resumed
  process received the grant change directly (§5).

## 5. Grant change and lane split (second process)

Owner, 2026-09-10 evening: Muse is metered (54 % of its period); Devin SWE-2 (free) takes every
remaining M-tier round, up to two Devin lanes; Muse only finishes in-flight rounds and I-tier rounds,
one lane at a time; Grok is the fallback for a misbehaving Devin card.

- On resume, two Muse rounds were running: REVIEW-FIX-15 (`rf13`, launched 21:19) and
  REVIEW-FIX-3/9/14 (`rf3`, launched 21:24, one minute before the process hit its cap). **`rf3` was
  stopped** at ~434 events with no commit and the card moved to Devin (`dv-rf3`); `rf13` was allowed
  to finish as the single in-flight Muse round.
- CONF-UNREAD-1 step 1 is I-tier (Rust wiring), so it runs on Muse (`cu1`) once `rf13` freed the
  one Muse slot. Its step 2 (docs) is M-tier and goes to Devin.

**Muse rounds tonight: 24 launched, 22 completed.** First process: 20 completed + `rf3` stopped at
resume. Second process: `rf13` (in flight at resume) completed; CONF-UNREAD-1 step 1 launched twice
(the first killed by the disk-full event, the resume completed). At most one Muse lane ran at a time
after the grant change.
**Devin SWE-2 rounds: 2, both completed, both merged** — `dv-rf3` (REVIEW-FIX-3/9/14, 98 agent steps)
and `dv-cu1` (CONF-UNREAD-1 step 2, 70 agent steps). The first build card passed its acceptance
audit: commit present, `%ae` byte-exact, trailer last, no comments, Home-only. One lane-discipline
note: the `dv-cu1` worker *read* a sibling lane (`/tmp/dv-rf3`) while diagnosing the fixture
failure; read-only, no edits outside its clone.
**Grok switches: none** — no Devin round triggered a fallback condition.

## 6. Not opened, by instruction

PROFILES-1 steps 2–3, TORTURE-1, NEVEROOM-1, AP-1 and BALLISTA-M2-A. O-2 (the BALLISTA-M2-A seed)
is done only if every row above has merged before 06:30.

## Pointers

- Up: [map.md](map.md) · Slate: [review-fix-slate-2026-09-10.md](review-fix-slate-2026-09-10.md) ·
  Procedure: [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)
