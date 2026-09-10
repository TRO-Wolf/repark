# Overnight report — run 5 of 2026-09-10

**Session:** Opus 5 orchestrator (**orchestrator A**), 05:21 → 13:20 local, grants G-1…G-5
(G-3 stop 13:20, G-4 GLM + Muse + Grok, G-5 yes) · **Order followed:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md) §7
run-5 order · **Cards:** [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) and
[cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md)

**Scope change mid-run (owner, 05:45 local).** A second orchestrator (**orchestrator B**) took the
Grok lane — BALLISTA-M1-A…D and REVIEW-1 — and FACADE-AUDIT-0. This report covers §7 items 1–5
only. The Ballista and facade PRs on `main` today (#459, #460, #466, #469) are orchestrator B's and
are not counted here. One consequence is worth recording: with two orchestrators merging, `main`
moved under every open branch, and several PRs needed two or three merge-forward-and-re-run cycles
before their window came (§4).

## 1. What landed

| Unit / step | PR | Result | Worker | Rounds |
|---|---|---|---|---|
| DISPLAY-BRIDGE-1 (the run-4 rebase, **unit complete**) | [#454](https://github.com/TRO-Wolf/repark/pull/454) | **merged `a3d916b0`**, tree-equal | Muse | 1 |
| AP-0 (measure) + the orchestrator's O-run (**unit complete**) | [#462](https://github.com/TRO-Wolf/repark/pull/462) | **merged `04a2e430`**, tree-equal | Muse | 1 |
| MAINT-POLICY-1 steps 1–4 + departure (**unit complete**) | [#465](https://github.com/TRO-Wolf/repark/pull/465) | **merged `62373134`**, tree-equal | Muse | 5 |
| TORTURE-1 step 1 | [#467](https://github.com/TRO-Wolf/repark/pull/467) | **merged `149147c1`**, tree-equal | GLM | 1 |
| PROFILES-1 step 1 | [#468](https://github.com/TRO-Wolf/repark/pull/468) | **merged `52b604ab`**, tree-equal | Muse | 1 |
| AP-1 step 1 | [#472](https://github.com/TRO-Wolf/repark/pull/472) | **merged `838532d1`**, tree-equal | Muse | 1 |
| TORTURE-1 step 2 + the live-oracle correction | [#473](https://github.com/TRO-Wolf/repark/pull/473) | **merged `862a0f1a`**, tree-equal | GLM | 1 |
| NEVEROOM-1 step 1 | [#475](https://github.com/TRO-Wolf/repark/pull/475) | **PARKED** (draft, green, §6 ruling questions — §3) | GLM | 2 |

**Three units finished and merged** — DISPLAY-BRIDGE-1, AP-0 and MAINT-POLICY-1, all three ledgers
in `completed/` with attestations. TORTURE-1, PROFILES-1 and AP-1 are in flight with their ledgers
in `staging/`, which is correct: each has steps left.

## 2. The two measurements that changed a decision

Both are cases where the cheapest path was to accept a plausible claim, and the measurement said
something different.

**AP-0's O-run — P-3's byte model is 76–88 % high.** The card asked whether the scoring model
predicts real rewritten file sizes within 20 %. The orchestrator ran the rewrite on **two** beds
rather than the card's one, so a single bed's quirk could not carry the answer:

| Bed | Predicted partitions | Actual | Predicted files | Actual | Predicted bytes | Actual bytes | Byte error |
|---|---|---|---|---|---|---|---|
| synthetic-uniform | 20 | **20** | 20 | **20** | 7,773,590 | 4,413,222 | **+76.2 %** |
| synthetic-skewed | 20 | **20** | 27 | 20 | 7,773,590 | 4,134,457 | **+88.0 %** |

Partition-count arithmetic is exact. The byte projection is not: P-3 sums the bytes of the files
that exist today, and a rewrite recompresses at 0.53–0.57×. C-005 is **REJECTED as stated** —
the negative answer is the deliverable — with residues AP-0-R-001 (project from `record_count` ×
a measured post-rewrite bytes-per-row) and AP-0-R-002 (the file-count column was never fairly
tested, because a partitioned CTAS writes one file per partition value and never binpacks).

AP-1 then implemented **exactly P-3 as ruled** — changing a ruled model is the owner's call — and
put the measured error in the plan frame's `notes` column, so nobody reads
`projected_files_at_target` as calibrated. **Owner question, §6:** should AP-1 adopt AP-0-R-001's
correction before AP-2 (apply) is built on top of it?

**TORTURE-1 step 2's three registry rows — two confirmed, one narrowed.** The round was forbidden
to start a JVM while another lane built, so it filed all three new rows with their Spark halves
labelled unmeasured, which was right. The orchestrator ran the live oracle on the audit
(PySpark 4.1.2, zulu-17, `local[1]`, ANSI on, the same fixtures):

| Row | Filed claim | Measured | Outcome |
|---|---|---|---|
| `CSV-INFER-HEADER-CASE` | Spark keeps capitalised headers verbatim | `struct<Order Total:string,Qty:int,…>`, `count() = 10000` | confirmed |
| `SUM-DEC-I128WRAP-1` | Spark raises on the decimal `SUM` overflow | `ARITHMETIC_OVERFLOW` / SQLSTATE 22003, both columns | confirmed |
| `DATE-INTERVAL-NSBOUND-1` | Spark **promotes to timestamp** and completes | `day + INTERVAL 1 DAY` is **`date`**; 10,000 rows; `+294247-01-10` → `+294247-01-11` | **narrowed** |

The third earned the JVM. Spark does **not** promote a day-only interval to a timestamp; it adds
whole days at `date` width. The filed reading ("the promotion window ends a thousandfold early")
was wrong and its prescribed fix ("do the arithmetic at microsecond width") would have chased the
wrong width. The surviving defect is repark computing day arithmetic through nanoseconds; the fix
is **day width**, and the row now says so.

A trap for the next live round, recorded in the ledger: collecting that answer into Python raises
`ValueError: year 294247 is out of range` from `datetime`. That is a Python limit, not a Spark
refusal — cast to `STRING` before collecting or the oracle reads backwards.

## 3. Owner questions

**These two block NEVEROOM-1 step 2, and the lane is parked on them ([#475](https://github.com/TRO-Wolf/repark/pull/475),
draft, green).** Both are readings of the card's D-2 that measurement forced, so under §6 they are
the owner's, not mine.

**Q-1 — `RLIMIT_AS = 3 × limit` is unrunnable as written.** Measured `VmSize` on this tree:
22.7 MB bare, **2.93 GB after `import pyarrow`**, 4.27 GB after `import repark`, 8.70 GB after a
64 MB-pool session build. An absolute 192 MB cap (CI tier) cannot import pyarrow and an absolute
3 GB cap (full tier) is under the import baseline alone — every cell would be `KILLED` and the
card's own "no `KILLED` cell" done-when would be unsatisfiable. The implemented reading is
`RLIMIT_AS = VmSize_at_apply + 3 × limit`, applied after the session builds and verified by
read-back (the formula H3-SPILL-RESIDUE-1 measured first), which keeps D-2's purpose — a silent
OOM kill stays observable — with `3 × limit` as the cell's own headroom. The step-1 cell used
2.10× of it. **Ask:** may step 2 run the full matrix that way?

**Q-2 — the refusal type is a family, not two names.** D-2 defines `refused` as `MemoryError` /
`AnalysisException`. The measured pool refusal through the facade is `PySparkException`
("Not enough memory to continue external sort … ExternalSorterMerge[0] … fair(pool_size: …)"),
and `AnalysisException` is a subclass of that family. Under the strict two-type reading every real
pool refusal folds to `KILLED` and D-4's cannot-spill cells fail the matrix. The implemented rule:
`refused` = `MemoryError`, **or** a `PySparkException`-family exception whose message names one of
the row's measured `refusal_names`; everything else folds to `KILLED`, never to `refused`.
**Ask:** confirm the family rule.

## 3b. The rest of the owner questions

1. **AP-1 and AP-0-R-001** (§2). Adopt the corrected byte model before AP-2, or ship P-3 as
   ruled with the note?
2. **PROFILES-1 steps 2–3 need a dedicated window.** The bed is merged and its harness refuses to
   time a box with a JVM running, which is exactly the guard the sweep needs — but the sweep is a
   release build over 20 knobs × 8 query shapes × 3 datasets × 3 repetitions with **no other lane
   open**. It does not fit beside anything. It wants a run of its own.
3. **The `datasets/` tree.** TORTURE-1's card puts its generators at
   `python/repark-parity/fixtures/torture/`, and the older DS-1…DS-3 tree at
   `python/repark-parity/datasets/` covers overlapping family names. The two now coexist. Retiring
   the old tree is a ruling, not a card.
4. **The `fixtures/` graft.** To satisfy the card's home directory *and* the module spelling
   `repark_parity.torture` with `pyproject.toml` edit-forbidden, step 1 grafts the whole
   `fixtures/` directory onto the package path in `src/repark_parity/__init__.py`. Accepted, with
   the over-broad part written down as a contract (**every** directory added under `fixtures/`
   becomes `repark_parity.<name>`). If that spelling should be earned rather than automatic, it
   needs a packaging ruling.

## 4. Decisions taken under §6

| # | Decision | Why it was mine |
|---|---|---|
| NEVEROOM-1 | **Not** taken: Q-1 and Q-2 are readings of a ruled D-row, so the lane is parked rather than merged under my own answer. | §6 puts a semantic change to a fixed D-row outside my authority, and the worker's premises are measured — exactly the case the rule is for. |
| AP-0 D-5 | The card's three beds become futures + synthetic-skewed + synthetic-uniform, all local. | The named bronze table lives in AWS Glue and this box carries no credentials, so it is unreachable. Bed choice is fixture layout. |
| AP-0 C-005 | The O-run ran on **two** beds, not the card's one. | One bed cannot distinguish a model error from a bed quirk — and it did not: the two disagree on the file-count column and agree on the byte error. |
| MAINT-POLICY-1 D-8 | The session-build stamp was added to step 3, which no step owned. | Without it the card's own "Done when" (a `repark.toml` policy maintained by one CALL) could not hold. Step order inside one card is mine. |
| TORTURE-1 | The `fixtures/` graft accepted, and its scope written down as a contract in `python/repark-parity/map.md`. | The card ruled both the home and the module spelling; the mechanism follows. Making it discoverable is not a new rule. |
| TORTURE-1 | The three new registry rows measured against live Spark on the audit. | The rows asserted Spark behaviour in their titles. A registry row is a parity claim; an unmeasured one is a guess with a citation. |
| AP-1 | P-3 implemented as ruled, with AP-0's measured error disclosed in the frame rather than corrected. | Changing a ruled D-row is the owner's, per §6; hiding a known-wrong number is nobody's. |
| — | `STATUS.md` compacted to stay under its 25,000 B ceiling while recording MAINT-POLICY-1. | The ceiling only ratchets down. The v1.1.0/v1.1.1 roll-calls and the display/eager paragraphs became pointers; no fact dropped. |

## 5. Two audit findings that would have shipped

Both were caught by re-running gates rather than by reading a hand-back.

**A public name outside the guard (MAINT-POLICY-1 step 4).** `run_maintenance` was first attached
by assignment — `ReparkSession.run_maintenance = …` in the package `__init__` — to avoid raising
`session_core.py`'s line baseline. It works at runtime, and the facade pins passed. But
`scripts/check_example_coverage.py` enumerates the public surface by an **AST walk of the class
body**, so the new method was invisible to it: the inventory stayed at 926 while a shipped public
method existed, and no later change to it would have been caught either. Fixed — declared in the
class body, inventory at **927** with a runnable example, and the lines paid for by a move that
ratchets `session_core.py` **2305 → 2304**.

**A refactor that moved an error (MAINT-POLICY-1 step 3).** Extracting a shared `run_rewrite` core
moved `catalog.load_table`, so the existing `rewrite_data_files` procedure began loading the table
**twice** when the CALL carried `where`, and a CALL that was both argument-malformed and
table-unloadable started reporting the argument error instead of the table error. Step 4 loads once
and restores the shipped order, with a red-first pin.

The shape both share: the worker's own gates were green and its hand-back was accurate. What found
them was re-reading the diff against files the card did not list.

## 6. What went wrong, and what to change

**The parity suite is still not in `preflight`, and the two mirror files are not enough.** Run 4's
lesson named `test_ex_0_example_coverage.py` and `test_cap_1_source_file_line_cap.py`. MAINT-POLICY-1
went red on CI anyway, on a **third** file — `test_pr_245_revalidation_record.py`, which pins an
exact inventory of the SQL-literal helper call sites, and the new facade wrapper added two.
**Change: run the whole parity suite before pushing, not the two named files.** It takes 40 seconds:
`PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q`.

**A new test directory that needs the native module breaks the isolated CI job.** `ci.yml`'s python
step runs the parity tests with **no native build**, so TORTURE-1's `tests/torture/` failed at
collection with `ModuleNotFoundError: No module named 'repark'`. The fix is
`pytest.importorskip("repark")` at the top of that directory's `conftest.py`, **not** an `--ignore`
in `make py-test` — that target states it mirrors the `ci.yml` step, and an ignore there would have
made the local target green while CI stayed red. Worth a line in the runbook: **a new test
directory under `python/repark-parity/tests/` declares whether it needs the native module.**

**Never reuse a clone that has a live worker in it.** I checked out a second branch in
`/tmp/oc-cfg` to fix a CI failure while a Muse round was running there. The round survived — its
work was untracked files, and restoring the branch plus recovering the stashed file cost about two
minutes — but it was luck, not design. **One clone, one lane, for as long as the lane lives.**

**`# noqa` directives are not free.** The import guard above was written with `# noqa: E402`; this
repo's ruff does not enable `E402`, so `RUF100` reds on the unused directive and CI failed a second
time on the same PR. Run `make verify` after every orchestrator edit, not only after a worker's.

**Two orchestrators means `main` moves under everything.** Six merge-forward cycles were spent
today purely on `main` advancing between a green CI run and the merge window. Nothing was lost, but
the throughput cost is real. Worth considering: when two orchestrators run, the second one's merges
should batch, or each should hold a merge window.

**GLM did the two torture rounds well, and cheaply** ($0.31 and $0.23). R-15 sends document and
long rounds to Muse; step 1 was a multi-file scaffold that GLM handled cleanly, including honouring
the "file a registry row, never invent an xfail" rule under pressure. The rule is still right, but
GLM's ceiling is higher than "narrow mechanical" when the card is precise.

## 7. Numbers

- **7 PRs merged, all tree-equal after the squash** (#454, #462, #465, #467, #468, #472, #473);
  1 PR **parked green as a draft** on ruling questions (#475, NEVEROOM-1 step 1); 1 report PR
  (#476).
- **Three units completed**: DISPLAY-BRIDGE-1, AP-0, MAINT-POLICY-1. Four units advanced:
  TORTURE-1 (steps 1–2 of 5), PROFILES-1 (step 1 of 3), AP-1 (step 1 of 2), NEVEROOM-1 (step 1 of 3).
- Worker rounds: Muse 9 (0 dropped), GLM 2 ($0.54 total).
- Orchestrator follow-up rounds: 1 (the MAINT-POLICY-1 class-body fix), fixed on the first return.
- Live-Spark oracle runs by the orchestrator: 2 (the three TORTURE-1 rows, then the date-width
  re-probe that narrowed the third).
- `make preflight` red twice on one branch: never. Every red was CI-only, and each named a file
  `preflight` does not reach.

## Pointers
- Up: [map.md](map.md) · Cards: [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md),
  [cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md)
- The runbook this followed: [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)
- Previous run: [overnight-report-2026-09-09-run4.md](overnight-report-2026-09-09-run4.md)
