# Run 27 (2026-09-20 → 21) — orchestrating note: the orchestrator bake-off, the tick driver, the close-out

**What this run was.** Run 26 was stopped at 07:54 on 2026-09-20 by the owner: five Opus orchestrators and eight Opus
executors (five at maximum effort) had taken the weekly Claude allowance from 37 % to 40 % in about an hour. Run 27
replaced that shape: orchestrators run as short stateless **ticks** under a bash driver that waits at no cost
(`_lib/xorch/`), executors are Muse at max, Devin or GLM 5.3 Flash on PR-sized work orders, the critic is Grok, and at
most one Opus orchestrator runs — through the same driver. From the stop to 15:15 the allowance moved 40 % → 42 %.

## 1. Gate scoreboard (842 cells, Spark 4.1.2 + Iceberg 1.11.0 recorded side)

| main | EQUAL | DIFFERENT | REFUSED-UNREGISTERED | NOT-PARSED | REFUSED-REGISTERED | SPARK-CANNOT |
|---|---|---|---|---|---|---|
| 2026-09-20 morning | 461 | 37 | 110 | 26 | 84 | 124 |
| 2026-09-21 05:10 (`6cff128f`) | **537** | 47 | 58 | 20 | 63 | 117 |

78 cells moved to EQUAL in a day (49 from unregistered refusals, 20 from registered refusals, 5 from parse failures,
4 from different answers). **188** cells that Spark answers still read non-EQUAL (185 after carve-out C-1).
Two regressions, both handed to the lane that owns the area: `P-RDF-PARTIAL-PROGRESS` (snapshot layout) and
`L-INSERT-OVERWRITE` (v3 `_row_id` assignment order after an overwrite). One ruling breach:
`D-X-CHANGE-COLUMN-RENAME` is accepted where INDEX decision 16 rules a refusal. Six schema-evolution cells now read
"RePark accepts what Spark refuses": Spark's refusal there (`Field col1 not found in source schema`) comes from the
cell's inline source, not from the feature — the cells need re-authoring so that Spark answers, which is owed.

## 2. The bake-off — protocol, measures, blind grades

The protocol was written before any lane finished a unit and is filed beside this note
(`run27-eval-protocol.md`); every later change is labelled post-hoc or owner-directed there. The four lanes ran
**different units**, so this is case evidence and pass/fail on minimum standards — **not a ranking**.

| Lane (first list) | Unit | PRs opened → merged | Critic rounds | Executor rounds | Cost | Report |
|---|---|---|---|---|---|---|
| Grok 4.6 xhigh | error conditions, 45 cells | 4 → 4 | 9 (7 PASS, 1 remediate, 1 stall) | 6 Devin + 5 Muse | $16.72 | yes (at the deadline tick) |
| Muse max | system functions, 15 cells | 1 → 1 | 3 (2 PASS, 1 remediate) | 4 Muse + 1 Devin | subscription | yes (before DONE) |
| GLM 5.3 | catalog and session, 11 cells | 1 → 0 (paused 18:27 by the owner) | 4 (2 remediate, 2 stalls) | 6 Muse | $12.51 | not applicable — paused before its closing tick |
| GLM 5.3 Flash | map gate step 1; small parser, 9 cells | 3 → 2 (one deliberate draft proof, closed) | 9 (6 remediate, 2 PASS, 1 stall) | 16 Devin | $0.91 | yes |

Tick counts are not reported as a measure: a harness defect (the wake fingerprint included the clone's dirty-file
count) woke every lane while its executor edited files. Blind grades, two non-contestant graders (Devin; GPT-5.6-Terra at medium — Astra could not run on the installed Codex
CLI), 0–4 on five criteria (Devin | Terra, of 20): Muse 20 | 20, GLM 5.3 Flash 17 | 18, Grok 19 | 15 (re-graded, see
below), GLM 5.3 14 | 13 of 16 (report honesty not applicable: the owner paused it before its closing tick).
**An evaluator error is recorded in the protocol:** the unit lists asked for `report-<your unit>.md`; the Muse and Grok
lanes wrote `report-ipi-29.md` and `report-ipi-51.md`, the evaluator's scripts looked for another name, and the
orchestrating session wrongly reported both lanes as having written no report and gave the graders a "not written"
stub for the Grok lane. Both statements are withdrawn. The Grok lane was re-graded by the same graders with its real
report, alone rather than side by side, so its numbers are less comparable than the others; the two graders differ by
two points on its plan correctness (4 | 2), and Terra's report-honesty remark misreads a UTC merge time as local.
The graders rated GLM 5.3 Flash's critic handling 4 of 4, which contradicts the orchestrating session's own unblinded
reading ("shallow remediation", from its six remediation verdicts); both are recorded. One event is counted once: the GLM 5.3 Flash lane challenged a wrong ruling of the
orchestrating session (the `gc.enabled` purge guard) with a Spark measurement and was right.

## 3. The Opus close-out lane

One Opus orchestrator at medium effort, tick-driven, Muse executors: 55 ticks, 15.5 hours, **$97.01** as the CLI
reports it. It closed the seven units stopped in the morning: the sort/zorder string parser, session write confs
(#733, eight earlier Opus executor rounds had not landed it), incremental and changelog reads (both halves), schema
evolution on write, the first metadata-columns PR, container accessors, the `position_deletes` scan, and two pin
bumps. Correction recorded here: the orchestrating session first priced the run-25 orchestrators at assumed list
rates (about $1,090); at the rates the CLI reports they were about $360, against $232 of Opus executor rounds.

## 4. Harness defects found in this run (tool fixes, not instructions)

Claims landing during a tick; lanes waking each other through the claims file; event storms on a busy lane (fixed by
a minimum gap); a bare `--force-with-lease` in the push tool; no clippy or panic-ban in the local gate; the review tool
nesting `xr-xr-…` names and reviewing a stale head; the launcher printing "launched" for a unit that died; lane clones
with `skip-worktree` manifests patched to a local fork path (false reds and false greens); lagging lanes gating
against an old fork pin; the dirty-count wake storm; **no removal of finished clones — the disk reached 98 % overnight**;
an ambiguous night rule that two lanes read as a box-wide limit. The ledger with evidence per entry is filed beside
this note (`run27-lessons-ledger.md`).
