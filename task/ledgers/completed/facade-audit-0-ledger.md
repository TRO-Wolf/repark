# Unit ledger — FACADE-AUDIT-0 step 1 · Half A: the Rust-backed facade facts

**Unit:** FACADE-AUDIT-0 steps 1–2 · **Date:** 2026-09-10 · **Branch:** `docs/facade-audit-0` · **Base:** `2fad8135`
**Model:** Muse Spark (muse-spark-1.3-contributor) — implementation, via muse-worker
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** READING. **risk_tier: standard.**
**Reading path:** this is a READING unit under R-10 — no code runs, no benchmark runs;
evidence is quoted measurements with file paths, verified by grep, not re-taken.

**Why now.** Card FACADE-AUDIT-0 (owner, 2026-09-10) splits the Rust-backed facade audit
into Half A (measure this tree) and Half B (weigh and sequence, step 2, another round).
This step writes Half A only: the per-module table, the IPC crossing sites, the
`Column`-renders-SQL sites, and the totals, in
[task/roadmap/epic-term/facade-audit-2026-09-10.md](../../roadmap/epic-term/facade-audit-2026-09-10.md).
No Python or Rust source is touched; the homes are the audit document, that
directory's `map.md`, and this ledger. Nothing else.

**Retires:** this ledger moves to `../completed/` in the unit's last commit (step 3).

**Not in this step:** Half B weighing and sequence (step 2); any public name; any
pickling behaviour; ML transformers and the library-wrapping parts of `ta.py` (D-3);
any dependency file; `STATUS.md`; `briefs/next-sequence.md`.

## PROPOSITION LEDGER — FACADE-AUDIT-0 step 1 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The audit carries one measured row per module under `python/repark/src/repark/` (106 files): lines, binding call sites with symbols, pyarrow-reference lines, class. | §2 of the audit; the three anchor facts reproduce (inference 101, types 43, core 37). | **PROVEN** | `find … -name '*.py` = 106 files; `wc -l` total 51,930; anchors reproduced exactly on `2fad8135` (see §1). |
| C-002 | The audit lists every IPC crossing site with file:line. | §3 of the audit. | **PROVEN** | 13 into-engine + 7 out-of-engine sites with file:line, comment-only mentions excluded by inspection. |
| C-003 | The audit lists every place a `Column` renders SQL text with file:line and what it renders. | §4 of the audit. | **PROVEN** | 7 `PyColumn.sql` entry points, 11 `column.py` method groups, 25 session scaffold sites, each with the rendered shape; `functions.py:358` identified as `expr()` by read-back. |
| C-004 | The audit carries the totals: facade line count, per-class totals, pyarrow-touching module count. | §5 of the audit. | **PROVEN** | 51,930 lines / 106 files; delegate 22/10,118, logic 72/35,466, pyarrow 12/6,346; 231 binding sites; 23 runtime pyarrow importers (27 with any textual reference). |
| C-005 | Map lockstep holds and the round gates are green. | `task/roadmap/epic-term/map.md` row, `task/ledgers/staging/map.md` row, `make check-docs-compaction`, `make check-ledgers`, fence grep clean. | **PROVEN** | `docs-compaction: clean`; `ledger-check: 284 ledgers in bins (218 archived), 796 ledger links resolve, frozen rule clean`; fence grep over staged rs/py/toml/sh/yml printed nothing (2026-09-10). |

VERDICT (step 1): 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## PROPOSITION LEDGER — FACADE-AUDIT-0 step 2 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-006 | The audit weighs each D-2 candidate by measured walls quoted with path and number, marks the rest UNMEASURED with the settling measurement named, and runs no benchmark. | §6 of the audit; every quoted number greps in its cited file. | **PROVEN** | 40 spot greps over the three perf files plus the freeze JSON, all OK, zero MISS (see Red first); UNMEASURED marks on FACADE-1 import, FACADE-4, FACADE-5 format, plus the roll-call. |
| C-007 | The audit constrains each unit by the freeze file (row ids, frozen/unfrozen) and names the isinstance-keeping mechanism, filing uncertainty as an open question. | §7 of the audit; §9 Q1. | **PROVEN** | Rows B1/B2/N1/C1/J/A2/O1/F1/K1/L1 cited with frozen state; Column keeps the Python wrapper, types keep the Python classes; Q1 filed, not asserted. |
| C-008 | The audit confirms or reorders the D-2 sequence with per-unit moves, masses, wins, freeze, risks, and pin lists, closes with an owner-readable conclusion, and respects D-3. | §§8–9 and the conclusion of the audit. | **PROVEN** | Order 1–5 confirmed with three evidence reasons; five unit cards each with §2 masses and named test files (all 24 exist on the base tree); ML/ta/library parts untouched; no public name changed. |
| C-009 | Map lockstep holds and the round gates are green. | `task/roadmap/epic-term/map.md` row, `task/ledgers/staging/map.md` row, `make check-docs-compaction`, `make check-ledgers`, `make check-docs-links`. | **PROVEN** | Both map rows trued to Half B landed with pins C-001…C-009; gates recorded below. |

VERDICT (step 2): 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.
VERDICT (unit so far): 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

This step changes no code, so no behavioral pin exists to fail first. The falsifiable
check is the anchor reproduction in §1: the card states three pyarrow-reference counts
(inference 101, types 43, core 37) measured on the base tree, and the audit's own
`grep -cE 'pyarrow|pa\.'` run reproduces all three exactly. A miscounted column —
wrong file set, wrong pattern, wrong tree — fails that check before any class is judged.
The run on `2fad8135` output `737|0|101`, `1834|0|43`, `4487|12|37` (lines|bind|pa),
matching all three anchors; only then were the classes assigned.

## Red first (step 2)

This step changes no code and runs no benchmark, so the falsifiable check is
quote-verification: before committing Half B, every wall number quoted in §6 was
confirmed present in its cited file with `grep -qF`, and every pin test file was
confirmed present on the base tree with `ls`. Result: 40 number greps across
`docs/perf/facade-boundary-baseline.md`, `docs/perf/eager-preview-baseline.md`,
`docs/perf/engine-iceberg-analysis-2026-09-04.md`, and
`docs/design/v1-0-api-freeze.json` — all OK, zero MISS; `pyproject.toml:20` confirms
the hard pyarrow dependency; all 24 cited `python/repark/tests/*.py` pin files exist.
A misquoted wall or a nonexistent pin would have failed this check first; only then
were the sections committed.

## Closing attestation (orchestrator, 2026-09-10)

The orchestrator re-measured the audit's anchor numbers on the same base commit before the PR:
`find python/repark/src/repark -name '*.py' | wc -l` = 106, the same tree's line total 51,930, and
`grep -cE 'pyarrow|pa\.' python/repark/src/repark/spark/types.py` = 43 — the three that Half A's
own red-first check turns on. The document quotes measurements; it takes none, ships no code, and
changes no behaviour, so rule B (pin binding) does not apply — this ledger is a READING unit
under R-10.

```
COVERAGE_ATTESTATION:
  pr_unit: facade-audit-0
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Card FACADE-AUDIT-0's D-1 (two halves), D-2 (the five candidate units) and D-3 (out of scope) were walked clause by clause against the document. Half A carries the per-module table, the IPC sites and the Column SQL sites D-1 names; Half B confirms D-2's order on quoted walls and holds D-3's exclusions (ML transformers, ta.py wrappers, public names, pickling).
      artifacts: [task/roadmap/epic-term/facade-audit-2026-09-10.md, task/ledgers/completed/facade-audit-0-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: The module set is the whole recursive .py tree under python/repark/src/repark (106 files, re-counted by the orchestrator), not a sample; modules with zero binding calls and zero pyarrow lines carry rows, and the four prose-only pyarrow mentions are separated from the 23 runtime imports rather than folded in.
      artifacts: [task/roadmap/epic-term/facade-audit-2026-09-10.md]
    - id: AT-3
      status: N/A
      justification: A reading unit: no code path ships, so there is no failure, retry or cleanup path to attack. The document's own failure mode — a wrong number — is attacked under AT-8 and AT-10.
    - id: AT-4
      status: N/A
      justification: No state, no concurrency, no ordering: the deliverable is one markdown document and its ledger.
    - id: AT-5
      status: N/A
      justification: No privileged action, no input parsing, no secret handling; the audit reads tracked source and tracked perf documents only.
    - id: AT-6
      status: ATTACKED
      evidence: The API freeze is the compatibility constraint the sequence must respect; Half B states per candidate unit what docs/design/v1-0-api-freeze.json forbids in it, and files the isinstance mechanism it cannot prove as an open question rather than asserting it.
      artifacts: [task/roadmap/epic-term/facade-audit-2026-09-10.md, docs/design/v1-0-api-freeze.json]
    - id: AT-7
      status: ATTACKED
      evidence: Weighing is by quoted measured walls with their source files, and every candidate whose cost no cell isolates is marked UNMEASURED with the measurement that would settle it — the audit does not convert a guess into a ranking.
      artifacts: [task/roadmap/epic-term/facade-audit-2026-09-10.md, docs/perf/facade-boundary-baseline.md, docs/perf/engine-iceberg-analysis-2026-09-04.md]
    - id: AT-8
      status: ATTACKED
      evidence: The card's premise that the native module is `repark._repark` was measured false and corrected in the document to `repark._native` (zero hits for the card's spelling); every wall quoted in Half B was confirmed present in its cited file by grep before the section was committed (40 greps, zero misses).
      artifacts: [task/roadmap/epic-term/facade-audit-2026-09-10.md, crates/repark-python/src/lib.rs]
    - id: AT-9
      status: N/A
      justification: Nothing runs in production from this unit; there is no log, metric or alarm surface.
    - id: AT-10
      status: ATTACKED
      evidence: A reading unit has no behavioural pin; its falsifiable check is the anchor reproduction recorded under "Red first" (three card-stated pyarrow counts reproduced exactly, a wrong file set or pattern failing first) and the quote verification under "Red first (step 2)". The orchestrator re-ran the file-count, line-count and types.py anchors independently and they hold. Each candidate unit in the sequence carries its own pin list, so the audit hands the next unit its test obligations rather than leaving them open.
      artifacts: [task/roadmap/epic-term/facade-audit-2026-09-10.md]
  complete: true
```
