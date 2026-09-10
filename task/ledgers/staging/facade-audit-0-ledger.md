# Unit ledger — FACADE-AUDIT-0 step 1 · Half A: the Rust-backed facade facts

**Unit:** FACADE-AUDIT-0 step 1 · **Date:** 2026-09-10 · **Branch:** `docs/facade-audit-0` · **Base:** `2fad8135`
**Model:** Muse Spark (muse-spark-1.3-contributor) — implementation, via muse-worker
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** Card FACADE-AUDIT-0 (owner, 2026-09-10) splits the Rust-backed facade audit
into Half A (measure this tree) and Half B (weigh and sequence, step 2, another round).
This step writes Half A only: the per-module table, the IPC crossing sites, the
`Column`-renders-SQL sites, and the totals, in
[task/roadmap/epic-term/facade-audit-2026-09-10.md](../roadmap/epic-term/facade-audit-2026-09-10.md).
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

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

This step changes no code, so no behavioral pin exists to fail first. The falsifiable
check is the anchor reproduction in §1: the card states three pyarrow-reference counts
(inference 101, types 43, core 37) measured on the base tree, and the audit's own
`grep -cE 'pyarrow|pa\.'` run reproduces all three exactly. A miscounted column —
wrong file set, wrong pattern, wrong tree — fails that check before any class is judged.
The run on `2fad8135` output `737|0|101`, `1834|0|43`, `4487|12|37` (lines|bind|pa),
matching all three anchors; only then were the classes assigned.
