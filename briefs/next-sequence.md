# Slate — the next sequence of work (opened 2026-08-21)

**What this is.** One ordered queue across three open tracks, written because the tracks now
interleave and the order between them is a decision rather than an accident. [../STATUS.md](../STATUS.md)
stays the SSOT for state; this file states **sequence and reasoning**, and each unit still earns its
own `task/<unit>-ledger.md` when it starts.

Rolling slate: a unit leaves this file when it merges, and the file closes when the queue empties.

## Standing rules for every unit below

Restated because a mixed queue makes it easy to assume the previous campaign's contract carried:

1. **Reproduce first.** The behaviour is demonstrated on this tree before anything is edited.
2. **Write the pin and watch it go red.** A pin that was never red proves nothing.
3. **Measure against the oracle, including the incidental controls.** A green pin asserting a
   divergence as parity is the most expensive wrong test in the repo.
4. **Gate alone.** `make preflight` runs by itself and its own exit code is read immediately.
5. **`map.md` in lockstep, in the same commit.** Not a follow-up.
6. **One group at a time**, manual PR, owner merges.

---

## The order, and why it is this order

| # | Unit | Track | Blocked by | Size |
|---|---|---|---|---|
| 1 | **PYC-4** | conventions | — | M |
| 2 | **PYC-5** | conventions | PYC-4 | S |
| 3 | **PYC-6** (decision) | conventions | PYC-5 + **owner ruling** | S |
| — | **MW-4** | maintenance | **OD-3 (owner)** | M |
| — | **MW-5** | maintenance | MW-4 | S |
| — | **A13** | write path | — | M |

**V3-1 merged as [#203](https://github.com/TRO-Wolf/repark/pull/203)** and left this file.
**PYC-1 merged as [#204](https://github.com/TRO-Wolf/repark/pull/204)** and left this file (the
rolling rule): 35 nested defs lifted across the two DataFrame modules plus `_emit_side`, the two
EXCEPTIONS rows deleted.
**PYC-2 merged as [#207](https://github.com/TRO-Wolf/repark/pull/207)** and left this file:
12 lifts + 2 pragmas across the remaining ten shipped files, plus `session_core.probe`
under `if`. Ten nested-def EXCEPTIONS rows deleted.
**PYC-3 leaves this file with this change:** `spark/merge.py` `_Clause` and the four
`_csv_smart.py` records are Pydantic v2 `BaseModel`; those two DATACLASS_EXCEPTIONS rows
are deleted, not zeroed; `pydantic>=2.10,<3` is the wheel's second hard runtime dep.
Remaining debt: **17 nested defs across 9 rows, 21 dataclass rows** (parity harness +
`scripts/check_parity_live_dual_wire.py`). PYC-4 is unblocked.

**PYC did not lead originally, despite being freshly measured.** The gate is already armed, so
new Python cannot make the debt worse while it waits — which is precisely the property that
made it safe to schedule behind V3-1 rather than ahead of it. Burning the tables down is
valuable; it is not urgent, and it is the one track in this queue with no user-visible outcome.

**MW-4 preempts everything the moment OD-3 lands.** It is the maintenance campaign's only
real-catalog evidence, and the campaign cannot close without it. If the owner executes OD-3 mid-
sequence, the unit in flight finishes and MW-4 goes next.

**A13 sits last on purpose, and that placement is worth challenging.** It is a data-loss vector:
the in-memory catalog's namespace-without-location fallback is keyed by name alone, so two
sessions sharing `mem.ns.events` share one directory. It ranks last only because MW-3 already
made the dangerous path refuse, so the exposure today is a shared scratch root rather than a
deletion. If that guard were ever relaxed, A13 becomes the first item in this file.

---

## PYC — the conventions burn-down

**Context.** `scripts/check_python_conventions.py` is armed and holds the measured debt in two
EXCEPTIONS tables. The tree is green and cannot regress. PYC empties those tables. The rules
themselves are [../AGENTS.md](../AGENTS.md) "Python"; the reasoning and the sanctioned exceptions
are [../.agent/skills/code-quality/SKILL.md](../.agent/skills/code-quality/SKILL.md).

**The campaign invariant, and it is the LRS one:** *no call that worked before returns a different
value.* Every unit here is a refactor of working code that 3,639 passing facade tests were never
written to pin. A change that cannot meet this bar is a behaviour change wearing a cleanup's
clothes and belongs in its own commit with its own justification.

**The two hazards, named before they are hit:**

- **Lifting a closure changes what it can see.** The nested function reads its parent's locals, so
  its real signature is invisible. Lifting it means writing that signature down, and getting it
  wrong produces a function that compiles, passes a smoke test, and reads stale state.
- **`dataclass` → `BaseModel` adds validation that was not running.** It can reject input the old
  container silently accepted. That is usually the bug being found rather than introduced, but it
  is a behaviour change and it goes in the commit message.

### PYC-4 — the parity harness and `scripts/`

20 `dataclass` files plus 16 nested defs plus 10 unannotated returns under `python/repark-parity`,
and one of each under `scripts/`.

Larger by file count and far lower risk: none of it ships in the wheel. The two signal handlers in
the TPC-H and TPC-DS bench runners close over the per-query timeout and are the callback case
(pragma). `compat/bootstrap.py`'s five monkeypatched setUp/tearDown factories each close over the
class being patched — judge them together, since they stand or fall as one pattern.

Fix the 10 unannotated returns in the same unit. They are already red under Ruff `ANN` in intent;
they survive only because the per-file ignores are broader than they need to be. **Narrow the
ignores rather than annotating around them**, and say in the ledger which ignores narrowed.

### PYC-5 — close

Tables emptied or reduced to genuinely-sanctioned rows; the `pyproject.toml` per-file `ANN` ignores
reviewed and narrowed to what tests actually need; STATUS.md truthed up; the guard's docstring
counts re-measured rather than left at the seed numbers.

**Also re-measure the hook cost.** The guard was 0.94 s at arming, against a stated sub-second
budget. If PYC leaves it above that, the honest outcome is dropping it from pre-commit and leaving
it dual-wired in `make ci` + CI, not quietly keeping a hook that everyone starts skipping.

### PYC-6 (decision) — arm a docstring-presence subset, and the declined armings

**Measured 2026-08-22** with the pinned Ruff (`uvx ruff@0.15.22`), check-only, recorded here per
the arming method in [../.agent/skills/code-quality/SKILL.md](../.agent/skills/code-quality/SKILL.md)
"Arming a rule" — a rule measured and declined is written down so nobody re-litigates it from a
fresh `--select` run. The armed baseline config is **clean: 0 findings** repo-wide.

**Proposed to arm (owner decision required): the docstring *presence* rules only.** Full `D`
costs 803 findings (556 facade / 234 parity / 13 scripts). The split matters:

- Presence rules — `D103` undocumented function (163), `D105` magic method (57), `D102` public
  method (27), `D107` `__init__` (17), `D101` class (2) — ≈266 findings, and they enforce the
  conventions' "every function has a docstring" rule mechanically.
- Style rules — `D401` imperative mood (193), `D202`/`D205`/`D413` blank-line shape (289),
  the rest — are churn on a facade whose docstrings deliberately mirror upstream PySpark's own
  text. **Declined.**

If armed: seeded ratchet table (measure first, per-file rows with reasons, ceilings down only),
per-file ignore for tests kept.

**Measured and declined, with the reasons:**

- **`PL` (1068: 738 facade / 326 parity / 4 scripts).** `PLC0415` import-outside-top-level (416
  facade) is the sanctioned lazy-import pattern — heavy imports moved into functions to keep
  parse/startup fast. `PLR0124` self-comparison (26) is the `value != value` NaN check (verified:
  `spark/_csv_smart.py:268`, `spark/dataframe/core.py:5157` and siblings) — flagging it would flag
  correct code. The `PLR09xx` complexity counters are refactor *indicators*, not gates.
- **`A` builtin shadowing (75, all facade).** The facade faithfully mirrors PySpark's API, which
  shadows `filter`/`type`/`id`/`format` by upstream design; renaming breaks the drop-in contract.
- **`print()` ban.** The four `dataframe/core.py` sites implement `df.show()`'s stdout contract
  (Spark itself prints there); the parity CLIs (`compat/redact.py`, `compat/runner.py`,
  `compat/compare_reports.py`) own their stdout as their interface.

**Standing constraint for every PYC unit:** `dataframe/core.py` sits ~14 lines under its 6880
`check_lib_py` ceiling and `ml/feature/_transformers.py` ~67 under 2800 (per the EXCEPTIONS
comments — re-measure before relying on either number). A unit that adds lines there budgets the
next split or the ratchet-raise reason first; it does not discover the ceiling at commit time.

---

## MW-4 / MW-5 — held on the owner

**MW-4 needs OD-3**: scoped delete on the tier-2 acceptance role's scratch prefix. The owner
executes it; the campaign never touches IAM. Until then MW-4 cannot run, and everything the
campaign claims about merge-on-read operability rests on unit-test evidence while the existing
live evidence covers copy-on-write only.

MW-5 (registry close, the re-measured delta against MW-0's 2.1× baseline, scorecard flip) is queued
behind it and is small.

---

## A13 — the shared CTAS fallback root

Roadmap item in [../task/roadmap-intake-2026-08-21.md](../task/roadmap-intake-2026-08-21.md),
surfaced by MW-3. The in-memory catalog's namespace-without-location fallback is keyed by name
alone, so two sessions sharing a namespace name share one directory on disk.

`remove_orphan_files` already refuses that root, which is why this is queued rather than urgent.
The write-path behaviour behind it is not fixed, and the guard is a fence around one procedure
rather than a fix to the addressing.
