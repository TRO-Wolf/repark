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
7. **Pickup ritual first, departure edit last.** First act of a unit: fetch, confirm the prior
   unit's PR merged and that the local base carries its departure edit, run the drift checks
   (`make check-map-sync`), and compact the context docs **against the just-merged delta only**,
   as a docs-only first commit. Last commit of the unit: the departure edit to this file, STATUS
   trued up for what this unit changed and nothing else, `map.md` in lockstep.

---

## The order, and why it is this order

| # | Unit | Track | Blocked by | Size |
|---|---|---|---|---|
| — | **MW-4** | maintenance | OD-3 executed | M |
| — | **MW-5** | maintenance | MW-4 | S |

**V3-1 merged as [#203](https://github.com/TRO-Wolf/repark/pull/203)** and left this file.
**PYC-1 merged as [#204](https://github.com/TRO-Wolf/repark/pull/204)** and left this file (the
rolling rule): 35 nested defs lifted across the two DataFrame modules plus `_emit_side`, the two
EXCEPTIONS rows deleted.
**PYC-2 merged as [#207](https://github.com/TRO-Wolf/repark/pull/207)** and left this file:
12 lifts + 2 pragmas across the remaining ten shipped files, plus `session_core.probe`
under `if`. Ten nested-def EXCEPTIONS rows deleted.
**PYC-3 merged as [#208](https://github.com/TRO-Wolf/repark/pull/208)** and left this file:
`spark/merge.py` `_Clause` and the four `_csv_smart.py` records are Pydantic v2
`BaseModel`; those two DATACLASS_EXCEPTIONS rows are deleted, not zeroed;
`pydantic>=2.10,<3` is the wheel's second hard runtime dep.

**PYC-4 merged as [#209](https://github.com/TRO-Wolf/repark/pull/209)** and left this file:
20 parity dataclass files → `BaseModel`; nested-def EXCEPTIONS emptied (lifts +
pragmas); dual-wire stays a dataclass because the gate runs as bare `python3`;
the ten unannotated returns in `test_compare.py` are annotated and the ANN
ignores are split.

**PYC-5 merged as [#211](https://github.com/TRO-Wolf/repark/pull/211)** and left this file:
hook re-measured n=5 median **0.996 s** (max 1.011 s) over 164 files and dropped
from pre-commit (stays in `make ci` + CI); facade tests no longer ignore ANN201;
dual-wire dataclass row stays the sanctioned leftover.

**PYC-6 merged as [#216](https://github.com/TRO-Wolf/repark/pull/216) and left this file:** public-docstring presence
(`D101`/`D102`/`D103`/`D105`/`D107`) armed with a seeded ratchet (136 findings /
39 files, tests excluded) in `scripts/check_docstring_presence.py`; style `D`
declined permanently. The dual-wire dataclass leftover and the D-presence
EXCEPTIONS table are remaining debt, not sequenced work.

**A13 merged as [#217](https://github.com/TRO-Wolf/repark/pull/217) and left this file:**
`register_memory_catalog` uses the supplied warehouse as the location-less CTAS
fallback root, so two sessions with different warehouses no longer share
`<temp>/repark_ctas/<catalog>/<ns>/<table>`. Same warehouse + same names still
share; MW-3 refuse stays on that fallback tree. The dual-wire dataclass leftover
is remaining debt, not sequenced work. The queue is MW-4 (OD-3 executed; this
unit) and MW-5.

**PYC did not lead originally, despite being freshly measured.** The gate is already armed, so
new Python cannot make the debt worse while it waits — which is precisely the property that
made it safe to schedule behind V3-1 rather than ahead of it. Burning the tables down is
valuable; it is not urgent, and it is the one track in this queue with no user-visible outcome.

**MW-4 is in flight.** OD-3 is owner-executed. It is the maintenance campaign's only
real-catalog evidence, and the campaign cannot close without it. MW-5 stays behind it.

**A13 sat last on purpose** because MW-3 already refused the dangerous sweep, so the
exposure was a shared scratch root rather than a deletion. That write-path addressing
is now warehouse-keyed.

---

## PYC — the conventions burn-down

**Context.** `scripts/check_python_conventions.py` is armed and holds the measured debt in two
EXCEPTIONS tables. The tree is green and cannot regress. PYC empties those tables. The rules
themselves are [../AGENTS.md](../AGENTS.md) "Python"; the reasoning and the sanctioned exceptions
are [../.agents/skills/code-quality/SKILL.md](../.agents/skills/code-quality/SKILL.md).

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

### PYC-4 — done (merged #209)

20 `dataclass` files plus nested defs plus 10 unannotated returns under `python/repark-parity`,
and one of each under `scripts/`. Signal handlers / shrink predicate / spy / dual-wire
comparator ended as pragmas; bootstrap factories lifted together. Dual-wire kept as the
one DATACLASS_EXCEPTIONS row (bare `python3`). ANN ignores split:
`python/repark/tests/**` kept ANN201/ANN202; `python/repark-parity/tests/**` does not.

### PYC-5 — done (merged #211)

Tables: nested-def EXCEPTIONS empty; one sanctioned DATACLASS row (dual-wire). ANN:
facade tests dropped ANN201 (isolated count 0); two nested helpers annotated
(ANN202; not what earned the drop). ANN202 stays. Guard: 164 files, n=5 median
0.996 s (max 1.011 s). Hook dropped from pre-commit; dual-wired in `make ci` + CI.

### PYC-6 — done (merged #216): arm the docstring-presence subset, and the declined armings

**Measured 2026-08-22** with the pinned Ruff (`uvx ruff@0.15.22`), check-only, recorded here per
the arming method in [../.agents/skills/code-quality/SKILL.md](../.agents/skills/code-quality/SKILL.md)
"Arming a rule" — a rule measured and declined is written down so nobody re-litigates it from a
fresh `--select` run. The armed baseline config is **clean: 0 findings** repo-wide.

**Owner-ruled 2026-08-22: arm the docstring *presence* rules only.** Full `D` costs 803 findings
(556 facade / 234 parity / 13 scripts). The split the ruling turns on:

- Presence rules — `D103` undocumented function (163), `D105` magic method (57), `D102` public
  method (27), `D107` `__init__` (17), `D101` class (2) — ≈266 findings, and they enforce the
  conventions' "every function has a docstring" rule mechanically.
- Style rules — `D401` imperative mood (193), `D202`/`D205`/`D413` blank-line shape (289),
  the rest — are churn on a facade whose docstrings deliberately mirror upstream PySpark's own
  text. **Declined permanently, same ruling.**

The unit: select the five presence rules, re-measure at execution time (the 266 is today's
count, not the seed), seed the ratchet table from that measurement with per-file rows and
reasons (ceilings down only), keep the tests per-file ignore, and ship the gate with
provocation proofs per [../docs/testing.md](../docs/testing.md).

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

## MW-4 / MW-5 — OD-3 executed; MW-4 is this unit

**OD-3 is owner-executed (2026-08-23):** scoped `s3:DeleteObject` on the tier-2 acceptance
role's warehouse scratch prefix. The campaign never touches IAM. MW-4 stays on this slate
until it merges. MW-5 (registry close, the re-measured delta against MW-0's 2.1× baseline,
scorecard flip) is queued behind it and is small.

---

## A13 — done (merged #217): warehouse-keyed CTAS fallback

Roadmap item in [../task/roadmap-intake-2026-08-21.md](../task/roadmap-intake-2026-08-21.md),
surfaced by MW-3. `register_memory_catalog`'s location-less fallback root is now the
supplied warehouse (`{warehouse}/repark_ctas|repark_ansi_ctas/…`). Two sessions with
different warehouses no longer share a directory. Same warehouse + same names still
share; `remove_orphan_files` still refuses that fallback tree (table location, CALL
`location`, parent prefix, `file://` aliases). Ledger:
[../task/a13-shared-ctas-fallback-ledger.md](../task/a13-shared-ctas-fallback-ledger.md).
