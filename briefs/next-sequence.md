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
| 1 | **PYC-1** | conventions | — | M |
| 2 | **PYC-2** | conventions | PYC-1 | S |
| 3 | **PYC-3** | conventions | — | S |
| 4 | **PYC-4** | conventions | — | M |
| 5 | **PYC-5** | conventions | PYC-1..4 | S |
| — | **MW-4** | maintenance | **OD-3 (owner)** | M |
| — | **MW-5** | maintenance | MW-4 | S |
| — | **A13** | write path | — | M |

**V3-1 merged as [#203](https://github.com/TRO-Wolf/repark/pull/203)** and left this file.
**PYC-1 is in flight** on `feat/pyc-1-dataframe-nested-defs`: the two DataFrame modules
(the 35 nested defs the gate counted at arming, plus `_emit_side` under `try:` which
that walker missed).

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
are [../skills/code-quality/SKILL.md](../skills/code-quality/SKILL.md).

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

### PYC-1 — the two DataFrame modules (35 of the 66 nested defs)

`spark/dataframe/core.py` (23) and `spark/dataframe/plan_collapse.py` (12).

- `core.py` is pandas/Arrow UDF execution: per-invocation closures over the user's UDF object, its
  slot bindings and the batch iterator. The fix is an explicit context object passed to
  module-level helpers, not a mechanical lift — several of these read three or four parent locals.
- `plan_collapse.py` splits cleanly in two: the `show`/`explain` formatters (`hline`, `row_line`,
  `fmt_row`, `is_numeric_cell`) close over computed column widths and lift by taking the width as
  an argument; the SQL token rewriters close over the join side map and need a context argument.
- **Start with the formatters.** They are the lowest-risk half of the lowest-risk file and they
  establish the lift pattern the rest of the unit reuses.
- Ratchet both ceilings down in the same commit; do not delete the rows until the count is zero.

### PYC-2 — the remaining shipped nested defs (14 across 10 files)

`joins_columns.py` (2), `session/session_core.py` (2), `session/_funcs.py` (2), `udtf.py` (2),
`types.py` (1), `functions.py` (1), `polars.py` (1), `row.py` (1), `ml/ext/_arrow_util.py` (1),
`ml/feature/_transformers.py` (1).

**Some of these are expected to end as pragmas, not lifts, and the unit fails if it forces them:**

- `types.py` — `_make_type_verifier` builds a verifier per type and **returns it**. The closure is
  the function's product. This is the callback case; it takes a pragma.
- `udtf.py` — the `udtf` decorator's `_build` is a decorator factory closing over its own
  arguments. Pragma.
- The two temp-view cleanup callbacks (`session/_funcs.py`, `ml/ext/_arrow_util.py`) are registered
  for later invocation and close over the view name. Judge each on whether passing it explicitly is
  clearer, not on whether it is possible.

The recursive walkers (`row.py`'s `asDict` converter, `_transformers.py`'s record walker) lift with
an accumulator argument and get shorter doing it.

### PYC-3 — the two shipped `dataclass` containers

`spark/merge.py` (MERGE INTO clause records built by the builder API) and `spark/_csv_smart.py`
(CSV type-inference state).

Small, but it is the unit where the validation hazard is real: `merge.py`'s records are built by a
public builder API, so a `BaseModel` will now validate whatever a user's MERGE chain produces.
**Pin the current accepted-input set before converting**, then confirm the model accepts exactly
that set and no less. A conversion that narrows what the builder accepts is a breaking change.

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
