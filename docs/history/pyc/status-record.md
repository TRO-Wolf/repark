# pyc — STATUS record

## Cut from STATUS.md — closed 2026-08-22 by #216

- **Python convention conformance (PYC)** (chartered 2026-08-21 by the owner; **PYC-1
  merged as [#204](https://github.com/TRO-Wolf/repark/pull/204)**; **PYC-2
  merged as [#207](https://github.com/TRO-Wolf/repark/pull/207)**; **PYC-3
  merged as [#208](https://github.com/TRO-Wolf/repark/pull/208)**; **PYC-4
  merged as [#209](https://github.com/TRO-Wolf/repark/pull/209)**; **PYC-5
  merged as [#211](https://github.com/TRO-Wolf/repark/pull/211)** / `b966c9b`). Four Python rules the owner stated, now written into the contract:
  types on everything; Pydantic v2 `BaseModel` rather than `dataclasses`/`attrs`; no function
  defined inside another function; functions named as verb phrases for the work they do. The rules
  themselves landed in [AGENTS.md](../../../AGENTS.md) "Python" and in the tier manuals (since 2026-08-24
  generalized into
  [.agents/skills/engineering-method/SKILL.md](../../../.agents/skills/engineering-method/SKILL.md)) with
  the guard that holds two of them, **merged as
  [#201](https://github.com/TRO-Wolf/repark/pull/201)** / `5f05d8c`; the conformance work is what
  remains.
  - **Measured debt (AST scan at guard arming 2026-08-21, not an estimate):**
    - *Types.* The shipped package is already clean — 2,170 functions, **zero** missing a return
      annotation, because Ruff's `ANN` rules are selected in [pyproject.toml](../../../pyproject.toml) and
      gate CI. **PYC-4** split the tests glob so `python/repark-parity/tests/**` no longer
      inherits ANN201/ANN202 and annotated the ten returns in `test_compare.py`. **PYC-5**
      dropped unearned facade `ANN201` (isolated count 0); `ANN202` stays for private
      helpers. `scripts/` has zero unannotated returns.
    - *Pydantic.* **PYC-3** converted `spark/merge.py` and `spark/_csv_smart.py` to
      Pydantic v2 `BaseModel` (and added `pydantic>=2.10,<3` as the wheel's second hard
      runtime dep). **PYC-4** converted the 20 `python/repark-parity` dataclass files
      (`pydantic>=2.10,<3` on that package too). Remaining sanctioned row:
      `scripts/check_parity_live_dual_wire.py` (runs as bare `python3`, no venv pydantic).
    - *Nested functions.* **66** nested `def`s in 21 files at arming. **PYC-1** lifted
      the 35 the gate counted in `spark/dataframe/core.py` (23) and
      `spark/dataframe/plan_collapse.py` (12), plus `_emit_side` under `try:`.
      **PYC-2** lifts or pragmas the remaining 14 shipped nested defs across 10 files
      (plus `session_core.probe` under `if`); those ten EXCEPTIONS rows are deleted,
      not zeroed. **PYC-4** emptied `NESTED_DEF_EXCEPTIONS`: walkers/factories/flush/
      execute lifted; signal handlers, shrink predicate, spy, and dual-wire comparator
      ended as `# nested-def:` pragmas. Dataclass remaining after **PYC-4**: 1 row.
    - *Docstrings.* **PYC-6** armed presence-only (`D101`/`D102`/`D103`/`D105`/`D107`)
      over the same SCAN_ROOTS as the conventions guard, tests excluded. Seeded
      2026-08-22 at **136** findings across **39** files (the slate's ~266 included
      tests). Style `D` (`D401`/`D202`/`D205`/`D413` and the rest) declined
      permanently — facade docstrings mirror PySpark. `PL` / `A` / `print()` stay
      declined with the reasons recorded in [briefs/next-sequence.md](../../../briefs/next-sequence.md)
      (the declined-armings record; PYC-6 left the rolling queue).
    - *Names.* Not machine-countable; it rides along with whatever the other three touch.
  - **The guard is armed** (owner ruled 2026-08-21). Ruff has no check for a nested `def` and none
    for "Pydantic rather than `dataclass`", so those two rules now live in
    `scripts/check_python_conventions.py`, dual-wired `make check-python-conventions` (in the
    `make ci` chain) + ci.yml's `python` job. **Not** on the pre-commit hook as of PYC-5. The
    measured debt above is seeded into its two EXCEPTIONS tables, so the tree is green today and
    **cannot get worse**: a new nested `def` or a new `dataclass` import is red on `make ci` / CI.
    PYC is now the burn-down of those tables rather than a rule nobody can enforce. **PYC-5**
    re-measured the hook at n=5 median **0.996 s** (max 1.011 s) over **164** files — at the
    sub-second budget line, with the max already over it — and dropped it from pre-commit; it
    stays dual-wired in `make ci` + CI. **PYC-6** added `scripts/check_docstring_presence.py`
    for public-docstring presence, dual-wired `make check-docstring-presence` + ci.yml's
    `python` job, and on the pre-commit hook (n=5 median **0.13 s**). Ruff `ANN` still
    holds types; naming stays review.
  - **The nested-`def` rule ships with an inline pragma**, `# nested-def: <reason>`, for the three
    cases the contract sanctions: a decorator closing over its own arguments, a callback whose
    closure over local state is the point, and a `functools.wraps` wrapper. An empty reason does
    not pass. Seeded rows that ended as pragmas: PYC-2 `udtf._build` and `types.py` `verifier`;
    PYC-4 the TPC-H/TPC-DS and census SIGALRM handlers, the fuzz shrink predicate, the
    harness spy, and the dual-wire `field` comparator.
  - **Sequenced** in [briefs/next-sequence.md](../../../briefs/next-sequence.md) as PYC-1 (merged,
    the two DataFrame modules), PYC-2 (merged: remaining shipped nested defs; the
    `udtf` builder and `types.py` verifier ended as pragmas, not lifts), PYC-3
    (merged as [#208](https://github.com/TRO-Wolf/repark/pull/208): the two shipped
    `dataclass` containers → `BaseModel`; accepted-input set pinned; pydantic
    becomes a wheel hard dep), PYC-4 (merged as [#209](https://github.com/TRO-Wolf/repark/pull/209):
    the parity harness and `scripts/`, plus narrowing the `ANN` per-file ignores),
    and PYC-5 (merged as [#211](https://github.com/TRO-Wolf/repark/pull/211): close —
    hook off pre-commit, unearned facade ANN201 dropped, dual-wire dataclass row
    stays the sanctioned leftover), and PYC-6 (merged as
    [#216](https://github.com/TRO-Wolf/repark/pull/216): public-docstring
    presence `D101`/`D102`/`D103`/`D105`/`D107` armed with a seeded ratchet;
    style `D` declined permanently). No further chartered PYC unit. The dual-wire
    dataclass leftover and the D-presence EXCEPTIONS table are remaining debt,
    not sequenced work.
  - **Rationale and the arming method are a portable skill**,
    [.agents/skills/code-quality/SKILL.md](../../../.agents/skills/code-quality/SKILL.md): each rule with the failure it
    prevents and whether it is held by a linter, a gate, or review, plus the ratchet pattern for
    arming a convention against a codebase that already violates it.
  - **The risk this campaign carries is that it is a pure refactor of working code.** The facade
    suite at arming was 3,639 passing tests, none of them about where a `def` sits; PYC-1
    and PYC-2 add layout pins for that, PYC-3 pins the accepted-input set of the two
    shipped containers, PYC-4 pins EXCEPTIONS identity plus the CensusRow type check
    (`test_id: str`; dummy denominator ids are strings). Lifting a closure changes what
    it can see; converting a `dataclass` to a `BaseModel` adds validation that was not
    running before and can reject input the old container accepted. The invariant to
    hold is the LRS one: no query that worked before returns a different value.
