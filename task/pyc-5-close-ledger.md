# PYC-5 — close

**Unit:** PYC-5
**Branch:** `feat/pyc-5-close`
**Base:** `main` @ `a6ebac0` (#209 PYC-4 + #210 `.agents/` rename)
**Date:** 2026-08-22
**Path:** STANDARD (size S; LIGHT fails file-count). **critic_engine: octo** (user `/sepmo-octo`)
**Critic engine:** `octo` (cycles=4, early_stop=true, claims_critic=true, floor S1)
**Approval:** user "proceed with PYC-5, follow our /sepmo-octo" after #209/#210 on main

Campaign close for the conventions burn-down. Dual-wire dataclass row stays sanctioned.

## PROPOSITION LEDGER — PYC-5 — 2026-08-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|--------|--------------------------|-------------------|---------|----------|
| C-001 | `NESTED_DEF_EXCEPTIONS` is empty (deleted, not zeroed) | AST of the table is `{}` | PROVEN | `test_pyc_5_nested_def_exceptions_empty` |
| C-002 | `DATACLASS_EXCEPTIONS` is only dual-wire | AST keys == `[scripts/check_parity_live_dual_wire.py]` | PROVEN | `test_pyc_5_dataclass_exceptions_only_dual_wire` |
| C-003 | Facade tests no longer ignore ANN201; ANN202/ANN001/S101 stay | pyproject per-file-ignores | PROVEN | `test_pyc_5_ann201_not_ignored_on_tests`; isolated ruff ANN201 count 0 |
| C-004 | Nested helpers `door` and `guarded` have return annotations | source pin | PROVEN | `test_lrs4_door_domain.py`; `test_polars_core.py` |
| C-005 | Conventions guard is not on the pre-commit hook | `.pre-commit-config.yaml` + `install-hooks` printf + AGENTS sentence | PROVEN | `test_pyc_5_conventions_guard_not_on_pre_commit_hook` |
| C-006 | Guard stays in `make ci` + ci.yml | Makefile `ci:` target + workflow step | PROVEN | `test_pyc_5_conventions_stays_in_make_ci_and_workflow` |
| C-007 | Hook cost re-measured against the sub-second budget | timed `python3 scripts/check_python_conventions.py` | PROVEN | n=5, samples 1.011/0.995/0.992/0.997/0.996, median **0.996 s** (max 1.011 s) over **164** files (arming 0.94 s). At the `< 1 s` line, with the max already over it — drop, do not round the median up to call it over budget. |
| C-008 | STATUS / sequence / maps / AGENTS / DEVELOPMENT name the drop | prose + map lockstep | PROVEN | this change |
| C-009 | LRS: no shipped call returns a different value | unit touches hook wiring, ANN ignore, two test helper annotations, docs | PROVEN | no shipped-package runtime edit |

**Approval gate:** every clause PROVEN, zero OPEN, zero REJECTED. User confirmation: the proceed message above.

LOGIC_SCORE = 9/9.

## ANN review

Isolated `ruff --select ANN201` on `python/repark/tests`: **0**. Nested `door`/`guarded`
are ANN202 (private-to-ruff); annotated anyway so every signature is typed
(`test_pyc_5_door_and_guarded_have_return_annotations`). ANN202 stays ignored
(58 private helpers). Parity tests stay without ANN201/ANN202 (PYC-4).

## Hook

Sub-second budget is `< 1 s`. Measured n=5 median **0.996 s**, max **1.011 s**,
164 files. Dropped from `make install-hooks` and `.pre-commit-config.yaml` because
the guard sits on the budget line with the max already over it. Dual-wired
`make check-python-conventions` + ci.yml python job.

## PR_SCOPING

One PR unit. LIGHT rubric fails criterion 3 (more than 5 files). STANDARD. User
asked `/sepmo-octo` → critic_engine octo.

## Gate

`make verify` exit 0. `make preflight` exit 0 — 3678 facade tests passed, 70 skipped, 0 failed.
`make py-test` 305 passed. python-conventions: 164 files, nested-def rows 0, dataclass rows 1.

**Octo:** cycles=4, early_stop after cycle 3 Half A CLEAN. Label `OCTO-CONVERGED`. Scratch `/tmp/critic-octo-repark-pyc-5-2026-08-22/`. CL-IDENTITY: `%ae` TRO-Wolf noreply + `Authored-By: Grok (grok-4.6)` at commit.
