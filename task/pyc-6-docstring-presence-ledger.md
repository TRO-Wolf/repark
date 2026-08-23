# PYC-6 — arm public-docstring presence

**Unit:** PYC-6
**Branch:** `feat/pyc-6-docstring-presence`
**Base:** `main` @ `92afd2a` (#215 SEPMO pickup-ritual + conventions-skill bindings)
**Date:** 2026-08-22
**Path:** STANDARD (size S; LIGHT fails file-count). **critic_engine: octo** (user `/sepmo-octo`)
**Critic engine:** `octo` (cycles=4, early_stop=true, claims_critic=true, floor S1)
**Approval:** user "proceed with PYC-6 under /sepmo-octo" after #215 on main

Arm the five docstring-*presence* rules with a seeded ratchet. Do not fill the 136
docstrings. Style `D` / `PL` / `A` / `print()` stay declined.

## Pickup ritual

Base `92afd2a` is #215. `make check-map-sync`: 143 maps clean. The #215 delta
(SEPMO binding-manifest rows, AGENTS.md map-sync roster row, `drop_dead_rows`
value compare, scripts/map.md `--fix` span) is already single-homed; STATUS and
next-sequence do not restate those facts. Compact against that delta is empty —
no docs-only first commit.

## PROPOSITION LEDGER — PYC-6 — 2026-08-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|--------|--------------------------|-------------------|---------|----------|
| C-001 | The gate selects exactly `D101`, `D102`, `D103`, `D105`, `D107` | AST of `PRESENCE_RULES` | PROVEN | `test_pyc_6_presence_rules_are_the_five_owner_ruled` |
| C-002 | Style `D` (`D401`/`D202`/`D205`/`D413`) is not selected here or in py-lint | AST + pyproject `select` | PROVEN | `test_pyc_6_style_d_not_selected` |
| C-003 | Tests keep a per-file `D` ignore | pyproject per-file-ignores | PROVEN | `test_pyc_6_tests_keep_d_per_file_ignore` |
| C-004 | EXCEPTIONS is the re-measured seed: 39 files, ceilings sum to 136, no `/tests/` path, sorted | AST of the table | PROVEN | `test_pyc_6_exceptions_seed_is_the_measured_table`; ruff JSON 2026-08-22 |
| C-005 | Ruff pin matches the Makefile | `RUFF_PIN` vs `RUFF :=` | PROVEN | `test_pyc_6_ruff_pin_matches_makefile` |
| C-006 | Dual-wired `make ci` + ci.yml | Makefile `ci:` + workflow `run:` | PROVEN | `test_pyc_6_dual_wired_make_ci_and_workflow` |
| C-007 | On the pre-commit hook; conventions stays off | `.pre-commit-config.yaml` + install-hooks printf | PROVEN | `test_pyc_6_on_pre_commit_hook`; n=5 median **0.13 s** |
| C-008 | Fail-closed on ruff missing / bad JSON / empty scan / stale key / zero-count row | unit tests + provocation proofs | PROVEN | `test_pyc_6_check_counts_stale_and_zero_rows_are_red`; `test_pyc_6_empty_ruff_stdout_is_fail_closed`; ledger § Provocations |
| C-009 | A new undocumented public name on an unlisted file is red; growing a listed file past its ceiling is red | unit tests + provocation proofs | PROVEN | `test_pyc_6_check_counts_unlisted_file_is_red`; `test_pyc_6_check_counts_over_ceiling_is_red` (closed-world ceiling-1 fixture); `test_pyc_6_ruff_is_invoked_on_collected_files`; ledger § Provocations |
| C-010 | STATUS / sequence / maps / AGENTS / DEVELOPMENT name the arming | prose pin | PROVEN | `test_pyc_6_prose_homes_name_the_gate` |
| C-011 | LRS: no shipped call returns a different value | unit is a gate + docs; no runtime edit of shipped functions | PROVEN | diff does not change shipped function bodies |
| C-012 | `PL` / `A` / `print()` stay declined (not armed) | pyproject `select` has none of them | PROVEN | `test_pyc_6_style_d_not_selected` covers `select`; `PL`/`A` absent from pyproject select |

**Approval gate:** every clause PROVEN, zero OPEN, zero REJECTED. User confirmation: the proceed message above.

LOGIC_SCORE = 12/12.

## Measurement (re-measured at execution, 2026-08-22)

Pinned Ruff `uvx ruff@0.15.22`, `--select D101,D102,D103,D105,D107`.

| Slice | Findings | Notes |
|---|---:|---|
| SCAN_ROOTS excluding `**/tests/**` | **136** (39 files) | the seed. D103=32, D105=57, D102=27, D107=18, D101=2 |
| SCAN_ROOTS including tests | 279 | D103 grows; not the seed |
| `python/repark/tests` + `python/repark-parity/tests` | 1325 D103 | out of scope; per-file ignore |
| Style `D` on production (those 5 codes ignored) | 526 | D401=194, D202=142, D205=93, D413=49; declined |

## PR_SCOPING

One PR unit. LIGHT rubric fails criterion 3 (more than 5 files). STANDARD. User
asked `/sepmo-octo` → critic_engine octo. `claims_critic=true` (STATUS, sequence,
maps, ledger).

## Provocations

Identifiers were never committed. Commands: `python3 scripts/check_docstring_presence.py`.

**P0 clean (must-PASS).** Exit 0. `docstring-presence: 152 files clean (presence rows 39, findings 136)`.

**P1 new undocumented public function in an unlisted file (must-FAIL).**
`scripts/pyc6_provoke_new.py` containing `def leftover() -> None: return None`.
Ruff: `D103 Missing docstring in public function`. Gate exit 1:
`ERROR: scripts/pyc6_provoke_new.py has 1 undocumented public name(s) and no EXCEPTIONS row.`
A leading-underscore filename (`_pyc6_provoke_new.py`) does **not** fire D103 —
Ruff treats that module as private; recorded as the parser's public definition,
not a missed class. Reverted.

**P2 grow a listed file past its ceiling (must-FAIL).** Appended
`def leftover_public() -> None: return None` to `scripts/check_lib_py.py`.
Exit 1: `ERROR: scripts/check_lib_py.py has 3 undocumented public name(s) (ceiling 2).`
Reverted.

**P3 stale EXCEPTIONS key (must-FAIL).** Inserted
`"scripts/no_such_pyc6_file.py": (1, "provoke stale key")`. Exit 1:
`ERROR: EXCEPTIONS key has no file on disk: scripts/no_such_pyc6_file.py`
and `not in the scan set`. Reverted.

**P4 zero-count row (must-FAIL).** Inserted
`"scripts/check_docstring_presence.py": (1, "provoke zero row")`. Exit 1:
`ERROR: EXCEPTIONS key scripts/check_docstring_presence.py measures 0 (ceiling 1) — delete the row rather than keep it.`
Reverted.

**P5 style D401 does not fail this gate (must-PASS).**
`scripts/pyc6_provoke_d401.py` with `"""Returns a token."""`. Isolated
`ruff --select D401` reports D401; the presence gate exit 0
(`153 files clean … findings 136`). Reverted.

**P6 tests stay out of scope (must-PASS).** The existing test trees hold 1325 D103;
the gate stays green at 152 files / 136 findings.

## Gate

`make ci` exit 0. `make test` exit 0. `make py-test` **321** passed (16 `test_pyc_6_*`).
`make preflight` exit 0 — 3678 facade tests passed, 70 skipped. Gate: 152 files,
39 presence rows, 136 findings.

## Actor Self Logic Review

Intent: arm presence-only D with a ratchet; do not fill 136 docstrings.
Assumptions discharged: Ruff's public-module definition (`_*.py` is private); tests out of scope.
Failure modes attacked by provocations P1–P6.
Verification: ci/test/py-test green; dual-wire pins; hook 0.13 s.
Verdict: PROCEED.

## Octo

cycles=4, early_stop after cycle 3 Half A CLEAN. Label `OCTO-CONVERGED`.
Scratch `/tmp/critic-octo-repark-pyc-6-2026-08-22/`.
Cycle 1 S1: C1-Q-001 (check_counts pins), C1-L-001 (explicit file list + isolated).
Cycle 2 S1: C2-Q-001 (relatives argv pin), C2-Q-002 (closed-world ceiling-1 fixture).
Cycle 3: all four Critics CLEAN.
CL-IDENTITY: checked at commit (`%ae` TRO-Wolf noreply + `Authored-By: Grok (grok-4.6)`).
