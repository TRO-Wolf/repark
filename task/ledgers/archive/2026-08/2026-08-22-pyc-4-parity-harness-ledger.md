# PYC-4 — the parity harness and `scripts/`

**Unit:** PYC-4
**Branch:** `feat/pyc-4-parity-harness`
**Base:** `main` @ `4cf7430` (#208 PYC-3 squash-merged; replayed the PYC-4 commit onto that squash)
**Date:** 2026-08-22
**Path:** STANDARD with user `/sepmo-octo` (HIGH engine by request; none of this ships in the wheel)
**Critic engine:** `octo` (cycles=4, early_stop=true, claims_critic=true, floor S1)
**Approval:** user "Commit it and open the PR, then proceed" after PYC-3 #208

The ordered queue is [briefs/next-sequence.md](../../../../briefs/next-sequence.md). PYC-4 converts
the parity harness dataclasses and remaining nested defs. Campaign invariant: **no call
that worked before returns a different value.**

---

## PROPOSITION LEDGER — PYC-4 — 2026-08-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|--------|--------------------------|-------------------|---------|----------|
| C-001 | `NESTED_DEF_EXCEPTIONS` is empty (rows deleted, not zeroed) | AST of the table is `{}` | PROVEN | `test_pyc_4_nested_def_exceptions_table_is_empty` |
| C-002 | The 20 converted parity files leave `DATACLASS_EXCEPTIONS`; dual-wire remains | AST keys == `[scripts/check_parity_live_dual_wire.py]` | PROVEN | `test_pyc_4_dataclass_exceptions_are_only_dual_wire` |
| C-003 | Converted files do not import `dataclasses` and subclass `BaseModel` | Per-file source pin | PROVEN | `test_pyc_4_converted_files_do_not_import_dataclasses` |
| C-004 | Dual-wire stays a dataclass and has no pydantic import | Source pin; `field` pragma | PROVEN | `test_pyc_4_dual_wire_stays_dataclass_and_pragma` |
| C-005 | Flush / execute / factories have zero nested defs; walkers are module-level with only the alarm/spy remaining nested | Ancestor-walk on `_LIFTED_TO_ZERO` plus runner/harness exclusive pins | PROVEN | `test_pyc_4_lifted_modules_have_no_nested_defs`; `test_pyc_4_compat_runner_only_nested_def_is_the_alarm_handler`; `test_pyc_4_compat_harness_suite_walker_is_module_level` |
| C-006 | Signal handlers, shrink predicate, spy, dual-wire comparator stay pragmas | Named nested defs + non-empty reasons | PROVEN | `test_pyc_4_callback_sites_stay_pragmas` |
| C-007 | `CensusRow` / `PatchEntry` are BaseModels; extra kwargs refused | Construction + `ValidationError` | PROVEN | extra-field pins |
| C-008 | `CensusRow.test_id` is `str`; int `0` raises (dataclass stored it) | `ValidationError` pin | PROVEN | `test_pyc_4_census_row_rejects_int_test_id` |
| C-009 | Recorded-denominator dummy ids are strings (`carried-{index}`), not `dict(enumerate(...))` | Source + `compute_denominators` pin | PROVEN | `test_pyc_4_denominator_dummy_ids_are_strings`; existing `test_compare_reports` recorded-denom tests |
| C-010 | `repark-parity` declares `pydantic>=2.10,<3` | `python/repark-parity/pyproject.toml` | PROVEN | `test_pyc_4_parity_package_declares_pydantic` |
| C-011 | ANN ignores split: facade keeps ANN201/202; parity tests do not | `pyproject.toml` per-file-ignores | PROVEN | `test_pyc_4_ann_ignores_split_parity_from_facade`; `test_compare.py` ten returns annotated |
| C-012 | `map.md` / STATUS / sequence / ledger land in the same change | maps updated in every converted directory | PROVEN | working-tree maps; `make check-map-md` after remediations; index-relative — re-check after `git add` |
| C-013 | PYC-1/PYC-2 exception-key helpers tolerate an empty nested-def table | Those tests stay green | PROVEN | `found_table` instead of `assert keys` |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 13/13.

KILLED_ASSUMPTIONS:
- "Every harness dataclass can take pydantic" — REMOVED. Dual-wire is invoked as `python3`
  from make / CI with no venv; converting it is `ModuleNotFoundError`. Sanctioned row.
- "`dict(enumerate(carried))` is a valid CensusRow constructor" — REMOVED. Dataclass stored
  int keys; pydantic 2.13 rejects int→str even without `strict=True`. Dummy ids are now
  `carried-{index}` strings. Denominator *values* are unchanged.
- "PYC-3's remaining-key pin (any `python/repark-parity/` DATACLASS row) is a forever
  invariant" — REMOVED. That pin was PYC-3 scope. PYC-4 retargets it to dual-wire only.

RISK_HEATMAP:
- pydantic validation on census/fuzz/TPC records can reject shapes dataclass stored.
  Mitigation: no `strict=True` on harness models (lax); `extra="forbid"`; pin int `test_id`
  refuse; `StageTiming` keeps positional `__init__`; TPC-DS `QueryResult.status` stays
  `str` so BANANA still constructs (exit_code/ledger are the gates). Existing
  comparator/harness/write-bench batteries stay green.
- Lifting bootstrap factories changes what they close over. Mitigation: they take `cls` /
  `self` as arguments and are assigned as `classmethod`s — the same binding Apache sees.
- Bulk `strict=True` string replace can strip `zip(..., strict=True)`. Restored on the six
  fuzz/TPC compare files that lost it.

---

## Domain

### Nested defs (gate-counted 16 + dual-wire)

| File | Site | Disposition |
|---|---|---|
| `bench/fuzz/bank.py` | `_flush_parsed_table` | lift (accumulator args) |
| `bench/fuzz/runner.py` | `_execute_minimize_pair` | lift |
| `bench/fuzz/minimizer.py` | `still_diverges` | **pragma** (shrink-loop predicate) |
| `bench/tpch/runner.py` | `_alarm_handler` | **pragma** (SIGALRM) |
| `bench/tpcds/runner.py` | `_alarm_handler` | **pragma** (SIGALRM) |
| `compat/bootstrap.py` | five `_reused_*` factories | lift together; assigned as `classmethod`s |
| `compat/runner.py` | three `_walk_*` | lift (accumulator args) |
| `compat/runner.py` | `_handler` | **pragma** (SIGALRM) |
| `tests/test_compat_harness.py` | `_collect_suite_test_ids` | lift (recursive walker) |
| `tests/test_compat_harness.py` | `spy` | **pragma** (closes over captured kwargs) |
| `scripts/check_parity_live_dual_wire.py` | `field` | **pragma** (comparator closes over both surfaces) |

### Dataclass → BaseModel (20 files)

All converted models use `extra="forbid"`. No `strict=True` (JSON/int fixtures; int→str
does not coerce in pydantic 2.13 lax either). Dual-wire not converted.

### ANN

`**/tests/**` split into `python/repark/tests/**` (ANN201/ANN202/ANN001/S101) and
`python/repark-parity/tests/**` (ANN001/S101). Ten returns in `test_compare.py` annotated
(`_table` → `pa.Table`, nine tests → `None`). Ruff per-file-ignores merge as a union, so
a file cannot un-ignore a broader glob — that is why the glob itself split.

---

## Pins

- `python/repark-parity/tests/test_pyc_4_parity_harness.py`
- PYC-1/PYC-2 helpers now treat an empty `NESTED_DEF_EXCEPTIONS` as bound
- PYC-3 remaining-key pin is dual-wire only
- Behaviour stays on `test_compare_reports.py`, `test_compat_harness.py`, `test_compare.py`

## Bait

Empty `# nested-def:` reason on `minimizer.still_diverges` → conventions gate exit 1.
Restored; gate green.

`dict(enumerate(carried))` restored in `compare_reports.py` → `CensusRow(test_id=0)`
`ValidationError`; recorded-denominator tests fail. Replaced with `carried-{index}`.

## Ceilings

No `check_lib_py` EXCEPTIONS files in this unit.

## Isolated `make py-test` (C1-Q-001)

`--no-project` ignores `python/repark-parity/pyproject.toml`. PYC-4 dual-wires
`--with pydantic` on `Makefile` `py-test` and `.github/workflows/ci.yml`
`parity-harness tests`. Pin: `test_pyc_4_isolated_py_test_installs_pydantic`.
Isolated run after the wire: **296 passed**.

## Octo (SEPMO-octo)

`critic_engine: octo` · cycles=4 · early_stop=true · claims_critic=true · floor S1.

| Cycle | Half A OPEN ≥floor | Half B | Notes |
|---|---|---|---|
| 1 | C1-Q-001, C1-Q-002, C1-CL-001 | remediations | Critic-2/3 CLEAN |
| 2 | C2-CL-001 (stale verify citation) | Gate rewrite + `'pydantic>=2.10,<3'` + recipe-line pin | C1/C2/C3 CLEAN |
| 3–4 | skipped (`early_stop`) | — | no OPEN ≥S1 after cycle-2 fix |

Scratch: `/tmp/critic-octo-repark-pyc-4-2026-08-22/`

## Gate

`make verify` exit 0 **after cycle-1 remediations** (Makefile `--with pydantic`, ci.yml, pins).
`make check-map-md` exit 0 after those remediations (working tree; index-relative — re-run after `git add`).
`make check-python-conventions`: 163 files, nested-def rows 0, dataclass rows 1.
`make py-test` (isolated `--no-project`): **296 passed**.
`make preflight` exit 0 immediately before the PR: facade **3678 passed, 70 skipped, 0 failed**
(after StageTiming positional `__init__` and TPC-DS `QueryResult.status: str`).
