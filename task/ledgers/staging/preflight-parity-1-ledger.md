# Unit ledger — PREFLIGHT-PARITY-1 · the parity mirror check joins `preflight`

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when PREFLIGHT-PARITY-1 merges, or when the owner closes the slate row.

**Unit:** PREFLIGHT-PARITY-1 · **Date:** 2026-09-09 · **Executor:** GLM 5.3 Flash (zai/glm-5.3-flash), Actor ·
**Branch:** `feat/preflight-parity-1` · **Card facts on:** `e24f4c68`
**Model:** GLM 5.3 Flash (zai/glm-5.3-flash)
**risk_tier:** standard.

Single-step unit (Rounds 1, M): D-1 + D-2. The CAP-1 source-file mirror
`python/repark-parity/tests/test_cap_1_source_file_line_cap.py` gains its own make target and a
`preflight` seat, so a size-gate ratchet that updates only `scripts/check_lib_py.py` reds the
pre-PR gate locally instead of failing CI's Python job (#427). `AGENTS.md` is untouched by the
card's own decision; its roster-sentence question travels in the PR body (C-005).

## Proposition ledger

| ID | Clause | Evidence | Verdict |
|---|---|---|---|
| C-001 | D-1: `make py-test-parity-cap` runs one file, the CAP-1 mirror test, with the same interpreter the parity suite uses (`py-test`'s recipe verbatim: `PYTHONPATH=python/repark-parity/src` + `uv run --no-project --with pyarrow --with pytest --with 'pydantic>=2.10,<3'`), and finishes under 60 s. | Red on base `e24f4c68`: `E StopIteration` at `test_preflight_parity_1_wiring.py:14` (`_target_block("py-test-parity-cap")` — no such target). Green after wiring: `2 passed in 0.02s`. Measured `time make py-test-parity-cap` → real **1.428 s**, `23 passed in 1.18s` (warm uv cache, 2026-09-09) — 1/42nd of the D-1 budget. | **PROVEN** |
| C-002 | D-2: `preflight` gains `py-test-parity-cap` after `py-test-facade`, before `audit`; `verify` is untouched (its prerequisite line stays `ci test`). | Red on base: `E ValueError: 'py-test-parity-cap' is not in list` at `test_preflight_parity_1_wiring.py:42`. Green after wiring: `test_preflight_places_the_cap_mirror_after_facade_before_audit` asserts the order on the `preflight:` line and the absence from the `verify:` line, `2 passed in 0.02s`. | **PROVEN** |
| C-003 | `DEVELOPMENT.md`'s gate roster names the new member (new `make py-test-parity-cap` row, updated `make preflight` row, "Where each suite runs" sentence, all dated), and the maps are in lockstep: `python/repark-parity/tests/map.md` lists the pin file and trued up the DF-EXPLAIN-1 "not reached by `make preflight`" sentence and the Debug `make py-test` note; the root `map.md` `Makefile` bullet's `preflight` enumeration now names `py-test-parity-cap` and the previously missing `py-test-dbt`; `task/ledgers/staging/map.md` links this ledger. | New bullet + dated corrections in `python/repark-parity/tests/map.md` (pins cited there: `preflight-parity-1/C-001, C-002, C-003, C-004`); root `map.md` Makefile bullet rewritten; `DEVELOPMENT.md` rows added; `make check-map-sync` green on the committed tree. | **PROVEN** |
| C-004 | `make preflight` green once, alone, at the end of the step; exit code recorded in the ledger's Gates table. | Run pending at commit 1 by design (the ledger rides commit 1, preflight runs after it and is recorded in commit 2). | OPEN |
| C-005 | The PR body carries one line asking the owner whether `AGENTS.md`'s gate-roster sentence ("Verify before done" / "Delegated-agent standing rules") should name `py-test-parity-cap`; the card leaves `AGENTS.md` untouched and assigns the question to the orchestrator at departure. | Orchestrator action at PR assembly; not executable by this step. | OPEN |

## Red first

The pin file `python/repark-parity/tests/test_preflight_parity_1_wiring.py` was written and run
on the base tree (`e24f4c68`) before the Makefile changed, with the parity suite's own
interpreter:

- `test_cap_mirror_target_runs_the_one_file_with_the_parity_interpreter` → `FAILED` with
  `StopIteration` from `_target_block("py-test-parity-cap")` — the target does not exist on the
  base.
- `test_preflight_places_the_cap_mirror_after_facade_before_audit` → `FAILED` with
  `ValueError: 'py-test-parity-cap' is not in list` — the base `preflight:` line has no such
  member.

Short summary: `2 failed in 0.03s`, exit 1. One pin adjustment after the red run, disclosed: the
C-001 scan was narrowed from the whole target block to the recipe lines, because the target's
own `##` help text legitimately points at `python/repark-parity/tests/map.md` and a path scan
over the header false-positived on it; both failing atoms (no target; absent from the
`preflight` line) are unchanged.

## Gates

| Command | Result |
|---|---|
| `PYTHONPATH=python/repark-parity/src uv run --no-project --with pyarrow --with pytest --with 'pydantic>=2.10,<3' pytest python/repark-parity/tests/test_preflight_parity_1_wiring.py -q` (base) | exit 1, `2 failed in 0.03s` — the recorded red |
| same command, after wiring | exit 0, `2 passed in 0.02s` |
| `make py-test-parity-cap` | exit 0, real 1.428 s, `23 passed in 1.18s` |
| `make ci` | exit 0 (2026-09-09): `ledger-grammar: 61 live ledgers clean`, `ledger-check: … 790 ledger links resolve`, `map-sync: 225 maps clean`, `manifest: 17 components … agree`, `docs-compaction: clean`, `owner-ruling: … present`, `All checks passed!` |
| `make preflight` | recorded in commit 2 |

## Pins

| Pin | Location |
|---|---|
| `test_cap_mirror_target_runs_the_one_file_with_the_parity_interpreter` | `python/repark-parity/tests/test_preflight_parity_1_wiring.py` |
| `test_preflight_places_the_cap_mirror_after_facade_before_audit` | `python/repark-parity/tests/test_preflight_parity_1_wiring.py` |

## Notes for the orchestrator

| Item | Note |
|---|---|
| No `COVERAGE_ATTESTATION` block | C-004 and C-005 stay `OPEN` at this step's hand-back (the preflight run and the PR-body owner question), so the unit is in flight and files no attestation yet; the orchestrator writes it at the departure edit. |
| `AGENTS.md` | Untouched, per the card. Its roster sentences ("Verify before done", "Delegated-agent standing rules") do not name `py-test-parity-cap`; the PR-body question is C-005. |
| C-003 scope note | The root `map.md` Makefile bullet was already missing `py-test-dbt` (pre-existing since DBT-1's merge, #429-era); the corrected enumeration names both members so the touched sentence is accurate rather than half-updated. |
| Out of scope observed | `CLAUDE.md`'s gate-roster pointer row ("preflight adds `py-test-facade` + `py-test-dbt` + audit + workflow lint") now omits the new member; not in the card's Home, left untouched. `python/dbt-repark/tests/map.md` ("wired into `make preflight`, immediately after `py-test-facade`") reads as if `py-test-dbt` is the immediate successor of `py-test-facade`; with the new member seated between them the sentence is loose — both belong to the roster true-up the owner question covers. |
| Disk | Cold clone: no `.venv`, no `target/` at start. `make py-test-parity-cap` needs no native build; the preflight run creates `target/` and `.venv` (both kept for the orchestrator's PR assembly; ~502 GB free at start). |
