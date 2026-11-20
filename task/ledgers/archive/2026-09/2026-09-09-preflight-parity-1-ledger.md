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
| C-004 | `make preflight` green once, alone, at the end of the step; exit code recorded in the ledger's Gates table. | One run, alone, from the clean commit-1 tree: exit **0**, real 14m18.7 s. Inside it, in order: `verify` (rust-test ok) → `py-test-facade` `5820 passed, 368 skipped` → `py-test-parity-cap` `23 passed in 0.24s` → `py-test-dbt` `59 passed, 1 skipped` → audit + workflows-lint clean (`No findings to report`). | **PROVEN** |
| C-005 | The owner question the card assigned to the orchestrator at departure — whether `AGENTS.md`'s gate-roster sentence should name `py-test-parity-cap` — is answered. | Answered by ruling **R-12** (owner, 2026-09-09, `task/roadmap/mid-term/cheap-tier-slate-2026-09-08.md` §0): `AGENTS.md`'s roster sentence stays as it is; `DEVELOPMENT.md` and the root `map.md` name new gate members. Both of those homes already name `py-test-parity-cap` (C-003), so the ruling closes this clause with no further edit. `CLAUDE.md`'s pointer row stays in lockstep with the untouched `AGENTS.md` sentence and is likewise unedited. | **PROVEN** |

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
| `make preflight` | exit **0**, one run, alone (2026-09-09, real 14m18.7 s): `py-test-parity-cap` member ran `23 passed in 0.24s` between `py-test-facade` (`5820 passed, 368 skipped`) and `py-test-dbt` (`59 passed, 1 skipped`); audit `No findings to report` |

## Pins

| Pin | Location |
|---|---|
| `test_cap_mirror_target_runs_the_one_file_with_the_parity_interpreter` | `python/repark-parity/tests/test_preflight_parity_1_wiring.py` |
| `test_preflight_places_the_cap_mirror_after_facade_before_audit` | `python/repark-parity/tests/test_preflight_parity_1_wiring.py` |

## Notes for the orchestrator

| Item | Note |
|---|---|
| `COVERAGE_ATTESTATION` | Written by the orchestrator at this departure edit, with C-005 closed under R-12. |
| `AGENTS.md` | Untouched, per the card. Its roster sentences ("Verify before done", "Delegated-agent standing rules") do not name `py-test-parity-cap`; the PR-body question is C-005. |
| C-003 scope note | The root `map.md` Makefile bullet was already missing `py-test-dbt` (pre-existing since DBT-1's merge, #429-era); the corrected enumeration names both members so the touched sentence is accurate rather than half-updated. |
| Out of scope observed | `CLAUDE.md`'s gate-roster pointer row ("preflight adds `py-test-facade` + `py-test-dbt` + audit + workflow lint") now omits the new member; not in the card's Home, left untouched. `python/dbt-repark/tests/map.md` ("wired into `make preflight`, immediately after `py-test-facade`") reads as if `py-test-dbt` is the immediate successor of `py-test-facade`; with the new member seated between them the sentence is loose — both belong to the roster true-up the owner question covers. |
| Disk | Cold clone: no `.venv`, no `target/` at start. `make py-test-parity-cap` needs no native build; the preflight run creates `target/` and `.venv` (both kept for the orchestrator's PR assembly; ~502 GB free at start). |

## Departure (orchestrator, 2026-09-09)

C-005 closed under R-12 without editing `AGENTS.md`; the unit's roster homes are
`DEVELOPMENT.md` and the root `map.md`, both already trued up in C-003. The two loose sentences
C-003's note observed (`CLAUDE.md`'s pointer row, `python/dbt-repark/tests/map.md`'s "immediately
after `py-test-facade`") stay as they are: the first tracks `AGENTS.md`, which R-12 leaves
untouched, and the second is DBT-1's home, outside this card.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: preflight-parity-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Both decisions were pinned in both directions on the base tree — no `py-test-parity-cap` target (StopIteration) and the member absent from the `preflight:` line (ValueError) — then green after the wiring, and the wired `preflight` was run once end to end, alone, with the new member's output read in place between `py-test-facade` and `py-test-dbt`.
      artifacts: [python/repark-parity/tests/test_preflight_parity_1_wiring.py, Makefile]
    - id: AT-2
      status: ATTACKED
      evidence: The D-1 budget is a measured number, not a claim - `time make py-test-parity-cap` measured 1.428 s against the 60 s ceiling, and the whole `preflight` run measured 14m18.7 s.
      artifacts: [task/ledgers/completed/preflight-parity-1-ledger.md]
    - id: AT-3
      status: N/A
      justification: No failure path is added; a make target either runs its file or the recipe fails, and both directions are pinned on the Makefile text.
    - id: AT-4
      status: ATTACKED
      evidence: Ordering IS the subject of D-2, so it is pinned directly - the member's position after `py-test-facade` and before `audit` on the `preflight:` line, and its absence from the `verify:` line.
      artifacts: [python/repark-parity/tests/test_preflight_parity_1_wiring.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, deserialization or path handling is added; the change is one make target running an existing test file.
    - id: AT-6
      status: ATTACKED
      evidence: The hole this closes is the one that let #427 pass locally and fail CI - a ratchet touching only `scripts/check_lib_py.py`. The mirror file now runs inside `preflight`, and the pin asserts the seat rather than the suite's own result, so deleting the target reds the pin.
      artifacts: [python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
    - id: AT-7
      status: ATTACKED
      evidence: Cost was the risk (a `preflight` member is paid on every pre-PR run) and it was measured, not assumed - 1.428 s, one forty-second of the budget D-1 set.
      artifacts: [Makefile]
    - id: AT-8
      status: N/A
      justification: No API, dependency or upstream contract is touched.
    - id: AT-9
      status: ATTACKED
      evidence: The whole unit exists so a red is diagnosable locally instead of in CI; the new member prints its own suite summary inside the `preflight` stream, read in the C-004 run.
      artifacts: [task/ledgers/completed/preflight-parity-1-ledger.md]
    - id: AT-10
      status: ATTACKED
      evidence: Both pins ran red on the base tree and green after, so the base Makefile is the mutant; the tests map carries the pins line for C-001..C-004.
      artifacts: [python/repark-parity/tests/map.md]
  complete: true
```
