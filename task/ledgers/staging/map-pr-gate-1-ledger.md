# Unit ledger — MAP-PR-GATE-1 · the map.md lockstep unit is the pull request

**Date:** 2026-09-20 · **Branch:** `chore/map-pr-gate-1` · **Base:** `6d029ab8` (`origin/main`)
**Model:** devin (SWE-2) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**What it is.** The invariant "`map.md` in every directory, updated in the same change" changes
unit: the same change is now the pull request, and CI holds it. `check_map_md.sh` gains a branch
mode (`--base <ref>`, `git diff <ref>...HEAD`); ci.yml's `map.md guard` runs it on pull requests
only; `make check-map-md` calls it with `BASE ?= origin/main`; the two hook paths keep the staged
mode, now warn-only. `.gitattributes` sets `map.md merge=union` so a local merge or rebase that
touches one map on both sides keeps both rows, and `sync_map_md.py` gains a third unconditional
rule — two list rows in one map sharing a first-link target are a `duplicate row` finding —
for exactly what the union resolution leaves behind.

**Union-merge measurement (scratch clone, local only).** Control: without the attribute, a merge
of two branches that each added a row to the same `map.md` conflicted. With
`map.md merge=union` / `**/map.md merge=union` committed on the base branch, the same merge
exited 0 with both rows preserved; a rebase of the same pair exited 0 with both rows preserved —
the duplicate rows that result are exactly what the new `sync_map_md.py` rule reports. GitHub's
server-side mergeability check was **not measured**: the lane cannot push throwaway branches, so
whether the server-side "mergeable" computation honours `merge=union` remains open (the work
order expected it to ignore the driver; the local pre-merge rebase is where the conflicts cost
time, and that path is measured).

**Pre-existing duplicates.** With the rule armed, `sync_map_md.py --check` found **32**
first-link collisions across the 310-map tree: `briefs` (1), `crates/repark-core/src` (2),
`crates/repark-core/src/dynamic_flatten` (1), `crates/repark-core/src/session` (2),
`crates/repark-core/src/session/df_guards` (1), `crates/repark-distributed` (1),
`crates/repark-functions/src` (1), `crates/repark-python` (2),
`crates/repark-spark/benches/ice_read_perf` (1), `crates/repark-spark/src` (6),
`python/repark/tests` (8), `scripts` (1), `task` (1), `task/ledgers/completed` (1),
`task/port` (2). Two were true duplicate rows (`python/repark/tests` `test_array_null_1.py` stub,
`task/ledgers/completed` `java-double-fd-1-ledger.md` stub) plus one duplicated `## Pointers`
block in `python/repark/tests`; the rest were rows whose first link was an incidental shared
pointer (a sub-map, a baseline doc, `../map.md`) — resolved by linking each row's own subject or
re-pointing file-named links at the real files. The tree is clean with the rule armed.

**Not in this unit:** the GitHub server-side mergeability measurement above; no map format
change, no sorting, no generator (step 2 owns map migration); no Rust or package source.

## PROPOSITION LEDGER — MAP-PR-GATE-1 — 2026-09-20

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `check_map_md.sh --base <ref>` exits 0 when a code file and its directory's `map.md` both changed in the `<ref>...HEAD` diff. | `test_branch_mode_passes_when_code_and_map_change` (scratch repo, committed pair). | PROVEN | Green in `python/repark-parity/tests/test_map_pr_gate_1.py`. |
| C-002 | The same mode exits 1 naming `<dir>/map.md` when code changed without its map. | `test_branch_mode_fails_when_only_code_changes`. | PROVEN | Green; the finding is `ERROR: pkg/map.md was not updated on this branch`. |
| C-003 | A new directory carrying code but no `map.md` fails branch mode. | `test_branch_mode_fails_for_a_new_directory_without_map`. | PROVEN | Green; the finding is `ERROR: newdir has changed code but no map.md`. |
| C-004 | A deletion-only code change is ignored: the code list reads `--diff-filter=d`. | `test_branch_mode_ignores_deletion_only_changes`. | PROVEN | Green. |
| C-005 | A root-level code file's map is `map.md`, not `./map.md`: red without it, green once it changes in a later commit of the same branch. | `test_branch_mode_handles_a_root_level_file`. | PROVEN | Green; also proves the "passes once the map changes in a later commit" requirement. |
| C-006 | Bare (staged) mode exits 0, prefixes each finding `WARNING:`, and names the CI check that holds the rule. | `test_staged_mode_warns_and_exits_zero`. | PROVEN | Green; the trailing line names ci.yml's `map.md guard` step. |
| C-007 | `sync_map_md.py` reports `duplicate row` when two list rows in one map share a first-link target, unconditionally, and `--fix` never resolves it. | `test_duplicate_row_rule_fires_on_two_rows`, `test_duplicate_row_rule_survives_fix`. | PROVEN | Both green; `--fix` leaves both rows in place. |
| C-008 | A single row that mentions the same target twice is not a duplicate (the comparison is between rows, on first links only). | `test_duplicate_row_rule_quiet_when_one_row_mentions_twice`. | PROVEN | Green. |
| C-009 | `make check-map-md` runs the branch mode over `BASE ?= origin/main`, and `install-hooks` plus `.pre-commit-config.yaml` invoke the staged mode identically. | diff read of `Makefile` and `.pre-commit-config.yaml`; `make check-map-md` exercised in gates. | PROVEN | `BASE ?= origin/main` added; the hook paths both read `scripts/check_map_md.sh` with no argument; `make check-map-md` green on this branch (§Gates). |
| C-010 | ci.yml's `map.md guard` step runs `bash scripts/check_map_md.sh --base "origin/${{ github.base_ref }}"` on pull requests and is skipped on a push to main; the checkout fetches the base ref so the merge base resolves. | diff read of `.github/workflows/ci.yml`. | PROVEN | The step carries `if: github.event_name == 'pull_request'` and a `fetch` of the base ref; the push path never reaches it. The end-to-end "CI step fails on such a PR" proof needs a draft PR — not measurable from this lane (no push); recorded as open in the hand-back, not claimed here. |
| C-011 | `map.md merge=union` resolves a two-sided same-map edit by keeping both rows — measured locally for merge and for rebase in a scratch clone. | scratch-clone measurement (transcript summarised above). | PROVEN | Control conflicted; union merge and union rebase each exited 0 with both rows preserved. GitHub server-side mergeability unmeasured — see the measurement note. |
| C-012 | `python3 scripts/sync_map_md.py --check` is clean on the tree with the duplicate-row rule armed; the 32 pre-existing findings are listed above and fixed in this pull request. | `python3 scripts/sync_map_md.py --check` on the branch tree. | PROVEN | `map-sync: 310 maps clean (strict=off)` after the fixes (§Gates). |

## Gates

- `python3 -m pytest python/repark-parity/tests/test_map_pr_gate_1.py` — 9 passed (uv-provisioned
  pytest; system python carries none).
- `make check-map-md check-map-sync check-docs-links check-manifest` — green on this branch.
- `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xb-map origin/main HEAD` — clean.
- `python3 scripts/check_ledger_grammar.py` — this ledger's clauses and attestation.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: map-pr-gate-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every work-order clause is pinned by a fixture test that exercises the exact
        wording — branch diff semantics, warn-only staged mode, the duplicate-row rule.
      artifacts: [python/repark-parity/tests/test_map_pr_gate_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary inputs are exercised: deletion-only diffs, a root-level file, a new
        directory with no map, a single row naming one target twice, `--fix` against duplicates.
      artifacts: [python/repark-parity/tests/test_map_pr_gate_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Red paths are asserted, not assumed — exit 1 with the named `ERROR:` text in
        branch mode, exit 0 with `WARNING:` in staged mode, exit 1 under `--fix` with the rows
        untouched.
      artifacts: [python/repark-parity/tests/test_map_pr_gate_1.py]
    - id: AT-4
      status: N/A
      justification: No shared or mutable state; the guard is a pure `git diff` read per run.
    - id: AT-5
      status: N/A
      justification: No credentials, network or privileged action; refs are local git objects.
    - id: AT-6
      status: ATTACKED
      evidence: `merge=union` changes conflict resolution on maps; measured locally on merge and
        rebase (both rows kept, exit 0). Server-side mergeability is unmeasured and recorded as
        such rather than claimed.
      artifacts: [.gitattributes, task/ledgers/staging/map-pr-gate-1-ledger.md]
    - id: AT-7
      status: N/A
      justification: One extra `git diff` per guard run; nothing hot or unbounded.
    - id: AT-8
      status: ATTACKED
      evidence: The interface surface — the `--base` CLI, the Makefile `BASE` contract, the two
        hook paths staying identical, the ci.yml event gate and base-ref fetch — is pinned by the
        tests plus a diff read.
      artifacts: [scripts/check_map_md.sh, Makefile, .pre-commit-config.yaml,
        .github/workflows/ci.yml]
    - id: AT-9
      status: ATTACKED
      evidence: Failure paths are self-diagnosing: staged findings name the CI check that holds
        the rule; `duplicate row` findings name both line numbers.
      artifacts: [scripts/check_map_md.sh, scripts/sync_map_md.py]
    - id: AT-10
      status: ATTACKED
      evidence: Nine fixture tests, one per clause; reverting any of the mode split, the
        warn-only exit, the diff-filter or the duplicate check reds a named test.
      artifacts: [python/repark-parity/tests/test_map_pr_gate_1.py]
  complete: true
```
