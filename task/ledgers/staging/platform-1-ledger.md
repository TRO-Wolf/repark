# Unit ledger — PLATFORM-1 step 1 · the abi3 wheel matrix (nightly + release tag)

**Unit:** PLATFORM-1 step 1 · **Date:** 2026-09-12 · **Branch:** `ci/platform-1-wheel-matrix` · **Base:** `origin/main`
**Model:** swe-2-high
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** One manylinux x86_64 wheel is the whole distribution today; nothing in the
product is platform-specific (S2-29) and the fork's CI already proves the Rust builds on
all three operating systems. The wheel is `cp312-abi3`, so it is one file per platform.
Step 1 (the whole card) wires the five-leg set: `wheels.yml` gains a `platform-matrix`
job that runs the four legs per-PR CI never sees on a nightly cron plus
`workflow_dispatch`, and `release.yml`'s `build-wheel` becomes the five-leg tag matrix.
Per-PR CI is deliberately untouched so tonight's merges are not slowed by macOS/Windows
runners.

**Not in this step:** STATUS.md, `briefs/next-sequence.md`, any dependency or source
file, the zig cross-build fallback (decision: maturin-action `manylinux: auto` on
`ubuntu-24.04-arm`, which runs the aarch64 manylinux container natively — the
orchestrator's post-merge `workflow_dispatch` is the proof and falls back to zig only if
that run shows maturin-action cannot target the runner), musllinux, wheel signing, the
`wheels.yml` `release-wheels` job (pre-existing tag-path x86_64 build; left as is —
widening it is a separate decision), and any trigger of the new schedule (nothing is
dispatched from this clone).

## PROPOSITION LEDGER — PLATFORM-1 step 1 — 2026-09-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `wheels.yml` `platform-matrix` runs exactly the four new legs — `manylinux-aarch64`/`ubuntu-24.04-arm`, `macos-arm64`/`macos-latest`, `macos-x86_64`/`macos-15-intel`, `windows-x86_64`/`windows-latest` — gated `github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'` behind a nightly cron (06:43 UTC) plus dispatch; each leg builds `--release`, installs into a fresh venv, `import repark`, one `ReparkSession` collect, uploads `wheels-<leg>`. | `test_nightly_matrix_names_the_four_new_legs_off_pull_request` in `python/repark-parity/tests/test_platform_1_wheel_matrix.py` | **PROVEN** | `6 passed in 0.02s` (`.venv/bin/python -m pytest python/repark-parity/tests/test_platform_1_wheel_matrix.py -q`, 2026-09-12). The `if:` is pinned byte-exact; the four leg→runner pairs are pinned in order. |
| C-002 | Per-PR CI is unchanged: the `pull_request:` trigger line and the `smoke` job (`if`, `runs-on`, every step) carry zero diff, and `platform-matrix` cannot fire on a pull request. | `test_pull_request_smoke_job_keeps_its_gate_and_host` + `test_doctored_nightly_matrix_fails` (the `pull_request` reachability arm) + the job diff | **PROVEN** | `git diff origin/main -- .github/workflows/wheels.yml` adds `schedule:`/`workflow_dispatch:` under `on:` and the `platform-matrix` job; no line inside `smoke` or `release-wheels` changed. The smoke-job pin asserts `if: github.event_name == 'pull_request' || github.ref == 'refs/heads/main'` and `runs-on: ubuntu-latest`; the doctored `if: github.event_name == 'pull_request'` fails. |
| C-003 | `release.yml` `build-wheel` is the five-leg matrix — the four nightly legs plus `manylinux-x86_64`/`ubuntu-latest` — with the tag/version consistency gate and the release-checklist smoke (`import repark.sql` must fail) per leg, artifacts `release-wheel-<leg>`, and `publish-pypi` downloading `pattern: release-wheel-*` with `merge-multiple: true` into `dist/`. | `test_release_matrix_names_exactly_the_five_legs` | **PROVEN** | Green in the same `6 passed` run. The pin asserts the ordered leg list, each leg's `runs-on`, and the merge download; the `import repark.sql` refusal line is carried verbatim into the per-leg smoke. |
| C-004 | The pin is red-first and mutation-proven, and `make workflows-lint` passes. | `test_doctored_release_matrix_fails`, `test_doctored_nightly_matrix_fails`, `make workflows-lint` | **PROVEN** | Red first below (3 failed on the base tree). Doctorings — dropped leg, renamed leg, appended sixth leg, re-hosted leg, `merge-multiple` removed, cron trigger removed, `pull_request` reachability — each fail (sampled output below). `make workflows-lint`: `workflows-parse: 13 workflows parse cleanly`; zizmor `No findings to report` (see Gates for the sandbox network caveat). |
| C-005 | `docs/release.md` lists the five wheels. | `test_release_doc_lists_the_five_wheels` | **PROVEN** | "Settled at the first tags" names `manylinux-x86_64`, `manylinux-aarch64`, `macos-arm64`, `macos-x86_64`, `windows-x86_64` with their runners; the open item narrows to musllinux. Pin asserts all five leg tokens present. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

The pin file was written before either workflow or the doc was touched. Run against the
base tree:

```text
FAILED python/repark-parity/tests/test_platform_1_wheel_matrix.py::test_release_matrix_names_exactly_the_five_legs
FAILED python/repark-parity/tests/test_platform_1_wheel_matrix.py::test_nightly_matrix_names_the_four_new_legs_off_pull_request
FAILED python/repark-parity/tests/test_platform_1_wheel_matrix.py::test_release_doc_lists_the_five_wheels
3 failed, 3 passed in 0.05s
```

(`test_release_matrix…` — `no matrix include block`; `test_nightly…` — `no nightly
schedule cron trigger`, `no workflow_dispatch trigger`, `no platform-matrix job`;
`test_release_doc…` — `assert 'manylinux-x86_64' in doc`. The three mutation tests read
green on the broken base because `findings != []` is already true; they become the live
proof now that the shape exists.)

Mutation proof, run live after the matrix landed (doctored copies, in memory):

```text
doctored (macos-x86_64 leg removed): ["release.yml build-wheel: legs ['manylinux-x86_64', 'manylinux-aarch64', 'macos-arm64', 'windows-x86_64'] != expected ['manylinux-x86_64', 'manylinux-aarch64', 'macos-arm64', 'macos-x86_64', 'windows-x86_64']"]
doctored (aarch64 leg moved to x86 runner): ["release.yml build-wheel: leg manylinux-aarch64 runs-on 'ubuntu-latest' != 'ubuntu-24.04-arm'"]
```

## Gates

- `make workflows-parse` — `13 workflows parse cleanly` (in `workflows-lint`).
- `make workflows-lint` — zizmor cannot reach the GitHub API from this clone (401 on the
  `artipacked` ref lookup, an environment limit, not a finding). Re-run with the pinned
  tool in offline mode — `make workflows-lint ZIZMOR="uvx zizmor@1.26.1 --offline"` —
  audits all 13 workflows: `No findings to report. Good job! (19 suppressed)`. CI runs
  the online audit on the PR.
- `.venv/bin/python -m pytest python/repark-parity/tests/test_platform_1_wheel_matrix.py -q`
  — `6 passed in 0.02s`.
- `make check-docs-links`, `make check-ledger-grammar`, `make check-manifest`,
  `make verify` — see the commit gate record in handback.json.

## COVERAGE_ATTESTATION

```yaml
COVERAGE_ATTESTATION:
  pr_unit: platform-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: walked C-001..C-005 against the workflow diff and the pin file
      artifacts: [python/repark-parity/tests/test_platform_1_wheel_matrix.py]
    - id: AT-2
      status: ATTACKED
      evidence: doctored leg lists — dropped, renamed, appended, re-hosted, unmerged download, cron removed, pull_request reachability — each red
      artifacts: [python/repark-parity/tests/test_platform_1_wheel_matrix.py]
    - id: AT-3
      status: N/A
      justification: no retries or partial state; fail-fast: false keeps legs independent and if-no-files-found: error fails loud
    - id: AT-4
      status: N/A
      justification: matrix legs are isolated runners; the only shared surface is the artifact namespace, kept distinct per leg
    - id: AT-5
      status: ATTACKED
      evidence: SHA-pinned actions, persist-credentials: false, sccache off on every leg, explicit-path wheel install, zizmor clean
      artifacts: [.github/workflows/wheels.yml, .github/workflows/release.yml]
    - id: AT-6
      status: N/A
      justification: no data surface; the tag/version consistency gate is kept per leg
    - id: AT-7
      status: N/A
      justification: the nightly schedule keeps macOS/Windows minutes off the PR path; no hot path touched
    - id: AT-8
      status: ATTACKED
      evidence: maturin-action v1.14.1 pin and the download-artifact pattern/merge contract are asserted by the pin
      artifacts: [python/repark-parity/tests/test_platform_1_wheel_matrix.py]
    - id: AT-9
      status: N/A
      justification: per-leg artifacts plus the matrix run UI are the diagnosis surface; the orchestrator's dispatch records the run
    - id: AT-10
      status: ATTACKED
      evidence: five-clause pin file, red-first on the base tree, mutation arms inside the suite
      artifacts: [python/repark-parity/tests/test_platform_1_wheel_matrix.py]
  reattested: []
  complete: true
```

**Errata (orchestrator, 2026-09-12 22:50):** the first `workflow_dispatch` of the matrix (run 34733285566) proved manylinux-aarch64 on `ubuntu-24.04-arm`, macos-arm64 and windows-x86_64 green; the macos-x86_64 leg was cancelled with zero steps because the `macos-15-intel` runner label is retired. The leg now runs on `macos-15-intel`; the pin names the new label; the second dispatch is the proof.

**Errata 2 (orchestrator, 2026-09-13 00:20):** the second dispatch (run 34736741165) had all four legs running — ten steps each, `macos-15-intel` included — when a push to `main` cancelled it: `wheels.yml` keyed its concurrency group on workflow + ref only, so any merge cancels a scheduled or dispatched matrix on `main` (which is also what cancelled the first run's macOS x86_64 leg, not the runner label). The group now includes `github.event_name`; the pin asserts it; the third dispatch is the proof.
