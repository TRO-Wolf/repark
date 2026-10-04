# ZIZMOR-GATE-1 — plain-mode zizmor blocks, and the map guard passes github.base_ref through env

**Date:** 2026-10-03 · **Filed by:** a Grok session (grok-4.7) · **Source:** owner delegate, 2026-10-03. **Grade:** C, clerk band. **Schedule:** after CI-1 (`ffee8692`, merged).

## Why

CI's zizmor step runs `uvx zizmor@1.26.1 --format sarif . > zizmor.sarif`, and `--format sarif` exits 0 even when findings exist. Measured on 2026-10-03: sarif mode exits 0 with 4 results, while plain mode exits 14 on the same tree. The workflow's own comment says "Findings remain blocking", so the gate has never blocked. Main today carries 3 high `template-injection` findings in `.github/workflows/ci.yml` lines 135–136 (`github.base_ref` expanded in a `run:` script).

Re-run the same day on checkout `ffee869`: sarif mode exits 0 with 3 results, plain mode exits 14, and the plain summary is `25 findings (22 suppressed, 3 unsafe fixes)`. All 3 results are those high `template-injection` findings (two expansions on line 135, one on line 136). The 22 suppressed are not rows in [`.github/zizmor.yml`](../../../.github/zizmor.yml): that file's `rules:` is `{}`. Exit 14 is the status this tree returns for the 3 findings. The gate below is exit 0 after the two edits, not a hunt for 14 items. A finding that remains after those edits is a halt. It is not a new suppression.

`make workflows-lint` already runs plain `uvx zizmor@1.26.1 .` ([Makefile](../../../Makefile) `ZIZMOR`). The CI step is the one that does not.

## Measured

| surface | what was run or read | result |
|---|---|---|
| sarif, the CI step | `uvx zizmor@1.26.1 --format sarif .` | exit 0. Filing count 4 results. Re-run on `ffee869`: 3 results, all `template-injection` |
| plain | `uvx zizmor@1.26.1 .` | exit 14. Summary: `25 findings (22 suppressed, 3 unsafe fixes)`, 3 high |
| finding | [ci.yml](../../../.github/workflows/ci.yml) line 135, first `github.base_ref` | high `template-injection` |
| finding | ci.yml line 135, second `github.base_ref` | high `template-injection` |
| finding | ci.yml line 136, `github.base_ref` | high `template-injection` |
| comment | [zizmor.yml](../../../.github/workflows/zizmor.yml), step `Run zizmor` | "Findings remain blocking" |

## Decisions

Owner delegate, 2026-10-03, verbatim: "(1) ci.yml map.md guard passes github.base_ref via env:, (2) zizmor.yml runs a plain-mode blocking step before the sarif upload (keep if: always() on the upload)."

The shape of (1), and the only edit inside the `map.md guard` step of [ci.yml](../../../.github/workflows/ci.yml). The `${{ }}` expression sits in `env:`. The `run:` block reads the shell variable. No other step in the file changes. Do not run `zizmor --fix`.

```yaml
      - name: map.md guard
        if: github.event_name == 'pull_request'
        env:
          BASE_REF: ${{ github.base_ref }}
        run: |
          git fetch --no-tags origin "+refs/heads/${BASE_REF}:refs/remotes/origin/${BASE_REF}"
          bash scripts/check_map_md.sh --base "origin/${BASE_REF}"
```

The shape of (2), in [zizmor.yml](../../../.github/workflows/zizmor.yml). Plain mode is the blocking step. The SARIF write has `if: always()` so a red plain-mode step still leaves a file. The upload keeps `if: always()`. The action pin stays. [`.github/zizmor.yml`](../../../.github/zizmor.yml) is not edited.

```yaml
      - name: Run zizmor
        # Plain mode exits non-zero when findings exist, so this step is what blocks.
        # Suppressions live in .github/zizmor.yml. Sarif mode exits 0 with findings.
        run: uvx zizmor@1.26.1 .
      - name: Write zizmor SARIF
        # Always write the SARIF, including when the plain-mode step failed.
        if: always()
        run: uvx zizmor@1.26.1 --format sarif . > zizmor.sarif
      - uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a  # v7
        if: always()  # upload the SARIF for triage even when the gate fails
        with:
          name: zizmor-sarif
          path: zizmor.sarif
```

In [`.github/workflows/map.md`](../../../.github/workflows/map.md), add one sentence to the existing `ci.yml` row and one to the existing `zizmor.yml` row. Leave the rest of each row as it is.

- `ci.yml` row: "The map.md guard passes `github.base_ref` in `env: BASE_REF`, and the `run:` script reads `$BASE_REF` (ZIZMOR-GATE-1)."
- `zizmor.yml` row: "The blocking step is plain `uvx zizmor@1.26.1 .`; `--format sarif` is the next step, and the upload keeps `if: always()` (ZIZMOR-GATE-1)."

## Steps

1. Replace the `map.md guard` step with the (1) skeleton. Touch no other step in `ci.yml`.
2. Replace the `Run zizmor` step with the two steps in the (2) skeleton, and leave the upload's `if: always()`. Touch no trigger, permission, or action pin.
3. Add the two sentences to `.github/workflows/map.md`. `git diff -- .github/zizmor.yml` is empty.
4. `python3 scripts/check_workflows_parse.py` exits 0. `uvx zizmor@1.26.1 .` exits 0. `make workflows-lint` exits 0.
5. Commit, TRO-Wolf identity, one `Authored-By:` trailer. Subject: `chore(zizmor-gate-1): plain-mode zizmor blocks, and the map guard takes base_ref from env (owner delegate, 2026-10-03)`. Paths: `ci.yml`, `zizmor.yml`, `.github/workflows/map.md`. No `--no-verify`.
6. Proof. Push the branch and open a draft PR so `pull_request` runs the zizmor workflow (a branch push that is not a PR and not `main` does not). On top of the fix commit, one commit puts the three `${{ github.base_ref }}` expansions back into the `run:` block, in the wording `ffee869` has on lines 135–136. Subject: `wip(zizmor-gate-1): reintroduce template injection (proof, drop)`. Push. Wait until the `zizmor` job's conclusion is `failure` and the log names `template-injection`. Record the run id. A red job for any other reason is a halt. Then drop the proof commit: `git reset --hard <fix-sha>` and `git push --force-with-lease` once. That force-push is the only one, and it only removes the proof commit. The merged history does not contain the injection. Mark the PR ready after the fix commit is the head and the plain-mode run is green.
7. Hand back the proof run id, the plain-mode exit 0, and `git diff -- .github/zizmor.yml` empty.

## Home and gates

**Home:** [`.github/workflows/ci.yml`](../../../.github/workflows/ci.yml) (the `map.md guard` step), [`.github/workflows/zizmor.yml`](../../../.github/workflows/zizmor.yml) (plain mode, then the SARIF write, then the upload), [`.github/workflows/map.md`](../../../.github/workflows/map.md) (one sentence on each of those two rows). [`.github/zizmor.yml`](../../../.github/zizmor.yml) stays empty of suppressions: `rules:` stays `{}`.

| gate | green |
|---|---|
| plain `uvx zizmor@1.26.1 .` | exits 0 on the branch |
| the proof commit's `zizmor` job | conclusion `failure`, log names `template-injection`; the run id is in the hand-back; the commit is then dropped |
| `.github/zizmor.yml` | `rules:` is `{}`, the diff is empty, no suppressions |
| `python3 scripts/check_workflows_parse.py` | exits 0 |
| `make workflows-lint` | exits 0 |

A plain-mode exit other than 0 after steps 1–3 is a halt. Hand back the finding. Do not add a suppression, and do not edit a workflow the two decisions do not name.

## Pointers

- Up: [map.md](map.md)
- CI-1, whose smoke split this card follows: [ci-1-wheel-smoke-time.md](../../wo/ci-1-wheel-smoke-time.md)
- The runs: [ci.yml](../../../.github/workflows/ci.yml), [zizmor.yml](../../../.github/workflows/zizmor.yml), [zizmor config](../../../.github/zizmor.yml), [workflow map](../../../.github/workflows/map.md)
