# CI-1 — the wheel smoke check in a quarter of the time · grade C · clerk band · tidy window, no release dependency

## 0. Why, and what is out

The required check "build + import smoke (debug, host)" in `.github/workflows/wheels.yml` takes
45 to 50 minutes on every PR, and a docs-only PR pays it twice when the depth-guard flake
strikes. The step timings of the last green run (2026-10-02 11:16Z) say where the time is: the
debug wheel builds in 7 minutes, the import smoke takes 11 seconds, the facade suite takes 38
minutes on four workers, dbt one minute, example coverage two and a half. CI-1 makes four
changes that cut the wall time without weakening what the check proves: a docs-only
short-circuit, one build fanned out to three test shards, the existing `ci` cargo profile with an
optimisation level chosen by measurement, and a proof run of the flaky test on the new wheel.
**Out:** paid runners, a release wheel on PRs, any change to what the suite asserts, any edit to
`crates/` (the flake's constants are a design decision, halt H-4), any change to `ci.yml`'s jobs.

## 1. Rulings already made

- Owner, 2026-10-02: the four items are one order; none may drop a test, an extra, or a check the job runs today.
- The required-check name **does not change**: the aggregate job keeps the key `smoke` and the name `build + import smoke (debug, host)`, so branch protection is untouched.
- Docs-only means: every changed path is a `.md` file, or lies under `docs/` or `task/` with a data extension (`json`, `html`, `txt`, `csv`, `svg`, `png`). Anything else, a `.py` under `docs/perf/` included, runs the full check. When the base commit cannot be resolved the answer is "code changed".
- The wheel cache is read on PRs and **written only on `main`** (`save-if`), so a PR branch can never seed the cache the next PR restores. Keys follow the existing pair: prefix `v2-df54-wheel`, shared key `wheel`, both in `wheels.yml` and `cache-warm.yml`.
- Every action stays SHA-pinned with a trailing version comment; `persist-credentials: false` on every checkout; no `pull_request_target`; `permissions: contents: read` unchanged. zizmor stays blocking with zero suppressions.
- The optimisation level of `[profile.ci]` is chosen by one rule: the lower sum of the build step and the slowest shard across the two measured runs wins; a tie picks `1`. Debug assertions and overflow checks stay on because the profile inherits `dev`.
- Three shards by file name modulo three, in sorted order, with `pytest-xdist -n 4` inside each. No new Python dependency.
- The depth-guard flake is a debug stack-frame margin (U11-EDGE-1 ledger, round 9). CI-1 proves whether the chosen profile removes it; it does not touch `deep_stack.rs` or any constant.
- No code comments from Anthropic models. The workflow files carry comments today; CI-1 adds none and deletes the ones on lines it removes.

## 2. Files

| action | path |
|---|---|
| edited | `.github/workflows/wheels.yml` — the `smoke` job becomes four jobs: `changes`, `build`, `facade` (matrix of three), and the aggregate `smoke` with the unchanged name; `release-wheels` and `abi3` jobs untouched |
| edited | `.github/workflows/cache-warm.yml` — one new job `warm-wheel` after `warm-test` |
| edited | `Cargo.toml` — `[profile.ci]` gains one line `opt-level = N` (N from step 6) |
| edited | `.github/workflows/map.md` — the `wheels.yml` row names the four-job shape, the docs-only rule, the cache keys and the `ci` profile; the `cache-warm.yml` row names the third cache |
| edited | `task/ledgers/staging/u11-edge-1-ledger.md` — one row "Round 10 (CI-1)" with the proof-run result of step 8 |
| map.md | no `.rs` / `.py` / `.toml` under a mapped directory changes except the root `Cargo.toml`, whose row in the root `map.md` does not list profiles; `scripts/check_map_md.sh` decides |

## 3. Skeletons

### 3.1 `wheels.yml`, the smoke section in full (replaces the current `smoke` job; the header comment and `on:`, `permissions:`, `concurrency:` stay)

```yaml
jobs:
  changes:
    name: code changed
    if: github.event_name == 'pull_request' || github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    outputs:
      code: ${{ steps.diff.outputs.code }}
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1  # v7.0.1
        with:
          persist-credentials: false
          fetch-depth: 0
      - id: diff
        env:
          BASE: ${{ github.event.pull_request.base.sha || github.event.before }}
        run: |
          if [ -z "$BASE" ] || ! git cat-file -e "$BASE" 2>/dev/null; then
            echo "code=true" >> "$GITHUB_OUTPUT"; exit 0
          fi
          CODE=$(git diff --name-only "$BASE" "$GITHUB_SHA" \
            | grep -vE '\.md$' \
            | grep -vE '^(docs|task)/.*\.(json|html|txt|csv|svg|png)$' \
            || true)
          if [ -n "$CODE" ]; then echo "code=true" >> "$GITHUB_OUTPUT"; else echo "code=false" >> "$GITHUB_OUTPUT"; fi

  build:
    name: build debug wheel (profile ci)
    needs: changes
    if: needs.changes.outputs.code == 'true'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1  # v7.0.1
        with:
          persist-credentials: false
      - uses: actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97  # v7.0.0
        with:
          python-version: "3.12"
      - uses: dtolnay/rust-toolchain@6c977a6ca4077a0ceb28ffbe03f59d46e9ac8772 # master 2026-07-11
        with:
          toolchain: "1.96.0"
      - uses: Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6  # v2.9.2 (node24)
        with:
          prefix-key: "v2-df54-wheel"
          shared-key: "wheel"
          save-if: ${{ github.ref == 'refs/heads/main' }}
      - uses: PyO3/maturin-action@e83996d129638aa358a18fbd1dfb82f0b0fb5d3b  # v1
        with:
          maturin-version: "v1.14.1"
          command: build
          args: --out dist --profile ci
          working-directory: python/repark
          container: "off"
      - name: import smoke test
        run: |
          python3 -m venv /tmp/wheeltest
          /tmp/wheeltest/bin/pip install --upgrade pip
          WHEEL=$(ls python/repark/dist/repark-*.whl)
          /tmp/wheeltest/bin/pip install "$WHEEL"
          /tmp/wheeltest/bin/python -c "import repark; print('repark', repark.__version__)"
      - uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a  # v7
        with:
          name: wheel-ci
          path: python/repark/dist/repark-*.whl
          retention-days: 1
          if-no-files-found: error

  facade:
    name: facade shard ${{ matrix.shard }}
    needs: build
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        shard: [0, 1, 2]
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1  # v7.0.1
        with:
          persist-credentials: false
      - uses: actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97  # v7.0.0
        with:
          python-version: "3.12"
      - uses: actions/download-artifact@37930b1c2abaa49bbe596cd826c3c89aef350131  # v7
        with:
          name: wheel-ci
          path: python/repark/dist
      - name: facade tests against the built wheel
        env:
          SHARD: ${{ matrix.shard }}
        run: |
          python3 -m venv /tmp/wheeltest
          /tmp/wheeltest/bin/pip install --upgrade pip
          WHEEL=$(ls "$PWD"/python/repark/dist/repark-*.whl)
          /tmp/wheeltest/bin/pip install "repark[numpy,pandas,polars,ml-ext] @ file://$WHEEL" pytest pytest-xdist ./python/repark-parity
          FILES=$(ls python/repark/tests/test_*.py | sort | awk -v s="$SHARD" 'NR % 3 == s')
          /tmp/wheeltest/bin/python -m pytest $FILES -q -n 4
      - name: dbt-adapter tests against the built wheel
        if: matrix.shard == 0
        run: |
          /tmp/wheeltest/bin/pip install "dbt-core==1.9.11" "dbt-spark==1.9.3"
          /tmp/wheeltest/bin/python -m pytest python/dbt-repark/tests -q
      - name: example-coverage execute (require native)
        if: matrix.shard == 0
        run: /tmp/wheeltest/bin/python -I scripts/check_example_coverage.py --require-execute

  smoke:
    name: build + import smoke (debug, host)
    needs: [changes, build, facade]
    if: always() && (github.event_name == 'pull_request' || github.ref == 'refs/heads/main')
    runs-on: ubuntu-latest
    steps:
      - env:
          CODE: ${{ needs.changes.outputs.code }}
          BUILD: ${{ needs.build.result }}
          FACADE: ${{ needs.facade.result }}
        run: |
          if [ "$CODE" = "false" ]; then echo "docs-only change, the wheel is unchanged"; exit 0; fi
          [ "$BUILD" = "success" ] && [ "$FACADE" = "success" ]
```

The `changes` filter, read as a rule: a path counts as code unless it ends in `.md`, or it lives
under `docs/` or `task/` and ends in one of the six data extensions. A `.py`, `.sh`, `.toml` or
`.yml` under `docs/` or `task/` therefore counts as code, which is the conservative side. Step 3
checks the two `grep` lines against three fixed inputs; if any disagrees, the lines are wrong,
not the rule.

### 3.2 `cache-warm.yml`, the new job (after `warm-test`, same `env:` block as `warm-test`)

```yaml
  warm-wheel:
    name: Warm wheel cache
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1  # v7.0.1
        with:
          persist-credentials: false
      - uses: actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97  # v7.0.0
        with:
          python-version: "3.12"
      - uses: dtolnay/rust-toolchain@6c977a6ca4077a0ceb28ffbe03f59d46e9ac8772 # master 2026-07-11
        with:
          toolchain: "1.96.0"
      - uses: Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6  # v2.9.2 (node24)
        with:
          prefix-key: "v2-df54-wheel"
          shared-key: "wheel"
      - uses: PyO3/maturin-action@e83996d129638aa358a18fbd1dfb82f0b0fb5d3b  # v1
        with:
          maturin-version: "v1.14.1"
          command: build
          args: --out dist --profile ci
          working-directory: python/repark
          container: "off"
```

### 3.3 `Cargo.toml`

```toml
[profile.ci]
inherits = "dev"
incremental = false
debug = false
opt-level = 1
```

(`1` is the placeholder; step 6 may change it to `2`.)

## 4. Steps

1. Record the baseline from the last green run on `main`: `gh run list --workflow wheels.yml --branch main --status success --limit 1 --json databaseId --jq '.[0].databaseId'`, then `gh run view <id> --log | grep -E '[0-9]+ passed'`. Write down the facade line (on 2026-10-02 it read `14451 passed, 489 skipped, 147 xfailed`) and the dbt line (`64 passed, 1 skipped`).
2. Edit `wheels.yml` as in §3.1, `cache-warm.yml` as in §3.2, `Cargo.toml` as in §3.3.
3. Check the docs-only rule against three inputs, each as a one-line shell test of the same three `grep` lines with `printf` in place of `git diff`: `task/wo/x.md` alone must yield `code=false`; `docs/perf/x/run.py` alone must yield `code=true`; `python/repark/tests/x_oracle.json` alone must yield `code=true`.
4. `python3 scripts/check_workflows_parse.py && make workflows-lint && uvx taplo@0.9.3 format --check` (the same taplo pin as `.github/workflows/taplo.yml`).
5. Commit `wip(ci-1): the four-job smoke, opt-level 1 (measure)`, push, open the PR against `main` as a draft. Wait for the check named `build + import smoke (debug, host)` to finish. Read the timings: `gh run view <run-id> --json jobs --jq '.jobs[] | "\(.name)\t\(.startedAt)\t\(.completedAt)\t\(.conclusion)"'`. Record the `build debug wheel (profile ci)` duration and the slowest `facade shard` duration.
6. Change `opt-level = 1` to `opt-level = 2`, commit `wip(ci-1): opt-level 2 (measure)`, push, wait, record the same two durations. Apply the ruling: the lower sum wins, a tie picks `1`. If the loser is what is on the branch, set the winner and commit `chore(ci-1): opt-level N by measurement`.
7. Sum the three shards' `passed` counts from the winning run's logs (`gh run view <run-id> --log | grep -E '[0-9]+ passed'`). The sum of `passed`, of `skipped` and of `xfailed` must each equal the baseline line from step 1. Shard 0's dbt line must equal the baseline dbt line.
8. Proof run of the flake, on the box, inside the slice, one cargo thing at a time: `CARGO_BUILD_JOBS=6 .venv/bin/maturin build --profile ci --out /tmp/ci1-dist -m crates/repark-python/Cargo.toml` (run from `python/repark` if the manifest path is relative there), `python3 -m venv /tmp/ci1-venv && /tmp/ci1-venv/bin/pip install /tmp/ci1-dist/repark-*.whl pytest pytest-xdist ./python/repark-parity`, then `for i in $(seq 30); do /tmp/ci1-venv/bin/python -m pytest python/repark/tests/test_ice_views_1.py -k test_nested_view_depth_guard -q -p no:cacheprovider || echo CRASH $i; done 2>&1 | grep -cE 'CRASH'` must print `0`.
9. Add the "Round 10 (CI-1)" row to `task/ledgers/staging/u11-edge-1-ledger.md`: the profile chosen, the 30-of-30 result, the run id. Update the two rows in `.github/workflows/map.md`.
10. `scripts/check_map_md.sh --base origin/main && make check-docs-links && make check-map-sync && python3 scripts/check_docs_compaction.py`.
11. Squash the wip commits into one: `chore(ci-1): the wheel smoke as changes → build → three shards → aggregate; docs-only short-circuit; profile ci at opt-level N; cache on main (owner, 2026-10-02)` plus the `Authored-By:` trailer, with the TRO-Wolf identity. Force-push the branch (the only force-push of the unit), mark the PR ready. The PR body carries the before/after table from steps 1, 5, 6 and 7.

## 5. Gates and the line that means green

| command | green |
|---|---|
| `python3 scripts/check_workflows_parse.py` | exits 0, no file named |
| `make workflows-lint` | zizmor reports no findings |
| the `changes` job on the final run | `code=true` for this PR (it edits `.yml` and `Cargo.toml`) |
| the check `build + import smoke (debug, host)` | success, and its wall time from `changes` start to `smoke` end is under 25 minutes |
| step 7 sums | `passed`, `skipped`, `xfailed` each equal the step 1 baseline; dbt line equal |
| step 8 proof | `0` |
| `scripts/check_map_md.sh --base origin/main` | `map-md: … clean` |
| `make check-docs-links` | exits 0 |
| CI's other required checks | all green; `Rust test (workspace)` and `Rust lint` are not touched by this change and must stay so |

## 6. Halt rules

- **H-1** the step 7 sums differ from the baseline by any amount: stop, hand back both lines and the shard logs. Do not re-run to "see if it changes" and do not adjust the shard rule.
- **H-2** zizmor reports a finding: stop, hand back the rule id and line. Do not add a suppression to `.github/zizmor.yml`.
- **H-3** the rust-cache save on `main` reports a size above 4 GB in its log, or the `build` job on the second run reports a cache miss after the first `main` push: hand back the sizes. Do not change the keys of the `lint` or `test` caches.
- **H-4** step 8 prints a number other than `0`: the chosen profile does not remove the flake. Hand back the count and the crash signature. Do not edit `crates/repark-python/src/deep_stack.rs`, any constant, or the test. The orchestrator opens a design unit.
- **H-5** the wall time of the final run is 25 minutes or more: hand back the per-job table. Do not add a fourth shard or raise `-n`.
- **H-6** ninety minutes of lane time without the step 5 run started: commit what exists as `wip(ci-1): …`, hand back.

## 7. Hand-back

```json
{"unit":"CI-1","baseline":{"passed":0,"skipped":0,"xfailed":0,"dbt_passed":0},"opt_level":{"1":{"build_s":0,"slowest_shard_s":0},"2":{"build_s":0,"slowest_shard_s":0},"chosen":0},"final_wall_s":0,"sums_equal":false,"flake_crashes_of_30":0,"cache_save_gb":0.0,"pr":0,"commit":"<sha>","halt":null}
```
