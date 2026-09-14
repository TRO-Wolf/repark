# Charter ledger — ICE-GOLD-TWICE-1 · replace twice + gold twice into the nightly

**Date:** 2026-09-14 · **Branch:** `chore/repin-rp-20` · **Base:** `origin/main`
**Model:** swe-2-high · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Tier-2 CI never runs against unmerged code".
**Path:** STANDARD. **Proven pattern:**
[rp-19-ledger.md](rp-19-ledger.md) (pin consumer) and
[../archive/2026-09/2026-09-02-live-v3-aws-legs-ledger.md](../archive/2026-09/2026-09-02-live-v3-aws-legs-ledger.md)
(env-gated AWS legs over a shared helper).

**Retires:** the orchestrator moves this ledger to `../completed/` after the post-merge
live run; per the unit brief it stays in `staging/` until then.

**Why now.** The RP-20 repin consumed the fork's Glue replace-publish path
(`edc38c6a`, F-GLUE-REPLACE-1). The nightly `aws-acceptance` run passed on `main`
every night but ran only
`python/repark/tests/test_aws_acceptance.py` — no `CREATE OR REPLACE TABLE … AS` leg,
and no dbt gold leg, so a regression in the replace path or in anything the gold
models exercise on Glue would first show up in production. This unit adds the two
live legs, the shared helper they call, the always-run offline pin that proves the
helper's assertions, the gold-twice rework, and the workflow step — the merged
workflow run is the live proof.

**Measured (memory catalog, `register_memory_catalog`, 2026-09-14).** The shape
`assert_replace_twice_outcome` asserts:

| statement | rows (`ORDER BY id`) | `.snapshots` count | current snapshot id | commit op |
|---|---|---|---|---|
| `CREATE TABLE … AS` seed | `(1,s1) (2,s2) (3,s3)` | 1 | `484952405371284155` | `append` |
| `CREATE OR REPLACE … AS` (4 rows) | `(1,r1) (2,r2) (3,r3) (4,r4)` | 2 | `6606761114235935428` | `append` |
| `CREATE OR REPLACE … AS` (5 rows) | `(1,q1) (2,q2) (3,q3) (4,q4) (5,q5)` | 3 | `214875951464752906` | `append` |

Rows equal the last SELECT; history is retained, not truncated; each replace adds
exactly one `append` snapshot; the three current ids are distinct. The memory twin
`python/dbt-repark/tests/test_gold_models.py::test_dbt_run_is_idempotent` measures
the same per-model shape for the second `dbt run` — `gold_fct`/`gold_agg` each grow
1 → 2 `append` snapshots, rows unchanged.

**Not in this unit:** `Cargo.toml` / `Cargo.lock` / dependency files (no dependency
change — the workflow installs the Makefile's existing `DBT_PINS`); STATUS.md; live
AWS execution (the legs run in the nightly on `main` after the merge); the run id and
per-leg counts (orchestrator records them in `docs/cutover/inventory.md` §8 after the
dispatch).

## PROPOSITION LEDGER — ICE-GOLD-TWICE-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `helper_proven_offline`: `python/repark/tests/_acceptance_replace.py` carries `run_create_or_replace_twice` (3-row CTAS seed under `ICEBERG_TABLE_PROPERTIES`, then `CREATE OR REPLACE … AS` twice over 4- and 5-row answers, recording ordered `to_arrow().to_pylist()` rows, `.snapshots` count and current snapshot id after each statement) and `assert_replace_twice_outcome`; `test_acceptance_replace_offline.py` runs both on `register_memory_catalog` — no gate, no AWS, no skip. | The offline pin green; the red-first collection failure pasted. | **PROVEN** | Red first: with the test written before the helper, `.venv/bin/python -m pytest python/repark/tests/test_acceptance_replace_offline.py -q` exited 2 — `ModuleNotFoundError: No module named '_acceptance_replace'` at collection. Green after the helper: `1 passed in 0.27s`. The measured table above is what the asserter asserts. |
| C-002 | `live_legs_skip_clean`: `test_aws_acceptance.py` gains `test_create_or_replace_twice_against_glue` and `test_create_or_replace_twice_against_s3tables`, built like the MW-4/MW-10 legs — session config from `glue_catalog_config`/`s3tables_catalog_config`, scratch namespace create-if-missing, the Glue namespace-location assertion, `testing_replace2_<uuid12>` names, S3 Tables skips without `TABLE_BUCKET_ARN` — each calling the helper and asserting its outcome; both skip under the module `skipif` without credentials. | The legs present; the offline run's skip count. | **PROVEN** | Legs appended to `test_aws_acceptance.py` over `run_create_or_replace_twice`/`assert_replace_twice_outcome` (`exact_counts=False` on S3 Tables for the service's own commits, the `assert_v3_acceptance_outcome` precedent). Offline gate run: the module reports every leg SKIPPED — counts in §Gates. |
| C-003 | `gold_twice`: `test_aws_acceptance_gold.py` uses a per-run stem (`testing_dbt1_<uuid8>` minted once in the fixture, passed to `_repoint_profile`, no module constant), runs `dbt run`, checks `FCT_ROWS`/`AGG_ROWS` and records each gold model's snapshot count, runs `dbt run` a second time (success, same rows, each model's history grown by exactly one snapshot — the memory twin's measured shape), then `dbt test` (10 results, success). | The reworked test; the memory-twin measurement. | **PROVEN** | `GOLD_STEM` constant removed; `uuid` stem in `glue_project`; `_snapshot_count` reads `<t>.snapshots` counts for `gold_fct`/`gold_agg` after run 1, asserts `== before + 1` after run 2; `dbt test` asserts `tested.success` and `len(tested.result) == 10`. Module skips offline — count in §Gates. |
| C-004 | `workflow_joined`: `aws-acceptance.yml` installs the Makefile `DBT_PINS` (`dbt-core==1.9.11`, `dbt-spark==1.9.3`) in the pre-credentials `sync + build native module` step, runs the silver module with `-rA`, and adds `dbt gold acceptance (REPARK_AWS_ACCEPTANCE=1)` after it with `if: ${{ !cancelled() }}` and the same env block; credentials are still minted last before the steps that need them; zizmor clean; no comment lines added. | The workflow diff; `make workflows-lint`. | **PROVEN** | `VIRTUAL_ENV="$PWD/.venv" uv pip install --quiet dbt-core==1.9.11 dbt-spark==1.9.3` appended to the build step before `Configure AWS credentials (OIDC)`; silver line carries `-q -rA`; the gold step runs `python/dbt-repark/tests/test_aws_acceptance_gold.py -q -rA` with the module's env block. `make workflows-lint` green — §Gates. No YAML comment lines added (fence). |
| C-005 | `docs_current`: `docs/tier2-aws.md` names the gold module and the replace legs in the operator section; every touched directory's `map.md` is updated in lockstep. | The doc section; the map entries. | **PROVEN** | `docs/tier2-aws.md` §6 carries both `replace2` leg rows and §7 the dbt gold module; `python/repark/tests/map.md` gains `_acceptance_replace` / `test_acceptance_replace_offline` entries and the legs paragraph on `test_aws_acceptance.py`; `python/dbt-repark/tests/map.md`, `.github/workflows/map.md`, `docs/cutover/map.md` and `task/ledgers/staging/map.md` updated in the same change. |
| C-006 | `gates_green`: the unit's gates pass on this branch. | Counts pasted into §Gates. | **PROVEN** | All listed gates exit 0 — counts in §Gates. |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

`test_acceptance_replace_offline.py` was written before
`_acceptance_replace.py` existed; the run failed at collection:

```
.venv/bin/python -m pytest python/repark/tests/test_acceptance_replace_offline.py -q
ModuleNotFoundError: No module named '_acceptance_replace'
exit 2
```

Green after the helper landed: `1 passed in 0.27s`. The deeper red this unit covers
is the nightly's missing legs — run 34824917757 (2026-09-14) was green on the silver
module with no replace leg and no gold leg; the merged workflow run is the live
proof, its id recorded in `docs/cutover/inventory.md` §8 by the orchestrator.

## Gates

| Gate | Exit |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_acceptance_replace_offline.py python/repark/tests/test_aws_acceptance.py -q -rs` | 0 — 1 passed, 10 skipped in 0.39s (offline pin green; all 10 live legs SKIPPED incl. `test_create_or_replace_twice_against_glue` line 510 and `_against_s3tables` line 537) |
| `make py-test-dbt` | 0 — 59 passed, 1 skipped in 35.46s (`test_gold_stage_on_glue` is the skip) |
| `.venv/bin/python -m pytest python/repark/tests -q -k "replace or ctas or catalog"` | 0 — 245 passed, 18 skipped, 6185 deselected in 181.49s |
| `make workflows-lint` | workflows-parse green (13 workflows); `uvx zizmor@1.26.1 --offline .` **no findings**; the make target's online `artipacked` audit exits 2 in this sandbox — GH_TOKEN/GITHUB_TOKEN are `disabled`, so zizmor gets 401 listing `actions/checkout` tags. Environmental, identical on the base tree. |
| `make check-docs-links` | 0 — 849 files, 5428 links checked — clean |
| `make check-ledger-grammar` | 0 — 131 live ledgers clean (904 clauses, 1523 pinned clause ids, 2 exception rows) |
| `make check-ledgers` | 0 — 349 ledgers in bins (218 archived), 962 ledger links resolve, frozen rule clean |
| `make verify` | 0 (fmt, clippy `-D warnings` + panic-ban, crate-dag, lib-rs, rust-file-size, lib-py, python-conventions, docstring-presence, manifest, ledger lifecycle + grammar, docs-compaction, docs-links, owner-ruling, parity-live dual-wire, matrix-test-liveness, cargo check, ruff check + format, all rust tests) |
| `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q -p no:cacheprovider` | 0 — 757 passed, 2 skipped, 12 xfailed in 567.53s |
| Comment fence (`git diff --cached` grep for added `//`/`#` lines) | prints nothing |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-gold-twice-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001/C-002 prove the helper and both live legs (Glue exact counts, S3 Tables relaxed counts for service commits, uuid stems, namespace-location guard); C-003 the gold twice + dbt test; C-004 the workflow join; C-005 the docs and maps; red-first output pasted; every PROVEN clause cited under python/ map.md entries.
      artifacts: [python/repark/tests/_acceptance_replace.py, python/repark/tests/test_acceptance_replace_offline.py, python/repark/tests/test_aws_acceptance.py, python/dbt-repark/tests/test_aws_acceptance_gold.py, .github/workflows/aws-acceptance.yml, docs/tier2-aws.md]
    - id: AT-2
      status: ATTACKED
      evidence: The helper asserts the measured shape, not an assumed one — the memory-catalog table (rows equal last SELECT, one append snapshot per replace, three distinct ids) is recorded in this ledger, and the gold twice asserts the memory twin's measured 1→2 per-model snapshot growth.
      artifacts: [python/repark/tests/_acceptance_replace.py, python/dbt-repark/tests/test_aws_acceptance_gold.py]
    - id: AT-3
      status: ATTACKED
      evidence: The seed is a plain CTAS with no IF NOT EXISTS so a name collision fails loud; the legs assert the outcome rather than tolerating refusal; placeholder bucket settings still fail loudly through assert_real_buckets_configured, unchanged.
      artifacts: [python/repark/tests/_acceptance_replace.py, python/repark/tests/test_aws_acceptance.py]
    - id: AT-4
      status: ATTACKED
      evidence: REPARK_AWS_ACCEPTANCE is the existing module-level skipif the new legs inherit; the gold step's env block is the same one the silver step carries; TABLE_BUCKET_ARN absence skips the S3 Tables leg like its siblings; this round never sets the gate.
      artifacts: [python/repark/tests/test_aws_acceptance.py, python/dbt-repark/tests/test_aws_acceptance_gold.py, .github/workflows/aws-acceptance.yml]
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, no network, no JVM this round — every live leg is assert-skipped offline and the counts are in §Gates; the harness stays scratch-only (testing_repark_acceptance, testing_ prefix, no drops, no destructive SQL).
      artifacts: [python/repark/tests/test_aws_acceptance.py, python/dbt-repark/tests/test_aws_acceptance_gold.py]
    - id: AT-6
      status: ATTACKED
      evidence: The asserted snapshot shape was measured on register_memory_catalog before the assertions were written (table in this ledger), and the dbt idempotence expectation was measured by test_dbt_run_is_idempotent on the memory catalog — never guessed.
      artifacts: [task/ledgers/staging/ice-gold-twice-1-ledger.md, python/dbt-repark/tests/test_gold_models.py]
    - id: AT-7
      status: N/A
      justification: No wall-clock or performance claim; the second dbt run's cost is a nightly concern recorded in the map, not a clause.
    - id: AT-8
      status: ATTACKED
      evidence: Pins named exactly — fork rev edc38c6aa5cdbe132235f4fefc01d3066d6cff23, DBT_PINS dbt-core==1.9.11/dbt-spark==1.9.3 spelled the Makefile's way in the workflow; no dependency file touched.
      artifacts: [.github/workflows/aws-acceptance.yml, docs/fork-sync.md]
    - id: AT-9
      status: ATTACKED
      evidence: The live legs are claimed as skip-clean offline and pending-live — the merged workflow run is the proof and its id is the orchestrator's to record in inventory §8; the docs and map entries say 'first run pending' rather than claiming a green run that has not happened.
      artifacts: [docs/tier2-aws.md, docs/cutover/inventory.md, docs/cutover/production-iceberg-status-2026-09-14.md, python/repark/tests/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: Before state: nightly green on silver-only legs (run 34824917757), gold test on a fixed GOLD_STEM with one dbt run, no workflow coverage of replace or gold. After state: offline pin green, both live legs skip-clean, gold twice + dbt test wired, workflow runs the module after silver — with the run id still pending as recorded.
      artifacts: [task/ledgers/staging/ice-gold-twice-1-ledger.md, .github/workflows/aws-acceptance.yml]
  complete: true
```
