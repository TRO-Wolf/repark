# v1.5.6 qualification record (Master Plan Step 1.4)

**Status:** prepared 2026-11-08 on branch `release/v1.5.6-prep` by the release-prep clerk (`claude-haiku-5-5`). This record tags nothing, publishes nothing and triggers no workflow. Section 4's conclusions were filled by the orchestrator (`claude-opus-5-5`) from the completed runs. The owner delegated the merge, the tag and the deployment approval (section 9).

## 1. Source revision

| Item | Value |
|---|---|
| Base | `origin/main` `03f925c5103faf8329c6e5eb5b0223bf7143c1e3` (the empty-DML guard, PR #1277, merge commit `03f925c51`) |
| Release-prep branch head | the head of PR `release/v1.5.6-prep` at review time (the reviewed sha) |
| Version | 1.5.6 (`[workspace.package] version` in `Cargo.toml`) |
| Previous tag | `v1.5.5` at `7fb5f6b2` (PyPI 1.5.5) |

## 2. Dependency lock

| File | sha256 at base `03f925c5` | sha256 on the release branch | Delta |
|---|---|---|---|
| `Cargo.lock` | `d3defbf391c1d195d52eb150adb6dca14603a2a4da386d3d967a85ae019ca590` | `adaa813f8fcc85c88fe2c9eecb32ddbab463bd65d803f78ed34d4add61c451f0` | the 11 `repark-*` workspace member `version` lines, `1.5.5` to `1.5.6` (11 removed, 11 added); nothing else |
| `uv.lock` | `d84e3889ac6fd119a692948e7db3124e97cbe8d3a031481353fd47559ab535ea` | unchanged | none |

## 3. Platform and profile

- Host: Linux 6.8.0-149-generic, x86_64, 64 threads. Local gates ran inside `repark.slice` (CPUQuota 26 cores).
- Rust 1.96.0 (`rust-version = "1.96"`); every cargo call uses `--locked`.
- Facade profile: `FACADE_PROFILE ?= ci` (Makefile).
- `make preflight` was not run locally. The owner's 2026-09-18 rule forbids a whole-workspace suite on the local box; CI runs it on the release PR.
- The release wheels are built by `release.yml` at the tag, not by this record.

## 4. CI on the merged-main commit

Runs on `03f925c5` (PR #1277 merged), read with `gh run view <id>` after both completed on 2026-11-08.

| Run id | Workflow | Conclusion |
|---|---|---|
| 42511828130 | CI | success; every job succeeded: `Python`, `Repo guards (map.md + workflow parse + crate-DAG + lib.rs)`, `Rust lint run (fmt + clippy + check)`, `Rust lint (fmt + clippy + check)`, `Rust test (cluster feature)`, `Rust test run (workspace)`, `Rust test (workspace)`, `code changed (ci)` |
| 42511828152 | smoke | success; every job succeeded: `code changed`, `build debug wheel (profile ci)`, `build + import smoke (debug, host)`, `facade shard 0` to `facade shard 6`, `dbt-adapter tests`, `example-coverage execute` |
| 42511828149 | typos | success |
| 42511828197 | wheels | skipped (push to main) |
| 42511828201 | Cache warm | success |

The previous main head, `bd9c5437` (PR #1272, docs only), was green on the same jobs: CI run 42501593565 and smoke run 42501593548 succeeded, and typos succeeded.

Executed suites, by CI job name, on `03f925c5`: `Python`, `Repo guards (map.md + workflow parse + crate-DAG + lib.rs)`, `Rust lint run (fmt + clippy + check)`, `Rust test (cluster feature)`, `Rust test run (workspace)`, `build debug wheel (profile ci)` (smoke). All succeeded.

## 5. Guard verifier verdicts (EMPTY-DML-SHARED-GUARD-1, both PASS)

- **Opus 5.5 verifier verdict** (`claude-opus-5-5`, reviewed head `a188225c4`, merge base `bd9c54375`): PASS. Every standing claim held, and no finding reached S1. Five S3 findings, none blocking: F1, plain `EXPLAIN` of an empty DML renders the rewritten plan; F2, `UPDATE ... SET prim_col = 1 / 0 WHERE false` on format v2 merge-on-read, which Spark refuses with `CAST_OVERFLOW_IN_TABLE_INSERT` and RePark answers with zero rows (a declared divergence under ruling D, as extended by the owner's 2026-11-08 ruling "All recommendations"); F3, the residue cards, ledger and registry rows were not on the reviewed head (landing in the records PR, see section 8); F4, a module docstring in the new Python test; F5, the Rust table test reads v3 lineage on the Spark door only.
- **Muse MAX verifier verdict** (same unit, PR #1277): PASS. No S0, S1 or S2 finding. One S3 advisory: the facade affected-row column shape depends on the route (`[{'count': 0}]` on a guarded cell, `[{}]` on a provider-path write), and the row count is preserved everywhere.

## 6. Gates on the release branch

Exit codes are the real codes from the run on the release branch, on the tree that this record describes.

| Gate | Command | Exit |
|---|---|---|
| Docs compaction | `make check-docs-compaction` | 0 |
| Docs links | `make check-docs-links` | 0 |
| Manifest | `make check-manifest` | 0 |
| Map size | `make check-map-size` | 0 |
| Spell check | `make spell-check` | 0 |
| Parity harness | `make py-test` | 0 |

## 7. Known weak checkers (Step 2.1)

- Hadoop-corpus checker: it can pass an engine error when the expected text lacks the Spark-version phrase. Master Plan Step 2.1 repairs it.
- Struct-domain CARD/ORACLE fall-through, on the parked branch only (PR #1267, paused at `8e929978`; not on main).

## 8. Residues and divergences carried into 1.5.6

- Declared divergences under ruling D, as extended by the owner's 2026-11-08 ruling "All recommendations" to Spark's errors raised from the `SET` expression of a no-op DML: [the corpus rule](../../../docs/corpus/deletes-rule-2026-11-03.md), section "Special values (2026-11-08, EMPTY-DML-SHARED-GUARD-1)". The `UPDATE ... SET prim_col = 1 / 0 WHERE false` cell on format-v2 merge-on-read (verdict F2 above) is one of them.
- Empty snapshot: a facade `DELETE ... WHERE id IS NULL` on a required column commits one empty snapshot, through the metadata-delete door that runs before planning. Rows and data files are unchanged, and Spark commits no snapshot. The release notes carry it as a known issue and the lead names it. The verifiers pinned it as a residue (`METADATA_DELETE_RESIDUE` on the wheel, `RESIDUES` in the Rust table test).
- Residue cards, ledger and registry rows (verdict F3): accepted as a deferral. They land in the separate records PR `docs/empty-dml-shared-guard-1-records`, which is in flight, not in this PR.
- Position-delete file: the native door's `DELETE ... WHERE true` on format-v2 merge-on-read writes a position-delete file. Rows match Spark. It is a non-empty control, outside the guard, and was already present.
- Plain `EXPLAIN` text of an empty DML now shows the rewritten plan (verdict F1); the release notes carry it as a behaviour line.

## 9. Owner actions before the tag

The runbook (`.agents/skills/publish-pypi/SKILL.md` and `docs/release.md`) names no merged-main cloud acceptance or live-Spark run before the tag, so none is listed. The scheduled `aws-acceptance` nightly is not triggered by this record.

The owner delegated items 1 to 4 to the orchestrator on 2026-11-08, verbatim: "Tag it and publish it when ready". The orchestrator merges after the section 4 conclusions are green, checks the tree, tags and approves the deployment; the go/no-go stays the owner's.

1. Merge word for the release PR (given, above). Squash-merge on green required checks.
2. Tree equality (runbook step 2.1): `git rev-parse <reviewed-sha>^{tree}` must equal `gh api repos/TRO-Wolf/repark/commits/<squash-sha> --jq .commit.tree.sha`.
3. Tag (runbook step 2.2): `git tag -a v1.5.6 -m "RePark v1.5.6" <squash-sha>`, then `git push origin v1.5.6`.
4. Approve the `release` deployment when `release.yml` pauses.
5. Registry check (runbook step 4.1): `https://pypi.org/pypi/repark/json` reports 1.5.6, and the wheel filename is `repark-1.5.6-cp312-abi3-manylinux...`.
6. Close-out: run the compact-context-docs ritual so STATUS.md and the release-state documents show the shipped tag.
