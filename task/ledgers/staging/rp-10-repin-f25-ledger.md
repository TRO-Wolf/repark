# Charter ledger — RP-10 · fork repin 594bdbe5 → 85a4aaf0 (consume F-25; close PERF-DVCLOSE-STMT-1)

**Date:** 2026-09-04 · **Branch:** `feat/rp-10-repin-f25` · **Base:** `origin/main`
`e6ebd40` · **Model:** grok-4.6 · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract".
**Path:** STANDARD. **Proven pattern:**
[rp-9-repin-f23-ledger.md](../completed/rp-9-repin-f23-ledger.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** RP-9 r2 filed `PERF-DVCLOSE-STMT-1`: after the F-23 close skip, a 192-manifest
pure-DV `DELETE` still opened every data manifest once at commit in
`validate_fresh_dvs_only`. Fork F-25 (`85a4aaf0cda9ea643bfe34c1666228178e363e94`, PR `#265`)
walks newest-first and stops once every `added_dvs` key is found.

**Not in this unit:** any dependency change beyond the one `[patch.crates-io]` rev; a
Spark-visible design choice not measured (HALT). Scan 3× (`PERF-SCAN-3PASS-1`) is recorded,
not fixed.

## PROPOSITION LEDGER — RP-10 — 2026-09-04

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `85a4aaf0cda9ea643bfe34c1666228178e363e94` and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main`; the pin-history row names F-25; the bare repin blast radius is tabled. | `make bump-fork-pin`; `grep` the five revs; compile at the untouched source. | **PROVEN** | Five revs and six lock sources are `85a4aaf0`. Family freeze holds (`datafusion` 54.1.0, `arrow`/`parquet` 58.4.0, toolchain 1.96.0). Bare-repin `cargo check --locked --workspace` exit 0. Citation: `docs/fork-sync.md`. |
| C-002 | On the RP-9 identity-path DELETE fixture (192 data manifests, pure DV, complete map), the COMMIT phase opens exactly 1 data manifest for the newest touched file; close phase 0; scan 3×N unchanged. Mutation: pin the old count → red. | The hide pin; mutation. | **PROVEN** | `a_newest_file_identity_delete_commits_with_one_data_manifest` green at `85a4aaf0` (191 of 192 data manifests hidden). Red-first on F-23 **1 red of 1**. Mutation (assert `is_err` / old 192-walk) **1 red of 1**. `hiding_the_newest_data_manifest_too_refuses_the_commit` green (needs that 1). `execute_predicate_dml_deletes_the_newest_id_on_a_192_manifest_table` green. Close-phase 0 is the RP-9 hide pin. Scan stays 3×N (`PERF-SCAN-3PASS-1`). Citation: `crates/repark-iceberg/src/write/merge/tests/map.md`. |
| C-003 | The RP-9 wall table re-run (8 / 48 / 192; three runs; medians) before/after the repin; opens per phase after. No wall-clock CI pin. | The `#[ignore]`d cell; the before/after table. | **PROVEN** | `measure_pure_dv_close_cost`, this clone, debug, `DELETE WHERE id = 0` (oldest file). §8. Citation: `crates/repark-spark/src/tests/map.md`. |
| C-004 | The record says what landed: registry `PERF-DVCLOSE-STMT-1` FIXED 2026-09-04 (RP-10) with the table; `PERF-SCAN-3PASS-1` updated if phase numbers moved; STATUS v3 workstream one line (25,000 B); every touched `map.md` in lockstep. | The registry row; STATUS; the maps; the gates. | **PROVEN** | Registry FIXED with the newest-file 1-open pin. Scan still 3×N. STATUS 24976 B. V1-GATE meta-pin reads `85a4aaf0`. Citation: `python/repark-parity/tests/map.md`. |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-10-repin-f25
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Newest-file hide pin requires the commit to succeed with 191 of 192 data manifests hidden; hiding the last one refuses. execute_predicate_dml deletes the newest id on the 192-fixture.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/dv_commit_opens.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Newest-file early-exit (1 open) and oldest-file full walk (F-25 documented). Close-phase 0 is the RP-9 hide pin. Scan 3×N recorded, not collapsed.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/dv_commit_opens.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Hiding the one remaining newest manifest refuses the commit. Red-first on F-23 was a missing-file error, not a silent skip.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/dv_commit_opens.rs]
    - id: AT-4
      status: N/A
      justification: No new shared state or concurrency; F-25's buffer-1 walk is fork-side inside the commit future.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM or secret handling. No .github change. The one dependency change is the single [patch.crates-io] rev and its lock rows.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-6
      status: ATTACKED
      evidence: No public API break. Bare-repin compiles. Family freeze holds.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-7
      status: ATTACKED
      evidence: RP-9 wall re-run at 8/48/192 before and after, three runs, medians in §8. The load-bearing pin is the one-manifest hide, not a wall-clock CI pin. id=0 is the oldest file and is not a claimed wall win.
      artifacts: [crates/repark-spark/src/tests/v3_legacy_delete.rs]
    - id: AT-8
      status: ATTACKED
      evidence: Five iceberg* revs and six lock sources are 85a4aaf0. Family freeze holds. Bare-repin compiled.
      artifacts: [Cargo.toml, Cargo.lock, docs/fork-sync.md]
    - id: AT-9
      status: ATTACKED
      evidence: Registry PERF-DVCLOSE-STMT-1 FIXED with the table; PERF-SCAN-3PASS-1 still 3×N; V1-GATE meta-pin moved with the pin.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark-parity/tests/test_v1_gate_docs.py]
    - id: AT-10
      status: ATTACKED
      evidence: Four clauses pinned; red-first 1 of 1; mutation 1 of 1; hide-all refuses.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/dv_commit_opens.rs]
  complete: true
```

## 6. What changed under us (C-001)

Range `594bdbe5f257455d77ac49f1a2d50794a1aea6fd..85a4aaf0cda9ea643bfe34c1666228178e363e94`.

| Fork change | Change | Engine site that absorbs it |
|---|---|---|
| `validate_fresh_dvs_only` data-manifest walk | newest-first; stops once every `added_dvs` key is found (buffer 1 until the first manifest is consumed); full walk only for an oldest-manifest or never-found key | no call-site change; production identity DELETE already supplies `added_dvs` |

Public API breaks absorbed: **zero**. Bare-repin compiles.

## 7. Pins (C-002)

| Pin | Observable |
|---|---|
| `a_newest_file_identity_delete_commits_with_one_data_manifest` | hide 191 of 192 data manifests, newest id, complete map → commit succeeds |
| `hiding_the_newest_data_manifest_too_refuses_the_commit` | hide all 192 → refuse |
| `execute_predicate_dml_deletes_the_newest_id_on_a_192_manifest_table` | production identity DELETE of id=191 leaves 191 rows |

Red-first (F-23): **1 red of 1** (`Failed to read file …-m0.avro`). Mutation (assert `is_err`): **1 red of 1**.

## 8. The walk, before and after (C-003)

`measure_pure_dv_close_cost`, this clone, debug, three runs a side. Fixture: N append commits at `commit.manifest-merge.enabled = false`, 0 legacy deletes, one `DELETE WHERE id = 0` (oldest file). "Before" is pin `594bdbe5`; "after" is this tree at `85a4aaf0`.

| Data manifests | Before (F-23) | After (F-25) | Median before | Median after |
|---|---|---|---|---|
| 8 | 489.5 / 101.8 / 106.0 ms | 102.9 / 957.6 / 101.2 ms | 106 ms | 103 ms |
| 48 | 414.6 / 1608 / 417.0 ms | 627.1 / 1454 / 430.8 ms | 417 ms | 627 ms |
| 192 | 2.364 / 1.603 / 1.539 s | 2.255 / 1.569 / 1.732 s | 1.603 s | 1.732 s |

id=0 is the oldest file: F-25 still full-walks at commit, so the statement wall stays inside run-to-run noise and is not a claimed win. The load-bearing close is the newest-file hide pin (commit opens = 1). Fork close-only (debug, launch line): commit opens 8/48/192 → 1/1/1; commit wall at 192 manifests 726 ms → 44 ms.

Opens per data manifest per phase, 192-fixture identity DELETE:

| phase | id=0 (oldest, RP-9 cell) | id=191 (newest, C-002 pin) |
|---|---|---|
| scan | **3×** (`PERF-SCAN-3PASS-1`, unchanged) | **3×** |
| close | **0×** | **0×** |
| commit | **192×** (oldest-manifest full walk) | **1×** (`PERF-DVCLOSE-STMT-1` FIXED) |

## 9. Delivery template

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: rp-10-repin-f25
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: rp-10-repin-f25
  artifacts_verified:
    ledger: PASS (C-001..C-004 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (none filed)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (see §10)
  status_update: PASS (STATUS v3 workstream names RP-10 / PERF-DVCLOSE-STMT-1)
  verdict: PENDING
  rejection_route: N/A
```

Duplicate-row guard (R5): `grep -oE '^- \[[^]]+\]' task/ledgers/completed/map.md | sort | uniq -d` must be empty.

## 10. Gates

| Gate | Exit |
|---|---|
| bare-repin `cargo check --locked --workspace` | 0 |
| F-23 hide pin (red-first) | 101 (1 red of 1) |
| `dv_commit_opens` after C-002 | 0 (3 passed) |
| mutation assert `is_err` | 1 red of 1 |
| `make verify` | 0 |
| `make develop` | 0 |
| `.venv/bin/python -m pytest python/repark/tests -q -x --deselect python/repark/tests/test_pyspark_compat_smoke.py` | 0 at 4403 passed, 180 skipped after skipping live Spark oracles that hit SemLock / SparkContext-stop on this box (not a pin outcome) |
| `.venv/bin/python -m pytest python/repark-parity/tests -q` | 0 (555 passed) |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |
| `cargo deny check` | 0 |
