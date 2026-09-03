# Charter ledger — B-MOR-3 · rewrite_position_delete_files on format-v3

**Date:** 2026-09-03 · **Branch:** `feat/b-mor-3-rewrite-position-deletes-v3` · **Base:** `main`
`e3ad67e` · **Model:** grok-4.6 → glm-5.3-flash (continuation) · **Path:** STANDARD
(`risk_tier: standard`).
**Policy:** [../../../AGENTS.md](../../../AGENTS.md) · **Registry:**
[docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) `B-MOR-3`,
`B-MOR-3-FLOOR-1`.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Owner ruling 2026-09-03: BUILD the PROC-1 addendum. OD-2 of record stays
orphan-files.

**Not in this unit:** Cargo pin change; fork patch; `.github/`; AWS.

## PROPOSITION LEDGER — B-MOR-3 — 2026-09-03

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Spark cells A–E recorded on live PySpark 4.1.2 + Iceberg 1.11.0 before any product edit. | §2 table. | **PROVEN** | §2 tables are the values of record (scratch dump `/tmp/oc-bmor3-oracle/b-mor-3-spark-cells.json`). Citation: `python/repark/tests/test_parity_live.py`. |
| C-002 | Refusal deleted; fork action runs on v3. Engine A/B5/E match Spark. B2/C/D convert below Spark min-input-files=5 — not patched in RePark; filed as `B-MOR-3-FLOOR-1` / F-24. | Engine dump vs Spark dump; floor pins. | **PROVEN** | §3 table is the comparison of record (scratch dump `/tmp/oc-bmor3-oracle/b-mor-3-engine-cells.json`); the 12 in-process pins re-prove it on the final tree. Citation: `crates/repark-spark/src/call.rs`, `crates/repark-spark/src/tests/call_v3_dv.rs`. |
| C-003 | Five refusal pins retire to Spark-compared zeros. New B5 conversion pin + live co-collected leg. Floor pins for B2/C/D. Always-run tests repark-only. | Named tests. | **PROVEN** | §3 pin table. Citation: `crates/repark-spark/src/tests/call_register.rs`, `call_v3_dv.rs`, `v3e3.rs`, `python/repark/tests/test_v3_dv_compaction.py`, `test_v3e3_fixtures.py`, `test_parity_live.py`. |
| C-004 | Registry `B-MOR-3` FIXED; `B-MOR-3-FLOOR-1` filed; north star row 13 / one-line ruling; STATUS drops `B-MOR-3` from Next; maps lockstep. | Doc gates. | **PROVEN** | §8. Citation: `docs/spark-sql-iceberg-parity.md`, `crates/repark-spark/src/map.md`. |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: b-mor-3-rewrite-position-deletes-v3
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: DV-only zeros and admitted parquet-to-DV pinned on Spark door and facade; live Spark comparison on cell B5.
      artifacts: [crates/repark-spark/src/tests/call_v3_dv.rs, python/repark/tests/test_v3_dv_compaction.py, python/repark/tests/test_parity_live.py]
    - id: AT-2
      status: ATTACKED
      evidence: Second-run zeros; 2-file / mixed / partition-scoped floor cells; v2 8-to-1 control unchanged.
      artifacts: [crates/repark-spark/src/tests/call_v3_dv.rs, crates/repark-spark/src/tests/call.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Fork divergence below min-input-files is a dated row, not a RePark planner patch.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-4
      status: N/A
      justification: No new concurrency.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, no .github, no Cargo pin, no secrets.
      artifacts: [crates/repark-spark/src/call.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Public CALL result schema unchanged; count_live_deletion_vectors deleted from the lib; tests use iceberg::live_deletion_vectors_by_data_file.
      artifacts: [crates/repark-spark/src/call.rs, crates/repark-spark/src/tests/common.rs]
    - id: AT-7
      status: ATTACKED
      evidence: V3-COV golden flipped ERROR to OK [0,0]; SCALE-v3 sequence no longer captures a refusal.
      artifacts: [python/repark/tests/_v3_statement_coverage_repark.py, python/repark-parity/bench/mw7/measure.py]
    - id: AT-8
      status: ATTACKED
      evidence: Always-run pins are repark-only; live Spark is skip-gated in test_parity_live.py.
      artifacts: [python/repark/tests/test_parity_live.py]
    - id: AT-9
      status: ATTACKED
      evidence: Mutations one knob at a time recorded in §4.
      artifacts: [crates/repark-spark/src/tests/call_v3_dv.rs]
    - id: AT-10
      status: ATTACKED
      evidence: STATUS compacted; maps lockstep; ledger grammar citations in crate/python maps.
      artifacts: [STATUS.md, crates/repark-spark/src/map.md, python/repark/tests/map.md]
  complete: true
```

## 2. Spark cells (C-001) — 2026-09-03, PySpark 4.1.2 + Iceberg 1.11.0

| Cell | Fixture | Spark four counts | Delete files after | Rows | Second run |
|---|---|---|---|---|---|
| A DV-only | v3, three MoR DELETE → 3 PUFFIN | 0,0,0,0 | 3 PUFFIN stay | [2,4,6] | 0,0,0,0 |
| B specified | v2 two file-scoped parquet, upgrade | 0,0,0,0 | 2 PARQUET stay (below floor 5) | [2,4] | 0,0,0,0 |
| B5 | v2 five file-scoped parquet, upgrade | 5,5,7351,210 | 5 PUFFIN rc=1 referenced | [101..105] | 0,0,0,0; next-row-id 0→10 |
| C mixed | B + MoR DELETE after upgrade | 0,0,0,0 | 1 PARQUET + 1 PUFFIN rc=2 | [4] | 0,0,0,0 |
| D partition | v2 one parquet covering two files, upgrade | 0,0,0,0 | 1 PARQUET rc=2 stays | [2,4] | 0,0,0,0 |
| D5 | five partition parquet, upgrade | 5,5,7390,210 | 5 PUFFIN rc=1 | — | — |
| E v2 control | eight parquet, partition granularity | 8,1,11787,1478 | 1 PARQUET rc=8 | [1..8] | 0,0,0,0 |

Re-measured 2026-09-03 (continuation session) cells A, B, C, D, E into the same dump after the
file held only B5/B8/D5: every count reproduced; byte counts vary run to run, so only the four
counts and the file census are pinned.

## 3. Engine vs Spark (C-002)

| Cell | Spark | Engine | Verdict |
|---|---|---|---|
| A | 0,0,0,0; 3 PUFFIN | 0,0,0,0; 3 PUFFIN | MATCH — land |
| B5 | 5,5; 5 PUFFIN | 5,5; 5 PUFFIN | MATCH — land |
| E | 8,1; 1 PARQUET | 8,1; 1 PARQUET | MATCH — land |
| B (2 files) | 0,0,0,0; 2 PARQUET | 2,2; 2 PUFFIN | DIVERGE — `B-MOR-3-FLOOR-1` |
| C | 0,0,0,0; mixed | 1,1; 2 PUFFIN | DIVERGE — `B-MOR-3-FLOOR-1` |
| D | 0,0,0,0; 1 PARQUET | 1,2; 2 PUFFIN | DIVERGE — `B-MOR-3-FLOOR-1` |

Proposed fork unit **F-24**: honor `MIN_INPUT_FILES_DEFAULT = 5` on the v3 parquet-to-DV arm.

## 4. Pins (C-003)

| Old | New |
|---|---|
| `call_rewrite_position_delete_files_refuses_spark_written_puffin_vectors` | `…_on_spark_written_puffin_vectors_returns_zeros` (37 rows, 3 DVs) |
| `call_rewrite_position_delete_files_still_refuses_engine_written_v3_dvs` | `…_on_engine_written_v3_dvs_returns_zeros` (6 DVs) |
| `partitioned_v3_dv_rewrite_position_delete_files_still_refuses` | `…_returns_zeros` |
| `partitioned_v3_dv_fork_rewrite_position_delete_files_measurement` | unchanged zeros |
| facade refuse in `test_v3_dv_compaction.py` / `test_v3e3_fixtures.py` | zeros |
| — | B5 conversion + live `test_live_rewrite_position_delete_files_upgraded_parquet_matches_spark` |
| — | B2 / C / D floor pins |

## 5. Mutation table

| Mutation | N red / M |
|---|---|
| Restore the live-DV refusal | Spark door 5 red / 12 CALL pins; facade 8 red (5 failed + 3 fixture errors) / 113 |
| Assert B5 `rewritten_delete_files_count == 0` | B5 Spark-door pin 1 red / 10 |
| Assert B2 four zeros | B2 floor pin 1 red / 3 floor pins |
| Assert C mixed parquet remains | C floor pin 1 red / 1 |
| Assert D still one PARQUET | D floor pin 1 red / 1 |

One knob at a time; every mutation reverted and the touched scope re-run green after each.

## 6. Retired comments

| Site | What left |
|---|---|
| `call.rs` refusal + `count_live_deletion_vectors` | fork `RewritePositionDeleteFiles::execute` |

## 7. Docs (C-004)

| File | Change |
|---|---|
| `docs/spark-sql-iceberg-parity.md` | `B-MOR-3` FIXED; `B-MOR-3-FLOOR-1` filed |
| north star §3 / §3.1 row 13 / owner paragraph | ruling BUILD |
| `STATUS.md` | drop `B-MOR-3` from Next |
| `docs/design/format-v3-track.md`, `v3-statement-coverage.md` | 72 EQUAL / 8 DIVERGES |
| `docs/guide/iceberg-guide.md` | v3 first CALL is zeros, not a raise |
| handoff F-7 addendum | acceptance pin renamed, landed |

## 8. Gates

| Gate | Exit |
|---|---|
| `make develop` (clean tree, after each Rust change) | 0 |
| `make verify` | 0 |
| `cargo test -p repark-spark --lib -- rewrite_position_delete` | 0 (12 passed) |
| `.venv/bin/python -m pytest python/repark/tests/test_v3_dv_compaction.py python/repark/tests/test_v3e3_fixtures.py -q` | 0 (7 passed) |
| `.venv/bin/python -m pytest test_v3_statement_coverage.py test_v1_gate_docs.py test_v3_cov_docs.py test_cap_1_source_file_line_cap.py test_proc_1_tiered_review.py -q` | 0 (148 passed, 81 skipped) |
| `.venv/bin/python -m pytest test_mw7_scale_smoke.py test_v3_live_oracle.py -q` | 0 (22 passed, 8 skipped) |
| `REPARK_PARITY_LIVE=1 … pytest python/repark/tests/test_parity_live.py -q -k "rewrite_position or disclosure"` | 0 (15 passed) |
| `make check-map-sync` / `check-ledger-grammar` / `check-ledgers` / `check-docs-compaction` | 0 (§8 close) |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 (§8 close) |
| `typos .` / `ruff check python` / `ruff format --check python` | 0 (§8 close) |

## 9. Delivery template

```yaml
DELIVERY_SIGNOFF:
  pr_unit: b-mor-3-rewrite-position-deletes-v3
  artifacts_verified:
    ledger: PASS (C-001..C-004 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10)
    findings_ledger: PASS (none open)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS
  status_update: B-MOR-3 FIXED; B-MOR-3-FLOOR-1 filed
  verdict: ACCEPTED
  rejection_route: N/A
SHIPPED_FLAG_REGISTER:
  pr_unit: b-mor-3-rewrite-position-deletes-v3
  flags: []
  count: 0
```
