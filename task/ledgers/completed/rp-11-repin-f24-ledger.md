# Charter ledger — RP-11 · fork repin 594bdbe5 → 189a73ed (consume F-24; close B-MOR-3-FLOOR-1)

**Date:** 2026-09-04 · **Branch:** `feat/rp-11-repin-f24` · **Base:** `origin/main`
`434cbac` · **Model:** grok-4.6 · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract".
**Path:** STANDARD. **Proven pattern:**
[../completed/rp-9-repin-f23-ledger.md](rp-9-repin-f23-ledger.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** B-MOR-3 filed `B-MOR-3-FLOOR-1`: the v3 parquet-to-DV arm converted below
Spark's `min-input-files=5`. Fork F-24 (`189a73ed86c9bd29888fbd545f7957df8df25f18`,
PR `#266`) honours that floor; `rewrite-all=true` bypasses it on both arms.

**Not in this unit:** any dependency change beyond the one `[patch.crates-io]` rev;
wiring CALL `options` (the CALL refuses `options` today); RP-10's `PERF-DVCLOSE-STMT-1`
pins.

## PROPOSITION LEDGER — RP-11 — 2026-09-04

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `189a73ed86c9bd29888fbd545f7957df8df25f18` and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main`; the pin-history row names F-24. | `make bump-fork-pin`; `grep` the five revs; family freeze. | **PROVEN** | Five revs one line `189a73ed86c9bd29888fbd545f7957df8df25f18`; six lock sources. Family freeze holds (`datafusion` 54.1.0, `datafusion-spark` 54.1.0, `parquet` 58.4, toolchain untouched). Cargo.toml/lock vs `origin/main`: 11 insertions, 11 deletions, iceberg revs only. Citation: `docs/fork-sync.md`. |
| C-002 | The B-MOR-3 below-floor pins flip to Spark's four zeros with parquet deletes live; the 5-file cell is unchanged; CALL `options` stays refused (no `rewrite-all` cell); one live co-collected below-floor leg. Mutation: pin the old conversion answer → red. | Named tests; mutation row; live leg. | **PROVEN** | Bare-repin 3 red of 3 (`rewritten` 0 vs 2/1/1). After flip: 3 floor pins green, B5 green, options-refuse green. Mutation `rewritten==2` on B2: **1 red of 1**. CALL `options` still refused. Citation: `crates/repark-spark/src/tests/call_v3_dv.rs`. |
| C-003 | Registry `B-MOR-3-FLOOR-1` FIXED 2026-09-04 (RP-11); B-MOR-3 residue line; STATUS v3 workstream one line under 25,000 B; every touched `map.md` in lockstep. | Registry; STATUS; maps; gates. | **PROVEN** | Registry FIXED block; STATUS 21265 B; V1-GATE meta-pin reads `189a73ed`. Citation: `docs/spark-sql-iceberg-parity.md`. |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-11-repin-f24
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Below-floor B2/C/D pins assert Spark's four zeros and live parquet; B5 conversion unchanged; live co-collected B2 leg.
      artifacts: [crates/repark-spark/src/tests/call_v3_dv.rs, python/repark/tests/test_parity_live.py]
    - id: AT-2
      status: ATTACKED
      evidence: 2-file, mixed parquet+DV, partition-scoped-2; 5-file control; second-run zeros on B5.
      artifacts: [crates/repark-spark/src/tests/call_v3_dv.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Below-floor no-op is four zeros, not a silent convert; CALL options still refuse loud.
      artifacts: [crates/repark-spark/src/call.rs, crates/repark-spark/src/tests/call.rs]
    - id: AT-4
      status: N/A
      justification: No new shared state or concurrency; the fork planner change is consumed as a pin bump.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM or secret handling. No .github change. The one dependency change is the single [patch.crates-io] rev and its lock rows.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-6
      status: ATTACKED
      evidence: Public CALL schema unchanged. options/where still refuse. No rewrite-all wiring.
      artifacts: [crates/repark-spark/src/call.rs]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim; the load-bearing pin is the four-zero floor, not a timing cell.
    - id: AT-8
      status: ATTACKED
      evidence: Five iceberg* revs and six lock sources are 189a73ed. Family freeze holds. Bare-repin 3 red of 3.
      artifacts: [Cargo.toml, Cargo.lock, docs/fork-sync.md]
    - id: AT-9
      status: ATTACKED
      evidence: Registry B-MOR-3-FLOOR-1 FIXED 2026-09-04 (RP-11); B-MOR-3 residue line; STATUS v3 one line; V1-GATE meta-pin moved.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark-parity/tests/test_v1_gate_docs.py]
    - id: AT-10
      status: ATTACKED
      evidence: Bare-repin 3 red of 3; mutation rewritten==2 on B2 1 red of 1; B5 still 5.
      artifacts: [crates/repark-spark/src/tests/call_v3_dv.rs]
  complete: true
```

## 2. Cells before / after the pin

| Cell | Spark | Fork before (F-23) | Fork now (F-24) |
|---|---|---|---|
| 5 file-scoped deletes (B5) | 5 → 5 Puffin DVs, second run zeros | same | same |
| 2 file-scoped (B2) | four zeros, parquet stays | converted | four zeros, parquet stays |
| mixed leftover parquet (C) | four zeros, parquet stays | converted | four zeros, parquet stays |
| partition-scoped-2 (D) | four zeros, parquet stays | converted | four zeros, parquet stays |
| `rewrite-all=true` | bypasses the floor | — | CALL refuses `options`; not wired |

## 3. CALL options

| Probe | Result |
|---|---|
| `args.has_named("options")` in `execute_rewrite_position_delete_files` | refuses: "options map is not supported in v1" |
| pin `call_rewrite_position_delete_files_refuses_options_and_where` | green |
| `rewrite-all` bypass cell | not added |

## 4. Mutation table

| Mutation | N red / M |
|---|---|
| Bare repin, floor pins still assert conversion | 3 red / 3 |
| After flip, assert B2 `rewritten_delete_files_count == 2` | 1 red / 1 |

One knob at a time; mutation reverted; floor pins re-run green.

## 5. Pins (C-002)

| Pin | Observable |
|---|---|
| `call_rewrite_position_delete_files_zeros_two_upgraded_parquet_deletes_below_spark_floor` | 0,0,0,0; 2 PARQUET stay; ids `[101, 102]` |
| `call_rewrite_position_delete_files_zeros_mixed_remaining_parquet_below_spark_floor` | 0,0,0,0; 1 PARQUET + 1 PUFFIN stay |
| `call_rewrite_position_delete_files_zeros_partition_parquet_below_spark_floor` | 0,0,0,0; 1 PARQUET rc=2 stays; ids `[2, 4]` |
| `call_rewrite_position_delete_files_converts_five_upgraded_parquet_deletes_to_puffin` | unchanged 5 → 5 PUFFIN |
| `test_live_rewrite_position_delete_files_below_floor_matches_spark` | live co-collected B2 |

## 6. Docs (C-003)

| File | Change |
|---|---|
| `docs/spark-sql-iceberg-parity.md` | `B-MOR-3-FLOOR-1` FIXED 2026-09-04 (RP-11); B-MOR-3 residue line |
| `STATUS.md` | v3 workstream: floor FIXED; F-24 drops from Next (21265 B) |
| `docs/fork-sync.md` | pin-history row F-24 |
| `task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md` | consumed pin `189a73ed`; surface residual FIXED |

## 9. Delivery template

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: rp-11-repin-f24
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: rp-11-repin-f24
  artifacts_verified:
    ledger: PASS (C-001..C-003 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (none filed)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (see §10)
  status_update: B-MOR-3-FLOOR-1 FIXED 2026-09-04 (RP-11)
  verdict: PENDING
  rejection_route: N/A
```

## 10. Gates

| Gate | Exit |
|---|---|
| `make develop` | 0 (`repark.__file__` = `/tmp/oc-rp11/python/repark/src/repark/__init__.py`) |
| bare-repin floor pins | 101 (3 red of 3) |
| floor pins after flip | 0 (3 passed) |
| B5 conversion pin | 0 |
| options-refuse pin | 0 |
| mutation B2 rewritten==2 | 101 (1 red of 1) |
| `make verify` | 0 |
| `.venv/bin/python -m pytest python/repark/tests -q --deselect python/repark/tests/test_pyspark_compat_smoke.py -k "not test_cross_validator_live_pyspark_shape"` | 0 (4450 passed, 177 skipped, 1 deselected) |
| `.venv/bin/python -m pytest python/repark-parity/tests -q` | 0 (555 passed) |
| `REPARK_PARITY_LIVE=1 … -k "rewrite_position or disclosure"` | 0 (16 passed, 98 deselected; B2 + B5 + disclosure co-collected) |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |
| `cargo deny check` | 0 |
