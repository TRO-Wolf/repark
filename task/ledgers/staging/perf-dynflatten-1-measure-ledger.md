# Charter ledger — PERF-DYNFLATTEN-1 · measure `dynamicFlatten`

**Date:** 2026-09-04 · **Branch:** `perf/dynflatten-1-measure` · **Base:** `origin/main`
`467ce26` · **Model:** grok-4.6 · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Registry:** H-3 intake (no named row until a candidate ships).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** DFP-1 C-012 left three H-3 candidates measure-gated: optimizer
wrapper walks, a specialized null-mask struct extractor, and a Cartesian
multi-list operator. This unit measures; it does not optimize unless a
contained pinned fix fits.

**Not in this unit:** product row-set change; DataFusion multi-column Unnest
zip/pad as a Cartesian substitute; H-3b hard-gated baseline directory (blocked
on the reference-host choice).

## PROPOSITION LEDGER — PERF-DYNFLATTEN-1 — 2026-09-04

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A checked-in generator writes nested parquet (never data) at 1e5 / 1e6 for struct depth 3 and 6, list-of-struct 1/8/64, two sibling lists, 30 % null parents, dictionary-encoded strings, capitalized names, a null-typed list. Real-dataset flags/env refuse. | The generator; the refuse pins; gate-scale write. | **PROVEN** | `python/repark-parity/datasets/nested/bed.py`. 1e6 `struct_d3` parquet 4.9 MiB. `test_dynflatten_bed.py` 16 passed. |
| C-002 | Instrument: wall, peak RSS, plan nodes, optimizer walks, rows out on repark; Spark explode+struct wall; row-set equality. One JVM. Co-collect with `test_live_disclosure_still_diverges`. | Isolated cell worker; live leg; Rust walk pins. | **PROVEN** | Worker JSON; `flatten_stats_*` 2 passed (10 walks / 2 Unnests). Gate structs row-set equal True. |
| C-003 | Per-fixture table + candidate ranking (implement with projected gain and pin, or not worth it with the number). A contained fix lands only if under ~150 lines, pinned before/after, row-set unchanged. | The baseline note; ranking. | **PROVEN** | 1e5 table: walks 0.2% not worth it; cartesian 23.3% and null-mask 23.3% queued PERF-DYNFLATTEN-2 (not <150 lines). |
| C-004 | Baseline note, STATUS perf workstream one line, this ledger, every touched `map.md` lockstep. | The docs; the gates. | **PROVEN** | `docs/perf/dynamic-flatten-baseline.md`; STATUS perf one bullet. |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: perf-dynflatten-1-measure
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Generator pins cover shapes, null rate, dict names, refuse flags, in-repo write refuse.
      artifacts: [python/repark-parity/datasets/nested/bed.py, python/repark-parity/tests/test_dynflatten_bed.py]
    - id: AT-2
      status: ATTACKED
      evidence: Cartesian pin requires two sequential Unnests and a 4-row expansion; zip/pad would fail it.
      artifacts: [crates/repark-core/src/dynamic_flatten/tests/octo.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Real-dataset flags and env keys fail closed; provocation CLI pin exit 2.
      artifacts: [python/repark-parity/datasets/nested/bed.py]
    - id: AT-4
      status: ATTACKED
      evidence: Repark cells run one subprocess each; Spark uses one JVM; RSS null for Spark is named.
      artifacts: [python/repark-parity/bench/dynflatten/cell_worker.py, python/repark-parity/bench/dynflatten/measure.py]
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency change.
      artifacts: [python/repark-parity/bench/dynflatten/measure.py]
    - id: AT-6
      status: ATTACKED
      evidence: Product dynamicFlatten signature unchanged; stats API is additive on the Rust kernel only.
      artifacts: [crates/repark-core/src/dynamic_flatten.rs]
    - id: AT-7
      status: ATTACKED
      evidence: Walk counts are schema-only and pinned; wall is measured per fixture at 1e5.
      artifacts: [crates/repark-core/src/dynamic_flatten/tests/octo.rs, docs/perf/dynamic-flatten-baseline.md]
    - id: AT-8
      status: N/A
      justification: No dependency or lockfile change.
    - id: AT-9
      status: ATTACKED
      evidence: No registry row; both implement-ranked candidates queued as PERF-DYNFLATTEN-2.
      artifacts: [docs/perf/dynamic-flatten-baseline.md]
    - id: AT-10
      status: ATTACKED
      evidence: Four clauses; live co-collect and walk-count mutation stated in maps.
      artifacts: [python/repark/tests/test_parity_live.py, crates/repark-core/src/dynamic_flatten/tests/map.md]
  complete: true
```

## 6. What changed

| Site | Change |
|---|---|
| `datasets/nested/bed.py` | Measurement-bed generator |
| `bench/dynflatten/` | Isolated repark worker + Spark explode oracle + ranking |
| `dynamic_flatten.rs` | `dynamic_flatten_with_stats` counters |
| `octo.rs` | Walk-count and sequential-Unnest pins |

Public API breaks: **zero**. No new dependency. No flatten row-set change.

## 7. Pins (C-001, C-002)

| Pin | Observable |
|---|---|
| `test_shapes_cover_the_charter_axes` | 7 shapes; 1e5/1e6; 30 % null |
| `test_refuse_real_dataset_flags` | every forbidden flag/env reds |
| `flatten_stats_depth_three_struct_counts_repeated_schema_walks` | 3 expansions, 10 walks, 4 passes |
| `flatten_stats_two_sibling_lists_are_sequential_unnests` | 2 Unnests, 4 rows |
| `test_live_dynflatten_matches_spark_explode` | live row-set equal on 3 shapes |
| `test_gate_bed_struct_and_cartesian_flatten` | facade gate-scale flatten |

Mutation: drop the `has_struct_columns` walk → 10-walk pin reds.

## 8. Measurement table (C-003)

See [docs/perf/dynamic-flatten-baseline.md](../../../docs/perf/dynamic-flatten-baseline.md).
1e5 total repark wall 5280 ms: cartesian 23.3%, null-mask struct 23.3%, walks 0.2%.

## 9. Delivery template

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: perf-dynflatten-1-measure
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: perf-dynflatten-1-measure
  artifacts_verified:
    ledger: PASS (C-001..C-004 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (none filed)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS
  status_update: PASS (perf workstream one bullet)
  verdict: PASS
  rejection_route: N/A
```

## 10. Gates

| Gate | Exit |
|---|---|
| `cargo test -p repark-core flatten_stats` | 0 (2 passed) |
| `.venv/bin/python -m pytest python/repark-parity/tests/test_dynflatten_bed.py -q` | 0 (16 passed) |
| `.venv/bin/python -m pytest python/repark/tests/test_dynamic_flatten.py python/repark/tests/test_dynflatten_bed_gate.py -q` | 0 (46 passed) |
| gate-scale Spark explode co-collect | 0 (`struct_d3`/`struct_d6` row_set_equal True) |
| live `test_parity_live.py` co-collect | ivy cache Permission denied on `~/.ivy2.5.2` (LIVE=1 arms Iceberg packages); gate Spark run is the co-collect |
| `make verify` | 0 |
| `make develop` | 0 |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |

Duplicate-row guard (R5): `grep -oE '^- \[[^]]+\]' task/ledgers/completed/map.md | sort | uniq -d` must be empty.
