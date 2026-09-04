# Charter ledger — PERF-DYNFLATTEN-1 · measure `dynamicFlatten`

**Date:** 2026-09-04 · **Branch:** `perf/dynflatten-1-measure` · **Base:** `origin/main`
`467ce26` · **Model:** grok-4.6 (round 1); opus-5 (round 2) · **Policy:**
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
| C-001 | A checked-in generator writes nested parquet (never data) at 1e5 / 1e6 for struct depth 3 and 6, list-of-struct 1/8/64, two sibling lists, 30 % null parents, dictionary-encoded strings, capitalized names, a null-typed list. Real-dataset flags/env refuse. | The generator; the refuse pins; gate-scale write. | **PROVEN** | `python/repark-parity/datasets/nested/bed.py`. 1e6 `struct_d3` parquet 4.9 MiB. `test_dynflatten_bed.py` 15 passed. |
| C-002 | Instrument: wall, peak RSS, plan nodes, optimizer walks, rows out on repark; Spark explode+struct wall; row-set equality. One JVM. Co-collect with `test_live_disclosure_still_diverges`. | Isolated cell worker; live leg; Rust walk pins. | **PROVEN** | Worker JSON; `flatten_stats_*` 2 passed (10 walks / 2 Unnests). Gate structs row-set equal True. |
| C-003 | Per-fixture table + candidate ranking (implement with projected gain and pin, or not worth it with the number). A contained fix lands only if under ~150 lines, pinned before/after, row-set unchanged. | The baseline note; ranking. | **PROVEN** | Release 1e5 (total 351.6 ms): null-mask 25.0% and cartesian 21.5% queued PERF-DYNFLATTEN-2; walks 0.6% not worth it. 1e6 cells re-measured. `DYNFLATTEN-QUALNAME-1` and `DYNFLATTEN-LISTNULL-1` filed, not fixed. |
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
      evidence: Product dynamicFlatten signature unchanged; the stats type and entry point are pub(crate), reachable only from the crate's own tests, so no public surface is added.
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
      evidence: Two registry rows filed and pinned (DYNFLATTEN-QUALNAME-1, DYNFLATTEN-LISTNULL-1); both implement-ranked candidates queued as PERF-DYNFLATTEN-2, neither fixed here.
      artifacts: [docs/spark-sql-iceberg-parity.md, docs/perf/dynamic-flatten-baseline.md]
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
| `Makefile` | `dynflatten-bench` renders its report under the bed, not over the baseline note |
| `test_dynamic_flatten_divergences.py` | New module: the `DYNFLATTEN-QUALNAME-1` pin |
| `spark-sql-iceberg-parity.md` | `DYNFLATTEN-QUALNAME-1`, `DYNFLATTEN-LISTNULL-1` |

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
| `test_dynamic_flatten_divergences.py::test_three_level_struct_with_keep_column_hits_qualified_name_clash` | `DYNFLATTEN-QUALNAME-1` error text |
| `test_live_dynflatten_matches_spark_explode[list_struct_1]` | `DYNFLATTEN-LISTNULL-1` column-set divergence |

Mutation (run 2026-09-04): delete the `has_struct_columns(fields, stats)` walk from the
struct branch of the rewrite → `cargo test -p repark-core flatten_stats` gives
**1 red of 2** (`flatten_stats_depth_three_struct_counts_repeated_schema_walks` fails on
`schema_walks` 10 → 6; the sibling Unnest pin stays green, which is the point: the counter
is walk-specific, not a blanket plan assertion). Reverted.

## 8. Measurement table (C-003)

See [docs/perf/dynamic-flatten-baseline.md](../../../docs/perf/dynamic-flatten-baseline.md).
Release 1e5 total repark wall 351.6 ms: null-mask 25.0%, cartesian 21.5%, walks 0.6%.
Queued (not fixed here): `DYNFLATTEN-QUALNAME-1`, `DYNFLATTEN-LISTNULL-1`, PERF-DYNFLATTEN-2.

| Round-2 decision | Record |
|---|---|
| Profile | Round 1 measured a 637 MB debug module; every number here is the 162505344 B `maturin develop --release` build, rebuilt and re-measured in round 2. |
| Re-measurement | Round-2 Opus rebuilt release and re-ran the 1e5 battery and all six 1e6 cells itself. Ranking order, shares and every `rows_out` agree with the round-2 draft; absolute wall differs up to 36 % on the two sub-30 ms cells, so the draft tables were REPLACED with the re-measured ones rather than kept. |
| Ranking change | Debug ranked cartesian and null-mask tied at 0.233 with walks 0.002. Release separates them and swaps ranks 1 and 2: null-mask 0.250, cartesian 0.215, walks 0.006. The verdicts do not change. |
| Size gate | No ceiling moved. Size rows in this repo ratchet DOWN only, so the `DYNFLATTEN-QUALNAME-1` pin went into a new `python/repark/tests/test_dynamic_flatten_divergences.py` at the split seam `check_lib_py.py` already names for that file. `test_dynamic_flatten.py` ends at 1618, its unchanged ceiling; `scripts/check_lib_py.py` is byte-identical to `origin/main`. |
| Live leg | `REPARK_PARITY_LIVE=1 … -k "dynflatten or disclosure"` now exits 0 (17 passed, 99 deselected) with ivy redirected into the clone. The ivy directory is untracked and is in no commit. |
| Clippy spelling | The review asked for bare `cargo clippy --locked --workspace --all-targets -- -D warnings`. That spelling exits 101 on this tree with 1774 pre-existing `disallowed_methods` errors — `.expect` in the test code of `repark-iceberg`, `repark-functions`, `repark-ml`, none of them files this branch touches and zero of them in `dynamic_flatten`. The repo's own gate passes `-A clippy::disallowed_methods` on purpose and pairs it with `rust-panic-ban`, which is the only place that list is live; both are run here and both exit 0. |

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
| `.venv/bin/python -m pytest python/repark-parity/tests/test_dynflatten_bed.py -q` | 0 (15 passed) |
| `.venv/bin/python -m pytest test_dynamic_flatten.py test_dynamic_flatten_divergences.py test_dynflatten_bed_gate.py -q` | 0 (47 passed) |
| gate-scale Spark explode co-collect | 0 (`struct_d3`/`struct_d6` row_set_equal True; the five False shapes are exactly the five carrying `user_properties`) |
| `REPARK_PARITY_LIVE=1 pytest test_parity_live.py -k "dynflatten or disclosure"` | 0 (17 passed, 99 deselected) |
| `make rust-clippy` (`cargo clippy --locked --workspace --all-targets -- -D warnings -A clippy::disallowed_methods`) | 0 |
| `make rust-panic-ban` | 0 |
| `typos .` | 0 |
| ruff check / format | 0 |
| `make verify` | 0 |
| `maturin develop --release` | 0 (`Finished \`release\` profile [optimized]`) |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |

Duplicate-row guard (R5): `grep -oE '^- \[[^]]+\]' task/ledgers/completed/map.md | sort | uniq -d` must be empty.
