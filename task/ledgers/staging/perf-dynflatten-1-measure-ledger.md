# Charter ledger — PERF-DYNFLATTEN-1 · measure `dynamicFlatten`

**Date:** 2026-09-04 · **Branch:** `perf/dynflatten-1-measure` · **Base:** `origin/main`
`467ce26` · **Model:** grok-4.6 (round 1); opus-5 (rounds 2-5) · **Policy:**
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
| C-003 | Per-fixture table + candidate ranking (implement with projected gain and pin, or not worth it with the number). A contained fix lands only if under ~150 lines, pinned before/after, row-set unchanged. | The baseline note; ranking. | **PROVEN** | Costs isolated PER FIXTURE against a 10.81 ms floor, never summed: null-mask clears 3x on `struct_d6` alone (59.98 ms, 5.5x) and is the only queued candidate; `struct_d3` 23.01 ms (2.1x), cartesian 26.91 ms (2.5x, unstable) and walks 0.41 ms (0.04x) are closed. Three registry rows filed, none fixed. |
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
      evidence: Product dynamicFlatten signature unchanged AND its work unchanged - the rewrite is generic over StatsSink and the product entry instantiates the ZST NoStats, so counters and the plan walk compile away; stats items are #[cfg(test)]. Pinned by product_dynamic_flatten_does_no_plan_walk.
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
      evidence: Three registry rows filed and pinned (DYNFLATTEN-QUALNAME-1, DYNFLATTEN-LISTNULL-1, DYNFLATTEN-READNULL-1); ONE candidate queued as PERF-DYNFLATTEN-2 (null-mask, on struct_d6 alone), none fixed here.
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
| `test_keep_column_at_depth_two_is_ambiguous_qualified_vs_unqualified` | `DYNFLATTEN-QUALNAME-1` depth-2 message (regex cannot match `unqualified`) |
| `test_keep_column_at_depth_three_and_deeper_duplicates_the_unqualified_name` | the distinct depth-≥3 message |
| `test_depth_one_with_keep_and_any_depth_without_keep_still_collect` | control: the clash needs a keep column AND depth ≥ 2 |
| `test_live_dynflatten_matches_spark_explode` | `DYNFLATTEN-READNULL-1` nullability, both engines on `read.parquet` |
| `test_rank_candidates_uses_isolated_cost_and_the_noise_floor` | queued only above 3x the floor |
| `test_live_dynflatten_matches_spark_explode[list_struct_1]` | `DYNFLATTEN-LISTNULL-1` column-set divergence |

Mutation (2026-09-04): delete the `has_struct_columns` walk → `schema_walks` 10 → 6,
`flatten_stats_depth_three_struct_counts_repeated_schema_walks` reds, **1 red of 2**; the
sibling Unnest pin stays green, so the counter is walk-specific. Reverted.

## 8. Measurement table (C-003)

See [docs/perf/dynamic-flatten-baseline.md](../../../docs/perf/dynamic-flatten-baseline.md).
Costs are per fixture against a 10.81 ms floor; none is a sum across fixtures.

| candidate | fixture | isolated | x floor | verdict |
|---|---|---:|---:|---|
| null_mask_struct_extractor | struct_d6 | 59.98 ms | 5.5 | queued PERF-DYNFLATTEN-2 |
| null_mask_struct_extractor | struct_d3 | 23.01 ms | 2.1 | below bar alone |
| cartesian_multi_list_operator | cartesian_two_lists | 26.91 ms | 2.5 | not worth it |
| optimizer_wrapper_walks | cartesian_two_lists | 0.41 ms | 0.04 | not worth it |

Filed, not fixed: `DYNFLATTEN-QUALNAME-1`, `DYNFLATTEN-LISTNULL-1`, `DYNFLATTEN-READNULL-1`.

| Round-2 decision | Record |
|---|---|
| Profile | Round 1 measured a 637 MB debug module; every number here is the 162505344 B `maturin develop --release` build, rebuilt and re-measured in round 2. |
| Re-measurement | Round-2 Opus rebuilt release and re-ran the 1e5 battery and all six 1e6 cells itself. Ranking order, shares and every `rows_out` agree with the round-2 draft; absolute wall differs up to 36 % on the two sub-30 ms cells, so the draft tables were REPLACED with the re-measured ones rather than kept. |
| Ranking change | Debug ranked cartesian and null-mask tied at 0.233 with walks 0.002. Release separates them and swaps ranks 1 and 2: null-mask 0.250, cartesian 0.215, walks 0.006. The verdicts do not change. |
| Round 4 — comparison | Round 3's table was not a fair comparison: Spark ran `local[1]` while repark used DataFusion's 64-thread default, and repark's timed region excluded the parquet scan while Spark's `read.parquet` re-scanned per iteration. Both engines now get a materialized frame (repark `createDataFrame`; Spark `.cache().count()`) and the timed region is flatten+collect only, at 8 threads each. |
| Round 4 — candidates | Shares of fixture-family wall are gone. Each candidate is timed as itself: walks = rewrite wall; null-mask = struct fixtures at 30 % nulls minus the same at 0 %; Cartesian = two-list minus (legs-only + tags-only). New bed shapes `struct_d3_nonull`, `struct_d6_nonull`, `cartesian_legs_only`, `cartesian_tags_only` exist only for those subtractions and are flagged `isolation` so they never enter a headline. |
| Round 4 — verdict rule | The 0.20 share cliff is gone. A candidate is queued only when its isolated cost exceeds the measured noise floor by 3x. Result: only null-mask qualifies; the Cartesian operator is NOT queued, reversing round 3. |
| Round 4 — noise floor | First estimator (a single \|A-B\| of two medians) gave 8.65 ms and 0.12 ms on two runs and flipped every verdict, once ranking a 0.95 ms cost "queued" at 7.7x. Replaced by the spread over 6 repeats of one cell: 10.81 ms. The three-run reproducibility table in the note is the evidence that null-mask is real and Cartesian is not. |
| Round 4 — release gate | `repark_core::built_with_debug_assertions()` is exposed as `repark._native.__debug_assertions__`; the runner and the cell worker REFUSE to measure or write a report on a debug build (H-3a). The old file-size heuristic is no longer the gate. |
| Round 4 — QUALNAME re-derived | Round 3's row was WRONG. Measured matrix keep ∈ {none, id, k} x depth 1-4: onset is depth 2 (not 3), any keep-column name fails identically (not an `id` collision), keep=None passes at every depth, and there are TWO messages — depth 2 `qualified ... which would be ambiguous`, depth ≥ 3 `duplicate unqualified field name`. One pin per message with regexes that cannot cross-match, plus a control. |
| Round 5 — product path | The measurement had routed product `dynamic_flatten` through `dynamic_flatten_with_stats`, so every product call ran `count_plan_kinds` (a full logical-plan walk) plus eight counter increments and discarded them. A measure-only unit must not add work to the product path. Fixed: the rewrite is generic over a `StatsSink`; the product entry instantiates the ZST `NoStats` so every counter call and the plan walk monomorphize away, and `DynamicFlattenStats`, its sink impl, `dynamic_flatten_with_stats` and `count_plan_kinds` are `#[cfg(test)]`. Pin `product_dynamic_flatten_does_no_plan_walk` asserts `PLAN_WALKS` stays 0 through the product entry and reaches 1 through the stats entry; routing the product entry back through the stats entry reds it, 1 of 1. |
| Round 5 — per-fixture costs | The 82.99 ms headline was `struct_d3` + `struct_d6` summed against a single-cell floor — the same aggregation error as a share of family wall, one level down. Costs are now reported per fixture and the verdict is stated on the strongest SINGLE fixture: null-mask clears 3x on `struct_d6` alone (59.98 ms, 5.5x); `struct_d3` (23.01 ms, 2.1x) would not have carried it. Pinned by `test_rank_candidates_never_sums_two_fixtures_into_one_cost`. |
| Round 5 — JVM transfer | The remaining asymmetry is NOT removed, by choice: Spark's `toArrow()` moves the whole result across the JVM→Python boundary, repark's `to_arrow()` is in-process. The alternative (time an in-JVM `.count()` after flatten on both sides) was rejected because a count over a flatten is exactly the shape DataFusion and Spark can satisfy without materializing the expansion, which would measure nothing. It is disclosed as do-not #4 and the "lower on every fixture" sentence is qualified in the note. No re-measurement was needed. |
| Round 5 — naming | `CandidateShare` → `CandidateCost`; the unreported `wall_share` field is deleted. |
| Round 4 — host | The box ran 3-4 sibling lanes throughout (load 25-45). Stated in the note rather than hidden; the one queued candidate clears the floor by 5.5x on `struct_d6` alone and survives it. |
| Round 4 — live pin | `test_live_dynflatten_matches_spark_explode` now hands BOTH engines `read.parquet` of the same file. Feasible for all three pinned shapes despite the `ARRAY<VOID>` column. Making it symmetric SURFACED a new divergence the asymmetric pin had hidden: `DYNFLATTEN-READNULL-1`. |
| Pre-existing, NOT fixed | `read.parquet(...).schema` misreports nested columns as `StringType`: measured `Payload` STRUCT and `Legs`/`Tags` ARRAY all report `StringType` while `user_properties` reports `ArrayType`. Flatten itself is correct. Out of scope for a measurement unit; no registry row filed because the pin would be a facade-schema contract, not a flatten contract. |
| Size gate | No ceiling moved. Size rows in this repo ratchet DOWN only, so the `DYNFLATTEN-QUALNAME-1` pin went into a new `python/repark/tests/test_dynamic_flatten_divergences.py` at the split seam `check_lib_py.py` already names for that file. `test_dynamic_flatten.py` ends at 1618, its unchanged ceiling; `scripts/check_lib_py.py` is byte-identical to `origin/main`. |
| Live leg | `REPARK_PARITY_LIVE=1 … -k "dynflatten or disclosure"` now exits 0 (17 passed, 103 deselected) with ivy redirected into the clone. The ivy directory is untracked and is in no commit. |
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
| `REPARK_PARITY_LIVE=1 pytest test_parity_live.py -k "dynflatten or disclosure"` | 0 (17 passed, 103 deselected) |
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
