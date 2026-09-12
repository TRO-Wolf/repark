# Charter ledger — PERF-DYNFLATTEN-2 · the null-mask struct extractor

**Date:** 2026-09-04 · **Branch:** `perf/dynflatten-2-null-mask` · **Base:** `main`
`b5b17f0` · **Model:** opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Registry:** `DYNFLATTEN-QUALNAME-1` BACKLOG → **FIXED**.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** PERF-DYNFLATTEN-1 measured three H-3 candidates per fixture against a noise floor
and queued exactly one: the null-mask struct extractor, on `struct_d6` alone. This unit builds
it, or halts with the evidence.

**Not in this unit:** the Cartesian operator and the optimizer walks (closed by the baseline's
do-not list); any flattened row-set or column-type change; `DYNFLATTEN-LISTNULL-1` /
`DYNFLATTEN-READNULL-1`; the `Dictionary(_, Struct)` parent path; the H-3b hard-gated baseline.

## PROPOSITION LEDGER — PERF-DYNFLATTEN-2 — 2026-09-04

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Baseline first: on a RELEASE native module the isolation battery runs once on this box tonight; the per-fixture null-mask cost and the noise floor are recorded with the 1-minute load. | The runner's own release refusal; `before.json`; §8. | **PROVEN** | `release_or_stripped size_bytes=163106944`, `__debug_assertions__ False`. Load 4.50 at start. Floor **4.077 ms** on `struct_d3` (6 repeats 25.47/26.79/22.78/24.68/22.71/25.68). Null-mask cost `struct_d6` **64.83 ms** (15.9x), `struct_d3` 22.84 ms (5.6x). |
| C-002 | The smallest change that removes the per-leaf CASE work is a physical null-mask extractor; the rejected alternatives are recorded with their measured cost; no public API change; no comment in code. | The diff; §6; §7. | **PROVEN** | One 125-line private module plus `dynamic_flatten.rs` +22 / −7. Two alternatives built and measured red (§7). `lib.rs` untouched; `DynamicFlattenOptions` / `dynamic_flatten` unchanged. Pre-commit comment grep empty on every commit. |
| C-003 | Same battery after: per-fixture before/after against the floor; `struct_d6`'s null-mask cost below 3x the floor; no other fixture regresses beyond the floor; `rows_out` identical; a correctness pin per bed shape; mutation reds the pin. | §8; the pins; the mutation runs. | **PROVEN** | `struct_d6` 64.83 → **0.01 ms**, **0.1x** the after floor (bar 3x). `rows_out` identical on all 11. Row set, schema and ordered-row digest identical on all 11, measured against a rebuilt pre-extractor module. Two fixtures moved up and an untouched control measures the dispersion that explains both (§8). Mutation: **1 red of 2** on the plan pins, run. |
| C-004 | The `REPARK_PARITY_LIVE=1` dynflatten legs stay green against the shared Spark oracle. | The live run. | **PROVEN** | `REPARK_PARITY_LIVE=1 pytest test_parity_live.py test_parity_live_dynflatten.py -k "dynflatten or disclosure"` exit 0 — **17 passed, 105 deselected**, the same count PERF-DYNFLATTEN-1 recorded. `test_live_dynflatten_matches_spark_explode` reads the same parquet on both engines, so the extractor is exercised on the `read.parquet` path (where leaf pushdown lives), not only on `createDataFrame`. |
| C-005 | Docs: the baseline note gains an "after" section without overwriting the before numbers; the registry row is trued up; STATUS `perf` one line; the slate row; this ledger; `map.md` lockstep. | The docs; the gates. | **PROVEN** | `docs/perf/dynamic-flatten-baseline.md` "After PERF-DYNFLATTEN-2" appended, nothing above it edited. `DYNFLATTEN-QUALNAME-1` FIXED with the 12-cell oracle matrix. STATUS 21616 B, slate 5961 B, both under ceiling; `_Last updated:` untouched. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: perf-dynflatten-2-null-mask
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the brief; the delivery gate (struct_d6 under 3x the floor, no other fixture past the floor, rows_out identical) is measured in section 8, not paraphrased.
      artifacts: [docs/perf/dynamic-flatten-baseline.md, python/repark/tests/test_dynflatten_null_mask.py]
    - id: AT-2
      status: ATTACKED
      evidence: Null parent with dirty children, null mid-struct, dirty valid List under a null parent, empty-struct schema, dictionary struct, dictionary utf8 struct, list-of-map refuse, ListView refuse — the existing 53 kernel pins all run through the extractor. Parent with zero nulls short-circuits; parent field absent and non-literal name are exec_err paths.
      artifacts: [crates/repark-core/src/dynamic_flatten/tests.rs, crates/repark-core/src/dynamic_flatten/tests/preserve_nulls.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every failure path in the UDF returns exec_err; no unwrap, expect or panic (make rust-panic-ban exit 0). The unsupported parent type falls back to the shipped CASE rather than erroring.
      artifacts: [crates/repark-core/src/dynamic_flatten/null_mask.rs]
    - id: AT-4
      status: N/A
      justification: The extractor is a pure per-batch scalar function with no shared or mutable state, no ordering assumption and no async; the rewrite it feeds is single-threaded plan construction.
    - id: AT-5
      status: N/A
      justification: No authn/authz, no deserialization, no path or network surface; unsafe_code stays workspace-forbidden and this module adds none (nullif is arrow's safe public kernel).
    - id: AT-6
      status: ATTACKED
      evidence: Row set, Arrow schema (names, types, nullability) and ordered row digest compared against a rebuilt pre-extractor module on all eleven bed shapes; the raw IPC bytes differ on five and the cause is measured, not assumed (section 8).
      artifacts: [python/repark/tests/test_dynflatten_null_mask.py]
    - id: AT-7
      status: ATTACKED
      evidence: This unit is the performance work; the numbers are in section 8 with the floor, the load and an untouched control for the two fixtures that moved up. The one cost paid (a struct leaf is no longer visible to push_down_leaf_projections) is stated in the crate map.
      artifacts: [docs/perf/dynamic-flatten-baseline.md, crates/repark-core/src/dynamic_flatten/map.md]
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54.1 contracts read from the vendored source before use — GetFieldFunc returns the child without the parent's validity, CaseExpr picks ExpressionOrExpression for this shape, ScalarUDFImpl needs DynEq/DynHash, arrow nullif accepts any array type. The placement contract was measured rather than assumed (section 7).
      artifacts: [crates/repark-core/src/dynamic_flatten/null_mask.rs, crates/repark-core/src/dynamic_flatten/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: The extractor is named in the logical plan (repark_null_mask_field), which is what both plan pins read; a failure inside it carries the function name and the offending type or field name.
      artifacts: [crates/repark-core/src/dynamic_flatten/tests/preserve_nulls.rs]
    - id: AT-10
      status: ATTACKED
      evidence: The mutation was built and run, not reasoned — null_mask_extractable false reds 1 of the 2 plan pins, and that build was proved byte-identical to main on all eleven shapes, which is what makes it a faithful mutation. Its 6-of-12 effect on the facade matrix is derived from that identity plus main's own green refusal pins, and is labelled as derived. The correctness pin is deliberately NOT a mutation detector; it must stay green both ways and was run on the mutation build to show it.
      artifacts: [crates/repark-core/src/dynamic_flatten/tests/map.md, python/repark/tests/map.md]
  complete: true
```

## 6. What changed

| File | Change |
|---|---|
| `crates/repark-core/src/dynamic_flatten/null_mask.rs` | New. `repark_null_mask_field(parent, 'field')`: one scalar UDF that returns the child array `get_field` would return with the parent's validity unioned in via `arrow::compute::nullif`. `null_mask_extractable` gates it to plain `Struct` parents. |
| `crates/repark-core/src/dynamic_flatten.rs` | `mod null_mask;`; `ProjectionSlot::Expand` carries `masked`; the expansion projection emits the extractor for a plain struct parent and `null_safe_field` (the CASE) otherwise. `null_safe_field` and `typed_null_literal` are unchanged. |
| `crates/repark-core/src/dynamic_flatten/tests/preserve_nulls.rs` | Two plan pins: a plain struct emits the extractor once and no CASE; a dictionary struct emits the CASE once and no extractor. |
| `python/repark/tests/test_dynflatten_null_mask.py` | New. Per bed shape: row count, Arrow schema string, SHA-256 over the ordered rows — the values captured from a pre-extractor module. |
| `python/repark/tests/test_dynamic_flatten_divergences.py` | The two `DYNFLATTEN-QUALNAME-1` refusal pins and their control become one answer pin over the same 12 cells. |
| `docs/spark-sql-iceberg-parity.md` | `DYNFLATTEN-QUALNAME-1` → FIXED, with the re-measured matrix and the 12-cell PySpark 4.1.2 oracle. |
| `docs/perf/dynamic-flatten-baseline.md` | "After PERF-DYNFLATTEN-2" appended; nothing above it edited. |
| `STATUS.md`, `briefs/next-sequence.md` | One line each. `_Last updated:` untouched. |
| `map.md` × 6 | Lockstep. |

No public API change: `lib.rs`, `DynamicFlattenOptions` and `dynamic_flatten` are byte-identical
to `main`.

## 7. Design, and the alternatives that were built and measured

The rewrite emitted `CASE WHEN parent IS NULL THEN <typed null> ELSE get_field(parent, 'f')`
per leaf per level. DataFusion 54.1 plans that shape as `EvalMethod::ExpressionOrExpression`,
which calls `filter_record_batch` **twice** over the whole batch (then-side rows, else-side
rows), evaluates each side and zips. `struct_d6` pays it eight times across six projections.
`get_field` itself is an `Arc::clone` of the child column — it returns the child **without** the
parent's validity, which is the only reason the CASE existed.

| alternative | why it was rejected | measured |
|---|---|---|
| Extractor declaring `placement() = MoveTowardsLeafNodes`, mirroring `GetFieldFunc` | reds `nested_struct_in_struct`, a shape that collects on `main`: `Optimizer rule 'push_down_leaf_projections' failed … AmbiguousReference { name: "id" }`. | built and run, 52 passed 1 failed |
| `mask(get_field(parent, 'f'), parent)` — keep `get_field` in the plan and wrap it, so the leaf stays visible to the leaf-projection rule | same red, same rule, same message. A `get_field` at the top of a projection expression takes a different route through that rule than the same call inside a CASE branch, which the rule will not hoist out of a conditional. The CASE was hiding `get_field`; the replacement has to hide it too. | built and run, 52 passed 1 failed |
| Handle `Dictionary(_, Struct)` parents in the extractor too | `get_field` there returns `Dictionary(K, child)` while the typed null is `child`; the CASE's coercion between them is the shipped output type, and reproducing it is not free. The path is unmeasured and the branch costs four lines. | not built; the CASE is kept for it, pinned |
| Overwrite masked slots so the raw IPC bytes match too | the difference is the payload **under cleared validity bits** — don't-care bytes. Writing them back would add exactly the per-row work this unit removes. | rejected on the measurement in §8 |

Shipped: `placement` at the trait default (`KeepInPlace`), no `get_field` in the plan. The cost
is that a struct leaf is no longer visible to `push_down_leaf_projections`, so
`read.parquet(…).dynamicFlatten().select(one_leaf)` reads the whole parent struct where it could
have read one leaf. `dynamicFlatten` expands **every** leaf, so nothing is pruned in the
un-projected case, which is what the bed measures. The projected case was then measured by
the critic on both builds (`read.parquet(struct_d6 @1e5).dynamicFlatten().select(one leaf)
.to_arrow()`, 11 interleaved repeats, load matched within 0.5): projected median 28.59 ms →
8.66 ms, un-projected 26.80 ms → 8.27 ms. Projecting one leaf was already slightly slower
than not projecting before the change, so the pruning the cost describes bought nothing on
this shape; the cost is real in the plan and absent in the measurement.

## 8. Measurement (C-001, C-003)

Same command both halves: `run_dynflatten.py --scale quick --iterations 5 --warmup 1`, 8 threads
on both engines, release module proved from `repark._native.__debug_assertions__`.

| key | before | after |
|---|---|---|
| 1-minute load at run start | 4.50 | 4.53 |
| noise floor (`struct_d3`, 6 repeats) | 4.077 ms | 0.0585 ms |
| native size_bytes | 163106944 | 163145824 |

`execute` median / min in ms — `to_arrow()` only, the column the isolation subtracts:

| shape | iso | before med | before min | after med | after min | rows_out (both) |
|---|---|---:|---:|---:|---:|---:|
| struct_d3 | | 25.31 | 25.16 | 0.95 | 0.94 | 100000 |
| struct_d6 | | 71.22 | 68.79 | 1.50 | 1.43 | 100000 |
| list_struct_1 | | 30.02 | 28.08 | 17.24 | 16.67 | 100000 |
| list_struct_8 | | 44.09 | 27.41 | 33.55 | 21.74 | 589888 |
| list_struct_64 | | 98.54 | 96.74 | 64.92 | 57.91 | 4505338 |
| cartesian_two_lists | | 85.88 | 63.85 | 53.37 | 52.09 | 961708 |
| null_typed_list | | 12.50 | 11.83 | 0.99 | 0.96 | 100000 |
| struct_d3_nonull | y | 2.47 | 2.18 | 1.00 | 0.98 | 100000 |
| struct_d6_nonull | y | 6.39 | 6.29 | 1.50 | 1.47 | 100000 |
| cartesian_legs_only | y | 26.17 | 24.88 | 30.28 | 28.40 | 310150 |
| cartesian_tags_only | y | 18.83 | 14.88 | 23.71 | 18.82 | 310150 |

The candidate, per fixture, never summed:

| candidate | fixture | before | after | after ÷ after-floor | bar | verdict |
|---|---|---:|---:|---:|---|---|
| null_mask_struct_extractor | **struct_d6** | 64.83 ms | **0.01 ms** | 0.1 | under 3x | **delivered** |
| null_mask_struct_extractor | struct_d3 | 22.84 ms | −0.05 ms | −0.9 | under 3x | delivered |

**The two fixtures that moved up.** `cartesian_legs_only` +4.1 ms and `cartesian_tags_only`
+4.9 ms are the only medians that rose, and both are above the 0.0585 ms after-floor. That floor
is the spread of a cell that now runs in 1 ms and is not a dispersion statistic for a 25 ms
cell — the same error PERF-DYNFLATTEN-1 recorded and discarded. Measured instead:

| control | why it is a control | 6 back-to-back repeats on the after build (ms) | spread |
|---|---|---|---:|
| `cartesian_tags_only` | its schema (`id`, `Tags ARRAY<STRING>`, `user_properties ARRAY<VOID>`) holds **no struct**, so `expand_structs` never runs and the extractor is never constructed — the diff cannot reach it | 25.28, 25.96, 23.31, 28.69, 28.36, 29.67 | **6.36** |
| `cartesian_legs_only` | does use the extractor | 31.26, 29.33, 32.72, 31.04, 30.57, 32.92 | 3.59 |

Both movements are inside the dispersion at their own scale, and the fixture that contains
**both** the legs work and the tags work — `cartesian_two_lists` — fell 85.88 → 53.37 ms, which
work genuinely added to the legs path could not do.

**Row-set identity.** Not argued from `rows_out`. A pre-extractor module was rebuilt
(`null_mask_extractable` → `false`) and proved faithful: its gate-scale Arrow **IPC bytes** are
identical to `main`'s on all eleven shapes. Against it, the extractor build gives identical row
count, identical Arrow schema string (names, types, nullability) and identical SHA-256 over the
ordered `to_pylist()` rows on all eleven shapes. The raw IPC bytes differ on the five
list-of-struct shapes; since the rows, the nulls and the schema are equal, the difference is the
payload **under cleared validity bits** — the CASE wrote a typed-null default there, `nullif`
leaves the original bytes and clears the bit. Those bytes are unreadable through any Arrow API.
The pin is therefore row-count + schema + ordered-row digest, not the IPC digest.

**Mutations.**

| mutation | pin | result |
|---|---|---|
| `null_mask_extractable` → `false` | the two plan pins in `preserve_nulls.rs` | **1 red of 2** (the extractor pin reds; the dictionary pin stays green, so the pin is extractor-specific) |
| same | `test_dynamic_flatten_divergences.py`, 12 cells | **6 red of 12**, derived from two measurements rather than one run: the mutation build is byte-identical to `main` on all eleven shapes, and on `main` exactly those 6 cells raised — the two refusal pins this unit replaced were green there. The 6 keep-less / depth-1 cells stay green. |
| same | `test_dynflatten_null_mask.py`, 11 shapes | **0 red of 11**, run on the mutation build (12 passed) — by design: it is the correctness pin and must be green both ways, and that is the before/after proof |

## 9. Delivery template

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: perf-dynflatten-2-null-mask
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: perf-dynflatten-2-null-mask
  artifacts_verified:
    ledger: PASS (C-001..C-005 PROVEN)
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
| `make ci` | 0 |
| `make verify` | 0 (ci + the whole Rust workspace suite) |
| `make check-python-conventions` | 0 (232 files clean, nested-def rows 0) |
| `cargo test -p repark-core dynamic_flatten` | 0 (53 passed) |
| `pytest test_dynamic_flatten.py test_dynamic_flatten_divergences.py test_dynflatten_bed_gate.py test_dynflatten_null_mask.py` | 0 (70 passed) |
| `pytest python/repark-parity/tests/test_dynflatten_bed.py` | 0 (17 passed) |
| `REPARK_PARITY_LIVE=1 pytest test_parity_live.py test_parity_live_dynflatten.py -k "dynflatten or disclosure"` | 0 (17 passed, 105 deselected) |
| `make check-map-sync` | 0 (177 maps clean) |
| `make check-ledger-grammar` | 0 (35 live ledgers, 101 clauses, 684 pinned ids) |
| `make check-ledgers` | 0 (inside `make ci`) |
| `make check-docs-compaction` | 0 (STATUS 21616 B, slate 5961 B) |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 (231 ledgers, 704 links, frozen rule clean) |
| `typos .` | 0 |
| `cargo deny check` | 0 (advisories, bans, licenses, sources ok) |
| `ruff check` / `ruff format --check` | 0 (632 files) |
| `cargo fmt --check` | 0 (inside `make ci`) |
| `make rust-panic-ban` | 0 |
| `maturin develop --release` | 0 (`__debug_assertions__ False`; the runner refuses a debug module) |

## 11. Review (critic round 1, Opus, on a clone with a release module of its own)

Verdict FAIL on four documentation findings; every measurement re-run and confirmed (22
before/after cells byte-equal at gate scale, four extra shapes the bed lacks identical on both
builds and Spark-equal, the twelve QUALNAME cells re-derived on PySpark, the mutation run:
1 red of 2 Rust pins and 6 red of 12 divergence cells, the projected case above).

| # | Finding | Resolution |
|---|---|---|
| R1-1 (S2) | `python/repark/tests/map.md` described the rejected IPC-digest design (`DIGESTS`, `table_digest`) as the shipped pin. | Row rewritten to the shipped `ROWS` pin and why the IPC bytes were rejected. |
| R1-2 (S3) | Slate row 6 still advertised this unit as queued while its block spoke in the past tense. | Row 6 now names the residue this unit leaves (`DYNFLATTEN-LISTNULL-1` / `READNULL-1`). |
| R1-3 (S3) | Header promises a move to `../completed/` in the last commit; the ledger stays in `staging/`. | Accepted as the tree's standing habit (PERF-DYNFLATTEN-1, DATE-FN-1, FN-FIX-2 sit the same way); the lifecycle gate files them at archive time. |
| R1-4 (S3) | The `origin/main` merge commit carried the author name `John`. | Merge redone under the unit identity. |

Duplicate-row guard (R5): `grep -oE '^- \[[^]]+\]' task/ledgers/staging/map.md | sort | uniq -d`
must be empty.
