# Charter ledger — H3-SPILL-1 · the Never-OOM truth table

**Date:** 2026-09-05 · **Branch:** `harden/h3-spill-1` · **Base:** `origin/main`
`6eaccd5e` · **Model:** opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Risk tier:** `risk_tier: standard` (measurement + pins; no product code
changed). **Registry:** §7 `H3-SPILL-NLJ-1`, `H3-SPILL-COLLECT-1`.

**Retires:** the orchestrator's departure edit moves this ledger to `../completed/`; this unit
leaves `STATUS.md` and `briefs/next-sequence.md` untouched by instruction.

**Why now.** [PROJECT.md](../../../PROJECT.md) carries "predictable memory via spill-to-disk by
default" and marks *never OOM on data larger than RAM* as pending a spill-coverage spike. The
slate row "H-3 spill matrix" is that spike. Without it the claim is an intention.

**Not in this unit:** any product change. The charter permits one only for a silent wrong answer
or a process abort, and the matrix found neither. The two defects it did find are failure-*shape*
defects, filed as registry rows with pins that red when they are fixed.

## PROPOSITION LEDGER — H3-SPILL-1 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A tracked harness runs one operator cell per fresh subprocess on a release module under a stated memory guard, records peak RSS / wall / load / per-operator plan metrics per cell, and rewrites its report after every cell so a crash costs one cell. | The harness; a cell's JSON; the crash-cheap write. | **PROVEN** | `python/repark-parity/bench/spill/`. Guard is a parent-side `VmHWM` watchdog (`--rss-cap-bytes` 8 GiB) with `RLIMIT_AS` 32 GiB behind it: the first draft's 12 GiB `RLIMIT_AS` killed cells for reasons unrelated to the operator, because a session that has only counted a view already reserves 8.2 GiB virtual against 183 MiB resident. |
| C-002 | The matrix covers every operator the engine can plan — sort, top-k, hash aggregate (many/few groups), hash join, sort-merge join, nested-loop join, window (unbounded / sliding ROWS / RANGE), repartition, distinct, `array_agg`, `dynamicFlatten`, the Iceberg DV scan, MERGE staging, `collect()`, `toPandas` — across pool ∈ {none, 8 GiB, 1 GiB, 256 MiB, 64 MiB} × scale ∈ {1e6, 1e7}, with 1e7 exceeding 1 GiB of live data, three runs wherever the first outcome was not `ok`. | The 180 cells and their repeats. | **PROVEN** | 18 × 5 × 2 = 180 cells; 121 ok, 16 spilled, 15 degraded, 27 clean_error, 1 internal_error. Wide row ≈ 120 B → 1e7 ≈ 1.2 GiB. Exactly one outcome-unstable cell (`sort` 256 MiB / 1e6, `spilled/clean_error/spilled`); it carries no pin. |
| C-003 | No cell aborts the process and no cell returns a wrong answer: every bounded cell that produces an answer is compared against the unbounded run at the same scale. | The digest comparison; the abort census. | **PROVEN** | 0 abort, 0 wrong. 72 bounded cells produced a digest; all 72 equalled `pool=none`. Two digest traps were found and fixed first, each of which had manufactured false `wrong` cells: `lag(h) OVER ()` over a sorted subquery does not see a sorted stream (the optimizer drops a sort nothing depends on), and a `double` sum over 1e7 rows is order-dependent — probes are now `lag(h) OVER (ORDER BY h)` and `sum(cast(s AS bigint))`. |
| C-004 | Pins: per operator family, a bounded pool spills or refuses cleanly and never lies; the refusal is the Spark-shaped exception the facade documents; a spilling operator's resident memory stays far under the unbounded run's while the un-accounted facade boundary does not. | `test_h3_spill_matrix.py`; the gate run. | **PROVEN** | 20 pins, 14.8 s. 3 spilling cells (spill_count > 0 **and** digest equal), 5 fitting cells, 7 refusal-shape cells (`fair(` required, `greedy(` and any caught panic forbidden, both resize knobs named), session-usable-after-refusal, and the two RSS pins in fresh subprocesses (aggregate at a 256 MiB pool ≥ 200 MiB under the unbounded run and < 3× pool; `toPandas` at 64 MiB > 6× pool). |
| C-005 | The matrix document records the machine, the module, the method, both scales, the Spark comparison and the limits of the claim; `docs/perf/map.md` carries it and the raw cells are committed. | The doc; the JSON; the map rows. | **PROVEN** | `docs/perf/spill-matrix-baseline.md` + `-cells.json` (180 repark cells with every repeat, 3 Spark cells). §2 reads DataFusion 54.1's spill support out of the vendored source: windows, `Unnest`, `CoalesceBatches`, the Iceberg scan and the facade boundary take **no** reservation, so the pool cannot bound them — `collect()` at 1e7 is 4,459 MiB resident at every pool including 64 MiB. |
| C-006 | Both failure-shape defects are filed as registry rows with pins that red when fixed, and neither is a wrong answer or a process abort — so neither triggers the charter's product-change permission. | §7 rows; the two defect pins; the classification. | **PROVEN** | `H3-SPILL-NLJ-1` (DataFusion's `expect("partition not used yet")` at `repartition/mod.rs:1277`, reached from the join's right side; 3/3 reproducible; every other operator at the same 8 MiB pool refuses cleanly) and `H3-SPILL-COLLECT-1` (null `PyObject` in `collect_rows.rows_from_record_batch` under an `RLIMIT_AS` ceiling; the 6 GiB-headroom control is `ok`). Both pins carry their green control, so a pin that reds has a reason. |
| C-007 | Two or three Apache Spark cells run on the same fixture under a bounded driver heap, with spill measured rather than asserted. | The Spark cells; the event-log totals. | **PROVEN** | PySpark 4.1.2 / Zulu 17, `local[4]`, `spark.driver.memory=1g`, 1e7 rows, `noop` sink (a `count()` lets Spark drop the very `ORDER BY` under test — the first draft measured a plan with no sort). `sort` ok, 318.8 MB memory / 228.6 MB disk spilled; `hash_join` ok, 721.4 / 433.7 MB; `collect_list` **`java.lang.OutOfMemoryError: Java heap space`**, SparkContext down — where repark refuses with a typed exception and a live session. |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: h3-spill-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every operator family carries a pin; the three outcome classes (spills, fits, refuses) are separately parametrized and the two defects carry green controls.
      artifacts: [python/repark/tests/test_h3_spill_matrix.py]
    - id: AT-2
      status: ATTACKED
      evidence: The wrong-answer check compares 72 bounded digests against the unbounded run; two digest constructions that could not have caught a real divergence were found and replaced before the matrix was believed.
      artifacts: [python/repark-parity/bench/spill/roster.py, docs/perf/spill-matrix-baseline.md]
    - id: AT-3
      status: ATTACKED
      evidence: No AWS, no network, no secrets, no .github. Every cell runs under an explicit resident-memory watchdog and a 600 s timeout, so a runaway cell dies rather than the box.
      artifacts: [python/repark-parity/bench/spill/measure.py]
    - id: AT-4
      status: ATTACKED
      evidence: One subprocess per cell, release module asserted by measurement rather than assumed; three driver lanes ran concurrently and each cell records the 1-minute load it started under, so wall is read against its own floor.
      artifacts: [python/repark-parity/bench/spill/cell_worker.py, docs/perf/spill-matrix-baseline-cells.json]
    - id: AT-5
      status: N/A
      justification: No dependency, lockfile, or workflow change; the pyspark install for the comparison cells is venv-local and untracked.
    - id: AT-6
      status: ATTACKED
      evidence: No product code changed. The diff is a harness, a test file, a baseline document, its raw cells, two registry rows and map lockstep.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: ATTACKED
      evidence: Outcomes are read from EXPLAIN ANALYZE counters, never from wall time; the one outcome-unstable cell is named in the document and carries no pin; every non-ok cell ran three times and its runs are in the JSON.
      artifacts: [docs/perf/spill-matrix-baseline.md, docs/perf/spill-matrix-baseline-cells.json]
    - id: AT-8
      status: N/A
      justification: No dependency or lockfile change.
    - id: AT-9
      status: ATTACKED
      evidence: Two BACKLOG rows filed with pins in the same change; nothing fixed, and the charter's product-change permission was checked against the measured outcomes rather than assumed.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_h3_spill_matrix.py]
    - id: AT-10
      status: ATTACKED
      evidence: Every touched directory's map.md moves in the same commit; the baseline, the registry and the harness each point at the others.
      artifacts: [docs/perf/map.md, docs/map.md, python/repark/tests/map.md, python/repark-parity/bench/map.md]
  complete: true
```

## Gates

| Gate | Exit |
|---|---|
| `make ci` | 0 |
| `make verify` (= `ci` + the Rust workspace suite) | 0 |
| `make check-python-conventions` | 0 |
| `make rust-panic-ban` | 0 |
| `.venv/bin/python -m pytest python/repark/tests -q` | 0 (4821 passed, 198 skipped, 419 s) |
| `.venv/bin/python -m pytest python/repark-parity/tests -q` | 0 (574 passed) |
| `.venv/bin/python -m pytest python/repark/tests/test_h3_spill_matrix.py -q` | 0 (20 passed, 30 s) |
| `REPARK_PARITY_LIVE=1 pytest test_parity_live.py -q` (Spark cells were measured) | 0 (119 passed, 76 s) |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |
