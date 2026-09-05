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
| C-003 | No cell aborts the process and no cell returns a wrong answer: every bounded cell that produces an answer is compared against the unbounded run at the same scale, on **every** repeat, with a content digest whose kind is disclosed. | The digest comparison; the abort census; the coverage arithmetic. | **PROVEN** | 0 abort, 0 wrong. Of 180 cells, 144 are bounded; **115 carry a digest and every one equals `pool=none`**, which is **163 run digests** once repeats are counted. The 29 without are **28 refusals** (a cell that raised has no answer to hash) plus **one probe failure** (`sort` 256M/1e6, whose probe exhausted the same pool); there is no undigested operator. Four probe traps were found and fixed before the matrix was believed: `lag(h) OVER ()` over a sorted subquery does not see a sorted stream, a `double` sum over 1e7 rows is order-dependent, the five `api` rows digested a row count rather than content, and the pandas probe taken whole pushed the worker past the resident cap (now chunked at 100 k rows, ~30 MiB on a 2.1 GiB cell). |
| C-004 | Pins: per operator family, a bounded pool spills or refuses cleanly and never lies; the refusal is the Spark-shaped exception the facade documents; a spilling operator's resident memory stays far under the unbounded run's while the un-accounted facade boundary does not. | `test_h3_spill_matrix.py`; the gate run. | **PROVEN** | 20 pins, 14.8 s. 3 spilling cells (spill_count > 0 **and** digest equal), 5 fitting cells, 7 refusal-shape cells (`fair(` required, `greedy(` and any caught panic forbidden, both resize knobs named), session-usable-after-refusal, and the two RSS pins in fresh subprocesses (aggregate at a 256 MiB pool ≥ 200 MiB under the unbounded run and < 3× pool; `toPandas` at 64 MiB > 6× pool). |
| C-005 | The matrix document records the machine, the module, the method, both scales, the Spark comparison and the limits of the claim; `docs/perf/map.md` carries it and the raw cells are committed. | The doc; the JSON; the map rows. | **PROVEN** | `docs/perf/spill-matrix-baseline.md` + `-cells.json` (180 repark cells with every repeat and every run digest, 3 Spark cells with the JVM's captured stderr). §1.1 discloses the digest kind per operator; §2 reads DataFusion 54.1's spill support out of the vendored source: windows, `Unnest`, `CoalesceBatches`, the Iceberg scan and the facade boundary take **no** reservation, so the pool cannot bound them — `collect()` at 1e7 is 4,393-4,471 MiB resident at every pool including 64 MiB, and returns the identical digest at each, so what the pool does not bound it also does not corrupt. |
| C-006 | Both failure-shape defects are filed as registry rows with pins that red when fixed, and neither is a wrong answer or a process abort — so neither triggers the charter's product-change permission. | §7 rows; the two defect pins; the classification. | **PROVEN** | `H3-SPILL-NLJ-1` (DataFusion's `expect("partition not used yet")` at `repartition/mod.rs:1277`, reached from the join's right side; 3/3 reproducible; every other operator at the same 8 MiB pool refuses cleanly) and `H3-SPILL-COLLECT-1` (null `PyObject` in `collect_rows.rows_from_record_batch` under an `RLIMIT_AS` ceiling; the 6 GiB-headroom control is `ok`). Both pins carry their green control, so a pin that reds has a reason. |
| C-007 | Two or three Apache Spark cells run on the same fixture under a bounded driver heap, with spill measured rather than asserted. | The Spark cells; the event-log totals. | **PROVEN** | PySpark 4.1.2 / Zulu 17, `local[4]`, `spark.driver.memory=1g`, 1e7 rows, `noop` sink (a `count()` lets Spark drop the very `ORDER BY` under test — the first draft measured a plan with no sort). `sort` ok, 318,765,888 B memory / 228,603,560 B disk spilled; `hash_join` ok, 721,418,048 / 433,678,966 B; `collect_list` **error**. The OOM claim is quoted, not inferred: file descriptor 2 is redirected for the run, so `java.lang.OutOfMemoryError: Java heap space` is in the committed evidence under `jvm_error_lines` — the driver only ever sees `Job 0 cancelled because SparkContext was shut down`. The event-log directory is wiped per run, because a second run into the same directory silently doubles the spill totals. |

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
      evidence: The wrong-answer check compares 115 bounded cells (163 run digests, every repeat) against the unbounded run, each with a disclosed content digest; four probe constructions that could not have caught a real divergence, or that distorted the cell they measured, were found and replaced before the matrix was believed.
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
      evidence: Outcomes are read from EXPLAIN ANALYZE counters, never from wall time; the one outcome-unstable cell is named in the document and carries no pin; every non-ok cell ran three times and every run's outcome AND digest are in the JSON, so the answer check is not first-run-only on exactly the unstable cells.
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

## Round-2 review gaps (Opus critic, 2026-09-05)

The critic re-ran 79 cells on its own release module — 79/79 outcomes identical, 41 bounded
digests equal, spills confirmed on disk, both registry rows reproduced, both mutations red — and
returned FAIL on five S2 and three S3 findings. **Every finding was about what the matrix
*checked* and *claimed*, not about what it measured; no measured outcome changed in round 2.**

| # | Finding | Remediation |
|---|---|---|
| S2-1 | The five `api` rows used `str(rows_out)` — a row count — as their answer digest, so 37 bounded cells were counted as answer-checked on a count alone, and `collect` at 1e7 (the product's largest unbounded allocation) was the least-checked answer in the matrix. | Every `api` row now hashes what it produced: `collect` per-row `crc32(str(tuple(row)))` summed and xored; `toPandas` `hash_pandas_object` summed in 100 k-row chunks; `dynamicFlatten`, the DV scan and the MERGE target `count(*)` + `sum(crc32(...))` over every column. Each kind is disclosed in `digest_kind`, in baseline §1.1 and in the harness map. All 50 cells re-run. |
| S2-2 | `repartition` had no `digest_sql`, so its 10 cells — two of them "spilled and answered exactly" — had zero wrong-answer coverage, on one of the four operators DataFusion can spill. | Order-independent digest added (`count`, `sum(c)`, `max(c)`, `sum(crc32(h))`); 10 cells re-run; `without_because_no_probe` is now 0 by measurement. |
| S2-3 | "72 bounded cells produced a digest and all 72 matched … refusals explain why the count is 72 and not 144" was both the wrong count and the wrong reason. | Corrected everywhere from the tool's own census: **115 of 144** bounded cells carry a digest, **163 run digests** counting repeats, 0 mismatches; the 29 without are 28 refusals + 1 probe failure. The census is now computed by `report.py --section census`, not written by hand. |
| S2-4 | The Spark `collect_list` OOM was published as measured but the committed evidence held only the driver-side `Job 0 cancelled because SparkContext was shut down`; the JVM's stderr was never captured. | `spark_cells.py` redirects file descriptor 2 for the run and records `jvm_error_lines` + a stderr tail. All three Spark cells re-run; `java.lang.OutOfMemoryError: Java heap space` is now in the evidence and is what §5 quotes. A second bug surfaced with it: re-running into an existing event-log directory doubled the spill totals, so the directory is wiped per run. |
| S2-5 | Repeats kept only the outcome and discarded the digest, so the wrong-answer check was first-run-only on exactly the nondeterministic cells. | `CellRecord.run_digests` keeps one digest per run; a cell whose own runs disagree is `wrong`, and so is one whose runs do not contain the unbounded digest. The whole matrix was re-run rather than patched, so every cell comes from one harness version. |
| S3-1 | `spark_cells.py` still used `ru_maxrss` (the launcher, not the JVM) and carried a dead `rows_out: -1`. | `RUSAGE_CHILDREN` measured 0 on this path, so the field is **dropped** rather than published as a zero; `rows_out` deleted. |
| S3-2 | A stray generator heading and duplicate table sat inside baseline §9.1. | §3 and §9 are now emitted by `report.py --section outcomes|numbers` with their own numbering, so the document is assembled by concatenation and the class of bug is gone. |
| S3-3 | `PROJECT.md` still called the claim "pending a spill-coverage spike". | Replaced with a pointer to the baseline, keeping the honest "spills where the engine can, documented where it cannot". |

One remediation changed a measured number rather than a claim, and it is worth naming: the
first version of the `toPandas` digest hashed the frame whole and pushed the worker past the
8 GiB resident cap, so two cells recorded `abort_at_cap` — the probe's memory, not the operator's.
Chunking the probe returned those cells to `ok` at 2,078-2,138 MiB. A probe that changes the
measurement is not a probe.

## Gates

Round 2, re-run after every remediation.

| Gate | Exit |
|---|---|
| `make ci` | 0 (inside `verify`) |
| `make verify` (= `ci` + the Rust workspace suite) | 0 |
| `make check-python-conventions` | 0 |
| `make rust-panic-ban` | 0 (inside `ci`) |
| `.venv/bin/python -m pytest python/repark/tests -q -p no:randomly -rs` | 0 (4823 passed, 198 skipped, 208 s) |
| `.venv/bin/python -m pytest python/repark-parity/tests -q -p no:randomly` | 0 (574 passed) |
| `.venv/bin/python -m pytest python/repark/tests/test_h3_spill_matrix.py -q` | 0 (22 passed, 43 s) |
| `REPARK_PARITY_LIVE=1 pytest test_parity_live.py -q` (Spark cells were measured) | 0 (119 passed, 65 s) |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 (needs `origin/main` present; it is, in this lane) |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |

The skip count is the same 198 as round 1, and 4823 = round 1's 4821 plus the two pins added here;
`-rs` names every skip and all of them are the documented environment gates (live tier off, real
AWS off, an empty fuzz-repro parameter set). An earlier round-2 run of the same suite read
4806/215 — 17 extra skips — because it was scheduled **alongside the live oracle leg's JVM**, and
the live-cell guards skip rather than fight a session that is already up. That is a scheduling
mistake in the gate run, not a result: the suite is recorded from the isolated run.

Provocation proofs for the two round-2 pins: a constant boundary digest reds
`test_the_boundary_digest_is_order_independent_and_content_sensitive` and leaves
`test_the_facade_boundary_answers_the_same_at_every_pool` green — which is exactly why the second
pin exists, since an equality between two constants proves nothing on its own.
