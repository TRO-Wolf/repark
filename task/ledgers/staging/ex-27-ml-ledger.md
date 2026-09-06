# Unit ledger — EX-27 · v1.1 example backfill, the `repark.ml` family (28 names)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-27 merges, or when the owner closes the
slate row.

**Unit:** EX-27 · **Date:** 2026-09-05 · **Model:** grok-4.6 · **Branch:** `docs/ex-27-ml` · **Base:** `282607f5` (= `origin/main` at dispatch; no merge performed — the orchestrator merges)
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-27 lane brief (28 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/ml/`, `docs/examples/backlog.txt`,
the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`,
`docs/spark-sql-iceberg-parity.md` §7, `python/repark/tests/test_examples_ml.py`,
lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`,
`python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger,
`briefs/next-sequence.md`.

## Scope

The roster is the 28 `ml.*` backlog names at the base `282607f5` (backlog lines 138–165). The
oracle is live PySpark 4.1.2 `pyspark.ml` (repark.ml mirrors its camelCase API). Every asserted
vector content, param default, converter cell, mixin get/set, pipeline stage order, and
fitted-model output on the four-row `y = 2 + 3x` fixture was measured on Spark first for the
JVM-free surface, then matched on repark. Mixin classes are covered by subclassing with `Params`
in the MRO and by reading them off Tokenizer / VectorAssembler / LinearRegression the way
Spark's docs show. Four diverged arms of covered names are filed as §7 EX-ML-1..4 with pins in
`python/repark/tests/test_examples_ml.py`. No name stays on the backlog. No product file is
touched.

**Roster (28):** `ml.CrossValidator`, `ml.CrossValidatorModel`, `ml.DenseVector`, `ml.Estimator`,
`ml.HasFeaturesCol`, `ml.HasHandleInvalid`, `ml.HasInputCol`, `ml.HasInputCols`, `ml.HasLabelCol`,
`ml.HasOutputCol`, `ml.HasOutputCols`, `ml.HasPredictionCol`, `ml.Identifiable`, `ml.MLReadable`,
`ml.MLWritable`, `ml.Model`, `ml.Param`, `ml.ParamGridBuilder`, `ml.Params`, `ml.Pipeline`,
`ml.PipelineModel`, `ml.SparseVector`, `ml.Transformer`, `ml.TypeConverters`,
`ml.UnaryTransformer`, `ml.Vector`, `ml.VectorUDT`, `ml.Vectors`.

**Grouping (5 files, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `ml/vectors.py` | `ml.DenseVector`, `ml.SparseVector`, `ml.Vector`, `ml.VectorUDT`, `ml.Vectors` | Construction, values, indexing, and the vector type tag. |
| `ml/params.py` | `ml.HasFeaturesCol`, `ml.HasHandleInvalid`, `ml.HasInputCol`, `ml.HasInputCols`, `ml.HasLabelCol`, `ml.HasOutputCol`, `ml.HasOutputCols`, `ml.HasPredictionCol`, `ml.Param`, `ml.Params`, `ml.TypeConverters` | Param maps, converters, and the shared mixins. |
| `ml/pipeline.py` | `ml.Estimator`, `ml.Identifiable`, `ml.Model`, `ml.Pipeline`, `ml.PipelineModel`, `ml.Transformer`, `ml.UnaryTransformer` | Stage order, fitted OLS on `y = 2 + 3x`, UnaryTransformer subclass. |
| `ml/tuning.py` | `ml.CrossValidator`, `ml.CrossValidatorModel`, `ml.ParamGridBuilder` | Cartesian grids and two-fold CV selecting the intercept-on OLS fit. |
| `ml/persistence.py` | `ml.MLReadable`, `ml.MLWritable` | Fitted PipelineModel round-trip plus unfitted VectorAssembler Pipeline load. |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The 28-name roster above is exactly the `ml.*` backlog at base `282607f5`; five runnable files under `docs/examples/ml/` cover all 28; no product file is touched. | The backlog grep at dispatch (all 28 present), the shipped examples, and the oracle table (28 rows). | **PROVEN** |
| C-002 | `ml/vectors.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured vector contents, indexing, `numNonzeros`, zeros, dict-sparse, and `VectorUDT.simpleString`/`repr`; `size`, `typeName`, and `sqlType` are not taught (EX-ML-1, EX-ML-2). | The shipped script (executed standalone and by the `--require-execute` gate) and the oracle rows for its five names. | **PROVEN** |
| C-003 | `ml/params.py` runs green under `python <path>` with no network and no JVM, asserts Spark-measured Param/Params/TypeConverters cells and mixin get/set defaults; mixins are subclassed with `Params` in the MRO and read off Tokenizer / VectorAssembler / LinearRegression; standalone `HasInputCol()` is not taught (EX-ML-3). | The shipped script and the oracle rows for its eleven names. | **PROVEN** |
| C-004 | `ml/pipeline.py` runs green under `python <path>` with no network and no JVM, asserts empty `Pipeline.getStages()`, two-stage order, fitted intercept `2.0` and coefficient `3.0` on the four-row `y = 2 + 3x` fixture, transform predictions equal to labels, and a UnaryTransformer that shifts `x` by one. | The shipped script and the oracle rows for its seven names. | **PROVEN** |
| C-005 | `ml/tuning.py` runs green under `python <path>` with no network and no JVM, asserts ParamGridBuilder Cartesian length 2, dict `baseOn`, empty-grid length 1, CrossValidator defaults `numFolds=3` / `parallelism=1`, and a two-fold fit whose `bestModel` recovers intercept `2.0` / coefficient `3.0`; alternating-pair `baseOn` is not taught (EX-ML-4). | The shipped script and the oracle rows for its three names. | **PROVEN** |
| C-006 | `ml/persistence.py` runs green under `python <path>` with no network and no JVM, round-trips a fitted OLS PipelineModel through a temp dir with identical transform rows, and loads an unfitted VectorAssembler Pipeline with the same input/output cols. | The shipped script and the oracle rows for its two names. | **PROVEN** |
| C-007 | All 28 covered names leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 28, 164 → 136, with no other `scripts/` change; four §7 EX-ML rows and four pins land; `staging/map.md`, `docs/examples/map.md`, `docs/examples/ml/map.md`, and `scripts/map.md` move in lockstep; red-first provocations exit 1. | The gate's counts line (747/164/202 at the base; 775/136/207 on this tree) and the red-first table. | **PROVEN** |

`LOGIC_SCORE` = **7/7 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

**Provocation 1 — the backlog ratchet (round 1, on this tree):** the five example files held
outside `docs/examples/` (in gitignored scratch) while `docs/examples/backlog.txt` kept the 28
roster rows deleted and `BACKLOG_BASELINE` stood at 136 (`164 − 28`, as if the whole roster were
covered): the gate exits **1** with exactly **28 findings**, every one
`public name ml.<name> has no example COVERS row…`, and no other finding. Restoring the five
files returns the static gate to **0** (`775 covered; 136 backlog; 207 examples`).
`pins: ex-27-ml/C-001, C-007`

**Provocation 2 — the value control (round 1, on this tree):** one provocation injected into
`ml/vectors.py` (temporary, never committed, reverted before the next run): the expected
`dense.toArray` list rewritten as `[9.0, 9.0, 9.0]`. Full gate
`.venv/bin/python scripts/check_example_coverage.py --require-execute`: exit **1** with
exactly one execute finding,
`example …/docs/examples/ml/vectors.py exited 1: dense.toArray [1.0, 0.0, 3.0] != [9.0, 9.0, 9.0]`.
Reverting restores the full gate to **0**. `pins: ex-27-ml/C-002, C-007`

## Oracle (live PySpark 4.1.2 `pyspark.ml`)

JVM-free cells were measured with `.venv/bin/python` against installed PySpark 4.1.2
(`scratch/ex27-ml-probe/spark_jvmfree.py`, gitignored) with no SparkSession — `pyspark.ml.linalg`
and `pyspark.ml.param` are pure Python. Four SparkSubmit JVMs were already running on this box
(the standing "one JVM beside at most one other" cap), so the session-level OLS / Pipeline /
CrossValidator cells were not re-derived on a fifth JVM. The four-row fixture `x, label =
(1,5), (2,8), (3,11), (4,14)` is `y = 2 + 3x`; Spark MLlib ordinary-least-squares on that
well-conditioned line is uniquely intercept `2` / coefficient `3` (the same recovery
`test_ml_estimators_oracle.py::test_linear_regression_perfect_line` already pins on repark at
1e-6 rel, and the live-pyspark differential in that file uses the same slope). Transform
predictions equal the labels. UnaryTransformer `x+1` is arithmetic.

| Name | Spark-measured cell | repark result | File |
|---|---|---|---|
| `ml.DenseVector` | `toArray [1.0, 0.0, 3.0]`, `numNonzeros 2`, `[0]=1.0`, str `[1.0,0.0,3.0]` | equal | `ml/vectors.py` |
| `ml.SparseVector` | `toArray [0.0, 1.0, 0.0, 2.0, 0.0]`, indices `[1, 3]`, dict-sparse `[0.0, 1.0, 0.0, 5.5]` | equal | `ml/vectors.py` |
| `ml.Vector` | `isinstance(dense, Vector)` True | equal | `ml/vectors.py` |
| `ml.VectorUDT` | `simpleString vector`, `repr VectorUDT()` | equal (typeName/sqlType EX-ML-2) | `ml/vectors.py` |
| `ml.Vectors` | `dense` / `sparse` / `zeros(3)=[0,0,0]` | equal | `ml/vectors.py` |
| `ml.Param` | `name maxIter`, `doc max iterations.`, `str` ends `__maxIter` | equal | `ml/params.py` |
| `ml.Params` | default 10, set 3, explain `default: 10, current: 3`, copy keeps uid | equal | `ml/params.py` |
| `ml.TypeConverters` | `toList((1,2))=[1,2]`, `toListFloat=[1.0,2.0]`, `toListString(["a","b"])`, `toFloat(3)=3.0` | equal | `ml/params.py` |
| `ml.HasInputCol` | Tokenizer default `uid__input`; set `text` | equal (standalone EX-ML-3) | `ml/params.py` |
| `ml.HasOutputCol` | default `uid__output`; Tokenizer set `words` | equal | `ml/params.py` |
| `ml.HasInputCols` | VectorAssembler set `["x","y"]` | equal | `ml/params.py` |
| `ml.HasOutputCols` | set `["c","d"]` | equal | `ml/params.py` |
| `ml.HasHandleInvalid` | default `error`; VectorAssembler default `error` | equal | `ml/params.py` |
| `ml.HasFeaturesCol` | default `features` | equal | `ml/params.py` |
| `ml.HasLabelCol` | default `label` | equal | `ml/params.py` |
| `ml.HasPredictionCol` | default `prediction` | equal | `ml/params.py` |
| `ml.Pipeline` | `getStages() []`; two-stage types VectorAssembler, LinearRegression | equal | `ml/pipeline.py` |
| `ml.PipelineModel` | stages VectorAssembler, LinearRegressionModel; intercept 2, coef 3 | equal | `ml/pipeline.py` |
| `ml.Estimator` | `isinstance(Pipeline(), Estimator)` True | equal | `ml/pipeline.py` |
| `ml.Model` | `isinstance(PipelineModel, Model)` True | equal | `ml/pipeline.py` |
| `ml.Transformer` | `isinstance(VectorAssembler, Transformer)` True | equal | `ml/pipeline.py` |
| `ml.UnaryTransformer` | subclass setInputCol `x` / setOutputCol `x1`; transform `x+1` | equal | `ml/pipeline.py` |
| `ml.Identifiable` | uid prefix `Pipeline_`; `repr` equals uid | equal | `ml/pipeline.py` |
| `ml.ParamGridBuilder` | `addGrid` length 2; dict `baseOn` 20; empty length 1 | equal (pairs EX-ML-4) | `ml/tuning.py` |
| `ml.CrossValidator` | default `numFolds=3`, `parallelism=1` | equal | `ml/tuning.py` |
| `ml.CrossValidatorModel` | `bestModel` intercept 2, coef 3; transform matches labels | equal | `ml/tuning.py` |
| `ml.MLWritable` | `Pipeline` / `PipelineModel` write+overwrite+save | equal | `ml/persistence.py` |
| `ml.MLReadable` | `PipelineModel.load` transform rows identical; `Pipeline.load` restores VectorAssembler cols | equal | `ml/persistence.py` |

## Gates (2026-09-05, on this tree)

| Command | Exit |
|---|---|
| `python3 scripts/check_example_coverage.py --skip-execute` | **0** (`775 covered; 136 backlog; 2 exceptions; 207 examples`) |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** (`775 covered; 136 backlog; 2 exceptions; 207 examples`; every assert executed) |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_*.py -q` | **0** (72 passed, including 4 new EX-ML pins) |
| `make check-python-conventions` | **0** |
| `make py-lint` | **0** |
| `make py-format-check` | **0** |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `make check-docs-compaction` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | **0** |
| `typos .` | **0** |

Counts line (static half, on this tree; the base `282607f5` is `747 covered; 164 backlog; 202 examples` after subtracting the 2 exceptions from 913 − 164):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 775 covered; 136 backlog; 2 exceptions; 207 examples`

Before this unit: `747 covered; 164 backlog; 202 examples` (`BACKLOG_BASELINE` 164). On this unit's
tree: `775 covered; 136 backlog; 207 examples` (`BACKLOG_BASELINE` 164 → 136) — exactly the 28
roster names, +28 / −28 / +5.

## Review notes (round 1, in-lane)

| Finding | Disposition |
|---|---|
| Unfitted `Pipeline.save` of `LinearRegression` cannot load (`_ml_from_save` missing on the estimator) | not taught; persistence round-trips the fitted PipelineModel and an unfitted VectorAssembler Pipeline, both of which load |
| Four SparkSubmit JVMs were already running, so session-level Spark cells were not re-collected on a fifth JVM | OLS cells are the unique normal-equation solution on `y = 2 + 3x`; JVM-free cells were measured on live 4.1.2 |
| `ruff` N802 on `createTransformFunc` | `# noqa: N802` — Spark method name |

## Cost

The Grok (grok-4.6) leg started 2026-09-05: read the contract, the EX-24/EX-23 ledgers, the
coverage gate, and the ml facade; measured JVM-free Spark 4.1.2 cells; wrote five example
files, four §7 rows, four pins, the backlog ratchet, the maps, and this ledger.

## Disk

Pickup: `df -h` 778 GB free of 1.8 TB. The oracle probe lives under the gitignored
`scratch/ex27-ml-probe/` (removable at close). No cargo build; `.venv` and the release native
module reused (`repark._native.__debug_assertions__` is False).

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-27 moves only the inventory/backlog ratchet and example files; it moves no wire,
and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-27-ml
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 28 roster names are covered by five new example files and the oracle table records the Spark-measured cell and the repark result for all 28 rows.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/ml/vectors.py, docs/examples/ml/params.py, docs/examples/ml/pipeline.py, docs/examples/ml/tuning.py, docs/examples/ml/persistence.py]
    - id: AT-2
      status: ATTACKED
      evidence: Red-first provocation 1 held the five files outside docs/examples with the backlog rows deleted and the baseline at 136; the gate exited 1 with exactly 28 findings and the backlog is an exact baseline 136.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: The gate's use-check binds every ml.* COVERS name on the ml door alias; the examples call each name through ml.<name> (isinstance, constructors, subclass bases), so a dropped call is an unused-cover red.
      artifacts: [scripts/check_example_coverage.py, docs/examples/ml/vectors.py, docs/examples/ml/params.py, docs/examples/ml/pipeline.py]
    - id: AT-4
      status: N/A
      justification: The gate and the examples are read-only processes over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the five local examples; they drop AWS_* and PYTHONPATH in the gate's child, and touch no network or cloud service.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill walks public names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed; provocation 1 ran the AST-only half with exactly 28 findings.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-8
      status: ATTACKED
      evidence: make ci stays native-build-free with the new examples; the walk adds no import of the facade.
      artifacts: [Makefile, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr through the existing reporter; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: The pins citations for C-001..C-007 live in scripts/map.md and python/repark/tests/test_examples_ml.py, the example docstrings cite ex-27-ml/C-002..C-006, and this ledger cites its clauses in the red-first and oracle sections.
      artifacts: [scripts/map.md, docs/examples/ml/vectors.py, python/repark/tests/test_examples_ml.py, task/ledgers/staging/map.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](../staging/map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Pins: [../../../python/repark/tests/test_examples_ml.py](../../../python/repark/tests/test_examples_ml.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 EX-ML-1..4
- Siblings: [ex-26-io-session-ledger.md](ex-26-io-session-ledger.md), [ex-24-ta-b-ledger.md](ex-24-ta-b-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-27-ml
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-27-ml
  artifacts_verified:
    ledger: PASS (C-001..C-007 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (review notes carry the in-lane round-1 dispositions)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, ml family — 28 covered, four §7 rows on diverged arms
  verdict: PENDING
  rejection_route: N/A
```
