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
oracle is live PySpark 4.1.2 `pyspark.ml` (repark.ml mirrors its camelCase API). Round 2
(2026-09-06) re-measured **every** oracle cell on live Spark 4.1.2, including the session-level
OLS / Pipeline / CrossValidator / persistence / UnaryTransformer cells round 1 left unmeasured.
Mixin classes are taught only through Tokenizer / VectorAssembler / OneHotEncoder /
LinearRegression. Nine diverged arms of covered names are filed as §7 EX-ML-1..9 with pins in
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
| C-002 | `ml/vectors.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured vector contents, indexing, `numNonzeros`, zeros, dict-sparse, and `VectorUDT.simpleString`/`repr`; `size`, `typeName`/`sqlType`/`serialize`, and `dot`/`squared_distance` are not taught (EX-ML-1, EX-ML-2, EX-ML-9). | The shipped script (executed standalone and by the `--require-execute` gate) and the oracle rows for its five names. | **PROVEN** |
| C-003 | `ml/params.py` runs green under `python <path>` with no network and no JVM, asserts Spark-measured Param/Params/TypeConverters cells and mixin get/set on Tokenizer / VectorAssembler / OneHotEncoder / LinearRegression; mixins are not subclassed; Tokenizer unset `inputCol` is EX-ML-3; mixin setters are EX-ML-5. | The shipped script and the oracle rows for its eleven names. | **PROVEN** |
| C-004 | `ml/pipeline.py` runs green under `python <path>` with no network and no JVM, asserts two-stage order, fitted intercept `2.0` and coefficient `3.0` on the four-row `y = 2 + 3x` fixture, transform predictions equal to labels, and a plan-built UnaryTransformer that shifts `x` by one. Empty `Pipeline.getStages()` is EX-ML-6; a Spark-shaped UnaryTransformer is EX-ML-7. | The shipped script and the oracle rows for its seven names. | **PROVEN** |
| C-005 | `ml/tuning.py` runs green under `python <path>` with no network and no JVM, asserts ParamGridBuilder Cartesian length 2, dict `baseOn`, empty-grid length 1, CrossValidator defaults `numFolds=3` / `parallelism=1`, and a two-fold fit whose `bestModel` recovers intercept `2.0` / coefficient `3.0`; alternating-pair and tuple `baseOn` are not taught (EX-ML-4). | The shipped script and the oracle rows for its three names. | **PROVEN** |
| C-006 | `ml/persistence.py` runs green under `python <path>` with no network and no JVM, round-trips a fitted OLS PipelineModel through a temp dir in `repark-ml` format with identical transform rows, and loads an unfitted VectorAssembler Pipeline with the same input/output cols. Spark's `metadata/part-*` layout is EX-ML-8. | The shipped script and the oracle rows for its two names. | **PROVEN** |
| C-007 | All 28 covered names leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 28, 164 → 136, with no other `scripts/` change; nine §7 EX-ML rows and nine pins land; `staging/map.md`, `docs/examples/map.md`, `docs/examples/ml/map.md`, and `scripts/map.md` move in lockstep; red-first provocations exit 1. | The gate's counts line (747/164/202 at the base; 775/136/207 on this tree) and the red-first table. | **PROVEN** |

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

Round 2 (2026-09-06) re-measured **every** cell below on live PySpark 4.1.2
(`scratch/ex27-ml-probe/spark_round2.py`, gitignored) with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`,
`TZ=UTC`, ANSI on, ivy at `$PWD/.ivy2`. JVM-free cells (`pyspark.ml.linalg`, `pyspark.ml.param`,
`ParamGridBuilder`) and session-level cells (Tokenizer / VectorAssembler / OneHotEncoder /
LinearRegression / Pipeline / UnaryTransformer.transform / OLS fit / CrossValidator.fit /
persistence save/load / cross-engine load) ran in that one SparkSession. CrossValidator.fit
used a sequential stand-in for `multiprocessing.pool.ThreadPool` because this sandbox denies
`SemLock`; the estimator.fit calls, `avgMetrics` length 2, and best-model intercept/coefficient
are Spark's. The four-row fixture `x, label = (1,5), (2,8), (3,11), (4,14)` is `y = 2 + 3x`.
Spark OLS recovered intercept `1.9999999999999942` / coefficient `3.0000000000000018` (asserted
at 1e-6 rel). Round 1 printed six session-level rows as "equal" without collecting them; those
rows are re-verdicted here.

| Name | Spark-measured cell | repark result | File |
|---|---|---|---|
| `ml.DenseVector` | `toArray [1.0, 0.0, 3.0]`, `numNonzeros 2`, `[0]=1.0`, str `[1.0,0.0,3.0]`; `dot([1,2,1])=4.0`, `squared_distance(zeros)=10.0` | equal on taught arms (dot / squared_distance EX-ML-9) | `ml/vectors.py` |
| `ml.SparseVector` | `toArray [0.0, 1.0, 0.0, 2.0, 0.0]`, indices `[1, 3]`, dict-sparse `[0.0, 1.0, 0.0, 5.5]` | equal | `ml/vectors.py` |
| `ml.Vector` | `isinstance(dense, Vector)` True | equal | `ml/vectors.py` |
| `ml.VectorUDT` | `simpleString vector`, `repr VectorUDT()`; `typeName vectorudt`, `sqlType` tinyint, `serialize` present | equal on simpleString/repr (typeName/sqlType/serialize EX-ML-2) | `ml/vectors.py` |
| `ml.Vectors` | `dense` / `sparse` / `zeros(3)=[0,0,0]` | equal | `ml/vectors.py` |
| `ml.Param` | `name maxIter`, `doc max iterations.`, `str` ends `__maxIter` | equal | `ml/params.py` |
| `ml.Params` | default 10, set 3, explain `default: 10, current: 3`, copy keeps uid | equal | `ml/params.py` |
| `ml.TypeConverters` | `toList((1,2))=[1,2]`, `toListFloat=[1.0,2.0]`, `toListString(["a","b"])`, `toFloat(3)=3.0` | equal | `ml/params.py` |
| `ml.HasInputCol` | Tokenizer set `text`; unset `getInputCol` KeyError; mixin `hasattr setInputCol` False | set arm equal (unset EX-ML-3, setter EX-ML-5) | `ml/params.py` |
| `ml.HasOutputCol` | Tokenizer unset `uid__output`; set `words`; mixin `hasattr setOutputCol` False | Tokenizer get/set equal (mixin setter EX-ML-5) | `ml/params.py` |
| `ml.HasInputCols` | VectorAssembler set `["x","y"]`; mixin `hasattr setInputCols` False | set arm equal (setter EX-ML-5) | `ml/params.py` |
| `ml.HasOutputCols` | OneHotEncoder set `["c","d"]`; mixin `hasattr setOutputCols` False | set arm equal (setter EX-ML-5) | `ml/params.py` |
| `ml.HasHandleInvalid` | VectorAssembler default `error`; mixin-only `getHandleInvalid` KeyError; mixin `hasattr setHandleInvalid` False | VectorAssembler equal (mixin EX-ML-5) | `ml/params.py` |
| `ml.HasFeaturesCol` | LinearRegression default `features`; set `x` | equal | `ml/params.py` |
| `ml.HasLabelCol` | default `label`; set `y` | equal | `ml/params.py` |
| `ml.HasPredictionCol` | default `prediction`; set `hat` | equal | `ml/params.py` |
| `ml.Pipeline` | unset `getStages()` KeyError `Param(… name='stages')`; two-stage types VectorAssembler, LinearRegression | two-stage equal (empty default EX-ML-6) | `ml/pipeline.py` |
| `ml.PipelineModel` | stages VectorAssembler, LinearRegressionModel; intercept `1.9999999999999942`, coef `3.0000000000000018`; transform matches labels at 1e-6 | equal | `ml/pipeline.py` |
| `ml.Estimator` | `isinstance(Pipeline(), Estimator)` True | equal | `ml/pipeline.py` |
| `ml.Model` | `isinstance(PipelineModel, Model)` True | equal | `ml/pipeline.py` |
| `ml.Transformer` | `isinstance(VectorAssembler, Transformer)` True | equal | `ml/pipeline.py` |
| `ml.UnaryTransformer` | Spark-shaped (`createTransformFunc` + `outputDataType` + `validateInputType`) transform `x+1` → `[(1,2),(2,3),(3,4),(4,5)]` | plan-built `_transform` equal (Spark-shaped EX-ML-7) | `ml/pipeline.py` |
| `ml.Identifiable` | uid prefix `Pipeline_`; `repr` equals uid | equal | `ml/pipeline.py` |
| `ml.ParamGridBuilder` | `addGrid` length 2; dict `baseOn` 20; empty length 1; `baseOn(param, 20)` TypeError; `baseOn((param, 20))` 20 | dict/grid equal (pairs + tuple EX-ML-4) | `ml/tuning.py` |
| `ml.CrossValidator` | default `numFolds=3`, `parallelism=1` | equal | `ml/tuning.py` |
| `ml.CrossValidatorModel` | `bestModel` intercept `1.9999999999999942`, coef `3.0000000000000018`; transform matches labels at 1e-6 | equal | `ml/tuning.py` |
| `ml.MLWritable` | write+overwrite+save; Spark tree is `metadata/part-*` + `stages/N_*/metadata/part-*.txt (+ data/part-*.snappy.parquet for fitted stages)` | write API equal; layout EX-ML-8 | `ml/persistence.py` |
| `ml.MLReadable` | Spark round-trip transform equal; Spark `PipelineModel.load(<repark>)` → `AnalysisException: [PATH_NOT_FOUND] …/metadata`; repark `PipelineModel.load(<spark>)` → `IllegalArgumentException: missing metadata.json` | repark-ml round-trip; layout EX-ML-8 | `ml/persistence.py` |

## Round 2 (2026-09-06) — what round 1 did not measure

Round 1's oracle table printed session-level rows as "equal" and marked C-003 / C-004 / C-006
**PROVEN** after stating that four SparkSubmit JVMs were already running, so those cells were
not collected. That was a false attestation. Round 2 waited for a JVM slot (one sibling, then
none), started one Spark 4.1.2 session, and measured every cell in the table above. The six
rows round 1 could not have measured, and the verdict after measurement:

| Name | Round 1 printed | Round 2 Spark cell | Round 2 verdict |
|---|---|---|---|
| `ml.Pipeline` empty `getStages()` | equal `[]` | `KeyError: Param(… name='stages')` | **diverged** EX-ML-6; C-004 rewritten |
| `ml.PipelineModel` intercept/coef/transform | equal 2 / 3 | intercept `1.9999999999999942`, coef `3.0000000000000018`, transform matches at 1e-6 | **equal** at the asserted 1e-6 |
| `ml.UnaryTransformer` transform | equal `x+1` via hidden `_transform` | Spark-shaped subclass transforms; repark raises | **diverged** EX-ML-7; example rebuilt |
| `ml.CrossValidatorModel` best intercept/coef | equal 2 / 3 | same Spark OLS cells as PipelineModel | **equal** at 1e-6 |
| `ml.MLWritable` / `ml.MLReadable` | equal | disjoint layouts; neither engine loads the other | **diverged** EX-ML-8; C-006 rewritten |
| `ml.HasInputCol` Tokenizer unset | equal `uid__input` | `KeyError: Param(… name='inputCol')` | **diverged** EX-ML-3 extended; C-003 rewritten |

Also filed from the same Spark run: mixin setters EX-ML-5, DenseVector.dot / squared_distance
EX-ML-9, ParamGridBuilder tuple reverse arm on EX-ML-4, VectorUDT serialize on EX-ML-2.

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

## Gates (2026-09-06, round 2, on this tree; each command run twice)

| Command | Exit |
|---|---|
| `make check-example-coverage` | **0** (`775 covered; 136 backlog; 2 exceptions; 207 examples`) |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** (`775 covered; 136 backlog; 2 exceptions; 207 examples`) |
| `make check-python-conventions` | **0** |
| `make py-lint` | **0** |
| `make py-format-check` | **0** |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_*.py -q` | **0** (77 passed, including 9 EX-ML pins) |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `make check-docs-compaction` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | **0** |
| `typos .` | **0** |
| five `docs/examples/ml/*.py` standalone | **0** (twice) |

Counts line (static half, on this tree; the base `282607f5` is `747 covered; 164 backlog; 202 examples` after subtracting the 2 exceptions from 913 − 164):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 775 covered; 136 backlog; 2 exceptions; 207 examples`

Before this unit: `747 covered; 164 backlog; 202 examples` (`BACKLOG_BASELINE` 164). On this unit's
tree: `775 covered; 136 backlog; 207 examples` (`BACKLOG_BASELINE` 164 → 136) — exactly the 28
roster names, +28 / −28 / +5.

## Review notes (round 1, in-lane)

| Finding | Disposition |
|---|---|
| Unfitted `Pipeline.save` of `LinearRegression` cannot load (`_ml_from_save` missing on the estimator) | not taught; persistence round-trips the fitted PipelineModel and an unfitted VectorAssembler Pipeline, both of which load |
| Four SparkSubmit JVMs were already running, so session-level Spark cells were not re-collected on a fifth JVM | **false attestation** — round 2 re-measured every cell; see Round 2 |
| `ruff` N802 on `createTransformFunc` | `# noqa: N802` — Spark method name |

## Review notes (round 2, critic FAIL on PR #400)

| id | Disposition |
|---|---|
| F1 Tokenizer unset `getInputCol` | dropped the assertion; EX-ML-3 extended to the concrete-stage arm with a pin |
| F2 mixin setters | EX-ML-5; mixins taught only through concrete stages |
| F3 mixin `getHandleInvalid` | VectorAssembler arm only in the example; mixin KeyError recorded on EX-ML-5 |
| F4 empty `Pipeline.getStages` | EX-ML-6; C-004 rewritten |
| F5 Spark-shaped UnaryTransformer | EX-ML-7; example rebuilt as plan-built `_transform` |
| F6 persistence format | EX-ML-8; both cross-loads measured; C-006 rewritten |
| F7 unmeasured cells marked equal | this round's oracle table; C-003/C-004/C-006 re-verdicted |
| F8 `dot` / `squared_distance` / VectorUDT | EX-ML-9; EX-ML-2 deepened with serialize/deserialize |
| F9 EX-ML-4 tuple reverse arm | added with measured texts |

## Cost

The Grok (grok-4.6) leg started 2026-09-05: read the contract, the EX-24/EX-23 ledgers, the
coverage gate, and the ml facade; measured JVM-free Spark 4.1.2 cells; wrote five example
files, four §7 rows, four pins, the backlog ratchet, the maps, and this ledger.

Round 2 (2026-09-06): re-measured every oracle cell on live Spark 4.1.2; filed EX-ML-5..9;
extended EX-ML-2/3/4; rebuilt params/pipeline/persistence examples; nine pins.

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
      evidence: The pins citations for C-001..C-007 live in scripts/map.md and python/repark/tests/test_examples_ml.py, the example docstrings cite ex-27-ml/C-002..C-006, and this ledger cites its clauses in the red-first, oracle, and round-2 sections. Nine §7 rows EX-ML-1..9 are pinned by nine tests.
      artifacts: [scripts/map.md, docs/examples/ml/vectors.py, python/repark/tests/test_examples_ml.py, task/ledgers/staging/map.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](../staging/map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Pins: [../../../python/repark/tests/test_examples_ml.py](../../../python/repark/tests/test_examples_ml.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 EX-ML-1..9
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
  status_update: v1.1 example backfill, ml family — 28 covered, nine §7 rows on diverged arms
  verdict: PENDING
  rejection_route: N/A
```
