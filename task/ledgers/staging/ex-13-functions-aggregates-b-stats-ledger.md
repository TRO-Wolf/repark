# Unit ledger — EX-13 · v0.7 example backfill, `F.*` aggregate (b) and statistics

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the orchestrator's departure move). This file closes when EX-13 merges, or when the owner closes the slate row.

**Unit:** EX-13 · **Date:** 2026-09-03 · **Model:** grok-4.6 (continuation of glm-5.3-flash) · **Branch:** `feat/ex-13-functions-aggregates-b-stats` · **Base:** `32c7f30` (dispatch base `32c7f30`)
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), batch roster aggregate (b) + statistics (24 names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v0.7 — Full example documentation".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`, the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`, lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`, `python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The roster is the 24 `F.*` aggregate (b) and statistics names that were backlog rows at the base `32c7f30`. Four files cover the twenty-two names the live oracle confirms; `F.skewness` and `F.kurtosis` stay on the backlog with both values recorded.

**Roster (24):** `F.corr`, `F.covar_pop`, `F.covar_samp`, `F.kurtosis`, `F.skewness`, `F.std`, `F.stddev`, `F.stddev_pop`, `F.stddev_samp`, `F.var_pop`, `F.var_samp`, `F.variance`, `F.regr_avgx`, `F.regr_avgy`, `F.regr_count`, `F.regr_intercept`, `F.regr_r2`, `F.regr_slope`, `F.regr_sxx`, `F.regr_sxy`, `F.regr_syy`, `F.bit_and`, `F.bit_or`, `F.bit_xor`.

**Grouping (4 files, 4–8 allowed, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `dispersion.py` | `F.std`, `F.stddev`, `F.stddev_pop`, `F.stddev_samp`, `F.var_pop`, `F.var_samp`, `F.variance` | Sample versus population spread on one grouped column; `std`/`stddev`/`stddev_samp` and `variance`/`var_samp` are alias pairs. |
| `covariance.py` | `F.corr`, `F.covar_pop`, `F.covar_samp` | Two-column agreement: Pearson correlation and the two covariance spellings. |
| `regression.py` | `F.regr_avgx`, `F.regr_avgy`, `F.regr_count`, `F.regr_intercept`, `F.regr_r2`, `F.regr_slope`, `F.regr_sxx`, `F.regr_sxy`, `F.regr_syy` | Spark's nine linear-regression aggregates over `(y, x)` pairs. |
| `bit_aggregates.py` | `F.bit_and`, `F.bit_or`, `F.bit_xor` | Bitwise folds over each group's integers. |

`F.col` is already covered by `abs.py`; it is listed where genuinely used and does not move the ratchet.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Four files under `docs/examples/functions/` land runnable local examples for the twenty-two roster names the live oracle confirms, every asserted value measured against PySpark 4.1.2 + Iceberg 1.11.0 before it was written; those twenty-two leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly twenty-two, 777 → 755, with no other `scripts/` change; the two others (`F.skewness`, `F.kurtosis`) stay backlog rows with both values recorded in the oracle table below, and no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture (24 findings before, 0 after), oracle table (24 rows, one per roster name, Spark value + repark value + kept/dropped + file), the four scripts each exit 0, and the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured at `32c7f30` (dispatch base `32c7f30`, before any of this batch's example files existed). At that base — 24 roster rows still in `docs/examples/backlog.txt`, `BACKLOG_BASELINE=777` — `python3 scripts/check_example_coverage.py` and the same with `--require-execute` both exit **0** (`913 public names; 134 covered; 777 backlog; 2 exceptions; 31 examples`). **Provocation:** delete the 24 roster rows from `backlog.txt` and lower `BACKLOG_BASELINE` to 753 (`777 − 24`) with the four example files absent; the same command exits **1** with 24 findings, one per roster name and no others. With the four files present, the twenty-two kept names removed and `BACKLOG_BASELINE=755`, the same command exits **0**; the two dropped names remain backlog rows so the gate does not name them as uncovered.

## Oracle (live PySpark 4.1.2 + Iceberg 1.11.0, JDK 17, warehouse `/tmp/oc-ex13-oracle/`)

Measured with `_live_parity.build_spark_iceberg_engine(Path(tmpdir)).session` at `/tmp/oc-ex13/.venv/bin/python` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `TZ=UTC` exported before the JVM, and `PYTHONPATH=/tmp/oc-ex13/python/repark/tests`, one throwaway script under `/tmp/oc-ex13-oracle/` printing per name Spark and repark values for identical inputs. Inputs: dispersion `v` grouped by `g` in `[(1, 2.0), (1, 4.0), (1, 6.0), (1, 8.0), (1, None), (2, 3.0), (2, None)]`; pairs `(y, x)` grouped by `g` in `[(1, 1.0, 2.0), (1, 2.0, 4.0), (1, 3.0, 5.0), (1, 4.0, 10.0), (1, None, 3.0), (2, 1.0, 2.0)]`; bits `b` grouped by `g` in `[(1, 5), (1, 3), (1, 12), (1, None), (2, 7), (2, None)]`. Group-1 then group-2. Float pairs that differ only past `rel_tol=1e-12` are MATCH. `pins: ex-13-functions-aggregates-b-stats/C-001`

| Name | Spark value (repr) | repark value (repr) | Kept / dropped | File | Note |
|---|---|---|---|---|---|
| `F.std` | `[2.581988897471611, None]` | same | kept | `dispersion.py` | alias of `stddev` |
| `F.stddev` | `[2.581988897471611, None]` | same | kept | `dispersion.py` | sample spelling |
| `F.stddev_pop` | `[2.23606797749979, 0.0]` | same | kept | `dispersion.py` | one-value group → 0.0 |
| `F.stddev_samp` | `[2.581988897471611, None]` | same | kept | `dispersion.py` | one-value group → NULL |
| `F.var_pop` | `[5.0, 0.0]` | same | kept | `dispersion.py` | |
| `F.var_samp` | `[6.666666666666667, None]` | same | kept | `dispersion.py` | |
| `F.variance` | `[6.666666666666667, None]` | same | kept | `dispersion.py` | alias of `var_samp` |
| `F.skewness` | `[0.0, None]` | `RAISED UnsupportedOperationException: functions.skewness is not supported yet (engine gap; disclosed R-FN-BATCH4)` | **dropped** | — | loud refusal, not a silent wrong answer |
| `F.kurtosis` | `[-1.36, None]` | `RAISED UnsupportedOperationException: functions.kurtosis is not supported yet (engine gap; disclosed R-FN-BATCH4)` | **dropped** | — | loud refusal, not a silent wrong answer |
| `F.corr` | `[0.9483040522636019, None]` | same | kept | `covariance.py` | one-pair group → NULL |
| `F.covar_pop` | `[3.125, 0.0]` | same | kept | `covariance.py` | one-pair group → 0.0 |
| `F.covar_samp` | `[4.166666666666667, None]` | same | kept | `covariance.py` | one-pair group → NULL |
| `F.regr_avgx` | `[5.25, 2.0]` | same | kept | `regression.py` | |
| `F.regr_avgy` | `[2.5, 1.0]` | same | kept | `regression.py` | |
| `F.regr_count` | `[4, 1]` | same | kept | `regression.py` | NULL pair skipped |
| `F.regr_intercept` | `[0.6115107913669069, None]` | `[0.6115107913669067, None]` | kept | `regression.py` | MATCH at `rel_tol=1e-12`; example asserts Spark |
| `F.regr_r2` | `[0.8992805755395683, None]` | same | kept | `regression.py` | |
| `F.regr_slope` | `[0.35971223021582727, None]` | `[0.3597122302158273, None]` | kept | `regression.py` | MATCH at `rel_tol=1e-12`; example asserts Spark |
| `F.regr_sxx` | `[34.75000000000001, 0.0]` | `[34.75, 0.0]` | kept | `regression.py` | MATCH at `rel_tol=1e-12`; example asserts Spark |
| `F.regr_sxy` | `[12.5, 0.0]` | same | kept | `regression.py` | |
| `F.regr_syy` | `[5.0, 0.0]` | same | kept | `regression.py` | |
| `F.bit_and` | `[0, 7]` | same | kept | `bit_aggregates.py` | NULL skipped |
| `F.bit_or` | `[15, 7]` | same | kept | `bit_aggregates.py` | |
| `F.bit_xor` | `[10, 7]` | same | kept | `bit_aggregates.py` | |

## Gates (2026-09-03, on this tree)

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py` (static half) | **0** |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `uv run --no-sync ruff check docs/examples` | **0** |
| `uv run --no-sync ruff format --check docs/examples` | **0** |

Counts line (both legs; native module importable; every example executed):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 156 covered; 755 backlog; 2 exceptions; 35 examples`

Before this unit: `913 public names; 134 covered; 777 backlog; 2 exceptions; 31 examples` (at `32c7f30`). After: `156 covered; 755 backlog; 35 examples` — exactly the twenty-two kept names.

## Cost

GLM/Spark legs produced the four example files, the backlog ratchet and the map rows, then died on API resets before the ledger and commit. Grok 4.6 continuation (2026-09-03 11:40–end UTC) re-ran the four examples (all green, not rewritten), measured all 24 names on live PySpark 4.1.2 + Iceberg 1.11.0 (`TZ=UTC`, JDK 17), dropped `F.skewness` / `F.kurtosis` with both values, filed this ledger, and committed. Base `32c7f30` (dispatch base `32c7f30`).

## Disk

Pickup: `df -h` free 569 GB of 1.8 TB. No worktree; unit works in `/tmp/oc-ex13`. `.venv` reused; `make develop` not run. Throwaway oracle under `/tmp/oc-ex13-oracle/` (not in the repo). Ivy redirected to that directory because the default `~/.ivy2.5.2/cache` refused writes.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job (`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke `python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is installed. EX-13 moves only the inventory/backlog ratchet and example files; it moves no wire, and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: EX-13
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 22 aggregate (b) and statistics names are covered by four new example files and the 2 dropped names stay backlog rows with both values in the oracle table.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/functions/dispersion.py, docs/examples/functions/covariance.py, docs/examples/functions/regression.py, docs/examples/functions/bit_aggregates.py]
    - id: AT-2
      status: ATTACKED
      evidence: A COVERS name on a wrong receiver is unused and red; the widened backlog is an exact baseline 755.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-3
      status: ATTACKED
      evidence: A missing class, missing nested class, or module with no __all__ raises a hard RuntimeError; there is no silent skip on shape drift.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-4
      status: N/A
      justification: The gate is a read-only process over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the four local examples; example children drop AWS_* and PYTHONPATH, exceptions ratchet is unchanged.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill is a walk of public names that already exist.
    - id: AT-7
      status: N/A
      justification: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed.
    - id: AT-8
      status: ATTACKED
      evidence: make ci stays native-build-free with the new examples; the walk adds no import of the facade.
      artifacts: [Makefile, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr through the existing reporter; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: The pin file cites C-001 of this unit alongside the prior units.
      artifacts: [scripts/map.md, docs/examples/functions/dispersion.py, docs/examples/functions/covariance.py, docs/examples/functions/regression.py, docs/examples/functions/bit_aggregates.py]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Sibling: [ex-11-functions-hash-url-random-ledger.md](ex-11-functions-hash-url-random-ledger.md), [ex-10-functions-null-cond-misc-ledger.md](ex-10-functions-null-cond-misc-ledger.md), [ex-2-functions-math-bitwise-ledger.md](ex-2-functions-math-bitwise-ledger.md)
