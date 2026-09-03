# Unit ledger — EX-14 · v0.7 example backfill, `F.*` window

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the orchestrator's departure move). This file closes when EX-14 merges, or when the owner closes the slate row.

**Unit:** EX-14 · **Date:** 2026-09-03 · **Model:** glm-5.3-flash · **Branch:** `feat/ex-14-functions-window` · **Base:** `32c7f30`
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), batch roster row (9 names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v0.7 — Full example documentation".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`, the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`, lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`, `python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The roster is the nine `F.*` window names that were backlog rows at the base `32c7f30`. Four files cover the nine names; the live oracle measures all nine Spark-equal, none dropped.

**Roster (9):** `F.cume_dist`, `F.dense_rank`, `F.rank`, `F.percent_rank`, `F.row_number`, `F.ntile`, `F.lag`, `F.lead`, `F.nth_value`.

**Grouping (4 files, 4–8 allowed, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `window_ranking.py` | `F.row_number`, `F.rank`, `F.dense_rank` | Ties counted three ways: sequential position, rank with gaps, dense rank without — one grouped ordered frame with a tie and one NULL row. |
| `window_position.py` | `F.percent_rank`, `F.cume_dist`, `F.ntile` | Relative position in the partition: the fraction of peers strictly below, the cumulative share including the peer group, and even tiling — floats checked with `math.isclose(rel_tol=1e-12)`. |
| `window_offset.py` | `F.lag`, `F.lead` | The previous and next row's value at two offsets, with and without the fill default; the default answers only where the offset row does not exist, never where it exists with NULL. |
| `window_nth_value.py` | `F.nth_value` | The nth value seen so far in the ordered frame: frames shorter than n and NULL rows answer NULL. |

`F.col` is already covered by `abs.py`; it is listed where genuinely used and does not move the ratchet.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Four files under `docs/examples/functions/` land runnable local examples for all nine roster names, every asserted value measured against PySpark 4.1.2 + Iceberg 1.11.0 before it was written; those nine leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly nine, 777 → 768, with no other `scripts/` change; no roster name is dropped (the oracle measures all nine Spark-equal, so no backlog row keeps a divergence record); no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture (9 findings before, 0 after), oracle table (9 rows, one per roster name, Spark value + repark value + kept/dropped + file), the four scripts each exit 0, and the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured at `32c7f30` (dispatch base, before any window example file existed), in a
throwaway worktree. At that base — the 9 roster rows still in
`docs/examples/backlog.txt`, `BACKLOG_BASELINE=777` — `python3
scripts/check_example_coverage.py` exits **0** (`913 public names; 134 covered; 777
backlog; 2 exceptions; 31 examples`). **Provocation:** delete the 9 roster rows from
`backlog.txt` and lower `BACKLOG_BASELINE` to 768 (`777 − 9`) with no window example
files present; the same gate exits **1** with 9 findings, one per roster name and no
others. With the four files present, the nine names removed and
`BACKLOG_BASELINE=768`, the gate exits **0**.

## Oracle (live PySpark 4.1.2 + Iceberg 1.11.0, JDK 17, warehouse in a temp dir)

Measured with `_live_parity.build_spark_iceberg_engine(Path(tmpdir)).session` at `/tmp/oc-ex14/.venv/bin/python` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` and `PYTHONPATH=/tmp/oc-ex14/python/repark/tests`, one throwaway script under `/tmp/opencode/ex14-oracle/` (outside the repo) printing per name Spark and repark values for identical inputs. Inputs: one 6-row frame `[("a",1,10),("a",2,20),("a",2,30),("a",3,40),("b",1,50),("b",2,None)]` over columns `g`, `k`, `v` (the last row carries NULL `v`); ordered window `partitionBy('g').orderBy('k','v')`; peers window `partitionBy('g').orderBy('k')`; `lag`/`lead` also measured with their fill defaults (`lag(v,2,-1)`, `lead(v,1,0)`). `pins: ex-14-functions-window/C-001`

| Name | Spark value (repr) | repark value (repr) | Kept / dropped | File | Note |
|---|---|---|---|---|---|
| `F.row_number` | `[1, 2, 3, 4, 1, 2]` | same | kept | `window_ranking.py` | ordered window |
| `F.rank` | `[1, 2, 2, 4, 1, 2]` | same | kept | `window_ranking.py` | peers window; the tie keeps a gap |
| `F.dense_rank` | `[1, 2, 2, 3, 1, 2]` | same | kept | `window_ranking.py` | peers window; no gap |
| `F.percent_rank` | `[0.0, 0.3333333333333333, 0.3333333333333333, 1.0, 0.0, 1.0]` | same | kept | `window_position.py` | peers window |
| `F.cume_dist` | `[0.25, 0.75, 0.75, 1.0, 0.5, 1.0]` | same | kept | `window_position.py` | peers window; includes the whole peer group |
| `F.ntile` | `[1, 1, 2, 2, 1, 2]` | same | kept | `window_position.py` | `ntile(2)` over the ordered window |
| `F.lag` | offset 1 `[None, 10, 20, 30, None, 50]`; offset 2 default −1 `[-1, -1, 10, 20, -1, -1]` | same | kept | `window_offset.py` | the default answers only where the offset row does not exist |
| `F.lead` | offset 1 `[20, 30, 40, None, None, None]`; offset 1 default 0 `[20, 30, 40, 0, None, 0]` | same | kept | `window_offset.py` | an existing NULL next row stays NULL, not the default |
| `F.nth_value` | `[None, 20, 20, 20, None, None]` | same | kept | `window_nth_value.py` | `nth_value(v, 2)`; frames shorter than n and NULL rows answer NULL |

## Gates (2026-09-03, on this tree)

| Command | Exit |
|---|---|
| `python3 scripts/check_example_coverage.py` (static half) | **0** |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `uv run --no-sync ruff check docs/examples` | **0** |
| `uv run --no-sync ruff format --check docs/examples` | **0** |

The system `python3` in this clone cannot import `repark._native`, so the `--require-execute` leg runs under `.venv/bin/python`, which carries the built module for this base; the static half under `python3` skips execution with a note and still exits 0.

Counts line (execute leg; the native module imports, every example executed, every module door's live `__all__` matched):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 143 covered; 768 backlog; 2 exceptions; 35 examples`

Before this unit: `913 public names; 134 covered; 777 backlog; 2 exceptions; 31 examples` (at `32c7f30`). After: `143 covered; 768 backlog; 35 examples` — exactly the nine kept names.

## Cost

The GLM (glm-5.3-flash) leg of this batch started 2026-09-03, wrote the four example
files, the backlog ratchet and the map rows, then died on a transport error before
commit; a fresh GLM (glm-5.3-flash) session resumed the tree, re-ran the four examples
green, re-measured the live oracle (all nine Spark-equal), and completed the ledger,
maps and commit. Base `32c7f30`.

## Disk

Pickup: `df -h` 318 GB free of 1.8 TB. No worktree kept; the unit works in the main clone (the throwaway red-first worktree was removed after the measurement). `.venv` and the built native module reused; `make develop` not run.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job (`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke `python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is installed. EX-14 moves only the inventory/backlog ratchet and example files; it moves no wire, and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: EX-14
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the nine window names are covered by four new example files and the oracle table records both engines' values per name.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/functions/window_ranking.py, docs/examples/functions/window_position.py, docs/examples/functions/window_offset.py, docs/examples/functions/window_nth_value.py]
    - id: AT-2
      status: ATTACKED
      evidence: A COVERS name on a wrong receiver is unused and red; the backlog is an exact baseline 768.
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
      artifacts: [scripts/map.md, docs/examples/functions/window_ranking.py, docs/examples/functions/window_position.py, docs/examples/functions/window_offset.py, docs/examples/functions/window_nth_value.py]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Sibling: [ex-11-functions-hash-url-random-ledger.md](ex-11-functions-hash-url-random-ledger.md), [ex-10-functions-null-cond-misc-ledger.md](ex-10-functions-null-cond-misc-ledger.md), [ex-9-functions-maps-structs-json-ledger.md](ex-9-functions-maps-structs-json-ledger.md)
