# Item 4: cost, base `68b09d18` against head `015fda5ae` (release builds)

**Gate:** ≤ 1.05x median of three. **Result: (b) exceeds it.**

| Case | What | Base runs (s) | Head runs (s) | Base median | Head median | Head / base |
|---|---|---|---|---|---|---|
| (a) | the domain table's 207 `spark.sql` read statements on the facade (after its 21 setups), sum of cell times | 0.798, 0.812, 0.779 | 0.819, 0.801, 0.919 | 0.798 | 0.819 | **1.0256** |
| (b) | 200 `EXPLAIN`s after 10 warm-ups: 300 `DOUBLE` columns, 50 comparisons against zero (7 operators × 4 zero shapes) | 27.530, 27.756, 27.680 | 29.294, 29.050, 30.838 | 27.680 | 29.294 | **1.0583** |

One `repark.slice` unit holding the build lock, interleaved per run (a base, a head, b base, b head), fresh process
and warehouse each, cores 32–47 ([cost_probe.py](cost_probe.py), `cost-raw.jsonl`). Case (a) answered 173 cells on
base and 188 on head (the 15 former refusals now answer, so head does more work).

## Profile of case (b) (Rust harness, release, in the slice; not committed)

The harness planned the case (b) query on a Spark-door `ReparkSession`, one rule at a time: it timed the analyzer's and
the optimizer's observer callbacks, with seat 1 or seat 1b turned off in turn, every variant in one process.

| Variant | Planning (ms) | Analyzer | Optimizer passes (1 / 2 / 3) | Physical |
|---|---|---|---|---|
| neither seat | 108.16 | 19.24 | 2.92 / 2.66 / none | 47.61 |
| seat 1 only | 112.20 | 21.44 | 3.20 / 2.92 / none | 48.81 |
| seat 1b only | 113.96 | 19.54 | 3.34 / 3.08 / 3.06 | 49.06 |
| both (head `015fda5ae`) | 115.31 | 21.55 | 3.28 / 3.04 / 3.04 | 48.39 |

- **The walks cost nothing measurable.** With every zero replaced by a one, so that no seat rewrites, all four variants
  are within noise of each other.
- **Seat 1b cost a third optimizer pass, about 3.0 ms.** On the Spark door, `CAST('-0.0' AS DOUBLE)` is a UDF call that
  only `simplify_expressions` folds. Seat 1b then rewrites `c27 <=> CAST('-0.0' AS DOUBLE)` into
  `c27 IS NOT NULL AND …`, and the next pass's simplify drops the `IS NOT NULL` on the non-null column. That change
  forces pass 3.
- **The bound form's extra nodes cost the rest: about 2.3 ms in the analyzer and 1 ms in physical planning.**
  `ReparkSession::sql` returns an analyzed plan, so the analyzer runs again at execution, over the bound form.
  Seat 1's own walk is 0.04 ms.

**Change.** Seat 1b now re-runs `SimplifyExpressions` on each node it rewrites, so its output needs no further pass.
The pin is `seat_1b_hands_the_next_pass_a_simplified_plan`: 2 passes, and 3 with the change reverted.

## Re-measure after the change (base `68b09d18` against head `015fda5ae` plus the change)

Same probe and procedure as above, interleaved, three runs ([cost-raw-r3.jsonl](cost-raw-r3.jsonl)).

| Case | Base runs (s) | Head runs (s) | Base median | Head median | Head / base |
|---|---|---|---|---|---|
| (a) | 0.777, 0.848, 0.818 | 0.876, 0.890, 0.852 | 0.818 | 0.876 | **1.0715** |
| (b) | 27.564, 28.128, 28.466 | 29.135, 28.341, 28.389 | 28.128 | 28.389 | **1.0093** |

**Case (a) diagnostic.** [cost_a_cells.py](cost_a_cells.py) times every cell, over five interleaved runs of base, the
item 2 head and this head ([cost-a-cells.jsonl](cost-a-cells.jsonl)). [cost_a_split.py](cost_a_split.py) splits the cells
by their item 3 label move.

| Cells | n | Base (ms) | Item 2 head | This head |
|---|---|---|---|---|
| all | 207 | 798.1 | 1.0513 | 1.0496 |
| label unchanged (same answer, same reads) | 164 | 687.2 | 1.0233 | **1.0189** |
| WRONG-READ → EQUAL | 24 | 65.4 | 1.2470 | 1.2204 |
| PRUNED-WRONG → EQUAL (now read the pruned file) | 10 | 21.8 | 1.3414 | 1.2521 |
| refused → answered | 8 | 19.3 | 1.24 | 1.24 |

Case (a) sits at the bar on both heads; item 4's 1.0256 and this round's 1.0715 are draws around it. The plan-time
cost, on the cells whose answer does not move, is 1.0189. The excess is in the 43 cells that now answer like Spark:
they read the file base pruned, or run where base refused.
