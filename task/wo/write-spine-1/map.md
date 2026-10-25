# task/wo/write-spine-1/ — the records of WRITE-SPINE-1 (card 2026-10-22)

The card is [task/roadmap/mid-term/write-spine-1-card-2026-10-22.md](../../roadmap/mid-term/write-spine-1-card-2026-10-22.md).
The owner adopted the evaluation's shape (option D) on 2026-10-22 and its §3 door table is the sketch; the orders are written per step.

- [crate-boundary-eval-2026-10-22.md](crate-boundary-eval-2026-10-22.md) — **CRATE-BOUNDARY-EVAL-1
  (2026-10-22, read-only, Opus 5.5):** what each crate owns from the code; the three rules that
  straddle plan time and store time; the 18 Iceberg write doors enumerated from the code (6 build
  their own store projection); how Sail draws its Iceberg-to-Spark boundary (one plan-time conform
  stage, the format crate receives a plan in table types, the same crate gap as RePark); options
  A–D with the recommendation of D, one plan-time conform function inside the existing graph, no
  new crate and no new edge; the layering is not the cause.
- [order-steps-1-3.md](order-steps-1-3.md) — **the order for steps (1) and (3) (2026-10-24, grade B
  with C-grade files, skeletons and gate lines; lane A, Opus 5.5 build, Opus 5.5 and Muse MAX
  verifiers; dispatches 2026-10-26):** one `repark_common::udf_names` home for the kernel names both
  crates name, a fail-closed kernel lookup in `ntz_store.rs` (a missing kernel refuses instead of
  storing the wall as UTC), a `WriteDoor` enum of the eighteen doors with an exhaustive `door_of`
  over the router's statement shapes, and the door-by-shape matrix test driven from that enum
  against Spark and main fixtures recorded in step 0. Six ledger rows, four mutants, hour budgets
  per step, halts on a nineteenth door or any value change.
- [census-2026-10-24.md](census-2026-10-24.md) — **WRITE-SPINE-1 step (0), the door-by-shape census (2026-10-24, lane B, Opus 5.5):** 12,192 cells (39 spellings × 8 sources × 10 targets × 3 zones, CoW and MoR) recorded on Spark 4.1.2 + Iceberg 1.11.0 and on main `7a789fc5`: 7,135 EQUAL, 1,731 DIFFERENT, 2,380 REFUSED, 946 ERROR; the families F1–F8 (Spark doors) and N1–N8 (`repark.sql()`), the census table by door × family × zone, and the red set by door, which is step (2)'s red-test list. D18 and D21 are not reached; `repark.sql()` does not reach the `repark-sql` paths of D14–D17, D19 and D20 (open question 1).
- [order-step-3b.md](order-step-3b.md) — The order for WRITE-SPINE-1 step (3b), the door matrix against the step-0 fixtures, committed unchanged; its Q1 and Q2 rulings are recorded in the hand-back, not in this file.
