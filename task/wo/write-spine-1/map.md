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
