# Card ICE-RESOURCE-BOUND-1 (U-11): a stated resource bound for ordered writes and merge-on-read MERGE

**Date:** 2026-10-20. **Filed by:** Claude (claude-opus-5-5), clerk, step 0 of the 2026-10-20
Iceberg re-prioritisation.

**Status:** open, not scheduled. **Awaits the owner's adopt of the 2026-10-20 plan**, which puts
the unit sixth on the one Opus 5.5 lane, after U-9 (Fri 10-30 to Mon 11-02). Order:
[task/wo/ice-resource-bound-1/order.md](../../wo/ice-resource-bound-1/order.md).

**Retires:** when the bound is measured and written in docs with its run ids, a write above it
refuses with one text and leaves the old snapshot intact on format v2 and v3, PP-01 and PP-02
read "bounded" or "declared" with that bound, and PA-10 and PA-11 read "declared" or "closed".

## Where it comes from

- **The closeout plan's v2 list** (`docs/artifacts/iceberg-closeout-plan-2026-10-13.html`, U-11
  row, adopted 2026-10-13): "A stated resource bound. Ordered writes and MoR MERGE measured, the
  bound written down, exceeded means refused." The U-11 row: measure the ordered write and the
  MoR MERGE at 1×, 4× and 16× the acceptance sizes with RSS outside the pool; write the bound
  into docs; refuse above it with a message; cgroup v2 nested detection and non-Linux RAM; exit
  proof a bound table in docs with the run ids, a refusal cell above the bound, detection unit
  tests; release 1.5.6.
- **The 10-17 replan** (`docs/artifacts/iceberg-closeout-replan-2026-10-17.html`, read on branch
  `docs/iceberg-closeout-replan-2026-10-17` at `2661f387`; not on main and not adopted, its
  D1 to D8 superseded by the 2026-10-20 plan's P1 to P10, which await the owner): the measurement
  grid of each audit row (below), and the fallback "PP-01 cannot be bounded without a redesign
  … the bound is declared and exceeded means refused; the definition allows 'refuses cleanly
  where it cannot spill'. Spilling is a later unit."
- **The 2026-10-20 plan** (step 6): ordered writes and MoR MERGE measured on the acceptance
  sizes, the bound written in docs, exceeded means refused with one text; PA-10 and PA-11
  declared or closed; it needs U-9's kill pin.

## What the bound is

A number per write path, in input bytes against the session's pool size, above which RePark
refuses the statement before process memory is exhausted. Two paths:

- **Ordered writes (PP-01,** `docs/perf/production-performance-audit-2026-10-11.md`**):**
  `sort_batches_by_default_order` (`crates/repark-iceberg/src/write/distribution.rs`, read on main
  `08b65ff1`) sorts under `TaskContext::default()`, which brings an unbounded pool, after the whole
  input is held in a `Vec<RecordBatch>`; the sorted fan-out arms and the lineage writer also drain
  before sorting. Nothing on that path is charged to the session's `FairSpillPool`.
- **Merge-on-read MERGE (PP-02):** `matched_work_mor` (now
  `crates/repark-iceberg/src/write/merge/merge_on_read.rs` after MERGE-MOD-SPLIT-1; the audit
  names `merge/mod.rs`) keeps every matched update batch and the delete-pair state until the
  matched stream ends, with no `MemoryReservation`.

## What "exceeded means refused" means

The production-grade definition (#1044, `docs/production-grade.md`, merged 2026-10-20, row
"Resource discipline"): "Memory, disk and file handles stay inside a declared bound; the engine
spills where it can and refuses cleanly where it cannot." Neither path spills today, so for both
the bound is declared and a statement whose input is above it is refused:
- before any file is staged where the input size is known up front, or else as soon as the
  charged bytes cross the bound;
- with one refusal text naming the statement, the bound and the session key that raises it,
  and naming no data-deleting action (ruling 76's hint rule, as the plan carries it);
- leaving the table at its old snapshot, with any staged files counted as orphans as U-9's pin K
  counts them.

A crash (the process killed or out of memory) and a refusal must be two different pinned
outcomes. That is why this unit follows U-9: pin K
(`task/wo/ice-integrity-pins-1/sketch.md` §0 and §3, merged with #1067) pins what a killed write
leaves, and this unit pins what a refused one leaves.

## Which rows close, and how

| Row | Source | Closes as |
|---|---|---|
| PP-01 ordered writes | performance audit 2026-10-11 | **bounded** (charged to the pool, refused above the bound) or **declared** (the bound measured and written, refused above it); spilling stays a later unit |
| PP-02 MoR MERGE retained state | performance audit 2026-10-11 | the same two outcomes; multi-match validation and `CommitStateUnknown` semantics are kept |
| PA-10 memory detection misses nested cgroups and non-Linux RAM | production audit 2026-10-11 (`crates/repark-core/src/session/spill.rs`) | **closed** by a detector that walks the process's cgroup and its ancestors (v2, and v1 or a refusal to guess), or **declared** with an explicit budget required where detection cannot be trusted |
| PA-11 changelog view materialises outside the pool | production audit 2026-10-11 (`crates/repark-iceberg/src/catalog/changelog_view.rs`) | **declared** (the bound written, a refusal above it) where "refuses cleanly where it cannot spill" applies; a budgeted transform is a later unit |

## Out of scope

Spilling either write path; a streaming MERGE redesign; PP-03 and the other PP rows; any change
to the commit path, the fork or the error taxonomy beyond the one refusal; the Python allocations
outside the pool (documented as headroom, not bounded).
