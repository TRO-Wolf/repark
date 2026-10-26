# Card PA-10-HEADROOM-DOC-1: a measured headroom document for the memory outside the pool

**Date:** 2026-10-26. **Filed by:** Claude (claude-haiku-5-5), docs clerk, from the ICE-RESOURCE-BOUND-1 verification round (the owner's acceptance of the PA-10 residue, Q-C7).

**Status:** open, not scheduled. Grade C (clerk): this card records the residue and what the document must contain; no build order is written.

**Why:** the production audit's PA-10 required result says "document the remaining headroom for decoded batches, Arrow results, caches, writer buffers and multiple sessions". ICE-RESOURCE-BOUND-1 bounds three paths (PP-01, PP-02, PA-11) and closes the default-pool detection for cgroup v2. It does not document the headroom that the pool does not account for on the other paths, and this unit did not write that document.

**What the document must hold (proposed):** for each class named by the audit, a dated, measured figure with its run conditions: decoded batches on the Python and Arrow export paths; Arrow results; caches; writer buffers, including the PP-06 fan-out buffers measured on the unordered 32-partition write (407 to 448 MiB at 1M rows, from the ICE-RESOURCE-BOUND-1 step 0 table); and the memory of several sessions in one process. Each figure states the pool it was measured under and whether the pool accounts for it.

**Retires:** when the dated document exists under `docs/perf/`, is linked from the PA-10 amendment in the production audit, and every class the audit names has a measured figure or a stated exclusion.

**Relation:** the bound record is [resource-bound-2026-10-26](../../../docs/perf/resource-bound-2026-10-26.md); the constrained-process test that the same PA-10 row requires is [PA-10-CONSTRAINED-PROCESS-TEST-1](pa-10-constrained-process-test-1-card-2026-10-26.md).
