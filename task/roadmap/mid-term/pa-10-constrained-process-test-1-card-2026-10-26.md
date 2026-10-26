# Card PA-10-CONSTRAINED-PROCESS-TEST-1: a disposable constrained process proves the default pool under a nested cgroup

**Date:** 2026-10-26. **Filed by:** Claude (claude-haiku-5-5), docs clerk, from the ICE-RESOURCE-BOUND-1 verification round (the owner's acceptance of the PA-10 residue, Q-C7).

**Status:** open, not scheduled. Grade C (clerk): this card records the residue and its test shape; no build order is written.

**Why:** the production audit's PA-10 required result names "a disposable constrained process integration test" beside the fixture tests. ICE-RESOURCE-BOUND-1 closed PA-10 for cgroup v2 with fixture cells only (the walk over the cgroup text and a reader closure). Under the owner's ruling O-13 the live-kernel test runs under acceptance only, and this unit did not write it.

**Shape (proposed):** a process started under a memory-capped scope below the host total (for example `systemd-run --scope -p MemoryMax=...`, or an equivalent cgroup the test can create). The process builds a session with no budget set and asserts that the default pool follows the nested cap, clamped as the detector rules, rather than the host `MemTotal`. The test runs in the acceptance lane on main, never in pull-request CI.

**Retires:** when the acceptance lane runs the constrained-process test on main and it passes, and the PA-10 amendment in the production audit links this card as closed.

**Relation:** the fixture cells that cover the walk are in `crates/repark-core/src/session/spill.rs` (ICE-RESOURCE-BOUND-1, C-009). The bound record is [resource-bound-2026-10-26](../../../docs/perf/resource-bound-2026-10-26.md). The headroom document that the same PA-10 row requires is [PA-10-HEADROOM-DOC-1](pa-10-headroom-doc-1-card-2026-10-26.md).
