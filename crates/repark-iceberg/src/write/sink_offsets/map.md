# map — repark-iceberg/src/write/sink_offsets

## Purpose

Children of the `write::sink_offsets` module (`write/sink_offsets.rs` declares them). The sink
offsets claim, the epoch check and the stamped commit arms live in the parent file; this
directory holds the piece split out of it. The test files of the module stay beside the parent in
`write/`, as `#[cfg(test)]` `#[path]` children of its probe module.

## Contents

- `body_scope.rs` — **MB-4-FOREACH-EO (2026-10-09, owner ruling "FIX IT"):** how a commit
  issued by a `foreachBatch` body finds its batch's stamp, and what stops a commit that cannot
  carry it.
  - **The ambient body scope.** `BatchScopeGuard::scope_body(future)` sets a thread-local
    `(sink TableUuid, ScopeToken)` for each poll of the body's future and restores the outer
    value when the poll returns. A thread-local, not a Tokio task-local: the crate has no
    Tokio dependency outside its tests, and the effect is the same, because the value is
    visible only to code polled inside the body's own future. That includes a Python callable
    the binding runs under `block_in_place` and the nested `block_on` of a statement it
    issues. It excludes a spawned task and any other thread. The token never enters the shared
    session config (MB-3 D-3).
  - **The claim.** `SiteStamp::claim_with` (parent file) reads the token from the commit's
    summary extras first and falls back to `ambient_token(table)`, which answers only for the
    scope's own sink. The `toTable` door always carries the token in its extras, so the
    fallback is dead on that door.
  - **The routing.** `in_body_scope()` makes `session_write_conf_is_set` answer true, so
    inside a body the two SQL doors send `INSERT`, `UPDATE` and `DELETE` to the RePark-owned
    commit arms, as they do whenever a session snapshot property is set. Without it a plain
    `INSERT` goes to the fork's table provider, which has no stamped arm.
  - **`BodySinkGuard`.** `guard_body_catalog` wraps a catalog when a body scope is active and
    returns it untouched otherwise; `repark-core`'s statement funnel applies it to its registry
    snapshot. The guard captures the scope when it is built, so it does not depend on the
    thread at commit time. At `update_table` and `publish_replace_table` on the scope's sink
    it admits the commit only while the scope holds a claimed, not yet recorded stamp, which
    is the window of a stamped arm's own commit, and refuses every other commit with a
    non-retryable `DataInvalid` whose source is `UnstampedSinkWrite` (MBE-19). It cannot sort
    shapes: the fork's `TableCommit` gives its updates away only by `take_updates`. It notes a
    `CommitStateUnknown` answer on the scope, and it forwards every other call. A guard that
    outlives its scope finds no entry under its token and commits as on main.
  - **`unstamped_above(table, base)`** is the newest snapshot on `main` above `base` with no
    `repark.cdc.query-id`: the driver's test for "the head moved without the stamp". A
    snapshot stamped by any query is not a finding.
  - **What it does not reach.** A commit on a catalog handle that did not come from the
    statement funnel inside a body (another thread, another process, the bare DataFusion
    `INSERT` of a session with no SQL door). Those commit unstamped; the driver's
    `unstamped_above` check names them when the body made no stamped commit.
  - **Pins.** `sink_offsets_body_scope_tests.rs` (a `#[path]` child of the probe module, in
    `write/`): the claim with no token in the extras and the second write's refusal; no claim
    outside the scope; `unstamped_above` over a stamped and a foreign snapshot; the guard's
    refusal, with another table passing; the stamped commit through the guard and the refusal
    of what follows it; a stale guard; a spawned task; the unknown outcome latched.
  pins: mb-4-foreach-eo/C-001, C-004, C-006, C-009
- `append_fence.rs` — **MB-2c closing slice (2026-10-07, owner ruling ~20:55 EDT:
  `F-APPEND-PIN-BASE-1` is not built in the fork now, and the RePark-side stopgap is allowed):**
  `AppendFence`, a catalog wrapper installed for one commit only, and only when the commit holds a
  claimed stamp (the append arm and the stamp-only door). At `update_table` it reads the
  refreshed base the fork hands in `TableCommit::base_table` and refuses with a non-retryable
  `DataInvalid` when this query's `repark.cdc.query-id` was stamped on `main` above
  `ClaimedStamp::base`, or when the base is no longer an ancestor of `main`. When the walk finds
  neither, it still runs the epoch check on the refreshed table and refuses on its error (an
  expired stamp the offsets property still names, C-012). `refusal_of` reads
  the typed `MicroBatchError` back at the two commit sites. Every other `Catalog` method forwards
  to the inner catalog. The full rule, the pins and the retirement list are in
  [the parent map](../map.md) under the MB-2c closing slice. Stopgap for
  [F-APPEND-PIN-BASE-1](../../../../../task/roadmap/mid-term/f-append-pin-base-1-2026-10-07.md);
  the file is deleted when the fork lands it and RP-N repins.
  pins: mb-2c/C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015
