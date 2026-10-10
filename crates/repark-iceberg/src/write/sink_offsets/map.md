# map — repark-iceberg/src/write/sink_offsets

## Purpose

Children of the `write::sink_offsets` module (`write/sink_offsets.rs` declares them). The sink
offsets claim, the epoch check and the stamped commit arms live in the parent file; this
directory holds the piece split out of it. The test files of the module stay beside the parent in
`write/`, as `#[cfg(test)]` `#[path]` children of its probe module.

## Contents

- `lineage.rs` — **MB-4-FOREACH-EO fold 4 (2026-10-10, owner ruling on the third verify's
  first S1): the walk goes under the newest stamp.** `stray_on_main(table, query, baseline)`
  replaces `unstamped_since_stamp`. It reads the main lineage once, in memory, and looks at
  two stretches: above this query's newest stamp (down to the starting head while nothing is
  stamped), and between the newest stamp and the previous one, or down to the head the first
  stamp records. The first unstamped snapshot it meets comes back as a `Stray`: the
  snapshot, its operation, this query's newest stamp, whether the stray sits below it, and
  the `Floor` a rollback would land on (`Newest`, `Previous(stamp)`, `Head(id or none)`, or
  `Shared` when another query's stamps sit in the stretch). Fold 2's walk stopped at the
  newest stamp; a stray under it was seen only by the in-process audit, and a process that
  died between the body's commit and that audit left it unseen for good (source 32 rows,
  sink 36, no signal). The cost is one pass over snapshots already in the loaded metadata:
  no catalog call, no file read. A first stamp with no recorded head (a query begun through
  the `toTable` door, or by a build before this fold) has no lower bound, and the walk does
  not go under it. `Stray::reason` turns the facts into `RecoveryReason::StraySinkCommit`,
  given the source positions the driver could verify; `Stray::records` lists the stamps
  whose positions it needs. Pins in `sink_offsets_lineage_tests.rs`: the stretch under the
  newest stamp and its floor; the first stamp recording the head and bounding the walk; an
  empty start, a markless first stamp and a shared stretch told apart; the remedy offering
  only what the walk can prove; a stamp alone refusing the mark (the third verify's
  surviving mutant N4).
  pins: mb-4-foreach-eo/C-032, C-033, C-039
- `append_fence.rs`, `body_scope.rs` — **MB-4-FOREACH-EO fold 3 (2026-10-10): one catalog
  wrapper, three rules.** Main's ENC-1 gate (`tests/enc_1_gate.rs`) allows `impl Catalog for`
  in three files only, so that every catalog handle stays inside the encryption guard's
  wrappers, and `append_fence.rs` is one of them. Fold 1's `BodySinkGuard` was a second
  wrapper in `body_scope.rs` and failed that gate after the merge. It is gone: `AppendFence`
  now carries a `Rule` (`Stamp` for the append fence, `StartingMark` for the write-once mark,
  `BodySink` for the body's guard) over one forwarding implementation, and
  `guard_body_catalog` builds it with `AppendFence::for_body`. The body rule's behaviour is
  unchanged: at `update_table` and `publish_replace_table` on the scope's sink it admits a
  commit only while the scope holds a claimed, unrecorded stamp, and it notes an unknown
  outcome. The gate's allow-list is not edited. The entries below that name `BodySinkGuard`
  describe this rule. The name `AppendFence` now says less than the type does; the file is
  still the stopgap for F-APPEND-PIN-BASE-1, and when the fork lands that ask only the
  `Stamp` rule retires.
  pins: mb-4-foreach-eo/C-006, C-028
- `lineage.rs`, `append_fence.rs` — **MB-4-FOREACH-EO fold 3 (2026-10-10, owner ruling D2):
  the starting mark.** `read_starting_mark(table, query)` reads the mark from the offsets
  property; `commit_starting_mark(catalog, table, query)` writes it in a property-only commit
  that records the table's current head. The parent's `property_record` reads a pending mark
  as no record, so `read_resume_point` answers "nothing durable" for it on every door.
  - **Write-once.** No table requirement can assert a property, and the fork retries a
    property commit on a refreshed base, so a mark written by a driver that loaded the sink
    before a racing driver's first stamp would overwrite that stamp's offsets record. The
    commit therefore goes through `AppendFence` with a second rule, `StartingMark(query)`:
    at `update_table` it refuses when the refreshed table already holds this query's offsets
    property or one of its stamps. `commit_starting_mark` then reloads and returns the table
    as it is; an unknown outcome is settled the same way, by the reload. `AppendFence` keeps
    its stamp rule unchanged; the forwarding is shared so no second wrapper exists.
  - **Pins.** `sink_offsets_lineage_tests.rs`: a pending mark reads as no durable record and
    the first stamp replaces it; the mark round-trips its head, is per query, and is written
    once; a corrupt offsets value is neither a mark nor a record.
  pins: mb-4-foreach-eo/C-028
- `lineage.rs` — **MB-4-FOREACH-EO fold 2 (2026-10-09, orchestrator ruling after the
  re-verify: an invariant on the sink's lineage, checked by the driver, not a list of
  routes).** The two checks the `foreachBatch` driver runs.
  - **`unstamped_since_stamp(table, query, baseline)`**, check (a): walk `main` from the head
    down to the newest snapshot this query stamped (or to `baseline`, the head a query with no
    stamp found at start) and name the first snapshot that carries no `repark.cdc.query-id`,
    with its operation. A snapshot stamped by another query is passed over: it is that query's
    own commit and cannot be a stray write of this body.
  - **`SinkMark::of(table)` / `violation(after)`**, check (b): the mark holds the table as it
    was when the batch entered its scope and nothing else, so taking it costs one clone;
    the set of snapshot ids is built only when the metadata location changed. `violation`
    answers none when the location is the same. Otherwise it names, in this order: another table uuid; a new snapshot with no
    stamp (on `main` first, then anywhere, so a branch write and a staged snapshot count); a
    removed snapshot; `main` moved to a snapshot that already existed; a table property other
    than a `repark.cdc.offsets.*` key; a schema, partition-spec or sort-order id; a branch or
    tag. The fork's metadata has no public iterator over refs, so refs are read from the
    serialized metadata, and only when the cheaper test fails: one new snapshot and the newest
    entry of the metadata log equal to the mark's location mean exactly one commit landed,
    the stamped one. Correctness does not rest on the metadata log; a catalog that does not
    keep one pays the serialization every batch.
  - **The reasons** are `RecoveryReason::UnstampedSinkCommit { snapshot, operation }` and the
    new `UnstampedSinkChange { what }`.
  - **Pins.** `sink_offsets_lineage_tests.rs` (a `#[path]` child of the probe module, in
    `write/`): the walk's two bounds and what lies below them; another query's stamp; the
    mark over nothing and over the one stamped commit; a stray before and after the stamped
    commit; a property change with and without it; a branch, a rollback and a replaced table.
  pins: mb-4-foreach-eo/C-016, C-017
- `body_scope.rs` — **MB-4-FOREACH-EO fold 2 (2026-10-09):** `unstamped_above` is gone (the
  lineage module replaces it). `refuse_planned_sink_write(table)` refuses, inside a body scope
  and for the scope's own sink only, a write the engine planned through DataFusion and that no
  stamped arm will commit; `repark-core`'s pre-execute belt calls it for every DML node a
  statement will run. Three pins joined `sink_offsets_body_scope_tests.rs` for the re-verify's
  surviving mutants and the claimed state: the guard loads the table when the commit brings
  no base, a token in the extras decides before the ambient scope, and a failed stamped
  attempt releases the claim (see the parent map).
  pins: mb-4-foreach-eo/C-018, C-020, C-023
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
    refusal of a commit and of a table replacement, with another table passing both; the
    stamped commit through the guard and the refusal
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
