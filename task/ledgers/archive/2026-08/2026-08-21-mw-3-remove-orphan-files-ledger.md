# MW-3 — the procedure with no undo

**Date:** 2026-08-21 · **Branch:** `feat/mw-3-remove-orphan-files` · **Base:** `1e73ef6`
(`main`, post-#197) · **Charter:** [mw-0-charter-ledger.md](../../completed/mw-0-charter-ledger.md) ·
**Slate:** [../briefs/iceberg-maintenance-wave.md](../../../../docs/history/iceberg-maintenance-wave/slate.md)

`remove_orphan_files` deletes files that no snapshot references. Every other procedure in this
campaign is recoverable — a bad compaction is compacted again, an expired snapshot was already
unreachable. This one has no rollback. The slate gave it the campaign's strictest contract before
a line was written, and this ledger records what that contract turned into once the oracle had
been asked.

## 1. What the measurement changed about the plan

The charter and slate said the `older_than` floor would be **stricter than Spark**: "the fork
already defaults `older_than` to `now − 3 days`; a caller-supplied value below the floor refuses
rather than being honoured."

That was wrong about Spark, and the correction makes the unit smaller and more defensible.

**Spark already has the floor.** `RemoveOrphanFilesProcedure` refuses an interval under 24 hours,
and its message says why: a short interval "may corrupt the table if other operations are
happening at the same time." Measured across the boundary on a live Spark 4.0.1 + Iceberg 1.10.0
oracle:

| `older_than` | Spark |
|---|---|
| `now` | refused — "less than 24 hours" |
| `now − 23h` | refused — same message |
| `now − 25h` | ran, listed 2 orphans, deleted them |

So the floor is **parity**, not a stricter posture, and it is not registered as a divergence. What
it did need was a decision about *where* it lives, and Java answered that too: the floor is in the
**procedure** layer, not the Action API — Java's own message points callers at the Action API to
bypass it, and the fork's `DeleteOrphanFiles` accordingly has no floor. This router is the
procedure layer, so the floor belongs here. That is convergence with Java's architecture, not a
workaround.

## 2. What is genuinely stricter, and why

Two things, both owner decision OD-2, both registered.

**`ORPHAN-1` — `older_than` is required.** Spark defaults it to `now − 3 days` and deletes.
Measured: a bare `CALL … remove_orphan_files(table => 't')` listed and removed two ten-day-old
planted orphans. A defaulted cutoff makes the single most dangerous argument the one the caller
never typed, and three days is not conservative enough to be safe by accident — it is short enough
to catch a long-running write.

**`ORPHAN-2` — `dry_run` defaults to true.** Spark's default is false: the bare call deletes.
Measured on the same fixture, the default call left three files where five had been.

The thing that makes `ORPHAN-2` cheap is that **the result shape is identical either way**. Spark's
`dry_run => true` returns the same one-row-per-orphan `orphan_file_location` listing as the armed
run, so the dry run is not a second surface bolted on beside the real one — it is Spark's own
result with the deletion withheld. A caller who reads the listing and re-runs with
`dry_run => false` gets Spark's behaviour exactly. All that changes is which of the two you get by
typing nothing.

`dry_run` also takes a boolean literal only. A quoted `'false'` refuses rather than being coerced,
because on this procedure guessing what a caller meant by a string is not a service.

## 3. A partial delete is not a success

The fork returns per-file `delete_failures` alongside the orphan list. Spark's result schema has
no failure column, so there is nowhere in the Spark-shaped result to put them.

Returning the rows anyway would mean the result says "these were orphans" while some of them are
still on disk. The call fails instead, naming how many succeeded, how many did not, and the first
failure's path and error. The files already deleted stay deleted, which needs no rollback — an
orphan removed is simply removed — and re-running retries the remainder.

## 3a. The finding: the shared CTAS fallback root

Writing the facade pins turned one up. The fixture's `mem.ns.events` resolved to
`<temp>/repark_ctas/mem/ns/events`, and a dry run there listed **139,179** files.

They were real orphans — leftovers of every prior test run that used that table name. The reason
they accumulate is the point. `register_memory_catalog` carries
`LocationPolicy::TempFallbackAllowed`, and a namespace created without a `location` property
places its tables at `<root>/repark_ctas/<catalog>/<namespace>/<table>`
(`ctas.rs::resolve_create_location`). Reproduced outside the test harness: the warehouse passed to
`register_memory_catalog` stays **empty**, and both CTAS and `CREATE` + `INSERT` land in the shared
root. That is the documented E-4 fallback, not a defect.

What it means for THIS procedure is a defect, and it is one MW-3 would have created.

The path is derived from **names alone**, with nothing process-specific in it. Two independent
sessions that both use `mem.ns.events` get the same directory. Orphan removal decides what to
delete by subtracting one table's reachable set from a directory listing, so in a shared directory
another session's live files are indistinguishable from orphans. An armed sweep would delete them.

Every other procedure on this surface only touches files its own metadata references, so none of
them can do this. This one lists a directory, so it can — which is why the hazard is new, even
though the shared location is not.

`remove_orphan_files` now refuses a table under the fallback root, and the refusal names the way
out: create the namespace with an explicit location so the table owns its directory. A namespace
that already does is unaffected, which is the case every other pin in the module runs in.

**The underlying location behaviour is untouched by this unit** and is queued as roadmap item
A13 — it is a write-path question, and this unit's job was to not build a deletion vector on top
of it.

## 4. The pins, and what each one is for

| Pin | Asserts | Red before |
|---|---|---|
| `call_remove_orphan_files_dry_run_lists_without_deleting` | Default lists 2 orphans, Spark's one-column non-nullable schema, and **not one file moves** | Procedure refused |
| `call_remove_orphan_files_armed_deletes_orphans_and_nothing_else` | `dry_run => false` removes **exactly** the planted set out of a directory holding live files too | Procedure refused |
| `call_orphan1_requires_an_explicit_older_than` | The bare call refuses by argument name and touches nothing | Refused under the old fork-queue message |
| `call_remove_orphan_files_enforces_sparks_twenty_four_hour_floor` | `now` and `now − 23h` refuse and delete nothing; `now − 25h` runs | Procedure refused |
| `call_remove_orphan_files_refuses_deferred_arguments` | Six deferred arguments refuse by name rather than being ignored | Procedure refused |
| `call_remove_orphan_files_refuses_a_quoted_dry_run` | `'false'` refuses instead of arming the deletion | Procedure refused |
| `call_orphan_shared_ctas_root_rule` | The shared-root rule fires on the fallback root, and NOT on an owned location under the same root, a similarly-named sibling, or a remote policy | (new; §3a) |
| `test_remove_orphan_files_refuses_the_shared_ctas_fallback_root` | The same refusal end to end through the facade, on the exact fixture that surfaced it | (new; §3a) |

Seven tests went red against the unwired router, which is the whole orphan set plus the retired
refusal pin.

**The "and nothing else" clause is the one that matters.** The slate said "it deleted the orphans"
is half a test, so the armed pin lists the entire table directory before and after and asserts the
set difference is exactly the planted orphans. A pin that only checked the orphans were gone would
pass just as happily against a procedure that deleted the whole table.

## 5. The fixture has to age its files, and that is a real property

The fork cuts on the LISTED file's `created_at_millis`, which for local storage is opendal's
`last_modified`. A freshly written orphan is newer than any cutoff the floor permits, so a naive
fixture — write a file, call the procedure — can never delete anything and would pass while
proving nothing.

The fixture therefore back-dates its planted orphans ten days through `std::fs::FileTimes`. No new
dependency: `filetime` was the obvious reach and `std::fs` has done this since 1.75.

The same constraint applies to the production path, and it is worth stating because it is
counter-intuitive: **this procedure cannot delete a file created in the last 24 hours**, no matter
what arguments it is given. That is the floor working, not a bug.

## 6. One dependency note

The floor needs a clock. `chrono` is a **dev** dependency of `repark-spark` on purpose — the crate
map says so — so `now_millis` uses `std::time::SystemTime` rather than promoting a dependency for
one subtraction. A clock before the epoch or beyond the representable millisecond range refuses
rather than deleting against an unknown cutoff.

## 7. Retired

`call_remove_orphan_files_refuses_loud_with_fork_queue` pinned this procedure refusing as a
fork-queue residual. The fork surface it was waiting on is now wired, so the refusal it asserted
is gone. It is replaced by `call_remove_orphan_files_is_no_longer_an_unsupported_procedure`, which
keeps the part still worth guarding, and the facade's equivalent is replaced by three pins carrying
the real behaviour.

## 8. What MW-4 and MW-5 inherit

Every procedure the campaign set out to wire is wired. MW-4's live acceptance can now exercise the
full maintenance surface against a real catalog, which is what the campaign was for.

Two things ride into MW-5 unchanged from MW-2: the `MOR-1` fork-planner question and the
`B-MOR-3` deletion-vector posture. The floor, the one open question MW-3 was chartered to raise,
turned out to have been answered by Java already.

MW-3 adds one: **roadmap item A13**, the shared CTAS fallback root (§3a). It is a write-path
question rather than a maintenance one, and this unit deliberately guarded against it instead of
fixing it.
