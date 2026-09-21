# map — repark-spark/src/time_travel

## Purpose

Relation rewrites that pin an Iceberg read onto a temp provider before planning, released
through `PinnedViews` after the statement. The time-travel half lives in `../time_travel.rs`;
this directory holds the siblings that share its machinery.

## Contents

- `changes.rs` — **ICE-CHANGELOG-1 (2026-09-20):** the `t.changes` relation on the SQL door.
  `sql_may_have_changes_relation` is the cheap guard `router.rs` calls; `prepare_changes_sql`
  finds every 4-part identifier whose last part is `changes` in a FROM / JOIN / comma position,
  loads the parent table, registers a `ChangelogTableProvider` temp view and splices the token
  span, right to left so the earlier spans' indices stay valid. It is a sibling of the
  metadata-table rewrite, never part of it: `changes` is not a `MetadataTableType`, and
  `metadata_tables.rs` holds an exact 1062-line baseline that this unit does not touch.
  pins: ice-changelog-1/C-009

## Pointers

- Up: [`../map.md`](../map.md) — the Spark door's module map.
- Related: [`../call/changelog.rs`](../call/changelog.rs) — the row transforms the PROCEDURE
  applies over this relation; the relation itself stays raw.
