# map — python/repark-parity/tests/live_db

## Purpose

C-0 (2026-10-02): live Postgres cells for the 1.6 connector units and the 1.7
capture harness. Each test gets a schema, a publication name and a slot name
that carry one random tag; all three drop on exit. The five cdc S0 pins are
`xfail(strict=True)` until the producer arrives. Cells skip when
`REPARK_PG_URL` is unset.

## Contents

- `__init__.py` — empty package marker.
- `conftest.py` — `pg_live` and `pg_live_factory`; unique names; drop the
  publication, the slot and the schema on exit. `psycopg` is imported after
  the `REPARK_PG_URL` skip so CI collection without the extra stays green.
- `test_c0_cdc_scenarios.py` — `test_fixture_isolates_names` (green) and the
  five S0 pins: crash after commit, snapshot-to-WAL handover, replay and
  duplicate delivery, schema change plus partial update image, lost slot.

## Pointers

- Up: [../map.md](../map.md)
- Container: [../../../../scripts/dev/pg/map.md](../../../../scripts/dev/pg/map.md)
- Contract: [../../../../docs/testing.md](../../../../docs/testing.md)
