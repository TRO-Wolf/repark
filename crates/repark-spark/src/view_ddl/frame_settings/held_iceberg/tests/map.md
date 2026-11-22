# map — repark-spark/src/view_ddl/frame_settings/held_iceberg/tests

## Purpose

`levels.rs` (`mod levels;` in [../tests.rs](../tests.rs)): ICE-HELD-FRAME-SNAPSHOT-1 unit 2 delta 2 (2026-11-22), the
owner's cache-shape rule on the two `CacheLevel`s, with a second Spark-door session over the same memory catalog as
the other writer. Each cell fills `[1, 2]`, lets the other writer insert `9` (the cached read before any fresh read
keeps the fill: a held action makes no catalog call), then takes a fresh SQL read (`[1, 2, 9]`, the epoch moves).

## Contents

- `levels.rs` —
  - a table-level fill keeps its rows and buffer after that read, after a same-session `SET TBLPROPERTIES` and a
    fresh read, and refills on `REFRESH TABLE`;
  - a table-level fill keeps its buffer after each of `SET TBLPROPERTIES`, `ADD COLUMN` and `CREATE BRANCH`;
  - a query-level fill refills after that read (`[1, 2, 9]`), keeps the refill after an unchanged fresh read and a
    same-session `SET TBLPROPERTIES`, refills on `REFRESH TABLE`, and keeps that refill after the next property
    commit (G5: `record` carries `refreshed`);
  - both levels refill after a same-session insert (`[1, 2, 8, 9]`);
  - four plans (the table, an alias, a filter, an aggregate) keep at `Table` and refill at `Query`: the level, not
    the plan, decides.

Up: [../map.md](../map.md).
