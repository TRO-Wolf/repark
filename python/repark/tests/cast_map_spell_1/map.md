# map — python/repark/tests/cast_map_spell_1

## Purpose

Recorded Spark 4.1.2 oracle for CAST-MAP-SPELL-1: every `CAST(… AS MAP<…>)` and
`.cast(MapType)` cell answers with schema string, per-field nullability and rows,
and the two refusal cells carry Spark's exception class plus error-class token.
pins: cast-map-spell-1/C-001, C-002

## Contents

- [cast_map_spell_1_spark_oracle.json](cast_map_spell_1_spark_oracle.json) — the 21
  recorded cells (17 SQL cells plus the ANSI-off legacy twin, the `UNION ALL` typed-NULL
  cell, and 3 DataFrame-door `.cast` cells).
  pins: cast-map-spell-1/C-001, C-002, C-003, C-004
- [cast_map_spell_1_round4_spark_oracle.json](cast_map_spell_1_round4_spark_oracle.json) —
  the orchestrator's 12 round-4 cells (ANSI in each key's suffix): DEL, tab and U+0085
  prefixes before `INT` / `BOOLEAN` leaves, the `128` to `TINYINT` overflow message, and
  `try_cast` of an overflowing key (Spark stores a NULL key; `keys`/`values` recorded).
  pins: cast-map-spell-1/C-013, C-015, C-016
- [cast_map_spell_1_round3_spark_oracle.json](cast_map_spell_1_round3_spark_oracle.json) —
  the 17 round-3 cells, each with its own `ansi` flag: colliding keys after a key cast
  (and their stored order through `map_keys`), `try_cast` / legacy key legality, leaf
  overflow, whitespace and fractional-text leaves, and a `/*! … */` comment hint.
  pins: cast-map-spell-1/C-011, C-012, C-013, C-014

## Provenance

Recorded by the orchestrator on 2026-09-19 from live PySpark 4.1.2
(`record_cast_map.py`, one short JVM, `local[2]`, UTC,
`spark.sql.session.timeZone=UTC`, ANSI on except the legacy cell) and copied verbatim
into this directory; the repo-idiomatic re-deriver is
[../_record_cast_map_spell_1.py](../_record_cast_map_spell_1.py), which exits
non-zero on drift under `--check`.

SHA-256 of the fixture file:

`9ac3edde6e4dc38766c7d23bb13e7a49f009070730c6449d6984e05f122e5690`

Round 3 (2026-09-19): recorded by claude-opus-5 from live PySpark 4.1.2 through the same
re-deriver (`record_round3_oracle`, one short JVM, `local[2]`, UTC, each cell under its own
`spark.sql.ansi.enabled`); it re-derives the orchestrator's measured cells of the same SQL
and adds `dup_keys_order_ansi`, `try_widen_key_*`, `bigint_overflow_leaf_*` and
`fraction_text_leaf_*`. Colliding keys: `map_keys` answers `[2, 1, 2]`, so Spark's cast
stores both entries and the `{1: 'b'}` a `collect()` shows is the Python dict built from
them.

SHA-256 of the round-3 fixture file:

`622fad330686b98e7efe07c4c17bb67d1c3416840914a2a6b09e1dc1fc454459`

Round 4 (2026-09-19): recorded by the orchestrator from live PySpark 4.1.2 and committed
verbatim (`json` holds Spark's `to_json` of the map). SHA-256:

`8748b958219af57d20daa94b58be9bd285f5670fb4e9ed6ade74a6c290864970`

## Debug

- A re-recorded fixture differs: run the re-deriver with `--check` to see the first
  drifting cell, then diff the JSON key by key; struct-in-map values serialize
  positionally (PySpark `Row` is a tuple subclass, so `json` renders structs as arrays
  and drops field names) while top-level maps keep their keys.
