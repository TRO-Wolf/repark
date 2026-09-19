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

## Provenance

Recorded by the orchestrator on 2026-09-19 from live PySpark 4.1.2
(`record_cast_map.py`, one short JVM, `local[2]`, UTC,
`spark.sql.session.timeZone=UTC`, ANSI on except the legacy cell) and copied verbatim
into this directory; the repo-idiomatic re-deriver is
[../_record_cast_map_spell_1.py](../_record_cast_map_spell_1.py), which exits
non-zero on drift under `--check`.

SHA-256 of the fixture file:

`9ac3edde6e4dc38766c7d23bb13e7a49f009070730c6449d6984e05f122e5690`

## Debug

- A re-recorded fixture differs: run the re-deriver with `--check` to see the first
  drifting cell, then diff the JSON key by key; struct-in-map values serialize
  positionally (PySpark `Row` is a tuple subclass, so `json` renders structs as arrays
  and drops field names) while top-level maps keep their keys.
