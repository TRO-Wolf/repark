# map — repark-spark/src/merge

## Purpose

MERGE INTO lowering helpers beside `../merge.rs` (IPI-19 + IPI-56, 2026-09-20).
`../merge.rs` sits exactly at the default size ceiling, so the evolution strip
and the star-sentinel rewrite live here instead of in it.

## Contents

- `schema_evolution.rs` — the `MERGE WITH SCHEMA EVOLUTION` pre-parse strip: a
  significant-token match on `MERGE` + `WITH` + `SCHEMA` + `EVOLUTION`
  (case-insensitive, comments and whitespace ignored), the four tokens removed
  so the rest parses as an ordinary MERGE, and a bool carried into the lowered
  plan. A plain `MERGE INTO` is never touched. File-backed tests in
  [schema_evolution/map.md](schema_evolution/map.md).
  pins: ipi-19-56-37-schema-evolution-write/C-005, C-007
- `stars.rs` — the `UPDATE SET *` / `INSERT *` sentinel rewrite, moved here from
  `../merge.rs` untouched: a `*` in star position becomes the `STAR_SENTINEL`
  form stock sqlparser parses, and only there.
  pins: ipi-19-56-37-schema-evolution-write/C-005

## Pointers

- Up: [../map.md](../map.md).
- Ledger: `../../../../task/ledgers/staging/ipi-19-56-37-schema-evolution-write-ledger.md`.
