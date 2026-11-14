# task/wo/nan-minmax-1/evidence/step0/ — the constant-file table and the read paths

Step 0 of NAN-MINMAX-1, 2026-11-13, model `claude-opus-5-5`, measured on main `631d283c` before any product change,
then re-run on the head.

- [table.md](table.md) — provenance, data, cells and the per-cell table: Spark 4.1.2 against main and the head. Every
  NaN of a constant file reads as `1.0` on main, through both doors, the round trip and the ingestion; the head
  equals Spark except four refused paths and six sign-set `max`/`min` cells owed to part 2.
- [paths.md](paths.md) — every read path that can reach the constant-column replacement or the statistics shortcut,
  with file:line, and whether it goes through `spark_nullable.rs`'s format.
- `probe.py` — writes the source files (`files`) and runs one engine's legs (`reads`, `paths`, `roundtrip`,
  `ingest`, `cross`) into a JSONL file.
- `render.py` — renders `table.md`'s table from the Spark legs and one leg set per build.
- [raw/](raw/map.md) — the JSONL legs.

Up: [../map.md](../map.md).
