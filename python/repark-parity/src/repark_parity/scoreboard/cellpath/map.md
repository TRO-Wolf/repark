# map — python/repark-parity/src/repark_parity/scoreboard/cellpath

## Purpose

Helper modules that the stored case code imports by bare name, for example
`from typed_inputs import input_table` in 2,160 stored cells and `from local_files import ...` in
130. A case definition is digest-bound, so the import text cannot change; the
launcher puts this directory on every engine process's import path instead. It is not a package.

## Contents

| File | Used by |
|---|---|
| `typed_inputs.py` | `input_table()`: the five-row table of eighteen typed columns of the 2026-11-16 coercion grid (nested-pairs and its write lanes) |
| `local_files.py` | `datafiles`, `text_content`, `read_written`, `codec_written`, `compression_header`: cold reads of written files without the engine (files, settings-files and write lanes) |
| `inventory_names.py` | `survey(...)`: public names and signatures per facade surface, the inventory lane (recorded exclusion) |

A new helper here keeps a bare, unique module name, and a change to an existing one re-records every
lane that imports it, since the recorded Spark answers were produced with the old code.

## Pointers

- Up: [../map.md](../map.md)
