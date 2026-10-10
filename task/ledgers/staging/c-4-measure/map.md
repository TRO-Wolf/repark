# map — task/ledgers/staging/c-4-measure/

## Purpose
Evidence for the C-4 measure round (2026-10-10, Frontier D13): raw
INSERT-versus-COPY cells for statement-level triggers and foreign tables, run
against the C-0 disposable Postgres. The source file carries no comments by the
lane's owner ruling; this map carries the reason. Branch `feat/c-4-routing`.

## Contents
- [probe_copy_vs_insert.py](probe_copy_vs_insert.py) — reads `REPARK_PG_URL`,
  builds one private schema with a BEFORE/AFTER EACH STATEMENT pair, a
  transition-table trigger, a loopback `postgres_fdw` table over a remote table
  with defaults, a check and row plus statement triggers, and a `file_fdw`
  table when the image ships it. Runs the same 3 rows through one plain
  `INSERT` and one `COPY ... FROM STDIN (FORMAT BINARY)` per cell, prints rows
  stored, audit rows in firing order, transition contents, and error class and
  text, then drops the schema and the servers. Run it as
  `REPARK_PG_URL=$(make pg-url) .venv/bin/python probe_copy_vs_insert.py`.
- [raw_output.txt](raw_output.txt) — the probe's stdout from the committed run,
  kept verbatim. The ledger's §11 cell table transcribes it.

## Pointers
- Up: [../map.md](../map.md)
- Ledger: [../c-4-ledger.md](../c-4-ledger.md)
