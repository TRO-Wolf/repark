# task/wo/listview-inner-join-key-1/evidence/

- [step0/](step0/) — the clause: the clerk's 2026-11-11 step-0 table (326 main and 300 Spark cells), committed by build item 1 with its paths scrubbed.
- [item1/](item1/) — build item 1: the cause confirmed on main (`cause.md`), the completed table on main and Spark 4.1.2 (`table.md`, `raw/`), the scratch experiment's legs, and the proposed fix shape.
- [pin_cells.py](pin_cells.py) — build item 2: writes the facade pin's cells file
  (`python/repark/tests/sql/data/listview_inner_join_key_1_cells.jsonl`) from the committed legs: the
  step-0 and item-1 Spark records, main's refusal for SQL `USING`, and the probes' own key literals.

The probes are committed lint-clean (ruff check and format) and path-free: each takes `--out` and reads the Iceberg jar from `ICEBERG_JAR`. Run a leg with the target tree's own interpreter from a scratch copy, never here.

Typos skips this directory (`extend-exclude` in `.typos.toml`): the evidence holds truncated engine messages and statement text that must match the raw records, the same reason as the sibling units' evidence entries.

Up: [../map.md](../map.md).
