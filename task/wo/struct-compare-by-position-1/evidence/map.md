# task/wo/struct-compare-by-position-1/evidence/

- [fable-brief-2026-11-09.md](fable-brief-2026-11-09.md) — Fable 5.1's brief for this unit, the design source; the order amends it.
- [fable-evaluation-2026-11-09.md](fable-evaluation-2026-11-09.md) — the Step 4.1–4.2 evaluation: the mechanism trace and the parked branch; its §1 cells 16, 22, 38 and 39 are superseded by the measured tables.
- [inventory.md](inventory.md) — the insertion inventory (brief §6, each row answered at the seat) and the type-variant inventory.
- [step0/](step0/) — the 4393-cell step-0 table on Spark 4.1.2 and 1.5.6: `table.md`, `groups.txt`, `notes.md` (with the derived-oracle section), `provenance.md`, the generator `gen_cmp.py`, the driver `drive_cmp.py`, the pass-2 cells `pass2_cmp.py`, the renderer `render2.py`, the oracle repro `probe_delete_literal.py`, `cells.jsonl`, `derive-cells.jsonl` and `raw/`.
- [variant-seat-c/](variant-seat-c/) — the variant table's seat C (P10, P10dml: 218 cells per leg): the extracted `table.md` section, `notes.md`, `provenance.md`, the probe and the seat C records of `raw/`. The full variant table is committed under `../../struct-setop-type-family-1/evidence/`; the probe here is that lint-clean copy.
- [variant-ext/](variant-ext/) — the 473-cell variant extension (orientation, encodings, depth, same-order control, typed and native doors, DML): `table.md`, `notes.md`, `provenance.md`, `probe_cmp_variant.py` and `raw/`.

The probes are committed lint-clean (ruff check and format). The lint edits are proven behaviour-identical by a
byte-identical re-render: `render2.py` reproduces `step0/table.md` (sha256 `95a7f29d…`) and `step0/groups.txt`
(`a7fff9c1…`), and `probe_cmp_variant.py --render` reproduces `variant-ext/table.md` (`50dfb064…`), before and after.
Absolute paths are scrubbed: the Iceberg jar to `<ivy-jar>` (override with `STRUCT_CMP_IVY_JAR`), the oracle repro's
warehouse to `<warehouse-spark>` (`STRUCT_CMP_WAREHOUSE`), the lock to `<spark-jvm-lock>`. Run a leg with the target
tree's own interpreter from a scratch copy, never here.

Typos skips this directory (`extend-exclude` in `.typos.toml`): the evidence holds verbatim shape names and truncated
engine messages that must match the raw keys, the same reason as the sibling unit's evidence entry.

Up: [../map.md](../map.md).
