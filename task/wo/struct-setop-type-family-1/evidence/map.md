# task/wo/struct-setop-type-family-1/evidence/ — the variant-table evidence

The measured type-variant inventory behind STRUCT-SETOP-TYPE-FAMILY-1, committed
verbatim except the lint pass on the probe and the path scrubs below.

- [variant_probe.py](variant_probe.py) — the 14-plan probe (legs spark/base/head) plus
  the renderer. Committed lint-clean (ruff check + format); the lint edits are proven
  behavior-identical by a byte-identical `--render` (sha256 `4a322398…`). The Ivy jar
  path is scrubbed to `<ivy-jar>` overridable via `VARIANT_TABLE_IVY_JAR`; run a leg
  with the target tree's own interpreter from a scratch copy, never here.
- [variant-table.md](variant-table.md) — the rendered table at the `b8ad7a9c` head leg,
  verbatim.
- [after-table.md](after-table.md) — the same render with the head leg re-run on this
  unit's head (`raw/head-66cd2c82.jsonl`).
- [notes.md](notes.md) — the measurer's construction notes, verbatim.
- [provenance.md](provenance.md) — legs, interpreters and tools; the lock path is
  scrubbed to `<spark-jvm-lock>`.
- [raw/](raw/) — `spark.jsonl`, `base.jsonl`, `head.jsonl` (the `b8ad7a9c` leg),
  verbatim, plus `head-66cd2c82.jsonl` (this unit's re-run head leg).

Typos skips this directory (`extend-exclude` in `.typos.toml`): the evidence holds
verbatim probe shape names (`reord`) and truncated engine messages (`Siz`) that must
match the raw keys, so it is excluded rather than edited — the same reason as the
`task/census/` and `task/mw-6-critic-evidence/` entries.

Up: [../map.md](../map.md).
