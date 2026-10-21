# task/wo/map-column-panic-1/ — MAP-COLUMN-PANIC-1

MAP-COLUMN-PANIC-1 (2026-10-20): `map(<key>, <non-literal value>)` answers Spark's rows and type on every door
instead of panicking `list array` or raising `map requires key and value lists to have the same length`. The
card is [map-column-panic-1-card-2026-10-20.md](../../roadmap/mid-term/map-column-panic-1-card-2026-10-20.md).
The unit built on branch `fix/map-column-panic-1` from main `e6ff5375` (builder Claude Sonnet 5.5, orchestrator
review, no Opus verifier). It retires when the pull request merges and the card is closed in the same change.

- [evidence/](evidence/map.md) — the red-on-main logs, the ZONE-ORACLE-1 corpus delta, the cost medians and the
  recorded Spark probe.

Up: [../map.md](../map.md).
