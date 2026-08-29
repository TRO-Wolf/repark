# map — task/roadmap/epic-term/

## Purpose
North-star tracks, shaped like [../../../PROJECT.md](../../../PROJECT.md) roadmap items: a
direction, not a unit list. A track leaves for `../mid-term/` when an intake evaluates it.
First track landed 2026-08-23 (the v1.0 north star); the release roadmap landed 2026-08-29 and
is now the epic list from v0.6 through 3.0 — PROJECT.md points here rather than restating it.

## Contents
- [v1-0-iceberg-v3-northstar.md](v1-0-iceberg-v3-northstar.md) — **the v1.0 north star
  (owner-set 2026-08-23):** full production-grade Iceberg format-v3 — the four pillars, the
  acceptance matrix that gates the v1.0 tag, and the two-lane path (guarded RP-2 salvage — landed; fork
  F-17 shared-Puffin closure — landed #237; immutable RP-3 at fork `d408da42` next, then V3-3+). Matrix cells carry dated updates as rows move;
  the 2026-08-24 owner rulings (Lane A charter, the encryption-keys DECLARED exclusion) are
  recorded in the matrix and sequenced on [../../../briefs/next-sequence.md](../../../briefs/next-sequence.md).
- [release-roadmap-2026-08-29.md](release-roadmap-2026-08-29.md) — **the release roadmap
  (owner-set 2026-08-29):** every tag from v0.6 to 3.0 with the owner's rulings folded in.
  Pre-1.0 closes the floating gaps (Track-B DML remainder with the verified F-5 correction,
  example docs, torture suite, Never-OOM matrix, `repark.toml` with named sources and the
  governing federated-SQL rule); 1.x is parity, connectors, dbt and the Spark Connect server;
  2.x is Flight SQL + the API freeze, then maintenance policy, change-data reads, CDC ingestion,
  MVs, observability, Substrait; 3.0 is the trust promise. Q&A log of every ruling at the end.

## Pointers
- Up: [../map.md](../map.md)
