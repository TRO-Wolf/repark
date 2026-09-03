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
  F-17 shared-Puffin closure — landed #237; RP-3 consumed at fork `d408da42` 2026-08-30, then V3-3+; OD-3b's IAM applied 2026-08-28, measured **allow** by MW-10 on format v2, 2026-08-30). Matrix cells carry dated updates as rows move;
  the 2026-08-24 owner rulings (Lane A charter, the encryption-keys DECLARED exclusion) are
  recorded in the matrix and sequenced on [../../../briefs/next-sequence.md](../../../briefs/next-sequence.md).
  Truth-up 2026-09-02: nightly row ✅ after #300 (first green nightly 2026-09-02).
  LIVE-v3-M (2026-09-02): the "Live: Glue + S3 Tables v3 legs" row is ✅ — `aws-acceptance`
  run 33635288918 on merged `main` `8c4bc55` ran both legs green, S3 Tables accepting
  `format-version = 3` at CREATE and Glue reproducing the local numbers exactly; the row cites
  registry `S3T-V3-1` and keeps the MW-10 format-v2 permission sentence and its evidence links.
  The §3 gate ("every row ✅ or a dated DECLARED residual") is satisfied for this row.
  pins: live-v3-aws-legs/C-004; live-v3-first-measurement/C-001
  **V1-GATE (2026-09-03): the gate is audited.** §3.1 is the audit — one row per §3 row with
  its glyph, claim, residual, that residual's registry class and date, and its pin — plus the
  five 🟡 fork rows the gate leans on, read at the consumed pin `594bdbe5`. Three matrix glyphs
  moved to the gate's own wording (types ⚠→✅ on `V3-GEO-1` / `V3-VARIANT-SHRED-1`, encryption
  ❌→✅ on `ENC-1`, DV maintenance ⚠→✅ on `B-MOR-3` under owner decision OD-2) and the
  `rewrite_manifests` row records its v3 exercise from SCALE-v3. The gate paragraph carries one
  dated line: every row ✅ or dated DECLARED as of 2026-09-03, the tag the owner's step.
  The audit is scoped to each row's "v1.0 requires" cell; residuals on the same surface but
  outside it (`RDF-1`, `ORPHAN-1/2`, `MANIFEST-1/3`) are tabled beneath, recorded not gating.
  **§2 pillar 4 is discharged (V3-COV, 2026-09-03):** the statement matrix is measured — 81
  programs, 267 cells, 72 EQUAL, 8 rows filed, 2 defects FIXED —
  [../../../docs/design/v3-statement-coverage.md](../../../docs/design/v3-statement-coverage.md).
  `B-MOR-3` FIXED 2026-09-03 (owner ruling: build). The v1.0 tag is what remains.
  pins: v1-gate-audit/C-001, C-002, C-004
  pins: v3-cov-statement-coverage/C-005
- [release-roadmap-2026-08-29.md](release-roadmap-2026-08-29.md) — **the release roadmap
  (owner-set 2026-08-29):** every tag from v0.6 to 3.0 with the owner's rulings folded in.
  Pre-1.0 closes the floating gaps (Track-B DML remainder with the verified F-5 correction,
  example docs, torture suite, Never-OOM matrix, `repark.toml` with named sources and the
  governing federated-SQL rule); 1.x is parity, connectors, dbt and the Spark Connect server;
  2.x is Flight SQL + the API freeze, then maintenance policy, change-data reads, CDC ingestion,
  MVs, observability, Substrait; 3.0 is the trust promise. Q&A log of every ruling at the end.
- [roadmap-design-plan-2026-08-29.md](roadmap-design-plan-2026-08-29.md) — **the design plan
  by crate (ruled 2026-08-29):** the release roadmap's *where* and *how* — one work card per
  roadmap item naming the crate (NEW or UPDATE, tier, `ALLOWED_EDGES` rows), the reference
  implementation to read first, ordered steps, pins, the done condition and the hand-back
  points, written for delegated sub-agents. §0 carries the tier map and the mechanical
  new-crate checklist; §6 records the six placement rulings (D-5: 2.2's changelog is a
  RePark-side snapshot diff over upstream-compatible primitives, so it survives a later
  migration off the fork).

## Pointers
- Up: [../map.md](../map.md)
