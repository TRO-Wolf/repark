# map — task/roadmap/epic-term/

## Purpose
North-star tracks, shaped like [../../../PROJECT.md](../../../PROJECT.md) roadmap items: a
direction, not a unit list. A track leaves for `../mid-term/` when an intake evaluates it.
The capability roadmap owns the planning identifiers and scope. Its historical version labels
remain lookup aids for existing briefs; package versions are assigned during release assembly.
PROJECT.md points here rather than restating the roadmap.

## Contents
- [project-performance-and-unsafe-rust-brief-2026-09-04.md](project-performance-and-unsafe-rust-brief-2026-09-04.md) A 2026-09-06 reconciliation note at its head lists the slate units that landed after it was written.
  — proposal opened 2026-09-04 consolidating production feedback, measured and candidate
  performance improvements, possible unsafe Rust exceptions, and agent isolation, patch
  admission, and safety review. Includes evidence limits, guard tests, delivery groups, and
  pickup decisions. Current unsafe prohibitions and agent permissions remain in force.
- [sepmo-efficiency-implementation-brief-2026-09-04.md](sepmo-efficiency-implementation-brief-2026-09-04.md)
  — proposal opened 2026-09-04 for SEPMO token use and agent performance. Records the review,
  measured document footprint, compact role packets, evidence collection, telemetry, a controlled
  pilot, and the amendment boundaries for review and verification policy. Includes delivery
  groups and a pickup checklist; implementation scope audit is pending.
- [rust-unification-implementation-brief-2026-09-04.md](rust-unification-implementation-brief-2026-09-04.md)
  — proposal opened 2026-09-04 for Rust-only batch, native database change capture, streaming,
  and Iceberg unification. Records the owner's JVM-free production constraint, the recommended
  PostgreSQL-to-Iceberg first milestone, recovery and performance evidence, open decisions,
  and the pickup procedure. Implementation scope audit is pending.
- **DataFrame core decomposition (2026-09-07) — closed the same day**, all seven units merged;
  the plan is archived at [docs/history/dfcore/](../../../docs/history/dfcore/map.md).
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
  five 🟡 fork rows the gate leans on, read at the consumed pin `189a73ed`. Three matrix glyphs
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
- [release-roadmap-2026-08-29.md](release-roadmap-2026-08-29.md) — capability IDs, legacy-label
  mapping, scope and dependency decisions, proposed Rust-first intake guidance, and pickup
  evidence links. The 2026-09-07 planning
  revision separates capability acceptance from package versions; original scope sections and
  dated decisions remain addressable.
- [roadmap-design-plan-2026-08-29.md](roadmap-design-plan-2026-08-29.md) — **the design plan
  by crate (ruled 2026-08-29):** historical implementation cards — one work card per
  roadmap item naming the crate (NEW or UPDATE, tier, `ALLOWED_EDGES` rows), the reference
  implementation to read first, ordered steps, pins, the done condition and the hand-back
  points, written for delegated sub-agents. Resolve its original labels through the capability
  roadmap and revalidate assumptions against the current contract before dispatch.

## Pointers
- Up: [../map.md](../map.md)
