# task/wo/microbatch/ — the micro-batch packet (docs only, 2026-10-05)

The design packet for the Bronze-to-Silver micro-batch sink (plan:
[../roadmap/epic-term/microbatch-cdc-sink-plan-2026-10-04.md](../../roadmap/epic-term/microbatch-cdc-sink-plan-2026-10-04.md)).
Owner, 2026-10-05: "Go ahead on items 1–4 … Packet PR is docs-only, no Opus
verifier." Markdown only; no code, no probes, no Spark runs. MB-0 records the
oracle later.

- [packet.md](packet.md) — D-1…D-8 in one line each, O-1…O-10 plus O-9a as
  ruled, the merge order and waves as amended, the fork asks, and the open
  decisions the design sketch must close.
- [mb-0-oracle.md](mb-0-oracle.md) — **MB-0, grade C:** the recorded-oracle
  order. The full cell list with statements, answer kinds and JSON fields;
  the runner mechanics; the SHA-256 rule.
- [mb-design-sketch.md](mb-design-sketch.md) — **the design-sketch order,
  grade B** (Opus 5.5, in the ATTR-ID-1 design-order shape). Closes every
  open decision, names every type and signature, fixes the error classes
  from MB-0, specifies the crash harness scenarios as test names.
- [mb-design-2026-10-06.md](mb-design-2026-10-06.md) — **the design sketch
  itself (Opus 5.5, 2026-10-06)**, read by the owner before MB-1 and MB-2a
  open: the North Star §7 checklist, W8 ruled, Q1…Q11 closed from the MB-0
  cells, every type and signature, the MBE error-class registry, the five
  harness pins, the slice corrections, the four-line records R-1…R-18 and
  the owner questions.
- [mb-1-source.md](mb-1-source.md) — **MB-1, grade B:** the batch source over
  the incremental append scan. Muse at max, beside C-1.
- [mb-2a-sink-offsets.md](mb-2a-sink-offsets.md) — **MB-2a, grade B:** summary
  stamping and the offset table property; resume-from-sink. Muse at max,
  beside C-1.
- [harness.md](harness.md) — **the crash harness, grade B:** the five
  scenarios as red-first pins, written before MB-2c. Muse at max.
- [mb-2c-replay-reconcile.md](mb-2c-replay-reconcile.md) — **MB-2c,
  grade B:** the epoch check before commit, the C-008 walk on unknown
  outcome, generation fencing. Turns the harness green.
- [mb-3-driver.md](mb-3-driver.md) — **MB-3, grade B:** the Session-owned
  driver task, both triggers, the four shutdown rules, progress reporting.
- [mb-4-facade.md](mb-4-facade.md) — **MB-4, grade B:** the builders,
  `StreamingQuery`, the error classes, the SES-DECL flips, the IPI-47 cells.
  Surface first against a stub binding, wire-up last.
- [mb-5-multi-source.md](mb-5-multi-source.md) — **MB-5, grade B:** vector
  offsets, inputs pinned at their end snapshots, the hook for S-2.
- [mb4_lineage_timing.py](mb4_lineage_timing.py) — **MB-4 fold 3 (2026-10-10, owner ruling
  D5):** the quiet-box measurement for the lineage-audit gate. `measure <result.json>` times a
  200-epoch `availableNow` run, three runs each, over a `foreachBatch` body that appends, one
  that writes nothing, and `toTable` as the control; `compare <no_audit.json> <audited.json>`
  prints the ratios and exits 1 when a `foreachBatch` ratio is above 1.05. Run `measure` on a
  build of the commit before the audit (`2877da20`) and on the head, each after `make
  develop`, with no other build holding the box. It retires when the D5 gate is recorded.

Up: [../map.md](../map.md).
