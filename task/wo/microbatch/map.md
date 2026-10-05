# task/wo/microbatch/ — the micro-batch packet (docs only, 2026-10-05)

The design packet for the Bronze-to-Silver micro-batch sink (plan:
[../roadmap/epic-term/microbatch-cdc-sink-plan-2026-10-04.md](../../roadmap/epic-term/microbatch-cdc-sink-plan-2026-10-04.md)).
Owner, 2026-10-05: "Go ahead on items 1–4 … Packet PR is docs-only, no Opus
verifier." Markdown only; no code, no probes, no Spark runs. MB-0 records the
oracle later. The slice orders land in a follow-up commit on this branch.

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

Up: [../map.md](../map.md).
