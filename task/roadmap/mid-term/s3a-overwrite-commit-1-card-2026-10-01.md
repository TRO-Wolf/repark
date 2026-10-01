# S3A-OVERWRITE-COMMIT-1 — s3a overwrite deletes the destination before the copy

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 63).
**Severity:** wrong results, not yet rated (owner) — data loss on failed
overwrite. **Status:** pre-existing (measured on `3d4f2030` and on base).
**Target:** mid-term.

## The divergence it records

An s3a `mode("overwrite")` text write deletes the destination's existing
objects BEFORE the COPY, so a failed overwrite leaves the destination empty.
Spark's commit protocol keeps the old data until commit. The same holds on
`3d4f2030` and on base. Measured by the #889 rollback fold on moto.

## Repro (from the source description; re-measure on moto on the day)

1. Write a text dataset to an s3a (moto) destination.
2. Overwrite it with a write that fails mid-commit.
3. RePark: the destination is empty. Spark 4.1.2: the old data is intact.

## Cause and suggested fix direction (a suggestion, not a decision)

The path writer deletes before it copies; Spark commits (copies) before it
exposes the new data. Any fix direction (stage-then-swap, commit protocol) is
suggested by the verifier/orchestrator, not a decision.

## Clauses (draft)

- **C-001** A failed s3a overwrite leaves the previous destination readable.
- **C-002** A successful s3a overwrite still replaces the destination exactly once.
- **C-003** The moto rollback repro joins the gate as a regression pin.

## Pointers

- Up: [map.md](map.md)
