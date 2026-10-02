# JSON-NTZ-NULL-INFERENCE-1 — mostly-null NTZ column drops out of JSON inference

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 65;
#889 fold). **Severity:** not rated. **Status:** pre-existing. **Target:**
mid-term.

## The divergence it records

A mostly-null NTZ column drops out of JSON read-back key inference, so
appending such a frame to its own seed fails append validation.

## Repro (from the source description; re-measure on the day)

1. Build a frame with a mostly-null NTZ (timestamp without time zone) column.
2. Round-trip it through JSON and read it back.
3. Append the read-back frame to its own seed: append validation fails.

## Cause and suggested fix direction (a suggestion, not a decision)

The JSON read-back key inference loses the NTZ column when its values are
almost all null. Any fix direction is suggested by the verifier/orchestrator,
not a decision.

## Clauses (draft)

- **C-001** The mostly-null NTZ column survives JSON read-back inference.
- **C-002** The append-to-own-seed repro passes validation with Spark's schema.
- **C-003** Neighbouring null-heavy inference cells (other temporal types) are measured first.

## Pointers

- Up: [map.md](map.md)
