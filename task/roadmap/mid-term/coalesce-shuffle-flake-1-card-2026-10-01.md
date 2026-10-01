# COALESCE-SHUFFLE-FLAKE-1 — attack3 coalesce/shuffle wrong-result flake

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 64;
#888 re-verify 7). **Severity:** wrong results, not yet rated (owner).
**Status:** pre-existing, reproduced on base. **Target:** mid-term.

## The divergence it records

An attack3 coalesce/shuffle wrong-result flake, reproduced on base. The result
rows are wrong (not a refusal), and the failure is intermittent.

## Evidence location

`/tmp/oc-worker/direct/wo/reverify7-nvl/` (retained; verified present
2026-10-01: subdirectories `attack`, `fold`, `replay`, `wh`).

## Cause and suggested fix direction (a suggestion, not a decision)

No cause recorded in the source line; the card opens by replaying the
retained evidence on base until the flake shows, then bisecting the shuffle /
coalesce path. Any direction that investigation proposes is a suggestion, not
a decision.

## Clauses (draft)

- **C-001** The retained attack3 repro answers Spark deterministically (pinned repetitions).
- **C-002** The flake's trigger (ordering assumption, race, or seed) is named in the card.
- **C-003** Neighbouring coalesce/shuffle cells still answer Spark.

## Pointers

- Up: [map.md](map.md) · Evidence (evidence location):
  `/tmp/oc-worker/direct/wo/reverify7-nvl/`
