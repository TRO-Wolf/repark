# Card CROSS-JOIN-CONDITION-1: a cross join ignores its condition

**Date:** 2026-10-08. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's brief. **Source:** the STAMP-2-R5P6-2 lane's hand-back (PR #997, not on main yet), its first out-of-scope item.

**Status:** closed 2026-10-08 by its unit. Sequenced after PR #997 merges, because both touch the same door.

## Why

**The repro.** `df.join(other, condition, "cross")` ignores the condition. The hand-back's counts: 12 rows on RePark, where live Spark 4.1 answers 2 rows. The input shape is not described in the hand-back: Not measured.

**Where.** It is on main and on the PR #997 branch alike.

**Cause.** The H1 door emits `CROSS JOIN` with no `ON`.

**The native route.** PR #997's native route reproduces the SQL route. The differential pin holds only their equality, not the row count, so it stays green on the wrong answer.

## The ask

The cross door applies the condition as Spark does, on both routes. Scheduled after #997 merges, because both touch the same door.

## Gates

- Spark's row count on the measured shape (2 rows, as the hand-back measured it);
- the differential pins between the two routes stay green;
- an Opus verifier on the product PR.

## Pointers

- The STAMP-2-R5P6-2 lane's hand-back (PR #997, not on main yet), its first out-of-scope item.
