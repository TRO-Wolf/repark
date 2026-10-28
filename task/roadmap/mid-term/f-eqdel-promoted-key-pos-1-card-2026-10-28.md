# F-EQDEL-PROMOTED-KEY-POS-1: a key promoted from int to long after an equality delete fails under a _pos or _row_id projection (fork card)

**Filed:** 2026-10-28. **Filed by:** the RP-61 clerk round (`claude-haiku-5-5`), from fork PR `#377`'s round-2 verifier (fresh Opus 5.5, PASS). Pre-existing on the fork; outside F-EQDEL-DROPPED-KEY-1.

**Status:** filed, not dispatched. Priority P3. This is the owner's fork work; no RePark lane implements it.

**Target:** the owned `iceberg-rust` fork, at the pin `56aa8913f3d1517c82e85394911b3cfc0c0d74ff` (RePark's current pin).

**Consumer:** RePark's merge-on-read DELETE and UPDATE on a v2 table whose equality-delete key was promoted, once the fork is pinned past the fix.

## The fault

A key column promoted from `int` to `long` after an equality delete was committed fails loud under any `_pos` or `_row_id` projection. The same projection fails the fork's DataFusion merge-on-read DELETE and UPDATE on such tables, and the table is unchanged. No silent wrong value was observed.

## The fix (proposed)

The key-set fast path compares the keys after the promotion, or falls back to the predicate path when the key type changed.

## Pins (proposed)

- Red first on the promotion sequence under a `_pos` projection and under a `_row_id` projection.
- A merge-on-read DELETE and UPDATE on the same table complete, with the survivors exact.

## Out of scope

The dropped-key read (F-EQDEL-DROPPED-KEY-1, fork PR `#377`). Position-delete formats (F-POSDEL-FORMAT-READ-1).

Up: [map.md](map.md).
