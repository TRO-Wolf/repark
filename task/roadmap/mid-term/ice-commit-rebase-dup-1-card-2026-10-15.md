# Card ICE-COMMIT-REBASE-DUP-1: the fork's re-base after a CAS rejection can append the same rows twice

**Date:** 2026-10-15. **Filed by:** Muse Spark (muse-spark-1.3-contributor), clerk, from the owner's rulings 84 and 85 of 2026-10-15.

**Status:** open, not scheduled.

**Retires:** when the fork change merges with a pin that runs the exact sequence below, and U-9's live drill shows no double append.

## Why

The owner's ruling 84, quoted:

```
84. Yes, and the card is wider than turning the SDK retry off. The fork
    already re-bases and re-appends after a CAS rejection
    (append.rs:1685). A lost reply, an SDK re-send, a stale rejection, and
    that re-base appends the same rows twice. The drill must run that
    exact sequence. The fork card: after any CAS rejection, refresh and
    check whether the table already holds our snapshot id before
    re-basing; then switch the SDK retry off on the two commit calls. If
    the drill shows a double append, the live leg stops and v2 waits.
```

The sequence, step by step: a commit whose reply is lost; the SDK re-sends it on its own retry; the catalog rejects the re-send as stale (it carries the old base version); the fork maps that rejection to a retryable conflict and re-bases, re-applying the append on the refreshed table, which already holds the first attempt's snapshot. The rows land twice, silently. The sketch predicts this path for cell U-5 in
[its §4.4](../../wo/ice-integrity-pins-1/sketch.md), where the outcome is labelled unmeasured; ruling 84 widens the drill from "turn the SDK retry off" to running that exact four-step sequence.

## The re-base site on this branch

`crates/repark-iceberg/src/write/append.rs:1685` — the owner's cited line matches this branch exactly (no difference). It is the expectation of the PIN P17 test `append_midflight_commit_conflict_retries_and_both_appends_land`: the victim append lands via the fork's CAS-rejection retry, with exactly two `update_table` attempts asserted at `:1687`-`:1691` (the rejected CAS, then the successful retry) and one snapshot per landed append at `:1692`-`:1696`. That test pins today's answer for two *different* writers; the unmeasured case is the same writer's own re-send landing first and the re-base appending its rows again.

## The change the owner named

In the fork: after any CAS rejection, refresh and check whether the table already holds our snapshot id before re-basing; then switch the SDK retry off on the two commit calls (`UpdateTable`, `UpdateTableMetadataLocation`). If the drill shows a double append, the live leg stops and v2 waits.

## Out of scope

- The SDK retry classifier itself (operation-agnostic today, measured in the sketch's M3); only its setting on the two commit calls changes.
- U-1 to U-4, which reset the re-send unforwarded and drill the fork alone.
- Any RePark-side change: the card is fork work with a RePark-side drill pin.
