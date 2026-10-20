# Card COMMIT-UNKNOWN-OPERATION-ID-1: thread the fork's operation id to the plain INSERT unknown-outcome error

**Date:** 2026-10-15. **Filed by:** Muse Spark (muse-spark-1.3-contributor), clerk, from the owner's rulings 84 and 85 of 2026-10-15.

**Status:** open, not scheduled.

**Retires:** when the id reaches the error, pinned.

## Why

The owner's ruling 85, quoted:

```
85. Confirmed. The error text still tells the user what to read back
    (the table's snapshot list). The operation id card is dated.
```

A plain `INSERT` whose commit outcome is unknown raises with `operation_id = None`: the fork mints an id but does not pass it up. The sketch names where in
[its Q7](../../wo/ice-integrity-pins-1/sketch.md): the fork mints and stamps the id at `fork:crates/integrations/datafusion/src/physical_plan/commit.rs:372` and RePark's plain-`INSERT` door (the §3.1 door table, first row) reports `None`. The option and `MERGE` doors already carry the id through `commit_err`. A user holding `None` resolves today only by the snapshot-set difference, sound only under the single-writer rule (the sketch's §4.3, second bullet); whether that fallback ever misfires live is unmeasured.

## The change

Thread the fork's minted id to the error on the plain `INSERT` door, so the unknown-outcome error carries the same `operation_id` string the other doors carry. The error text keeps telling the user to read back the table's snapshot list, per the ruling: `SELECT count(*) FROM <t>.snapshots WHERE summary['engine.operation-id'] = 'X'`, as the sketch's §4.3 resolver does.

## The pin

U-2's plain-door cell asserts the id is present (today it asserts `None`); the resolver asks by `summary['engine.operation-id']` instead of the gate's captured snapshot id. The offline twin in `commit_unknown_doors.rs` gains the same assertion. The gate-captured snapshot-id fallback stays as the harness cross-check, not as the user path.

## Out of scope

- The reconciliation itself (`reconcile_unknown_commit_outcome`) and the status-check properties: unchanged.
- The snapshot-list wording of the error text: unchanged by ruling.
- Any change to which kinds are unknown: the fork's classifier owns that.
