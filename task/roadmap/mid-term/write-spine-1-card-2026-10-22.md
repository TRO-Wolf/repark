# Card WRITE-SPINE-1: one conform stage every write door passes through, and conversion helpers both plan time and store time can call

**Date:** 2026-10-22. **Filed by:** Claude (claude-fable-5-1), orchestrator, on the owner's word
("I like that recommendation, proceed") after the ZONE-SEAM-1 halt of the same morning (ruling
ZS-9). Main `9d873074`.

**Status:** open, scheduled as the architecture unit after GATE-RETIRE-1 (Opus 5.5, grade B,
sketch to the orchestrator before step 1, hostile verifier). The owner picks its place against
the three write-path cards of this week: ICE-HADOOP-HINT-WINDOW-1, ZONE-HORIZON-INSTANT-1 and
WRITE-NTZ-TO-LTZ-NESTED-1. The orchestrator's lean: the spine first, because it turns the third
card from a three-site fix into a one-site fix.

**Retires:** when (1) every write door reaches the sink through one conform stage that owns the
session-zone rule, the nanosecond floor and struct pairing, so a rule written once applies on
every door by construction; (2) the zone and unit conversion helpers live in a crate that both
`repark-functions` and `repark-iceberg` depend on, so plan time and store time call the same
function; (3) a door-by-shape matrix test enumerates the doors mechanically from the code, so a
door missing from the stage fails a pin instead of waiting for a verifier; and (4) the U-8
over-refusal lists and the ZS-9 refusal shrink to the cells Spark itself refuses, with dated rows.

## Why this card exists

Four closeout units in October were the same unit in different clothes: U-3 "every staged door
floors a nanosecond timestamp", U-5 "one local-file policy for both doors", U-6 "one rule pairs
struct fields on every write door", U-8 "outside UTC every door refuses". Each unified a
behaviour that existed as separate copies across the write doors, and every product fix round
on them was a hostile verifier finding a door or a shape the builder's copy had missed. U-8 took
four rounds. The rule of 2026-10-14 ("one round per mechanism, the unit does not grow") keeps
the cost of that pattern bounded; it does not remove the pattern. This card does.

## What the seam measured (ruling ZS-9, 2026-10-22)

The nested `TIMESTAMP_NTZ` to `TIMESTAMP` store conversion, the mechanism behind 74 U-8 matrix
cells and 29 `ntz_literal` cells, is three sites, not one:

| Site | Where | Doors | What it can see |
|---|---|---|---|
| A | `crates/repark-iceberg/src/write/conform.rs`, `entry_store_cast` | the insert and overwrite doors (also reached from `overwrite.rs`) | the sink's Arrow batch; no session zone in scope |
| B | `crates/repark-iceberg/src/write/update_cast.rs`, `nested_leaf_cast_sql` | the UPDATE and MERGE-update doors | an `arrow_cast` text it builds itself |
| C | `crates/repark-iceberg/src/write/merge/insert.rs`, `insert_stream_checked`, through `store_overflow::wrap_store_outputs` | the MERGE-insert doors | the checked stream's outputs |

`repark-iceberg` depends only on `repark-common` (its `Cargo.toml`, `[dependencies]`), as does
`repark-functions`; the seam's session-zone helper lives in `repark-functions`, so none of the
three sites can call it. The builder found the right rule and had nowhere to put it once. The
seam therefore lands with a plan-time refusal of that shape (ZS-9) and the fix waits.

## The ask

1. **Sketch first.** A map of every write door (the U-8 ledger counts 28, the seam's census 37;
   the sketch settles the number and names each) to the point where its rows become the sink's
   batch, marking which of the three rules (zone, floor, pairing) each door applies today and
   where. The sketch names the one stage that becomes the spine and the doors that bypass it.
2. **The leaf crate.** Move the zone and unit conversion helpers the seam and the three sites
   need into `repark-common` or a new leaf crate under it; `repark-functions` and
   `repark-iceberg` both depend on it. No behaviour change in this step; the pins are the
   existing ones.
3. **The spine.** Route every door through the one conform stage; delete the per-door copies as
   each door moves (relocated code sheds its comments). One slice per rule, one commit each,
   the door matrix run after every slice with zero cells leaving EQUAL.
4. **The mechanical door matrix.** A test that enumerates the doors from the code (the
   statement router and the DataFrame writers), not from a hand-kept list, and runs each shape
   through each door; a new door that skips the stage fails here.
5. **Retire the symptom refusals** that the spine makes redundant, with dated rows, the same
   pattern as GATE-RETIRE-1.

## What this card does not do

It does not fix the DST-edge instant kernel (ZONE-HORIZON-INSTANT-1), the hadoop version-hint
read (ICE-HADOOP-HINT-WINDOW-1) or the read-side container cast (CONTAINER-CAST-ZONE-1, folded
into the seam). It does not touch the Spark-side oracle. A halt in step 1 that finds a door the
spine cannot reach without a public signature change parks the unit for the owner.

## Process rule filed with this card (owner, 2026-10-22)

Every write-path unit's order carries a step 0 **door-by-shape census**, measured against Spark
before the build opens: each door, each shape the unit's rule touches, Spark's answer and
main's, so the verifier's matrix is the builder's input. Every hand-back carries a line
**"doors I did not touch, and why"**. Recorded in [task/lessons.md](../../lessons.md) under
2026-10-22 and in the work-order template; U-11's order is the first to carry it.
