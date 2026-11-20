# Card ICE-HELD-PATH-READ-1: a read by table location is a static provider, so a held frame does not see a same-session write, and 4 of its 27 cells store old rows

**Date:** 2026-11-20. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance Q4 (b) of 2026-11-20 on the ICE-HELD-FRAME-SNAPSHOT-1 step 0 (the owner's delegation, about 00:22 UTC). **Sources:** the [sketch §7, Q4](../../../wo/ice-held-frame-snapshot-1/sketch.md), which sets the 27 cells and the separate S2 card; the [domain table §7](../../../wo/ice-held-frame-snapshot-1/domain-table.md), the path read row; and Fable's reply titled `Fable reply — ICE-HELD-FRAME-SNAPSHOT-1 step 0 2026-11-20`, §4, kept outside the repository. The name is the records clerk's, since Fable's reply names no card.

**Status:** filed 2026-11-20; not dispatched.

**Severity: S2,** as the sketch's Q4 (b) sets it and Fable adopts. Silent.

**Label:** not on the label-blocking list. Fable's condition: the label's kept-divergences file must name this path read before the label is declared, and Fable found no such line there today.

## Finding

The path read, `read.format("iceberg").load(<table location>)`, is a static provider in RePark: a same-session write does not reach a frame held on it. Spark refreshes this read like a catalog table (domain table §7; inventory F4). Measured per build and mode: 27 cells WRONG, 4 of them storing old rows. Fable: "the 27 cells are silent and 4 of them store old rows."

## Why it is its own card

Unit 1a could re-bind this read too, by re-running Hadoop discovery at each re-bind (sketch Q4 (a)). The label covers catalog tables on Glue and S3 Tables, and re-running discovery at every re-bind is a design question of its own, per Fable §4. Fable adopts the separate card (Q4 (b)).

## First step

Its own step 0 comes first, and it decides the re-bind question in sketch Q4 (a). No seat is set here.

Up: [map.md](map.md).
