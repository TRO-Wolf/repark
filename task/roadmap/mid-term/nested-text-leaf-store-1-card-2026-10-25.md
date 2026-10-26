# Card NESTED-TEXT-LEAF-STORE-1: a text leaf into a nested TIMESTAMP stores the UTC reading where Spark refuses

**Date:** 2026-10-25. **Filed by:** the WRITE-SPINE-1 step (2) clerk round (`claude-haiku-5-5`), from the builder's finding F-1 (lane A, `claude-opus-5-5`, head `d45eb72d`).

**Status:** open. Grade C (clerk): the measurement is recorded here; no build order is written.

**Why:** a text leaf (a `STRING` value) written into a nested `TIMESTAMP` or `TIMESTAMP_NTZ` leaf stores the UTC reading of the text on main, where Spark refuses the write with `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`. The conform stage of WRITE-SPINE-1 step (2) excludes text leaves from the zone seam, so these cells do not move in step (2).

**Measured** on head `d45eb72d` (the census recount): 102 cells, D3 12, D4 42 and D10 48, every one a text leaf in a nested instant; main stores `[[1767323045123456]]`-shaped values and Spark refuses each cell. The cell ids and Spark's refusal are in the [step (2) evidence file](../../wo/write-spine-1/evidence/step-2-census-2026-10-25.md), the F-1 table.

**Retires:** when each of the 102 cells is REFUSED as Spark refuses it, or EQUAL to Spark's value, and the census recount moves no other cell.

**Relation:** WRITE-SPINE-1 step (2), finding F-1 in the [step (2) ledger section](../../ledgers/staging/write-spine-1-ledger.md). The card needs an owner ruling before a build order.
