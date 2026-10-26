# Card PARTITION-PAIRING-CARVE-OUT-CLOSE-1: plan-time pairing on the PARTITION overwrite site stores Spark's rows, and the owner rules whether the carve-out closes

**Date:** 2026-10-25. **Filed by:** the WRITE-SPINE-1 step (2) clerk round 2 (`claude-haiku-5-5`), from the builder's fix round 1 (commit `c3f22afb`) and its carve-out measurement.

**Status:** open. Grade C (clerk): the measurement is recorded here; no build order is written. The owner rules whether a routing unit closes the carve-out.

**Why:** the ice-struct-pairing-1 carve-out (ledger [ice-struct-pairing-1-ledger.md](../../ledgers/staging/ice-struct-pairing-1-ledger.md) C-031, owner ruling 58(a)) keeps `INSERT OVERWRITE ... PARTITION` refusing a struct whose field names are not the table's, in order, with hint H-1. WRITE-SPINE-1 step (2) paired the struct at plan time on the PARTITION site, which stored Spark's rows. PR #1189's CI went red on that path (106 carve-out cells, 6 partition refusal-text cells, 1 arity-order cell, 1 Rust pin). Fix round 1 restored the refusal, because the carve-out ruling stands (WS2-27); the owner decides whether the carve-out closes.

**Measured** on the `d45eb72d` build, before any change: for the 106 carve-out cells, 106 of 106 stored rows equal Spark's oracle rows (`ice_struct_pairing_1_spark_oracle.json`); 0 differ, 0 error. The 106 ids are in the [step (2) evidence file](../../wo/write-spine-1/evidence/step-2-census-2026-10-25.md), section "Fix round 1: the 106 carve-out cells".

**Retires:** the owner's ruling on the carve-out. If it closes, the 106 cells move from refusal to Spark's rows and their pins change with the routing unit; if it stands, this card closes as declined.

**Relation:** [ice-struct-pairing-1 ledger](../../ledgers/staging/ice-struct-pairing-1-ledger.md) C-031 (the carve-out); [WRITE-SPINE-1 step (2), fix round 1](../../ledgers/staging/write-spine-1-ledger.md) and its row C-025.
