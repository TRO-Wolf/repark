# Card REPLACE-WHERE-ORPHAN-1: a failed INSERT ... REPLACE WHERE leaves an orphan data file

**Date:** 2026-10-29. **Filed by:** the ICE-DELETE-FORMAT-GUARD-1 clerk round (`claude-haiku-5-5`), from the verifier's round 2 pre-existing observations (Opus 5.5 verifier at `b9d83f38`). Filed on the ICE-DELETE-FORMAT-GUARD-1 branch; not in that unit's scope.

**Status:** open. Priority P3. Not dispatched. Measured only by the verifier's probe (copied, not re-run by the clerk). Pre-existing on main: the same result on `main`.

**Why:** an `INSERT INTO ... REPLACE WHERE` that fails with "Cannot delete file where some, but not all, rows match" leaves an orphan data file in the warehouse. The verifier's probe recorded the orphan file and no other effect. The replace-where path is `crates/repark-spark/src/router/insert_positional/replace_where.rs`.

**Fix (proposed):** the partial-match check runs before the statement writes its data file, or the statement removes the files it wrote when it refuses. Red-first pin: a failing `REPLACE WHERE` leaves the warehouse file count unchanged, as the refused DELETE and UPDATE pins of ICE-DELETE-FORMAT-GUARD-1 do.

**Retires:** the card when the failing `REPLACE WHERE` has a red run on the current head that shows the orphan and a green run that shows the unchanged file count.

**Relation:** [ICE-DELETE-FORMAT-GUARD-1 ledger](../../ledgers/staging/ice-delete-format-guard-1-ledger.md) (the verifier round that found it; the refusal pins it reuses); [registry row ICE-DELETE-FORMAT-GUARD-1](../../../docs/spark-sql-iceberg-parity.md).
