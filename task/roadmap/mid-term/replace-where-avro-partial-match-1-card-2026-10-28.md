# Card REPLACE-WHERE-AVRO-PARTIAL-MATCH-1: INSERT ... REPLACE WHERE on Avro data reports a false partial match

**Date:** 2026-10-28. **Filed by:** the ICE-DELETE-FORMAT-GUARD-1 clerk round (`claude-haiku-5-5`), from the verification round 3 records. Filed on the ICE-DELETE-FORMAT-GUARD-1 branch; not in that unit's scope.

**Status:** open. Priority P3. Not dispatched. The measurement is copied from the round-3 verification records and was not re-run by the clerk. Pre-existing on main: the same result on `main`.

**Why:** an `INSERT INTO ... REPLACE WHERE id >= 0 SELECT ...` on an Avro table fails with "Cannot delete file where some, but not all, rows match", although every row matches the predicate. The same statement on a Parquet table succeeds. The message is false, because no row is left out of the match. The failure also leaves an orphan data file, which REPLACE-WHERE-ORPHAN-1 covers. The statement goes through the replace-where path in `crates/repark-spark/src/router/insert_positional/replace_where.rs`; where the Avro check raises the message is not located yet.

**Measured:** by the Opus 5.5 round-3 verifier's throwaway probe on the `delete_format_guard` harness (the verifier's report, second question). The probe's result is copied from the round-3 records; the clerk did not re-run it.

**Fix (proposed):** locate the Avro delete that raises the partial-match message, and make the all-rows-match case delete whole files as the Parquet path does. Red-first pin: `INSERT ... REPLACE WHERE id >= 0` on an Avro table with every row matching replaces the rows, and the Parquet control of the same statement is unchanged.

**Retires:** the card when the statement on Avro has a red run on the current head and a green run that replaces the rows, with the Parquet control unchanged.

**Relation:** [ICE-DELETE-FORMAT-GUARD-1 ledger](../../ledgers/staging/ice-delete-format-guard-1-ledger.md) (the round-3 verification records that carded it); [REPLACE-WHERE-ORPHAN-1 card](replace-where-orphan-1-card-2026-10-28.md) (the orphan file the failure leaves); [registry row ICE-DELETE-FORMAT-GUARD-1](../../../docs/spark-sql-iceberg-parity.md).
