# Card UPDATE-WHERE-BINARY-FLAKE-1: `UPDATE … WHERE` fails about one time in four after a `BINARY` literal insert

**Date:** 2026-10-20. **Filed by:** Claude (claude-fable-5-1), orchestrator, from the U-8 verifier's
measurement on main `c516c54a` and PR #1124's head.

**Status:** open. Not scheduled. An engine defect on main, outside U-8 ICE-NTZ-NESTED-REFUSE-1,
which found it while pinning a mixed-column table and dropped the `BINARY` column from its pin.

## What fails

On a fresh Hadoop catalog in a UTC session, this sequence fails on the last statement with
`Error during planning: Mismatch between schema and batches`, 14 times in 60 on main and 13 in 60
on PR #1124's head (box load 15 to 22 while measured; the rate at a quiet box is not recorded):

1. `CREATE TABLE ice.ns.t (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT, z BINARY)`
2. one seed row through `INSERT … VALUES`
3. `INSERT INTO ice.ns.t (id, k) VALUES (10, 0)`
4. `INSERT INTO ice.ns.t (id, k) SELECT …`
5. `INSERT INTO ice.ns.t VALUES (12, NULL, 0, x'00')`
6. `INSERT INTO ice.ns.t BY NAME SELECT 0 AS k, id + 13 AS id …`
7. `UPDATE ice.ns.t SET k = k + 1 WHERE id >= 10`

A minimal sequence (one seed, the `x'00'` insert, the `UPDATE`) passes 20 of 20 on both builds, so
the trigger needs the mixed set of insert doors above, each writing its own file. The verifier's
repro script is in its scratch hand-back, not in the tree; the sequence above reproduces it.

## Lead, by reading

The same text was DECIMAL-CACHE-1's symptom in September (`task/ledgers/completed/decimal-cache-1-ledger.md`):
`MemTable::try_new` refuses when a batch's schema differs from the declared one, there on
`.cache()`. Here the `UPDATE`'s copy-on-write scan collects batches from files written by four
doors; a `BINARY` column whose Arrow field differs between those files (nullability, field
metadata, `Binary` against `LargeBinary` or a view type) would explain a mismatch that depends on
which file lands first, which is what a one-in-four rate on a fixed sequence looks like. Not run.

## The ask

One unit: reproduce at a quiet box, read the batch schemas at the failing `MemTable`, pin the
sequence as a ledger clause, and make every insert door write the same Arrow field for a
`BINARY` column (or normalise at the scan). The fix is not on the Iceberg v2 label path of the
2026-10-20 plan; it goes after U-11 unless the owner moves it.

## Out of scope

U-8's refusal (PR #1124) and the nested NTZ rows; the DECIMAL-CACHE-1 cache path, already fixed.
