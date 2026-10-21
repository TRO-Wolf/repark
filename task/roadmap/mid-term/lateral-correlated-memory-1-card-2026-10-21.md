# Card LATERAL-CORRELATED-MEMORY-1: a correlated `LATERAL` subquery with its own `FROM` grows past 100 GB before any result

**Date:** 2026-10-21. **Filed by:** Claude (claude-opus-5-5), fix round 3 of PR #1124
(ICE-NTZ-NESTED-REFUSE-1, U-8), from the verifier's round-2 note S3-2. **Status:** open, not
scheduled; the owner schedules.

**The statement.** Source `ice.ns.src (id INT, x TIMESTAMP, w TIMESTAMP_NTZ)` holding one row,
target `ice.ns.t (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT)`, both Iceberg in a memory
catalog, session `UTC`: `INSERT INTO ice.ns.t SELECT a.id, l.s, 0 FROM (SELECT id,
CAST(named_struct('v', x, 'n', 1) AS STRUCT<v: TIMESTAMP_NTZ, n: INT>) AS c FROM ice.ns.src) a,
LATERAL (SELECT a.c AS s FROM ice.ns.src b WHERE b.id = a.id) l`. A lateral whose body has a
`FROM` and a predicate on the outer column. The verifier ran it on main `c516c54a` and the
process reached 112 GB resident before it was killed, with no result and no error. Fix round 3
re-ran it on main `c516c54a` under `systemd-run --user --scope -p MemoryMax=40G -p
MemorySwapMax=0` (`taskset -c 0-25`, load 10.17 at start): the scope was stopped by the OOM
killer at the 40 G cap after 166 s, with no result.

**Not U-8's.** The growth predates this unit: main `c516c54a` carries none of U-8's code, and in
a UTC session U-8's gate does not run. Outside UTC, U-8's type read only plans the source and
never runs it (ledger C-018), so the growth is in executing the lateral join, not in the gate.
Any probe of this shape runs under a memory cap until this card is closed. Evidence: the
verifier's round-2 hand-back (S3-2) and the fix round's repro, both in the session scratch of
PR #1124; ledger `task/ledgers/staging/ice-ntz-nested-refuse-1-ledger.md` §13.5.
