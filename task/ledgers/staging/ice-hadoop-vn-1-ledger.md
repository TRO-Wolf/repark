# Charter ledger — ICE-HADOOP-VN-1 · stale Hadoop `vN` commit raises loud, loses nothing

**Date:** 2026-09-17 · **Branch:** `fix/ice-hadoop-vn-1` · **Base:** `a003f9f5` ·
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `ICE-HADOOP-VN-1` near `V3-ADOPT-1` in
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

**Why now.** Rating row V2-20c (probes `conc` / `conc2`): two RePark memory catalogs
on one Spark Hadoop table at v2 — catalog 1 INSERT wrote v3, catalog 2 (stale v2)
INSERT overwrote v3 and catalog 1's row was lost silently; `conc2`: Spark committed
v3, a stale RePark INSERT overwrote it and Spark's row was lost for good. The fork
now exclusive-creates `vN` names and returns retryable `CatalogCommitConflicts`
(fork PR #286 `75da2b58`, in this tree via RP-21 `a003f9f5`; fork ledger
`task/f-ice-hadoop-vn-1-ledger.md` in the read-only clone at `/tmp/ib-forkread`).
This unit is the RePark side: surface the conflict loud after the retry budget,
lose nothing, pin it, row it.

**Not in this unit:** any fork edit (table-format semantics live in the fork; the
pin moves only in its own PR); `STATUS.md`; `.github/`; dependency files.

**Oracle.** Live PySpark 4.1.2 cells recorded by the unit recorder into
`python/repark/tests/ice_hadoop_vn_1_spark_oracle.json`: Spark's rows after each
shape, Spark's PySpark-visible class and message prefix for its own stale commit.
No hand-computed Spark expectation.

**Rulings.**

- R-1 (brief, step 4 parenthetical): if the surfaced conflict is untyped base
  `PySparkException`, it takes the class RePark already uses for commit conflicts;
  no new public class is invented; if none exists the unit HALTs with the question.
  Standing evidence: the pinned contract keeps `CatalogCommitConflicts` in the
  `Error::Iceberg` base bucket (`crates/repark-core/src/session/tests/session.rs`
  CQ-015, `crates/repark-core/src/session/tests/map.md`, `crates/repark-common`,
  `crates/repark-python/src/tests.rs`). No typed commit-conflict class exists
  anywhere in the tree. The ledger records the measured surface against V2-20a in
  C-004 and the verdict on R-1 lands there.

## PROPOSITION LEDGER — ICE-HADOOP-VN-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `conc`: two RePark catalogs over a copied Spark fixture table; catalog 1 INSERT lands v3; catalog 2 (stale v2) INSERT, MERGE, DELETE, UPDATE each raise the conflict; both catalogs and Spark read catalog 1's rows; v3 bytes unchanged. | `python/repark/tests/test_ice_hadoop_vn_1.py` offline pins; Red-first: the rating's recorded silent outcomes plus a Rust test in `repark-iceberg` or `repark-core` committing twice from stale memory catalogs over a tempdir Hadoop table asserting the conflict. | OPEN |  |
| C-002 | `conc2` (live): Spark commits first, stale RePark INSERT raises, Spark reads its own row and can keep committing. | Live pins in the same file (`REPARK_PARITY_LIVE=1`). | OPEN |  |
| C-003 | Recovery: re-registering the stale catalog at the newest metadata then committing succeeds and Spark reads all rows. | Offline + live pins in the same file. | OPEN |  |
| C-004 | The RePark exception class and message stand against Spark's mirrored stale-commit error (PySpark-visible class and message prefix) and against the existing OCC conflict contract (V2-20a / `occ_conflict`); R-1 verdict recorded. | Oracle JSON + pins asserting class and prefix + this ledger's R-1 verdict. | OPEN |  |
| C-005 | The DataFrame door writers (`writeTo().append()`, `saveAsTable(append)`) as the stale writer raise the same conflict. | Pins in the same file. | OPEN |  |
| C-006 | Repro: `/tmp/ib-scratch/probes/p_failures.py` conc / conc2 blocks copied into `p_hadoop_vn.py`, run through `/tmp/oc-worker/jb-jvm.sh`, output pasted. | Evidence paste below. | OPEN |  |
| C-007 | Spark oracle recorded as a checked-in fixture (recorder + truth JSON per the `test_ice_spark_table_1.py` / `_oracle_pins.py` convention). | Recorder script + `ice_hadoop_vn_1_spark_oracle.json`. | OPEN |  |
| C-008 | Registry row ICE-HADOOP-VN-1 near V3-ADOPT-1 (FIXED 2026-09-17 by fork #286 at pin sha, typed error, recovery recipe, D-2's loud wedge on an orphan version file); residue sentence at ~6897 replaced with a pointer; tests map and ledgers map in lockstep. | The registry diff. | OPEN |  |
| C-009 | No regression: the new file offline and live, `make verify`, the whole facade suite, the whole parity suite, green with real exit codes and counts. | §Gates. | OPEN |  |

## Red-first log

Pasted per clause as it lands.

## Evidence

### C-006 repro output

Pending.

## §Gates

Pending.

## Open questions

1. R-1 verdict (see C-004): no typed commit-conflict class exists in the tree; the
   pinned contract is base `PySparkException`, message-typed with
   `CatalogCommitConflicts` leading. If the measured stale-writer surface matches
   that contract exactly, the unit records conformance and concludes without
   inventing a class; if the orchestrator wants a new typed class instead, that is
   a new unit (it must move the pinned classification tests).
