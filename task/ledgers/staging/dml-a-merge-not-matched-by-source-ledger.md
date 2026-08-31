# Charter ledger — DML-A · MERGE … WHEN NOT MATCHED BY SOURCE

**Date:** 2026-08-30 · **Branch:** `feat/dml-a-merge-not-matched-by-source` · **Base:**
`60225cc427673cbc2e4bf23e90db376e602773dd` · **Path:** HIGH · **risk_tier:** high
(source-empty MERGE can rewrite the whole table).

**Retires:** moved to `completed/` in this unit's last commit.

**Why now.** v0.6 Track-B DML remainder, merge order 3 of 4. The engine already owns
MATCHED and NOT MATCHED (BY TARGET). The third arm is a refuse (`NotImplemented`) on both
SQL doors and a split disclosure in the MERGE differential corpus. Spark 4.1.2 + Iceberg
1.11.0 runs the arm. This unit lands DELETE and UPDATE on COW and MOR, reusing the existing
cardinality and store-assignment gates (import, never duplicate). DML-B (#273) and DML-C
(#274) stay parked.

## PROPOSITION LEDGER — DML-A — 2026-08-30

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | **Measure first.** Live PySpark 4.1.2 + Iceberg 1.11.0 records, before any engine edit, every claimed cell: values, Arrow types, v2 snapshot summary / per-action counts, cardinality error class, arm-ordering, source-empty wipe, MOR delete-file shape vs the existing MATCHED arms, and incidental controls. | Oracle transcript in this ledger; each cell names the command and the observed result. | OPEN | No engine edit before the matrix is filed. |
| C-002 | **DELETE arm is Spark-equal.** `WHEN NOT MATCHED BY SOURCE THEN DELETE` (plain and `AND <cond>`) on COW and MOR is three-door Spark-equal on values and Arrow types (Spark SQL door, ANSI door, facade `.sql()` / `whenNotMatchedBySource().delete()`). | Red-first pins per door × mode; live-oracle goldens. | OPEN | Facade builder already renders the SQL; the engine refuse pin must flip. |
| C-003 | **UPDATE arm is Spark-equal.** `WHEN NOT MATCHED BY SOURCE THEN UPDATE SET …` (explicit assignments; `UPDATE SET *` only if C-001 shows Spark accepts it) on COW and MOR is three-door Spark-equal on values and Arrow types. Store-assignment runs through the existing MERGE gate (`store_assign.rs` / `update_stream_checked`) — never a second matrix. | Red-first pins per door × mode; store-assignment reuse pin. | OPEN | Measure whether Spark allows `UPDATE SET *` / source-column refs on this arm. |
| C-004 | **Cardinality class is unchanged.** A target row matching multiple source rows still raises the existing `MERGE_CARDINALITY_VIOLATION` class (byte-identical needle). The skip remains Spark's lone unconditional MATCHED DELETE. The new arm does not grow a second cardinality checker. | Existing cardinality pins stay green; one NMBS+MATCHED duplicate-source pin. | OPEN | Import `skip_cardinality` / the existing message; do not copy them. |
| C-005 | **Arm interaction and order match Spark.** MATCHED + NOT MATCHED + NOT MATCHED BY SOURCE in one statement; first-match-wins inside the new kind; unconditioned NMBS clause must be last of its kind (`NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION`). Spark's kind-independence is the oracle. | Three-arm pin; non-last-unconditional pin; order pin from C-001. | OPEN | Spark allows all three kinds together. |
| C-006 | **Source-empty MERGE is a loud, deliberate wipe pin.** Empty source + unconditional NMBS DELETE leaves zero live rows (every target row is not-matched-by-source). Pinned against the live oracle on COW and MOR, values and Arrow types. Residual join-key pruning must not drop those rows. | Wipe pins COW+MOR; scan-prune control. | OPEN | This is the high-tier blast radius. |
| C-007 | **MOR delete mechanism matches the other MERGE arms; v3 stay-refused.** On the current fork rev, NMBS DELETE/UPDATE on MOR writes the same delete artefact the MATCHED arms write (v2 position deletes, not a new path). v3 MERGE stays the measured `V3-COW-1` keep-refusal — this unit does not lift lineage reassignment. | MOR `$files` / delete-file pin vs MATCHED DELETE; v3 refuse pin. | OPEN | Measure, do not assume DVs. Do not pick a `write.delete.granularity` default — reuse `parse_delete_granularity`. |
| C-008 | **Documents match the pins.** Registry DML-3 boundary moves (NMBS is no longer a refuse-gap); STATUS (stay under 25000 B); facade `merge.py` / differential corpus disclosure flips to content; maps lockstep. | `check-map-sync`, `check-ledger-grammar`, registry + STATUS + maps. | OPEN | DML-B/C surfaces untouched. |

VERDICT: OPEN — 8 clauses, 0 PROVEN, 0 REJECTED. The gate passes when every row is PROVEN with
`pins: dml-a-merge-not-matched-by-source/C-NNN`.

## 1. Out of scope

- DML-B `INSERT OVERWRITE … PARTITION` and DML-C `TRUNCATE TABLE`.
- Lifting `V3-COW-1` / format-v3 MERGE (V3-3 keep-refusal stands).
- Choosing a `write.delete.granularity` default (MW-9 hand-back). Reuse the existing parser.
- A new MERGE result DataFrame schema, unless C-001 shows current MATCHED MERGE already
  returns per-action columns; otherwise pin snapshot-summary counts on v2.

## 2. Sequence

1. This charter (commit 1). No engine edit.
2. C-001 live-oracle matrix filed into this ledger.
3. Red-first pins (the refuse tests go red on purpose, then the arm lands).
4. Executor + both-door lowering + facade disclosure flip.
5. C-008 docs, `make verify`, `make check-map-sync check-ledger-grammar`,
   `python3 scripts/ledger_lifecycle.py check --base 60225cc427673cbc2e4bf23e90db376e602773dd`,
   full `make py-test`.

## 3. Owner actions

- Pre-authorized 2026-08-30 (v0.6 plan). No AWS, no IAM, no fork pin change.
