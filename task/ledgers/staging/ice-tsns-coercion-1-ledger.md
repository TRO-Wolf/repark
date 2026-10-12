# Unit ledger — ICE-TSNS-COERCION-1 · slice 0 measures the twin oracle and the Spark lattice

**Scope (2026-10-12):** measurement only, on main `572130dda35f739795578134348f99fd7b530cfd`.
No product file changes. The twin is the same statement with `__repark_cast_timestamp_ns__`
or `__repark_cast_timestamptz_ns__` written by hand. Spark 4.1.2 is the second oracle:
`timestamp_ns` stands where `TIMESTAMP_NTZ` stands, and `timestamptz_ns` where `TIMESTAMP` stands.

**Date:** 2026-10-12 · **Branch:** `test/ice-tsns-coercion-1-slice-0` · **Base:** `572130dd` ·
**Model:** Grok 4.7 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** the owner's ruling of 2026-10-10 on parity row R-017, card
[ice-tsns-coercion-1-card-2026-10-10.md](../../roadmap/mid-term/ice-tsns-coercion-1-card-2026-10-10.md).
**This ledger closes when the unit's pull request merges.** Slice 0 does not merge that change.

## PROPOSITION LEDGER — ICE-TSNS-COERCION-1 — 2026-10-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The twin of every card spelling is recorded from main 572130dd on every door, five zones. | The twin fixture and its recorder. | **OPEN** | Recorder `_record_ice_tsns_coercion_1_twins.py` committed; fixture pending. |
| C-002 | Spark 4.1.2's lattice for the temporal set is recorded in both ANSI modes. | The lattice JSON and its recorder. | **OPEN** | Recorder `_record_ice_tsns_coercion_1_spark_lattice.py` committed; lattice JSON pending. |
| C-003 | Every twin's kind and zone placement matches Spark's analogue. | The comparison table. | **OPEN** | PENDING_SUMMARY |
| C-004 | The gate-open rate, the cost baseline and the four censuses are recorded. | Section 9 of the slice brief. | **OPEN** | Recorded in later commits of this slice. |
| C-005 | Before the first type_coercion, every card site that holds a nanosecond value is wrapped in the existing cast kernels. | Slice 1. | **OPEN** | Not this slice. |
| C-006 | `nvl` and `ifnull` over the temporal set become `coalesce`, then those kernels apply. | Slice 3. | **OPEN** | Not this slice. SQL `nvl` on main still has no timestamp signature. |
| C-007 | A written `CAST`, `::timestamp`, `to_timestamp` or `TIMESTAMP` literal is unchanged by the card. | Slice 2. | **OPEN** | Not this slice. |

## 1. What step A records (C-001, C-002)

Sources are `timestamp_ns` and `timestamptz_ns`, each holding `2026-01-02 03:04:05.123456789`
and `1969-12-31 23:59:59.999999999`. Targets are `timestamp_ns`, `timestamptz_ns`, and a
microsecond `TIMESTAMP` control. Spellings are `coalesce`, `array(...)[0]`, `CASE`, `if`,
`nvl`, `ifnull`, and `UNION ALL`. Siblings are an untyped NULL, `CAST(NULL AS TIMESTAMP)`,
`CAST(NULL AS TIMESTAMP_NTZ)`, a microsecond column and literal of each kind, and `DATE`.
Doors are the 64 R-017 doors, including MERGE, UPDATE, table-creating doors and the cached
frame. Zones are UTC, America/New_York, Asia/Kolkata, Australia/Lord_Howe and `-03:30`.

Stored cells are read from the Parquet files as int64 ticks. Reads, including field metadata,
are `toArrow()`. The `UNION ALL` write doors use `max(v) GROUP BY id` because a
`ScalarSubquery` does not plan. `INSERT VALUES` of a column sibling is skipped: `VALUES`
has no column scope. `nvl` and `ifnull` twins are `coalesce` with the kernels; the statement
role keeps SQL `nvl` and `ifnull`.

The Spark lattice is five zones, both ANSI modes, the same spellings, and the three range
siblings. The summary `twins-summary.md` lands in step A, written by
`_summarize_ice_tsns_coercion_1.py`.

## 2. Kind and placement (C-003)

PENDING_SUMMARY

## 3. Product clauses left open (C-005, C-006, C-007)

Nothing in `crates/*/src/` or `python/repark/src/` changes in this slice. The gate, the
lattice rewrite, and the temporal `nvl` rewrite are later slices.

## Q. Questions

No ruling is requested by this ledger until the summary names a kind mismatch or an
untyped-NULL stored-value mismatch on a nanosecond target.
