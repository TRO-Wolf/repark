# Unit ledger — ICE-TSNS-UPDATE-RANGE-CLASS-1 (U-4, row R-012) · the unfiltered UPDATE reports an out-of-range literal as INSERT does (closed as already fixed, pinned)

**Date:** 2026-10-26 · **Branch:** `fix/ice-tsns-update-range-class-1` · **Base:** `d114a2cf` (`origin/main`), measured on main `9fd60149`
**Models:** builder `muse-spark-1.3-contributor` (Muse MAX); clerk `claude-haiku-5-5`. **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: 1.5.6.

**Order:** [the order](../../wo/ice-tsns-update-range-class-1/order.md), §7 (outcome). The order's §3 red premise is false on
this head, so no product line changes; the unit's output is the pins and the record.

**Why.** Row R-012 says the unfiltered `UPDATE` of an out-of-range `TIMESTAMP` literal into a `timestamp_ns` column under
ANSI reports `[CAST_INVALID_INPUT]`, where `INSERT` reports `[CAST_OVERFLOW]`. ICE-TSNS-TYPE-PLANNER-1 (`c7121437`,
PR #1082, merged 2026-10-16) re-planned the literal to microseconds, so the planner-cast peel no longer reaches the
literal's string, and the kernel reports the range failure on every path. The unit pins that behaviour and closes R-012.

## PROPOSITION LEDGER — ICE-TSNS-UPDATE-RANGE-CLASS-1 — 2026-10-26

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The 16 R-012 cells (the two far literals in three zones plus each zone's edge instant, both row-level modes) report INSERT's byte-identical `[CAST_OVERFLOW]` text on the unfiltered UPDATE, at the facade door and at the Rust door. | The facade test over the 16 cases and the Rust pin on the head. | **PROVEN** | `test_unfiltered_update_reports_inserts_overflow_class` in `python/repark/tests/timestamps/test_ice_tsns_merge_wall_1.py` (16 cells, `update_error == insert_error`) and `an_out_of_range_literal_reports_inserts_overflow_on_unfiltered_update` in `crates/repark-spark/tests/timestamp_ns_wall_doors.rs`; 16 of 16 green on the builder's run of 2026-10-26. pins: ice-tsns-update-range-class-1/C-001 |
| C-002 | No product change was needed: ICE-TSNS-TYPE-PLANNER-1 fixed the path, and the cells were green on the tree before the pins were added. | The same tests on the unmodified tree, plus the orchestrator's reproduction on main `9fd60149`. | **PROVEN** | The builder measured all 16 cells green on the unmodified tree (facade 546 passed, one pre-existing skip; Rust 16 of 16). The orchestrator reproduced on `9fd60149`: INSERT, unfiltered UPDATE and filtered UPDATE of `TIMESTAMP '3000-01-01 00:00:00.000001+00:00'` into a v3 `timestamp_ns` column under ANSI in America/New_York all raise `[CAST_OVERFLOW] The value 32503680000000001 of the type "TIMESTAMP" …`, and the table keeps its one row. Commits `46184319` and `d0d60540` are test-only. pins: ice-tsns-update-range-class-1/C-002 |
| C-003 | A Muse MAX verifier, given the pins only, finds no pin that stays green under a deliberate change of the literal's typing path. | One verifier round on the pins (mutants on the typing path; the 16 cells, the filtered and strict-off controls, the Rust pin). | **OPEN** | Not run. The order rules one Muse verifier round on the pins only. Its verdict is filed here when the round runs. |

## Controls (pinned, not clauses)

- `test_filtered_update_reports_inserts_overflow_class`: the same 16 inputs through `UPDATE … WHERE`; the class and text stay INSERT's.
- `test_out_of_range_literal_stores_null_without_ansi`: ANSI off, all three paths store NULL.
- `test_range_cases_are_the_sixteen_row_cells`: the parametrization is exactly 16 cases.
- `test_update_with_no_where_stores_a_literal`: the in-range value of the same statement still stores.

## Out of scope, observed (dated 2026-10-26, recorded in R-012, no unit)

- A bare far STRING past the range: the unfiltered UPDATE refuses `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]` at plan time where INSERT reports `[CAST_INVALID_INPUT]`. Pre-existing.
- An in-range 9-digit `TIMESTAMP` literal stores the microsecond-truncated wall on INSERT and on the unfiltered UPDATE alike (R-024's territory).
- The R-008 zoned mirror keeps its own unit; its head behaviour is unmeasured here.

## Clerk record

- `handback-notes.md`, committed at the clone root by mistake in `9e7356a6`, is removed; its facts are in the order's §7.
- Registry row R-012 is FIXED (status 2026-10-26), with the 2026-10-09 measurement kept as history.
- The 1.5.6 release-notes draft carries one line under "Integrity under failure (no stored value or behaviour changes)".
