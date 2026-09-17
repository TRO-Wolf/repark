# Unit ledger — ICE-REGISTRY-SWEEP-1 part A · cutover corrections + three registry claims

**Date:** 2026-09-17 · **Branch:** `docs/ice-cutover-corrections-1` · **Base:** `79e328f2`
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** READING. Docs-only unit: no Rust, no Python, no JVM, no build.
Numbers and verdicts come only from the 2026-09-16 measured Iceberg rating
(run at `a92a68db`, repark 1.4.2, fork `edc38c6a`); no claim the rating
contradicts is restated.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The 2026-09-14 cutover assessment ran nothing; the 2026-09-16
rating measured `a92a68db` and contradicts twelve of its rows plus three
`docs/spark-sql-iceberg-parity.md` claims (§9 C-3, C-8, C-9). This unit
rewrites those rows in place, dated, with an audit trail.

**Sources.** `/tmp/oc-worker/ice-rating/report.md` §§1–9 (evidence:
`/tmp/oc-worker/ice-rating/worker-findings.md`); the two edited files at
their pre-edit lines (each clause names them).

**Not in this unit:** product code; `STATUS.md`; rows ID-1, V3-6, V3-COV-2,
V3-COV-5, REF-*, NaN / Hadoop registry rows (other units own them); any
disposition change except C-3 and C-9 FIXED → OPEN; push; PRs; `gh`.

## PROPOSITION LEDGER — ICE-REGISTRY-SWEEP-1A — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Cutover R1 rewritten to what the rating measured (NaN equality silent, no `count(*)` fold, pins self-skip, C-9), row grammar and column count kept, tagged `(corrected 2026-09-17 from the 2026-09-16 rating)`. | Before/after line numbers of row R1. | **OPEN** | Pre-edit row at `docs/cutover/production-iceberg-status-2026-09-14.md:173`. |
| C-002 | Cutover R4 rewritten (15 metadata tables Spark-equal), same tagging and grammar. | Before/after line numbers of row R4. | **OPEN** | Pre-edit row at `docs/cutover/production-iceberg-status-2026-09-14.md:176`. |
| C-003 | Cutover R5 rewritten (DECLARED against the dated owner ruling), same tagging and grammar. | Before/after line numbers of row R5. | **OPEN** | Pre-edit row at `docs/cutover/production-iceberg-status-2026-09-14.md:177`. |
| C-004 | Cutover W3 rewritten (`partitionOverwriteMode=dynamic` ignored on PARTITION-less overwrite, V2-24b), same tagging and grammar. | Before/after line numbers of row W3. | **OPEN** | Pre-edit row at `docs/cutover/production-iceberg-status-2026-09-14.md:185`. |
| C-005 | Cutover W6 rewritten (plain INSERT INTO ignores the declared sort order, C-7), same tagging and grammar. | Before/after line numbers of row W6. | **OPEN** | Pre-edit row at `docs/cutover/production-iceberg-status-2026-09-14.md:188`. |
| C-006 | Cutover E1 rewritten from PROVEN to what §9 C-6 and §2 measured, same tagging and grammar. | Before/after line numbers of row E1. | **OPEN** | Pre-edit row at `docs/cutover/production-iceberg-status-2026-09-14.md:209`. |
| C-007 | Cutover M1 rewritten (options map refused on every key, C-4), same tagging and grammar. | Before/after line numbers of row M1. | **OPEN** | Pre-edit row at `docs/cutover/production-iceberg-status-2026-09-14.md:215`. |
| C-008 | Cutover K3 rewritten (typed `CommitStateUnknownException`, never drilled live), same tagging and grammar. | Before/after line numbers of row K3. | **OPEN** | Pre-edit row at `docs/cutover/production-iceberg-status-2026-09-14.md:228`. |
| C-009 | Cutover K4 rewritten (Hadoop vN stale-base overwrite, V2-20c), same tagging and grammar. | Before/after line numbers of row K4. | **OPEN** | Pre-edit row at `docs/cutover/production-iceberg-status-2026-09-14.md:229`. |
| C-010 | Cutover I1 and I4 rewritten (§2), same tagging and grammar. | Before/after line numbers of rows I1/I4. | **OPEN** | Pre-edit rows at `docs/cutover/production-iceberg-status-2026-09-14.md:243` and `:246`. |
| C-011 | Cutover V1 rewritten (write-default not applied, C-1), same tagging and grammar. | Before/after line numbers of row V1. | **OPEN** | Pre-edit row at `docs/cutover/production-iceberg-status-2026-09-14.md:208`. |
| C-012 | Cutover file carries the dated superseding note under the title and the end-of-file Audit trail with the rating §9 table (C-1 … C-11, claim, file:line, measured). | Note text present; audit table rows C-1 … C-11 present. | **OPEN** | Title at `docs/cutover/production-iceberg-status-2026-09-14.md:1`. |
| C-013 | Registry C-3 (V3-COV-3 FanoutWriter claim) rewritten, dated 2026-09-17, citing the rating and probe `p_rowid_order`; disposition FIXED → OPEN, row id kept. | Before/after line numbers of the V3-COV-3 claim. | **OPEN** | Claim at `docs/spark-sql-iceberg-parity.md:1276-1278` (lines drifted since `a92a68db`). |
| C-014 | Registry C-8 (`statistics`/`partition-statistics` sentence) rewritten to hold only for tables without prior statistics, dated 2026-09-17, citing the rating and probes `p_meta_stats`, `p_stats_expire`; Hadoop sentence untouched. | Before/after line numbers of the sentence. | **OPEN** | Sentence at `docs/spark-sql-iceberg-parity.md:6947`. |
| C-015 | Registry C-9 (PERF-ICE-COUNTSTAR-1) rewritten (no fold at the current pin, answer correct, fold pins self-skip), dated 2026-09-17, citing the rating and probe `p_countstar`; disposition FIXED → OPEN, row id kept. | Before/after line numbers of the row. | **OPEN** | Row at `docs/spark-sql-iceberg-parity.md:7197-7211`. |
| C-016 | `make spell-check check-docs-links check-map-sync check-ledgers check-ledger-grammar` green with no installs. | Quoted exit-0 output per target. | **OPEN** | Targets named in the brief step 3. |

VERDICT: 16 clauses, 0 PROVEN, 16 OPEN, 0 REJECTED.

## Open questions

None. Ambiguity would halt here.

## Gates

| Command | Exit | Output |
|---|---|---|
| `make spell-check check-docs-links check-map-sync check-ledgers check-ledger-grammar` | — | Not yet run. |

## Evidence

Step 0: ledger created; edits not yet made.
