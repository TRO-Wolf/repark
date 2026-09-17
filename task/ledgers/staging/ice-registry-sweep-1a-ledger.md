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
`/tmp/oc-worker/ice-rating/worker-findings.md`). Before-lines below are the
pre-edit working tree (identical to `79e328f2`); after-lines are `git diff`.

**Not in this unit:** product code; `STATUS.md`; rows ID-1, V3-6, V3-COV-2,
V3-COV-5, REF-*, NaN / Hadoop registry rows (other units own them); any
disposition change except C-3 and C-9 FIXED → OPEN; push; PRs; `gh`.

## PROPOSITION LEDGER — ICE-REGISTRY-SWEEP-1A — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Cutover R1 rewritten to what the rating measured (NaN equality silent, no `count(*)` fold, pins self-skip, C-9), row grammar and column count kept, tagged `(corrected 2026-09-17 from the 2026-09-16 rating)`. | Before/after line numbers of row R1. | **PROVEN** | Row R1 at `docs/cutover/production-iceberg-status-2026-09-14.md:180` (was :173). One-row diff; 5 columns kept; tag present. |
| C-002 | Cutover R4 rewritten (15 metadata tables Spark-equal), same tagging and grammar. | Before/after line numbers of row R4. | **PROVEN** | Row R4 at `docs/cutover/production-iceberg-status-2026-09-14.md:183` (was :176). One-row diff; 5 columns kept; tag present. |
| C-003 | Cutover R5 rewritten (DECLARED against the dated owner ruling), same tagging and grammar. | Before/after line numbers of row R5. | **PROVEN** | Row R5 at `docs/cutover/production-iceberg-status-2026-09-14.md:184` (was :177). Status NOT ESTABLISHED → DECLARED, ruling dated 2026-08-23; 5 columns kept; tag present. |
| C-004 | Cutover W3 rewritten (`partitionOverwriteMode=dynamic` ignored on PARTITION-less overwrite, V2-24b), same tagging and grammar. | Before/after line numbers of row W3. | **PROVEN** | Row W3 at `docs/cutover/production-iceberg-status-2026-09-14.md:192` (was :185). One-row diff; 5 columns kept; tag present. |
| C-005 | Cutover W6 rewritten (plain INSERT INTO ignores the declared sort order, C-7), same tagging and grammar. | Before/after line numbers of row W6. | **PROVEN** | Row W6 at `docs/cutover/production-iceberg-status-2026-09-14.md:195` (was :188). One-row diff; 5 columns kept; tag present. |
| C-006 | Cutover E1 rewritten from PROVEN to what §9 C-6 and §2 measured, same tagging and grammar. | Before/after line numbers of row E1. | **PROVEN** | Row E1 at `docs/cutover/production-iceberg-status-2026-09-14.md:216` (was :209). Bare PROVEN → part-scoped status; 5 columns kept; tag present. |
| C-007 | Cutover M1 rewritten (options map refused on every key, C-4), same tagging and grammar. | Before/after line numbers of row M1. | **PROVEN** | Row M1 at `docs/cutover/production-iceberg-status-2026-09-14.md:222` (was :215). One-row diff; 5 columns kept; tag present. |
| C-008 | Cutover K3 rewritten (typed `CommitStateUnknownException`, never drilled live), same tagging and grammar. | Before/after line numbers of row K3. | **PROVEN** | Row K3 at `docs/cutover/production-iceberg-status-2026-09-14.md:235` (was :228). Status NOT ESTABLISHED → PROVEN-offline; 5 columns kept; tag present. |
| C-009 | Cutover K4 rewritten (Hadoop vN stale-base overwrite, V2-20c), same tagging and grammar. | Before/after line numbers of row K4. | **PROVEN** | Row K4 at `docs/cutover/production-iceberg-status-2026-09-14.md:236` (was :229). Status NOT ESTABLISHED → measured-failing on the adopted-Hadoop path only; 5 columns kept; tag present. |
| C-010 | Cutover I1 and I4 rewritten (§2), same tagging and grammar. | Before/after line numbers of rows I1/I4. | **PROVEN** | Rows I1/I4 at `docs/cutover/production-iceberg-status-2026-09-14.md:250` and `:253` (were :243 and :246). Both NOT ESTABLISHED → PROVEN; 5 columns kept; tag on each. |
| C-011 | Cutover V1 rewritten (write-default not applied, C-1), same tagging and grammar. | Before/after line numbers of row V1. | **PROVEN** | Row V1 at `docs/cutover/production-iceberg-status-2026-09-14.md:215` (was :208). One-row diff; 5 columns kept; tag present. |
| C-012 | Cutover file carries the dated superseding note under the title and the end-of-file Audit trail with the rating §9 table (C-1 … C-11, claim, file:line, measured). | Note text present; audit table rows C-1 … C-11 present. | **PROVEN** | Note at `docs/cutover/production-iceberg-status-2026-09-14.md:3-9`, names the 2026-09-16 measured Iceberg rating run at `a92a68db` and states the 2026-09-14 assessment ran nothing. Audit trail `## 11` at :715 with all eleven C-rows, file:line as cited. |
| C-013 | Registry C-3 (V3-COV-3 FanoutWriter claim) rewritten, dated 2026-09-17, citing the rating and probe `p_rowid_order`; disposition FIXED → OPEN, row id kept. | Before/after line numbers of the V3-COV-3 claim. | **PROVEN** | Heading `docs/spark-sql-iceberg-parity.md:1253` now OPEN (measured 2026-09-16); rationale OPEN at :1270 with the 1-of-12 correction at :1278-1284 (was :1276-1278). Row id kept. |
| C-014 | Registry C-8 (`statistics`/`partition-statistics` sentence) rewritten to hold only for tables without prior statistics, dated 2026-09-17, citing the rating and probes `p_meta_stats`, `p_stats_expire`; Hadoop sentence untouched. | Before/after line numbers of the sentence. | **PROVEN** | Sentence at `docs/spark-sql-iceberg-parity.md:6952-6955` (was :6947). Adjacent Hadoop `vN` sentence byte-identical in `git diff`. |
| C-015 | Registry C-9 (PERF-ICE-COUNTSTAR-1) rewritten (no fold at the current pin, answer correct, fold pins self-skip), dated 2026-09-17, citing the rating and probe `p_countstar`; disposition FIXED → OPEN, row id kept. | Before/after line numbers of the row. | **PROVEN** | Row at `docs/spark-sql-iceberg-parity.md:7205-7212` (was :7197-7211) now OPEN — measured 2026-09-16, RP-14 account kept as history. Row id kept. |
| C-016 | `make spell-check check-docs-links check-map-sync check-ledgers check-ledger-grammar` green with no installs. | Quoted exit-0 output per target. | **PROVEN** | All five exit 0, nothing installed; outputs in §Gates. |

VERDICT: 16 clauses, 16 PROVEN, 0 OPEN, 0 REJECTED.

## Open questions

None. No ambiguity surfaced.

## Gates

`make spell-check check-docs-links check-map-sync check-ledgers check-ledger-grammar`, exit 0, 2026-09-17:

```
uvx typos@1.47.2
python3 scripts/check_docs_links.py
docs-links: 935 files, 5768 links checked — clean
python3 scripts/sync_map_md.py --check
map-sync: 281 maps clean (strict=off)
python3 scripts/ledger_lifecycle.py check
ledger-check: 395 ledgers in bins (218 archived), 1018 ledger links resolve, frozen rule clean
python3 scripts/check_ledger_grammar.py
ledger-grammar: 177 live ledgers clean (1384 clauses, 1981 pinned clause ids, 2 exception rows)
```

`spell-check` passes silent (typos clean, no output past the tool line).

## Evidence

Diff scope: `git diff --stat` shows two files —
`docs/cutover/production-iceberg-status-2026-09-14.md` (12 removed row lines,
one per corrected row, plus the note and §11) and
`docs/spark-sql-iceberg-parity.md` (hunks only at the three claim sites).
Removed-line check confirms exactly the twelve listed rows changed; protected
registry rows ID-1, V3-6, V3-COV-2, V3-COV-5, REF-*, NaN and Hadoop rows are
byte-identical.

```
COVERAGE_ATTESTATION:
  pr_unit: ice-registry-sweep-1a
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked against the rating text (report §2 for the summary, §3/§4/§6 for the cells, §9 for the claims) and the git diff. Every rewritten row carries the measured fact plus the dated tag; the audit trail records the untransferred claims C-2, C-5, C-10, C-11 so none is silently dropped.
      artifacts: [docs/cutover/production-iceberg-status-2026-09-14.md, docs/spark-sql-iceberg-parity.md, task/ledgers/staging/ice-registry-sweep-1a-ledger.md]
    - id: AT-2
      status: N/A
      justification: Docs-only reading unit. No code path, no input, no boundary to exercise.
    - id: AT-3
      status: N/A
      justification: Docs-only reading unit. No runtime behavior, no failure path, no retry.
    - id: AT-4
      status: N/A
      justification: Docs-only reading unit. No shared state, no ordering, no concurrency.
    - id: AT-5
      status: N/A
      justification: Docs-only reading unit. No privileged action, no secret, no AWS call, no JVM.
    - id: AT-6
      status: ATTACKED
      evidence: Row ids and 5-column grammar preserved on every rewritten cutover row; registry row ids kept with dispositions changed only where the brief orders (C-3, C-9 FIXED to OPEN). Compatibility is the diff itself: only the twelve listed rows plus the note and §11 changed in the cutover file.
      artifacts: [docs/cutover/production-iceberg-status-2026-09-14.md, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: Docs-only reading unit. No growth, loop, or leak surface.
    - id: AT-8
      status: ATTACKED
      evidence: No upstream behavior presumed: every number and verdict cites the rating section or probe name, file:line values in the audit trail are cited at a92a68db as the rating states, and protected rows are byte-identical per the removed-line check.
      artifacts: [docs/cutover/production-iceberg-status-2026-09-14.md, docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: N/A
      justification: Docs-only reading unit. Provenance travels in the file: dated correction tags on every row plus the §11 audit trail.
    - id: AT-10
      status: ATTACKED
      evidence: Every clause pins before/after line numbers checkable by git diff; the five gates (spell, docs-links, map-sync, ledgers, ledger-grammar) exit 0 with outputs quoted in §Gates.
      artifacts: [task/ledgers/staging/ice-registry-sweep-1a-ledger.md]
  complete: true
```
