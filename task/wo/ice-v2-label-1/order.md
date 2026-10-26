# U-13 ICE-V2-LABEL-1 — the format-v2 PRODUCTION label     grade: C (clerk)   engine band: Haiku 5.5 xhigh or Muse xhigh clerk; one Muse MAX verifier   release: 1.5.6

Drafted by the orchestrator (Claude Fable 5.1) on 2026-10-26 from the 2026-10-13 closeout plan, the 2026-10-20 priorities (row 7), the production-grade definition (`docs/production-grade.md`, #1044) and the day's merges. Opens the moment #1191 (U-11) is on main. Date of record: v2 label Tue 2026-11-10, buffer 11-12.

## 0. Why, and what is out of scope

The v2 list is complete on main: boundary and secrecy (PA-01 to PA-04), the silent-value writes (U-8, EXPLAIN-DML-WRITE-1, ZONE-SEAM-1, GATE-RETIRE-1, WRITE-SPINE-1 steps 1 to 3), integrity under failure (U-9: K, U and X proven, live nightly green since 10-24), a second engine reading what we write (the nightly cross-read, 768 rows EQUAL on 10-26), and the stated resource bound (U-11). The two gates outside the list hold: parity-live green every morning since 10-21; aws-acceptance green 10-24, 10-25, 10-26; v1.5.5 tagged and on PyPI. This unit writes the label and nothing else: no product code, no new test, no new measurement beyond the one matrix run below.

Out of scope: v3 (V3-LABEL-1 follows as its own PR, decision D5); WRITE-SPINE-1 step 4; NESTED-LITERAL-SEAM-1; the nanosecond rows (they carry dated rows on the v3 label).

## 1. Rulings already made

- A label moves when every row is green on main with a dated pin or carries a dated owner carve-out; a silent value disqualifies and no carve-out covers one; a refusal with a hint does not (10-13 plan, adopted).
- Carve-out rows the label carries, each signed by the owner: ENC-1 items 1 and 5 (parked under ruling 121(b); item 1's harm on main is an unreferenced orphan file never served; rebuild in 1.5.7) and ENC-CTAS-REPLACE-1 (dated row 2026-10-15 under ruling 98(a)).
- Two green parity mornings before the label PR (10-17 decision D5): met and exceeded.
- The acceptance run id the label cites: the most recent green aws-acceptance run on the day the PR opens.
- The text-leaf leniency (381 census cells where a string into a nested timestamp stores where Spark refuses) is a dated row on the v2 label, fix in 1.5.7 (owner, 2026-10-26).
- STATUS.md sits at 24,991 bytes of its 25,000-byte compaction ceiling: the maturity section lands only with a compaction of older prose of at least its own size; the gate `make check-docs-compaction` decides.

## 2. Steps (each its own commit; comment gate before each; docs only)

0. **The matrix, once, on the label's main**: the 842-cell scoreboard harness (the 2026-09-27 and 2026-10-13 runs, re-run 2026-10-26 on d114a2cf: 705 EQUAL / 132 SPARK-CANNOT / 5 REFUSED-REGISTERED / 0 DIFFERENT), run once more on the main that carries #1191; the orchestrator runs it (it is a shell chain, not model work) and hands the clerk the family tallies and the main sha. A DIFFERENT cell halts the unit and the owner hears first.
1. **Registry re-date** (`docs/spark-sql-iceberg-parity.md`): every row the v2 list names carries its dated status as of the label's main; the three carve-out rows get their dated label line; the text-leaf leniency row is filed (NESTED-TEXT-LEAF-STORE-1's card is the link).
2. **STATUS maturity section**: a per-capability maturity line per the definition's three levels, citing tests: format-v2 reads and writes PRODUCTION (with the five review questions answered by pointer: the parity doc rows, the integrity ledger, the bound record, the cross-read nightly, the carve-outs), format-v3 core HARDENED-NARROW until V3-LABEL-1, nanosecond columns and struct writes EXPERIMENTAL as rated 10-13. Compaction of older STATUS prose by at least the section's size; the 25,000-byte gate green.
3. **The label line and its evidence block** in the order's outcome: the matrix sha and tallies, the parity-live run ids of the two mornings before the PR, the acceptance run id, the merged PR numbers of every v2 unit, and the owner's signature line for the carve-outs (left for the owner to fill on the PR).
4. **Release notes**: the 1.5.6 draft's label line.
5. Maps, ledger `task/ledgers/staging/ice-v2-label-1-ledger.md` (one PROVEN row per list item citing its pins, one row per carve-out citing its ruling), card status lines, `make check-docs-links check-docs-compaction check-ledger-grammar check-ledgers`, `python3 scripts/sync_map_md.py --check`.

## 3. Gates

CI green before the verifier; one Muse MAX verifier reads every cited pin and run id against the tree and GitHub (every `test -e`, every `gh run view`), and attacks the maturity section's claims against the definition's disqualifiers. Merge on the owner's word after the owner signs the carve-out rows on the PR.

## 4. Halt rules

A DIFFERENT cell on the matrix; a cited pin that does not exist; a parity-live or acceptance red on the morning the PR opens; the compaction gate red with no older prose left to compact; any need for a product change.

## 5. Hand-back

One JSON object: `status`, `head`, `commits`, `matrix` (sha, tallies), `runs` (parity-live ids, acceptance id), `rows` (re-dated, carve-outs, new), `gates`, `questions`.
