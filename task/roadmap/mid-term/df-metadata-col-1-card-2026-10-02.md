# DF-METADATA-COL-1 — DataFrame.metadataColumn and the hidden _metadata struct on file scans

**Filed: 2026-10-02, re-cut from the closed draft PR #662.** The PR body
and close comment were not re-read in this lane (`gh` is banned for
worker lanes); the card is re-cut from
[overnight-report-2026-09-16-18b.md](overnight-report-2026-09-16-18b.md)
§7 with rulings R-18b-16..19 from
`/tmp/oc-worker/sb-meta/followup-4.md` (verified present 2026-10-02). The
work is two weeks stale (2026-09-16): it re-opens with a rebase, not a
resume. Kept branch: `feat/df-metadata-col-1` at
`e07acb9c3330684f7dbaf2319b1fd43fa505fe52` (remote head read 2026-10-02).
The round-4 working tree (P1 L-501 and the three P2s in progress) is saved
as a local round-4 WIP patch outside the repository
(verified present 2026-10-02). **Status:** draft, parked with review
findings open (logic NEEDS_REMEDIATION: 1 P1 L-501 + 3 P2 + 1 P3; Rust
perf NEEDS_REMEDIATION: 2 P2; Python perf PASS). **Target:** mid-term.

## The gap it records

`DataFrame.metadataColumn` and the hidden `_metadata` struct on parquet /
csv / json / text scans. At the 2026-09-16 census 114 of 115 DataFrame
names answered and only `metadataColumn` was left (draft PR — 18b §7).

## Repro and measured answers

The orchestrator measured the logic findings on live PySpark 4.1.2
(probe `/tmp/oc-worker/run18b/oracle/probe_meta_r2.py`, answers
`/tmp/oc-worker/run18b/oracle/meta_r2_spark_2026-09-16.json`, over one
parquet file with rows `(1,'a'),(1,'a'),(2,'b')`; both verified present
2026-10-02):

- R-18b-16 (L-501 P1): `df.distinct().select("i","s",_metadata.row_index)`
  answers 3 rows on the branch; Spark answers 2 —
  `[(1,'a',0),(2,'b',2)]` (cell `distinct_then_metadata`).
  `dropDuplicates(["i"])` then `row_index` answers `[(1,0),(2,2)]` on
  Spark (cell `dropduplicates_then_metadata`). Distinct must dedupe on
  the VISIBLE columns only.
- R-18b-17 (L-502/503/504 P2): stacked
  `select("i","s").select("i","_metadata.file_name")` (cell
  `stacked_select_keep_and_add`), SQL-string
  `filter("_metadata.row_index > 0")` (cell `filter_sql_string_metadata`)
  and aggregates over `_metadata` (cells `agg_over_metadata`,
  `agg_max_row_index`). Spark answers all three; the branch legs are the
  open work.

The critic reports are retained at
`/tmp/oc-worker/run18b/reviews/critic-meta-logic-report.md` and
`/tmp/oc-worker/run18b/reviews/perf-meta-rust-report.md` (both verified
present 2026-10-02).

## Scope

Apply the round-4 patch in a build clone, finish R-18b-16..19 from
`followup-4.md`, move the `fromDDL` NOT NULL pre-step into the Rust type
table (a merge condition per Q-18b-4 — it parses a type string in
Python), then a verification critic.

## Out of scope

- SQL-METADATA-COL-1 (a BACKLOG registry row, R-18b-18).
- DF-METADATA-COL-PERF-1 (a referenced `_metadata` rereads every file
  as its own scan; deferred perf, R-18b-19).
- IO-PARQUET-PARTITION-DISCOVERY-1 (base drops hive partition columns;
  Q-18b-5, its own card).

## Clauses (draft)

- **C-001** The `distinct_then_metadata` and `dropduplicates_then_metadata`
  cells answer Spark.
- **C-002** The three R-18b-17 cells answer Spark or carry strict xfails
  naming the finding.
- **C-003** No type decision stays in Python (Q-18b-4).
- **C-004** A verification critic closes every finding.

## Pointers

- Up: [map.md](map.md) · Handoff (in-repo):
  [overnight-report-2026-09-16-18b.md](overnight-report-2026-09-16-18b.md)
  §7 · Findings (evidence location):
  `/tmp/oc-worker/sb-meta/followup-4.md` · Oracle (evidence location):
  `/tmp/oc-worker/run18b/oracle/meta_r2_spark_2026-09-16.json`
