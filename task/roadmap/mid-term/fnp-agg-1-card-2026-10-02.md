# FNP-AGG-1 — the aggregate names still only on the closed #625 branch

**Filed: 2026-10-02, re-cut from the closed draft PR #625** (step 1 of 5
per the brief). The PR body and close comment were not re-read in this
lane (`gh` is banned for worker lanes); the card is re-cut from
[overnight-report-2026-09-16-18a.md](overnight-report-2026-09-16-18a.md)
(Q-18a-1, the #625 row), the 2026-09-21 rescue report
[night-report-2026-09-21-27-muse7.md](night-report-2026-09-21-27-muse7.md)
(unit 1), and the slice-(d) ledger
[task/ledgers/staging/fnp-agg-1-ledger.md](../../ledgers/staging/fnp-agg-1-ledger.md)
(landing commit `4153485151b5d196bbf06771acede627e4a1dc0a`, verified
in-repo). Kept branch: `feat/fnp-agg-1` at
`4a3379fe40343ba40acd7b20fe24e3430f3afe4e` (remote head read 2026-10-02;
matches the 18a and muse7 records, untouched since). **Status:** slice
(d) (`grouping_id` plus the shared foundation) landed; slices (a), (b)
and (c) are unbuilt — per the 09-21 night report, 10 names remain only
on the branch. **Target:** mid-term.

## The gap it records

Twelve aggregate names stood on the branch: `any_value`,
`count_min_sketch`, `grouping_id`, `histogram_numeric`,
`listagg_distinct`, `max_by`, `min_by`, `percentile`, `product`,
`string_agg_distinct`, `sum_distinct`, `sumDistinct`. Slice (d) landed
`grouping_id` plus the shared foundation; the rest were never gated on
their final head (Q-18a-1) and the branch sat CONFLICTING and untouched
at the 09-21 rescue, which planned slices (a)–(c) after (d) lands and
closing #625 with a pointer once they do.

## Repro and measured answers

The oracle is recorded: fourteen aggregate names against live PySpark
4.1.2 in `/tmp/oc-worker/pa-agg/agg_misc_spark_oracle.json`
(2026-09-14; verified present 2026-10-02). Which of the 12 branch names
are still missing from main is not yet measured in the re-cut sources.
Repro to run on the day: bind all 12 names on current main, both doors,
against the recorded oracle cells, and list the still-missing set; that
list is slices (a)–(c).

## Scope

The rescue slices (a)–(c) per the ledger's slice plan, both doors, with
the recorded oracle cells as pins. Any revived branch code gets a
verification critic first — `sum_distinct` was never reviewed
(R-18a-28), and Q-18a-1's gate chain (whole facade, whole parity,
verify, clippy, sizes) runs after the rebase.

## Out of scope

- Slice (d) (landed as `41534851`).
- FNP-MATH-1 (the #628 rescue runs separately; 09-21: PR #779).

## Clauses (draft)

- **C-001** The still-missing set is measured against the recorded
  oracle before any slice opens.
- **C-002** Each slice lands both-door pins against the recorded cells.
- **C-003** No slice merges without the verification critic and the
  Q-18a-1 gate chain.

## Pointers

- Up: [map.md](map.md) · Handoff (in-repo):
  [overnight-report-2026-09-16-18a.md](overnight-report-2026-09-16-18a.md)
  Q-18a-1 · Rescue plan (in-repo):
  [night-report-2026-09-21-27-muse7.md](night-report-2026-09-21-27-muse7.md)
  unit 1 · Slice ledger (in-repo):
  [task/ledgers/staging/fnp-agg-1-ledger.md](../../ledgers/staging/fnp-agg-1-ledger.md)
  · Oracle (evidence location):
  `/tmp/oc-worker/pa-agg/agg_misc_spark_oracle.json`
