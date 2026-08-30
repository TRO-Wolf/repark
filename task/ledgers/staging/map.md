# map — task/ledgers/staging/

## Purpose
Ledgers of units in flight. A ledger here on `main` is a charter whose retirement event has not
happened yet; every other ledger leaves for `../completed/` in its unit's last commit.

## Contents
- [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) — **the Spark function parity campaign's
  scope audit and approval gate (2026-08-20):** the twelve-clause proposition ledger, the spike
  evidence behind it; C-007 (the four sub-project families) was closed by ruling D-7 on
  2026-08-20 and the gate passed. Design:
  [../docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md); CAP-1
  appends a compatibility note that points its dated file-size premise at the live guards; slate:
  [../briefs/spark-function-parity.md](../../../briefs/spark-function-parity.md).
- [mw-10-s3tables-mor-ledger.md](mw-10-s3tables-mor-ledger.md) — **MW-10 (2026-08-28), drafted
  for the owner's gate:** the S3 Tables merge-on-read leg the intake called "MW-4b" (that ledger
  id is taken by the archived Glue metadata-rewrite unit). Measure-first on OD-3b: the Glue
  maintenance helper against the table bucket, a bounded retry for service-side compaction, and
  the one question the ruling left open — whether `s3tables:PutTableData` lets `expire_snapshots`
  remove files; a denial is a stop. Six clauses, all OPEN.
- [rp-3-fork-repin-ledger.md](rp-3-fork-repin-ledger.md) — **RP-3 (2026-08-28), on
  `feat/rp-3-fork-repin`:** one frozen fork repin at `d408da42` (F-17, F-14,
  F-7 U3, F-16, F-9, F-15, the public R114 DV API), the engine-side wiring of the fork's DV
  container closure — opt-in for callers, so the engine's own MOR path must make the call — and
  the eight-cell DV input-state matrix on all three doors. Eleven clauses; C-001 and C-002
  PROVEN (2026-08-29). The four #254 clauses ride here. Only the cells green everywhere lift
  the `V3-COW-1` live-DV refusal.
- [sem-0-charter-ledger.md](sem-0-charter-ledger.md) — **SEM-0 (2026-08-21), queued and HELD at
  its approval gate:** the scope audit for closing the two silently wrong answers the low-risk
  sweep registered rather than fixed — `RE-1` (`regexp_extract_all` defaults to capture group 0,
  Spark to 1) and `LOG-1` (the Spark door's `log` is base 10, Spark's is natural). Carries the
  measured implementation scope for both: RE-1's single default site and its three collateral test
  failures (two of which fail as runtime errors and appear in no other RE-1 document), LOG-1's need
  for a new dual-arity null-guarded kernel rather than a redirect to `ln`, the ratchet move that
  comes with it, and the two adjacent defects that should ride along. Both units change a computed
  answer, so the gate wants a dated owner ruling before either writes code.
- [v3-0-charter-ledger.md](v3-0-charter-ledger.md) —
  **V3-0 (2026-08-21):** the format-v3 scope audit, and the defect it found. Intended as a
  charter with no product change and it does not close that way. **Read §3 first**:
  `rewrite_data_files` had no format-version check and reassigned every row's lineage on a v3
  table while returning the correct rows, where Spark carries lineage through unchanged. It is
  reachable on a v3 table that was already in the catalog, which is the drop-in case, so the
  guard shipped with the audit (`V3-LINEAGE-1`). §2 is the other half of the news, and it is
  good: v3 reads and v3 appends are already correct, round-tripped through Spark, including the
  row lineage the format mandates. §4 answers A12's stated first question — adoption, through
  `register_table`, whose Spark signature is measured there.

## Pointers
- Up: [../map.md](../map.md)
