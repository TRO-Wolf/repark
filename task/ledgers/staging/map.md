# map — task/ledgers/staging/

## Purpose
Ledgers of units in flight. A ledger here on `main` is a charter whose retirement event has not
happened yet; every other ledger leaves for `../completed/` in its unit's last commit.

## Contents
- [dfp-1-preserve-null-unnest-ledger.md](dfp-1-preserve-null-unnest-ledger.md) — **DFP-1
  (2026-08-31), owner-approved and queued:** remove redundant null-replacement projections from
  `dynamic_flatten` by using preserve-null Unnest. The finite List/LargeList/FixedSizeList/
  Dictionary<List> matrix and plan-shape proof are the delivery gate.
- [rp-4-fork-repin-ledger.md](rp-4-fork-repin-ledger.md) — **RP-4 (2026-08-31), in
  flight:** fork repin `d408da42` → `33be9a0` (F-7 slice 1 consume, F-6 carry).
  Family frozen. Ledger born on `feat/rp-4-fork-repin`.
- [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) — **the Spark function parity campaign's
  scope audit and approval gate (2026-08-20):** the twelve-clause proposition ledger, the spike
  evidence behind it; C-007 (the four sub-project families) was closed by ruling D-7 on
  2026-08-20 and the gate passed. Design:
  [../docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md); CAP-1
  appends a compatibility note that points its dated file-size premise at the live guards; slate:
  [../briefs/spark-function-parity.md](../../../briefs/spark-function-parity.md).
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
- [fnp-7-try-inversions-ledger.md](fnp-7-try-inversions-ledger.md) — **FNP-7a/7b (2026-08-31):**
  the twelve `try_*` NULL-yielding inversions. Design §7 rows FNP-7a (8) and FNP-7b (4,
  unblocked by F-Y10-1). `risk_tier: standard`. Branch `feat/fnp-7-try-inversions`.

## Pointers
- Up: [../map.md](../map.md)
