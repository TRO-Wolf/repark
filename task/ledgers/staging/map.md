# map — task/ledgers/staging/

## Purpose
Ledgers of units in flight. A ledger here on `main` is a charter whose retirement event has not
happened yet; every other ledger leaves for `../completed/` in its unit's last commit.

## Contents
- [ex-0-example-drift-gate-ledger.md](ex-0-example-drift-gate-ledger.md) —
  **EX-0 (2026-08-31), in flight:** v0.7 example drift gate + public-surface
  inventory. `risk_tier: standard`. Branch `feat/ex-0-example-drift-gate`.
  Ruling: [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md)
  §v0.7 deliverable 2.
- [ex-1-class-surfaces-ledger.md](ex-1-class-surfaces-ledger.md) —
  **EX-1 (2026-08-31), in flight:** widens the EX-0 example-coverage inventory
  with the class surfaces the owner ruled into v0.7 — Column, Window,
  WindowSpec, Catalog, the `types` module surface, `ml`, and Row (150 names,
  763 → 913). `risk_tier: standard`. Branch `feat/ex-1-class-surfaces`, stacked
  on `feat/ex-0-example-drift-gate`.
- [ex-2-functions-math-bitwise-ledger.md](ex-2-functions-math-bitwise-ledger.md) —
  **EX-2 (2026-09-01), in flight:** the v0.7 example backfill's `F.*` math +
  bitwise family — the campaign pilot. One clause per batch; batch 1 covers
  eleven roots / exponential / power / sign / rounding names and moves the
  backlog ratchet 892 → 881; the twelfth, `F.expm1`, is measured, reported and
  left on the backlog rather than taught by an example that omits its reason
  for existing. `risk_tier: standard`. Branch
  `feat/ex-2-functions-math-bitwise`. Slate:
  [../briefs/example-backfill.md](../../../briefs/example-backfill.md).
- [v3-5-dv-compaction-ledger.md](v3-5-dv-compaction-ledger.md) — **V3-5 (2026-08-31),
  in flight:** DV-aware v3 compaction (`V3-DANGLE-1`, B-MOR-3 residue, true
  result counts). Measure `rewrite_data_files` on live Puffin DVs at fork
  `33be9a0` before any product edit. Ledger born on `feat/v3-5-dv-compaction`.
- [ref-branch-tag-wap-ledger.md](ref-branch-tag-wap-ledger.md) — **REF (2026-09-01), in flight:**
  Iceberg branch / tag operations + write-audit-publish. The C-001 matrix measures every
  reachable ref door at fork pin `33be9a0` against live PySpark 4.1.2 + Iceberg 1.11.0 and
  moves the write-to-branch gap: F-6 gave `to_branch` to the transaction actions, not to the
  `iceberg-datafusion` write path `INSERT`/`UPDATE`/`DELETE` execute through.
  `risk_tier: standard`. Branch `feat/ref-branch-tag-wap`. **2026-09-01 delivered, parks in
  staging until the owner merges:** C-001..C-006 PROVEN — the `branch_`/`tag_` read selectors
  resolve (registry `REF-4` FIXED), both `WITH SNAPSHOT RETENTION` halves land on both doors,
  the write leg and WAP stay DECLARED with re-measured reasons (`REF-1`, `REF-3`) and the
  restated fork ask filed as F-6b.
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
  **Owner ruling 2026-08-31:** both rows fix to Spark. Delivery:
  [sem-1-spark-answer-parity-ledger.md](sem-1-spark-answer-parity-ledger.md).
- [sem-1-spark-answer-parity-ledger.md](sem-1-spark-answer-parity-ledger.md) — **SEM-1 (2026-08-31):**
  close RE-1 and LOG-1 to Spark semantics under the dated owner ruling. RE-1's default is already
  group 1 on this tree (prior SEM-1, PR #193); this unit re-measures it and builds the dual-arity
  null-guarded Spark `log` kernel (charter SEM-2), the `EXPECTED_DIVERGENCES` ratchet, and `F.log`'s
  two-argument form.
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
- [v3-6-v3-types-ledger.md](v3-6-v3-types-ledger.md) — **V3-6 (2026-08-31; 2026-09-01
  delivered, parks in staging until the owner merges):** remaining v3 types (binary
  variant, nanosecond timestamps, `unknown`, column defaults). C-001 measurement matrix
  in the ledger; C-002..C-007 PROVEN — ns timestamps consumed, column defaults
  consumed on write/read with Spark-equal DEFAULT refusals, variant/unknown refusals
  pinned, registry row landed, upgrade surface untouched.
- [rp-6-fork-repin-ledger.md](rp-6-fork-repin-ledger.md) — **RP-6 (2026-09-01), in flight:**
  fork repin `00cdde0` → `fb0cacfa` (PR-1..PR-7). Consume REPLACE added>deleted refuse,
  evolved-spec rewrite, Glue/S3 Tables commit seams, V3 MoR UPDATE lineage, branch MoR
  UPDATE, V2→V3 upgrade, PR-7 closeout. Re-measure and lift `V3-COW-1` where Spark-equal;
  F-7 preserve-half; RDF-1 honesty; evolved-spec Spark-door pin.
  `risk_tier: standard`. Branch `feat/rp-6-fork-repin`.

## Pointers
- Up: [../map.md](../map.md)
