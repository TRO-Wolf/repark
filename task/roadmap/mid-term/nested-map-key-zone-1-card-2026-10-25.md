# Card NESTED-MAP-KEY-ZONE-1: a date into a MAP key of TIMESTAMP type stores UTC midnight where Spark stores the session midnight

**Date:** 2026-10-25. **Filed by:** the WRITE-SPINE-1 step (2) clerk round 3 (`claude-haiku-5-5`), from the Muse MAX critic's round 1 verdict on PR #1189 (head `5ce6524c`), finding S1, and ruling WS2-30.

**Status:** closed 2026-10-27 by NESTED-LITERAL-SEAM-1 (folded before step (4), as filed). Pre-existing on main (base equals head), not a regression of WRITE-SPINE-1 step (2).

**Closed 2026-10-27** by NESTED-LITERAL-SEAM-1, ledger [nested-literal-seam-1-ledger.md](../../ledgers/staging/nested-literal-seam-1-ledger.md), row C-004: the key pair reaches the seam (ruling NLS-1 Q1). The probe's values now match Spark in both zones (New York `1767330000000000`, Kolkata `1767292200000000`; main stored the UTC midnight `1767312000000000`). The timestamp-key axis is measured: the `insert_map_key` spelling joins the census with 48 cells, of which 4 move. WRITE-SPINE-1's C-026 closes by citation, with a dated closure line in that ledger.

**Why:** the stage's map arm (`zone_partial` in `crates/repark-iceberg/src/write/conform_stage.rs`) clones the key field and partials only the value, so a key pair never reaches the nested zone seam. A date into a `TIMESTAMP` key therefore stores the UTC midnight where Spark stores the session midnight. The ZS-9 nested refusal fires on wall and instant key pairs (probes P21 and P22 refuse), which is why only the date form slips through. The unit pin `a_map_value_converts_and_its_key_keeps_its_field` covers `Utf8` keys only, and the census has no timestamp-key cell, so no fixture measures this axis.

**Measured.** Statement: `INSERT INTO t BY NAME SELECT 1 AS id, map(DATE '2026-01-02', 'x') AS m` into `(id INT, m MAP<TIMESTAMP, STRING>)`, door D3. The column-key form (`map(c_date, 'x')`) gives the same triple in both zones.
- New York: base `1767312000000000`, head `1767312000000000`, Spark `1767330000000000`.
- Kolkata: base `1767312000000000`, head `1767312000000000`, Spark `1767292200000000`.

The critic reproduced this four or more times, and the orchestrator reproduced it on the head build and on Spark 4.1.2 in New York. The probe table is in the [step (2) evidence file](../../wo/write-spine-1/evidence/step-2-census-2026-10-25.md), section "Verification round 1".

**Retires:** when the key pair of a TIMESTAMP-typed key reaches the zone seam (or is otherwise converted), the three values above match Spark in both zones, and a timestamp-key cell joins the census so the axis is measured. It folds with NESTED-LITERAL-SEAM-1 before step (4), because both need the seam to see every nested zone pair.

**Relation:** [WRITE-SPINE-1 step (2) ledger](../../ledgers/staging/write-spine-1-ledger.md), finding F-5 and row C-026; [NESTED-LITERAL-SEAM-1](nested-literal-seam-1-card-2026-10-25.md) (the same fold before step 4).
