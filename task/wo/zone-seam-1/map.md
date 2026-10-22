# task/wo/zone-seam-1/ — the work order for ZONE-SEAM-1

ZONE-SEAM-1 (adopted 2026-10-20): one session-zone conversion seam for every conversion of a
zoned timestamp, so a container cast, `to_json` and every door render the session wall as the
leaf cast already does. The root of R-007, R-015, R-011, ENC-1 and CONTAINER-CAST-ZONE-1.

- [order.md](order.md) — the grade B order (2026-10-20): the two conversion paths on main and
  why the write-side gates cannot close the gap; the files to reuse; the seam as a plan-level
  rewrite in `repark-functions`; steps 0–7 with the sketch to the orchestrator first and the
  ZONE-ORACLE-1 corpus as the pin; gates, halts, hand-back.
- [sketch.md](sketch.md) — step 0 (2026-10-21, base `b7a20167`): the corpus replayed once on
  the base (1,199 EQUAL / 411 DIFFERENT / 20 REFUSED / 10 ERROR, equal to the committed list);
  seven corrections to the order's premises; the seam's placement (`rewrite_cast` of
  `SparkLtzTimestampCast`) and signature (a partial-type container UDF that invokes the bare
  leaf cast's own function at each seam leaf); the eleven (source, target) pairs with how
  `CAST` and `TRY_CAST` fail per leaf; the `to_json` and `to_utc_timestamp` sites; the R-011 arm
  A2 named unreachable for its purpose, A1, A3 and R-015 unchanged; the card's rows and 313
  corpus cells by step, the residue; the §5 halts checked; questions Q1–Q5.

Up: [../map.md](../map.md).
