# task/wo/ice-integrity-pins-1/ — the U-9 integrity-pins sketch

ICE-INTEGRITY-PINS-1 (U-9 of the Iceberg closeout, 2026-10-14): three integrity pins (a writer
killed between its data files and its commit; the unknown commit outcome drilled live on Glue
and S3 Tables; the v1.5.4 wheel and the head reading each other's tables). The sketch is
documents only. Pin K was built from it on 2026-10-22 as test code
(`crates/repark-spark/src/tests/kill_mid_write.rs`; the
[unit ledger](../../ledgers/staging/ice-integrity-pins-1-ledger.md)); pins U and X await #1070.

- [sketch.md](sketch.md) — the local measurements (kill points, orphan report, catalog endpoint gate, SDK retries, resolution by id, two interpreters), each pin's place on the commit path, the injection options and the one chosen, the cells, the three legs and the workflow change proposed to the owner, risks, questions Q1 to Q10, the order's outline, and the owner's rulings 79 to 88 of 2026-10-15; its fork lines are cited at `076d5f98` and are to be re-read at the current pin `d80a2884` before building. §3 carries the dated "built" line of pin K (2026-10-22) with the premises corrected on main `18626942`.

Up: [../map.md](../map.md).
