# task/wo/ice-integrity-pins-1/ — the U-9 integrity-pins sketch

ICE-INTEGRITY-PINS-1 (U-9 of the Iceberg closeout, 2026-10-14): three integrity pins (a writer
killed between its data files and its commit; the unknown commit outcome drilled live on Glue
and S3 Tables; the v1.5.4 wheel and the head reading each other's tables). The sketch is
documents only. Pin K was built from it on 2026-10-22 as test code
(`crates/repark-spark/src/tests/kill_mid_write.rs`; the
[unit ledger](../../ledgers/staging/ice-integrity-pins-1-ledger.md)). Pins U and X were built on 2026-10-22: U's
offline twin (`commit_unknown_doors.rs`, 24 cells), the commit gate, the cross-release runner, the
local control X-L, and the two live legs under `python/repark/tests/parity/`. Their measured
results and remaining cross-read obligation are in [the live-run record](live-runs-2026-10-23.md).

- [sketch.md](sketch.md) — the local measurements (kill points, orphan report, catalog endpoint gate, SDK retries, resolution by id, two interpreters), each pin's place on the commit path, the injection options and the one chosen, the cells, the three legs and the workflow change proposed to the owner, risks, questions Q1 to Q10, the order's outline, and the owner's rulings 79 to 88 of 2026-10-15; its fork lines are cited at `076d5f98` and are to be re-read at the current pin `d80a2884` before building. §3 carries the dated "built" line of pin K (2026-10-22) with the premises corrected on main `18626942`.
- [live-runs-2026-10-23.md](live-runs-2026-10-23.md) — five dispatched runs (2026-10-23/24). Run 5 confirms hosted native capture and ends at the job cap after U/X pass; its single permitted rerun is pending. C-060 retains the unresolved cross-read obligation.
- [aws-leg-wiring-2026-10-22.md](aws-leg-wiring-2026-10-22.md) — the #1070 refresh for pins U and X, a proposal (the workflow file stays untouched, H-6): the exact diff on main `a2de82aa` with the legs and U-10's cross-read module at their `parity/` paths, the previous release at 1.5.5, the §6.3 step order and a 120-minute cap; pin K's live leg, never built, is left out.

Up: [../map.md](../map.md).
