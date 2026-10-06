# task/wo/attr-view-semantics-1/ — ATTR-VIEW-SEMANTICS-1 order and its measurement

The grade-B order for the card [attr-view-semantics-1-card-2026-10-04.md](../../roadmap/mid-term/attr-view-semantics-1-card-2026-10-04.md),
and the measurement the card cites, copied out of the boot-wiped `/tmp` work directory on the owner's ruling of 2026-10-05.

- [order.md](order.md) — **grade B:** items (a), (b) and (c): SQL temp views and SQL aliases mint attribute ids, and a foreign column raises `MISSING_ATTRIBUTES`. An Opus 5.5 design sketch comes first, then one Muse slice per item. They land on main right after the ATTR-ID-1 stack merges, with a release note naming V3, V4, V9, V12 and V15. Item (d) is already fixed on the stack (PR #951).
- [design.md](design.md) — **the Opus 5.5 sketch (2026-10-05):** the measured mint table. Item (a) is delivered on the stack. (b) uses the B1 written-alias marker on the user SQL door, with a planning fallback and a reserved-name refusal. (c) is a general per-operation `MISSING_ATTRIBUTES` refusal through `locate`, with Q5 drop folded in. The blast radius against the (a) baseline is 0 away, and the document gives slices B and C with pins, mutations, gates and halts, plus the release note.
- [design-evidence/](design-evidence/map.md) — the sketch round's raw probe outputs (Spark, base, b1, head) and the throwaway diff.
- [matrix.md](matrix.md) — the 28-row measurement (2026-10-04): attribute-id rows and outcome rows for Spark 4.1.2, RePark 1.5.2 and the stack at `d5c97862`.
- [views_probe.py](views_probe.py) — the probe that produced the matrix. Run it as `python views_probe.py <spark|repark152|stack> <out.json>`; any engine name other than `spark` imports RePark. The repo copy is lint-normalized (type annotations, `pathlib`, the `functions` import renamed to `sf`) with no change in behaviour. Re-run on the stack at `b9db3536`, it differs from `stack.json` only on V-3a and V-3b, which PR #951 fixed.
- [spark.json](spark.json), [repark152.json](repark152.json), [stack.json](stack.json) — the probe's raw output per engine, as the matrix reads them.

Up: [../map.md](../map.md).
