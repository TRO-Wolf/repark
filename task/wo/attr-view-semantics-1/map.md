# task/wo/attr-view-semantics-1/ — ATTR-VIEW-SEMANTICS-1 order and its measurement

The grade-B order for the card [attr-view-semantics-1-card-2026-10-04.md](../../roadmap/mid-term/attr-view-semantics-1-card-2026-10-04.md),
and the measurement the card cites, copied out of the boot-wiped `/tmp` work directory on the owner's ruling of 2026-10-05.

- [order.md](order.md) — **grade B:** items (a), (b) and (c): SQL temp views and SQL aliases mint attribute ids, and a foreign column raises `MISSING_ATTRIBUTES`. An Opus 5.5 design sketch comes first, then one Muse slice per item. They land on main right after the ATTR-ID-1 stack merges, with a release note naming V3, V4, V9, V12 and V15. Item (d) is already fixed on the stack (PR #951).
- [matrix.md](matrix.md) — the 28-row measurement (2026-10-04): attribute-id rows and outcome rows for Spark 4.1.2, RePark 1.5.2 and the stack at `d5c97862`.
- [views_probe.py](views_probe.py) — the probe that produced the matrix. Run it as `python views_probe.py <spark|repark152|stack> <out.json>`; any engine name other than `spark` imports RePark.
- [spark.json](spark.json), [repark152.json](repark152.json), [stack.json](stack.json) — the probe's raw output per engine, as the matrix reads them.

Up: [../map.md](../map.md).
