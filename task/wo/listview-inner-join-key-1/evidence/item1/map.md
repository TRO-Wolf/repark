# task/wo/listview-inner-join-key-1/evidence/item1/

Build item 1 (`claude-opus-5-5`, 2026-11-11): the cause confirmed on main, the completed table on main and Spark 4.1.2, and the proposed fix shape. No product code.

- [cause.md](cause.md) — the record: the plans before and after `eliminate_cross_join`, the scratch experiment, the candidates ruled out, the second DataFusion cause behind SQL `USING`, the completed table's counts, the type-variant inventory with its strikes, the proposed fix and its predicted effect on every row, the questions for a ruling and what contradicts the order.
- [table.md](table.md) — the rendered table: counts and cells for each leg (`main` and the three scratch experiment legs) against the Spark oracle, then the oracle cells.
- [probe_item1.py](probe_item1.py) — the probe: sections `plan`, `failing`, `variant`, `lateral`, `native`, `routes`, `mixed` and `using`; `--render <labels>` writes `table.md` from `raw/`.
- [raw/](raw/) — one directory per leg (`main`, `spark`, `exp-norule`, `exp-filter`, `exp-cast`), one JSONL file per section, each opening with a meta line.

The `exp-*` legs ran on a scratch build of this branch's base with an experiment patch that is not committed (`cause.md` §3 describes it). They are evidence for the cause and the shape choice, not for any pin.

Up: [../map.md](../map.md).
