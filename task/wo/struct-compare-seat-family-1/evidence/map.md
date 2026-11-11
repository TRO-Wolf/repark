# task/wo/struct-compare-seat-family-1/evidence/

- [after/](after/) — build item 3: the after leg on this unit's release build (`raw/`, v3 records merged), `table.md` rendered beside the before, `moves.md` (moves by seat, the out-of-clause moves, rulings 1 to 5), `int_twin.py` (ruling 5: every residual key-list cell against its `INT` twin), `probe_listview.py` and `listview.txt` (`LISTVIEW-INNER-JOIN-KEY-1`), and `rerun-1292.md` (#1292's three tables and the pins, re-run).
- [cost.md](cost.md) — build item 4: cases (a), (b) and (c), main `68f70b0f` against this head, median of three; [cost_probe.py](cost_probe.py) is the probe (case (b) loads #1292's), [cost-raw.jsonl](cost-raw.jsonl) the runs.
- [frontier-review-memo-2026-11-10.md](frontier-review-memo-2026-11-10.md) — the frontier review memo of 2026-11-10, the design source. It has the measured family (§1), the option-B recommendation the owner adopted (§2), the join-key design (§3), the comparison-seat inventory (§4) and the rule texts (§2.4, §5). The order amends §6.
- [inventory.md](inventory.md) — build item 1, written before the arms: the insertion inventory re-answered for the four new arms, the consumers-of-the-pairing inventory read from the DataFusion 54.1.0 source (every entry a table seat or named out of clause), and the type-variant inventory for join keys from the `K-KIND` rows.
- [names/](names/) — build item 2, ruling 4: `probe_names.py` measures 18 unaliased name cells (`BETWEEN`, `NOT BETWEEN`, simple `CASE`, `nullif` both ways; SQL, `selectExpr`, `F.expr`, typed) on `spark`, `base` (main `68f70b0f`) and `head`, in `raw/`. The pin holds the Spark name where base matched it (5 cells) and no relabel name elsewhere.
- [pin_cells.py](pin_cells.py) — build item 2: writes the pins' cell file from the step-0 Spark legs, a measured after leg and `names/raw/`; every non-matching after cell must carry a card that holds a refusal or an error, or it stops. Q2059 and Q2063 are left out under `LISTVIEW-INNER-JOIN-KEY-1`.
- [step0/](step0/) — the clause: the clerk's 2274-cell family table on Spark 4.1.2, #1292's head (this unit's base) and main before #1292, with the build's completeness additions (148 query and 6 DML cells on Spark and head). It holds `table.md`, `notes.md` (with the orchestrator's additions and the item 1 section), `provenance.md`, the probes `probe_family.py`, `probe_family_add.py` and `rerun_v3.py`, Fable's `probe.py` and `dml.py`, Fable's memo outputs in `memo-probes/`, and `raw/`.

The probes are committed lint-clean (ruff check and format). `probe_family.py --render` reproduces the clerk's
`table.md` byte for byte (sha256 `ca543a36…`) before and after the lint edits; `probe_family_add.py --render` writes
that table plus the additions section. Absolute paths are scrubbed (`STRUCT_FAMILY_IVY_JAR`,
`STRUCT_FAMILY_MEMO_PROBES`). Run a leg with the target tree's own interpreter from a scratch copy, never here.

Typos skips this directory (`extend-exclude` in `.typos.toml`): the evidence holds truncated engine messages and
statement text that must match the raw records, the same reason as the sibling units' evidence entries.

Up: [../map.md](../map.md).
