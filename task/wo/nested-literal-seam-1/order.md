# NESTED-LITERAL-SEAM-1 — nested date and wall literals, constant nested values and MAP timestamp keys reach the zone seam     grade: B   engine band: Muse MAX builder (sketch and build); Opus 5.5 verifier; Muse MAX second verifier   release: 1.5.6

Opened by the orchestrator (Claude Fable 5.1) on 2026-10-27 under the owner ruling of the same day ("adopt all three leans"): NESTED-MAP-KEY-ZONE-1 is fixed first, folded here, and the format-v2 label (ICE-V2-LABEL-1, PR #1195) re-dates on the main that carries this unit. Cards: [NESTED-LITERAL-SEAM-1](../../roadmap/mid-term/nested-literal-seam-1-card-2026-10-25.md), [NESTED-MAP-KEY-ZONE-1](../../roadmap/mid-term/nested-map-key-zone-1-card-2026-10-25.md). Source findings: WRITE-SPINE-1 step (2) ledger F-2, F-3, F-3 widened (P25) and F-5 (C-026), ruling WS2-25 and WS2-30.

## 0. The gap, measured

WRITE-SPINE-1 step (2) put one plan-time conform stage on every routed write door and seams every nested `arrow_cast` whose argument reads a column (site B, ruling B2). Four shapes stay on main's UTC reading because the column-reference guard excludes them:

- F-2: on D8 (MERGE whole-struct or `SET *`), a nested date or wall literal under the row-merge CASE. 32 census cells. The first attempt to seam literals failed with `column types must match schema types`: the projected list lost its `PARQUET:field_id` metadata under the volatile UDF.
- F-3: a constant nested value outside a CASE. 8 D10 `update_whole_where` struct cells (a date literal, or a scalar-subquery date source). The first attempt broke the pin `a_plain_update_whose_only_naive_leaf_is_a_try_cast_stores_spark_instant`: `TRY_CAST(instant AS TIMESTAMP_NTZ)` inside a literal struct was already the UTC wall when the seam read it in the session zone (`1767341045123456` against Spark's `1767323045123456`), a double conversion.
- P25: the F-3 mechanism on a partitioned UPDATE target (`UPDATE t SET st = named_struct('a', DATE '2026-01-02') WHERE k = 0`), outside the census.
- F-5: a date into a `MAP<TIMESTAMP, …>` key on D3 (`map(DATE '2026-01-02', 'x')` and `map(c_date, 'x')`): `zone_partial` in `crates/repark-iceberg/src/write/conform_stage.rs` clones the key field and partials only the value, so the key pair never reaches the seam. Spark: New York `1767330000000000`, Kolkata `1767292200000000`; main stores `1767312000000000` in both.

Silent values all four; the v2 label cannot carry them.

## 1. Rulings already made

- Dialect and door classes are WRITE-SPINE-1's; this unit changes no door routing and retires no refusal (ZS-9 stays).
- A value Spark refuses stays refused; a value Spark stores is stored as Spark stores it; nothing new is silent (owner, standing).
- Cost gate: the write census replay wall and the C-012 set within 1.05x of main (coercion ruling 2026-10-12).
- The census is the measurement: `python/repark/tests/writes/_record_write_spine_1.py --main --base <sha>` replays 12,192 cells; this unit adds cells for the four shapes so the axis is measured from now on.

## 2. Steps

0. **Sketch (hand back before step 1; the orchestrator rules, then step 1 starts).** Reproduce the four shapes on this head and on Spark 4.1.2 (the oracle on this box; one JVM) in New York and Kolkata; record the six values per shape. Find the F-2 cause (why the projected list under the row-merge CASE loses `PARQUET:field_id` when a literal is seamed, when a plain SELECT with the same CASE does not) and the F-3 cause (where the literal struct's `TRY_CAST` is folded before the seam sees it). Propose the seam rule that replaces the column-reference guard: which nested expressions carry a zone pair the seam must convert (a date or wall literal, a `named_struct`/`map`/`array` constant, a scalar subquery of date type) and which already carry the converted instant (an instant cast to a wall by an explicit cast). Propose the map-key arm: the key pair reaches the seam (or the key is converted in the stage) for `MAP<TIMESTAMP, …>` and `MAP<TIMESTAMP_NTZ, …>` keys, literal and column. State the expected census moves exactly (the 40 cells by id; the new cells) and the pins. Questions, with a lean each. No product change in this step.
1. **Build.** The seam rule and the map-key arm, in `crates/repark-iceberg/src/write/conform_stage.rs` and the seam modules under `conform_stage/` only, within the file-size ratchets. Unit pins per mechanism (F-2 CASE literal on D8, F-3 constant struct on D10, P25 partitioned target, map key literal and column on D3, each asserted against Spark's recorded values in both zones, and the `TRY_CAST` double-conversion pin stays green). New census cells for the four shapes in the fixture, re-recorded with the recorder; the replay on the head shows the 40 F-2/F-3 cells and the new cells DIFFERENT to EQUAL and no other cell moving.
2. **Measure.** `pytest python/repark/tests/writes -q -n 8` never worse; `cargo test -p repark-iceberg` and `cargo test -p repark-spark` green; the replay counts and wall; the C-012 set median within 1.05x; the 842-cell matrix untouched (the orchestrator runs it on the merged main).
3. **Records** (clerk, Haiku 5.5 xhigh): ledger `task/ledgers/staging/nested-literal-seam-1-ledger.md`, the two cards retire, registry rows, the write-spine-1 ledger's C-026 closes by citation, maps, release-notes line.

## 3. Gates

`make ci` surface locally for the touched crates, `make rust-panic-ban`, `make check-rust-file-size`, the comment gate (no comments in code from Anthropic models; Muse writes none either under this unit), `bash scripts/check_map_md.sh --base origin/main` before the PR. CI green before any verifier; Opus 5.5 verifier (value-moving) and Muse MAX second verifier, merge on the union of PASS and the owner's word.

## 4. Halts

Any census cell moving that the sketch did not list; any EQUAL cell regressing; the cost gate; a need to touch a door's routing or a refusal; the file-size ratchet with no honest split.
