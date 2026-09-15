# Charter ledger — FNP-BITMAP-FACADE-1 · the Spark-facade names for the bitmap aggregates

**Date:** 2026-09-15 · **Branch:** `feat/fnp-bitmap-facade-1` · **Base:** `main`
23ba2e53 · **Model:** zai/glm-5.3-flash · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** FNP-16 bitmap rows: none filed against the facade names; FNP-6D (#609) is
FIXED on the SQL door at the base commit and this unit binds its three names through the
existing aggregate path.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** FNP-6D landed the three bitmap aggregates as Rust UDAFs
(`crates/repark-functions/src/bitmap_agg.rs`) with SQL-door pins at 23ba2e53. The Spark
facade has none of the three names (`F.bitmap_construct_agg` / `F.bitmap_or_agg` /
`F.bitmap_and_agg` are absent from every installer and from the split-identity inventory at
base — statically checked, and the red run below shows the import-level absence). The owner's
shape rule (2026-09-14): a name is done when it is present on `repark.spark.functions` AND
answers PySpark 4.1.2 on BOTH doors, with pins. The standing instruction is Rust-first: the
UDAFs exist; this unit is the thin wrapper round — three `unary_aggregate_udaf` arms plus
three one-line facade wrappers over `column._inner.aggregate(kind, False)`.

**Not in this unit:** any kernel change (`bitmap_agg.rs` is untouched); `collect_list`-style
collection aggregates; the SQL door (already pinned by
`python/repark/tests/test_fnp_6d_bitmap_aggregates.py`, green at base); the
`dataframe/**`, `column.py`, `session/**`, `catalog.py`, `types.py` or SQL-parser files
(card decision D-2 fences them).

**Round shape (owner, run 15a):** this round is the whole unit in five commits — (1) ledger +
fixture + red pins, (2) the Rust arms, (3) the Python wrappers, then a HALT while the
orchestrator copies in a native carrying the arms, then (4) green pins on both doors and
(5) census + gates. This clone carries no native module by design; the orchestrator supplies
it at resume.

## PROPOSITION LEDGER — FNP-BITMAP-FACADE-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `bitmap_construct_agg(col)` / `bitmap_or_agg(col)` / `bitmap_and_agg(col)` are present on `repark.spark.functions` (installed through `functions_bitwise.INSTALL_NAMES`), each with PySpark 4.1.2's one-parameter `col` signature, and the aggregate result type on the Python door is non-null `binary` of exactly 4096 bytes equal to the SQL door's. | `test_fnp_bitmap_facade_1.py::test_names_present_with_pyspark_signature`, `::test_construct_agg_global_matches_sql_door_and_fixture`, `::test_construct_agg_grouped_schema_and_bytes`. | **OPEN** | Red on the base tree (see C-002). The wrappers land in commit 3; the values go green after the orchestrator's native with the commit-2 arms arrives. pins: fnp-bitmap-facade-1/C-001 |
| C-002 | The Python door answers Spark-equal bytes and counts for every card shape: global and grouped `bitmap_construct_agg`, grouped/global `bitmap_or_agg` / `bitmap_and_agg`, NULL rows skipped, empty and all-NULL identities (OR all-zero, AND all-one), a STRING argument coerced like BIGINT, `bitmap_count` over each result, and one sliding-window cell through `Window.rowsBetween` — each equal to the SQL door's answer and to the recorded fixture cell. | `test_fnp_bitmap_facade_1.py` (the remaining pins), against `fnp_bitmap_facade_1_spark_oracle.json` (F6D-\*/B8-\* cells copied verbatim from `/tmp/oc-worker/pc-oracle/fixtures-batch3.json` / `fixtures-batch8.json`, Spark 4.1.2). | **OPEN** | RED on the base tree, recorded 2026-09-15 (this clone carries no native module; the pins fail at collection, and the three names are statically absent from the facade at base — `grep -rn "bitmap_construct_agg\|bitmap_or_agg\|bitmap_and_agg" python/repark/src/repark/spark/` returns nothing): `.venv/bin/python -m pytest python/repark/tests/test_fnp_bitmap_facade_1.py -q -p no:cacheprovider` → `ImportError while loading conftest 'python/repark/tests/conftest.py'` … `E   ModuleNotFoundError: No module named 'repark._native'`, pytest exit 4. pins: fnp-bitmap-facade-1/C-002 |
| C-003 | Out-of-range positions (`32768`, `-1`) raise Spark's `[INVALID_BITMAP_POSITION]` through the Python door with the SQL door's message body (the 0-indexed position, the 32768-bit/4096-byte bounds, `SQLSTATE: 22003`), and `32767` still answers count 1. | `test_fnp_bitmap_facade_1.py::test_out_of_range_positions_raise_invalid_bitmap_position` against fixture cells `B8-pos-32768` / `B8-pos-neg` / `B8-pos-32767`. | **OPEN** | Red on the base tree (C-002's run). pins: fnp-bitmap-facade-1/C-003 |
| C-004 | Census: the three names install through `install_into` so `test_functions_split_identity.py`'s alias tail carries them and its total-length constant moves 6 → 9 on the alias segment; an example exists at `docs/examples/functions/bitmap_aggregates.py` in the ReparkSession idiom; the EX-0 example-coverage count and `inventory.txt` are re-derived by running the gate; any FNP-16 bitmap registry row in `docs/spark-sql-iceberg-parity.md` is flipped with date and unit id; every touched directory's `map.md` is in lockstep. | The split-identity pin, the EX-0 gate, `scripts/check_example_coverage.py --write-inventory`, the registry, the map gates. | **OPEN** | Census work is step 5, after the native lands (the walk counts installed names; it must run against the native-carrying tree). pins: fnp-bitmap-facade-1/C-004 |

VERDICT: 4 clauses, 0 PROVEN, 4 OPEN, 0 REJECTED.

## Round state

| Commit | Content |
|---|---|
| 1 | This ledger, the staging-map row, the verbatim F6D-\*/B8-\* fixture, the red pins (C-001..C-003 obligations), tests `map.md` rows. |
| 2 | Three `unary_aggregate_udaf` arms (`bitmap_construct_agg` / `bitmap_or_agg` / `bitmap_and_agg` → `repark_functions::bitmap_agg::{bitmap_construct_agg_udaf, bitmap_or_agg_udaf, bitmap_and_agg_udaf}`) plus the one-word `mod bitmap_agg;` → `pub mod bitmap_agg;` enabler in `crates/repark-functions/src/lib.rs` (the card's prescribed arm path crosses the crate boundary, and `bitmap_agg` was private — the same cross-crate shape as the neighbouring `repark_functions::aggregate::try_sum_udaf()` arm; `lib.rs` stays at its exact 175-line ceiling). `function_dispatch.rs` was at the exact 1000-line default ceiling, so the four fully-qualified `binary_expr` arms condense onto the existing `datafusion::logical_expr` import (8 → 4 lines each) before the three arms are added — net −13 lines, no baseline raised. |
| 3 | The three facade wrappers in `functions_bitwise.py` over `column._inner.aggregate(kind, False)` exactly as `sum` builds, `INSTALL_NAMES` extended, the split-identity constant updated. |
| 4–5 | After the HALT (`native needed`): pins green on both doors, census, example, EX-0 recount, registry, maps, gates. |
