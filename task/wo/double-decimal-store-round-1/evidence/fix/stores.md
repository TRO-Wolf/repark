# DOUBLE-DECIMAL-STORE-ROUND-1 narrowed to stores — doors, comparisons, cost (2026-11-14)

The owner's ruling (2026-11-14, about 13:38 UTC, "Merge and go with the split plan") and the seats are in the
[card](../../../../roadmap/mid-term/double-decimal-store-round-1-card-2026-11-13.md#narrowed-to-stores-2026-11-14).
Builds: main `d843d37e` (base) and the stores head `d2b517c6` (head), release wheels. Oracle: Spark 4.1.2 on JDK
17.0.15 with Iceberg 1.11.0. Probes: verifier 2's door probe (`doors.py`, 31 doors × 5 groups, Parquet, 9 doors also
on ORC and Avro) and its 14 extra doors, its coercion fuzz, its structured `d = v` set and its DML cells (`flip.py`);
the native-door copy [flip_native.py](flip_native.py); [small_cells.py](small_cells.py).

## Doors (245 door-group cells per mode; the same in both ANSI modes)

| build | equal to Spark | doors still unlike Spark |
|---|---|---|
| main | 49 / 245 | every store and cast door |
| head | **201 / 245** | `sel_cast` 4, `sel_try` 4, `where_cmp` 4, `ctas` 12, `rtas` 4 (written casts, step 4); `ins_values` 12, `nn_ins_values` 4 (VALUES, step 5) |

The 14 extra doors (MERGE `INSERT *`, `UPDATE SET *`, `UPDATE SET v = s.v * 1.0`, a mixed MERGE, `UPDATE SET v = w`
with and without `WHERE`, `writeTo().overwrite(cond)`, `insertInto(overwrite=True)`): head 14/14, main 1/14.

Native door (`PyReparkSession.native()`, both modes): `INSERT … SELECT` stores `1.26, 1.01, 2.68` on the head where main
stores `1.25, 1.00, 2.68`; `INSERT … UNION` and per-row `UPDATE SET` are pinned. Its SQL `MERGE` refuses ("Unsupported
SQL statement") on main and on the head, and `UPDATE SET d = (SELECT …)` refuses on both.

## Comparisons (head against main, cell for cell, both ANSI modes)

| set | cells per mode | head against main |
|---|---|---|
| coercion fuzz (3,455 doubles × 48 shapes) | 165,846 | byte-identical |
| `d = v`, `d` = m/100 as DECIMAL(10,2), `v` = ±1 to 3 ulps, m = 1 to 10,000 | 60,000 | identical: 2,501 matches, same ids |
| DML cells (21 scalar cells, then `SELECT`/`DELETE`/`UPDATE … WHERE d = v`), Spark door | 25 | identical |
| the same DML cells, native door | 25 | identical (only its added store line moves, to Spark's value) |

## Red and green

- `bool_decimal::tests::a_float_cast_to_decimal_is_left_to_its_cast`: red on the previous head's rule (the plan showed
  `__repark_float_to_decimal__(t.v)`), green after.
- `conform_stage::tests::every_stage_site_stores_a_float_leaf_by_the_jdk_17_kernel`: red (the plan showed
  `CAST(d AS Decimal128(10, 2))` and Arrow's values), green after, for a bare float leaf and a planner-shaped cast.
- `python/repark/tests/sql/test_double_decimal_store_round_1.py`: main 36 failed, 10 passed, 16 xfailed; head 46
  passed, 16 xfailed.

## Cost

10M rows, median seconds of five alternating base/head pairs; each sample a fresh process under `flock -x` measure,
then the opus-cargo and opus2-cargo locks; cores 32-47 (16 of the box's 64); load 2.4 to 4.2 at the samples. The
no-cast rows ran 15 repeats per sample. Raw: [raw/stores-cost-cast-raw.jsonl](raw/stores-cost-cast-raw.jsonl),
[raw/stores-cost-nocast-raw.jsonl](raw/stores-cost-nocast-raw.jsonl), [raw/stores-cost-samples.jsonl](raw/stores-cost-samples.jsonl).

| case | query | base median (5) | head median (5) | head/base | pairwise head/base |
|---|---|---|---|---|---|
| cast | `select_cast_10_2` | 0.0629 | 0.0549 | **0.873** | 0.816, 0.956, 0.821, 1.189, 1.005 |
| cast | `select_cast_38_18` | 0.0604 | 0.0618 | **1.023** | 0.966, 0.886, 0.871, 1.258, 1.324 |
| cast | `filter_decimal_eq_double` | 0.0902 | 0.0893 | **0.991** | 0.94, 1.111, 0.857, 1.051, 0.946 |
| cast | `store_10_2` | 0.1784 | 0.1617 | **0.906** | 1.098, 0.781, 0.738, 1.397, 1.108 |
| cast | `store_38_18` | 0.2995 | 0.3944 | **1.317** | 1.911, 0.598, 1.299, 2.444, 1.328 |
| mixed | `project_decimal` | 0.0770 | 0.0728 | **0.945** | 1.047, 0.989, 1.039, 0.912, 0.928 |
| mixed | `project_double` | 0.0464 | 0.0456 | **0.983** | 0.995, 1.002, 1.09, 0.98, 0.981 |
| mixed | `filter_decimal` | 0.0433 | 0.0441 | **1.019** | 0.968, 1.025, 1.035, 0.95, 0.967 |
| mixed | `filter_double` | 0.0325 | 0.0324 | **0.998** | 0.977, 1.032, 0.976, 1.011, 1.007 |
| mixed | `aggregate_both` | 0.1177 | 0.1171 | **0.994** | 0.982, 0.947, 0.977, 0.994, 1.043 |
| mixed | `store_decimal_20_6` | 0.2466 | 0.2411 | **0.977** | 0.976, 0.761, 0.975, 0.978, 0.663 |
| double | `project_double` | 0.0516 | 0.0506 | **0.981** | 0.989, 0.984, 1.034, 0.93, 0.999 |
| double | `store_double` | 0.1816 | 0.1782 | **0.982** | 1.028, 0.734, 0.965, 1.288, 1.015 |

- **The exception (stores only, as ruled):** the store into DECIMAL(38,18) reads 1.32x (about +0.095 s per 10M rows on
  16 cores); the store into DECIMAL(10,2) reads 0.91x.
- **Equal to main again:** both `CAST` rows and the `decimal = double` filter (0.87x to 1.02x): no cast and no
  comparison runs K1.
- **The no-cast rows** hold 1.05x (0.95x to 1.02x).

Up: [map.md](map.md).
