# DOUBLE-DECIMAL-STORE-ROUND-1 step 0 — the domain table

RePark main against Spark 4.1.2, ANSI on and off. Measurement only; no product code.

## What was measured

- **Values (21):** halfway and near-halfway in binary `1.255`, `1.005`, `2.675`, `0.125`, `0.375`, `2.5` and
  the negatives `-1.255`, `-0.125`, `-2.5`; plain `1e-20`, `123456789.125`, `-0.0`, `0.30000000000000004`,
  `9.995`, `99.95`; overflowing `1e10`; two values whose JDK 17 text is not the shortest
  (`2.82879384806159E17`, `1e23`); `NaN`, `Infinity`, `-Infinity`.
- **Sources:** a DOUBLE and a FLOAT column of an Iceberg table (`CAST('<text>' AS DOUBLE|FLOAT)`, written
  once per run); the literal door casts the same text inline.
- **Targets:** DECIMAL(10,2), (5,4), (38,18), (38,0), (3,1).
- **The twelve brief doors:** `sel_cast_col` (`SELECT CAST(v AS DECIMAL(p,s))` over the table),
  `sel_cast_lit` (`CAST(CAST('<text>' AS DOUBLE) AS DECIMAL(p,s))`), `df_cast` (`F.col("v").cast(...)`),
  `ins_values` (`INSERT … VALUES` of the literals), `ins_select`, `ins_union` (`INSERT … SELECT … UNION
  ALL SELECT …`), `ctas` (with the explicit cast), `update` (`UPDATE … SET v = <literal>`), `merge_update`,
  `merge_insert`, `df_writeto` (`writeTo(t).append()` of the DOUBLE/FLOAT frame), `df_saveastable`
  (`write.mode("append").saveAsTable`).
- **The four doors the inventory added:** `ins_overwrite` (`INSERT OVERWRITE`), `ins_overwrite_part`
  (static-partition `INSERT OVERWRITE … PARTITION (p = 1)`), `df_insertinto` (`write.insertInto`),
  `df_overwrite` (`writeTo(t).overwritePartitions()`).
- **Per cell:** the stored or answered value, read back by the engine that wrote it; where a statement
  refuses, each row is run alone, so every row has its own answer or error.

## Provenance

- **RePark:** the scratchpad package `nwc/pkg-base` (product-identical to main `2707924e`), imported read
  only through `PYTHONPATH` by the lane interpreter; each raw file's `provenance` row names the loaded
  `repark/_native.abi3.so`. Version `1.5.6`.
- **Spark:** PySpark 4.1.2 in the c26 oracle environment, Zulu JDK 17, `local[1]`, UI off, session zone
  UTC; Iceberg through `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`; a private hadoop catalog per run.
- **When and how:** 2026-11-13, 13:40–14:45 local time, cores 32–47, every probe under the shared measure
  lock and every Spark JVM under a Spark slot. Commands (from `probes/`): `run.py <spark|repark>
  <true|false> <warehouse-parent> <out> <iceberg-jar> [<groups>]`, the groups being the door names,
  `source`, `card` and `selects`; `render.py ../raw <summary|selects|DOUBLE|FLOAT> [<ansi>]` prints the
  tables below.
- **One probe change during the run:** `spark-ansi-true.jsonl` was written before the probe's door
  lambdas became one `write_action` function (a lint fix; the only statement it changed is `ins_union`'s
  batch, which gained a `true AND` conjunct). Spark re-run with the final probe at 14:45 gave the same
  answer in all 128 records; RePark's files are from the final probe.
- **A harness observation, not root-caused:** on the read doors (`sel_cast_col`, `df_cast`), after the
  whole-table statement refused, a one-row retry of `NaN` or `±Infinity` sometimes reported the batch's
  `too large to store` text instead of `Overflowing on NaN`; the same one-row query in a fresh script
  reports `Overflowing on NaN`. The verdict (RePark refuses where Spark answers NULL) does not depend on
  the text.

## 1. The verdicts, door by door

Per door and source: 21 values × 5 targets = 105 cells per ANSI mode. `EQUAL-ERR`: both refuse.
`ERR-FOR-NULL`: Spark answers NULL, RePark refuses. `WRONG-VALUE`: both answer, the numbers differ.

| ANSI | door | source | EQUAL | EQUAL-ERR | WRONG-VALUE | ERR-FOR-NULL | NULL-FOR-ERR | MISSING-VALUE |
|---|---|---|---|---|---|---|---|---|
| true | `sel_cast_col` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `sel_cast_col` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `sel_cast_lit` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `sel_cast_lit` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `df_cast` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `df_cast` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `ins_values` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `ins_values` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `ins_select` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `ins_select` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `ins_union` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `ins_union` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `ctas` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `ctas` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `update` | DOUBLE | 68 | 15 | 7 | 15 | 0 | 0 |
| true | `update` | FLOAT | 62 | 14 | 14 | 15 | 0 | 0 |
| true | `merge_update` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `merge_update` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `merge_insert` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `merge_insert` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `df_writeto` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `df_writeto` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `df_saveastable` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `df_saveastable` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `ins_overwrite` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `ins_overwrite` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `ins_overwrite_part` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `ins_overwrite_part` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `df_insertinto` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `df_insertinto` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| true | `df_overwrite` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| true | `df_overwrite` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| false | `sel_cast_col` | DOUBLE | 65 | 0 | 10 | 30 | 0 | 0 |
| false | `sel_cast_col` | FLOAT | 66 | 0 | 10 | 29 | 0 | 0 |
| false | `sel_cast_lit` | DOUBLE | 65 | 0 | 10 | 30 | 0 | 0 |
| false | `sel_cast_lit` | FLOAT | 66 | 0 | 10 | 29 | 0 | 0 |
| false | `df_cast` | DOUBLE | 65 | 0 | 10 | 30 | 0 | 0 |
| false | `df_cast` | FLOAT | 66 | 0 | 10 | 29 | 0 | 0 |
| false | `ins_values` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| false | `ins_values` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| false | `ins_select` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| false | `ins_select` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| false | `ins_union` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| false | `ins_union` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| false | `ctas` | DOUBLE | 65 | 0 | 10 | 30 | 0 | 0 |
| false | `ctas` | FLOAT | 66 | 0 | 10 | 29 | 0 | 0 |
| false | `update` | DOUBLE | 68 | 15 | 7 | 15 | 0 | 0 |
| false | `update` | FLOAT | 62 | 14 | 14 | 15 | 0 | 0 |
| false | `merge_update` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| false | `merge_update` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| false | `merge_insert` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| false | `merge_insert` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| false | `df_writeto` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| false | `df_writeto` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| false | `df_saveastable` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| false | `df_saveastable` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| false | `ins_overwrite` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| false | `ins_overwrite` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| false | `ins_overwrite_part` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| false | `ins_overwrite_part` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| false | `df_insertinto` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| false | `df_insertinto` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |
| false | `df_overwrite` | DOUBLE | 65 | 15 | 10 | 15 | 0 | 0 |
| false | `df_overwrite` | FLOAT | 66 | 14 | 10 | 15 | 0 | 0 |

## 2. DOUBLE, ANSI on: Spark's answer, and RePark's in bold where it differs

The cell is the `sel_cast_col` door; a door whose verdict differs from it is named in brackets with
RePark's answer there. Error rows name Spark's class; RePark has no class (Arrow's text).

| value | (10,2) | (5,4) | (38,18) | (38,0) | (3,1) |
|---|---|---|---|---|---|
| `1.255` | 1.26 / **1.25** (update: 1.26) | 1.2550 | 1.255000000000000000 | 1 | 1.3 |
| `1.005` | 1.01 / **1.00** (update: 1.01) | 1.0050 | 1.005000000000000000 / **1.004999999999999872** | 1 | 1.0 |
| `2.675` | 2.68 | 2.6750 | 2.675000000000000000 | 3 | 2.7 |
| `0.125` | 0.13 | 0.1250 | 0.125000000000000000 | 0 | 0.1 |
| `0.375` | 0.38 | 0.3750 | 0.375000000000000000 | 0 | 0.4 |
| `2.5` | 2.50 | 2.5000 | 2.500000000000000000 | 3 | 2.5 |
| `-1.255` | -1.26 / **-1.25** (update: -1.26) | -1.2550 | -1.255000000000000000 | -1 | -1.3 |
| `-0.125` | -0.13 | -0.1250 | -0.125000000000000000 | 0 | -0.1 |
| `-2.5` | -2.50 | -2.5000 | -2.500000000000000000 | -3 | -2.5 |
| `1e-20` | 0.00 | 0.0000 | 0E-18 | 0 | 0.0 |
| `123456789.125` | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR NUMERIC_VALUE_OUT_OF_RANGE | 123456789.125000000000000000 / **123456789.125000007446953984** | 123456789 | ERR NUMERIC_VALUE_OUT_OF_RANGE |
| `-0.0` | 0.00 | 0.0000 | 0E-18 | 0 | 0.0 |
| `0.30000000000000004` | 0.30 | 0.3000 | 0.300000000000000040 / **0.300000000000000064** | 0 | 0.3 |
| `9.995` | 10.00 / **9.99** (update: 10.00) | 9.9950 | 9.995000000000000000 | 10 | 10.0 |
| `99.95` | 99.95 | ERR NUMERIC_VALUE_OUT_OF_RANGE | 99.950000000000000000 | 100 | ERR NUMERIC_VALUE_OUT_OF_RANGE |
| `1e10` | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR NUMERIC_VALUE_OUT_OF_RANGE | 10000000000.000000000000000000 / **9999999999.999999583119736832** | 10000000000 | ERR NUMERIC_VALUE_OUT_OF_RANGE |
| `2.82879384806159E17` | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR NUMERIC_VALUE_OUT_OF_RANGE | 282879384806159008.000000000000000000 / **282879384806159016.242322491289108480** | 282879384806159008 (update: 282879384806159003) | ERR NUMERIC_VALUE_OUT_OF_RANGE |
| `1e23` | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR NUMERIC_VALUE_OUT_OF_RANGE | 99999999999999990000000 / **99999999999999991611392** (update: 99999999999999997748810) | ERR NUMERIC_VALUE_OUT_OF_RANGE |
| `NaN` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-precision** |
| `Infinity` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-precision** |
| `-Infinity` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-precision** |

## 3. FLOAT, ANSI on

| value | (10,2) | (5,4) | (38,18) | (38,0) | (3,1) |
|---|---|---|---|---|---|
| `1.255` | 1.25 (update: 1.26) | 1.2550 | 1.254999995231628400 / **1.254999995231628544** | 1 | 1.3 |
| `1.005` | 1.00 (update: 1.01) | 1.0050 | 1.004999995231628400 / **1.004999995231628416** | 1 | 1.0 |
| `2.675` | 2.67 (update: 2.68) | 2.6750 | 2.674999952316284000 / **2.674999952316284416** | 3 | 2.7 |
| `0.125` | 0.13 | 0.1250 | 0.125000000000000000 | 0 | 0.1 |
| `0.375` | 0.38 | 0.3750 | 0.375000000000000000 | 0 | 0.4 |
| `2.5` | 2.50 | 2.5000 | 2.500000000000000000 | 3 | 2.5 |
| `-1.255` | -1.25 (update: -1.26) | -1.2550 | -1.254999995231628400 / **-1.254999995231628544** | -1 | -1.3 |
| `-0.125` | -0.13 | -0.1250 | -0.125000000000000000 | 0 | -0.1 |
| `-2.5` | -2.50 | -2.5000 | -2.500000000000000000 | -3 | -2.5 |
| `1e-20` | 0.00 | 0.0000 | 0E-18 | 0 | 0.0 |
| `123456789.125` | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR NUMERIC_VALUE_OUT_OF_RANGE | 123456792.000000000000000000 / **123456792.000000005278531584** | 123456792 | ERR NUMERIC_VALUE_OUT_OF_RANGE |
| `-0.0` | 0.00 | 0.0000 | 0E-18 | 0 | 0.0 |
| `0.30000000000000004` | 0.30 | 0.3000 | 0.300000011920928960 | 0 | 0.3 |
| `9.995` | 9.99 | 9.9950 | 9.994999885559082000 / **9.994999885559083008** | 10 | 10.0 |
| `99.95` | 99.95 | ERR NUMERIC_VALUE_OUT_OF_RANGE | 99.949996948242190000 / **99.949996948242186240** | 100 | 99.9 |
| `1e10` | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR NUMERIC_VALUE_OUT_OF_RANGE | 10000000000.000000000000000000 / **9999999999.999999583119736832** | 10000000000 | ERR NUMERIC_VALUE_OUT_OF_RANGE |
| `2.82879384806159E17` | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR NUMERIC_VALUE_OUT_OF_RANGE | 282879379476840448.000000000000000000 / **282879379476840456.813544470764060672** | 282879379476840448 | ERR NUMERIC_VALUE_OUT_OF_RANGE |
| `1e23` | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR NUMERIC_VALUE_OUT_OF_RANGE | 99999997781963080000000 / **99999997781963083612160** | ERR NUMERIC_VALUE_OUT_OF_RANGE |
| `NaN` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-precision** |
| `Infinity` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-precision** |
| `-Infinity` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-precision** |

## 4. DOUBLE, ANSI off

| value | (10,2) | (5,4) | (38,18) | (38,0) | (3,1) |
|---|---|---|---|---|---|
| `1.255` | 1.26 / **1.25** (update: 1.26) | 1.2550 | 1.255000000000000000 | 1 | 1.3 |
| `1.005` | 1.01 / **1.00** (update: 1.01) | 1.0050 | 1.005000000000000000 / **1.004999999999999872** | 1 | 1.0 |
| `2.675` | 2.68 | 2.6750 | 2.675000000000000000 | 3 | 2.7 |
| `0.125` | 0.13 | 0.1250 | 0.125000000000000000 | 0 | 0.1 |
| `0.375` | 0.38 | 0.3750 | 0.375000000000000000 | 0 | 0.4 |
| `2.5` | 2.50 | 2.5000 | 2.500000000000000000 | 3 | 2.5 |
| `-1.255` | -1.26 / **-1.25** (update: -1.26) | -1.2550 | -1.255000000000000000 | -1 | -1.3 |
| `-0.125` | -0.13 | -0.1250 | -0.125000000000000000 | 0 | -0.1 |
| `-2.5` | -2.50 | -2.5000 | -2.500000000000000000 | -3 | -2.5 |
| `1e-20` | 0.00 | 0.0000 | 0E-18 | 0 | 0.0 |
| `123456789.125` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | 123456789.125000000000000000 / **123456789.125000007446953984** | 123456789 | NULL / **ERR arrow-precision** |
| `-0.0` | 0.00 | 0.0000 | 0E-18 | 0 | 0.0 |
| `0.30000000000000004` | 0.30 | 0.3000 | 0.300000000000000040 / **0.300000000000000064** | 0 | 0.3 |
| `9.995` | 10.00 / **9.99** (update: 10.00) | 9.9950 | 9.995000000000000000 | 10 | 10.0 |
| `99.95` | 99.95 | NULL / **ERR arrow-precision** | 99.950000000000000000 | 100 | NULL / **ERR arrow-precision** |
| `1e10` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | 10000000000.000000000000000000 / **9999999999.999999583119736832** | 10000000000 | NULL / **ERR arrow-precision** |
| `2.82879384806159E17` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | 282879384806159008.000000000000000000 / **282879384806159016.242322491289108480** | 282879384806159008 (update: 282879384806159003) | NULL / **ERR arrow-precision** |
| `1e23` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | 99999999999999990000000 / **99999999999999991611392** (update: 99999999999999997748810) | NULL / **ERR arrow-precision** |
| `NaN` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-precision** |
| `Infinity` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-precision** |
| `-Infinity` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-precision** |

## 5. FLOAT, ANSI off

| value | (10,2) | (5,4) | (38,18) | (38,0) | (3,1) |
|---|---|---|---|---|---|
| `1.255` | 1.25 (update: 1.26) | 1.2550 | 1.254999995231628400 / **1.254999995231628544** | 1 | 1.3 |
| `1.005` | 1.00 (update: 1.01) | 1.0050 | 1.004999995231628400 / **1.004999995231628416** | 1 | 1.0 |
| `2.675` | 2.67 (update: 2.68) | 2.6750 | 2.674999952316284000 / **2.674999952316284416** | 3 | 2.7 |
| `0.125` | 0.13 | 0.1250 | 0.125000000000000000 | 0 | 0.1 |
| `0.375` | 0.38 | 0.3750 | 0.375000000000000000 | 0 | 0.4 |
| `2.5` | 2.50 | 2.5000 | 2.500000000000000000 | 3 | 2.5 |
| `-1.255` | -1.25 (update: -1.26) | -1.2550 | -1.254999995231628400 / **-1.254999995231628544** | -1 | -1.3 |
| `-0.125` | -0.13 | -0.1250 | -0.125000000000000000 | 0 | -0.1 |
| `-2.5` | -2.50 | -2.5000 | -2.500000000000000000 | -3 | -2.5 |
| `1e-20` | 0.00 | 0.0000 | 0E-18 | 0 | 0.0 |
| `123456789.125` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | 123456792.000000000000000000 / **123456792.000000005278531584** | 123456792 | NULL / **ERR arrow-precision** |
| `-0.0` | 0.00 | 0.0000 | 0E-18 | 0 | 0.0 |
| `0.30000000000000004` | 0.30 | 0.3000 | 0.300000011920928960 | 0 | 0.3 |
| `9.995` | 9.99 | 9.9950 | 9.994999885559082000 / **9.994999885559083008** | 10 | 10.0 |
| `99.95` | 99.95 | NULL / **ERR arrow-precision** | 99.949996948242190000 / **99.949996948242186240** | 100 | 99.9 |
| `1e10` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | 10000000000.000000000000000000 / **9999999999.999999583119736832** | 10000000000 | NULL / **ERR arrow-precision** |
| `2.82879384806159E17` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | 282879379476840448.000000000000000000 / **282879379476840456.813544470764060672** | 282879379476840448 | NULL / **ERR arrow-precision** |
| `1e23` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | 99999997781963080000000 / **99999997781963083612160** | NULL / **ERR arrow-precision** |
| `NaN` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-precision** |
| `Infinity` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-precision** |
| `-Infinity` | NULL / **ERR arrow-precision** | NULL / **ERR arrow-precision** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-overflow** | NULL / **ERR arrow-precision** |

## 6. The card cells, containers, the subscript, TRY_CAST and literals

| ANSI | cell | Spark | RePark |
|---|---|---|---|
| true | `card_cast` | 1.01, 1.01 | 1.00, 1.00 |
| true | `subscript_struct` | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | 7 |
| true | `subscript_array` | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | 7 |
| true | `cast_array` | [Decimal('1.26'), Decimal('1.01')] | [Decimal('1.25'), Decimal('1.00')] |
| true | `cast_struct` | Row(a=Decimal('1.26')) | {'a': Decimal('1.25')} |
| true | `cast_map_value` | {'k': Decimal('1.26')} | {'k': Decimal('1.25')} |
| true | `cast_map_key` | {Decimal('1.26'): 1} | {Decimal('1.25'): 1} |
| true | `try_cast_round` | 1.26 | 1.25 |
| true | `try_cast_overflow` | None | None |
| true | `try_cast_nan` | None | None |
| true | `float_literal` | 1.25 | 1.25 |
| true | `double_literal_d` | 1.26, 1.26 | 1.25, 1.25 |
| true | `card/ins_values_dec_round` | {'1': 'dec:0.10', '2': 'dec:1.26'} | {'1': 'dec:0.10', '2': 'dec:1.25'} |
| true | `card/ins_union_dec_round` | {'1': 'dec:0.10', '2': 'dec:1.26'} | {'1': 'dec:0.10', '2': 'dec:1.26'} |
| true | `card/ins_values_double_lit` | {'1': 'dec:1.26', '2': 'dec:1.01'} | {'1': 'dec:1.25', '2': 'dec:1.00'} |
| true | `card/ins_select_double_lit` | {'1': 'dec:1.26', '2': 'dec:1.01'} | {'1': 'dec:1.25', '2': 'dec:1.00'} |
| false | `card_cast` | 1.01, 1.01 | 1.00, 1.00 |
| false | `subscript_struct` | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | spark_map_subscript |
| false | `subscript_array` | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | 7 |
| false | `cast_array` | [Decimal('1.26'), Decimal('1.01')] | [Decimal('1.25'), Decimal('1.00')] |
| false | `cast_struct` | Row(a=Decimal('1.26')) | {'a': Decimal('1.25')} |
| false | `cast_map_value` | {'k': Decimal('1.26')} | {'k': Decimal('1.25')} |
| false | `cast_map_key` | DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION | DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION |
| false | `try_cast_round` | 1.26 | 1.25 |
| false | `try_cast_overflow` | None | None |
| false | `try_cast_nan` | None | None |
| false | `float_literal` | 1.25 | 1.25 |
| false | `double_literal_d` | 1.26, 1.26 | 1.25, 1.25 |
| false | `card/ins_values_dec_round` | {'1': 'dec:0.10', '2': 'dec:1.26'} | {'1': 'dec:0.10', '2': 'dec:1.25'} |
| false | `card/ins_union_dec_round` | {'1': 'dec:0.10', '2': 'dec:1.26'} | {'1': 'dec:0.10', '2': 'dec:1.26'} |
| false | `card/ins_values_double_lit` | {'1': 'dec:1.26', '2': 'dec:1.01'} | {'1': 'dec:1.25', '2': 'dec:1.00'} |
| false | `card/ins_select_double_lit` | {'1': 'dec:1.26', '2': 'dec:1.01'} | {'1': 'dec:1.25', '2': 'dec:1.00'} |

## 7. Reading the table

- **One mechanism on every door.** Every plan door (the twelve brief doors and `insertInto`) gives the
  same RePark value for the same value and target: Arrow's product rounding (inventory A1). The literal
  doors fold it in DataFusion's `simplify_expressions` (an overflowing literal refuses as
  `Optimizer rule 'simplify_expressions' failed`); the column doors run it in the physical cast. The
  three `INSERT OVERWRITE` doors (`ins_overwrite`, `ins_overwrite_part`, `df_overwrite`) give the same
  values through the batch store cast (inventory D1).
- **The card reproduces.** `INSERT … VALUES (1, 0.1), (2, CAST(1.255 AS DOUBLE))` stores `1.25` where Spark
  stores `1.26`; the `UNION` form stores `1.26` only through its `decimal(30,15)` common type.
  CAST-DOUBLE-TO-DECIMAL-ROUNDING-1's cells reproduce: `CAST(CAST('1.005' AS DOUBLE) AS DECIMAL(5,2))` and
  `CAST(1.005D AS DECIMAL(5,2))` give `1.00` against Spark's `1.01`. **It is the same mechanism** (A1),
  measured as one here.
- **Where the binary product and the text disagree** (`WRONG-VALUE`, 10 per door, source and mode): DOUBLE
  `1.255`, `-1.255`, `1.005`, `9.995` at scale 2; at scale 18 `1.005`, `0.30000000000000004`,
  `123456789.125`, `1e10`, `2.82879384806159E17` (the product `10^18 · v` is past `2^53`, so its low
  digits are binary noise); `1e23` at scale 0, where Spark keeps JDK 17's text `9.999999999999999E22`.
  FLOAT agrees at scale 2 today (the widened `1.2549999952316284` rounds the same both ways) and differs
  at scale 18 (nine values) and on `1e23` at scale 0.
- **The `update` door is the odd one.** Its SET value meets the old DECIMAL value in a common type before
  the store (inventory B5): DOUBLE takes `decimal(30,15)` first, which turns `1.255`, `1.005`, `-1.255` and
  `9.995` at scale 2 EQUAL today by accident; FLOAT takes `decimal(14,7)` first, which makes it WRONG at
  scale 2 where every other door is EQUAL (`1.255` gives `1.26`, Spark `1.25`).
- **Non-finite values:** Spark answers NULL on every door in both modes; RePark refuses on every door.
  These are the `ERR-FOR-NULL` cells, the CAST-INFINITY-TO-DECIMAL-NULL-1 card's family.
- **Overflow:** both engines refuse on every store door in both modes (Spark's store assignment is ANSI
  whatever `spark.sql.ansi.enabled` says) and on the SELECT doors and CTAS with ANSI on. With ANSI off,
  Spark answers NULL on `sel_cast_col`, `sel_cast_lit`, `df_cast` and `ctas` (an explicit CAST in the
  query, not a store) where RePark refuses: the extra `ERR-FOR-NULL` cells of §1 (15 per DOUBLE door, 14
  per FLOAT door). Not this unit's rounding; question Q2 of the step 0 hand-off.
- **Containers, `TRY_CAST` and literals (§6):** array, struct and map casts, `TRY_CAST`, and the `1.255D` /
  `1.255E0` literals all round the binary product (`1.25`, Spark `1.26`); a FLOAT literal agrees.
- **The subscript cells did not reproduce C-044 here:** over a literal map Spark 4.1.2 refuses the
  struct-key and array-key lookups with `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` in both modes, where
  RePark answers `7` (ANSI on; ANSI off the struct form refuses with no class). That is the map-key type
  check (the ANSI-OFF-LEGACY-CAST-1 / MAP-STRUCT-KEY-SUBSCRIPT-1 family), not rounding; the ledger's C-044
  cells use a stored map and are re-measured in the build.
