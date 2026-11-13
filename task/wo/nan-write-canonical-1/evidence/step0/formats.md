# NAN-WRITE-CANONICAL-1 step 0 addendum — data-file formats, nested leaves, map keys, the rewrite repair

Added after the inventory critic (2026-11-13). Main `1d9d2808` against Spark 4.1.2 / Iceberg 1.11.0, same builds and
locks as [map.md](map.md). `format_cells_probe.py` writes the step 0 `ROWS` into tables with `write.format.default`
set to each format: `u_` (INSERT, unpartitioned), `p_` (INSERT, `PARTITIONED BY (d, f)`), `c_` (partitioned CTAS),
`n_` (INSERT into `STRUCT<a DOUBLE, b FLOAT>`, `ARRAY<DOUBLE>`, `MAP<STRING, DOUBLE>`), then rewrites `u_` with
`CALL … rewrite_data_files(… options => map('rewrite-all', 'true'))` on a copy written by main. Rendered by
`render_formats.py --repark raw/formats-repark-1d9d2808.jsonl --spark raw/formats-spark-4.1.2.jsonl` (map-key rows
dropped here; they are in §3). "NaN data bits" is every NaN bit pattern in the data files, nested leaves included.

## 1. Every format

| table | main NaN data bits | Spark NaN data bits | main NaN partition values (files) | Spark NaN partition values (files) | non-NaN bits equal |
|---|---|---|---|---|---|
| c_avro | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 (4) | 7fc00000 7ff8000000000000 (1) | yes |
| c_orc | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7ff8000000000000 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 (4) | 7fc00000 7ff8000000000000 (1) | yes |
| c_parquet | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7ff8000000000000 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 (4) | 7fc00000 7ff8000000000000 (1) | yes |
| n_avro | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | - (0) | - (0) | yes |
| n_orc | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7ff8000000000000 | - (0) | - (0) | yes |
| n_parquet | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7ff8000000000000 | - (0) | - (0) | yes |
| p_avro | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7ff8000000000000 (3) | 7fc00000 7ff8000000000000 (1) | yes |
| p_orc | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 (3) | 7fc00000 7ff8000000000000 (1) | yes |
| p_parquet | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 (3) | 7fc00000 7ff8000000000000 (1) | yes |
| u_avro | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | - (0) | - (0) | yes |
| u_avro after rewrite | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | - (0) | - (0) | yes |
| u_avro before rewrite | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | - (0) | - (0) | yes |
| u_orc | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7ff8000000000000 | - (0) | - (0) | yes |
| u_orc after rewrite | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7ff8000000000000 | - (0) | - (0) | yes |
| u_orc before rewrite | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | - (0) | - (0) | yes |
| u_parquet | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7ff8000000000000 | - (0) | - (0) | yes |
| u_parquet after rewrite | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7ff8000000000000 | - (0) | - (0) | yes |
| u_parquet before rewrite | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | - (0) | - (0) | yes |

- **Spark canonicalises Parquet and ORC data files and keeps Avro's raw**, nested leaves included; it canonicalises
  the NaN partition value on all three formats and writes one NaN-partition file.
- **Main keeps the raw bits on every format.** Its Avro data files therefore already equal Spark's. Its partition
  values are canonical on the INSERT door (the JSON transport) and raw on CTAS; its NaN partition spans 3–4 files.
- **The rewrite cell (rule 7).** Each engine runs `rewrite_data_files` (`rewrite-all`) on its own copy of a
  main-written `u_` table (Spark reads the copy in place through its Hadoop catalog; RePark adopts its copy with
  `register_table`). Spark's rewrite repairs Parquet and ORC (one compacted file, canonical) and keeps Avro raw;
  main's rewrite keeps the raw bits on all three. The head's rewrite is §4.

## 2. Nested leaves on ORC and Avro

`n_orc`, `n_avro` above: Spark's ORC leaves are canonical, its Avro leaves raw; main's are raw on both.

## 3. Float map keys (`MAP<DOUBLE, INT>`)

From `raw/keys-*.jsonl` (`--tables k1,k2`): `k1` stores `map(<sign-set payload NaN>, 1)`, `k2` stores
`map(<payload NaN>, 1, <sign-set payload NaN>, 2)`.

| table | main key bits | Spark key bits |
|---|---|---|
| `k1_parquet`, `k1_orc` | `fff8deadbeef0001` | `7ff8000000000000` |
| `k1_avro` | `fff8deadbeef0001` | `fff8deadbeef0001` |
| `k2_parquet` | `7ff8deadbeef0001`, `fff8deadbeef0001` (two keys) | refused: `[DUPLICATED_MAP_KEY] Duplicate map key NaN` |
| `k2_orc`, `k2_avro` | one key `7ff8deadbeef0001` (value 2) | refused, as above |

Spark's map builder treats every NaN key as one key and refuses the duplicate; main accepts it (and its ORC and
Avro writers keep one entry). Rewriting keys would give `k2_parquet` two identical keys, so the seat leaves map keys
alone (orchestrator ruling, item 6); `k1` on Parquet and ORC stays unlike Spark.

## 4. The head, `e100717f` (fork `d6592644`)

Same probe and locks on the head's release build (§5 of [table.md](table.md)); `formats`, then `rewrite` on copies
written by main's build (`raw/formats-repark-e100717f.jsonl`), and `--tables k1,k2` (`raw/keys-repark-e100717f.jsonl`).
Rendered by `render_formats.py --repark raw/formats-repark-e100717f.jsonl --spark raw/formats-spark-4.1.2.jsonl
--label head`, map-key rows dropped.

| table | head NaN data bits | Spark NaN data bits | head NaN partition values (files) | Spark NaN partition values (files) | non-NaN bits equal |
|---|---|---|---|---|---|
| c_avro | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7ff8000000000000 (1) | 7fc00000 7ff8000000000000 (1) | yes |
| c_orc | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 (1) | 7fc00000 7ff8000000000000 (1) | yes |
| c_parquet | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 (1) | 7fc00000 7ff8000000000000 (1) | yes |
| n_avro | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | - (0) | - (0) | yes |
| n_orc | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 | - (0) | - (0) | yes |
| n_parquet | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 | - (0) | - (0) | yes |
| p_avro | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7ff8000000000000 (1) | 7fc00000 7ff8000000000000 (1) | yes |
| p_orc | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 (1) | 7fc00000 7ff8000000000000 (1) | yes |
| p_parquet | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 (1) | 7fc00000 7ff8000000000000 (1) | yes |
| u_avro | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | - (0) | - (0) | yes |
| u_avro after rewrite | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | - (0) | - (0) | yes |
| u_avro before rewrite | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | - (0) | - (0) | yes |
| u_orc | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 | - (0) | - (0) | yes |
| u_orc after rewrite | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 | - (0) | - (0) | yes |
| u_orc before rewrite | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | - (0) | - (0) | yes |
| u_parquet | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 | - (0) | - (0) | yes |
| u_parquet after rewrite | 7fc00000 7ff8000000000000 | 7fc00000 7ff8000000000000 | - (0) | - (0) | yes |
| u_parquet before rewrite | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | 7fc00000 7fc00123 7ff8000000000000 7ff8deadbeef0001 ffc00000 ffc00123 fff8000000000000 fff8deadbeef0001 | - (0) | - (0) | yes |

- **Every cell equals Spark:** Parquet and ORC data files and nested leaves canonical, Avro's raw, one canonical NaN
  partition value in one file on all three formats, CTAS and INSERT alike.
- **The rewrite repairs Parquet and ORC and keeps Avro raw,** as Spark's does: the head's `rewrite_data_files` of a
  main-written `u_parquet` and `u_orc` stores only the canonical NaN; `u_avro` keeps its bits.
- **Non-NaN bits** equal main's on every cell (rendered against `raw/formats-repark-1d9d2808.jsonl`).
- **Map keys** (§3): unchanged from main, as ruled. `k1_parquet` and `k1_orc` keep `fff8deadbeef0001` where Spark
  stores `7ff8000000000000`; `k1_avro` equals Spark; `k2` stores what main stores (two keys on Parquet, one on ORC and Avro), where Spark refuses.
