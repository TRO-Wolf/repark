# Card FILE-EXTENSION-FILTER-1: Parquet directory reads skip files whose names do not end in `.parquet`

**Date:** 2026-11-25. **Filed by:** the orchestrator (`claude-opus-5-5`), from finding C2 of the first verifier of
PARQUET-PARTITION-DISCOVERY-1 (PR #1430), re-measured here. **Severity:** S1 (owner, 2026-11-25): rows go missing
silently. **Status:** **built** on branch `fix/file-extension-filter-1` off main `30288badc`, not merged; evidence in
[task/wo/file-extension-filter-1/evidence/](../../../wo/file-extension-filter-1/evidence/).

**Built on main, not after #1430.** The card said to merge after PR #1430 (PARQUET-PARTITION-DISCOVERY-1), which
changes the same listing on its own branch; #1430 is parked for the owner, and the orchestrator ruled to build on main
now and fix main's seats only. Nothing was read or copied from #1430's branch. If #1430 is unparked, it takes main,
and its re-check covers this rule (its partition seat, `partition.rs:755-757` on that branch, filters by the same
suffix).

## Measured

Measured on live Spark 4.1.2 and on a build of main `14d2404a6` (#1429's tested head), in both ANSI modes, with the
same answers in each. Every file is a valid Parquet file with one `id bigint` column.

| Directory | Spark 4.1.2 | Main |
|---|---|---|
| `flat/a.parquet` and `flat/b` | ids `10, 11`; `count()` 2 | **id `10`; `count()` 1** |
| `p=1/part-00000`, `p=2/part-00000.parquet`, `p=3/part-00000.snappy.parquet`, `p=4/data.bin` | ids `1, 2, 3, 4`, with `p int` | **ids `2, 3`**, without `p` |

Spark lists every file except those whose names start with `_` or `.`, whatever the extension. The verifier also
measured an upper-case `.PARQUET` skipped on main where Spark reads it. The missing `p` column is
PARQUET-PARTITION-DISCOVERY-1 (#1430), which keeps the same extension filter.

**Step 0 (2026-11-25):** 48 Parquet cells on up to six doors (214 door cells per mode), 28 neighbour cells (CSV,
JSON, text, ORC), writes in `append` and `overwrite`, Spark-written directories read by RePark, and an Iceberg
control, on Spark 4.1.2 and main's package, both ANSI modes ([domain-table.md](../../../wo/file-extension-filter-1/evidence/domain-table.md)). Spark's rule,
measured: a name starting with `_` that holds no `=`, a name starting with `.`, or a name ending in `._COPYING_` is
skipped on every level below the read root, and a single file path is judged by its own name (`_b.parquet` read
directly is refused with `UNABLE_TO_INFER_SCHEMA`); a root directory named `_root/` or `.root/` is read. Main also read
`_b.parquet` and `.b.parquet` beside `a.parquet`, where Spark skips them.

## Fixed

On `spark.read.parquet`, `format("parquet").load` and the native `read_parquet`, both ANSI modes: every name with
another or no suffix (`b`, `b.PARQUET`, `b.parq`, `b.bin`, `b.x.y.z`), the card's partition layout (ids `1, 2, 3,
4`), a `p=1/b` beside markers, the `*` glob, a single file path with any suffix, `_root/` and `.root/` roots, and an
append beside `b` now answer Spark's rows, `count()` and `inputFiles()`. `_b.parquet` and `.b.parquet` beside
`a.parquet` are skipped, and `_b.parquet`, `.b.parquet` or the `_*` glob read alone are refused, as on Spark. A
directory that holds a non-Parquet file with an ordinary name (`README.txt`) is refused, as on Spark, where main read
the other files. Every marker Spark's writers leave (`_SUCCESS`, `_committed_*`, `_started_*`, `.crc`,
`._COPYING_`) is still skipped; no cell that equalled Spark on main moved.

**Delta 1 (2026-11-25), the write paths.** The first verifier found an S3 `append` refused when the prefix held
`_b.parquet` or `.c.parquet`: append validation read every existing `.parquet` key through the new listing, which
refuses a hidden single key. Spark never reads existing files on append. S3 and local append validation now skip
the names Spark's listing skips, so an append beside `_b.parquet`, `.c.parquet` (also when not Parquet or of another
schema), `_temporary/…` or only hidden files succeeds, as on Spark; a visible file of another schema still refuses,
as on main. Overwrite (S3 deletes every key under the prefix, local replaces the directory), ignore,
error-if-exists and rollback are unchanged and equal Spark. Pinned on an in-memory S3 bucket
(`hidden_files_tests.rs`) and measured locally on Spark, main and head
([write-paths.md](../../../wo/file-extension-filter-1/evidence/write-paths.md)).

**Integration with #1430 (2026-11-25).** PARQUET-PARTITION-DISCOVERY-1 merged into main first (`9d566e21`) and
was merged into this branch. Its discovery listing kept a `.parquet` filter and its own hidden-name copy; it now lists
every name and skips by `hidden_files::spark_hidden_below`, the one home of the rule, and its unpartitioned, glob and
single-file fallback reads through the Spark listing. Re-measured on live Spark 4.1.2 against #1430's head package
(`a2b2902f`): this unit's step 0 fixes 114 cells and moves none off Spark; #1430's `probe_d2` (522 cells),
`repro_d1_findings` (33) and attack (898) regress none. The #1430 cells that change: `L21_part_sub_txt` (4) and
`B_two_stray_mid` (4) now refuse as Spark does (a `.txt` or suffix-less file inside the partition tree is listed);
`B_stray_leaf_txt` and `B_two_stray_leaf` (8) answered rows where Spark fails reading the stray file and now refuse
too, with RePark's Parquet error instead of `FAILED_READ_FILE.NO_HINT`; `path_file1` (2), a suffix-less single file
under partition directories, refused and now reads its rows without the partition column Spark adds. The card's
partition layout now answers Spark's ids with `p int`.

## Still unlike Spark

Measured on head, both ANSI modes:

- **Partition columns** now come from #1430's discovery and equal Spark on the step-0 cells, including the
  `CONFLICTING_PARTITION_COLUMN_NAMES` refusal for `_p=2/` beside `p=1/`.
- **`dir/p=*`:** Spark reads ids `1, 2, 3, 4`; RePark refuses with `No files found` (DataFusion's glob matches the file
  name, not the `key=value` directory).
- **`inputFiles()` omits `_metadata`, `_common_metadata` and 0-byte files**, which Spark lists but never scans; rows
  and `count()` are equal.
- **The refusal's class:** where both refuse, Spark answers `UNABLE_TO_INFER_SCHEMA` (hidden single file, `_*`,
  nested plain directories) or a read failure (`README.txt`); RePark answers `No files found` or its Parquet error.
- **A list of paths** (`spark.read.parquet(a, b)`, `format("parquet").load([a, b])`): Spark reads; RePark refuses
  (`TypeError` on the reader, `No files found` on `load`).
- **Reader options** `pathGlobFilter`, `recursiveFileLookup`, `ignoreMissingFiles`, `mergeSchema` and
  `ignoreCorruptFiles`: Spark answers; RePark refuses each loudly, as on main.
- **Unwired doors:** `spark.read.load(p)` with the default format, SQL ``SELECT * FROM parquet.`p` `` and `CREATE
  TEMPORARY VIEW … USING parquet OPTIONS (path …)` refuse on every cell, as on main.
- **Append schema checks:** RePark refuses an append beside a visible file of another schema (Spark writes it), and
  an append into a directory whose `_temporary/` holds a non-Parquet `part-*.parquet` now succeeds where Spark's
  committer fails and removes the directory (main refused the append).
- **Neighbours, not fixed (a different seat, `crates/repark-core/src/read_options.rs:56,86`):** CSV and JSON reads
  drop `b`, `b.CSV`/`b.JSON` and `b.bin` (rows lost silently) and read `_b.csv`, `.b.csv`, `_b.json` and `.b.json`,
  which Spark skips. Text and ORC (their own listing, `text_glob.rs:5`) read `b.txt._COPYING_` and `b.orc._COPYING_`,
  which Spark skips, and answer an empty `inputFiles()`. Iceberg reads equal Spark.

## Seat

- `crates/repark-core/src/spark_nullable.rs:48,60` (main before #1430): `ParquetReadOptions::default()`, whose file
  extension is `.parquet`, so the listing drops every other name. Fixed through
  `crates/repark-core/src/spark_nullable/hidden_files.rs`; the full inventory heads the evidence table.
- PARQUET-PARTITION-DISCOVERY-1's partition listing filtered by the same suffix (`partition.rs:513` on main
  `9d566e21`), with its own hidden-name rule. Since the integration it lists every name and skips by
  `hidden_files::spark_hidden_below`, the one home of the rule.

Up: [map.md](map.md).
