# Card CSV-JSON-PARTITION-DISCOVERY-1: CSV and JSON reads of a partitioned directory drop the partition columns

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from the Step 0 of
PARQUET-PARTITION-DISCOVERY-1 (PR #1430), which measured these readers as out of its seat. **Severity:** S1 (owner,
2026-11-25). It is the same shape as PARQUET-PARTITION-DISCOVERY-1: a read-then-write copy stores the table without its
partition columns, silently. **Status:** fixed on `fix/csv-json-partition-discovery-1` (2026-11-26), awaiting gates and
review (delta 2 for N3). It follows #1430, whose Parquet seat these readers now share.

## Measured

Measured on live Spark 4.1.2 and on main `514281bd4`. #1430 does not change these cells. The fixtures are a Spark
`partitionBy("p")` write of four rows over `p=1/` and `p=2/`, in both ANSI modes. Each probe is in
[the PARQUET-PARTITION-DISCOVERY-1 evidence](../../../wo/parquet-partition-discovery-1/evidence/summary.md).

| Read | Query | Spark 4.1.2 | Main |
|---|---|---|---|
| `spark.read.option("header", "true").csv(dir)` | schema and collect | `id, s, p int`, 4 rows | **`id, s`**, 4 rows |
| the same | `where("p = 2")` | 2 rows | refuses (`AnalysisException`) |
| the same | read, `write.parquet`, read back | `id, s, p`, 4 rows | **`id, s`**, 4 rows, silently |
| `spark.read.json(dir)` | schema and collect | `id bigint, s, p int`, 4 rows | **`id bigint, s`**, 4 rows |
| the same | `where("p = 2")` | 2 rows | refuses (`AnalysisException`) |
| the same | read, `write.parquet`, read back | `id, s, p`, 4 rows | **`id, s`**, 4 rows, silently |

- **Agreement with Spark:** CSV with a header 5 of 100 cells, CSV without a header 5 of 99, and JSON 5 of 100.
- **What does match:** `count()`, because it does not need the partition column.

## Seat

- **The two readers:**
  - CSV reads through `read_csv_path` and `_apply_reader_schema_semantics`;
  - JSON reads through `context.read_json`.
- **What they share:** neither shares #1430's Parquet seat (`crates/repark-core/src/spark_nullable/partition.rs`).
  ORC and text reads already use `partition_discovery::discover_partitions`.
- **The narrow fix:** each reader discovers partitions the way #1430's Parquet seat does: the hidden-path rule, Spark's
  type inference and its setting, overlap, conflicts, and partition-only file pruning. Unpartitioned reads stay on
  main's path.

## Fixed (2026-11-26)

- **Seat:** the CSV and JSON readers call #1430's discovery, now `spark_nullable/partition.rs::read_discovered`
  parameterised by the file format; when it answers `None`, the read is main's listing read, so an unpartitioned read
  is unchanged.
- **Measured:** [the evidence summary](../../../wo/csv-json-partition-discovery-1/evidence/summary.md) has the step 0
  scorecard, the #1430 corpora replay and the pin counts.

## Still unlike Spark

- **A stray file in a partition:** a file without the reader's suffix (`.csv`, `.json`, or the compressed suffix) is
  not read; Spark reads every non-hidden file. Main's listing suffix is kept, as on a flat directory; a follow-up in
  the shape of FILE-EXTENSION-FILTER-1. An unreadable root file without the suffix (`root.crc`): Spark opens it and
  refuses; main and head skip it and answer.
- **CSV with a user schema** (`spark.read.schema(...).csv(dir)`) and **CSV with `nullValue`:** main's read, so the
  partition columns are missing. The positional text path and the facade's `nullValue` mapping do not yet know the
  partition columns.
- **CSV whose header names the partition column** (`p` in the file and in `p=10/`): main's read, the file's text.
- **Mixed-case directories** (`p=1/`, `P=2/`, case-insensitive): one column, named after the first directory in path
  order (`p`); Spark names it `P`. Shared with the Parquet seat.
- **`where("p = 2")` on a string or date partition:** the pruning cast raises an Arrow cast error in both ANSI modes,
  where Spark raises `CAST_INVALID_INPUT` (ANSI on), answers no rows (ANSI off), or raises
  `DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES` (date). Shared with the Parquet seat.
- **A JSON user schema that names a partition column** (`spark.read.schema("id BIGINT, s STRING, p STRING").json(dir)`):
  main's answer, that column NULL and the other partition columns absent. Spark casts the directory text to the user's
  type (`p=01` as `STRING` is `01`) and raises `INVALID_PARTITION_VALUE` where the cast fails (a timestamp directory
  as `INT`, `abc` as `DOUBLE`). Delta 1 took main's answer: the first round cast the inferred value (`01` became `1`,
  `2` became `2.0`) and refused a `DATE` schema over non-date directories with ANSI off, where Spark and main answer
  NULL. Spark's rule needs the user schema on the discovery seat. Over an unreadable root file, where main refuses,
  head answers with that column NULL, because a user schema opens no root file (`D2/json_usp`, 44 cells).
- **`format("json").load(path, schema=...)`:** the schema reaches the reader as an option, not a user schema, so head
  refuses with main over an unreadable root file where Spark answers.
- **CSV `inferSchema=true` in a case-sensitive session:** partition names compare case-insensitively.
- **Root files beside partitions on Parquet:** `inputFiles` leaves out a file at the root of a partitioned Parquet
  directory (`part-0.parquet` beside `p=1/`), where Spark lists it. CSV and JSON list it since delta 1; Parquet is
  unchanged.
- **Root files beside partitions, schema (CSV-JSON-ROOT-FILE-SCHEMA-1, the follow-up card).** Spark samples the root
  files for the schema; head infers from the partition files, and a CSV header comes from the first file in the local
  listing. Fixtures: the verifier's `hd`, `xf`, `tc` beside `p=1/`, `p=2/`.
  - **N1, CSV header from a root file** (`aaa-root.csv` with header `X,Y`): Spark `id, s, p`; main `X, Y` plus the root
    row; head `X, Y, p`.
  - **N2, JSON fields and types from root files:** a root `{"id":9,"zzz":1}`: Spark `id, s, zzz, p`; main `id, zzz, s`
    plus the root row; head `id, s, p`. A root `{"id":"x"}`: Spark `id string`; main refuses (schema merge); head
    `id bigint`. A malformed root file: Spark adds `_corrupt_record`; main refuses; head `id, s, p`.
  - **The header scan opens a file Spark never reads:** an unreadable `.x=1/root.csv` (hidden in Spark), or an
    unreadable root file smaller than the partition files that the local listing returns first. Spark answers; main
    and head refuse. Which file comes first is the directory's order.
  - **`inferSchema` CSV over a 0-byte unreadable root file:** Spark answers (no split, never opened); main and head
    refuse in the UTF-8 schema pass. So do an unreadable hidden root file (`_x.csv`, `.x.csv`) and a dangling root
    symlink, where Spark answers.
  - **JSON `multiLine` with no schema over a 0-byte unreadable root file:** Spark refuses (its whole-file reader opens
    0-byte files); main answers without `p`; head answers with `p`.
- **Unreadable root files Spark opens, beyond delta 2's rule** (N3's waiver, owner 2026-11-26):
  - **Files above 4 MB, CSV without `inferSchema`:** Spark opens the first split of its packing, ordered by
    `min(size, maxSplitBytes)` with `maxSplitBytes = max(4 MB, totalBytes / defaultParallelism)`, so the answer
    depends on the cluster's cores. A 5 MB unreadable root beside a 6 MB partition file: Spark answers at `local[2]`,
    refuses at `local[16]`; main refuses; head answers. A 6 MB root beside a 5 MB file: Spark refuses at both; main
    refuses; head answers. Cells: `D2B/cores=16/csvh/r5_p6`, `D2B/cores={2,16}/csvh/r6_p5`. When the unreadable root
    file is the largest file and above 4 MB, beside small partition files, it is the first split at every core count:
    Spark refuses, main refuses, head answers (the delta 2 re-check's `e4m1` cells).
  - **Two equal largest root files, one unreadable, CSV without `inferSchema`:** Spark opens the one its file-system
    listing returns first (`root-b` before `root-a` on ext4, whatever the creation order), so it refuses or answers by
    directory order; main refuses; head answers. Cells:
    `D2T/cores={2,16}/ansi={true,false}/{csvh,csvn}/t_c{f,s}_nl/{read,count,collect}` (Spark refuses) beside the
    `t_c{f,s}_nf` cells (Spark answers, as head).
- **Refused as on the Parquet seat:** `basePath` and `recursiveFileLookup`. **Main's unpartitioned path:** several
  paths in one `load()`, and globs.

## Cost note

An unpartitioned CSV or JSON read lists its prefix twice: the discovery lists it, answers `None`, and main's read lists
it again. The verifier measured +19 to 37 % on a 5000-file flat local directory (under 1.5x); an S3 prefix pays two
LIST walks (not measured). Not fixed: hand the listing to the fallback, or stop the walk early when no entry has `=`.

## Delta 1 (2026-11-26)

- **Root files beside partitions:** CSV and JSON `inputFiles` list a visible file beside the partition directories
  (`part-0.csv` beside `p=1/`, `_c0=1/` or `_x=1/`) as Spark does, and still drop its rows.
- **JSON user schema naming a partition column:** main's answer, above.

## Delta 2 (2026-11-26)

- **N3, an unreadable root file:** Spark raises `FAILED_READ_FILE` for a root file it opens at schema inference.
  Discovery now reads one byte of each such file and refuses with main's object-store error:
  - JSON without a user schema, and CSV with `inferSchema`: every non-empty root file;
  - CSV without `inferSchema`: the largest non-empty file, when every file is at most 4 MB and it is a root file; at a
    tie between root files only when each tied file is unreadable;
  - a user schema: none. The facade flags a JSON user schema (`user_schema`); a CSV user schema stays on main's read.
- **Measured:** step 0 delta 2 (`D2`, `D2B`, `D2T`), 1164 cells: TARGET 212 of 215 equal Spark (the 3 left are the waived `D2B` cells), PROTECTED 643 of 643, regressed 0; step 0 of rounds 1 and delta 1: TARGET 490 of 490, PROTECTED 348 of 348; the verifier suites, `lo.py` and #1430's corpora regressed 0; Parquet `EXPLAIN` identical to main on 78 of 78 plans.
- **Cost:** JSON read with no user schema over 200 non-empty root files (`lo_many`): about +1.2 ms per read (delta 1 head 4.9 ms → 6.2 ms, pooled over six rounds on two builds; 1.19× to 1.32×), recorded as an absolute line by the owner on 2026-11-26; CSV 1.09×; `count()` equal.
- **C5 (listed, not changed):** `EXPLAIN` over root files shows a `ListedOnlyExec` with a `RoundRobinBatch` repartition
  over an empty node and lists every root file. Correct, if noisy.

Up: [map.md](map.md).
