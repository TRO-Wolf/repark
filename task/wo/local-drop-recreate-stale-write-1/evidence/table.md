# LOCAL-DROP-RECREATE-STALE-WRITE-1 before and after table

One row per matrix cell, one session and one fresh Hadoop warehouse per cell, metadata cache on
(the default). `before` is the build of main `ae247767` (fork `66901f02`); `after` is this
unit's build (fork `79b6f19b`). Rows are `(id, n)`, sorted.

- **session** is the session's catalog read.
- **path read** is `read.format("iceberg").load(<table dir>)`, which loads through
  `StaticTable::from_metadata_file` with its own FileIO and never consults the session cache.
  The proof it bypasses the cache is the before column of the wrong-read rows: in the same
  session, the catalog read returns the dropped table's row while the path read returns the
  truth.
- **current file uuid** names the file `version-hint.text` points at and whether its
  `table-uuid` is the recreated table's (read from the recreate's `v1.metadata.json`) or the
  dropped table's.
- The purge row's before session read failed with `Failed to load manifest list in cache …
  Failed to read file <warehouse>/ns/t/metadata/snap-*.avro` (the stale metadata names a purged
  manifest list).

| kind | sequence | truth | before: session | before: path read | before: current file uuid | after: session | after: path read | after: current file uuid |
|---|---|---|---|---|---|---|---|---|
| wrong-read | `C,I7,R,D,C,R` | (2,1) (3,1) (4,1) | (7,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated |
| wrong-read | `C,I7,R,D,C,R,S` | (2,1) (3,1) (4,1) | (7,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated |
| wrong-read | `C,I7,R,D,C,RC` | (empty) | (7,1) | (empty) | v2.metadata.json recreated | (empty) | (empty) | v2.metadata.json recreated |
| wrong-read | `C,I7,R,D,CT,R` | (2,1) (3,1) (4,1) | (7,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated |
| wrong-write | `C,I7,R,D,C,R,I8` | (2,1) (3,1) (4,1) (8,1) | (7,1) (8,1) | (7,1) (8,1) | v3.metadata.json DROPPED | (2,1) (3,1) (4,1) (8,1) | (2,1) (3,1) (4,1) (8,1) | v3.metadata.json recreated |
| wrong-write | `C,I7,R,D,C,R,M` | (2,1) (3,1) (4,1) (9,1) | (7,1) (9,1) | (7,1) (9,1) | v3.metadata.json DROPPED | (2,1) (3,1) (4,1) (9,1) | (2,1) (3,1) (4,1) (9,1) | v3.metadata.json recreated |
| wrong-write | `C,I7,R,D,C,R,M3` | (2,1) (3,5) (4,1) | (3,5) (7,1) | (3,5) (7,1) | v3.metadata.json DROPPED | (2,1) (3,5) (4,1) | (2,1) (3,5) (4,1) | v3.metadata.json recreated |
| wrong-write | `C,I7,R,D,C,R,U` | (2,5) (3,5) (4,5) | (7,5) | (7,5) | v3.metadata.json DROPPED | (2,5) (3,5) (4,5) | (2,5) (3,5) (4,5) | v3.metadata.json recreated |
| wrong-write | `C,I7,R,D,C,R,DEL` | (2,1) (3,1) (4,1) | (empty) | (empty) | v3.metadata.json DROPPED | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | v3.metadata.json recreated |
| wrong-write | `C,I7,R,D,C,R,A,I8Z` | (2,1) (3,1) (4,1) (8,1) | (7,1) (8,1) | (7,1) (8,1) | v4.metadata.json DROPPED | (2,1) (3,1) (4,1) (8,1) | (2,1) (3,1) (4,1) (8,1) | v4.metadata.json recreated |
| replace-only | `R1,R2,R3,D,R4,R5` | (5,1) | (2,1) | (5,1) | v2.metadata.json recreated | (5,1) | (5,1) | v2.metadata.json recreated |
| purge | `C,I7,R,DP,C,R` | (2,1) (3,1) (4,1) | ERR PySparkException: Unexpected | (2,1) (3,1) (4,1) | v2.metadata.json recreated | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated |
| control | `C,I7,D,C,R` | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated |
| control | `C,I7,I7,D,C,R` | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated |
| control | `C,I7,S,D,C,R` | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated |
| control | `C,R,D,C,R` | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated | (2,1) (3,1) (4,1) | (2,1) (3,1) (4,1) | v2.metadata.json recreated |
| control | `C,I7,R,D,C,I8` | (8,1) | (8,1) | (8,1) | v2.metadata.json recreated | (8,1) | (8,1) | v2.metadata.json recreated |
| control | `C,I7,R,D,C,M` | (9,1) | (9,1) | (9,1) | v2.metadata.json recreated | (9,1) | (9,1) | v2.metadata.json recreated |
| control | `R1,R2,D,R3,R4` | (4,1) | (4,1) | (4,1) | v2.metadata.json recreated | (4,1) | (4,1) | v2.metadata.json recreated |

| wrong-write row | truth | Spark reads disk, before | Spark reads disk, after |
|---|---|---|---|
| `C,I7,R,D,C,R,I8` | (2,1) (3,1) (4,1) (8,1) | (7,1) (8,1) | (2,1) (3,1) (4,1) (8,1) |
| `C,I7,R,D,C,R,M` | (2,1) (3,1) (4,1) (9,1) | (7,1) (9,1) | (2,1) (3,1) (4,1) (9,1) |
| `C,I7,R,D,C,R,M3` | (2,1) (3,5) (4,1) | (3,5) (7,1) | (2,1) (3,5) (4,1) |
| `C,I7,R,D,C,R,U` | (2,5) (3,5) (4,5) | (7,5) | (2,5) (3,5) (4,5) |
| `C,I7,R,D,C,R,DEL` | (2,1) (3,1) (4,1) | (empty) | (2,1) (3,1) (4,1) |
| `C,I7,R,D,C,R,A,I8Z` | (2,1) (3,1) (4,1) (8,1) | (7,1) (8,1) | (2,1) (3,1) (4,1) (8,1) |

The second table is the Spark 4.1.2 + Iceberg 1.11.0 leg (`spark_read.py`): Spark loads each
wrong-write row's table directory by path after the RePark session stopped.
