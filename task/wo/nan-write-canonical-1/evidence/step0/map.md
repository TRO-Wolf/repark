# task/wo/nan-write-canonical-1/evidence/step0/ — the door inventory and stored NaN bits per door

Step 0 of NAN-WRITE-CANONICAL-1, 2026-11-13, model `claude-opus-5-5`. No product code changed.

**Provenance.** Main `1d9d2808` (fork pin `92242cb4`), built with `maturin build --profile release` (module sha256
`9c420a37a8881dee1d2f6b3f570e3c3ca48860ffb704b1c44b26e6904fc15b94`). Spark 4.1.2 `local[2]`, JDK 17, Iceberg Spark
runtime 1.11.0 with the Iceberg SQL extensions, default ANSI mode, one JVM at a time under the JVM lock. Both engines
write to a local Hadoop catalog; the native door writes to a native session's memory catalog on local disk.
After the runs the scripts changed only by `ruff format`, split string literals and two equivalent spellings
(`next(iter(row))`, `next(...)` for a one-element list index); `render_step0.py` re-renders `table.md` §1 unchanged.
For the addendum, `leaf_bits` also records a map's keys (as `{"keys", "values"}`); the doors legs predate that
change and record map values only.

**Method.** `write_doors_probe.py` builds one source view (`ROWS`: eight NaN rows, six controls, the payload rows
from `createDataFrame`), writes it through every door the inventory lists on both engines, unpartitioned and
`PARTITIONED BY (d, f)`, and reads the stored bits from the Parquet files and the partition values, bounds and NaN
counts from the Avro manifests itself (`avro_bits.py`). The rewrite leg updates one row of a main-written table with
each engine. The legs ran as `--engine spark --legs doors,nested,plain`, `--engine repark` (all legs), and
`--legs rewrite` on separate copies written by main (RePark adopts them with `register_table`).

**Result.**

1. Spark stores the canonical NaN (`7ff8000000000000` / `7fc00000`) for every NaN row on every door, in every nested
   leaf, as its single NaN partition value, in one NaN-partition file; `-0.0` and the other controls keep their bits.
2. Main stores the bits each row arrived with on every Iceberg door (Spark door, DataFrame doors, native door), in
   nested leaves too; the NaN partition value is the raw bits on the fan-out and staged doors, and the NaN partition
   is split across files. `= CAST('NaN' …)` misses the non-canonical rows on RePark's read.
3. Spark's copy-on-write UPDATE canonicalises the NaN of every row it rewrites, changed or not; main's keeps the bits.
4. The addendum (`formats.md`): Spark canonicalises Parquet and ORC data files and keeps Avro's raw bits; it canonicalises partition values on all three; it refuses a map with two NaN keys. The seat, corrected after the inventory critic, is in `inventory.md`.

- [inventory.md](inventory.md) — every write door with its entry and writer (file:line), how the list was searched, the partition-value sites, and the intended seat.
- [formats.md](formats.md) — the critic addendum: Parquet, ORC and Avro data files, nested leaves on each, float map keys, and `rewrite_data_files` per format.
- [table.md](table.md) — the stored bits per door, partitioned and not, against Spark; the nested, rewrite and plain-Parquet cells.
- [write_doors_probe.py](write_doors_probe.py) — the probe: `--engine repark|spark --warehouse <new dir> --out <leg.jsonl> [--legs …] [--only …] [--source …]`.
- [avro_bits.py](avro_bits.py) — a minimal Avro container reader that keeps float and double bits, and the current-snapshot manifest walk.
- [format_cells_probe.py](format_cells_probe.py) — the addendum probe: `--engine repark|spark --warehouse <dir> --out <leg.jsonl> [--legs formats|rewrite] [--source <dir>] [--tables k1,k2]`.
- [render_formats.py](render_formats.py) — renders two addendum legs as the table in `formats.md` §1.
- [render_step0.py](render_step0.py) — renders two legs as the table in `table.md` §1.
- [raw/](raw/map.md) — the legs as recorded.

Up: [../map.md](../map.md).
