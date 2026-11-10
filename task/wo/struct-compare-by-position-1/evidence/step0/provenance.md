# Provenance: struct comparison step 0 table

Legs and interpreters

| leg | interpreter | version |
|---|---|---|
| spark (oracle) | `<pyspark-4.1.2-oracle>` (CPython 3.12.3) | PySpark 4.1.2 |
| spark JVM | `<java-zulu-17>` | OpenJDK 17.0.15 (Zulu) |
| spark Iceberg runtime | `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0` | sha256 `d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242` |
| main | `<venv-repark-1.5.6>` (CPython 3.12.3) | `repark` 1.5.6 from PyPI (`importlib.metadata.version` returns `1.5.6`) |

RePark product code: the orchestrator recorded that main at `18e88a19` has the same product code as the 1.5.6 tag `ae60c359`, and that since the tag only one test file and `scripts/check_map_size.py` changed. The 1.5.6 wheel is therefore the branch point's product. No RePark build was made.

Spark session: local[2], driver memory 4g, UI off, session time zone UTC, shuffle partitions 4, Iceberg SparkSessionExtensions, catalog `hd` (hadoop type) under `<warehouse-spark>`. ANSI on unless a cell says off. Case sensitivity is false unless a cell says sensitive; conf is reset after every cell.

RePark session: `ReparkSession` with catalog `hd` (hadoop type) under `<warehouse-repark>`, session time zone UTC.

Source fixtures: the parked `struct_domain_grid.py`, `struct_domain_driver.py` and `struct_domain_cells.jsonl` at `8e929978` were fetched unmodified into `src/` as reference. The generator and driver here are new files; no line of the parked files is executed.
