# Provenance, DOOR-PROTECTION-INVENTORY-1 (measurement only)

Base: `origin/main` at `a4284f4d`; branch `docs/door-protection-inventory-1`. The wheel under test is the
release build of `fdc61f26`; `a4284f4d` differs from it only in documents. Paths are placeholders:
`<scratch>` the session scratch directory, `<warehouse>` a per-leg temporary catalog root,
`<oracle-python>` the PySpark 4.1.2 interpreter, `<ivy-jar>` the Iceberg Spark runtime jar,
`<venv-main-fdc61f26>` the RePark venv, `<main-wheel>` the built wheel, `<jdk-17>` the Zulu 17 JDK.

## Artifacts and hashes

| Leg | Artifact | sha256 | Python | Version |
|---|---|---|---|---|
| `spark` | Spark 4.1.2 with `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, Hadoop catalog | jar `d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242` | 3.12.3 | spark 4.1.2 |
| `native`, `facade` | `repark-1.5.5-cp312-abi3-manylinux_2_39_x86_64.whl` (release build of `fdc61f26`) | wheel `07f94d4dcf9852b7a6f895382efe4fdc3bbb6c1d090c807e15d9191850ead479` | 3.12.3 | repark 1.5.5, pyarrow 26.0.1 |

The first JSONL line of each leg (`"record": "leg"`) records the wheel or jar hash and the versions. The probe,
`evidence/probe_doors.py`, has sha256 `b1e6ec2851f49c4a3f4540b3591bf98d146678c54ce77ea696c76bdbbe3afe4a` at the time of the render. The native and facade legs share one
wheel and one process model; the native leg builds `PyReparkSession.native()` and assigns it to
`repark._ANSI_NATIVE` so `repark.sql()` is the door under test, the facade leg builds `ReparkSession` and calls
`spark.sql()`.

## Commands run (placeholders for paths)

```
<venv-main-fdc61f26>/bin/python probe_doors.py --engine native --wheel <main-wheel> --out raw/native.jsonl
<venv-main-fdc61f26>/bin/python probe_doors.py --engine facade --wheel <main-wheel> --out raw/facade.jsonl
JAVA_HOME=<jdk-17> <oracle-python> probe_doors.py --engine spark --ivy-jar <ivy-jar> --out raw/spark.jsonl
<venv-main-fdc61f26>/bin/python probe_doors.py --render --native raw/native.jsonl \
    --facade raw/facade.jsonl --spark raw/spark.jsonl --output measured.md
```

Each leg opens its own temporary warehouse (`<warehouse>`), registers one memory catalog (native, facade) or one
Hadoop catalog (Spark) named `ice` or `hc`, and runs every cell on fresh tables with a single-statement seed. The
scrub replaces the warehouse root, the temporary directory and the home directory inside the writer, so the
committed JSONL carries no local path.

## What was not done, and deviations from the brief

- **The router was not run.** The Python wheel has no `repark-sql` edge, so no wheel can run it, and the brief asked
  for native cells to be judged against the router's documented behaviour. The router column of
  [measured.md](measured.md) is the Rust pin each row names (each grepped to exist at `a4284f4d`), not a router
  run. Cells L1 to L3 (empty-DML literals) have no router pin and are judged against Spark: no row may change.
  A router leg would need a cargo build of the `repark-sql` test binary and was not attempted.
- **No Trino oracle.** None is available; no Trino claim is made.
- **Ten inventoried protections (X01 to X10 in [inventory.md](inventory.md)) were not measured.** Two need a live
  Postgres server, the rest need fixtures or conf the native door cannot reach, or change error text only.
- **Fixture-bound rows (T1 to T3) have no native cell.** The native door has no `WITH`, `PARTITIONED BY` or
  `TBLPROPERTIES` syntax (`CREATE TABLE ... WITH (...)` and `ALTER TABLE ... SET PROPERTIES` both fail to
  parse), so a merge-on-read, partitioned or keyed table cannot be built through it. A catalog opened by
  `spark.sql.catalog.<name>.type=hadoop` over a warehouse the Spark oracle had written showed no tables to either
  the native door or the facade, so a fixture could not be handed over either. These rows are recorded
  `NOT-REACHABLE` and describe the fixture, not the protection; the facade and Spark cells ran.
- **Cells N1, O1, P1 and T4 are assigned `NOT-REACHABLE` by cell id** in `native_verdict` when the native door
  refuses the statement as unsupported syntax or unsupported statement. Their raw cells show the refusal text.
- **Spark column.** Cells E1, E2, Q1, R2 and T2 are not Spark SQL (or need a property Spark refuses at table
  creation), so the Spark leg records `SKIP` for them. The Spark oracle ran with Iceberg 1.11.0 and the default
  `spark.sql.ansi.enabled`.
- The probe was run without `-I`; the working directory held only the probe's own scratch scripts.
- The native door is one shared process session: tables from earlier cells stay in the catalog, so the "state
  changed" check compares the set of table directories before and after each statement, plus a post statement
  for the schema cell (R2).

Up: [map.md](map.md).
