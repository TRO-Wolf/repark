# Provenance — Step 0.3 probe tables (measurement only)

Base: `origin/main` at `fdc61f26`; branch `docs/step-0-3-probe-tables`. Paths below are placeholders:
`<scratch>` is the session scratch directory, `<warehouse>` a per-leg catalog root, `<oracle-python>` the
PySpark 4.1.2 interpreter, `<ivy-jar>` the Iceberg Spark runtime jar, `<venv-pypi-1-5-5>` and
`<venv-main-fdc61f26>` the two RePark venvs, `<main-wheel-dir>` the built-wheel directory, `<jdk-17>` the
Zulu 17 JDK.

## Artifacts and their hashes

| Leg | Artifact | sha256 (wheel, or jar) | Venv or interpreter | Python | pyarrow | Version string |
|---|---|---|---|---|---|---|
| `spark` | Spark 4.1.2 + Iceberg `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` | jar `d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242` | `<oracle-python>` (JDK 17.0.15) | 3.12.3 | 25.0.1 | spark 4.1.2 |
| `repark-main-fdc61f26` | `repark-1.5.5-cp312-abi3-manylinux_2_39_x86_64.whl` (release build of `fdc61f26`) | `07f94d4dcf9852b7a6f895382efe4fdc3bbb6c1d090c807e15d9191850ead479` | `<venv-main-fdc61f26>` | 3.12.3 | 26.0.1 | 1.5.5 (main has not bumped its version) |
| `repark-pypi-1.5.5` | `repark-1.5.5-cp312-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl` (PyPI) | `eb6944ac741ef4d6b3ec648202d2c93411c7546c926d5f5bf10f727cbc37d615` | `<venv-pypi-1-5-5>` | 3.12.3 | 26.0.1 | 1.5.5 |

The two RePark legs report the same version string, so they are told apart by wheel sha256 and venv
path. The first JSONL line of each RePark leg (`"record": "leg"`) records `wheel_file`, `wheel_sha256`,
`repark_file`, `venv` and `native_config`. The first line of `raw/spark.jsonl` records the jar and the JDK.

- Cargo.lock at `fdc61f26` (sha256): `d3defbf391c1d195d52eb150adb6dca14603a2a4da386d3d967a85ae019ca590`
- Build profile: release (`Finished \`release\` profile [optimized]` in the build log, 12 m 08 s).
- `uv` 0.11.16. The PyPI leg was installed with `uv pip install --python <venv>/bin/python repark==1.5.5`,
  then reinstalled from the downloaded file so the installed wheel is the hashed one.

## Commands run (placeholders for paths)

```
uv venv --python 3.12 <scratch>/venv-pypi-1-5-5
uv pip install --python <venv-pypi-1-5-5>/bin/python repark==1.5.5
uv run --no-project --with pip python -m pip download repark==1.5.5 --no-deps \
    --only-binary :all: -d <scratch>/wheel-pypi-1-5-5
uv pip install --python <venv-pypi-1-5-5>/bin/python --reinstall --no-deps <pypi-wheel>
uv venv --python 3.12 <scratch>/venv-main-fdc61f26
uv pip install --python <venv-main-fdc61f26>/bin/python <main-wheel-dir>/<main-wheel>
JAVA_HOME=<jdk-17> <oracle-python> -I probe.py --engine spark --label spark \
    --warehouse <warehouse> --out raw/spark.jsonl --matrix all --ivy-jar <ivy-jar>
<venv-pypi-1-5-5>/bin/python -I probe.py --engine repark --label repark-pypi-1.5.5 \
    --warehouse <warehouse> --out raw/repark-pypi-1-5-5.jsonl --matrix all --wheel <pypi-wheel>
<venv-main-fdc61f26>/bin/python -I probe.py --engine repark --label repark-main-fdc61f26 \
    --warehouse <warehouse> --out raw/repark-main-fdc61f26.jsonl --matrix all --wheel <main-wheel>
python3 -I probe.py --render --spark raw/spark.jsonl --main raw/repark-main-fdc61f26.jsonl \
    --pypi raw/repark-pypi-1-5-5.jsonl --tables-dir . --detail <scratch>/detail.txt
uvx ruff@0.15.22 format probe.py && uvx ruff@0.15.22 check probe.py
```

Cell counts per leg: Spark 99 (answered 73, oracle-missing 18, refused 8); each RePark leg 174 (answered
118, refused 2, setup-refused 54). Each leg ran in a fresh warehouse directory, from a scratch working
directory. The committed raw files come from the last full run of each leg, after the final edit to
`probe.py`.

## Native door: what was tried for v2-mor, v3 and UUID (the `setup` field records this per cell)

The orchestrator's tip asked for `CREATE TABLE … WITH (…)` first, then ALTER. Every spelling below was run
on both wheels. Each refusal is the door's own error text, quoted here in short.

| Route | Spelling | Main | 1.5.5 |
|---|---|---|---|
| WITH, v3 | `CREATE TABLE … WITH (format_version = '3')` | Unsupported SQL statement | Unsupported SQL statement |
| WITH, v3 | `CREATE TABLE … WITH (format_version = 3)` | Unsupported SQL statement | Unsupported SQL statement |
| WITH, mor | `CREATE TABLE … WITH (extra_properties = MAP(ARRAY['write.delete.mode', 'write.update.mode'], ARRAY['merge-on-read', 'merge-on-read']))` | Unsupported SQL statement | Unsupported SQL statement |
| ALTER, v3 | `ALTER TABLE … SET PROPERTIES (format_version = '3')` | ParseException (expected `(`, found PROPERTIES) | ParseException (same) |
| ALTER, v3 | `ALTER TABLE … SET TBLPROPERTIES ('format-version' = '3')` | Unsupported SQL statement | Unsupported SQL statement |
| ALTER, v3 | `ALTER TABLE … SET (format_version = '3')` | Unsupported SQL statement | Unsupported SQL statement |
| ALTER, mor | `ALTER TABLE … SET PROPERTIES (extra_properties = MAP(…))` | Unsupported SQL statement | Unsupported SQL statement |
| ALTER, mor | `ALTER TABLE … SET TBLPROPERTIES ('write.delete.mode' = 'merge-on-read', …)` | Unsupported SQL statement | Unsupported SQL statement |
| ALTER, mor | `ALTER TABLE … SET ("write.delete.mode" = 'merge-on-read', …)` | Unsupported SQL statement | Unsupported SQL statement |
| UUID | `CREATE TABLE … (uuid_col UUID)` | Unsupported SQL type UUID | Unsupported SQL type UUID |

The native door refuses the v3 allow-flag route too: the constructor accepts
`config={"repark.sql.allowCreateFormatVersion3": "true"}` (the facade's key) and the probe passes it, but
the flag cannot change the outcome above.

Why the spellings are unreachable from Python: `crates/repark-python/Cargo.toml` states that
`repark-sql` has no edge to the Python binding ("no ANSI door from Python"). The WITH options and the ALTER
spellings are parsed by `crates/repark-sql/src/properties.rs` and `crates/repark-sql/src/alter.rs`, which
only the Rust-embedded router uses. The native Python session plans through `plan_session_sql`
(DataFusion), which refuses these statements with the message above. No crate outside `repark-sql`
depends on it.

Cross-session check (the coordinator's second route): a facade session (main wheel) created and seeded a
v3 merge-on-read table `ice.ns.shared` in a catalog root. A separate native process registered the same
root and queried it: `Error during planning: table 'ice.ns.shared' not found`. The memory catalog is per
session, so the native door cannot see a facade-created table.

Consequence: the native v2-mor, native v3 and native UUID cells are `SETUP-REFUSED` with every attempt
listed in their `setup` field, and they carry no measured outcome. The native door's v2-cow cells are
created without properties and take the door default (format 2, copy-on-write; checked directly: the
metadata reports `format-version` 2 and no `write.delete.mode` property).

Facade and Spark create the three table kinds with `TBLPROPERTIES` and verify them from the metadata
(`setup_properties` in each record).

## Measurement facts (unchanged from the first round)

- Native `CREATE TABLE` with `STRUCT`/`ARRAY` columns and `named_struct` / `ARRAY[n]` seeds works; the
  facade and Spark take `named_struct` and `array(n)`.
- The facade door creates a v3 UUID table and accepts a quoted UUID string literal as the seed.
- Spark cannot declare an Iceberg uuid column, so the Spark UUID cells are `ORACLE-MISSING`. The Spark
  native-route struct cells are `ORACLE-MISSING` too (the native door's reference is Trino, not Spark).

## Deviations from the brief (all recorded so the reviewer can rule on them)

1. Bookkeeping labels, not in the brief's list: `SETUP-REFUSED` (table creation, seed or property setup
   refused; the statement never runs) and `UNPREDICTED` (the prediction is `unknown`, `measure`, an
   unprobed inference, or the statement did not run). The orchestrator confirmed `SETUP-REFUSED` as a
   bookkeeping label.
2. `REPORT-DIFFERS` is split (orchestrator ruling). `REPORT-SHAPE` means only the returned affected-row
   report differs, while the persisted state and the read rows are equal; it is the DML return-shape
   difference (RePark returns an affected-row `count` or an empty row, Spark returns no rows).
   `RESULT-ROWS-DIFFER` means a read's returned rows differ from Spark's (the `u-read` cell).
3. Struct readbacks (`SELECT * FROM S` and `U`) are sorted in Python by canonical JSON, not by
   `ORDER BY` every column. This gives the same content and avoids engine-specific `ORDER BY` on struct
   columns. Matrix-1 readbacks use `ORDER BY id` as the brief says.
4. Table properties are written as quoted strings (`'format-version' = '2'`), the form the brief names.
5. `spark.local.dir` is set to a directory under the leg's warehouse so Spark scratch stays out of the
   system temp directory. No other Spark setting is changed.
6. The Spark side of each cell is also checked against its own prediction. Those results are reported in
   the hand-back and are not in the labels.
7. The scrub replaces the warehouse root, the temporary directory and the home directory with
   `<warehouse>`, `<tmp>` and `<home>`. The raw files carry those placeholders and no real path.
8. Error excerpts are the first 300 characters of the scrubbed message, cut back to a word boundary, so
   no word is left half-written in the raw files (a half-written word failed the spell gate).
9. The native constructor is given the facade's v3 allow key (`native(config=...)`), on both wheels. It
   changes no outcome in this round (see the native section above).
10. `c-selective` (`id = 1`, DELETE and `SET flag = 'z'`) is added as a control on every door, table kind
    and leg, on the main table and on the UUID table (orchestrator delta item 1). It is the only added
    axis.
11. A refusal at write time (the typed struct routes) keeps the post-refusal state in the record:
    `persisted_after` and `snapshot_after` are read after the refusal, so a partial write would show.
    Both typed cells show no change (`snap` same, `files` same).

## Gates

Gate results are recorded in the pull request body, with exit codes.
