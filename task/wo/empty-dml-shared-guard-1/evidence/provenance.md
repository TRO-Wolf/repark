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
`repark_file` and `venv`. The first line of `raw/spark.jsonl` records the jar and the JDK.

- Cargo.lock at `fdc61f26` (sha256): `d3defbf391c1d195d52eb150adb6dca14603a2a4da386d3d967a85ae019ca590`
- Build profile: release (`Finished \`release\` profile [optimized]` in the build log, 12 m 08 s).
- `uv` 0.11.16. PyPI leg installed with `uv pip install --python <venv>/bin/python repark==1.5.5`, then
  reinstalled from the downloaded file so that the installed wheel is the hashed one.

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

Each leg ran in a fresh warehouse directory, from a scratch working directory (so the Spark metastore
and any logs stay out of the clone). The committed raw files come from the last full run of each leg,
after the final edit to `probe.py`. Earlier runs were superseded and their warehouses removed.

## Measurement facts

- Native door (PyPI 1.5.5 and main, identical behaviour): `CREATE TABLE ... TBLPROPERTIES` is refused
  (`Unsupported SQL statement`). Native v2-mor and v3 tables therefore never exist (44 `SETUP-REFUSED`
  cells per RePark leg: all native v2-mor and v3 cells, and the native UUID cells). Native v2-cow is
  created without the clause, which is the door's default: a table created that way reports
  `format-version` 2 and no `write.delete.mode` property, so the Iceberg default copy-on-write applies
  (checked on the PyPI venv).
- Native door has no `UUID` type (`Unsupported SQL type UUID`). The facade door creates a v3 UUID table
  and accepts a quoted UUID string literal as the seed.
- Spark cannot declare an Iceberg uuid column, so all six Spark UUID cells are `ORACLE-MISSING`. The
  Spark native-route struct cells are `ORACLE-MISSING` too (the native door's reference is Trino).
- Seeds: the native door takes `named_struct('x', n)` and `ARRAY[n]`; the facade door and Spark take
  `named_struct` and `array(n)`.

## Deviations from the brief (all recorded here so the reviewer can rule on them)

1. Two labels are added for bookkeeping, not in the brief's list: `SETUP-REFUSED` (table creation or
   seed refused; the statement never runs) and `UNPREDICTED` (the prediction is `unknown`, `measure`,
   an unprobed inference, or the statement did not run). A cell whose prediction is `unknown` cannot be
   labelled MATCHES or DIFFERS, so it carries `UNPREDICTED`. The brief's label list is otherwise
   applied as written.
2. Struct readbacks (`SELECT * FROM S` and `U`) are sorted in Python by canonical JSON, not by
   `ORDER BY` every column. This gives the same content and avoids engine-specific `ORDER BY` on struct
   columns. Matrix-1 readbacks use `ORDER BY id` as the brief says.
3. Table properties are written as quoted strings (`'format-version' = '2'`), the form the brief names.
4. `spark.local.dir` is set to a directory under the leg's warehouse so Spark scratch stays out of the
   system temp directory. No other Spark setting is changed.
5. `REPORT-DIFFERS` is a return-shape difference, not a result difference. RePark's DML returns an
   affected-row count (`count`) or an empty row, and Spark's DML returns no rows. The label fires on every
   answered statement whose persisted rows match. The raw JSON carries the returned rows for each cell.
6. The Spark side of each cell is also checked against its own prediction. Those results are reported in
   the hand-back and are not in the labels.
7. The scrub replaces the warehouse root, the temporary directory and the home directory with
   `<warehouse>`, `<tmp>` and `<home>`. The raw files carry those placeholders and no real path.
8. Error excerpts are the first 300 characters of the scrubbed message, cut back to a word boundary,
   so no word is left half-written in the raw files (a half-written word failed the spell gate).

## Gates

Gate results are recorded in the pull request body, with exit codes.
