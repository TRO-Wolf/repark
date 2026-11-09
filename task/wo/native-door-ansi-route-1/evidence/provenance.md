# Provenance: the native-door dry run (NATIVE-DOOR-ANSI-ROUTE-1 sizing)

Run by the Claude Haiku 5.5 measurement clerk (`claude-haiku-5-5`) on 2026-11-08, on a local Linux box under `repark.slice`. Results are in [dry-run.md](dry-run.md); the patch is [prototype.patch](prototype.patch); the raw JUnit XML is in [raw/base.xml](raw/base.xml) and [raw/prototype.xml](raw/prototype.xml).

## Trees

- Base: a scratch clone, detached at `origin/main` = `03f925c5103faf8329c6e5eb5b0223bf7143c1e3`. The evidence branch `docs/native-ansi-dry-run` is cut from this commit.
- Prototype: a second scratch clone at the same commit. Its push URL is a disabled placeholder, and it was never pushed. Its only change is [prototype.patch](prototype.patch): `repark-sql` as a dependency of `repark-python` (`Cargo.toml` and `Cargo.lock`), and `.with_sql_dialect(Arc::new(repark_sql::AnsiDialect))` in `PyReparkSession::native()`. `with_local_fs_guard()`, `NativeDoorTypes` and `register_native_door_functions` are kept.

## Environment

- Python 3.12.3 in each clone's own `.venv`. pytest 9.1.1 and pytest-xdist 3.8.0.
- Provisioning: `uv sync --locked --extra numpy --extra pandas --extra polars --extra ml-ext --no-install-package repark`, then `make develop` (`maturin develop`, default profile) in each clone. Both builds exited 0 and installed `repark-1.5.5`.
- The brief said `make develop` creates the `.venv`. It does not: maturin needs an existing venv and fails without one. The `uv sync` step is the repo's own facade provisioning (step 1 of `make py-test-facade`). The four extras matter, because `importorskip` turns a missing extra into a skip.
- Import check, run in each clone before testing: `repark.__file__` resolves to `python/repark/src/repark/__init__.py` in that clone, and `_native.abi3.so` resolves beside it.
- Builds ran under `systemd-run --user --scope --collect --slice=repark.slice -- env CARGO_BUILD_JOBS=12`. Tests ran in two units, one after the other, not in parallel.

## Test list

- `git grep -l -e 'repark.sql(' -e 'PyReparkSession.native(' -- python/repark/tests` returns 68 paths: 58 `test_*.py` files, 5 `_*.py` helpers, and 5 `map.md` files.
- The run uses the 58 `test_*.py` files, which is pytest's default collection pattern. The helpers and `map.md` files are not tests and are not collected.
- Call sites are counted from the base clone's file contents, as occurrences of `repark.sql(` plus `PyReparkSession.native(` (fixed strings). This gives 215 in the 58 collected files and 18 in the helpers.

## Command

Each run used the Makefile's facade flags (`FACADE_WORKERS` = 16, `--dist loadgroup --maxschedchunk=8`), with the venv's interpreter instead of `uv run`, as the brief requires:

    PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=<clone>/.venv \
      .venv/bin/python -m pytest <58 files> -p no:cacheprovider \
      --junitxml=<raw> -n 16 --dist loadgroup --maxschedchunk=8 -q -rA

The live tier is not armed (`REPARK_PARITY_LIVE` unset), as in `make ci`.

## Results

| run | passed | failed | skipped | xfailed | wall time |
|---|---:|---:|---:|---:|---:|
| base | 1681 | 0 | 130 | 5 | 678 s |
| prototype | 1667 | 14 | 130 | 5 | 298 s |

Both runs collected 1816 items with the same identifiers. Pytest's summary counts 5 xfails and 130 skips. JUnit records the same 135 skipped elements, because each xfail is also written as a `skipped` element with `type="pytest.xfail"`. The base run was slower. The cause was not checked.

## Classification

- A scratch script (not committed) reads the two JUnit files, matches tests by `(classname, name)`, and assigns each test to one bucket from its base and prototype outcomes, as described in [dry-run.md](dry-run.md).
- The exception class comes from the JUnit message prefix, with `repark.errors.` removed.
- One manual override: the `test_a_non_default_parser_dialect_still_refuses` row is a new refusal with a changed reason. Its failure is an assertion on the G3-E8 text, raised after a ParseException.
- Call sites are attributed to files, and each file takes the worst bucket of its tests.

## Scrubbing

Clone paths were replaced with `<base-clone>` and `<prototype-clone>`, scratch paths with `<scratchpad>`, the session directory with `<session>`, the home directory with `~`, pytest's temporary directory with `<pytest-tmp>`, any other system temporary path with `<tmp>`, and the host name with `<host>`. A grep after scrubbing finds no remaining absolute path in the committed files.

## Hashes (scrubbed files as committed)

- `raw/base.xml` sha256 `79a2414308fb39eb429890eabb25a4a4ac3c6a63b4a25ea4711e24d9e78d3ca7`
- `raw/prototype.xml` sha256 `6f76add4b46d64dff4dde7375bff8550d6c2c646ad6b9773c2a1b61b04d5a8b0`
- `prototype.patch` sha256 `42b4e494576c61639cc1bb2366e56d2b4da8bb234cc62478373a8ec320332ad3`

## Deviations from the brief

1. Venv provisioning: `uv sync` before `make develop` (see Environment).
2. Skips and xfails are reported beside the six buckets, not forced into one of them.
3. The call-site count is 215 in the collected files (233 with the helpers), not the brief's 236.
4. The brief's `Authored-By` trailer is used on the commit. No other attribution line is added.

## Write cells (2026-11-08)

Run by the Claude Haiku 5.5 clerk (`claude-haiku-5-5`) under the owner's 2026-11-08 "Proceed with any measurements you need". The table is [write-cells.md](write-cells.md); the probe is [probe_write_cells.py](probe_write_cells.py); the raw rows are [raw/write-cells-base.jsonl](raw/write-cells-base.jsonl), [raw/write-cells-proto.jsonl](raw/write-cells-proto.jsonl) and [raw/write-cells-spark.jsonl](raw/write-cells-spark.jsonl). The reference is Spark 4.1.2 with Iceberg 1.11.0. Trino is the native door's reference and is unmeasured, so Spark stands in for it.

### Legs

- `base`: the base venv of the base clone at `03f925c5`. Native door (`repark.sql()`, stock DataFusion) and facade (`ReparkSession.sql()`).
- `proto`: the prototype venv (`AnsiDialect` routing, the same three-line patch). Native door and facade.
- `spark`: `c26-oracle` Python, PySpark 4.1.2, a Hadoop catalog `ice` with the Iceberg 1.11.0 runtime jar, ANSI on, UTC, `local[2]`.

Each leg ran in its own process. The base and proto legs ran with the venv interpreter, and the render step ran with the base venv (stdlib only).

### Commands

    probe_write_cells.py --leg base  --scratch <scratch>/base  --raw-dir raw
    probe_write_cells.py --leg proto --scratch <scratch>/proto --raw-dir raw
    JAVA_HOME=<jdk-17> probe_write_cells.py --leg spark --scratch <scratch>/spark --spark-jar <iceberg-1.11.0-runtime.jar> --raw-dir raw
    probe_write_cells.py --render --raw-dir raw --out write-cells.md

### Hashes (as committed)

- `probe_write_cells.py` sha256 51433920213ded83401fb0295ee05a1362733558eb48ec1bae6ab3f568baf918
- `raw/write-cells-base.jsonl` sha256 2d4bb1e46d4c1815994455ff7f918b9fc880291f3404ac5d119be1132d42553d
- `raw/write-cells-proto.jsonl` sha256 27df2c11306b7bf32f39a72d6dde200337841af8ab5bdec2350640f07a745b68
- `raw/write-cells-spark.jsonl` sha256 698e3f08cbbcf4de26361beeded30c80a97902798e68491f0a97dfb2054cc6ac
- `write-cells.md` sha256 626321eb9a9b65f51f7aa4f301da8265f3a7fe97399383b4021af1c7ebfd560e

### Deviations and measured facts

1. The native door's `CREATE TABLE ... USING iceberg TBLPROPERTIES (...)` is refused by the parser (`Expected: end of statement, found: USING`). The native door then refuses the bare `TBLPROPERTIES` spelling as `This feature is not implemented: Unsupported SQL statement`. So proto/native v2-mor and v3 tables cannot be created and are SETUP-MISSING. The probe retried the native spelling without `USING iceberg`; the first run's refusal is recorded here and the final raw rows show the second refusal.
2. Group E tables declare `id INT NOT NULL`, seeded with the group W rows (1,10),(2,20),(3,30),(4,40). The brief names the NOT NULL requirement but gives no seed; this is the chosen seed.
3. Spark uses one Hadoop catalog and one warehouse for the leg. Each cell drops `ice.ns.t` and `ice.ns.s` with PURGE and removes their directories before its own create and seed. The repark legs use a fresh memory-catalog warehouse per cell. The Spark leg therefore does not use a private catalog per cell.
4. Group R on facade was not run, because the brief names base against proto against Spark only.
5. R1 on Spark is the statement as written. Spark rejects `generate_series`, so its outcome is a refusal, not a comparison of answers.
6. The Spark leg ran before the coordinator's rule that every Spark JVM runs under `flock /tmp/oc-worker/build-slots/spark-jvm.lock`. It was the only Spark run, and no later Spark run is planned for this table.
7. Files are counted as every non-hidden file under `data/` except `.crc` files, so Iceberg merge-on-read delete files are included.
