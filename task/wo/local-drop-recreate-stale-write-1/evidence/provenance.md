# Provenance

Measured 2026-11-12 on branch `fix/local-drop-recreate-stale-write-1`.

- **before:** main `ae247767a83efbe4f4d1344e45ba43885e8b36af`, fork rev
  `66901f02a2df26098cfe073605152aeb518808f5`, RePark 1.5.6 built with
  `uvx maturin@1.14.1 develop --profile ci` into the clone's venv (`uv sync --locked --extra numpy
  --extra pandas --extra polars --extra ml-ext --no-install-package repark`), CPython 3.12.3.
- **after:** the same tree with the five `TRO-Wolf/iceberg-rust` revs in `Cargo.toml` moved to
  `79b6f19b5d19a43d30241ee0242da9de2fdd629f` (fork PR #382) and `cargo update -p iceberg`, which
  moved only the six fork packages' source lines in `Cargo.lock`; same build command and profile.
- **Spark oracle:** PySpark 4.1.2 with `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, `local[2]`,
  JDK 17, under the single-JVM lock.
- **Commands:** `python -I cells_repark.py --root <dir> --out <cells.jsonl>` on each build, then
  `python -I spark_read.py --cells <cells.jsonl> --jar <iceberg runtime jar>` with the oracle
  interpreter. `table.md` is rendered from the four JSON-line files, which stay in the lane logs
  because they carry machine paths.
- **Pins:** the facade file `python/repark/tests/iceberg/test_local_drop_recreate_stale_write_1.py`
  failed 12 of 20 offline cells on the before build (every wrong-read, wrong-write, replace-only
  and purge row; the 7 controls and the cache-off cell passed) and passes 20 of 20 on the after
  build; its six live cells (`REPARK_PARITY_LIVE=1`) pass on the after build.
