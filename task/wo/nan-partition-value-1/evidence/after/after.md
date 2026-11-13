# NAN-PARTITION-VALUE-1 after the fix: red, green and gates

Model `claude-opus-5-5`, 2026-11-12. Fork `945d3326` (branch `fix/nan-partition-value-1` off
`66901f02`); RePark branch `fix/nan-partition-value-1` off `a3f8c7d9` with the pin moved to it.
Spark 4.1.2, Iceberg 1.11.0, `REPARK_PARITY_LIVE=1`.

## Red and green

`python/repark/tests/iceberg/test_nan_partition_value_1.py`, 21 cells
([raw/pin-red-a3f8c7d9.txt](raw/pin-red-a3f8c7d9.txt), [raw/pin-green-945d3326.txt](raw/pin-green-945d3326.txt)):

| Cells | main `a3f8c7d9` | fixed |
|---|---|---|
| RePark stores and reads its NaN partition: INSERT, `writeTo`, `saveAsTable` × DOUBLE, FLOAT (6) | FAILED | passed |
| RePark stores `±Infinity` partitions (1) | FAILED | passed |
| Spark reads RePark's table as its own: INSERT, `writeTo`, `saveAsTable` × DOUBLE, FLOAT (6), and the infinity twin (1) | FAILED | passed |
| CTAS × DOUBLE, FLOAT, RePark-own and Spark-reads (4) | passed (CTAS never crossed the JSON step) | passed |
| Controls: INT/STRING NULL partitions, unpartitioned `isnan` and `d > 1.0` (1) | passed | passed |
| The read half: RePark reads Spark's table as Spark does × DOUBLE, FLOAT (2) | passed | passed |

14 red on main, 21 green on the fix. The 7 green-on-main cells are the controls the brief asks
to keep unchanged; they are not on the seat.

## The probe legs on the fixed build

Step 0's cells re-run: [table-fixed.md](table-fixed.md), 150 EQUAL and 0 DIFFERS (step 0 on
main: 41 DIFFERS). The manifests now hold `d = NaN` with bits `7ff8000000000000` for the
INSERT tables and `7ff0…`/`fff0…` for the infinity twin, the bits Spark writes.
`.partitions` lists four rows (NULL, 0.0, 1.5, NaN): the NaN half of
`ICEBERG-PARTITIONS-TABLE-FLOAT-1` is gone; the signed-zero half is part B's.

## Gates

| Repository | Gate | Exit |
|---|---|---|
| fork | `cargo fmt --all -- --check` | 0 |
| fork | `scripts/check_rust_file_size.py` (`mod.rs` stays at its 1237-line ceiling) | 0 |
| fork | `cargo clippy -p iceberg --all-targets -- -D warnings` | 0 |
| fork | `cargo test -p iceberg --lib spec::manifest` (76 passed, 6 new) | 0 |
| fork | `cargo test -p iceberg-datafusion --lib physical_plan` (273 passed) | 0 |
| fork | `comment_ban.py . origin/main HEAD` | 0 hits |
| RePark | `cargo fmt --all -- --check` | 0 |
| RePark | `make rust-clippy` | 0 |
| RePark | `make rust-panic-ban` | 0 |
| RePark | `cargo test --locked -p repark-iceberg --lib` (no RePark crate source changed; the fork pin's main consumer) | 0 |
| RePark | `make py-test-facade FACADE_WORKERS=6` (27908 passed, 722 skipped, 150 xfailed) | 0 |
| RePark | `make py-test-facade FACADE_PROFILE=release FACADE_WORKERS=6` (27913 passed, 717 skipped, 150 xfailed) | 0 |
| RePark | `sync_map_md.py --check`, `check_map_md.sh`, `check_crate_dag.sh`, `check_manifest.sh` | 0 (lockstep advisory on `scripts/map.md`) |
| RePark | `check_docs_compaction.py`, `make check-map-size`, `make check-ledger-grammar`, `make check-docs-links` | 0 |
| RePark | `uvx typos@1.47.2`, `uvx ruff@0.15.22 check .`, `uvx ruff@0.15.22 format --check .` | 0 |
| RePark | `comment_ban.py . origin/main HEAD` | 0 hits |

Builds used the local fork commit through a `url.insteadOf` rewrite with
`CARGO_NET_GIT_FETCH_WITH_CLI`; the pinned URL in `Cargo.toml` and `Cargo.lock` is GitHub's, so
the fork commit must be pushed before CI can resolve it.
