# UPDATE-SCHEMA-BATCH-MISMATCH-1, the gates

Run on lane 2 (`taskset -c 48-57`, `CARGO_BUILD_JOBS=6`, the measure lock then the lane's cargo lock), on the
branch with the fix, 2026-11-13.

| Gate | Exit | Note |
|---|---|---|
| `cargo test --locked -p repark-iceberg` (lib and test targets) | 0 | lib: 1066 passed, 2 ignored |
| `cargo test --locked -p repark-iceberg --lib predicate_dml::tests::batch_nullability` | 0 | 4 passed (red on main: 4 failed) |
| `make rust-clippy` | 0 | |
| `make rust-panic-ban` | 0 | |
| `cargo fmt --all -- --check` | 0 | |
| `make py-test-facade FACADE_WORKERS=6` (profile `ci`) | 0 | 28406 passed, 728 skipped, 150 xfailed |
| `make py-test-facade FACADE_PROFILE=release FACADE_WORKERS=6` | 0 | 28411 passed, 723 skipped, 150 xfailed |
| the facade pin alone, release build | 0 | 47 passed, twice (red on main: 14 failed) |
| `python3 scripts/sync_map_md.py --check` | 0 | |
| `scripts/check_map_md.sh` | 0 | |
| `scripts/check_crate_dag.sh` | 0 | |
| `scripts/check_manifest.sh` | 0 | |
| `python3 scripts/check_docs_compaction.py` | 0 | |
| `make check-map-size` | 0 | `task/wo/map.md`, `python/repark/tests/iceberg/map.md`, `crates/repark-iceberg/src/write/map.md` and `task/roadmap/mid-term/map.md` held at their exact baselines |
| `make check-ledger-grammar` | 0 | |
| `make check-docs-links` | 0 | |
| `uvx typos@1.47.2` | 0 | |
| `uvx ruff@0.15.22 check .` | 0 | |
| `uvx ruff@0.15.22 format --check .` | 0 | |
| the comment gate (`comment_ban.py <clone> origin/main HEAD`) | 0 | 0 hits |

Up: [../map.md](../map.md).
