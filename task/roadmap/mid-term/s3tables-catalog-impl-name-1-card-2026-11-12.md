# Card S3TABLES-CATALOG-IMPL-NAME-1: tests and records name an S3 Tables catalog class that does not exist

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`).

**Status:** filed 2026-11-12, not dispatched. Severity **S4**. No product behavior is wrong today.

**What is wrong:** several sites name `org.apache.iceberg.aws.s3tables.S3TablesCatalog` as the S3 Tables `catalog-impl`. No such class exists. The class in AWS's S3 Tables client jar is `software.amazon.s3tables.iceberg.S3TablesCatalog`.

**Why it works anyway:** RePark resolves a `catalog-impl` by suffix. `kind_from_catalog_impl` in `crates/repark-core/src/catalog_kind.rs` maps any value ending in `S3TablesCatalog` to the S3 Tables kind, so both spellings reach the same builder. The same file's tests already check the real class name (line 80). Spark does not match by suffix: the wrong name is why Spark never opened the S3 Tables catalog before the harness moved to the REST setup ([AWS-EPHEMERAL-RUNS-1](aws-ephemeral-runs-1-card-2026-10-24.md), [lessons](../../lessons.md)).

**Every site on main `ae247767`** (`git grep -n 'org.apache.iceberg.aws.s3tables.S3TablesCatalog' origin/main`):

| Site | What it is |
|---|---|
| `crates/repark-core/src/catalog_config.rs:461` | Rust unit test `s3tables_catalog_impl_resolves`: the `catalog-impl` value it parses. |
| `crates/repark-core/src/catalog_config.rs:891` | Rust unit test in the same module: the `catalog-impl` entry of the `s3t_impl` config. |
| `crates/repark-core/src/config_file/tests/mod.rs:453` | Config-file test: the `catalog-impl` line of the `s3tables_java` profile. |
| `python/repark/tests/_acceptance.py:273` | The acceptance harness's S3 Tables catalog config, the block RePark reads. |
| `python/repark/tests/parity/test_acceptance_helpers.py:117` | The AWS-free test that asserts that config block's shape. |
| `task/ledgers/staging/ice-integrity-pins-1-ledger.md:578` | Ledger row C-067, which records that Spark was handed this name. |
| `task/lessons.md:496` | The lesson "DO NOT hand Spark a catalog class name because RePark accepts it", which quotes the name. |
| `task/roadmap/mid-term/aws-ephemeral-runs-1-card-2026-10-24.md:37` | The measured Spark error text that names the class. |

**Fix direction:** use the real class name, `software.amazon.s3tables.iceberg.S3TablesCatalog`, in the test configs (the first five sites) and in any user-facing doc; no page under `docs/` names the wrong class on main. Keep the suffix match in `kind_from_catalog_impl`, so a user config with either spelling still resolves. The last three sites are records of what was measured; they keep the name they quote. This records PR changes no code.

**Close:** the five test and harness sites name the real class and the suffix match is unchanged, or the owner declines the card.
