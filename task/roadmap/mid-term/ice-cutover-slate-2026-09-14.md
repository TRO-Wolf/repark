# Iceberg production-cutover slate — run 14 (2026-09-14)

**Charter:** the production Iceberg assessment of 2026-09-14
(`docs/cutover/production-iceberg-status-2026-09-14.md`, docs PR #585) — §7 gaps G-1..G-11 and
the owner's rulings Q-ICE-1..7 in its §10 addendum, recorded in `docs/cutover/inventory.md` §8.
**Base:** `main` at 1.4.0, fork pin `3ebf7d36`. **Worked by:** run 14 (cards 1–4, in order).
Cards 5 and 6 are recorded for later work and for the owner; run 14 does not start them.

Every card: no code comments (AGENTS.md, 2026-08-26); ledger `COVERAGE_ATTESTATION` block when
every clause is PROVEN; the pipeline's real names never enter the tree ("the cutover pipeline").

---

### Card F-GLUE-REPLACE-1 — Glue replace publish in the fork (G-1)

- **Gap.** `GlueCatalog` inherits the `Catalog::publish_replace_table` default, which returns
  `FeatureUnsupported`; `S3TablesCatalog` and `MemoryCatalog` implement it. The fork's GAP_MATRIX
  row R158 names two replace-publish residues: the staged metadata is not read-validated before
  the pointer swap, and metadata-file versioning restarts at v0.
- **Failing scenario.** The second scheduled gold run after C6: dbt-repark marks every relation
  Iceberg, dbt-spark's `table` materialization emits `create or replace table … as`, RePark
  streams the SELECT into data files, and the publish fails with `FeatureUnsupported`. The model
  fails and the staged files stay unreferenced. `writeTo().createOrReplace()` fails the same way.
- **Evidence.** Assessment §7 G-1 and row C2. The fork trait default is in
  `crates/iceberg/src/catalog/mod.rs`, and the S3 Tables implementation is
  `crates/catalog/s3tables/src/catalog.rs` `publish_replace_table`. The Glue gold acceptance uses
  a constant stem, so only a first build ever ran live.
- **Done when.** Glue `publish_replace_table` is a version-id-checked `UpdateTable` of the
  metadata location through the Glue commit transport that `update_table` uses. The staged
  metadata is read back and checked before the send. The R158 row and `ENGINE_CONTRACT.md` §8a
  are updated. The fork PR is merged after one Grok critic-logic round.
- **Pins (offline, commit-transport seam).** The pins cover: a staged replace publishes; an
  expected-base mismatch returns a retryable `CatalogCommitConflicts` before any send; a missing
  or foreign staged metadata file is refused before any send; a lost response (maybe-sent,
  accepted-then-lost) returns `CommitStateUnknown` after one attempt and leaves the staged file in
  place; a service conflict is retryable and an access denial terminal. The versioning residue is
  either fixed with a pin or named in the ledger.

---

### Card RP-20 + ICE-GOLD-TWICE-1 — repin, replace in the nightly, gold twice (G-1, G-2)

- **Gap.** RePark consumes the Glue replace only through a repin. The nightly `aws-acceptance`
  run passes on `main` every night, but it has no replace leg and no gold dbt leg. The Glue gold
  test (`python/dbt-repark/tests/test_aws_acceptance_gold.py`) uses the constant stem
  `ACCEPTANCE_TABLE_PREFIX + "dbt1"` and runs `dbt run` once.
- **Failing scenario.** A regression in the replace path, or in anything the gold models exercise
  on Glue, first shows up in production, because no workflow runs those shapes.
- **Evidence.** `.github/workflows/aws-acceptance.yml` runs only
  `python/repark/tests/test_aws_acceptance.py`. Nightly run 34824917757 (2026-09-14) is green on
  the silver legs.
- **Done when.**
  - The fork pin moves to the merged F-GLUE-REPLACE-1 revision (rp-NN ledger).
  - `test_aws_acceptance.py` gains a leg that runs `CREATE OR REPLACE TABLE … AS` twice on Glue
    and on S3 Tables. It checks rows and snapshot history, and skips without credentials like its
    siblings.
  - The gold test uses unique stems per run, runs `dbt run` twice, then `dbt test`.
  - The gold module joins `aws-acceptance.yml` (owner-approved `.github` change), with zizmor
    clean and the OIDC credential step still after the build.
  - After the merge, the workflow is dispatched on `main`. Its run id and per-leg counts go in
    `docs/cutover/inventory.md` §8.
- **Pins.** The two new live legs. They are skip-clean offline, and their skip count is asserted
  in the ledger. The merged workflow run is the live proof.

---

### Card ICE-SPARK-TABLE-1 — RePark writes into a Spark-created table (G-3, Spark half of G-4)

- **Gap.** Every table RePark writes in its tests was created by RePark. Production tables, and
  per Q-ICE-3 the shadow tables, are created by Spark. The owner ruled that writing into
  Spark-created tables is a standing requirement.
- **Failing scenario.** At the C4 switch, RePark's MERGE meets Spark-written metadata for the
  first time: Spark's stamped properties, `write.distribution-mode`, a sort order, Spark-written
  manifests and snapshot summaries. It then refuses or writes a snapshot Spark reads wrongly, after
  the Spark task is already paused.
- **Evidence.** Assessment row I4. No cell pins it. The parity-live tier already runs PySpark
  4.1.2 with the Iceberg 1.11.0 runtime (`.github/workflows/parity-live.yml`, the Makefile live
  targets, the ivy redirect in `python/repark-parity`). RePark exposes
  `CALL <catalog>.system.register_table` (`crates/repark-spark/src/call.rs`).
- **Done when.**
  - Spark, on a local filesystem catalog, creates and seeds a v2 copy-on-write table with the
    production properties (inventory §1 row 3, §3).
  - RePark registers that metadata and runs the production MERGE shape
    (`UPDATE SET * / INSERT *`) twice, then the weekly maintenance CALLs.
  - Spark reads the RePark-written snapshot back: `EXCEPT ALL` both ways plus counts.
  - If the live cell cannot run in CI, the fallback is a Spark-created metadata fixture checked
    in beside the torture DV fixture.
  - Every property or metadata shape RePark handles differently on a Spark-created table than on
    a RePark-created one is named in the ledger and the registry.
- **Pins.** The live cell (or the fixture cell) per step above. The transform sort-order refusal
  (`WRITE-ORDER-TRANSFORM-1`) is pinned as a stated residual, not a failure.

---

### Card ICE-COMMIT-UNKNOWN-1 — an ambiguous commit gets its own exception class (G-6)

- **Gap.** `crates/repark-core/src/error_map.rs` `classify_iceberg_error` folds
  `ErrorKind::CommitStateUnknown` into the generic `Error::Iceberg`. The caller sees a base
  `PySparkException` and cannot tell an ambiguous commit from a definite failure.
- **Failing scenario.** A Glue `UpdateTable` response is lost and the fork cannot reconcile
  within its budget. Airflow cannot alert on the ambiguous case, or restrict its retry to
  MERGE and CTAS IF NOT EXISTS (Q-ICE-7), so a retried append duplicates rows.
- **Evidence.** Assessment row K3, fork GAP_MATRIX R157. The match arm is in `error_map.rs`, and
  the exception hierarchy is in `crates/repark-python/src/exceptions.rs`.
- **Done when.**
  - `CommitStateUnknown` maps to its own Python exception class, a subclass of
    `PySparkException`, so every existing `except PySparkException` still catches it.
  - The exception carries the commit's `engine.operation-id`.
  - The class is wired through `error_map.rs` and `exceptions.rs`.
  - A registry row is added, plus a line in inventory §8: Airflow alerts on the class and retries
    MERGE and CTAS IF NOT EXISTS only.
  - If surfacing the kind or the operation id needs a fork change, the card parks and names it.
    This card does not change the fork.
- **Pins.** A test catalog or transport that returns `CommitStateUnknown`. The caller catches the
  new class and the base class, and reads the operation id. A definite conflict still raises the
  old class.

---

### Card ICE-TRINO-READ-1 — Trino reads RePark-written silver and gold (G-4, Trino half)

- **Gap.** Q-ICE-5 names Spark and Trino as the production readers. No cell has Trino read a
  snapshot RePark wrote.
- **Failing scenario.** A Trino query over a RePark-written silver or gold table refuses the
  snapshot or answers different rows, and nothing here catches it.
- **Evidence.** Assessment §7 G-4, re-scoped by Q-ICE-5. There is no Trino tier in this
  repository yet.
- **Done when.** Trino reads RePark-written silver and gold tables (`count(*)`, a checksum, and
  `EXCEPT` against the RePark answer) at the candidate pin.
- **Pins.** A Trino cell beside the Spark cell of ICE-SPARK-TABLE-1.
- **Parked** behind the 1.8 Trino qualification; not started.

---

### Card SHADOW-1 change — Spark creates the shadow tables (pipeline side, for the owner)

- **Gap.** SHADOW-1 as briefed (2026-09-04 cutover rulings) has RePark create the shadow tables
  `<ns>_silver_repark`. Those tables never carry Spark's metadata, so the shadow run never
  exercises G-3.
- **Failing scenario.** The shadow period passes green on RePark-created tables. The switch then
  meets Spark-created production metadata for the first time (card ICE-SPARK-TABLE-1's
  scenario).
- **Evidence.** Q-ICE-3 ruling: the shadow tables are created by Spark from the production DDL and
  properties.
- **Done when.** The cutover pipeline's shadow bootstrap creates each shadow table with Spark,
  using the production `CREATE TABLE` DDL and `TBLPROPERTIES`, before RePark's first write. The
  Airflow diff task reads both sides as before.
- **Pins.** Pipeline side. This repository does not change for this card, and run 14 does not
  touch the pipeline.

## Pointers
- Up: [map.md](map.md) · Contract: [../../../AGENTS.md](../../../AGENTS.md)
