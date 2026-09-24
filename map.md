# map — repository root

CC-3 (2026-08-30): AGENTS.md house-style clause and rustfmt.toml comment now state one-line comments; `// === name ===` markers stay. Compaction ceiling restored to 32000.

## Purpose

RePark: a pure-Rust, no-JVM data engine over DataFusion + Arrow + the owned iceberg-rust fork,
with two SQL doors (native ANSI/Trino-style and a near-drop-in PySpark facade). This is the
front-door navigation map. See [README.md](README.md) for the overview,
[AGENTS.md](AGENTS.md) for the agent contract, and **[STATUS.md](STATUS.md) for current state**
(release state, delivery, active workstreams — the single source of truth; do not restate it here).
F-Y10-1 closed 2026-08-30.

The workspace carries ten delivered crates: `crates/repark-common` (error seed + the
surface-matrix registry), `crates/repark-iceberg` (catalog + write over the owned iceberg-rust
fork, `[patch.crates-io]`-pinned), `crates/repark-core` (the `ReparkSession` engine API + the
frozen `SqlDialect` / `SessionExtension` seams, and since 2026-09-09 the `repark.toml` loader
family `src/config_file/` seeded by CFG-1), `crates/repark-functions` (Spark-semantics
scalar/aggregate function shims, tier 3), `crates/repark-spark` (the Spark-SQL door: router +
`SparkDialect` + `SparkExtension`), `crates/repark-ta` (bit-exact TA-Lib kernels + the optional
window-UDF layer, tier 3), `crates/repark-sql` (the ANSI/Trino-flavoured door: `AnsiDialect` +
guard set + wrong-door sniff + the curated `WITH (…)` vocabulary), `crates/repark-ml` (native ML
estimator kernels, tier 3), `crates/repark-distributed` (distributed execution, tier 3, role
`runtime`: the `DistributedExecutor` seam and the local executor, with Ballista behind the
off-by-default `cluster` feature — seeded by BALLISTA-M1-A), and `crates/repark-python` (the
PyO3 cdylib, **tier 4 "bindings"**).
The Python tree ships `python/repark-parity` (the parity harness + census machinery + report
comparator) and `python/repark` (the PySpark facade wheel, published to PyPI — see
[STATUS.md](STATUS.md) "Release state").

## Contents

- `Cargo.toml` — **also the version SSOT (release PR, 2026-08-14):** `[workspace.package] version` (1.4.2) is the single release version; maturin injects it into the wheel (pyproject `dynamic`); bump here, nowhere else (internal deps are path-only — no version requirements to chase). With `Cargo.lock`, `rust-toolchain.toml`, `rustfmt.toml`, `clippy.toml`, `deny.toml`,
  `.cargo/` — Rust workspace + tooling. `[workspace.dependencies]` is the single version table
  (the four Ballista crates and, since the BALLISTA-M2-A seed of 2026-09-11, `datafusion-proto` are
  pinned `=54.1.0` and consumed only behind `repark-distributed`'s `cluster` feature);
  workspace lints (`unsafe_code = "forbid"`) and the clippy `disallowed-methods` panic/spawn bans
  are in force. **IO-ORC-1 (2026-09-16):** `orc-rust 0.8.0` (`default-features = false`, sync
  reader) in `[workspace.dependencies]` for the read-only ORC scan (owner ruling Q-15B-1),
  plus the codec crates the scan decodes with (DEFLATE/zlib, LZ4, LZO, Snappy, Zstandard).
  **ICE-READ-PERF-0 (2026-09-19):** `bytes = "1"` — already in the lock under the fork's
  storage traits — is declared for `repark-iceberg`'s counting `Storage`; no new package.
  **ICE-MIXED-CASE-1 run 22b (2026-09-18, Q-22b-MC-1):** `stacker 0.1.25` — already in the lock
  under DataFusion's `recursive` — is declared for `repark-core`'s column repair, which runs
  each poll of Spark-door planning on a stack grown to the statement's nesting depth.
  The iceberg* `[patch.crates-io]` family is a single shared `rev` (five lines);
  each dedicated bump is one row in the [docs/fork-sync.md](docs/fork-sync.md) pin-history table.
  **RP-1 (2026-08-23):** `5e7b2e4` (F-0 / F-1 / F-2 / F-8a); DataFusion family frozen.
  **RP-2 (2026-08-27):** `ce92a7bf` (F-3 / F-5 / F-13 / F-7 U1+U2); DataFusion family frozen.
  **RP-3 (2026-08-30):** `d408da42` (F-7 U3 / F-16 / F-9 / F-15 / F-14 / F-17 / H7-P1); DataFusion family frozen.
  **RP-4 (2026-08-31):** `33be9a0` (F-7 slice 1 / F-6 carry / #241 test pin); DataFusion family frozen.
  **RP-5 (2026-09-01):** `00cdde0` (F-6b / F-6c / F-8 / F-16r / R91); DataFusion family frozen.
  **RP-6 (2026-09-01):** `fb0cacfa` (PR-1..PR-6B / #250–#257); DataFusion family frozen.
  **RP-7 (2026-09-02):** `ff4764d3` (F-18 `#260`, the Spark-equal DV container close); DataFusion family frozen.
  **RP-8 (2026-09-03):** `c1d6c9de` (F-19/F-20 `#261`, F-21 `#262`, F-22 `#263` — the close owns the legacy-delete merge and the fanout drains ascending); DataFusion family frozen.
  **RP-9 (2026-09-03):** `594bdbe5` (F-23 — the DV close skips the data-manifest walk on the pure-DV path when `known_partitions` is complete); DataFusion family frozen.
  **RP-10 (2026-09-04):** `85a4aaf0` (F-25 — `validate_fresh_dvs_only` stops once every `added_dvs` key is found); DataFusion family frozen.
  **RP-11 (2026-09-04):** `189a73ed` (F-24 `#266` — v3 parquet-to-DV honours `min-input-files=5`); DataFusion family frozen.
  **RP-12 (2026-09-05):** `79119643` (F-26 `#267` DV container close `known_partitions`; F-CATIO `#268` one load per planning round + shared `ObjectCache`, default off — RePark wires it in a follow-up unit).
  **RP-13 (2026-09-05):** `2ed39cb0` (F-28 `#269` Arrow-kernel partition splitter; F-CATIO-KEY `#270` the shared manifest cache stores the raw parse — `PERF-CATALOG-LINEAGE-CACHE-1` FIXED; the default-ON flip is the next unit; F-27 rides RP-14).
  **RP-14 (2026-09-06):** `8bc325a3` (F-27 `#271` count(*) folds from exact scan statistics, empty projections read no column bytes, small tables split for parallel scans — the PERF-ICE-SCAN-1 pins un-skip).
  **RP-15 (2026-09-06):** `85db42f2` (F-RDF-EVO-1 `#272` compaction reads old files through the current schema — `RDF-SCHEMA-EVO-1` FIXED, the owner's post-`ADD COLUMN` `rewrite_data_files` refusal).
  **RP-16 (2026-09-11):** `090bc821` (F-WRITE-COMPRESS-1 `#276` — `INSERT INTO` data files carry the table's `write.parquet.compression-codec`, default zstd, instead of parquet-rs's UNCOMPRESSED; riders `#273`–`#275`, `#277`).
  Optional `mimalloc = "0.1"` (conductor-19 AL-1a; default-off `allocator-mimalloc` on
  `repark-python` only — not a family pin).
  **RP-17 (2026-09-12):** `41e25ba2` (F-WRITE-COMPRESS-2 `#278` — the maintenance, COW/MoR rewrite and position-delete writers honour `write.parquet.compression-codec` too, closing the four residue writer sites; RePark re-measures AP-1 a third time under one codec).
  **RP-18 (2026-09-12):** `9e3522e3` (F-REWRITE-SIZE-1 step 2 `#280` — the maintenance rewrite disables the dictionary per column from the input footers, so compaction no longer writes dead dictionary pages, and an unset compression level means zstd 3 like Java; RePark re-measures AP-1 a fourth time).
  **RP-19 (2026-09-12):** `3ebf7d36` (F-S3ROOT-1 `#281` — a bare-bucket object-store location, every S3 Tables table's, resolves to the bucket root like Java's S3URI, for S3, GCS and OSS; the S3 Tables orphan door itself is ORPHAN-S3TABLES-1).
  **RP-20 (2026-09-14):** `edc38c6a` (F-GLUE-REPLACE-1 `#282` — the Glue catalog publishes a staged `CREATE OR REPLACE` through a version-id-checked `UpdateTable`, read-validating the staged metadata first, so a gold dbt model rebuilds on Glue).
  **RP-21 (2026-09-17):** `75da2b58` (F-ICE-NAN-PUSHDOWN-1 `#284` — `= NaN` / `IN (NaN)` push down as `is_nan`, never as a NaN literal, so Iceberg scans return the NaN rows; F-ICE-HADOOP-VN-1 `#286` — a Hadoop `vN` metadata commit exclusive-creates its version file and a stale writer gets a typed conflict instead of overwriting another writer's commit).
  **RP-22 (2026-09-17):** `96fc9f1f` (F-RDF-OPTIONS-1 `#283` — `rewrite_data_files` options parity with Java 1.11: one commit by default, partial progress, `rewrite-all`, `output-spec-id`, job order; F-SORTED-INSERT-1 `#287` — `INSERT INTO` sorts each writer stream by the default sort order and stamps `sort_order_id`; F-PROMOTE-READ-1 `#285` — values written before a type promotion read under the promoted type).
  **RP-23 (2026-09-17):** `4151b488` (F-EVO-SCAN-1 `#289` — scans and DML after ADD/RENAME COLUMN bind the current schema; F-RTAS-OPS-1 `#290` — a staged replace can commit overwrite/delete like Java's RTAS; F-TARGET-FILE-SIZE-1 `#288` — the rolling writer checks the target every thousand rows, dictionary-OFF engine writes, new tables stamp zstd).
  **RP-24 (2026-09-17):** `8fb44a39` (F-OCC-SCOPED-1 `#291` — serializable conflict validation is filter-scoped: every concurrently added or deleted file is tested against the operation's conflict-detection filter through its own spec's partition projection before inclusive metrics, as Java's `ManifestGroup.filterData`, and a disjoint-partition concurrent commit rebases and retries instead of aborting; the `AlwaysTrue` default is unchanged).
  **RP-25 (2026-09-18):** `64705c99` (F-NESTED-EVO-1 `#292` — nested struct/list/map children project by field id; a child the file lacks fills from its `initial-default` or NULL, a required one without a default is refused, identical nested columns pass through, and a struct that mixes stamped and unstamped nested ids fails loud).
  **RP-26 (2026-09-18):** `8477b249` (F-HADOOP-VN-REPLACE-1 `#293` — a stale staged Hadoop replace conflicts instead of splitting the brain; F-CHERRYPICK-WAP-ORDER-1 `#293` — a duplicate WAP cherry-pick refuses with Java's text; F-UPDATE-SCHEMA-SAME-1 `#293` — a no-op schema update commits nothing).
  **RP-27 (2026-09-18):** `fdaa82d4` (F-OCC-EXEC-1 `#294` — plain-WHERE DELETE/UPDATE scope conflict validation by their own scan filter; unsound prune conversions are dropped).
  **RP-28 (2026-09-18):** `3296ffc7` (F-FORK-ASKS-22A `#296` — `hour` on nanosecond timestamps; COW DML and binpack output sorted by the default order and stamped).
  **RP-29 (2026-09-18):** `9e67e000` (F-LIST-INSERT-1 `#295` — nested fields relabelled to the Iceberg types on every write; `INSERT … VALUES` into nested columns; one advertised schema).
  **RP-30 (2026-09-18):** `18ab9761` (F-SHED-295 `#297` — relocated comment lines shed, no behaviour change; F-RP-SUMMARY-USER-1 `#298` — a caller's `replace-partitions` summary value wins).
  **RP-31 (2026-09-18):** `50350e33` (F-LIST-NULL-ACCESSOR-1 `#299` — `IS [NOT] NULL` on list, map and struct columns in DELETE/UPDATE; F-ROWID-ORDER-1 `#300` — a DataFusion INSERT commits its data files in ascending partition value, then task index).
  **RP-32 (2026-09-18):** `e3eef24f` (F-RDF-COW-BYTES-1 `#301` — `rewrite_data_files` keeps parquet position deletes that still apply; merging commits retire delete files older than every live data file; F-RDF-GRANULARITY-1 `#302` — `rewrite_data_files` output count follows Java's read-split planning).
  **RP-33 (2026-09-19):** `587d3592` (F-RPD-COMMITS-1 `#304` — `rewrite_position_delete_files` makes one replace commit with file-scoped outputs per referenced data file; dangling positions dropped, data sequence preserved).
  **RP-34 (2026-09-19):** `43fcd243` (F-PARQUET-SIZE-1 `#306` — fork-written parquet drops the `ARROW:schema` footer key and writes Java's `iceberg.schema`, plus `delete-type` on delete files; F-DANGLING-DV-COMMIT-1 `#305` — every merging commit drops the deletion vectors of removed data files).
  **RP-35 (2026-09-19):** `7bd2fea3` (F-AVRO-NAME-1 `#308` — Avro field names in manifests are sanitised as Java's `AvroSchemaUtil`; a partition column such as `my col` writes and reads back, IPI-52).
  **RP-36 (2026-09-19):** `fa77fb2b` (F-TS-PUSHDOWN-1 `#312` — a filter on a timestamp column with a timezone-bearing literal reaches the Iceberg scan predicate; F-PAGE-PRUNE-1 `#310` — page-index row selection on by default on the DataFusion scan, knob `iceberg.row_selection_enabled`).
  **RP-37 (2026-09-19):** `27e0d5fa` (F-META-DELETE-1 `#309` — `Table::can_delete_using_metadata`, Java's metadata-only DELETE decision, API only until RePark routes DELETE through it; F-CATALOG-CACHE-1 `#311` — the S3 Tables and Glue builders accept the scoped, byte-bounded metadata cache and the shared manifest cache, both off until RePark passes them).
  **RP-38 (2026-09-19):** `f3bdd598` (F-FOOTER-CACHE-1 `#316` — a bounded, shared Parquet footer cache keyed by storage context, path and file size, on every catalog builder, default off in the fork and on in RePark at 64 MiB; F-METRICS-CONFIG-1 `#313` — Java's table metrics config in every fork writer; F-TRANSFORM-ARROW-TYPES-1 `#315` — truncate/bucket over LargeBinary and view types).
  **RP-39 (2026-09-19):** `44834673` (F-EXACT-COUNT-1 `#317` — the scan's exact row count sums the planned data files' record counts, deduplicated across split tasks, instead of trusting a snapshot summary a `replace` could write as 0, and an empty table reports `Exact(0)`; F-RM-DELETES-1 `#318` — `rewrite_manifests` gains the `rewrite_delete_manifests` opt-in; F-COMMIT-JITTER-1 `#320` — Java's commit-retry schedule, jitter included).
  **RP-40 (2026-09-20):** `882f1300` (F-RPD-TARGET-SMALL-1 `#307` — the delete-file parquet layout follows Java and the RPD selection rule is measured identical, leaving a declared byte-size residue; F-LOCATION-1 `#314` — Java's location providers and `write.metadata.path`; F-PAGE-PRUNE-2 `#319` — page selection costs nothing when it prunes nothing; F-WRITE-ORDER-TRANSFORM-1 `#321` — transform sort terms; RDF-SORT-1 `#323` — `rewrite_data_files` sort and zorder; F-RDF-SUMMARY-1 `#322` — replace summaries carry their added files).
  **RP-41 (2026-09-20):** `3ed905c6` (F-BRANCH-SCHEMA-1 `#324` — a branch read projects the table's current schema while tags and snapshot ids keep the snapshot's, and the current-table provider stops failing loud on a `SELECT *` after a snapshot-less RENAME; F-ADD-FILES-1 `#325` — the `add_files` action adopts existing parquet files in place with Java's name mapping, hive partition parsing and duplicate check; F-OUTPUT-SPEC-ID-1 `#328` — `merge_append` commits a two-spec change instead of panicking, and the DataFusion write path stops hard-wiring the default spec at every stage; F-RESIDUAL-NESTED-NAME-1 `#329` — the residual of a nested predicate keeps its full column name instead of the leaf, which had rebound it to a different top-level column and folded it to `AlwaysFalse`, silently losing rows on an ordinary scan; F-S3-FIXTURE-RACE-1 `#327` — test-only).
  **RP-42 (2026-09-20):** `886b94c1` (F-POSDEL-SCAN-1 `#332` — the `position_deletes` metadata table becomes scannable: a scan plans its delete manifests and serves Java's `file_path`/`pos`/`row`/`partition`/`spec_id`/`delete_file_path` schema, with a declared residue on manifest pruning, whose ledger clause C-010 is demoted PROVEN -> OPEN because both `ManifestEvaluator`s still run under `AlwaysTrue`; F-CHANGELOG-READER-1 `#333` — an Arrow reader for `ChangelogScanTask` that serves `changelog_arrow_fields()` field-by-field and refuses a `DeletedRows` task loud with `FeatureUnsupported` instead of reading it as a whole-file delete).
  **RP-43 (2026-09-20):** `df62cdee` (F-CONTAINER-ACCESSOR-1 `#326` — a nested single-leaf STRUCT root reads its leaf correctly again: parquet 58.4's async predicate-result cache reports no definition levels through `CachedArrayReader`, and `without_nested_types` misses single-leaf non-LIST STRUCT roots, so the predicate pushdown is kept and the predicate cache is disabled only when the root is a single-leaf non-LIST group whose projection mask selects that leaf).
  **RP-44 (2026-09-21):** `caebaf7e` (`#335` — `rewrite_data_files` gains the branch option and `rewrite_manifests` gains `cluster_by_columns`; `#334` — a hand-rolled ORC data-file writer; F-ADD-FILES-SUMMARY-1 `#337` — `add_files` commits omit the untrusted changed-partition-count summary key).
  **RP-45 (2026-09-21):** `97f9b8a3` (`#336` — ORC column metrics under MetricsConfig, mirroring the parquet writer; F-RDF-DETERMINISTIC-ORDER-1 `#338` — `rewrite_data_files` sorts plan_file_groups partitions; F-STAGE-ONLY-1 `#330` — staged (WAP) commits on every write action, the wap.id lookup and publish_changes; F-META-COLS `#340` — the `_deleted` scan mode, `_spec_id`/`_partition` serving, snapshot-scoped metadata tables and schema-first name resolution).
  **RP-46 (2026-09-21):** `311b9fa4` (`5a317f07` — WAP `stage_only` + snapshot properties threaded from `IcebergTableProvider` to the commit arms, unwired until repark#787; `3e4d0bd7` — the IPI-41 `AnyFileWriter` format-selection seam; `#342` — F-RM-SORTBY-PACK-1 `sort_by_columns` sort-then-pack on `RewriteManifestsAction`; `#343` — IPI-41 D-4 sites 1+2, INSERT + CoW rewrite through `AnyFileWriter`).
  **RP-47 (2026-09-22):** `604edca0` (`#344` — IPI-41 D-4 sites 3+4, v2 position deletes and compaction through `AnyFileWriter`; RePark half — ORC and Avro Iceberg data files, IPI-41).
  **PR-B hadoop naming (2026-09-24):** `84f92593` (`#347` — the MemoryCatalog property `metadata-naming` = `uuid` | `hadoop`, constant `iceberg::memory::MEMORY_CATALOG_METADATA_NAMING`; hadoop mode writes `v<N>.metadata.json` plus `metadata/version-hint.text` and refuses `rename_table` with `FeatureUnsupported` "Cannot rename Hadoop tables"). The workspace built unchanged, so no call site needed a fix. `cargo update` touched only the six fork packages. The `Catalog` trait file (`crates/iceberg/src/catalog/mod.rs`) is byte-identical between `604edca0` and `84f92593`, so the `NamespaceScopedCatalog` forwarding audit holds unchanged.
  **RP-48 snapshot order + hadoop drop (2026-09-24):** `714a5fb2` (`#348` — Hadoop-mode `drop_table` removes the metadata chain and `drop_namespace` refuses a non-empty namespace; `#349` F-SNAPORDER-1 — every written metadata file lists `snapshots` by sequence number, then timestamp, then id, where the fork wrote HashMap order before, the cause of the TP-SUMMARY-PARTITION-LIMIT flake). The workspace built unchanged; `cargo update` touched only the six fork packages.
- `crates/` — the Cargo workspace members (the engine). See [crates/map.md](crates/map.md).
- `pyproject.toml`, `.python-version`, `uv.lock` — the **uv workspace root** (virtual — not
  itself a package): the member list, the `dev` dependency group, and the Ruff config (line 100).
  Both members are declared (`python/repark`, `python/repark-parity`); the three facade
  per-file-ignore blocks (`spark/ml/**`, `spark/session/**`, `spark/dataframe/**`) are
  **load-bearing**, not style — they are how the r26 region splits keep their pre-split
  import paths after the Q1 re-home (design §2.3 / §4 Q1). **PYC-4:** the tests glob is
  split — `python/repark-parity/tests/**` does not inherit ANN201/ANN202 (the ten
  unannotated returns in `test_compare.py` became visible and annotated). **PYC-5:**
  facade tests dropped ANN201 (isolated count 0); ANN202 stays for private helpers.
  **PYC-6:** both tests globs keep `D` so the presence ratchet cannot flag tests.
  **PROC-1 (2026-08-25):** `[tool.ruff] extend-exclude` carries the recorded Critic evidence
  (`task/mw-6-critic-evidence/`), which `.typos.toml` excludes too — verbatim oracle output a
  linter must not rewrite, the same rationale as `task/census/`.
  Isolated `make py-test` / ci.yml `parity-harness tests` pass `--with pydantic` because
  `--no-project` ignores package metadata.
  `uv.lock` is checked in from phase 3 on and is validated, never rewritten, by `uv lock --locked`.
- `Makefile` — developer command surface (`make help`). `make ci` is the canonical gate;
  `make verify` = ci + rust-test (JVM-free, native-build-free); `make preflight` = verify +
  `py-test-facade` + `py-test-parity-cap` + `py-test-dbt` + audit + workflow lint (G14,
  2026-08-12; `py-test-dbt` joined with DBT-1, 2026-09-04; `py-test-parity-cap` with
  PREFLIGHT-PARITY-1, 2026-09-09). `check-docs-links` (DOCS-LINKS-1, 2026-09-09) joins `ci`
  beside `check-docs-compaction`: every tracked `*.md`'s relative links resolve to tracked
  files, `#anchors` match GitHub-style heading slugs, and `docs:` ledger evidence cells follow
  the same rule; the measured baseline lives in `scripts/docs_links_allowlist.txt` (one
  `path:link` entry per line — the line is not part of the key, so a line shift never reds
  the gate — ratchet down only: a stale entry that matched no finding fails the gate).
  Tool pins match the
  workflow pins. The tier-2 `parity-live` target is dual-wired with
  [.github/workflows/parity-live.yml](.github/workflows/parity-live.yml) step for step — including
  its `uv sync` flag set, which is load-bearing rather than cosmetic (`uv sync` is exact: a missing
  `--extra` silently uninstalls a facade extra and the suite skips those tests instead of failing).
- `repo-manifest.toml` — the **machine-readable structural facts**: the component inventory
  (path / layer / status for every crate, delivered and planned), the current phase, the
  canonical gate commands, and the documentation index (which carries `divergences` →
  `docs/spark-sql-iceberg-parity.md`, the divergence registry, because ~16 live sources cite that
  path by name). It is a validated MIRROR, never a second
  source of truth — `scripts/check_manifest.py` (`make check-manifest`, in `make ci`) checks
  every field against the Cargo workspace, the Makefile, STATUS.md, the declared documents and
  the crate-root `map.md` files, and cross-checks each `layer` against the dependency-policy
  SSOT in `scripts/check_crate_dag.py`. Structural drift is a red gate, not a stale sentence.
- `.typos.toml`, `.taplo.toml`, `.pre-commit-config.yaml`, `.gitignore`, `.gitattributes`,
  `scripts/` — tooling/config and the mechanical guards (including the exact-baseline Rust and
  Python source file-size ratchets; `scripts/check_map_md.sh` is the map.md lockstep
  oracle — held on the pull-request diff by `make check-map-md` (`BASE ?= origin/main`) and
  ci.yml's `map.md guard`, warn-only on the hook — and `scripts/sync_map_md.py` its content
  companion — every relative link in every map must resolve and no two list rows may share a
  first link; `make install-hooks` wires both; `.gitattributes` sets `map.md merge=union` so a
  local merge or rebase that touches the same map on both sides resolves by keeping both rows). `.typos.toml`'s `extend-words` carries the domain
  vocabulary the checker would otherwise "correct" — including the TA-Lib indicator names
  (`TEMA`, `CMO`) that arrived with `crates/repark-ta`; the lines are carried from the
  port-source pin's own config, never invented to silence a real misspelling. Its
  `extend-exclude` also covers the two Spark-written Iceberg fixture trees
  (`crates/repark-spark/src/tests/fixtures/` and, since TORTURE-1 step 4,
  `python/repark-parity/fixtures/torture/data/` — binary Avro/Parquet/Puffin, not prose).
  `.gitignore` also
  carries `handback.json`, the hand-back artifact an agent writes at the lane root: a `git add -A`
  picked it up twice during H3-SPILL-1, and it is never repository content.
  pins: h3-spill-1/C-001
- `CODEOWNERS` — maintainer ownership. `LICENSE`, `README.md` — repo front matter.
- `python/` — the uv workspace members: the facade wheel (`repark`) and the parity harness
  (`repark-parity`). See [python/map.md](python/map.md).
- `examples/` — runnable notebooks for humans (currently one notebook touring the torture-dataset
  families). Illustration, never a gate. The 1.1 (was v0.7) drift-gated examples live under
  [docs/examples/](docs/examples/map.md). See [examples/map.md](examples/map.md).
- `docs/` — contracts, ADRs, the port plan, per-tier manuals, `docs/guide/` (the **user-facing**
  guides — install, session + conf, the DataFrame API, the two SQL doors; the only docs written
  for a user rather than a contributor), `docs/examples/` (the 1.1, formerly v0.7, executable public-surface
  examples + inventory/backlog, gated by `make check-example-coverage`), and `docs/history/`
  (the archive of closed campaigns — the v1 → v2 port and the Agent-Agnostic Front-Door campaign,
  both off the normal read path; see [docs/history/map.md](docs/history/map.md)).
  `task/` — the rules in force (`lessons.md`), the process metrics ledger (`metrics.md`), the
  ledgers by state (`task/ledgers/`), and the live acceptance inputs (`task/port/`, the facade
  pin under `task/census/`); the
  backlog itself lives in [STATUS.md](STATUS.md). `briefs/` — slate briefs for campaigns that are
  still running (currently the V2 Engine Hardening slate; a closed campaign's slate is archived
  with it, and between campaigns the directory holds only its `map.md`).
  `.github/` — tier-1 CI + Dependabot.   `PROJECT.md` — north-star charter. `STATUS.md` — the
  single source of truth for current state (release state, delivery, active workstreams, deferred
  work). Truth-up 2026-09-03: v1.0.0 cut (release PR, four files); 2026-09-02: v0.6.0 shipped; RP-5 ledger pointer refiled to archive/2026-09/; LIVE-v3 added the wired-but-unmeasured live v3 legs to the v3 workstream and the `V3-ROWID-3` line to Known correctness issues, paying for both by compacting the V3-4/V3-6/OD-3b restatements that already live in the north star and the registry (the 25,000-byte ceiling is dual-pinned and was not raised). pins: live-v3-aws-legs/C-004 `AGENTS.md` — **the single authoritative contributor contract** (holds the precedence
  chain, invariants, safety boundaries, the markdown document lifecycle — which class every
  doc belongs to and what retires it — and the version-pin duties: metadata-projection shim,
  Catalog wrap, IcebergSchemaProvider name-directory freeze; written for any human or agent,
  names no tool).
  `ARCHITECTURE.md` — component boundaries, the crate DAG, and the three runtime flows.
  `DEVELOPMENT.md` — build / test / verify, the `make` targets, the CI surface, troubleshooting.
  `CLAUDE.md` — the **Claude adapter** (tool mechanics only; zero authoritative facts). The owner
  ruling at the start of `AGENTS.md` and its adapter copy are byte-preserved by
  `make check-owner-ruling`; the model-family restriction itself is held by compliance and review.
  `CONTRIBUTING.md` / `SECURITY.md` — public-repo policy.
- `.agents/` — tool-neutral + per-tool agent adapters (`common.md` + `claude.md` + `codex.md` /
  `cursor.md` stubs) and `skills/` (agent-facing runbooks: release-to-PyPI, context-doc
  truth-up, disk headroom; the portable code-quality convention reasoning; and, since
  2026-08-25, the SEPMO control plane and CCC, the Critic engine it binds);
  each is a thin pointer into the spine, carrying no authoritative facts. See
  [.agents/map.md](.agents/map.md).
- `.claude/` — one entry, `skills/`, a symlink to `../.agents/skills`. Claude Code only discovers
  skills under `.claude/skills/`, so the symlink makes the `.agents/skills/` runbooks natively
  invocable without giving them a second home to drift from. See [.claude/map.md](.claude/map.md).

## I want to...

| ...do this | go to |
|---|---|
| Know the current state (release / delivery / what's next) | [STATUS.md](STATUS.md) |
| Learn to *use* repark (install, session, DataFrame, SQL doors) | [docs/guide/map.md](docs/guide/map.md) |
| Find or add a gated public-API example | [docs/examples/map.md](docs/examples/map.md) |
| Understand the project intent / north star | [PROJECT.md](PROJECT.md) |
| Follow the authoritative contributor contract | [AGENTS.md](AGENTS.md) |
| Understand the architecture / crate DAG / runtime flows | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Build / test / verify locally (setup, `make` targets, CI) | [DEVELOPMENT.md](DEVELOPMENT.md) |
| Onboard as an agent (any tool) | [.agents/map.md](.agents/map.md) |
| Understand the port plan / what arrives when | [docs/port/PLAN.md](docs/port/PLAN.md) |
| Read/extend the testing contract | [docs/testing.md](docs/testing.md) |
| Know which class a markdown doc belongs to, and what retires it | [AGENTS.md](AGENTS.md) "Markdown document lifecycle" |
| Find how repark differs from Apache Spark, and why | [docs/spark-sql-iceberg-parity.md](docs/spark-sql-iceberg-parity.md) (the divergence registry) |
| Understand why a load-bearing decision was made | [docs/adr/map.md](docs/adr/map.md) |
| Operate under the SEPMO control plane | [.agents/skills/sepmo/map.md](.agents/skills/sepmo/map.md) |
| Read the code-quality conventions and why each is held by a linter, a gate, or review | [.agents/skills/code-quality/SKILL.md](.agents/skills/code-quality/SKILL.md) |
| Read the portable engineering method | [.agents/skills/engineering-method/SKILL.md](.agents/skills/engineering-method/SKILL.md) |
| See in-flight work / lessons | [task/map.md](task/map.md) |
| Read how the engine got here (the archived port record) | [docs/history/port-v2/README.md](docs/history/port-v2/README.md) |
| Read how the front door got here (the archived campaign record) | [docs/history/frontdoor/README.md](docs/history/frontdoor/README.md) |
| See what a closed campaign cost, caught and missed | [task/metrics.md](task/metrics.md) |
| Touch CI | [.github/map.md](.github/map.md) |
| Read a running campaign's slate brief | [briefs/map.md](briefs/map.md) |
| Navigate the engine crates | [crates/map.md](crates/map.md) |
| Navigate the Python tree | [python/map.md](python/map.md) |
| Build the wheel / run the facade suite | [python/repark/map.md](python/repark/map.md) |
| Read a runnable example / the dataset tour notebook | [examples/map.md](examples/map.md) |
| Run or compare a census | [docs/port/census.md](docs/port/census.md) |
| Run the canonical gate | `make ci` (see `make help`) |
| Declare a new crate / doc / gate command structurally | [repo-manifest.toml](repo-manifest.toml) (validated by `make check-manifest`) |
| Understand the mechanical guards | [scripts/map.md](scripts/map.md) |
| Understand the cargo tooling config | [.cargo/map.md](.cargo/map.md) |

## Pointers

- Up: — (repository root)
- Related: the private v1 repository is the port source; this repo is the public V2 target.

## Debug

First checks: `make ci`, then `make help` for the full target list. CI mirrors `make ci`.

| Symptom | First check |
|---|---|
| A cargo target loudly no-ops | Should no longer happen — the workspace has members; see the Makefile header |
| Pre-commit hook warns on map.md lockstep | the touched directory's map.md rides in the same pull request — ci.yml's `map.md guard` (`make check-map-md`) holds it |
| A gate is unclear | `make help`; [docs/testing.md](docs/testing.md) and [AGENTS.md](AGENTS.md) are authoritative |
| `manifest: FAIL …` | `bash scripts/check_manifest.sh` — [repo-manifest.toml](repo-manifest.toml) disagrees with the workspace, a doc, a make target, STATUS.md, or a crate map ([scripts/map.md#debug](scripts/map.md) has the per-message table) |

FNP-8 (2026-09-07): STATUS links the measured residuals in the parity registry;
implementation and oracle evidence remain in the staging unit ledger until delivery.
