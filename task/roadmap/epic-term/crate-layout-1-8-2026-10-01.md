# The crate layout through 1.8 — fourteen crates, one split, four arrivals (ruled 2026-10-01)

**Date:** 2026-10-01 · **Ruled by:** the owner ("I love that layout"), in discussion with a Claude session (claude-fable-5-1) · **Measured on:** `origin/main` at `db3a1f37` (v1.5.1) · **Builds on:** [../mid-term/crate-layout-review-2026-09-30.md](../mid-term/crate-layout-review-2026-09-30.md) (the test-placement review), [roadmap-design-plan-2026-08-29.md](roadmap-design-plan-2026-08-29.md) (cards 1.3, 1.6, 1.7), [unified-database-query-cdc-silver-plan-2026-09-13.md](unified-database-query-cdc-silver-plan-2026-09-13.md) (the three independent lifecycles), [deterministic-silver-layer-compiler-2026-09-12.md](deterministic-silver-layer-compiler-2026-09-12.md).

## 1. The ruling in one table

| crate | tier · role | owns | edges down | arrives |
|---|---|---|---|---|
| `repark-common` | 0 · foundation | names, errors, shared types | — | shipped |
| `repark-iceberg` | 1 · table service | catalogs (Glue, S3 Tables, Postgres, memory/Hadoop; **REST joins here**, no separate crate), DML, maintenance over the fork | common | shipped |
| `repark-connect` | 1 · table service | Postgres and SQL Server providers, pushdown, pools, type maps (card 1.6) | common, core | **1.6** |
| `repark-cdc` | 1 · table service | logical-replication capture → Arrow → Bronze append, capture checkpoints | common, iceberg, core | **1.7** |
| `repark-io` | 1 · table service | smart CSV, Excel, JSON, IPC, Avro, Hive-partitioned directory discovery (card 1.3) | common, core | **1.8** |
| `repark-core` | 2 · engine | Session, planning, `df_guards` (ATTR-ID-1), path writes, and the **`silver/` module**: the `SilverPlan` compiler and publication | common, iceberg | shipped; `silver/` at 1.7 |
| `repark-spark-dialect` | 3 · door | the Spark SQL grammar: AST, normalize, keyword case, literals and literal typing, rewrites, collation, windows, `void`, the type table | common, core, functions | the tidy window |
| `repark-spark` | 3 · door | the router and the four command families `ddl/`, `dml/`, `inspect/`, `procedures/` | spark-dialect, common, core, iceberg, functions, ta | shipped, reshaped in the tidy window |
| `repark-sql` | 3 · door | the ANSI door | common, core, iceberg, functions, ta; **`repark-spark` as a dev-dependency only** | shipped, edge corrected in the tidy window |
| `repark-functions` | 3 · tool | Spark function semantics and the cast tables | — | shipped |
| `repark-ta` | 3 · tool | unchanged | core | shipped |
| `repark-ml` | 3 · tool | unchanged | — | shipped |
| `repark-crawler` | 3 · tool | bounded discovery runs, profiling under a budget, the evidence store, proposed Bronze and Silver specifications and diffs | common, core, connect | **1.7** |
| `repark-distributed` | 3 · runtime | `DistributedExecutor`, Ballista M1, feature-gated | core | shipped |
| `repark-python` | 4 · bindings | the thin PyO3 adapter, the only `unsafe` | common, core, spark, sql, functions, ta, ml | shipped |

Every crate carries one integration binary, `tests/it/main.rs`, with one module per former test file (option 1 of the 2026-09-30 review), so `src/` holds product code only.

```
tier 4  bindings     repark-python

tier 3  doors        repark-spark ──► repark-spark-dialect       repark-sql
        tools        repark-functions   repark-ta   repark-ml   repark-crawler
        runtime      repark-distributed

tier 2  engine       repark-core  (… silver/)

tier 1  services     repark-iceberg   repark-connect   repark-cdc   repark-io

tier 0  foundation   repark-common
```

## 2. Why these names

**`repark-crawler`, `repark-cdc`, and no `repark-silver`.** The unified plan rules three independent lifecycles (its §3–4, 2026-09-13): the crawler/planner is a bounded run that inspects source catalogs, optionally profiles, proposes mappings and exits, never publishing data; capture is the snapshot/CDC producer with its own checkpoint state; Silver execution compiles an accepted plan over a pinned Bronze snapshot and publishes. The crawler and the producer each get a crate because each has its own lifecycle, credentials and state. Silver execution is "a typed plan → a DataFusion plan → one publication commit", which is what `repark-core`'s Session already does for the two doors, and its S-0/S-1 foundation is chartered inside 1.5; it starts as `repark-core/src/silver/` and earns a crate only if it grows a grammar of its own. The owner's first instinct, `repark-silver → repark-crawler`, would have put the component that commits under the name of the component the plan says must not; the three-way reading keeps the plan's words.

**Not `repark-streaming`.** Capture is one producer with one checkpoint. "Streaming" names Structured Streaming, which the v1.5 parity gate carves out (ruling C-1, IPI-47) and the roadmap places at 2.2 as micro-batch change-data reads; a crate by that name at 1.7 would collect every such finding. The plan's own word is capture; `repark-cdc` is the one people search for.

**`repark-spark-dialect`, not `repark-spark-ddl`.** Measured on `main`, `repark-spark` is 143k lines of Rust, of which 87k (61 %) is `src/tests/`. The product 56k falls into six families:

| family | modules | ~lines |
|---|---|---|
| dialect | `spark_ast`, `dialect/`, `normalize`, `keyword_lower`, `spark_literals`, `spark_literal_typing`, `spark_typed`, `spark_rewrites`, `collation`, `void_type`, `type_table`, `window_range`, `time_window`, `bare_*`, `uuid_cast`, `update_cast`, `spark_tree_string`, `matrix`, `extension` | 16k |
| ddl | `create_table`, `ctas`, `alter*`, `nested_column_ddl`, `replace_columns`, `column_move`, `ref_ddl`, `namespace_ddl`, `use_ddl`, `view_ddl`, `table_props_*`, `format_version`, `sort_order_parse`, `local_fs_ddl`, `truncate`, `wap`, `write_to_branch`, `time_travel` | 15k |
| inspect | `describe_*`, `show_*`, `metadata_tables`, `catalog_ops` | 5k |
| dml | `insert_*`, `merge*`, `append_with_options`, `write_options` | 8k |
| procedures | `call/`, `call_args` | 9k |
| router | `router/` | 3k |

DDL is one of four side-effecting families, and the router that dispatches to all four would have to sit in the crate that depends on the pulled-out one. So the extraction goes the other way: the dialect (parse, normalize, type; no catalog, no side effects, no `repark-iceberg` edge) moves *below* into `repark-spark-dialect`, and `repark-spark` keeps its name, its `_native` binding and every `cargo test -p repark-spark` filter, reorganised as `router/ ddl/ dml/ inspect/ procedures/`. The dialect is also the part a Spark Connect analyzer would need at 1.12. "Dialect" is the word ARCHITECTURE.md already uses ("each door keeps its own grammar") and the `dialect/` module already exists. Had the commands been the extracted half, Spark's own word is `commands` (`RunnableCommand`, `execution/command/`), never `ddl`.

**No `repark-functions` split.** 74k lines, but the families are already directory-grouped and share the cast tables; grouping the flat root (option 3 of the review) is enough.

## 3. What the measurement surfaced, and what the layout fixes

1. **`repark-sql` → `repark-spark` is a product edge today** (`crates/repark-sql/Cargo.toml:59`) whose every use is in `tests/` (`SparkDialect`, `SparkExtension` for the cross-door two-session protocol) plus two fixture paths that join `../repark-spark/src/tests/fixtures`. ARCHITECTURE.md says the doors share nothing above tier 1. The edge becomes `[dev-dependencies]`, and the fixture paths move with the `tests/it/` unit, which must carry them: a hard-coded join into another crate's `src/tests` breaks silently when that directory moves.
2. **`scripts/check_crate_dag.py` declares six edges; the `Cargo.toml` files carry fourteen.** `repark-spark → iceberg / functions / ta / common`, `repark-sql → every lower crate`, and `repark-python → *` are undeclared. The table is trued up in the tidy window before any new crate adds a row, so the gate enforces the rule it states.
3. **`repark-functions` depends on nothing.** `repark-spark-dialect` will depend on it for typing. That is the right direction; nothing in functions ever imports the dialect.
4. **`repark-exec`** in PROJECT.md's crate list never materialised; its concerns live in `repark-core`. It is struck from the list, not built (PROJECT.md edit for the owner).

## 4. What stays out until after 1.8

| not a crate before 1.8 | why |
|---|---|
| `repark-silver` | a `repark-core` module until it earns a grammar (§2) |
| `repark-api` | an internal-API crate with one caller is a boundary nobody tests; it is cut when the second caller exists (the 1.12 server, or earlier if the migration scorecard re-opens the order — Q&A row 2026-10-01) |
| `repark-server`, `repark-server-spark-connect`, `repark-cli` | 1.12 (card 1.8 of the design plan) |
| `repark-streaming` | §2 |
| a REST catalog crate | the fork's `iceberg-catalog-rest` does the protocol; `repark-iceberg` gets a `CatalogKind::Rest` arm beside Glue and S3 Tables |
| `repark-exec` | struck (§3.4) |

## 5. The tidy window and its units

The window is **after the v1.5.2 merge queue drains and before the first 1.6 connector unit opens**: every unit is a pure move, and the cost of a move is merge conflicts with in-flight branches, so it runs when few are open. Each unit is a clerk-tier Muse round (moves, no logic), gated by an identical `cargo test -p <crate>` count before and after, `scripts/check_rust_file_size.py` with its `EXCEPTIONS` table re-keyed, map.md lockstep, and the DAG gate. Relocated code sheds its comments (the 2026-09-18 comment-gate ruling). No DIFF-PROBE or Opus verifier on a pure-move unit: the test-count match and the gates are the whole proof, and an Opus pass on a move has no catch rate behind it.

| # | unit | what moves | carries |
|---|---|---|---|
| T-1 | `tests/it/` for `repark-spark` | 87k lines of `src/tests/` into one binary, one module per former file; fixtures to `tests/fixtures/` | the two `repark-sql` fixture joins; every `--test <name>` filter in briefs, gates and CI becomes a module filter |
| T-2 | `tests/it/` for `repark-iceberg`, `repark-core`, then the rest | same shape | the `EXCEPTIONS` rows for `dynamic_flatten/tests.rs` and `catalog/tests/catalog.rs` |
| T-3 | the `repark-sql` Spark edge to `[dev-dependencies]`; the DAG table trued up to the fourteen real edges | one `Cargo.toml` line, one gate table | ARCHITECTURE.md's tier map names the dev-only edge |
| T-4 | `repark-spark` directories `router/ ddl/ dml/ inspect/ procedures/` | the modules of §2's table, `use` paths, five `map.md` files | nothing else |
| T-5 | extract `repark-spark-dialect` | the dialect directory becomes a crate; `repark-spark` imports it; a DAG row; a workspace member | the version bump mechanics name a new member |

The `#[path]` rule question from the 2026-09-30 review (allow `#[path]` for a `#[cfg(test)]` child module and nothing else, or keep the directories) needs its one-line answer before T-1, since T-1 decides whether the single-file test directories survive the move.

Then 1.6 opens with `repark-connect` on a clean DAG; 1.7 adds `repark-crawler`, `repark-cdc` and `repark-core/src/silver/`; 1.8 adds `repark-io`.

## 6. Decisions recorded

| id | decision | status |
|---|---|---|
| CL-1 | `repark-crawler` (tier 3 tool) and `repark-cdc` (tier 1 service) are the 1.7 crates; Silver execution is `repark-core/src/silver/` | **ruled 2026-10-01** |
| CL-2 | no `repark-streaming`; the producer crate is `repark-cdc` | **ruled 2026-10-01** |
| CL-3 | the `repark-spark` split extracts the dialect below as `repark-spark-dialect`; `repark-spark` keeps its name and gains `router/ ddl/ dml/ inspect/ procedures/` | **ruled 2026-10-01** |
| CL-4 | the tidy window is post-v1.5.2 queue drain, pre-1.6; units T-1…T-5, clerk tier, no verifier on pure moves | **ruled 2026-10-01** |
| CL-5 | `repark-sql → repark-spark` becomes dev-only; the DAG table is trued up | **ruled 2026-10-01** |
| CL-6 | the `#[path]` rule for `#[cfg(test)]` children | **open** — one line from the owner before T-1 |
| CL-7 | strike `repark-exec` from PROJECT.md's crate list | **open** — PROJECT.md is the owner's file |

## Leaves this directory when

T-1…T-5 have merged and ARCHITECTURE.md's tier map shows the fourteen crates with their declared edges; then this file is the dated record and the design plan's cards 1.3, 1.6 and 1.7 name the homes above.
