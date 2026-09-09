# Cheap-tier slate — the owner's 2026-09-08 feature set, cut for mechanical-tier workers

**Date:** 2026-09-08 · **Author:** Claude Fable 5.1 (orchestrator) · **Base read:** `origin/main`
`f00ed9ea` (v1.1.1) · **Status:** owner-chartered in conversation 2026-09-08 (§0); each card earns
its ledger under `task/ledgers/staging/` when it starts.

**What this is.** The owner asked for eight items on 2026-09-08 and ruled on the open questions
the same day. This document turns them into work cards a cheap worker (GLM 5.3 Flash class via
`oc-worker`, or Muse Spark 1.3 via `muse-worker`) can execute one round at a time, with every
design decision made here so the worker never has to make one. The orchestrator keeps only
what the mechanical tier cannot hold: the seed commits that touch dependencies, the audit of
each hand-back, the gates, push, PR and the one Slack note.

Reading order for a worker: §1 (how a round runs), then the one card it was launched on.
Nothing else in this file is required reading for a round.

## 0. Owner rulings folded in (2026-09-08)

| # | Ruling | Where it binds |
|---|---|---|
| R-1 | Polars display shows **5 head + 5 tail** rows (polars' own default), not 5 + 10. | DISPLAY-POLARS-1 D-3 |
| R-2 | A lazy frame's `repr` **renders data with a count**, not the plan. | DISPLAY-POLARS-1 D-4 |
| R-3 | The recommended order stands (§4). | §4 |
| R-4 | `repark.toml` (roadmap v1.4, card CFG-1) is pulled **ahead of 1.2 and 1.3**. | CFG-1 |
| R-5 | The Ballista audit runs **as Milestone 0 of the Rust migration pilot** (the rust-unification brief), not as a separate track. | BALLISTA-AUDIT-0 |
| R-6 | Flipping the default display style may break `show()` parity pins; **test fixtures set `spark` explicitly**. | DISPLAY-POLARS-1 D-2 |
| R-7 | Work is packaged so a GLM-class worker does most of it; token cost is the binding constraint. | §1, every card's tier column |
| R-9 | **DESCRIBE type spelling (2026-09-09):** Spark's two spellings are both right — `long` for `printSchema`, `bigint` for `DESCRIBE`; one DDL-spelling function in `repark-spark` serves DESCRIBE, CTAS and the Python surface. | SQL-DESCRIBE-1 D-3 |
| R-10 | **Reading units and pins (2026-09-09):** a ledger whose header says `**Path:** READING` may mark clauses PROVEN on document evidence; the attestation block stays required. | LEDGER-READING-1 |
| R-8 | **The object stays a RePark DataFrame.** Polars is the example for the look and the names; no card returns a polars object from the Spark surface, adds polars as a runtime dependency, or changes what `repark.DataFrame` is. Polars is imported only inside tests, as an oracle, and skipped when absent. | DISPLAY-POLARS-1, DF-EAGER-1, X-1 |

## 1. How a card runs on the cheap tier

### 1.1 The loop (orchestrator side)

Exact commands for every line below, plus grants and stop rules for an unattended run:
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md).

1. **Seed commit first, when the card names one.** Dependency edits (`Cargo.toml`, `Cargo.lock`,
   `pyproject.toml`) are never a worker's; the orchestrator lands them on the unit branch before
   the first round.
2. Clone the unit branch to `/tmp/oc-<lane>` (never `~/CodeRepos`), `make install-hooks` in the
   clone.
3. Brief = **§1.2 preamble** + **the card's one step** + the launcher's hand-back contract. One
   step per round. A round that finishes early may take the next step only if the card says
   "may chain".
4. Launch through `systemd-run --user` (rounds exceed the 10-minute Bash cap), watch the run
   dir's `exit` file.
5. Read `handback.py <run-dir>`; on `HALT` answer with `--followup --resume`; on `CONCLUDED`
   audit the diff yourself (`git log origin/main..HEAD`, `git diff --stat`, then the files the
   card names) and re-run the card's gates yourself. Never trust a hand-back that names green
   gates without a matching commit.
6. Critic: `oc-worker --role critic` (edit-denied) or a Grok critic on a fresh clone. Never
   Opus. Findings go back as the next step's brief.
7. Orchestrator: `make preflight` alone, push by explicit URL, PR, squash-merge on green when
   the owner's grant covers the repo, one `notify.sh` message, ledger `move` in the last commit.

### 1.2 The preamble every brief carries (copy verbatim)

```text
You are the Actor for unit <UNIT> step <N> on branch <branch> in this clone. Work only in this
directory. Do exactly the step below; when it is done, run the gates listed, commit, and write
handback.json at the clone root. Never end a turn to report progress or emit placeholders.

Hard rules (violations are rejected at audit):
- NO comments in code: no `//`, `///`, `//!` in Rust, no `#` in Python beyond `# noqa`,
  no docstrings beyond the one-line docstring the thinness gate demands, no `#` in TOML,
  YAML, shell. Explanations go in the directory's map.md or the ledger.
- No dependency changes: never edit Cargo.toml, Cargo.lock, pyproject.toml, uv.lock, .github/.
- No `git push`, no `gh`, no `aws`, no `--no-verify`, no edits to STATUS.md or
  briefs/next-sequence.md (the orchestrator's departure edit).
- map.md in lockstep: any new or moved file is listed in its directory's map.md in the same
  commit; the pre-commit hook enforces it.
- Red first: write the pin, run it, confirm it FAILS on the base tree, then make it pass.
  Paste the red output into the ledger clause's evidence cell.
- Measure, never guess, any Spark/polars/DataFusion behaviour the step calls "measured".
- Hand back (status HALT, question in `questions`) the moment the step needs a decision the
  card did not make. Do not invent an answer.
- Commit identity: git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com
  Last line of every commit message: Authored-By: <MODEL-LINE>
  (GLM: `Authored-By: GLM 5.3 Flash (zai/glm-5.3-flash) <noreply@z.ai>`;
   Muse: `Authored-By: Muse Spark (muse-spark-1.3) <noreply@meta.ai>`;
   Grok: `Authored-By: Grok (grok-4.6) <noreply@x.ai>`)
- Ledger: task/ledgers/staging/<unit>-ledger.md holds the clause table (C-001…), one row per
  clause the card lists, Verdict OPEN until its pin is green; the Model: row names your model.
```

### 1.3 Tier legend used in the cards

| Tier | Who | What it is trusted with |
|---|---|---|
| **M** (mechanical) | GLM 5.3 Flash via `oc-worker` | Precisely specified edits, sweeps, measurement scripts, test scaffolds, docs mirrors. Hands back on any decision. |
| **I** (implementation) | Muse Spark 1.3 (`muse-worker`) or Grok 4.6 (`grok-worker`) | Multi-file edits under a fixed design; wiring across a crate boundary; Rust parsers and executors. |
| **O** (orchestrator) | this session | Seed commits, audits, gates, push, PR, Slack, departure edit, any step marked O. |

Each step names its tier. When a step is M and the worker hands back twice on the same
question, the orchestrator answers once in the card (a new D-row) and relaunches; it does not
escalate the tier for that.

### 1.4 Gates by language (the short list a round runs)

| Touches | Round gates (worker) | Pre-PR gate (orchestrator) |
|---|---|---|
| Rust | `cargo test -p <crate> <filter>`, then `make verify` | `make preflight` |
| Python facade | `.venv/bin/python -m pytest python/repark/tests/<file>.py -q`, then `make py-test-facade` when the card says so | `make preflight` |
| Docs only | `make check-docs-compaction`, `make check-ledgers` | `make preflight` |

`make develop` builds DEBUG natives; a card that measures time says so and asks for a release
build (`maturin develop --release`) explicitly. None of the cards below measure time except
PROFILES-1.

## 2. Cards

Card fields: **Home** (files), **Decisions** (pre-made, numbered D-n, binding), **Steps** (one
row per round, each with a tier), **Pins** (test names the worker writes red-first), **Done
when**, **Hand back when**, **Rounds** (estimate).

---

### Card SQL-DESCRIBE-1 — `DESCRIBE [TABLE] [EXTENDED|FORMATTED] <iceberg table>`

**Bug.** `repark.sql("DESCRIBE TABLE EXTENDED cat.db.t")` raises
`ParseException: Expected: end of statement, found: EXTENDED`. Cause, read on `f00ed9ea`:
`crates/repark-spark/src/router.rs` `try_preparse_intercepts` intercepts only
`DESCRIBE {NAMESPACE|DATABASE|SCHEMA} [EXTENDED]` (`describe_show.rs`
`try_parse_describe_namespace`); every other `DESCRIBE` falls to DataFusion's parser, whose
grammar has no `EXTENDED`.

**Home.** `crates/repark-spark/src/describe_show.rs` (parser + executor),
`crates/repark-spark/src/router.rs` (one new intercept arm after the namespace one),
`crates/repark-spark/src/tests/describe_table.rs` (new), `python/repark/tests/test_describe_table.py`
(new), `docs/spark-sql-iceberg-parity.md` (row), `crates/repark-spark/map.md` and
`python/repark/tests/map.md` (lockstep).

**Decisions.**

- **D-1 Scope of the intercept.** `DESCRIBE|DESC [TABLE] [EXTENDED|FORMATTED] <ident>` where
  `<ident>` resolves to an Iceberg table in a registered catalog. If it does not resolve (temp
  view, DataFusion-native table, metadata-table suffix like `t.snapshots`), the SQL falls
  through **unchanged**, so the existing Z6 pin ("`DESCRIBE <table>` is not shadowed") keeps its
  meaning for non-Iceberg names. Plain `DESCRIBE t` on an Iceberg table takes the new path too
  (Spark's shape, without the extended sections).
- **D-2 Output shape is the measured Spark shape**, three `string` columns `col_name`,
  `data_type`, `comment` — **measured 2026-09-09 (PR #428 ledger): `col_name` and `data_type` are
  `nullable=False`, `comment` nullable; the detailed section also carries a `Statistics` row;
  `FORMATTED` output is byte-identical to `EXTENDED`** — rows in Spark's order. The exact
  rows for an Iceberg table under the DataSourceV2 path are **measured in step 1** on the Spark
  4.1.2 on this box, never typed from memory. Expected families, to be confirmed by the
  measurement: the columns; a blank row and `# Partitioning` with `Part 0…` rows when the
  table is partitioned; under EXTENDED/FORMATTED a blank row and `# Metadata Columns`, then a
  blank row and `# Detailed Table Information` with `Name`, `Type`, `Location`, `Provider`,
  `Owner`, `Table Properties`.
- **D-3 Type spelling (owner ruling 2026-09-09, R-9).** Spark has two spellings and both are
  right: `printSchema` prints `long` (Spark's `simpleString`) and `DESCRIBE` prints `bigint`
  (the DDL / `catalogString`). Today both live in `crates/repark-python/src/dataframe.rs`
  (`arrow_type_key_at_depth` → `long`; `spark_array_element_simple_string_at_depth` →
  `bigint`), which `repark-spark` cannot reach. Step 2 moves the DDL spelling into a new module
  `crates/repark-spark/src/spark_type_names.rs` as `pub fn spark_ddl_type_name(&ArrowDataType)
  -> String` (every arm the Python function has today, byte-identical output), re-exported from
  the crate root, and makes `repark-python` call it (`repark-python` already depends on
  `repark-spark`); `long` stays where it is for `printSchema`. DESCRIBE converts the Iceberg
  schema to Arrow with the iceberg crate's `schema_to_arrow_schema` and spells from Arrow; the
  capture's `timestamp` for a `TIMESTAMP` column (Iceberg `timestamptz`) is the pin.
- **D-4 Missing table** raises `AnalysisException` with Spark's `[TABLE_OR_VIEW_NOT_FOUND]`
  text, by class identity on the facade (same pattern as `test_describe_namespace.py` Z4).
- **D-5 `Table Properties` value** renders Spark's bracketed comma list in key order, with
  `prop_key_is_secret` keys redacted the same way `DESCRIBE NAMESPACE EXTENDED` redacts.
- **D-6 `FORMATTED`** is accepted as a synonym of `EXTENDED` (Spark treats it so for v2 tables).
  Confirm in the measurement; if Spark's output differs, hand back.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | M | **Done 2026-09-09 (PR #428 branch `feat/sql-describe-1`, ledger).** Measurement. Under `REPARK_PARITY_LIVE=1` and `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` (the default `java` is 11) (see `python/repark/tests/_oracle_pins.py` for the harness), create in live Spark an Iceberg table with: a comment on one column, three types (`bigint`, `string`, `timestamp`), partition `days(ts)`, one table property `k=v`. Capture `DESCRIBE t`, `DESCRIBE TABLE t`, `DESC EXTENDED t`, `DESCRIBE TABLE FORMATTED t`, and a missing table, as `collect()` rows plus `.schema`. Write them verbatim into the ledger §"Oracle capture". No engine edits. |
| 2 | I | On the existing branch `feat/sql-describe-1` (clone it, not `main`). First the D-3 move (`spark_type_names.rs`, the Python call site, `make verify` green, one commit), then parser + executor. `try_parse_describe_table(sql) -> Option<Result<DescribeTable>>` in `describe_show.rs` (fields: catalog, namespace, table, extended); the router arm after the namespace arm; `execute_describe_table` loads the table through `catalog_handle(...)`, then builds the batch from `table.metadata()` (current schema, default partition spec, location, properties, current snapshot id, format version) to the step-1 shape. Rust tests: parser accepts the four forms, refuses `DESCRIBE EXTENDED` with no name, leaves `DESCRIBE NAMESPACE …` to the namespace parser, and a non-catalog name returns `None` from the intercept. |
| 3 | M | Facade pins `test_describe_table.py` (fixture: memory catalog, the same table as step 1), one test per D-row; live leg under `REPARK_PARITY_LIVE=1` compares rows to the capture. Parity doc row in `docs/spark-sql-iceberg-parity.md`; map lockstep; ledger clauses to PROVEN. |

**Pins.** `test_describe_table_plain_matches_spark_rows`, `test_describe_table_extended_sections`,
`test_describe_formatted_is_extended`, `test_describe_missing_table_analysis_exception`,
`test_describe_temp_view_falls_through`, `test_describe_table_properties_redacted`,
`test_describe_table_show_truncate_false_prints` (the owner's exact call).

**Done when.** The owner's call prints a table; every pin green; `make verify` and the facade
file green; the Z6 pin unchanged.

**Hand back when.** `schema_to_arrow_schema` cannot express a type the table carries;
`catalog_handle` cannot load a table by the parsed identifier.

**Rounds.** 3 (M, I, M).

---

### Card DF-EXPLAIN-1 — `DataFrame.explain()` prints a plan, not `Row(...)`

**Done 2026-09-09, merged #427** (`explain.py` beside `core.py`, rows via `toLocalIterator`; D-5 ruled by the orchestrator: the `core.py` exact baseline ratchets down, the helper lives in its own module).

**Bug.** `explain()` (`python/repark/src/repark/spark/dataframe/core.py`, the method at the
`def explain(` line) runs `EXPLAIN SELECT * FROM <scratch view>` and prints each result **Row
object**, so the plan arrives as `Row(plan_type='physical_plan', plan='ProjectionExec…\n  …')`
with literal `\n`.

**Home.** `core.py` (`explain` and a new `_explain_text`), `python/repark/tests/test_df_explain_1.py`
(new), `docs/guide/` (the DataFrame guide's explain paragraph), maps.

**Decisions.**

- **D-1 Rendering.** Print sections in Spark's header form, each plan's text verbatim with its
  real newlines, a blank line between sections, nothing else:
  ```text
  == Physical Plan ==
  ProjectionExec: expr=[…]
    RepartitionExec: …
  ```
- **D-2 Mode map** (DataFusion 54.1.0 rows are `plan_type` / `plan`):

  | Call | SQL | Sections printed |
  |---|---|---|
  | `explain()` / `mode="simple"` | `EXPLAIN` | `== Physical Plan ==` from `physical_plan` |
  | `explain(True)` / `mode="extended"` | `EXPLAIN` | `== Optimized Logical Plan ==` from `logical_plan`, then `== Physical Plan ==` |
  | `mode="formatted"` | `EXPLAIN FORMAT TREE` | `== Physical Plan ==` holding DataFusion's tree rendering |
  | `mode="cost"` / any mode containing `ANALYZE` | `EXPLAIN ANALYZE` (unchanged) | `== Physical Plan ==` with metrics |
  | `mode="codegen"` | `EXPLAIN` | `== Physical Plan ==` plus one trailing line `codegen: not applicable (RePark has no generated code)` |

  `extended` may be a `bool` or a mode string, as PySpark accepts; an unknown string raises
  `PySparkValueError` naming the five modes. Existing mode pins in `test_df_batch2.py` stay green.
- **D-3 `_explain_text(...) -> str`** builds the string; `explain` prints it and returns `None`.
  Tests assert on the string, not on `capsys`, except one smoke test that `explain()` prints.
- **D-4 `EXPLAIN FORMAT TREE` passes through the Spark door unchanged.** If the router or the
  bare-name expansion rejects the `FORMAT` clause, hand back; do not patch the router in this card.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | M | Red-first pins in `test_df_explain_1.py`: no `Row(` in output, header present, at least three lines for a `select`+`filter`+`withColumn` plan, extended has both headers in order, formatted output contains DataFusion's tree glyphs, unknown mode raises, `collect()` is not invoked by `explain` (spy). Run; paste the red. |
| 2 | M | Implement D-1…D-3 in `core.py` (one helper + the method body); docstring stays one line; run the file and `test_df_batch2.py -k explain`; guide paragraph + maps; ledger. May chain from step 1. |

**Done when.** The owner's pasted plan prints as an indented tree; both test files green;
`make py-test-facade` green.

**Hand back when.** D-4 fails; `test_df_batch2.py` pins contradict D-2.

**Rounds.** 1–2 (M).

---

### Card DISPLAY-POLARS-1 — polars-style rendering is the default, for `show()` **and** `repr`

**Facts on `f00ed9ea`.** `repark.display.style` ∈ {`spark`, `polars`, `duckdb`} already drives
`show()` (`core.py` `_resolve_display_style`, `_render_styled_show`; the renderer
`_format_polars_show` in `python/repark/src/repark/spark/dataframe/plan_collapse.py`) with shape
line, `---` dtype row, 5+5 edges and the `…` row. `__repr__`/`_repr_html_` ignore the style: they
print `DataFrame[col: type, …]` unless `spark.sql.repl.eagerEval.enabled` is set. Styled
`show()` runs `count()` before the head and tail fetches. The default is `spark`, pinned by
`test_display_styles.py::test_default_show_byte_identical_spark_grid`.

**Home.** `core.py` (`__repr__`, `_repr_html_`, `_render_styled_show`, `_resolve_display_style`),
`plan_collapse.py` (polars renderer, cell text, type labels), `session_core.py`
(`_DEFAULT_DISPLAY_STYLE`, `normalize_display_style`, new keys), `python/repark/tests/conftest.py`
and `python/repark-parity`'s conftest (R-6), `test_display_styles.py`, new
`test_display_polars_default.py`, `docs/guide/session-and-conf.md` (`repark.display.style`
section), maps.

**Decisions.**

- **D-0 What changes is rendering only (R-8).** `repark.DataFrame` keeps its class, its Spark surface and its lazy plan; the polars look is text produced by `plan_collapse.py` from Arrow batches. No polars import outside tests.
- **D-1 Default flips to `polars`.** `_DEFAULT_DISPLAY_STYLE = "polars"`, resolved through a
  new `default_display_style()` that reads `REPARK_DISPLAY_STYLE` first (validated by
  `normalize_display_style`, invalid = refuse-loud naming the three styles) and then the
  constant. Builder `.config("repark.display.style", …)` and `session.display_style` keep
  outranking both.
- **D-2 Test fixtures set `spark`** (R-6): both conftests `os.environ.setdefault("REPARK_DISPLAY_STYLE", "spark")`
  at import, before any session exists. `test_default_show_byte_identical_spark_grid` becomes
  two pins: under `REPARK_DISPLAY_STYLE=spark` the grid is byte-identical (unchanged
  expectation), and under a clean environment (`monkeypatch.delenv`) a fresh session's
  `display_style` is `polars`.
- **D-3 Edges are 5 + 5** (R-1), the current renderer's numbers; `repark.display.max_rows`
  (default `10`) replaces the hard-coded `2 * edge`; edges are `max_rows // 2`.
- **D-4 `repr` renders data with a count under `polars` and `duckdb`** (R-2), regardless of
  `spark.sql.repl.eagerEval.enabled`; under `spark` the existing eagerEval behaviour is
  untouched. `_repr_html_` returns `None` under the two styled modes (Jupyter then shows the
  text repr, which is the polars look the owner asked for); an HTML table is DISPLAY-POLARS-2,
  not this card.
- **D-5 One fetch when the frame is small.** The styled renderer first fetches
  `limit(max_rows + 1)`; when fewer than `max_rows + 1` rows return, the frame is rendered whole
  with the exact shape and **no `count()` and no tail fetch**. Only a frame with more rows pays
  the count and the tail fetch. `show(n)` keeps its keep-set cap semantics from
  `test_display_styles.py`.
- **D-6 Fidelity targets, measured against polars itself.** `polars>=1.0` is an optional extra
  of this package; the pins build the same Arrow table in polars and compare
  `str(pl.from_arrow(table))` to RePark's rendering byte-for-byte, skipping when polars is not
  importable. Targets: `repark.display.str_len` (default `32`, polars `POLARS_FMT_STR_LEN`,
  cells longer are cut to 31 chars plus `…`); `repark.display.max_cols` (default `8`, polars
  `POLARS_FMT_MAX_COLS`, wider frames show the first four columns, a `…` column, the last
  four); float, int, bool (`true`/`false`), null (`null`), date, `datetime[μs]` and
  `decimal[p,s]` spellings; nested `struct[n]` / `list[i64]` labels. Any row where polars'
  output is not reproducible from Arrow alone (for example timezone-aware timestamps) is a
  disclosed residue row in the ledger, not a guessed rendering.
- **D-7 `show(truncate=…)` maps onto `str_len`**: `True` → the session `str_len`, `False` →
  no cut, an int → that width. Existing `truncate` diagnostics pins stay.
- **D-9 / D-10 (orchestrator rulings 2026-09-09; step 1 merged #429):** `default_display_style()`
  lives in `session_configuration.py` beside `normalize_display_style`, re-exported through
  `_funcs.py`; no parity `conftest.py` exists, so the parity suite's `spark` pin rides the facade
  conftest only.
- **D-8 The four keys** `repark.display.style|max_rows|max_cols|str_len` are facade-local
  (stored in the alive token beside `display_style`, read at render time so `conf.set` at
  runtime takes effect); `conf.get` returns them; `repark.toml` `[default.display]` (CFG-1)
  maps onto them 1:1.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | M | **Done 2026-09-09 (#429).** D-1 + D-2 only: `default_display_style()`, the env override, both conftests, the split pin. Whole facade suite must stay green (`make py-test-facade`), which proves R-6 holds. |
| 2 | M | D-5: the small-frame single fetch in `_render_styled_show`; pins: a spy proves `count()` is not called for a 7-row frame and is called once for a 12-row frame; existing `test_styled_show_does_not_full_collect` stays green. |
| 3 | M | D-4: `__repr__`/`_repr_html_` honour the style; pins for `repr(df)` under each style, eagerEval on and off. |
| 4 | I | D-3, D-6, D-7, D-8: the keys, the renderer fidelity, the polars-oracle pins, residues filed. This step is I because the renderer edits cross `plan_collapse.py` and `core.py` and the residue calls need judgement. |
| 5 | M | Docs: `session-and-conf.md` section rewritten (default polars, the four keys, the env override, the count note now "only past max_rows"); maps; ledger PROVEN. |

**Pins.** `test_default_style_polars_clean_env`, `test_env_override_spark_restores_grid`,
`test_polars_repr_renders_table_without_eager_eval`, `test_spark_repr_unchanged`,
`test_small_frame_renders_without_count`, `test_large_frame_counts_once`,
`test_polars_oracle_ints_strings_nulls`, `test_polars_oracle_floats_bools_dates`,
`test_polars_oracle_wide_frame_col_ellipsis`, `test_str_len_cuts_with_ellipsis`,
`test_display_keys_conf_get_set`, plus the existing `test_display_styles.py` file green.

**Done when.** A notebook cell `df` prints polars' table; every pin green; `make py-test-facade`
and the parity suite green with fixtures on `spark`.

**Hand back when.** A polars oracle row cannot be matched from Arrow data alone (file it as
residue and continue); `normalize_display_style` lives somewhere other than `session_core.py`.

**Rounds.** 5 (M, M, M, I, M).

---

### Card CFG-1 — `repark.toml` (roadmap v1.4, pulled forward by R-4)

The design is already ruled in
[../epic-term/roadmap-design-plan-2026-08-29.md](../epic-term/roadmap-design-plan-2026-08-29.md)
"Card CFG-1 — the loader (Rust-owned)": discovery `$REPARK_CONFIG` → `./repark.toml` →
`~/.config/repark/repark.toml`, first hit wins; `[default]` plus `[<profile>]` selected by
`REPARK_ENV` with a deep merge; `${ENV_VAR}` interpolation, missing var refuses loud; sources
→ `CatalogSpec` / `SourceSpec`; `conf_dump()` redacted; precedence builder `.config()` >
profile > default. This card only splits it into rounds and adds the three tables the
2026-09-08 items need.

**Home.** `crates/repark-core/src/config_file.rs` + `config_file/{discovery,profile,interpolate,redact,sources}.rs`
(new), `crates/repark-core/src/session.rs` (`ReparkSessionBuilder::from_config_file`),
`crates/repark-python/src/session.rs` (`config_path`), `python/repark/src/repark/config.py`
(Pydantic mirror), `python/repark/src/repark/spark/session/session_core.py` (`Builder.configFile`),
`docs/guide/repark-toml.md` (new), maps.

**Decisions (additions to the ruled card).**

- **D-1 Three more tables per profile:** `[<profile>.display]` (`style`, `max_rows`,
  `max_cols`, `str_len` → the DISPLAY-POLARS-1 keys), `[<profile>.session]`
  (`memory_limit_gb`, `batch_size`, `target_partitions`, the three builder knobs), and
  `[<profile>.conf]` (free-form string keys applied exactly as `.config(k, v)` calls, in
  file order). Unknown keys inside `display` and `session` refuse loud; `conf` accepts any key.
- **D-2 Discovery is automatic at `getOrCreate()`**; `Builder.configFile(path)` forces a file;
  `REPARK_CONFIG=""` (set but empty) disables discovery for one process. The dump shows a
  `source` column with `builder`, `file:<path>#<profile>`, or `default`.
- **D-3 Seed commit (O, or the overnight orchestrator under grant G-5):** in the root
  `Cargo.toml` `[workspace.dependencies]`, `serde = { version = "1.0.229", features = ["derive"] }`
  and `toml = "0.8.23"` (both already in the local registry cache); in
  `crates/repark-core/Cargo.toml` `[dependencies]`, `serde.workspace = true` and
  `toml.workspace = true`; `cargo build -p repark-core` refreshes `Cargo.lock`; `make verify`
  green; one commit before round 1. Workers never touch the lockfile.
- **D-4 Profiles for PROFILES-1** ride here as ordinary named profiles (`[read]`, `[write]`)
  chosen by `REPARK_ENV=read`; no special key.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 0 | O | Seed commit: workspace deps; the empty module family with `pub fn load() -> Result<ConfigFile>` returning the empty config; `make verify`. |
| 1 | M | `discovery.rs` + `profile.rs` + `interpolate.rs` with unit tests per ruled step 1–3 (tempdir fixtures; `REPARK_CONFIG` precedence; empty-string disable; deep merge; unknown top-level key path in the error; missing `${VAR}` names the key path). No wiring. |
| 2 | M | `sources.rs` + `redact.rs` per ruled steps 4–5, tests reuse `catalog_config.rs` fixtures so specs compare byte-identical. |
| 3 | I | Wiring: `from_config_file` in the builder, precedence merge before `build()`, the dump's `source` column, the Python `config_path` argument and `Builder.configFile`, D-1's three tables applied through the existing `.config()` path. |
| 4 | M | `python/repark/src/repark/config.py` Pydantic mirror (typed construction only) and its tests; `docs/guide/repark-toml.md` with one complete example file (`[default]`, `[prod]`, `[read]`, `[write]`, one catalog, one database source, the display and session tables); maps; ledger. |

**Pins.** The ruled card's list (discovery order, profile merge, missing var refuses, name
collision refuses, redacted dump, precedence table parametrised, Java-key ↔ native-key
equivalence) plus `test_toml_display_table_sets_style`, `test_toml_session_table_sets_builder_knobs`,
`test_toml_conf_table_applies_in_order`, `test_repark_config_empty_disables_discovery`.

**Done when.** The ruled done condition: a session built from a file registers the same catalogs
the equivalent `.config()` calls would, byte-identical specs; plus the three new tables round-trip.

**Hand back when.** A key does not map onto an existing `CatalogSpec` field (ruled); the Python
builder has no single choke point for applying config pairs.

**Rounds.** 4 after the seed (M, M, I, M).

---

### Card DF-EAGER-1 — `.eager()`, `.compute()`, `.lazy()`

**Facts on `f00ed9ea`.** Every RePark DataFrame is a lazy DataFusion plan; `df.pl.lazy()` is a
no-op returning the `PolarsFrame`; `df.pl.collect()` already returns a **real `polars.DataFrame`**
and keeps polars' meaning (unchanged by this card). `collect()` on the Spark surface returns
rows and is untouched. `cache()`/`persist()` materialise through
`_materialize_cache_if_needed` → `session.materialize_as_cache_view(view, lineage, max_bytes)`
(guarded by `repark.cache.max_bytes`), and `unpersist()` drops that view.

**Home.** `core.py` (three methods, an `_eager_shape` slot, `__repr__` shortcut), `polars.py`
(`PolarsFrame.eager()` mirror), `python/repark/tests/test_df_eager_1.py` (new), the DataFrame
guide, maps.

**Decisions.**

- **D-1 `eager()`** materialises the plan through the cache-view path and returns a **new**
  `repark.DataFrame` (the same class, the full Spark surface, never a polars object — R-8) whose plan is `SELECT * FROM <cache view>` and whose `_eager_shape` is
  `(rows, cols)` read from the materialised table (no separate count). The source frame is
  unchanged. Over `repark.cache.max_bytes` the existing guard refuses; the message names
  `.eager()` and the key.
- **D-2 `compute()`** is an alias: same function object, `DataFrame.compute is DataFrame.eager`.
- **D-3 `lazy()`** on a non-eager frame returns `self`; on an eager frame returns a copy without
  `_eager_shape` over the same view (no re-execution, no drop). The pair `df.lazy()` /
  `lf.eager()` is the documentation, as the owner framed it.
- **D-4 `repr` of an eager frame** uses `_eager_shape` and skips `count()`; the head/tail
  fetches still run against the view (cheap). `count()` on an eager frame returns
  `_eager_shape[0]` without a query.
- **D-5 Release.** An eager frame owns its view; `unpersist()` drops it (existing behaviour).
  No `__del__`; the session's `stop()` drops every view already.
- **D-6 `PolarsFrame.eager()`** wraps `self._frame.eager()`; `PolarsFrame.collect()` is not
  touched.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | M | Red-first pins (`test_df_eager_1.py`): `eager` returns a new frame with `_eager_shape` equal to `(count, len(columns))`; source unchanged; `compute is eager`; `lazy` identities per D-3; `repr` of an eager frame does not call `count` (spy) and prints the same table as the lazy one; a CSV read `eager()`ed then the file deleted still answers (materialised); over-limit refuses naming `.eager()`; Spark `collect()` still returns rows; `df.pl.collect()` still returns a polars DataFrame. |
| 2 | I | Implement D-1…D-6 (reuse `_materialize_cache_if_needed`'s call; do not duplicate the guard). |
| 3 | M | Guide section "Lazy and eager" with the polars/RePark pair table; maps; ledger. May chain from step 2. |

**Done when.** All pins green; `make py-test-facade` green; the eager `repr` path shows zero
`count()` calls under the spy.

**Hand back when.** `materialize_as_cache_view` cannot report the row count without a query
(then D-1 needs a Rust return-value change, which is an I step in `crates/repark-python`).

**Rounds.** 3 (M, I, M).

---

### Card BALLISTA-AUDIT-0 — Milestone 0 of the Rust migration pilot (R-5)

**Done 2026-09-09, merged #426** at Ballista tag 54.1.0 / `f4e66525`: recommendation DEPEND on four Ballista crates, not import; ADR-0004's write ban stands on "no commit issues from a task" (the extension codec can carry write nodes). Its twelve clauses sit OPEN until LEDGER-READING-1 lands.

**What.** The audit the owner's runtime plan names as its next action: inspect the current
`apache/datafusion-ballista` and determine the smallest coherent subset that can serve as
RePark's owned distributed runtime. A **reading unit**: no RePark code changes, one document.
It joins the rust-unification brief
([../epic-term/rust-unification-implementation-brief-2026-09-04.md](../epic-term/rust-unification-implementation-brief-2026-09-04.md))
as its Milestone 0, and it must answer the standing question in `docs/adr/0004-server-prep-disciplines.md`:
Ballista's protobuf plan serialization cannot carry RePark's Iceberg write/commit nodes, so
"do not build Ballista-for-writes" — the audit's serialization chapter says whether an
extension codec changes that, and the commit-coordinator boundary (executors produce files,
one authority commits) is kept either way.

**Home.** `task/roadmap/epic-term/ballista-audit-2026-09-XX.md` (new), the epic-term map, one
row in the unification brief pointing at it. Clone of upstream under the worker's scratch
directory only; nothing vendored.

**Decisions.**

- **D-1 Version pair.** The audited upstream commit is the newest Ballista tag whose DataFusion
  dependency is `54.x`, matching the workspace pin `datafusion = "54.1.0"`. If no tag matches,
  the nearest lower major and the delta is a named risk.
- **D-2 Two halves, two tiers.** Half A (facts, M): line counts by crate with the tool the
  worker has (`cargo`-free: `find … | xargs wc -l` by directory, tests split by `tests/` and
  `#[cfg(test)]` files), `cargo tree -e normal` per crate, the protobuf file inventory, the
  extension-point inventory by grep (`PhysicalExtensionCodec`, `LogicalExtensionCodec`,
  `SessionState`, `RuntimeEnv`, `TableProvider`, `ObjectStore`), the file list per lifecycle
  step (client submit → scheduler → stage → task → executor → shuffle write → shuffle read →
  result). Half B (judgement, I): the KEEP / MODIFY / WRAP / DROP classification per module,
  the crate-structure proposal, the risks, and the dependency-vs-import recommendation against
  the plan's decision gate.
- **D-3 Output sections** mirror the owner's plan §26 A–H and §27, in that order, so the owner
  reads it against the plan without a crosswalk.
- **D-4 No rewriting, no proposals to rename.** Divergence proposals are listed only where a
  RePark requirement is named (streaming microbatch, Iceberg commit coordination, Python API).

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | M | Half A into the document's appendix tables; every number carries the command that produced it. |
| 2 | I | Half B; the decision-gate table (§28 of the plan) with one line of evidence per criterion. |
| 3 | O | Read, add the ADR-0004 disposition line, map lockstep, PR. |

**Done when.** The document answers the plan's sixteen audit outputs with file paths and
numbers from the pinned commit, and the decision gate has a recommendation.

**Rounds.** 2 (M, I).

---

### Card PROFILES-1 — measured read and write configuration profiles

**What.** Two named profiles shipped as `repark.toml` examples and as documented `.config()`
sets: `read` (analytics scans, joins, aggregations) and `write` (bulk appends, INSERT OVERWRITE,
MERGE). Every value in a profile is the winner of a measurement on this box; a knob whose
setting does not move a measurement by more than 5 % stays at the default and is listed as
"no effect measured".

**Home.** `python/repark-parity` bench roster (a new `profiles` bed), `docs/perf/config-profiles-2026-09-XX.md`
(the measurements), `docs/guide/repark-toml.md` (the two profile tables, after CFG-1),
`docs/guide/session-and-conf.md` (the `.config()` form), maps.

**Decisions.**

- **D-1 Knob inventory to measure.** Read: `datafusion.optimizer.prefer_hash_join`,
  `datafusion.execution.target_partitions`, `datafusion.execution.batch_size`,
  `datafusion.optimizer.repartition_joins`, `…repartition_aggregations`,
  `…repartition_file_scans`, `datafusion.execution.parquet.pushdown_filters`,
  `…parquet.enable_page_index`, `…parquet.bloom_filter_on_read`,
  `datafusion.execution.coalesce_batches`, `repark.scan.concurrency_limit`,
  `repark.batch.size`. Write: `datafusion.execution.parquet.compression`,
  `…parquet.max_row_group_size`, `…parquet.bloom_filter_on_write`,
  `…parquet.write_batch_size`, Iceberg `write.target-file-size-bytes`,
  `write.distribution-mode`, `repark.merge.file_scoped_rewrite`, `repark.merge.scan_pruning`.
  **Step 0 measures whether `datafusion.*` keys pass through `.config()` at all**; if not, the
  card hands back and the pass-through becomes its own I step before any measurement.
- **D-2 Beds.** Three datasets already on the box: the owner's futures parquet
  (`~/CodeRepos/myTemp/reTest/test_futures.parquet`, wide window workload), a TPC-H SF10
  generated once under the scratch directory, and a local Iceberg v2 table with 200 files.
  Five read queries (scan+filter, group-by, hash join, sort-merge join, window) and three
  write shapes (append 8 files, INSERT OVERWRITE one partition, MERGE 10 % updates).
- **D-3 Release build only** (`maturin develop --release`), three repetitions, median, machine
  idle-checked with `pgrep -f java` (one JVM rule) before every run.
- **D-4 The profile is the argmax per knob, not per combination**; interactions are a
  DYNCFG-1 concern.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 0 | M | Pass-through probe: set each D-1 key through `.config()`, read it back through `conf_dump()` and through an `EXPLAIN` that would change (for example `prefer_hash_join=false` on a join). Report a table; no edits. |
| 1 | M | The bed script under the parity roster (datasets, queries, timing harness writing a CSV). |
| 2 | M | The sweep; the measurements document with one table per knob. |
| 3 | I | The two profiles, the docs, the "no effect" list, the ledger. |

**Done when.** Both profiles exist as TOML and as `.config()` blocks, every value traced to a
table row, and a reader can reproduce any row with one command from the doc.

**Rounds.** 4 (M, M, M, I). Needs CFG-1 for the TOML form only; the `.config()` form can land first.

---

### Card LEDGER-READING-1 — reading units may prove clauses on document evidence (R-10)

**Why.** `scripts/check_ledger_grammar.py` rule B says every `PROVEN` clause in a staging ledger
must be cited by a `pins: <unit>/C-NNN` in a test; a reading unit adds no test, so
BALLISTA-AUDIT-0's twelve discharged clauses had to stay `OPEN`. The script already carries an
`EXCEPTIONS` table for two charters; that table is per-file and ratchets down, so it is the wrong
home for a class of units.

**Home.** `scripts/check_ledger_grammar.py`, its test file (find it with
`grep -rl check_ledger_grammar scripts python | grep -i test`; if none exists, create
`scripts/tests/test_check_ledger_grammar.py` and list it in `scripts/tests/map.md`),
`docs/testing.md` "Pinning a charter clause", `task/ledgers/staging/ballista-audit-0-ledger.md`,
maps.

**Decisions.**

- **D-1 The marker** is the existing header field: `**Path:** READING` (beside `LIGHT`,
  `STANDARD`, `HIGH`). A ledger whose first 40 lines carry it is exempt from rule B for every
  clause; rule A (clause table shape) and rule C (`COVERAGE_ATTESTATION` block) still apply, and
  the attestation block names the document sections that discharge each clause instead of tests.
- **D-2 No new exception rows.** The `EXCEPTIONS` table is untouched.
- **D-3 Evidence cell shape** for a reading clause: `docs: <path>#<heading-anchor>`; the script
  does not validate the anchor (that is `make check-docs-links`' job), only the prefix.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | M | Red-first test: a fixture ledger with `**Path:** READING` and one unpinned `PROVEN` clause passes; the same ledger without the marker fails with the existing rule-B message; a READING ledger with no attestation block fails rule C. Then the script change (one header regex, one branch), `docs/testing.md` paragraph, maps. |
| 2 | M | Flip BALLISTA-AUDIT-0's twelve clauses to `PROVEN` with `docs:` evidence cells and add its attestation block (an `oc-worker --role critic` round writes the block; the actor pastes it). `make check-ledgers check-ledger-grammar` green. |

**Done when.** Both gates green on the audit ledger; the two charters in `EXCEPTIONS` unchanged.

**Rounds.** 2 (M, M).

---

### Card PREFLIGHT-PARITY-1 — the parity mirror check joins `preflight`

**Why.** The CAP-1 source-file mirror test under `python/repark-parity` is not a `preflight`
member, so a baseline ratchet passed `preflight` locally and failed CI on #427.

**Home.** `Makefile` (`preflight` target and a new `py-test-parity-cap` target running only the
CAP-1 mirror file), `DEVELOPMENT.md` gate roster, `python/repark-parity/map.md` if the file is
renamed (it is not), maps. **Not** `AGENTS.md`: its gate roster sentence is policy and sits under a
compaction ceiling; the card leaves it and the orchestrator files one line in the PR body asking
the owner whether the roster sentence should name the new member.

**Decisions.**

- **D-1** The new target runs one file, the CAP-1 mirror test, with the same interpreter the
  parity suite uses; it must finish under 60 s or hand back.
- **D-2** `preflight` gains it after `py-test-facade`, before the audit; `verify` is untouched
  (Rust-only by definition).

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | M | Locate the mirror test (`grep -rl "CAP-1\|cap_1" python/repark-parity`), the target, the roster line, `make preflight` green once alone, maps. |

**Rounds.** 1 (M).

---

## 3. Epic intakes (chartered as direction; the first unit of each is cut here)

### ADAPT-PART — adaptive partitioning for Iceberg

**Goal.** Given a target file size, RePark profiles a table, proposes the partition spec that
lands files near the target, prints the plan, and on request applies it: spec evolution
(`ALTER TABLE … DROP/ADD PARTITION FIELD`, which the router already intercepts), a full rewrite
under the new spec (`rewrite_data_files`, which already exists in `crates/repark-spark/src/call/`),
`rewrite_manifests`, `expire_snapshots`, `remove_orphan_files`. Roadmap 2.1 (maintenance
policy) is the declarative layer above this; this epic is its analysis engine.

**Design rules (binding on every unit).**

- **P-1 Statistics come from manifests first, data second.** The `files` metadata table
  already exposes per-file `record_count`, `file_size_in_bytes`, `partition`, and per-column
  lower/upper bounds in `readable_metrics`. Candidate specs are scored from those bounds without
  scanning data; a data sample is taken only when bounds are missing for the candidate column.
- **P-2 Candidate generation.** For each column: timestamp/date → `year`, `month`, `day`,
  `hour`; string/int with low distinct count (≤ 1 000 from the sketch or sample) → `identity`;
  high-cardinality int/string → `bucket(N)` with N ∈ {8, 16, 32, 64, 128}; plus "no partitioning".
  Two-field specs only from the top three single fields.
- **P-3 Score** = projected bytes per partition value under the candidate (sum of file bytes
  whose bound range falls in the value; a file spanning k values contributes 1/k to each);
  penalise partitions below 0.25 × target and above 4 × target; the plan reports the
  projected partition count and the projected file count at the target size.
- **P-4 Dry run is the default and the apply requires the dry-run plan's id**, so nothing
  rewrites a table without a printed plan the caller saw.
- **P-5 Sort orders are preserved; multi-spec tables are rewritten to one spec; tables with
  branches other than `main` refuse** until a later unit.

**Units.**

| Unit | Tier | Scope |
|---|---|---|
| AP-0 (measure) | M | A script over the `files` and `partitions` metadata tables that prints, for three real tables (the futures Iceberg table, one bronze table from the cutover namespace, one synthetic), the P-2 candidates and P-3 scores as a table; no engine changes. The output decides whether P-3's model predicts actual file sizes within 20 % after a manual rewrite of one table (an O-run check). |
| AP-1 (plan) | I | `CALL <catalog>.system.plan_partitioning(table => …, target_file_size_bytes => …)` returning the plan frame (candidate, score, projected partitions, projected files, the DDL and CALL statements it would run) and a plan id. |
| AP-2 (apply) | I | `CALL <catalog>.system.apply_partitioning(table => …, plan_id => …)` executing the chain with one commit per step and a result frame; refusals per P-5. |
| AP-3 (policy) | I | The 2.1 hook: `[<profile>.maintenance]` `target_file_size` drives plan + apply on a schedule or by `CALL run_maintenance()`. Waits for 2.1. |

### DYNCFG-1 — dynamic configuration planning

**Goal.** A boolean in `repark.toml` (`[<profile>.autotune] enabled = true`, `dataset = …`)
starts a session on defaults, runs a bounded knob matrix over a sample of the named dataset,
and writes the winning `read` and `write` profiles back into the file. **Entry criteria:**
PROFILES-1 has shown at least one knob moving a measurement by more than 5 %; otherwise this
unit stays deferred, because the 2026-09-04 performance analysis found the large costs in code
paths, not settings. Cut as one I unit after PROFILES-1 reports; no card until then.

## 4. Sequence, dependencies, cost

| # | Unit | Depends on | Tiers | Rounds | Worker cost (GLM ≈ $0.005/round; Muse unmetered) |
|---|---|---|---|---|---|
| 1 | DF-EXPLAIN-1 | — | M | 1–2 | ≈ $0.01 |
| 2 | SQL-DESCRIBE-1 | live Spark on this box | M, I, M | 3 | ≈ $0.01 + one I round |
| 3 | DISPLAY-POLARS-1 | — | M×4, I | 5 | ≈ $0.02 + one I round |
| 4 | CFG-1 | seed commit (O) | M, M, I, M | 4 | ≈ $0.02 + one I round |
| 5 | DF-EAGER-1 | DISPLAY-POLARS-1 step 3 (repr path) | M, I, M | 3 | ≈ $0.01 + one I round |
| 6 | BALLISTA-AUDIT-0 | — (parallel-safe, no repo code) | M, I | 2 | ≈ $0.01 + one I round |
| 7 | PROFILES-1 | CFG-1 for the TOML form only | M×3, I | 4 | ≈ $0.02 + one I round |
| 8 | AP-0 | — (script only) | M | 1 | ≈ $0.01 |
| 9 | AP-1 → AP-3, DYNCFG-1 | AP-0 result; PROFILES-1 result | I | — | after the measurements |

**Status 2026-09-09 morning:** 1 merged (#427), 6 merged (#426), 3 step 1 merged (#429), 2 parked at
step 2 on R-9 (now ruled, PR #428 resumes). New units: 10 LEDGER-READING-1 (M, 2 rounds), 11
PREFLIGHT-PARITY-1 (M, 1 round). Night-2 order is runbook §7.

Units 1, 2 and 6 can run in parallel lanes today; 3 and 4 in parallel after 1–2 merge; 5 after
3; 7 after 4. The binding cost is orchestrator tokens, not worker dollars: each round costs
one audit. Keep audits to `git diff --stat` plus the files the card names.

## 5. Decisions log

Every D-row above is binding on its card. Cross-card decisions:

| # | Decision | Reason |
|---|---|---|
| X-1 | `df.pl.collect()` keeps polars' meaning (real `polars.DataFrame`); the Spark-surface `collect()` keeps Spark's; `.eager()`/`.compute()` are the RePark names for lazy → materialised. | The owner's naming analysis; no method changes meaning on either surface. |
| X-2 | The display defaults (`10`, `8`, `32`) are polars' own environment-variable defaults. | A polars user recognises the output without reading docs. |
| X-3 | Tests run on `spark` style through an environment variable, not through builder calls in every test. | R-6, and one line in two conftests instead of hundreds of edits. |
| X-4 | Dependency edits are orchestrator seed commits. | Workers are denied lockfile changes; a seed keeps the audit surface small. |
| X-5 | Critics never run on Opus. | Owner ruling 2026-09-06. |

## Pointers
- Up: [map.md](map.md)
- The ruled TOML card: [../epic-term/roadmap-design-plan-2026-08-29.md](../epic-term/roadmap-design-plan-2026-08-29.md)
- The pilot this audit joins: [../epic-term/rust-unification-implementation-brief-2026-09-04.md](../epic-term/rust-unification-implementation-brief-2026-09-04.md)
- Worker launchers: `~/.claude/skills/oc-worker/SKILL.md`, `~/.claude/skills/muse-worker/`, `~/.claude/skills/grok-worker/SKILL.md`
