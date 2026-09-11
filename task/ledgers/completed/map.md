# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [ap-0-ledger.md](ap-0-ledger.md) —
  **AP-0 (measure, 2026-09-10), in flight:** ADAPT-PART partition-candidate
  measurement — three local Iceberg beds (futures CTAS unpartitioned, generated
  uniform/skewed 400k-row beds, 206 files each) scored from `files`/`partitions`
  manifest bounds at a 512 KiB target, one ranked P-2/P-3 table per bed in
  [../../../docs/perf/ap-0-partition-candidates-2026-09-10.md](../../../docs/perf/ap-0-partition-candidates-2026-09-10.md),
  runnable method in
  [../../../python/repark-parity/bench/adaptpart/map.md](../../../python/repark-parity/bench/adaptpart/map.md).
  4 PROVEN, 1 OPEN (C-005, the 20 percent prediction check needs the orchestrator
  O-run rewrite). `risk_tier: standard`. Branch `feat/ap-0`.
  pins: ap-0/C-001, C-002, C-003, C-004
- [ballista-audit-0-ledger.md](ballista-audit-0-ledger.md) —
  **BALLISTA-AUDIT-0 steps 1–3 (2026-09-08), in flight:** Half A facts plus Half B
  judgement for the Ballista audit at upstream tag `54.1.0` (`f4e66525`) —
  clauses C-001…C-012 PROVEN on document evidence under the grammar gate's
  reading-unit rule (R-10; LEDGER-READING-1 step 2, 2026-09-09), §26 A–H +
  §27 + §28 gate filled, appendix A6 closes the python/tooling gap; the ADR
  disposition and the brief's Milestone 0 row landed in #426.
  Branch `docs/ballista-audit-0`.
- [ballista-m1-a-ledger.md](ballista-m1-a-ledger.md) —
  **BALLISTA-M1-A step 1 (2026-09-10), merged** as part of Ballista Milestone 1
  (#460/#466/#469/#470): `DistributedExecutor` + `LocalDataFusionExecutor` in
  `crates/repark-distributed`; range-sum, status, and cancel pins green; `cluster`
  feature builds with no cluster code. `risk_tier: standard`.
  Branch `feat/ballista-m1-a`.
  pins: ballista-m1-a/C-001, C-002, C-003, C-004
- [ballista-m1-b-ledger.md](ballista-m1-b-ledger.md) —
  **BALLISTA-M1-B step 2 (2026-09-10), merged** as part of Ballista Milestone 1
  (#460/#466/#469/#470): UDF-on-executor pin
  (`repark_times_ten` through `ReparkSessionProvider`; vanilla session fails to resolve);
  cancel mid-flight (`Cancelled`, no running tasks within 5 s); codec install pin
  (`repark_ballista_codec()` is Ballista's defaults; two-executor shuffle completes).
  C-004 round-trip serde stays OPEN / PARKED (needs `datafusion-proto`). `risk_tier: standard`.
  Branch `feat/ballista-m1-b`.
  pins: ballista-m1-b/C-001, C-002, C-003, C-005, C-006
- [ballista-m1-c-ledger.md](ballista-m1-c-ledger.md) —
  **BALLISTA-M1-C step 2 (2026-09-10), merged** as part of Ballista Milestone 1
  (#460/#466/#469/#470): D-1 three shapes PROVEN; D-3
  `Completed { stages, retried_stages }` shuffle bytes > 0 on the two-stage hash aggregate;
  D-4 session spill dir has no shuffle files after complete or cancel. D-2 retry stays
  OPEN (ChaosExec is not fail-once; stopping an executor needs `arrow_flight`).
  `risk_tier: standard`. Branch `feat/ballista-m1-c`.
  pins: ballista-m1-c/C-001, C-003, C-004
- [ballista-m1-d-ledger.md](ballista-m1-d-ledger.md) —
  **BALLISTA-M1-D step 2 (2026-09-10), merged** as part of Ballista Milestone 1
  (#460/#466/#469/#470): Iceberg provider codec and
  two-executor scan pins (step 1) plus the design-doc Iceberg section and success-list
  line 17. Runtime abstraction is in place; Iceberg writes and the commit coordinator
  are Milestone 3 (ADR-0004). Residues: S3/Glue credentials; `IcebergTableScan` rewrite
  to parquet file groups (`datafusion-proto` wall, third time). `risk_tier: standard`.
  Branch `feat/ballista-m1-d`.
  pins: ballista-m1-d/C-001, C-002, C-003
- [df-eager-1-ledger.md](df-eager-1-ledger.md) —
  **DF-EAGER-1 step 1 (2026-09-09), in flight:** `.eager()` / `.compute()` / `.lazy()` on the
  facade DataFrame. Step 1 only: the red-first pins in
  [../../../python/repark/tests/test_df_eager_1.py](../../../python/repark/tests/test_df_eager_1.py)
  — seven `xfail(strict=True)` pins red on the base tree (marker-less run recorded in the
  ledger) plus two keep-green guards; step 2 builds D-1..D-6 in `core.py` + `polars.py`.
  Clauses C-001…C-006 OPEN. Branch `feat/df-eager-1`.
  pins: df-eager-1/C-001, C-002, C-003, C-004, C-005, C-006
- [df-explain-1-ledger.md](df-explain-1-ledger.md) —
  **DF-EXPLAIN-1 (2026-09-08), in flight:** `DataFrame.explain()` prints plan text, not
  `Row(...)` reprs. Step 1 done: the seven red-first pins in
  `python/repark/tests/test_df_explain_1.py` run RED on base `f00ed9ea` (run recorded in the
  ledger); D-4 measured — `EXPLAIN FORMAT TREE` passes the Spark door unchanged. Step 2 open:
  implement D-1..D-3 in `core.py` (`_explain_text` + the method body) plus the guide
  paragraph. `risk_tier: standard`. Branch `feat/df-explain-1`.
  pins: df-explain-1/C-001, C-002, C-003, C-004, C-005, C-006
- [dfcore-1-ledger.md](dfcore-1-ledger.md) —
  **DFCORE-1 (2026-09-07), in flight:** leaf helpers out of `core.py` — Arrow cell
  conversion to `rows_export.py`, export-error mapping to new `export_errors.py`,
  mapInArrow schema checks to new `udf_schema.py`, grouped-UDF assembly to new
  `grouped_udf.py`. Move-only: `core.py` 6302 → 5954, `joins_columns.py` 1239 → 1238,
  export surfaces pinned identical, 6093 pre-existing collected IDs unchanged, 6 added. `risk_tier: standard`.
  Branch `refactor/dfcore-1`.
  pins: dfcore-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [dfcore-2-ledger.md](dfcore-2-ledger.md) — Unit ledger — DFCORE-2 · the four UDF projection rewrites out of `core.py`
- [dfcore-3-ledger.md](dfcore-3-ledger.md) — Unit ledger — DFCORE-3 · the statistics family out of `core.py`
- [dfcore-4a-ledger.md](dfcore-4a-ledger.md) — Unit ledger — DFCORE-4a · sampling out of `core.py`
- [dfcore-4b-ledger.md](dfcore-4b-ledger.md) — Unit ledger — DFCORE-4b · display out of `core.py`
- [dfcore-5-ledger.md](dfcore-5-ledger.md) — Charter ledger — DFCORE-5 · `approxQuantile` in one collect per frame
- [dfcore-6-ledger.md](dfcore-6-ledger.md) — Charter ledger — DFCORE-6 · eager previews fetch N+1, never `count()`
- [display-bridge-1-ledger.md](display-bridge-1-ledger.md) —
  **DISPLAY-BRIDGE-1 step 1 (2026-09-09), in flight:** a bridged frame's `show()` follows
  the display style (R-13) — `_show` resolves the style before the bridge peek; under
  `polars` / `duckdb` the peeked table renders through `_render_styled_show` (short peek →
  exact shape, no count; full peek → one count), under `spark` the grid is byte-identical.
  Closes DISPLAY-POLARS-1-S3-Q-001 (flipped FIXED in the completed display-polars-1
  ledger). Three clauses, pins red-first in
  [../../../python/repark/tests/test_display_bridge_1.py](../../../python/repark/tests/test_display_bridge_1.py).
  `risk_tier: standard`. Branch `feat/display-bridge-1`.
  pins: display-bridge-1/C-001, C-002, C-003
- [display-lazy-1-ledger.md](display-lazy-1-ledger.md) — Unit ledger — DISPLAY-LAZY-1 step 1 · a lazy frame's `repr` shows the schema, not the data (R-22)
- [display-polars-1-ledger.md](display-polars-1-ledger.md) —
  **DISPLAY-POLARS-1 step 1 (2026-09-09), in flight:** polars-style rendering becomes the
  default. Step 1 only (D-1 + D-2): `_DEFAULT_DISPLAY_STYLE` flips to `polars`, resolved
  through new `default_display_style()` (env `REPARK_DISPLAY_STYLE` first, refuse-loud via
  `normalize_display_style`), and the facade suite pins `spark` via conftest so no existing
  expectation moves. Clauses C-001…C-002 green. Branch `feat/display-polars-1`.
  pins: display-polars-1/C-001, C-002
- [docs-links-1-ledger.md](docs-links-1-ledger.md) —
  **DOCS-LINKS-1 step 1 (2026-09-09), in flight:** the gate LEDGER-READING-1 D-3 assumed —
  `make check-docs-links` checks every tracked `*.md`'s relative links, GitHub-style anchors
  and `docs:` evidence cells; the measured 10-link baseline is the allowlist residue
  (`scripts/docs_links_allowlist.txt`). Clauses C-001…C-003 green. Branch `feat/docs-links-1`.
- [facade-audit-0-ledger.md](facade-audit-0-ledger.md) — Unit ledger — FACADE-AUDIT-0 step 1 · Half A: the Rust-backed facade facts
- [fnp-8-review-ledger.md](fnp-8-review-ledger.md) —
  **FNP-8-REVIEW (2026-09-07), in flight:** remediation round 1 for FNP-8 (PR #412,
  merged unreviewed) — the round-1 critic's F1–F7, each red-first against live
  PySpark 4.1.2, plus the no-regression held set. `risk_tier: standard`. Branch
  `review/fnp-8-review`.
- [ledger-reading-1-ledger.md](ledger-reading-1-ledger.md) —
  **LEDGER-READING-1 step 1 (2026-09-09), in flight:**   reading units may prove clauses on
  document evidence (R-10). A staging ledger whose first 40 lines carry the READING value of
  the `Path` header field is exempt from the grammar gate's rule B; rules A and C stay armed,
  the reading evidence shape is `docs: <path>#<heading-anchor>` (a convention, not a gate
  check), and `EXCEPTIONS` is untouched. Step 2 (2026-09-09) flipped BALLISTA-AUDIT-0's
  twelve clauses to `PROVEN` on `docs:` cells. Branch `feat/ledger-reading-1`.
  pins: ledger-reading-1/C-001, C-002, C-003, C-004
- [maint-policy-1-ledger.md](maint-policy-1-ledger.md) — Unit ledger — MAINT-POLICY-1 step 1 · typed `[<profile>.maintenance]` policy
- [preflight-parity-1-ledger.md](preflight-parity-1-ledger.md) —
  **PREFLIGHT-PARITY-1 (2026-09-09), in flight:** the CAP-1 source-file mirror joins
  `make preflight` as `make py-test-parity-cap` — the mirror file alone
  (`test_cap_1_source_file_line_cap.py`), `py-test`'s isolated interpreter recipe verbatim,
  measured 1.428 s against the 60 s budget, seated after `py-test-facade` before `audit`;
  `verify` untouched. A size-gate ratchet that updates only `scripts/check_lib_py.py` now reds
  the pre-PR gate locally instead of failing CI's Python job (#427). `risk_tier: standard`.
  Branch `feat/preflight-parity-1`.
  pins: preflight-parity-1/C-001, C-002, C-003, C-004, C-005
- [review-fix-1-ledger.md](review-fix-1-ledger.md) — Unit ledger — REVIEW-FIX-1 · the CFG-1 mirror agrees with the loader
- [review-fix-10-ledger.md](review-fix-10-ledger.md) —
  **REVIEW-FIX-10 (2026-09-10), in flight:** `is_reading` parses the `Path` header field —
  the marker matches anywhere in the first 40 lines except inside backtick code spans, the
  value is the leading identifier run compared to `READING` exactly (D-1 as amended by
  R10-A/R10-B, recorded as D-1a). Four scratch-ledger pins, red first. `risk_tier: standard`.
  Branch `fix/review-fix-10`.
  pins: review-fix-10/C-001
- [review-fix-11-ledger.md](review-fix-11-ledger.md) — Unit ledger — REVIEW-FIX-11 step 1 · the unpinned `explain()` rows (Q-32, with Q-17)
- [review-fix-12-ledger.md](review-fix-12-ledger.md) —
  **REVIEW-FIX-12 step 1 (2026-09-10), in flight:** the docs-links gate measures what it
  claims — rendered-text slugs, final-slug duplicates, table-cell-only `docs:` cells, the
  unclosed-fence finding, the absolute-target skip, same-file anchors (which caught the
  stale V3-COV-8 fragment at `docs/spark-sql-iceberg-parity.md:3053`, repaired in the same
  commit), and the three-space fence rule. `risk_tier: standard`. Branch
  `fix/review-fix-12`.
  pins: review-fix-12/C-001, C-002, C-003, C-004, C-005, C-006
- [review-fix-2-ledger.md](review-fix-2-ledger.md) — Unit ledger — REVIEW-FIX-2 · CFG-1's own pins stop reading the developer's HOME
- [review-fix-4-ledger.md](review-fix-4-ledger.md) — Unit ledger — REVIEW-FIX-4 · the eager frame's checkpoint paths (Q-12, Q-13)
- [review-fix-5-ledger.md](review-fix-5-ledger.md) — Unit ledger — REVIEW-FIX-5 step 1 · DESCRIBE metadata-name intercept, Owner, short names, redaction
- [review-fix-6-ledger.md](review-fix-6-ledger.md) — Unit ledger — REVIEW-FIX-6 step 1 · `explain()` refuses the both-set shape (Q-17)
- [review-fix-7-ledger.md](review-fix-7-ledger.md) —
  **REVIEW-FIX-7 step 1 (2026-09-10), in flight:** the sweep's security-shaped fixes —
  header-injection close in the Python mirror (refuse + quote), secret-free TOML parse
  errors in the engine, and the RF-3 one-warning disclosure for ambiently discovered
  cloud catalogs. 3 PROVEN, 0 OPEN. `risk_tier: standard`. Branch `fix/review-fix-7`.
  pins: review-fix-7/C-001, C-002, C-003
- [sql-describe-1-ledger.md](sql-describe-1-ledger.md) — Unit ledger — SQL-DESCRIBE-1 · `DESCRIBE [TABLE] [EXTENDED|FORMATTED]` on Iceberg tables (step 1: measurement)

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
