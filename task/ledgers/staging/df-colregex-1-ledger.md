# Unit ledger — DF-COLREGEX-1 · `colRegex` expands every match in `select`, like Spark

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move).

**Unit:** DF-COLREGEX-1 step 1 · **Date:** 2026-09-11 · **Model:** swe-2-high ·
**Branch:** `fix/df-colregex-1` · **Base:** `origin/main` at dispatch (no merge performed —
the orchestrator merges)

**Slate:** card DF-COLREGEX-1 — EX-DF-1's two arms (Spark strips surrounding backticks and
expands every full-match in `select`; repark compiled the raw string and answered the first
match only).

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `python/repark/src/repark/spark/dataframe/core.py`,
`python/repark/src/repark/spark/dataframe/colregex.py` (new marker module),
`python/repark/tests/test_examples_dataframe_a.py`,
`python/repark/tests/test_examples_dataframe_d.py`, `python/repark/tests/test_df_easy.py`,
`docs/spark-sql-iceberg-parity.md` §7 EX-DF-1, this ledger, and the lockstep `map.md` files
(`python/repark/src/repark/spark/dataframe/map.md`, `python/repark/tests/map.md`,
`task/ledgers/staging/map.md`, `docs/examples/dataframe/map.md`). Closed: `crates/`,
`STATUS.md`, `briefs/next-sequence.md`, `.github/`, every other ledger.

## Scope

`DataFrame.colRegex` / `col_regex` adopt the measured Spark 4.1.2 contract (D-1): a backticked
pattern returns a marker `Column` (the facade's `UnresolvedRegex`) that `select` expands to
every column the inner pattern full-matches, in frame order, case-insensitively; a bare
(non-backticked) pattern resolves as a literal column name at the `colRegex` call, raising
`AnalysisException` when absent — as Spark's eager resolution does. The marker is a `Column`
subclass in the new sibling module `colregex.py` (D-2 — no engine change, `column.py`
untouched because its exact baseline cannot grow). On every other surface the marker's
unresolvable native ref fails loudly with `AnalysisException`, matching Spark's
`INVALID_USAGE_OF_STAR_OR_REGEX` refusal class; `drop` no-ops on it because an absent name is
already a no-op. `column.py` keeps its 1589 baseline and `core.py` stays line-neutral at 4485.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The oracle re-measured `select`, `drop`, and `withColumn` over a backticked multi-match pattern and a bare pattern on live PySpark 4.1.2 in one session (stopped before the gates), plus the expansion shape arms (position, zero-match, case-fold, full-match). | The oracle table below — one row per measured arm. | **PROVEN** |
| C-002 | Both rewritten pins (`test_colregex_backtick_spelling_parity` in `test_examples_dataframe_a.py`, `test_colregex_multi_match_expands` in `test_examples_dataframe_d.py`) ran red on the base tree. | The red output pasted in the Red-first section. | **PROVEN** |
| C-003 | After the fix both pins run green and the whole `python/repark/tests` suite passes under `.venv/bin/python -m pytest -q`. | The gates table below. | **PROVEN** |
| C-004 | Every `docs/examples/` script naming `DataFrame.colRegex` / `col_regex` still executes — `.venv/bin/python scripts/check_example_coverage.py --require-execute` exits 0 (no script names the pair; the execute leg holds the whole suite). | The gate's own counts line on the shipped tree. | **PROVEN** |
| C-005 | §7 EX-DF-1 is rewritten FIXED with both arms (backtick strip, all-match expansion) recorded against the re-measured oracle answers. | The amended row plus the flipped pins. | **PROVEN** |
| C-006 | Remediation (ruling S2-21, review P2-1): `expand_col_regex` reads `frame.columns` once and binds only full-matching names on unique-name frames; frames carrying a display/engine overlay or duplicate names keep the positional `_iter_bound_columns` path. Re-measured on the reviewer's harness shape (500 columns × 10k rows; 1, 10, and 500 matches; three reps after warmup, medians). | The remediation table below; the duplicate-name pin `test_colregex_duplicate_names_expand_positionally`. | **PROVEN** |

`LOGIC_SCORE` = 6/6.

## Oracle (live PySpark 4.1.2, ANSI on, UTC, 2026-09-11)

One PySpark session (`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `.venv/bin/python`,
`spark.sql.ansi.enabled=true`, `spark.sql.session.timeZone=UTC`, warehouse a tmpdir,
`spark.ui.enabled=false`), stopped before any gate ran. Frame: `[("a", 1, 10.0), ("b", 2,
20.0)]` as `["g", "k", "v"]`; a second frame `[("k", "kx")]` for the full-match arm. Probe
script `scratch/dfcolregex/oracle.py` (gitignored).

| Surface + spelling | Spark answer |
|---|---|
| `colRegex("`^(k)$`")` alone | returns `Column<'unresolvedregex()'>` — no error at construction |
| `colRegex("^(k)$")` alone | raises `AnalysisException` `UNRESOLVED_COLUMN.WITH_SUGGESTION` naming `^(k)$` at the call |
| `select(colRegex("`^(k)$`"))` | `['k']` |
| `select(colRegex("`^(g|k)$`"))` | `['g', 'k']` — every match, frame order |
| `select(colRegex("`^zzz$`"))` | `[]` — zero matches expand to zero columns (collect → `[Row(), Row()]`) |
| `select("v", colRegex("`^(g|k)$`"))` | `['v', 'g', 'k']` — expansion keeps its select position |
| `select(colRegex("`k`"))` on `["k", "kx"]` | `['k']` — Java full-match (`matches()`), not substring |
| `select(colRegex("`^(K)$`"))` | `['k']` — case-insensitive under default `caseSensitive=false` |
| `select(colRegex("k"))` | `['k']` — bare name resolves as a literal column |
| `drop(colRegex("`^(k)$`"))` | `['g', 'k', 'v']` — the regex Column is a no-op in `drop` |
| `drop(colRegex("k"))` | `['g', 'v']` — the literal-resolved column drops |
| `withColumn("w", colRegex("`^(k)$`"))` | raises `AnalysisException` `INVALID_USAGE_OF_STAR_OR_REGEX` |
| `withColumn("w", colRegex("k"))` | `['g', 'k', 'v', 'w']` |
| `select(colRegex("`^(g|k)$`").alias("z"))` | raises `INVALID_USAGE_OF_STAR_OR_REGEX` (alias context) |
| `groupBy` / `orderBy` of the marker | raise `INVALID_USAGE_OF_STAR_OR_REGEX` |
| `colRegex("``")` | bare ```` `` ```` resolves as literal empty name → `UNRESOLVED_COLUMN` |

## Red-first (docs/testing.md "Gate provocation proofs")

Both rewritten pins ran against the unchanged base tree
(`python/repark` cwd, `../../.venv/bin/python -m pytest
tests/test_examples_dataframe_a.py::test_colregex_backtick_spelling_parity
tests/test_examples_dataframe_d.py::test_colregex_multi_match_expands -q`):

```text
FF                                                                       [100%]
=================================== FAILURES ===================================
____________________ test_colregex_backtick_spelling_parity ____________________
>       assert frame.select(frame.colRegex("`^(k)$`")).columns == ["k"]
E       repark.errors.AnalysisException: No column matched regex '`^(k)$`'
src/repark/spark/dataframe/core.py:2681: AnalysisException
______________________ test_colregex_multi_match_expands _______________________
>       assert frame.select(frame.colRegex("`^(g|k)$`")).columns == ["g", "k"]
E       repark.errors.AnalysisException: No column matched regex '`^(g|k)$`'
src/repark/spark/dataframe/core.py:2681: AnalysisException
2 failed in 0.24s
```

The old body compiled the raw argument — the surrounding backticks became literal regex
characters, so both backticked patterns matched nothing. The base-tree failure is the
measured divergence itself, not a pin artifact.

## Gates (on this tree)

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest -q python/repark/tests` | **5967 passed, 359 skipped** in 18m35s — on the final tree |
| `.venv/bin/python -m pytest tests/test_examples_dataframe_a.py tests/test_examples_dataframe_d.py tests/test_df_easy.py -q` (cwd `python/repark`) | **20 passed** |
| `.venv/bin/python -m pytest -q python/repark/tests/test_dfcore_1_exports.py` | **10 passed** (freeze set gains `colregex`, line-neutral at the 1000 default ceiling) |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **exit 0** — 927 public names, 797 covered, 128 backlog, 2 exceptions, 212 examples |
| `make check-docs-links` | **clean** — 765 files, 4894 links |
| `make check-ledger-grammar` | **clean** — 101 live ledgers, 647 clauses |
| `make check-lib-py` | **clean** — 649 files, `core.py` held at its 4485 exact baseline, `colregex.py` (52) under the default |
| `make verify` | **exit 0** — fmt, clippy, panic-ban, Rust tests, doc-tests all green |
| pre-commit sweep (`check_map_md.sh`, `sync_map_md --check`, `check_manifest`, `check_docs_compaction`, `typos`) | **clean** |
| `git diff --cached … grep -P '^\+\s*(//|#(?! noqa))'` (comment fence) | **no output** |

The first full-suite run reported one failure — `test_dfcore_1_exports.py
::test_package_export_set_unchanged` — because the lazily imported `colregex`
submodule bound a new attribute on the dataframe package. The freeze set gained
exactly `colregex` with the binding imported in the pin file itself (the file's own
pattern for order-independent submodule binding), line-neutral at its 1000-line
default ceiling; the suite re-run on the final tree is above.

## Remediation — review P2-1 (ruling S2-21, 2026-09-11)

The read-only performance review (`/tmp/oc-worker/grok-rev-colregex/report.md`) measured the
landed change and found one P2: `expand_col_regex` bound every column through
`_iter_bound_columns` and then filtered, and probed `frame.columns` twice (once inside
`_iter_bound_columns`, once in the zip). The reviewer's 500-column / 10-match instrumented
measurement: 500 binds, 2 schema probes, 5.23 ms; binding only the hits: 0.31 ms.

The fix reads `names = frame.columns` once and, when the frame carries no display/engine
overlay and names are unique, binds only the full-matching names with
`_bind_schema_column(name, name)`. The positional `_iter_bound_columns` path is kept when
`_display_names`/`_engine_names` is set or names repeat — that branch is the correctness
guard: duplicate display names must expand per position (a name lookup would raise
`AMBIGUOUS_REFERENCE`), and the overlay path resolves origin metadata through
`_origin_map`. Harness `scratch/dfcolregex/perf_expand.py` (gitignored), same frame shape
as the review (`spark.range(10000).select(col("id").alias(f"c{i:03d}") for i in 0..499)`),
one warmup then three timed reps, medians; `before` is the round-1 body verbatim, `after`
the shipped body; outputs asserted identical on all arms.

| Matches | Before median | After median | Speedup |
|---|---:|---:|---:|
| 1 (`^c042$`) | 5.368 ms | 0.229 ms | 23.5× |
| 10 (`^c00[0-9]$`) | 5.395 ms | 0.323 ms | 16.7× |
| 500 (`.*`) | 5.413 ms | 5.436 ms | 1.0× (all columns bound either way) |

The 500-match arm shows the unique-name guard costs nothing measurable when every column
expands anyway. P3-1 (withColumn/alias refusal plans the frame first) and P3-2 (+19 µs
`isinstance` on no-regex select) needed no change per the remediation card.

## Cost

The Devin (SWE-2) leg started 2026-09-11: read the contract, the EX-29 ledger, the §7 row,
`colRegex`/`select`/`drop`/`withColumns` in `core.py`, `column.py`, the size gates, and the
export freeze; measured the oracle; rewrote the pins red-first; built the marker module.

## Disk

The oracle probe lives under the gitignored `scratch/dfcolregex/` (removable at close).

## Dual-wire

Unchanged by this unit. No inventory, backlog, baseline, or workflow moves; `.github/` is
closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: df-colregex-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every ruled behaviour is pinned in both directions — backticked
        patterns expand (single, multi, positional, case-fold, zero-match arms) and
        a bare pattern resolves as a literal name in both directions (a hit selects,
        a miss raises); the marker's non-`select` doors are pinned shut (`drop`
        no-op, `withColumn` refusal, alias refusal).
      artifacts: [python/repark/tests/test_examples_dataframe_a.py, python/repark/tests/test_examples_dataframe_d.py]
    - id: AT-2
      status: N/A
      justification: No numeric or performance claim is made; the unit is a name-resolution contract.
    - id: AT-3
      status: ATTACKED
      evidence: The failure paths are the spec's second half — a bare unresolvable
        pattern raises `AnalysisException` at the `colRegex` call, `withColumn` and
        `.alias` on the marker raise on the never-resolvable ref, and a malformed
        regex raises naming the pattern; each pin asserts the raise, not just the
        exception class.
      artifacts: [python/repark/tests/test_examples_dataframe_a.py, python/repark/tests/test_examples_dataframe_d.py]
    - id: AT-4
      status: ATTACKED
      evidence: Ordering is load-bearing and pinned — expansion emits matches in
        frame order and keeps the marker's position in the select list
        (`["v", "g", "k"]`); a multi-name frame expands each duplicate display name
        to its own bound column. No shared state — the marker carries its pattern
        per instance.
      artifacts: [python/repark/tests/test_examples_dataframe_d.py, python/repark/src/repark/spark/dataframe/colregex.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, environment read, or secret; the unit changes name resolution only.
    - id: AT-6
      status: ATTACKED
      evidence: The hole this design opens is silent passthrough of the marker on a
        non-`select` surface; it is closed by construction — the marker's native ref
        can never resolve, so `withColumn`/`groupBy`/`orderBy`/`filter`/alias all
        fail loudly, and `drop`'s measured no-op is the Spark answer rather than a
        silent expansion.
      artifacts: [python/repark/src/repark/spark/dataframe/colregex.py]
    - id: AT-7
      status: N/A
      justification: Not a system-breaking change; expansion compiles the pattern
        once per `select` call. The remediation round measured the wide-frame arm
        directly (C-006) — binding matches only is 16.7–23.5× on 1–10 hits and
        parity at 500 — so the helper is strictly no worse than the shipped round-1
        body and far better on the common sparse-match shape.
    - id: AT-8
      status: ATTACKED
      evidence: No dependency or workflow edits; the one public-surface gain — the
        `colregex` submodule attribute on the dataframe package — is pinned in the
        export freeze (`EXPECTED_NEW_PACKAGE_SUBMODULES` plus the binding import),
        funded line-neutrally at the file's default ceiling.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py]
    - id: AT-9
      status: ATTACKED
      evidence: Failures stay diagnosable — the literal-name refusal lists the
        available columns, and the malformed-pattern refusal names the pattern and
        the `re` error; the pins assert the `AnalysisException` class Spark maps to
        `UNRESOLVED_COLUMN` / `INVALID_USAGE_OF_STAR_OR_REGEX`.
      artifacts: [python/repark/src/repark/spark/dataframe/colregex.py]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first was honest — both rewritten pins ran against the
        unchanged base tree and the verbatim `AnalysisException: No column matched
        regex` failures are pasted in the Red-first section; no test was edited to
        manufacture the red.
      artifacts: [python/repark/tests/test_examples_dataframe_a.py, python/repark/tests/test_examples_dataframe_d.py]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 EX-DF-1
- Marker: [../../../python/repark/src/repark/spark/dataframe/colregex.py](../../../python/repark/src/repark/spark/dataframe/colregex.py)
- Pins: [../../../python/repark/tests/test_examples_dataframe_a.py](../../../python/repark/tests/test_examples_dataframe_a.py), [../../../python/repark/tests/test_examples_dataframe_d.py](../../../python/repark/tests/test_examples_dataframe_d.py)
