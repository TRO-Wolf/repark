# Charter ledger — SESSION-SURFACE-1 · the 19 SparkSession surface names

**Date:** 2026-09-14 · **Branch:** `feat/session-surface-1` · **Base:** `origin/main`
`7693ef23` · **Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** seven rows filed at §5 end — `SES-TAG-1`, `SES-INTERRUPT-1`,
`SES-DECL-readStream`, `SES-DECL-streams`, `SES-DECL-dataSource`, `SES-ARTIFACT-1`,
`SES-PROFILE-1`, `SES-TVF-1`, all DECLARED 2026-09-14.

**Why now.** The 1.5 PySpark-parity campaign takes the SparkSession facade surface name by
name; this card carries the 19 that are neither catalog nor frame-builder: job tags, the
interrupt trio, the five Connect-only names, `readStream`/`streams`/`dataSource`,
`addArtifact`/`addArtifacts`, `profile`, and `tvf`. The oracle is a live PySpark 4.1.2
classic local-mode session recorded 2026-09-14 (`facade_session_oracle.json`, copied
unchanged into `python/repark/tests/`).

**Not in this unit:** `functions*.py` (run 15a owns the generator implementations whose
refusals ride through `tvf` tonight); `session/sql*.py`, `session_configuration.py` (run
15c owns the `sql()` hook line); any Rust change (the card's names are API plumbing — see
the per-name table); Structured Streaming or Connect-client plumbing.

## PROPOSITION LEDGER — SESSION-SURFACE-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `addTag`/`removeTag`/`getTags`/`clearTags` answer Spark over a per-session set: round-trip, `getTags` a fresh `set`, `removeTag` on a missing or empty tag a no-op, `""` and `,` tags raising `IllegalArgumentException` with Spark's own messages, non-str raising `NOT_STR` (SES-TAG-1 declared divergence from Spark's Py4J leak). | `test_session_surface_1.py` tag pins, cells `addTag`, `getTags*`, `removeTag*`, `clearTags`, `addTag_empty`, `addTag_comma`, `addTag_not_str`. | **PROVEN** | 34/34 red on the base (`AttributeError: 'ReparkSession' object has no attribute 'addTag'` and peers — names absent); all green after. `str()` of each validation error is pinned byte-equal to the oracle cell. pins: session-surface-1/C-001 |
| C-002 | `interruptAll`/`interruptTag`/`interruptOperation` answer `[]` (Spark's idle-session answer, ruling R-2) and `interruptOperation` validates numeric-string ids with Spark's message. | Interrupt pins, cells `interruptAll`, `interruptTag`, `interruptOperation`, `interruptOperation_numeric`. | **PROVEN** | All three return `[]`; non-numeric id raises `IllegalArgumentException` `executionId must be a number in string form.` byte-equal to the cell. Registry `SES-INTERRUPT-1` records that nothing is ever actually interrupted. pins: session-surface-1/C-002 |
| C-003 | The five Connect-only names raise classic's own refusal byte-for-byte: `PySparkRuntimeError` `ONLY_SUPPORTED_WITH_SPARK_CONNECT` `{"feature": "SparkSession.<name>"}`. | Cells `client`, `copyFromLocalToFs`, `registerProgressHandler`, `removeProgressHandler`, `clearProgressHandlers` — `str()` equality, not just the class. | **PROVEN** | Parametrized pin compares `str(error)` to the recorded classic message verbatim for all five (matches classic, so no registry row per the card). pins: session-surface-1/C-003 |
| C-004 | `readStream`, `streams`, `dataSource` are dated `NOT_IMPLEMENTED` declared refusals — no hollow DataStreamReader / StreamingQueryManager / DataSourceRegistration. | Cells `readStream_type`, `streams_type`, `streams_active`, `dataSource`; registry rows `SES-DECL-*`. | **PROVEN** | Each property raises `PySparkNotImplementedError` `NOT_IMPLEMENTED` `{"feature": "<name>"}`; three registry rows filed. pins: session-surface-1/C-004 |
| C-005 | `addArtifact`/`addArtifacts` reproduce Spark's validation order: two flags → `INVALID_MULTIPLE_ARGUMENT_CONDITIONS` (Spark's own formatter crash recorded in the row), duplicate-different target → `DUPLICATED_ARTIFACT`, `pyfile=True` copies to a per-session dir prepended to `sys.path` once, missing source → `FileNotFoundError` naming it, `archive`/`file` declared, no-flag call the classic no-op. | Cells `addArtifact`, `addArtifact_real`, `addArtifacts_two_flags`; `test_add_artifact_pyfile_lands_on_sys_path` imports the landed module. | **PROVEN** | The pyfile pin writes a real module, adds it twice (second add rides the identical-content `continue`), imports it, and asserts `MARKER`. `file=True`/`archive=True` raise `NOT_IMPLEMENTED`; `addArtifact` and `addArtifacts` share one body like Spark's alias. pins: session-surface-1/C-005 |
| C-006 | `profile` answers a `Profile` object with Spark's public method surface; `show`/`dump`/`clear` are empty-collector no-ops carrying Spark's `memory_profiler` warning; `type=` validates `VALUE_NOT_ALLOWED`; `render` is declared. | Cells `profile_type`, `profile_methods`, `profile_show`; registry row `SES-PROFILE-1`. | **PROVEN** | `dir()` of the facade object is pinned equal to the oracle's method list; `show()` prints nothing and warns exactly as classic does with no profiles; `render` raises `NOT_IMPLEMENTED` `{"feature": "profile.render"}`. pins: session-surface-1/C-006 |
| C-007 | `tvf` is a `TableValuedFunction` with exactly Spark's public methods; `range` delegates to `session.range`, generators select the same-named `repark.spark.functions` call over a one-row `range(1)`, non-Column args raise `NOT_COLUMN`, `json_tuple` with no fields raises `CANNOT_BE_EMPTY`, and names whose engine path is absent keep a loud refusal — pinned *as* refusals so they go red when run 15a lands. | Cells `tvf_type`, `tvf_methods_full`, `tvf_range`, `tvf_explode`, `tvf_explode_outer_empty`, `tvf_stack`, `tvf_sql_keywords`, `tvf_collations`, `tvf_posexplode`, `tvf_json_tuple`, `tvf_inline`, `tvf_variant_explode`. | **PROVEN** | `dir(spark.tvf)` equals the oracle's 14-method list. `range`/`explode`/`explode_outer`/`stack` frames pin columns, schema and rows against the cells. Refusals pinned today: `posexplode`, `posexplode_outer`, `json_tuple` (function refusals ride through), `inline`, `inline_outer`, `variant_explode`, `variant_explode_outer` (no `F.*` name → `tvf.<name>`), `sql_keywords`, `collations` (SQL door has no table function → `tvf.<name>`), `python_worker_logs`. pins: session-surface-1/C-007 |
| C-008 | Registry rows, maps, fixture and examples are in lockstep: eight `SES-*` rows at §5 end; `facade_session_oracle.json` copied unchanged with a tests `map.md` row; `session/map.md`, `docs/examples/session/map.md`, `task/ledgers/staging/map.md` updated; three example scripts cover all 19 `SparkSession.*` names; `docs/examples/inventory.txt` regenerated. | `test_reg_1_registry_truth_up` 6 passed; `check_example_coverage.py` zero coverage findings; `check_map_md` clean. | **PROVEN** | `git diff docs/examples/inventory.txt` adds exactly the 19 session rows; coverage gate reports the three new scripts covering all 19; every touched directory's `map.md` names its new file in the same commit. pins: session-surface-1/C-008 |
| C-009 | No regression: the session test files stay green and the API inventory count is updated for the 19 names. | `test_session*.py`; `test_ex_0_example_coverage.py`; `check_lib_py.py`. | **PROVEN** | `test_session.py` + `test_session_range.py` + `test_session_surface_1.py` 91 passed; `test_session_config_knobs.py` + `test_session_sources.py` 82 passed; `len(rows)` moved 930 → 949. `session_core.py` holds its exact 2304 baseline (five one-line signature joins funded the import plus 19 bindings). Three `test_session_timezone_parity` cells fail on `repark._native` lacking `default_timestamp_descriptor` — a stale vendored native module in this Python-only clone, reproduced on paths this diff never touches; recorded as out-of-scope. pins: session-surface-1/C-009 |

## Per-name decisions

| Name | Disposition | Why (one line) |
|---|---|---|
| `addTag` | implemented | per-session tag set; classic answers it without Connect |
| `removeTag` | implemented | same set; missing/empty tag is Spark's no-op |
| `getTags` | implemented | fresh `set` copy — mutation cannot leak into session state |
| `clearTags` | implemented | same set; empties it |
| `interruptAll` | implemented | `[]` is Spark's idle answer; R-2 records no cancellable registry |
| `interruptTag` | implemented | same; the tag argument is validated then `[]` |
| `interruptOperation` | implemented | same; numeric-string validation kept byte-equal |
| `client` | implemented | byte-identical classic `ONLY_SUPPORTED_WITH_SPARK_CONNECT` refusal |
| `copyFromLocalToFs` | implemented | same refusal, argument shapes kept |
| `registerProgressHandler` | implemented | same refusal |
| `removeProgressHandler` | implemented | same refusal |
| `clearProgressHandlers` | implemented | same refusal |
| `readStream` | declared | Structured Streaming engine absent — `SES-DECL-readStream` |
| `streams` | declared | same engine gap — `SES-DECL-streams` |
| `dataSource` | declared | Python data-source execution deferred — `SES-DECL-dataSource` |
| `addArtifact` | implemented | alias of `addArtifacts`, like Spark |
| `addArtifacts` | implemented | driver-local `pyfile` only; `archive`/`file` declared — `SES-ARTIFACT-1` |
| `profile` | implemented | empty-collector `Profile`; `render` declared — `SES-PROFILE-1` |
| `tvf` | implemented | wrapper over `range` + `F.*` generators; missing engines declared — `SES-TVF-1` |

**Rust-first note (per name):** tags and interrupts are Python argument validation over a
session dict; the Connect-only and `SES-DECL-*` names are pure refusals; `addArtifacts`
moves bytes on the driver filesystem and edits `sys.path` — Python-only by definition;
`profile`/`tvf` are facade objects whose working legs delegate to `session.range` and the
already-Rust `repark.spark.functions` calls. No new kernel, planner rule or renderer
exists in this card, so there is nothing to move into Rust.

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.
