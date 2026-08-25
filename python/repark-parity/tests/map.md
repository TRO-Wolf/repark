# map — python/repark-parity/tests

## Purpose

Unit tests for the parity comparison core **and the dataset generators** (no Spark, no
JVM, no repark required). See [../map.md](../map.md).

## Contents

- `test_dl_4_live_doc_compaction.py` — **DL-4 (2026-08-25; 11 tests, incl. the Critic's three
  pinned findings and the two tree pins C-006 / C-008):** the live-document compaction on
  a scratch repository — a merged unit leaves the slate whole and the table renumbers (C-002), a
  closed campaign is cut from STATUS into its history bin with links rewritten (C-003), the
  touched-path set (C-004), the parser's refusals (parametrized) and the coverage check (C-001; a wrapped closed-campaigns
  row is one row; a marker in a code span is prose), and the gate red
  on each of its four classes and green on the compacted tree (C-005).
- `test_dl_2_ledger_grammar.py` — **DL-2 (2026-08-23):** the ledger grammar gate on a scratch
  tree seeded with the script's own `EXCEPTIONS` rows at their ceilings: a clean ledger counts;
  a bad verdict cell, a duplicate id and a row without evidence go red; an unpinned `PROVEN`
  clause and a dead `pins:` citation go red, archived and completed clauses can be cited; the attestation is
  required once no clause is `OPEN` and its shape defects (no artifacts, no justification, a
  missing category, an inconsistent `complete:`) go red; a ledger with no clause table goes
  red; `FINDING:` fields are checked; a raised ceiling or a stale `EXCEPTIONS` row goes red
  against the real tree. Each test cites the DL-2 clause it pins. The DL-1 file's archive-row
  tests likewise cite the DL-3 clauses (the condense rule).
- `test_dl_1_ledger_lifecycle.py` — **DL-1 (2026-08-23):** the ledger lifecycle
  script on a scratch git repository: `archive` moves a `completed/` ledger to
  its dated archive name, rewrites every link to it (fragments kept, code spans
  untouched), re-expresses the ledger's own links, relocates its map row — whole
  into the live bins, condensed to one line (first sentence, `+ `-continuations
  joined) into an archive month map (DL-3) — and stages the lot; idempotent; a ledger not on `main` is left when unnamed (the pickup case)
  and refused when named; `move` to
  `completed`; `archive` is not a `move` target. The provocation proofs of
  `check`: a ledger outside the bins, an archive prefix disagreeing with its
  month, a dead ledger link in a non-map document, and the frozen rule (link
  repair and a prepended errata pass; a prose edit and a deletion fail).
- `test_pyc_6_docstring_presence.py` — **PYC-6 (2026-08-22; the prose-homes pin retargeted to PYC's
  history record by DL-4, 2026-08-25):** five presence
  rules only; style `D` not in py-lint select; tests keep the `D` per-file
  ignore; EXCEPTIONS is 39 keys summing to 136, no `/tests/` path, sorted;
  Ruff pin matches the Makefile; dual-wired `make ci` + ci.yml; on the
  pre-commit hook (conventions stays off).
- `test_pyc_5_close.py` — **PYC-5 (2026-08-22; the prose-homes pin retargeted to PYC's history
  record by DL-4, 2026-08-25):** nested-def EXCEPTIONS empty;
  DATACLASS leftover is dual-wire only; facade tests no longer ignore ANN201;
  conventions guard is not on the pre-commit hook and stays in `make ci` +
  ci.yml.
- `test_pyc_4_parity_harness.py` — **PYC-4 (2026-08-22):** nested-def EXCEPTIONS
  table empty; DATACLASS_EXCEPTIONS is only the dual-wire script; 20 converted
  files import-free of `dataclasses`; every converted `BaseModel` AST-pins
  `extra="forbid"` and not `strict=True`; lifted modules have zero nested `def`s;
  signal-handler / shrink-predicate / spy / dual-wire comparator keep a
  `# nested-def:` pragma *on the def line*; `CensusRow` extra-field refuse + int
  `test_id` refuse; recorded-denominator dummy ids are strings; `repark-parity`
  declares `pydantic>=2.10,<3`; isolated `make py-test` / ci.yml `--with pydantic`
  (C1-Q-001); root Ruff ANN ignores split so parity tests see ANN201/ANN202.
  **PYC-5:** facade ANN201 pin retargeted (ignore dropped).
  Behaviour stays on `test_compare_reports.py` / `test_compat_harness.py`.
- `test_datasets_secrets.py` — **DS-3** secrets fixture: A9 defaults, table-identity
  determinism, manifest class→column coverage in schema order, the needle labels
  re-derived with the `prop_key_is_secret` fold (lowercase, hyphen/dot → underscore,
  underscores stripped for the compact form) **without importing repark**, the
  `bucket_key` `_key` carve-out as a negative control, the hard hygiene fence (every
  value starts with `repark-fake-`; no `AKIA…`/`ghp_…`/`sk-…`/`xoxb-…` shape, no `@`,
  no URL), the one nullable credential column, parquet identity, CSV columns, CLI.
  **Acceptance pin in the module docstring:** reads behave NORMALLY today — the opt-in
  secrets-flagging mechanism is a roadmap feature this fixture predates, so nothing
  here asserts redaction. Facade read pins are DS-4.
- `test_datasets_smartcsv.py` — **DS-3** messy-CSV torture generator: A9 defaults,
  table-identity determinism, both manifest scopes (**column** classes present in
  `small()`; **file** classes provable in the emitted text at 64 rows), the delimiter
  zoo (comma / semicolon / tab / pipe, one file each, byte-equal to `render_csv`),
  BOM + preamble, duplicate header row emitted twice, ragged rows in both directions
  with the short-wins tie (row 137), bool spellings vs yes/no tokens that only look
  boolean, recognized
  vs literal null tokens, currency + decimal width variants, embedded-delimiter
  quoting in every scheme, parquet identity, CLI.
- `test_datasets_manifest_types.py` — **DS-3 rider (from the DS-2 review):** the
  manifest↔schema cross-check over all four labeled families (`schema_inference`,
  `extreme_types`, `secrets`, `smartcsv`). Every manifest-declared type string must
  equal the real Arrow field type after normalizing spacing (`decimal128(24, 21)` vs
  `decimal128(24,21)`) and pyarrow's rendering aliases (`double` → `float64`,
  `date32[day]` → `date32`). Both directions are closed: no manifest row may name a
  column the schema lacks, and no schema field may go unlabeled outside the explicit
  `EXPECTED_UNLABELED` set (`id` in the two DS-2 families). The normalizer is itself
  pinned so a no-op normalizer cannot hide a mismatch, and class ids must be unique.
- `test_datasets_schema_inference.py` — **DS-2** schema-inference generator: manifest
  class→column pin, A9 defaults, `conflict_at` int32→int64 + string/float halves,
  parquet identity, CSV text patterns, CLI `--conflict-at`. **DS-3 rider:** the
  `leading_zero_id` pad width is derived from the requested row count (a fixed `06d`
  loses the leading zero at `row_index >= 1_000_000`, and `MAX_ROWS` is 10M) — pinned
  at the >1M boundary through `leading_zero_width` / `leading_zero_id` with explicit
  widths, never by generating a million rows, plus a helper↔column binding test.
- `test_datasets_extreme_types.py` — **DS-2** extreme-types generator: manifest
  classes, decimal128(24,21), beyond-38 digit strings, uuid5, paragraph length,
  HTML example.com-only, parquet identity, CLI.
- `test_datasets_nested.py` — **DS-1** nested / dynamicFlatten generator: A9 defaults
  (64 / seed 42), table-identity determinism (not raw bytes), parquet + JSON-lines
  re-read under `SCHEMA`, labeled classes (depth ≥ 6, capitalized `Legs`, mixed list
  types, null-typed lists, empty/null list rows), cache symlink + in-repo refuse, CLI
  `--out`. Loads `repark_datasets` via the bench sys.modules loader. Ledger:
  `task/c18-datasets-ledger.md`.
- `test_compare.py` — equal/unequal frames, order-insensitivity, null handling, schema/row-count
  mismatches, and a **field-nullability difference** (part of the schema signature — a differing
  `nullable` flag with identical name/type/values is a parity failure). **G18 nested invariants:**
  (1) flat-schema `sort_by` path unchanged, (2) nested row-permutation invariance for
  list/struct/map, (3) multiset sensitivity mutation per nested kind, (4) `order_sensitive=True`
  untouched on nested tables; plus list-element-order significance and nested-only schemas.
- `test_compat_harness.py` — C2 / R-PYSPARK-COMPAT unit pins: message-first JVM classification,
  HARNESS narrowness, both denominators (incl. MODULE-TIMEOUT stay-in), `tag_for_pyspark_version`,
  worker env scrub, method-name filter (no prefix steal), `Py4J*` type → NEEDS-JVM,
  `validate_module_short` path-injection refuse, zero-padded tag normalize,
  TimeoutError → MODULE-TIMEOUT (RecordingResult + classify), third-party ImportError → HARNESS
  (incl. site-packages TB + **X1** cache path `repark-pyspark-tests` must not false-FAIL-MISSING;
  **octo C1** site-packages/repark frame + pandas still HARNESS; product `repark.*`
  ModuleNotFoundError + cache path stays FAIL-MISSING),
  unknown status clamp in rank+denominators, module-name dotted IDs, markdown cell escape;
  **C3** `STRETCH_MODULES`/`C3_EXPAND_MODULES` order pin (**U8:** `test_udf` IN);
  **octo C3** `resolve_census_modules` dual-denom pin (`--c3-expand` ignores night-1/`--stretch`);
  C3 series markdown never claims C2 zero-fix /345; ruff-clean import order + format;
  **r20 C4** `C4_EXPAND_MODULES` charter-order pin + `resolve_census_modules(--c4-expand)` dual-denom
  (never blend classic /345 or C3); C4 series/scratch distinct; `_KNOWN_FATAL_TESTS` disjoint
  from C4 cohort + exact-method deselect (not endswith prefix collision); dual-denom markdown %
  pin; filter×fatal dual-denom; testing.utils error rebind pin; PATCH_MAP AssertionError note;
  install_redirect errors-before-factories order; testing.utils rebind via **fake modules**
  (parity isolation — no pyspark/repark); `REPARK_COMPAT_SERIES` worker-only series branding
  pin (no C2 zero-fix on C4 workers; parent ignores leaked env — octo C5); frozen
  `_KNOWN_FATAL_TESTS` exact map pin (octo C6); worker JSON unknown-status clamp.
- **Phase-3 EC-8 additions to `test_compat_harness.py`** (6 tests, new in this repository):
  `CLASSIC_MODULES` charter order + disjointness from both expand cohorts;
  `resolve_census_modules(classic=True)` denominator isolation (hostile `--modules` ignored);
  the fixed precedence `--c4-expand` > `--c3-expand` > `--classic`; the keyword-only default
  that keeps every ported call site unchanged; the **`--stretch` blending pin** (eleven modules,
  not five — the trap `--classic` exists to avoid, documented in executable form); and the CLI
  wiring pin that `--classic` actually reaches the resolver.
  **G-6:** `default_markdown_report_path` pin — markdown defaults under gitignored
  `target/census-reports/` (C-2 alignment), never `task/`.
- `test_compare_reports.py` — the battery for `compat/compare_reports.py`, the census report
  comparator (NEW code; design §6.4). Over synthetic reports: identical → exit 0; each of the
  five delta directions (pass→fail, fail→pass, class-change, appeared, vanished) named and
  exit 1; both denominators re-asserted; deferred subtraction from the baseline side only, with
  the echo, the not-present entry, and the "deferred id appeared in the candidate" finding;
  quarantine excluded both sides; mismatched environment manifests → loud exit 2 **with no diff
  body at all**; `generated_at` / `repark_version` deliberately not gated; sorted-rendering byte
  exactness (a case-only class difference is still a difference); duplicate ids and malformed
  JSON → loud failure; junit mode with skips first-class (skip→pass detected, xfail
  distinguished from skip, manifests required); and the **provoked undeclared subtraction** that
  proves the checked-in ledger file is the only way a row leaves the diff (five plausible
  environment variables set, an `--exclude` flag attempted, the frozen option set asserted).
  Also pins the three properties that keep the manifest gate from being nominal: an external
  `--manifest-*` file may not overwrite a key the report records (the shared-manifest attack that
  makes two different environments render identical); `pandas_version` is refused when it
  **differs** and equally when it is **unrecorded** (a key nobody records compares equal by
  absence); and each report's own recorded denominator block is validated against its own rows —
  including the case where both sides are byte-identical *and* identically wrong, which the
  post-subtraction re-assert alone cannot catch.
- `test_deferred_ledger.py` — **phase-3 PR-5 (EC-4)**: the harness that binds the checked-in
  deferral ledger to the comparator's allowlist. There is exactly one file
  (`task/port/deferred-python-tests.txt`), so byte-identity is pinned by proving the single-file
  property — the comparator's documented acceptance invocation names that path, and its own
  `load_ledger` is what parses it here. Both EC-4 failure directions are closed: every deferred id
  must be a **pin-collected** name (under-subtraction — a row that names nothing removes nothing)
  and must be **absent from the ported tree** (over-subtraction — listed AND ported means a row is
  subtracted from the baseline while still running here). Absence is checked statically via `ast`,
  so the test needs no wheel and runs in the ordinary `make py-test` loop. Also pins that the
  prose ledger names every machine-readable id, so the two halves cannot drift. **PR-5 fixer:**
  a third direction is closed — every deferred id must resolve to a row of the recorded baseline
  JUnit XML *through `load_junit_report`*, the loader the facade cohort's `--junit` gate actually
  uses; the id-space mismatch that assertion catches made the ledger subtract nothing.
- `test_ta_bench_conf.py` — **BH-1 (conductor-19):** default-conf `target_partitions`
  contract for `bench/ta` (omit knob + emit `default`; isolation emits `1` +
  `isolation=single_core`). Helper unit pins + AST scan of the six scripts.
  No engine / numpy / native module.
- `test_redact.py` — the battery for `compat/redact.py`, the recorded path-redaction transform.
  Its one hard property is that the artifact still parses afterwards, so the two regressions are
  explicit contrasts: a naive text substitution over a traceback-bearing census report emits an
  unescaped quote and stops being JSON, and one over a JUnit XML turns `<scratch>` into an element
  start tag; the parser-based transform cannot do either. Plus key redaction, non-string scalars
  untouched, XML attributes, longest-prefix-wins ordering, malformed-input loud failures, plain
  text passthrough, in-place rewrite idempotence, and the CLI exit codes.

## Pointers

- Up: [../map.md](../map.md)

### PR-4 orchestrator additions
- `test_duplicate_test_id_loads_when_quarantined` / `…_without_quarantine_still_refuses` —
  both directions of the comparator's one duplicate escape (quarantined ids may repeat,
  first row wins), with the fixture recomputing the recorded denominator block over rows AS
  CARRIED — matching the real v1 expand artifact.

### PR-7: --added tests
- `test_added_cell_present_on_candidate_side_only_passes` / `test_added_does_not_subtract_from_the_baseline_side`
  — both directions of the additions mirror (candidate-side subtraction), plus the frozen-option
  pin now includes `--added`.

## Debug

| Symptom | First check |
|---|---|
| `test_datasets_manifest_types` reds | A schema field and its `manifest.json` row were edited one-sidedly; the failure names the family and class id |
| `test_datasets_secrets` reds on the hygiene fence | A value stopped starting with `repark-fake-` or picked up a real credential shape — fix the value, never the fence |
| `test_deferred_ledger` reds on "ALSO ported" | A node id is in the txt AND still defined in `python/repark/tests` — excise the test or drop the row |
| `test_deferred_ledger` reds on "absent from the recorded pin collection" | The id does not name a real v1 node; check it against `task/census/baseline-fc3f48102/facade/collected.txt` |
| `test_deferred_ledger` reds on the human summary | `task/port/deferred-tests.md` must name every id in the txt verbatim |
| `test_deferred_ledger` reds on "would subtract nothing" | The id does not survive `junit_node_id` into a row of `…/facade/facade.xml`; check the id form, not the XML (the XML is recorded evidence — never hand-edit it) |

First checks: `PYTHONPATH=python/repark-parity/src pytest python/repark-parity/tests -q`.
Escalate to: [../map.md#debug](../map.md).
