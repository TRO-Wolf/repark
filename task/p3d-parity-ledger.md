# Unit ledger — P3D: `python/repark-parity` + the census foundation (PR-4)

**Unit:** phase-3 PR-4 · **Brief:**
[../briefs/phase-3-python-facade.md](../briefs/phase-3-python-facade.md) §1 "PR-4" · **Design:**
[../docs/design/python-facade.md](../docs/design/python-facade.md) §1 (`none (verbatim)` + NEW
comparator), §3 EC-8/EC-9, §5 F1, §6 (the census procedure end to end), §9 PR-4 ·
**Port-Source:** the private v1 engine repository at the frozen port pin · **Status:** IN FLIGHT ·
**Runs parallel to:** phase-3 PR-3 (disjoint footprint: crate tree vs Python-harness tree)

## Scope

Land the parity package and the machinery the port's acceptance gate is made of. Five
deliverables, of which exactly one is a verbatim port and the rest are declared new code or
declared documentation.

1. **`python/repark-parity`, ported verbatim** — the comparison core (`src/repark_parity/`:
   `compare.py`, `__init__.py`, `py.typed`), `tests/` (`test_compare.py`,
   `test_compat_harness.py`), `compat/` (`bootstrap.py`, `classify.py`, `runner.py`, `fetch.py`,
   `smoke_suite.py`, `__init__.py`, `__main__.py`), `bench/` (tpch / tpcds / write / fuzz), and
   `record_ta_goldens.py`. `pyproject.toml` ported byte-identical (hatchling, `pyarrow>=25.0.0`,
   the `record` extra, version `0.0.0` — the `dynamic = ["version"]` change is a release-PR edit
   per design §4 Q6 and deliberately does **not** land here).
2. **The uv workspace root returns** — `pyproject.toml` gains `[tool.uv.workspace] members =
   ["python/repark-parity"]` **only**. The facade member joins in PR-5: declaring a member whose
   directory does not exist fails `uv lock`, so the list only ever names what is on disk. The
   `dev` dependency group and the ruff `src` / `known-first-party` / `**/tests/**` blocks are
   copied from the port source, minus the three facade-only per-file-ignore blocks (`ml/**`,
   `session/**`, `dataframe/**`) which name paths that do not exist yet and land with the facade.
   `uv.lock` is generated and **committed**; `.python-version` (3.12) was already present.
3. **The additive classic cohort (EC-8, design §5 F1)** — `compat/runner.py` gains
   `CLASSIC_MODULES` and a `--classic` flag with denominator-isolation semantics. The ported
   `--stretch` flag is left **byte-identical** and gains a test that pins its append-blending
   behavior.
4. **The NEW report comparator** — `compat/compare_reports.py` implementing design §6.4 exactly,
   with a 30-test battery over synthetic reports.
5. **`docs/port/census.md`** — the recorded census procedure (both sides' argument vectors, the
   environment recipe, the stability run + quarantine rule, the facade-cohort definition and its
   environment clauses, the comparator usage + attribution rule, the golden-corpus `basis:`
   designations). `scripts/run_census.sh` ported with one behavioral change (`--classic`).

Out of scope, and deliberately absent from this commit: the `.github/` python-job
extension/rename and `pip-audit.yml`, the Makefile targets, and the **v1-pin census baseline
artifacts** — all orchestrator carve-outs (see "Note on what lands after this commit"). No AWS
call was made; no `REPARK_*` / `TABLE_BUCKET_ARN` / `AWS_*` variable was set at any point (the
runner's secret-scrub name lists are ported **data**, not variables anyone set). `todo.md` is
untouched — the box turns at merge.

## Census obligation — verbatim port, name-identity (REQUIRED, DISCHARGED)

The port source is read-only; both sides were enumerated with the same generated command, never
hand-counted.

```
# both sides, identical invocation
PYTHONPATH=python/repark-parity/src uv run --no-project --with pyarrow --with pytest \
  pytest python/repark-parity/tests -q --collect-only | grep '::' | sort

v1 pin : 64 names   (tests/test_compare.py 9  +  tests/test_compat_harness.py 55)
this PR: 126 names  (64 identical + 36 declared new + 26 from the fixer pass)

$ diff pin.txt v2.txt | grep '^<'      # names REMOVED or RENAMED
(nothing)
$ diff pin.txt v2.txt | grep '^>' | wc -l
62
```

The 26 fixer-pass additions are enumerated in "Adversarial-review remediation" below; they are
new names in `test_compare_reports.py` (+9) and the new `test_redact.py` (+17). The ported half
remains an identity map: zero ported names removed, renamed, or moved.

**Zero ported names removed, zero renamed, zero moved between files.** The 64 ported names are
byte-identical to the pin, so the ported half is an identity map and no rename declaration is
needed.

### Correction to a stale orientation count (recorded, not hidden)

The design and the brief both describe this package as "58 unit tests" (49 + 9). **The generated
count at the pin is 64** (55 + 9). The counts are static orientation numbers carried forward from
an earlier snapshot; `def test_` in `test_compat_harness.py` is 44, and parametrization brings the
collected count to 55. Per the standing rule ("every count that gates anything is *generated*,
never hand-written"), the generated number is truth and the prose is stale. Nothing about the port
changes — 64 ported = 64 collected, empty diff — but the design/brief cell should be refreshed at
phase close rather than silently disagreeing with the gate.

### The 36 declared new tests

**`tests/test_compat_harness.py` (+6, EC-8 — the classic cohort):**

| test | what it pins |
|---|---|
| `test_classic_modules_charter_order` | `CLASSIC_MODULES` is the five-module /345 cohort in charter order, disjoint from both expand cohorts, and equal to night-1 + the two classic stretch modules |
| `test_resolve_census_modules_classic_is_denominator_isolated` | `--classic` returns ONLY that list; `--stretch` and a hostile `--modules` cannot leak in |
| `test_resolve_census_modules_classic_precedence_under_expand_flags` | fixed precedence `--c4-expand` > `--c3-expand` > `--classic` |
| `test_resolve_census_modules_classic_defaults_off_preserves_ported_behavior` | `classic` is keyword-only, default `False` — every ported call site is unchanged |
| `test_stretch_blends_c3_into_the_classic_denominator` | **the trap pin**: `--stretch` yields eleven modules, not five, and strictly contains `--classic`'s five. The trap is documented in executable form rather than merely dodged |
| `test_cli_classic_flag_reaches_the_resolver` | CLI wiring: `--classic` actually arrives at `resolve_census_modules(classic=True)` (constant equality is not enough — the composition site is what runs) |

**`tests/test_compare_reports.py` (+30, the comparator battery):** identical → exit 0; each of
the five delta directions named with exit 1 (pass→fail, fail→pass, class-change, appeared,
vanished); both denominators re-asserted and reported; deferred subtraction from the baseline side
only, with the echo, the "ledger entry matched nothing" echo, and the "deferred id nonetheless
appeared in the candidate" finding; quarantine excluded on both sides and reported separately;
mismatched environment manifests → loud exit 2 **with an empty stdout** (nothing was diffed);
`generated_at` / `repark_version` explicitly not gated; external manifest files compared too;
sorted-rendering byte exactness (a case-only class difference is still a difference); duplicate
ids and malformed JSON → loud failure; junit mode with skips first-class (a skip→pass detected,
xfail distinguished from skip, manifests required); and
`test_the_ledger_file_is_the_only_subtraction_input`, which **provokes** an undeclared
subtraction.

## The comparator (design §6.4, item by item)

| §6.4 requirement | where |
|---|---|
| (a) environment manifest compared FIRST, loud fail on any difference | `compare()` raises `ComparatorError` before `apply_ledgers` is reached; `main` prints to **stderr** and returns exit 2 with an empty stdout |
| (b) `{test_id → class}` per side; `(node id → outcome)` incl. skipped/xfailed in `--junit` | `load_census_report` / `load_junit_report` |
| (c) deferred allowlist subtracted from the v1 side only, echoed | `apply_ledgers` + `_echo_lines` (six echo buckets) |
| (d) quarantined rows excluded both sides, reported separately | `apply_ledgers` + the two `quarantined_*` echo buckets |
| (e) sorted-rendering byte comparison, no fuzzy match, no tolerance | `render_side` → UTF-8 byte equality |
| (f) both denominators re-asserted, fail on any difference | `compute_denominators` — census mode delegates to `compat.classify.denominators` itself |
| (g) delta grouped by direction | `compute_delta` / `_delta_lines` |
| (h) non-zero exit on ANY difference | `EXIT_DIFFERENT`; empty diff is the only pass |
| (i) the checked-in ledger file is the ONLY subtraction input | no `os.environ` / `getenv` anywhere in the module; frozen option set; provocation test |

### Judgement calls, declared (these are what a verifier should attack)

1. **Which JSON keys are the "environment manifest".** The v1 report writer
   (`CompatReport.to_dict`) emits `generated_at`, `pyspark_version`, `spark_tag`,
   `spark_commit_sha`, `repark_version`, `python_version`, plus `denominators`, `ranked_census`,
   `patch_log`, `findings`, `modules`. The gated manifest is the four **environment** keys
   (`python_version`, `pyspark_version`, `spark_tag`, `spark_commit_sha`).
   `generated_at` (wall clock) and `repark_version` (differs across the two repositories **by
   construction** — it is the thing being ported) are excluded, each with its reason recorded in
   `MANIFEST_EXCLUDED` and echoed in every report as "(not gated)". Gating either would make the
   comparator refuse every real comparison. The `pip freeze` half of the manifest (§6.1: "two runs
   whose freezes differ are not comparable") is supplied as an **external** JSON via
   `--manifest-baseline` / `--manifest-candidate` and compared key-for-key in full; in `--junit`
   mode those files are **required**, because JUnit XML carries no environment at all.
2. **Denominators are recomputed over the compared rows, not read from the report.** Subtracting
   the deferred ledger legitimately changes the recorded numbers, so comparing the reports' own
   `denominators` blocks would fail every honest run. The comparator therefore recomputes both
   denominators post-subtraction and post-quarantine — and, in census mode, does it by importing
   `compat.classify.denominators` and feeding it `CensusRow`s, so it can never drift from the
   runner's own definition of the rule. The reports' recorded blocks are still echoed, labelled
   "pre-subtraction, FYI only".
3. **The junit-mode denominator mapping is an invention, and is documented as one.** The charter
   classes do not exist in JUnit XML. Mapping: `pass` = `passed`; `engine_relevant` = everything
   except `skipped` / `xfailed` (the outcomes that mean the test never reached the engine); `error`
   is non-pass and engine-relevant. The multiset comparison is the real gate here and is strictly
   stronger than either ratio.
4. **JUnit node ids are reconstructed as `classname::name`.** That is the only node-identifying
   pair JUnit XML carries without a pytest plugin, and the procedure is deliberately
   plugin-free (§6.3). It is stable across two identical trees, which is exactly the comparison
   being made.
5. **A ledger entry that matches nothing does not by itself fail the run** — it is echoed loudly
   under `deferred_not_present_in_baseline`. One ledger is shared across cohorts and an entry
   belongs to at most one of them, so failing on non-match would red every cohort but one. The
   ledger-vs-allowlist byte-identity check is assigned by design EC-4 to its own harness test in
   the PR that generates the ledger (PR-5), not to the comparator.
6. **Duplicate test ids are a loud failure, not a merge.** A report with duplicate keys cannot be
   multiset-compared honestly; silently keeping the last one would hide a real defect.

## Declared edits beyond the verbatim copy

- **EC-8 (the classic cohort)** — `compat/runner.py`: +12 lines (`CLASSIC_MODULES`), +1 branch and
  a docstring in `resolve_census_modules`, +1 argparse option, +1 call-site kwarg. `STRETCH_MODULES`
  and the `stretch` branch are **byte-identical**. `diff -u` against the pin is 4 hunks, all
  additive.
- **EC-9 (public hygiene)** — `tests/test_compat_harness.py`: four traceback-fixture string
  literals embedded a real developer home-directory path — a forbidden pattern in this public
  repository. Each is rewritten to `/home/ci/…`. **Outcome-neutral by construction**: the
  three affected classifier tests assert on the `~/.cache/repark-pyspark-tests` cache-path
  substring and the `site-packages/repark` frame marker, neither of which contains the home
  directory; node ids are unchanged, and all three still pass with the same assertions.
  Enumerated: lines 304, 324, 327, 347.
- **EC-7 (map.md regeneration)** — eleven `map.md` files carried port-source-only pointers.
  Truthful rewrites, PR-2 F-1 precedent (a `map.md` with dead links violates the repo's hard
  map-accuracy rule; EC-7 exists for exactly this):
  - `compat/map.md` — five dead pointers re-pointed: the facade smoke-test path, the v1 unit
    ledger → this ledger, two v1 briefs → the in-repo design + `docs/port/census.md`, and the v1
    report directory → `task/census/`. Also gains the `compare_reports.py` row, the `--classic`
    note, and three new debug rows.
  - `map.md`, `src/repark_parity/map.md`, `tests/map.md` — `make parity` (a target that does not
    exist here yet; the Makefile wiring is an orchestrator carve-out) replaced by the real
    invocation; the parity map gains the comparator and census-procedure rows.
  - `tests/map.md` — gains the two new-test blocks.
  - six `bench/**/map.md` — the `python/repark/tests/…` rows are annotated "arrives with the
    facade package"; the tpch scheduled-workflow link is replaced by prose (no such workflow is
    wired here); and each file carries one declared note that the `task/…-report-*.md` scoreboards
    named in its recipe rows are port-source measurement artifacts that were not ported.
  - **New:** `python/map.md` (the container directory), and rows added to `map.md` (root),
    `docs/map.md`, `docs/port/map.md`, `scripts/map.md`, `task/map.md`.
- **`scripts/run_census.sh`** — ported with exactly one behavioral change: `run_cohort classic
  --stretch` → `run_cohort classic --classic`. Report output paths unchanged in shape. The header
  comment records the change, its reason, and the fact that the script needs the facade package
  (arriving in PR-5).

`compat/map.md` and `python/map.md` were re-checked with a link walker: every relative markdown
link in the ported tree now resolves to a file that exists.

## Gate results

Run in the PR worktree. `--workspace`, never `--all-features`, never `--no-verify`.

- **Parity harness — 126 passed.**
  `PYTHONPATH=python/repark-parity/src uv run --no-project --with pyarrow --with pytest pytest
  python/repark-parity/tests -q` → `126 passed` (64 ported + 36 declared new + **26 from the
  fixer pass**: 9 more comparator tests over the two defeated gates, and 17 for the new
  `compat/redact.py`). Generated per file: `test_compare.py` 9, `test_compat_harness.py` 61,
  `test_compare_reports.py` 39, `test_redact.py` 17.
  The `PYTHONPATH` prefix mirrors the port source's own `py-test` recipe — without it
  `test_compare.py` cannot import `repark_parity`.
- **`uvx ruff@0.15.22 check .`** → `All checks passed!`;
  **`uvx ruff@0.15.22 format --check .`** → `54 files already formatted`. The root ruff-config
  change reds nothing.
- **`uv lock --locked`** → `Resolved 18 packages` (validate-only, no rewrite). The lock was
  generated with the **pinned** `uv@0.9.5` (Makefile `UV_VERSION`); byte-compared against a lock
  generated by the locally installed uv — **identical**, so no toolchain-version drift is being
  smuggled in.
- **`make ci` — exit 0.** `cargo fmt --check`; clippy with the panic-ban deny list clean;
  `crate-dag: 11 internal edges clean across 8 of 9 mapped crates` (the ninth is `repark-python`,
  pre-declared in PR-1, not yet in the workspace); `lib-rs: 8 crate roots clean`;
  `cargo check --locked --workspace`; ruff check + format; taplo format + lint; typos.
- **`make test` — exit 0.** 28 test binaries, **1,209 passed**, every line `0 failed; 0 ignored`;
  doc-tests 0 across the workspace. Nothing in this PR touches the Rust side.
- Public hygiene: both mandated passes returned **0** matches against the forbidden-pattern list
  (the four home-directory literals were the only hits at copy time and are scrubbed under EC-9).
- Pre-commit hook (map.md lockstep, crate-DAG, lib.rs, `cargo fmt --check`, taplo, typos) passed
  on the single commit.

## Note on what lands after this commit (orchestrator carve-outs)

Three PR-4 deliverables are **not** in this commit by design; they land as orchestrator commits on
this same branch, before the PR opens:

1. `.github/workflows/ci.yml` — the `python` job extended (`uv lock --locked`, the parity-harness
   pytest) and renamed from "Python (ruff)", with the branch-protection context swap in the same
   change.
2. `.github/workflows/pip-audit.yml` — ported as-is (weekly cron + path-filtered; **not** a
   required check — a path-filtered required check deadlocks PRs).
3. The **v1-pin census baseline**: four cohorts + the mandatory stability self-diff + the
   quarantine list + the environment manifests, under `task/census/baseline-<pin>/`. Generating it
   is an operator/orchestrator local procedure (scratch interpreters, a network sparse clone,
   hours of wall clock) and was **not** run by this workstream. The procedure it must follow is
   `docs/port/census.md` §3.

Also deferred by design, not omitted: `make census` stays local + slate-run and is never CI-wired
(§4 Q4d); `scripts/check_lib_py.*` and `scripts/test_lock_gate.sh` return with the facade PR.

## Notes for the verifier

1. **The comparator's JSON-shape handling is the highest-value target.** The port source commits
   only markdown census reports (`task/pyspark-compat-report-*.md`) — **no JSON report is checked
   in anywhere**, so the shape was derived from the writer itself:
   `CompatReport.to_dict()` / `ModuleCensus.to_dict()` / `CensusRow.to_dict()` in
   `compat/runner.py` + `compat/classify.py`. The test fixtures reproduce that shape faithfully
   (all ten `CensusRow` fields, the module block, the top-level manifest keys, `denominators`,
   `ranked_census`, `patch_log`, `findings`). Attack it by generating a real report and feeding it
   in: the rows live at `modules[].rows[]` and the key is `test_id`.
2. **The six judgement calls above are the design surface.** Each is argued rather than assumed;
   disagreement with any of them is a design finding, not a bug report.
3. **The 58-vs-64 count correction** is a documentation finding against the design/brief, raised
   here rather than papered over. The port itself is unaffected.
4. The EC-9 scrub is the only edit to a ported test file's *content*. It is enumerated
   line-by-line above and is assertion-neutral; verify by reading the three tests' assertions.
5. `--stretch` is byte-identical on purpose. If a reviewer's instinct is "just fix
   `STRETCH_MODULES`", the trap pin (`test_stretch_blends_c3_into_the_classic_denominator`) is
   what stops that, and design §5 F1 is why.

## Baseline artifacts + carve-outs (orchestrator commit)

**The freeze-point baseline is COMMITTED** at `task/census/baseline-fc3f48102/` (procedure
run 2026-08-08 at the pin, per docs/port/census.md):

- classic **142/345** — run TWICE, `stability-self-diff.txt` ZERO rows (nothing quarantined);
- expand **44/171**; expand2 **87/167**;
- facade pair: **2,509 collected / 2,517 junit outcomes** (2,471 passed + 46 skipped, exit 0,
  debug wheel). The 8-count delta is reconciled by name: junit records module-level skip
  entries for the eight pyspark/duckdb-gated modules (compat smoke, four oracle modules,
  tpch/tpcds/fuzz smoke) that never reach per-test collection — the environment clauses
  (pyspark ABSENT, duckdb ABSENT) working as designed. The v2 acceptance run must reproduce
  the same pair shape.
- Design F2 empirically closed: PLAN.md's 135/345 and 41/167 were stale; the recorded runs
  above are the baseline. PLAN.md's table is replaced at phase close (PR-7), per the design.

**Recorded mechanical transform (public hygiene):** every absolute scratch path inside the
artifacts (pip-freeze editable URLs, report metadata, junit traces) is redacted to
`<v1-pin>` / `<baseline>` / `<scratch>` before commit. The identical transform applies to the
v2-side artifacts before manifest comparison. Rows/classes are untouched — paths do not
participate in the multiset.

**Operational finding (release-relevant):** the first facade-leg run installed
`repark==0.0.1` FROM PYPI — the name-reservation placeholder — because `uv` preferred the
higher index version over the local 0.0.0 wheel under `--find-links`. Rule from here on:
local wheels are installed **by explicit file path only**. docs/release.md's PyPI
pending-publisher wording needs the existing-project correction before the release PR
(carried as an open note).

**Carve-outs landed in this commit (orchestrator):** ci.yml `python` job renamed
"Python (ruff)" → "Python" + `uv lock --locked` + parity-harness pytest steps (required
context updates in the same change — command in the PR body); `pip-audit.yml` ported
(path-filtered, never required); Makefile `py-test` / `parity` / `py-lock-check` / `census`
(census inert until PR-5's facade, noted in-target).

**Builder-flagged count correction:** design/brief said "58 unit tests" for the parity
package; the generated count at the pin is **64** (53 static `def test_` + parametrization).
Both documents corrected in this PR; the port census is 64 = 64 + 36 declared additions.

## Adversarial-review remediation (fixer pass)

Two lenses (`port-process`, `design-census`) returned overlapping HIGH/MED findings against the
baseline-artifact commit and the comparator. They split cleanly into **code/procedure defects,
fixed here** and **artifact defects, which are evidence and are therefore handed back as a
regeneration obligation**.

### Fixed in code (with tests)

| # | Finding | Fix | Pinned by |
|---|---|---|---|
| 1 | **The manifest gate — the comparator's FIRST hard gate — was defeatable from the CLI.** `_load_sides` did `baseline.manifest.update(external_baseline)`, so `--manifest-baseline` / `--manifest-candidate` *overwrote* the reports' own gated keys. Two runs from genuinely different environments, handed one shared external manifest, printed "environment manifest (identical — gate passed)" over fabricated values and exited **0**. | External manifests now **augment only** (`merge_external_manifest`): they may fill a key the report does not record or restate one identically; a contradiction is a loud exit 2 naming every conflicting key. This is also what census.md §5 always claimed ("plus every key of any external manifest supplied"). | `test_external_manifest_cannot_overwrite_a_key_the_report_records`, `test_external_manifest_may_fill_a_key_the_report_does_not_record`, `test_restating_a_recorded_key_with_the_same_value_is_allowed` |
| 2 | **Nothing could detect a pandas-major difference**, though census.md:43 asserted "the comparator refuses to diff them" and design §6.1 calls the major non-negotiable. No pandas key existed in the report JSON or in `MANIFEST_KEYS`. | `pandas_version` + `pyarrow_version` added to `MANIFEST_KEYS`; **and** a required-key gate (`check_manifest_recorded`) — `python_version` + `pandas_version` (+ `pyspark_version` in census mode; the facade cohort is *defined* by pyspark being absent) must be present and non-empty on both sides. Equality alone was never a gate here: a key that neither side records compares equal by absence. `run_census.sh` now emits `census-manifest.json` with those versions and **aborts** under pandas ≥ 3. | `test_a_pandas_major_difference_is_refused`, `test_an_unrecorded_pandas_major_is_a_loud_failure`, `test_junit_mode_requires_the_pandas_major_but_not_pyspark`, `test_manifest_keys_are_the_documented_set` |
| 3 | **The denominator re-assert was tautological.** Denominators were recomputed over the same post-subtraction row dicts the byte comparison already compares, so `denominator_differences` could never be non-empty unless `byte_identical` was already `False`. A report whose *recorded* counts were wrong sailed through unremarked — which the real `expand` baseline exhibits (recorded `all_collected` 171 vs 169 actual unique ids). | The post-subtraction re-assert stays (design §6.4 (f) requires it), and the independent half is added: `check_recorded_denominators` validates **each report's own recorded block against the rows that report carries**, loud exit 2 on disagreement. Judgement call #2 below is amended accordingly. | `test_recorded_denominators_are_validated_against_the_reports_own_rows`, `test_the_recorded_denominator_gate_is_not_implied_by_the_byte_comparison`, `test_a_report_without_a_recorded_denominator_block_still_compares` |
| 4 | **The path-redaction transform was a mandatory procedure step that existed nowhere.** Not implemented in the repo, absent from census.md, and the token set documented in the baseline `map.md` (`<v1-pin>`/`<baseline>`/`<scratch>`) did not match the tokens actually applied (`<home>` 410×, `<baseline>` 290×, `<v1-pin>` 217×, `<scratch>` 0×). A v2 runner could not reproduce it — which is *how* it silently corrupted five artifacts. | **NEW `compat/redact.py`**: format-aware redaction **through each artifact's parser** (JSON loaded → string values rewritten → re-serialized; XML likewise; everything else plain text), with validity re-asserted before writing, longest-prefix-first mapping, and a loud failure on unparsable input. Tokens fixed at `<scratch>` / `<repo>` / `<home>`. Wired into `run_census.sh`; recorded as §3 step 5 of census.md; the baseline `map.md` token claim corrected. | `tests/test_redact.py` (18 tests) — including the two regressions stated as explicit contrasts: naive substitution over a traceback-bearing report emits an unescaped quote and stops being JSON, and over a JUnit XML turns `<scratch>` into a start tag |
| 5 | **No quarantine ledger was committed**, so census.md §5's own acceptance command fails: a missing ledger file is exit 2 by design, and "recording zero quarantined rows is a result" requires an empty file, not an absent one. | `task/census/baseline-fc3f48102/quarantine.txt` added — a ledger with a header and zero entries, matching the recorded (empty) stability self-diff, and flagged to be **re-derived** at regeneration rather than carried forward. Verified: the documented invocation now loads it and exits 0. | exercised by the documented invocation; ledger parsing itself is pinned by `test_ledger_parsing_ignores_comments_and_blanks` |

Also recorded: `run_census.sh` now fails the run at provisioning time on an empty `pip freeze`
or a missing gated version, so the "0-byte manifest" state cannot recur silently, and census.md
§3 gains a mandatory pre-commit assertion set (every JSON loads, every XML parses, the freeze is
non-empty, `classic-run1` vs `classic-run2` exits 0 **through the comparator**).

**Amendment to judgement call #2 above.** "Denominators are recomputed over the compared rows,
not read from the report" remains correct as the *comparison* rule — subtracting the deferred
ledger legitimately changes the recorded numbers. What was wrong was inferring from that that the
recorded block needs no checking at all. It is now checked against its own rows (an intra-report
consistency property, unaffected by subtraction), and only the cross-report comparison uses the
recomputed values.

### Handed back: the baseline artifacts must be regenerated (NOT fixed here)

`task/census/baseline-fc3f48102/` is **evidence**, and the contract is explicit that evidence is
never hand-edited — a re-run replaces the whole directory in one commit. The corruption is not
losslessly reversible in any case: the textual transform collapsed escaped `\"` and genuine
string-terminating `"` into the same character, so a blanket un-escape breaks the other case.
The defects are enumerated with reproductions in
[`task/census/baseline-fc3f48102/map.md`](census/baseline-fc3f48102/map.md) "REGENERATION
REQUIRED", and in summary:

1. All four `compat-report.json` files are **invalid JSON** (214/214/137/110 broken escape sites);
   the comparator exits 2 on every one, so the "stability empty diff, exit 0" claim was never
   produced through the instrument that gates the phase. Repairing the escapes in a scratch copy
   *does* reproduce the empty diff at 142/345 both sides — the stability result stands, the
   artifact does not.
2. `facade/facade.xml` is **not well-formed** (46 raw `<v1-pin>` tokens in character data); junit
   acceptance cannot run even against itself.
3. `census-venv-freeze.txt` is **0 bytes**; the three Apache cohorts' pandas major is recorded
   nowhere. Design §5 F2: a baseline whose environment is not recorded is not a baseline. With
   fix #2 above, the regenerated baseline cannot pass the gate without it.
4. `expand/compat-report.json` has **duplicate `test_id`s with conflicting statuses** (two
   `UDFInitializationTests` rows), refused by design, plus recorded 171 vs 169 actual unique ids.
5. **The facade cohort violated two clauses of its own definition** (§6.3): pandas **3.0.5**
   against the mandated `pandas>=2.1,<3`, and a **JVM on PATH** (`java-11-openjdk`, not the pinned
   Temurin 17). Neither was declared as a deviation in this ledger — they are declared here now.
   The manifest also summarises gate variables as "all `REPARK_*` + `TABLE_BUCKET_ARN`" where
   §6.3 requires each of the thirteen names listed individually, and never states the
   pyspark-ABSENT / duckdb-ABSENT clauses. Mitigating and verified: pyspark and duckdb *are*
   genuinely absent and the skips fired for the recorded reason, and all four extras are present.

**Consequence for the phase.** Design §6.6 items 1 and 3 take this directory as the gate input at
phase close. Until it is re-run under the corrected procedure it cannot serve that role, and no
comparator invocation against it can be cited as evidence. The regeneration is an
operator/orchestrator procedure (scratch interpreters, network sparse clone, hours of wall
clock), exactly as its generation was.

### Notes for the orchestrator (not edited here, by instruction)

- Nothing in `Makefile` or `.github/` required a change for these findings. Worth considering at
  regeneration time: a cheap `make` / CI check that every committed `task/census/**/*.json` loads
  and every `*.xml` parses would have caught findings 1 and 2 at commit time for ~1 second of
  wall clock. Not added here since Makefile/workflow edits are carve-outs.
- The corrected procedure now requires `--manifest-baseline` / `--manifest-candidate` in census
  mode as well as junit mode; any orchestrator runbook that reproduces census.md §5's older
  four-flag invocation should be updated with it.

## Baseline REGENERATED (orchestrator, post-verification)

The verify lenses found the first committed baseline unusable (sed-corrupted JSON escapes ×677,
XML-hostile placeholder tokens, 0-byte census freeze, un-conforming facade env) — every defect
in the ORCHESTRATOR's assembly commit, none in the builder's. Regeneration record:

- **Redaction is now the format-aware `compat.redact`** (fixer-built, unit-tested, wired into
  run_census.sh): JSON rewritten through the JSON parser, XML through the XML parser, validity
  re-asserted before write. Tokens `<repo>` / `<scratch>` / `<home>`. Both corruption modes are
  pinned as explicit test contrasts. All committed artifacts re-validated: every JSON loads,
  the junit XML parses, both freezes non-empty, zero forbidden-pattern hits.
- **Census freeze recorded** (pandas 2.3.3 / pyarrow 25.0.0 / pyspark 4.1.2 — the venv was
  conforming all along; only the recording had failed), manifest enriched with the load-bearing
  versions + gate variables enumerated by name. `census-manifest.json` + `facade-manifest.json`
  carry the external halves the comparator gates on.
- **Facade cohort re-run TWICE, and the second run was evidence:** under the recipe's literal
  pandas<3 (2.3.3) the suite FAILS one test (`test_to_pandas_with_nulls_values_and_dtypes`) —
  v1's own CI is green under pandas 3 (extras resolve fresh). The clause was wrong, not the
  run: census.md now scopes `<3` to the Apache cohorts and records pandas major 3 for the
  facade cohort, with this measurement as the citation. Final recorded run: pandas 3.0.5,
  JVM-free PATH (symlink-shim, verified), 2,509 collected / 2,517 outcomes (2,471 passed +
  46 skipped, exit 0).
- **Expand duplicate ids quarantined** (the runner emits two `test_udf` rows twice with
  conflicting classes — pin behavior): `quarantine.txt` names them with the reason; the
  comparator's duplicate refusal gained its ONE escape (quarantined ids may repeat; first row
  wins; excluded + echoed), and the recorded-denominator gate validates against rows AS
  CARRIED (duplicates included). Both directions pinned by new tests (128 total now).
- **The gate has now actually RUN on the committed artifacts:** classic-run1 vs classic-run2 →
  exit 0 (the stability claim, through the real instrument); expand and expand2 self-checks →
  exit 0 with the quarantine ledger; facade junit self-check → exit 0. Command lines as in
  census.md §5.
- EC-7 map-count correction: 10 rewritten map.md files, not 11 (src/map.md ported verbatim).
