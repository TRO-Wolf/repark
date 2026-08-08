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
this PR: 100 names  (64 identical + 36 declared new)

$ diff pin.txt v2.txt | grep '^<'      # names REMOVED or RENAMED
(nothing)
$ diff pin.txt v2.txt | grep '^>' | wc -l
36
```

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

- **Parity harness — 100 passed.**
  `PYTHONPATH=python/repark-parity/src uv run --no-project --with pyarrow --with pytest pytest
  python/repark-parity/tests -q` → `100 passed in 0.28s` (64 ported + 36 declared new).
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
