# The census procedure — recorded, end to end

Status: **RECORDED 2026-08-08** (phase-3 PR-4). This file is the executable-in-prose SSOT for the
port's acceptance measurement. It exists so that two people, in two repositories, months apart,
run the *same* measurement — and so that a run whose environment was not recorded is recognizable
as not-a-baseline.

Authority: [../design/python-facade.md](../design/python-facade.md) §6 is the design; this file is
the procedure. Where they disagree, the design wins and the disagreement is a finding.
Companion: [PLAN.md](PLAN.md) (the port phases and the acceptance gate),
[../../task/port/deferred-tests.md](../../task/port/deferred-tests.md) (the deferral ledger and its
reconciliation rule).

**Who runs this.** The baseline generation (§3) and the acceptance run (§4) are **local operator /
orchestrator procedures**: scratch interpreters, a network sparse-clone of the Apache Spark test
tree, and hours of wall clock. They are never CI-wired and never delegated to an agent with
environment-variable access. The artifacts are committed as evidence.

---

## 1. The environment is part of the pin

The census classifies real test outcomes, so the interpreter environment is an **input to the
result**, not a detail of how it was obtained. Both sides run the identical recipe:

| Component | Pin | Note |
|---|---|---|
| Python | **3.12** | `.python-version`; abi3-py312 floor |
| `pyspark` | **==4.1.2** | the Apache test tree is fetched at the tag matching this version |
| `pyarrow` | `>=25` | the wheel's single runtime dependency |
| `pandas` | **`>=2.1,<3`** | the major is load-bearing — see below |
| `maturin` | **1.14.1** | same pin as the Makefile / workflows |
| `uv` | **0.9.5** | same pin as the Makefile `UV_VERSION` / every `setup-uv` job |
| Rust | **1.96.0** | `rust-toolchain.toml` |
| JDK | Temurin **17** | only where a JVM is needed; the cohorts themselves need none |

**Why the pandas major is load-bearing, and non-negotiable.** Apache's own test helpers import
`pandas.core.common._builtin_table`, a private symbol **removed in pandas 3**. Under pandas 3 those
rows do not fail as engine gaps — they raise `ImportError` inside a third-party frame and the
classifier correctly calls them `HARNESS`, which moves them out of the engine-relevant denominator
entirely. 55 rows moved class this way in the port source's own history (they were dropped from the
always-green pin list for exactly this reason). **A census run under pandas 3 is a different
measurement, not a noisier one.** Two runs that disagree on the pandas major are not comparable, and
the comparator refuses to diff them.

**The Apache test tree is never committed.** `compat.fetch.ensure_spark_tests` sparse-clones the
Spark tag at run time into `~/.cache/repark-pyspark-tests/<tag>/`. Nothing from that tree enters
either repository.

**The full `pip freeze` of the scratch interpreter is recorded verbatim** alongside the run, as the
run's environment manifest. The comparator compares manifests **first** and fails loudly on any
difference rather than diffing anyway (§5).

---

## 2. The cohorts and their exact argument vectors

Four cohorts. **Denominators are never blended** — each cohort is measured against its own
collected set, and a run that mixes two cohorts has measured neither.

All census invocations run from the repository root with:

```
export PYTHONPATH="$PWD/python/repark-parity:$PWD/python/repark/src"
export REPARK_COMPAT_SCRATCH="<per-cohort scratch dir>"     # keeps worker JSON per cohort
```

| Cohort | v1 (port source) argument vector | v2 (this repository) argument vector |
|---|---|---|
| **classic** (/345) | `python -m compat.runner --modules test_functions,test_dataframe,test_types,test_column,test_readwriter --output classic.json --markdown classic.md` | `python -m compat.runner --classic --output classic.json --markdown classic.md` |
| **expand (C3)** | `python -m compat.runner --c3-expand --output expand.json --markdown expand.md` | *identical* |
| **expand2 (C4)** | `python -m compat.runner --c4-expand --output expand2.json --markdown expand2.md` | *identical* |
| **full-extras facade** | §4 | §4 |

**The classic-cohort trap, and why the two vectors differ (design §5 F1, EC-8).** The port source's
census script ran the classic cohort with `--stretch`. `--stretch` **appends** — night-1 plus
`test_column` / `test_readwriter` **plus the whole six-module C3 cohort** — an eleven-module run
scored against a five-module denominator. The classic denominator is exactly
`test_functions` + `test_dataframe` + `test_types` + `test_column` + `test_readwriter`.

The fix is deliberately two-layered:

1. **Invocation pinning on both sides.** The v1 side passes the five modules explicitly with
   `--modules` and **never** `--stretch`. The port source needs no edit and stays read-only; a
   v1-side script bugfix is optional and is the operator's call.
2. **An additive v2 flag.** This repository's `compat.runner` gained `CLASSIC_MODULES` and
   `--classic`, which returns **only** that five-module list (precedence: `--c4-expand` >
   `--c3-expand` > `--classic` > `--modules`/`--stretch`). The ported `--stretch` flag is
   byte-identical to the port source and carries a unit test that **pins its append-blending
   behavior**, so the trap is documented in executable form rather than merely dodged.
   `scripts/run_census.sh` uses `--classic`.

Both vectors resolve to the same five modules in the same order — that is the property that makes
the two sides comparable, and it is the reason the argument vectors are recorded here rather than
retyped per run.

---

## 3. Generating the freeze-point baseline (v1 side, read-only)

In a **read-only** worktree of the port source at the frozen pin — never a push, never a fetch,
never a write to a tracked file:

1. **Provision the scratch interpreter.** Create the venv, install the facade editable plus the
   pinned oracle stack (§1), then `maturin develop`. Record `pip freeze` verbatim as the run's
   environment manifest.
2. **Stability run — first, and mandatory.** Run the **classic cohort twice** and diff the two JSON
   outputs *against each other* before anything is compared across repositories. The Apache suite
   touches the filesystem, the clock, and a network-fetched source tree. **A row that is not stable
   against itself cannot be evidence about a port.**
   - **Quarantine rule (hard):** every row that self-differs is quarantined **by name** in the
     baseline directory as known-unstable, excluded from the gate on **both** sides, and counted
     separately in every comparator report. The quarantine list is a checked-in ledger file — it is
     the only mechanism by which a row leaves the gate, and it is visible in the diff forever.
   - A gate whose flake floor is unmeasured cannot tell a port defect from suite noise. Recording
     "zero quarantined rows" is a result; skipping the stability run is not.
3. **classic**, **expand**, **expand2** — the v1 argument vectors from §2.
4. **full-extras facade** — §4.

The four JSON reports, the four markdown reports, the stability self-diff, the quarantine list, and
the freeze manifest are committed **here** under `task/census/baseline-<pin>/`. They are **evidence,
not source**: never hand-edited, and a re-run replaces the whole directory in one commit.

`PLAN.md`'s historical numeric table is replaced at phase close by a pointer to this recorded run —
a stale baseline table invites comparison against the table instead of against the measurement.

---

## 4. The full-extras facade cohort — definition

The fourth acceptance row has no counterpart in the port source under any name. It is defined as:

> **The entire facade test suite, executed against an installed wheel, with every optional extra
> present and every gate variable unset.**

Concretely: build the wheel; create a bare interpreter **outside** the uv workspace; install
`repark[pandas,polars,numpy,ml-ext]` plus `pytest` plus the parity package **explicitly** (the bare
interpreter is outside the workspace, so the parity package is not resolved implicitly — and nine
facade test files import it); then run two invocations whose outputs are **both** recorded
artifacts:

```
python -m pytest python/repark/tests --collect-only -q  > collected.txt
python -m pytest python/repark/tests -q --junitxml=facade.xml
```

**The environment clauses are part of the definition.** Each is enumerated in the recorded manifest
with the evidence that it held:

- **Every gate variable unset, by name.** The manifest lists each one it verified absent:
  `REPARK_AWS_ACCEPTANCE`, `REPARK_ACCEPT_DS`, `REPARK_ACCEPT_ENTITY`, `REPARK_ACCEPT_ID_COL`,
  `REPARK_PG_DSN`, `REPARK_PG_SCALE`, `REPARK_PARITY_LIVE`, `REPARK_TPCDS_FULL`,
  `REPARK_WRITE_BENCH_RELEASE`, `REPARK_ML_FORMAT`, `REPARK_ML_VERSION`, `REPARK_LOG`, and
  `TABLE_BUCKET_ARN`. Verification is by absence check only — **the procedure never sets one.**
- **No JVM on `PATH`.** pyspark-gated tests must skip for the recorded reason, not accidentally run.
- **pyspark ABSENT** from the interpreter. Ten `importorskip` sites would otherwise silently change
  outcome class.
- **duckdb ABSENT.** Three `importorskip` sites; duckdb is a dev-group dependency rather than an
  extra, so an unstated venv choice would silently vary the cohort.
- **The four extras present, by name:** `numpy`, `pandas`, `polars`, `ml-ext`.

**The recorded quantity is a pair of multisets, not a number:** the collected-name multiset (the
relocation-discipline artifact — names identical across repositories modulo the declared deferral
list) and the `(node id → outcome)` multiset from the JUnit XML, outcome ∈ `passed | failed |
skipped | xfailed | error`. **Skips are first-class outcomes**: a test that silently stops skipping
is exactly as interesting as one that stops passing. The headline `passed / total` is *derived* and
reproduced for human use only.

JUnit XML keeps the procedure dependency-free (no pytest plugin) and node-id-keyed, which is what
makes this a multiset comparison rather than a score comparison.

Why full-extras: the ML, polars, pandas and numpy paths are a large fraction of the facade and are
exactly the paths a partial install silently skips. **A cohort that lets an install decision change
its denominator is not a gate.**

---

## 5. The comparator

`python/repark-parity/compat/compare_reports.py` turns two reports into a verdict. It is new code in
this repository (the port source emits reports; nothing in either repository judged them).

```
PYTHONPATH=python/repark-parity python -m compat.compare_reports \
    --baseline  task/census/baseline-<pin>/classic.json \
    --candidate task/census/v2-<sha>/classic.json \
    --deferred  task/port/deferred-python-tests.txt \
    --quarantine task/census/baseline-<pin>/quarantine.txt
```

For the facade cohort, add `--junit` and pass the two JUnit XMLs plus
`--manifest-baseline` / `--manifest-candidate` (JUnit XML carries no environment, and a run whose
environment is not recorded is not comparable — so junit mode **requires** the manifests).

What it does, in order — every step a hard gate:

1. **Environment manifests first.** Any difference is a loud failure (exit **2**) *before any row is
   looked at*. The gated keys are `python_version`, `pyspark_version`, `spark_tag`,
   `spark_commit_sha`, plus every key of any external manifest supplied. `generated_at` and
   `repark_version` are deliberately **not** gated (they differ by construction) and are echoed as
   such.
2. **Ledger subtraction, echoed.** The deferred ledger is subtracted from the **baseline (v1) side
   only**; the quarantine ledger is excluded on **both** sides. Everything removed — and every
   ledger entry that matched nothing, and every "deferred" id that nonetheless appears on the
   candidate side — is printed, so the reconciliation identity is visible in the output.
3. **Sorted-rendering byte comparison.** Each side renders to sorted `test_id<TAB>class` lines and
   the two renderings are compared byte for byte. No fuzzy matching, no aggregate-only comparison,
   no per-class tolerance.
4. **Both denominators re-asserted** — `pass / all_collected` and `pass / engine_relevant` —
   recomputed over the compared rows using `compat.classify.denominators` itself (so the comparator
   cannot drift from the runner's definition). Any difference fails.
5. **Delta grouped by direction:** pass→fail, fail→pass, class-change, appeared, vanished, with both
   classifications per cell.
6. **Exit non-zero on any difference.** Empty diff is the only pass. Exit codes: `0` identical,
   `1` any difference, `2` loud failure (manifest mismatch, malformed or duplicate-keyed report,
   missing ledger file).

**The checked-in ledger files are the ONLY subtraction inputs.** There is no flag, environment
variable, or config path by which a row can be excluded without appearing in a ledger file: the
module reads no environment at all and its option surface is frozen. A unit test **provokes** an
undeclared subtraction — it sets five plausible exclusion environment variables, passes an
`--exclude` flag, and asserts the comparator still exits non-zero and still names the moved row.

### Attribution rule

**Zero movement is the bar.** Where a cell does move, acceptance requires it to be *attributable*:
the moving cell must map to a **deferred-by-name surface** (the anticipated case is an Apache row
that exercises a JDBC or Excel read, most plausibly inside `test_readwriter`). Every such cell is
enumerated by name in the phase-close ledger with the surface it depends on and the
post-milestone-one row that will close it.

**Unattributed movement fails the phase.** The comparator exits non-zero on *any* movement; the
attribution table is the phase-close evidence that the movement was enumerated, not waved through.
Findings get resolved by naming, not by tolerance.

---

## 6. Golden-corpus basis designations

Two recorded parity bases coexist in the ported facade suite, and they disagree about ANSI mode.
Both port unchanged (design §4 Q9); the tension is recorded here so it stays legible, and so the
live oracle tier cannot quietly erase it.

| Corpus | `basis:` | What that means |
|---|---|---|
| The live scenario registry (`python/repark/tests/_live_parity.py` + `test_parity_live.py`) | **`live-recorded/ansi-on`** | Goldens derived from live PySpark 4.1.2 under Spark 4 defaults — **ANSI mode ON**. One disclosure depends literally on it. |
| The SQL passthrough corpus (`python/repark/tests/test_sql_passthrough_parity.py`) | **`hand-computed/non-ansi`** | Goldens hand-computed from Spark's documented **non-ANSI** semantics (divide-by-zero → NULL, invalid array index → NULL), authored when live recording was unavailable. |

**The rule (hard): the live oracle tier may only re-derive goldens whose basis is
`live-recorded`.** A live re-derivation of a `hand-computed/non-ansi` golden under an ANSI-on
session would silently change results — which is census movement dressed as a refresh.

Re-recording the passthrough corpus against live Spark 4.1.2 under a single basis is the right end
state and is **not** phase-3 work: it changes goldens, which changes results, which is census
movement. It is scheduled post-milestone-one, gated on the live oracle tier having run green at
least once on merged code, and it ships **alone** as a golden-changing (declared-rename-free) unit.

The two guard tests that hard-assert the live registry's scenario count (**27**) and its disclosure
name set port unchanged and must only ever move deliberately.

---

## 7. What a complete census submission contains

1. The four baseline reports (JSON + markdown) plus the freeze manifest, committed — including the
   stability-run self-diff and any quarantined rows, **named**.
2. The four v2 reports plus their manifest, committed.
3. Comparator output for all four cohorts: empty diff, matching denominators, exit 0 — or an
   attributed-movement table where every row names a deferred surface.
4. Empty sorted `--list` diffs for `repark-ml` and `repark-python`; an empty `--collect-only` diff
   for the facade suite after applying the deferral list.
5. The deferred manifest reconciled and appended to its reconciliation log.
6. `PLAN.md`'s baseline table replaced by the recorded-run pointer.
