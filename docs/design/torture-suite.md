# The v0.8 torture-test dataset suite — what the roadmap section actually costs

**Settled 2026-08-31 · base `main` at `749eff4` (post-#287) ·
roadmap [v0.8](../../task/roadmap/epic-term/release-roadmap-2026-08-29.md#v08--torture-test-dataset-suite) ·
intent [PROJECT.md](../../PROJECT.md) validation workstream 2**

The v0.8 section names five batteries and reads as a build from zero. It is not. Four of the five
already exist as checked-in generators with class manifests, determinism tests and facade pins —
delivered by DS-1…DS-4 on 2026-08-16
([ledger](../../task/ledgers/archive/2026-08/2026-08-16-c18-datasets-ledger.md)) and untouched
since. What does not exist is everything the *release* is actually for: the scale, a manifest a
perf harness can enumerate, a recorded baseline, and one product surface.

This document is what happened when the existing suite was run at the roadmap's own scale.

## 1. Ground truth (measured 2026-08-31)

`python/repark-parity/datasets/` holds all five bound family slugs. Every generator is pyarrow +
stdlib, seeded, and writes only to a cache root or an explicit `--out` outside the checkout
(`_cache.refuse_repository_output`). Every one already defaults its CLI to `--rows 1_000_000`.

| Family | Emits | Class manifest | Pinned scale to date |
|---|---|---|---|
| `nested` | `data.parquet`, `data.jsonl` | `CLASSES` labels in `datagen.py` | 64 rows, seed 42 |
| `schema_inference` | `data.csv`, `data.parquet` | `manifest.json` | 64 rows, `conflict_at = rows // 2` |
| `extreme_types` | `data.csv`, `data.parquet` | `manifest.json` | 64 rows |
| `secrets` | `data.csv`, `data.parquet` | `manifest.json` | 64 rows |
| `smartcsv` | four delimiter CSVs + `data.parquet` | `manifest.json` | 64 rows |

**Nothing has ever been generated at a million rows.** `ROWS = 64` / `SEED = 42` are the only
scales in `python/repark/tests/test_datasets_facade.py` (DS-4 clause C-031, deliberately — the 1M
default was kept out of CI), and `python/repark-parity/tests/` runs the same `small()` door. The
`--rows 1_000_000` default is a declared intent that no test, report or run has ever exercised.

### 1.1 What a million rows costs, measured

Single process, one core, local NVMe, this box; `write_files(rows=1_000_000, seed=42)` per family.

| Family | Wall | Bytes on disk | Note |
|---|---|---:|---|
| `nested` | 30.6 s | 279.0 MB | JSON-lines dominates; parquet is a fraction of it |
| `schema_inference` | 27.3 s | 130.4 MB | |
| `extreme_types` | 51.4 s | 1084.0 MB | paragraph strings and HTML fragments; four times the next family |
| `secrets` | 21.4 s | 515.5 MB | |
| `smartcsv` | 40.9 s | 516.4 MB | `render_csv` is per-row Python across four delimiter schemes |
| **suite** | **171.6 s** | **2 525 MB** | |

Two consequences, and they set §4's decisions. The suite is **minutes, not seconds** — so it
cannot join `make ci` or the `ci.yml` PR gate, both of which run the parity harness on every push.
And one full generation is **2.5 GB**, so a cache root that is never cleaned is a
disk-consumption surface the generators do not currently own.

### 1.2 The findings already standing against these corpora

DS-4 read all five families through the facade and reported six findings. One was fixed
(`count()` on the full-depth flatten plan — DEFECT-2, 2026-08-18). **Five are still open**, each
pinned so a later fix reds the pin:

| # | Finding | Class |
|---|---|---|
| 1 | A euro-comma decimal column resolves `decimal128(5,2)`, then the generated cast is handed the raw cell text and refuses | BUG-CANDIDATE |
| 2 | Delimiter auto-detect loses to an embedded rival delimiter and eats a data row as the header; `sep=` declared is the remedy | known-limit after B4 #175 round 4 |
| 3 | `explode_outer` refuses on `array<struct>` where plain `explode` succeeds | BUG-CANDIDATE |
| 5 | Zero-padded identifiers lose their padding to inference | POLICY |
| 6 | The under-sampled cast fails loud rather than corrupting | POLICY, working as documented |

Findings 1, 5 and 6 are all consequences of one property: **`samplingRows` defaults to 10 000
while the corpora are a million rows long.** At 64 rows the sampling cap is invisible. At 1M it is
the dominant behaviour of the whole CSV side of the suite, and the roadmap's own conflict class —
`int32 → int64` at row 500 000 — sits 490 000 rows past the cap. Generating the corpus at scale
does not test inference; it tests inference *under sampling*, which is a different claim and has
to be made deliberately.

### 1.3 What the measurement bed's consumers already have

`bench/windows/` (W-0, run 2026-08-31) is the shape v1.2 will reuse: a `roster.py` of named cells
with `quick` / `full` / `gate` scales, a seeded `datagen.py`, pydantic `RunResult` records, a
markdown report under `task/`, and a `gate`-scale smoke test in the facade suite. Its numbers live
planning-side as ratios, per the P-2 posture. `bench/tpch/` adds the other half: a committed
`baseline-ratios.json` and `check_baseline_ratios.py`, which is how a measured number becomes a
gate rather than a memory. v0.8 needs no new machinery for either — it needs the datasets to be
addressable by those harnesses, which today they are not.

## 2. The gap, stated as four things

1. **Scale has never been run.** §1.1 is the first time; nothing pins what a family emits at 1M,
   and `leading_zero_width` is the only class whose >1M boundary was ever reasoned about.
2. **There is no suite-level identity.** Each `manifest.json` is a *class* registry — column → type
   → torture class. Nothing enumerates *datasets*: no id a harness can name, no declared row count,
   no schema digest, no recorded size.
3. **No baseline exists.** Not one timing has been recorded against any of the five families.
4. **The secrets mechanism does not exist.** DS-3 shipped the fixture and DS-4 pinned that reads of
   it behave *normally* (C-040). That pin is the acceptance criterion of the fixture and the
   **inverse** of the feature; v0.8 must flip it deliberately, not quietly.

## 3. The units

The roadmap has five bullets and the natural reading is one unit per bullet. That cut is wrong
here, because four of the five bullets are already delivered as fixtures and the remaining work
does not partition by battery — it partitions by *what kind of claim is being made*. Three of the
bullets share one gap (scale), two share another (the CSV reader's behaviour past the sampling
cap), and exactly one is a product change. The units follow the claims.

| Unit | Scope | Depends on | Risk tier |
|---|---|---|---|
| **TT-1** | Scale. Every family generated at ≥ 1M for real, with a per-family row/byte/wall budget recorded and a cache-eviction door. | — | mechanical |
| **TT-2** | The suite manifest. One `suite.json` naming every dataset with a stable id, declared rows, formats and an Arrow-schema digest. | TT-1 | mechanical |
| **TT-3** | The nested measurement bed. The `nested` family grown into the shapes W-0…W-2 and the v0.9 spill matrix consume, with recorded baselines. | TT-1, TT-2 | standard |
| **TT-4** | The CSV side at scale. `schema_inference`, `extreme_types` and `smartcsv` read end to end at ≥ 1M; the five standing findings each get a disposition. | TT-1 | standard |
| **TT-5** | Opt-in secrets flagging — the mechanism. | TT-2 | **high** |

Order is TT-1 → TT-2 → (TT-3 ‖ TT-4) → TT-5. TT-5 is last because it is the only unit that changes
what a user's read does, and because its acceptance flips a pin TT-4 may have moved.

### TT-1 — scale

Generate all five families at 1M and at the `MAX_ROWS` ceiling's order of magnitude, fix what only
breaks past 64 rows, and make the cost a recorded fact rather than a surprise.

- C — every family completes `write_files(rows=1_000_000, seed=42)` and the re-read table is
  identical to a second run at the same seed (the A3 identity contract, at scale).
- C — a per-family budget (rows, bytes, wall) is committed and a regression past it reds.
- C — every labeled class in the family's manifest is still exhibited at 1M, not only at 64. Classes
  whose density is row-count dependent (`empty_list_row`, `null_list_row`, the ragged tail) are
  asserted present, and `leading_zero_id` keeps a leading zero at the largest emitted index.
- C — the cache root has a stated eviction door; §1.1's gigabytes are not left to accumulate
  silently. `check-disk-headroom`'s consumer list gains the row.
- C — `make ci`, `make py-test` and `ci.yml` still run at `small(64, 42)` only. The 1M tier never
  enters the PR gate.

### TT-2 — the suite manifest

- C — `python/repark-parity/datasets/suite.json` enumerates every dataset as
  `<family>/<variant>` with declared rows, seed, emitted files and formats, and a digest of the
  Arrow schema (not of the file bytes — see D-2).
- C — a test cross-checks every `suite.json` row against the generator it names, both directions,
  and reds when either side moves alone. This is the `test_datasets_manifest_types.py` contract
  raised one level.
- C — the per-family `manifest.json` files keep their present job (class → column → type) and gain
  nothing; `suite.json` does not restate them.
- C — a consumer can enumerate the suite without importing `repark`.

### TT-3 — the nested measurement bed

The bullet the roadmap explicitly calls "the measurement bed for v1.2". Its consumers are named
and its output is a baseline, not a pass.

- C — the family exposes the shape knobs W-0…W-2 need as declared variants: nesting depth, list
  cardinality, and `list<struct>` width, each a `suite.json` dataset with its own id.
- C — a driver on the `run_w0.py` shape times `dynamicFlatten` over each variant at `quick` and
  `full` scales, writes a pydantic record plus a markdown report under `task/`, and carries the
  hardware fields `bench/windows/hardware.py` already collects.
- C — baselines are committed as **ratios** against a stated reference (the `check_baseline_ratios.py`
  contract), never as absolute wall clock. The P-2 posture holds: this box is `schedutil` and noisy.
- C — a `gate`-scale smoke test proves the driver runs, in the facade suite, at a row count that
  costs seconds.
- C — the DFP-1 fast path
  ([ledger](../../task/ledgers/completed/dfp-1-preserve-null-unnest-ledger.md)) is measured against
  the pre-DFP-1 plan on this bed. TT-3 does not change `dynamic_flatten.rs`; a divergence it finds
  is filed, and the three candidates DFP-1 deferred under C-012 (optimizer-wrapper traversal,
  struct null-mask extraction, a Cartesian multi-list operator) get the evidence they were told to
  wait for.

### TT-4 — the CSV side at scale

- C — each of the three CSV families is read end to end through `smartCsv` at ≥ 1M, with `sep`
  declared (finding 2's documented remedy), and the result compared against the generator's parquet
  typed truth — the oracle is the generator, never a hand-typed expectation.
- C — the sampling-cap question is answered explicitly for each family: with the default
  `samplingRows = 10_000`, and with a cap above the conflict row. The roadmap's `int32 → int64` at
  row 500 000 is exercised on both sides of the cap.
- C — each of the five standing findings ends the unit as a fix, a divergence-registry row, or a
  dated deferral naming what it waits on. A finding that survives with none of the three is a unit
  failure.
- C — no finding is closed by narrowing the corpus.

### TT-5 — opt-in secrets flagging (the only product surface)

One bool conf, default off, that makes a read flag or refuse a **data column whose name is
credential-shaped**. Values are not inspected. Configuration secrets are v0.10 and are not touched.

Risk tier **high**, on three counts: it is public API; it adds a refusal path, and a refusal that
fires when it should not is a read that used to work and now does not; and it reuses a classifier
whose two implementations are hand-synced (§7).

- C — the conf key follows the `spark.sql.ansi.enabled` pattern in `crates/repark-functions/src/ansi.rs`:
  a named constant, a declared default, a parser that refuses a value it cannot parse while naming the
  key, and a `ConfigExtension` fixed at session build. It is not a runtime-settable option.
- C — **default off is pinned**: with the conf absent, every DS-4 secrets pin still passes
  byte-for-byte. C-040 ("reads behave NORMALLY") stays green unmodified; the feature adds a second
  arm, it does not replace the first.
- C — the classifier is the existing `prop_key_is_secret` fold and no second inventory is written.
  The Rust source and its Python mirror agree, proven by a test, not by a comment (§7 names the
  present state).
- C — flagging and refusal are distinct and separately pinned: flag surfaces the column names,
  refusal names them in the error and reads no rows.
- C — the `secrets` family's two negative controls (`id`, `bucket_key`) are pinned as not flagged.
  `bucket_key` is the documented `_key` carve-out and is the whole point of having a control.
- C — a divergence-registry row exists, because this is a read that Spark performs and RePark may
  refuse.

## 4. Decisions (dated)

**D-1 (2026-08-31) — Generators stay Python, and the home does not move.**
`python/repark-parity/datasets/`, pyarrow + stdlib, zero new dependencies, loaded as
`repark_datasets` through the bench `sys.modules` loader. Rust was weighed and declined for one
reason: a torture corpus is an *input* to the engine, and a generator built on the engine's own
stack can encode the engine's bug into the fixture that is supposed to catch it. The present tree
cannot import `repark` at all — `make py-test` runs it with no native build — and that
independence is exactly what let DS-4 report six findings against product code. The price is
§1.1's wall clock, single-threaded and in Python. That price is paid out of CI, not out of the PR
gate, so it buys nothing to remove it.

**D-2 (2026-08-31) — Determinism is table identity, not file bytes.** Inherited from DS-1's A3
contract and restated because `suite.json` could be read as promising the stronger thing: parquet
writers embed a writer version and JSON-lines float rendering is not a stable target, so
byte-reproducibility is neither achievable nor what a consumer needs. Same seed and row count ⇒
identical pyarrow table (schema and values), and the digest `suite.json` carries is of the Arrow
schema, not of the file.

**D-3 (2026-08-31) — ≥ 1M is a floor, `MAX_ROWS = 10_000_000` stays the ceiling.** The floor is the
roadmap's. The ceiling is already in every `datagen.py` and does not move in v0.8; §1.1 says why —
one full generation is 2.5 GB, and ten times it is a disk decision, not a fixture decision.

**D-4 (2026-08-31) — Output format follows the battery, and none changes.** Each family already
emits what its claim needs — parquet as typed truth everywhere; JSON-lines where nested inference
is the subject; CSV where the text *is* the battleground; four delimiter schemes where delimiter
detection is. Adding a format to a family that does not need it adds generation cost and pins
nothing.

**D-5 (2026-08-31) — Generated data never enters git, and three existing fences say so.**
`refuse_repository_output` raises when `--out` resolves inside a checkout; the default root is
`$XDG_CACHE_HOME/repark-datasets/<family>`; symlinked roots and dangling symlinks are refused. No
`.gitignore` entry is needed or wanted — an ignore rule would make an in-tree write silent, and the
refusal makes it loud. v0.8 adds no fourth fence and weakens none.

**D-6 (2026-08-31) — CI generates on the fly, at two scales, in two places.** The PR gate keeps
`small(64, 42)` and nothing larger; this is DS-4 clause C-031 and it does not move. The ≥ 1M tier
is a scheduled `workflow_dispatch` + weekly-cron job on the `parity-live.yml` shape: generate into
runner scratch, run the battery, upload the report, delete. It carries an explicit
`timeout-minutes` — `parity-live.yml` and `ci.yml` currently set none, and a job that generates
gigabytes is the wrong place to inherit a 360-minute default. Budget: the batteries plus generation
fit one hour, or the tier is cut down until they do. Nothing this job produces is committed by the
job; a report is filed by a human in a PR, as W-0's was.

## 5. The measurement-bed contract

What v1.2 (W-0…W-2) and the v0.9 spill matrix consume, and where each piece lives:

| Consumer needs | v0.8 delivers | Lives at |
|---|---|---|
| Name a dataset without knowing how it is built | a stable `<family>/<variant>` id | `datasets/suite.json` (TT-2) |
| Enumerate the suite programmatically | one JSON file, no `repark` import | same |
| Know a dataset's shape before generating it | declared rows, seed, formats, schema digest | same |
| Regenerate it byte-for-byte-equivalently | seed + rows on the family CLI | unchanged generators |
| Compare against a previous run | committed ratio ceilings + a checker | `datasets/baseline-ratios.json` (TT-3), on the `bench/tpch/check_baseline_ratios.py` contract |
| Read the numbers | a dated markdown report | `task/torture-suite-report-<date>.md`, on the W-0 report precedent |

Two boundaries. `suite.json` names datasets; it does not name *queries* — the window cells stay in
`bench/windows/roster.py` where W-0 already put them, and a v1.2 cell references a dataset id
rather than restating its shape. And the v0.9 spill matrix consumes the same ids: "which operators
spill" is answered against named inputs, so the W-3 window row and a v1.2 timing can cite the same
dataset and mean the same bytes.

## 6. Risks

**R-1 — Scale changes what the corpora test, silently.** §1.2 is the evidence: three of five
standing findings are sampling-cap consequences that 64 rows cannot see. TT-4 must state, per
family, whether a result is a property of the data or of the cap. A number reported without that
distinction is worse than no number.

**R-2 — TT-3's baselines can be recorded against a defect.** The bed exists to measure
`dynamicFlatten`, and `dynamicFlatten` has an open `explode_outer` asymmetry (§1.2 finding 3) on
exactly the `array<struct>` shape the nested family is built around. A ratio recorded over a path
that later gets fixed is not a regression when it moves. Baselines carry the base SHA, as
`baseline-ratios.json` already does.

**R-3 — TT-5 inverts a pin that is currently an acceptance criterion.** DS-4's C-040 asserts
secrets reads are unredacted. That assertion is correct today and must stay correct with the conf
off. The failure mode is a unit that "fixes" the pin instead of adding an arm beside it.

**R-4 — Disk.** §1.1 measures 2.5 GB per full generation and the cache root has no eviction.
A weekly CI job and a developer running the suite twice are different problems; TT-1 owns the
door, and the repo's disk-consumer inventory gains the row rather than discovering it during a
release build.

## 7. What v0.8 does not claim

- **No PySpark comparison.** Nothing here is validated against the live oracle. Cross-engine
  validation of these corpora is PROJECT.md workstream 4, not this release; the generator's own
  parquet is the only oracle v0.8 uses.
- **No AWS.** Local filesystem and the cache root only, like every other bench in the tree.
- **Generating a corpus closes no finding.** The five standing DS-4 findings are open when v0.8
  opens. TT-4 dispositions them; a deferral is dated and names what it waits on.
- **1M is a floor, not a scale study.** `MAX_ROWS` is 10M and no family has been run there. Nothing
  in v0.8 claims linear cost above 1M.
- **The 1M tier is not CI-affordable and v0.8 does not pretend otherwise.** §1.1's numbers are why
  D-6 puts it on a schedule instead of the PR gate. A green PR gate says nothing about the suite at
  scale.
- **The two `prop_key_is_secret` implementations are not proven equal today.** The Rust `fn` in
  `crates/repark-core/src/catalog_config.rs` is private with no PyO3 binding; the Python mirror in
  `python/repark/src/repark/spark/_secrets.py` is a documented **superset** (it adds `userinfo`,
  exact `key`, and a `_key` suffix arm carving out `bucket` / `arn`). Sync is a comment and two
  hand-maintained fixture lists, with no test comparing the two. TT-5 either closes that or states
  which side the conf reads and why the other is allowed to differ. v0.8 claims neither until it
  does.
- **Secrets flagging is about column names, never values.** No value is inspected, no entropy is
  scored, and no configuration key is touched — configuration secrets are v0.10.
- **The nested family is not yet the v1.2 bed.** TT-3 makes it one. Until TT-3 lands, W-0's own
  `bench/windows/datagen.py` frames remain the only shapes any window measurement has used.
