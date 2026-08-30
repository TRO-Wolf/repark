# map — scripts/

CC-3 (2026-08-30): comments condensed to one line; banners removed. Size-gate rows ratcheted with each slice.

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Repository helper scripts wired into the dev workflow. Q1 re-home (2026-08-14):
`check_lib_py.py` EXCEPTIONS paths moved under `python/repark/src/repark/spark/`.

CC-2 audits the tracked Python helper scripts after all crate and package slices finish.
pins: comment-condensation-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011.

CC-2 scripts slice: every tracked `scripts/*.py` audited; gate-rule statements and constant
tables kept, narration and dated history removed. The same unit ratcheted 17
`check_lib_py.py` test-module baselines down to their condensed lengths and retired 2 rows
that fell below the default ceiling; 4 more parity-harness baselines ratcheted in the
repark-parity slice.

## Contents

- `bump_fork_pin.sh` — bumps the iceberg-rust fork `[patch.crates-io]` pin: rewrites all five
  `rev` lines + `Cargo.lock` together (single-writer-per-pin invariant-checked), prints the
  fork changelog URL for the PR body. Wrapped by `make bump-fork-pin REV=<sha|branch>`;
  contract in [../docs/fork-sync.md](../docs/fork-sync.md).
- `check_map_md.sh` — the map.md lockstep guard: fails a commit if a staged `.rs`/`.py`/
  `Cargo.toml`/`pyproject.toml` file's directory has no same-change `map.md` update (lockfiles
  excluded; root-level manifests map to `map.md`, not `./map.md`). Invoked by
  `.pre-commit-config.yaml`, `make check-map-md`, and the hook installed by `make install-hooks`.
- `sync_map_md.py` — the map.md **content** guard, companion to `check_map_md.sh` (that one forces
  a map to be TOUCHED; this one checks what the map actually says) and the SSOT for both rules.
  Over every tracked `map.md` (`git ls-files`, so untracked build trees are never walked):
  (1) **link validity** — every relative markdown link resolves to an existing file or directory
  (`http(s)`/`mailto` links and bare `#anchors` are out of scope, nothing local can check them;
  an ABSOLUTE target like `/docs/foo.md` is a finding of its own, since resolving it would mean
  asking the machine's filesystem root; only inline `[text](target)` links are parsed, so
  reference-style links get no checking);
  (2) **coverage**, behind `--strict` — every mappable tracked file in the map's own directory
  (`.rs` `.py` `.sh` `.md` `.toml`, minus `map.md` itself, lockfiles and dotfiles) is mentioned in
  the map by name. Measured before arming (2026-08-22): link validity **2** findings, both
  path-depth typos fixed in the arming commit; coverage **24** pre-existing unmentioned files — a
  FLOOR, not an exact debt, because a name counts as mentioned wherever it appears as a whole
  token — so the coverage rule is deliberately NOT armed: it lives behind `--strict` and is run by
  hand (`python3 scripts/sync_map_md.py --check --strict`). `--fix` is mechanical only: it deletes
  a missing-target row when that row is a list item whose ONLY link is the dead one, taking the
  item's wrapped continuation lines with it — the deleted span is the bullet line plus every
  following indented line, ending at the first blank line, the first unindented line, or the first
  nested list item (which refuses the deletion outright) — and appends a
  `- [name](name) — TODO(describe)` stub
  for an unmentioned file. It refuses to delete a row carrying a nested sub-list (the children
  would be orphaned) and never deletes an absolute-target row (repointing it is the repair),
  reporting both for a hand edit. It never writes a description —
  the description is the whole value of a map, and a generated one would be a lie with a link on
  it. Exit 0 clean / 1 findings / 2 usage or environment error; fail-closed when the tree has no
  tracked `map.md` at all. Wired into `make check-map-sync`, `.pre-commit-config.yaml`
  (`map-sync-guard`) and the hook `make install-hooks` writes — measured n=5 **median 0.08 s**
  over 143 maps, well inside the sub-second hook budget. Wired at every path
  `check_map_md.sh` uses, including ci.yml's guards job (owner-granted wiring, 2026-08-23). The
  document-lifecycle rules it serves are
  [../AGENTS.md](../AGENTS.md) "Markdown document lifecycle".
- `check_ledger_grammar.py` — the ledger **grammar** guard (DL-2, 2026-08-23), over the live
  bins `task/ledgers/staging/` and `completed/` (a ledger retires into `completed/` in its own
  departure commit, so CI meets it there; the archive is immutable and read for citations only). Three
  rules, shape only — the meanings stay in `.agents/skills/sepmo/`: **(A)** every clause row (`| C-NNN |`)
  has a unique id, exactly one verdict cell (`PROVEN` / `OPEN` / `REJECTED`, bold or a
  parenthetical allowed — measured: 12 of 32 live verdict cells are annotated) and an evidence
  cell, and a governed ledger carries a clause table at all; **(B)** a test cites a clause with
  `pins: <unit>/C-NNN[, C-MMM]` (`<unit>` = the ledger filename without `-ledger.md` and, in the
  archive, without its date prefix), read from every tracked file under `crates/`, `python/`,
  `scripts/` — every `PROVEN` clause in staging must be cited, every citation must resolve to a
  clause in any bin (staging, completed, the archive); **(C)** the `COVERAGE_ATTESTATION:` block (ref 05's shape) is
  checked — `AT-1`..`AT-10` once each, `ATTACKED` with artifacts or `N/A` with a justification,
  `complete:` consistent — and required once a governed ledger has no `OPEN` clause (it is the
  Critic's artifact); `FINDING:` records carry the ref 05 fields. `EXCEPTIONS` seeds the measured
  pre-rule floor per ledger, ratchets down only, and a row naming a ledger in no
  live bin is a finding. Two sub-rules were measured and **declined** (an `OPEN` row carries a `?`;
  a quantified clause names its enumeration): they fake a meaning a regex cannot read. Exit
  0 / 1 / 2. Wired as `make check-ledger-grammar` in the `make ci` chain and as ci.yml's `ledger
  grammar guard` step (dual-wired, 2026-08-23). Proofs:
  `python/repark-parity/tests/test_dl_2_ledger_grammar.py`.
- `doc_blocks.py` — the **block grammar** of the two live documents (DL-4, 2026-08-25;
  `history=` must name one bin under `docs/history/`):
  HTML-comment `ws` blocks around every `STATUS.md` workstream bullet and `unit` markers on the
  slate's rows and reasoning; the parser (every violation a finding with file and line), the
  coverage check, and the two transforms — a merged unit's rows and blocks leave whole, a
  `state=closed` campaign is cut for `docs/history/` (the closed-campaigns list treats a wrapped
  row as one row, in the writer and in the coverage check alike; a marker inside a code span or
  a fence is prose). Pure text; consumed by
  `ledger_lifecycle.py compact` and `check_docs_compaction.py`.
- `check_owner_ruling.py` — the **PR #247 owner-ruling preservation gate**. It requires the
  complete ruling block in a regular file at the start of both `AGENTS.md` and `CLAUDE.md`,
  byte-for-byte, and the adjacent enforcement boundary in `AGENTS.md`. Each protected block must
  appear exactly once. It rejects symlink redirection and makes no model-attribution claim.
  Dual-wired through `make check-owner-ruling` in `make ci` and a raw guard step in ci.yml.
  Provocations:
  `python/repark-parity/tests/test_pr_247_owner_ruling.py`.
- `check_docs_compaction.py` (AGENTS.md ceiling 32,000 B since the 2026-08-26 owner ruling) — the **live-document gate** (DL-4, `make check-docs-compaction`, in
  `make ci`, `make install-hooks`, `.pre-commit-config.yaml` and `ci.yml`'s guards job (wired under a
  one-time owner grant, 2026-08-25) at n=5 median 0.05 s: no closed campaign still in STATUS, no
  merged unit still on the slate, every workstream bullet inside a `ws` block, and the byte
  ceilings (`CEILINGS`: STATUS.md, the slate, AGENTS.md, engineering-method, the SEPMO
  unit-runbook; (a)–(c) still only the two live documents; (d) every key). Seeded DL-4
  2026-08-25 at 31,000 / 6,000 B; ratcheted DL-5 2026-08-25 to 25,000 / 6,000 /
  31,000 / 35,000 B from the unit's final measurement; PROC-1 2026-08-25 added
  `.agents/skills/sepmo/unit-runbook.md` at 5,000 B (pointer-only, cannot become a second
  spine). Raised only in the PR that needs it. Tests:
  `python/repark-parity/tests/test_dl_4_live_doc_compaction.py`,
  `python/repark-parity/tests/test_dl_5_contract_compaction.py`,
  `python/repark-parity/tests/test_proc_1_tiered_review.py`.
- `ledger_lifecycle.py` — the ledger **lifecycle** script (DL-1, 2026-08-23): a ledger's state is
  its directory (`task/ledgers/staging/` → `completed/` → `archive/yyyy-mm/yyyy-mm-dd-<name>.md`),
  and moving one is a repository-wide link rewrite, so the two are one operation. `archive` files
  `completed/` (or the paths given) under a date read from `main`'s first-parent history — never
  the clock, so any machine produces the same name; a ledger not yet on `main` (the current unit's
  own, retired in its departure commit) is left for the next pickup when unnamed and refused when
  named (found by the first real pickup, DL-2); since DL-4, `archive` and a `move` to
  `completed/` end by running `compact` — merged units leave the slate and closed campaigns
  leave STATUS for `docs/history/<campaign>/status-record.md` (bin and map created, map rows
  appended in place — `append_row`, never the archive maps' sort — links rewritten, refused on
  a dangling one);
  `move PATH BIN` is the agent's `staging` → `completed` step and the roadmap promotions
  (`mid-term` / `epic-term`; `archive` is not a `move` target); `check` is the gate. The rewrite
  is resolution-based — a link changes only if it *resolved* to the moved file — and covers the
  moved file's own outgoing links and its `map.md` row — the bullet plus every indented line
  under it, wrapped text and sub-lists alike — which travels to the destination map with its
  description. Rows travel **whole into the live bins**; into an **archive month map** they are
  condensed to one line, link plus first sentence (DL-3, owner ruling 2026-08-23 — the record is
  the ledger, git history keeps the long row, and the month maps say so in their Purpose).
  Nothing is written unless every rewritten link resolves, and the result is staged
  as one change. Prose mentions in code spans are not links and are left alone (the basename
  survives the move, so they still find the file). `check` fails on a `*-ledger.md` under `task/` outside the bins, an
  archive name whose date prefix disagrees with its month directory, a dead `-ledger.md` link in
  **any** tracked markdown (`sync_map_md.py` covers maps only), and a `completed/` or `archive/`
  file changed since the base commit beyond a link repair or a prepended errata note (the frozen
  and immutable rules; no rename heuristics — a vanished `completed/` ledger must have its dated
  twin by name; a target carrying whitespace is prose, not a path, and stays in the comparison).
  Refuses to pass closed when no base commit resolves (`--base`, else the merge-base with
  `origin/main` / `main`). Reuses `sync_map_md.py`'s link parser. Exit 0 / 1 findings or
  refused / 2 usage. Reviewed adversarially before its first real run (2026-08-23): the
  blocker it caught — a map row wrapped onto a line starting with `+ ` read as a nested bullet
  and split — is pinned in the tests. Wired as `make check-ledgers` (in the `make ci` chain since
  the DL-1 backfill, the commit that made the tree pass it; and as ci.yml's `ledger lifecycle guard`
  step with `fetch-depth: 0`, dual-wired 2026-08-23) and `make ledger-archive`. Proofs:
  `python/repark-parity/tests/test_dl_1_ledger_lifecycle.py`.
- `check_workflows_parse.py` — every GitHub Actions workflow must be parseable YAML. zizmor
  SKIPS files it cannot parse (exits 0 with "no auditable inputs"), so a broken workflow would
  pass the blocking lint gate while GitHub silently never runs it. Wired as a prerequisite of
  `make workflows-lint`.
- `check_crate_dag.sh` + `check_crate_dag.py` — the crate **dependency-policy** guard. The `.sh`
  runs `cargo metadata --format-version 1 --no-deps --locked` and pipes it to the `.py`, which
  holds three tables and is the **SSOT** for all three: the **tier map** (`TIERS`), the crate
  **roles** (`ROLES` — foundation / table service / engine / capability / door / bindings), and
  the explicit **allowed-edge table** (`ALLOWED_EDGES`: every internal edge, the dependency
  KINDS it may take, and why it exists). Prose points here and never restates them. Four rules,
  in order: (1) the declared policy must itself obey the structural rules — a forbidden edge
  cannot be legalized by writing it down; (2) every observed `repark-*` edge must be DECLARED,
  with its kind (`normal` / `optional` / `dev` / `build`) permitted for that pair — a new
  same-tier edge reds until it is declared with a reason, and a stale row whose edge is gone
  reds too; (3) the structural rules over roles — no door → door edge outside `dev`, nothing may
  depend on the bindings adapter, the foundation crate depends on nothing internal, a capability
  crate never depends on a door; (4) layering — no PRODUCT edge (`normal`/`optional`) may point
  at a strictly higher tier, same-tier edges ALLOWED. Kinds are what let the cross-door
  `repark-sql → repark-spark` test edge be permitted as `dev` while the same edge as `normal`
  is the forbidden door→door product edge. Third-party crates are out of scope (internal = any
  Cargo workspace member — membership, not the `repark-` name, is the test); a new workspace
  member missing from `TIERS`/`ROLES` fails the guard; mapped crates that have not landed yet are
  simply not inspected. NOTE the binding's deliberate **non-edges** (no `repark-sql`, no
  `repark-iceberg`) are still enforced by review, not here — this guard bans edges, it never
  requires one. Wired into `make check-crate-dag` (in the `make ci` chain),
  `.pre-commit-config.yaml`, and the hook installed by `make install-hooks`.
  **Dual-wired:** the `crate-DAG layering guard` step in the ci.yml `guards` job mirrors the
  Makefile target — change one, change the other.

- `check_manifest.sh` + `check_manifest.py` — the **structural-manifest** guard (FD-3), which
  makes [`../repo-manifest.toml`](../repo-manifest.toml) true instead of decorative. Pure text
  (no cargo, no network): it reads `Cargo.toml`, the `Makefile`, `STATUS.md`, the declared
  documents and the crate-root `map.md` files. Nine rules — inventory both ways (every Cargo
  member is declared; every `delivered` component is a member at that exact path), delivered
  components exist (`<path>/Cargo.toml`), `planned` paths must NOT exist (planned ≠ delivered),
  layers are recognized **and equal the tier name `check_crate_dag.py` assigns** (imported, not
  copied — the manifest mirrors the dependency-policy SSOT and can never override it), every
  delivered crate is covered by `TIERS` + `ROLES`, the `[project.gates]` commands name live
  `make` targets, every `[documentation]` path exists, `STATUS.md` states the manifest's
  phase/release words (`## Current milestone` / `## Release state`), and each delivered
  component's crate-root `map.md` exists at the declared path, names the component in its
  heading and names its layer as `tier N`. That last rule is the **only** `map.md` automation in
  the repository and it **checks** a hand-written file — it never generates, scaffolds or
  rewrites one. Wired into `make check-manifest` (in the `make ci` chain),
  `.pre-commit-config.yaml`, and the hook installed by `make install-hooks`. **Dual-wired:** the
  `repo-manifest guard (check_manifest)` step in the ci.yml `guards` job mirrors the Makefile
  target.
- `check_lib_rs.sh` + `check_lib_rs.py` — the lib.rs thinness guard. No inline
  `#[cfg(test)] mod {…}` (file-backed only; same-line `#[cfg(test)] mod … {` also fails);
  non-test line ceilings with an EXCEPTIONS-with-reason table in the `.py` (SSOT; ratchet down
  only; empty at phase-1 PR-A; rows so far: `repark-functions` — registration glue
  (ceiling 175 after U5 `pub mod ansi;` — Q10 kept 175 by net-zero crate-doc, and FN-GT2 X8
  kept 175 again by sanctioned out (1): `pub mod url;` + its `register_all` loop went in while
  the `shim_udf_boilerplate!` body went out to `src/shim_macros.rs`, measured 168, no raise),
  `repark-python` — the 180-line PyO3 crate root, a MANIFEST (module decls incl. the
  file-backed `exceptions` taxonomy module, the two error folds, the `#[pymodule]`
  registration) that already uses the sanctioned file-backed test module (phase-3 PR-3, EC-10;
  ceiling ratcheted 230 → 190 when the taxonomy moved to src/exceptions.rs — without the row
  every slate reds on the crate's arrival), and `repark-ta` — the verbatim-ported kernel root's `TaError`
  contract + flat re-export surface).
  **Stale EXCEPTIONS keys fail closed** (WC 2026-08-11): a crate-name key whose
  `crates/<key>/src/lib.rs` is missing is an ERROR (G-8 mold; keys are crate names, not
  paths). Dual-wired: `make check-lib-rs` (in `make ci`) AND a ci.yml
  `guards`-job step; pre-commit via `install-hooks` and `.pre-commit-config.yaml`
  (`lib-rs-guard`). Pure text — sub-second. EXCEPTIONS reason
  strings stay ≤100 cols (ruff E501; keep ruff-format clean).

- `run_census.sh` — one-command census gate (classic + expand + expand2): provisions a scratch
  venv, builds the native module, then runs the three cohorts and writes JSON + markdown reports.
  Not CI-wired (~20 min wall per module). Ported with **three** declared changes, each stated in
  the script header:
  1. (phase-3 EC-8) the classic cohort runs `--classic`, never `--stretch` — `--stretch` appends
     the C3 modules and blends them into the classic /345 denominator. Report output paths are
     unchanged in shape.
  2. the run's **environment is recorded, not assumed** — a verbatim `pip freeze` (empty = fatal)
     plus `census-manifest.json` carrying the versions the comparator gates (`python_version`,
     `pyspark_version`, `pandas_version`, `pyarrow_version`), and the run aborts outright under
     pandas ≥ 3.
  3. the markdown reports default to the **gitignored** `target/census-reports/` (not `task/`).
     A run is a run OUTPUT until it is curated, so it must not land look-alike markdown beside
     the committed evidence in `task/census/<run>/`, nor dirty `git status`. Override with
     `CENSUS_REPORT_DIR=…`; promotion to evidence stays a deliberate copy into
     `task/census/<run>/` in the commit that records it.

  Artifacts are then redacted via `python -m compat.redact` (through each format's parser), never
  `sed`. The script needs the facade package at `python/repark`, which arrives with the facade
  PR; the recorded procedure it implements is
  [../docs/port/census.md](../docs/port/census.md).

- `check_lib_py.sh` + `check_lib_py.py` — the **Python source-size and facade thinness guard**.
  The line scan covers every `*.py` under `python/` and `scripts/`; the default and every exact
  exception baseline live only in the script. An exception records its debt reason and cohesive
  split seam. Growth fails, and shrinkage fails until the baseline ratchets down or the row retires.
  Only generated-test sources under `tests/goldens/` or `tests/fixtures/` are excluded. The
  facade-only no-stub rule retains its narrower scope: a re-export-only module under
  `python/repark/src/repark/` must open its docstring with `re-export binding`; package
  `__init__.py` files remain exempt from that syntax rule, not the size rule. Fail-closed on a
  missing scan root, unreadable source, empty scan, or exception outside the scan. Dual-wired by
  `make check-lib-py` and the ci.yml `python` job.

- `check_python_conventions.sh` + `check_python_conventions.py` — the **Python conventions**
  guard: the three rules Ruff cannot express, and the SSOT for them (the prose homes that point at
  it: [AGENTS.md](../AGENTS.md) "Python", the code-quality and engineering-method skills under
  [.agents/skills/](../.agents/skills/map.md)). Over every `*.py` under
  `python/repark/src`, `python/repark-parity` and `scripts/`: (1) **no function defined inside
  another function**, with an inline `# nested-def: <reason>` pragma for the three sanctioned
  cases (a decorator closing over its own arguments, a callback whose closure over local state is
  the point, a `functools.wraps` wrapper — an empty reason does NOT pass) and a
  `NESTED_DEF_EXCEPTIONS` per-file ceiling table that ratchets DOWN only; (2) **no `dataclasses`
  or `attrs`** — Pydantic v2 `BaseModel` is the single structured-data container — with a
  `DATACLASS_EXCEPTIONS` table and deliberately no inline pragma; (3) **no direct constant
  quote-doubling `replace` call for SQL (SQP-1)** — a receiver-blind AST rule evaluates strings,
  bounded integer `+`/`-`, `chr`, concatenation, and repetition, then forbids the one-quote to
  two-quote call outside the product `_idents.py` and standalone `repark_parity/sql.py` homes. Its
  iterative text walk limits depth, nodes, and output before allocation. A PR-245 pin inventories
  shipped helper calls; the exact whitelist does not claim semantic completeness. File parsing
  catches syntax and parser-resource failures as one
  controlled diagnostic, including valid expressions that exhaust AST construction. The other Python
  conventions are enforced elsewhere and are not duplicated here: type coverage is Ruff's `ANN`
  rule set, public-docstring presence is `check_docstring_presence.py`, and naming is a review
  duty. Seeded from the measured tree (2026-08-21): 66 nested
  defs in 21 files, 23 files importing `dataclasses`. **PYC-1** deleted the `core.py` (23) and
  `plan_collapse.py` (12) nested-def rows. **PYC-2** deleted the remaining ten
  shipped-package nested-def rows (12 lifts + 2 pragmas). **PYC-3** deleted the two
  shipped-package dataclass rows (`merge.py`, `_csv_smart.py`). **PYC-4** emptied
  `NESTED_DEF_EXCEPTIONS` (lifts + pragmas in the harness; dual-wire `field` is a
  pragma) and converted the 20 parity dataclass files; remaining dataclass row is
  `scripts/check_parity_live_dual_wire.py` (runs as bare `python3`, no venv pydantic).
  Fail-closed on an unreadable file, a parse
  failure, an empty scan set, or a stale `EXCEPTIONS` key. **PYC-5:** re-measured n=5
  median **0.996 s** (max 1.011 s) over 164 files — at the sub-second budget line, with
  the max already over it, so not on pre-commit. Dual-wired
  `make check-python-conventions` (in the `make ci` chain) + ci.yml's `python` job.
  Rationale and the arming method:
  [../.agents/skills/code-quality/SKILL.md](../.agents/skills/code-quality/SKILL.md).

- `check_docstring_presence.sh` + `check_docstring_presence.py` — the **public-docstring
  presence** guard (PYC-6, 2026-08-22): Ruff `D101`/`D102`/`D103`/`D105`/`D107` with an
  `EXCEPTIONS` per-file ceiling table that ratchets DOWN only. SSOT for the five presence
  rules the owner ruled; style `D` is declined permanently (facade docstrings mirror
  PySpark) and is not selected. Over every `*.py` under `python/repark/src`,
  `python/repark-parity` and `scripts/` except `**/tests/**`. Seeded from the measured
  tree at arming: **136** findings across **39** files (the slate's ~266 included tests).
  Ruff is the parser (`uvx ruff@0.15.22`, pin locked to the Makefile); this wrapper is
  the ratchet. Fail-closed on a missing ruff, a JSON parse miss, an empty scan, a stale
  key, or a row whose file dropped to zero (delete it). Dual-wired `make
  check-docstring-presence` (in the `make ci` chain) + ci.yml's `python` job, and on
  pre-commit (n=5 median **0.13 s**, inside the sub-second hook budget).

- `check_rust_file_size.sh` + `check_rust_file_size.py` — the **general Rust file-size** guard
  (G-8 companion to `check_lib_rs`). RP-2 (2026-08-27): `call.rs` ratcheted 1404 → 1111 — the
  argument bag moved to `crates/repark-spark/src/call_args.rs` along the row's stated seam.
  RP-3 (2026-08-30): `crates/repark-spark/src/tests/call.rs` ratcheted 1407 → 1361 after the
  R114 public-API replacement dropped the private DV-walker tests. The scan covers every
  `*.rs` under `crates/**`; the default
  and every exact exception baseline live only in the script. Each exception carries its debt
  reason and cohesive split seam. Growth fails, and shrinkage fails until the row ratchets down
  or retires; comment-only shrink or restoration has the same exact-baseline duty. Only
  generated-test sources under
  `tests/goldens/` or `tests/fixtures/` are excluded.
  The catalog-registration test split ratchets `crates/repark-core/src/session/tests/session.rs` from
  1,485 to 1,461 lines; the new focused module stays under the default.
  Fail-closed on an unreadable file, empty scan, or exception outside the scan. Dual-wired through
  `make check-rust-file-size`, the ci.yml guards job, and both pre-commit surfaces.

- `check_parity_live_dual_wire.sh` + `check_parity_live_dual_wire.py` — the **parity-live dual-wire**
  guard (G-6). Compares `make parity-live` and `.github/workflows/parity-live.yml` to **each
  other** on load-bearing tokens (`uv sync` flag/extra set, `--no-install-package repark`,
  maturin pin + `develop`, `uv run --locked --no-sync` + pytest path, `REPARK_PARITY_LIVE` /
  `SPARK_LOCAL_IP`). No third hand-maintained expected-flags list. Fail-closed on a parse miss.
  Scope is this one pair only (a one-line extensibility comment lives in the `.py`; there is no
  multi-pair framework). Dual-wired: `make check-parity-live-dual-wire` (in `make ci`) AND the
  ci.yml `guards`-job step. **PYC-4:** this file stays a `dataclass` (the script is invoked as
  bare `python3` from make, no wheel venv); `field` is a nested-def pragma.

- `check_matrix_test_liveness.sh` + `check_matrix_test_liveness.py` — the **surface-matrix
  test-name liveness** guard (H-2 G8). Diffs every `Row::Tested { test }` in
  `crates/repark-spark/src/matrix.rs` and `crates/repark-sql/src/matrix.rs` against
  `cargo test --locked --workspace --lib --tests --bins -- --list`. A dead cite reds the
  gate. Fail-closed on a parse miss, a missing matrix file, zero extracted cites, zero
  listed names, or a non-zero cargo exit. Dual-wired: `make check-matrix-test-liveness`
  (in `make ci` / `make preflight`) AND the ci.yml `rust-test` job step (not the guards
  job — the check needs compiled test binaries).

Not re-homed (the port is complete — each returns only with a concrete driver):
`test_lock_gate.sh` (uv lock-gate detector self-test — a lock-gate change that needs it),
`generate_excel_fixtures.py` (synthetic .xlsx fixtures — the deferred `repark-excel` reader; see
[../STATUS.md](../STATUS.md) "Deferred capabilities").

## I want to...

| I want to... | go to |
|---|---|
| Understand why a commit was blocked on map.md | `check_map_md.sh` |
| Find map.md links that no longer resolve | `make check-map-sync` (`sync_map_md.py`) |
| See which files their directory's map never mentions | `python3 scripts/sync_map_md.py --check --strict` (not armed — measured 24 at 2026-08-22) |
| Change or inspect the crate tier map | `check_crate_dag.py` (`TIERS` — the SSOT) |
| Add / remove an internal crate dependency | `check_crate_dag.py` (`ALLOWED_EDGES` — declare the edge, its kind and a reason) |
| Declare a new crate, doc, or gate command | [`../repo-manifest.toml`](../repo-manifest.toml), then `bash scripts/check_manifest.sh` |
| Raise/lower a lib.rs line ceiling | `check_lib_rs.py` (`EXCEPTIONS` — reason required) |
| Ratchet a Python source baseline | `check_lib_py.py` (`EXCEPTIONS` — exact count, debt reason, split seam; owner approval for growth) |
| Ratchet a general Rust source baseline | `check_rust_file_size.py` (`EXCEPTIONS` — exact count, debt reason, split seam; owner approval for growth) |
| Sanction a nested `def`, or lower a nested-def ceiling | `check_python_conventions.py` (`# nested-def: <reason>` pragma for the three allowed cases; `NESTED_DEF_EXCEPTIONS` for debt — ratchet down only) |
| Keep a `dataclass` that cannot become a `BaseModel` | `check_python_conventions.py` (`DATACLASS_EXCEPTIONS` — reason required; no inline pragma exists on purpose) |
| Lower a docstring-presence ceiling, or add a row | `check_docstring_presence.py` (`EXCEPTIONS` — reason required; ceilings ratchet down only; tests are out of scope) |
| Validate workflow YAML locally | `make workflows-parse` |
| Check `make parity-live` still matches `parity-live.yml` | `make check-parity-live-dual-wire` |
| Check a matrix.rs Tested cite still exists | `make check-matrix-test-liveness` |
| Install the pre-commit hook | `make install-hooks` |
| Run the Apache-suite census | `bash scripts/run_census.sh` + [../docs/port/census.md](../docs/port/census.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../.pre-commit-config.yaml](../.pre-commit-config.yaml), [../Makefile](../Makefile),
  [../repo-manifest.toml](../repo-manifest.toml) (the structural facts `check_manifest.py`
  validates).

## Debug

| Symptom | First check |
|---|---|
| Guard blocks a commit | Add/update the `map.md` in the directory of the staged code |
| Guard not running | `make install-hooks` (or use the `pre-commit` framework) |
| CI's Ruff job reds on a `scripts/*.py` edit that pre-commit let through | `scripts/` is inside the Ruff gate (repo-root `pyproject.toml`, line-length 100) but the pre-commit hook set does not run Ruff over it — run `uvx ruff@<pinned> check scripts/` locally before pushing, docstring-only edits included (an E501 here red CI on 2026-08-24) |
| `crate-dag: layering inversion …` | The named edge points UP a tier. Either the edge is wrong (remove it) or the tier map is wrong — fix `check_crate_dag.py` `TIERS` and say why in the commit |
| `ERROR: undeclared dependency edge …` | A new internal dependency landed without a policy row — declare it in `check_crate_dag.py` `ALLOWED_EDGES` with its kind and a reason, or drop the dependency |
| `ERROR: dependency kind not permitted …` | The edge exists in the policy but not in this kind (the classic case: a `dev`-only cross-door edge promoted to `normal`) |
| `ERROR: forbidden edge …` / `the policy DECLARES a forbidden edge` | A structural rule fired (door→door, anything→bindings, foundation→internal, capability→door). The second form means the *table* was edited to legalize it — the rule fires on the declaration too |
| `ERROR: stale policy row …` | An `ALLOWED_EDGES` row survives an edge that was removed; delete the row |
| `… is not in the tier map` | A new `repark-*` crate was added; classify it in `check_crate_dag.py` `TIERS` (and `ROLES`) |
| `manifest: … is not declared in repo-manifest.toml` | A new Cargo member landed; add its `[components.<name>]` entry (path, layer, status) |
| `manifest: … declared delivered, but …` / `declared planned, but … exists` | The manifest and the tree disagree about what exists; fix whichever is stale — a component is delivered exactly when its code is there |
| `manifest: … layer … disagrees with the dependency-policy SSOT` | `repo-manifest.toml` mirrors `check_crate_dag.py`; change the tier map there, then the mirror |
| `manifest: … map.md never names its layer` | Say the crate's tier in its crate-root `map.md` (hand-written — the guard never writes one) |
| `manifest: … STATUS.md … does not state …` | STATUS.md is the status SSOT; the phase moved in one file and not the other |
| `crate-dag inspected zero internal crates/edges` | `cargo metadata` returned nothing internal — wrong manifest path or a broken workspace (a single-crate workspace with zero edges is fine) |
| `lib-rs: … inline #[cfg(test)] mod` | Move the test body to a file-backed module (`src/<name>.rs` + `#[cfg(test)] mod <name>;`) |
| `lib-rs: … lines (ceiling …)` | Extract production code into a named module, or add an `EXCEPTIONS` entry with a reason (ratchet down only) |
| `lib-py: … lines (default …)` / `grew to …` | Split the module, make the edit line-neutral, or obtain owner approval for an exact-baseline exception amendment |
| `lib-py: … shrank to …` | Lower the exact baseline to the measured count, or remove the row when the file meets the default |
| `lib-py: … re-export-only module must start its docstring …` | Open the module docstring's FIRST line with the exact substring `re-export binding`, or give the module real content |
| `lib-py: scan root not found` | Run from the repository tree and restore the named `python/` or `scripts/` root |
| `rust-file-size: … lines (default …)` / `grew to …` | Split the module, make the edit line-neutral, or obtain owner approval for an exact-baseline exception amendment |
| `rust-file-size: … shrank to …` | Lower the exact baseline to the measured count, or remove the row when the file meets the default |
| `rust-file-size: … scan set is empty` | Fail-closed: the guard found zero `crates/**/*.rs` files — fix the tree or the scan root |
| `… EXCEPTIONS key is outside the scan set` | Remove the stale row or restore the source path (fail-closed; not a silent skip) |
| `python-conventions: … defines N nested function(s)` | Lift the definition to module or class level and pass what it needs as arguments; or add `# nested-def: <reason>` if it is a decorator factory, a state-capturing callback, or a `functools.wraps` wrapper; or raise the `NESTED_DEF_EXCEPTIONS` row with a reason (ratchet down only) |
| `python-conventions: … imports \`dataclasses\`` | Convert the container to a Pydantic v2 `BaseModel` (`model_config = ConfigDict(frozen=True)` for the frozen case), or add a `DATACLASS_EXCEPTIONS` row with a reason |
| `python-conventions: … does not parse` / `scan set is empty` | Fail-closed: the guard refuses to report success over a file it could not read or a tree it could not find |
| `docstring-presence: … undocumented public name(s)` | Add a Google-style docstring, or add/raise an `EXCEPTIONS` row in `check_docstring_presence.py` with a reason (ratchet down only). Tests are out of scope; style `D` is declined |
| `docstring-presence: EXCEPTIONS key … measures 0` | Delete the row rather than keep a zero — the file converted |
| `docstring-presence: ruff … refuse to pass closed` | `uvx`/`ruff@0.15.22` missing, ruff exit other than 0/1, or JSON did not parse — environment error, not a finding |
| `map-sync: … dead link` | The map points at a path that moved or was deleted — repoint it, or `python3 scripts/sync_map_md.py --fix` if the whole list row should go |
| `map-sync: … unmentioned` | Only under `--strict`: the directory's map never names that file — add a row with a real description (`--fix` writes a `TODO(describe)` stub, never prose) |
| `workflows-parse` red | Fix the named workflow's YAML — GitHub would never run it as-is |
| `run_census.sh` fails on `python/repark` | The facade package arrives with the facade PR; until then only the port-source side of the procedure is runnable |
| A census cohort's denominator looks blended | `--stretch` was used for the classic cohort; use `--classic` ([../docs/port/census.md](../docs/port/census.md) §2) |
| `run_census.sh` aborts on the environment | Intended: an empty `pip freeze`, a missing gated version, or pandas ≥ 3 all fail the run at provisioning time. A run whose environment is not recorded is not a baseline (design §5 F2) |
| A census run's markdown reports are "missing" from `task/` | They are not written there: the default `CENSUS_REPORT_DIR` is the gitignored `target/census-reports/` (declared change 3). The final line of the run echoes the directory it wrote |
| `parity-live dual-wire: FAIL` / parse incomplete | A load-bearing flag drifted between `Makefile` `parity-live` and `.github/workflows/parity-live.yml` — change one, change the other. A parse miss is also red (fail-closed); fix the surface or the extractor in `check_parity_live_dual_wire.py` |
| `matrix-test-liveness: FAIL` / dead cite | A `matrix.rs` `Tested` row names a test `cargo test -- --list` does not print — rename the cite with the test, or flip the row to `DeliberatelyAbsent`. A parse miss or cargo non-zero is also red (fail-closed); SSOT: `check_matrix_test_liveness.py` |

First checks: `bash scripts/check_map_md.sh`, `python3 scripts/sync_map_md.py --check`,
`bash scripts/check_crate_dag.sh`,
`bash scripts/check_lib_rs.sh`, `bash scripts/check_lib_py.sh`,
`bash scripts/check_python_conventions.sh`, `bash scripts/check_docstring_presence.sh`,
`bash scripts/check_manifest.sh`,
`bash scripts/check_parity_live_dual_wire.sh`, `bash scripts/check_matrix_test_liveness.sh`,
`make workflows-parse`. Escalate to:
[../map.md#debug](../map.md).
