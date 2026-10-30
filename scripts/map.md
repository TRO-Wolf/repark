# map — scripts/

Dated ratchet history (2026-08 to 2026-10) is being moved out of this map: each baseline table holds its current value, and `git log -- scripts/map.md` and the merged pull requests record each change.

`build_api_freeze.py` writes the frozen-surface register (API-FREEZE). It reads the answered API-review packet and the tree (the `check_example_coverage.py` enumerator for public Python names and their required parameters, `repark_common::surfaces::ALL` plus both door matrices, the conf-key constants, `repark.errors.__all__`, the two `pyproject.toml`/`Cargo.toml` packaging facts) and writes `docs/design/v1-0-api-freeze.json`. `--write` regenerates; bare invocation checks the tree against the checked-in file. The register is pinned by `python/repark-parity/tests/test_api_freeze.py`, so `make py-test` is the gate; regenerate in the same commit as any intended additive change.

## Purpose

Repository helper scripts wired into the dev workflow. Q1 re-home (2026-08-14):
`check_lib_py.py` EXCEPTIONS paths moved under `python/repark/src/repark/spark/`.

CC-2 audits the tracked Python helper scripts after all crate and package slices finish.

CC-2 scripts slice: every tracked `scripts/*.py` audited; gate-rule statements and constant
tables kept, narration and dated history removed. The same unit ratcheted 17
`check_lib_py.py` test-module baselines down to their condensed lengths and retired 2 rows
that fell below the default ceiling; 4 more parity-harness baselines ratcheted in the
repark-parity slice.

## Contents

- [dev/](dev/map.md) — **C-0 (2026-10-02):** disposable Postgres Compose file and
  `pg_disposable.sh` (`up` / `down` / `url` / `reap`) for the live-database cells.
- `coordinator/` — the tick-driven lane coordinator (2026-09-22, moved in from the campaign's
  scratch tooling and renamed from `xorch`): a bash driver that waits for free and wakes a model
  for one bounded tick at a time, one lane per systemd unit; engines for Grok, Muse, GLM and Codex;
  `gate.sh`, `review.sh`, `pr.sh`, `verdict.sh` helpers; the handbook every tick starts with; the lessons ledger.
  Operator tooling, not a CI gate. See [coordinator/map.md](coordinator/map.md).
- `aws_acceptance_cleanup.py` — **AWS-EPHEMERAL-RUNS-1 step 2 (2026-10-24):** removes the scratch tables of
  finished `aws-acceptance` runs. `--run-prefix testing_r<digits>a<digits>_<acc|u|x>_` takes one run's one leg (tables and the leg's
  `u12s3path_r<digits>a<digits>_<leg>_` S3 path-write objects), `--older-than-hours N` takes every run-and-leg prefix whose
  newest table is older than N hours (tables only); any other prefix, the bench table `ice_read_perf_bench`
  (it has no run shape) and every name without the `testing_r<digits>a<digits>_` shape are refused; a placeholder or unset
  warehouse is refused before a client exists. Glue tables go in batches of 100, then the
  objects below each table's location, only when the location is strictly under
  `<warehouse>/testing_repark_acceptance/`; S3 Tables tables go one by one in the scratch namespace. Dry run
  unless `--execute`. `boto3` is imported inside `boto3_client` only, because `uv.lock` carries no `boto3`; each job
  installs it at an exact version (ledger C-014), and `main(argv, clients)` takes a client factory for tests. Wired into `aws-acceptance.yml`: each leg job runs it last with `--run-prefix`, and the `backstop` job runs it with `--older-than-hours 24`. Tests: `python/repark-parity/tests/test_aws_acceptance_cleanup.py`.
- `sepmo_packet.py` — **SEPMO-E2 (2026-09-06, round 3):** compact worker packet
  assembler. `build --unit --role --base --brief` writes Markdown (stable prefix
  first) and a JSON sidecar; `check` validates the schema, every stable-prefix
  rule, sidecar `authority.constraints == STABLE_RULES` (order and text), the
  adapter trailer, a re-render of the dynamic section, `bash -n` on each gate
  command, and prefix-negating phrases; `diff` prints the dynamic section only.
  Field extractors live in `sepmo_packet_extract.py` (so the assembler stays
  under the default Python ceiling). The banned-trailer literals the checker
  scans for are assembled at runtime, so the tree never carries them (the
  pre-push hook forbids them). Fixtures under
  `python/repark-parity/tests/fixtures/sepmo_packets/`. Not a CI gate.
  Invocation: `python3 scripts/sepmo_packet.py build …` or
  `make sepmo-packet ARGS='check <packet>'`.
- `sepmo_packet_extract.py` — **SEPMO-E2 (2026-09-06, round 3):** brief field
  extractors for the packet assembler: fenced/inline gate commands, writable /
  closed / never-touch lists, a PATH_TOKEN plus bare-directory scan of boundary
  spans that fails `build` when a path is not captured, brief-declared
  hand-back keys, trailer and prefix-negation scans. Loaded as a sibling of
  `sepmo_packet.py`.
- `sepmo_usage.py` — **SEPMO-E0E1 (2026-09-06, round 3):** local-only usage collector.
  `collect <run-dir>` emits one normalized JSON record; `index <dir>` writes the inventory
  table (or `--jsonl`). Muse tokens come from the session store via `runs.tsv` column 6
  (env `SEPMO_MUSE_SESSIONS_ROOT` / `SEPMO_MUSE_RUNS_TSV` for tests). Grok reads
  `cache_read_input_tokens` and `modelUsage`. A minority of unparsed JSONL lines, or an
  `exit` file with no `run.terminal.completed`, emits a degraded record (`truncated: true`,
  `missing_reason` on `steps`/`tool_calls`); session-store tokens stay valid. Majority-bad
  JSONL and a non-run directory still fail loudly. Any argument containing `://` is refused
  on the raw path. Null, never zero, when an adapter does not report a field. No network.
  Invocation: `python3 scripts/sepmo_usage.py collect <run-dir>` or
  `make sepmo-usage ARGS='collect <run-dir>'` (not a CI gate).
- `bump_fork_pin.sh` — bumps the iceberg-rust fork `[patch.crates-io]` pin: rewrites all five
  `rev` lines + `Cargo.lock` together (single-writer-per-pin invariant-checked), prints the
  fork changelog URL for the PR body. Wrapped by `make bump-fork-pin REV=<sha|branch>`;
  contract in [../docs/fork-sync.md](../docs/fork-sync.md).
- `check_map_md.sh` — the map.md **lockstep** advisory (MAP-PR-GATE-1; a warning since MAP-GOVERNANCE-1). Bare mode reads `git diff --cached`; `--base <ref>` reads `<ref>...HEAD`, so a map that lands in any commit of the branch satisfies it. A `.rs`, `.py`, `Cargo.toml` or `pyproject.toml` change (lockfiles excluded; root-level manifests map to `map.md`) that does not change its directory's `map.md` in the same diff prints a `WARNING:` line. Both modes exit 0: lockstep is review, not a gate, and the map gates are `check_map_size.py` and `sync_map_md.py --check`. Invoked by `.pre-commit-config.yaml`, the hook `make install-hooks` writes, `make check-map-md` and ci.yml's `map.md guard` step. Proofs: `python/repark-parity/tests/test_map_pr_gate_1.py`, `test_map_governance_1.py`.
- `sync_map_md.py` — the map.md **content** guard, companion to `check_map_md.sh` (that one
  prints a warning when code changes without its map; this one checks what the map actually
  says) and the SSOT for its rules.
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
  hand (`python3 scripts/sync_map_md.py --check --strict`);
  (3) **duplicate rows**, unconditional — two list rows whose FIRST link carries the same target
  are a finding (the shape a `merge=union` resolution leaves behind when two branches edited the
  same row; a single row that mentions the same file twice is not a duplicate), which `--fix`
  never resolves — which row keeps the target is a hand decision. `--fix` is mechanical only: it deletes
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
  document-lifecycle rules it serves are [../AGENTS.md](../AGENTS.md) "Markdown document lifecycle".
- `check_map_size.py` — the **size ratchet** (MAP-GOVERNANCE-1): a tracked `map.md` stays within 200 lines and 16 KiB. An over-ceiling map is an exact-baseline row in `EXCEPTIONS`: an excepted map fails only past both its row and its `git merge-base HEAD origin/main` size (main's growth is not the branch's), and shrinkage fails until the row is lowered; `--update-baselines` lowers rows, raises one to the tree's size up to the merge-base size, and refuses a new violator. A push to `main` cannot red (HEAD is the base); the PR run holds it. `EXEMPT` and `EXEMPT_PATTERNS` name lifecycle inventories. Gated in `make ci` and ci.yml. Proofs: `python/repark-parity/tests/test_map_governance_1.py`, `test_map_governance_1_lifecycle.py`.
- `check_ledger_grammar.py` — the ledger **grammar** guard (DL-2; pin binding moved to sidecars by MAP-GOVERNANCE-1), over the live bins `task/ledgers/staging/` and `completed/`; the archive is read for clause ids and its sidecars for bindings. Three rules, shape only (the meanings stay in `.agents/skills/sepmo/`). **(A)** every clause row (`| C-NNN |`) has a unique id, exactly one verdict cell (`PROVEN` / `OPEN` / `REJECTED`) and an evidence cell. **(B)** a clause is bound by its unit's sidecar `task/ledgers/<bin>/<unit>-pins.md`, which counts only beside its own `<unit>-ledger.md`: one line `C-NNN: <path>[::<name>], ...`, or `C-NNN: none (records)`, which the gate counts and lists. A path is a tracked test file or a file under `scripts/`; a `::name` is a fn, type, module, constant or class that file defines, so a renamed or deleted test goes red. A `pins: <unit>/C-NNN` citation in a source file under `crates/`, `python/` or `scripts/` still binds; one in a `map.md` does not. Every `PROVEN` clause of a staging or completed ledger must be bound at least once; the measured floor per ledger is in `EXCEPTIONS` and ratchets down only. A staging or completed ledger whose first 40 lines carry `**Path:**` valued `READING` outside a code span is a reading unit: rule B does not apply to it. **(C)** the `COVERAGE_ATTESTATION:` block lists `AT-1`..`AT-10` once each, `ATTACKED` with artifacts or `N/A` with a justification, and is required once a governed ledger has no `OPEN` clause; `FINDING:` records carry the ref 05 fields. Two sub-rules were measured and declined (an `OPEN` row carries a `?`; a quantified clause names its enumeration). Exit 0 / 1 / 2. Wired as `make check-ledger-grammar` in the `make ci` chain and as ci.yml's `ledger grammar guard` step. Proofs: `python/repark-parity/tests/test_dl_2_ledger_grammar.py`, `test_map_governance_1.py`.
- `migrate_pins_to_sidecars.py` — the one-time, idempotent mover (MAP-GOVERNANCE-1) of `pins:` citations out of maps into each ledger's sidecar `task/ledgers/<bin>/<unit>-pins.md`. A clause binds the union of its citing entries' bindable files and the files its own row names; a row-named test binds by name, and a name that two files define is a hand candidate. `--report hand-candidates` lists the rest; `--check` writes nothing. Proofs: `python/repark-parity/tests/test_map_governance_1.py`, `test_map_governance_1_union.py`.
- `migrate_pins_text.py` — the map-text half of the migration (MAP-GOVERNANCE-1, split out in fix round 11 for the size ceiling): a map's entries, the cut of a `pins:` citation with its separators, and the disclaimer sentences. Imported by the migration; not run on its own.
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
  The expected text moved once (2026-09-07, owner-ruled): the adjustment's enforcement sentence
  now says review holds condensation and the file-size ratchets are the gates, replacing the
  `make check-comment-density` claim PR #247 had already retired.
  Dual-wired through `make check-owner-ruling` in `make ci` and a raw guard step in ci.yml.
  Provocations:
  `python/repark-parity/tests/test_pr_247_owner_ruling.py`.
  Since 2026-10-17 it also preserves the owner's Opus 5.0 ban block byte-for-byte in both files,
  directly after the enforcement boundary in `AGENTS.md` and after the comment ruling in `CLAUDE.md`.
- `check_model_ban.py` — the **2026-10-17 Opus 5.0 ban gate**: every git-tracked text file is
  scanned for a 5.0 Opus model id in any spelling (hyphens, underscores or a dot between the
  digits, any case); a hit fails with `path:line`. The allowed id is `claude-opus-5-5`; prose such as
  "Opus 5.0" with a space is not an id and passes. A self-test assembles the banned spellings at
  run time so the script never contains one, and fails closed if the pattern misses any.
  Dual-wired through `make check-model-ban` in `make ci` and a raw guard step in ci.yml.
  Provocations: `python/repark-parity/tests/test_model_ban.py`.
- `check_docs_compaction.py` (AGENTS.md ceiling 27,816 B, ratcheted 2026-09-27 by READ-PATH-1) — the **live-document gate** (DL-4, `make check-docs-compaction`, in
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
- `check_docs_links.py` — the **markdown link gate** (DOCS-LINKS-1, 2026-09-09; `make
  check-docs-links`, in `make ci` beside `check-docs-compaction`, so in `make preflight`
  through `verify`). `sync_map_md.py` generalized from maps to the whole tree: over every
  tracked `*.md` (`git ls-files`; code spans and fenced blocks hold documentation, not links;
  `http(s)`/`mailto` and absolute `/…` targets are out of scope, said here once): (1) every
  inline relative link (`[t](p)`, `[t](p#anchor)`) resolves to a tracked file relative to the
  linking file — a file by name, a directory by anything tracked beneath it — and a
  same-file `[t](#anchor)` resolves against the linking file's own headings; (2) a `#anchor`
  on an `.md` target matches the GitHub-style slug of the rendered heading text (link
  syntax reduced to its text before slugging; lower-case, spaces to `-`, punctuation
  dropped, duplicate headings suffixed by occurrence of the final slug, so `Foo`, `Foo`,
  `Foo-1` anchor `foo`, `foo-1`, `foo-1-1`); a fence opener carries at most three leading
  spaces, so a four-space-indented fence is no fence, and an unclosed fence at end of file
  is its own finding; (3) `docs:` evidence cells under `task/ledgers/**` follow the same
  rule with repo-root-relative paths, table cells only, never prose. One line per broken
  link `path:line: <link> -> <reason>`, exit 1; the counts of files and links checked print
  on exit 0. Whole-tree run measured 0.82 s (692 files, 4478 links; 0.83/0.81 s on repeats,
  2026-09-09; 736 files, 4772 links on 2026-09-10 after same-file anchors started counting).
  REVIEW-FIX-12 (2026-09-10) closed the REVIEW-1 rounds Q-35, Q-36, Q-37, Q-38, Q-46, Q-47:
  rendered-text slugs, final-slug duplicates, table-cell-only `docs:` cells, the unclosed-
  fence finding, the absolute-target skip, same-file anchors — which caught one genuinely
  stale same-file fragment (`docs/spark-sql-iceberg-parity.md:3053`, repaired in the same
  commit) — and the three-space fence rule. The D-3 baseline seeded
  `scripts/docs_links_allowlist.txt` with the 10
  pre-existing broken links (6 missing targets, 5 of them in immutable archive ledgers, and
  4 stale anchors into the divergence registry) — one `path:link` entry per line (D-9, audit
  round 2, 2026-09-09: the line number is not part of the key, so an unrelated edit that
  shifts lines never reds the gate, and two identical broken links in one file collapse to
  one entry), a `#`-prefixed header tolerated, a malformed entry fails closed (exit 2), and
  the list ratchets down under the gate (D-8, audit round 1, 2026-09-09): an entry that
  matched no finding in the run is itself a failure (`stale entry … — the link is no longer
  broken; remove this row`), so a fixed link takes its row away under gate pressure. The
  baseline was re-measured on the merged tree at the D-9 reseed: the same 10 links, 9
  entries after the collapse. **Dual-wired since RF-8 (O-1,
  2026-09-10):** `make check-docs-links` in `make ci` AND the `docs link guard
  (check_docs_links)` step in ci.yml's `guards` job, seated after `check_docs_compaction` the way
  `make ci` runs it — the DOCS-LINKS-1 card's Home named the Makefile only, which is why the
  ci.yml half arrived later as an orchestrator step. Not wired to the pre-commit hook. Proofs:
  `python/repark-parity/tests/test_dl_6_docs_links.py`.
- `ledger_lifecycle.py` — the ledger **lifecycle** script (DL-1): a ledger's state is its directory (`task/ledgers/staging/` → `completed/` → `archive/yyyy-mm/yyyy-mm-dd-<name>.md`), and moving one is a repository-wide link rewrite, so the two are one operation. `archive` files `completed/` (or the paths given) under a date read from `main`'s first-parent history, never the clock; a ledger not yet on `main` is left for the next pickup when unnamed and refused when named. `move PATH BIN` is the agent's `staging` → `completed` step and the roadmap promotions (`mid-term`, `epic-term`). Both `move` and `archive` carry the unit's `<unit>-pins.md` sidecar with its ledger (no map row is made for a sidecar; `archive` files it under the ledger's own date and month), and they move each excepted `check_map_size.py` row by exactly the link growth or shrinkage the plan causes. Both also run `compact`, which takes merged units off the slate and closed campaigns out of STATUS into `docs/history/`. The rewrite is resolution-based: a link changes only if it resolved to the moved file, and the moved file's map row travels whole into a live bin's map or condensed to one line in an archive month map. Nothing is written unless every rewritten link resolves, and the result is staged as one change. `check` is the gate: an out-of-bin `*-ledger.md`, an archive name whose prefix disagrees with its month directory, a dead `-ledger.md` link in any tracked markdown, and a `completed/` or `archive/` file other than a map or a `-pins.md` sidecar changed since the base beyond a link repair or a prepended errata (the frozen and immutable rules). It refuses to pass closed without a base commit. Exit 0 / 1 / 2. Wired as `make check-ledgers` (in `make ci`), ci.yml's `ledger lifecycle guard` step and `make ledger-archive`. Proofs: `python/repark-parity/tests/test_dl_1_ledger_lifecycle.py`, `test_map_governance_1_lifecycle.py`.
- `check_workflows_parse.py` — every GitHub Actions workflow must be parseable YAML. zizmor
  SKIPS files it cannot parse (exits 0 with "no auditable inputs"), so a broken workflow would
  pass the blocking lint gate while GitHub silently never runs it. Wired as a prerequisite of
  `make workflows-lint`.
- `check_crate_dag.sh` + `check_crate_dag.py` — the crate **dependency-policy** guard. The `.sh`
  runs `cargo metadata --format-version 1 --no-deps --locked` and pipes it to the `.py`, which
  holds three tables and is the **SSOT** for all three: the **tier map** (`TIERS`), the crate
  **roles** (`ROLES` — foundation / table service / engine / capability / door / bindings /
  runtime, the last added for `repark-distributed`), and
  the explicit **allowed-edge table** (`ALLOWED_EDGES`: every internal edge, the dependency
  KINDS it may take, and why it exists; F-Y10-1 added `repark-sql → repark-functions` `normal`;
  IPI-51 PR6 slice 3 promoted `repark-spark → repark-common` `dev` → `normal` (product code
  renders through `repark_common::spark_error`); C-2d fold 1 added `repark-functions →
  repark-common` `normal` (the shared zone horizon).
  Prose points here and never restates them. Four rules,
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
  simply not inspected — and since 2026-10-01 the table carries the layout of record ahead of
  the code: `repark-connect`, `repark-cdc`, `repark-io` (tier 1), `repark-spark-dialect` and
  `repark-crawler` (tier 3) are pre-declared with their edges from
  `task/roadmap/epic-term/crate-layout-1-8-2026-10-01.md` (CL-8), so each arrives onto an
  enforced layout. A pre-declared row is still audited: the declaration audit rejected the
  first draft's tier-1 → `repark-core` edges as layering inversions. NOTE the binding's deliberate **non-edges** (no `repark-sql`, no
  `repark-iceberg`) are still enforced by review, not here — this guard bans edges and never
  asks for an undeclared one. A declared row is a different matter once BOTH of its crates are
  workspace members: the drift rule then reports it as a stale policy row if no dependency
  backs it, because the table describes the workspace.
  **C-1 (2026-10-05, ruling R-14):** `repark-connect` became a member with C-1, and nothing
  calls it yet, so the pre-declared `repark-core → repark-connect` row left `ALLOWED_EDGES`.
  `repark-connect`'s `TIERS` and `ROLES` entries stay. C-2 restores the row (`normal`, reason
  "PRE-DECLARED for release 1.6 (crate-layout-1-8-2026-10-01.md): Session registers the
  configured database sources in the one federated namespace, the way it registers Iceberg
  catalogs today") in the change that adds the dependency and mounts the providers.
  **C-2d (2026-10-07):** the row is restored, `normal`, with that reason verbatim, in the
  change that adds `repark-core`'s `repark-connect` dependency and mounts the providers; the
  gate reads the edge as real.
  Wired into `make check-crate-dag` (in the `make ci` chain),
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
  the `shim_udf_boilerplate!` body went out to `src/shim_macros.rs`, measured 168, no raise;
  FNP-WIN-1 step 2 raised 175 → 180 and step 3 180 → 182, both via sanctioned
  out (2) with stated reasons in the row; the audit (F-1) moved the step-3
  measurement note off the ceiling line into the reason string, per the
  comment ban),
  `repark-python` — the 180-line PyO3 crate root, a MANIFEST (module decls incl. the
  file-backed `exceptions` taxonomy module, the two error folds, the `#[pymodule]`
  registration) that already uses the sanctioned file-backed test module (phase-3 PR-3, EC-10;
  ceiling ratcheted 230 → 190 when the taxonomy moved to src/exceptions.rs — without the row
  every slate reds on the crate's arrival), `repark-ta` — the verbatim-ported kernel root's `TaError`
  contract + flat re-export surface, and `repark-core` — the metadata-columns module decl +
  three-line re-export (ice-metadata-cols-1, IPI-20, measured 154; the root sat exactly at
  the 150 default), and `repark-spark` — `mod insert_arity;` (IPI-51 PR9, measured 151,
  ceiling 152; the root sat exactly at the 150 default).
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

- [check_python_conventions.sh](check_python_conventions.sh) + `check_python_conventions.py` — the **Python conventions**
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

- `check_example_coverage.sh` + `check_example_coverage.py` — the **v0.7 example-drift**
  guard (EX-0, 2026-08-31). Enumerates the public surface from an AST walk of the
  facade (`F.*`, DataFrame/GroupedData/na/stat, TA kernels, reader/writer,
  SparkSession + `repark.sql`), compares it to `docs/examples/**/COVERS` plus
  the backlog ratchet and cloud exceptions file, and executes every example
  when `repark._native` imports. `BACKLOG_BASELINE` and `EXCEPTIONS_BASELINE`
  are exact and ratchet down only (additions to exceptions must bump the
  baseline in the same commit). A `COVERS` name must be used in that script's
  body (class-surface names on a repark-rooted local; `repark.sql` on the
  module alias). `F.*` unions `functions.py` `__all__` with the installer
  export tables `install_into` appends. Dual-wired: `make check-example-coverage`
  in `make ci` and ci.yml's python job (static half). wheels.yml smoke runs
  `python -I … --require-execute`. The `.sh` wrapper forwards `"$@"`.
  **CI-1 (2026-10-03, owner ruling on H-2):** the `--require-execute` run lives in
  `smoke.yml`'s `examples` job now; the wrapper's header shed its two `Wired:` lines
  (one named `wheels.yml`) rather than reword a comment, so this map is where the wiring lives.
  Example children drop PYTHONPATH.
  EX-1 (2026-08-31) widened the closed set with the seven surfaces the owner
  ruled into v0.7 — Column, Window, WindowSpec, Catalog, the `types` module
  surface, `ml`, and Row — under five new families (`column`, `window`,
  `catalog`, `types`, `ml`): 150 names, 763 → 913, `BACKLOG_BASELINE`
  742 → 892. EX-2 batch 1 (2026-09-01) is the first backfill to move the
  ratchet the other way: eleven `F.*` math names covered, `BACKLOG_BASELINE`
  892 → 881. Its ledger records why the twelfth, `F.expm1`, stayed a backlog
  row. Class surfaces are `CLASS_SURFACES` rows and module surfaces
  `MODULE_SURFACES` rows; `MODULE_DOORS` holds the alias/import rules that
  decide whether a `types.*` or `ml.*` cover binds; `Window.*` binds on the
  `Window` class root the way `SparkSession.Builder.*` does. Every door's live
  `__all__` is cross-checked on the execute leg, so a dynamic export table
  cannot hide the way `install_into` did.
  Proofs: `python/repark-parity/tests/test_ex_0_example_coverage.py`.

- `check_rust_file_size.sh` + `check_rust_file_size.py` — the **general Rust file-size** guard
  (G-8 companion to `check_lib_rs`). DML-A (2026-08-30): `merge/mod.rs` 2131 → 2086.
  RP-2 (2026-08-27): `call.rs` ratcheted 1404 → 1111 — the
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
  DML-C ratchets `crates/repark-python/src/session.rs` 1178 → 1177 and
  `crates/repark-sql/src/tests.rs` 1523 → 1520.
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
- `check_lib_py.py` — **CATALOG-1 (2026-09-26):** exact baselines ratchet down:
  `python/repark/src/repark/spark/session/session_core.py` 2325 → 2293 and
  `python/repark/src/repark/spark/dataframe/writer_readwriter.py` 1029 → 1023 (the facade
  auto-flip and alias removal).

## I want to...

| I want to... | go to |
|---|---|
| Read why a branch was warned about map.md lockstep (a warning, never a block) | `check_map_md.sh` |
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
| Add a public-API example | `docs/examples/<family>/` with `COVERS`, then drop the name from `docs/examples/backlog.txt` and ratchet `BACKLOG_BASELINE` in `check_example_coverage.py` |
| Validate workflow YAML locally | `make workflows-parse` |
| Check `make parity-live` still matches `parity-live.yml` | `make check-parity-live-dual-wire` |
| Check a matrix.rs Tested cite still exists | `make check-matrix-test-liveness` |
| Install the pre-commit hook | `make install-hooks` |
| Run the Apache-suite census | `bash scripts/run_census.sh` + [../docs/port/census.md](../docs/port/census.md) |
| Collect SEPMO worker usage | `python3 scripts/sepmo_usage.py collect <run-dir>` / `index <dir>` (`make sepmo-usage`) |
| Assemble a SEPMO compact worker packet | `python3 scripts/sepmo_packet.py build --unit <id> --role actor --base <sha> --brief <md> --out-dir <dir>` |

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
| `example-coverage: public name … has no example` | Add a `docs/examples/<family>/*.py` with that name in `COVERS`, or (existing names only) a backlog row |
| `example-coverage: backlog still lists` / `backlog names` | Drop the name from `docs/examples/backlog.txt` and lower `BACKLOG_BASELINE`; a stale name is not in the inventory |
| `example-coverage: skipping example execution` | Native module is not importable — `make develop`, then re-run |
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
- **FNP-11A (2026-09-15, on 440b2773):** `check_lib_py.py` ratchets `functions_expr.py` to 2235 lines, the FNP-11A forwarder trim landing on top of main's baseline.
- **FNP-4B remediation (2026-09-15):** size ceilings set to the real line counts (`check_rust_file_size.py`: `column/mod.rs` 1038, `cross_door.rs` 1258; `check_lib_py.py`: `_live_parity.py` 1753).
- **FNP-WIN-1 (2026-09-15, remediation round 16a):** `check_rust_file_size.py`
  records `analyzer/time_window/mod.rs` 1268 and `spark_time_window.rs` 1125
  (window rules, kernels, and their tests); `check_lib_py.py` records
  `test_fnp_win_1.py` 1209 (critic pins). The CAP-1 mirror rows and counts
  (38/32) move with them. `datetime.rs` keeps 1700 (audit S-2; the month
  helper lives in `spark_session_window.rs`).
- **FNP-WIN-1 (2026-09-15, verification round 2, L-002/L-003):**
  `check_rust_file_size.py` moves `analyzer/time_window/mod.rs` 1268 → 1338
  (the running-end chaining test); `check_lib_py.py` moves
  `test_fnp_win_1.py` 1209 → 1314 (the `C2-L002`/`C2-L003` pins). No new
  exception row; the CAP-1 mirror rows move with them, counts still 38/32.
- **FNP-WIN-1 (2026-09-15, verification round 2, L-001):**
  `check_rust_file_size.py` moves `analyzer/time_window/mod.rs` 1338 → 1416
  (provenance recursion plus rule tests); `check_lib_py.py` moves
  `test_fnp_win_1.py` 1314 → 1415 (the `C2-L001` pins). Ratchets only.
- **FNP-WIN-1 (2026-09-15, verification round 2, L-004):**
  `repark-python/src/dataframe.rs` 1019 → 1021 (the two-spec pre-check
  call); `check_lib_py.py` moves `test_fnp_win_1.py` 1415 → 1454 (the
  `C2-L004` pin). Ratchets only.
FNP-WIN-1 orchestrator fix-up (2026-09-15, run 16a): `check_rust_file_size.py` puts `crates/repark-python/src/dataframe.rs` back at 1019 after the round-2 binding pre-check was reverted (owner ruling Q-15c-4); `check_lib_py.py` ratchets `python/repark/tests/functions/test_fnp_win_1.py` down 1454 → 1449.
- **FNP-11B step-4 orchestrator fix-up (2026-09-15, run 17a):** the step-3 row's numeral phrase is reworded to "sits exactly on the default line ceiling". CAP-1's prose pin forbids the literal old default in any carrier `map.md`, so the numeral reds `test_cap_1_prose_and_navigation_name_the_generalized_gate`; the fact is unchanged.
- **FNP-GEN-1 orchestrator fix-up (2026-09-16, run 17a):** the round added a `check_lib_py`
  EXCEPTIONS row for `check_example_coverage.py` at 1002 lines when the generator names pushed it
  two lines past the default ceiling. Size ceilings only move down (owner ruling Q-15c-4), and a
  new exception row on a **gate script** is ratchet erosion, so the module docstring's EX-1
  closed-set paragraph was reflowed instead: the file is back at the default ceiling and the
  row is gone. Compact before you except.
- **FNP-GEN-1 rebase onto #640 (2026-09-16, run 17a):** `check_example_coverage.py`'s
  `BACKLOG_BASELINE` is set to the count the script itself reports (108), not to an arithmetic
  combination of the two sides' values. Both sides of the rebase had moved it; a measured count is
  exact where an arithmetic one is only usually right.
- **ICE-V3-WRITE-DEFAULT-1 (2026-09-17):** `check_lib_py.py` ratchets
  `dataframe/writer_readwriter.py` 1101 → 1095 (the by-name projection returns the
  target column list and stops refusing missing frame columns).
  Run 21b round 2 (2026-09-18): 1095 → 1094 — `overwritePartitions()` passes the
  same column list into its `INSERT OVERWRITE` in two lines instead of three.

IPI-19 + IPI-37 (2026-09-20): `check_lib_py.py` ratchets
`dataframe/writer_readwriter.py` 1091 → 1077 (both `_by_name_projection`
bodies collapse into `dataframe/writer_schema.py`), shrink-only.

U11-EDGE-1 round 2 (2026-09-26): `check_rust_file_size.py` ratchets `repark-python/src/dataframe.rs` 1017 → 1016 and `check_lib_py.py` ratchets `dataframe/core.py` 3991 → 3981 (the join projection and the unionByName match move into `repark_core::frame_names`), both shrink-only; the CAP-1 mirror moves with them.

U11-EDGE-1 round 4 (2026-09-26): `check_rust_file_size.py` ratchets `repark-python/src/dataframe.rs` 1016 → 1005 (the `drop` method moves to the new `dataframe_names.rs` binding), shrink-only; the CAP-1 mirror moves with it.

U11-EDGE-1 round 4, V-002 (2026-09-26): `check_lib_py.py` ratchets `dataframe/core.py` 3981 → 3979 (the join's ambiguity check and requalification paid for by the compressed `drop` bookkeeping), shrink-only; the CAP-1 mirror moves with it.

U11-EDGE-1 round 5 (2026-09-26): `check_lib_py.py` ratchets `dataframe/core.py` 3979 → 3976 (the `select("*")` expansion folds into `_iter_bound_columns`), shrink-only; the CAP-1 mirror moves with it.

U11-EDGE-1 round 6 (2026-09-26): `check_lib_py.py` ratchets `dataframe/core.py` 3976 → 3973 (two docstrings condensed pay for the attribute-copy route), shrink-only; the CAP-1 mirror moves with it.

**ICE-RESOURCE-BOUND-1 (U-11, 2026-10-26):** `check_rust_file_size.py` lowers the `crates/repark-iceberg/src/write/append.rs` exception from 1737 to 1736 lines. The bound's call sites in that file net one line fewer; no ceiling rises.
