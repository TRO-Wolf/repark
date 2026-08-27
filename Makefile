# repark developer command surface. `make help` lists targets. `make ci` is the fast canonical
# gate; `make preflight` runs the FULL CI surface locally (verify + facade suite + security
# audits + workflow lint) and is the pre-PR gate — if preflight is green, the PR checks are
# green. `make verify` stays Rust-only (inner-loop). Every
# CI-enforced tool runs here too, version-pinned identically — nothing silently skips when a
# tool is absent (uvx provisions the pinned tool on demand).
#
# Every target's subject exists. The port completed at phase 3 (STATUS.md), so nothing here
# no-ops waiting for a phase; the test surface is split by what each suite NEEDS to run — see the
# note on the `test` target below.
#
# Note: Rust uses `cargo test --locked --workspace` (NOT `--all-features`) on purpose — `--all-features`
# enables repark-python's `extension-module`, which breaks linking a standalone test binary.
# See AGENTS.md "PyO3 build notes".

.DEFAULT_GOAL := help

# Tool pins — keep in lockstep with .github/workflows (identical values Makefile==workflow).
# Bump a version here and in every workflow that uses it in the same change.
#   ruff/taplo/typos/zizmor — uvx pins in ci/taplo/typos/zizmor.yml
#   UV_VERSION              — setup-uv `version:` in every setup-uv job
#   CARGO_DENY_VERSION      — taiki-e install-action tool: cargo-deny@… in cargo-deny.yml
#   CARGO_AUDIT_VERSION     — cargo install pin in the rust-audit target + audit.yml
REPO_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
MATURIN := uvx maturin@1.14.1
RUFF   := uvx ruff@0.15.22
TAPLO  := uvx taplo@0.9.3
TYPOS  := uvx typos@1.47.2
ZIZMOR := uvx zizmor@1.26.1
CARGO_DENY_VERSION := 0.19.9
CARGO_AUDIT_VERSION := 0.22.1
UV_VERSION := 0.9.5
# Local JVM for `make parity-live` (Spark 4.1.2 needs Java 17; CI uses Temurin 17 via setup-java).
# Defaults to the caller's JAVA_HOME so no machine-specific path is committed; set it to a Java 17
# home if your default JAVA_HOME is a different major: `make parity-live PARITY_LIVE_JAVA_HOME=...`.
PARITY_LIVE_JAVA_HOME ?= $(JAVA_HOME)

.PHONY: help
help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort | awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ------------------------------------------------------------------------------------------------
# Aggregated gates
# ------------------------------------------------------------------------------------------------

.PHONY: ci
ci: rust-fmt-check rust-clippy rust-panic-ban check-crate-dag check-lib-rs check-rust-file-size check-lib-py check-python-conventions check-docstring-presence check-manifest check-ledgers check-ledger-grammar check-docs-compaction check-parity-live-dual-wire check-matrix-test-liveness rust-check py-lint py-format-check py-lock-check toml-check spell-check ## Fast gate (lint + format + static checks); see preflight for the full CI surface

# `test` is the Rust workspace suite, and that is the whole of it — deliberately, not pending.
# The three Python suites are excluded because each needs something `cargo test` cannot give it:
#   * the FACADE suite (python/repark/tests) needs the compiled native module, so it runs behind a
#     build step — locally `make py-test-facade` (maturin develop + pytest, extras provisioned);
#     in CI the wheels.yml `smoke` job, which runs the same suite against the PACKAGED wheel (a
#     facade regression must not pass CI on an import smoke alone).
#   * the PARITY harness (python/repark-parity/tests) is pyarrow + pydantic (PYC-4 records) —
#     `make py-test`, mirrored by ci.yml's `parity-harness tests` step.
#   * the LIVE oracle tier needs a JVM — `make parity-live` / parity-live.yml, never in `verify`.
# `make verify` = `ci` + `test`; it is JVM-free and native-build-free on purpose
# (inner-loop speed). The compiled-module facade suite is `make py-test-facade` and is
# wired into `make preflight` (2026-08-12, G14), not into `verify`. CI still runs that
# suite against the built wheel (`wheels.yml` smoke).
.PHONY: test
test: rust-test ## Rust workspace suite only (facade: `make py-test-facade`, also in preflight; parity: `make py-test`)

.PHONY: verify
verify: ci test ## ci + rust-test — JVM-free, native-build-free (inner-loop)

.PHONY: preflight
preflight: verify py-test-facade audit workflows-lint ## The pre-PR gate: verify + facade suite + security + workflow lint

.PHONY: audit
audit: rust-audit rust-deny py-audit ## Security gates (cargo-audit + cargo-deny + pip-audit)

# ------------------------------------------------------------------------------------------------
# Rust
# ------------------------------------------------------------------------------------------------

.PHONY: rust-fmt-check
rust-fmt-check: ## cargo fmt --check
	cargo fmt --check

.PHONY: rust-clippy
rust-clippy: ## clippy with -D warnings
	# disallowed_methods list is active only in rust-panic-ban (lib/bin);
	# keep the general gate inert for test unwrap (clippy folds the lint into `all`).
	cargo clippy --locked --workspace --all-targets -- -D warnings -A clippy::disallowed_methods

.PHONY: rust-panic-ban
rust-panic-ban: ## Panic ban + async cancel-safety ban (ci.yml's rust-lint job runs this same target)
	# Production only (`--lib --bins`): disallowed-methods has no allow-in-tests.
	# This is the ONLY gate where clippy.toml's `disallowed-methods` list is live (the general
	# rust-clippy target passes -A clippy::disallowed_methods on purpose). It enforces BOTH
	# lists: unwrap/expect AND the tokio::spawn / spawn_blocking cancel-safety ban. Sanctioned
	# escapes are per-call-site #[expect(clippy::disallowed_methods, reason=…)].
	# repark-python (phase 3): EXCLUDED from the workspace invocation only because its second
	# invocation below must drop -D expect_used for the five create_exception! macro sites —
	# but disallowed_methods (and with it the tokio spawn/spawn_blocking cancel-safety ban)
	# STAYS LIVE for the crate: the macro sites sit inside a module-scoped
	# #![expect(clippy::disallowed_methods, reason=…)] (the narrowest escape that works —
	# a per-call-site #[expect] cannot reach inside the macro expansion; provocations
	# P-2/P-4/P-5 in task/p3c-binding-ledger.md).
	cargo clippy --locked --workspace --lib --bins --exclude repark-python -- \
		-D clippy::disallowed_methods \
		-D clippy::unwrap_used \
		-D clippy::expect_used \
		-D clippy::panic \
		-D clippy::todo \
		-D clippy::unimplemented \
		-D clippy::unreachable
	cargo clippy --locked -p repark-python --lib -- \
		-D clippy::disallowed_methods \
		-D clippy::unwrap_used \
		-D clippy::expect_used \
		-D clippy::panic \
		-D clippy::todo \
		-D clippy::unimplemented \
		-D clippy::unreachable

.PHONY: check-crate-dag
check-crate-dag: ## Crate dependency-policy guard (declared edges + kinds; no edge to a higher tier)
	@# The tier map, the crate roles and the allowed-edge table are the SSOT in
	@# scripts/check_crate_dag.py — prose points there, never restates.
	@# DUAL-WIRED: the `crate-DAG layering guard` step in ci.yml's guards job mirrors this
	@# target. Change one, change the other.
	@./scripts/check_crate_dag.sh

.PHONY: check-lib-rs
check-lib-rs: ## lib.rs thinness guard (no inline tests; line ceilings)
	@# Ceilings + EXCEPTIONS SSOT: scripts/check_lib_rs.py — dual-wired with ci.yml's guards job.
	@# Mirrors make check-crate-dag (dual-wire lesson: Makefile AND ci.yml, never one alone).
	@./scripts/check_lib_rs.sh

.PHONY: check-rust-file-size
check-rust-file-size: ## Rust source ceiling with exact-baseline exceptions
	@# Default ceiling + EXCEPTIONS SSOT: scripts/check_rust_file_size.py — dual-wired with
	@# ci.yml's guards job. Companion to check-lib-rs (crate-root thinness). Exception baselines
	@# stay exact and ratchet down; prose points at the script and never restates the numbers.
	@./scripts/check_rust_file_size.sh

.PHONY: check-python-conventions
check-python-conventions: ## The two Python rules Ruff cannot express (nested def; Pydantic not dataclasses)
	@# Rules + EXCEPTIONS SSOT: scripts/check_python_conventions.py — dual-wired with ci.yml's
	@# python job. Companion to check-lib-py (source size + facade thinness). Ceilings ratchet down;
	@# prose points at the script and never restates the tables.
	@./scripts/check_python_conventions.sh

.PHONY: check-docstring-presence
check-docstring-presence: ## Public-docstring presence (D101/D102/D103/D105/D107) with a ratchet
	@# Rules + EXCEPTIONS SSOT: scripts/check_docstring_presence.py — dual-wired with ci.yml's
	@# python job. Ruff is the parser; the wrapper is the per-file ceiling table. Ceilings
	@# ratchet down only; prose points at the script and never restates the table.
	@./scripts/check_docstring_presence.sh

.PHONY: check-parity-live-dual-wire
check-parity-live-dual-wire: ## Fail if make parity-live and parity-live.yml drift (load-bearing flags)
	@# SSOT: scripts/check_parity_live_dual_wire.py — compares the two surfaces to EACH OTHER,
	@# never to a third hand-maintained list. Fail-closed on parse miss.
	@# DUAL-WIRED: the `parity-live dual-wire guard` step in ci.yml's guards job mirrors this
	@# target. Change one, change the other.
	@./scripts/check_parity_live_dual_wire.sh

.PHONY: check-matrix-test-liveness
check-matrix-test-liveness: ## Fail if a matrix.rs Tested cite is missing from cargo test -- --list
	@# SSOT: scripts/check_matrix_test_liveness.py — cargo test -- --list vs both door
	@# matrices' Tested names. Fail-closed on a parse miss or a dead cite.
	@# DUAL-WIRED: the `matrix test-name liveness` step in ci.yml's rust-test job mirrors
	@# this target. Change one, change the other. In make ci / make preflight.
	@./scripts/check_matrix_test_liveness.sh

.PHONY: rust-check
rust-check: ## cargo check
	cargo check --locked --workspace

.PHONY: rust-test
rust-test: ## cargo test (workspace; see note above re: --all-features)
	cargo test --locked --workspace

# ------------------------------------------------------------------------------------------------
# Python
# ------------------------------------------------------------------------------------------------

.PHONY: py-lint
py-lint: ## ruff check
	$(RUFF) check .

.PHONY: py-format-check
py-format-check: ## ruff format --check
	$(RUFF) format --check .

.PHONY: py-test
py-test: ## Parity-harness tests (isolated env; no native build) — mirrors ci.yml python step
	PYTHONPATH=python/repark-parity/src \
		uv run --no-project --with pyarrow --with pytest --with 'pydantic>=2.10,<3' \
		pytest python/repark-parity/tests -q

.PHONY: parity
parity: py-test ## Run the Spark-parity differential harness (alias)

.PHONY: py-lock-check
py-lock-check: ## uv lock --locked — RED if uv.lock lags its pyproject floors (mirrors ci.yml step)
	uv lock --locked

.PHONY: census
census: ## Hermetic Apache-suite census (classic/expand/expand2); local+slate only, never CI-wired
	@# SSOT: docs/port/census.md; the classic cohort runs via the additive --classic flag (F1).
	@./scripts/run_census.sh

.PHONY: check-lib-py
check-lib-py: ## Python source ceiling + facade no-stub guard
	@# Exact baselines + EXCEPTIONS SSOT: scripts/check_lib_py.py — dual-wired with ci.yml python job.
	@./scripts/check_lib_py.sh

.PHONY: develop
develop: ## Build + install the native module editable into the root .venv (maturin develop)
	cd python/repark && VIRTUAL_ENV="$${VIRTUAL_ENV:-$(REPO_ROOT)/.venv}" $(MATURIN) develop

# The canonical facade extras — the same four docs/port/census.md §4 fixes for the full-extras
# cohort and wheels.yml's `smoke` job installs (`repark[numpy,pandas,polars,ml-ext]`). Keep the
# three in lockstep: a cohort whose denominator moves with an install decision is not a gate.
FACADE_EXTRAS := --extra numpy --extra pandas --extra polars --extra ml-ext

.PHONY: py-test-facade
py-test-facade: ## Facade tests against the real native module (provisions extras, builds via maturin)
	@# Step 1 PROVISIONS the declared extras. Without it a thin `.venv` (root `dev` group only —
	@# pandas + numpy, no polars, no ml-ext) silently SKIPPED the polars and ML paths while
	@# reporting green: importorskip turns a missing extra into a skip, and skips do not fail.
	@# `--locked` resolves from the checked-in uv.lock (RED if it lags) so the set is
	@# deterministic; `--no-install-package repark` keeps the maturin step below AUTHORITATIVE
	@# for repark itself instead of racing it with a from-source install.
	uv sync --locked $(FACADE_EXTRAS) --no-install-package repark
	@# `--no-project` on the RUN makes the maturin step authoritative too: without it, `uv run`
	@# prefers the PROJECT's environment over VIRTUAL_ENV — in a linked worktree it silently built
	@# a second `.venv` from source (the maturin install above went unused), and everywhere it
	@# could re-sync over maturin's editable install.
	cd python/repark && VIRTUAL_ENV="$${VIRTUAL_ENV:-$(REPO_ROOT)/.venv}" $(MATURIN) develop
	PYTHONPATH=python/repark-parity/src VIRTUAL_ENV="$${VIRTUAL_ENV:-$(REPO_ROOT)/.venv}" \
		uv run --no-project python -m pytest python/repark/tests -q

.PHONY: parity-live
parity-live: ## Live PySpark oracle tier: re-derive every pinned golden from real Spark 4.1.2 (needs a JVM; JVM-free `ci`/`verify` are unaffected)
	@# Mirrors .github/workflows/parity-live.yml step for step. Provisions pyspark 4.1.2 (the
	@# `record` extra, pinned in uv.lock), builds the native module, then runs the full facade
	@# suite AND the live tier (REPARK_PARITY_LIVE=1) so it asserts repark == pinned golden ==
	@# live Spark. Flag unset (ci/verify) → the live tests SKIP; this target arms them.
	@# Every sync flag below is load-bearing, because `uv sync` is EXACT — it uninstalls whatever
	@# the requested extras do not name:
	@#   --locked : uv.lock is an INPUT here, never an output. A bare `uv sync` may rewrite the
	@#     checked-in lockfile as a side effect of a test run.
	@#   --extra numpy/pandas/polars/ml-ext : the four facade extras (docs/port/census.md §4).
	@#     Without them a run that touches an already-provisioned .venv REMOVES polars, xgboost,
	@#     lightgbm and scikit-learn, and the facade suite then silently SKIPs every polars/ML
	@#     test — a green live tier over a shrunken denominator, which is not a gate.
	@#   --no-install-package repark : keeps the `maturin develop` below AUTHORITATIVE; otherwise
	@#     uv installs repark from source and shadows maturin's editable build of the cdylib.
	@# The pytest step uses `uv run --locked --no-sync` for the same two reasons: `uv run` re-syncs
	@# the project environment by default (undoing both of the above), and it has no
	@# --no-install-package escape — --no-sync is the only way to keep an explicitly provisioned
	@# environment intact (`--locked` there is inert while --no-sync is present; kept as a guard
	@# should --no-sync ever be dropped). The VIRTUAL_ENV pin below matches the other Python
	@# targets: `uv` ignores a foreign active virtualenv (with a warning) while `maturin` honors
	@# it — unpinned, the two would target DIFFERENT environments under an activated unrelated
	@# venv. Keep the uv/maturin/pytest flags identical in parity-live.yml (dual-wired).
	VIRTUAL_ENV="$${VIRTUAL_ENV:-$(REPO_ROOT)/.venv}" uv sync --locked --extra record \
		--extra numpy --extra pandas --extra polars --extra ml-ext \
		--no-install-package repark
	cd python/repark && VIRTUAL_ENV="$${VIRTUAL_ENV:-$(REPO_ROOT)/.venv}" $(MATURIN) develop
	JAVA_HOME=$(PARITY_LIVE_JAVA_HOME) SPARK_LOCAL_IP=127.0.0.1 REPARK_PARITY_LIVE=1 \
		VIRTUAL_ENV="$${VIRTUAL_ENV:-$(REPO_ROOT)/.venv}" uv run --locked --no-sync pytest python/repark/tests -q

.PHONY: py-audit
py-audit: ## Python dependency CVE scan (mirrors pip-audit.yml)
	@# --no-emit-workspace drops the local workspace packages; pip-audit scans published deps.
	uv export --frozen --no-emit-workspace --format requirements-txt > requirements-audit.txt
	uvx pip-audit --strict -r requirements-audit.txt
	@rm -f requirements-audit.txt

.PHONY: build-wheel
build-wheel: ## Build the repark release wheel with maturin
	cd python/repark && $(MATURIN) build --release

# ------------------------------------------------------------------------------------------------
# TOML + spelling (uvx-provisioned — always run, same pinned version as CI)
# ------------------------------------------------------------------------------------------------

.PHONY: toml-check
toml-check: ## taplo format --check + lint (mirrors taplo.yml)
	$(TAPLO) format --check
	$(TAPLO) lint

.PHONY: spell-check
spell-check: ## typos (mirrors typos.yml)
	$(TYPOS)

# ------------------------------------------------------------------------------------------------
# Repo-structure guards
# ------------------------------------------------------------------------------------------------

.PHONY: check-manifest
check-manifest: ## Structural-manifest guard (repo-manifest.toml vs workspace, docs, gates, maps)
	@# SSOT: repo-manifest.toml (the structural facts) + scripts/check_manifest.py (the rules);
	@# layers are cross-checked against scripts/check_crate_dag.py, never restated.
	@# DUAL-WIRED: the `repo-manifest guard` step in ci.yml's guards job mirrors this target.
	@# Change one, change the other.
	@./scripts/check_manifest.sh

.PHONY: check-map-md
check-map-md: ## map.md lockstep guard over staged changes (also wired into the pre-commit hook)
	bash scripts/check_map_md.sh

.PHONY: check-ledgers
check-ledgers: ## Ledger lifecycle guard: bins, archive names, every ledger link, frozen bins (DL-1)
	@# SSOT: scripts/ledger_lifecycle.py `check` — a `*-ledger.md` under task/ outside its bins,
	@# an archive name whose date prefix disagrees with its month directory, a dead `-ledger.md`
	@# link in ANY tracked markdown (sync_map_md covers maps only), and a completed/ or archive/
	@# ledger edited beyond a link repair or a prepended errata note. Policy: AGENTS.md
	@# "Markdown document lifecycle". DUAL-WIRED: the `ledger lifecycle guard` step in ci.yml's
	@# guards job mirrors this target (fetch-depth: 0 there). Change one, change the other.
	python3 scripts/ledger_lifecycle.py check

.PHONY: check-ledger-grammar
check-ledger-grammar: ## Ledger grammar guard: clause rows, pins: citations, the Critic's attestation form (DL-2)
	@# SSOT: scripts/check_ledger_grammar.py — over task/ledgers/{staging,completed}/ (the archive
	@# is immutable and read for citations only). Meanings stay in .agents/skills/sepmo (SKILL.md "The gate is a ledger, not a score",
	@# references/05-critic.md); the script owns the shape. EXCEPTIONS (seeded 2026-08-23) ratchet
	@# down only. DUAL-WIRED: the `ledger grammar guard` step in ci.yml's guards job mirrors
	@# this target. Change one, change the other.
	python3 scripts/check_ledger_grammar.py

.PHONY: ledger-archive
ledger-archive: ## Pickup step 0: file task/ledgers/completed/ into archive/yyyy-mm/, then compact + check (zero tokens)
	@# Dates come from `main`'s first-parent history, never the clock; links across the tree are
	@# rewritten and the result is staged. Idempotent. Mark a unit finished with
	@#   python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed
	@# Since DL-4 the archive step also runs `compact` (merged units leave the slate, closed
	@# campaigns leave STATUS for docs/history/) and the gate reads the result.
	python3 scripts/ledger_lifecycle.py archive
	python3 scripts/check_docs_compaction.py

.PHONY: check-docs-compaction
check-docs-compaction: ## Live-document guard: no closed campaign in STATUS, no merged unit on the slate, every workstream marked, byte ceilings (DL-4)
	@# SSOT: scripts/check_docs_compaction.py over STATUS.md + briefs/next-sequence.md; the block
	@# grammar is scripts/doc_blocks.py and `ledger_lifecycle.py compact` is what keeps it green.
	@# The byte ceilings are the load-bearing half: raise CEILINGS only in the PR that needs it.
	@# Measured 2026-08-25: n=5 median 0.05 s (pure text + one `git ls-files`) — in the hook too.
	python3 scripts/check_docs_compaction.py

.PHONY: check-map-sync
check-map-sync: ## map.md CONTENT guard: every relative link in every map resolves (add --strict for coverage)
	@# Companion to check-map-md: that one forces a map to be TOUCHED, this one checks what it
	@# says. SSOT: scripts/sync_map_md.py. Link validity is armed here, on both pre-commit
	@# paths (measured n=5 median 0.08 s over 143 maps) and — since 2026-08-23 — as the
	@# `map.md link-validity guard` step in ci.yml's guards job. Change one, change the other. The COVERAGE rule — every mappable
	@# tracked file mentioned by its directory's map — is behind `--strict` and NOT armed: the
	@# tree measured 24 pre-existing unmentioned files at the arming commit (2026-08-22) — a
	@# FLOOR, since a name counts as mentioned anywhere it appears as a whole token — and a
	@# gate nobody can run green is not a gate. Run it by hand:
	@#   python3 scripts/sync_map_md.py --check --strict
	@# `--fix` is mechanical only (drop dead link rows, append TODO(describe) stubs); it never
	@# writes a description.
	python3 scripts/sync_map_md.py --check

# ------------------------------------------------------------------------------------------------
# Security gates (mirror cargo-deny.yml / zizmor.yml)
# ------------------------------------------------------------------------------------------------

.PHONY: rust-audit
rust-audit: ## cargo audit — RustSec CVE scan (ignores in .cargo/audit.toml)
	@# VERSION-enforcing, not presence-checking: `command -v` alone lets local run one version
	@# while CI runs the pin, silently breaking local==CI.
	@[ "$$(cargo-audit --version 2>/dev/null | awk '{print $$2}')" = "$(CARGO_AUDIT_VERSION)" ] \
		|| cargo install cargo-audit --locked --version $(CARGO_AUDIT_VERSION) --force
	cargo audit

.PHONY: rust-deny
rust-deny: ## cargo deny check all — licenses/bans/sources (deny.toml; mirrors cargo-deny.yml)
	@# VERSION-enforcing — see rust-audit. Pin must match .github/workflows/cargo-deny.yml
	@# (cargo-deny@$(CARGO_DENY_VERSION)).
	@[ "$$(cargo-deny --version 2>/dev/null | awk '{print $$2}')" = "$(CARGO_DENY_VERSION)" ] \
		|| cargo install cargo-deny --locked --version $(CARGO_DENY_VERSION) --force
	cargo deny check all

.PHONY: workflows-lint
workflows-lint: workflows-parse ## zizmor over .github/workflows — BLOCKING, like CI (mirrors zizmor.yml)
	@$(ZIZMOR) .

.PHONY: workflows-parse
workflows-parse: ## Every workflow must be parseable YAML (zizmor SKIPS files it cannot parse)
	# zizmor reports "collection yielded no auditable inputs" and exits 0 on unparsable YAML,
	# so a broken workflow would pass the security gate while GitHub silently never runs it.
	# --no-project: the repo root is not a uv project until phase 3; without it uv would
	# materialize a stray uv.lock at the root and warn about a missing requires-python.
	@uv run --no-project --with pyyaml==6.0.3 python scripts/check_workflows_parse.py

# ------------------------------------------------------------------------------------------------
# Autofix + hooks
# ------------------------------------------------------------------------------------------------

.PHONY: format
format: ## Autoformat Rust + Python
	cargo fmt
	$(RUFF) format .
	$(RUFF) check --fix .

.PHONY: lint
lint: ## Clippy + ruff (autofix Python)
	@$(MAKE) rust-clippy
	$(RUFF) check --fix .

.PHONY: bump-fork-pin
bump-fork-pin: ## Bump the iceberg-rust fork pin: make bump-fork-pin REV=<sha|branch> (docs/fork-sync.md)
	@scripts/bump_fork_pin.sh "$(REV)"

.PHONY: install-hooks
install-hooks: ## Wire .git/hooks/pre-commit to map.md lockstep + map.md links + crate-DAG + lib.rs + rust file-size + Python thinness + docstring presence + manifest guards + cargo fmt + taplo + typos
	@# check_crate_dag.sh and check_lib_rs.sh are hook-eligible because they are measured fast
	@# (sub-second: a `cargo metadata` read and a pure text scan). Hook budget stays < 1 s
	@# beyond cargo fmt; check_lib_py.sh rejoined at phase-3 PR-5 (same sub-second class),
	@# check_manifest.sh joined at FD-3 (pure text: no cargo, no network), and
	@# check_rust_file_size.sh joined at G-8 (same pure-text class as check_lib_rs).
	@# check_python_conventions.sh left the hook at PYC-5: n=5 median 0.996 s (max
	@# 1.011 s) over 164 files, at the sub-second budget line. It stays in `make ci` + ci.yml.
	@# sync_map_md.py --check joined at the markdown-lifecycle unit: n=5 median 0.08 s over
	@# 143 maps (pure text + one `git ls-files`), comfortably inside the hook budget.
	@# check_docstring_presence.sh joined at PYC-6: n=5 median 0.13 s (uvx ruff JSON +
	@# ratchet compare), well inside the sub-second hook budget.
	@# check_docs_compaction.py joined at DL-4: n=5 median 0.05 s (pure text + one `git ls-files`).
	@printf '#!/usr/bin/env bash\nset -e\nscripts/check_map_md.sh\npython3 scripts/sync_map_md.py --check\nscripts/check_crate_dag.sh\nscripts/check_lib_rs.sh\nscripts/check_rust_file_size.sh\nscripts/check_lib_py.sh\nscripts/check_docstring_presence.sh\npython3 scripts/check_docs_compaction.py\nscripts/check_manifest.sh\ncargo fmt --check\n$(TAPLO) format --check\n$(TAPLO) lint\n$(TYPOS)\n' > .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "installed .git/hooks/pre-commit"
