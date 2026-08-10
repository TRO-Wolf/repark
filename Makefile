# repark developer command surface. `make help` lists targets. `make ci` is the fast canonical
# gate; `make preflight` runs the FULL CI surface locally (verify + security audits + workflow
# lint) and is the pre-PR gate — if preflight is green, the PR checks are green. Every
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
ci: rust-fmt-check rust-clippy rust-panic-ban check-crate-dag check-lib-rs check-lib-py check-manifest rust-check py-lint py-format-check py-lock-check toml-check spell-check ## Fast gate (lint + format + static checks); see preflight for the full CI surface

# `test` is the Rust workspace suite, and that is the whole of it — deliberately, not pending.
# The three Python suites are excluded because each needs something `cargo test` cannot give it:
#   * the FACADE suite (python/repark/tests) needs the compiled native module, so it runs behind a
#     build step — locally `make py-test-facade` (maturin develop + pytest, extras provisioned);
#     in CI the wheels.yml `smoke` job, which runs the same suite against the PACKAGED wheel (a
#     facade regression must not pass CI on an import smoke alone).
#   * the PARITY harness (python/repark-parity/tests) is pure pyarrow — `make py-test`, mirrored
#     by ci.yml's `parity-harness tests` step.
#   * the LIVE oracle tier needs a JVM — `make parity-live` / parity-live.yml, never in `verify`.
# `make verify` = `ci` + `test`; it is JVM-free and native-build-free on purpose.
.PHONY: test
test: rust-test ## Rust workspace suite (facade: `make py-test-facade`; parity: `make py-test`)

.PHONY: verify
verify: ci test ## ci + test — full local verification

.PHONY: preflight
preflight: verify audit workflows-lint ## The pre-PR gate: everything CI runs

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
		uv run --no-project --with pyarrow --with pytest \
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
check-lib-py: ## Python thinness guard (line ceilings + no-stub)
	@# Ceilings + EXCEPTIONS SSOT: scripts/check_lib_py.py — dual-wired with ci.yml python job.
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
	uv sync --extra record
	cd python/repark && $(MATURIN) develop
	JAVA_HOME=$(PARITY_LIVE_JAVA_HOME) SPARK_LOCAL_IP=127.0.0.1 REPARK_PARITY_LIVE=1 \
		uv run --extra record pytest python/repark/tests -q

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

.PHONY: install-hooks
install-hooks: ## Wire .git/hooks/pre-commit to map.md + crate-DAG + lib.rs + Python thinness + manifest guards + cargo fmt + taplo + typos
	@# check_crate_dag.sh and check_lib_rs.sh are hook-eligible because they are measured fast
	@# (sub-second: a `cargo metadata` read and a pure text scan). Hook budget stays < 1 s
	@# beyond cargo fmt; check_lib_py.sh rejoined at phase-3 PR-5 (same sub-second class), and
	@# check_manifest.sh joined at FD-3 (pure text: no cargo, no network).
	@printf '#!/usr/bin/env bash\nset -e\nscripts/check_map_md.sh\nscripts/check_crate_dag.sh\nscripts/check_lib_rs.sh\nscripts/check_lib_py.sh\nscripts/check_manifest.sh\ncargo fmt --check\n$(TAPLO) format --check\n$(TAPLO) lint\n$(TYPOS)\n' > .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "installed .git/hooks/pre-commit"
