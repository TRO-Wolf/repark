# repark developer command surface. `make help` lists targets. `make ci` is the fast canonical
# gate; `make preflight` runs the FULL CI surface locally (verify + security audits + workflow
# lint) and is the pre-PR gate — if preflight is green, the PR checks are green. Every
# CI-enforced tool runs here too, version-pinned identically — nothing silently skips when a
# tool is absent (uvx provisions the pinned tool on demand).
#
# Phase 1: the workspace has members; the phase-0 empty-workspace guard is gone. Targets whose
# subject does not exist yet (parity, census, maturin/wheels, Python thinness, Python tests,
# the uv lock gate) return with their phase — see docs/port/PLAN.md.
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
RUFF   := uvx ruff@0.15.22
TAPLO  := uvx taplo@0.9.3
TYPOS  := uvx typos@1.47.2
ZIZMOR := uvx zizmor@1.26.1
CARGO_DENY_VERSION := 0.19.9
CARGO_AUDIT_VERSION := 0.22.1
UV_VERSION := 0.9.5

.PHONY: help
help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort | awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ------------------------------------------------------------------------------------------------
# Aggregated gates
# ------------------------------------------------------------------------------------------------

.PHONY: ci
ci: rust-fmt-check rust-clippy rust-panic-ban check-crate-dag check-lib-rs rust-check py-lint py-format-check toml-check spell-check ## Fast gate (lint + format + static checks); see preflight for the full CI surface

.PHONY: test
test: rust-test ## All tests (Rust only until the Python packages land in phase 3)

.PHONY: verify
verify: ci test ## ci + test — full local verification

.PHONY: preflight
preflight: verify audit workflows-lint ## The pre-PR gate: everything CI runs

.PHONY: audit
audit: rust-audit rust-deny ## Security gates (cargo-audit + cargo-deny; pip-audit returns in phase 3)

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
rust-panic-ban: ## Panic ban + async cancel-safety ban (ci.yml's rust job runs this same target)
	# Production only (`--lib --bins`): disallowed-methods has no allow-in-tests.
	# This is the ONLY gate where clippy.toml's `disallowed-methods` list is live (the general
	# rust-clippy target passes -A clippy::disallowed_methods on purpose). It enforces BOTH
	# lists: unwrap/expect AND the tokio::spawn / spawn_blocking cancel-safety ban. Sanctioned
	# escapes are per-call-site #[expect(clippy::disallowed_methods, reason=…)]. When
	# repark-python lands (phase 3) it is excluded here and gated separately with
	# unwrap_used/expect_used only (PyO3 create_exception! macro expect) — see clippy.toml.
	cargo clippy --locked --workspace --lib --bins -- \
		-D clippy::disallowed_methods \
		-D clippy::unwrap_used \
		-D clippy::expect_used \
		-D clippy::panic \
		-D clippy::todo \
		-D clippy::unimplemented \
		-D clippy::unreachable

.PHONY: check-crate-dag
check-crate-dag: ## Crate-DAG layering guard (no edge to a strictly higher tier)
	@# The tier map is the SSOT in scripts/check_crate_dag.py — prose points there, never restates.
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
install-hooks: ## Wire .git/hooks/pre-commit to map.md + crate-DAG + lib.rs guards + cargo fmt + taplo + typos
	@# check_crate_dag.sh and check_lib_rs.sh are hook-eligible because they are measured fast
	@# (sub-second: a `cargo metadata` read and a pure text scan). Hook budget stays < 1 s
	@# beyond cargo fmt; check_lib_py.sh rejoins when it is ported (phase 3).
	@printf '#!/usr/bin/env bash\nset -e\nscripts/check_map_md.sh\nscripts/check_crate_dag.sh\nscripts/check_lib_rs.sh\ncargo fmt --check\n$(TAPLO) format --check\n$(TAPLO) lint\n$(TYPOS)\n' > .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "installed .git/hooks/pre-commit"
