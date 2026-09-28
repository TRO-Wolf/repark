# map — repository tooling

## Purpose

Standalone Rust tools compile repository facts and approved records into navigation,
context, diagnostics and local workflow decisions. They have no engine dependencies.

## Contract

Snapshot commands use explicit Git inputs. The checks command validates the current worktree. Generated views do not prove behavioral claims.
The [unit ledger](../../task/ledgers/completed/repo-compiler-ledger.md) records delivery evidence.
`context.json` approves source lists. `gates.json` declares target and pin checks.

[CHECKS.md](CHECKS.md) records docs-link and ledger-grammar compatibility.
The standalone Cargo workspace has its own lockfile. `make audit` checks it alongside the engine
workspace; Dependabot tracks both workspaces separately.

## Debug

Run `make repo-tool-check` before repository-wide gates. Command pages describe schemas.

## Contents

<!-- repo-tool:contents:start -->
- [CHECKS.md](CHECKS.md)
- [CONTEXT.md](CONTEXT.md)
- [Cargo.toml](Cargo.toml)
- [INTERFACE.md](INTERFACE.md)
- [MAPS.md](MAPS.md)
- [README.md](README.md)
- [WORKFLOW.md](WORKFLOW.md)
- [src/map.md](src/map.md)
- [tests/map.md](tests/map.md)
<!-- repo-tool:contents:end -->
