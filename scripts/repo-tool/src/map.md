# map — repository tooling source

## Purpose

`repository.rs` captures Git inputs. `main.rs` dispatches commands and exit codes.
`cache.rs` retains structural results. The other modules own their named command.
Runtime workflow evidence is explicit and cannot be replaced by a cached structural result.
Map and context inputs have explicit scopes. Runtime evidence requires the full snapshot.
Map link diagnostics index newline positions once per file. GFM parsing and the legacy
line scan both validate links; intermediate path components must exist as directories.
Tracked POSIX backslashes remain filename bytes.

`validation.rs` shares worktree inputs for `docs_links.rs` and `ledger_grammar.rs`.
`ledger_records.rs` parses legacy ledger forms. Compatibility details live in
[CHECKS.md](../CHECKS.md); the snapshot trace command keeps its separate rules.

## Contents

<!-- repo-tool:contents:start -->
- [cache.rs](cache.rs)
- [context.rs](context.rs)
- [docs_links.rs](docs_links.rs)
- [evidence.rs](evidence.rs)
- [gates.rs](gates.rs)
- [ledger_grammar.rs](ledger_grammar.rs)
- [ledger_records.rs](ledger_records.rs)
- [lib.rs](lib.rs)
- [main.rs](main.rs)
- [maps.rs](maps.rs)
- [repository.rs](repository.rs)
- [state.rs](state.rs)
- [trace.rs](trace.rs)
- [validation.rs](validation.rs)
- [workflow.rs](workflow.rs)
<!-- repo-tool:contents:end -->
