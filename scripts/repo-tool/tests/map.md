# map — repository tooling tests

## Purpose

Temporary Git repositories exercise snapshot isolation, negative inputs and repeatability.
CLI fixtures cover exit codes and staged hooks. Evidence fixtures do not require live services.
Scoped-input fixtures cover cache reuse, changed dependencies, staged context, stale writes,
link line numbers and refusal to use partial snapshots for runtime evidence.

Trace fixture strings encode the citation delimiter so the repository-wide grammar gate does
not mistake deliberate invalid test input for a real project citation.

`maps_compat.rs` compares the Python map gate against the Rust CLI on legacy syntax classes,
the real map corpus, staged snapshots and POSIX backslash filenames.

`docs_links.rs` and `ledger_grammar.rs` compare Rust findings against the Python migration
references. `validation.rs` covers the combined CLI, fresh filesystem checks and refused modes.

## Pins

pins: repo-compiler/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008

pins: repo-compiler-checks/C-001, C-002, C-003, C-004

## Contents

<!-- repo-tool:contents:start -->
- [cache.rs](cache.rs)
- [cli.rs](cli.rs)
- [context.rs](context.rs)
- [docs_links.rs](docs_links.rs)
- [evidence.rs](evidence.rs)
- [gates.rs](gates.rs)
- [ledger_grammar.rs](ledger_grammar.rs)
- [maps.rs](maps.rs)
- [maps_compat.rs](maps_compat.rs)
- [repository.rs](repository.rs)
- [state.rs](state.rs)
- [trace.rs](trace.rs)
- [validation.rs](validation.rs)
- [workflow.rs](workflow.rs)
<!-- repo-tool:contents:end -->
