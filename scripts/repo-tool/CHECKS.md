# Worktree documentation checks

`checks` runs docs-link validation and ledger grammar in one process. `checks docs-links`
and `checks ledger-grammar` select one check. The combined command shares one Git path list
and a lazy text cache. Make and CI use the combined command; named Make targets remain
available for diagnosis.

These are worktree checks. They use Git's tracked names and current filesystem contents,
including the distinction between absent targets and existing untracked targets. The command
rejects index/revision inputs and persistent caching. Results have `ok`, `source: worktree`,
and a `checks` object keyed by check name. Each report has `ok`, `diagnostics`, and counts.
Exit 0 means clean, 1 means findings, and 2 means invalid arguments or environment failure.
They do not produce a content digest or a runtime evidence receipt. Input files are read on
first use and reused within that invocation; no atomic filesystem snapshot is claimed.

## Compatibility

The retained Python scripts are executable migration references, covered by differential
fixtures. They are not additional Make/CI implementations. The port preserves their current
rules, including their limited Markdown grammar. This command does not replace these rules
with CommonMark parsing or broaden the accepted syntax.

Docs links retain inline-link matching, angle targets and titles, code-span removal, fence
toggling, ATX heading slugs and duplicate suffixes, and ledger `docs:` cells. Path resolution
follows filesystem symlinks before checking repository bounds and tracked membership.
The allowlist retains `path:target` syntax and reports stale entries. External and absolute
inline links retain the reference gate's exclusions. Reference-style links are not added by
this port; the map compiler has its own broader parser.

Ledger grammar retains the live/archive split, verdict cells, duplicate row checks, citation
roots, full-text citations including fences, and the first-40-lines reading exemption.
Attestations and findings retain the reference's form checks, including its first-entry
behavior for duplicate category IDs and disposition-prefix matching. This port does not
silently strengthen those rules. Trace and state commands keep their separate contracts.

[ledger_grammar_exceptions.json](../ledger_grammar_exceptions.json) is the single exception
baseline. Its entries map ledger basenames to `[unpinned_clause_ceiling, attestation_required]`.
The Rust binary embeds it at build time; the wrapper's Cargo freshness check rebuilds when it
changes. The Python reference reads the same file beside its source. Baselines ratchet down.

Missing or unreadable mandatory inputs fail. Citation files that cannot be decoded or read
are skipped, matching the legacy citation scan. The Rust CLI reports environment failures
with exit 2 instead of the uncaught traceback some legacy ledger I/O errors produced.
Newline normalization, control whitespace and line splitting follow Python text reads.
Heading and word-boundary classifications use Unicode letters and numbers, not the broader
Rust alphabetic predicate. Invalid status diagnostics escape nonprintable characters.
Symlinks resolve iteratively: active cycles fail, long valid chains and repeated resolved
links remain valid. A symlink cycle returns exit 2 with an environment error. The Python reference raises an
uncaught RuntimeError with exit 1; both refuse the cycle. This error normalization is a
recorded compatibility exception, not a weaker link check.

## Validation

`make repo-tool-check` includes differential fixtures against both Python references.
Fixtures cover clean and broken inputs, current-worktree behavior, symlink resolution,
allowlists, exact exception ratchets, archive citations, row order, and malformed review
records. `checks` results must agree with invoking each check separately.
