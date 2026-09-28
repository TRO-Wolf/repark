# Snapshot views

These commands read the selected Git snapshot. Each JSON response includes its input SHA-256 identity. They do not run tests, resolve evidence by reading arbitrary prose, or change the repository. A malformed command or config returns a CLI error; a valid request with missing repository evidence returns `ok: false` and sorted diagnostics.

`context --config path/to/context.json --role critic --unit unit-1 --max-bytes 90000` selects approved source text. Version 1 configuration is JSON:

```json
{
  "version": 1,
  "global": [{"path": "README.md", "section": "What repark is"}],
  "roles": {
    "executor": [{"path": "DEVELOPMENT.md"}],
    "critic": [{"path": "docs/testing.md"}],
    "orchestrator": [{"path": "ARCHITECTURE.md"}]
  },
  "units": {
    "unit-1": [{"path": "task/ledgers/staging/unit-1-ledger.md"}]
  }
}
```

The command always includes the full `AGENTS.md` first. A config cannot narrow that file to a section. A source without `section` includes its full text. A section names the exact Markdown heading text, including all subsections up to the next heading of equal or higher level. Headings inside fenced code do not count; a fence closes only with the same marker and at least the opening length. Duplicate heading matches, missing sources, and a byte budget overflow are diagnostics; no text is truncated. The `global`, selected role, and selected unit arrays must each contain at least one source. A critic config should select the critic's own review inputs; actor execution narrative must not be put in the critic's global, role, or unit source lists. The command cannot classify prose by intent, so the approval boundary is the explicit source list reviewed with the config.

Context loading reads the selected config first, then captures only that config and the approved
source files. The config must have identical bytes in both reads. The JSON `input_scope.files`
list declares those dependencies; the digest includes their paths, modes and whole file bytes,
even when the output selects a section. Unselected role sources do not enter the snapshot.
The source mode applies to both reads, so staged configs select staged source contents.

`trace [--unit ID]` scans clause rows in tracked ledgers and `pins:` markers in tracked files under `crates/`, `python/`, and `scripts/`, including their `map.md` files. It accepts one verdict cell anywhere after each clause ID and normalizes an archived ledger's date prefix to its unit ID. It reports each `C-NNN` status, ledger line, and recorded citation lines. Duplicate clauses or units, unknown pins, ledgers with no clause rows, missing requested units, and PROVEN clauses without citations are diagnostics. Citations inside fenced code do not count. A marker line is a recorded citation, not proof that a named test exists or passes. The command never derives a test identity from a sentence.

`state` derives unit locations from `task/ledgers/staging/`, `completed/`, and dated `archive/` paths. It reads `ws` and `unit` lifecycle markers in `STATUS.md` and `briefs/next-sequence.md`. A closed workstream needs `closed=yyyy-mm-dd`, `by=#N`, and a `history=docs/history/name` path. Conflicting ledger bins and malformed or unterminated markers are diagnostics. This view does not infer a merge or alter an archive.

`gates --config path/to/gates.json` checks declared target names against Makefile rules and parsed assignments or TOML values in tracked canonical files. Version 1 configuration is JSON:

```json
{
  "version": 1,
  "targets": ["ci", "verify", "preflight", "rust-clippy"],
  "pins": [
    {"name": "ruff", "kind": "make", "path": "Makefile", "key": "RUFF", "expected": "uvx ruff@0.15.22"},
    {"name": "datafusion", "kind": "toml", "path": "Cargo.toml", "key": "workspace.dependencies.datafusion", "expected": "54.1.0"}
  ]
}
```

The three required targets `ci`, `verify`, and `preflight` cannot be omitted. Target and pin names must be unique. A `make` pin reads an uncommented assignment to the named variable in `Makefile`; a `toml` pin reads a dotted key path from parsed TOML. Missing or ambiguous assignments and exact value mismatches are findings or errors. The command does not execute a Make recipe or interpret shell syntax. Gate definitions and actual tool execution remain in the Makefile.

The public `run` functions return errors for invalid CLI syntax and malformed config. Their error contract is documented here to keep Rust source free of new comments.
