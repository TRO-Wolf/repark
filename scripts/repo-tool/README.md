# Repository compiler

This standalone Rust tool builds deterministic views of repository facts. It has no engine
or DataFusion dependency. Make targets keep its build, tests and checks separate from the engine.
The unit closes when its implementation is accepted; ongoing usage documentation remains here.

## Commands

Run `scripts/repo-tool.sh --help` for the command surface. Global options precede the command.
Use `--snapshot index` for staged content or `--snapshot <revision>` for a committed Git tree.
The default is `worktree`: tracked paths with current working-file contents.
Untracked files are not silently included; stage intended new sources before compiling views.

| Command | Contract |
|---|---|
| `checks` | [Combined worktree docs-link and ledger-grammar validation](CHECKS.md). |
| `index` | Sorted paths, directories and content identity from one Git snapshot. |
| `maps` | [Map checking and opt-in generation](MAPS.md). |
| `context` | [Approved role context with source provenance](CONTEXT.md). |
| `trace` | [Recorded clause-to-citation relationships](CONTEXT.md). |
| `state` | [Read-only lifecycle views](CONTEXT.md). |
| `gates` | [Declared target and pin validation](CONTEXT.md). |
| `evidence` | [Typed review evidence validation](WORKFLOW.md). |
| `workflow` | [Persistent local transition decisions](WORKFLOW.md). |

The `checks` command uses current worktree inputs rather than a Git content snapshot.
Its compatibility and cache restrictions are documented in [CHECKS.md](CHECKS.md).

Output is JSON. Exit 0 means the selected check succeeded, exit 1 means reported findings,
and exit 2 means invalid arguments, malformed inputs or an environment failure.
A structurally valid record is not proof that its assertions are correct.

## Snapshots and paths

Snapshots use Git's tracked path set, sorted deterministically. Index and revision modes read
Git blobs in one batch. Working files can differ from the index; the selected mode is explicit.
Full snapshots bind all paths, modes and bytes, including binary content. Symlink targets are
hashed as link text and are never opened as source content. Reads outside the root and symlink
writes are refused. Path aliases with repeated separators or dot components are refused. A conflict in the index is an error.

Map commands use `input_scope: "maps"`: all tracked names, modes and worktree presence,
but only map file contents. Context commands use `input_scope: {"files": [...]}`: their
config, full `AGENTS.md`, and approved global, role and unit sources. Each scoped digest
includes its scope. Source-body changes outside these inputs do not invalidate their caches.
Selected contents are still hashed on each invocation; timestamps do not establish freshness.
Other commands retain full snapshots. Evidence and workflow reject scoped inputs.

Generated text writes require worktree mode and reject a file changed since it was indexed.
Authored sections remain authored. Archives are excluded from map mutation. Existing ledger
archive commands remain responsible for moving records; lifecycle reporting never infers a merge.

## Cache

`--cache-dir .repo-tool/cache` enables optional caching for read-only maps, context, trace,
state and gate checks. The key includes source mode, scoped input identity, arguments and the executable.
Both passing and failing results are retained. A malformed or damaged entry is recomputed.
Evidence, runtime tests, workflow events and write commands cannot use this cache.
The cache is local build data, not an attestation or a shared trust boundary.

## Validation

`make repo-tool-check` runs formatting, Clippy with the repository's test/production distinction,
and fixtures over temporary Git repositories. `make verify` includes this tool's gate.
The engine's existing gates remain unchanged in meaning.

Public functions are implementation entry points for the CLI, not a supported external library.
Their errors are documented here and in the command pages. Local missing-errors-doc expectations
keep the zero-new-source-comments rule while retaining all other lint checks.

## Isolation

The workflow command emits local action records. It does not launch agents, execute configured
shell commands, push branches, merge pull requests, mutate services or change campaign launchers.
An operator must integrate action consumption under the existing authorization policy.

## Repository presets

`make repo-context ARGS="--config scripts/repo-tool/context.json --role critic --unit repo-compiler --max-bytes 120000"`
compiles the explicit source list for this unit. Review that list before adding other units.
A budget measures bytes, not model tokens. Required context is never silently truncated.

`make check-repo-gates` checks the supplied target and pin configuration. It supplements the
existing manifest and workflow guards; it does not replace their cross-file checks.

Stage new input files before using worktree or index views. To opt a directory into generated
inventory, run `make maps ARGS="--init --path scripts/repo-tool"`, then review and stage the result.
Use `make check-maps ARGS="--path scripts/repo-tool"` to check it without writing.
