# Internal command interface

Snapshot commands expose `run(&Repository, &[String]) -> anyhow::Result<serde_json::Value>`.
The value carries `ok: bool`. Semantic findings return `ok:false` and exit 1. Malformed
input or environmental failures return an error and exit 2. Arguments are never shell commands.

`Repository` captures tracked paths and bytes once. It exposes sorted `paths`, UTF-8 `files`,
`root`, `source`, `input_scope` and a SHA-256 `snapshot`, plus checked reads, directory lookup and guarded
writes. Binary files and symlinks contribute to identity but are not parsed as text.
Gitlink identities contribute their recorded object IDs; nested repositories are not traversed.

`Repository::load` captures full inputs. `load_scoped` accepts `InputScope::Maps` or an
explicit `InputScope::Files` set. Map scope keeps the full tracked inventory but reads only
map contents. File scope restricts both inventory and content to the selected paths.
`context::load_repository` resolves that set from the selected config and checks the config
again after loading. Scoped identities cannot substitute for full runtime evidence.

Structural commands consume that snapshot. Runtime evidence uses explicit filesystem records
and validates their revision and input identity. The cache accepts only structural read commands.
Module-specific schemas and failure conditions live in the command pages linked from [README.md](README.md).

The standalone Cargo workspace prevents repository administration from depending on engine
compilation. Keep command registration in `main.rs`; implementation modules share snapshot and
path helpers. Public entry points are internal implementation details, not a compatibility promise.

`validation::run(&Path, &[String])` implements `checks` without a `Repository` snapshot.
Its `Inputs` captures tracked names once and caches text lazily for the two validators.
Filesystem existence and resolved targets stay worktree observations. See [CHECKS.md](CHECKS.md).
