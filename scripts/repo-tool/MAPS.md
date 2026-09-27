# Map compiler

`maps --check` validates every tracked `map.md` in the selected Git snapshot. It checks local inline links and duplicate list rows. `--strict` also checks that each supported sibling file is mentioned. A map with a managed contents block must match the snapshot inventory in both modes. Findings set `ok` to `false` and produce stable diagnostics.

`maps --check --require-managed --path DIR` also requires that directory's map to contain an active managed block. Markers inside fenced examples do not satisfy the requirement. `--require-managed` cannot be combined with `--write`.

The public `run` entry point returns an error for malformed options, unsafe paths, and failed writes. It returns `ok:false` for map findings. This CLI error contract is recorded here because source comments are disallowed for this unit.

A map opts in to generated contents with these two markers on separate lines:

```markdown
<!-- repo-tool:contents:start -->
<!-- repo-tool:contents:end -->
```

`maps --write` updates only these blocks. `maps --write --path DIR` limits the write to one directory. `maps --write --init --path DIR` adds a block to that directory's map, preserving existing authored text. An absent map receives a minimal heading. Generated rows list supported local source and prose files plus child map links, in sorted order. The generated block uses links without placeholder descriptions; authored descriptions belong outside it. Archives are immutable. Writes require a worktree snapshot and fail if the map changed after snapshot capture.

Markers count only when they occupy their own lines outside fenced code. Fenced examples stay authored. A partial, duplicate, reversed, or non-standalone marker outside code is a finding. `--check --path DIR` can inspect an archived map; archive writes remain forbidden.

Supported local files use `.rs`, `.py`, `.sh`, `.md`, or `.toml`. `map.md`, lockfiles, and dotfiles are excluded. Link validation resolves relative `..` within the repository and refuses paths that escape it. External URLs and anchors, plus link-shaped text in inline or fenced code, are ignored. The check uses the selected snapshot's files, so staged changes and deleted names are reported accurately.

Inline links, images, and defined reference links use their parsed Markdown targets. Undefined reference syntax is literal Markdown text. Code spans and fenced blocks do not create link events.

Generated labels escape Markdown delimiters and destinations encode special filename bytes.
Local link destinations are percent-decoded before path-boundary validation.

Duplicate-row checks apply to authored lists. Generated inventories may repeat an authored
link; exact inventory comparison catches duplicates or drift inside the managed block.

The CLI captures map contents plus the tracked inventory, without opening unrelated source
bodies. The result labels this digest `input_scope: "maps"`. Names, modes, missing tracked
files and map edits invalidate it. Worktree writes recheck the same inputs before changing a
map, and check the destination again before writing. An unrelated source-body edit does not
block a map write. Link line numbers come from one newline index per map.
