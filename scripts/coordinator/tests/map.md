# map — scripts/coordinator/tests/

The coordinator's own tests. The coordinator is operator tooling, not a CI gate; its tests run
directly with pytest, not through the workspace suites.

## Contents

- `test_claude_usage.py` — pytest for `../claude_usage.py`: builds a two-file transcript fixture
  in `tmp_path` (one main session, one sub-agent, with one idle and one non-idle turn),
  asserts per-file sums with a total row, the `--since` stamp filter, the idle-turn
  columns, and the default table mode. Run:
  `python3 -m pytest -q scripts/coordinator/tests/test_claude_usage.py`.
