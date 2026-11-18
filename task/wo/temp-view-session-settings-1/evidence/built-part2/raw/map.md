# task/wo/temp-view-session-settings-1/evidence/built-part2/raw/ — every cell of part 2's runs (compacted)

Scratch and home paths replaced by placeholders.

- `<build>-true.jsonl`, `<build>-unset.jsonl` — the step 0's `run.py` on `spark`, `pre` (main before part 1,
  `pkg-avg-27d6149f`), `part1` (main with part 1, `pkg-view1-c54f5fc7`) and `head` (`513208ce`, delta 1); `rev-<build>.jsonl`,
  `rc-<build>.jsonl`, `sweep-<build>.json` — `rev.py`, the re-critic's `rc.py` and the delta 1 sweep on each.
- `bar2-cells.jsonl`, `bar2-summary.md` — `../probes/bar2.py`'s pairing: every cell's verdict per build, every move,
  every non-EQUAL part 2 cell; `part2-left.jsonl` — those cells with their named class.
- `pins-<label>.json` — the part 2 pin file's cells on Spark, `pre`, `part1` and the head (`_record_temp_view_session_settings_2.py`).
- [delta1/](delta1/) — delta 1's runs on Spark, `pre`, `part1`, `b74` (`74c22dfe`) and `head` (`513208ce`): the reader
  sweep, the join sweep (`joins-task2.jsonl` with task 2's backed-out fix, `task2.patch`), `smartCsv`, the verifier's
  probes, Fable's probe, the plans; `summary.txt` the tallies.

Up: [../map.md](../map.md).
