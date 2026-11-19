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
- [delta2/](delta2/) — `wsweep.jsonl`: the write sweep's 3,100 cells under matching settings, each cell's answer on
  `pre`, `part1`, delta 1 (`513208ce`), the head (`9d71e030`) and, for six doors, Spark.
- [delta3/](delta3/) — `../probes/race.py`'s outputs: `race-d4423a69.json` (the merged delta 2), `race-head-1.json`,
  `race-head-2.json` (`9aa2f182`), `race-main-1.json`, `race-main-2.json` (main `6286ba19`), 5,000 writes each, and the
  head's 75-second hang runs `hang-head-unrelated.json` and `hang-head-captured.json`.

Up: [../map.md](../map.md).
