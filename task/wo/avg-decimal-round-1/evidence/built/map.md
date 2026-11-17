# task/wo/avg-decimal-round-1/evidence/built/ — the build's bar, cost and raw data (2026-11-17)

Built by Claude Opus 5.5 (`claude-opus-5-5`) on branch `fix/avg-decimal-round-1`, product commit `27d6149f`, package
`pkg-avg-27d6149f` (built from a clean worktree of that commit; its `BUILT_FROM` names it). Main's package is
`pkg-dct-7bf32859`; main's product tree equals it.

- [bar.md](bar.md) — step 0's whole domain table re-run on the head (7,258 comparisons, 0 EQUAL → non-EQUAL, native
  door and other families unchanged), the critic's cell sets, the pins (red on main, green on the head), the
  non-decimal `mean` cells, the partial sum past 38 digits, and the `mean` reachability.
- [cost.md](cost.md) — K1 to K4, the reported decimal `mean`, and the C-012 set, at least five alternating runs.
- [raw/](raw/) — the head's compacted cells (`head/`, step 0's layout), `compare.jsonl` (per cell: main and head
  verdicts against Spark), `critic.jsonl` and the critic sets' outputs (`critic/`), the non-decimal cells
  (`nondec/`), Spark's partition probe (`partition/`), and the cost samples (`cost/`). Scratch paths are scrubbed to
  `<scratchpad>`.

The runners stayed in the lane's scratchpad (they are not comment-gated repository code). Step 0's probes in
[../probes/](../probes/map.md) produced `head/`; the cost runner was modelled on DECIMAL-COMMON-TYPE-1's.

Up: [../map.md](../map.md).
