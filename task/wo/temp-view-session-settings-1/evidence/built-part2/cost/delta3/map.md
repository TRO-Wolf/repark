# task/wo/temp-view-session-settings-1/evidence/built-part2/cost/delta3/ — delta 3's held-row samples and plans

- `samples.jsonl` — delta 3's 105 samples on `pkg-view2-9aa2f182`: five alternating main, head, main-aa runs for each of
  the seven held rows, every sample's exit code, and each C-012 sample's wall time. The view rows' per-process timings
  are not in this file (their rows carry `wall` 0).
- `explain-base.json`, `explain-head.json` — the six matching cost cases' plans on main now (`pkg-main-6286ba19`) and on the head.

Up: [../map.md](../map.md).
