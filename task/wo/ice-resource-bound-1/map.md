# task/wo/ice-resource-bound-1/ — the work order for ICE-RESOURCE-BOUND-1 (U-11)

ICE-RESOURCE-BOUND-1 (U-11 of the Iceberg closeout, card 2026-10-20): ordered writes and
merge-on-read MERGE measured, the bound written in docs, exceeded means refused; PA-10 and PA-11
declared or closed. Documents only; no product code and no test.

- [order.md](order.md) — the grade B order (2026-10-20, awaits the owner's adopt of the 2026-10-20 plan; opens after U-9's kill pin): the measurement step first (the PP-01, PP-02 and PA-11 sizes, the command, the number that becomes the bound, then a halt for the owner's ruling), then the refusal, the detector or the PA-10 declaration, the pins on format v2 and v3 with the kill and the refusal as two outcomes, gates, halts, hand-back.
- [probe_bound.py](probe_bound.py) — the step-0 probe: measures peak RSS per statement under a memory scope, one statement per process, and `render` writes the bound table; no test cites it (a probe, not a pin).
- [probe_cost.py](probe_cost.py) — the cost gate probe (2026-10-26): `cell` runs one write-set statement in its own process and prints the median wall time; `rounds` alternates a base and a head interpreter over every cell for twelve rounds and writes the rows and the markdown table. The table is in `docs/perf/resource-bound-2026-10-26.md` once step 2 files it.
- [sketch-step-1.md](sketch-step-1.md) — the step-1 sketch (2026-10-26, on the owner's Q1 to Q5 rulings): one seam per path (PP-01 a new shared ordered-input drain, PP-02 the `matched_work_mor` loop with one budget across both legs, PA-11 the changelog view's partition), the `ResourcesExhausted` refusal text, the three fraction keys and their carrier, the cgroup v2 detector walk and the declared rows, the PA-11 declaration, the red-first cells with their pools, the kill-against-refusal pair, the cost-gate plan, the file list against the size ceilings and thirteen open questions.
- [evidence/](evidence/map.md) — the step-0 bound table of 2026-10-26, as JSON and as markdown (261 cells on formats v2 and v3).


Up: [../map.md](../map.md).
