# task/wo/ns-array-floor-1/salvage-1047/ — the measurement corpus of PR #1047, kept for U-2

PR #1047 (NS-ARRAY-APPLY-IN-PANDAS-REFUSE-1, branch `fix/ns-array-apply-in-pandas-refuse-1`,
head `1a3ac2e5`'s lineage, 2026-10-13) built the mechanism the owner replaced on 2026-10-13 late
(adopt item C): the adopted design, NS-ARRAY-FLOOR-1, floors a `datetime64[ns]` array to
microseconds under `array<timestamp>` by dtype, as Spark 4.1.2 does, where #1047 refused by an
integer rule. The PR was closed on 2026-10-20 after the morning triage. Its product code, its
two refusal texts, the facade-callback exemption and its R-020 wording are discarded with it.
What is kept here is the measurement corpus the registry row R-020 and both cards cite as their
source, so step 1 of U-2 re-records the same cells on live Spark instead of rebuilding them.

These files are a record, not a test module: nothing under this directory is collected by
pytest, cited as a pin, or run by a gate. Paths inside the files still name
`python/repark/tests/`, where they lived before #1113 moved the facade tests into subsystem
directories; the step-1 branch places them under `python/repark/tests/pandas/` (or wherever
the subsystem map says) and re-records `ns_array_apply_in_pandas_refuse_1_main.json`, which
holds main `f3f98823`'s answers and is stale by design.

- [_ns_array_apply_in_pandas_refuse_1_cases.py](_ns_array_apply_in_pandas_refuse_1_cases.py) —
  the case matrix the recorder and the pins shared: 197 cases over `USER_CASES`,
  `TABLE_CASES` (expressions over the nanosecond source table `ice.ns.src`), `FRAME_CASES`
  (Arrow columns in `large_list`, `fixed_size_list` and `list_view` layouts), and the facade's
  GROUPED_AGG and ordered-window routes; nine `open_` cases hold main's stored ticks for a later
  unit. Its `answer` helper returns the Arrow type and int64 ticks, or the error class, cause
  class and text with the cause's own text cut. This is the numpy-array shape that U-2's step 0
  (#1064) did not measure.
- [_record_ns_array_apply_in_pandas_refuse_1_main.py](_record_ns_array_apply_in_pandas_refuse_1_main.py)
  — the recorder: one session over a fresh warehouse per cell, run against a build of a named
  commit's Python source; prints the path of the `repark` package it measured. Re-run it; do not
  trust the JSON beside it.
- [ns_array_apply_in_pandas_refuse_1_main.json](ns_array_apply_in_pandas_refuse_1_main.json) —
  what main `f3f98823` answered in every cell (pandas 3.0.5, numpy 2.5.1, pyarrow 25.0.1), one
  cell per line. Stale: the type planner (#1082) and the floor doors (#1059) landed after it.
- [_time_ns_array_apply_in_pandas_refuse_1.py](_time_ns_array_apply_in_pandas_refuse_1.py) —
  the cost timer: seconds of a grouped `applyInPandas` over ten shapes of one million rows in
  1,000 groups and 50,000 groups of four rows; the recorded sets are in the ledger's Fold 1.
- [ns-array-apply-in-pandas-refuse-1-ledger-at-1047.md](ns-array-apply-in-pandas-refuse-1-ledger-at-1047.md)
  — the unit ledger as the branch left it: §F1.1 (what was measured before the design), §F1.5
  (the mutants), §F1.7 (what the door cannot know), §F1.9 (the verdict's findings, each closed
  or recorded). It is outside `task/ledgers/`, so the ledger-grammar gate does not read it and
  its clause ids are citations of a closed unit, not live pins.

Before U-2 step 1 is ordered, the owner should see the 2026-10-20 triage card's reading: U-2's
step 0 found Spark floors under `array<timestamp_ntz>` on pandas 3.0.6, so the card's "refuse
under ntz" half may need a re-ruling, and step 1 should re-record the numpy-array shape on live
Spark first.

Up: [../map.md](../map.md).
