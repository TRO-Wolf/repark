# Cost baseline run — ICE-TSNS-COERCION-1 slice 0

`cost-baseline.json` is the output of `_time_ice_tsns_coercion_1.py`: three
sets of fifteen repeats on the installed module, median of set medians.

- The cargo lock was free immediately before the run (two earlier checks found
  another lane's release build holding it).
- Load was decaying from that build: 5.70 / 5.60 / 5.63 at the three sets, and
  the only counted processes were two `python` (this run and ambient). No
  `cargo`, `rustc` or `java` was up. Set medians agree to <2% on every
  statement except `merge_mor`, which drifts down over the sets (79.35 / 75.61
  / 72.29 ms).
- What else was running: the usual ambient desktop and service processes only
  (browser, airflow, postgres); nothing building.
