# map — task/wo/native-dataframe-write-silent-1/evidence/after

## Purpose

The head runs of NATIVE-DATAFRAME-WRITE-SILENT-1 against main `1d9d2808`.

- **The facade pin** (`python/repark/tests/writes/test_native_dataframe_write_silent_1.py`)
  runs all 2436 step 0 cells on both doors. On main 406 tests fail: the 399 guarded cells, the
  three relative-path naming pins, the two dot-path naming pins and the two workaround pins. On
  the head's release build all 2448 tests pass.
- **The Rust pins** (`crates/repark-python/src/session_write_options_tests.rs`), with only the
  guard's call removed, fail the two native refusal pins. The staging-name helper and the
  read and Spark-door control pass. On the head all four pass.

## Contents

- [red-main.txt](red-main.txt) — both runs on main, failures grouped by test.
- [green-head.txt](green-head.txt) — both runs on the head.

## Pointers

- Up: [evidence map](../map.md)
