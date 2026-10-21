# task/wo/panic-fatal-suite-1/evidence/ — measurements of PANIC-FATAL-SUITE-1

Measured 2026-10-21 on main `f0918cb5` and the head of `fix/panic-fatal-suite-1` (`b868db05` for the code); the wheel is
`maturin develop --profile ci`, every run under `taskset -c 0-25` with `CARGO_BUILD_JOBS=12`. The box was shared with
another builder, so the load figures sit beside the two wall-clock failures.

- [facade-counts.txt](facade-counts.txt) — the full facade suite on main and armed on the head: 1 failed, 20161 passed,
  484 skipped, 151 xfailed on main; 0 failed, 20167 passed in the second armed run and 1 failed, 20166 passed in the
  third. The failures are timing tests that pass alone. The first armed run stalled at 99 % and is described there.
- [pin-green.txt](pin-green.txt) — the pin file serial on the head: 5 passed.
- [xdist-n2-each-green.txt](xdist-n2-each-green.txt) — the same file under `-n 2 --dist each`: 10 passed, every test on
  both workers.
- [mutation-flag-zero-red.txt](mutation-flag-zero-red.txt) — the `conftest.py` line set to `0`: the order pin and both
  inheriting child legs go red (3 failed, 2 passed). Not committed.
- [mutation-order-red.txt](mutation-order-red.txt) — the line moved below the first `repark` import: the order pin goes
  red (1 failed, 4 passed). Not committed.
- [child-output.txt](child-output.txt) — what the four children print: `PanicException` and exit 1 through the
  `sql`-method fence, SIGABRT and exit -6 through the Arrow C stream fence, and the unchanged message with the flag
  removed.
- [rust-tests.txt](rust-tests.txt) — the 15 fence tests, the mutation that reads the environment again (the new test
  goes red), the `repark-python` counts on main (189) and the head (191), `make verify` (7715 passed, 97 skipped) and
  `make rust-panic-ban`.

Up: [../map.md](../map.md).
