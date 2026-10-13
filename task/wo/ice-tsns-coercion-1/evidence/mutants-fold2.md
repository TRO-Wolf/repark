# Mutants, slice 1 fold 2 (2026-10-13)

Ruling 5 (verdict finding G-2). Each mutant is the second verifier's diff, applied to
`timestamp_ns_coercion_sites.rs` in the working tree with `git apply`; the named pin was run
alone (`cargo test -p repark-functions --lib timestamp_ns_coercion::tests::<pin>`), the
file was restored with `git checkout`, and the pin was run again.

| Mutant | Pin | On the mutant | After the restore |
|---|---|---|---|
| m4 — a microsecond operand classed as a nanosecond one | `null_beside_a_microsecond_sibling_is_not_wrapped` | FAILED | ok |
| m5 — mixed `ns` / `tzns` treated as one type | `null_beside_mixed_ns_and_tzns_follows_the_mixed_rule` | FAILED | ok |
| m7 — a NULL `CASE WHEN` condition treated as a site | `null_case_when_condition_is_not_a_site` | FAILED | ok |
| n1 — the ask skipped | `aggregate_with_a_non_timestamp_null_argument_matches_main` | FAILED | ok |

The whole module is green with no mutant: 54 passed.

The ruled form for m4, `coalesce(ns, µs, NULL)` equal to main's rule alone, does not kill
m4. First run of that form on the mutant:

```
== MUTANT m4_wraps_beside_us applied
test timestamp_ns_coercion::tests::null_beside_a_microsecond_sibling_is_not_wrapped ... ok
```

DataFusion's common type of `ns` and `µs` is `µs`, so the ask never answers the nanosecond
site target and the mutant changes no plan; the same holds on the mutant for `array`,
`greatest`, `if` and `CASE` over `(ns, µs, NULL)` or `(µs, NULL)` (probed, not committed).
The pin keeps the ruled form and adds a call whose signature takes two nanosecond
timestamps over `(µs, NULL)`; that form is the one that fails on m4.

Run log, m5, m7, n1 (first pass) and m4 (second pass, after the extra form):

```
== GREEN (no mutant), whole module
test result: ok. 54 passed; 0 failed; 0 ignored; 0 measured; 930 filtered out; finished in 0.02s
== MUTANT m5_mixed_one_type applied:  2 files changed, 57 insertions(+), 13 deletions(-)
test timestamp_ns_coercion::tests::null_beside_mixed_ns_and_tzns_follows_the_mixed_rule ... FAILED
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 983 filtered out; finished in 0.01s
== restored
test timestamp_ns_coercion::tests::null_beside_mixed_ns_and_tzns_follows_the_mixed_rule ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 983 filtered out; finished in 0.01s
== MUTANT m7_case_conditions_sites applied:  2 files changed, 61 insertions(+), 12 deletions(-)
test timestamp_ns_coercion::tests::null_case_when_condition_is_not_a_site ... FAILED
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 983 filtered out; finished in 0.01s
== restored
test timestamp_ns_coercion::tests::null_case_when_condition_is_not_a_site ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 983 filtered out; finished in 0.00s
== MUTANT n1_ask_skipped applied:  2 files changed, 58 insertions(+), 15 deletions(-)
test timestamp_ns_coercion::tests::aggregate_with_a_non_timestamp_null_argument_matches_main ... FAILED
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 983 filtered out; finished in 0.01s
== restored
test timestamp_ns_coercion::tests::aggregate_with_a_non_timestamp_null_argument_matches_main ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 983 filtered out; finished in 0.01s
== GREEN (no mutant), whole module, second pass
test result: ok. 54 passed; 0 failed; 0 ignored; 0 measured; 930 filtered out; finished in 0.02s
== MUTANT m4_wraps_beside_us applied:  2 files changed, 65 insertions(+), 13 deletions(-)
test timestamp_ns_coercion::tests::null_beside_a_microsecond_sibling_is_not_wrapped ... FAILED
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 983 filtered out; finished in 0.01s
== restored
test timestamp_ns_coercion::tests::null_beside_a_microsecond_sibling_is_not_wrapped ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 983 filtered out; finished in 0.01s
```
