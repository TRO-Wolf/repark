# mb-fence-body-flake-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-core/src/microbatch/fence_tests.rs::a_winner_that_commits_during_the_batch_load_never_lets_a_fenced_body_run
C-002: crates/repark-core/src/microbatch/fence_tests.rs::two_drivers_on_one_sink_land_each_epoch_exactly_once
C-003: crates/repark-core/src/microbatch/fence_tests.rs::a_sink_replaced_between_batches_takes_the_next_batch_under_its_own_scope
C-004: crates/repark-core/src/microbatch/reload_tests.rs::the_driver_loads_the_sink_fresh_for_every_batch
C-005: none (records)
C-006: none (records)
C-007: crates/repark-core/src/microbatch/lifecycle_tests.rs::two_sessions_on_one_sink_wait_for_the_scope
