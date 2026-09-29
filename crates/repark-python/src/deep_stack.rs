use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::LogicalPlan;
use repark_core::column_resolution::on_grown_stack_with;
use std::future::Future;
use std::io;
use tokio::runtime::{Builder, Runtime};

pub(crate) const GROWN_STACK_SEGMENT_BYTES: usize = 128 * 1024 * 1024;
pub(crate) const RUNTIME_THREAD_STACK_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const DEEP_PLAN_INPUT_DEPTH: usize = 16;

pub(crate) fn build_shared_runtime() -> io::Result<Runtime> {
    Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(RUNTIME_THREAD_STACK_BYTES)
        .build()
}

pub(crate) fn block_on<F: Future>(runtime: &Runtime, future: F) -> F::Output {
    runtime.block_on(on_grown_stack_with(
        GROWN_STACK_SEGMENT_BYTES,
        GROWN_STACK_SEGMENT_BYTES,
        future,
    ))
}

pub(crate) fn block_on_grown_if<F: Future>(runtime: &Runtime, future: F, grown: bool) -> F::Output {
    if grown {
        block_on(runtime, future)
    } else {
        runtime.block_on(future)
    }
}

pub(crate) fn frame_needs_grown_stack(frame: &DataFrame) -> bool {
    plan_input_depth(frame.logical_plan()) > DEEP_PLAN_INPUT_DEPTH
}

pub(crate) fn plan_input_depth(plan: &LogicalPlan) -> usize {
    let mut deepest = 0;
    let mut pending = vec![(plan, 1_usize)];
    while let Some((node, depth)) = pending.pop() {
        deepest = deepest.max(depth);
        for input in node.inputs() {
            pending.push((input, depth + 1));
        }
    }
    deepest
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hint::black_box;
    use std::panic::AssertUnwindSafe;

    fn recurse_with_kilobyte_frames(levels: u32) -> u32 {
        fn descend(remaining: u32, tally: u32) -> u32 {
            let pad = [1u8; 1024];
            black_box(pad);
            if remaining == 0 {
                tally
            } else {
                descend(remaining - 1, tally + 1)
            }
        }
        descend(levels, 0)
    }

    fn test_runtime() -> Runtime {
        Runtime::new().expect("a test runtime builds")
    }

    #[test]
    fn block_on_returns_the_future_output() {
        let runtime = test_runtime();
        assert_eq!(block_on(&runtime, async { 40 + 2 }), 42);
    }

    #[test]
    fn block_on_drives_recursion_beyond_default_thread_stacks() {
        let runtime = test_runtime();
        let levels = 4_000;
        assert_eq!(
            block_on(&runtime, async { recurse_with_kilobyte_frames(levels) }),
            levels,
            "twelve megabytes of recursion complete on the grown stack"
        );
    }

    #[test]
    fn block_on_grown_if_drives_deep_recursion_when_grown() {
        let runtime = test_runtime();
        let levels = 4_000;
        assert_eq!(
            block_on_grown_if(
                &runtime,
                async { recurse_with_kilobyte_frames(levels) },
                true
            ),
            levels,
            "twelve megabytes of recursion complete on the grown stack"
        );
    }

    #[test]
    fn block_on_grown_if_returns_output_when_ungrown() {
        let runtime = test_runtime();
        assert_eq!(block_on_grown_if(&runtime, async { 40 + 2 }, false), 42);
    }

    #[test]
    fn frame_needs_grown_stack_splits_at_the_depth_threshold() {
        use datafusion::logical_expr::lit;
        use datafusion::prelude::SessionContext;
        let context = SessionContext::new();
        let shallow = context.read_empty().expect("an empty frame builds");
        assert_eq!(plan_input_depth(shallow.logical_plan()), 1);
        assert!(!frame_needs_grown_stack(&shallow));
        let mut chained = shallow.filter(lit(true)).expect("a first filter builds");
        for _ in 0..DEEP_PLAN_INPUT_DEPTH {
            chained = chained.filter(lit(true)).expect("a chained filter builds");
        }
        assert_eq!(
            plan_input_depth(chained.logical_plan()),
            DEEP_PLAN_INPUT_DEPTH + 2
        );
        assert!(frame_needs_grown_stack(&chained));
    }

    #[test]
    fn block_on_drives_a_borrowing_future() {
        let runtime = test_runtime();
        let borrowed = 21_u32;
        assert_eq!(
            block_on(&runtime, async { borrowed * 2 }),
            42,
            "a future borrowing a caller local drives without an owned rewrite"
        );
    }

    #[test]
    fn block_on_propagates_a_future_panic_to_the_caller() {
        let runtime = test_runtime();
        let caught = std::panic::catch_unwind(AssertUnwindSafe(|| {
            block_on(&runtime, async {
                panic!("deep-stack-probe-panic");
            });
        }));
        let payload = caught.expect_err("a panicking future must unwind through block_on");
        let text = payload
            .downcast_ref::<&str>()
            .expect("the probe payload is a static str");
        assert!(
            text.contains("deep-stack-probe-panic"),
            "the panic payload crosses the grown stack unchanged: {text}"
        );
    }

    #[test]
    fn shared_runtime_blocking_threads_survive_deep_recursion() {
        let runtime = build_shared_runtime().expect("the shared runtime builds");
        let levels = 4_000;
        let joined =
            runtime.block_on(runtime.spawn_blocking(move || recurse_with_kilobyte_frames(levels)));
        assert_eq!(
            joined.expect("the blocking task joins"),
            levels,
            "twelve megabytes of recursion complete on a runtime blocking thread"
        );
    }

    #[test]
    fn shared_runtime_drives_futures() {
        let runtime = build_shared_runtime().expect("the shared runtime builds");
        assert_eq!(runtime.block_on(async { 7 * 6 }), 42);
    }
}
