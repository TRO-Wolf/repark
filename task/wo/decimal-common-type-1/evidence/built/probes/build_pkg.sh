#!/usr/bin/env bash
set -u
C=$S/lane-double-decimal-round-1
SHA=$(git -C $C rev-parse "$1")
N=dct-${SHA:0:8}
B=${LOCKS:?}
L=$S/ddu-work/cost-dct/build-$N.log
rm -rf $S/ddu-work/pkg-$N $S/ddu-work/wheels-$N $S/ddu-work/wt-$N
git -C $C worktree add --detach $S/ddu-work/wt-$N $SHA > $L 2>&1 || exit 1
test -z "$(git -C $S/ddu-work/wt-$N status --porcelain)" || { echo DIRTY >> $L; exit 1; }
flock -s $B/measure.lock flock $B/link.lock flock $B/opus-cargo.lock nice -n 10 taskset -c 32-47 env CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=$S/ddu-work/target-head prlimit --as=51539607552 bash -c "cd $S/ddu-work/wt-$N/python/repark && uvx maturin@1.14.1 build --profile release -o $S/ddu-work/wheels-$N -i $C/.venv/bin/python" >> $L 2>&1 || { echo WHEEL-FAIL >> $L; exit 1; }
flock -s $B/measure.lock bash -c "uv pip install --python $C/.venv/bin/python --target $S/ddu-work/pkg-$N --no-deps $S/ddu-work/wheels-$N/*.whl" >> $L 2>&1 || exit 1
echo $SHA > $S/ddu-work/pkg-$N/BUILT_FROM
git -C $C worktree remove --force $S/ddu-work/wt-$N >> $L 2>&1
echo BUILD-DONE $N >> $L
