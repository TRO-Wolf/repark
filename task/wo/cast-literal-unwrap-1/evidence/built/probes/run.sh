#!/usr/bin/env bash
set -u
S=${S:?set S to the lane scratch directory}
C=$S/ddu-work/cost-cu
P1=$S/lane-double-decimal-round-1/task/wo/decimal-double-union-coercion-1/evidence/fix/probes
PY=$S/lane-nan-write-canonical-1/.venv/bin/python
B=${BUILD_SLOTS:?set BUILD_SLOTS to the lock directory}
DATA=$S/ddu-work/cost-data
CUDATA=$S/ddu-work/cost-data-cu
MAIN=$S/ddu-work/pkg-p2-12101e9b
HEAD=${HEAD:?}
RUNS=${RUNS:-5}
FIRST=${FIRST:-1}
GROUPS_=${GROUPS_:-"held cu c012"}
CU_CASES=${CU_CASES:-"plan_pred200 c2 in40 c4 c3_cow c3_mor del_cow del_mor"}
HELD_CASES=${HELD_CASES:-"plan_free200 plan_wide_dd plan_union200 plan_values10k"}
pkg() { case $1 in head) echo $HEAD;; *) echo $MAIN;; esac; }
LOCK="flock -x $B/measure.lock flock $B/link.lock flock $B/opus-cargo.lock flock $B/opus2-cargo.lock nice -n 10 taskset -c 32-47"
sample() {
  local group=$1 build=$2 case=$3 run=$4
  local wh=$S/ddu-work/cu-wh/cost-$build-$case-$run; rm -rf $wh; mkdir -p $wh
  local start=$(date +%s.%N) rc
  case $group in
    held) $LOCK env PYTHONPATH=$(pkg $build) prlimit --as=51539607552 $PY $P1/cost_probe.py $case $build $DATA $wh $C/raw-held.jsonl > /dev/null 2>> $C/err.log; rc=$?;;
    cu) $LOCK env PYTHONPATH=$(pkg $build) prlimit --as=51539607552 $PY $C/cu_cost.py $case $build $CUDATA $wh $C/raw-cu.jsonl > /dev/null 2>> $C/err.log; rc=$?;;
    c012)
      local p=$(pkg $build) out=$C/c012/$build-$run-$case WANT
      if [ $case = c012p6 ]; then WANT=r5p6; else WANT=cs2,cs2join,cs2x,r1,r1b,r1dupt,r1j3,r2,r2trunc,r3,r3lam,r3lam2,r3lam3,r3lam4,r4p4,r4p5,r5p6t,r5p7,r5p8,r5p9,r5p10,r5min,oracle; fi
      rm -rf $out; mkdir -p $out
      $LOCK env PYTHONPATH=$p bash -c "cd $C012_DIR && $PY replay_work.py $build $out '' $WANT > $out.log 2>&1"; rc=$?
      rm -rf $out/wh;;
  esac
  local wall=$(python3 -c "print(round($(date +%s.%N)-$start,2))")
  printf '{"group":"%s","build":"%s","case":"%s","run":%s,"rc":%s,"wall":%s}\n' "$group" "$build" "$case" "$run" "$rc" "$wall" >> $C/samples.jsonl
  rm -rf $wh
}
cases() {
  case $1 in
    held) echo $HELD_CASES;;
    cu) echo $CU_CASES;;
    c012) echo "c012p6 c012rest";;
  esac
}
for run in $(seq $FIRST $RUNS); do
  for group in $GROUPS_; do
    for case in $(cases $group); do
      for build in main head main-aa; do
        sample $group $build $case $run
      done
    done
    echo "$(date +%T) run $run $group done" >> $C/progress.txt
  done
done
echo "$(date +%T) ALL-DONE" >> $C/progress.txt
