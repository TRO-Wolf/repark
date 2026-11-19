#!/usr/bin/env bash
set -u
S=${S:?}
C=$S/ddu-work/cost-sum
PY=$S/lane-nan-write-canonical-1/.venv/bin/python
B=${B:?}
DATA=$S/ddu-work/cost-data-sum
MAIN=$S/ddu-work/pkg-avg-27d6149f
HEAD=${HEAD:?}
RUNS=${RUNS:-5}
FIRST=${FIRST:-1}
GROUPS_=${GROUPS_:-"sum c012"}
SUM_CASES=${SUM_CASES:-"k1_sum_d10_on k1_sum_d10_off k1_sum_d38_on k1_sum_d38_off k2_rows3_d10 k2_rows3_d38 k2_running_d10 k2_running_d38 k2_rows1000_d10 k2_rows1000_d38 k3_sum_d10 k3_sum_d36 k3_sum_d38 k4_sum_b_grouped k4_sum_x_grouped k4_sum_b k4_sum_x k4_sum_linear k5_try_sum_d10_grouped"}
pkg() { case $1 in head) echo $HEAD;; *) echo $MAIN;; esac; }
LOCK="flock -x $B/measure.lock flock $B/link.lock flock $B/opus-cargo.lock flock $B/opus2-cargo.lock nice -n 10 taskset -c 48-57"
sample() {
  local group=$1 build=$2 case=$3 run=$4
  local start=$(date +%s.%N) rc
  case $group in
    sum) $LOCK env PYTHONPATH=$(pkg $build) prlimit --as=51539607552 $PY $C/sum_cost.py $case $build $DATA $C/raw-sum.jsonl > /dev/null 2>> $C/err.log; rc=$?;;
    c012)
      local p=$(pkg $build) out=$C/c012/$build-$run-$case WANT
      if [ $case = c012p6 ]; then WANT=r5p6; else WANT=cs2,cs2join,cs2x,r1,r1b,r1dupt,r1j3,r2,r2trunc,r3,r3lam,r3lam2,r3lam3,r3lam4,r4p4,r4p5,r5p6t,r5p7,r5p8,r5p9,r5p10,r5min,oracle; fi
      rm -rf $out; mkdir -p $out
      $LOCK env PYTHONPATH=$p bash -c "cd ${REPLAY:?} && $PY replay_work.py $build $out '' $WANT > $out.log 2>&1"; rc=$?
      rm -rf $out/wh;;
  esac
  local wall=$(python3 -c "print(round($(date +%s.%N)-$start,2))")
  printf '{"group":"%s","build":"%s","case":"%s","run":%s,"rc":%s,"wall":%s}\n' "$group" "$build" "$case" "$run" "$rc" "$wall" >> $C/samples.jsonl
}
cases() {
  case $1 in
    sum) echo $SUM_CASES;;
    c012) echo "c012p6 c012rest";;
  esac
}
mkdir -p $C/c012
for run in $(seq $FIRST $RUNS); do
  for group in $GROUPS_; do
    for case in $(cases $group); do
      for build in main head main-aa; do
        sample $group $build $case $run
      done
    done
    echo "$(date -u +%T) run $run $group done" >> $C/progress.txt
  done
done
echo "$(date -u +%T) ALL-DONE" >> $C/progress.txt
