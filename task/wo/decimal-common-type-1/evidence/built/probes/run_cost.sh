#!/usr/bin/env bash
set -u
C=$S/ddu-work/cost-dct
EV=$S/lane-double-decimal-round-1/task/wo/decimal-double-union-coercion-1/evidence
P1=$EV/fix/probes
P2=$EV/part2/probes
PY=$S/lane-nan-write-canonical-1/.venv/bin/python
B=${LOCKS:?}
DATA=$S/ddu-work/cost-data
CUDATA=$S/ddu-work/cost-data-cu
DCTDATA=$S/ddu-work/cost-data-dct
MAIN=$S/ddu-work/pkg-cu-88999454
HEAD=${HEAD:?}
RUNS=${RUNS:-5}
FIRST=${FIRST:-1}
GROUPS_=${GROUPS_:-"held cu p2 dct c012"}
HELD_CASES=${HELD_CASES:-"plan_free200 plan_wide_dd plan_union200 plan_values10k"}
CU_CASES=${CU_CASES:-"plan_pred200"}
P2_CASES=${P2_CASES:-"plan_values10k_none plan_values10k_dbl ins_values10k_dec ins_values10k_dbl merge_values1000"}
DCT_CASES=${DCT_CASES:-"k1_pred200 k1_union32 k4_case200 k5_dml k3_prune k2_pred200_wide k2_union32_wide r_union5 r_values10k_mixed k2_exec"}
pkg() { case $1 in head) echo $HEAD;; *) echo $MAIN;; esac; }
LOCK="flock -x $B/measure.lock flock $B/link.lock flock $B/opus-cargo.lock flock $B/opus2-cargo.lock nice -n 10 taskset -c 32-47"
sample() {
  local group=$1 build=$2 case=$3 run=$4
  local wh=$S/ddu-work/dct-wh/cost-$build-$case-$run; rm -rf $wh; mkdir -p $wh
  local start=$(date +%s.%N) rc
  case $group in
    held) $LOCK env PYTHONPATH=$(pkg $build) prlimit --as=51539607552 $PY $P1/cost_probe.py $case $build $DATA $wh $C/raw-held.jsonl > /dev/null 2>> $C/err.log; rc=$?;;
    cu) $LOCK env PYTHONPATH=$(pkg $build) prlimit --as=51539607552 $PY $S/ddu-work/cost-cu/cu_cost.py $case $build $CUDATA $wh $C/raw-cu.jsonl > /dev/null 2>> $C/err.log; rc=$?;;
    p2) $LOCK env PYTHONPATH=$(pkg $build) prlimit --as=51539607552 $PY $P2/cost_p2.py $case $build $DATA $wh $C/raw-p2.jsonl > /dev/null 2>> $C/err.log; rc=$?;;
    dct) $LOCK env PYTHONPATH=$(pkg $build) prlimit --as=51539607552 $PY $C/dct_cost.py $case $build $DCTDATA $wh $C/raw-dct.jsonl > /dev/null 2>> $C/err.log; rc=$?;;
    c012)
      local p=$(pkg $build) out=$C/c012/$build-$run-$case WANT
      if [ $case = c012p6 ]; then WANT=r5p6; else WANT=cs2,cs2join,cs2x,r1,r1b,r1dupt,r1j3,r2,r2trunc,r3,r3lam,r3lam2,r3lam3,r3lam4,r4p4,r4p5,r5p6t,r5p7,r5p8,r5p9,r5p10,r5min,oracle; fi
      rm -rf $out; mkdir -p $out
      $LOCK env PYTHONPATH=$p bash -c "cd ${C012:?} && $PY replay_work.py $build $out '' $WANT > $out.log 2>&1"; rc=$?
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
    p2) echo $P2_CASES;;
    dct) echo $DCT_CASES;;
    c012) echo "c012p6 c012rest";;
  esac
}
mkdir -p $C/c012 $S/ddu-work/dct-wh
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
