#!/usr/bin/env bash
set -u
S=${S:?}
C=$S/ddu-work/cost-ddl
PY=$S/lane-nan-write-canonical-1/.venv/bin/python
B=${BUILD_SLOTS:?}
DATA=$S/ddu-work/cost-data-ddl
MAIN=$S/ddu-work/pkg-avg-27d6149f
HEAD=${HEAD:?}
RUNS=${RUNS:-5}
FIRST=${FIRST:-1}
GROUPS_=${GROUPS_:-"ddl c012"}
DDL_CASES=${DDL_CASES:-"s1_sql_d10_div3 s1_sql_d38_div3 s2_df_d10_div3 s2_df_d10_mod3 s2_df_d10_pmod s2_df_d10_expr_mod s3_sql_int_div_int s3_sql_double_div_double s3_df_int_div_int s3_df_double_div_double"}
pkg() { case $1 in head) echo $HEAD;; *) echo $MAIN;; esac; }
LOCK="flock -x $B/measure.lock flock $B/link.lock flock $B/opus-cargo.lock flock $B/opus2-cargo.lock nice -n 10 taskset -c 48-57"
sample() {
  local group=$1 build=$2 case=$3 run=$4
  local start=$(date +%s.%N) rc
  case $group in
    ddl) $LOCK env PYTHONPATH=$(pkg $build) prlimit --as=51539607552 $PY $C/ddl_cost.py $case $build $DATA $C/raw-ddl.jsonl > /dev/null 2>> $C/err.log; rc=$?;;
    c012)
      local p=$(pkg $build) out=$C/c012/$build-$run-$case WANT
      if [ $case = c012p6 ]; then WANT=r5p6; else WANT=cs2,cs2join,cs2x,r1,r1b,r1dupt,r1j3,r2,r2trunc,r3,r3lam,r3lam2,r3lam3,r3lam4,r4p4,r4p5,r5p6t,r5p7,r5p8,r5p9,r5p10,r5min,oracle; fi
      rm -rf $out; mkdir -p $out
      $LOCK env PYTHONPATH=$p bash -c "cd ${C012_REPLAY_DIR:?} && $PY replay_work.py $build $out '' $WANT > $out.log 2>&1"; rc=$?
      rm -rf $out/wh;;
  esac
  local wall=$(python3 -c "print(round($(date +%s.%N)-$start,2))")
  printf '{"group":"%s","build":"%s","case":"%s","run":%s,"rc":%s,"wall":%s}\n' "$group" "$build" "$case" "$run" "$rc" "$wall" >> $C/samples.jsonl
}
cases() {
  case $1 in
    ddl) echo $DDL_CASES;;
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
