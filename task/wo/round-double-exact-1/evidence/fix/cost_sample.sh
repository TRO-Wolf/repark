#!/usr/bin/env bash
set -u
S=${S:?set S to the lane scratch directory}
W=$S/round-exact-work
PY=$S/lane-round-exact-1/.venv/bin/python
build=$1; case=$2; run=$3
pkg=$build
[ "$build" = "base-aa" ] && pkg=base
[ "$build" = "head" ] && pkg=head2
load0=$(cut -d' ' -f1 /proc/loadavg)
waited=0
if awk -v l="$load0" 'BEGIN{exit !(l>4)}'; then sleep 60; waited=1; fi
load1=$(cut -d' ' -f1 /proc/loadavg)
export PYTHONPATH=$W/pkg-$pkg
wh=$W/cost-wh/$build-$case-$run; rm -rf $wh; mkdir -p $wh
cd $wh
start=$(date +%s.%N)
case $case in
  c012p6|c012rest)
    if [ $case = c012p6 ]; then WANT=r5p6; else WANT=cs2,cs2join,cs2x,r1,r1b,r1dupt,r1j3,r2,r2trunc,r3,r3lam,r3lam2,r3lam3,r3lam4,r4p4,r4p5,r5p6t,r5p7,r5p8,r5p9,r5p10,r5min,oracle; fi
    out=$W/cost/c012/$build-$run-${case#c012}; rm -rf $out; mkdir -p $out
    (cd "${C012_DIR:?set C012_DIR to the C-012 replay directory}" && $PY replay_work.py $build $out "" $WANT > $out.log 2>&1)
    rc=$?
    rm -rf $out/wh
    ;;
  round)
    $PY $W/fix/cost.py $build $W/cost/round-raw.jsonl $wh $run > $W/cost/logs/$build-$case-$run.log 2>&1
    rc=$?
    ;;
esac
wall=$(python3 -c "print(round($(date +%s.%N)-$start,2))")
printf '{"build":"%s","case":"%s","run":%s,"rc":%s,"wall":%s,"load_before":%s,"load_at_sample":%s,"waited":%s,"load_after":%s}\n' "$build" "$case" "$run" "$rc" "$wall" "$load0" "$load1" "$waited" "$(cut -d' ' -f1 /proc/loadavg)" >> $W/cost/samples.jsonl
rm -rf $wh
