#!/usr/bin/env bash
set -u
S=${S:?set S to the lane scratch directory}
W=$S/round-exact-work
B=${BUILD_SLOTS:?set BUILD_SLOTS to the lock directory}
RUNS=${RUNS:-5}
CASES=${CASES:-"round c012p6 c012rest"}
BUILDS=(base head base-aa)
for run in $(seq 1 $RUNS); do
  shift_by=$(( (run - 1) % ${#BUILDS[@]} ))
  order=("${BUILDS[@]:$shift_by}" "${BUILDS[@]:0:$shift_by}")
  echo "$(date +%T) run $run order ${order[*]}" >> $W/cost/progress.txt
  for case in $CASES; do
    for build in "${order[@]}"; do
      flock -x $B/measure.lock flock $B/link.lock flock $B/opus-cargo.lock flock $B/opus2-cargo.lock nice -n 10 taskset -c 32-47 prlimit --as=51539607552 bash $W/cost/sample.sh $build $case $run
      echo "$(date +%T) run $run $case $build done" >> $W/cost/progress.txt
    done
  done
done
echo "$(date +%T) ALL-DONE" >> $W/cost/progress.txt
