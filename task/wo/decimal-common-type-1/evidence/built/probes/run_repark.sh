#!/usr/bin/env bash
set -u
D=${S:?}/ddu-work/bar-dct
PY=$1
PP=$2
OUT=$3
JAR=${JAR:?}
mkdir -p $OUT $D/wh
cd $D/probes
run() {
  flock -s ${LOCKS:?}/measure.lock nice -n 10 taskset -c 32-47 prlimit --as=51539607552 env PYTHONPATH=$PP $PY "$@"
}
for ansi in false true; do
  run run_cells.py repark $ansi $D/wh $JAR $OUT/repark-$ansi.jsonl > $OUT/cells-$ansi.log 2>&1
  run run_side.py repark $ansi $D/wh $JAR $OUT/side-repark-$ansi.jsonl > $OUT/side-$ansi.log 2>&1
  run run_revision.py repark $ansi $D/wh $JAR $OUT/revision-repark-$ansi.jsonl > $OUT/revision-$ansi.log 2>&1
done
run run_native.py $OUT/native.jsonl > $OUT/native.log 2>&1
rm -rf $D/wh/wh-*
echo RUN-DONE > $OUT/done
