#!/usr/bin/env bash
set -u
label=$1
pkg=$2
B=$(cd "$(dirname "$0")" && pwd)
PY=${PY:?set PY to the probe interpreter}
JAR=${JAR:?set JAR to the iceberg-spark-runtime jar}
out=$B/out-$label
wh=$B/wh-$label
rm -rf "$out" "$wh"
mkdir -p "$out" "$wh"
run() {
  flock -s $BUILD_SLOTS/measure.lock nice -n 10 taskset -c 32-47 prlimit --as=51539607552 env PYTHONPATH="$pkg" "$PY" "$@"
}
for ansi in false true; do
  mkdir -p "$wh/cells-$ansi" "$wh/shapes-$ansi" "$wh/critic-reads-$ansi" "$wh/critic-dml-$ansi" "$wh/critic-shapes-$ansi"
  (cd "$B/probes" && run run_cells.py repark $ansi "$wh/cells-$ansi" $JAR "$out/repark-$ansi.jsonl") > "$out/log-cells-$ansi.txt" 2>&1
  (cd "$B/probes" && run run_shapes.py repark $ansi "$wh/shapes-$ansi" "$out/shapes-repark-$ansi.jsonl") > "$out/log-shapes-$ansi.txt" 2>&1
  for p in reads dml shapes; do
    (cd "$B/critic" && run $p.py repark $ansi "$wh/critic-$p-$ansi" $JAR "$out/critic-$p-repark-$ansi.jsonl") > "$out/log-critic-$p-$ansi.txt" 2>&1
  done
done
(cd "$B/probes" && run run_native.py "$out/native.jsonl" "$wh/cells-false") > "$out/log-native.txt" 2>&1
(cd "$B/critic" && run native.py "$out/critic-native.jsonl") > "$out/log-critic-native.txt" 2>&1
echo "$label done"
