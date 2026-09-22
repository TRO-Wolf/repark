#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$(readlink -f "$0")")/env.sh"
RUN="$1"; UNIT="$2"; ENGINE="$3"; UNTIL="${4:-2359}"
[ -f $RUN/list-$UNIT.md ] || { echo "REFUSED: $RUN/list-$UNIT.md missing"; exit 1; }
[ -x $HERE/engine-$ENGINE.sh ] || { echo "REFUSED: unknown engine $ENGINE ($(ls $HERE | sed -n 's/^engine-\(.*\)\.sh$/\1/p' | tr '\n' '|'))"; exit 1; }
[ -d $LIB ] || { echo "REFUSED: $LIB is not a directory (set COORDINATOR_ROOT or COORDINATOR_LIB)"; exit 1; }
systemctl --user show -p MemoryMax repark.slice | grep -qv infinity || { echo "REFUSED: repark.slice has no memory cap"; exit 1; }
FORWARD=(); for V in $(compgen -A variable COORDINATOR_); do FORWARD+=("--setenv=$V=${!V}"); done
systemd-run --user --slice=repark.slice --unit="$UNIT" --working-directory=$SCRATCH "${FORWARD[@]}" \
  --setenv=HOME=$HOME --setenv=PATH="$PATH" --setenv=USER=$USER --setenv=CARGO_BUILD_JOBS=6 --setenv=RUST_TEST_THREADS=6 \
  -p LimitNOFILE=65536 -p Restart=always -p RestartSec=30 -p RestartPreventExitStatus=0 \
  -p StartLimitIntervalSec=7200 -p StartLimitBurst=4 \
  -p StandardOutput=append:$SCRATCH/$UNIT.log -p StandardError=append:$SCRATCH/$UNIT.err \
  $HERE/drive.sh "$RUN" "$UNIT" "$ENGINE" "$UNTIL"
