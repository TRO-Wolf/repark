#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$(readlink -f "$0")")/env.sh"
LANE="$1"; shift
systemd-run --user --slice=repark.slice --collect --quiet --unit="gate-$LANE-$(date -u +%H%M%S)" \
  --setenv=HOME=$HOME --setenv=PATH="$PATH" --setenv=USER=$USER -p LimitNOFILE=65536 -p WorkingDirectory=$SCRATCH \
  -- $LIB/build-slot.sh $LIB/local-gate.sh "$LANE" "$@"
echo "gate queued for $LANE at $(date +%H:%M:%S); result lands in $ROOT/$LANE-localgate.done"
