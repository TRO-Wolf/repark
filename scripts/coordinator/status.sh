#!/usr/bin/env bash
set -uo pipefail
. "$(dirname "$(readlink -f "$0")")/env.sh"
RUN="$1"; UNIT="$2"; MODE="${3:-full}"
STATE=$RUN/state-$UNIT.md
LANES=$(grep -m1 '^LANES:' $STATE 2>/dev/null | cut -d: -f2-)
echo "## world status for $UNIT"
[ "$MODE" = full ] && echo "time: $(date '+%F %H:%M %Z')"
for L in $LANES; do
  echo "### lane $L"
  echo "active worker units: $(systemctl --user list-units --no-legend --state=active "*-$L-*" 2>/dev/null | awk '{print $1}' | tr '\n' ' ')"
  for T in devin muse grok; do
    D=$(ls -d $SCRATCH/$T-worker/$L/*/ 2>/dev/null | tail -1)
    [ -n "$D" ] || continue
    echo "latest $T round: $D exit=$(cat $D/exit 2>/dev/null || echo running) handback=$([ -f $D/handback.json ] && echo yes || echo no)"
  done
  D=$(ls -d $ROOT/$L/20[0-9]*T[0-9]*Z/ 2>/dev/null | tail -1)
  [ -n "$D" ] && echo "latest glmflash round: $D exit=$(cat $D/exit 2>/dev/null || echo running) handback=$([ -f $D/handback.json ] && echo yes || echo no)"
  D=$(ls -d $ROOT/codex-worker/$L/*/ 2>/dev/null | tail -1)
  [ -n "$D" ] && echo "latest codex round: $D exit=$(cat $D/exit 2>/dev/null || echo running) handback=$([ -f $D/handback.json ] && echo yes || echo no)"
  if [ -d $SCRATCH/$L/.git ] && [ "$MODE" = full ]; then
    echo "clone: branch=$(git -C $SCRATCH/$L branch --show-current) head=$(git -C $SCRATCH/$L rev-parse --short HEAD) dirty=$(git -C $SCRATCH/$L status --porcelain | grep -vc handback.json)"
  fi
  G=$ROOT/$L-localgate
  [ -f $G.done ] && echo "local gate finished: $(cat $G.done) (logs $G*.log)"
  [ -f $G.log ] && [ ! -f $G.done ] && echo "local gate running or never finished: $G.log$([ "$MODE" = full ] && echo " last='$(tail -1 $G.log | cut -c1-100)'")"
done
PRS=$(grep -m1 '^PRS:' $STATE 2>/dev/null | cut -d: -f2-)
C=$RUN/.prcache-$UNIT
if [ -n "$PRS" ]; then
  if [ ! -f $C ] || [ $(( $(date +%s) - $(stat -c %Y $C) )) -ge 300 ]; then
    : > $C.tmp
    for P in $PRS; do
      R=${P%%#*}; N=${P##*#}
      gh pr view $N -R $GH_OWNER/$R --json state,mergeable,statusCheckRollup --jq "\"$P state=\(.state) mergeable=\(.mergeable) checks: \([.statusCheckRollup[]?|(.conclusion // .status)]|group_by(.)|map(\"\(.[0])=\(length)\")|join(\" \"))\"" >> $C.tmp 2>/dev/null || { grep -m1 -F "$P " $C 2>/dev/null || echo "$P (gh failed)"; } >> $C.tmp
    done
    mv $C.tmp $C
  fi
  echo "### pull requests"; cat $C
fi
[ "$MODE" = full ] && { echo "### merge queue"; cat $MERGE_QUEUE 2>/dev/null | tail -8; }
echo "### claims lines addressed to you or to everyone: $(grep -cE "^[0-9: -]+ ORCHESTRATING SESSION|^[0-9: -]+ ASK $UNIT" $RUN/claims.txt 2>/dev/null)"
