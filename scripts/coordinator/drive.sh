#!/usr/bin/env bash
set -uo pipefail
. "$(dirname "$(readlink -f "$0")")/env.sh"
RUN="$1"; UNIT="$2"; ENGINE="$3"; UNTIL="${4:-2359}"
STATE=$RUN/state-$UNIT.md; LOG=$RUN/coordinator-$UNIT.log; WD=$SCRATCH/co-$UNIT
mkdir -p $RUN/ticks-$UNIT $WD; [ -d $WD/.git ] || git -C $WD init -q
[ -f $STATE ] || printf 'STATUS: NEW\nLANES:\nPRS:\n\n(no state yet — this is the first tick)\n' > $STATE
say() { echo "$(date '+%F %T') $*" >> $LOG; }
past_until() { [ -s "$RUN/until-$UNIT" ] && UNTIL=$(tr -dc 0-9 < "$RUN/until-$UNIT"); if [ ${#UNTIL} -gt 4 ]; then [ "$(date +%s)" -ge "$UNTIL" ]; else [ "$(date +%H%M)" -ge "$UNTIL" ]; fi; }
fingerprint() { $HERE/status.sh $RUN $UNIT quiet 2>/dev/null | md5sum | cut -d' ' -f1; }
handbook() { sed -e "s#{{HERE}}#$HERE#g" -e "s#{{LIB}}#$LIB#g" -e "s#{{ROOT}}#$ROOT#g" -e "s#{{SCRATCH}}#$SCRATCH#g" -e "s#{{MERGE_QUEUE}}#$MERGE_QUEUE#g" "$1"; }
say "driver start engine=$ENGINE until=$UNTIL pid=$$"
FAILS=0
while :; do
  ST=$(grep -m1 '^STATUS:' $STATE | awk '{print $2}')
  [ "$ST" = DONE ] && { say "state is DONE — driver exits"; exit 0; }
  LATE=0; past_until && LATE=1
  N=$(( $(ls $RUN/ticks-$UNIT | wc -l) + 1 )); T=$RUN/ticks-$UNIT/$(printf '%03d' $N); mkdir -p $T
  {
    handbook $HERE/handbook.md; echo
    [ -f $HERE/addendum-$ENGINE.md ] && { echo "# ADDENDUM FOR THIS ENGINE (from earlier runs' evidence)"; cat $HERE/addendum-$ENGINE.md; echo; }
    echo "# YOUR UNIT LIST"; cat $RUN/list-$UNIT.md; echo
    echo "# YOUR STATE FILE ($STATE) — you wrote this at the end of your last tick"; cat $STATE; echo
    $HERE/status.sh $RUN $UNIT full; echo
    echo "# claims file tail ($RUN/claims.txt)"; tail -c 2500 $RUN/claims.txt 2>/dev/null; echo
    echo "# THIS TICK"
    echo "Tick $N of unit $UNIT. Your coordinator scratch directory is $WD. Tick output directory: $T."
    [ $LATE = 1 ] && echo "THE RUN'S FINISH TIME HAS PASSED: launch nothing new; write $RUN/report-$UNIT.md from your state and the evidence, then set STATUS: DONE."
    echo "Do the next actions now, then REWRITE $STATE in full (first line STATUS: WORKING, WAITING or DONE; then LANES:, PRS:, then your notes) and end the tick. Never wait inside a tick for a worker, a build gate or CI — the driver waits for you at no cost and wakes you when the world changes."
  } > $T/prompt.md
  CL0=$(grep -cE "^[0-9: -]+ ORCHESTRATING SESSION|^[0-9: -]+ ASK $UNIT" $RUN/claims.txt 2>/dev/null)
  say "tick $N start (state=$ST late=$LATE)"
  S0=$(date +%s); $HERE/engine-$ENGINE.sh $WD $T/prompt.md $T; RC=$?
  say "tick $N end rc=$RC secs=$(( $(date +%s) - S0 )) $(cat $T/meter.txt 2>/dev/null | tr '\n' ' ')"
  if [ $RC -ne 0 ]; then FAILS=$((FAILS+1)); [ $FAILS -ge 3 ] && { say "three failed ticks in a row — driver exits 1"; exit 1; }; sleep 60; continue; fi
  FAILS=0
  ST=$(grep -m1 '^STATUS:' $STATE | awk '{print $2}')
  [ "$ST" = DONE ] && continue
  [ "$ST" = WORKING ] && { sleep 5; continue; }
  CL1=$(grep -cE "^[0-9: -]+ ORCHESTRATING SESSION|^[0-9: -]+ ASK $UNIT" $RUN/claims.txt 2>/dev/null)
  [ "$CL1" != "$CL0" ] && { say "wake: claims changed during the tick"; continue; }
  FP=$(fingerprint); W0=$(date +%s); IDLE=${COORDINATOR_MAX_IDLE:-1500}
  INFLIGHT=$($HERE/status.sh $RUN $UNIT quiet 2>/dev/null | grep -cE "active worker units: [a-z]|exit=running|local gate running|state=OPEN")
  if [ "$INFLIGHT" = 0 ]; then QUIET=$(( ${QUIET:-60} * 2 )); [ $QUIET -gt $IDLE ] && QUIET=$IDLE; IDLE=$QUIET; say "nothing in flight — next look in ${IDLE}s"; else QUIET=60; fi
  while :; do
    sleep 30
    [ "$(fingerprint)" != "$FP" ] && { say "wake: world changed after $(( $(date +%s) - W0 ))s"; break; }
    [ $(( $(date +%s) - W0 )) -ge $IDLE ] && { say "wake: idle limit ${IDLE}s"; break; }
    past_until && [ $LATE = 0 ] && { say "wake: finish time"; break; }
  done
  GAP=${COORDINATOR_MIN_GAP:-0}; SINCE=$(( $(date +%s) - W0 ))
  [ $SINCE -lt $GAP ] && { say "batching events: holding $(( GAP - SINCE ))s (min gap ${GAP}s)"; sleep $(( GAP - SINCE )); }
done
