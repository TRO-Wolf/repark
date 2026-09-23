#!/usr/bin/env bash
set -uo pipefail
. "$(dirname "$(readlink -f "$0")")/env.sh"
RUN="$1"; UNIT="$2"; MODE="${3:-full}"
STATE=$RUN/state-$UNIT.md
REPO=${COORDINATOR_REPO:-repark}
LANES=$(grep -m1 '^LANES:' $STATE 2>/dev/null | cut -d: -f2-)
PRS=$(grep -m1 '^PRS:' $STATE 2>/dev/null | cut -d: -f2-)
C=$RUN/.prcache-$UNIT
MK=$RUN/.digest-$UNIT
MC=$RUN/.maincache-$UNIT
CLAIMS=$RUN/claims.txt
CI_JQ='[.statusCheckRollup[]? | {n:(.name // .context // "?"), r:(((.conclusion // "") | if . == "" then null else . end) // .state // (if (.status // "COMPLETED") == "COMPLETED" then "SUCCESS" else "PENDING" end))}] as $x
  | ([$x[] | select(.r | IN("FAILURE","CANCELLED","TIMED_OUT","ACTION_REQUIRED","STARTUP_FAILURE","ERROR")) | .n] | unique) as $red
  | ([$x[] | select(.r | IN("PENDING","EXPECTED","QUEUED","IN_PROGRESS","WAITING","REQUESTED"))] | length) as $p
  | if ($red | length) > 0 then "red:" + ($red | join(",")) elif $p > 0 then "pending:\($p)" else "green" end'

stale() { [ ! -f "$1" ] || [ $(( $(date +%s) - $(stat -c %Y "$1") )) -ge 300 ]; }

refresh_prs() {
  [ -n "$PRS" ] || return 0
  stale $C || return 0
  : > $C.tmp; : > $C.d.tmp
  local P R N J
  for P in $PRS; do
    R=${P%%#*}; N=${P##*#}
    if J=$(gh pr view $N -R $GH_OWNER/$R --json state,mergeable,statusCheckRollup,headRefOid,mergeStateStatus 2>/dev/null) && [ -n "$J" ]; then
      jq -r --arg p "$P" '"\($p) state=\(.state) mergeable=\(.mergeable) checks: \([.statusCheckRollup[]?|(.conclusion // .status)]|group_by(.)|map("\(.[0])=\(length)")|join(" "))"' <<< "$J" >> $C.tmp
      jq -r --arg p "$P" "\"pr \\(\$p): \\(.state) mergeable=\\(.mergeable) ci=\\($CI_JQ) head=\\((.headRefOid // \"-\")[0:7]) behind_main=\\(if .mergeStateStatus == \"BEHIND\" then \"yes\" else \"no\" end)\"" <<< "$J" >> $C.d.tmp
    else
      { grep -m1 -F "$P " $C 2>/dev/null || echo "$P (gh failed)"; } >> $C.tmp
      { grep -m1 -F "pr $P: " $C.d 2>/dev/null || echo "pr $P: (gh failed)"; } >> $C.d.tmp
    fi
  done
  mv $C.tmp $C; mv $C.d.tmp $C.d
}

main_sha() {
  local S
  if stale $MC; then
    S=$(gh api repos/$GH_OWNER/$REPO/commits/main --jq .sha 2>/dev/null)
    if [ -n "$S" ]; then echo "$S" 2>/dev/null > $MC; else [ -f $MC ] && touch $MC; fi
  fi
  [ -s $MC ] && cut -c1-7 $MC || echo "(gh failed)"
}

rounds_ended() {
  local L=$1 T
  for T in devin muse grok opus; do
    [ -d $SCRATCH/$T-worker/$L ] && find $SCRATCH/$T-worker/$L -mindepth 2 -maxdepth 2 -name exit -newer $MK -printf "$T %h\n" 2>/dev/null
  done
  [ -d $ROOT/codex-worker/$L ] && find $ROOT/codex-worker/$L -mindepth 2 -maxdepth 2 -name exit -newer $MK -printf "codex %h\n" 2>/dev/null
  [ -d $ROOT/$L ] && find $ROOT/$L -mindepth 2 -maxdepth 2 -path '*/20*T*Z/exit' -newer $MK -printf "glmflash %h\n" 2>/dev/null
}

runs_file() {
  case $1 in
    codex) echo $ROOT/codex-worker/runs.tsv ;;
    glmflash) echo $ROOT/runs.tsv ;;
    *) echo $SCRATCH/$1-worker/runs.tsv ;;
  esac
}

ended_line() {
  local T=$1 D=$2 L=$3 S HB ST CM ROW SRC=
  S=$(basename $D)
  ROW=$(awk -F'\t' -v s="$S" -v l="$L" '{f=0; for(i=1;i<=NF;i++){if($i==s)f++; if($i==l)f++} if(f==2){r=$0}} END{print r}' "$(runs_file $T)" 2>/dev/null)
  if [ -f $D/handback.json ]; then SRC=$D/handback.json; HB=yes
  elif [ -f $D/out.json ] && jq -e '.structuredOutput | type == "object"' $D/out.json >/dev/null 2>&1; then HB=yes
  else HB=no; fi
  case "$ROW" in *hb=yes\(text\)*) HB=text ;; esac
  if [ -n "$SRC" ]; then
    ST=$(jq -r '.status // "-"' $SRC 2>/dev/null || echo -); CM=$(jq -r '.commits | length' $SRC 2>/dev/null || echo -)
  elif [ $HB = yes ]; then
    ST=$(jq -r '.structuredOutput.status // "-"' $D/out.json); CM=$(jq -r '.structuredOutput.commits | length' $D/out.json)
  else
    ST=-; CM=-
  fi
  echo "ended: $T-$L exit=$(cat $D/exit 2>/dev/null) round=$D handback=$HB status=$ST commits=$CM"
  if [ $HB = no ]; then
    if [ -f $D/meter.txt ]; then echo "  meter: $(tr '\n' ' ' < $D/meter.txt | cut -c1-160)"
    elif [ -n "$ROW" ]; then echo "  run: $(tr '\t' ' ' <<< "$ROW" | cut -c1-160)"; fi
  fi
}

since_block() {
  local ML=0 MM= MT=0 L U TS E F M
  if [ -f $MK ]; then
    ML=$(grep -o 'lines=[0-9]*' $MK | cut -d= -f2); MM=$(grep -o 'main=[0-9a-f]*' $MK | cut -d= -f2); MT=$(grep -o 't=[0-9]*' $MK | cut -d= -f2)
    ML=${ML:-0}; MT=${MT:-0}
  fi
  echo "## since your last tick"
  if [ -f $MK ]; then echo "marker: $(date -d @$MT '+%H:%M') claims_lines=$ML"; else echo "marker: none (first digested tick)"; fi
  M=$(main_sha)
  echo "main: $M"
  if [ -z "$MM" ] || [ "$M" = "(gh failed)" ]; then echo "main moved: unknown"
  elif [ "${MM:0:7}" = "$M" ]; then echo "main moved: no"; else echo "main moved: yes (was ${MM:0:7})"; fi
  for L in $LANES; do
    echo "### lane $L"
    if [ -f $MK ]; then
      rounds_ended $L | awk '{n=split($2,a,"/"); print a[n], $0}' | sort | cut -d' ' -f2- | while read -r T D; do ended_line $T ${D%/} $L; done
      for F in $(find $ROOT -maxdepth 1 -name "$L-*.done" -newer $MK 2>/dev/null | sort); do
        echo "done: $(basename $F) $(head -1 $F | cut -c1-120)"
      done
    fi
    for U in $(systemctl --user list-units --no-legend --state=active "*-$L-*" 2>/dev/null | awk '{print $1}'); do
      TS=$(systemctl --user show -p ActiveEnterTimestamp --value $U 2>/dev/null)
      E=$(date -d "$TS" +%s 2>/dev/null || echo 0)
      [ "$E" -gt "$MT" ] && echo "started: $U at $(date -d @$E '+%H:%M')"
    done
  done
  [ -f $C.d ] && cat $C.d
  if [ -f $CLAIMS ]; then
    local TOT PAT
    TOT=$(wc -l < $CLAIMS)
    PAT="ORCHESTRATING SESSION|ASK $UNIT|-> $UNIT|HANDED .* $UNIT|TAKEOVER .* $UNIT|$UNIT"
    if [ ! -f $MK ]; then
      echo "claims for you (no marker; the last 15):"; grep -E "$PAT" $CLAIMS | tail -15
    elif [ "$TOT" -lt "$ML" ]; then
      echo "claims file SHRANK ($ML -> $TOT lines); the last 15 for you:"; grep -E "$PAT" $CLAIMS | tail -15
    else
      echo "claims for you since the marker:"; tail -n +$((ML + 1)) $CLAIMS | grep -E "$PAT"
    fi
    echo "claims lines total: $TOT"
  else
    echo "claims: (no claims file)"
  fi
  echo "disk free: $(df --output=avail -BG $SCRATCH 2>/dev/null | tail -1 | tr -d ' ')  mem avail: $(awk '/^MemAvailable/{printf "%.0fG", $2/1048576}' /proc/meminfo)"
  echo
}

refresh_prs
[ "$MODE" = full ] && since_block
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
  [ "$MODE" = quiet ] && echo "done files: $(cd $ROOT 2>/dev/null && ls -1 $L-*.done 2>/dev/null | tr '\n' ' ')"
done
if [ -n "$PRS" ]; then
  echo "### pull requests"; cat $C
  [ "$MODE" = quiet ] && [ -f $C.d ] && cat $C.d
fi
[ "$MODE" = full ] && { echo "### merge queue"; cat $MERGE_QUEUE 2>/dev/null | tail -8; }
echo "### claims lines addressed to you or to everyone: $(grep -cE "^[0-9: -]+ ORCHESTRATING SESSION|^[0-9: -]+ ASK $UNIT" $RUN/claims.txt 2>/dev/null)"
