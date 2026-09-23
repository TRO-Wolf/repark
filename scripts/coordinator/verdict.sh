#!/usr/bin/env bash
set -uo pipefail
. "$(dirname "$(readlink -f "$0")")/env.sh"
[ $# -eq 1 ] || { echo "usage: verdict.sh <lane>" >&2; exit 64; }
L=$1

NEWEST=$( {
  ls -d $ROOT/codex-worker/xr-$L/*/ 2>/dev/null | sed 's|^|codex |'
  ls -d $SCRATCH/grok-worker/xr-$L/*/ 2>/dev/null | sed 's|^|grok |'
} | awk '{n=split($2,a,"/"); print a[n-1], $0}' | sort | tail -1 | cut -d' ' -f2-)

if [ -z "$NEWEST" ]; then
  echo "VERDICT NONE"
  echo "engine=- round=- head=-"
  echo "tools=- turns=- cost=- valid=no: no critic round for xr-$L"
  exit 3
fi
ENGINE=${NEWEST%% *}; D=${NEWEST#* }; D=${D%/}; S=$(basename $D)

if [ ! -f $D/exit ]; then
  echo "VERDICT RUNNING"
  echo "engine=$ENGINE round=$D head=-"
  echo "tools=- turns=- cost=- valid=no: round still running"
  exit 3
fi

TOOLS=-; TURNS=-; COST=-; SUMMARY=; QS=; WHY=
if [ $ENGINE = codex ]; then
  ROW=$(awk -F'\t' -v s="$S" -v l="xr-$L" '$1 == l && $2 == s {r=$0} END {print r}' $ROOT/codex-worker/runs.tsv 2>/dev/null)
  [ -n "$ROW" ] && { TURNS=$(cut -f7 <<< "$ROW"); TOOLS=$(cut -f8 <<< "$ROW"); }
  if [ -f $D/handback.json ] && jq -e 'type == "object"' $D/handback.json >/dev/null 2>&1; then
    SUMMARY=$(jq -r '.summary // ""' $D/handback.json)
    QS=$(jq -r '.questions[]? | "finding \(.id // "?"): \((.question // "") | gsub("\\s+"; " ") | .[0:160])"' $D/handback.json)
  else
    WHY="hand-back missing"
  fi
else
  if [ -f $D/out.json ] && jq -e 'type == "object"' $D/out.json >/dev/null 2>&1; then
    TURNS=$(jq -r '.num_turns // "-"' $D/out.json)
    TOOLS=$(jq -r 'if (.num_tool_calls // .tool_calls // null) != null then (.num_tool_calls // .tool_calls) else ([.modelUsage[]?.modelCalls // 0] | add // "-") end' $D/out.json)
    COST=$(jq -r 'if .total_cost_usd == null then "-" else "$\(.total_cost_usd * 100 | round / 100)" end' $D/out.json)
    if jq -e '.structuredOutput | type == "object"' $D/out.json >/dev/null 2>&1; then
      SUMMARY=$(jq -r '.structuredOutput.summary // ""' $D/out.json)
      QS=$(jq -r '.structuredOutput.questions[]? | "finding \(.id // "?"): \((.question // "") | gsub("\\s+"; " ") | .[0:160])"' $D/out.json)
    else
      WHY="hand-back missing (no structuredOutput)"
    fi
  else
    WHY="hand-back missing (out.json absent or unreadable)"
  fi
fi

WORD=$(awk '{print $1; exit}' <<< "$SUMMARY" | tr -cd 'A-Z_')
HEAD=$(grep -oiE 'head[^0-9a-f]{0,20}[0-9a-f]{7,40}' <<< "$SUMMARY" | head -1 | grep -oE '[0-9a-f]{7,40}$')
[ -n "$HEAD" ] || HEAD=$(grep -oE '\b[0-9a-f]{40}\b' <<< "$SUMMARY" | head -1)
HEAD=${HEAD:--}

if [ -z "$WHY" ] && [ "$WORD" != PASS ] && [ "$WORD" != NEEDS_REMEDIATION ]; then
  WHY="summary lacks a verdict word (starts '$(cut -c1-40 <<< "$SUMMARY" | tr '\n' ' ')')"
fi
if [ -z "$WHY" ] && [[ "$TOOLS" =~ ^[0-9]+$ ]] && [ "$TOOLS" -lt 3 ]; then
  WHY="tools=$TOOLS < 3"
fi

if [ -n "$WHY" ]; then V=VOID; VALID="no: $WHY"; RC=2
elif [ "$WORD" = PASS ]; then V=PASS; VALID=yes; RC=0
else V=NEEDS_REMEDIATION; VALID=yes; RC=1; fi

echo "VERDICT $V"
echo "engine=$ENGINE round=$D head=$HEAD"
echo "tools=$TOOLS turns=$TURNS cost=$COST valid=$VALID"
[ -n "$QS" ] && echo "$QS"
exit $RC
