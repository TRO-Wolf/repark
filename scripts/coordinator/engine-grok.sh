#!/usr/bin/env bash
set -uo pipefail
WD="$1"; PROMPT="$2"; OUT="$3"
timeout ${COORDINATOR_TICK_TIMEOUT:-2700} grok --cwd "$WD" --model "${COORDINATOR_MODEL:-grok-4.7}" --reasoning-effort "${COORDINATOR_EFFORT:-xhigh}" \
  --prompt-file "$PROMPT" --verbatim --no-auto-update --always-approve --max-turns "${COORDINATOR_TURNS:-80}" \
  --no-subagents --disable-web-search --output-format json \
  --deny 'Bash(aws *)' --deny 'Bash(git * --no-verify*)' > "$OUT/out.json" 2> "$OUT/stderr.log"
rc=$?
python3 - "$OUT" <<'PY'
import json,sys,pathlib
o=pathlib.Path(sys.argv[1])
try: d=json.loads(o.joinpath("out.json").read_text() or "{}")
except Exception: d={}
o.joinpath("meter.txt").write_text(f"turns={d.get('num_turns','')} cost_usd={d.get('total_cost_usd',0) or 0:.4f} stop={d.get('stopReason','')}\n")
PY
exit $rc
