#!/usr/bin/env bash
set -uo pipefail
WD="$1"; PROMPT="$2"; OUT="$3"
BIN=$(command -v codex || echo "$HOME/.local/bin/codex")
timeout ${COORDINATOR_TICK_TIMEOUT:-2700} "$BIN" exec --json -o "$OUT/last.txt" -m "${COORDINATOR_MODEL:-gpt-5.6-sol}" -s workspace-write -C "$WD" \
  -c "model_reasoning_effort=\"${COORDINATOR_EFFORT:-high}\"" -c 'approval_policy="never"' - < "$PROMPT" > "$OUT/events.jsonl" 2> "$OUT/stderr.log"
rc=$?
python3 - "$OUT" <<'PY'
import json,sys,pathlib
o=pathlib.Path(sys.argv[1]); i=t=tools=0
for l in o.joinpath("events.jsonl").read_text(errors="replace").splitlines():
    try: d=json.loads(l)
    except Exception: continue
    k=str(d.get("type","")); u=d.get("usage") or {}
    i+=u.get("input_tokens",0); t+=u.get("output_tokens",0)
    if k=="item.completed" and (d.get("item") or {}).get("type") in ("command_execution","file_change"): tools+=1
o.joinpath("meter.txt").write_text(f"tools={tools} input_tokens={i} output_tokens={t}\n")
PY
exit $rc
