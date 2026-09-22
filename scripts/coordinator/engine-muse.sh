#!/usr/bin/env bash
set -uo pipefail
WD="$1"; PROMPT="$2"; OUT="$3"
BIN=$(command -v muse || echo "$HOME/.local/bin/muse")
timeout ${COORDINATOR_TICK_TIMEOUT:-2700} "$BIN" exec --json --prompt-file "$PROMPT" --model "${COORDINATOR_MODEL:-muse-spark-1.3-contributor}" \
  --reasoning-effort "${COORDINATOR_EFFORT:-max}" --max-model-steps "${COORDINATOR_TURNS:-120}" --approval-mode never \
  --workspace "$WD" --trust-workspace --no-foreign-personal-context --disable-web-tools --user-input-auto-resolve \
  --disable-sandbox > "$OUT/out.jsonl" 2> "$OUT/stderr.log"
rc=$?
python3 - "$OUT" <<'PY'
import json,sys,pathlib
o=pathlib.Path(sys.argv[1]); tools=0; term=""
for l in o.joinpath("out.jsonl").read_text(errors="replace").splitlines():
    try: d=json.loads(l)
    except Exception: continue
    t=str(d.get("payload_type",""))
    if "tool" in t and t.endswith("completed"): tools+=1
    if t.startswith("run.terminal"): term=t
o.joinpath("meter.txt").write_text(f"tools={tools} terminal={term}\n")
PY
exit $rc
