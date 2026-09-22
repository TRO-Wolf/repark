#!/usr/bin/env bash
set -uo pipefail
WD="$1"; PROMPT="$2"; OUT="$3"
BIN=$(command -v opencode || echo "$HOME/.opencode/bin/opencode")
python3 - "$OUT" "${COORDINATOR_MODEL:-zai/glm-5.3}" "${COORDINATOR_TURNS:-80}" <<'PY'
import json,sys,pathlib
out,model,steps=sys.argv[1:4]
perm={"bash":{"*aws *":"deny","*--no-verify*":"deny","*claude *":"deny"},"webfetch":"deny","websearch":"deny","external_directory":"allow"}
cfg={"autoupdate":False,"share":"disabled","model":model,"permission":perm,
     "agent":{"coordinator":{"description":"tick-driven lane coordinator","mode":"primary","maxSteps":int(steps),"permission":perm}}}
pathlib.Path(out,"config.json").write_text(json.dumps(cfg,indent=2)+"\n")
PY
cmd=("$BIN" run --dir "$WD" --agent coordinator --format json)
[ -n "${COORDINATOR_VARIANT:-high}" ] && cmd+=(--variant "${COORDINATOR_VARIANT:-high}")
timeout ${COORDINATOR_TICK_TIMEOUT:-2700} env OPENCODE_CONFIG=$OUT/config.json OPENCODE_DISABLE_AUTOUPDATE=1 OPENCODE_DISABLE_PROJECT_CONFIG=1 \
  "${cmd[@]}" --auto "$(cat $PROMPT)" > "$OUT/out.ndjson" 2> "$OUT/stderr.log"
rc=$?
python3 - "$OUT" <<'PY'
import json,sys,pathlib
o=pathlib.Path(sys.argv[1]); steps=0; cost=0.0
for l in o.joinpath("out.ndjson").read_text(errors="replace").splitlines():
    try: d=json.loads(l)
    except Exception: continue
    if d.get("type")=="step_finish":
        steps+=1; cost+=float((d.get("part") or {}).get("cost") or 0)
o.joinpath("meter.txt").write_text(f"steps={steps} cost_usd={cost:.4f}\n")
PY
exit $rc
