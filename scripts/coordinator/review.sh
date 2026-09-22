#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$(readlink -f "$0")")/env.sh"
LANE="$1"; BRIEF="$2"; REPO_NAME="${COORDINATOR_REPO:-repark}"
SRC=$SCRATCH/$LANE; RV="rv-$LANE"; REPO=$SCRATCH/$RV
CRITIC=${COORDINATOR_CRITIC:-$HOME/.claude/skills/grok-worker/grok-worker.sh}
rm -rf "$REPO"; git clone -q "$SRC" "$REPO"
git -C "$REPO" remote set-url origin https://github.com/$GH_OWNER/$REPO_NAME.git
git -C "$REPO" remote set-url --push origin no_push
git -C "$REPO" fetch -q origin main:refs/remotes/origin/main
printf '/handback.json\n' >> "$REPO/.git/info/exclude"
[ -d "$SRC/.venv" ] && ln -s "$SRC/.venv" "$REPO/.venv" 2>/dev/null || true
mkdir -p $ROOT/$RV
systemd-run --user --slice=repark.slice --collect --quiet --unit="grok-$RV-$(date -u +%H%M%S)" -p MemoryMax=32G -p LimitNOFILE=65536 \
  --setenv=HOME=$HOME --setenv=PATH="$PATH" --setenv=USER=$USER --setenv=CARGO_BUILD_JOBS=6 --setenv=RUST_TEST_THREADS=6 -p WorkingDirectory=$HOME \
  -p StandardOutput=append:$ROOT/$RV/launch.log -p StandardError=append:$ROOT/$RV/launch.log \
  -- $CRITIC --lane $RV --repo $REPO --brief "$BRIEF" --role critic-logic --max-turns 120
echo "critic launched on $REPO at $(git -C $REPO rev-parse --short HEAD); add '$RV' to your LANES line; verdict lands in $SCRATCH/grok-worker/$RV/<stamp>/out.json (structuredOutput)"
