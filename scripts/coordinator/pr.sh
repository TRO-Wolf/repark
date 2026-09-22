#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$(readlink -f "$0")")/env.sh"
LANE="$1"; TITLE="$2"; BODY="$3"; DRAFT="${4:-}"; REPO="${COORDINATOR_REPO:-repark}"
BANNED_TRAILER='co-authored[-]by|claude[-]session|claude\.ai'
BANNED_BODY='co-authored[-]by|claude\.ai|generated[ ]with'
cd $SCRATCH/$LANE
BR=$(git branch --show-current)
[ -z "$(git status --porcelain --untracked-files=no)" ] || { echo "STOP: dirty tree"; exit 3; }
[ -x .git/hooks/pre-push ] || { echo "STOP: the pre-push hook is missing in $SCRATCH/$LANE"; exit 4; }
git log --format='%ae %ce' origin/main..HEAD | tr ' ' '\n' | sort -u | grep -v '^64240326+TRO-Wolf@users.noreply.github.com$' | grep . && { echo "STOP: a commit carries a foreign identity"; exit 5; }
git log --format=%B origin/main..HEAD | grep -iE "$BANNED_TRAILER" && { echo "STOP: a banned trailer is in a commit message"; exit 6; }
python3 $LIB/comment_ban.py $SCRATCH/$LANE origin/main HEAD || { echo "STOP: comment gate"; exit 7; }
grep -iE "$BANNED_BODY" "$BODY" && { echo "STOP: banned text in the PR body"; exit 8; }
REMOTE_SHA=$(git ls-remote https://github.com/$GH_OWNER/$REPO.git "refs/heads/$BR" | cut -f1)
if [ -n "$REMOTE_SHA" ]; then LEASE="--force-with-lease=refs/heads/$BR:$REMOTE_SHA"; else LEASE=""; fi
git push -q https://github.com/$GH_OWNER/$REPO.git "HEAD:refs/heads/$BR" $LEASE 2>&1 | tail -3
N=$(gh pr list -R $GH_OWNER/$REPO --head "$BR" --state open --json number --jq '.[0].number // empty')
if [ -z "$N" ]; then gh pr create -R $GH_OWNER/$REPO --base main --head "$BR" --title "$TITLE" --body-file "$BODY" ${DRAFT:+--draft} | tail -1; else echo "pushed; PR #$N already open"; fi
