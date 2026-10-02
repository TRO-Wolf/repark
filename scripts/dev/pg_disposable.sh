#!/usr/bin/env bash
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
COMPOSE=(docker compose -f "$HERE/pg/compose.yaml")
if test_dir="${XDG_RUNTIME_DIR:-/tmp}" && touch "$test_dir/.repark-pg-w" 2>/dev/null; then
  rm -f "$test_dir/.repark-pg-w"
  STATE="$test_dir/repark-pg.env"
else
  STATE="/tmp/repark-pg.env"
fi
case "${1:-}" in
  up)
    [ "$(docker ps -q --filter label=repark.disposable=1 | wc -l)" -lt 4 ] || { echo "four disposable containers already running" >&2; exit 3; }
    P="repark-pg-${USER:-u}-$$"
    "${COMPOSE[@]}" -p "$P" up -d --wait >/dev/null
    PORT=$("${COMPOSE[@]}" -p "$P" port postgres 5432 | cut -d: -f2)
    printf 'REPARK_PG_PROJECT=%s\nREPARK_PG_URL=postgresql://postgres:repark@127.0.0.1:%s/postgres\n' "$P" "$PORT" > "$STATE"
    cat "$STATE" ;;
  down) . "$STATE"; "${COMPOSE[@]}" -p "$REPARK_PG_PROJECT" down -v >/dev/null; rm -f "$STATE" ;;
  url) . "$STATE"; echo "$REPARK_PG_URL" ;;
  reap)
    cutoff=$(date -u -d '-2 hours' +%s)
    for id in $(docker ps -q --filter label=repark.disposable=1); do
      started=$(date -u -d "$(docker inspect --format '{{.State.StartedAt}}' "$id")" +%s)
      [ "$started" -lt "$cutoff" ] && docker rm -f "$id" >/dev/null
    done ;;
  *) echo "usage: $0 up|down|url|reap" >&2; exit 2 ;;
esac
