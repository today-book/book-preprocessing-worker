#!/usr/bin/env bash
set -euo pipefail

APP_BASE="/home/ubuntu/app"

APP_NAME="${APP_NAME:?APP_NAME is required}"
APP_PORT="${APP_PORT:?APP_PORT is required}"
VERSION="${VERSION:?VERSION is required}"

APP_DIR="${APP_BASE}/${APP_NAME}"
ENV_FILE=".env"

# 호스트 기준 헬스체크
HEALTH_URL="http://localhost:${APP_PORT}/internal/health"

HEALTH_RETRIES="${HEALTH_RETRIES:-30}"
HEALTH_INTERVAL_SEC="${HEALTH_INTERVAL_SEC:-2}"

cd "$APP_DIR"
touch "$ENV_FILE"

prev_tag="$(grep -E '^IMAGE_TAG=' "$ENV_FILE" | tail -n1 | cut -d= -f2- || true)"
new_tag="$VERSION"

set_env() {
  key="$1"; val="$2"
  if grep -q "^${key}=" "$ENV_FILE"; then
    sed -i "s/^${key}=.*/${key}=${val}/" "$ENV_FILE"
  else
    echo "${key}=${val}" >> "$ENV_FILE"
  fi
}

# IMAGE_TAG만 교체 (포트/이름은 고정)
set_env IMAGE_TAG "$new_tag"
set_env APP_NAME "$APP_NAME"
set_env APP_PORT "$APP_PORT"

deploy() {
  sudo docker compose pull
  sudo docker compose up -d
}

healthy() {
  for ((i=1; i<=HEALTH_RETRIES; i++)); do
    curl -fsS "$HEALTH_URL" >/dev/null && return 0
    sleep "$HEALTH_INTERVAL_SEC"
  done
  return 1
}

echo "Deploy ${APP_NAME}: ${prev_tag:-<none>} -> ${new_tag}"
deploy

if healthy; then
  echo "✅ healthy: ${new_tag}"
  exit 0
fi

echo "❌ unhealthy: ${new_tag}"
sudo docker compose logs --tail=200 || true

if [[ -z "$prev_tag" ]]; then
  echo "❌ rollback skipped (no previous IMAGE_TAG)"
  exit 1
fi

echo "↩️ rollback -> ${prev_tag}"
set_env IMAGE_TAG "$prev_tag"
deploy

healthy && echo "✅ rollback healthy: ${prev_tag}" && exit 0

echo "❌ rollback unhealthy too"
exit 1
