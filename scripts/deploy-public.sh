#!/usr/bin/env bash
# Deploys deploy/ to the public instance and restarts what changed.
# Usage: TEKEL_DEPLOY_HOST=user@host scripts/deploy-public.sh
# The server keeps only .env (secrets, TEKEL_VERSION, COMPOSE_FILE, COMPOSE_PROFILES).
set -euo pipefail
host="${TEKEL_DEPLOY_HOST:?set TEKEL_DEPLOY_HOST=user@host}"
dir="${TEKEL_DEPLOY_DIR:-tekel}"
cd "$(dirname "$0")/../deploy"
rsync -a --delete-after --exclude .env \
  docker-compose.yml compose.public.yml grafana prometheus "$host:$dir/"
ssh "$host" "cd $dir && docker compose config --quiet && docker compose pull --quiet && docker compose up -d --remove-orphans"
