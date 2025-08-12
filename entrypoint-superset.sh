#!/usr/bin/env bash
set -e

echo "[entrypoint] applying database migrations..."
superset db upgrade

echo "[entrypoint] creating admin user if missing..."
if ! superset fab list-users | grep -q '^admin\s'; then
  superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@example.com \
    --password admin
else
  echo "[entrypoint] admin already exists"
fi

echo "[entrypoint] initializing Superset..."
superset init

echo "[entrypoint] launching Superset webserver..."
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger
