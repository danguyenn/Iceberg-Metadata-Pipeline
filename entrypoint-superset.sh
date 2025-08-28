#!/usr/bin/env bash
set -euo pipefail

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

# Build the Spark/Thrift URI for PyHive (binary, NOSASL)
SPARK_HOST="${SPARK_HOST:-spark-thrift-server}"
SPARK_PORT="${SPARK_PORT:-10000}"
export SPARK_URI="hive://${SPARK_HOST}:${SPARK_PORT}/default?auth=NOSASL"

echo "[entrypoint] ensuring 'Iceberg via Spark Thrift' database exists (${SPARK_URI})..."
superset shell <<'PY'
import os
from superset import db
from superset.models.core import Database

NAME = "Iceberg via Spark Thrift"
URI  = os.environ.get("SPARK_URI")

existing = db.session.query(Database).filter_by(database_name=NAME).one_or_none()
if existing:
    if existing.sqlalchemy_uri != URI:
        existing.set_sqlalchemy_uri(URI)
        db.session.commit()
        print("[entrypoint] updated database URI for:", NAME)
    else:
        print("[entrypoint] database already present with correct URI:", NAME)
else:
    new_db = Database(database_name=NAME)
    new_db.set_sqlalchemy_uri(URI)
    db.session.add(new_db)
    db.session.commit()
    print("[entrypoint] created database:", NAME)
PY

echo "[entrypoint] initializing Superset..."
superset init

echo "[entrypoint] launching Superset webserver..."
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger
