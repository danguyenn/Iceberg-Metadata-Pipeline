# superset_config.py
import os
import logging
import pyhive_spark_patch  # keep your existing patch import

# 1) Superset metadata DB (Postgres in your docker-compose)
SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")

# 2) Spark Thrift host/port available to the container (used only for logs/visibility here)
SPARK_HOST = os.environ.get("SPARK_HOST", "spark-thrift-server")
SPARK_PORT = os.environ.get("SPARK_PORT", "10000")

# --- Spark/Hive: rewrite unsupported SHOW CREATE VIEW to SHOW CREATE TABLE ---
from sqlalchemy import event
from sqlalchemy.engine import Engine

_logger = logging.getLogger(__name__)

def _hive_rewrite_show_create_view(conn, cursor, statement, parameters, context, executemany):
    """
    Spark SQL doesn't support `SHOW CREATE VIEW`. Superset sometimes issues it
    when fetching metadata. This hook rewrites it to `SHOW CREATE TABLE`, which
    Spark understands for both tables and Iceberg views.
    """
    try:
        eng = getattr(conn, "engine", None)
        if eng and getattr(eng, "name", "") == "hive":
            s = statement.lstrip()
            up = s.upper()
            prefix = "SHOW CREATE VIEW"
            if up.startswith(prefix):
                fixed = "SHOW CREATE TABLE" + s[len(prefix):]
                if context is not None and hasattr(context, "statement"):
                    context.statement = fixed
                return fixed, parameters
    except Exception as ex:
        _logger.warning("[spark-patch] SHOW CREATE VIEW rewrite skipped due to: %r", ex)
    return statement, parameters

# Register once at import time
event.listen(Engine, "before_cursor_execute", _hive_rewrite_show_create_view, retval=True)
_logger.info("[spark-patch] Registered SHOW CREATE VIEW → SHOW CREATE TABLE rewriter for Hive engines")
