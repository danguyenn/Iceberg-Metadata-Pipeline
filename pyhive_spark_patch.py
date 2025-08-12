# pyhive_spark_patch.py
from sqlalchemy import text
from pyhive import sqlalchemy_hive

def _spark_get_table_names(self, connection, schema=None, **kw):
    if not schema:
        return []
    rows = connection.execute(text(f"SHOW TABLES IN `{schema}`")).fetchall()
    return [r[1] if len(r) >= 2 else r[0] for r in rows]  # Spark 3: (namespace, name, isTemp)

def _spark_get_view_names(self, connection, schema=None, **kw):
    rows = connection.execute(text(f"SHOW VIEWS IN `{schema}`")).fetchall()
    return [r[1] if len(r) >= 2 else r[0] for r in rows]

sqlalchemy_hive.HiveDialect.get_table_names = _spark_get_table_names
sqlalchemy_hive.HiveDialect.get_view_names = _spark_get_view_names
