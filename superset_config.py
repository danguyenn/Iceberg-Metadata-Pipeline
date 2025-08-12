# superset_config.py
import pyhive_spark_patch
import os

# 1) Keep Superset's internal metadata DB on Postgres
SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")

# 2) Spark Thrift host/port for Iceberg access
SPARK_HOST = os.environ.get("SPARK_HOST", "spark-thrift-server")
SPARK_PORT = os.environ.get("SPARK_PORT", "10000")

# 3) Register Iceberg-on-Spark as a secondary database for SQL Lab
# Connect to the default schema first, then you can switch to nyc in queries
DATABASES = {
    "iceberg_thrift": {
        "name": "Iceberg via Spark Thrift",
        # Connect to default schema for compatibility
        "SQLALCHEMY_URI": f"hive://{SPARK_HOST}:{SPARK_PORT}/default?auth=NOSASL",
        "engine_params": {
            "connect_args": {
                "auth": "NOSASL",
            },
        },
        "expose_in_sqllab": True,
    }
}

# Additional debugging settings
# SQLLAB_TIMEOUT = 300
# SQLLAB_ASYNC_TIME_LIMIT_SEC = 300

# Enable logging for debugging
# import logging
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
# logging.getLogger('pyhive').setLevel(logging.DEBUG)