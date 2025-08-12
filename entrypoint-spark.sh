#!/usr/bin/env bash
set -euo pipefail

echo "[spark-entrypoint] launching Java metadata importer…"
java -jar /opt/app/target/iceberg-importer-1.0.0.jar \
     --input-path /data \
     --output-path nyc \
     --warehouse-uri "${WAREHOUSE_URI:-file:///warehouse}"

echo "[spark-entrypoint] launching Spark Thrift Server…"
exec "${SPARK_HOME}/bin/spark-submit" \
  --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
  --jars $SPARK_HOME/jars/iceberg-spark-runtime-3.4_2.12-1.9.2.jar,$SPARK_HOME/jars/iceberg-core-1.9.2.jar \
  --master local[*] \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hadoop \
  --conf spark.sql.catalog.iceberg.warehouse="${WAREHOUSE_URI:-file:///warehouse}" \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.sql.warehouse.dir="${WAREHOUSE_URI:-file:///warehouse}" \
  --conf spark.thriftserver.thrift.bind.host=0.0.0.0 \
  --conf spark.thriftserver.thrift.port=10000 \
  --properties-file $SPARK_HOME/conf/spark-defaults.conf