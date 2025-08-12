FROM openjdk:17-slim

ENV SPARK_VERSION=3.4.1 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PATH=$PATH:$SPARK_HOME/bin

USER root

# 1) Install OS packages (including procps for `ps`) & Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
      procps \
      wget python3-pip python3-dev build-essential \
      libsasl2-dev libsasl2-modules libsnappy-dev \
      maven curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /tmp \
    && wget --tries=3 --retry-connrefused -O /tmp/spark.tgz \
         https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME \
    && rm /tmp/spark.tgz

# 2) Spark config for Iceberg
COPY conf/spark-defaults.conf $SPARK_HOME/conf/

# 3) Install Python libs: Superset w/ Iceberg + Hive Thrift client + Postgres driver
RUN pip3 install --no-cache-dir \
      apache-superset[iceberg] \
      pyhive[hive] thrift-sasl \
      psycopg2-binary \
    && rm -rf /root/.cache/pip

# 4) Build your Java metadata importer
WORKDIR /opt/app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# 5) Superset config & entrypoint
WORKDIR /
COPY superset_config.py /app/pythonpath/superset_config.py
COPY entrypoint-superset.sh /entrypoint.sh
RUN chmod +x /entrypoint-superset.sh

EXPOSE 8088 10000

ENTRYPOINT ["/entrypoint.sh"]
