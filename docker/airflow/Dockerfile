FROM apache/airflow:3.3.2-python3.12

# --- Java for Spark (local mode inside the Airflow worker) ---
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow

# Install project requirements together with the pinned Airflow version,
# as the Airflow docs recommend, so pip cannot silently upgrade/downgrade Airflow.
COPY --chown=airflow:0 --chmod=644 requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-airflow.txt

# --- S3A connector so Spark can read/write the MinIO lake ---
# PySpark 4.0.1 bundles Hadoop 3.4.1; hadoop-aws must match it exactly. The AWS SDK v2 bundle
# version follows Hadoop 3.4.x (HADOOP-19082: 2.24.6). Downloaded at build time, not at runtime.
ARG HADOOP_AWS_VERSION=3.4.1
ARG AWS_SDK_BUNDLE_VERSION=2.24.6
RUN SPARK_JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
 && curl -fsSL -o "${SPARK_JARS}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
 && curl -fsSL -o "${SPARK_JARS}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
      "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE_VERSION}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
 && ls -la "${SPARK_JARS}" | grep -E "hadoop-aws|bundle-|hadoop-client"
