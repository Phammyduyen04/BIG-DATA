# ============================================================
# Dockerfile: Spark ETL Production Image (ISOLATED)
# ============================================================
# Context: spark_processing/
# ============================================================
FROM apache/spark:3.5.4

USER root

# Cài đặt wget để tải thư viện
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# Cài đặt các thư viện Python
RUN pip install --no-cache-dir python-dotenv boto3 pandas pyarrow

# Thư mục làm việc
WORKDIR /opt/project

# Tải các thư viện JAR cần thiết (Maven Central)
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P /opt/spark/jars/

# Copy mã nguồn (Local paths relative to spark_processing/)
COPY jobs /opt/project/spark/jobs/
COPY config.py /opt/project/config.py

ENV PYTHONPATH="/opt/project"
ENV SPARK_HOME="/opt/spark"

# ENV SPARK_HOME="/opt/spark"
# Neutral Entrypoint: Inherit from base image (/opt/entrypoint.sh)

