#!/usr/bin/env bash
# One-time setup for Spark client mode on the Control Node.
# Run as root or a user with sudo.
# Usage: bash scripts/setup_control_spark.sh
set -euo pipefail

SPARK_VERSION="3.5.4"
SPARK_SCALA="2.12"
SPARK_HOME="/opt/spark"
SPARK_TGZ="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
SPARK_URL="https://downloads.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"

POSTGRES_JAR="postgresql-42.7.3.jar"
HADOOP_AWS_JAR="hadoop-aws-3.3.4.jar"
AWS_SDK_JAR="aws-java-sdk-bundle-1.12.262.jar"

MAVEN="https://repo1.maven.org/maven2"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "${SCRIPT_DIR}")"

echo "========================================"
echo " CryptoDW Control Node — Spark Setup"
echo " Spark ${SPARK_VERSION}, SPARK_HOME=${SPARK_HOME}"
echo "========================================"

# ── 1. Install Java 17 (OpenJDK) ─────────────────────────────────────────────
if ! java -version 2>&1 | grep -q "17\|21"; then
    echo "[setup] Installing OpenJDK 17 ..."
    sudo apt-get update -qq
    sudo apt-get install -y openjdk-17-jdk-headless
fi
echo "[setup] Java: $(java -version 2>&1 | head -1)"

# ── 2. Download and extract Spark ─────────────────────────────────────────────
if [ ! -d "${SPARK_HOME}" ]; then
    echo "[setup] Downloading Spark ${SPARK_VERSION} ..."
    wget -q "${SPARK_URL}" -O "/tmp/${SPARK_TGZ}"
    echo "[setup] Extracting to ${SPARK_HOME} ..."
    sudo tar -xzf "/tmp/${SPARK_TGZ}" -C /opt/
    sudo mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" "${SPARK_HOME}"
    rm -f "/tmp/${SPARK_TGZ}"
else
    echo "[setup] Spark already installed at ${SPARK_HOME}"
fi

# ── 3. Download JARs into $SPARK_HOME/jars/ ───────────────────────────────────
JARS_DIR="${SPARK_HOME}/jars"

download_jar() {
    local url="$1"
    local jar="$2"
    if [ ! -f "${JARS_DIR}/${jar}" ]; then
        echo "[setup] Downloading ${jar} ..."
        sudo wget -q "${url}" -O "${JARS_DIR}/${jar}"
    else
        echo "[setup] Already present: ${jar}"
    fi
}

download_jar \
    "${MAVEN}/org/postgresql/postgresql/42.7.3/${POSTGRES_JAR}" \
    "${POSTGRES_JAR}"

download_jar \
    "${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/${HADOOP_AWS_JAR}" \
    "${HADOOP_AWS_JAR}"

download_jar \
    "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/${AWS_SDK_JAR}" \
    "${AWS_SDK_JAR}"

# ── 4. Copy executor pod template ─────────────────────────────────────────────
CONF_DIR="${SPARK_HOME}/conf"
sudo mkdir -p "${CONF_DIR}"
sudo cp "${PROJECT_DIR}/k8s/executor-pod-template.yaml" \
        "${CONF_DIR}/executor-pod-template.yaml"
echo "[setup] executor-pod-template.yaml → ${CONF_DIR}/"

# ── 5. pip install benchmark requirements ─────────────────────────────────────
echo "[setup] Installing Python packages ..."
pip3 install --quiet -r "${PROJECT_DIR}/benchmark/requirements.txt"

# ── 6. Verify ─────────────────────────────────────────────────────────────────
echo ""
echo "[setup] Verification:"
echo "  Spark: $("${SPARK_HOME}/bin/spark-submit" --version 2>&1 | head -1)"
echo "  JARs present:"
ls "${JARS_DIR}/${POSTGRES_JAR}" "${JARS_DIR}/${HADOOP_AWS_JAR}" "${JARS_DIR}/${AWS_SDK_JAR}" \
    | xargs -I{} echo "    {}"
echo "  Pod template: ${CONF_DIR}/executor-pod-template.yaml"
echo ""
echo "[setup] Done. Add to your shell profile:"
echo "  export SPARK_HOME=${SPARK_HOME}"
echo "  export PATH=\$SPARK_HOME/bin:\$PATH"
