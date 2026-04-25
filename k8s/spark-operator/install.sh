#!/usr/bin/env bash
# ============================================================
# Install Spark Operator on K3s Cluster (Tailscale / VM nodes)
# Tested with: kubeflow spark-operator helm chart >= 1.4.x
#              K3s v1.28+, Spark 3.5.x
#
# Run once from any node with kubectl access (e.g. control1):
#   chmod +x k8s/spark-operator/install.sh
#   bash k8s/spark-operator/install.sh
# ============================================================
set -euo pipefail

NAMESPACE_OPERATOR="spark-operator"
NAMESPACE_JOB="spark-etl"
RELEASE_NAME="spark-operator"

echo "==> Adding kubeflow spark-operator helm repo..."
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

# Create namespace for spark-etl jobs (idempotent)
kubectl create namespace "$NAMESPACE_JOB" --dry-run=client -o yaml | kubectl apply -f -

echo "==> Installing spark-operator in namespace: $NAMESPACE_OPERATOR"
helm upgrade --install "$RELEASE_NAME" spark-operator/spark-operator \
  --namespace "$NAMESPACE_OPERATOR" \
  --create-namespace \
  --set "spark.jobNamespaces={$NAMESPACE_JOB}" \
  --set webhook.enable=true \
  --set webhook.port=9443 \
  --set controller.workers=4 \
  --set batchScheduler.enable=false \
  --wait --timeout=120s

echo ""
echo "==> spark-operator installed. Verify pods:"
kubectl get pods -n "$NAMESPACE_OPERATOR"

echo ""
echo "==> Apply RBAC + SparkApplication:"
echo "    kubectl apply -f k8s/spark-operator/rbac.yaml"
echo "    kubectl apply -f k8s/spark-operator/spark-application.yaml"
echo ""
echo "==> To trigger a run:"
echo "    kubectl apply -f k8s/spark-operator/spark-application.yaml"
echo ""
echo "==> Monitor:"
echo "    kubectl get sparkapplication -n spark-etl"
echo "    kubectl describe sparkapplication cryptodw-production -n spark-etl"
echo "    kubectl logs -f -n spark-etl -l spark-role=driver"
