#!/bin/bash

set -e

echo "============================================"
echo "STEP 5: Install ClickHouse Operator (Proper)"
echo "============================================"
echo ""

NAMESPACE="clickhouse-operator"

# Verify cluster
echo "Verifying cluster access..."
if ! kubectl cluster-info &>/dev/null; then
    echo "❌ Cannot access cluster"
    exit 1
fi
echo "✓ Cluster accessible"
echo ""

# Create namespace
echo "Step 1/4: Creating namespace..."
kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -
echo "✓ Namespace created: ${NAMESPACE}"
echo ""

# Download manifest
echo "Step 2/4: Downloading operator manifest..."
MANIFEST_URL="https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-bundle.yaml"
curl -sL ${MANIFEST_URL} -o /tmp/clickhouse-operator-bundle.yaml
echo "✓ Manifest downloaded"
echo ""

# Replace namespace in manifest
echo "Step 3/4: Configuring namespace..."
sed -i.bak "s/namespace: kube-system/namespace: ${NAMESPACE}/g" /tmp/clickhouse-operator-bundle.yaml
echo "✓ Namespace configured"
echo ""

# Apply manifest
echo "Step 4/4: Installing operator..."
kubectl apply -f /tmp/clickhouse-operator-bundle.yaml -n ${NAMESPACE}

echo ""
echo "Waiting for operator to be ready (max 5 minutes)..."
sleep 10

# Wait for deployment
kubectl wait --for=condition=available deployment/clickhouse-operator -n ${NAMESPACE} --timeout=300s 2>/dev/null || {
    echo ""
    echo "Deployment taking longer than expected, checking status..."
    kubectl get deployment -n ${NAMESPACE}
    kubectl get pods -n ${NAMESPACE}
}

echo ""
echo "============================================"
echo "✓ STEP 5 COMPLETE"
echo "============================================"
echo ""

# Show status
echo "Operator Status:"
kubectl get deployment -n ${NAMESPACE}
echo ""
kubectl get pods -n ${NAMESPACE}

echo ""
echo "CRDs:"
kubectl get crd | grep clickhouse.altinity.com

echo ""
echo "Resource Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"

echo ""
echo "============================================"
echo "Next: STEP 6 - Deploy PostgreSQL"
echo "Run: ./scripts/step-06-deploy-postgres.sh"
echo "============================================"