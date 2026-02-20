#!/bin/bash

set -e

echo "============================================"
echo "STEP 3: Create Lightweight Kind Cluster"
echo "============================================"
echo ""

# Check prerequisites
echo "Verifying prerequisites..."
if ! command -v kind &> /dev/null; then
    echo "❌ kind not found"
    exit 1
fi

if ! command -v kubectl &> /dev/null; then
    echo "❌ kubectl not found"
    exit 1
fi

if ! docker ps &> /dev/null; then
    echo "❌ Docker not accessible"
    exit 1
fi

echo "✓ Prerequisites OK"
echo ""

# Memory check
TOTAL_MEM=$(free -g | awk '/^Mem:/{print $2}')
USED_MEM=$(free -g | awk '/^Mem:/{print $3}')
FREE_MEM=$((TOTAL_MEM - USED_MEM))

echo "System Memory Status:"
echo "  Total: ${TOTAL_MEM}GB"
echo "  Used:  ${USED_MEM}GB"
echo "  Free:  ${FREE_MEM}GB"
echo ""

if [ "$FREE_MEM" -lt 4 ]; then
    echo "⚠️  Warning: Less than 4GB free memory"
    echo "   Cluster may experience resource pressure"
    read -p "Continue? (yes/no): " -r
    echo
    if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        exit 1
    fi
fi

# Create cluster
echo "Creating Kind cluster..."
echo "  - 1 control-plane node"
echo "  - 1 worker node"
echo "  - Port mappings configured"
echo ""

if ! kind create cluster --config kind-cluster-config-light.yaml; then
    echo "❌ Failed to create cluster"
    exit 1
fi

echo ""
echo "Waiting for cluster to be ready..."
kubectl wait --for=condition=Ready nodes --all --timeout=180s

echo ""
echo "============================================"
echo "✓ STEP 3 COMPLETE"
echo "============================================"
echo ""

# Display cluster info
echo "Cluster Information:"
echo "-------------------"
kubectl cluster-info --context kind-data-engineering-challenge

echo ""
echo "Nodes:"
kubectl get nodes -o wide

echo ""
echo "System Pods:"
kubectl get pods -n kube-system

echo ""
echo "Namespaces:"
kubectl get namespaces

echo ""
echo "Resource Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"

echo ""
echo "Port Mappings (localhost):"
echo "  PostgreSQL:     localhost:5432"
echo "  MongoDB:        localhost:27017"
echo "  Kafka:          localhost:9092"
echo "  Kafka Connect:  localhost:8083"
echo "  ClickHouse HTTP: localhost:8123"
echo "  ClickHouse TCP:  localhost:9000"
echo "  Airflow UI:     localhost:8080"

echo ""
echo "============================================"
echo "Next: STEP 4 - Install Strimzi Operator"
echo "Run: ./scripts/step-04-install-strimzi.sh"