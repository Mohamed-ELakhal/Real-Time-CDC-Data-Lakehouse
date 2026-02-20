#!/bin/bash

set -e

echo "============================================"
echo "REINSTALL: Strimzi Operator (Full Clean)"
echo "============================================"
echo ""

STRIMZI_VERSION="0.41.0"
NAMESPACE="kafka"

# ----------------------------------------------------------------
# ROOT CAUSE ANALYSIS:
# The Strimzi install manifest creates ClusterRoles for CRD access
# AND namespace-scoped Roles for lease/configmap access. The
# ClusterRoleBindings contain a 'subjects' entry that must reference
# the correct namespace for the service account.
#
# When applied with `-n kafka`, the manifest substitutes namespace
# correctly in most places, but previous failed installs + patching
# left the operator in a broken state where:
#   1. Lease RBAC was missing → operator couldn't acquire leader lock
#   2. After we added lease RBAC, the operator started but then
#      crashed because kafkaconnects ClusterRoleBinding had the
#      wrong namespace in its subject, so it could never watch CRDs
#
# Solution: nuclear wipe of the kafka namespace + fresh install
# ----------------------------------------------------------------

echo "Step 1/7: Full wipe of kafka namespace..."
kubectl delete namespace ${NAMESPACE} --ignore-not-found=true
echo "  Waiting for namespace to be fully deleted..."
kubectl wait --for=delete namespace/${NAMESPACE} --timeout=120s 2>/dev/null || true

# Also clean up ClusterRoles and ClusterRoleBindings left behind
# (they are cluster-scoped, not namespace-scoped, so survive namespace deletion)
echo "  Cleaning up cluster-scoped Strimzi RBAC..."
kubectl get clusterrolebinding -o name | grep strimzi | xargs kubectl delete 2>/dev/null || true
kubectl get clusterrole -o name | grep strimzi | xargs kubectl delete 2>/dev/null || true
echo "✓ Full wipe complete"
echo ""

echo "Step 2/7: Recreating kafka namespace..."
kubectl create namespace ${NAMESPACE}
echo "✓ Namespace created"
echo ""

echo "Step 3/7: Downloading Strimzi ${STRIMZI_VERSION} manifest..."
MANIFEST_URL="https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml"
MANIFEST_FILE="/tmp/strimzi-${STRIMZI_VERSION}.yaml"

# Check network access
if ! curl -fsSL --max-time 30 -o "${MANIFEST_FILE}" "${MANIFEST_URL}" 2>/dev/null; then
    echo "  Network unavailable, checking for cached manifest..."
    if [ ! -f "${MANIFEST_FILE}" ]; then
        echo "❌ Cannot download Strimzi manifest and no cache found"
        echo "   Please check your network settings"
        exit 1
    fi
    echo "  Using cached manifest"
fi
echo "✓ Manifest ready"
echo ""

echo "Step 4/7: Applying Strimzi manifests into namespace '${NAMESPACE}'..."
# sed replaces the default 'myproject' namespace placeholder in the manifest
# with our actual namespace. This fixes the ClusterRoleBinding subjects.
sed "s/namespace: myproject/namespace: ${NAMESPACE}/g" "${MANIFEST_FILE}" | \
    kubectl apply -f - -n ${NAMESPACE}
echo "✓ Strimzi manifests applied"
echo ""

echo "Step 5/7: Adding lease RBAC (not included in default manifest)..."
kubectl apply -f - <<YAML
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: strimzi-lease-operator
  namespace: ${NAMESPACE}
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: strimzi-lease-operator
  namespace: ${NAMESPACE}
subjects:
  - kind: ServiceAccount
    name: strimzi-cluster-operator
    namespace: ${NAMESPACE}
roleRef:
  kind: Role
  name: strimzi-lease-operator
  apiGroup: rbac.authorization.k8s.io
YAML
echo "✓ Lease RBAC created"
echo ""

echo "Step 6/7: Fixing operator env vars (lease namespace + identity)..."
# These env vars exist in the manifest but are blank.
# We use 'kubectl set env' which updates in-place without duplicating.
kubectl set env deployment/strimzi-cluster-operator \
    -n ${NAMESPACE} \
    STRIMZI_LEADER_ELECTION_LEASE_NAMESPACE=${NAMESPACE} \
    STRIMZI_LEADER_ELECTION_IDENTITY='$(POD_NAME)'

# Also add POD_NAME downward API env var if not present
kubectl patch deployment strimzi-cluster-operator -n ${NAMESPACE} \
  --type=strategic -p '{
  "spec": {"template": {"spec": {"containers": [{
    "name": "strimzi-cluster-operator",
    "env": [{
      "name": "POD_NAME",
      "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}
    }]
  }]}}}
}' 2>/dev/null || true
echo "✓ Env vars set"
echo ""

echo "Step 7/7: Waiting for Strimzi operator to be ready..."
kubectl rollout status deployment/strimzi-cluster-operator \
    -n ${NAMESPACE} --timeout=180s
echo "✓ Operator pod ready"
echo ""

echo "Waiting 15s for operator to acquire lease and initialize..."
sleep 15

echo "Checking operator health..."
echo ""
echo "--- Last 20 log lines ---"
kubectl logs deployment/strimzi-cluster-operator -n ${NAMESPACE} --tail=20 2>/dev/null | \
    grep -v "at java\." | grep -v "at io\.fabric8" | grep -v "at io\.vertx" | \
    grep -v "at io\.netty" | grep -v "\.\.\." | grep -v "^$" | head -20

echo ""
if kubectl get lease strimzi-cluster-operator -n ${NAMESPACE} &>/dev/null; then
    HOLDER=$(kubectl get lease strimzi-cluster-operator -n ${NAMESPACE} \
        -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "unknown")
    echo "✓ Leader election lease acquired: ${HOLDER}"
else
    echo "⚠  Lease not yet created — operator may still be starting"
fi

echo ""
echo "--- Verifying RBAC (spot check) ---"
kubectl auth can-i list kafkas \
    --as=system:serviceaccount:${NAMESPACE}:strimzi-cluster-operator \
    -n ${NAMESPACE} 2>/dev/null \
    && echo "✓ Can list Kafka CRs" \
    || echo "✗ Cannot list Kafka CRs — RBAC still broken"

kubectl auth can-i list kafkaconnects \
    --as=system:serviceaccount:${NAMESPACE}:strimzi-cluster-operator \
    -n ${NAMESPACE} 2>/dev/null \
    && echo "✓ Can list KafkaConnect CRs" \
    || echo "✗ Cannot list KafkaConnect CRs"

kubectl auth can-i get leases \
    --as=system:serviceaccount:${NAMESPACE}:strimzi-cluster-operator \
    -n ${NAMESPACE} 2>/dev/null \
    && echo "✓ Can get Leases" \
    || echo "✗ Cannot get Leases"

echo ""
echo "============================================"
echo "✓ Strimzi reinstall complete"
echo "============================================"
echo ""
echo "If RBAC checks above all show ✓, run:"
echo "  ./scripts/step-09-deploy-kafka.sh"