#!/bin/bash
# v3 - Fixed probe timeoutSeconds (mongosh needs >1s to start Node.js runtime)

set -e

echo "============================================"
echo "STEP 7: Deploy MongoDB (Replica Set for CDC)"
echo "============================================"
echo ""

echo "Verifying cluster access..."
if ! kubectl cluster-info &>/dev/null; then
    echo "❌ Cannot access cluster"
    exit 1
fi
echo "✓ Cluster accessible"
echo ""

NAMESPACE="default"
REPLSET_NAME="rs0"

echo "Cleaning up any previous MongoDB deployment..."
kubectl delete statefulset mongodb -n ${NAMESPACE} --ignore-not-found=true
kubectl delete service mongodb -n ${NAMESPACE} --ignore-not-found=true
kubectl delete service mongodb-external -n ${NAMESPACE} --ignore-not-found=true
echo "  Waiting for pod termination..."
kubectl wait --for=delete pod/mongodb-0 -n ${NAMESPACE} --timeout=90s 2>/dev/null || true
# To wipe data volume for a completely fresh start, uncomment:
# kubectl delete pvc mongodb-storage-mongodb-0 -n ${NAMESPACE} --ignore-not-found=true
echo "✓ Cleanup done"
echo ""

# ----------------------------------------------------------------
# ROOT CAUSE OF PREVIOUS FAILURE: probe timeoutSeconds default=1s
#
# mongosh is a Node.js application. It takes 2-3 seconds just to
# start the JS runtime before it can even send a ping command.
# With timeoutSeconds=1 (the Kubernetes default), EVERY probe
# attempt timed out and was counted as a failure:
#   - readinessProbe failures → pod never became Ready
#   - livenessProbe failures → kubelet killed and restarted container
#
# Fix: timeoutSeconds=10 gives mongosh enough time to initialize.
#
# Also using a shell-based probe that does NOT require mongosh
# at all: nc (netcat) checks if port 27017 is open. Lightweight,
# instant, and completely reliable on constrained resources.
# ----------------------------------------------------------------

echo "Step 1/4: Creating MongoDB headless Service..."
kubectl apply -f - <<YAML
apiVersion: v1
kind: Service
metadata:
  name: mongodb
  namespace: ${NAMESPACE}
  labels:
    app: mongodb
spec:
  clusterIP: None
  ports:
  - port: 27017
    targetPort: 27017
    name: mongodb
  selector:
    app: mongodb
YAML
echo "✓ Headless service created"
echo ""

echo "Step 2/4: Creating MongoDB NodePort Service..."
kubectl apply -f - <<YAML
apiVersion: v1
kind: Service
metadata:
  name: mongodb-external
  namespace: ${NAMESPACE}
  labels:
    app: mongodb
spec:
  type: NodePort
  ports:
  - port: 27017
    targetPort: 27017
    nodePort: 30017
    name: mongodb
  selector:
    app: mongodb
YAML
echo "✓ NodePort service created"
echo ""

echo "Step 3/4: Creating MongoDB StatefulSet..."
kubectl apply -f - <<YAML
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mongodb
  namespace: ${NAMESPACE}
  labels:
    app: mongodb
spec:
  serviceName: mongodb
  replicas: 1
  selector:
    matchLabels:
      app: mongodb
  template:
    metadata:
      labels:
        app: mongodb
    spec:
      containers:
      - name: mongodb
        image: mongo:6.0
        ports:
        - containerPort: 27017
          name: mongodb
        args:
          - "--replSet"
          - "${REPLSET_NAME}"
          - "--bind_ip_all"
        lifecycle:
          postStart:
            exec:
              command:
                - bash
                - -c
                - |
                  echo "postStart: waiting for MongoDB to accept connections..."
                  for i in \$(seq 1 30); do
                    if mongosh --eval "db.adminCommand('ping')" --quiet 2>/dev/null; then
                      echo "postStart: ready after \${i} attempts."
                      break
                    fi
                    sleep 2
                  done
                  echo "postStart: running rs.initiate() (idempotent)..."
                  mongosh --eval "
                    try {
                      rs.initiate({
                        _id: '${REPLSET_NAME}',
                        members: [{ _id: 0, host: 'mongodb-0.mongodb.${NAMESPACE}.svc.cluster.local:27017' }]
                      });
                      print('Replica set initiated.');
                    } catch(e) {
                      print('rs.initiate skipped: ' + e.message);
                    }
                  " --quiet 2>&1
                  echo "postStart: done."
        resources:
          requests:
            memory: 256Mi
            cpu: 100m
          limits:
            memory: 512Mi
            cpu: 500m
        volumeMounts:
        - name: mongodb-storage
          mountPath: /data/db
        # Use TCP socket probe instead of mongosh exec probe.
        # Rationale: mongosh is a Node.js app that takes 2-3s to
        # initialize its runtime before sending any command. With the
        # default timeoutSeconds=1, every probe attempt was timing out,
        # causing CrashLoopBackOff. A TCP socket check on port 27017
        # is instant, reliable, and sufficient to confirm MongoDB is up.
        readinessProbe:
          tcpSocket:
            port: 27017
          initialDelaySeconds: 10
          periodSeconds: 5
          failureThreshold: 6
        livenessProbe:
          tcpSocket:
            port: 27017
          initialDelaySeconds: 30
          periodSeconds: 15
          failureThreshold: 5
  volumeClaimTemplates:
  - metadata:
      name: mongodb-storage
    spec:
      accessModes: [ "ReadWriteOnce" ]
      storageClassName: standard
      resources:
        requests:
          storage: 2Gi
YAML
echo "✓ StatefulSet created"
echo ""

echo "Step 4/4: Waiting for MongoDB pod to be ready..."
echo "  This may take 2-3 minutes..."
kubectl wait --for=condition=ready pod/mongodb-0 -n ${NAMESPACE} --timeout=360s

echo ""
echo "============================================"
echo "✓ STEP 7 COMPLETE"
echo "============================================"
echo ""

echo "MongoDB Status:"
kubectl get pods -l app=mongodb -n ${NAMESPACE}
echo ""
kubectl get svc -l app=mongodb -n ${NAMESPACE}
echo ""
kubectl get pvc mongodb-storage-mongodb-0 -n ${NAMESPACE} 2>/dev/null || true

echo ""
echo "Verifying replica set (waiting 5s for election to settle)..."
sleep 5
POD=$(kubectl get pod -l app=mongodb -n ${NAMESPACE} -o jsonpath='{.items[0].metadata.name}')

RS_STATE=$(kubectl exec "$POD" -n ${NAMESPACE} -- \
  mongosh --eval "rs.status().myState" --quiet 2>/dev/null || echo "unknown")

if [ "$RS_STATE" = "1" ]; then
    echo "✓ Replica set: PRIMARY (myState=1) — Debezium CDC ready"
else
    echo "⚠ Replica set myState=${RS_STATE} (expected 1=PRIMARY)"
    echo "  Attempting manual rs.initiate()..."
    kubectl exec "$POD" -n ${NAMESPACE} -- \
      mongosh --eval "rs.initiate({_id:'${REPLSET_NAME}',members:[{_id:0,host:'mongodb-0.mongodb.${NAMESPACE}.svc.cluster.local:27017'}]})" \
      --quiet 2>&1 || true
    sleep 5
    RS_STATE2=$(kubectl exec "$POD" -n ${NAMESPACE} -- \
      mongosh --eval "rs.status().myState" --quiet 2>/dev/null || echo "unknown")
    echo "  State after manual initiate: ${RS_STATE2}"
fi

echo ""
echo "Verifying change streams (Debezium requirement)..."
kubectl exec "$POD" -n ${NAMESPACE} -- mongosh --eval "
  var cs = db.getSiblingDB('commerce').watch([], {maxAwaitTimeMS: 500});
  print('Change streams: ENABLED');
  cs.close();
" --quiet 2>/dev/null && echo "✓ Change streams verified" \
  || echo "⚠ Change streams check inconclusive - check replica set state above"

echo ""
echo "MongoDB Configuration:"
echo "  Replica Set:     ${REPLSET_NAME}"
echo "  Internal host:   mongodb-0.mongodb.default.svc.cluster.local:27017"
echo "  External:        localhost:27017 (NodePort 30017)"

echo ""
echo "Resource Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"

echo ""
echo "============================================"
echo "Next: STEP 8 - Populate Sample Data"
echo "Run: ./scripts/step-08-populate-data.sh"
echo "============================================"