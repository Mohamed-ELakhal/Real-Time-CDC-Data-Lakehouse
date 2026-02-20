#!/bin/bash

set -e

echo "============================================"
echo "STEP 8: Populate Sample Data"
echo "============================================"
echo ""

echo "Verifying cluster access..."
if ! kubectl cluster-info &>/dev/null; then
    echo "❌ Cannot access cluster"
    exit 1
fi

PG_POD=$(kubectl get pod -l app=postgres -n default -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
MG_POD=$(kubectl get pod -l app=mongodb  -n default -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [ -z "$PG_POD" ]; then echo "❌ PostgreSQL pod not found"; exit 1; fi
if [ -z "$MG_POD" ]; then echo "❌ MongoDB pod not found";   exit 1; fi

echo "✓ PostgreSQL pod: $PG_POD"
echo "✓ MongoDB pod:    $MG_POD"
echo ""

# ============================================================
# PART 1: PostgreSQL — users table
# ============================================================
echo "============================================"
echo "PART 1: PostgreSQL — users table"
echo "============================================"
echo ""

echo "Step 1/3: Creating users table and inserting sample data..."
# Pass SQL via -c flag as a single string to avoid heredoc/stdin issues with kubectl exec.
# ON_ERROR_STOP=1 makes psql exit non-zero on any SQL error so set -e catches it.
kubectl exec "$PG_POD" -n default -- \
  psql -U postgres -d commerce -v ON_ERROR_STOP=1 -c "
    CREATE TABLE IF NOT EXISTS users (
        user_id    SERIAL PRIMARY KEY,
        full_name  VARCHAR(255) NOT NULL,
        email      VARCHAR(255) NOT NULL UNIQUE,
        created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
    );

    INSERT INTO users (full_name, email, created_at, updated_at) VALUES
        ('Alice Johnson',  'alice@example.com',  NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days'),
        ('Bob Smith',      'bob@example.com',    NOW() - INTERVAL '25 days', NOW() - INTERVAL '25 days'),
        ('Carol Williams', 'carol@example.com',  NOW() - INTERVAL '20 days', NOW() - INTERVAL '20 days'),
        ('David Brown',    'david@example.com',  NOW() - INTERVAL '18 days', NOW() - INTERVAL '18 days'),
        ('Eva Martinez',   'eva@example.com',    NOW() - INTERVAL '15 days', NOW() - INTERVAL '15 days'),
        ('Frank Lee',      'frank@example.com',  NOW() - INTERVAL '12 days', NOW() - INTERVAL '12 days'),
        ('Grace Kim',      'grace@example.com',  NOW() - INTERVAL '10 days', NOW() - INTERVAL '10 days'),
        ('Henry Davis',    'henry@example.com',  NOW() - INTERVAL '7 days',  NOW() - INTERVAL '7 days'),
        ('Iris Chen',      'iris@example.com',   NOW() - INTERVAL '3 days',  NOW() - INTERVAL '3 days'),
        ('Jack Wilson',    'jack@example.com',   NOW() - INTERVAL '1 day',   NOW() - INTERVAL '1 day')
    ON CONFLICT (email) DO NOTHING;

    SELECT COUNT(*) AS users_inserted FROM users;
  "
echo "✓ Users inserted"
echo ""

echo "Step 2/3: Performing UPDATE operations (tests CDC update capture)..."
kubectl exec "$PG_POD" -n default -- \
  psql -U postgres -d commerce -v ON_ERROR_STOP=1 -c "
    UPDATE users SET email = 'alice.johnson@example.com', updated_at = NOW()
      WHERE email = 'alice@example.com';

    UPDATE users SET full_name = 'Robert Smith', updated_at = NOW()
      WHERE email = 'bob@example.com';

    UPDATE users SET email = 'eva.m@example.com', updated_at = NOW()
      WHERE email = 'eva@example.com';

    SELECT user_id, full_name, email, updated_at FROM users ORDER BY user_id;
  "
echo "✓ Updates applied"
echo ""

echo "Step 3/3: Performing DELETE operations (soft-deleted in ClickHouse silver layer)..."
kubectl exec "$PG_POD" -n default -- \
  psql -U postgres -d commerce -v ON_ERROR_STOP=1 -c "
    DELETE FROM users WHERE email = 'jack@example.com';
    SELECT COUNT(*) AS users_remaining FROM users;
  "
echo "✓ Deletes applied"
echo ""

# ============================================================
# PART 2: MongoDB — events collection
# ============================================================
echo "============================================"
echo "PART 2: MongoDB — events collection"
echo "============================================"
echo ""

echo "Inserting user action events into MongoDB..."
kubectl exec "$MG_POD" -n default -- mongosh --eval '
  db = db.getSiblingDB("commerce");

  // Drop for idempotency on re-runs
  db.events.drop();

  const now  = new Date();
  const hour = 3600000;
  const day  = 86400000;

  const events = [
    // Alice (user_id=1) — active shopper
    { user_id: 1, event_type: "login",           page: null,          metadata: { ip: "192.168.1.10" },                 created_at: new Date(now - 2*day) },
    { user_id: 1, event_type: "page_view",        page: "/products",   metadata: { duration_secs: 45 },                  created_at: new Date(now - 2*day + hour) },
    { user_id: 1, event_type: "add_to_cart",      page: "/cart",       metadata: { product_id: "PROD-001", qty: 2 },     created_at: new Date(now - 2*day + 2*hour) },
    { user_id: 1, event_type: "purchase",         page: "/checkout",   metadata: { order_id: "ORD-1001", total: 89.99 }, created_at: new Date(now - 2*day + 3*hour) },

    // Bob (user_id=2) — browser only
    { user_id: 2, event_type: "login",            page: null,          metadata: { ip: "192.168.1.11" },                 created_at: new Date(now - 1*day) },
    { user_id: 2, event_type: "page_view",        page: "/products",   metadata: { duration_secs: 120 },                 created_at: new Date(now - 1*day + hour) },
    { user_id: 2, event_type: "page_view",        page: "/sale",       metadata: { duration_secs: 200 },                 created_at: new Date(now - 1*day + 2*hour) },

    // Carol (user_id=3)
    { user_id: 3, event_type: "login",            page: null,          metadata: { ip: "192.168.1.12" },                 created_at: new Date(now - 1*day) },
    { user_id: 3, event_type: "add_to_cart",      page: "/cart",       metadata: { product_id: "PROD-007", qty: 1 },     created_at: new Date(now - 1*day + hour) },
    { user_id: 3, event_type: "remove_from_cart", page: "/cart",       metadata: { product_id: "PROD-007" },             created_at: new Date(now - 1*day + 2*hour) },

    // David (user_id=4)
    { user_id: 4, event_type: "login",            page: null,          metadata: { ip: "10.0.0.4" },                     created_at: new Date(now - 3*hour) },
    { user_id: 4, event_type: "page_view",        page: "/about",      metadata: { duration_secs: 15 },                  created_at: new Date(now - 2*hour) },

    // Eva (user_id=5)
    { user_id: 5, event_type: "login",            page: null,          metadata: { ip: "10.0.0.5" },                     created_at: new Date(now - 5*day) },
    { user_id: 5, event_type: "add_to_cart",      page: "/cart",       metadata: { product_id: "PROD-042", qty: 3 },     created_at: new Date(now - 5*day + hour) },
    { user_id: 5, event_type: "purchase",         page: "/checkout",   metadata: { order_id: "ORD-1002", total: 149.50 },created_at: new Date(now - 5*day + 2*hour) },

    // Frank (user_id=6)
    { user_id: 6, event_type: "login",            page: null,          metadata: { ip: "10.0.0.6" },                     created_at: new Date(now - 4*day) },
    { user_id: 6, event_type: "page_view",        page: "/new-arrivals",metadata: { duration_secs: 300 },                created_at: new Date(now - 4*day + hour) },

    // Grace (user_id=7)
    { user_id: 7, event_type: "login",            page: null,          metadata: { ip: "10.0.0.7" },                     created_at: new Date(now - 2*hour) },
    { user_id: 7, event_type: "page_view",        page: "/products",   metadata: { duration_secs: 80 },                  created_at: new Date(now - hour) },
    { user_id: 7, event_type: "add_to_cart",      page: "/cart",       metadata: { product_id: "PROD-015", qty: 1 },     created_at: new Date(now - 30*60*1000) },

    // Henry (user_id=8)
    { user_id: 8, event_type: "login",            page: null,          metadata: { ip: "10.0.0.8" },                     created_at: new Date(now - 6*day) },
    { user_id: 8, event_type: "page_view",        page: "/sale",       metadata: { duration_secs: 250 },                 created_at: new Date(now - 6*day + hour) },
    { user_id: 8, event_type: "purchase",         page: "/checkout",   metadata: { order_id: "ORD-1003", total: 59.99 }, created_at: new Date(now - 6*day + 2*hour) },

    // Iris (user_id=9)
    { user_id: 9, event_type: "login",            page: null,          metadata: { ip: "10.0.0.9" },                     created_at: new Date(now - hour) },
    { user_id: 9, event_type: "page_view",        page: "/products",   metadata: { duration_secs: 60 },                  created_at: new Date(now - 30*60*1000) },

    // Jack (user_id=10, deleted from PG) still has events — tests orphan handling in gold layer JOIN
    { user_id: 10, event_type: "login",           page: null,          metadata: { ip: "10.0.0.10" },                    created_at: new Date(now - 10*day) },
    { user_id: 10, event_type: "page_view",       page: "/products",   metadata: { duration_secs: 30 },                  created_at: new Date(now - 10*day + hour) },
  ];

  const result = db.events.insertMany(events);
  // insertedIds is a plain object in mongosh 2.x, not an array
  print("Inserted " + Object.keys(result.insertedIds).length + " events into commerce.events");

  db.events.createIndex({ user_id: 1 });
  db.events.createIndex({ created_at: -1 });
  print("Indexes created.");

  print("Event type breakdown:");
  db.events.aggregate([
    { $group: { _id: "$event_type", count: { $sum: 1 } } },
    { $sort:  { count: -1 } }
  ]).forEach(r => print("  " + r._id + ": " + r.count));
' --quiet
echo "✓ Events inserted"
echo ""

# ============================================================
# SUMMARY
# ============================================================
echo "============================================"
echo "✓ STEP 8 COMPLETE — Final Data Summary"
echo "============================================"
echo ""

echo "PostgreSQL — users table (commerce database):"
kubectl exec "$PG_POD" -n default -- \
  psql -U postgres -d commerce -v ON_ERROR_STOP=1 -c \
  "SELECT user_id, full_name, email FROM users ORDER BY user_id;"

echo ""
echo "MongoDB — events collection (commerce database):"
kubectl exec "$MG_POD" -n default -- mongosh --eval '
  db = db.getSiblingDB("commerce");
  print("Total events:            " + db.events.countDocuments());
  print("Unique users with events:" + db.events.distinct("user_id").length);
  print("Collections:             " + db.getCollectionNames().join(", "));
' --quiet

echo ""
echo "============================================"
echo "Next: STEP 9 - Deploy Kafka Cluster"
echo "Run: ./scripts/step-09-deploy-kafka.sh"
echo "============================================"