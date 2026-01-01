#!/bin/bash

# MongoDB Initialization Script
# Creates necessary users for the application

set -e

echo "🔧 Initializing MongoDB..."

# Wait for MongoDB to be ready
echo "  → Waiting for MongoDB..."
sleep 5

# Initialize replica set (if not already done)
docker exec mongodb mongosh --eval "
try {
  rs.status();
  print('Replica set already initialized');
} catch (e) {
  rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'mongodb:27017'}]});
  print('Replica set initialized');
}
" 2>/dev/null || echo "  → Replica set initialization skipped"

# Wait for replica set
sleep 3

# Create application user
echo "  → Creating application user..."
docker exec mongodb mongosh admin --eval "
try {
  db.createUser({
    user: 'mongo',
    pwd: 'mongo',
    roles: ['root']
  });
  print('User created successfully');
} catch (e) {
  if (e.code === 51003) {
    print('User already exists');
  } else {
    throw e;
  }
}
"

echo ""
echo "✅ MongoDB initialized!"
echo "   Connection: mongodb://mongo:mongo@mongodb:27017"
