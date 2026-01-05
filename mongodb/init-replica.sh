#!/bin/bash
# MongoDB Replica Set Initialization Script

echo "Waiting for MongoDB to start..."
sleep 10

echo "Initializing replica set..."
mongosh -u mongo -p mongo --authenticationDatabase admin <<EOF
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongodb:27017" }
  ]
});
EOF

echo "Waiting for replica set to be ready..."
sleep 5

echo "Creating CDC user..."
mongosh -u mongo -p mongo --authenticationDatabase admin <<EOF
use admin;
db.createUser({
  user: "cdc_user",
  pwd: "cdc_password",
  roles: [
    { role: "read", db: "mydb" },
    { role: "readAnyDatabase", db: "admin" }
  ]
});
EOF

echo "MongoDB replica set and CDC user configured successfully!"
