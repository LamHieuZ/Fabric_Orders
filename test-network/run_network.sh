#!/bin/bash
set -e

# Bật network với Org1, Org2
echo ">>> Starting network with Org1 & Org2..."
./network.sh up createChannel -ca -s couchdb



# Deploy chaincode cho cả Org1, Org2, Org3
echo ">>> Deploying chaincode..."
./network.sh deployCC \
  -ccn mycc \
  -ccp ../chaincode-go \
  -ccl go \
  -cccg ../chaincode-go/collections_config.json

echo ">>> Done!"
