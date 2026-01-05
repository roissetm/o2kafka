#!/bin/bash
# ============================================
# Deploy Debezium Oracle Connector
# ============================================
# Deploys the Oracle CDC connector to Kafka Connect

set -e

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_CONFIG="${CONNECTOR_CONFIG:-/kafka/connectors/oracle-ecom-connector.json}"
CONNECTOR_NAME="oracle-ecom-connector"

echo "============================================"
echo "Deploying Debezium Oracle Connector"
echo "============================================"
echo "Connect URL: $CONNECT_URL"
echo "Config file: $CONNECTOR_CONFIG"
echo ""

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
until curl -s "$CONNECT_URL/connectors" > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo " Ready!"
echo ""

# Check if connector already exists
echo "Checking existing connectors..."
EXISTING=$(curl -s "$CONNECT_URL/connectors" | grep -o "$CONNECTOR_NAME" || true)

if [ -n "$EXISTING" ]; then
    echo "Connector '$CONNECTOR_NAME' already exists."
    echo ""
    read -p "Do you want to delete and recreate it? (y/N): " -n 1 -r
    echo ""

    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Deleting existing connector..."
        curl -s -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME"
        sleep 2
    else
        echo "Keeping existing connector. Exiting."
        exit 0
    fi
fi

# Deploy the connector
echo "Deploying connector from $CONNECTOR_CONFIG..."

if [ -f "$CONNECTOR_CONFIG" ]; then
    CONFIG_CONTENT=$(cat "$CONNECTOR_CONFIG")
else
    # Use default config if file doesn't exist
    CONFIG_CONTENT=$(cat <<'EOF'
{
  "name": "oracle-ecom-connector",
  "config": {
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "tasks.max": "1",
    "database.hostname": "oracle",
    "database.port": "1521",
    "database.user": "debezium",
    "database.password": "dbz_password",
    "database.dbname": "XEPDB1",
    "topic.prefix": "o2k",
    "schema.include.list": "ECOM",
    "table.include.list": "ECOM.CATEGORIES,ECOM.PRODUCTS,ECOM.INVENTORY,ECOM.CUSTOMERS,ECOM.ORDERS,ECOM.ORDER_ITEMS,ECOM.PAYMENTS,ECOM.SHIPMENTS,ECOM.SHIPMENT_ITEMS,ECOM.RETURNS",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "o2k.schema-history",
    "provide.transaction.metadata": "true",
    "tombstones.on.delete": "false",
    "log.mining.strategy": "online_catalog",
    "log.mining.query.filter.mode": "cte",
    "snapshot.mode": "initial",
    "decimal.handling.mode": "double",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true"
  }
}
EOF
)
fi

RESPONSE=$(curl -s -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d "$CONFIG_CONTENT")

echo ""
echo "Response:"
echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
echo ""

# Wait and check status
echo "Waiting for connector to start..."
sleep 5

echo ""
echo "Connector Status:"
curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq . 2>/dev/null || \
    curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"

echo ""
echo "============================================"
echo "Deployment complete!"
echo "============================================"
