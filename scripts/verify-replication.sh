#!/bin/bash
# ============================================
# Verify CDC Replication Status
# ============================================
# Checks connector status, topic offsets,
# and compares row counts between Oracle and PostgreSQL

set -e

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
KAFKA_BROKER="${KAFKA_BROKER:-localhost:29092}"
CONNECTOR_NAME="oracle-ecom-connector"

echo "============================================"
echo "O2Kafka Replication Verification"
echo "============================================"
echo ""

# 1. Check connector status
echo "1. Connector Status"
echo "--------------------------------------------"
STATUS=$(curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status")
CONNECTOR_STATE=$(echo "$STATUS" | jq -r '.connector.state' 2>/dev/null)
TASK_STATE=$(echo "$STATUS" | jq -r '.tasks[0].state' 2>/dev/null)

echo "Connector: $CONNECTOR_STATE"
echo "Task 0: $TASK_STATE"

if [ "$CONNECTOR_STATE" != "RUNNING" ] || [ "$TASK_STATE" != "RUNNING" ]; then
    echo ""
    echo "WARNING: Connector or task is not running!"
    echo "Full status:"
    echo "$STATUS" | jq . 2>/dev/null || echo "$STATUS"
fi
echo ""

# 2. Check topic message counts
echo "2. Topic Message Counts"
echo "--------------------------------------------"
TOPICS=(
    "o2k.ECOM.CATEGORIES"
    "o2k.ECOM.PRODUCTS"
    "o2k.ECOM.INVENTORY"
    "o2k.ECOM.CUSTOMERS"
    "o2k.ECOM.ORDERS"
    "o2k.ECOM.ORDER_ITEMS"
    "o2k.ECOM.PAYMENTS"
    "o2k.ECOM.SHIPMENTS"
    "o2k.ECOM.SHIPMENT_ITEMS"
    "o2k.ECOM.RETURNS"
    "o2k.transaction"
)

printf "%-30s %10s\n" "Topic" "Messages"
printf "%-30s %10s\n" "-----" "--------"

for topic in "${TOPICS[@]}"; do
    # Get latest offset (approximate message count for single partition)
    OFFSET=$(kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list "$KAFKA_BROKER" \
        --topic "$topic" \
        --time -1 2>/dev/null | awk -F: '{sum+=$3} END {print sum}')
    printf "%-30s %10s\n" "$topic" "${OFFSET:-0}"
done
echo ""

# 3. Check transaction topic for BEGIN/END markers
echo "3. Recent Transactions"
echo "--------------------------------------------"
echo "Last 5 transaction markers:"
kafka-console-consumer \
    --bootstrap-server "$KAFKA_BROKER" \
    --topic "o2k.transaction" \
    --from-beginning \
    --max-messages 10 \
    --timeout-ms 5000 2>/dev/null | \
    jq -r 'select(.status != null) | "\(.status): \(.id) (events: \(.event_count // "N/A"))"' 2>/dev/null | \
    tail -5 || echo "No transaction markers found"
echo ""

# 4. Compare row counts (if psql is available)
echo "4. PostgreSQL Row Counts"
echo "--------------------------------------------"
if command -v psql &> /dev/null; then
    PGPASSWORD=postgres psql -h localhost -U postgres -d ecom -t -c "
        SELECT * FROM ecom.get_table_counts()
        ORDER BY table_name;
    " 2>/dev/null | while read line; do
        echo "$line"
    done || echo "Could not connect to PostgreSQL"
else
    echo "psql not available - skipping PostgreSQL check"
fi
echo ""

# 5. Check for errors in connector logs
echo "5. Recent Connector Errors"
echo "--------------------------------------------"
ERRORS=$(docker logs o2k-kafka-connect 2>&1 | grep -i "error\|exception" | tail -5)
if [ -n "$ERRORS" ]; then
    echo "$ERRORS"
else
    echo "No recent errors found"
fi
echo ""

echo "============================================"
echo "Verification complete!"
echo "============================================"
