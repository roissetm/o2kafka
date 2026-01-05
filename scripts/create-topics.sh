#!/bin/bash
# ============================================
# Create Kafka Topics for O2Kafka CDC
# ============================================
# Creates all required topics with appropriate
# partition counts and retention settings

set -e

KAFKA_BROKER="${KAFKA_BROKER:-kafka:9092}"
RETENTION_MS="${RETENTION_MS:-604800000}"  # 7 days

echo "============================================"
echo "Creating Kafka Topics for O2Kafka"
echo "============================================"
echo "Broker: $KAFKA_BROKER"
echo "Retention: $RETENTION_MS ms ($(($RETENTION_MS / 86400000)) days)"
echo ""

# Topic configurations: topic_name:partitions
declare -A TOPICS=(
    ["o2k.ECOM.CATEGORIES"]=1
    ["o2k.ECOM.PRODUCTS"]=3
    ["o2k.ECOM.INVENTORY"]=3
    ["o2k.ECOM.CUSTOMERS"]=3
    ["o2k.ECOM.ORDERS"]=6
    ["o2k.ECOM.ORDER_ITEMS"]=6
    ["o2k.ECOM.PAYMENTS"]=3
    ["o2k.ECOM.SHIPMENTS"]=3
    ["o2k.ECOM.SHIPMENT_ITEMS"]=3
    ["o2k.ECOM.RETURNS"]=3
    ["o2k.transaction"]=1
    ["o2k.schema-history"]=1
)

# Internal Kafka Connect topics
declare -A CONNECT_TOPICS=(
    ["_connect-configs"]=1
    ["_connect-offsets"]=25
    ["_connect-status"]=5
)

create_topic() {
    local topic=$1
    local partitions=$2
    local config_opts=$3

    echo -n "Creating topic '$topic' with $partitions partition(s)... "

    if kafka-topics --bootstrap-server "$KAFKA_BROKER" --list | grep -q "^${topic}$"; then
        echo "already exists"
    else
        kafka-topics --create \
            --bootstrap-server "$KAFKA_BROKER" \
            --topic "$topic" \
            --partitions "$partitions" \
            --replication-factor 1 \
            $config_opts \
            2>/dev/null && echo "created" || echo "failed"
    fi
}

echo "Creating CDC topics..."
echo "--------------------------------------------"
for topic in "${!TOPICS[@]}"; do
    create_topic "$topic" "${TOPICS[$topic]}" "--config retention.ms=$RETENTION_MS"
done

echo ""
echo "Creating Kafka Connect internal topics..."
echo "--------------------------------------------"
for topic in "${!CONNECT_TOPICS[@]}"; do
    create_topic "$topic" "${CONNECT_TOPICS[$topic]}" "--config cleanup.policy=compact"
done

echo ""
echo "============================================"
echo "Topic creation complete!"
echo "============================================"
echo ""
echo "Listing all topics:"
kafka-topics --bootstrap-server "$KAFKA_BROKER" --list | grep -E "^o2k\.|^_connect" | sort
