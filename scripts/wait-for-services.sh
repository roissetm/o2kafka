#!/bin/bash
# ============================================
# Wait for All Services to be Ready
# ============================================
# Waits for all O2Kafka services to be healthy
# before proceeding with connector deployment

set -e

TIMEOUT="${TIMEOUT:-300}"  # 5 minutes default
INTERVAL="${INTERVAL:-5}"

echo "============================================"
echo "Waiting for O2Kafka Services"
echo "============================================"
echo "Timeout: ${TIMEOUT}s"
echo ""

wait_for_service() {
    local name=$1
    local check_cmd=$2
    local start_time=$(date +%s)

    echo -n "Waiting for $name... "

    while true; do
        if eval "$check_cmd" > /dev/null 2>&1; then
            echo "ready!"
            return 0
        fi

        local elapsed=$(($(date +%s) - start_time))
        if [ $elapsed -ge $TIMEOUT ]; then
            echo "timeout!"
            return 1
        fi

        echo -n "."
        sleep $INTERVAL
    done
}

# Check each service
echo "Checking services..."
echo "--------------------------------------------"

wait_for_service "Zookeeper" "nc -z localhost 2181"
wait_for_service "Kafka" "nc -z localhost 29092"
wait_for_service "Schema Registry" "curl -sf http://localhost:8081/subjects"
wait_for_service "PostgreSQL" "pg_isready -h localhost -p 5432 -U postgres"
wait_for_service "Oracle" "echo 'SELECT 1 FROM DUAL;' | sqlplus -s system/oracle_pwd@localhost:1521/XEPDB1"
wait_for_service "Kafka Connect" "curl -sf http://localhost:8083/connectors"

echo ""
echo "============================================"
echo "All services are ready!"
echo "============================================"
