#!/bin/bash
# Cyclone Tracking - Consumer Startup Script

set -e  # Exit on error

echo "=========================================="
echo "🌀 Starting Cyclone Data Consumer"
echo "=========================================="

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Set defaults
export KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
export CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
export CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-9000}
export CLICKHOUSE_USER=${CLICKHOUSE_USER:-admin}
export CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-admin123}
export CLICKHOUSE_DATABASE=${CLICKHOUSE_DATABASE:-cyclones}
export REDIS_HOST=${REDIS_HOST:-localhost}
export REDIS_PORT=${REDIS_PORT:-6379}

echo "Configuration:"
echo "  Kafka: $KAFKA_BOOTSTRAP_SERVERS"
echo "  ClickHouse: $CLICKHOUSE_HOST:$CLICKHOUSE_PORT/$CLICKHOUSE_DATABASE"
echo "  Redis: $REDIS_HOST:$REDIS_PORT"
echo ""

# Wait for dependencies
echo "⏳ Waiting for Kafka..."
timeout 60 bash -c 'until nc -z localhost 9092 2>/dev/null; do sleep 2; done' || {
    echo "❌ Kafka not available after 60s"
    exit 1
}
echo "✅ Kafka is ready"

echo "⏳ Waiting for ClickHouse..."
timeout 60 bash -c 'until nc -z localhost 9000 2>/dev/null; do sleep 2; done' || {
    echo "❌ ClickHouse not available after 60s"
    exit 1
}
echo "✅ ClickHouse is ready"

echo "⏳ Waiting for Redis..."
timeout 30 bash -c 'until nc -z localhost 6379 2>/dev/null; do sleep 2; done' || {
    echo "⚠️  Redis not available (optional, continuing...)"
}

# Initialize database
echo ""
echo "📊 Initializing ClickHouse database..."
python3 init_db.py || {
    echo "⚠️  Database initialization failed or already exists"
}

# Start consumer
echo ""
echo "=========================================="
echo "🚀 Consumer Starting..."
echo "=========================================="
echo ""

python3 consumer.py