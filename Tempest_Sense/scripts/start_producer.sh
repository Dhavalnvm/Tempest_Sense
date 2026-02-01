#!/bin/bash
# Cyclone Tracking - Producer Startup Script

set -e  # Exit on error

echo "=========================================="
echo "🌀 Starting Cyclone Data Producer"
echo "=========================================="

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Set defaults
export KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
export NOAA_API_URL=${NOAA_API_URL:-https://www.nhc.noaa.gov/CurrentStorms.json}
export NOAA_FETCH_INTERVAL=${NOAA_FETCH_INTERVAL:-300}
export REDIS_HOST=${REDIS_HOST:-localhost}
export REDIS_PORT=${REDIS_PORT:-6379}

echo "Configuration:"
echo "  Kafka: $KAFKA_BOOTSTRAP_SERVERS"
echo "  NOAA API: $NOAA_API_URL"
echo "  Fetch Interval: ${NOAA_FETCH_INTERVAL}s"
echo "  Redis: $REDIS_HOST:$REDIS_PORT"
echo ""

# Wait for dependencies
echo "⏳ Waiting for Kafka..."
timeout 60 bash -c 'until nc -z localhost 9092 2>/dev/null; do sleep 2; done' || {
    echo "❌ Kafka not available after 60s"
    exit 1
}
echo "✅ Kafka is ready"

echo "⏳ Waiting for Redis..."
timeout 30 bash -c 'until nc -z localhost 6379 2>/dev/null; do sleep 2; done' || {
    echo "⚠️  Redis not available (optional, continuing...)"
}
echo "✅ Redis is ready"

# Create Kafka topics
echo ""
echo "📋 Creating Kafka topics..."
python3 topics.py || echo "⚠️  Topics may already exist"

# Start producer
echo ""
echo "=========================================="
echo "🚀 Producer Starting..."
echo "=========================================="
echo ""

python3 producer.py