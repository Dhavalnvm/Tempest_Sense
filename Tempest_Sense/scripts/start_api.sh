#!/bin/bash
# Cyclone Tracking - FastAPI Server Startup Script

set -e  # Exit on error

echo "=========================================="
echo "🌀 Starting Cyclone Tracking API"
echo "=========================================="

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Set defaults
export API_HOST=${API_HOST:-0.0.0.0}
export API_PORT=${API_PORT:-8000}
export API_WORKERS=${API_WORKERS:-4}
export CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
export CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-9000}
export REDIS_HOST=${REDIS_HOST:-localhost}
export REDIS_PORT=${REDIS_PORT:-6379}
export LOG_LEVEL=${LOG_LEVEL:-INFO}

echo "Configuration:"
echo "  API: http://$API_HOST:$API_PORT"
echo "  Workers: $API_WORKERS"
echo "  ClickHouse: $CLICKHOUSE_HOST:$CLICKHOUSE_PORT"
echo "  Redis: $REDIS_HOST:$REDIS_PORT"
echo "  Log Level: $LOG_LEVEL"
echo ""

# Wait for dependencies
echo "⏳ Waiting for ClickHouse..."
timeout 60 bash -c 'until nc -z localhost 9000 2>/dev/null; do sleep 2; done' || {
    echo "⚠️  ClickHouse not available (API may run in degraded mode)"
}

echo "⏳ Waiting for Redis..."
timeout 30 bash -c 'until nc -z localhost 6379 2>/dev/null; do sleep 2; done' || {
    echo "⚠️  Redis not available (API may run in degraded mode)"
}

# Health check before starting
echo ""
echo "🏥 Running health checks..."
python3 healthcheck.py --verify || echo "⚠️  Some components unhealthy"

# Start API server
echo ""
echo "=========================================="
echo "🚀 API Server Starting..."
echo "=========================================="
echo ""
echo "API Documentation: http://localhost:$API_PORT/docs"
echo "API Health Check: http://localhost:$API_PORT/health"
echo ""

# For development: reload on code changes
if [ "$ENVIRONMENT" = "development" ]; then
    echo "🔧 Development mode - auto-reload enabled"
    uvicorn main:app --host $API_HOST --port $API_PORT --reload --log-level info
else
    # For production: use workers
    echo "🏭 Production mode - using $API_WORKERS workers"
    uvicorn main:app --host $API_HOST --port $API_PORT --workers $API_WORKERS --log-level $LOG_LEVEL
fi