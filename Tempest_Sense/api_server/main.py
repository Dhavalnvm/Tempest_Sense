"""
FastAPI Application - Real-time Cyclone Tracking API
Provides endpoints for live cyclone data, historical tracks, and forecasts
"""
import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Lifecycle management
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup
    logger.info("Starting Cyclone Tracking API...")
    logger.info(f"API Version: {app.version}")

    # Initialize connections
    from services.clickhouse_service import ClickHouseService
    from services.redis_service import RedisService

    try:
        ch_service = ClickHouseService()
        ch_service.test_connection()
        logger.info("ClickHouse connection verified")
    except Exception as e:
        logger.warning(f"ClickHouse connection failed: {e}")

    try:
        redis_service = RedisService()
        redis_service.test_connection()
        logger.info("Redis connection verified")
    except Exception as e:
        logger.warning(f"Redis connection failed: {e}")

    yield

    # Shutdown
    logger.info("Shutting down Cyclone Tracking API...")


# Create FastAPI app
app = FastAPI(
    title="Cyclone Real-Time Tracking API",
    description="Real-time cyclone tracking and prediction system powered by NOAA data",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import routes
from routes import live, history, forecast

# Register routes
app.include_router(live.router, prefix="/api/v1/cyclones", tags=["Live Data"])
app.include_router(history.router, prefix="/api/v1/cyclones", tags=["Historical Data"])
app.include_router(forecast.router, prefix="/api/v1/cyclones", tags=["Forecasts"])


# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    """API root endpoint"""
    return {
        "service": "Cyclone Real-Time Tracking API",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "live": "/api/v1/cyclones/live",
            "history": "/api/v1/cyclones/history/{storm_id}",
            "forecast": "/api/v1/cyclones/forecast/{storm_id}",
            "stats": "/api/v1/cyclones/stats",
            "health": "/health"
        }
    }


# Health check endpoint
@app.get("/health", tags=["Health"])
async def health_check():
    """System health check"""
    from services.clickhouse_service import ClickHouseService
    from services.redis_service import RedisService

    health = {
        "status": "healthy",
        "services": {}
    }

    # Check ClickHouse
    try:
        ch_service = ClickHouseService()
        ch_service.test_connection()
        health["services"]["clickhouse"] = "healthy"
    except Exception as e:
        health["services"]["clickhouse"] = f"unhealthy: {str(e)}"
        health["status"] = "degraded"

    # Check Redis
    try:
        redis_service = RedisService()
        redis_service.test_connection()
        health["services"]["redis"] = "healthy"
    except Exception as e:
        health["services"]["redis"] = f"unhealthy: {str(e)}"
        health["status"] = "degraded"

    status_code = 200 if health["status"] != "unhealthy" else 503
    return JSONResponse(content=health, status_code=status_code)


# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail, "status_code": exc.status_code}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "message": str(exc)}
    )


if __name__ == "__main__":
    # Get config from environment
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    workers = int(os.getenv("API_WORKERS", "4"))

    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        workers=1,  # Use 1 for development, increase for production
        reload=True,
        log_level="info"
    )