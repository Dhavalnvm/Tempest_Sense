"""
Live Cyclone Data Routes
Endpoints for real-time cyclone information
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from services.redis_service import RedisService
from services.clickhouse_service import ClickHouseService

logger = logging.getLogger(__name__)

router = APIRouter()


# Response models
class CyclonePosition(BaseModel):
    id: str
    name: str
    basin: str
    classification: str
    intensity: str
    latitude: float
    longitude: float
    movement_speed: float
    movement_direction: float
    central_pressure: Optional[float]
    max_sustained_wind: Optional[float]
    timestamp: str
    data_source: str = "NOAA"


class LiveCyclonesResponse(BaseModel):
    total_active: int
    cyclones: List[CyclonePosition]
    last_update: str
    source: str = "NOAA CurrentStorms API"


class StatisticsResponse(BaseModel):
    total_active: int
    total_observations_24h: int
    basins: dict
    last_update: str


@router.get("/live", response_model=LiveCyclonesResponse)
async def get_live_cyclones(
        basin: Optional[str] = Query(None, description="Filter by basin (e.g., Atlantic, Pacific)"),
        limit: int = Query(100, ge=1, le=500, description="Maximum number of cyclones to return")
):
    """
    Get currently active cyclones in real-time

    Returns the latest position and status for all active tropical cyclones
    worldwide, sourced from NOAA's National Hurricane Center.
    """
    try:
        # Try Redis first for fastest response
        redis_service = RedisService()
        cyclones = redis_service.get_all_active_cyclones()

        # If Redis is empty or unavailable, fall back to ClickHouse
        if not cyclones:
            logger.info("Redis cache empty, fetching from ClickHouse")
            ch_service = ClickHouseService()
            cyclones = ch_service.get_active_cyclones(limit=limit)

        # Filter by basin if specified
        if basin:
            cyclones = [c for c in cyclones if basin.lower() in c.get('basin', '').lower()]

        # Apply limit
        cyclones = cyclones[:limit]

        # Get last update time
        last_update = max(
            [c['timestamp'] for c in cyclones],
            default=""
        )

        return LiveCyclonesResponse(
            total_active=len(cyclones),
            cyclones=cyclones,
            last_update=last_update
        )

    except Exception as e:
        logger.error(f"Error fetching live cyclones: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/live/{storm_id}", response_model=CyclonePosition)
async def get_live_cyclone(storm_id: str):
    """
    Get real-time data for a specific cyclone

    Returns the latest position and status for the specified storm ID
    (e.g., AL012025 for Atlantic storm #1 in 2025).
    """
    try:
        # Try Redis first
        redis_service = RedisService()
        cyclone = redis_service.get_live_cyclone(storm_id)

        # Fall back to ClickHouse if not in Redis
        if not cyclone:
            ch_service = ClickHouseService()
            history = ch_service.get_cyclone_history(storm_id, hours=6)

            if not history:
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for storm {storm_id}"
                )

            # Get most recent position
            cyclone = history[-1]

            # Cache it
            redis_service.cache_cyclone(storm_id, cyclone)

        return CyclonePosition(**cyclone)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching cyclone {storm_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats", response_model=StatisticsResponse)
async def get_statistics():
    """
    Get global cyclone statistics

    Returns summary statistics including active storm counts,
    observations, and basin-level breakdowns.
    """
    try:
        # Get from ClickHouse
        ch_service = ClickHouseService()

        global_stats = ch_service.get_global_statistics()
        basin_stats = ch_service.get_basin_statistics(hours=24)

        # Get from Redis for last update time
        redis_service = RedisService()
        redis_stats = redis_service.get_statistics()

        return StatisticsResponse(
            total_active=int(global_stats.get('total_active', 0)),
            total_observations_24h=int(global_stats.get('total_observations', 0)),
            basins=basin_stats,
            last_update=redis_stats.get('last_update', '')
        )

    except Exception as e:
        logger.error(f"Error fetching statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))