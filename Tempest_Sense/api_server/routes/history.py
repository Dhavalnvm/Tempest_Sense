"""
Historical Cyclone Data Routes
Endpoints for cyclone track history and metadata
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from datetime import datetime

from services.clickhouse_service import ClickHouseService

logger = logging.getLogger(__name__)

router = APIRouter()


# Response models
class HistoricalPosition(BaseModel):
    latitude: float
    longitude: float
    max_sustained_wind: Optional[float]
    central_pressure: Optional[float]
    timestamp: str


class CycloneMetadata(BaseModel):
    id: str
    name: str
    basin: str
    formation_date: Optional[str]
    dissipation_date: Optional[str]
    peak_intensity: str
    peak_wind: Optional[float]
    min_pressure: Optional[float]
    total_advisories: int
    is_active: bool


class HistoricalTrackResponse(BaseModel):
    storm_id: str
    storm_name: str
    total_points: int
    time_range: dict
    track: List[HistoricalPosition]
    metadata: Optional[CycloneMetadata]


@router.get("/history/{storm_id}", response_model=HistoricalTrackResponse)
async def get_cyclone_history(
        storm_id: str,
        hours: int = Query(72, ge=1, le=720, description="Number of hours of history to retrieve")
):
    """
    Get historical track for a specific cyclone

    Returns the complete movement history for the specified storm,
    including position, intensity, and pressure at each observation point.
    """
    try:
        ch_service = ClickHouseService()

        # Get historical positions
        history = ch_service.get_cyclone_history(storm_id, hours=hours)

        if not history:
            raise HTTPException(
                status_code=404,
                detail=f"No historical data found for storm {storm_id}"
            )

        # Get metadata
        metadata = ch_service.get_cyclone_metadata(storm_id)

        # Calculate time range
        timestamps = [h['timestamp'] for h in history]
        time_range = {
            'start': min(timestamps),
            'end': max(timestamps),
            'duration_hours': hours
        }

        # Extract storm name from first position
        storm_name = history[0].get('name', 'UNNAMED')

        return HistoricalTrackResponse(
            storm_id=storm_id,
            storm_name=storm_name,
            total_points=len(history),
            time_range=time_range,
            track=history,
            metadata=metadata
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching history for {storm_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metadata/{storm_id}", response_model=CycloneMetadata)
async def get_cyclone_metadata(storm_id: str):
    """
    Get metadata and lifecycle information for a cyclone

    Returns formation date, peak intensity, status, and other
    lifecycle information for the specified storm.
    """
    try:
        ch_service = ClickHouseService()
        metadata = ch_service.get_cyclone_metadata(storm_id)

        if not metadata:
            raise HTTPException(
                status_code=404,
                detail=f"No metadata found for storm {storm_id}"
            )

        return CycloneMetadata(**metadata)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching metadata for {storm_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/track/{storm_id}/geojson")
async def get_track_geojson(
        storm_id: str,
        hours: int = Query(72, ge=1, le=720)
):
    """
    Get cyclone track in GeoJSON format

    Returns the historical track as a GeoJSON LineString,
    ready for direct use in mapping libraries.
    """
    try:
        ch_service = ClickHouseService()
        history = ch_service.get_cyclone_history(storm_id, hours=hours)

        if not history:
            raise HTTPException(
                status_code=404,
                detail=f"No track data found for storm {storm_id}"
            )

        # Create GeoJSON LineString
        coordinates = [
            [h['longitude'], h['latitude']]
            for h in history
        ]

        # Add properties for each point
        properties = []
        for h in history:
            properties.append({
                'timestamp': h['timestamp'],
                'wind_speed': h.get('max_sustained_wind'),
                'pressure': h.get('central_pressure')
            })

        geojson = {
            'type': 'Feature',
            'geometry': {
                'type': 'LineString',
                'coordinates': coordinates
            },
            'properties': {
                'storm_id': storm_id,
                'storm_name': history[0].get('name', 'UNNAMED'),
                'total_points': len(history),
                'points': properties
            }
        }

        return geojson

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating GeoJSON for {storm_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))