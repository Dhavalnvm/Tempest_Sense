"""
Cyclone Forecast Routes
Endpoints for cyclone trajectory forecasts with ML capabilities
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from services.clickhouse_service import ClickHouseService
from services.redis_service import RedisService
from services.forecast_service import ForecastService
from services.ml_forecast_service import MLCycloneForecast  # NEW

logger = logging.getLogger(__name__)

router = APIRouter()


# Response models
class ForecastPoint(BaseModel):
    forecast_hour: int
    forecast_timestamp: str
    latitude: float
    longitude: float
    max_wind: Optional[float]
    min_pressure: Optional[float] = None
    forecast_type: str
    confidence: Optional[str] = None
    uncertainty_bounds: Optional[dict] = None  # NEW: For ML predictions


class ForecastResponse(BaseModel):
    storm_id: str
    storm_name: str
    issued_at: str
    total_points: int
    forecast_hours: int
    forecast: List[ForecastPoint]
    methods: List[str]
    model_info: Optional[dict] = None  # NEW: ML model metadata


class ForecastGeoJSON(BaseModel):
    type: str = "Feature"
    geometry: dict
    properties: dict


class FormationPrediction(BaseModel):  # NEW
    """Cyclone formation prediction"""
    formation_probability: float
    risk_level: str
    potential_location: Optional[dict]
    estimated_time_hours: Optional[int]
    confidence: str
    factors: dict


@router.get("/forecast/{storm_id}", response_model=ForecastResponse)
async def get_cyclone_forecast(
        storm_id: str,
        hours: int = Query(48, ge=6, le=120, description="Forecast duration in hours"),
        method: str = Query("auto", description="Forecast method: auto, ml, extrapolation, or persistence")
):
    """
    Get forecast trajectory for a specific cyclone

    Returns predicted path for the next 6-120 hours using:
    - **ml**: Prophet (trajectory) + LSTM (intensity) hybrid model
    - **extrapolation**: Simple movement-based extrapolation
    - **persistence**: Stationary assumption
    - **auto**: Automatically choose best method (ML if available)
    """
    try:
        # Check cache first
        redis_service = RedisService()
        cache_key = f"{storm_id}_{method}_{hours}"
        cached_forecast = redis_service.get_cached_forecast(cache_key)

        if cached_forecast and method == "auto":
            logger.info(f"Returning cached forecast for {storm_id}")
            return ForecastResponse(
                storm_id=storm_id,
                storm_name=cached_forecast[0].get('name', 'UNNAMED'),
                issued_at=cached_forecast[0].get('forecast_timestamp', ''),
                total_points=len(cached_forecast),
                forecast_hours=hours,
                forecast=cached_forecast,
                methods=['cached']
            )

        # Get historical data
        ch_service = ClickHouseService()
        history = ch_service.get_cyclone_history(storm_id, hours=72)  # Get more history for ML

        if not history:
            raise HTTPException(
                status_code=404,
                detail=f"No historical data found for storm {storm_id}"
            )

        forecast_service = ForecastService()
        ml_service = MLCycloneForecast()
        methods_used = []
        forecast = []
        model_info = None

        # Generate forecast based on method
        if method == "ml" or (method == "auto" and len(history) >= 10):
            # ML-based forecast (requires sufficient data)
            try:
                logger.info(f"Generating ML forecast for {storm_id}")
                forecast = ml_service.hybrid_forecast(
                    history,
                    hours_ahead=hours,
                    interval_hours=6
                )
                methods_used.append('prophet_lstm_hybrid')
                model_info = {
                    'trajectory_model': 'Facebook Prophet',
                    'intensity_model': 'LSTM Neural Network',
                    'training_samples': len(history),
                    'confidence': 'high' if len(history) >= 20 else 'medium'
                }
                logger.info(f"✅ ML forecast generated with {len(forecast)} points")
            except Exception as e:
                logger.warning(f"ML forecast failed: {e}, falling back to extrapolation")
                # Fallback to extrapolation if ML fails
                forecast = forecast_service.simple_extrapolation_forecast(
                    history,
                    hours_ahead=hours,
                    interval_hours=6
                )
                methods_used.append('extrapolation_fallback')

        elif method == "persistence":
            # Simple persistence forecast
            forecast = forecast_service.persistence_forecast(
                history[-1],
                hours_ahead=hours
            )
            methods_used.append('persistence')

        elif method == "extrapolation" or (method == "auto" and len(history) >= 2):
            # Extrapolation forecast
            forecast = forecast_service.simple_extrapolation_forecast(
                history,
                hours_ahead=hours,
                interval_hours=6
            )
            methods_used.append('extrapolation')

        else:
            # Fall back to persistence if insufficient data
            logger.warning(f"Insufficient data for {method}, using persistence")
            forecast = forecast_service.persistence_forecast(
                history[-1],
                hours_ahead=hours
            )
            methods_used.append('persistence_fallback')

        if not forecast:
            raise HTTPException(
                status_code=500,
                detail="Failed to generate forecast"
            )

        # Cache the forecast
        redis_service.cache_forecast(cache_key, forecast)

        return ForecastResponse(
            storm_id=storm_id,
            storm_name=history[-1].get('name', 'UNNAMED'),
            issued_at=history[-1]['timestamp'],
            total_points=len(forecast),
            forecast_hours=hours,
            forecast=forecast,
            methods=methods_used,
            model_info=model_info
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating forecast for {storm_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/forecast/{storm_id}/intensity", response_model=ForecastResponse)
async def get_intensity_forecast(
        storm_id: str,
        hours: int = Query(48, ge=6, le=120)
):
    """
    Get detailed intensity forecast using LSTM model

    Focuses specifically on wind speed and pressure predictions
    """
    try:
        ch_service = ClickHouseService()
        history = ch_service.get_cyclone_history(storm_id, hours=72)

        if not history:
            raise HTTPException(
                status_code=404,
                detail=f"No historical data found for storm {storm_id}"
            )

        if len(history) < 10:
            raise HTTPException(
                status_code=400,
                detail="Insufficient historical data for intensity prediction (minimum 10 points required)"
            )

        # Use LSTM specifically for intensity
        ml_service = MLCycloneForecast()
        forecast = ml_service.lstm_intensity_forecast(
            history,
            hours_ahead=hours,
            interval_hours=6
        )

        return ForecastResponse(
            storm_id=storm_id,
            storm_name=history[-1].get('name', 'UNNAMED'),
            issued_at=history[-1]['timestamp'],
            total_points=len(forecast),
            forecast_hours=hours,
            forecast=forecast,
            methods=['lstm_intensity'],
            model_info={
                'model': 'LSTM Deep Learning',
                'focus': 'Wind speed and pressure prediction',
                'training_samples': len(history)
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating intensity forecast: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/forecast/formation/predict", response_model=FormationPrediction)
async def predict_cyclone_formation(
        latitude: float = Query(..., ge=-90, le=90),
        longitude: float = Query(..., ge=-180, le=180),
        hours_ahead: int = Query(48, ge=24, le=120)
):
    """
    Predict potential cyclone formation in a region

    Uses ML model trained on historical formation patterns and current weather conditions
    """
    try:
        ml_service = MLCycloneForecast()

        # Predict formation probability
        prediction = ml_service.predict_formation(
            latitude=latitude,
            longitude=longitude,
            hours_ahead=hours_ahead
        )

        return FormationPrediction(
            formation_probability=prediction['probability'],
            risk_level=prediction['risk_level'],
            potential_location={
                'latitude': prediction['location']['lat'],
                'longitude': prediction['location']['lon']
            },
            estimated_time_hours=prediction['estimated_time_hours'],
            confidence=prediction['confidence'],
            factors=prediction['factors']
        )

    except Exception as e:
        logger.error(f"Error predicting cyclone formation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/forecast/{storm_id}/geojson")
async def get_forecast_geojson(
        storm_id: str,
        hours: int = Query(48, ge=6, le=120),
        method: str = Query("auto", description="Forecast method")
):
    """
    Get forecast track in GeoJSON format

    Returns the predicted path as a GeoJSON LineString,
    ready for direct use in mapping libraries.
    """
    try:
        # Get forecast data
        ch_service = ClickHouseService()
        history = ch_service.get_cyclone_history(storm_id, hours=72)

        if not history:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for storm {storm_id}"
            )

        # Generate forecast based on method
        if method == "ml" and len(history) >= 10:
            ml_service = MLCycloneForecast()
            forecast = ml_service.hybrid_forecast(history, hours_ahead=hours)
        else:
            forecast_service = ForecastService()
            forecast = forecast_service.simple_extrapolation_forecast(
                history,
                hours_ahead=hours
            )

        if not forecast:
            raise HTTPException(
                status_code=500,
                detail="Failed to generate forecast"
            )

        # Create GeoJSON LineString
        coordinates = [
            [f['longitude'], f['latitude']]
            for f in forecast
        ]

        # Add current position as first point
        coordinates.insert(0, [history[-1]['longitude'], history[-1]['latitude']])

        # Properties for each point
        properties = []
        properties.append({
            'hour': 0,
            'timestamp': history[-1]['timestamp'],
            'wind_speed': history[-1].get('max_sustained_wind'),
            'pressure': history[-1].get('central_pressure'),
            'type': 'current'
        })

        for f in forecast:
            point_props = {
                'hour': f['forecast_hour'],
                'timestamp': f['forecast_timestamp'],
                'wind_speed': f.get('max_wind'),
                'pressure': f.get('min_pressure'),
                'type': f['forecast_type']
            }

            # Add uncertainty bounds if available (from ML)
            if f.get('uncertainty_bounds'):
                point_props['uncertainty'] = f['uncertainty_bounds']

            properties.append(point_props)

        geojson = {
            'type': 'Feature',
            'geometry': {
                'type': 'LineString',
                'coordinates': coordinates
            },
            'properties': {
                'storm_id': storm_id,
                'storm_name': history[-1].get('name', 'UNNAMED'),
                'forecast_hours': hours,
                'total_points': len(forecast),
                'issued_at': history[-1]['timestamp'],
                'forecast_method': method,
                'points': properties
            }
        }

        return geojson

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating forecast GeoJSON for {storm_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/forecast/{storm_id}/cone")
async def get_forecast_cone(
        storm_id: str,
        method: str = Query("auto", description="Forecast method")
):
    """
    Get forecast uncertainty cone

    Returns uncertainty cone showing the potential area where the cyclone could travel.
    ML methods provide statistically-derived uncertainty bounds.
    """
    try:
        ch_service = ClickHouseService()
        history = ch_service.get_cyclone_history(storm_id, hours=72)

        if not history:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for storm {storm_id}"
            )

        # Generate forecast
        if method == "ml" and len(history) >= 10:
            ml_service = MLCycloneForecast()
            forecast = ml_service.hybrid_forecast(history, hours_ahead=48)
            uncertainty_method = "ML ensemble-based"
        else:
            forecast_service = ForecastService()
            forecast = forecast_service.simple_extrapolation_forecast(history, hours_ahead=48)
            uncertainty_method = "statistical approximation"

        if not forecast:
            raise HTTPException(
                status_code=500,
                detail="Failed to generate forecast"
            )

        # Create uncertainty cone
        cone_points = []

        for i, point in enumerate(forecast):
            # ML provides actual uncertainty bounds, otherwise use simple heuristic
            if point.get('uncertainty_bounds'):
                uncertainty_radius_km = point['uncertainty_bounds'].get('radius_km', 50 + (i * 20))
            else:
                # Increase uncertainty with time (simplified)
                uncertainty_radius_km = 50 + (i * 20)

            cone_points.append({
                'hour': point['forecast_hour'],
                'center': {
                    'latitude': point['latitude'],
                    'longitude': point['longitude']
                },
                'uncertainty_radius_km': uncertainty_radius_km,
                'confidence_level': point.get('confidence', 'medium')
            })

        return {
            'storm_id': storm_id,
            'storm_name': history[-1].get('name', 'UNNAMED'),
            'cone': cone_points,
            'uncertainty_method': uncertainty_method,
            'forecast_method': method,
            'note': 'Uncertainty bounds represent probable areas of cyclone movement'
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating forecast cone for {storm_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/forecast/compare/{storm_id}")
async def compare_forecast_methods(storm_id: str):
    """
    Compare different forecasting methods for the same cyclone

    Returns forecasts from ML, extrapolation, and persistence for comparison
    """
    try:
        ch_service = ClickHouseService()
        history = ch_service.get_cyclone_history(storm_id, hours=72)

        if not history:
            raise HTTPException(
                status_code=404,
                detail=f"No historical data found for storm {storm_id}"
            )

        forecast_service = ForecastService()
        ml_service = MLCycloneForecast()

        results = {
            'storm_id': storm_id,
            'storm_name': history[-1].get('name', 'UNNAMED'),
            'forecasts': {}
        }

        # Extrapolation forecast
        try:
            extrap_forecast = forecast_service.simple_extrapolation_forecast(
                history, hours_ahead=48
            )
            results['forecasts']['extrapolation'] = {
                'method': 'Mathematical extrapolation',
                'points': len(extrap_forecast),
                'forecast': extrap_forecast[:5]  # First 5 points only
            }
        except Exception as e:
            logger.warning(f"Extrapolation failed: {e}")

        # ML forecast (if enough data)
        if len(history) >= 10:
            try:
                ml_forecast = ml_service.hybrid_forecast(history, hours_ahead=48)
                results['forecasts']['ml_hybrid'] = {
                    'method': 'Prophet + LSTM',
                    'points': len(ml_forecast),
                    'forecast': ml_forecast[:5]  # First 5 points only
                }
            except Exception as e:
                logger.warning(f"ML forecast failed: {e}")

        # Persistence forecast
        try:
            persist_forecast = forecast_service.persistence_forecast(
                history[-1], hours_ahead=48
            )
            results['forecasts']['persistence'] = {
                'method': 'No movement assumption',
                'points': len(persist_forecast),
                'forecast': persist_forecast[:5]  # First 5 points only
            }
        except Exception as e:
            logger.warning(f"Persistence failed: {e}")

        return results

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparing forecasts: {e}")
        raise HTTPException(status_code=500, detail=str(e))