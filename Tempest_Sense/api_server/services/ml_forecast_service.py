"""
Real-time ML Cyclone Forecasting (Zero Pre-training Required)
Uses Prophet for instant trajectory prediction and statistical models for intensity
Similar to Infra-Pulse approach - predictions happen in real-time
"""
import logging
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from prophet import Prophet
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

logger = logging.getLogger(__name__)


class MLCycloneForecast:
    """
    Real-time ML forecasting without pre-training
    Models are fitted on-the-fly using historical data
    """

    def __init__(self):
        # No pre-loaded models - everything happens in real-time
        self.min_data_points = 5  # Minimum points needed for ML
        logger.info("ML Forecast Service initialized (real-time mode)")

    def hybrid_forecast(
        self,
        historical_track: List[Dict[str, Any]],
        hours_ahead: int = 48,
        interval_hours: int = 6
    ) -> List[Dict[str, Any]]:
        """
        Generate real-time hybrid forecast
        Prophet (trajectory) + Statistical models (intensity)
        NO PRE-TRAINING REQUIRED - fits models instantly

        Args:
            historical_track: Recent cyclone positions
            hours_ahead: Forecast duration
            interval_hours: Time between forecast points

        Returns:
            List of forecast points
        """
        try:
            logger.info(f"🚀 Real-time ML forecast for {hours_ahead}h ahead")

            if len(historical_track) < self.min_data_points:
                raise ValueError(
                    f"Need at least {self.min_data_points} points, got {len(historical_track)}"
                )

            # 1. Prophet for trajectory (trains instantly - 2-5 seconds)
            lat_forecast, lon_forecast = self._instant_prophet_forecast(
                historical_track,
                hours_ahead,
                interval_hours
            )

            # 2. Statistical intensity forecast (instant)
            intensity_forecast = self._statistical_intensity_forecast(
                historical_track,
                hours_ahead,
                interval_hours
            )

            # 3. Combine results
            combined = self._combine_forecasts(
                historical_track,
                lat_forecast,
                lon_forecast,
                intensity_forecast,
                interval_hours
            )

            logger.info(f"✅ Generated {len(combined)} forecast points in real-time")
            return combined

        except Exception as e:
            logger.error(f"Real-time forecast failed: {e}")
            raise

    def _instant_prophet_forecast(
        self,
        historical_track: List[Dict[str, Any]],
        hours_ahead: int,
        interval_hours: int
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Prophet forecast - trains instantly on historical data
        NO PRE-TRAINING needed, fits in 2-5 seconds
        """
        logger.info("⚡ Running instant Prophet forecast...")

        # Convert to DataFrame
        df = pd.DataFrame(historical_track)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')

        # Latitude forecast
        lat_df = df[['timestamp', 'latitude']].rename(
            columns={'timestamp': 'ds', 'latitude': 'y'}
        )

        lat_model = Prophet(
            changepoint_prior_scale=0.05,
            interval_width=0.8,
            daily_seasonality=False,
            weekly_seasonality=False,
            yearly_seasonality=False
        )

        # Fit instantly (takes 1-3 seconds)
        lat_model.fit(lat_df)

        # Generate future
        num_periods = hours_ahead // interval_hours
        future = lat_model.make_future_dataframe(
            periods=num_periods,
            freq=f'{interval_hours}H',
            include_history=False
        )
        lat_forecast = lat_model.predict(future)

        # Longitude forecast
        lon_df = df[['timestamp', 'longitude']].rename(
            columns={'timestamp': 'ds', 'longitude': 'y'}
        )

        lon_model = Prophet(
            changepoint_prior_scale=0.05,
            interval_width=0.8,
            daily_seasonality=False,
            weekly_seasonality=False,
            yearly_seasonality=False
        )
        lon_model.fit(lon_df)
        lon_forecast = lon_model.predict(future)

        logger.info(f"✅ Prophet trained & predicted in real-time")
        return lat_forecast, lon_forecast

    def _statistical_intensity_forecast(
        self,
        historical_track: List[Dict[str, Any]],
        hours_ahead: int,
        interval_hours: int
    ) -> List[Dict[str, float]]:
        """
        Statistical intensity forecasting
        Uses polynomial regression fitted instantly on historical trends
        NO PRE-TRAINING required
        """
        logger.info("⚡ Running statistical intensity forecast...")

        df = pd.DataFrame(historical_track)
        df = df.sort_values('timestamp')

        # Extract wind and pressure data
        wind_data = df['max_sustained_wind'].fillna(method='ffill').fillna(0).values
        pressure_data = df['central_pressure'].fillna(method='ffill').fillna(1013).values

        # Create time indices
        time_indices = np.arange(len(wind_data))

        # Fit polynomial models (degree 2 for smooth trends)
        poly = PolynomialFeatures(degree=2)

        # Wind model
        if len(wind_data[wind_data > 0]) >= 3:  # Need at least 3 valid points
            valid_wind_mask = wind_data > 0
            X_wind = poly.fit_transform(time_indices[valid_wind_mask].reshape(-1, 1))
            y_wind = wind_data[valid_wind_mask]

            wind_model = LinearRegression()
            wind_model.fit(X_wind, y_wind)
        else:
            wind_model = None

        # Pressure model
        if len(pressure_data[pressure_data > 0]) >= 3:
            valid_pressure_mask = pressure_data > 0
            X_pressure = poly.fit_transform(time_indices[valid_pressure_mask].reshape(-1, 1))
            y_pressure = pressure_data[valid_pressure_mask]

            pressure_model = LinearRegression()
            pressure_model.fit(X_pressure, y_pressure)
        else:
            pressure_model = None

        # Generate predictions
        num_periods = hours_ahead // interval_hours
        future_indices = np.arange(len(time_indices), len(time_indices) + num_periods)

        predictions = []
        for future_idx in future_indices:
            X_future = poly.transform([[future_idx]])

            # Predict wind
            if wind_model:
                wind_pred = wind_model.predict(X_future)[0]
                # Ensure reasonable bounds (25-200 kt)
                wind_pred = np.clip(wind_pred, 25, 200)
            else:
                # Fallback: use last known or decay
                wind_pred = wind_data[-1] * 0.95 if wind_data[-1] > 0 else 50

            # Predict pressure
            if pressure_model:
                pressure_pred = pressure_model.predict(X_future)[0]
                # Ensure reasonable bounds (900-1013 mb)
                pressure_pred = np.clip(pressure_pred, 900, 1013)
            else:
                # Fallback: use last known
                pressure_pred = pressure_data[-1] if pressure_data[-1] > 0 else 1000

            predictions.append({
                'wind': float(wind_pred),
                'pressure': float(pressure_pred)
            })

        logger.info(f"✅ Statistical intensity forecast complete")
        return predictions

    def _combine_forecasts(
        self,
        historical_track: List[Dict[str, Any]],
        lat_forecast: pd.DataFrame,
        lon_forecast: pd.DataFrame,
        intensity_forecast: List[Dict[str, float]],
        interval_hours: int
    ) -> List[Dict[str, Any]]:
        """Combine Prophet trajectory + statistical intensity"""

        combined = []
        last_timestamp = pd.to_datetime(historical_track[-1]['timestamp'])
        storm_id = historical_track[-1]['id']
        storm_name = historical_track[-1]['name']

        for i in range(len(lat_forecast)):
            forecast_time = last_timestamp + timedelta(hours=(i + 1) * interval_hours)

            # Prophet uncertainty bounds
            lat_lower = lat_forecast.iloc[i]['yhat_lower']
            lat_upper = lat_forecast.iloc[i]['yhat_upper']
            lon_lower = lon_forecast.iloc[i]['yhat_lower']
            lon_upper = lon_forecast.iloc[i]['yhat_upper']

            # Calculate uncertainty radius (km)
            lat_uncertainty = (lat_upper - lat_lower) / 2
            lon_uncertainty = (lon_upper - lon_lower) / 2
            uncertainty_radius = np.sqrt(lat_uncertainty**2 + lon_uncertainty**2) * 111

            # Determine confidence
            if uncertainty_radius < 100:
                confidence = "high"
            elif uncertainty_radius < 200:
                confidence = "medium"
            else:
                confidence = "low"

            point = {
                'id': storm_id,
                'name': storm_name,
                'forecast_hour': (i + 1) * interval_hours,
                'forecast_timestamp': forecast_time.isoformat(),
                'latitude': round(lat_forecast.iloc[i]['yhat'], 4),
                'longitude': round(lon_forecast.iloc[i]['yhat'], 4),
                'max_wind': round(intensity_forecast[i]['wind'], 1),
                'min_pressure': round(intensity_forecast[i]['pressure'], 1),
                'forecast_type': 'prophet_statistical_hybrid',
                'confidence': confidence,
                'uncertainty_bounds': {
                    'lat_lower': round(lat_lower, 4),
                    'lat_upper': round(lat_upper, 4),
                    'lon_lower': round(lon_lower, 4),
                    'lon_upper': round(lon_upper, 4),
                    'radius_km': round(uncertainty_radius, 1)
                }
            }

            combined.append(point)

        return combined

    def lstm_intensity_forecast(
        self,
        historical_track: List[Dict[str, Any]],
        hours_ahead: int = 48,
        interval_hours: int = 6
    ) -> List[Dict[str, Any]]:
        """
        Intensity-only forecast using advanced statistical methods
        (Replaces LSTM - no training needed)
        """
        logger.info("⚡ Running advanced intensity forecast...")

        try:
            intensity_predictions = self._statistical_intensity_forecast(
                historical_track,
                hours_ahead,
                interval_hours
            )

            # Format results
            forecast = []
            last_timestamp = pd.to_datetime(historical_track[-1]['timestamp'])
            storm_id = historical_track[-1]['id']
            storm_name = historical_track[-1]['name']
            last_lat = historical_track[-1]['latitude']
            last_lon = historical_track[-1]['longitude']

            for i, pred in enumerate(intensity_predictions):
                forecast_time = last_timestamp + timedelta(hours=(i + 1) * interval_hours)

                point = {
                    'id': storm_id,
                    'name': storm_name,
                    'forecast_hour': (i + 1) * interval_hours,
                    'forecast_timestamp': forecast_time.isoformat(),
                    'latitude': last_lat,
                    'longitude': last_lon,
                    'max_wind': pred['wind'],
                    'min_pressure': pred['pressure'],
                    'forecast_type': 'statistical_intensity',
                    'confidence': 'high'
                }

                forecast.append(point)

            logger.info(f"✅ Intensity forecast complete: {len(forecast)} points")
            return forecast

        except Exception as e:
            logger.error(f"Intensity forecast failed: {e}")
            raise

    def predict_formation(
        self,
        latitude: float,
        longitude: float,
        hours_ahead: int = 48
    ) -> Dict[str, Any]:
        """
        Real-time cyclone formation prediction
        Uses climatological data + current conditions
        NO TRAINING required
        """
        logger.info(f"⚡ Predicting formation at ({latitude}, {longitude})")

        # Formation zones (based on climatology)
        formation_zones = {
            'Atlantic': {
                'lat_range': (5, 30),
                'lon_range': (-80, -20),
                'season_months': [6, 7, 8, 9, 10, 11],
                'base_probability': 0.35
            },
            'Eastern Pacific': {
                'lat_range': (5, 20),
                'lon_range': (-120, -80),
                'season_months': [5, 6, 7, 8, 9, 10, 11],
                'base_probability': 0.40
            },
            'Western Pacific': {
                'lat_range': (5, 25),
                'lon_range': (120, 180),
                'season_months': [5, 6, 7, 8, 9, 10, 11],
                'base_probability': 0.45
            },
            'Indian Ocean': {
                'lat_range': (5, 20),
                'lon_range': (40, 100),
                'season_months': [4, 5, 10, 11, 12],
                'base_probability': 0.30
            }
        }

        # Check location
        in_zone = False
        zone_info = None

        for zone_name, zone in formation_zones.items():
            lat_min, lat_max = zone['lat_range']
            lon_min, lon_max = zone['lon_range']

            if lat_min <= latitude <= lat_max and lon_min <= longitude <= lon_max:
                in_zone = True
                zone_info = {'name': zone_name, **zone}
                break

        # Calculate probability
        if in_zone:
            probability = zone_info['base_probability']

            # Seasonal adjustment
            current_month = datetime.utcnow().month
            if current_month in zone_info['season_months']:
                probability += 0.25  # Peak season boost

            # Distance from typical formation center
            # (closer to center = higher probability)
            lat_center = sum(zone_info['lat_range']) / 2
            lon_center = sum(zone_info['lon_range']) / 2

            distance_factor = 1 - (abs(latitude - lat_center) + abs(longitude - lon_center)) / 50
            distance_factor = max(0, min(1, distance_factor))
            probability *= (0.7 + 0.3 * distance_factor)
        else:
            probability = 0.05  # Very low outside formation zones
            zone_info = {'name': 'Unknown', 'base_probability': 0.05}

        # Cap probability
        probability = min(0.95, max(0.01, probability))

        # Risk level
        if probability >= 0.7:
            risk_level = "high"
            estimated_time = 24
        elif probability >= 0.4:
            risk_level = "medium"
            estimated_time = 48
        else:
            risk_level = "low"
            estimated_time = 72

        # Confidence (based on data quality)
        confidence = "high" if in_zone else "low"

        return {
            'probability': round(probability, 3),
            'risk_level': risk_level,
            'location': {
                'lat': latitude,
                'lon': longitude,
                'zone': zone_info['name']
            },
            'estimated_time_hours': estimated_time,
            'confidence': confidence,
            'factors': {
                'in_formation_zone': in_zone,
                'current_season': 'favorable' if in_zone and datetime.utcnow().month in zone_info.get('season_months', []) else 'unfavorable',
                'zone_probability': zone_info['base_probability']
            }
        }