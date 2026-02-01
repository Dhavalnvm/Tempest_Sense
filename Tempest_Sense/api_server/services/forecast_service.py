"""
Forecast Service - Generate cyclone trajectory forecasts
Combines NOAA official forecasts with simple extrapolation methods
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import math

logger = logging.getLogger(__name__)


class ForecastService:
    """Generate and manage cyclone forecasts"""

    @staticmethod
    def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate distance between two points on Earth (in km)
        Uses Haversine formula
        """
        R = 6371  # Earth's radius in km

        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)

        a = (math.sin(dlat / 2) ** 2 +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
             math.sin(dlon / 2) ** 2)

        c = 2 * math.asin(math.sqrt(a))

        return R * c

    @staticmethod
    def calculate_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate bearing between two points (in degrees)
        """
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        dlon = math.radians(lon2 - lon1)

        y = math.sin(dlon) * math.cos(lat2_rad)
        x = (math.cos(lat1_rad) * math.sin(lat2_rad) -
             math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon))

        bearing = math.degrees(math.atan2(y, x))
        return (bearing + 360) % 360

    @staticmethod
    def extrapolate_position(lat: float, lon: float, bearing: float,
                             distance_km: float) -> tuple[float, float]:
        """
        Extrapolate new position given current position, bearing, and distance
        """
        R = 6371  # Earth's radius in km

        bearing_rad = math.radians(bearing)
        lat_rad = math.radians(lat)
        lon_rad = math.radians(lon)

        new_lat_rad = math.asin(
            math.sin(lat_rad) * math.cos(distance_km / R) +
            math.cos(lat_rad) * math.sin(distance_km / R) * math.cos(bearing_rad)
        )

        new_lon_rad = lon_rad + math.atan2(
            math.sin(bearing_rad) * math.sin(distance_km / R) * math.cos(lat_rad),
            math.cos(distance_km / R) - math.sin(lat_rad) * math.sin(new_lat_rad)
        )

        new_lat = math.degrees(new_lat_rad)
        new_lon = math.degrees(new_lon_rad)

        # Normalize longitude to -180 to 180
        new_lon = ((new_lon + 180) % 360) - 180

        return new_lat, new_lon

    @classmethod
    def simple_extrapolation_forecast(cls, historical_track: List[Dict[str, Any]],
                                      hours_ahead: int = 48,
                                      interval_hours: int = 6) -> List[Dict[str, Any]]:
        """
        Generate simple extrapolation forecast based on recent movement

        Args:
            historical_track: List of past positions (must be chronologically ordered)
            hours_ahead: How many hours to forecast
            interval_hours: Time interval between forecast points

        Returns:
            List of forecast points
        """
        if len(historical_track) < 2:
            logger.warning("Insufficient data for extrapolation (need at least 2 points)")
            return []

        try:
            # Use last few points to calculate average movement
            recent_points = historical_track[-5:]  # Use last 5 points

            if len(recent_points) < 2:
                recent_points = historical_track

            # Calculate average speed and direction
            total_distance = 0
            total_time_hours = 0
            bearings = []

            for i in range(len(recent_points) - 1):
                p1 = recent_points[i]
                p2 = recent_points[i + 1]

                # Calculate distance
                dist = cls.haversine_distance(
                    p1['latitude'], p1['longitude'],
                    p2['latitude'], p2['longitude']
                )
                total_distance += dist

                # Calculate time difference
                t1 = datetime.fromisoformat(p1['timestamp'].replace('Z', '+00:00'))
                t2 = datetime.fromisoformat(p2['timestamp'].replace('Z', '+00:00'))
                time_diff = (t2 - t1).total_seconds() / 3600  # hours
                total_time_hours += time_diff

                # Calculate bearing
                bearing = cls.calculate_bearing(
                    p1['latitude'], p1['longitude'],
                    p2['latitude'], p2['longitude']
                )
                bearings.append(bearing)

            # Average speed (km/h)
            avg_speed = total_distance / total_time_hours if total_time_hours > 0 else 0

            # Average bearing (circular mean)
            avg_bearing = sum(bearings) / len(bearings) if bearings else 0

            # Get last known position
            last_point = historical_track[-1]
            current_lat = last_point['latitude']
            current_lon = last_point['longitude']
            last_timestamp = datetime.fromisoformat(
                last_point['timestamp'].replace('Z', '+00:00')
            )

            # Generate forecast points
            forecast = []
            num_points = hours_ahead // interval_hours

            for i in range(1, num_points + 1):
                hours = i * interval_hours
                distance = avg_speed * hours

                # Extrapolate position
                new_lat, new_lon = cls.extrapolate_position(
                    current_lat, current_lon, avg_bearing, distance
                )

                forecast_time = last_timestamp + timedelta(hours=hours)

                # Estimate intensity change (simple linear decay - very basic)
                last_wind = last_point.get('max_sustained_wind', 0)
                # Assume 5% decay per 24 hours (very rough estimate)
                decay_factor = 0.95 ** (hours / 24)
                forecast_wind = last_wind * decay_factor if last_wind else None

                forecast.append({
                    'id': last_point['id'],
                    'name': last_point['name'],
                    'forecast_hour': hours,
                    'forecast_timestamp': forecast_time.isoformat(),
                    'latitude': round(new_lat, 4),
                    'longitude': round(new_lon, 4),
                    'max_wind': round(forecast_wind, 1) if forecast_wind else None,
                    'forecast_type': 'extrapolation',
                    'confidence': 'low',  # Simple extrapolation has low confidence
                    'avg_speed_kph': round(avg_speed, 2),
                    'bearing': round(avg_bearing, 1)
                })

            logger.info(f"Generated {len(forecast)} extrapolation points")
            return forecast

        except Exception as e:
            logger.error(f"Error in extrapolation forecast: {e}")
            return []

    @classmethod
    def persistence_forecast(cls, current_position: Dict[str, Any],
                             hours_ahead: int = 48,
                             interval_hours: int = 6) -> List[Dict[str, Any]]:
        """
        Simple persistence forecast - assumes cyclone doesn't move
        Useful as a baseline or when movement data is unavailable
        """
        try:
            forecast = []
            current_time = datetime.fromisoformat(
                current_position['timestamp'].replace('Z', '+00:00')
            )

            num_points = hours_ahead // interval_hours

            for i in range(1, num_points + 1):
                hours = i * interval_hours
                forecast_time = current_time + timedelta(hours=hours)

                forecast.append({
                    'id': current_position['id'],
                    'name': current_position['name'],
                    'forecast_hour': hours,
                    'forecast_timestamp': forecast_time.isoformat(),
                    'latitude': current_position['latitude'],
                    'longitude': current_position['longitude'],
                    'max_wind': current_position.get('max_sustained_wind'),
                    'forecast_type': 'persistence',
                    'confidence': 'very_low'
                })

            logger.info(f"Generated {len(forecast)} persistence points")
            return forecast

        except Exception as e:
            logger.error(f"Error in persistence forecast: {e}")
            return []

    @classmethod
    def combine_forecasts(cls, official: List[Dict[str, Any]],
                          extrapolated: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Combine official NOAA forecasts with extrapolated forecasts
        Prefer official data when available, fill gaps with extrapolation
        """
        if not official:
            return extrapolated

        if not extrapolated:
            return official

        # Create a set of forecast hours from official data
        official_hours = {f['forecast_hour'] for f in official}

        # Add extrapolated points for hours not covered by official forecast
        combined = official.copy()

        for extrap_point in extrapolated:
            if extrap_point['forecast_hour'] not in official_hours:
                combined.append(extrap_point)

        # Sort by forecast hour
        combined.sort(key=lambda x: x['forecast_hour'])

        return combined