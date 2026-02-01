"""
Parser for NOAA CurrentStorms.json format
Handles extraction and normalization of cyclone data
"""
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import re

logger = logging.getLogger(__name__)


class CycloneDataParser:
    """Parse NOAA CurrentStorms.json format"""

    BASIN_MAPPING = {
        'AL': 'Atlantic',
        'EP': 'Eastern Pacific',
        'CP': 'Central Pacific',
        'WP': 'Western Pacific',
        'IO': 'Indian Ocean',
        'SH': 'Southern Hemisphere'
    }

    @staticmethod
    def parse_storm_id(storm_id: str) -> Dict[str, str]:
        """
        Parse storm ID (e.g., 'AL012025')
        Returns: {basin: 'AL', number: '01', year: '2025'}
        """
        if len(storm_id) >= 8:
            return {
                'basin': storm_id[:2],
                'number': storm_id[2:4],
                'year': storm_id[4:8]
            }
        return {'basin': '', 'number': '', 'year': ''}

    @staticmethod
    def extract_coordinates(location_str: str) -> tuple[Optional[float], Optional[float]]:
        """
        Extract latitude and longitude from various formats
        Examples: "25.5N 80.3W", "25.5°N, 80.3°W"
        """
        if not location_str:
            return None, None

        try:
            # Remove degree symbols and clean
            clean_str = location_str.replace('°', '').replace(',', ' ')
            parts = clean_str.split()

            lat, lon = None, None

            for part in parts:
                if 'N' in part or 'S' in part:
                    lat = float(part.replace('N', '').replace('S', ''))
                    if 'S' in part:
                        lat = -lat
                elif 'E' in part or 'W' in part:
                    lon = float(part.replace('E', '').replace('W', ''))
                    if 'W' in part:
                        lon = -lon

            return lat, lon
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to parse coordinates from '{location_str}': {e}")
            return None, None

    @staticmethod
    def extract_wind_speed(wind_str: str) -> Optional[float]:
        """Extract wind speed in knots from string"""
        if not wind_str:
            return None

        try:
            # Extract numbers from strings like "85 kt", "85kt", "85 knots"
            match = re.search(r'(\d+(?:\.\d+)?)', str(wind_str))
            if match:
                return float(match.group(1))
        except (ValueError, AttributeError):
            pass
        return None

    @staticmethod
    def extract_pressure(pressure_str: str) -> Optional[float]:
        """Extract pressure in mb from string"""
        if not pressure_str:
            return None

        try:
            # Extract numbers from strings like "985 mb", "985mb"
            match = re.search(r'(\d+(?:\.\d+)?)', str(pressure_str))
            if match:
                return float(match.group(1))
        except (ValueError, AttributeError):
            pass
        return None

    @classmethod
    def parse_current_storms(cls, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse CurrentStorms.json format from NOAA

        Returns list of normalized cyclone dictionaries
        """
        storms = []

        try:
            # NOAA format can vary, handle different structures
            active_storms = json_data.get('activeStorms', [])

            for storm in active_storms:
                parsed_storm = cls._parse_single_storm(storm)
                if parsed_storm:
                    storms.append(parsed_storm)

            logger.info(f"Parsed {len(storms)} active storms from NOAA data")

        except Exception as e:
            logger.error(f"Error parsing CurrentStorms.json: {e}")

        return storms

    @classmethod
    def _parse_single_storm(cls, storm: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a single storm entry"""
        try:
            # Extract basic info
            storm_id = storm.get('id', '')
            storm_name = storm.get('name', 'UNNAMED')

            # Parse basin info
            basin_info = cls.parse_storm_id(storm_id)
            basin_code = basin_info.get('basin', '')
            basin_name = cls.BASIN_MAPPING.get(basin_code, 'Unknown')

            # Extract position
            lat, lon = None, None
            if 'lat' in storm and 'lon' in storm:
                lat = float(storm['lat'])
                lon = float(storm['lon'])
            elif 'latitudeNumeric' in storm and 'longitudeNumeric' in storm:
                lat = float(storm['latitudeNumeric'])
                lon = float(storm['longitudeNumeric'])
            elif 'location' in storm:
                lat, lon = cls.extract_coordinates(storm['location'])

            # Extract intensity
            classification = storm.get('classification', 'Unknown')
            intensity = storm.get('intensity', 'Unknown')

            # Wind and pressure
            max_wind = None
            if 'windSpeed' in storm:
                max_wind = cls.extract_wind_speed(storm['windSpeed'])
            elif 'maxSustainedWind' in storm:
                max_wind = float(storm['maxSustainedWind'])

            pressure = None
            if 'pressure' in storm:
                pressure = cls.extract_pressure(storm['pressure'])
            elif 'centralPressure' in storm:
                pressure = float(storm['centralPressure'])

            # Movement
            movement_speed = storm.get('movementSpeed', 0)
            movement_dir = storm.get('movementDir', 0)

            # Timestamp
            timestamp = datetime.utcnow()
            if 'lastUpdate' in storm:
                try:
                    timestamp = datetime.fromisoformat(
                        storm['lastUpdate'].replace('Z', '+00:00')
                    )
                except:
                    pass

            return {
                'id': storm_id,
                'name': storm_name,
                'basin': basin_name,
                'basin_code': basin_code,
                'classification': classification,
                'intensity': intensity,
                'latitude': lat,
                'longitude': lon,
                'movement_speed': movement_speed,
                'movement_direction': movement_dir,
                'central_pressure': pressure,
                'max_sustained_wind': max_wind,
                'timestamp': timestamp.isoformat(),
                'raw_data': storm  # Keep original for reference
            }

        except Exception as e:
            logger.error(f"Error parsing storm {storm.get('id', 'unknown')}: {e}")
            return None

    @classmethod
    def parse_forecast_data(cls, storm_id: str, forecast_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse forecast data for a specific storm

        Returns list of forecast points
        """
        forecasts = []

        try:
            storm_name = forecast_json.get('name', 'UNNAMED')
            issued_at = datetime.utcnow()

            forecast_track = forecast_json.get('forecastTrack', [])

            for idx, point in enumerate(forecast_track):
                lat = point.get('lat') or point.get('latitude')
                lon = point.get('lon') or point.get('longitude')

                if lat is None or lon is None:
                    continue

                forecast_hour = point.get('forecastHour', idx * 6)

                forecasts.append({
                    'id': storm_id,
                    'name': storm_name,
                    'forecast_hour': forecast_hour,
                    'forecast_timestamp': issued_at.isoformat(),
                    'latitude': float(lat),
                    'longitude': float(lon),
                    'max_wind': point.get('maxWind'),
                    'min_pressure': point.get('minPressure'),
                    'forecast_type': 'official',
                    'issued_at': issued_at.isoformat()
                })

            logger.info(f"Parsed {len(forecasts)} forecast points for {storm_id}")

        except Exception as e:
            logger.error(f"Error parsing forecast for {storm_id}: {e}")

        return forecasts


def validate_cyclone_data(data: Dict[str, Any]) -> bool:
    """Validate that cyclone data has minimum required fields"""
    required = ['id', 'latitude', 'longitude']
    return all(field in data and data[field] is not None for field in required)