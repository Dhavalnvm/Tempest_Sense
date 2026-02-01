"""
ClickHouse Service - Database operations for cyclone data
"""
import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import clickhouse_connect

logger = logging.getLogger(__name__)


class ClickHouseService:
    """Service for ClickHouse database operations"""

    def __init__(self):
        self.host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
        self.user = os.getenv('CLICKHOUSE_USER', 'admin')
        self.password = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')
        self.database = os.getenv('CLICKHOUSE_DATABASE', 'cyclones')

        self.client = None
        self._connect()

    def _connect(self):
        """Establish ClickHouse connection"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.database
            )
            logger.info(f"Connected to ClickHouse at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            result = self.client.query("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"ClickHouse connection test failed: {e}")
            return False

    def get_active_cyclones(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get currently active cyclones (last 6 hours)"""
        query = """
        SELECT 
            id,
            name,
            basin,
            classification,
            intensity,
            latitude,
            longitude,
            movement_speed,
            movement_direction,
            central_pressure,
            max_sustained_wind,
            timestamp,
            data_source
        FROM cyclone_positions
        WHERE timestamp >= now() - INTERVAL 6 HOUR
        AND (id, timestamp) IN (
            SELECT id, max(timestamp) as timestamp
            FROM cyclone_positions
            WHERE timestamp >= now() - INTERVAL 6 HOUR
            GROUP BY id
        )
        ORDER BY timestamp DESC
        LIMIT {limit}
        """.format(limit=limit)

        try:
            result = self.client.query(query)

            cyclones = []
            for row in result.result_rows:
                cyclones.append({
                    'id': row[0],
                    'name': row[1],
                    'basin': row[2],
                    'classification': row[3],
                    'intensity': row[4],
                    'latitude': float(row[5]),
                    'longitude': float(row[6]),
                    'movement_speed': float(row[7]),
                    'movement_direction': float(row[8]),
                    'central_pressure': float(row[9]) if row[9] else None,
                    'max_sustained_wind': float(row[10]) if row[10] else None,
                    'timestamp': row[11].isoformat() if hasattr(row[11], 'isoformat') else str(row[11]),
                    'data_source': row[12]
                })

            logger.info(f"Retrieved {len(cyclones)} active cyclones")
            return cyclones

        except Exception as e:
            logger.error(f"Error fetching active cyclones: {e}")
            raise

    def get_cyclone_history(self, storm_id: str, hours: int = 72) -> List[Dict[str, Any]]:
        """Get historical track for a specific cyclone"""
        query = """
        SELECT 
            id,
            name,
            latitude,
            longitude,
            max_sustained_wind,
            central_pressure,
            timestamp
        FROM cyclone_positions
        WHERE id = '{storm_id}'
        AND timestamp >= now() - INTERVAL {hours} HOUR
        ORDER BY timestamp ASC
        """.format(storm_id=storm_id, hours=hours)

        try:
            result = self.client.query(query)

            history = []
            for row in result.result_rows:
                history.append({
                    'id': row[0],
                    'name': row[1],
                    'latitude': float(row[2]),
                    'longitude': float(row[3]),
                    'max_sustained_wind': float(row[4]) if row[4] else None,
                    'central_pressure': float(row[5]) if row[5] else None,
                    'timestamp': row[6].isoformat() if hasattr(row[6], 'isoformat') else str(row[6])
                })

            logger.info(f"Retrieved {len(history)} historical points for {storm_id}")
            return history

        except Exception as e:
            logger.error(f"Error fetching history for {storm_id}: {e}")
            raise

    def get_cyclone_metadata(self, storm_id: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific cyclone"""
        query = """
        SELECT 
            id,
            name,
            basin,
            formation_date,
            dissipation_date,
            peak_intensity,
            peak_wind,
            min_pressure,
            total_advisories,
            is_active
        FROM cyclone_metadata
        WHERE id = '{storm_id}'
        ORDER BY last_updated DESC
        LIMIT 1
        """.format(storm_id=storm_id)

        try:
            result = self.client.query(query)

            if not result.result_rows:
                return None

            row = result.result_rows[0]
            return {
                'id': row[0],
                'name': row[1],
                'basin': row[2],
                'formation_date': row[3].isoformat() if row[3] else None,
                'dissipation_date': row[4].isoformat() if row[4] else None,
                'peak_intensity': row[5],
                'peak_wind': float(row[6]) if row[6] else None,
                'min_pressure': float(row[7]) if row[7] else None,
                'total_advisories': int(row[8]),
                'is_active': bool(row[9])
            }

        except Exception as e:
            logger.error(f"Error fetching metadata for {storm_id}: {e}")
            raise

    def get_basin_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get statistics by basin for recent period"""
        query = """
        SELECT 
            basin,
            count() as observations,
            uniq(id) as active_storms,
            avg(max_sustained_wind) as avg_wind,
            max(max_sustained_wind) as max_wind
        FROM cyclone_positions
        WHERE timestamp >= now() - INTERVAL {hours} HOUR
        GROUP BY basin
        ORDER BY active_storms DESC
        """.format(hours=hours)

        try:
            result = self.client.query(query)

            stats = {}
            for row in result.result_rows:
                stats[row[0]] = {
                    'observations': int(row[1]),
                    'active_storms': int(row[2]),
                    'avg_wind_speed': float(row[3]) if row[3] else 0,
                    'max_wind_speed': float(row[4]) if row[4] else 0
                }

            return stats

        except Exception as e:
            logger.error(f"Error fetching basin statistics: {e}")
            raise

    def get_global_statistics(self) -> Dict[str, Any]:
        """Get global cyclone statistics"""
        queries = {
            'total_active': """
                            SELECT count(DISTINCT id)
                            FROM cyclone_positions
                            WHERE timestamp >= now() - INTERVAL 6 HOUR
                            """,
            'total_observations': """
                                  SELECT count()
                                  FROM cyclone_positions
                                  WHERE timestamp >= now() - INTERVAL 24 HOUR
                                  """,
            'avg_intensity': """
                             SELECT avg(max_sustained_wind)
                             FROM cyclone_positions
                             WHERE timestamp >= now() - INTERVAL 24 HOUR
                             """
        }

        stats = {}

        try:
            for key, query in queries.items():
                result = self.client.query(query)
                value = result.result_rows[0][0] if result.result_rows else 0
                stats[key] = float(value) if value else 0

            return stats

        except Exception as e:
            logger.error(f"Error fetching global statistics: {e}")
            raise

    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logger.info("ClickHouse connection closed")