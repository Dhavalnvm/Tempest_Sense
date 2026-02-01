"""
Redis Service - Caching layer for fast data access
"""
import os
import json
import logging
from typing import List, Dict, Any, Optional
import redis

logger = logging.getLogger(__name__)


class RedisService:
    """Service for Redis caching operations"""

    def __init__(self):
        self.host = os.getenv('REDIS_HOST', 'localhost')
        self.port = int(os.getenv('REDIS_PORT', '6379'))
        self.db = int(os.getenv('REDIS_DB', '0'))
        self.ttl = int(os.getenv('REDIS_TTL', '3600'))

        self.client = None
        self._connect()

    def _connect(self):
        """Establish Redis connection"""
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            self.client.ping()
            logger.info(f"Connected to Redis at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    def test_connection(self) -> bool:
        """Test Redis connection"""
        try:
            return self.client.ping()
        except Exception as e:
            logger.error(f"Redis connection test failed: {e}")
            return False

    def get_live_cyclone(self, storm_id: str) -> Optional[Dict[str, Any]]:
        """Get live cyclone data from cache"""
        try:
            key = f"cyclone:live:{storm_id}"
            data = self.client.get(key)

            if data:
                return json.loads(data)
            return None

        except Exception as e:
            logger.error(f"Error fetching from Redis: {e}")
            return None

    def get_all_active_cyclones(self) -> List[Dict[str, Any]]:
        """Get all active cyclones from cache"""
        try:
            # Get all active storm IDs
            active_ids = self.client.smembers('cyclone:active_ids')

            if not active_ids:
                return []

            cyclones = []
            for storm_id in active_ids:
                data = self.get_live_cyclone(storm_id)
                if data:
                    cyclones.append(data)

            return cyclones

        except Exception as e:
            logger.error(f"Error fetching active cyclones: {e}")
            return []

    def cache_cyclone(self, storm_id: str, data: Dict[str, Any]):
        """Cache cyclone data"""
        try:
            key = f"cyclone:live:{storm_id}"
            self.client.setex(
                key,
                self.ttl,
                json.dumps(data)
            )

            # Add to active set
            self.client.sadd('cyclone:active_ids', storm_id)
            self.client.expire('cyclone:active_ids', self.ttl)

        except Exception as e:
            logger.error(f"Error caching cyclone data: {e}")

    def cache_forecast(self, storm_id: str, forecast_data: List[Dict[str, Any]]):
        """Cache forecast data for a cyclone"""
        try:
            key = f"cyclone:forecast:{storm_id}"
            self.client.setex(
                key,
                self.ttl,
                json.dumps(forecast_data)
            )
        except Exception as e:
            logger.error(f"Error caching forecast: {e}")

    def get_cached_forecast(self, storm_id: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached forecast data"""
        try:
            key = f"cyclone:forecast:{storm_id}"
            data = self.client.get(key)

            if data:
                return json.loads(data)
            return None

        except Exception as e:
            logger.error(f"Error fetching cached forecast: {e}")
            return None

    def get_statistics(self) -> Dict[str, Any]:
        """Get cached statistics"""
        try:
            stats_key = 'cyclone:stats:realtime'
            stats = self.client.hgetall(stats_key)

            if stats:
                # Convert string values to appropriate types
                return {
                    'total_observations': int(stats.get('total_observations', 0)),
                    'active_storms': int(stats.get('active_storms', 0)),
                    'last_update': stats.get('last_update', '')
                }

            return {
                'total_observations': 0,
                'active_storms': 0,
                'last_update': ''
            }

        except Exception as e:
            logger.error(f"Error fetching statistics: {e}")
            return {}

    def clear_cache(self, pattern: str = "cyclone:*"):
        """Clear cache by pattern"""
        try:
            keys = self.client.keys(pattern)
            if keys:
                self.client.delete(*keys)
                logger.info(f"Cleared {len(keys)} keys matching {pattern}")
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")

    def close(self):
        """Close Redis connection"""
        if self.client:
            self.client.close()
            logger.info("Redis connection closed")