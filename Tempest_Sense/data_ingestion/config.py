"""
Configuration module for cyclone data ingestion
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class KafkaConfig:
    """Kafka connection configuration"""
    bootstrap_servers: str
    topic_updates: str
    topic_positions: str
    topic_forecasts: str

    @classmethod
    def from_env(cls):
        return cls(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            topic_updates=os.getenv('KAFKA_TOPIC_CYCLONE_UPDATES', 'cyclone-updates'),
            topic_positions=os.getenv('KAFKA_TOPIC_CYCLONE_POSITIONS', 'cyclone-positions'),
            topic_forecasts=os.getenv('KAFKA_TOPIC_CYCLONE_FORECASTS', 'cyclone-forecasts')
        )


@dataclass
class RedisConfig:
    """Redis connection configuration"""
    host: str
    port: int
    db: int
    ttl: int

    @classmethod
    def from_env(cls):
        return cls(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', '6379')),
            db=int(os.getenv('REDIS_DB', '0')),
            ttl=int(os.getenv('REDIS_TTL', '3600'))
        )


@dataclass
class NOAAConfig:
    """NOAA API configuration"""
    api_url: str
    fetch_interval: int
    timeout: int = 30
    max_retries: int = 3

    @classmethod
    def from_env(cls):
        return cls(
            api_url=os.getenv('NOAA_API_URL', 'https://www.nhc.noaa.gov/CurrentStorms.json'),
            fetch_interval=int(os.getenv('NOAA_FETCH_INTERVAL', '300'))
        )


@dataclass
class LogConfig:
    """Logging configuration"""
    level: str
    format: str

    @classmethod
    def from_env(cls):
        return cls(
            level=os.getenv('LOG_LEVEL', 'INFO'),
            format=os.getenv('LOG_FORMAT', 'json')
        )


class Settings:
    """Global settings singleton"""
    _instance: Optional['Settings'] = None

    def __init__(self):
        self.kafka = KafkaConfig.from_env()
        self.redis = RedisConfig.from_env()
        self.noaa = NOAAConfig.from_env()
        self.log = LogConfig.from_env()

    @classmethod
    def get_instance(cls) -> 'Settings':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance


# Global settings instance
settings = Settings.get_instance()