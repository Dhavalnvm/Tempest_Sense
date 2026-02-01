"""
Kafka Consumer - Reads cyclone data from Kafka and stores in ClickHouse & Redis
Handles real-time stream processing and data persistence
"""
import json
import logging
import sys
from typing import Dict, Any, List
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import clickhouse_connect
import redis

# Import from parent directory
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_ingestion.config import settings

logging.basicConfig(
    level=getattr(logging, settings.log.level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CycloneDataConsumer:
    """Consumes cyclone data from Kafka and persists to storage"""

    def __init__(self):
        self.config = settings
        self.consumer = None
        self.clickhouse_client = None
        self.redis_client = None
        self.batch_size = 100
        self.batch_buffer = []

        self._init_clickhouse()
        self._init_redis()
        self._init_kafka()

    def _init_clickhouse(self):
        """Initialize ClickHouse connection"""
        try:
            # Get ClickHouse config from environment
            host = os.getenv('CLICKHOUSE_HOST', 'localhost')
            port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
            user = os.getenv('CLICKHOUSE_USER', 'admin')
            password = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')
            database = os.getenv('CLICKHOUSE_DATABASE', 'cyclones')

            self.clickhouse_client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=user,
                password=password,
                database=database
            )

            # Test connection
            result = self.clickhouse_client.query("SELECT 1")
            logger.info(f"ClickHouse connected to {host}:{port}/{database}")

        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def _init_redis(self):
        """Initialize Redis connection"""
        try:
            self.redis_client = redis.Redis(
                host=self.config.redis.host,
                port=self.config.redis.port,
                db=self.config.redis.db,
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info(f"Redis connected to {self.config.redis.host}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None

    def _init_kafka(self):
        """Initialize Kafka consumer"""
        try:
            # Get Kafka config
            bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

            self.consumer = KafkaConsumer(
                self.config.kafka.topic_positions,
                self.config.kafka.topic_updates,
                bootstrap_servers=bootstrap,
                auto_offset_reset='latest',
                enable_auto_commit=False,
                group_id='cyclone-consumer-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                max_poll_records=500,
                session_timeout_ms=30000
            )

            logger.info(f"Kafka consumer connected, subscribed to topics")

        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise

    def store_position_clickhouse(self, data: Dict[str, Any]):
        """Store cyclone position in ClickHouse"""
        try:
            # Prepare data for insertion
            row = [
                data.get('id', ''),
                data.get('name', ''),
                data.get('basin', ''),
                data.get('classification', ''),
                data.get('intensity', ''),
                float(data.get('latitude', 0)),
                float(data.get('longitude', 0)),
                float(data.get('movement_speed', 0)),
                float(data.get('movement_direction', 0)),
                float(data.get('central_pressure', 0)) if data.get('central_pressure') else 0,
                float(data.get('max_sustained_wind', 0)) if data.get('max_sustained_wind') else 0,
                datetime.fromisoformat(data.get('timestamp', datetime.utcnow().isoformat()).replace('Z', '+00:00')),
                'NOAA'
            ]

            # Add to batch
            self.batch_buffer.append(row)

            # Insert batch if buffer is full
            if len(self.batch_buffer) >= self.batch_size:
                self._flush_batch()

        except Exception as e:
            logger.error(f"Error preparing data for ClickHouse: {e}")

    def _flush_batch(self):
        """Flush batch buffer to ClickHouse"""
        if not self.batch_buffer:
            return

        try:
            self.clickhouse_client.insert(
                'cyclone_positions',
                self.batch_buffer,
                column_names=[
                    'id', 'name', 'basin', 'classification', 'intensity',
                    'latitude', 'longitude', 'movement_speed', 'movement_direction',
                    'central_pressure', 'max_sustained_wind', 'timestamp', 'data_source'
                ]
            )

            count = len(self.batch_buffer)
            logger.info(f"Inserted {count} records to ClickHouse")
            self.batch_buffer = []

        except Exception as e:
            logger.error(f"Failed to insert batch to ClickHouse: {e}")
            self.batch_buffer = []

    def store_position_redis(self, data: Dict[str, Any]):
        """Store cyclone position in Redis for fast access"""
        if not self.redis_client:
            return

        try:
            storm_id = data.get('id')

            # Store individual storm data
            redis_key = f"cyclone:live:{storm_id}"
            self.redis_client.setex(
                redis_key,
                self.config.redis.ttl,
                json.dumps(data)
            )

            # Update active storms set
            self.redis_client.sadd('cyclone:active_ids', storm_id)
            self.redis_client.expire('cyclone:active_ids', self.config.redis.ttl)

            # Store latest position by basin
            basin = data.get('basin', 'unknown')
            self.redis_client.setex(
                f"cyclone:basin:{basin}:{storm_id}",
                self.config.redis.ttl,
                json.dumps(data)
            )

            # Update statistics
            self._update_redis_stats(data)

        except Exception as e:
            logger.error(f"Error storing to Redis: {e}")

    def _update_redis_stats(self, data: Dict[str, Any]):
        """Update real-time statistics in Redis"""
        try:
            stats_key = 'cyclone:stats:realtime'

            # Increment total observations
            self.redis_client.hincrby(stats_key, 'total_observations', 1)

            # Track active storms count
            active_count = self.redis_client.scard('cyclone:active_ids')
            self.redis_client.hset(stats_key, 'active_storms', active_count)

            # Update last update time
            self.redis_client.hset(
                stats_key,
                'last_update',
                datetime.utcnow().isoformat()
            )

            self.redis_client.expire(stats_key, self.config.redis.ttl)

        except Exception as e:
            logger.error(f"Error updating Redis stats: {e}")

    def update_cyclone_metadata(self, data: Dict[str, Any]):
        """Update cyclone metadata table"""
        try:
            storm_id = data.get('id')

            # Check if metadata exists
            query = f"SELECT count() FROM cyclone_metadata WHERE id = '{storm_id}'"
            result = self.clickhouse_client.query(query)
            exists = result.result_rows[0][0] > 0

            if not exists:
                # Insert new metadata
                self.clickhouse_client.insert(
                    'cyclone_metadata',
                    [[
                        storm_id,
                        data.get('name', ''),
                        data.get('basin', ''),
                        datetime.fromisoformat(
                            data.get('timestamp', datetime.utcnow().isoformat()).replace('Z', '+00:00')),
                        None,
                        data.get('intensity', ''),
                        float(data.get('max_sustained_wind', 0)) if data.get('max_sustained_wind') else 0,
                        float(data.get('central_pressure', 0)) if data.get('central_pressure') else 0,
                        1,
                        True
                    ]],
                    column_names=[
                        'id', 'name', 'basin', 'formation_date', 'dissipation_date',
                        'peak_intensity', 'peak_wind', 'min_pressure', 'total_advisories', 'is_active'
                    ]
                )
                logger.debug(f"Created metadata for {storm_id}")
            else:
                # Update existing (ReplacingMergeTree will handle)
                logger.debug(f"Metadata exists for {storm_id}")

        except Exception as e:
            logger.error(f"Error updating metadata: {e}")

    def process_message(self, message):
        """Process a single Kafka message"""
        try:
            data = message.value
            topic = message.topic

            if topic == self.config.kafka.topic_positions:
                # Store position data
                self.store_position_clickhouse(data)
                self.store_position_redis(data)
                self.update_cyclone_metadata(data)

            elif topic == self.config.kafka.topic_updates:
                # Process batch update
                storms = data.get('storms', [])
                logger.info(f"Processing batch update with {len(storms)} storms")

                for storm in storms:
                    self.store_position_redis(storm)

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    def run(self):
        """Main consumer loop"""
        logger.info("Starting Kafka consumer...")
        logger.info(f"Subscribed to: {list(self.consumer.subscription())}")

        message_count = 0

        try:
            for message in self.consumer:
                message_count += 1

                self.process_message(message)

                # Commit offset periodically
                if message_count % 100 == 0:
                    self._flush_batch()
                    self.consumer.commit()
                    logger.info(f"Processed {message_count} messages, committed offset")

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}", exc_info=True)
        finally:
            self.shutdown()

    def shutdown(self):
        """Cleanup resources"""
        logger.info("Shutting down consumer...")

        # Flush remaining batch
        self._flush_batch()

        if self.consumer:
            self.consumer.commit()
            self.consumer.close()

        if self.clickhouse_client:
            self.clickhouse_client.close()

        if self.redis_client:
            self.redis_client.close()

        logger.info("Consumer shutdown complete")


def main():
    """Entry point"""
    try:
        consumer = CycloneDataConsumer()
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()