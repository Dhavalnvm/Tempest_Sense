"""
Kafka Producer - Fetches NOAA CurrentStorms.json and publishes to Kafka
Runs continuously to provide real-time cyclone data stream
"""
import json
import logging
import time
import sys
from typing import Dict, Any, List
from datetime import datetime
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
import redis

from config import settings
from parser import CycloneDataParser, validate_cyclone_data

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log.level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NOAADataProducer:
    """Fetches NOAA cyclone data and publishes to Kafka"""

    def __init__(self):
        self.config = settings
        self.parser = CycloneDataParser()
        self.producer = None
        self.redis_client = None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CycloneTracker/1.0 (Real-time Monitoring System)'
        })

        self._init_kafka()
        self._init_redis()

    def _init_kafka(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.kafka.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip',
                linger_ms=10,
                batch_size=16384
            )
            logger.info(f"Kafka producer connected to {self.config.kafka.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def _init_redis(self):
        """Initialize Redis client for caching"""
        try:
            self.redis_client = redis.Redis(
                host=self.config.redis.host,
                port=self.config.redis.port,
                db=self.config.redis.db,
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info(f"Redis client connected to {self.config.redis.host}")
        except Exception as e:
            logger.error(f"Failed to initialize Redis: {e}")
            # Redis is optional, continue without it
            self.redis_client = None

    def fetch_noaa_data(self) -> Dict[str, Any]:
        """Fetch current storms data from NOAA API"""
        try:
            logger.info(f"Fetching data from NOAA: {self.config.noaa.api_url}")

            response = self.session.get(
                self.config.noaa.api_url,
                timeout=self.config.noaa.timeout
            )
            response.raise_for_status()

            data = response.json()
            logger.info(f"Successfully fetched NOAA data")

            # Cache raw response in Redis
            if self.redis_client:
                try:
                    self.redis_client.setex(
                        'noaa:current_storms:raw',
                        self.config.redis.ttl,
                        json.dumps(data)
                    )
                except Exception as e:
                    logger.warning(f"Failed to cache NOAA data in Redis: {e}")

            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch NOAA data: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse NOAA JSON response: {e}")
            raise

    def publish_storm_data(self, storm: Dict[str, Any]):
        """Publish a single storm to Kafka topics"""
        try:
            storm_id = storm.get('id')

            if not validate_cyclone_data(storm):
                logger.warning(f"Invalid storm data for {storm_id}, skipping")
                return

            # Publish to positions topic
            future = self.producer.send(
                self.config.kafka.topic_positions,
                key=storm_id,
                value=storm
            )

            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            logger.debug(
                f"Published {storm_id} to {record_metadata.topic} "
                f"partition {record_metadata.partition} offset {record_metadata.offset}"
            )

            # Cache in Redis for instant access
            if self.redis_client:
                try:
                    redis_key = f"cyclone:live:{storm_id}"
                    self.redis_client.setex(
                        redis_key,
                        self.config.redis.ttl,
                        json.dumps(storm)
                    )

                    # Add to active storms set
                    self.redis_client.sadd('cyclone:active_ids', storm_id)
                    self.redis_client.expire('cyclone:active_ids', self.config.redis.ttl)

                except Exception as e:
                    logger.warning(f"Failed to cache storm in Redis: {e}")

        except KafkaError as e:
            logger.error(f"Kafka error publishing storm {storm.get('id')}: {e}")
        except Exception as e:
            logger.error(f"Error publishing storm data: {e}")

    def publish_update_event(self, storms: List[Dict[str, Any]]):
        """Publish a batch update event with all active storms"""
        try:
            update_event = {
                'timestamp': datetime.utcnow().isoformat(),
                'total_active_storms': len(storms),
                'storm_ids': [s['id'] for s in storms],
                'basins': list(set(s['basin'] for s in storms)),
                'storms': storms
            }

            future = self.producer.send(
                self.config.kafka.topic_updates,
                key='global_update',
                value=update_event
            )

            future.get(timeout=10)
            logger.info(f"Published global update with {len(storms)} active storms")

        except Exception as e:
            logger.error(f"Error publishing update event: {e}")

    def run(self):
        """Main loop - fetch and publish cyclone data continuously"""
        logger.info("Starting NOAA data producer...")
        logger.info(f"Fetch interval: {self.config.noaa.fetch_interval} seconds")

        fetch_count = 0

        while True:
            try:
                fetch_count += 1
                logger.info(f"Starting fetch #{fetch_count}")

                # Fetch from NOAA
                noaa_data = self.fetch_noaa_data()

                # Parse storms
                storms = self.parser.parse_current_storms(noaa_data)

                if not storms:
                    logger.warning("No active storms found in NOAA data")
                else:
                    # Publish each storm
                    for storm in storms:
                        self.publish_storm_data(storm)

                    # Publish global update
                    self.publish_update_event(storms)

                    logger.info(f"Successfully processed {len(storms)} storms")

                # Flush producer
                self.producer.flush()

                # Wait before next fetch
                logger.info(f"Waiting {self.config.noaa.fetch_interval}s until next fetch...")
                time.sleep(self.config.noaa.fetch_interval)

            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                logger.info("Retrying in 60 seconds...")
                time.sleep(60)

        self.shutdown()

    def shutdown(self):
        """Cleanup resources"""
        logger.info("Shutting down producer...")

        if self.producer:
            self.producer.flush()
            self.producer.close()

        if self.redis_client:
            self.redis_client.close()

        self.session.close()

        logger.info("Producer shutdown complete")


def main():
    """Entry point"""
    try:
        producer = NOAADataProducer()
        producer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()