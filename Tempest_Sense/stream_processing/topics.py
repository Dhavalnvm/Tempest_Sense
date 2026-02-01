"""
Kafka topics configuration and management
"""
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logger = logging.getLogger(__name__)


class TopicManager:
    """Manage Kafka topics for cyclone data"""

    TOPICS = {
        'cyclone-updates': {
            'partitions': 1,
            'replication_factor': 1,
            'config': {
                'retention.ms': '604800000',  # 7 days
                'compression.type': 'gzip'
            }
        },
        'cyclone-positions': {
            'partitions': 3,
            'replication_factor': 1,
            'config': {
                'retention.ms': '2592000000',  # 30 days
                'compression.type': 'gzip'
            }
        },
        'cyclone-forecasts': {
            'partitions': 2,
            'replication_factor': 1,
            'config': {
                'retention.ms': '1209600000',  # 14 days
                'compression.type': 'gzip'
            }
        }
    }

    @classmethod
    def create_topics(cls, bootstrap_servers: str):
        """Create all required Kafka topics"""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id='topic_manager'
            )

            topics = []
            for name, config in cls.TOPICS.items():
                topic = NewTopic(
                    name=name,
                    num_partitions=config['partitions'],
                    replication_factor=config['replication_factor'],
                    topic_configs=config['config']
                )
                topics.append(topic)

            try:
                result = admin_client.create_topics(topics, validate_only=False)
                logger.info(f"Created topics: {list(cls.TOPICS.keys())}")
            except TopicAlreadyExistsError:
                logger.info("Topics already exist, skipping creation")

            admin_client.close()

        except Exception as e:
            logger.error(f"Failed to create topics: {e}")
            raise