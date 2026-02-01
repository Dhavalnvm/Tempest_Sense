"""
Kafka Message Serializers
Handles serialization and deserialization of cyclone data for Kafka
"""
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass, asdict
import msgpack
import pickle

logger = logging.getLogger(__name__)


class SerializationError(Exception):
    """Exception raised for serialization errors"""
    pass


class DeserializationError(Exception):
    """Exception raised for deserialization errors"""
    pass


class JSONSerializer:
    """JSON serializer for Kafka messages"""

    @staticmethod
    def serialize(data: Any) -> bytes:
        """
        Serialize data to JSON bytes

        Args:
            data: Data to serialize (dict, list, etc.)

        Returns:
            Serialized bytes
        """
        try:
            # Convert datetime objects to ISO format
            json_str = json.dumps(data, default=str)
            return json_str.encode('utf-8')
        except Exception as e:
            logger.error(f"JSON serialization failed: {e}")
            raise SerializationError(f"Failed to serialize: {e}")

    @staticmethod
    def deserialize(data: bytes) -> Any:
        """
        Deserialize JSON bytes to Python object

        Args:
            data: Serialized bytes

        Returns:
            Deserialized object
        """
        try:
            json_str = data.decode('utf-8')
            return json.loads(json_str)
        except Exception as e:
            logger.error(f"JSON deserialization failed: {e}")
            raise DeserializationError(f"Failed to deserialize: {e}")


class MessagePackSerializer:
    """MessagePack serializer for efficient binary serialization"""

    @staticmethod
    def serialize(data: Any) -> bytes:
        """
        Serialize data using MessagePack

        Args:
            data: Data to serialize

        Returns:
            Serialized bytes
        """
        try:
            return msgpack.packb(data, use_bin_type=True, datetime=True)
        except Exception as e:
            logger.error(f"MessagePack serialization failed: {e}")
            raise SerializationError(f"Failed to serialize: {e}")

    @staticmethod
    def deserialize(data: bytes) -> Any:
        """
        Deserialize MessagePack bytes

        Args:
            data: Serialized bytes

        Returns:
            Deserialized object
        """
        try:
            return msgpack.unpackb(data, raw=False, timestamp=3)
        except Exception as e:
            logger.error(f"MessagePack deserialization failed: {e}")
            raise DeserializationError(f"Failed to deserialize: {e}")


class PickleSerializer:
    """Pickle serializer (use with caution - only for trusted data)"""

    @staticmethod
    def serialize(data: Any) -> bytes:
        """
        Serialize data using pickle

        Args:
            data: Data to serialize

        Returns:
            Serialized bytes
        """
        try:
            return pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            logger.error(f"Pickle serialization failed: {e}")
            raise SerializationError(f"Failed to serialize: {e}")

    @staticmethod
    def deserialize(data: bytes) -> Any:
        """
        Deserialize pickle bytes

        Args:
            data: Serialized bytes

        Returns:
            Deserialized object
        """
        try:
            return pickle.loads(data)
        except Exception as e:
            logger.error(f"Pickle deserialization failed: {e}")
            raise DeserializationError(f"Failed to deserialize: {e}")


@dataclass
class CycloneMessage:
    """
    Structured cyclone message for Kafka
    """
    id: str
    name: str
    basin: str
    latitude: float
    longitude: float
    max_sustained_wind: Optional[float] = None
    central_pressure: Optional[float] = None
    classification: str = "Unknown"
    intensity: str = "Unknown"
    movement_speed: float = 0.0
    movement_direction: float = 0.0
    timestamp: str = ""
    data_source: str = "NOAA"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CycloneMessage':
        """Create from dictionary"""
        return cls(**data)

    def serialize(self, format: str = 'json') -> bytes:
        """
        Serialize message

        Args:
            format: Serialization format ('json', 'msgpack', 'pickle')

        Returns:
            Serialized bytes
        """
        data = self.to_dict()

        if format == 'json':
            return JSONSerializer.serialize(data)
        elif format == 'msgpack':
            return MessagePackSerializer.serialize(data)
        elif format == 'pickle':
            return PickleSerializer.serialize(data)
        else:
            raise ValueError(f"Unknown format: {format}")

    @classmethod
    def deserialize(cls, data: bytes, format: str = 'json') -> 'CycloneMessage':
        """
        Deserialize message

        Args:
            data: Serialized bytes
            format: Serialization format

        Returns:
            CycloneMessage instance
        """
        if format == 'json':
            dict_data = JSONSerializer.deserialize(data)
        elif format == 'msgpack':
            dict_data = MessagePackSerializer.deserialize(data)
        elif format == 'pickle':
            dict_data = PickleSerializer.deserialize(data)
        else:
            raise ValueError(f"Unknown format: {format}")

        return cls.from_dict(dict_data)


class KafkaValueSerializer:
    """
    Kafka value serializer that handles different formats
    """

    def __init__(self, format: str = 'json'):
        """
        Initialize serializer

        Args:
            format: Serialization format ('json', 'msgpack', 'pickle')
        """
        self.format = format.lower()

        self.serializers = {
            'json': JSONSerializer,
            'msgpack': MessagePackSerializer,
            'pickle': PickleSerializer
        }

        if self.format not in self.serializers:
            raise ValueError(f"Unknown format: {self.format}")

    def __call__(self, data: Any) -> bytes:
        """
        Serialize data for Kafka

        Args:
            data: Data to serialize

        Returns:
            Serialized bytes
        """
        serializer = self.serializers[self.format]
        return serializer.serialize(data)


class KafkaValueDeserializer:
    """
    Kafka value deserializer that handles different formats
    """

    def __init__(self, format: str = 'json'):
        """
        Initialize deserializer

        Args:
            format: Serialization format
        """
        self.format = format.lower()

        self.deserializers = {
            'json': JSONSerializer,
            'msgpack': MessagePackSerializer,
            'pickle': PickleSerializer
        }

        if self.format not in self.deserializers:
            raise ValueError(f"Unknown format: {self.format}")

    def __call__(self, data: bytes) -> Any:
        """
        Deserialize data from Kafka

        Args:
            data: Serialized bytes

        Returns:
            Deserialized object
        """
        if data is None:
            return None

        deserializer = self.deserializers[self.format]
        return deserializer.deserialize(data)


class KafkaKeySerializer:
    """Simple key serializer (strings to UTF-8)"""

    @staticmethod
    def __call__(key: Optional[str]) -> Optional[bytes]:
        """Serialize key"""
        if key is None:
            return None
        return key.encode('utf-8')


class KafkaKeyDeserializer:
    """Simple key deserializer (UTF-8 to strings)"""

    @staticmethod
    def __call__(data: Optional[bytes]) -> Optional[str]:
        """Deserialize key"""
        if data is None:
            return None
        return data.decode('utf-8')


def validate_cyclone_message(data: Dict[str, Any]) -> bool:
    """
    Validate cyclone message has required fields

    Args:
        data: Message data dictionary

    Returns:
        True if valid, False otherwise
    """
    required_fields = ['id', 'latitude', 'longitude']

    for field in required_fields:
        if field not in data:
            logger.warning(f"Missing required field: {field}")
            return False

        if data[field] is None:
            logger.warning(f"Required field is None: {field}")
            return False

    return True


def create_cyclone_message(data: Dict[str, Any]) -> Optional[CycloneMessage]:
    """
    Create cyclone message from raw data

    Args:
        data: Raw cyclone data

    Returns:
        CycloneMessage or None if invalid
    """
    try:
        if not validate_cyclone_message(data):
            return None

        return CycloneMessage.from_dict(data)
    except Exception as e:
        logger.error(f"Failed to create cyclone message: {e}")
        return None


# Example usage and testing
if __name__ == "__main__":
    # Test data
    test_cyclone = {
        'id': 'AL012025',
        'name': 'ANNA',
        'basin': 'Atlantic',
        'latitude': 25.5,
        'longitude': -80.3,
        'max_sustained_wind': 85.0,
        'central_pressure': 985.0,
        'classification': 'Hurricane',
        'intensity': 'Category 2',
        'movement_speed': 15.5,
        'movement_direction': 270.0,
        'timestamp': datetime.utcnow().isoformat(),
        'data_source': 'NOAA'
    }

    print("=" * 60)
    print("Testing Serializers")
    print("=" * 60)

    # Test JSON
    print("\nJSON Serialization:")
    json_bytes = JSONSerializer.serialize(test_cyclone)
    print(f"  Serialized: {len(json_bytes)} bytes")
    json_data = JSONSerializer.deserialize(json_bytes)
    print(f"  Deserialized: {json_data['id']}")

    # Test MessagePack
    print("\nMessagePack Serialization:")
    try:
        msgpack_bytes = MessagePackSerializer.serialize(test_cyclone)
        print(f"  Serialized: {len(msgpack_bytes)} bytes")
        msgpack_data = MessagePackSerializer.deserialize(msgpack_bytes)
        print(f"  Deserialized: {msgpack_data['id']}")
        print(f"  Size reduction: {(1 - len(msgpack_bytes) / len(json_bytes)) * 100:.1f}%")
    except ImportError:
        print("  MessagePack not installed (pip install msgpack)")

    # Test CycloneMessage
    print("\nCycloneMessage:")
    msg = CycloneMessage.from_dict(test_cyclone)
    print(f"  Created: {msg.id} - {msg.name}")

    msg_bytes = msg.serialize('json')
    print(f"  Serialized: {len(msg_bytes)} bytes")

    msg_restored = CycloneMessage.deserialize(msg_bytes, 'json')
    print(f"  Restored: {msg_restored.id} - {msg_restored.name}")

    # Test Kafka serializers
    print("\nKafka Serializers:")
    value_serializer = KafkaValueSerializer('json')
    key_serializer = KafkaKeySerializer()

    key_bytes = key_serializer(test_cyclone['id'])
    value_bytes = value_serializer(test_cyclone)

    print(f"  Key: {len(key_bytes)} bytes")
    print(f"  Value: {len(value_bytes)} bytes")

    value_deserializer = KafkaValueDeserializer('json')
    key_deserializer = KafkaKeyDeserializer()

    key_restored = key_deserializer(key_bytes)
    value_restored = value_deserializer(value_bytes)

    print(f"  Restored key: {key_restored}")
    print(f"  Restored value: {value_restored['id']}")

    # Test validation
    print("\nValidation:")
    print(f"  Valid message: {validate_cyclone_message(test_cyclone)}")

    invalid_cyclone = {'name': 'TEST'}
    print(f"  Invalid message: {validate_cyclone_message(invalid_cyclone)}")

    print("\n" + "=" * 60)