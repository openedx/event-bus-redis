"""
Redis message wrapper.
"""
from typing import Dict, NamedTuple, Optional

from openedx_events.tooling import EventsMetadata

from edx_event_bus_redis.internal.utils import get_headers_from_metadata, get_metadata_from_headers


class UnusableMessageError(Exception):
    """
    Indicates that a message was successfully received but could not be processed.

    This could be invalid headers, an unknown signal, or other issue specific to
    the contents of the message.
    """


class RedisMessage(NamedTuple):
    """
    Redis message wrapper with ability to parse to & from redis msg tuple.
    """

    topic: str
    event_data: bytes
    event_metadata: EventsMetadata
    msg_id: Optional[bytes] = None

    def to_binary_dict(self) -> Dict[bytes, bytes]:
        """
        Convert instance to dictionary with binary key value pairs.
        """
        data = get_headers_from_metadata(self.event_metadata)
        data[b"event_data"] = self.event_data
        return data

    @classmethod
    def parse(cls, msg: tuple, topic: str):
        """
        Take message from redis stream and parses it to return an instance of RedisMessage.

        Args:
            msg: Tuple with 1st item being msg_id and 2nd data from message.
            topic: Stream name.

        Returns:
            RedisMessage with msg_id
        """
        try:
            msg_id, data = msg
            event_data_bytes = data[b'event_data']
            metadata = get_metadata_from_headers(data)
        except Exception as e:
            raise UnusableMessageError(f"Error determining metadata from message headers: {e}") from e

        return cls(msg_id=msg_id, event_data=event_data_bytes, event_metadata=metadata, topic=topic)
