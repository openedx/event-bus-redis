from typing import NamedTuple, Optional, Union

from openedx_events.tooling import EventsMetadata, OpenEdxPublicSignal

from edx_event_bus_redis.internal.utils import get_headers_from_metadata, get_metadata_from_headers

class UnusableMessageError(Exception):
    """
    Indicates that a message was successfully received but could not be processed.

    This could be invalid headers, an unknown signal, or other issue specific to
    the contents of the message.
    """


class RedisMessage(NamedTuple):
    topic: str
    event_data: bytes
    event_metadata: EventsMetadata
    msg_id: Optional[bytes] = None

    def to_binary_dict(self):
        data = get_headers_from_metadata(self.event_metadata)
        data["event_data"] = self.event_data
        return data

    @classmethod
    def parse(cls, msg: Union[list, tuple], topic: str, expected_signal: Optional[OpenEdxPublicSignal] = None):
        if isinstance(msg, list):
            msg = msg[0]
        msg_id, data = msg
        event_data_bytes = data.pop(b'event_data')
        try:
            metadata = get_metadata_from_headers(data)
        except Exception as e:
            raise UnusableMessageError(f"Error determining metadata from message headers: {e}") from e

        if expected_signal and metadata.event_type != expected_signal.event_type:
            raise UnusableMessageError(
                f"Signal types do not match. Expected {expected_signal.event_type}. "
                f"Received message of type {metadata.event_type}."
            )
        return cls(msg_id=msg_id, event_data=event_data_bytes, event_metadata=metadata, topic=topic)
