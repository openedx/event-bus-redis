"""
Utilities for converting between message headers and EventsMetadata
"""

import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Optional, Tuple
from uuid import UUID

import openedx_events.data as oed
from edx_toggles.toggles import SettingToggle

logger = logging.getLogger(__name__)

# .. toggle_name: EVENT_BUS_REDIS_AUDIT_LOGGING_ENABLED
# .. toggle_implementation: SettingToggle
# .. toggle_default: True
# .. toggle_description: If True, whenever an event is produced or consumed, log enough
#   information to uniquely identify it for debugging purposes. This will not include
#   all the data on the event, but at a minimum will include topic, partition, offset,
#   message ID, and key. Deployers may wish to disable this if log volume is excessive.
# .. toggle_use_cases: opt_out
# .. toggle_creation_date: 2023-02-24
AUDIT_LOGGING_ENABLED = SettingToggle('EVENT_BUS_REDIS_AUDIT_LOGGING_ENABLED', default=True)


def _sourcelib_tuple_to_str(sourcelib: Tuple):
    return ".".join(map(str, sourcelib))


def _sourcelib_str_to_tuple(sourcelib_as_str: str):
    return tuple(map(int, sourcelib_as_str.split(".")))


class MessageHeader:
    """
    Utility class for converting between message headers and EventsMetadata objects
    """
    _mapping = {}
    instances = []

    def __init__(self, message_header_key, event_metadata_field=None, to_metadata=None, from_metadata=None):
        self.message_header_key = message_header_key
        self.event_metadata_field = event_metadata_field
        self.to_metadata = to_metadata or (lambda x: x)
        self.from_metadata = from_metadata or (lambda x: x)
        self.__class__.instances.append(self)
        self.__class__._mapping[self.message_header_key] = self


HEADER_EVENT_TYPE = MessageHeader("type", event_metadata_field="event_type")
HEADER_ID = MessageHeader("id", event_metadata_field="id", from_metadata=str, to_metadata=UUID)
HEADER_SOURCE = MessageHeader("source", event_metadata_field="source")
HEADER_TIME = MessageHeader("time", event_metadata_field="time",
                            to_metadata=lambda x: datetime.fromisoformat(x),  # pylint: disable=unnecessary-lambda
                            from_metadata=lambda x: x.isoformat())
HEADER_MINORVERSION = MessageHeader("minorversion", event_metadata_field="minorversion", to_metadata=int,
                                    from_metadata=str)
HEADER_SOURCEHOST = MessageHeader("sourcehost", event_metadata_field="sourcehost")
HEADER_SOURCELIB = MessageHeader("sourcelib", event_metadata_field="sourcelib",
                                 to_metadata=_sourcelib_str_to_tuple, from_metadata=_sourcelib_tuple_to_str)


def get_message_header_values(headers: List, header: MessageHeader) -> List[str]:
    """
    Return all values for this header.

    Arguments:
        headers: List of key/value tuples. Keys are strings, values are bytestrings.
        header: The MessageHeader to look for.

    Returns:
        List of zero or more header values decoded as strings.
    """
    return [value.decode("utf-8") for key, value in headers if key == header.message_header_key]


def get_metadata_from_headers(headers: dict):
    """
    Create an EventsMetadata object from the headers of a Redis message

    Arguments
        headers: The list of headers returned from calling message.headers() on a consumed message

    Returns
        An instance of EventsMetadata with the parameters from the headers. Any fields missing from the headers
         are set to the defaults of the EventsMetadata class
    """
    # go through all the headers we care about and set the appropriate field
    metadata = {}
    for header in MessageHeader.instances:
        metadata_field = header.event_metadata_field
        if not metadata_field:
            continue
        header_key = header.message_header_key
        header_value = headers.get(header_key.encode("utf8"))
        if header_value:
            metadata[header.event_metadata_field] = header.to_metadata(header_value.decode("utf8"))
    return oed.EventsMetadata(**metadata)


def get_headers_from_metadata(event_metadata: oed.EventsMetadata):
    """
    Create a dictionary of headers from an EventsMetadata object.

    This method assumes the EventsMetadata object was the one sent with the event data to the original signal handler.

    Arguments:
        event_metadata: An EventsMetadata object sent by an OpenEdxPublicSignal

    Returns:
        A dictionary of headers where the keys are strings and values are binary
    """
    values = {}
    for header in MessageHeader.instances:
        if not header.event_metadata_field:
            continue
        event_metadata_value = getattr(event_metadata, header.event_metadata_field)
        # Convert string to utf8 encoded bytes
        values[header.message_header_key] = header.from_metadata(event_metadata_value).encode("utf8")

    return values
