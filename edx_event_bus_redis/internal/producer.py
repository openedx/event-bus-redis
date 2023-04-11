"""
Produce Redis events from signals.

Main function is ``create_producer()``, which should be referred to from ``EVENT_BUS_PRODUCER``.
"""

import logging
from typing import Optional

import attr
from edx_django_utils.monitoring import record_exception
from openedx_events.data import EventsMetadata
from openedx_events.event_bus import EventBusProducer
from openedx_events.event_bus.avro.serializer import serialize_event_data_to_bytes
from openedx_events.tooling import OpenEdxPublicSignal
from walrus import Database

from edx_event_bus_redis.internal.message import RedisMessage

from .config import get_full_topic, load_common_settings
from .utils import AUDIT_LOGGING_ENABLED

logger = logging.getLogger(__name__)


def record_producing_error(error, context):
    """
    Record an error in producing an event to both the monitoring system and the regular logs

    Arguments:
        error: The exception or error raised during producing
        context: An instance of ProducingContext containing additional information about the message
    """
    try:
        # record_exception() is a wrapper around a New Relic method that can only be called within an except block,
        # so first re-raise the error
        raise Exception(error)  # pylint: disable=broad-exception-raised
    except BaseException:
        record_exception()
        logger.exception(f"Error delivering message to Redis event bus. {error=!s} {context!r}")


@attr.s(kw_only=True, repr=False)
class ProducingContext:
    """
    Wrapper class to allow us to link a call to produce() with the on_event_deliver callback
    """
    full_topic = attr.ib(type=str, default=None)
    event_key = attr.ib(type=str, default=None)
    signal = attr.ib(type=OpenEdxPublicSignal, default=None)
    initial_topic = attr.ib(type=str, default=None)
    event_key_field = attr.ib(type=str, default=None)
    event_data = attr.ib(type=dict, default=None)
    event_metadata = attr.ib(type=EventsMetadata, default=None)

    def __repr__(self):
        """Create a logging-friendly string"""
        return " ".join([f"{key}={value!r}" for key, value in attr.asdict(self, recurse=False).items()])

    def on_event_deliver(self, redis_msg_id):
        """
        Simple method for debugging event production

        Arguments:
            msg_id: Stream event msg_id.
        """
        # TODO: see if below ADR can be moved to openedx_events.
        # Audit logging on success.
        # See ADR: https://github.com/openedx/event-bus-kafka/blob/main/docs/decisions/0010_audit_logging.rst
        if not AUDIT_LOGGING_ENABLED.is_enabled():
            return

        message_id = self.event_metadata.id
        # See ADR for details on why certain fields were included or omitted.
        logger.info(
            f"Message delivered to Redis event bus: topic={self.full_topic}, "
            f"message_id={message_id}, signal={self.signal}, redis_msg_id={redis_msg_id}"
        )


class RedisEventProducer(EventBusProducer):
    """
    API singleton for event production to Redis.

    Only one instance (of Producer or this wrapper) should be created,
    since it is stateful and needs lifecycle management.
    """

    def __init__(self, client):
        self.client = client

    def send(
            self, *, signal: OpenEdxPublicSignal, topic: str, event_key_field: str, event_data: dict,
            event_metadata: EventsMetadata
    ) -> None:
        """
        Send a signal event to the event bus under the specified topic.

        Arguments:
            signal: The original OpenEdxPublicSignal the event was sent to
            topic: The base (un-prefixed) event bus topic for the event
            event_key_field: Path to the event data field to use as the event key (period-delimited
              string naming the dictionary keys to descend). Not used in redis event bus.
            event_data: The event data (kwargs) sent to the signal
            event_metadata: An EventsMetadata object with all the metadata necessary for the CloudEvent spec
        """

        # keep track of the initial arguments for recreating the event in the logs if necessary later
        context = ProducingContext(
            signal=signal,
            initial_topic=topic,
            event_key_field=event_key_field,
            event_data=event_data,
            event_metadata=event_metadata
        )
        try:
            full_topic = get_full_topic(topic)
            context.full_topic = full_topic
            event_bytes = serialize_event_data_to_bytes(event_data, signal)
            message = RedisMessage(topic=full_topic, event_data=event_bytes, event_metadata=event_metadata)
            stream_data = message.to_binary_dict()

            stream = self.client.Stream(full_topic)
            msg_id = stream.add(stream_data)
            context.on_event_deliver(msg_id)
        except Exception as e:  # pylint: disable=broad-except
            record_producing_error(e, context)


def create_producer() -> Optional[RedisEventProducer]:
    """
    Create a Producer API instance. Caller should cache the returned object.
    """
    producer_settings = load_common_settings()
    if producer_settings is None or "url" not in producer_settings:
        return None

    client = Database.from_url(producer_settings['url'])
    return RedisEventProducer(client)
