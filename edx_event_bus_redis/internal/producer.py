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
from openedx_events.tooling import OpenEdxPublicSignal

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

    def on_event_deliver(self, err, evt):
        """
        Simple callback method for debugging event production

        If there is any error, log all the known information about the calling context so the event can be recreated
        and/or resent later. This log will not contain the exact headers but will contain the EventsMetadata object
        that can be used to recreate them.

        Arguments:
            err: Error if event production failed
            evt: Event that was delivered (or failed to be delivered)
        """
        if err is not None:
            record_producing_error(err, self)
        else:
            # Audit logging on success. See ADR: docs/decisions/0010_audit_logging.rst
            if not AUDIT_LOGGING_ENABLED.is_enabled():
                return

            # `evt.headers()` is None in this callback, so we need to use the bound data.
            message_id = self.event_metadata.id
            # See ADR for details on why certain fields were included or omitted.
            logger.info(
                f"Message delivered to Redis event bus: topic={evt.topic()} "
                f"message_id={message_id}, key={evt.key()}"
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
              string naming the dictionary keys to descend)
            event_data: The event data (kwargs) sent to the signal
            event_metadata: An EventsMetadata object with all the metadata necessary for the CloudEvent spec
        """

        # keep track of the initial arguments for recreating the event in the logs if necessary later
        context = ProducingContext(signal=signal, initial_topic=topic, event_key_field=event_key_field,
                                   event_data=event_data, event_metadata=event_metadata)
        try:
            full_topic = get_full_topic(topic)
            context.full_topic = full_topic

            # TODO: add event to redis stream
        except Exception as e:  # pylint: disable=broad-except
            # Errors caused by the produce call should be handled by the on_delivery callback.
            # Here we might expect serialization errors, or any errors from preparing to produce.
            record_producing_error(e, context)


def create_producer() -> Optional[RedisEventProducer]:
    """
    Create a Producer API instance. Caller should cache the returned object.
    """
    producer_settings = load_common_settings()
    if producer_settings is None:
        return None

    # TODO create redis client
    client = None
    return RedisEventProducer(client)
