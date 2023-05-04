"""
Core consumer and event-loop code.
"""
import logging
import time
from typing import Optional

from django.conf import settings
from django.db import connection
from edx_django_utils.monitoring import record_exception, set_custom_attribute
from edx_toggles.toggles import SettingToggle
from openedx_events.event_bus import EventBusConsumer
from openedx_events.event_bus.avro.deserializer import deserialize_bytes_to_event_data
from openedx_events.tooling import OpenEdxPublicSignal
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from walrus import Database
from walrus.containers import ConsumerGroupStream

from edx_event_bus_redis.internal.message import RedisMessage

from .config import get_full_topic, load_common_settings
from .utils import AUDIT_LOGGING_ENABLED

logger = logging.getLogger(__name__)

# .. toggle_name: EVENT_BUS_REDIS_CONSUMERS_ENABLED
# .. toggle_implementation: SettingToggle
# .. toggle_default: True
# .. toggle_description: If set to False, consumer will exit immediately. This can be used as an emergency kill-switch
#   to disable a consumer—as long as the management command is killed and restarted when settings change.
# .. toggle_use_cases: opt_out
# .. toggle_creation_date: 2022-01-31
REDIS_CONSUMERS_ENABLED = SettingToggle('EVENT_BUS_REDIS_CONSUMERS_ENABLED', default=True)

# .. setting_name: EVENT_BUS_REDIS_CONSUMER_POLL_TIMEOUT
# .. setting_default: 1.0
# .. setting_description: How long the consumer should wait, in seconds, for the Redis broker
#   to respond to a poll() call.
CONSUMER_POLL_TIMEOUT = getattr(settings, 'EVENT_BUS_REDIS_CONSUMER_POLL_TIMEOUT', 1)

# .. setting_name: EVENT_BUS_REDIS_CONSUMER_POLL_FAILURE_SLEEP
# .. setting_default: 1.0
# .. setting_description: When the consumer fails to retrieve an event from the broker,
#   it will sleep for this many seconds before trying again. This is to prevent fast error-loops
#   if the broker is down or the consumer is misconfigured. It *may* also sleep for errors that
#   involve receiving an unreadable event, but this could change in the future to be more
#   specific to "no event received from broker".
POLL_FAILURE_SLEEP = getattr(settings, 'EVENT_BUS_REDIS_CONSUMER_POLL_FAILURE_SLEEP', 1.0)


class ReceiverError(Exception):
    """
    Indicates that one or more receivers of a signal raised an exception when called.
    """

    def __init__(self, message: str, causes: list):
        """
        Create ReceiverError with a message and a list of exceptions returned by receivers.
        """
        super().__init__(message)
        self.causes = causes  # just used for testing


def _reconnect_to_db_if_needed():
    """
    Reconnects the db connection if needed.

    This is important because Django only does connection validity/age checks as part of
    its request/response cycle, which isn't in effect for the consume-loop. If we don't
    force these checks, a broken connection will remain broken indefinitely. For most
    consumers, this will cause event processing to fail.
    """
    has_connection = bool(connection.connection)
    requires_reconnect = has_connection and not connection.is_usable()
    if requires_reconnect:
        connection.connect()


class RedisEventConsumer(EventBusConsumer):
    """
    Construct consumer for the given topic, group, and signal. The consumer can then
    emit events from the event bus using the configured signal.

    Note that the topic should be specified here *without* the optional environment prefix.

    Can also consume messages indefinitely off the queue.

    Attributes:
        topic: Topic/stream name.
        group_id: consumer group name.
        signal: openedx_events signal.
        consumer_name: unique name for consumer within a group.
        last_read_msg_id: Start reading msgs from a specific redis msg id.
        check_backlog: flag to process all messages that were not read by this consumer group.
        db: Walrus object for redis connection.
        full_topic: topic prefixed with environment name.
        consumer: consumer instance.
    """

    def __init__(self, topic, group_id, signal, consumer_name, last_read_msg_id=None, check_backlog=False):
        self.topic = topic
        self.group_id = group_id
        self.signal = signal
        self.consumer_name = consumer_name
        self.last_read_msg_id = last_read_msg_id
        self.check_backlog = check_backlog
        self.db = self._create_db()
        self.full_topic = get_full_topic(self.topic)
        self.consumer = self._create_consumer(self.db, self.full_topic)
        self._shut_down_loop = False

    def _create_db(self) -> Database:
        """
        Create a connection to redis
        """
        config = load_common_settings()
        if config is None:
            raise ValueError("Missing redis connection url")
        return Database.from_url(config['url'])

    def _create_consumer(self, db: Database, full_topic: str) -> ConsumerGroupStream:
        """
        Create a redis stream consumer group and a consumer for events of the given signal instance.

        Returns
            ConsumerGroupStream
        """

        # It is possible to track multiple streams using single consumer group.
        # But for simplicity, we are only supporting one stream till the need arises.
        consumer = db.consumer_group(self.group_id, [full_topic], consumer=self.consumer_name)
        try:
            consumer.create(mkstream=True)
        except ResponseError:
            logger.warning("Stream already created by another consumer.")
        # If last_read_msg_id is set, we will replay events after this msg.
        if self.last_read_msg_id:
            consumer.set_id(self.last_read_msg_id)
        # If check_backlog and last_read_msg_id option is not set, only process new messages.
        elif not self.check_backlog:
            consumer.set_id("$")
        return consumer.streams[full_topic]

    def _shut_down(self):
        """
        Test utility for shutting down the consumer loop.
        """
        self._shut_down_loop = True

    def _read_pending_msgs(self) -> Optional[tuple]:
        """
        Read pending messages, if no messages found set check_backlog to False.
        """
        logger.debug("Consuming pending msgs first.")
        msg_meta = self.consumer.pending(count=1, consumer=self.consumer_name)
        if not msg_meta:
            logger.debug("No more pending messages.")
            self.check_backlog = False
            return None
        return self.consumer[msg_meta[0]['message_id']]

    def _consume_indefinitely(self):
        """
        Consume events from a topic in an infinite loop.
        """

        if not REDIS_CONSUMERS_ENABLED.is_enabled():
            logger.error("Redis consumers not enabled, exiting.")
            return

        # .. setting_name: EVENT_BUS_REDIS_CONSUMER_CONSECUTIVE_ERRORS_LIMIT
        # .. setting_default: None
        # .. setting_description: If the consumer encounters this many consecutive errors, exit with an
        #   error. This is intended to be used in a context where a management system (such as Kubernetes)
        #   will relaunch the consumer automatically. The effect is that all runtime state is cleared,
        #   allowing consumers in arbitrary "stuck" states to resume their work automatically. (The impetus
        #   for this setting was a Django DB connection failure that was staying failed.) Process
        #   managers like Kubernetes will use delays and backoffs, so this may also help with transient
        #   issues such as networking problems or a burst of errors in a downstream service. Errors may
        #   include failure to poll, failure to decode events, or errors returned by signal handlers.
        #   This does not prevent committing of offsets back to the broker; any messages that caused an
        #   error will still be marked as consumed, and may need to be replayed.
        CONSECUTIVE_ERRORS_LIMIT = getattr(settings, 'EVENT_BUS_REDIS_CONSUMER_CONSECUTIVE_ERRORS_LIMIT', None)
        run_context = {
            'full_topic': self.full_topic,
            'consumer_group': self.group_id,
            'expected_signal': self.signal,
            'consumer_name': self.consumer_name,
        }

        try:  # pylint: disable=too-many-nested-blocks
            logger.info(f"Running consumer for {run_context!r}")

            # How many errors have we seen in a row? If this climbs too high, exit with error.
            # Any error counts, here — whether due to a polling failure or a message processing
            # failure. But only a successfully processed message clears the counter. Just
            # being able to talk to the broker and get a message (or a normal poll timeout) is
            # not sufficient to show that progress can be made.
            consecutive_errors = 0

            while True:
                # Allow unit tests to break out of loop
                if self._shut_down_loop:
                    break

                # Detect probably-broken consumer and exit with error.
                if CONSECUTIVE_ERRORS_LIMIT and consecutive_errors >= CONSECUTIVE_ERRORS_LIMIT:
                    raise Exception(  # pylint: disable=broad-exception-raised
                        f"Too many consecutive errors, exiting ({consecutive_errors} in a row)"
                    )

                redis_raw_msg = None
                msg: Optional[RedisMessage] = None
                try:
                    # The first time we want to read our pending messages, in case we crashed and are recovering.
                    # Once we consumed our history, we can start getting new messages.
                    if self.check_backlog:
                        redis_raw_msg = self._read_pending_msgs()
                    else:
                        # poll for msg
                        redis_raw_msg = self.consumer.read(count=1, block=CONSUMER_POLL_TIMEOUT * 1000)
                    if redis_raw_msg:
                        if isinstance(redis_raw_msg, list):
                            redis_raw_msg = redis_raw_msg[0]
                        msg = RedisMessage.parse(redis_raw_msg, self.full_topic, expected_signal=self.signal)
                        # Before processing, make sure our db connection is still active
                        _reconnect_to_db_if_needed()
                        self.emit_signals_from_message(msg)
                        consecutive_errors = 0

                    self._add_message_monitoring(run_context=run_context, message=msg)
                except Exception as e:  # pylint: disable=broad-except
                    consecutive_errors += 1
                    self.record_event_consuming_error(run_context, e, msg)
                    # Kill the infinite loop if the error is fatal for the consumer
                    if self._is_fatal_redis_error(error=e):
                        raise e
                    # Prevent fast error-looping when no event received from broker.
                    if not redis_raw_msg:
                        time.sleep(POLL_FAILURE_SLEEP)
                finally:
                    # Acknowledge message as long as it is received. If only successfully processed events are
                    # acknowledged, we might end up trying to process incorrect messages whenever we run with
                    # check_backlog option. This is necessary as redis streams cannot enforce a schema while publishing
                    # like kafka with avro.
                    if redis_raw_msg:
                        self.consumer.ack(redis_raw_msg[0])
        finally:
            self.db.close()

    def consume_indefinitely(self):
        """
        Consume events from a topic in an infinite loop.
        """
        self._consume_indefinitely()

    def emit_signals_from_message(self, msg: RedisMessage):
        """
        Determine the correct signal and send the event from the message.

        Arguments:
            msg (RedisMessage): Consumed message.
        """
        self._log_message_received(msg)

        signal = OpenEdxPublicSignal.get_signal_by_type(msg.event_metadata.event_type)
        event_data = deserialize_bytes_to_event_data(msg.event_data, signal)
        send_results = signal.send_event_with_custom_metadata(msg.event_metadata, **event_data)
        # Raise an exception if any receivers errored out. This allows logging of the receivers
        # along with partition, offset, etc. in record_event_consuming_error. Hopefully the
        # receiver code is idempotent and we can just replay any messages that were involved.
        self._check_receiver_results(send_results)

        # At the very end, log that a message was processed successfully.
        # Since we're single-threaded, no other information is needed;
        # we just need the logger to spit this out with a timestamp.
        # See ADR: docs/decisions/0010_audit_logging.rst
        if AUDIT_LOGGING_ENABLED.is_enabled():
            logger.info('Message from Redis processed successfully')

    def _check_receiver_results(self, send_results: list):
        """
        Raises exception if any of the receivers produced an exception.

        Arguments:
            send_results: Output of ``send_events``, a list of ``(receiver, response)`` tuples.
        """
        error_descriptions = []
        errors = []
        for receiver, response in send_results:
            if not isinstance(response, BaseException):
                continue

            # Probably every receiver will be a regular function or even a lambda with
            # these attrs, so this check is just to be safe.
            try:
                receiver_name = f"{receiver.__module__}.{receiver.__qualname__}"
            except AttributeError:
                receiver_name = str(receiver)

            # The stack traces are already logged by django.dispatcher, so just the error message is fine.
            error_descriptions.append(f"{receiver_name}={response!r}")
            errors.append(response)

        if len(error_descriptions) > 0:
            raise ReceiverError(
                f"{len(error_descriptions)} receiver(s) out of {len(send_results)} "
                "produced errors (stack trace elsewhere in logs) "
                f"when handling signal {self.signal}: {', '.join(error_descriptions)}",
                errors
            )

    def _log_message_received(self, msg: RedisMessage):
        """
        Log that a message was received, for audit log purposes.

        See ADR: docs/decisions/0010_audit_logging.rst

        This will not be sufficient to reconstruct the message; it will just
        be enough to establish a timeline during debugging and to find a message
        by offset.
        """
        if not AUDIT_LOGGING_ENABLED.is_enabled():
            return

        try:
            message_id = msg.event_metadata.id

            timestamp_ms = msg.event_metadata.time.timestamp()

            # See ADR for details on why certain fields were included or omitted.
            logger.info(
                f'Message received from Redis: topic={msg.topic}, '
                f'message_id={message_id}, redis_msg_id={msg.msg_id}, '
                f'event_timestamp_ms={timestamp_ms}'
            )
        except Exception as e:  # pragma: no cover  pylint: disable=broad-except
            # Use this to fix any bugs in what should be benign logging code
            set_custom_attribute('redis_logging_error', repr(e))

    def record_event_consuming_error(self, run_context, error, maybe_message):
        """
        Record an error caught while consuming an event, both to the logs and to telemetry.

        Arguments:
            run_context: Dictionary of contextual information: full_topic, consumer_group,
              and expected_signal.
            error: An exception instance
            maybe_message: None if event could not be fetched or decoded, or a Redis Message if
              one was successfully deserialized but could not be processed for some reason
        """
        context_msg = ", ".join(f"{k}={v!r}" for k, v in run_context.items())
        # Pulls the event message off the error for certain exceptions.
        if maybe_message is None:
            event_msg = "no event available"
        else:
            event_msg = f"event details: {maybe_message}"

        try:
            # This is gross, but our record_exception wrapper doesn't take args at the moment,
            # and will only read the exception from stack context.
            raise Exception(error)  # pylint: disable=broad-exception-raised
        except BaseException:
            self._add_message_monitoring(run_context=run_context, message=maybe_message, error=error)
            record_exception()
            logger.exception(
                f"Error consuming event from Redis: {error!r} in context {context_msg} -- {event_msg}"
            )

    def _add_message_monitoring(self, run_context, message, error=None):
        """
        Record additional details for monitoring.

        Arguments:
            run_context: Dictionary of contextual information: full_topic, consumer_group,
              and expected_signal.
            message: None if event could not be fetched or decoded, or a Message if one
              was successfully deserialized but could not be processed for some reason
            error: (Optional) An exception instance, or None if no error.
        """
        try:
            fatal = self._is_fatal_redis_error(error=error)

            # .. custom_attribute_name: redis_stream
            # .. custom_attribute_description: The full topic of the message or error.
            set_custom_attribute('redis_stream', run_context['full_topic'])

            # .. custom_attribute_name: redis_consumer_group
            # .. custom_attribute_description: The consumer group in redis stream.
            set_custom_attribute('redis_consumer_group', run_context['consumer_group'])

            # .. custom_attribute_name: redis_consumer_name
            # .. custom_attribute_description: The consumer name in redis stream group.
            set_custom_attribute('redis_consumer_name', run_context['consumer_name'])

            if message:
                # .. custom_attribute_name: redis_msg_id
                # .. custom_attribute_description: The message id from redis stream.
                set_custom_attribute('redis_msg_id', message.msg_id)
                # .. custom_attribute_name: id
                # .. custom_attribute_description: The message id which can be matched to the logs.
                set_custom_attribute('id', str(message.event_metadata.id))
                # .. custom_attribute_name: redis_event_type
                # .. custom_attribute_description: The event type of the message.
                set_custom_attribute('event_type', message.event_metadata.event_type)

            if error:
                # .. custom_attribute_name: redis_error_fatal
                # .. custom_attribute_description: Boolean describing if the error is fatal.
                set_custom_attribute('redis_error_fatal', fatal)

        except Exception as e:  # pragma: no cover  pylint: disable=broad-except
            # Use this to fix any bugs in what should be benign monitoring code
            set_custom_attribute('redis_monitoring_error', repr(e))

    def _is_fatal_redis_error(self, error: Optional[Exception]) -> bool:
        """
        Returns True if error is a RedisConnectionError, False otherwise.

        The redis.ConnectionError can be considered fatal as it is the base exception for errors from which consumer
        might not be able to recover like AuthenticationError, AuthorizationError, MaxConnectionsError etc.
        https://redis.readthedocs.io/en/stable/exceptions.html#redis.exceptions.ConnectionError

        Arguments:
            error: An exception instance, or None if no error.
        """
        if error and isinstance(error, RedisConnectionError):
            return True

        return False
