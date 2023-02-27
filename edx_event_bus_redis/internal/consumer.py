"""
Core consumer and event-loop code.
"""
import logging
import time
import warnings
from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection
from edx_django_utils.monitoring import record_exception, set_custom_attribute
from edx_toggles.toggles import SettingToggle
from openedx_events.tooling import OpenEdxPublicSignal
from redis.exceptions import RedisError

from .config import get_full_topic, load_common_settings
from .utils import (
    AUDIT_LOGGING_ENABLED,
    HEADER_EVENT_TYPE,
    HEADER_ID,
    get_message_header_values,
    get_metadata_from_headers,
    last_message_header_value,
)

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
CONSUMER_POLL_TIMEOUT = getattr(settings, 'EVENT_BUS_REDIS_CONSUMER_POLL_TIMEOUT', 1.0)

# .. setting_name: EVENT_BUS_REDIS_CONSUMER_POLL_FAILURE_SLEEP
# .. setting_default: 1.0
# .. setting_description: When the consumer fails to retrieve an event from the broker,
#   it will sleep for this many seconds before trying again. This is to prevent fast error-loops
#   if the broker is down or the consumer is misconfigured. It *may* also sleep for errors that
#   involve receiving an unreadable event, but this could change in the future to be more
#   specific to "no event received from broker".
POLL_FAILURE_SLEEP = getattr(settings, 'EVENT_BUS_REDIS_CONSUMER_POLL_FAILURE_SLEEP', 1.0)


class UnusableMessageError(Exception):
    """
    Indicates that a message was successfully received but could not be processed.

    This could be invalid headers, an unknown signal, or other issue specific to
    the contents of the message.
    """


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


class RedisEventConsumer:
    """
    Construct consumer for the given topic, group, and signal. The consumer can then
    emit events from the event bus using the configured signal.

    Note that the topic should be specified here *without* the optional environment prefix.

    Can also consume messages indefinitely off the queue.
    """

    def __init__(self, topic, group_id, signal):
        self.topic = topic
        self.group_id = group_id
        self.signal = signal
        self.consumer = self._create_consumer()
        self._shut_down_loop = False

    # return type (Optional[DeserializingConsumer]) removed from signature to avoid error on import
    def _create_consumer(self):
        """
        Create a DeserializingConsumer for events of the given signal instance.

        Returns
            DeserializingConsumer if it is.
        """

        consumer_config = load_common_settings()

        # We do not deserialize the key because we don't need it for anything yet.
        # Also see https://github.com/openedx/openedx-events/issues/86 for some challenges on determining key schema.
        consumer_config.update({
            'group.id': self.group_id,
            'value.deserializer': None,
            # Turn off auto commit. Auto commit will commit offsets for the entire batch of messages received,
            # potentially resulting in data loss if some of those messages are not fully processed. See
            # https://newrelic.com/blog/best-practices/redis-consumer-config-auto-commit-data-loss
            'enable.auto.commit': False,
        })

        # create redis client and consumer.
        return consumer_config

    def _shut_down(self):
        """
        Test utility for shutting down the consumer loop.
        """
        self._shut_down_loop = True

    def _consume_indefinitely(self):
        """
        Consume events from a topic in an infinite loop.
        """

        # This is already checked at the Command level, but it's possible this loop
        # could get called some other way, so check it here too.
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

        try:
            full_topic = get_full_topic(self.topic)
            run_context = {
                'full_topic': full_topic,
                'consumer_group': self.group_id,
                'expected_signal': self.signal,
            }
            # TODO: Consume events
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

                msg = None
                try:
                    # poll for msg
                    msg = None
                    if msg is not None:
                        # Before processing, make sure our db connection is still active
                        _reconnect_to_db_if_needed()

                        self.emit_signals_from_message(msg)
                        consecutive_errors = 0

                    self._add_message_monitoring(run_context=run_context, message=msg)
                except Exception as e:  # pylint: disable=broad-except
                    consecutive_errors += 1
                    self.record_event_consuming_error(run_context, e, msg)
                    # Kill the infinite loop if the error is fatal for the consumer
                    _, redis_error = self._get_redis_message_and_error(message=msg, error=e)
                    # https://redis.readthedocs.io/en/stable/exceptions.html#redis.exceptions.TryAgainError
                    if redis_error:  # and redis_error.fatal()
                        raise e
                    # Prevent fast error-looping when no event received from broker. Because
                    # DeserializingConsumer raises rather than returning a Message when it has an
                    # error() value, this may be triggered even when a Message *was* returned,
                    # slowing down the queue. This is probably close enough, though.
                    if msg is None:
                        time.sleep(POLL_FAILURE_SLEEP)
                # if msg:
                    # theoretically we could just call consumer.commit() without passing the specific message
                    # to commit all this consumer's current offset across all partitions since we only process one
                    # message at a time, but limit it to just the offset/partition of the specified message
                    # to be super safe
                    # self.consumer.commit(message=msg)
        finally:
            # TODO: close consumber
            pass

    def consume_indefinitely(self, offset_timestamp=None):
        """
        Consume events from a topic in an infinite loop.

        Arguments:
            offset_timestamp (datetime): Optional and deprecated; if supplied, calls
                ``reset_offsets_and_sleep_indefinitely`` instead. Relying code should
                switch to calling that method directly.
        """
        # TODO: Once this deprecated argument can be removed, just
        # remove this delegation method entirely and rename
        # `_consume_indefinitely` to no longer have the `_` prefix.
        if offset_timestamp is None:
            self._consume_indefinitely()
        else:
            warnings.warn(
                "Calling consume_indefinitely with offset_timestamp is deprecated; "
                "please call reset_offsets_and_sleep_indefinitely directly instead."
            )
            # self.reset_offsets_and_sleep_indefinitely(offset_timestamp)

    def emit_signals_from_message(self, msg):
        """
        Determine the correct signal and send the event from the message.

        Arguments:
            msg (Message): Consumed message.
        """
        self._log_message_received(msg)

        # DeserializingConsumer.poll() always returns either a valid message
        # or None, and raises an exception in all other cases. This means
        # we don't need to check msg.error() ourselves. But... check it here
        # anyway for robustness against code changes.
        if msg.error() is not None:
            raise UnusableMessageError(
                f"Polled message had error object (shouldn't happen): {msg.error()!r}"
            )

        headers = msg.headers() or []  # treat None as []

        event_types = get_message_header_values(headers, HEADER_EVENT_TYPE)
        if len(event_types) == 0:
            raise UnusableMessageError(
                "Missing type header on message, cannot determine signal"
            )
        if len(event_types) > 1:
            raise UnusableMessageError(
                "Multiple type headers found on message, cannot determine signal"
            )
        event_type = event_types[0]

        if event_type != self.signal.event_type:
            raise UnusableMessageError(
                f"Signal types do not match. Expected {self.signal.event_type}. "
                f"Received message of type {event_type}."
            )
        try:
            event_metadata = get_metadata_from_headers(headers)
        except Exception as e:
            raise UnusableMessageError(f"Error determining metadata from message headers: {e}") from e
        send_results = self.signal.send_event_with_custom_metadata(event_metadata, **msg.value())
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

    def _log_message_received(self, msg):
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
            message_id = last_message_header_value(msg.headers(), HEADER_ID)

            timestamp_ms = msg.timestamp()
            timestamp_info = str(timestamp_ms)

            # See ADR for details on why certain fields were included or omitted.
            logger.info(
                f'Message received from Redis: topic={msg.topic()}, '
                f'message_id={message_id}, key={msg.key()}, '
                f'event_timestamp_ms={timestamp_info}'
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
        maybe_redis_message, _ = self._get_redis_message_and_error(message=maybe_message, error=error)
        if maybe_redis_message is None:
            event_msg = "no event available"
        else:
            event_details = {
                'headers': maybe_redis_message.headers(),
                'key': maybe_redis_message.key(),
                'value': maybe_redis_message.value(),
            }
            event_msg = f"event details: {event_details!r}"

        try:
            # This is gross, but our record_exception wrapper doesn't take args at the moment,
            # and will only read the exception from stack context.
            raise Exception(error)  # pylint: disable=broad-exception-raised
        except BaseException:
            self._add_message_monitoring(run_context=run_context, message=maybe_redis_message, error=error)
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
            redis_message, redis_error = self._get_redis_message_and_error(message=message, error=error)

            # .. custom_attribute_name: redis_topic
            # .. custom_attribute_description: The full topic of the message or error.
            set_custom_attribute('redis_topic', run_context['full_topic'])

            if redis_message:
                # .. custom_attribute_name: redis_partition
                # .. custom_attribute_description: The partition of the message.
                # set_custom_attribute('redis_partition', redis_message.partition())
                headers = redis_message.headers() or []  # treat None as []
                message_ids = get_message_header_values(headers, HEADER_ID)
                if len(message_ids) > 0:
                    # .. custom_attribute_name: redis_message_id
                    # .. custom_attribute_description: The message id which can be matched to the logs. Note that the
                    #   header in the logs will use 'ce_id'.
                    set_custom_attribute('redis_message_id', ",".join(message_ids))
                event_types = get_message_header_values(headers, HEADER_EVENT_TYPE)
                if len(event_types) > 0:
                    # .. custom_attribute_name: redis_event_type
                    # .. custom_attribute_description: The event type of the message. Note that the header in the logs
                    #   will use 'ce_type'.
                    set_custom_attribute('redis_event_type', ",".join(event_types))

            if redis_error:
                # .. custom_attribute_name: redis_error_fatal
                # .. custom_attribute_description: Boolean describing if the error is fatal.
                # TODO: https://redis.readthedocs.io/en/stable/exceptions.html#redis.exceptions.TryAgainError
                # set_custom_attribute('redis_error_fatal', redis_error.fatal())
                # .. custom_attribute_name: redis_error_retriable
                # .. custom_attribute_description: Boolean describing if the error is retriable.
                # https://redis.readthedocs.io/en/stable/exceptions.html#redis.exceptions.TryAgainError
                # set_custom_attribute('redis_error_retriable', redis_error.retriable())
                pass

        except Exception as e:  # pragma: no cover  pylint: disable=broad-except
            # Use this to fix any bugs in what should be benign monitoring code
            set_custom_attribute('redis_monitoring_error', repr(e))

    def _get_redis_message_and_error(self, message, error):
        """
        Returns tuple of (redis_message, redis_error), if they can be found.

        Notes:
            * If the message was sent as a parameter, it will be returned.
            * If the message was not sent, and a RedisException was sent, the
                message will be pulled from the exception if it exists.
            * A RedisError will be returned if it is either passed directly,
                or if it was wrapped by a RedisException.

        Arguments:
            message: None if event could not be fetched or decoded, or a Message if one
              was successfully deserialized but could not be processed for some reason
            error: An exception instance, or None if no error.
        """
        if not error or isinstance(error, RedisError):
            return message, error

        redis_error = getattr(error, 'redis_error', None)
        # RedisException uses args[0] to wrap the RedisError
        if not redis_error and len(error.args) > 0 and isinstance(error.args[0], RedisError):
            redis_error = error.args[0]

        redis_message = getattr(error, 'redis_message', None)
        if message and redis_message and redis_message != message:  # pragma: no cover
            # If this unexpected error ever occurs, we can invest in a better error message
            #   with a test, that includes event header details.
            logger.error("Error consuming event from Redis: (UNEXPECTED) The event message did not match"
                         " the message packaged with the error."
                         f" -- event message={message!r}, error event message={redis_message!r}.")
        # give priority to the passed message, although in theory, it should be the same message if not None
        redis_message = message or redis_message

        return redis_message, redis_error


class ConsumeEventsCommand(BaseCommand):
    """
    Management command for Redis consumer workers in the event bus.
    """
    help = """
    Consume messages of specified signal type from a Redis topic and send their data to that signal.

    Example::

        python3 manage.py cms consume_events -t user-login -g user-activity-service \
            -s org.openedx.learning.auth.session.login.completed.v1
    """

    def add_arguments(self, parser):

        parser.add_argument(
            '-t', '--topic',
            nargs=1,
            required=True,
            help='Topic to consume (without environment prefix)'
        )

        parser.add_argument(
            '-g', '--group_id',
            nargs=1,
            required=True,
            help='Consumer group id'
        )
        parser.add_argument(
            '-s', '--signal',
            nargs=1,
            required=True,
            help='Type of signal to emit from consumed messages.'
        )
        parser.add_argument(
            '-o', '--offset_time',
            nargs=1,
            required=False,
            default=None,
            help='The timestamp (in ISO format) that we would like to reset the consumers to. '
                 'If this is used, the consumers will only reset the offsets of the topic '
                 'but will not actually consume and process any messages.'
        )

    def handle(self, *args, **options):
        if not REDIS_CONSUMERS_ENABLED.is_enabled():
            logger.error("Redis consumers not enabled, exiting.")
            return

        try:
            signal = OpenEdxPublicSignal.get_signal_by_type(options['signal'][0])
            if options['offset_time'] and options['offset_time'][0] is not None:
                try:
                    offset_timestamp = datetime.fromisoformat(options['offset_time'][0])
                except ValueError:
                    logger.exception('Could not parse the offset timestamp.')
                    raise
            else:
                offset_timestamp = None

            event_consumer = RedisEventConsumer(
                topic=options['topic'][0],
                group_id=options['group_id'][0],
                signal=signal,
            )
            if offset_timestamp is None:
                event_consumer.consume_indefinitely()
            else:
                pass
                # event_consumer.reset_offsets_and_sleep_indefinitely(offset_timestamp=offset_timestamp)
        except Exception:  # pylint: disable=broad-except
            logger.exception("Error consuming Redis events")
