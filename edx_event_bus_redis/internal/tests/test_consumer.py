"""
Tests for event_consumer module.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, call, patch
from uuid import uuid1

import ddt
import pytest
from django.core.management import call_command
from django.test import TestCase
from django.test.utils import override_settings
from openedx_events.learning.data import UserData, UserPersonalData
from openedx_events.learning.signals import SESSION_LOGIN_COMPLETED
from openedx_events.tooling import EventsMetadata
from redis import ResponseError
from redis.exceptions import ConnectionError as RedisConnectionError

from edx_event_bus_redis.internal.consumer import ReceiverError, RedisEventConsumer
from edx_event_bus_redis.internal.message import RedisMessage
from edx_event_bus_redis.internal.tests.test_utils import side_effects
from edx_event_bus_redis.management.commands.consume_events import Command


def fake_receiver_returns_quietly(**kwargs):
    return


def fake_receiver_raises_error(**kwargs):
    raise Exception("receiver whoops")  # pylint: disable=broad-exception-raised


@override_settings(
    EVENT_BUS_REDIS_CONNECTION_URL='redis://:password@localhost:6379/',
)
@ddt.ddt
class TestConsumer(TestCase):
    """
    Tests for signal-sending.
    """

    def setUp(self):
        super().setUp()
        self.normal_event_data = {
            'user': UserData(
                id=123,
                is_active=True,
                pii=UserPersonalData(
                    username='foobob',
                    email='bob@foo.example',
                    name='Bob Foo'
                )
            )
        }
        self.event_data_bytes = b'\xf6\x01\x01\x0cfoobob\x1ebob@foo.example\x0eBob Foo'
        self.message_id = uuid1()
        self.message_id_bytes = str(self.message_id).encode('utf-8')

        self.signal_type_bytes = b'org.openedx.learning.auth.session.login.completed.v1'
        self.signal_type = self.signal_type_bytes.decode('utf-8')
        self.normal_message = RedisMessage(
            event_data=self.event_data_bytes,
            topic='local-some-topic',
            event_metadata=EventsMetadata(
                id=self.message_id,
                event_type=self.signal_type,
                time=datetime.fromtimestamp(1678773365.314331, timezone.utc)
            ),
            msg_id=b'1',
        )
        self.mock_receiver = Mock()
        self.signal = SESSION_LOGIN_COMPLETED
        self.signal.connect(fake_receiver_returns_quietly)
        self.signal.connect(fake_receiver_raises_error)
        self.signal.connect(self.mock_receiver)
        with patch('edx_event_bus_redis.internal.consumer.Database.from_url', autospec=True):
            self.event_consumer = RedisEventConsumer(
                'local-some-topic',
                'test_group_id',
                self.signal,
                check_backlog=True
            )

    def tearDown(self):
        self.signal.disconnect(fake_receiver_returns_quietly)
        self.signal.disconnect(fake_receiver_raises_error)
        self.signal.disconnect(self.mock_receiver)

    def assert_signal_sent_with(self, signal, data):
        """
        Check that a signal-send came in as expected to the mock receiver.
        """
        self.mock_receiver.assert_called_once()
        call_kwargs = self.mock_receiver.call_args[1]

        # Standard signal stuff
        assert call_kwargs['signal'] == signal
        assert call_kwargs['sender'] is None

        # There should just be one key-value pair in the data for all OpenEdxPublicEvents
        ((event_top_key, event_contents),) = data.items()
        assert call_kwargs[event_top_key] == event_contents

        # There should also be a metadata key -- spot-check it
        metadata = call_kwargs['metadata']
        assert metadata.event_type == signal.event_type
        assert metadata.sourcehost is not None

    @patch('edx_event_bus_redis.internal.consumer.logger', autospec=True)
    @ddt.data(('some-id', False), (None, True), ('some-id', True), (None, False))
    @ddt.unpack
    def test_consumer_creation(self, last_read_msg_id, check_backlog, mock_logger):
        """
        Check consumer msg id value based on initialization values.
        """
        mock_consumer = MagicMock()
        mock_consumer.create.side_effect = ResponseError()
        mock_db = MagicMock(**{'consumer_group.return_value': mock_consumer})
        with patch('edx_event_bus_redis.internal.consumer.RedisEventConsumer._create_db', return_value=mock_db):
            RedisEventConsumer(
                'local-some-topic',
                'test_group_id',
                self.signal,
                last_read_msg_id=last_read_msg_id,
                check_backlog=check_backlog,
            )
            mock_logger.warning.assert_called_once()
            assert "Stream already created" in mock_logger.warning.call_args.args[0]
            mock_consumer.create.assert_called_once()
            if last_read_msg_id:
                mock_consumer.set_id.assert_called_once_with(last_read_msg_id)
            elif not check_backlog:
                mock_consumer.set_id.assert_called_once_with('$')
            else:
                mock_consumer.set_id.assert_not_called()

    @patch('edx_event_bus_redis.internal.consumer.set_custom_attribute', autospec=True)
    @patch('edx_event_bus_redis.internal.consumer.logger', autospec=True)
    @patch('edx_event_bus_redis.internal.consumer.time.sleep', autospec=True)
    @ddt.data(
        None,  # no pending msg
        [{'message_id': b'some-id'}],  # one pending msg
    )
    def test_consume_loop(self, pending_return_value, mock_sleep, mock_logger, mock_set_custom_attribute):
        """
        Check the basic loop lifecycle.
        """
        def raise_exception():
            raise ValueError("something broke")

        # How the emit_signals_from_message() mock will behave on each successive call.
        mock_emit_side_effects = [
            lambda: None,  # accept and ignore a message
            raise_exception,
            lambda: None,  # accept another message (exception didn't break loop)

            # Final "call" just serves to stop the loop
            self.event_consumer._shut_down  # pylint: disable=protected-access
        ]

        with patch.object(
                self.event_consumer, 'emit_signals_from_message',
                side_effect=side_effects(mock_emit_side_effects),
        ) as mock_emit:
            mock_consumer = MagicMock(
                **{'read.return_value': [(b'1', self.normal_message.to_binary_dict())], 'pending.return_value':
                    pending_return_value, '__getitem__.return_value': (b'1', self.normal_message.to_binary_dict())},
                autospec=True
            )
            self.event_consumer.db = Mock()
            self.event_consumer.consumer = mock_consumer
            self.event_consumer.consume_indefinitely()

        # Check that each of the mocked out methods got called as expected.
        if pending_return_value:
            mock_consumer.pending.assert_called_with(count=1, consumer='test_group_id.c1')
        mock_consumer.ack.assert_called_with(b'1')
        # Check that emit was called the expected number of times
        assert mock_emit.call_args_list == [call(self.normal_message)] * len(mock_emit_side_effects)

        # Check that there was one error log message and that it contained all the right parts,
        # in some order.
        mock_logger.exception.assert_called_once()
        (exc_log_msg,) = mock_logger.exception.call_args.args
        assert "Error consuming event from Redis: ValueError('something broke') in context" in exc_log_msg
        assert "full_topic='local-some-topic'" in exc_log_msg
        assert "consumer_group='test_group_id'" in exc_log_msg
        assert ("expected_signal=<OpenEdxPublicSignal: "
                "org.openedx.learning.auth.session.login.completed.v1>") in exc_log_msg
        assert "-- event details: " in exc_log_msg
        assert str(self.normal_message) in exc_log_msg

        mock_set_custom_attribute.assert_has_calls(
            [
                call('redis_consumer_group', 'test_group_id'),
                call('redis_consumer_name', 'test_group_id.c1'),
                call('redis_stream', 'local-some-topic'),
                call('redis_msg_id', b'1'),
                call('id', str(self.message_id)),
                call('event_type', 'org.openedx.learning.auth.session.login.completed.v1'),
            ] * len(mock_emit_side_effects),
            any_order=True,
        )

        # Check that each message got committed (including the errored ones)
        assert len(mock_consumer.ack.call_args_list) == len(mock_emit_side_effects)

        mock_sleep.assert_not_called()
        self.event_consumer.db.close.assert_called_once_with()  # since shutdown was requested, not because of exception

    @override_settings(
        EVENT_BUS_REDIS_CONSUMER_CONSECUTIVE_ERRORS_LIMIT=4,
    )
    def test_consecutive_error_limit(self):
        """Confirm that consecutive errors can break out of loop."""
        def raise_exception():
            raise ValueError("something broke")

        exception_count = 4

        with patch.object(
                self.event_consumer, 'emit_signals_from_message',
                side_effect=side_effects([raise_exception] * exception_count),
        ) as mock_emit:
            mock_consumer = Mock(
                **{'read.return_value': (b'1', self.normal_message.to_binary_dict()), 'pending.return_value': None},
                autospec=True
            )
            self.event_consumer.consumer = mock_consumer
            with pytest.raises(Exception) as exc_info:
                self.event_consumer.consume_indefinitely()

        assert mock_emit.call_args_list == [call(self.normal_message)] * exception_count
        assert exc_info.value.args == ("Too many consecutive errors, exiting (4 in a row)",)

    @patch('edx_event_bus_redis.internal.consumer.connection')
    @ddt.data(
        (False, False, False),  # no connection, don't reconnect
        (True, False, True),  # connection unusable, reconnect expected
        (False, True, False),  # usable connection, no need to reconnect
    )
    @ddt.unpack
    def test_connection_reset(self, has_connection, is_usable, reconnect_expected, mock_connection):
        """Confirm we reconnect to the database as required"""
        if not has_connection:
            mock_connection.connection = None
        mock_connection.is_usable.return_value = is_usable

        with patch.object(
                self.event_consumer, 'emit_signals_from_message',
                side_effect=side_effects([self.event_consumer._shut_down])  # pylint: disable=protected-access
        ):
            mock_consumer = Mock(
                **{'read.return_value': (b'1', self.normal_message.to_binary_dict()), 'pending.return_value': None},
                autospec=True
            )
            self.event_consumer.consumer = mock_consumer
            self.event_consumer.consume_indefinitely()

        if reconnect_expected:
            mock_connection.connect.assert_called_once()
        else:
            mock_connection.connect.assert_not_called()

    @override_settings(
        EVENT_BUS_REDIS_CONSUMER_CONSECUTIVE_ERRORS_LIMIT=4,
    )
    def test_non_consecutive_errors(self):
        """Confirm that non-consecutive errors may not break out of loop."""
        def raise_exception():
            raise ValueError("something broke")

        mock_emit_side_effects = [
            raise_exception, raise_exception,
            lambda: None,  # an iteration that doesn't raise an exception
            raise_exception, raise_exception,
            # Stop the loop, since the non-consecutive exceptions won't do it
            self.event_consumer._shut_down,  # pylint: disable=protected-access
        ]

        with patch.object(
                self.event_consumer, 'emit_signals_from_message',
                side_effect=side_effects(mock_emit_side_effects)
        ) as mock_emit:
            mock_consumer = Mock(
                **{'read.return_value': (b'1', self.normal_message.to_binary_dict()), 'pending.return_value': None},
                autospec=True
            )
            self.event_consumer.consumer = mock_consumer
            self.event_consumer.consume_indefinitely()  # exits normally

        assert mock_emit.call_args_list == [call(self.normal_message)] * len(mock_emit_side_effects)

    @patch('edx_event_bus_redis.internal.consumer.set_custom_attribute', autospec=True)
    @patch('edx_event_bus_redis.internal.consumer.logger', autospec=True)
    @patch('edx_event_bus_redis.internal.consumer.time.sleep', autospec=True)
    @override_settings(
        EVENT_BUS_REDIS_CONSUMER_POLL_FAILURE_SLEEP=1
    )
    @ddt.data(
        (ValueError("something random"), False),
        (RedisConnectionError(), True),
    )
    @ddt.unpack
    def test_record_error_for_fatal_and_non_fatal_error(
        self, exception, is_fatal, mock_sleep, mock_logger, mock_set_custom_attribute,
    ):
        """
        Covers reporting of an error in the consumer loop for various types of errors.
        """
        def read_side_effect(*args, **kwargs):
            # Only run one iteration
            self.event_consumer._shut_down()  # pylint: disable=protected-access
            raise exception

        mock_consumer = Mock(**{'read.side_effect': read_side_effect, 'pending.return_value': None}, autospec=True)
        self.event_consumer.consumer = mock_consumer
        if is_fatal:
            with pytest.raises(RedisConnectionError) as exc_info:
                self.event_consumer.consume_indefinitely()
            assert exc_info.value == exception
        else:
            self.event_consumer.consume_indefinitely()

        # Check that there was one exception log message and that it contained all the right parts,
        # in some order.
        mock_logger.error.assert_not_called()
        mock_logger.exception.assert_called_once()
        (exc_log_msg,) = mock_logger.exception.call_args.args
        assert f"Error consuming event from Redis: {repr(exception)} in context" in exc_log_msg
        assert "full_topic='local-some-topic'" in exc_log_msg
        assert "consumer_group='test_group_id'" in exc_log_msg
        assert ("expected_signal=<OpenEdxPublicSignal: "
                "org.openedx.learning.auth.session.login.completed.v1>") in exc_log_msg
        assert "-- no event available" in exc_log_msg

        expected_custom_attribute_calls = [
            call("redis_stream", "local-some-topic"),
        ]
        expected_custom_attribute_calls += [
            call('redis_error_fatal', is_fatal),
        ]
        mock_set_custom_attribute.assert_has_calls(expected_custom_attribute_calls, any_order=True)

        # For non-fatal errors, "no-event" sleep branch was triggered
        if not is_fatal:
            mock_sleep.assert_called_once_with(1)

        mock_consumer.commit.assert_not_called()

    @override_settings(EVENT_BUS_REDIS_CONSUMERS_ENABLED=False)
    @patch('edx_event_bus_redis.internal.consumer.logger', autospec=True)
    def test_consume_loop_disabled(self, mock_logger):
        self.event_consumer.consume_indefinitely()  # returns at all
        mock_logger.error.assert_called_once_with("Redis consumers not enabled, exiting.")

    @override_settings(EVENT_BUS_REDIS_CONNECTION_URL=None)
    def test_missing_url_setting(self):
        with pytest.raises(ValueError, match="Missing"):
            RedisEventConsumer('local-some-topic', 'test_group_id', self.signal)

    @override_settings(
        EVENT_BUS_TOPIC_PREFIX='local',
    )
    @patch('edx_event_bus_redis.internal.consumer.set_custom_attribute', autospec=True)
    @patch('edx_event_bus_redis.internal.consumer.logger', autospec=True)
    @ddt.data(True, False)
    def test_emit_success(self, audit_logging, mock_logger, mock_set_attribute):
        self.signal.disconnect(fake_receiver_raises_error)  # just successes for this one!

        with override_settings(EVENT_BUS_REDIS_AUDIT_LOGGING_ENABLED=audit_logging):
            self.event_consumer.emit_signals_from_message(self.normal_message)
        self.assert_signal_sent_with(self.signal, self.normal_event_data)
        # Specifically, not called with 'redis_logging_error'
        mock_set_attribute.assert_not_called()
        if audit_logging:
            mock_logger.info.assert_has_calls([
                call(
                    "Message received from Redis: topic=local-some-topic, "
                    f"message_id={self.message_id}, "
                    "redis_msg_id=b'1', event_timestamp_ms=1678773365.314331"
                ),
                call('Message from Redis processed successfully'),
            ])
        else:
            mock_logger.info.assert_not_called()

    @patch('django.dispatch.dispatcher.logger', autospec=True)
    def test_emit(self, mock_logger):
        with pytest.raises(ReceiverError) as exc_info:
            self.event_consumer.emit_signals_from_message(self.normal_message)
        self.assert_signal_sent_with(self.signal, self.normal_event_data)
        assert exc_info.value.args == (
            "1 receiver(s) out of 3 produced errors (stack trace elsewhere in logs) "
            "when handling signal <OpenEdxPublicSignal: "
            "org.openedx.learning.auth.session.login.completed.v1>: "
            "edx_event_bus_redis.internal.tests.test_consumer.fake_receiver_raises_error="
            "Exception('receiver whoops')",
        )

        # Check that django dispatch is logging the stack trace. Really, we only care that
        # *something* does it, though. This test just ensures that our "(stack trace
        # elsewhere in logs)" isn't a lie.
        (receiver_error,) = exc_info.value.causes
        mock_logger.error.assert_called_once_with(
            "Error calling %s in Signal.send_robust() (%s)",
            'fake_receiver_raises_error',
            receiver_error,
            exc_info=receiver_error,
        )

    def test_malformed_receiver_errors(self):
        """
        Ensure that even a really messed-up receiver is still reported correctly.
        """
        with pytest.raises(ReceiverError) as exc_info:
            self.event_consumer._check_receiver_results([  # pylint: disable=protected-access
                (lambda x:x, ValueError("for lambda")),
                # This would actually raise an error inside send_robust(), but it will serve well enough for testing...
                ("not even a function", ValueError("just plain bad")),
            ])
        assert exc_info.value.args == (
            "2 receiver(s) out of 2 produced errors (stack trace elsewhere in logs) "
            "when handling signal <OpenEdxPublicSignal: "
            "org.openedx.learning.auth.session.login.completed.v1>: "

            "edx_event_bus_redis.internal.tests.test_consumer.TestConsumer."
            "test_malformed_receiver_errors.<locals>.<lambda>=ValueError('for lambda'), "

            "not even a function=ValueError('just plain bad')",
        )


class TestCommand(TestCase):
    """
    Tests for the consume_events management command
    """

    @override_settings(EVENT_BUS_REDIS_CONSUMERS_ENABLED=False)
    @patch('edx_event_bus_redis.internal.consumer.logger', autospec=True)
    @patch('edx_event_bus_redis.internal.consumer.RedisEventConsumer._create_consumer')
    def test_redis_consumers_disabled(self, mock_create_consumer, mock_logger):
        call_command(Command(), topic='test', group_id='test', signal='')
        assert not mock_create_consumer.called
        mock_logger.error.assert_called_once_with("Redis consumers not enabled, exiting.")

    @patch('edx_event_bus_redis.internal.consumer.OpenEdxPublicSignal.get_signal_by_type', return_value="test-signal")
    @patch('edx_event_bus_redis.internal.consumer.RedisEventConsumer')
    def test_redis_consumers_normal(self, mock_consumer, _):
        call_command(
            Command(),
            topic=['test'],
            group_id=['test_group'],
            signal=['openedx'],
        )
        mock_consumer.assert_called_once_with(
            topic='test',
            group_id='test_group',
            signal='test-signal',
            consumer_name=None,
            last_read_msg_id=None,
            check_backlog=False,
        )

    @patch('edx_event_bus_redis.internal.consumer.OpenEdxPublicSignal.get_signal_by_type', return_value="test-signal")
    @patch('edx_event_bus_redis.internal.consumer.RedisEventConsumer')
    def test_redis_consumers_with_consumer_name(self, mock_consumer, _):
        call_command(
            Command(),
            topic=['test'],
            group_id=['test_group'],
            signal=['openedx'],
            consumer_name='c1',
        )
        mock_consumer.assert_called_once_with(
            topic='test',
            group_id='test_group',
            signal='test-signal',
            consumer_name='c1',
            last_read_msg_id=None,
            check_backlog=False,
        )

    @patch('edx_event_bus_redis.internal.consumer.OpenEdxPublicSignal.get_signal_by_type', return_value="test-signal")
    @patch('edx_event_bus_redis.internal.consumer.RedisEventConsumer._create_db', autospec=True)
    @patch(
        'edx_event_bus_redis.internal.consumer.RedisEventConsumer.consume_indefinitely',
        side_effect=ValueError("some error")
    )
    @patch('edx_event_bus_redis.internal.consumer.logger', autospec=True)
    def test_consume_command_exception(self, mock_logger, _mock_consume, _mock_create_db, _):
        call_command(
            Command(),
            topic=['test'],
            group_id=['test_group'],
            signal=['openedx'],
            consumer_name='c1',
        )
        mock_logger.exception.assert_called_once()
        (exc_log_msg,) = mock_logger.exception.call_args.args
        assert "Error consuming Redis events" in exc_log_msg
