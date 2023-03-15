"""
Tests for event_consumer module.
"""

from datetime import datetime, timezone
from unittest.mock import Mock, call, patch
from uuid import uuid1

import ddt
import pytest
from django.core.management import call_command
from django.test import TestCase
from django.test.utils import override_settings
from openedx_events.learning.data import UserData, UserPersonalData
from openedx_events.learning.signals import SESSION_LOGIN_COMPLETED
from openedx_events.tooling import EventsMetadata

from edx_event_bus_redis.internal.consumer import ReceiverError, RedisEventConsumer
from edx_event_bus_redis.internal.message import RedisMessage
from edx_event_bus_redis.management.commands.consume_events import Command


def fake_receiver_returns_quietly(**kwargs):
    return


def fake_receiver_raises_error(**kwargs):
    raise Exception("receiver whoops")  # pylint: disable=broad-exception-raised


@override_settings(
    EVENT_BUS_REDIS_CONNECTION_URL='redis://:password@locahost:6379/0',
)
@ddt.ddt
class TestEmitSignals(TestCase):
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
            )
        )
        self.mock_receiver = Mock()
        self.signal = SESSION_LOGIN_COMPLETED
        self.signal.connect(fake_receiver_returns_quietly)
        self.signal.connect(fake_receiver_raises_error)
        self.signal.connect(self.mock_receiver)
        self.event_consumer = RedisEventConsumer('some-topic', 'test_group_id', self.signal)

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

    @override_settings(EVENT_BUS_REDIS_CONSUMERS_ENABLED=False)
    @patch('edx_event_bus_redis.internal.consumer.logger', autospec=True)
    def test_consume_loop_disabled(self, mock_logger):
        self.event_consumer.consume_indefinitely()  # returns at all
        mock_logger.error.assert_called_once_with("Redis consumers not enabled, exiting.")

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
                    "redis_msg_id=None, event_timestamp_ms=1678773365.314331"
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
                (lambda x:x, Exception("for lambda")),
                # This would actually raise an error inside send_robust(), but it will serve well enough for testing...
                ("not even a function", Exception("just plain bad")),
            ])
        assert exc_info.value.args == (
            "2 receiver(s) out of 2 produced errors (stack trace elsewhere in logs) "
            "when handling signal <OpenEdxPublicSignal: "
            "org.openedx.learning.auth.session.login.completed.v1>: "

            "edx_event_bus_redis.internal.tests.test_consumer.TestEmitSignals."
            "test_malformed_receiver_errors.<locals>.<lambda>=Exception('for lambda'), "

            "not even a function=Exception('just plain bad')",
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
            consumer_name=None
        )

    @patch('edx_event_bus_redis.internal.consumer.OpenEdxPublicSignal.get_signal_by_type', return_value="test-signal")
    @patch('edx_event_bus_redis.internal.consumer.RedisEventConsumer')
    def test_redis_consumers_with_consumer_name(self, mock_consumer, _):
        call_command(
            Command(),
            topic=['test'],
            group_id=['test_group'],
            signal=['openedx'],
            consumer_name=['c1'],
        )
        mock_consumer.assert_called_once_with(
            topic='test',
            group_id='test_group',
            signal='test-signal',
            consumer_name='c1'
        )
