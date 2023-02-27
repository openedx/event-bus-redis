"""
Tests for event_consumer module.
"""

import copy
from unittest.mock import Mock, call, patch
from uuid import uuid1

import ddt
import pytest
from django.core.management import call_command
from django.test import TestCase
from django.test.utils import override_settings
from openedx_events.learning.data import UserData, UserPersonalData
from openedx_events.learning.signals import SESSION_LOGIN_COMPLETED

from edx_event_bus_redis.internal.consumer import RedisEventConsumer, ReceiverError, UnusableMessageError
from edx_event_bus_redis.internal.tests.test_utils import FakeMessage
from edx_event_bus_redis.management.commands.consume_events import Command

def fake_receiver_returns_quietly(**kwargs):
    return


def fake_receiver_raises_error(**kwargs):
    raise Exception("receiver whoops")


@override_settings(
    EVENT_BUS_REDIS_CONNECTION_URL='redis://:password@locahost:6379/0',
)
@ddt.ddt
class TestEmitSignals(TestCase):
    """
    Tests for message parsing and signal-sending.
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
        self.message_id = uuid1()
        self.message_id_bytes = str(self.message_id).encode('utf-8')

        self.signal_type_bytes = b'org.openedx.learning.auth.session.login.completed.v1'
        self.signal_type = self.signal_type_bytes.decode('utf-8')
        self.normal_message = FakeMessage(
            topic='local-some-topic',
            headers=[
                ('id', self.message_id_bytes),
                ('type', self.signal_type_bytes),
            ],
            key=b'\x00\x00\x00\x00\x01\x0cfoobob',  # Avro, as observed in manual test
            value=self.normal_event_data,
            error=None,
            timestamp=1675114920123,
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
        # Specifically, not called with 'kafka_logging_error'
        mock_set_attribute.assert_not_called()
        if audit_logging:
            mock_logger.info.assert_has_calls([
                call(
                    "Message received from Redis: topic=local-some-topic, "
                    f"message_id={self.message_id}, "
                    "key=b'\\x00\\x00\\x00\\x00\\x01\\x0cfoobob', event_timestamp_ms=1675114920123"
                ),
                call('Message from Redis processed successfully'),
            ])
        else:
            mock_logger.info.assert_not_called()

    @override_settings(
        EVENT_BUS_TOPIC_PREFIX='local',
    )
    @patch('edx_event_bus_redis.internal.consumer.set_custom_attribute', autospec=True)
    @patch('edx_event_bus_redis.internal.consumer.logger', autospec=True)
    def test_emit_success_tolerates_missing_timestamp(self, mock_logger, mock_set_attribute):
        self.signal.disconnect(fake_receiver_raises_error)  # just successes for this one!
        self.normal_message._timestamp = None  # pylint: disable=protected-access

        self.event_consumer.emit_signals_from_message(self.normal_message)
        self.assert_signal_sent_with(self.signal, self.normal_event_data)
        # Specifically, not called with 'kafka_logging_error'
        mock_set_attribute.assert_not_called()
        mock_logger.info.assert_has_calls([
            call(
                "Message received from Redis: topic=local-some-topic, "
                f"message_id={self.message_id}, "
                "key=b'\\x00\\x00\\x00\\x00\\x01\\x0cfoobob', event_timestamp_ms=None"
            ),
            call('Message from Redis processed successfully'),
        ])

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

    def test_no_type(self):
        msg = copy.copy(self.normal_message)
        msg._headers = []  # pylint: disable=protected-access

        with pytest.raises(UnusableMessageError) as excinfo:
            self.event_consumer.emit_signals_from_message(msg)

        assert excinfo.value.args == (
            "Missing type header on message, cannot determine signal",
        )
        assert not self.mock_receiver.called

    def test_multiple_types(self):
        """
        Very unlikely case, but this gets us coverage.
        """
        msg = copy.copy(self.normal_message)
        msg._headers = [['type', b'abc'], ['type', b'def']]  # pylint: disable=protected-access

        with pytest.raises(UnusableMessageError) as excinfo:
            self.event_consumer.emit_signals_from_message(msg)

        assert excinfo.value.args == (
            "Multiple type headers found on message, cannot determine signal",
        )
        assert not self.mock_receiver.called

    def test_unexpected_signal_type_in_header(self):
        msg = copy.copy(self.normal_message)
        msg._headers = [  # pylint: disable=protected-access
            ['type', b'xxxx']
        ]
        with pytest.raises(UnusableMessageError) as excinfo:
            self.event_consumer.emit_signals_from_message(msg)

        assert excinfo.value.args == (
            "Signal types do not match. Expected org.openedx.learning.auth.session.login.completed.v1. "
            "Received message of type xxxx.",
        )
        assert not self.mock_receiver.called

    def test_bad_headers(self):
        """
        Check that if we cannot process the message headers, we raise an UnusableMessageError

        The various kinds of bad headers are more fully tested in test_utils
        """
        self.normal_message._headers = [  # pylint: disable=protected-access
            ('type', b'org.openedx.learning.auth.session.login.completed.v1'),
            ('id', b'bad_id')
        ]
        with pytest.raises(UnusableMessageError) as excinfo:
            self.event_consumer.emit_signals_from_message(self.normal_message)

        assert excinfo.value.args == (
            "Error determining metadata from message headers: badly formed hexadecimal UUID string",
        )

        assert not self.mock_receiver.called


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

    @patch('edx_event_bus_redis.internal.consumer.OpenEdxPublicSignal.get_signal_by_type')
    @patch('edx_event_bus_redis.internal.consumer.RedisEventConsumer._create_consumer')
    @patch('edx_event_bus_redis.internal.consumer.RedisEventConsumer.consume_indefinitely')
    def test_redis_consumers_normal(self, mock_consume, mock_create_consumer, _gsbt):
        call_command(
            Command(),
            topic='test',
            group_id='test',
            signal='openedx',
        )
        assert mock_create_consumer.called
        assert mock_consume.called

    @patch('edx_event_bus_redis.internal.consumer.OpenEdxPublicSignal.get_signal_by_type')
    @patch('edx_event_bus_redis.internal.consumer.RedisEventConsumer._create_consumer')
    def test_redis_consumers_with_timestamp(self, mock_reset_offsets, mock_create_consumer):
        call_command(
            Command(),
            topic='test',
            group_id='test',
            signal='openedx',
            offset_time=['2019-05-18T15:17:08.132263']
        )
        assert mock_create_consumer.called
        assert mock_reset_offsets.called

    @patch('edx_event_bus_redis.internal.consumer.logger', autospec=True)
    @patch('edx_event_bus_redis.internal.consumer.OpenEdxPublicSignal.get_signal_by_type')
    @patch('edx_event_bus_redis.internal.consumer.RedisEventConsumer._create_consumer')
    @patch('edx_event_bus_redis.internal.consumer.RedisEventConsumer.consume_indefinitely')
    def test_redis_consumers_with_bad_timestamp(self, _ci, _cc, _gsbt, mock_logger):
        call_command(Command(), topic='test', group_id='test', signal='openedx', offset_time=['notatimestamp'])
        mock_logger.exception.assert_any_call("Could not parse the offset timestamp.")
        mock_logger.exception.assert_called_with("Error consuming Redis events")
