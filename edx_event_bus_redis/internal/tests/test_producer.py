"""
Test the event producer code.
"""

import warnings
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import Mock, patch

import ddt
import openedx_events.event_bus
import openedx_events.learning.signals
from django.core.management import call_command
from django.test import override_settings
from openedx_events.data import EventsMetadata
from openedx_events.event_bus.avro.tests.test_utilities import SubTestData0, create_simple_signal
from openedx_events.learning.data import UserData, UserPersonalData

import edx_event_bus_redis.internal.producer as ep
from edx_event_bus_redis.management.commands.produce_event import Command


@ddt.ddt
class TestEventProducer(TestCase):
    """Test producer."""

    def setUp(self):
        super().setUp()
        self.signal = openedx_events.learning.signals.SESSION_LOGIN_COMPLETED
        self.event_data = {
            'user': UserData(
                id=123,
                is_active=True,
                pii=UserPersonalData(
                    username='foobob',
                    email='bob@foo.example',
                    name="Bob Foo",
                )
            )
        }

    def test_create_producer_unconfigured(self):
        """With missing essential settings, just warn and return None."""
        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter('always')
            assert ep.create_producer() is None
            assert len(caught_warnings) == 1
            assert str(caught_warnings[0].message).startswith("Cannot configure event-bus-redis: Missing setting ")

    def test_create_producer_configured(self):
        """
        Creation succeeds when all settings are present.

        Also tests basic compliance with the implementation-loader API in openedx-events.
        """
        with override_settings(
            EVENT_BUS_PRODUCER='edx_event_bus_redis.create_producer',
            EVENT_BUS_REDIS_CONNECTION_URL='redis://:password@locahost:6379/0'
        ):
            assert isinstance(openedx_events.event_bus.get_producer(), ep.RedisEventProducer)

    @patch('edx_event_bus_redis.internal.producer.logger')
    @ddt.data(True, False)
    def test_on_event_deliver(self, audit_logging, mock_logger):
        metadata = EventsMetadata(
            event_type=self.signal.event_type,
            time=datetime.now(timezone.utc),
            sourcelib=(1, 2, 3),
        )

        context = ep.ProducingContext(
            full_topic='some_topic',
            event_key='foobob',
            signal=self.signal,
            initial_topic='bare_topic',
            event_key_field='a.key.field',
            event_data=self.event_data,
            event_metadata=metadata,
        )

        # ensure on_event_deliver reports the entire calling context if there was an error
        ep.record_producing_error(Exception("problem!"), context)

        # extract the error message that was produced and check it has all relevant information (order isn't guaranteed
        # and doesn't actually matter, nor do we want to worry if other information is added later)
        (error_string,) = mock_logger.exception.call_args.args
        assert "full_topic='some_topic'" in error_string
        assert "error=problem!" in error_string

        with override_settings(EVENT_BUS_REDIS_AUDIT_LOGGING_ENABLED=audit_logging):
            context.on_event_deliver(b'random-msg-id')

        if audit_logging:
            mock_logger.info.assert_called_once_with(
                "Message delivered to Redis event bus: topic=some_topic, "
                f"message_id={metadata.id}, signal={self.signal}, redis_msg_id=b'random-msg-id'"
            )
        else:
            mock_logger.info.assert_not_called()

    # Mock out the serializers for this one so we don't have to deal with expected Avro bytes
    @patch(
        'edx_event_bus_redis.internal.producer.serialize_event_data_to_bytes', autospec=True,
        return_value=b'value-bytes-here',
    )
    def test_send_to_event_bus(self, mock_serializer):
        with override_settings(
            EVENT_BUS_PRODUCER='edx_event_bus_redis.create_producer',
            EVENT_BUS_REDIS_CONNECTION_URL='redis://locahost:6379/0',
            EVENT_BUS_TOPIC_PREFIX='prod',
            SERVICE_VARIANT='test',
        ):
            now = datetime.now(timezone.utc)
            metadata = EventsMetadata(event_type=self.signal.event_type,
                                      time=now, sourcelib=(1, 2, 3))
            producer_api = ep.create_producer()
            with patch.object(producer_api, 'client', autospec=True) as mock_client:
                stream_mock = Mock()
                mock_client.Stream.return_value = stream_mock
                producer_api.send(
                    signal=self.signal, topic='user-stuff',
                    event_key_field='user.id', event_data=self.event_data, event_metadata=metadata
                )

        mock_serializer.assert_called_once_with(self.event_data, self.signal)
        mock_client.Stream.assert_called_once_with('prod-user-stuff')
        expected_headers = {
            b'type': b'org.openedx.learning.auth.session.login.completed.v1',
            b'id': str(metadata.id).encode("utf8"),
            b'source': b'openedx/test/web',
            b'sourcehost': metadata.sourcehost.encode("utf8"),
            b'time': now.isoformat().encode("utf8"),
            b'minorversion': b'0',
            b'sourcelib': b'1.2.3',
        }

        stream_mock.add.assert_called_once_with({b'event_data': b'value-bytes-here', **expected_headers})

    @patch(
        'edx_event_bus_redis.internal.producer.serialize_event_data_to_bytes', autospec=True,
        return_value=b'value-bytes-here',
    )
    @patch('edx_event_bus_redis.internal.producer.logger')
    def test_full_event_data_present_in_redis_error(self, mock_logger, *args):
        simple_signal = create_simple_signal({'test_data': SubTestData0})
        with override_settings(
            EVENT_BUS_PRODUCER='edx_event_bus_redis.create_producer',
            EVENT_BUS_REDIS_CONNECTION_URL='redis://locahost:6379/0',
            EVENT_BUS_TOPIC_PREFIX='dev',
            SERVICE_VARIANT='test',
        ):
            metadata = EventsMetadata(event_type=simple_signal.event_type, minorversion=0)
            producer_api = ep.create_producer()
            with patch.object(producer_api, 'client', autospec=True) as mock_client:
                # imitate a failed send to Redis
                mock_client.Stream = Mock(side_effect=Exception('bad!'))
                producer_api.send(
                    signal=simple_signal,
                    topic='topic',
                    event_key_field='bad_field',
                    event_data={'test_data': SubTestData0(sub_name="name", course_id="id")},
                    event_metadata=metadata
                )

        (error_string,) = mock_logger.exception.call_args.args
        assert "event_data={'test_data': SubTestData0(sub_name='name', course_id='id')}" in error_string
        assert "signal=<OpenEdxPublicSignal: simple.signal>" in error_string
        assert "initial_topic='topic'" in error_string
        assert "full_topic='dev-topic'" in error_string
        assert "event_key_field='bad_field'" in error_string
        assert "event_type='simple.signal'" in error_string
        assert "source='openedx/test/web'" in error_string
        assert f"id=UUID('{metadata.id}')" in error_string
        assert f"sourcehost='{metadata.sourcehost}'" in error_string


class TestCommand(TestCase):
    """
    Test produce_event management command
    """
    @override_settings(
        EVENT_BUS_REDIS_CONNECTION_URL='redis://locahost:6379/0',
        EVENT_BUS_TOPIC_PREFIX='dev',
        SERVICE_VARIANT='test',
    )
    @patch(
        'edx_event_bus_redis.internal.producer.serialize_event_data_to_bytes', autospec=True,
        return_value=b'value-bytes-here',
    )
    @patch('edx_event_bus_redis.management.commands.produce_event.logger')
    def test_command(self, fake_logger, _):
        producer_api = ep.create_producer()
        mocked_client = Mock(autospec=True)
        producer_api.client = mocked_client
        stream_mock = Mock()
        mocked_client.Stream.return_value = stream_mock

        with patch('edx_event_bus_redis.management.commands.produce_event.create_producer') as mock_create_producer:
            mock_create_producer.return_value = producer_api
            call_command(Command(),
                         topic=['test'],
                         signal=['openedx_events.learning.signals.SESSION_LOGIN_COMPLETED'],
                         data=['{"user": {"id": 123, "is_active": true,'
                               ' "pii":{"username": "foobob", "email": "bob@foo.example", "name": "Bob Foo"}}}'],
                         key_field=['user.pii.username'],
                         )
        mocked_client.Stream.assert_called_once_with('dev-test')
        # Actual event producing is tested elsewhere, this is just to make sure the command produces *something*
        stream_mock.add.assert_called_once()
        assert stream_mock.add.call_args.args[0][b"event_data"] == b'value-bytes-here'
        fake_logger.exception.assert_not_called()
