"""
Tests for message module.
"""

import re
from datetime import datetime, timezone
from uuid import UUID

import ddt
import pytest
from django.test import TestCase
from openedx_events.learning.signals import SESSION_LOGIN_COMPLETED
from openedx_events.tooling import EventsMetadata

from edx_event_bus_redis.internal.message import RedisMessage, UnusableMessageError
from edx_event_bus_redis.internal.utils import encode


@ddt.ddt
class TestMessage(TestCase):
    """
    Tests for message parsing.
    """

    def setUp(self):
        super().setUp()
        self.event_id = b'629f9892-c258-11ed-8dac-1c83413013cb'
        self.event_data_bytes = b'\xf6\x01\x01\x0cfoobob\x1ebob@foo.example\x0eBob Foo'
        self.signal = SESSION_LOGIN_COMPLETED
        self.event_type = b'org.openedx.learning.auth.session.login.completed.v1'

    def test_normal_msg(self):
        msg_time = datetime.now(timezone.utc)
        msg = (
            b'1',
            {
                b'id': self.event_id,
                b'event_data': self.event_data_bytes,
                b'type': self.event_type,
                b'time': encode(msg_time.isoformat()),
            }
        )
        parsed_msg = RedisMessage.parse(msg, topic='some-local-topic')
        expected_msg = RedisMessage(
            topic='some-local-topic',
            event_data=self.event_data_bytes,
            event_metadata=EventsMetadata(
                id=UUID(self.event_id.decode()),
                event_type=self.event_type.decode(),
                time=msg_time
            ),
            msg_id=b'1',
        )
        self.assertEqual(parsed_msg, expected_msg)

    def test_no_type(self):
        msg = (
            b'1',
            {
                b'id': b'629f9892-c258-11ed-8dac-1c83413013cb',
                b'type': self.event_type,
            }
        )
        with pytest.raises(UnusableMessageError) as excinfo:
            RedisMessage.parse(msg, topic='some-local-topic')

        assert excinfo.value.args == (
            "Error determining metadata from message headers: b'event_data'",
        )

    def test_no_event_data(self):
        msg = (
            b'1',
            {
                b'id': b'629f9892-c258-11ed-8dac-1c83413013cb',
                b'event_data': self.event_data_bytes,
            }
        )
        with pytest.raises(UnusableMessageError) as excinfo:
            RedisMessage.parse(msg, topic='some-local-topic')
        expected_error_pattern = re.compile(
            r"Error determining metadata from message headers: .*__init__\(\) "
            r"missing 1 required positional argument: 'event_type'"
        )
        assert expected_error_pattern.search(str(excinfo.value)) is not None

    def test_bad_msg(self):
        """
        Check that if we cannot process the message headers, we raise an UnusableMessageError

        The various kinds of bad headers are more fully tested in test_utils
        """
        msg = (
            b'1',
            {
                b'id': b'bad_id',
                b'event_data': self.event_data_bytes,
                b'type': b'org.openedx.learning.auth.session.login.completed.v1',
            }
        )

        with pytest.raises(UnusableMessageError) as excinfo:
            RedisMessage.parse(msg, topic='some-local-topic')

        assert excinfo.value.args == (
            "Error determining metadata from message headers: badly formed hexadecimal UUID string",
        )
