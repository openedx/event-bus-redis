"""
Test header conversion utils
"""
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from uuid import uuid1

import attr
import ddt
import pytest
from django.test import TestCase, override_settings
from openedx_events.data import EventsMetadata

from edx_event_bus_redis.internal.utils import (
    HEADER_EVENT_TYPE,
    HEADER_ID,
    HEADER_SOURCELIB,
    HEADER_TIME,
    get_headers_from_metadata,
    get_metadata_from_headers,
)


def side_effects(functions: list):
    """
    Given a list of functions, return a new function that will call each one in turn
    on successive invocations. (The returned function ignores any arguments it is
    called with.) Each function's return value will be returned. Behavior is
    undefined if insufficient functions are supplied.
    """
    f_iter = iter(functions)

    def inner(*_args, **_kwargs):
        nonlocal f_iter
        return next(f_iter)()

    return inner


class TestTestHelpers(TestCase):
    """Tests for local unit test utilities."""

    def test_side_effects(self):
        f = side_effects([
            lambda: 5,
            lambda: 1/0,
            lambda: 6,
        ])
        assert f() == 5
        with pytest.raises(ArithmeticError):
            f()
        assert f(1, 2, 3, a=4, b=5) == 6


TEST_UUID = uuid1()


@ddt.ddt
class TestUtils(TestCase):
    """ Tests for header conversion utils """

    def test_headers_from_event_metadata(self):
        """
        Check we can generate message headers from an EventsMetadata object
        """
        with override_settings(SERVICE_VARIANT='test'):
            metadata = EventsMetadata(event_type="org.openedx.learning.auth.session.login.completed.v1",
                                      id=TEST_UUID,
                                      sourcelib=(1, 2, 3),
                                      sourcehost="host",
                                      minorversion=0,
                                      time=datetime.fromisoformat("2023-01-01T14:00:00+00:00"))
            headers = get_headers_from_metadata(event_metadata=metadata)
            self.assertDictEqual(headers, {
                b'type': b'org.openedx.learning.auth.session.login.completed.v1',
                b'id': str(TEST_UUID).encode("utf8"),
                b'source': b'openedx/test/web',
                b'sourcehost': b'host',
                b'time': b'2023-01-01T14:00:00+00:00',
                b'sourcelib': b'1.2.3',
                b'minorversion': b'0',
            })

    def test_metadata_from_headers(self):
        """
        Check we can generate an EventsMetadata object from valid message headers
        """
        uuid = uuid1()
        headers = {
            b'type': b'org.openedx.learning.auth.session.login.completed.v1',
            b'id': str(uuid).encode("utf8"),
            b'source': b'openedx/test/web',
            b'sourcehost': b'testsource',
            b'time': b'2023-01-01T14:00:00+00:00',
            b'sourcelib': b'1.2.3',
            b'minorversion': b'0'
        }
        generated_metadata = get_metadata_from_headers(headers)
        expected_metadata = EventsMetadata(
            event_type="org.openedx.learning.auth.session.login.completed.v1",
            id=uuid,
            minorversion=0,
            source='openedx/test/web',
            sourcehost='testsource',
            time=datetime.fromisoformat("2023-01-01T14:00:00+00:00"),
            sourcelib=(1, 2, 3),
        )
        self.assertDictEqual(attr.asdict(generated_metadata), attr.asdict(expected_metadata))

    TEST_UUID_BYTES = str(TEST_UUID).encode("utf8")

    @patch('edx_event_bus_redis.internal.utils.oed.datetime')
    @ddt.data(
        (TEST_UUID_BYTES, None, None, False),  # As long as we have a id header, we can continue
        (b'bad', None, None, True),  # bad uuid
        (TEST_UUID_BYTES, b'bad', None, True),  # badly-formatted time
        (TEST_UUID_BYTES, None, b'bad', True),  # badly-formatted sourcelib
    )
    @ddt.unpack
    def test_generate_metadata_from_missing_or_bad_headers(self, msg_id, msg_time, source_lib, should_raise, mock_dt):
        """
        Check that we raise an exception iff there are missing required headers, or some of them are unparseable
        """
        now = datetime.now(timezone.utc)
        mock_dt.now = Mock(return_value=now)
        headers = {
            HEADER_ID.message_header_key.encode('utf8'): msg_id,
            HEADER_TIME.message_header_key.encode('utf8'): msg_time,
            HEADER_SOURCELIB.message_header_key.encode('utf8'): source_lib,
            HEADER_EVENT_TYPE.message_header_key.encode('utf8'): b'abc'
        }
        if should_raise:
            with pytest.raises(Exception):
                get_metadata_from_headers(headers)
        else:
            # check that we use all the regular EventsMetadata defaults for missing fields by constructing one
            # and comparing it to the one generated from get_metadata_from_headers
            expected_metadata = EventsMetadata(event_type="abc", id=TEST_UUID)
            generated_metadata = get_metadata_from_headers(headers)
            self.assertDictEqual(attr.asdict(generated_metadata), attr.asdict(expected_metadata))
