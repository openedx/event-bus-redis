"""
Test the event producer code.
"""

import warnings
from unittest import TestCase

import ddt
import openedx_events.event_bus
import openedx_events.learning.signals
from django.test import override_settings
from openedx_events.learning.data import UserData, UserPersonalData

import edx_event_bus_redis.internal.producer as ep


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
