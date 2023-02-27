"""
Test common configuration loading.
"""

from unittest import TestCase

from django.test.utils import override_settings

from edx_event_bus_redis.internal import config


class TestCommonSettings(TestCase):
    """
    Test loading of settings common to producer and consumer.
    """
    def test_unconfigured(self):
        assert config.load_common_settings() is None

    def test_minimal(self):
        with override_settings(
                EVENT_BUS_REDIS_CONNECTION_URL='redis://:password@locahost:6379/0',
        ):
            assert config.load_common_settings() == {
                'url': 'redis://:password@locahost:6379/0',
            }


class TestTopicPrefixing(TestCase):
    """
    Test autoprefixing of base topic.
    """
    def test_no_prefix(self):
        assert config.get_full_topic('user-logins') == 'user-logins'

    @override_settings(EVENT_BUS_TOPIC_PREFIX='')
    def test_empty_string_prefix(self):
        """Check that empty string is treated the same as None."""
        assert config.get_full_topic('user-logins') == 'user-logins'

    @override_settings(EVENT_BUS_TOPIC_PREFIX='stage')
    def test_regular_prefix(self):
        assert config.get_full_topic('user-logins') == 'stage-user-logins'
