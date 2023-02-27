"""
Configuration loading and validation.

This module is for internal use only.
"""

import warnings
from typing import Optional

from django.conf import settings


def load_common_settings() -> Optional[dict]:
    """
    Load common settings, a base for either producer or consumer configuration.

    Warns and returns None if essential settings are missing.
    """
    # .. setting_name: EVENT_BUS_REDIS_CONNECTION_URL
    # .. setting_default: None
    # .. setting_description: Redis connection url.
    #   For example::
    #       redis://[[username]:[password]]@localhost:6379/0
    #       rediss://[[username]:[password]]@localhost:6379/0
    url = getattr(settings, 'EVENT_BUS_REDIS_CONNECTION_URL', None)
    if url is None:
        warnings.warn("Cannot configure event-bus-redis: Missing setting EVENT_BUS_REDIS_CONNECTION_URL")
        return None

    base_settings = {
        'url': url,
    }
    return base_settings


def get_full_topic(base_topic: str) -> str:
    """
    Given a base topic name, add a prefix (if configured).
    """
    # .. setting_name: EVENT_BUS_TOPIC_PREFIX
    # .. setting_default: None
    # .. setting_description: If provided, add this as a prefix to any topic names (delimited by a hyphen)
    #   when either producing or consuming events. This can be used to support separation of environments,
    #   e.g. if multiple staging or test environments are sharing a cluster. For example, if the base topic
    #   name is "user-logins", then if EVENT_BUS_TOPIC_PREFIX=stage, the producer and consumer would instead
    #   work with the topic "stage-user-logins".
    topic_prefix = getattr(settings, 'EVENT_BUS_TOPIC_PREFIX', None)
    if topic_prefix:
        return f"{topic_prefix}-{base_topic}"
    else:
        return base_topic
