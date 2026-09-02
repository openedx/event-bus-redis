"""
Redis Streams implementation for the Open edX event bus.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as get_version

from edx_event_bus_redis.internal.consumer import RedisEventConsumer
from edx_event_bus_redis.internal.producer import create_producer

try:
    __version__ = get_version("edx-event-bus-redis")
except PackageNotFoundError:  # pragma: no cover
    pass
