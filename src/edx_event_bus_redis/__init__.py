"""
Redis Streams implementation for the Open edX event bus.
"""

from importlib.metadata import PackageNotFoundError, version

from edx_event_bus_redis.internal.consumer import RedisEventConsumer
from edx_event_bus_redis.internal.producer import create_producer

try:
    __version__ = version("edx-event-bus-redis")
except PackageNotFoundError:
    pass
