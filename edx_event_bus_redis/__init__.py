"""
Redis Streams implementation for the Open edX event bus.
"""

from edx_event_bus_redis.internal.consumer import RedisEventConsumer
from edx_event_bus_redis.internal.producer import create_producer

__version__ = "0.6.1"
