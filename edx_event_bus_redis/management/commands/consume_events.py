"""
Makes ``consume_events`` management command available.
"""

from edx_event_bus_redis.internal.consumer import ConsumeEventsCommand as Command  # pylint: disable=unused-import
