Setup example with openedx, course discovery and tutor.
=======================================================

* Setup `tutor-nightly <https://docs.tutor.overhang.io/tutorials/nightly.html>`_ till Palm is released, after that we can use stable tutor version.
* Enable discovery tutor plugin using ``tutor plugins enable discovery``
* Make sure ``edx-event-bus-redis`` is part of the pip requirements file in
  both edx-platform and course-discovery.
* Add below settings to your tutor setup with your preferred method:

  .. code-block:: python

     EVENT_BUS_PRODUCER = 'edx_event_bus_redis.create_producer'
     EVENT_BUS_REDIS_CONNECTION_URL = 'redis://@redis:6379/'
     EVENT_BUS_TOPIC_PREFIX = 'dev'
     EVENT_BUS_CONSUMER = 'edx_event_bus_redis.RedisEventConsumer'

* One of the ways to add these settings to tutor is to create a small plugin using below steps:

  * Create a plugin file as mentioned in `tutor tutorial <https://docs.tutor.overhang.io/tutorials/plugin.html#writing-a-plugin-as-a-single-python-module>`_
  * Replace the contents of the plugin file with:

    .. code-block:: python

       from tutor import hooks

       redis_config = [
           "EVENT_BUS_PRODUCER = 'edx_event_bus_redis.create_producer'",
           "EVENT_BUS_REDIS_CONNECTION_URL = 'redis://@redis:6379/'",
           "EVENT_BUS_TOPIC_PREFIX = 'dev'",
           "EVENT_BUS_CONSUMER = 'edx_event_bus_redis.RedisEventConsumer'",
       ]

       hooks.Filters.ENV_PATCHES.add_item(
           (
               "discovery-common-settings", "\n".join(redis_config)
           )
       )

       hooks.Filters.ENV_PATCHES.add_item(
           (
               "openedx-common-settings", "\n".join(redis_config)
           )
       )

  * Enable the plugin as mentioned in the tutorial.

* Save tutor config and run Open Edx using below commands:

  .. code-block:: shell

     tutor config save
     tutor dev start

* To consume events, start a consumer in the IDA. For example, if we want to consume events from event bus in discovery:

  .. code-block:: shell

     # Run below command and delete any xblock from studio to see the event logs flow in the consumer.
     tutor dev run discovery ./manage.py consume_events --signal org.openedx.content_authoring.xblock.deleted.v1 --topic xblock-deleted --group_id test_group --extra '{"consumer_name": "c1"}'

Troubleshooting
***************

If the ``consume_events`` command fails with ``unknown argument --extra``
error, rebuild discovery image to use latest changes from the repository and re
run the command. To rebuild image run below command.

.. code-block:: shell

   tutor images build -a DISCOVERY_REPOSITORY=https://github.com/openedx/course-discovery/ discovery
