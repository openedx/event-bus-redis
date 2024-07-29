edx_event_bus_redis
#############################

|pypi-badge| |ci-badge| |codecov-badge| |doc-badge| |pyversions-badge|
|license-badge| |status-badge|

Purpose
*******

Redis Streams implementation for the Open edX event bus.

Overview
********
This package implements an event bus for Open EdX using Redis streams.

The event bus acts as a broker between services publishing events and other services that consume these events.

This package contains both the publishing code, which processes events into
messages to send to the stream, and the consumer code, which polls the stream
using a `while True` loop in order to turn messages back into django signal to
be emitted. This django signal contains event data which can be consumed by the
host application which does the actual event handling.
The actual Redis host is configurable.

The repository works together with the openedx/openedx-events repository to make the fully functional event bus.

Documentation
*************

To use this implementation of the Event Bus with openedx-events, you'll need to ensure that below the following Django settings are set::

    # redis connection url
    # https://redis.readthedocs.io/en/stable/examples/ssl_connection_examples.html#Connecting-to-a-Redis-instance-via-a-URL-string
    EVENT_BUS_REDIS_CONNECTION_URL: redis://:password@localhost:6379/
    EVENT_BUS_TOPIC_PREFIX: dev

    # Required, on the producing side only:
    # https://github.com/openedx/openedx-events/blob/06635f3642cee4020d6787df68bba694bd1233fe/openedx_events/event_bus/__init__.py#L105-L112
    # This will load a producer class which can send events to redis streams.
    EVENT_BUS_PRODUCER: edx_event_bus_redis.create_producer

    # Required, on the consumer side only:
    # https://github.com/openedx/openedx-events/blob/06635f3642cee4020d6787df68bba694bd1233fe/openedx_events/event_bus/__init__.py#L150-L157
    # This will load a consumer class which can consume events from redis streams.
    EVENT_BUS_CONSUMER: edx_event_bus_redis.RedisEventConsumer

Optional settings that are worth considering::

    # If the consumer encounters this many consecutive errors, exit with an error. This is intended to be used in a context where a management system (such as Kubernetes) will relaunch the consumer automatically.
    EVENT_BUS_REDIS_CONSUMER_CONSECUTIVE_ERRORS_LIMIT (defaults to None)

    # How long the consumer should wait for new entries in a stream.
    # As we are running the consumer in a while True loop, changing this setting doesn't make much difference
    # expect for changing number of monitoring messages while waiting for new events.
    # https://redis.io/commands/xread/#blocking-for-data
    EVENT_BUS_REDIS_CONSUMER_POLL_TIMEOUT (defaults to 60 seconds)

    # Limits stream size to approximately this number
    EVENT_BUS_REDIS_STREAM_MAX_LEN (defaults to 10_000)

For manual local testing, see ``Testing locally`` section below.


Getting Started
***************

Developing
==========

One Time Setup
--------------
.. code-block::

  # Clone the repository
  git clone git@github.com:openedx/event-bus-redis.git
  cd event-bus-redis

  # Set up a virtualenv using virtualenvwrapper with the same name as the repo and activate it
  mkvirtualenv -p python3.11 event-bus-redis


Every time you develop something in this repo
---------------------------------------------
.. code-block::

  # Activate the virtualenv
  workon event-bus-redis

  # Grab the latest code
  git checkout main
  git pull

  # Install/update the dev requirements
  make requirements

  # Run the tests and quality checks (to verify the status before you make any changes)
  make validate

  # Make a new branch for your changes
  git checkout -b <your_github_username>/<short_description>

  # Using your favorite editor, edit the code to make your change.
  vim ...

  # Run your new tests
  pytest ./path/to/new/tests

  # Run all the tests and quality checks
  make validate

  # Commit all your changes
  git commit ...
  git push

  # Open a PR and ask for review.

Testing locally
---------------

* Please execute below commands in virtual environment to avoid messing with
  your main python installation.
* Install all dependencies using ``make requirements``
* Run ``make redis-up`` in current directory.
* Run ``make consume_test_event`` to start running a single consumer or ``make multiple_consumer_test_event`` to run two consumers with different consumer names.
* Run ``make produce_test_event`` in a separate terminal to produce a fake event, the consumer should log this event.
* You can also add a fake handler to test emitted signal via consumer. Add below code snippet to ``edx_event_bus_redis/internal/consumer.py``.

.. code-block:: python

  from django.dispatch import receiver
  from openedx_events.content_authoring.signals import XBLOCK_DELETED
  @receiver(XBLOCK_DELETED)
  def deleted_handler(sender, signal, **kwargs):
      print(f"""=======================================  signal: {signal}""")
      print(f"""=======================================  kwargs: {kwargs}""")

Deploying
=========

After setting up required configuration, events are produced using the
``openedx_events.get_producer().send()`` method which needs to be called from
the producing side. For more information, visit this `link`_.

.. _link: https://openedx.atlassian.net/wiki/spaces/AC/pages/3508699151/How+to+start+using+the+Event+Bus#Producing-a-signal

To consume events, openedx_events provides a management command called
``consume_events`` which can be called like so:

.. code-block:: bash

   # consume events from topic xblock-status
   python manage.py consume_events --topic xblock-status --group_id test_group --extra '{"consumer_name": "test_group.c1"}'

   # replay events from specific redis msg id
   python manage.py consume_events --topic xblock-deleted --group_id test_group --extra '{"consumer_name": "test_group.c1", "last_read_msg_id": "1679676448892-0"}'

   # process all messages that were not read by this consumer group.
   python manage.py consume_events -t user-login -g user-activity-service --extra '{"check_backlog": true, "consumer_name": "c1"}'

   # claim messages pending for more than 30 minutes (1,800,000 milliseconds) from other consumers in the group.
   python manage.py consume_events -t user-login -g user-activity-service --extra '{"claim_msgs_older_than": 1800000, "consumer_name": "c1"}'

Note that the ``consumer_name`` in ``--extra`` argument is required for redis
event bus as this name uniquely identifies the consumer in a group and helps
with tracking processed and pending messages.

If required, you can also replay events i.e. process messages from a specific
point in history.

.. code-block:: bash

   # replay events from specific redis msg id
   python manage.py consume_events --signal org.openedx.content_authoring.xblock.deleted.v1 --topic xblock-deleted --group_id test_group --extra '{"consumer_name": "c1", "last_read_msg_id": "1684306039300-0"}'

The redis message id can be found from the producer logs in the host application, example:

.. code-block::

   Message delivered to Redis event bus: topic=dev-xblock-deleted, message_id=ab289110-f47e-11ed-bd90-1c83413013cb, signal=<OpenEdxPublicSignal: org.openedx.content_authoring.xblock.deleted.v1>, redis_msg_id=b'1684306039300-0'

Getting Help
************

Documentation
=============

PLACEHOLDER: Start by going through `the documentation`_.  If you need more help see below.

.. _the documentation: https://docs.openedx.org/projects/event-bus-redis

(TODO: `Set up documentation <https://openedx.atlassian.net/wiki/spaces/DOC/pages/21627535/Publish+Documentation+on+Read+the+Docs>`_)

More Help
=========

If you're having trouble, we have discussion forums at
https://discuss.openedx.org where you can connect with others in the
community.

Our real-time conversations are on Slack. You can request a `Slack
invitation`_, then join our `community Slack workspace`_.

For anything non-trivial, the best path is to open an issue in this
repository with as many details about the issue you are facing as you
can provide.

https://github.com/openedx/event-bus-redis/issues

For more information about these options, see the `Getting Help`_ page.

.. _Slack invitation: https://openedx.org/slack
.. _community Slack workspace: https://openedx.slack.com/
.. _Getting Help: https://openedx.org/getting-help

License
*******

The code in this repository is licensed under the AGPL 3.0 unless
otherwise noted.

Please see `LICENSE.txt <LICENSE.txt>`_ for details.

Contributing
************

Contributions are very welcome.
Please read `How To Contribute <https://openedx.org/r/how-to-contribute>`_ for details.

This project is currently accepting all types of contributions, bug fixes,
security fixes, maintenance work, or new features.  However, please make sure
to have a discussion about your new feature idea with the maintainers prior to
beginning development to maximize the chances of your change being accepted.
You can start a conversation by creating a new issue on this repo summarizing
your idea.

The Open edX Code of Conduct
****************************

All community members are expected to follow the `Open edX Code of Conduct`_.

.. _Open edX Code of Conduct: https://openedx.org/code-of-conduct/

People
******

The assigned maintainers for this component and other project details may be
found in `Backstage`_. Backstage pulls this data from the ``catalog-info.yaml``
file in this repo.

.. _Backstage: https://open-edx-backstage.herokuapp.com/catalog/default/component/event-bus-redis

Reporting Security Issues
*************************

Please do not report security issues in public. Please email security@openedx.org.

.. |pypi-badge| image:: https://img.shields.io/pypi/v/edx-event-bus-redis.svg
    :target: https://pypi.python.org/pypi/edx-event-bus-redis/
    :alt: PyPI

.. |ci-badge| image:: https://github.com/openedx/event-bus-redis/workflows/Python%20CI/badge.svg?branch=main
    :target: https://github.com/openedx/event-bus-redis/actions
    :alt: CI

.. |codecov-badge| image:: https://codecov.io/github/openedx/event-bus-redis/coverage.svg?branch=main
    :target: https://codecov.io/github/openedx/event-bus-redis?branch=main
    :alt: Codecov

.. |doc-badge| image:: https://readthedocs.org/projects/edx-event-bus-redis/badge/?version=latest
    :target: https://event-bus-redis.readthedocs.io/en/latest/
    :alt: Documentation

.. |pyversions-badge| image:: https://img.shields.io/pypi/pyversions/edx-event-bus-redis.svg
    :target: https://pypi.python.org/pypi/event-bus-redis/
    :alt: Supported Python versions

.. |license-badge| image:: https://img.shields.io/github/license/openedx/event-bus-redis.svg
    :target: https://github.com/openedx/event-bus-redis/blob/main/LICENSE.txt
    :alt: License

.. |status-badge| image:: https://img.shields.io/badge/Status-Experimental-yellow
