0002 Redis streams based event bus
##################################

Status
******

**Approved**

Context
*******

The draft `OEP-52: Event Bus Architecture`_ explains how the Open edX platform
would benefit from an event bus, as well as providing some additional decisions
around the event bus.

The first implementation for event bus was using Kafka. The issue at stake is
that Kafka is overkill for all but the largest and most complex deployments,
however the utility of a global event bus is undisputed, and making a more
universally deployable event bus will encourage code that leverages this
capability.

.. _`OEP-52: Event Bus Architecture`: https://github.com/openedx/open-edx-proposals/pull/233

Decision
********

A second implementation of event bus using `Redis Streams`_ as the backend with
`Walrus`_ as its python client.

.. _`Walrus`: https://walrus.readthedocs.io/en/latest/
.. _`Redis Streams`: https://redis.io/docs/data-types/streams/

Why Redis stream?
*****************

A `Redis stream`_ is a data structure that acts like an append-only log. You can
use streams to record and simultaneously syndicate events in real time.

It allows creating persistent streams which can be subscribed to by consumers.
It supports consumer groups which can track all messages that are currently
pending, that is, messages that were delivered to some consumer of the consumer
group, but are yet to be acknowledged as processed. Thanks to this feature,
when accessing the message history of a stream, each consumer will only see
messages that were delivered to it.

An application consuming these events can create a single consumer group
(this does not affect the main list of events in the stream) for itself. Then
the replicas/workers of this application can each create a unique consumer in
the group. Since consumer groups keep track of messages for each consumer, the
application will `not lose any event`_ even in case it goes down for some time.
The consumer group will serve the pending events to the consumer when it comes
back up with the same consumer name.

.. _`not lose any event`: https://github.com/redis/redis-doc/blob/936de39da1098a1053febdb3defa88338b16c25a/docs/data-types/streams-tutorial.md?plain=1#L401
.. _`Redis stream`: https://redis.io/docs/data-types/streams-tutorial/#introduction

Please note that consumer name will be a required parameter for Redis consumers
in order to support above mentioned features.

Why Walrus?
***********

Walrus is a lightweight python utility for Redis and it subclasses and extends
redis-py client, allowing it to be used as a drop in replacement. The main
reason for choosing Walrus is its `simple API`_ for working with streams which
is much better than having to work with `raw redis commands via redis-py`_.

.. _`simple API`: https://walrus.readthedocs.io/en/latest/streams.html
.. _`raw redis commands via redis-py`: https://redis-py.readthedocs.io/en/stable/examples/redis-stream-example.html

Consequences
************

Pros:

* As we already have a dependency on redis, no additional infrastructure will
  be required.
* Easier local development setup.
* Can support small-to-medium scale production Open edX installations.
* Has support for persistent streams.
* Support consumer groups to keep track of messages consumed by each consumer.
* Allows replaying events from any point in time.

Cons:

* Support for proper order of execution is limited. It is recommended to run
  one consumer per consumer group in order to process events in order. Having
  multiple consumers in a group does not guarantee of order of execution.
* Does not have anything similar to schema evolution in Kafka.

Rejected Alternatives
*********************

None
