0003 Limiting stream length
###########################

Status
******

**Approved**

Context
*******

The redis streams are an append-only data structure that stores entries
forever. All the triggered events will be stored in the stream and eventually
consume all the available memory. However storing these events forever is
not required so we can set a limit to the number entries that can be stored,
removing older entries whenever the limit is crossed.

Decision
********

Setting ``MAXLEN`` while publishing the message will limit the maximum
length of stream.

.. code-block::

   stream.add(data, id='*', maxlen=None, approximate=True)

We can set maxlen value via django settings with a default value of 10000. To
check the amount of memory required to store 10K events, we published 40 events
of ``XBLOCK_DELETED`` signal into the stream and checked its memory usage like
so:

.. code-block::

   127.0.0.1:6379> MEMORY USAGE dev-xblock-deleted
   (integer) 13227 <bytes>

So the memory usage for a single event is around 350 bytes for this event but it
might be a bit on the higher side for events with more data. So we can consider
a value of 500 bytes per event. So for 10K deleted events we need around 5 Mb
of memory.

This will prevent memory issues by capping the maximum number of events and
setting ``approximate=True`` makes this operation efficient and fast. Detailed
explanation can be found `here`_.

.. _here: https://redis.io/docs/data-types/streams-tutorial/#capped-streams

Note that the stream like any other Redis data structure, is asynchronously
replicated to replicas and `persisted`_ into AOF and RDB files. So while they may
get removed from the stream they're not unrecoverable in the case of an issue.

.. _persisted: https://redis.io/docs/data-types/streams-tutorial/#persistence-replication-and-message-safety

Consequences
************

Pros: Since redis streams act as a log data structure, adding more entries to
streams increases it’s memory and could impact performance. Capping it to an
approximate number value of will help keep the performance good and not consume
the whole RAM.

Cons: Only last N events will be stored in the stream, so we can only replay last N
events.

Rejected Alternatives
*********************

Running `XTRIM`_ command regularly by creating custom django command. It
supports two strategies to evict older entries.

.. _XTRIM: https://redis.io/commands/xtrim/


* MAXLEN: Evicts entries as long as the stream's length exceeds the specified
  threshold, where threshold is a positive integer.
* MINID: Evicts entries with IDs lower than threshold, where threshold is a
  stream ID.

This method involves additional effort in terms of implementation as well as
maintenance than the proposed method.
