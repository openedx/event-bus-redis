0001 Purpose of This Repo
#########################

Status
******

**Provisional**

Context
*******

To assist in wider community adoption of the Open edX event bus pattern, this
repository is created to maintain a second concrete implementation of the
event bus pattern introduced in https://github.com/openedx/event-bus-kafka .

The hope is that using a technology already in our stack and familiar to
operators will smooth the transition to event bus patterns and prevent a more
fundamental split in architecture going forward.

Decision
********

We will create a repository to investigate running the event bus against redis
streams as a second concrete implementation.

Consequences
************

The event bus architecture is designed to be pluggable, however if this
work is adopted as a full alternative to Kafka there may need to be updates
to the current architecture to ensure a common set of working standards across
implementations. Before this project is declared ready for use we expect to
document those trade-offs and make a considered decision about whether the
effort of maintaining two implementations is a worthwhile investment for the
Open edX community.

Rejected Alternatives
*********************

There are other options for message bus concrete implementations, each with
their own strengths and weaknesses. In the end, what mattered most was making
the pattern available to operators of all sizes without a need for deep
knowledge of new technology or requiring 3rd party hosting options. For the
purposes of small operators operating with tight monetary and human resource
constraints redis was the clear winner.
