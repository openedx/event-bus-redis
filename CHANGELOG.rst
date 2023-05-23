Change Log
##########

..
   All enhancements and patches to edx_event_bus_redis will be documented
   in this file.  It adheres to the structure of https://keepachangelog.com/ ,
   but in reStructuredText instead of Markdown (for ease of incorporation into
   Sphinx documentation and the PyPI description).

   This project adheres to Semantic Versioning (https://semver.org/).

.. There should always be an "Unreleased" section for changes pending release.

Unreleased
**********

*

[0.3.0] - 2023-05-23
************************************************

Changed
=======
* **BREAKING CHANGE**: Removed deprecated ``signal`` argument from consumer.

[0.2.1] - 2023-05-12
************************************************

Changed
=======
* Deprecated ``signal`` argument in consumer (made optional in preparation for removal)

[0.1.1] - 2023-05-12
************************************************

Added
=====

* Option to claim messages from other consumers based on idle time.

Changed
=======

* Setting ``check_backlog`` will read messages that were not read by this consumer group.

[0.1.0] - 2023-05-04
************************************************

Added
=====

* First release on PyPI.
* Redis streams consumer and producer implemented.
