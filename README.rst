edx_event_bus_redis
#############################

.. note::

  This README was auto-generated. Maintainer: please review its contents and
  update all relevant sections. Instructions to you are marked with
  "PLACEHOLDER" or "TODO". Update or remove those sections, and remove this
  note when you are done.

|pypi-badge| |ci-badge| |codecov-badge| |doc-badge| |pyversions-badge|
|license-badge| |status-badge|

Purpose
*******

Redis Streams implementation for the Open edX event bus.

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
  mkvirtualenv -p python3.8 event-bus-redis


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

TODO: How can a new user go about deploying this component? Is it just a few
commands? Is there a larger how-to that should be linked here?

PLACEHOLDER: For details on how to deploy this component, see the `deployment how-to`_

.. _deployment how-to: https://docs.openedx.org/projects/event-bus-redis/how-tos/how-to-deploy-this-component.html

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

Please do not report security issues in public. Please email security@tcril.org.

.. |pypi-badge| image:: https://img.shields.io/pypi/v/event-bus-redis.svg
    :target: https://pypi.python.org/pypi/event-bus-redis/
    :alt: PyPI

.. |ci-badge| image:: https://github.com/openedx/event-bus-redis/workflows/Python%20CI/badge.svg?branch=main
    :target: https://github.com/openedx/event-bus-redis/actions
    :alt: CI

.. |codecov-badge| image:: https://codecov.io/github/openedx/event-bus-redis/coverage.svg?branch=main
    :target: https://codecov.io/github/openedx/event-bus-redis?branch=main
    :alt: Codecov

.. |doc-badge| image:: https://readthedocs.org/projects/event-bus-redis/badge/?version=latest
    :target: https://event-bus-redis.readthedocs.io/en/latest/
    :alt: Documentation

.. |pyversions-badge| image:: https://img.shields.io/pypi/pyversions/event-bus-redis.svg
    :target: https://pypi.python.org/pypi/event-bus-redis/
    :alt: Supported Python versions

.. |license-badge| image:: https://img.shields.io/github/license/openedx/event-bus-redis.svg
    :target: https://github.com/openedx/event-bus-redis/blob/main/LICENSE.txt
    :alt: License

.. |status-badge| image:: https://img.shields.io/badge/Status-Experimental-yellow
