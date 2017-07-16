Microsoft Azure SDK for Python
==============================

This is the Microsoft Azure Network Resource Management Client Library.

Azure Resource Manager (ARM) is the next generation of management APIs that
replace the old Azure Service Management (ASM).

This package has been tested with Python 2.7, 3.3, 3.4 and 3.5.

For the older Azure Service Management (ASM) libraries, see
`azure-servicemanagement-legacy <https://pypi.python.org/pypi/azure-servicemanagement-legacy>`__ library.

For a more complete set of Azure libraries, see the `azure <https://pypi.python.org/pypi/azure>`__ bundle package.


Compatibility
=============

**IMPORTANT**: If you have an earlier version of the azure package
(version < 1.0), you should uninstall it before installing this package.

You can check the version using pip:

.. code:: shell

    pip freeze

If you see azure==0.11.0 (or any version below 1.0), uninstall it first:

.. code:: shell

    pip uninstall azure


Usage
=====

For code examples, see `Compute and Network Resource Management 
<https://azure-sdk-for-python.readthedocs.org/en/latest/resourcemanagementcomputenetwork.html>`__
on readthedocs.org.


Provide Feedback
================

If you encounter any bugs or have suggestions, please file an issue in the
`Issues <https://github.com/Azure/azure-sdk-for-python/issues>`__
section of the project.


.. :changelog:

Release History
===============

1.0.0 (2017-05-15)
++++++++++++++++++

- Tag 1.0.0rc3 as stable (same content)

1.0.0rc3 (2017-05-03)
+++++++++++++++++++++

**Features**

- Added check connectivity api to network watcher

1.0.0rc2 (2017-04-18)
+++++++++++++++++++++

**Features**

- Add ApiVersion 2016-12-01 and 2017-03-01
- 2017-03-01 is now default ApiVersion

**Bugfixes**

- Restore access to NetworkWatcher and PacketCapture from 2016-09-01

1.0.0rc1 (2017-04-11)
+++++++++++++++++++++

**Features**

To help customers with sovereign clouds (not general Azure),
this version has official multi ApiVersion support for 2015-06-15 and 2016-09-01

0.30.1 (2017-03-27)
+++++++++++++++++++

* Add NetworkWatcher
* Add PacketCapture
* Add new methods to Virtualk Network Gateway

  * get_bgp_peer_status
  * get_learned_routes
  * get_advertised_routes

0.30.0 (2016-11-01)
+++++++++++++++++++

* Initial preview release. Based on API version 2016-09-01.


0.20.0 (2015-08-31)
+++++++++++++++++++

* Initial preview release. Based on API version 2015-05-01-preview.


