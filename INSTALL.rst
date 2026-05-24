..
    SPDX-FileCopyrightText: 2015-2019 CERN.
    SPDX-License-Identifier: MIT

Installation
============

Invenio-Index-Migrator is on PyPI. When you install invenio-index-migrator you must specify the
appropriate extras dependency for the version of Elasticsearch you use:

.. code-block:: console

    $ # For Elasticsearch 2.x:
    $ pip install invenio-index-migrator[elasticsearch2]

    $ # For Elasticsearch 5.x:
    $ pip install invenio-index-migrator[elasticsearch5]

    $ # For Elasticsearch 6.x:
    $ pip install invenio-index-migrator[elasticsearch6]
