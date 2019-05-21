..
    This file is part of Invenio.
    Copyright (C) 2015-2019 CERN.

    Invenio is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.

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
