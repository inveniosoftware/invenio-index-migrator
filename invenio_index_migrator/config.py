# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Invenio module for information retrieval."""

from __future__ import absolute_import, print_function

INDEX_MIGRATOR_JOBS = {}
"""Index sync job definitions.

Example:

.. code-block:: python

    INDEX_MIGRATOR_JOBS = dict(
        records=dict(
            cls='index_sync.sync.RecordSyncJob',
            params=dict(
                rollover_threshold=10,
                src_es_client=dict(
                    prefix='',
                    suffix='',
                    version=2,
                    params=dict(
                        host='es2',
                        port=9200,
                        use_ssl=True,
                        http_auth='user:pass',
                        url_prefix='on-demand',
                    ),
                ),
                reindex_params=dict(
                    script=dict(
                        source="if (ctx._source.foo == 'bar') {ctx._version++; ctx._source.remove('foo')}",
                        lang='painless',
                    ),
                    source=dict(
                        sort=dict(
                            date='desc'
                        )
                    ),
                    dest=dict(
                        op_type='create'
                    ),
                ),
                pid_mappings={
                    'recid': 'records-record-v1.0.0'
                }
            )
        )
    )
"""

INDEX_MIGRATOR_INDEX_NAME = '.invenio-index-sync'
