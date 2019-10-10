# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Invenio module for information retrieval."""

from __future__ import absolute_import, print_function

# TODO: refactoring of the `strategy` field:
# since it currently has two variations only and seems to be used only at one
# specific place, it might be simplified in a boolean or extended for other uses in the migration/jobs.
INDEX_MIGRATOR_RECIPES = dict(
    deposits=dict(
        cls='invenio_index_migrator.api.Migration',
        params=dict(
            strategy='cross_cluster_strategy',
            src_es_client=dict(
                prefix='',
                version=2,
                params=dict(
                    host='zenodo-qa-search-c1.cern.ch',
                    port=443,
                    use_ssl=True,
                    http_auth='zenodo:fjaX9b3V1YcrnXsoE2',
                    url_prefix='',
                    verify_certs=False
                ),
            ),
            jobs=dict(
                deposit_simple_reindex=dict(
                    cls='invenio_index_migrator.api.ReindexAndSyncJob',
                    pid_type='depid',
                    index='deposits-records-record-v1.0.0',
                    rollover_threshold=10,
                    reindex_params=dict(
                        source=dict(
                            sort=dict(
                                _created='desc'
                            )
                        ),
                        dest=dict(
                            op_type='create'
                        ),
                    ),
                ),
            )
        )
    )
)

"""Index sync job definitions.

Example:

.. code-block:: python

    INDEX_MIGRATOR_RECIPES = dict(
        records=dict(
            cls='invenio_index_migrator.api.Migration',
            params=dict(
                strategy='cross_cluster_strategy',
                src_es_client=dict(
                    prefix='',
                    version=2,
                    params=dict(
                        host='es2',
                        port=9200,
                        use_ssl=True,
                        http_auth='user:pass',
                        url_prefix='on-demand',
                    ),
                ),
                jobs=dict(
                    records_simple_reindex=dict(
                        cls='invenio_index_migrator.api.ReindexJob',
                        pid_type='recid',
                        index='records-record-v1.0.0',
                        rollover_threshold=10,
                        reindex_params=dict(
                            script=dict(
                                source="if (ctx._source.foo == 'bar') {...}",
                                lang='painless'
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
                    )
                )
            )
        )
    )
"""

INDEX_MIGRATOR_INDEX_NAME = '.invenio-index-migrator'
