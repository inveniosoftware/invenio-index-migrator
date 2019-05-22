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
                rollover_threshold=100,
                src_es_client=dict(
                    version=2,
                    prefix='',
                    suffix='',
                    params={...}
                ),
                pid_mappings={...}
            )
        )
)
"""

INDEX_MIGRATOR_INDEX_NAME = '.invenio-index-sync'
