# SPDX-FileCopyrightText: 2015-2019 CERN.
# SPDX-License-Identifier: MIT

"""Index syncing module."""

from __future__ import absolute_import, print_function

from .ext import InvenioIndexMigrator
from .proxies import current_index_migrator

__all__ = (
    'current_index_migrator',
    'InvenioIndexMigrator',
)
