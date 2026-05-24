# SPDX-FileCopyrightText: 2015-2019 CERN.
# SPDX-License-Identifier: MIT

"""Index sync proxies."""

from flask import current_app
from werkzeug.local import LocalProxy

current_index_migrator = LocalProxy(
    lambda: current_app.extensions['invenio-index-migrator'])
