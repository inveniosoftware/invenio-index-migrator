#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2015-2019 CERN.
# SPDX-License-Identifier: MIT

pydocstyle invenio_index_migrator tests docs && \
isort -rc -c -df && \
check-manifest --ignore ".travis-*" && \
sphinx-build -qnNW docs docs/_build/html && \
python setup.py test
