# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Utility functions for index migration."""

import json
import six
from werkzeug.utils import import_string


def obj_or_import_string(value, default=None):
    """Import string or return object.

    :params value: Import path or class object to instantiate.
    :params default: Default object to return if the import fails.
    :returns: The imported object.
    """
    if isinstance(value, six.string_types):
        return import_string(value)
    elif value:
        return value
    return default


def extract_doctype_from_mapping(mapping_fp):
    """Extract the doc_type from mapping filepath."""
    from elasticsearch import VERSION as ES_VERSION

    lt_es7 = ES_VERSION[0] < 7
    _doc_type = None
    if lt_es7:
        with open(mapping_fp, 'r') as mapping_file:
            mapping = json.loads(mapping_file.read())
            _doc_type = mapping[list(mapping.keys())[0]]
    else:
        _doc_type = '_doc'
    return _doc_type
