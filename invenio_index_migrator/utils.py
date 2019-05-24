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
from werkzeug.utils import cached_property, import_string

from invenio_search.proxies import current_search_client
from invenio_search.utils import build_alias_name


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


class ESClient():
    """ES clinet for sync jobs."""

    def __init__(self, es_config):
        """."""
        self.config = es_config

    @cached_property
    def reindex_remote(self):
        """Return ES client reindex API host."""
        client = self.client.transport.hosts[0]
        params = {}
        params['host'] = client.get('host', 'localhost')
        params['port'] = client.get('port', 9200)
        params['protocol'] = 'https' if client.get('use_ssl', False) else 'http'
        params['url_prefix'] = client.get('url_prefix', '')

        remote = dict(
            host='{protocol}://{host}:{port}/{url_prefix}'.format(**params)
        )

        username, password = self.reindex_auth
        if username and password:
            remote['username'] = username
            remote['password'] = password

        return remote

    @cached_property
    def reindex_auth(self):
        """Return username and password for reindex HTTP authentication."""
        username, password = None, None

        client = self.client.transport.hosts[0]
        http_auth = client.get('http_auth', None)
        if http_auth:
            if isinstance(http_auth, six.string_types):
                username, password = http_auth.split(':')
            else:
                username, password = http_auth

        return username, password

    @cached_property
    def client(self):
        """Return ES client."""
        return self._get_es_client()

    def _get_es_client(self):
        """Get ES client."""
        if self.config['version'] == 2:
            from elasticsearch2 import Elasticsearch as Elasticsearch2
            return Elasticsearch2([self.config['params']])
        else:
            raise Exception('unsupported ES version: {}'.format(self.config['version']))


class RecipeState:
    """Synchronization recipe state.

    The state is stored in ElasticSearch and can be accessed similarly to a
    python dictionary.
    """

    def __init__(self, index, document_id=None, client=None,
                 force=False, initial_state=None):
        """Synchronization job state in ElasticSearch."""
        self.index = build_alias_name(index)
        self.document_id = document_id or 'state'
        self.doc_type = '_doc'
        self.force = force
        self.client = client or current_search_client
        self._state = {}

    @property
    def state(self):
        """Get the full state."""
        self._state = self.client.get(
            index=self.index,
            doc_type=self.doc_type,
            id=self.document_id,
            ignore=[404],
        )['_source']
        return self._state


    def __getitem__(self, key):
        """Get key in state."""
        return self.state[key]

    def __setitem__(self, key, value):
        """Set key in state."""
        state = self.state
        state[key] = value
        self._save(state)

    def __delitem__(self, key):
        """Delete key in state."""
        state = self.state
        del state[key]
        self._save(state)

    def __iter__(self):
        return iter(self.state)

    def __len__(self):
        return len(self.state)

    def update(self, **changes):
        """Update multiple keys in the state."""
        state = self.state
        for key, value in changes.items():
            state[key] = value
        self._save(state)

    def create(self, initial_state, force=False):
        """Create state index and the document."""
        if (self.force or force) and self.client.indices.exists(self.index):
            self.client.indices.delete(self.index)
        self.client.indices.create(self.index)
        return self._save(initial_state)

    def _save(self, state):
        """Save the state to ElasticSearch."""
        # TODO: User optimistic concurrency control via "version_type=external_gte"
        self.client.index(
            index=self.index,
            id=self.document_id,
            doc_type=self.doc_type,
            body=state
        )
        return self.client.get(
            index=self.index,
            id=self.document_id,
            doc_type=self.doc_type,
        )

    def __repr__(self):
        """String representation of the state."""
        return str(self.state)