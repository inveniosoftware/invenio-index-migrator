# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Utility functions for index migration."""

import json

import requests
import six
from celery import current_app as current_celery_app
from invenio_search.proxies import current_search_client
from invenio_search.utils import build_alias_name
from six.moves.urllib.parse import urljoin
from werkzeug.utils import cached_property, import_string

from .indexer import SYNC_INDEXER_MQ_QUEUE


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


def get_queue_size(queue):
    """Get the queue size."""
    with current_celery_app.pool.acquire(block=True) as connection:
        bound_queue = queue.bind(connection)
        _, size, _ = bound_queue.queue_declare(passive=True)
    return size


# NOTE: This is a requests-only client for some read-only operations on
# Elasticsearch clusters.
class _BasicESClient(object):
    """Stripped-down basic ES client."""

    def __init__(self, host, port, http_auth, use_ssl, verify_certs):
        """."""
        self.verify_certs = verify_certs
        protocol = 'https' if use_ssl else 'http'
        self.base_url = '{0}://{1}@{2}:{3}/'.format(
            protocol, http_auth, host, port)

    def count(self, index):
        """Get the document count of an index."""
        if isinstance(index, list):
            index = ','.join(index)
        req = requests.get(
            urljoin(self.base_url, '{0}/_count'.format(index)),
            verify=self.verify_certs)
        return req.json()['count']

    def index_exists(self, index):
        """Check if an index/alias exists."""
        req = requests.head(
            urljoin(self.base_url, '{0}'.format(index)),
            verify=self.verify_certs)
        return req.status_code == 200

    def alias_exists(self, alias):
        """Check if an alias exists."""
        req = requests.head(
            urljoin(self.base_url, '_alias/{0}'.format(alias)),
            verify=self.verify_certs)
        return req.status_code == 200

    def get_indexes_from_alias(self, alias):
        """Get the indices of an alias."""
        return requests.get(
            urljoin(self.base_url, '*/_alias/{0}'.format(alias)),
            verify=self.verify_certs).json()


class ESClient(object):
    """ES clinet for sync jobs."""

    def __init__(self, es_config):
        """."""
        self.config = es_config

    @cached_property
    def reindex_remote(self):
        """Return ES client reindex API host."""
        params = {}
        params['host'] = self.config['params'].get('host', 'localhost')
        params['port'] = self.config['params'].get('port', 9200)
        params['protocol'] = 'https' \
            if self.config['params'].get('use_ssl', False) else 'http'
        params['url_prefix'] = self.config['params'].get('url_prefix', '')

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

        http_auth = self.config['params'].get('http_auth', None)
        if http_auth:
            if isinstance(http_auth, six.string_types):
                username, password = http_auth.split(':')
            else:
                username, password = http_auth

        return username, password

    @cached_property
    def client(self):
        """Return ES client."""
        params = self.config['params']
        return _BasicESClient(
            http_auth=params['http_auth'],
            host=params['host'],
            port=params['port'],
            use_ssl=params['use_ssl'],
            verify_certs=params['verify_certs']
        )


class State(object):
    """Migration ES state.

    The state is stored in ElasticSearch and can be accessed similarly to a
    python dictionary.
    """

    def __init__(self, index, document_id, client=None):
        """Synchronization job state in ElasticSearch."""
        self.index = index
        self.document_id = document_id
        self.doc_type = '_doc'
        self.client = client or current_search_client

    def read(self):
        """Fetch the current state from Elasticsearch."""
        return self.client.get(
            index=self.index,
            doc_type=self.doc_type,
            id=self.document_id,
            ignore=[404],
        )['_source']

    def create(self, initial_state, force=False):
        """Create state document."""
        if force and self.client.indices.exists(self.index):
            self.client.indices.delete(self.index)
        self.client.indices.create(self.index)
        return self.commit(initial_state)

    def commit(self, state):
        """Save the state to ElasticSearch."""
        # TODO: User optimistic concurrency control via
        # "version_type=external_gte"
        return self.client.index(
            index=self.index,
            id=self.document_id,
            doc_type=self.doc_type,
            body=state
        )
