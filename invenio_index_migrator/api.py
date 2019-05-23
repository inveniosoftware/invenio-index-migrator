# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Index syncing API."""

from __future__ import absolute_import, print_function

import json

from datetime import datetime

from elasticsearch import VERSION as ES_VERSION
from flask import current_app
from invenio_search.api import RecordsSearch
from invenio_search.proxies import current_search, current_search_client
from invenio_search.utils import prefix_index, build_index_name, build_alias_name
from six import string_types
from werkzeug.utils import cached_property

from .indexer import SyncIndexer
from .tasks import run_sync_job
from .utils import extract_doctype_from_mapping


class SyncJob:
    """Index synchronization job base class."""

    def __init__(self, rollover_threshold, pid_mappings, src_es_client,
                 reindex_params={}):
        """Initialize the job configuration."""
        self.rollover_threshold = rollover_threshold
        self.pid_mappings = pid_mappings
        self.reindex_params = reindex_params
        self.src_es_client = SyncEsClient(src_es_client)
        self._state = SyncJobState(index=current_app.config['INDEX_MIGRATOR_INDEX_NAME'])

    def _build_index_mapping(self, dry_run=False):
        """Build index mapping."""

        def get_src(name, prefix, suffix):
            index_name = None
            src_index_name = build_index_name(name, prefix=prefix,
                                              suffix=suffix)
            src_alias_name = build_alias_name(name, prefix=prefix)
            if old_client.indices.exists(src_index_name):
                index_name = src_index_name
            elif old_client.indices.exists_alias(src_alias_name):
                indexes = list(old_client.indices.get_alias(name=src_alias_name).keys())
                if len(indexes) > 1:
                    raise Exception('Multiple indexes found for alias {}.'.format(src_alias_name))
                index_name = indexes[0]
            else:
                raise Exception(
                    "alias({}) or index({}) doesn't exist".format(
                        src_alias_name, src_index_name)
                )
            return dict(
                index=index_name,
            )

        def find_aliases_for_index(index_name, aliases):
            if isinstance(aliases, str):
                return None
            for key, values in aliases.items():
                if key == index_name:
                    return [build_alias_name(key)]
                else:
                    found_aliases = find_aliases_for_index(index_name, values)
                    if isinstance(found_aliases, list):
                        found_aliases.append(build_alias_name(key))
                        return found_aliases


        def get_dst(name):
            dst_index = build_index_name(name)
            mapping_fp = current_search.mappings[name]
            dst_index_aliases = find_aliases_for_index(name, current_search.aliases) or []
            return dict(
                index=dst_index if dry_run else name,
                aliases= dst_index_aliases,
                mapping=mapping_fp,
                doc_type=extract_doctype_from_mapping(mapping_fp),
            )

        old_client = self.src_es_client.client
        index_mapping = {}
        for pid_type, name in self.pid_mappings.items():
            mapping = dict(
                src=get_src(
                    name,
                    self.src_es_client.config.get('prefix'),
                    self.src_es_client.config.get('suffix')
                ),
                dst=get_dst(name)
            )
            index_mapping[pid_type] = mapping
        return index_mapping

    def init(self, dry_run=False):
        # Check if there's an index sync already happening (and bail)
        if current_search_client.indices.exists(self.state.index):
            raise Exception('The index {} already exists, a job is already active.'.format(self.state.index))

        # Get old indices
        index_mapping = self._build_index_mapping(dry_run=dry_run)

        if dry_run:
           return index_mapping

        # Create new indices
        for indexes in index_mapping.values():
            dst = indexes['dst']
            index = dst['index']
            index_result, _ = current_search.create_index(index, create_alias=False)
            print('[*] created index: {}'.format(index_result[0]))
            indexes['dst']['index'] = index_result[0]

        # Store index mapping in state
        initial_state = {
            'index_mapping': index_mapping,
            'last_record_update': None,
            'reindex_api_task_id': None,
            'threshold_reached': False,
            'rollover_ready': False,
            'rollover_finished': False,
            'stats': {},
        }
        self.state.create(initial_state)

    def iter_indexer_ops(self, start_date=None, end_date=None):
        """Iterate over documents that need to be reindexed."""
        from datetime import datetime, timedelta
        from invenio_db import db
        from invenio_pidstore.models import PersistentIdentifier, PIDStatus
        from invenio_records.models import RecordMetadata
        import sqlalchemy as sa

        q = db.session.query(
            RecordMetadata.id.distinct(),
            PersistentIdentifier.status,
            PersistentIdentifier.pid_type
        ).join(
            PersistentIdentifier,
            RecordMetadata.id == PersistentIdentifier.object_uuid
        ).filter(
            PersistentIdentifier.pid_type.in_(self.state['index_mapping'].keys()),
            PersistentIdentifier.object_type == 'rec',
            RecordMetadata.updated >= start_date
        ).yield_per(500)  # TODO: parameterize


        for record_id, pid_status, pid_type in q:
            _dst = self.state['index_mapping'][pid_type]['dst']
            _index = _dst['index']
            _doc_type = _dst['doc_type']
            payload = {'id': record_id, 'index': _index, 'doc_type': _doc_type}
            if pid_status == PIDStatus.DELETED:
                payload['op'] = 'delete'
            else:
               payload['op'] = 'create'
            yield payload

    def rollover(self):
        """Perform a rollover action."""
        raise NotImplementedError()

    @property
    def state(self):
        return self._state

    def run(self):
        """Run the index sync job."""
        # determine bounds
        start_time = self.state['last_record_update']
        index_mapping = self.state['index_mapping']

        if not start_time:
            # use reindex api
            for pid_type, indexes in index_mapping.items():
                print('[*] running reindex for pid type: {}'.format(pid_type))
                reindex_params = self.reindex_params
                source_params = reindex_params.pop('source', {})
                dest_params = reindex_params.pop('dest', {})

                payload = {
                    "source": {
                        "remote": self.src_es_client.reindex_remote,
                        "index": indexes['src']['index'],
                        **source_params,
                    },
                    "dest": {
                        "index": indexes['dst']['index'],
                        **dest_params,
                    },
                    **reindex_params
                }
                # Reindex using ES Reindex API synchronously
                # Keep track of the time we issued the reindex command
                start_date = datetime.utcnow()
                current_search_client.reindex(body=payload)
                self.state['last_record_update'] = \
                    str(datetime.timestamp(start_date))
            print('[*] reindex done')
        else:
            start_time = datetime.fromtimestamp(float(start_time))

            # Fetch data from start_time from db
            indexer = SyncIndexer()

            # Send indexer actions to special reindex queue
            start_date = datetime.utcnow()
            indexer._bulk_op(self.iter_indexer_ops(start_time), None)
            self.state['last_record_update'] = \
                    str(datetime.timestamp(start_date))
            # Run synchornous bulk index processing
            # TODO: make this asynchronous by default
            succeeded, failed = indexer.process_bulk_queue()
            total_actions = succeeded + failed
            print('[*] indexed {} record(s)'.format(total_actions))
            if total_actions <= self.rollover_threshold:
                self.state['threshold_reached'] = True
                self.rollover()


class SyncEsClient():
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
            if isinstance(http_auth, string_types):
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


class SyncJobState(object):
    """Synchronization job state.

    The state is stored in ElasticSearch and can be accessed similarly to a
    python dictionary.
    """

    def __init__(self, index, document_id=None, client=None,
                 force=False, initial_state=None):
        """Synchronization job state in ElasticSearch."""
        self.index = index
        self.document_id = document_id or 'state'
        self.doc_type = '_doc'
        self.force = force
        self.client = client or current_search_client

    @property
    def state(self):
        """Get the full state."""
        _state = self.client.get(
            index=self.index,
            doc_type=self.doc_type,
            id=self.document_id,
            ignore=[404],
        )
        return _state['_source']


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
