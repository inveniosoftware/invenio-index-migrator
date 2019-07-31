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
import warnings
from datetime import datetime

from elasticsearch import VERSION as ES_VERSION
from elasticsearch.exceptions import NotFoundError
from flask import current_app
from invenio_search.api import RecordsSearch
from invenio_search.proxies import current_search, current_search_client
from invenio_search.utils import build_alias_name, build_index_name, \
    prefix_index
from six import string_types
from werkzeug.utils import cached_property

from ..indexer import SYNC_INDEXER_MQ_QUEUE, MigrationIndexer
from ..tasks import run_sync_job
from ..utils import ESClient, State, extract_doctype_from_mapping, \
    get_queue_size, obj_or_import_string


class Job(object):
    """Index migration job."""

    def __init__(self, name, migration, config):
        """Initialize a migration job."""
        self.name = name
        self.migration = migration
        self.config = config
        self.state = State(
            self.migration.index, document_id=self.document_name)

    @property
    def document_name(self):
        """Get the document name for the job."""
        return '{}-{}'.format(self.migration.name, self.name)

    @property
    def src_es_client(self):
        """Get the source ES client."""
        return self.migration.src_es_client

    def run(self):
        """Run the job."""
        raise NotImplementedError()

    def cancel(self):
        """Cancel the job."""
        raise NotImplementedError()

    def status(self):
        """Return the status of the job."""
        job_state = self.state.state
        current = {}
        current['completed'] = False
        current['last_updated'] = str(datetime.fromtimestamp(
            float(job_state['last_record_update'])))
        current['queue_size'] = get_queue_size(SYNC_INDEXER_MQ_QUEUE)
        if job_state['reindex_task_id']:
            task = current_search_client.tasks.get(
                task_id=job_state['reindex_task_id'])
            current['_es_task_response'] = task
            current['completed'] = task['completed']
            if task['completed']:
                current['status'] = 'Finished reindex'
                current['seconds'] = task['response']['took'] / 1000.0
                current['total'] = task['task']['status']['total']
            else:
                current['status'] = 'Reindexing...'
                current['duration'] = '{:.1f} second(s)'.format(
                    task['task']['running_time_in_nanos'] / 1000000000.0)
                current['current'] = current_search_client.count(
                    index=job_state['dst']['index']
                )['count']
                current['percent'] = \
                    100.0 * current['current'] / current['total']
            current['created'] = task['task']['status']['created']
            current['total'] = task['task']['status']['total']
        else:
            current['status'] = 'Finished'
        current['threshold_reached'] = job_state['threshold_reached']
        return current

    def create_index(self, index):
        """Create indexes needed for the job."""
        index_result, _ = current_search.create_index(
            index,
            create_write_alias=False
        )
        index_name = index_result[0]
        refresh_interval = self.config.get('refresh_interval', '60s')
        if refresh_interval:
            current_search_client.indices.put_settings(
                index=index_name,
                body=dict(
                    index=dict(
                        refresh_interval=refresh_interval
                    )
                )
            )
        print('[*] created index: {}'.format(index_name))

        state = self.state.state
        state['dst']['index'] = index_name
        self.state.commit(state)


class ReindexJob(Job):
    """Reindex job that uses Elasticsearch's reindex API."""

    def run(self):
        """Fetch source index using ES Reindex API."""
        pid_type = self.config['pid_type']
        print('[*] running reindex for pid type: {}'.format(pid_type))
        reindex_params = self.config.get('reindex_params', {})
        source_params = reindex_params.pop('source', {})
        dest_params = reindex_params.pop('dest', {})

        state = self.state.state
        payload = dict(
            source=dict(
                index=state['src']['index'],
                **source_params
            ),
            dest=dict(
                index=state['dst']['index'],
                version_type='external',
                **dest_params
            ),
            **reindex_params
        )
        if self.migration.strategy == self.migration.CROSS_CLUSTER_STRATEGY:
            payload['source']['remote'] = \
                self.migration.src_es_client.reindex_remote

        # Reindex using ES Reindex API synchronously
        # Keep track of the time we issued the reindex command
        start_date = datetime.utcnow()
        wait_for_completion = self.config.get('wait_for_completion') or False
        response = current_search_client.reindex(
            wait_for_completion=wait_for_completion,
            body=payload
        )
        state['stats']['total'] = self.migration.src_es_client.client.count(
            index=state['src']['index']
        )['count']
        state['last_record_update'] = str(datetime.timestamp(start_date))
        task_id = response.get('task', None)
        state['reindex_task_id'] = task_id
        if wait_for_completion:
            state['threshold_reached'] = True
            state['status'] = 'COMPLETED'
        self.state.commit(state)
        print('reindex task started: {}'.format(task_id))
        return state

    def cancel(self):
        """Cancel reindexing job."""
        state = self.state.state
        task_id = state['reindex_task_id']
        cancel_response = current_search_client.tasks.cancel(task_id)
        state['status'] = 'CANCELLED'
        self.state.commit(state)
        if 'node_failures' in cancel_response:
            print('failed to cancel task', cancel_response)
        else:
            print('- successfully cancelled task: {}'.format(task_id))


class ReindexAndSyncJob(ReindexJob):
    """Job that both reindexes with ES reindex API and syncs with the DB.

    The first run will use the reindex API and the subsequent runs will fetch
    from the database and sync the data.
    """

    def iter_indexer_ops(self, start_date=None, end_date=None):
        """Iterate over documents that need to be reindexed."""
        from invenio_db import db
        from invenio_pidstore.models import PersistentIdentifier, PIDStatus
        from invenio_records.models import RecordMetadata

        q = db.session.query(
            RecordMetadata.id.distinct(),
            PersistentIdentifier.status,
            PersistentIdentifier.pid_type
        ).join(
            PersistentIdentifier,
            RecordMetadata.id == PersistentIdentifier.object_uuid
        ).filter(
            PersistentIdentifier.pid_type == self.config['pid_type'],
            PersistentIdentifier.object_type == 'rec',
            RecordMetadata.updated >= start_date
        ).yield_per(500)  # TODO: parameterize

        for record_id, pid_status, pid_type in q:
            _dst = self.state.state['dst']
            _index = _dst['index']
            _doc_type = _dst['doc_type']
            payload = {'id': record_id, 'index': _index, 'doc_type': _doc_type}
            if pid_status == PIDStatus.DELETED:
                payload['op'] = 'delete'
            else:
                payload['op'] = 'create'
            yield payload

    def run_delta_job(self):
        """Calculate delta from DB changes since the last update."""
        state = self.state.state
        # Check if reindex task is running - abort
        task = current_search_client.tasks.get(
            task_id=state['reindex_task_id'])
        if not task['completed']:
            raise RuntimeError(
                'Reindex is currently running - aborting delta.')

        # determine bounds
        start_time = state['last_record_update']
        if not start_time:
            raise RuntimeError(
                'no reindex task running nor start time - aborting')
        else:
            start_time = datetime.fromtimestamp(float(start_time))

            # Fetch data from start_time from db
            indexer = MigrationIndexer()

            # Send indexer actions to special reindex queue
            start_date = datetime.utcnow()
            indexer._bulk_op(self.iter_indexer_ops(start_time), None)
            last_record_update = str(datetime.timestamp(start_date))
            # Run synchornous bulk index processing
            # TODO: make this asynchronous by default
            succeeded, failed = indexer.process_bulk_queue(
                es_bulk_kwargs=dict(raise_on_error=False)
            )
            total_actions = succeeded + failed
            print('[*] indexed {} record(s)'.format(total_actions))
            threshold_reached = False
            if total_actions <= state['rollover_threshold']:
                threshold_reached = True
            state['last_record_update'] = last_record_update
            state['threshold_reached'] = threshold_reached
            self.state.commit(state)
            return state

    def run(self):
        """Run reindexing and syncing job."""
        if not self.config.get('wait_for_completion', False):
            if self.state.state['reindex_task_id']:
                return self.run_delta_job()
        else:
            if self.state.state['status'] != 'COMPLETED':
                return super(ReindexAndSyncJob, self).run()

    def cancel(self):
        """Cancel reinding and syncing job."""
        super(ReindexAndSyncJob, self).cancel()
