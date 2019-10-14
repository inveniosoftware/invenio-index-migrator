# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Index syncing API."""

from __future__ import absolute_import, print_function

import six
from datetime import datetime

from invenio_search.proxies import current_search, current_search_client
from invenio_search.utils import build_alias_name, build_index_name, \
    prefix_index

from ..indexer import SYNC_INDEXER_MQ_QUEUE, MigrationIndexer
from ..utils import State, extract_doctype_from_mapping, \
    get_queue_size


class Job(object):
    """Index migration job."""

    def __init__(self, name, migration, config):
        """Initialize a migration job.

        :param name: job's name.
        :param migration: an invenio_index_migrator.api.migration.Migration
            object.
        :param config: job's configuration.
        """
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

    # TODO: Define the attributes and values of the returned dict
    def status(self):
        """Return the status of the job."""
        job_state = self.state.read()
        current = {}
        current['completed'] = False
        current['last_updated'] = job_state['last_record_update']
        current['queue_size'] = get_queue_size(SYNC_INDEXER_MQ_QUEUE)
        if job_state['reindex_task_id']:
            task = current_search_client.tasks.get(
                task_id=job_state['reindex_task_id'])
            current['total'] = task['task']['status']['total']
            current['_es_task_response'] = task
            current['completed'] = task['completed']
            if task['completed']:
                current['status'] = 'Finished reindex'
                current['seconds'] = task['response']['took'] / 1000.0
            else:
                current['status'] = 'Reindexing...'
                current['duration'] = '{:.1f} second(s)'.format(
                    task['task']['running_time_in_nanos'] / 1000000000.0)
                current['current'] = current_search_client.count(
                    index=job_state['dst']['index'])['count']
                if current['total'] > 0:
                    current['percent'] = \
                        100.0 * current['current'] / current['total']
            current['created'] = task['task']['status']['created']

        else:
            current['status'] = 'Finished'
        current['threshold_reached'] = job_state['threshold_reached']
        return current

    # TODO:
    def create_index(self, index):
        """Create indexes needed for the job."""
        index_result, _ = current_search.create_index(
            index,
            create_write_alias=False
        )
        index_name = index_result[0]
        refresh_interval = self.config.get('refresh_interval', '300s')
        current_search_client.indices.put_settings(
            index=index_name,
            body=dict(index=dict(refresh_interval=refresh_interval))
        )
        print('[*] created index: {}'.format(index_name))

        state = self.state.read()
        state['dst']['index'] = index_name
        self.state.commit(state)

    def rollover_actions(self):
        actions = []
        state = self.state.read()
        src_index = state["src"]["index"]
        dst_index = state["dst"]["index"]

        # Reset the "refresh_interval" setting for the destination index
        current_search_client.indices.put_settings(
            index=dst_index,
            body=dict(index=dict(refresh_interval=None))
        )
        # Preform a "flush + refresh" before rolling over the aliases
        current_search_client.indices.flush(
            wait_if_ongoing=True, index=dst_index)
        current_search_client.indices.refresh(index=dst_index)

        for alias in state["dst"]["aliases"]:
            if self.strategy == self.migration.IN_CLUSTER_STRATEGY:
                actions.append(
                    {"remove": {"index": src_index, "alias": alias}})
            actions.append({"add": {"index": dst_index, "alias": alias}})
        return actions


class ReindexJob(Job):
    """Reindex job that uses Elasticsearch's reindex API."""

    def run(self):
        """Fetch source index using ES Reindex API."""
        pid_type = self.config['pid_type']
        print('[*] running reindex for pid type: {}'.format(pid_type))
        reindex_params = self.config.get('reindex_params', {})
        source_params = reindex_params.pop('source', {})
        dest_params = reindex_params.pop('dest', {})

        state = self.state.read()
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
        wait_for_completion = self.config.get('wait_for_completion', False)
        response = current_search_client.reindex(
            wait_for_completion=wait_for_completion,
            body=payload
        )
        state['stats']['total'] = self.migration.src_es_client.client.count(
            index=state['src']['index'])
        state['last_record_update'] = str(start_date)
        task_id = response.get('task', None)
        state['reindex_task_id'] = task_id
        if wait_for_completion:
            if response.get('timed_out') or len(response['failures']) > 0:
                state['status'] = 'FAILED'
            else:
                state['threshold_reached'] = True
                state['status'] = 'COMPLETED'
        self.state.commit(state)
        print('reindex task started: {}'.format(task_id))
        return state

    def cancel(self):
        """Cancel reindexing job."""
        state = self.state.read()
        task_id = state['reindex_task_id']
        cancel_response = current_search_client.tasks.cancel(task_id)
        if cancel_response.get('timed_out') or \
           len(cancel_response.get('failures', [])) > 0:
            state['status'] = 'FAILED'
        else:
            state['status'] = 'CANCELLED'

        self.state.commit(state)
        if 'node_failures' in cancel_response:
            print('failed to cancel task', cancel_response)
        else:
            print('- successfully cancelled task: {}'.format(task_id))

    def initial_state(self, dry_run=False):
        """Build job's initial state."""
        def get_src(name, prefix):
            index_name = None
            src_alias_name = build_alias_name(name, prefix=prefix)
            if old_client.index_exists(src_alias_name):
                index_name = src_alias_name
                if old_client.alias_exists(
                        alias=src_alias_name):
                    indexes = list(old_client.get_indexes_from_alias(
                        alias=src_alias_name).keys())
                    if len(indexes) > 1:
                        raise Exception(
                            'Multiple indexes found for alias {}.'.format(
                                src_alias_name))
                    index_name = indexes[0]
            else:
                raise Exception(
                    "alias or index ({}) doesn't exist".format(src_alias_name)
                )
            return dict(
                index=index_name,
            )

        def find_aliases_for_index(index_name, aliases):
            """Find all aliases for a given index."""
            if isinstance(aliases, str):
                return None
            for key, values in aliases.items():
                if key == index_name:
                    return [build_alias_name(key)]
                else:
                    # TODO: refactoring
                    found_aliases = find_aliases_for_index(index_name, values)
                    if isinstance(found_aliases, list):
                        found_aliases.append(build_alias_name(key))
                        return found_aliases

        def get_dst(name):
            dst_index = name
            mapping_fp = current_search.mappings[name]
            dst_index_aliases = find_aliases_for_index(
                name, current_search.aliases) or []
            return dict(
                index=dst_index,
                aliases=dst_index_aliases,
                mapping=mapping_fp,
                doc_type=extract_doctype_from_mapping(mapping_fp),
            )

        old_client = self.migration.src_es_client.client
        index = self.config['index']
        initial_state = dict(
            type="job",
            name=self.name,
            status='INITIAL',
            migration_id=self.name,
            config=self.config,
            pid_type=self.config['pid_type'],
            src=get_src(
                index, self.migration.src_es_client.config.get('prefix')),
            dst=get_dst(index),
            last_record_update=None,
            reindex_task_id=None,
            threshold_reached=False,
            rollover_threshold=self.config['rollover_threshold'],
            rollover_ready=False,
            rollover_finished=False,
            stats={},
            reindex_params=self.config.get('reindex_params', {})
        )
        return initial_state


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
            _dst = self.state.read()['dst']
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
        state = self.state.read()
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
            start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f')
            # Fetch data from start_time from db
            indexer = MigrationIndexer()

            # Send indexer actions to special reindex queue
            start_date = datetime.utcnow()
            indexer._bulk_op(self.iter_indexer_ops(start_time), None)
            last_record_update = str(start_date)
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
        if self.state.read()['reindex_task_id']:
            return self.run_delta_job()
        else:
            if self.state.read()['status'] != 'COMPLETED':
                return super(ReindexAndSyncJob, self).run()

    def cancel(self):
        """Cancel reinding and syncing job."""
        super(ReindexAndSyncJob, self).cancel()


# FIXME: extract common code into parent class methods
class MultiIndicesReindexJob(Job):
    """Reindex job that uses Elasticsearch's reindex API."""

    def run(self):
        """Fetch source index using ES Reindex API."""
        print('[*] running reindex for templates')
        reindex_params = self.config.get('reindex_params', {})
        source_params = reindex_params.pop('source', {})
        dest_params = reindex_params.pop('dest', {})

        state = self.state.read()
        payload = dict(
            source=dict(
                index=state['src']['index'],
                **source_params
            ),
            dest=dict(
                index=state['dst']['index'],
                version_type='external_gte',
                **dest_params
            ),
            **reindex_params
        )
        if self.migration.strategy == self.migration.CROSS_CLUSTER_STRATEGY:
            payload['source']['remote'] = \
                self.migration.src_es_client.reindex_remote

        # Make sure needed templates are there

        # Keep track of the time we issued the reindex command
        start_date = datetime.utcnow()
        # Reindex using ES Reindex API synchronously
        wait_for_completion = self.config.get('wait_for_completion', False)
        response = current_search_client.reindex(
            wait_for_completion=wait_for_completion,
            body=payload
        )
        state['stats']['total'] = self.migration.src_es_client.client.count(
            index=state['src']['index'])
        state['last_record_update'] = str(start_date)
        task_id = response.get('task', None)
        state['reindex_task_id'] = task_id
        if wait_for_completion:
            if response.get('timed_out') or len(response['failures']) > 0:
                state['status'] = 'FAILED'
            else:
                state['threshold_reached'] = True
                state['status'] = 'COMPLETED'
        self.state.commit(state)
        print('reindex task started: {}'.format(task_id))
        return state

    def cancel(self):
        """Cancel reindexing job."""
        state = self.state.read()
        task_id = state['reindex_task_id']
        cancel_response = current_search_client.tasks.cancel(task_id)
        if cancel_response.get('timed_out') or \
            len(cancel_response.get('failures', [])) > 0:
            state['status'] = 'FAILED'
        else:
            state['status'] = 'CANCELLED'

        self.state.commit(state)
        if 'node_failures' in cancel_response:
            print('failed to cancel task', cancel_response)
        else:
            print('- successfully cancelled task: {}'.format(task_id))

    def initial_state(self, dry_run=False):
        """Build job's initial state."""
        old_client = self.migration.src_es_client.client
        index = self.config['index']
        prefix = self.config.get('src_es_client', {}).get('prefix')

        reindex_params = self.config.get('reindex_params', {})
        source_params = reindex_params.pop('source', {})
        source_index = source_params.get('index') or index
        dest_params = reindex_params.pop('dest', {})
        dest_index = dest_params.get('index') or index

        if prefix:
            if isinstance(source_index, six.string_types):
                source_index = prefix + source_index
            source_indices = []
            for sindex in source_index:
                source_indices.append(prefix + sindex)
            source_index = source_indices

        initial_state = dict(
            type="job",
            name=self.name,
            status='INITIAL',
            migration_id=self.name,
            config=self.config,
            pid_type=self.config['pid_type'],
            src=dict(
                index=source_index,
            ),
            dst=dict(index=dest_index),
            last_record_update=None,
            reindex_task_id=None,
            threshold_reached=False,
            rollover_threshold=self.config['rollover_threshold'],
            rollover_ready=False,
            rollover_finished=False,
            stats={},
            reindex_params=self.config.get('reindex_params', {})
        )
        return initial_state

    def create_index(self, index):
        # Only templates need to bre created
        current_search.put_templates(ignore=[400, 404])

    def rollover_actions(self):
        # TODO: Investigate for in-cluster migrations what kind of rollover
        # actions are needed
        return []
