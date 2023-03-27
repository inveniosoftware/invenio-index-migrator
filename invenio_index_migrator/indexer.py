# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Indexer class for syncing."""

from __future__ import absolute_import, print_function

from invenio_indexer.api import RecordIndexer
from invenio_records.api import Record
from invenio_records.models import RecordMetadata
from kombu import Exchange, Queue

SYNC_INDEXER_MQ_EXCHANGE = Exchange('sync-indexer', type='direct')
"""Default exchange for message queue."""

SYNC_INDEXER_MQ_QUEUE = Queue(
    'sync-indexer', exchange=SYNC_INDEXER_MQ_EXCHANGE, routing_key='sync-indexer')

SYNC_INDEXER_MQ_ROUTING_KEY = 'sync-indexer'
"""Default routing key for message queue."""


class MigrationIndexer(RecordIndexer):
    """Indexer class for ES syncing module."""

    def __init__(self, **kwargs):
        """Initialize the migration indexer."""
        self._queue = SYNC_INDEXER_MQ_QUEUE
        self._routing_key = SYNC_INDEXER_MQ_ROUTING_KEY
        self._exchange = SYNC_INDEXER_MQ_EXCHANGE
        super(MigrationIndexer, self).__init__(
            queue=self._queue,
            routing_key=self._routing_key,
            exchange=self._exchange,
            **kwargs)

    #
    # Low-level implementation
    #
    def _bulk_op(self, bulk_ops_iterator, op_type, **kwargs):
        """Index record in Elasticsearch asynchronously.

        :param bulk_ops_iterator: Iterator that yields dictionaries with ``op``
            (``create`` or ``delete``) and ``id`` values.
        :param kwargs: Not used.
        """
        _ = op_type
        with self.create_producer() as producer:
            for op_payload in bulk_ops_iterator:
                producer.publish(op_payload)

    def _get_record(self, payload):
        """Return record to sync."""
        id_ = payload['id']
        model = RecordMetadata.query.filter_by(id=id_).one()
        return Record(data=model.json, model=model)

    def _delete_action(self, payload):
        """Bulk delete action.

        :param payload: Decoded message body.
        :returns: Dictionary defining an Elasticsearch bulk 'delete' action.
        """
        index = payload.get('index')
        return {
            '_op_type': 'delete',
            '_index': index,
            '_id': payload['id'],
        }

    def _index_action(self, payload):
        """Bulk index action.

        :param payload: Decoded message body.
        :returns: Dictionary defining an Elasticsearch bulk 'index' action.
        """
        record = self._get_record(payload)
        index = payload.get('index')

        arguments = {}
        body = self._prepare_record(record, index, '_doc', arguments)

        action = {
            '_op_type': 'index',
            '_index': index,
            '_id': str(record.id),
            '_version': record.revision_id,
            '_version_type': self._version_type,
            '_source': body
        }
        action.update(arguments)

        return action
