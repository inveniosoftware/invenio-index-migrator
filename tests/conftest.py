# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.


"""Pytest configuration."""

from __future__ import absolute_import, print_function

import json
import os
import shutil
import sys
import tempfile

import mock
import pytest
from flask import Flask, current_app
from invenio_app.factory import create_app as create_app_factory
from invenio_db import InvenioDB, db
from invenio_indexer import InvenioIndexer
from invenio_indexer.api import RecordIndexer
from invenio_pidstore.minters import recid_minter
from invenio_pidstore.providers.recordid import RecordIdProvider
from invenio_records import InvenioRecords
from invenio_records.api import Record
from invenio_search import InvenioSearch
from sqlalchemy_utils.functions import create_database, database_exists, \
    drop_database

from invenio_index_migrator import InvenioIndexMigrator
from invenio_index_migrator.api import Migration

sys.path.append(
    os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                 'tests/mock_module'))


def load_json_from_datadir(filename):
    """Load JSON from dir."""
    _data_dir = os.path.join(os.path.dirname(__file__), "data")
    with open(os.path.join(_data_dir, filename), "r") as fp:
        return json.load(fp)


@pytest.fixture(scope='module')
def in_cluster_app_config(app_config):
    """In-cluster app config fixture."""
    app_config['INDEX_MIGRATOR_RECIPES'] = dict(
        my_recipe=dict(
            cls='invenio_index_migrator.api.Migration',
            params=dict(
                strategy=Migration.IN_CLUSTER_STRATEGY,
                src_es_client=dict(
                    prefix='old-',
                    version=7,
                    params=dict(
                        host='localhost',
                        port=9200,
                        use_ssl=False,
                    ),
                ),
                jobs=dict(
                    reindex_author_job=dict(
                        cls='invenio_index_migrator.api.job.ReindexAndSyncJob',
                        pid_type='authid',
                        index='authors-author-v1.0.0',
                        rollover_threshold=10,
                        refresh_interval=None,
                        wait_for_completion=True
                    ),
                    reindex_record_job=dict(
                        cls='invenio_index_migrator.api.job.ReindexAndSyncJob',
                        pid_type='recid',
                        index='records-record-v1.0.0',
                        rollover_threshold=10,
                        refresh_interval=None,
                        wait_for_completion=True
                    )
                )
            )
        )
    )
    app_config['SEARCH_ELASTIC_HOSTS'] = [
        dict(host='localhost', port=9200)
    ]
    app_config['SEARCH_INDEX_PREFIX'] = 'old-'
    app_config['PIDSTORE_RECID_FIELD'] = 'recid'
    return app_config


@pytest.fixture()
def in_cluster_app(request, in_cluster_app_config):
    """Initialize InvenioRecords."""
    app = Flask('in_cluster_testapp')
    app.config.update(in_cluster_app_config)
    InvenioDB(app)
    InvenioRecords(app)
    search = InvenioSearch(app)
    InvenioIndexer(app)
    InvenioIndexMigrator(app)

    search.register_mappings('records', 'mock_module.mappings')
    search.register_mappings('authors', 'mock_module.mappings')

    with app.app_context():
        if not database_exists(str(db.engine.url)):
            create_database(str(db.engine.url))
        db.create_all()
        list(search.create())
        search.flush_and_refresh('*')

    def teardown():
        with app.app_context():
            db.drop_all()
            list(search.delete(ignore=[404]))
            search.client.indices.delete('*')

    request.addfinalizer(teardown)
    with app.app_context():
        yield app


def record_minter(record_uuid, data):
    """Minter for records."""
    pid_field = current_app.config['PIDSTORE_RECID_FIELD']
    assert pid_field not in data
    provider = RecordIdProvider.create(
        object_type='rec', object_uuid=record_uuid, pid_type='recid')
    data[pid_field] = provider.pid.pid_value
    return provider.pid


def author_minter(record_uuid, data):
    """Minter for authors."""
    pid_field = current_app.config['PIDSTORE_RECID_FIELD']
    assert pid_field not in data
    provider = RecordIdProvider.create(
        object_type='rec', object_uuid=record_uuid, pid_type='authid')
    data[pid_field] = provider.pid.pid_value
    return provider.pid


@pytest.fixture()
def testdata(in_cluster_app):
    """Create, index and return test data."""
    indexer = RecordIndexer()

    filenames = ("records.json", "authors.json")
    with mock.patch('invenio_records.api.Record.validate',
                    return_value=None):
        records = load_json_from_datadir('records.json')
        for record in records:
            record = Record.create(record)
            record_minter(record.id, record)
            record.commit()
            db.session.commit()
            indexer.index(record)
        authors = load_json_from_datadir('authors.json')
        for record in authors:
            record = Record.create(record)
            author_minter(record.id, record)
            record.commit()
            db.session.commit()
            indexer.index(record)
