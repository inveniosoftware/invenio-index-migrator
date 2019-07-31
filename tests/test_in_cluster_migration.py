import json
import os
import tempfile

import pytest
from click.testing import CliRunner
from invenio_search import current_search, current_search_client

from invenio_index_migrator import current_index_migrator
from invenio_index_migrator.cli import init_migration, rollover_sync, \
    run_migration


def assert_indices_exists(exists, not_exists):
    for index in exists:
        assert current_search_client.indices.exists(index)
    for index in not_exists:
        assert not current_search_client.indices.exists(index)


def test_basic_in_cluster_migration(in_cluster_app, testdata):
    """Test a basic in-cluster migration."""
    recipe_id = 'my_recipe'
    in_cluster_app.config['SEARCH_INDEX_PREFIX'] = 'new-'
    old_suffix = current_search.current_suffix
    current_search._current_suffix = '-new'
    records_mapping_file, mapping_filepath = tempfile.mkstemp()
    with open(current_search.mappings['authors-author-v1.0.0']) as old:
        data = json.load(old)
    data['mappings']['properties']['author_id']['type'] = 'text'
    with open(mapping_filepath, 'w') as new:
        new.write(json.dumps(data))

    assert_indices_exists(
        exists=[
            'old-records-record-v1.0.0{}'.format(old_suffix),
            'old-authors-author-v1.0.0{}'.format(old_suffix),
        ],
        not_exists=[
            'new-.invenio-index-migrator',
            'new-records-record-v1.0.0-new',
            'new-authors-author-v1.0.0-new',
        ]
    )

    runner = in_cluster_app.test_cli_runner()
    init_result = runner.invoke(init_migration, [recipe_id, '--yes-i-know'])

    assert_indices_exists(
        exists=[
            'old-records-record-v1.0.0{}'.format(old_suffix),
            'old-authors-author-v1.0.0{}'.format(old_suffix),
            'new-.invenio-index-migrator',
            'new-records-record-v1.0.0-new',
            'new-authors-author-v1.0.0-new',
        ],
        not_exists=[
            'new-records-record-v1.0.0',
        ]
    )

    # TODO: assert new indexes are created and no aliases
    migrate_result = runner.invoke(run_migration, [recipe_id])

    assert_indices_exists(
        exists=[
            'old-records-record-v1.0.0{}'.format(old_suffix),
            'old-authors-author-v1.0.0{}'.format(old_suffix),
            'new-.invenio-index-migrator',
            'new-records-record-v1.0.0-new',
            'new-authors-author-v1.0.0-new',
        ],
        not_exists=[
            'new-records-record-v1.0.0',
        ]
    )

    migrate_result = runner.invoke(run_migration, [recipe_id])

    recipe_doc = current_search_client.get(
        index='new-.invenio-index-migrator',
        id='my_recipe'
    )
    assert recipe_doc['_source']['status'] == 'COMPLETED'

    rollover_result = runner.invoke(rollover_sync, [recipe_id])

    assert_indices_exists(
        exists=[
            'old-records-record-v1.0.0{}'.format(old_suffix),
            'old-authors-author-v1.0.0{}'.format(old_suffix),
            'new-.invenio-index-migrator',
            'new-records-record-v1.0.0-new',
            'new-authors-author-v1.0.0-new',
            'new-records-record-v1.0.0',
        ],
        not_exists=[]
    )

    os.close(records_mapping_file)
