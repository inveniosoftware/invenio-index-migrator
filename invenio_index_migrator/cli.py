# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Index syncing CLI commands."""

import click

from flask import current_app
from flask.cli import with_appcontext
from invenio_search.cli import index as index_cmd
from .proxies import current_index_migrator
from .utils import obj_or_import_string


@index_cmd.group()
def sync():
    """Manage index syncing."""
    pass


@sync.command('init')
@with_appcontext
@click.argument('job_id')
@click.option('--yes-i-know', is_flag=True)
def init_sync(job_id, yes_i_know):
    """Initialize index syncing."""
    job = current_index_migrator.jobs[job_id]
    sync_job = job['cls'](**job['params'])

    recipe = sync_job.init(dry_run=True)
    click.secho(
        '******* Information collected for this migration *******', fg='green')
    for pid_type, mapping in recipe.items():
        dst = mapping['dst']
        click.secho('****************************', fg='green')
        click.echo('For pid_type: {}'.format(pid_type))
        click.echo('Index: {}'.format(dst['index']))
        click.echo('Aliases: {}'.format(dst['aliases']))
        click.echo('Mapping file path: {}'.format(dst['mapping']))
        click.echo('Doc type: {}'.format(dst['doc_type']))

    confirm = yes_i_know or click.confirm(
        'Are you sure you want to apply this recipe?',
        default=False, abort=True
    )
    if confirm:
        sync_job.init()
    else:
        click.echo('Aborting migration...')


@sync.command('run')
@with_appcontext
@click.argument('job_id')
def run_sync(job_id):
    """Run current index syncing."""
    job = current_index_migrator.jobs[job_id]
    sync_job = job['cls'](**job['params'])
    sync_job.run()


@sync.command('rollover')
def rollover_sync():
    """Perform the index syncing rollover action."""
    pass


@sync.command('status')
def status_sync():
    """Get current index syncing status."""
    pass


@sync.command('cancel')
def cancel_sync():
    """Cancel the current index syncing."""
    pass
